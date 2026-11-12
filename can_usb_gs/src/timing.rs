//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! Bit timing: from a bit rate and sample point to the quanta a device's
//! limits allow.

use gs_usb_protocol::{BtConst, BtConstExt, DeviceBittiming};

/// The ranges one phase's timing fields may take, from [`BtConst`] or the
/// data half of [`BtConstExt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Limits {
    pub tseg1: (u32, u32),
    pub tseg2: (u32, u32),
    pub sjw_max: u32,
    pub brp: (u32, u32),
    pub brp_inc: u32,
}

impl Limits {
    pub fn nominal(bt: &BtConst) -> Self {
        Self {
            tseg1: (bt.tseg1_min, bt.tseg1_max),
            tseg2: (bt.tseg2_min, bt.tseg2_max),
            sjw_max: bt.sjw_max,
            brp: (bt.brp_min, bt.brp_max),
            brp_inc: bt.brp_inc,
        }
    }

    pub fn data(ext: &BtConstExt) -> Self {
        Self {
            tseg1: (ext.dtseg1_min, ext.dtseg1_max),
            tseg2: (ext.dtseg2_min, ext.dtseg2_max),
            sjw_max: ext.dsjw_max,
            brp: (ext.dbrp_min, ext.dbrp_max),
            brp_inc: ext.dbrp_inc,
        }
    }
}

/// The timing for exactly `bitrate` from a `clock_hz` controller clock, with
/// the sample point (in per mille) nearest `sample_point`, or `None` if no
/// prescaler divides the clock to that rate within `limits`.
///
/// It takes the most quanta per bit (the smallest prescaler) whose sample
/// point is within [`TOLERANCE`] of the one asked for, or failing that the
/// nearest sample point. The first segment is split between propagation and
/// phase 1 as Linux does (propagation half, rounded down), and the
/// synchronisation jump width is Linux's default: half of phase 2, at least
/// 1, no more than phase 1 or the device allows.
/// How far, in per mille, a sample point may be from the one asked for and
/// still be taken for having more quanta.
pub(crate) const TOLERANCE: u64 = 10;

pub(crate) fn solve(clock_hz: u32, bitrate: u32, sample_point: u16, limits: &Limits) -> Option<DeviceBittiming> {
    if bitrate == 0 {
        return None;
    }
    let sample_point = u64::from(sample_point);
    let inc = limits.brp_inc.max(1);
    let first = limits.brp.0.max(1).div_ceil(inc) * inc;
    // The nearest so far: (error numerator over quanta, quanta, tseg1, tseg2, brp).
    let mut nearest: Option<(u64, u64, u32, u32, u32)> = None;
    for brp in (first..=limits.brp.1).step_by(inc as usize) {
        let per_bit = u64::from(brp) * u64::from(bitrate);
        if u64::from(clock_hz) % per_bit != 0 {
            continue;
        }
        let Ok(quanta) = u32::try_from(u64::from(clock_hz) / per_bit) else { continue };
        let Some((tseg1, tseg2)) = split(quanta, sample_point, limits) else { continue };
        // |sample point - wanted| = |1000 * (1 + tseg1) - wanted * quanta| / (1000 * quanta)
        let error = (1000 * u64::from(1 + tseg1)).abs_diff(sample_point * u64::from(quanta));
        let candidate = (error, u64::from(quanta), tseg1, tseg2, brp);
        // The prescaler rises, so the first within tolerance has the most quanta.
        if error <= TOLERANCE * u64::from(quanta) {
            nearest = Some(candidate);
            break;
        }
        if nearest.is_none_or(|(best_error, best_quanta, ..)| error * best_quanta < best_error * u64::from(quanta)) {
            nearest = Some(candidate);
        }
    }
    nearest.map(|(_, _, tseg1, tseg2, brp)| {
        let prop_seg = tseg1 / 2;
        let phase_seg1 = tseg1 - prop_seg;
        DeviceBittiming {
            prop_seg,
            phase_seg1,
            phase_seg2: tseg2,
            sjw: (tseg2 / 2).max(1).min(phase_seg1).min(limits.sjw_max),
            brp,
        }
    })
}

/// The two segments of a `quanta`-long bit (one quantum for sync) with the
/// sample point nearest `sample_point` per mille, within `limits`.
fn split(quanta: u32, sample_point: u64, limits: &Limits) -> Option<(u32, u32)> {
    let (tseg1_min, tseg1_max) = limits.tseg1;
    let (tseg2_min, tseg2_max) = limits.tseg2;
    if quanta < 1 + tseg1_min + tseg2_min || quanta > 1 + tseg1_max + tseg2_max {
        return None;
    }
    // The quanta up to the sample point, rounded to nearest.
    let before = u32::try_from((u64::from(quanta) * sample_point + 500) / 1000).ok()?;
    let tseg1 = before.saturating_sub(1).clamp(tseg1_min, tseg1_max);
    let tseg2 = (quanta - 1 - tseg1).clamp(tseg2_min, tseg2_max);
    let tseg1 = quanta - 1 - tseg2;
    (tseg1_min..=tseg1_max).contains(&tseg1).then_some((tseg1, tseg2))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ferrocan's bxCAN limits (the SH-C30A), and candleLight's.
    const BXCAN: Limits = Limits {
        tseg1: (1, 16),
        tseg2: (1, 8),
        sjw_max: 4,
        brp: (1, 1024),
        brp_inc: 1,
    };

    /// A wide FD controller's nominal limits, as ST's FDCAN.
    const FDCAN: Limits = Limits {
        tseg1: (1, 256),
        tseg2: (1, 128),
        sjw_max: 128,
        brp: (1, 512),
        brp_inc: 1,
    };

    const FDCAN_DATA: Limits = Limits {
        tseg1: (1, 32),
        tseg2: (1, 16),
        sjw_max: 16,
        brp: (1, 32),
        brp_inc: 1,
    };

    fn quanta(bt: &DeviceBittiming) -> u32 {
        1 + bt.prop_seg + bt.phase_seg1 + bt.phase_seg2
    }

    fn rate(clock: u32, bt: &DeviceBittiming) -> u32 {
        clock / (bt.brp * quanta(bt))
    }

    fn per_mille(bt: &DeviceBittiming) -> u32 {
        1000 * (1 + bt.prop_seg + bt.phase_seg1) / quanta(bt)
    }

    #[test]
    fn bxcan_250k_at_87_5() {
        let bt = solve(48_000_000, 250_000, 875, &BXCAN).unwrap();
        assert_eq!(rate(48_000_000, &bt), 250_000);
        assert_eq!(per_mille(&bt), 875);
        // 16 quanta, the most that reach 87.5% within tseg1 16.
        assert_eq!((bt.brp, quanta(&bt)), (12, 16));
        assert_eq!((bt.prop_seg, bt.phase_seg1, bt.phase_seg2, bt.sjw), (6, 7, 2, 1));
    }

    #[test]
    fn bxcan_500k_and_1m() {
        for bitrate in [125_000, 500_000, 1_000_000] {
            let bt = solve(48_000_000, bitrate, 875, &BXCAN).unwrap();
            assert_eq!(rate(48_000_000, &bt), bitrate);
            assert_eq!(per_mille(&bt), 875, "{bitrate}");
        }
    }

    #[test]
    fn most_quanta_within_tolerance() {
        // 70% at 1 Mbit/s from 48 MHz: 24 quanta give 70.8%, within 1%.
        let bt = solve(48_000_000, 1_000_000, 700, &BXCAN).unwrap();
        assert_eq!((bt.brp, quanta(&bt), per_mille(&bt)), (2, 24, 708));
        // 87.5% at 250 kbit/s: 24 quanta reach only 70.8% (tseg1 16), so 16.
        let bt = solve(48_000_000, 250_000, 875, &BXCAN).unwrap();
        assert_eq!(quanta(&bt), 16);
    }

    #[test]
    fn nearest_when_none_within_tolerance() {
        // tseg1 at most 8: 16 quanta reach 56%, 12 reach 75%, 8 make 87.5%.
        let short = Limits { tseg1: (1, 8), ..BXCAN };
        let bt = solve(48_000_000, 250_000, 875, &short).unwrap();
        assert_eq!((quanta(&bt), per_mille(&bt)), (8, 875));
        // tseg1 at most 4 and tseg2 at least 2: nothing within 1%; of 12
        // (41.7%), 8 (62.5%), 6 (66.7%) and 4 quanta (50%), 6 is nearest.
        let tight = Limits { tseg1: (1, 4), tseg2: (2, 8), ..BXCAN };
        let bt = solve(48_000_000, 250_000, 875, &tight).unwrap();
        assert_eq!((quanta(&bt), per_mille(&bt)), (6, 666));
    }

    #[test]
    fn fd_nominal_and_data() {
        // The SH-C31A's 170 MHz. 500 kbit/s at 87.5%: 340 quanta at brp 1
        // reach only 75.6% (tseg1 256); 170 at brp 2 give 87.6%.
        let nominal = solve(170_000_000, 500_000, 875, &FDCAN).unwrap();
        assert_eq!(rate(170_000_000, &nominal), 500_000);
        assert_eq!((nominal.brp, quanta(&nominal), per_mille(&nominal)), (2, 170, 876));
        // 2 Mbit/s at 80%: 85 quanta per bit split only as 5 x 17; 17
        // quanta give 82.4%, outside 1%, so 5 quanta at 80% exactly.
        let data = solve(170_000_000, 2_000_000, 800, &FDCAN_DATA).unwrap();
        assert_eq!(rate(170_000_000, &data), 2_000_000);
        assert_eq!((data.brp, quanta(&data), per_mille(&data)), (17, 5, 800));
        // 5 Mbit/s: 34 quanta at brp 1, 79.4%.
        let fast = solve(170_000_000, 5_000_000, 800, &FDCAN_DATA).unwrap();
        assert_eq!((fast.brp, quanta(&fast), per_mille(&fast)), (1, 34, 794));
    }

    #[test]
    fn refuses_what_cannot_be_reached() {
        // 48 MHz doesn't divide to 83.3 kbit/s exactly.
        assert_eq!(solve(48_000_000, 83_333, 875, &BXCAN), None);
        assert_eq!(solve(48_000_000, 0, 875, &BXCAN), None);
        // Faster than four quanta at brp 1 allow.
        assert_eq!(solve(8_000_000, 4_000_000, 875, &BXCAN), None);
    }

    #[test]
    fn honours_brp_steps_and_limits() {
        let stepped = Limits { brp_inc: 4, brp: (2, 64), ..BXCAN };
        let bt = solve(48_000_000, 250_000, 875, &stepped).unwrap();
        assert_eq!(bt.brp % 4, 0);
        assert_eq!(rate(48_000_000, &bt), 250_000);
        // tseg2 at least 3: the sample point moves earlier.
        let long_tseg2 = Limits { tseg2: (3, 8), ..BXCAN };
        let bt = solve(48_000_000, 250_000, 875, &long_tseg2).unwrap();
        assert!(bt.phase_seg2 >= 3);
        assert!(bt.sjw <= bt.phase_seg2 && bt.sjw >= 1);
    }
}
