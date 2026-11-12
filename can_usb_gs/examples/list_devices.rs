//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! Lists the `gs_usb` devices attached, with each channel's features, clock,
//! timing limits and state.

use can_usb_gs::protocol::feature;
use can_usb_gs::{GsUsb, list_devices};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    for info in list_devices().await.expect("Failed to list devices") {
        println!("{info}");
        let device = match GsUsb::open(info).await {
            Ok(device) => device,
            Err(e) => {
                println!("  can't open: {e}");
                continue;
            }
        };
        let config = device.config();
        let (interface, bulk_in, bulk_out) = device.endpoints();
        println!("  interface {interface}, bulk IN {bulk_in:#04x}, OUT {bulk_out:#04x}");
        println!("  firmware {}, hardware {}, {} channel(s)", config.sw_version(), config.hw_version(), config.channels());
        match device.timestamp().await {
            Ok(us) => println!("  timer {us} µs"),
            Err(e) => println!("  timer: {e}"),
        }
        for channel in 0..config.channels() {
            let bt = match device.bt_const(channel).await {
                Ok(bt) => bt,
                Err(e) => {
                    println!("  channel {channel}: {e}");
                    continue;
                }
            };
            println!("  channel {channel}: clock {} Hz, features {:#06x} ({})", bt.fclk_can, bt.feature, features(bt.feature));
            println!(
                "    nominal: tseg1 {}-{}, tseg2 {}-{}, sjw ≤ {}, brp {}-{} step {}",
                bt.tseg1_min, bt.tseg1_max, bt.tseg2_min, bt.tseg2_max, bt.sjw_max, bt.brp_min, bt.brp_max, bt.brp_inc
            );
            if bt.feature & feature::BT_CONST_EXT != 0 {
                match device.bt_const_ext(channel).await {
                    Ok(ext) => println!(
                        "    data: tseg1 {}-{}, tseg2 {}-{}, sjw ≤ {}, brp {}-{} step {}",
                        ext.dtseg1_min, ext.dtseg1_max, ext.dtseg2_min, ext.dtseg2_max, ext.dsjw_max, ext.dbrp_min, ext.dbrp_max, ext.dbrp_inc
                    ),
                    Err(e) => println!("    data: {e}"),
                }
            }
            if bt.feature & feature::GET_STATE != 0 {
                match device.state(channel).await {
                    Ok(state) => println!("    state {}, rx errors {}, tx errors {}", state.state, state.rxerr, state.txerr),
                    Err(e) => println!("    state: {e}"),
                }
            }
        }
    }
}

/// The feature bits' names.
fn features(bits: u32) -> String {
    const NAMES: [&str; 14] = [
        "listen-only",
        "loopback",
        "triple-sample",
        "one-shot",
        "hw-timestamp",
        "identify",
        "user-id",
        "pad-pkts",
        "fd",
        "lpc546xx-quirk",
        "bt-const-ext",
        "termination",
        "berr-reporting",
        "get-state",
    ];
    NAMES.iter().enumerate().filter(|(bit, _)| bits & (1 << bit) != 0).map(|(_, name)| *name).collect::<Vec<_>>().join(" ")
}
