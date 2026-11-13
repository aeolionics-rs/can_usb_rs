//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! Starts the first `gs_usb` device that opens (others may be in use; with
//! `--fd`, the first with CAN FD; with `--serial`, the one with that USB
//! serial number), sends some frames, waiting for each one's echo, and
//! prints what it receives, error frames decoded.
//!
//! ```text
//! cargo run --example bus -- [--serial <serial number>] [--bitrate 250000]
//!     [--count 10] [--listen 5] [--fd <data bitrate>] [--one-shot]
//!     [--listen-only] [--bus-errors] [--abort-after <ms>]
//! ```
//!
//! `--help` describes each option.
//!
//! Frames are standard ID 0x123 with a counter in the data, sampled at 87.5%.
//! With `--fd` (the data phase sampled at 75%, as Linux does above
//! 800 kbit/s) they are FD with bit rate switching, cycling through the FD
//! lengths, 0 to 64 bytes.

use std::time::Duration;

use can_hal::{BusStatus, CanFdFrame, CanFrame, CanId, Frame, SamplePoint};
use can_usb_gs::protocol::{can_err, feature};
use can_usb_gs::{ErrorFrame, Event, GsUsb, Receiver, Settings, list_devices};
use clap::Parser;

/// The FD data lengths, one per DLC.
const FD_LENGTHS: [usize; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64];

/// Sends frames on a `gs_usb` device, waiting for each one's echo, and
/// prints what it receives, error frames decoded.
#[derive(Parser)]
struct Options {
    /// The device with this USB serial number (default: the first that opens).
    #[arg(long)]
    serial: Option<String>,
    /// The nominal bit rate, in bit/s.
    #[arg(long, default_value_t = 250_000)]
    bitrate: u32,
    /// How many frames to send.
    #[arg(long, default_value_t = 10)]
    count: u32,
    /// How long to go on receiving after the frames are sent, in seconds.
    #[arg(long, default_value_t = 5)]
    listen: u64,
    /// CAN FD with bit rate switching, with this data bit rate, in bit/s.
    #[arg(long = "fd", value_name = "DATA_BITRATE")]
    data_bitrate: Option<u32>,
    /// Send each frame once, not again after a failure.
    #[arg(long)]
    one_shot: bool,
    /// Only listen: send nothing, acknowledge nothing.
    #[arg(long)]
    listen_only: bool,
    /// Report each bus error as an error frame.
    #[arg(long)]
    bus_errors: bool,
    /// Abort a frame not confirmed within this many milliseconds (a device
    /// with transmit confirmation).
    #[arg(long, value_name = "MS")]
    abort_after: Option<u64>,
}

/// An error frame in words, as Linux's `candump -e` would put it.
fn describe(error: &ErrorFrame) -> String {
    let names = [
        (can_err::LOSTARB, "lost-arbitration"),
        (can_err::CRTL, "controller"),
        (can_err::PROT, "protocol"),
        (can_err::ACK, "no-ack"),
        (can_err::BUSOFF, "bus-off"),
        (can_err::BUSERROR, "bus-error"),
        (can_err::RESTARTED, "restarted"),
        (can_err::CNT, "counters"),
    ];
    let mut words: Vec<String> = names.iter().filter(|(bit, _)| error.class & bit != 0).map(|(_, name)| (*name).to_string()).collect();
    if error.class & can_err::CRTL != 0 {
        let crtl = [
            (can_err::crtl::RX_OVERFLOW, "rx-overflow"),
            (can_err::crtl::RX_WARNING, "rx-warning"),
            (can_err::crtl::TX_WARNING, "tx-warning"),
            (can_err::crtl::RX_PASSIVE, "rx-passive"),
            (can_err::crtl::TX_PASSIVE, "tx-passive"),
            (can_err::crtl::ACTIVE, "active"),
        ];
        let set: Vec<&str> = crtl.iter().filter(|(bit, _)| error.data[1] & bit != 0).map(|(_, name)| *name).collect();
        words.push(format!("[{}]", set.join(" ")));
    }
    if error.class & can_err::PROT != 0 {
        let prot = [(can_err::prot::FORM, "form"), (can_err::prot::STUFF, "stuff"), (can_err::prot::BIT0, "bit0"), (can_err::prot::BIT1, "bit1")];
        let set: Vec<&str> = prot.iter().filter(|(bit, _)| error.data[2] & bit != 0).map(|(_, name)| *name).collect();
        words.push(format!("[{} at {:#04x}]", set.join(" "), error.data[3]));
    }
    if error.class & can_err::CNT != 0 {
        words.push(format!("tx {} rx {}", error.data[6], error.data[7]));
    }
    words.join(" ")
}

/// Prints what `rx` receives for `listen` seconds and a second more.
async fn receive(mut rx: Receiver, listen: u64) -> Receiver {
    let (mut frames, mut errors) = (0u32, 0u32);
    let deadline = tokio::time::sleep(Duration::from_secs(listen + 1));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            event = rx.next_event() => match event {
                Ok(Event::Received(received)) => {
                    frames += 1;
                    let micros = received.timestamp().device_micros();
                    match received.frame() {
                        Frame::Can(frame) => println!("rx {:?} {:02X?} at {micros:?} µs", frame.id(), frame.data()),
                        Frame::Fd(frame) => println!(
                            "rx FD{}{} {:?} [{}] {:02X?} at {micros:?} µs",
                            if frame.brs() { " BRS" } else { "" },
                            if frame.esi() { " ESI" } else { "" },
                            frame.id(),
                            frame.len(),
                            frame.data()
                        ),
                    }
                }
                Ok(Event::Error(error)) => {
                    errors += 1;
                    println!("error {} at {:?} µs; bus {:?}", describe(&error), error.timestamp.device_micros(), rx.bus_state());
                }
                Ok(other) => println!("rx {other:?}"),
                Err(e) => {
                    println!("rx ended: {e}");
                    break;
                }
            },
            () = &mut deadline => break,
        }
    }
    println!(
        "received {frames} frame(s), {errors} error frame(s); {} dropped, {} overflowed; bus {:?} {:?}",
        rx.dropped(),
        rx.overflows(),
        rx.bus_state(),
        rx.error_counters()
    );
    rx
}

#[tokio::main]
async fn main() -> Result<(), can_usb_gs::Error> {
    let options = Options::parse();
    let mut device = None;
    for info in list_devices().await? {
        if options.serial.as_deref().is_some_and(|wanted| info.serial_number() != Some(wanted)) {
            continue;
        }
        let name = info.to_string();
        match GsUsb::open(info).await {
            Ok(opened) if options.data_bitrate.is_some() && opened.bt_const(0).await?.feature & feature::FD == 0 => println!("{name}: no CAN FD"),
            Ok(opened) => {
                println!("{name}");
                device = Some(opened);
                break;
            }
            Err(e) => println!("{name}: {e}"),
        }
    }
    let device = device.unwrap_or_else(|| match &options.serial {
        Some(serial) => panic!("no gs_usb device with serial number {serial} could be opened"),
        None => panic!("no gs_usb device could be opened"),
    });
    let mut settings = Settings::new(options.bitrate).with_sample_point(SamplePoint::PCT_87_5);
    if let Some(data_bitrate) = options.data_bitrate {
        settings = settings.with_data_sample_point(data_bitrate, SamplePoint::PCT_75);
    }
    if options.one_shot {
        settings = settings.one_shot();
    }
    if options.listen_only {
        settings = settings.listen_only();
    }
    if options.bus_errors {
        settings = settings.bus_errors();
    }
    let (mut tx, rx) = device.start(0, &settings).await?;
    println!("started at {} bit/s, data {:?}, TX confirmation {}", options.bitrate, options.data_bitrate, tx.confirms_transmission());

    let receiving = tokio::spawn(receive(rx, options.listen));

    let id = CanId::new_standard(0x123).expect("11 bits");
    let mut previous = None;
    for n in 0..options.count {
        let frame = if options.data_bitrate.is_some() {
            let len = FD_LENGTHS[n as usize % FD_LENGTHS.len()];
            let data: Vec<u8> = (0..len).map(|i| n.to_le_bytes()[0].wrapping_add(i.to_le_bytes()[0])).collect();
            Frame::Fd(CanFdFrame::new(id, &data, true, false).expect("an FD length"))
        } else {
            Frame::Can(CanFrame::new(id, &n.to_be_bytes()).expect("4 bytes"))
        };
        // The device stops taking frames while its queue is full, as on a
        // bus where nothing acknowledges them.
        let confirmation = match tokio::time::timeout(Duration::from_secs(1), tx.send(&frame)).await {
            Ok(Ok(confirmation)) => confirmation,
            Ok(Err(e)) => {
                println!("tx {n}: {e}");
                continue;
            }
            Err(_) => {
                println!("tx {n}: not taken by the device within 1 s; no more sent");
                break;
            }
        };
        // With --abort-after, the confirmation if it came in time, else it
        // is aborted and waited for as without.
        let mut confirmation = confirmation;
        let early = match options.abort_after {
            Some(ms) => {
                if let Ok(result) = tokio::time::timeout(Duration::from_millis(ms), &mut confirmation).await {
                    Some(result)
                } else {
                    match tx.abort(&confirmation).await {
                        Ok(()) => println!("tx {n}: not confirmed within {ms} ms, abort asked"),
                        Err(e) => println!("tx {n}: abort failed: {e}"),
                    }
                    None
                }
            }
            None => None,
        };
        let outcome = match early {
            Some(result) => Ok(result),
            None => tokio::time::timeout(Duration::from_secs(1), confirmation).await,
        };
        match outcome {
            Ok(Ok(timestamp)) => {
                let micros = timestamp.device_micros();
                let gap = micros.zip(previous).map(|(now, then): (u64, u64)| now - then);
                println!("tx {n} echoed at {micros:?} µs (+{gap:?}); bus {:?}", tx.bus_state());
                previous = micros;
            }
            Ok(Err(e)) => println!("tx {n}: {e}"),
            Err(_) => println!("tx {n}: no echo within 1 s; bus {:?} {:?}", tx.bus_state(), tx.error_counters()),
        }
    }
    let _rx = receiving.await.expect("receiver task");
    // The counters now, not as of the last error frame.
    match device.state(0).await {
        Ok(state) => println!("device: {}, counters tx {} rx {}", state_name(state.state), state.txerr, state.rxerr),
        Err(e) => println!("device state: {e}"),
    }
    Ok(())
}

/// A `GET_STATE` state in words.
fn state_name(code: u32) -> String {
    use can_usb_gs::protocol::state;
    match code {
        state::ERROR_ACTIVE => "error active".into(),
        state::ERROR_WARNING => "error warning".into(),
        state::ERROR_PASSIVE => "error passive".into(),
        state::BUS_OFF => "bus-off".into(),
        state::STOPPED => "stopped".into(),
        state::SLEEPING => "sleeping".into(),
        other => format!("state {other}"),
    }
}
