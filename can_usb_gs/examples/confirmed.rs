//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! Starts a `gs_usb` device with transmit confirmation for can-hal's
//! confirmed transmission and sends frames through [`ConfirmedTransmit`],
//! printing every [`Event`] in the order it comes: frames received, each
//! transmission's outcome, changes of bus state. The can-hal traits are
//! blocking, so this is a plain `main` with a tokio runtime alongside for
//! the driver's reader.
//!
//! ```text
//! cargo run --example confirmed -- [--serial <serial number>]
//!     [--bitrate 250000] [--count 5] [--listen 1] [--abort-after <ms>]
//! ```

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use can_hal::{CanFrame, CanId, ConfirmedTransmit, Event, ReceiveEvents, SamplePoint, TransmitId, TryTransmitError};
use can_usb_gs::{ConfirmedChannel, GsUsb, Settings, list_devices};
use clap::Parser;

/// Sends frames through can-hal's confirmed transmission and prints the
/// events that follow.
#[derive(Parser)]
struct Options {
    /// The device with this USB serial number (default: the first that opens).
    #[arg(long)]
    serial: Option<String>,
    /// The nominal bit rate, in bit/s.
    #[arg(long, default_value_t = 250_000)]
    bitrate: u32,
    /// How many frames to send.
    #[arg(long, default_value_t = 5)]
    count: u32,
    /// How long to go on receiving once every frame is reported, in seconds.
    #[arg(long, default_value_t = 1)]
    listen: u64,
    /// Abort a frame not reported within this many milliseconds.
    #[arg(long, value_name = "MS")]
    abort_after: Option<u64>,
}

/// Opens the device with `serial`, or the first that opens, and starts its
/// channel 0 for confirmed transmission.
async fn open(serial: Option<&str>, settings: &Settings) -> Result<(GsUsb, ConfirmedChannel), can_usb_gs::Error> {
    for info in list_devices().await? {
        if serial.is_some_and(|wanted| info.serial_number() != Some(wanted)) {
            continue;
        }
        let name = info.to_string();
        match GsUsb::open(info).await {
            Ok(device) => {
                println!("{name}");
                let channel = device.start_confirmed(0, settings).await?;
                return Ok((device, channel));
            }
            Err(e) => println!("{name}: {e}"),
        }
    }
    panic!("no gs_usb device could be opened")
}

fn main() -> Result<(), can_usb_gs::Error> {
    let options = Options::parse();
    let runtime = tokio::runtime::Runtime::new().expect("a tokio runtime");
    let settings = Settings::new(options.bitrate).with_sample_point(SamplePoint::PCT_87_5);
    // The device is kept open while the channel runs.
    let (_device, mut channel) = runtime.block_on(open(options.serial.as_deref(), &settings))?;
    println!("started at {} bit/s, confirmed, capacity {}", options.bitrate, ConfirmedTransmit::<CanFrame>::capacity(&channel));

    // Frames handed over and not yet reported, with when they were.
    let mut outstanding: BTreeMap<TransmitId, Instant> = BTreeMap::new();
    let id = CanId::new_standard(0x123).expect("11 bits");
    let mut sent = 0;
    let mut quiet_until: Option<Instant> = None;
    loop {
        while sent < options.count {
            let frame = CanFrame::new(id, &sent.to_be_bytes()).expect("4 bytes");
            match channel.try_transmit(&frame) {
                Ok(tid) => {
                    println!("tx {sent} handed over as {tid:?}");
                    outstanding.insert(tid, Instant::now());
                    sent += 1;
                }
                Err(TryTransmitError::Full) => break,
                Err(TryTransmitError::Backend(e)) => return Err(e),
            }
        }
        if let Some(ms) = options.abort_after {
            let late: Vec<TransmitId> = outstanding.iter().filter(|(_, at)| at.elapsed() >= Duration::from_millis(ms)).map(|(tid, _)| *tid).collect();
            for tid in late {
                match ConfirmedTransmit::<CanFrame>::abort(&mut channel, tid) {
                    Ok(()) => println!("{tid:?}: not reported within {ms} ms, abort asked"),
                    Err(e) => println!("{tid:?}: abort failed: {e:?}"),
                }
                // Asked once; its report is awaited as any other's.
                outstanding.insert(tid, Instant::now() + Duration::from_secs(3600));
            }
        }
        if sent == options.count && outstanding.is_empty() {
            let until = *quiet_until.get_or_insert_with(|| Instant::now() + Duration::from_secs(options.listen));
            if Instant::now() >= until {
                break;
            }
        }
        match ReceiveEvents::<CanFrame>::receive_event_timeout(&mut channel, Duration::from_millis(10))? {
            Some(Event::Transmitted { id: tid, outcome }) => {
                outstanding.remove(&tid);
                println!("{tid:?}: {outcome:?}");
            }
            Some(Event::Received(received)) => println!("rx {:?} {:02X?}", received.frame().id(), received.frame().data()),
            Some(Event::BusStateChanged(state)) => println!("bus {state:?}"),
            None => {}
        }
    }
    Ok(())
}
