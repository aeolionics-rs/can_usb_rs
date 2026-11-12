//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! Starts the first `gs_usb` device that opens (others may be in use) at a bit
//! rate, sends some frames, waiting for each one's echo, and prints what it
//! receives.
//!
//! `cargo run --example bus -- [bitrate] [count] [listen seconds]`, by default
//! 250000, 10 and 5. Frames are standard ID 0x123 with a counter in the data.

use std::time::Duration;

use can_hal::{BusStatus, CanFrame, CanId, Frame, SamplePoint};
use can_usb_gs::{Event, GsUsb, Settings, list_devices};

#[tokio::main]
async fn main() -> Result<(), can_usb_gs::Error> {
    let mut args = std::env::args().skip(1).map(|arg| arg.parse::<u32>().expect("a number"));
    let bitrate = args.next().unwrap_or(250_000);
    let count = args.next().unwrap_or(10);
    let listen = args.next().unwrap_or(5);

    let mut device = None;
    for info in list_devices().await? {
        let name = info.to_string();
        match GsUsb::open(info).await {
            Ok(opened) => {
                println!("{name}");
                device = Some(opened);
                break;
            }
            Err(e) => println!("{name}: {e}"),
        }
    }
    let device = device.expect("no gs_usb device could be opened");
    let settings = Settings::new(bitrate).with_sample_point(SamplePoint::PCT_87_5);
    let (mut tx, mut rx) = device.start(0, &settings).await?;
    println!("started at {bitrate} bit/s");

    let receiving = tokio::spawn(async move {
        let mut frames = 0u32;
        let deadline = tokio::time::sleep(Duration::from_secs(u64::from(listen) + 1));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                event = rx.next_event() => match event {
                    Ok(Event::Received(received)) => {
                        frames += 1;
                        let frame = received.frame();
                        println!("rx {:?} {:02X?} at {:?} µs", frame.id(), frame.data(), received.timestamp().device_micros());
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
        println!("received {frames} frame(s); {} dropped, {} overflowed; bus {:?} {:?}", rx.dropped(), rx.overflows(), rx.bus_state(), rx.error_counters());
        rx
    });

    let mut previous = None;
    for n in 0..count {
        let frame = CanFrame::new(CanId::new_standard(0x123).expect("11 bits"), &n.to_be_bytes()).expect("4 bytes");
        let confirmation = tx.send(&Frame::Can(frame)).await?;
        match tokio::time::timeout(Duration::from_secs(1), confirmation).await {
            Ok(Ok(timestamp)) => {
                let micros = timestamp.device_micros();
                let gap = micros.zip(previous).map(|(now, then): (u64, u64)| now - then);
                println!("tx {n} sent at {micros:?} µs (+{gap:?})");
                previous = micros;
            }
            Ok(Err(e)) => println!("tx {n}: {e}"),
            Err(_) => println!("tx {n}: no echo within 1 s; bus {:?} {:?}", tx.bus_state(), tx.error_counters()),
        }
    }
    let _rx = receiving.await.expect("receiver task");
    Ok(())
}
