//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! Starts the first `gs_usb` device that opens (others may be in use; with a
//! data bit rate, the first with CAN FD) at a bit rate, sends some frames,
//! waiting for each one's echo, and prints what it receives.
//!
//! `cargo run --example bus -- [bitrate] [count] [listen seconds] [data
//! bitrate]`, by default 250000, 10, 5 and none. Frames are standard ID 0x123
//! with a counter in the data. With a data bit rate (sampled at 75%, as Linux
//! does above 800 kbit/s) the channel is CAN FD and the frames are FD with
//! bit rate switching, cycling through the FD lengths, 0 to 64 bytes.

use std::time::Duration;

use can_hal::{BusStatus, CanFdFrame, CanFrame, CanId, Frame, SamplePoint};
use can_usb_gs::protocol::feature;
use can_usb_gs::{Event, GsUsb, Settings, list_devices};

/// The FD data lengths, one per DLC.
const FD_LENGTHS: [usize; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64];

#[tokio::main]
async fn main() -> Result<(), can_usb_gs::Error> {
    let mut args = std::env::args().skip(1).map(|arg| arg.parse::<u32>().expect("a number"));
    let bitrate = args.next().unwrap_or(250_000);
    let count = args.next().unwrap_or(10);
    let listen = args.next().unwrap_or(5);
    let data_bitrate = args.next();

    let mut device = None;
    for info in list_devices().await? {
        let name = info.to_string();
        match GsUsb::open(info).await {
            Ok(opened) if data_bitrate.is_some() && opened.bt_const(0).await?.feature & feature::FD == 0 => println!("{name}: no CAN FD"),
            Ok(opened) => {
                println!("{name}");
                device = Some(opened);
                break;
            }
            Err(e) => println!("{name}: {e}"),
        }
    }
    let device = device.expect("no gs_usb device could be opened");
    let mut settings = Settings::new(bitrate).with_sample_point(SamplePoint::PCT_87_5);
    if let Some(data_bitrate) = data_bitrate {
        settings = settings.with_data_sample_point(data_bitrate, SamplePoint::PCT_75);
    }
    let (mut tx, mut rx) = device.start(0, &settings).await?;
    println!("started at {bitrate} bit/s, data {data_bitrate:?}");

    let receiving = tokio::spawn(async move {
        let mut frames = 0u32;
        let deadline = tokio::time::sleep(Duration::from_secs(u64::from(listen) + 1));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                event = rx.next_event() => match event {
                    Ok(Event::Received(received)) => {
                        frames += 1;
                        let micros = received.timestamp().device_micros();
                        match received.frame() {
                            Frame::Can(frame) => println!("rx {:?} {:02X?} at {micros:?} µs", frame.id(), frame.data()),
                            Frame::Fd(frame) => println!("rx FD{}{} {:?} [{}] {:02X?} at {micros:?} µs", if frame.brs() { " BRS" } else { "" }, if frame.esi() { " ESI" } else { "" }, frame.id(), frame.len(), frame.data()),
                        }
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

    let id = CanId::new_standard(0x123).expect("11 bits");
    let mut previous = None;
    for n in 0..count {
        let frame = if data_bitrate.is_some() {
            let len = FD_LENGTHS[n as usize % FD_LENGTHS.len()];
            let data: Vec<u8> = (0..len).map(|i| n.to_le_bytes()[0].wrapping_add(i.to_le_bytes()[0])).collect();
            Frame::Fd(CanFdFrame::new(id, &data, true, false).expect("an FD length"))
        } else {
            Frame::Can(CanFrame::new(id, &n.to_be_bytes()).expect("4 bytes"))
        };
        let confirmation = tx.send(&frame).await?;
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
