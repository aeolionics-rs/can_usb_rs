//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

/// Example of send and receive using async I/O ([`Sink`] and [`Stream`])
use can_usb_kvaser::KvaserLeaf;
use can_usb_kvaser::message::{KvaserMessage, TransmitMessage};
use core::error::Error;
use core::time::Duration;
use futures::{SinkExt, StreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let info = can_usb_kvaser::list_devices().await?.next().expect("No device found");
    let mut device = KvaserLeaf::from_info(info).await?;
    device.set_bus_params(250_000, 13, 2, 1, 1).await?;
    device.start().await?;

    let (mut writer, mut reader) = device.split();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(3000)).await;
        loop {
            let msg = TransmitMessage::extended(0, 2, 0x12345, &[1, 2, 3, 4, 5, 6, 7, 8], 0x40);
            let msg = KvaserMessage::TransmitExtended(msg);
            _ = writer.send(msg).await;
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });

    while let Some(result) = reader.next().await {
        if let Ok(frame) = result {
            println!("data: {:02X?}", frame);
        } else {
            eprintln!("error: {}", result.unwrap_err());
            break;
        }
    }

    Ok(())
}
