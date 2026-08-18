//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

/// Example of send and receive using can_hal async_channel
///
use can_hal::{AsyncReceive, AsyncTransmit, CanFrame, CanId};
use can_usb_kvaser::KvaserLeaf;
use core::error::Error;
use core::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let info = can_usb_kvaser::list_devices().await?.next().expect("No device found");
    let mut device = KvaserLeaf::from_info(info).await?;
    device.set_bus_params(250_000, 13, 2, 1, 1).await?;
    device.start().await?;

    let (mut tx, mut rx) = device.channels();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(3000)).await;
        loop {
            let frame = CanFrame::new(CanId::Extended(0x12345), b"deadbeef").unwrap();
            println!("Sending: {:X?} {:02X?}", frame.id(), frame.data());
            _ = tx.transmit(&frame).await;
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });
    while let Ok(msg) = rx.receive().await {
        println!("Received at {}: {:X?} {:02X?}", msg.timestamp(), msg.frame().id(), msg.frame().data());
    }

    Ok(())
}
