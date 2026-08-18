//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

use can_usb_kvaser::{list_devices, KvaserLeaf};

#[tokio::main]
async fn main() {
    for info in list_devices().await.expect("Failed to list devices") {
        println!("{info}");
        let mut device = KvaserLeaf::from_info(info).await.expect("Failed to open device");
        let card_info = device.card_info().await.expect("Failed to get card info");
        println!("  EAN:    {}", card_info.ean());
        println!("  Serial: {:06}", card_info.serial_number());
    }
}
