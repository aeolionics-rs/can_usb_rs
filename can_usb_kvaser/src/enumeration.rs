//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fmt::{Display, Formatter};

/// The USB Vendor Id assigned to Kvaser AB.
pub const KVASER_VID: u16 = 0x0bfd;

/// List the Kvaser USB devices in the system.
///
/// Enumerating the devices may block. Devices are matched by vendor id.
///
/// ```rust
/// #[tokio::main]
/// async fn main() {
///     for device in can_usb_kvaser::list_devices().await.expect("Failed to list devices") {
///         println!("{device}")
///     }
/// }
/// ```
pub async fn list_devices() -> Result<impl Iterator<Item = DeviceInfo>, Box<dyn Error>> {
    Ok(nusb::list_devices().await?.filter(|dev| dev.vendor_id() == KVASER_VID).map(|d| DeviceInfo { inner: d }))
}

/// Information about a Kvaser device as reported through the USB subsystem.
///
/// This may differ from information stored on the device itself. Access to on-device
/// information can be obtained after opening the device.
pub struct DeviceInfo {
    inner: nusb::DeviceInfo,
}

impl DeviceInfo {
    /// The manufacturer string reported by the device, if any.
    pub fn manufacturer(&self) -> Option<&str> {
        self.inner.manufacturer_string()
    }

    /// The product string reported by the device, if any.
    pub fn product_name(&self) -> Option<&str> {
        self.inner.product_string()
    }

    /// The serial number reported by the device during USB enumeration, if any.
    ///
    /// This is probably not the serial number you are looking for as
    /// it may be completely unrelated to the number printed on the device enclosure.
    ///
    /// See [`card_info()`][crate::KvaserLeaf::card_info()]
    /// and [`CardInfo::serial_number()`][crate::message::CardInfo::serial_number()].
    pub fn serial_number(&self) -> Option<&str> {
        self.inner.serial_number()
    }

    /// The device's vendor id.
    pub fn vendor_id(&self) -> u16 {
        self.inner.vendor_id()
    }

    /// The device's product id.
    pub fn product_id(&self) -> u16 {
        self.inner.product_id()
    }

    /// Consumes the `DeviceInfo`, returning the underlying nusb [`DeviceInfo`]
    ///
    /// [`DeviceInfo`]: nusb::DeviceInfo
    pub fn into_inner(self) -> nusb::DeviceInfo {
        self.inner
    }
}

impl Display for DeviceInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.product_name().is_some() {
            write!(f, "{}, ", self.product_name().unwrap())?;
        }
        if self.manufacturer().is_some() {
            write!(f, "{} ", self.manufacturer().unwrap())?;
        }
        write!(f, "[{:04X},{:04X}]", self.vendor_id(), self.product_id())
    }
}
