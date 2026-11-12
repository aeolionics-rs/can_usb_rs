//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! A driver for CAN interfaces that speak the `gs_usb` protocol (candleLight
//! firmware and its descendants), written in pure Rust over [`nusb`], with no
//! kernel driver: it works on macOS and, through `WinUSB`, on Windows. On Linux
//! the kernel's `gs_usb` driver owns these devices and `SocketCAN` is the
//! usual way in.
//!
//! So far: finding devices, opening one, and its control requests (the
//! device's configuration, timing limits, state and timer).
//!
//! ```no_run
//! # async fn example() -> Result<(), can_usb_gs::Error> {
//! for info in can_usb_gs::list_devices().await? {
//!     let device = can_usb_gs::GsUsb::open(info).await?;
//!     println!("{} channel(s)", device.config().channels());
//! }
//! # Ok(())
//! # }
//! ```

use core::time::Duration;
use std::fmt::{Display, Formatter};

use gs_usb_protocol::{BtConst, BtConstExt, DecodeError, DeviceConfig, DeviceState, Request, decode_u32, encode_u32};
use nusb::Interface;
use nusb::descriptors::TransferType;
use nusb::transfer::{ControlIn, ControlOut, ControlType, Direction, Recipient, TransferError};

pub use gs_usb_protocol as protocol;

/// The USB vendor and product IDs of `gs_usb` devices, as Linux's `gs_usb`
/// driver lists them.
pub const DEVICE_IDS: &[(u16, u16)] = &[(0x1D50, 0x606F), (0x1209, 0x2323), (0x1209, 0xCA01), (0x1CD2, 0x606F), (0x16D0, 0x10B8), (0x16D0, 0x0F30)];

/// The `bInterfaceClass` of the `gs_usb` interface: vendor-specific.
const VENDOR_CLASS: u8 = 0xFF;

/// Time to wait for the device to answer a control request.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(1);

/// What went wrong.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A USB error opening or configuring the device.
    Usb(nusb::Error),
    /// A control request failed: the device refused it (a stall), timed out
    /// or went away.
    Transfer(TransferError),
    /// The device's answer could not be decoded.
    Decode(DecodeError),
    /// The device has no vendor-specific interface with a bulk IN and a bulk
    /// OUT endpoint.
    NoInterface,
    /// The device has no such channel.
    NoChannel(u8),
    /// The device lacks a feature this request needs.
    Unsupported(u32),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Usb(e) => write!(f, "USB error: {e}"),
            Error::Transfer(e) => write!(f, "control request failed: {e}"),
            Error::Decode(e) => write!(f, "could not decode the device's answer: {e:?}"),
            Error::NoInterface => write!(f, "no gs_usb interface"),
            Error::NoChannel(channel) => write!(f, "no channel {channel}"),
            Error::Unsupported(feature) => write!(f, "the device lacks feature {feature:#x}"),
        }
    }
}

impl core::error::Error for Error {}

impl From<nusb::Error> for Error {
    fn from(value: nusb::Error) -> Self {
        Error::Usb(value)
    }
}

impl From<TransferError> for Error {
    fn from(value: TransferError) -> Self {
        Error::Transfer(value)
    }
}

impl From<DecodeError> for Error {
    fn from(value: DecodeError) -> Self {
        Error::Decode(value)
    }
}

/// Lists the `gs_usb` devices attached, matched by [`DEVICE_IDS`].
///
/// # Errors
///
/// [`Error::Usb`] if the system's devices can't be listed.
pub async fn list_devices() -> Result<impl Iterator<Item = DeviceInfo>, Error> {
    Ok(nusb::list_devices().await?.filter(|dev| DEVICE_IDS.contains(&(dev.vendor_id(), dev.product_id()))).map(|inner| DeviceInfo { inner }))
}

/// A `gs_usb` device as the USB subsystem reports it, before opening.
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    inner: nusb::DeviceInfo,
}

impl DeviceInfo {
    /// The manufacturer string, if any.
    #[must_use]
    pub fn manufacturer(&self) -> Option<&str> {
        self.inner.manufacturer_string()
    }

    /// The product string, if any.
    #[must_use]
    pub fn product_name(&self) -> Option<&str> {
        self.inner.product_string()
    }

    /// The USB serial number, if any.
    #[must_use]
    pub fn serial_number(&self) -> Option<&str> {
        self.inner.serial_number()
    }

    /// The vendor ID.
    #[must_use]
    pub fn vendor_id(&self) -> u16 {
        self.inner.vendor_id()
    }

    /// The product ID.
    #[must_use]
    pub fn product_id(&self) -> u16 {
        self.inner.product_id()
    }

    /// The underlying [`nusb::DeviceInfo`].
    #[must_use]
    pub fn into_inner(self) -> nusb::DeviceInfo {
        self.inner
    }
}

impl Display for DeviceInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(product) = self.product_name() {
            write!(f, "{product}, ")?;
        }
        if let Some(manufacturer) = self.manufacturer() {
            write!(f, "{manufacturer} ")?;
        }
        write!(f, "[{:04X},{:04X}]", self.vendor_id(), self.product_id())?;
        if let Some(serial) = self.serial_number() {
            write!(f, " {serial}")?;
        }
        Ok(())
    }
}

/// The device's configuration, from [`Request::DeviceConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    raw: DeviceConfig,
}

impl Config {
    /// The number of CAN channels.
    #[must_use]
    pub fn channels(&self) -> u8 {
        self.raw.icount.saturating_add(1)
    }

    /// The firmware's version, as the device reports it.
    #[must_use]
    pub fn sw_version(&self) -> u32 {
        self.raw.sw_version
    }

    /// The hardware's version, as the device reports it.
    #[must_use]
    pub fn hw_version(&self) -> u32 {
        self.raw.hw_version
    }
}

/// An open `gs_usb` device, its interface claimed.
pub struct GsUsb {
    interface: Interface,
    number: u8,
    bulk_in: u8,
    bulk_out: u8,
    config: Config,
}

impl GsUsb {
    /// Opens the device, claims its `gs_usb` interface, tells it the host's
    /// byte order and reads its configuration.
    ///
    /// # Errors
    ///
    /// [`Error::Usb`] if it can't be opened or claimed (on Linux, while the
    /// kernel's driver has it), [`Error::NoInterface`] if it has no `gs_usb`
    /// interface, and [`Error::Transfer`] or [`Error::Decode`] if it doesn't
    /// answer the configuration request.
    pub async fn open(info: DeviceInfo) -> Result<Self, Error> {
        let device = info.into_inner().open().await?;
        if device.active_configuration().is_err() {
            device.set_configuration(1).await?;
        }
        let (number, bulk_in, bulk_out) = device
            .active_configuration()
            .ok()
            .and_then(|config| {
                config.interface_alt_settings().filter(|alt| alt.alternate_setting() == 0 && alt.class() == VENDOR_CLASS).find_map(|alt| {
                    let bulk = |direction| alt.endpoints().find(|ep| ep.transfer_type() == TransferType::Bulk && ep.direction() == direction).map(|ep| ep.address());
                    Some((alt.interface_number(), bulk(Direction::In)?, bulk(Direction::Out)?))
                })
            })
            .ok_or(Error::NoInterface)?;
        let interface = device.claim_interface(number).await?;
        let mut gs = Self {
            interface,
            number,
            bulk_in,
            bulk_out,
            config: Config {
                raw: DeviceConfig { icount: 0, sw_version: 0, hw_version: 0 },
            },
        };
        // As Linux does: the device-wide requests carry 1 in wValue.
        let mut buf = [0; 4];
        gs.control_out(Request::HostFormat, 1, encode_u32(gs_usb_protocol::HOST_FORMAT, &mut buf)).await?;
        gs.config = Config {
            raw: DeviceConfig::decode(&gs.control_in(Request::DeviceConfig, 1, 12).await?)?,
        };
        Ok(gs)
    }

    /// The configuration read when the device was opened.
    #[must_use]
    pub fn config(&self) -> Config {
        self.config
    }

    /// The `gs_usb` interface's number and its bulk IN and OUT endpoint
    /// addresses.
    #[must_use]
    pub fn endpoints(&self) -> (u8, u8, u8) {
        (self.number, self.bulk_in, self.bulk_out)
    }

    /// The channel's features, clock and nominal timing limits.
    ///
    /// # Errors
    ///
    /// [`Error::NoChannel`], or the request failing.
    pub async fn bt_const(&self, channel: u8) -> Result<BtConst, Error> {
        self.check_channel(channel)?;
        Ok(BtConst::decode(&self.control_in(Request::BtConst, channel.into(), 40).await?)?)
    }

    /// The channel's timing limits including the CAN FD data phase, for a
    /// device with [`feature::BT_CONST_EXT`](gs_usb_protocol::feature::BT_CONST_EXT).
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] if the channel lacks the feature,
    /// [`Error::NoChannel`], or the request failing.
    pub async fn bt_const_ext(&self, channel: u8) -> Result<BtConstExt, Error> {
        self.require(channel, gs_usb_protocol::feature::BT_CONST_EXT).await?;
        Ok(BtConstExt::decode(&self.control_in(Request::BtConstExt, channel.into(), 72).await?)?)
    }

    /// The channel's bus state and error counters, for a device with
    /// [`feature::GET_STATE`](gs_usb_protocol::feature::GET_STATE).
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] if the channel lacks the feature,
    /// [`Error::NoChannel`], or the request failing.
    pub async fn state(&self, channel: u8) -> Result<DeviceState, Error> {
        self.require(channel, gs_usb_protocol::feature::GET_STATE).await?;
        Ok(DeviceState::decode(&self.control_in(Request::GetState, channel.into(), 12).await?)?)
    }

    /// The device's microsecond timer, which stamps frames.
    ///
    /// # Errors
    ///
    /// The request failing.
    pub async fn timestamp(&self) -> Result<u32, Error> {
        Ok(decode_u32(&self.control_in(Request::Timestamp, 1, 4).await?)?)
    }

    /// Starts or stops the channel's identification (a blinking LED), for a
    /// device with [`feature::IDENTIFY`](gs_usb_protocol::feature::IDENTIFY).
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] if the channel lacks the feature,
    /// [`Error::NoChannel`], or the request failing.
    pub async fn identify(&self, channel: u8, on: bool) -> Result<(), Error> {
        use gs_usb_protocol::identify::{OFF, ON};
        self.require(channel, gs_usb_protocol::feature::IDENTIFY).await?;
        let mut buf = [0; 4];
        self.control_out(Request::Identify, channel.into(), encode_u32(if on { ON } else { OFF }, &mut buf)).await
    }

    fn check_channel(&self, channel: u8) -> Result<(), Error> {
        if channel < self.config.channels() { Ok(()) } else { Err(Error::NoChannel(channel)) }
    }

    async fn require(&self, channel: u8, feature: u32) -> Result<(), Error> {
        if self.bt_const(channel).await?.feature & feature == feature { Ok(()) } else { Err(Error::Unsupported(feature)) }
    }

    async fn control_in(&self, request: Request, value: u16, length: u16) -> Result<Vec<u8>, Error> {
        let data = ControlIn {
            control_type: ControlType::Vendor,
            recipient: Recipient::Interface,
            request: request.code(),
            value,
            index: self.number.into(),
            length,
        };
        Ok(self.interface.control_in(data, CONTROL_TIMEOUT).await?)
    }

    async fn control_out(&self, request: Request, value: u16, data: &[u8]) -> Result<(), Error> {
        let data = ControlOut {
            control_type: ControlType::Vendor,
            recipient: Recipient::Interface,
            request: request.code(),
            value,
            index: self.number.into(),
            data,
        };
        Ok(self.interface.control_out(data, CONTROL_TIMEOUT).await?)
    }
}
