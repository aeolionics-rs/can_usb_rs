//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! Messages sent to/from Kvaser devices over USB.
//!

use deku::{DekuRead, DekuWrite};
use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime};

/// A EAN that identifies a specific product.
pub struct Ean(pub u64);

impl Display for Ean {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02x}-{:05x}-{:05x}-{:01x}", self.0 >> 44 & 0xff, self.0 >> 24 & 0xfffff, self.0 >> 4 & 0xfffff, self.0 & 0xf)
    }
}

/// Information on the device.
#[derive(Debug, DekuRead, DekuWrite)]
pub struct CardInfo {
    transaction: u8,
    channel_count: u8,
    #[deku(endian = "little")]
    serial_number: u32,
    #[deku(pad_bytes_before = "4", endian = "little")]
    clock_resolution: u32,
    #[deku(endian = "little")]
    manufacture_date: u32,
    #[deku(endian = "little")]
    ean: u64,
    hardware_revision: u8,
    usb_hs_mode: u8,
    hardware_type: u8,
    can_time_sample_reference: u8,
}

impl CardInfo {
    /// Returns the EAN that identifies the product.
    pub fn ean(&self) -> Ean {
        Ean(self.ean)
    }

    /// Returns the serial number of the individual device (as printed on the enclosure).
    pub fn serial_number(&self) -> u32 {
        self.serial_number
    }

    /// Date/time the adapter was manufactured.
    pub fn manufacture_date(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(self.manufacture_date as u64)
    }
}

/// Representation used to encode a CAN message to be sent by the adapter.
#[derive(Debug, DekuRead, DekuWrite)]
pub struct TransmitMessage {
    channel: u8,
    transaction: u8,
    raw_data: [u8; 14],
    #[deku(pad_bytes_before = "1")]
    flags: u8,
}

impl TransmitMessage {
    pub fn standard(channel: u8, transaction: u8, id: u16, data: &[u8], flags: u8) -> Self {
        let len = data.len();
        assert!(len <= 8);

        let mut raw_data = [0u8; 14];
        raw_data[0] = (id >> 6 & 0x1f) as u8;
        raw_data[1] = (id & 0x3f) as u8;
        raw_data[5] = len as u8;
        raw_data[6..6 + len].copy_from_slice(&data[0..len]);
        TransmitMessage { channel, transaction, raw_data, flags }
    }
    pub fn extended(channel: u8, transaction: u8, id: u32, data: &[u8], flags: u8) -> Self {
        let len = data.len();
        assert!(len <= 8);

        let mut raw_data = [0u8; 14];
        raw_data[0] = (id >> 24 & 0x1f) as u8;
        raw_data[1] = (id >> 18 & 0x3f) as u8;
        raw_data[2] = (id >> 14 & 0x0f) as u8;
        raw_data[3] = (id >> 6 & 0xff) as u8;
        raw_data[4] = (id & 0x3f) as u8;
        raw_data[5] = len as u8;
        raw_data[6..6 + len].copy_from_slice(&data[0..len]);
        TransmitMessage { channel, transaction, raw_data, flags }
    }
}

/// A Kvaser USB message.
///
/// Defintion of the messages in the vendor-specific interface definition.
/// All USB devices use the same messages, although not all devices support all messages.
#[derive(Debug, DekuRead, DekuWrite)]
#[deku(id_type = "u8")]
pub enum KvaserMessage {
    #[deku(id = "0x0D")]
    TransmitStandard(TransmitMessage),
    #[deku(id = "0x0F")]
    TransmitExtended(TransmitMessage),
    #[deku(id = "0x10")]
    SetBusParams {
        transaction: u8,
        channel: u8,
        #[deku(endian = "little")]
        bit_rate: u32,
        tseg1: u8,
        tseg2: u8,
        sjw: u8,
        no_samp: u8,
    },
    #[deku(id = "0x11")]
    GetBusParams { transaction: u8, channel: u8 },
    #[deku(id = "0x12")]
    BusParams {
        transaction: u8,
        channel: u8,
        #[deku(endian = "little")]
        bit_rate: u32,
        tseq1: u8,
        tseq2: u8,
        sjw: u8,
        no_samp: u8,
    },
    #[deku(id = "0x13")]
    GetChipState { transaction: u8, channel: u8 },
    #[deku(id = "0x14")]
    ChipState {
        transaction: u8,
        channel: u8,
        #[deku(bytes = "6", endian = "little")]
        timestamp: u64,
        tx_errors: u8,
        rx_errors: u8,
        #[deku(pad_bytes_after = "3")]
        bus_status: u8,
    },
    #[deku(id = "0x15")]
    SetDriverMode {
        transaction: u8,
        channel: u8,
        #[deku(pad_bytes_after = "3")]
        mode: u8,
    },
    #[deku(id = "0x16")]
    GetDriverMode { transaction: u8, channel: u8 },
    #[deku(id = "0x17")]
    DriverMode {
        transaction: u8,
        channel: u8,
        #[deku(pad_bytes_after = "3")]
        mode: u8,
    },
    #[deku(id = "0x18")]
    ResetChip { transaction: u8, channel: u8 },
    #[deku(id = "0x19")]
    ResetCard {
        #[deku(pad_bytes_after = "1")]
        transaction: u8,
    },
    #[deku(id = "0x1A")]
    StartChip { transaction: u8, channel: u8 },
    #[deku(id = "0x1B")]
    ChipStarted { transaction: u8, channel: u8 },
    #[deku(id = "0x1C")]
    StopChip { transaction: u8, channel: u8 },
    #[deku(id = "0x1D")]
    ChipStopped { transaction: u8, channel: u8 },
    #[deku(id = "0x1E")]
    ReadClock { transaction: u8, flags: u8 },
    #[deku(id = "0x1F")]
    Clock {
        transaction: u8,
        #[deku(bytes = "6", endian = "little", pad_bytes_before = "1", pad_bytes_after = "2")]
        timestamp: u64,
    },
    #[deku(id = "0x20")]
    CardInfo2 {
        transaction: u8,
        #[deku(pad_bytes_before = "1")]
        pcb_id: [u8; 24],
        #[deku(endian = "little")]
        oem_unlock_code: u32,
    },
    #[deku(id = "0x22")]
    GetCardInfo { transaction: u8, level: u8 },
    #[deku(id = "0x23")]
    CardInfo(CardInfo),
    #[deku(id = "0x24")]
    GetInterfaceInfo { transaction: u8, channel: u8 },
    #[deku(id = "0x25")]
    InterfaceInfo {
        transaction: u8,
        channel: u8,
        #[deku(endian = "little")]
        capabilities: u32,
        chip_type: u8,
        #[deku(pad_bytes_after = "2")]
        chip_sub_type: u8,
    },
    #[deku(id = "0x26")]
    GetSoftwareInfo { transaction: u8, channel: u8 },
    #[deku(id = "0x27")]
    SoftwareInfo {
        transaction: u8,
        #[deku(pad_bytes_before = "1", endian = "little")]
        options: u32,
        #[deku(endian = "little")]
        version: u32,
        #[deku(pad_bytes_after = "6", endian = "little")]
        max_outstanding_tx: u16,
    },
    #[deku(id = "0x28")]
    GetBusLoad { transaction: u8, channel: u8 },
    #[deku(id = "0x29")]
    BusLoad {
        transaction: u8,
        channel: u8,
        #[deku(bytes = "6", endian = "little")]
        timestamp: u64,
        #[deku(endian = "little")]
        sample_interval: u16,
        #[deku(endian = "little")]
        active_samples: u16,
        #[deku(endian = "little")]
        delta_t: u16,
    },
    #[deku(id = "0x2A")]
    ResetStatistics { transaction: u8, channel: u8 },
    #[deku(id = "0x2B")]
    CheckLicense {
        #[deku(pad_bytes_after = "1")]
        transaction: u8,
    },
    #[deku(id = "0x2C")]
    License {
        #[deku(pad_bytes_after = "1")]
        transaction: u8,
        #[deku(endian = "little")]
        license_mask: u32,
        #[deku(endian = "little")]
        kvaser_mask: u32,
    },
    #[deku(id = "0x2D")]
    ErrorEvent {
        transaction: u8,
        error: u8,
        #[deku(bytes = "6", endian = "little", pad_bytes_after = "2")]
        timestamp: u64,
        #[deku(endian = "little")]
        add_info1: u16,
        #[deku(endian = "little")]
        add_info2: u16,
    },
    #[deku(id = "0x30")]
    FlushQueue {
        transaction: u8,
        channel: u8,
        #[deku(pad_bytes_after = "3")]
        flags: u8,
    },
    #[deku(id = "0x31")]
    ResetErrorCounter { transaction: u8, channel: u8 },
    // #[deku(id = "0x32")]
    // TransmitAcknowledge,
    #[deku(id = "0x33")]
    CanErrorEvent {
        transaction: u8,
        flags: u8,
        #[deku(bytes = "6", endian = "little")]
        timestamp: u64,
        #[deku(pad_bytes_after = "1")]
        channel: u8,
        tx_errors: u8,
        rx_errors: u8,
        bus_status: u8,
        error_factor: u8,
    },
    #[deku(id = "0x4D")]
    UsbThrottle(#[deku(endian = "little")] u16),

    #[deku(id = "0x6A")]
    LogMessage {
        channel: u8,
        flags: u8,
        #[deku(bytes = "6", endian = "little")]
        timestamp: u64,
        dlc: u8,
        time_offset: u8,
        #[deku(bits = "29", bit_order = "lsb", pad_bits_after = "2")]
        id: u32,
        #[deku(bits = "1")]
        extended: bool,
        #[deku(read_all)]
        data: Vec<u8>,
    },
    #[deku(id_pat = "_")]
    Unknown {
        command: u8,
        #[deku(read_all)]
        data: Vec<u8>,
    },
}
