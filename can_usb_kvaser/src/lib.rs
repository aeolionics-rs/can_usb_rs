//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! A driver for Kvaser CAN interfaces that connect using USB (for example, the Leaf range).
//! Written in pure Rust with no dependency on Kvaser's CANLIB driver library.
//!
//! The implementation targets `async` operation in a `std` environment, supporting both the
//! [`Sink`] and [`Stream`] traits and
//! the [`AsyncTransmit`] and [`AsyncReceive`] traits from [`can_hal::async_channel`].
//!
//! # Futures
//! ```no_run
//! # use can_usb_kvaser::KvaserLeaf;
//! # use can_usb_kvaser::message::{KvaserMessage, TransmitMessage};
//! # use futures::{SinkExt, StreamExt};
//! # async fn example() -> Result<(), can_usb_kvaser::Error> {
//! # let info = can_usb_kvaser::list_devices().await.unwrap().next().unwrap();
//! let device = KvaserLeaf::from_info(info).await?;
//! let (mut writer, mut reader) = device.split();
//! let msg = KvaserMessage::TransmitExtended(TransmitMessage::extended(0, 0, 0x12345, b"data", 0));
//! writer.send(msg).await?;
//! while let Some(result) = reader.next().await {
//!     println!("{:?}", result?);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # CAN HAL Async Channel
//! ```no_run
//! # use can_hal::{AsyncReceive, AsyncTransmit, CanFrame, CanId};
//! # use can_usb_kvaser::KvaserLeaf;
//! # async fn example() -> Result<(), can_usb_kvaser::Error> {
//! # let info = can_usb_kvaser::list_devices().await.unwrap().next().unwrap();
//! let device = KvaserLeaf::from_info(info).await?;
//! let (mut tx, mut rx) = device.channels();
//! let frame = CanFrame::new(CanId::Extended(0x12345), b"data").unwrap();
//! tx.transmit(&frame).await?;
//! while let Ok(msg) = rx.receive().await {
//!     println!("{:?}", msg.frame());
//! }
//! # Ok(())
//! # }
//! ```

pub mod codec;
mod enumeration;
pub mod message;

pub use enumeration::{DeviceInfo, list_devices};
use message::*;

use can_hal::async_channel::{AsyncReceive, AsyncTransmit};
use can_hal::{CanFrame, CanId, Timestamped};
use codec::KvaserCodec;
use core::time::Duration;
use futures::{Sink, SinkExt, Stream, StreamExt};
use nusb::io::{EndpointRead, EndpointWrite};
use nusb::transfer::{Bulk, In, Out};
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, warn};

/// Time to wait for the device to respond to a request.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);

/// CAN Errors, including wrapper for I/O and USB errors from dependencies.
#[derive(Debug)]
pub enum Error {
    DeviceNotFound,
    Timeout,
    DeviceClosed,
    IoError(std::io::Error),
    UsbError(nusb::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<nusb::Error> for Error {
    fn from(value: nusb::Error) -> Self {
        Error::UsbError(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IoError(value)
    }
}

impl core::error::Error for Error {}

/// Interface to a KvaserLeaf device.
pub struct KvaserLeaf {
    sink: FramedWrite<EndpointWrite<Bulk>, KvaserCodec>,
    stream: FramedRead<EndpointRead<Bulk>, KvaserCodec>,
}

impl KvaserLeaf {
    /// Open a Kvaser Leaf device from its USB information.
    pub async fn from_info(info: DeviceInfo) -> Result<Self, Error> {
        let device = info.into_inner().open().await?;

        // Check the configuration is active.
        if device.active_configuration().is_err() {
            device.set_configuration(1).await?;
        }

        // Claim the device and the interface.
        let interface = device.claim_interface(0).await?;

        let rx = interface.endpoint::<Bulk, In>(0x82)?.reader(512).with_num_transfers(4);
        let stream = FramedRead::new(rx, KvaserCodec {});

        let tx = interface.endpoint::<Bulk, Out>(0x02)?.writer(512).with_num_transfers(4);
        let sink = FramedWrite::new(tx, KvaserCodec {});

        Ok(Self { sink, stream })
    }

    /// Return information on the adapter, such as its model and serial number.
    ///
    /// Returns [`Error::Timeout`] if the device does not respond within one second.
    pub async fn card_info(&mut self) -> Result<CardInfo, Error> {
        let request = KvaserMessage::GetCardInfo { transaction: 0x22, level: 0 };
        self.sink.send(request).await?;
        let response = async {
            while let Some(response) = self.stream.next().await {
                match response {
                    Ok(KvaserMessage::CardInfo(info)) => {
                        return Ok(info);
                    }
                    Ok(_) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            Err(Error::DeviceClosed)
        };
        tokio::time::timeout(RESPONSE_TIMEOUT, response).await.map_err(|_| Error::Timeout)?
    }

    /// Set the CAN bus parameters.
    pub async fn set_bus_params(&mut self, bit_rate: u32, tseg1: u8, tseg2: u8, sjw: u8, no_samp: u8) -> Result<(), Error> {
        let request = KvaserMessage::SetBusParams {
            transaction: 0x10,
            channel: 0,
            bit_rate,
            tseg1,
            tseg2,
            sjw,
            no_samp,
        };
        self.sink.send(request).await.map_err(Into::into)
    }

    /// Start the device and go on-bus.
    pub async fn start(&mut self) -> Result<(), Error> {
        self.sink.feed(KvaserMessage::SetDriverMode { transaction: 0x15, channel: 0, mode: 1 }).await?;
        self.sink.feed(KvaserMessage::StartChip { transaction: 0x22, channel: 0 }).await?;
        self.sink.flush().await?;
        Ok(())
    }

    /// Returns CAN HAL async channels for transmitting and receiving [`CanFrame`] messages.
    ///
    /// The receive timestamp is provided by the adapter hardware
    /// and is expressed in adapter clock ticks (e.g. 1/24MHz for a Leaf Light V2).
    /// It is **not tied** to Rust's monotonic system clock ([`std::time::Instant`]) and may drift.
    pub fn channels(self) -> (impl AsyncTransmit<Error = Error>, impl AsyncReceive<Error = Error, Timestamp = u64>) {
        (TransmitChannel(self.sink), ReceiveChannel(self.stream))
    }

    /// Return write and read USB endpoints for interacting with the device at a low level.
    ///
    /// Any data buffered by the codec (unsent writes or partially decoded reads) is discarded.
    pub fn endpoints(self) -> (EndpointWrite<Bulk>, EndpointRead<Bulk>) {
        (self.sink.into_inner(), self.stream.into_inner())
    }
}

struct TransmitChannel(FramedWrite<EndpointWrite<Bulk>, KvaserCodec>);

impl TransmitChannel {
    async fn transmit(&mut self, frame: &CanFrame) -> Result<(), Error> {
        let msg = match frame.id() {
            CanId::Standard(id) => KvaserMessage::TransmitStandard(TransmitMessage::standard(0, 0, id, frame.data(), 0)),
            CanId::Extended(id) => KvaserMessage::TransmitExtended(TransmitMessage::extended(0, 0, id, frame.data(), 0)),
        };
        self.0.send(msg).await.map_err(Into::into)
    }
}

impl AsyncTransmit for TransmitChannel {
    type Error = Error;

    fn transmit(&mut self, frame: &CanFrame) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.transmit(frame)
    }
}

struct ReceiveChannel(FramedRead<EndpointRead<Bulk>, KvaserCodec>);

impl ReceiveChannel {
    async fn receive(&mut self) -> Result<Timestamped<CanFrame, u64>, Error> {
        while let Some(result) = self.0.next().await {
            if let Some(frame) = received_frame(result?) {
                return Ok(frame);
            }
        }
        Err(Error::DeviceClosed)
    }
}

/// Convert a received message to a CAN data frame.
///
/// Returns `None` for other messages, and for error and remote frames which [`CanFrame`] cannot represent.
fn received_frame(msg: KvaserMessage) -> Option<Timestamped<CanFrame, u64>> {
    let (msg_flags, timestamp, id, data) = match &msg {
        KvaserMessage::LogMessage { flags, timestamp, extended, id, dlc, data, .. } => {
            let id = if *extended { CanId::Extended(*id) } else { CanId::Standard((*id & 0x7ff) as u16) };
            let len = (*dlc as usize).min(8).min(data.len());
            (*flags, *timestamp, id, &data[..len])
        }
        KvaserMessage::ReceiveStandard(rx) => (rx.flags(), rx.timestamp(), CanId::Standard(rx.standard_id()), rx.data()),
        KvaserMessage::ReceiveExtended(rx) => (rx.flags(), rx.timestamp(), CanId::Extended(rx.extended_id()), rx.data()),
        _ => {
            debug!("ReceiveChannel ignoring message: {:?}", msg);
            return None;
        }
    };
    if msg_flags & (flags::ERROR_FRAME | flags::OVERRUN | flags::NERR) != 0 {
        warn!("ReceiveChannel ignoring error frame (flags {:02X})", msg_flags);
        return None;
    }
    if msg_flags & flags::REMOTE_FRAME != 0 {
        debug!("ReceiveChannel ignoring remote frame {:X?}", id);
        return None;
    }
    CanFrame::new(id, data).map(|frame| Timestamped::new(frame, timestamp))
}

impl AsyncReceive for ReceiveChannel {
    type Error = Error;
    type Timestamp = u64;

    fn receive(&mut self) -> impl Future<Output = Result<Timestamped<CanFrame, Self::Timestamp>, Self::Error>> + Send {
        self.receive()
    }
}

impl Sink<KvaserMessage> for KvaserLeaf {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().sink.poll_ready_unpin(cx).map_err(Into::into)
    }

    fn start_send(self: Pin<&mut Self>, item: KvaserMessage) -> Result<(), Self::Error> {
        self.get_mut().sink.start_send_unpin(item).map_err(Into::into)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().sink.poll_flush_unpin(cx).map_err(Into::into)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().sink.poll_close_unpin(cx).map_err(Into::into)
    }
}

impl Stream for KvaserLeaf {
    type Item = Result<KvaserMessage, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_message(flags: u8, extended: bool, id: u32, dlc: u8) -> KvaserMessage {
        KvaserMessage::LogMessage { channel: 0, flags, timestamp: 42, dlc, time_offset: 0, id, extended, data: vec![1, 2, 3, 4, 5, 6, 7, 8] }
    }

    #[test]
    fn test_received_frame() {
        let frame = received_frame(log_message(0, false, 0x123, 3)).unwrap();
        assert_eq!(*frame.timestamp(), 42);
        assert_eq!(frame.frame().id(), CanId::Standard(0x123));
        assert_eq!(frame.frame().data(), &[1, 2, 3]);

        let frame = received_frame(log_message(0, true, 0x12345, 8)).unwrap();
        assert_eq!(frame.frame().id(), CanId::Extended(0x12345));
    }

    #[test]
    fn test_received_frame_dlc_over_8() {
        let frame = received_frame(log_message(0, false, 0x123, 15)).unwrap();
        assert_eq!(frame.frame().data(), &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_received_frame_skips_error_and_remote() {
        assert!(received_frame(log_message(flags::ERROR_FRAME, false, 0x123, 3)).is_none());
        assert!(received_frame(log_message(flags::OVERRUN, false, 0x123, 3)).is_none());
        assert!(received_frame(log_message(flags::REMOTE_FRAME, false, 0x123, 3)).is_none());
        assert!(received_frame(KvaserMessage::ResetCard { transaction: 0 }).is_none());
    }
}
