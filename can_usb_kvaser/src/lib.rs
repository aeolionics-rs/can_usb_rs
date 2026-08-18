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
//! ```rust
//! let (mut writer, mut reader) = device.split();
//! _ = writer.send(msg).await;
//! while let Some(result) = reader.next().await {
//!     ...
//! }
//! ```
//!
//! # CAN HAL Async Channel
//! ```rust
//! let (mut tx, mut rx) = device.channels();
//! _ = tx.transmit(&frame).await;
//! while let Ok(msg) = rx.receive().await {
//!     ...
//! }
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
use nusb::Interface;
use nusb::io::{EndpointRead, EndpointWrite};
use nusb::transfer::{Bulk, In, Out};
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::debug;

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
    interface: Interface,
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

        let rx = interface.endpoint::<Bulk, In>(0x82)?.reader(512).with_num_transfers(4).with_read_timeout(Duration::from_millis(100));
        let stream = FramedRead::new(rx, KvaserCodec {});

        let tx = interface.endpoint::<Bulk, Out>(0x02)?.writer(512).with_num_transfers(4);
        let sink = FramedWrite::new(tx, KvaserCodec {});

        Ok(Self { interface, sink, stream })
    }

    /// Return information on the adapter, such as its model and serial number.
    pub async fn card_info(&mut self) -> Result<CardInfo, Error> {
        let request = KvaserMessage::GetCardInfo { transaction: 0x22, level: 0 };
        self.sink.send(request).await?;
        while let Some(response) = self.stream.next().await {
            match response {
                Ok(KvaserMessage::CardInfo(info)) => {
                    return Ok(info);
                }
                Ok(_) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::Timeout)
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
    pub fn channels(self) -> (impl AsyncTransmit, impl AsyncReceive<Timestamp = u64>) {
        (TransmitChannel(self.sink), ReceiveChannel(self.stream))
    }

    /// Return read and write USB endpoints for interacting with the device at a low level.
    pub fn endpoints(self) -> Result<(EndpointWrite<Bulk>, EndpointRead<Bulk>), Error> {
        let rx = self.interface.endpoint::<Bulk, In>(0x82)?.reader(512).with_num_transfers(4).with_read_timeout(Duration::from_millis(100));
        let tx = self.interface.endpoint::<Bulk, Out>(0x02)?.writer(512).with_num_transfers(4);
        Ok((tx, rx))
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
            match result {
                Ok(KvaserMessage::LogMessage { timestamp, extended, id, dlc, data, .. }) => {
                    let can_id = if extended { CanId::Extended(id) } else { CanId::Standard(id as u16) };
                    let frame = CanFrame::new(can_id, &data.as_slice()[..dlc as usize]).unwrap();
                    return Ok(Timestamped::new(frame, timestamp));
                }
                Ok(msg) => {
                    debug!("ReceiveChannel ignoring message: {:?}", msg);
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::DeviceClosed)
    }
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
