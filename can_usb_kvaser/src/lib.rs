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
//! The [`Sink`] and [`Stream`] implementations exchange raw [`KvaserMessage`]s and do not flow control
//! transmission. Callers should keep no more than [`KvaserLeaf::max_outstanding_tx`] transmit messages
//! awaiting a [`KvaserMessage::TransmitAcknowledge`].
//!
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
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, warn};

/// Time to wait for the device to respond to a request.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);

/// Maximum number of received frames queued for the [`AsyncReceive`] channel.
const RECEIVE_QUEUE_LEN: usize = 1024;

/// CAN Errors, including wrapper for I/O and USB errors from dependencies.
#[derive(Debug)]
pub enum Error {
    /// The device did not respond to a request in time.
    Timeout,
    /// The device has been closed or disconnected.
    DeviceClosed,
    /// An I/O error communicating with the device.
    IoError(std::io::Error),
    /// A USB error opening or configuring the device.
    UsbError(nusb::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Timeout => write!(f, "timed out waiting for the device to respond"),
            Error::DeviceClosed => write!(f, "device closed"),
            Error::IoError(e) => write!(f, "I/O error: {e}"),
            Error::UsbError(e) => write!(f, "USB error: {e}"),
        }
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
    max_outstanding_tx: u16,
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

        let mut leaf = Self { sink, stream, max_outstanding_tx: 0 };
        let request = KvaserMessage::GetSoftwareInfo { transaction: 0x26, channel: 0 };
        leaf.max_outstanding_tx = leaf
            .request(request, |msg| match msg {
                KvaserMessage::SoftwareInfo { max_outstanding_tx, .. } => Some(max_outstanding_tx),
                _ => None,
            })
            .await?;
        Ok(leaf)
    }

    /// Return information on the adapter, such as its model and serial number.
    ///
    /// Returns [`Error::Timeout`] if the device does not respond within one second.
    pub async fn card_info(&mut self) -> Result<CardInfo, Error> {
        let request = KvaserMessage::GetCardInfo { transaction: 0x22, level: 0 };
        self.request(request, |msg| match msg {
            KvaserMessage::CardInfo(info) => Some(info),
            _ => None,
        })
        .await
    }

    /// The maximum number of transmit messages the device can have awaiting acknowledgement.
    pub fn max_outstanding_tx(&self) -> u16 {
        self.max_outstanding_tx
    }

    /// Send a request and wait for the first message accepted by `response`, skipping any others.
    async fn request<T>(&mut self, request: KvaserMessage, mut response: impl FnMut(KvaserMessage) -> Option<T>) -> Result<T, Error> {
        self.sink.send(request).await?;
        let wait = async {
            while let Some(msg) = self.stream.next().await {
                if let Some(value) = response(msg?) {
                    return Ok(value);
                }
            }
            Err(Error::DeviceClosed)
        };
        tokio::time::timeout(RESPONSE_TIMEOUT, wait).await.map_err(|_| Error::Timeout)?
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
    ///
    /// Transmission is flow controlled: [`AsyncTransmit::transmit`] waits while
    /// [`max_outstanding_tx`](Self::max_outstanding_tx) messages are awaiting acknowledgement by the device.
    /// A background task processes received messages so acknowledgements are handled even if the
    /// receive channel is not polled. Up to 1024 received frames are queued, after which new frames are dropped.
    ///
    /// # Panics
    /// Panics if called outside a Tokio runtime.
    pub fn channels(self) -> (impl AsyncTransmit<Error = Error>, impl AsyncReceive<Error = Error, Timestamp = u64>) {
        let (transmit, receive, task) = flow_controlled(self.sink, self.stream, self.max_outstanding_tx);
        tokio::spawn(task);
        (transmit, receive)
    }

    /// Return write and read USB endpoints for interacting with the device at a low level.
    ///
    /// Any data buffered by the codec (unsent writes or partially decoded reads) is discarded.
    pub fn endpoints(self) -> (EndpointWrite<Bulk>, EndpointRead<Bulk>) {
        (self.sink.into_inner(), self.stream.into_inner())
    }
}

type ReceiveResult = Result<Timestamped<CanFrame, u64>, Error>;

/// Create flow-controlled transmit and receive channels, and the task that processes received messages.
fn flow_controlled<Si, St>(sink: Si, stream: St, max_outstanding_tx: u16) -> (TransmitChannel<Si>, ReceiveChannel, impl Future<Output = ()> + Send + 'static)
where
    Si: Sink<KvaserMessage, Error = std::io::Error> + Unpin + Send + 'static,
    St: Stream<Item = Result<KvaserMessage, std::io::Error>> + Unpin + Send + 'static,
{
    // Transaction ids are a u8 so at most 256 messages can be distinguished.
    let max_outstanding_tx = (max_outstanding_tx as usize).clamp(1, 256);
    let transmitter = Arc::new(Transmitter {
        sink: tokio::sync::Mutex::new(sink),
        permits: Semaphore::new(max_outstanding_tx),
        outstanding: std::sync::Mutex::new(Outstanding::default()),
    });
    let (frames, received) = mpsc::channel(RECEIVE_QUEUE_LEN);
    let (transmit_open, transmit_closed) = mpsc::channel(1);
    let task = process_received(stream, transmitter.clone(), frames, transmit_closed);
    (TransmitChannel { transmitter, _open: transmit_open }, ReceiveChannel(received), task)
}

/// Process received messages, handling acknowledgements and queuing frames for the [`ReceiveChannel`].
///
/// Frames are dropped when the queue is full so that acknowledgements are still processed if the application is not receiving.
/// Returns when the stream ends, or both channels have been dropped (`transmit_closed` completes when the [`TransmitChannel`] is dropped).
async fn process_received<Si, St>(mut stream: St, transmitter: Arc<Transmitter<Si>>, frames: mpsc::Sender<ReceiveResult>, mut transmit_closed: mpsc::Receiver<()>)
where
    Si: Sink<KvaserMessage, Error = std::io::Error> + Unpin + Send,
    St: Stream<Item = Result<KvaserMessage, std::io::Error>> + Unpin,
{
    loop {
        let result = tokio::select! {
            result = stream.next() => match result {
                Some(result) => result,
                None => break,
            },
            _ = async {
                frames.closed().await;
                transmit_closed.recv().await
            } => break,
        };
        match result {
            Ok(KvaserMessage::TransmitAcknowledge { transaction, .. }) => transmitter.acknowledge(transaction),
            Ok(KvaserMessage::ChipState { bus_status: status, .. } | KvaserMessage::CanErrorEvent { bus_status: status, .. }) if status & (bus_status::BUS_OFF | bus_status::BUS_RESET) != 0 => {
                // Messages waiting to be sent will not be acknowledged, so discard them.
                warn!("Bus off (status {:02X})", status);
                if let Err(e) = transmitter.discard_outstanding().await {
                    warn!("Failed to flush transmit queue: {}", e);
                }
            }
            Ok(KvaserMessage::ChipStopped { .. }) => {
                if let Err(e) = transmitter.discard_outstanding().await {
                    warn!("Failed to flush transmit queue: {}", e);
                }
            }
            Ok(msg) => {
                if let Some(frame) = received_frame(msg)
                    && let Err(TrySendError::Full(_)) = frames.try_send(Ok(frame))
                {
                    warn!("Receive queue full, dropping frame");
                }
            }
            Err(e) => {
                _ = frames.try_send(Err(e.into()));
                break;
            }
        }
    }
    // No more acknowledgements will arrive, so fail any waiting transmits.
    transmitter.permits.close();
}

/// Transaction ids of messages awaiting acknowledgement.
#[derive(Default)]
struct Outstanding {
    transactions: HashSet<u8>,
    next: u8,
}

impl Outstanding {
    /// Allocate an unused transaction id.
    fn allocate(&mut self) -> u8 {
        while self.transactions.contains(&self.next) {
            self.next = self.next.wrapping_add(1);
        }
        let transaction = self.next;
        self.transactions.insert(transaction);
        self.next = self.next.wrapping_add(1);
        transaction
    }
}

/// Transmit state shared between the [`TransmitChannel`] and the task processing received messages.
struct Transmitter<Si> {
    /// The sink, locked while sending so that flushing the device queue is ordered with transmits.
    sink: tokio::sync::Mutex<Si>,
    /// Limits the messages awaiting acknowledgement to the maximum the device supports.
    permits: Semaphore,
    outstanding: std::sync::Mutex<Outstanding>,
}

impl<Si: Sink<KvaserMessage, Error = std::io::Error> + Unpin + Send> Transmitter<Si> {
    async fn transmit(&self, frame: &CanFrame) -> Result<(), Error> {
        // Wait until the device can accept another message. The permit is returned when it is acknowledged.
        self.permits.acquire().await.map_err(|_| Error::DeviceClosed)?.forget();
        let mut sink = self.sink.lock().await;
        let transaction = self.outstanding.lock().unwrap().allocate();
        let msg = match frame.id() {
            CanId::Standard(id) => KvaserMessage::TransmitStandard(TransmitMessage::standard(0, transaction, id, frame.data(), 0)),
            CanId::Extended(id) => KvaserMessage::TransmitExtended(TransmitMessage::extended(0, transaction, id, frame.data(), 0)),
        };
        sink.send(msg).await.inspect_err(|_| self.acknowledge(transaction)).map_err(Into::into)
    }

    /// Release the permit for an acknowledged message. Acknowledgements for unknown transactions are ignored.
    fn acknowledge(&self, transaction: u8) {
        if self.outstanding.lock().unwrap().transactions.remove(&transaction) {
            self.permits.add_permits(1);
        }
    }

    /// Flush messages awaiting acknowledgement from the device and release their permits.
    async fn discard_outstanding(&self) -> Result<(), Error> {
        if self.outstanding.lock().unwrap().transactions.is_empty() {
            return Ok(());
        }
        // Hold the sink so no transmit is sent between the flush and clearing the outstanding transactions.
        let mut sink = self.sink.lock().await;
        let result = sink.send(KvaserMessage::FlushQueue { transaction: 0, channel: 0, flags: 0 }).await;
        let discarded = {
            let mut outstanding = self.outstanding.lock().unwrap();
            let discarded = outstanding.transactions.len();
            outstanding.transactions.clear();
            discarded
        };
        warn!("Discarded {} frames awaiting acknowledgement", discarded);
        self.permits.add_permits(discarded);
        result.map_err(Into::into)
    }
}

struct TransmitChannel<Si> {
    transmitter: Arc<Transmitter<Si>>,
    /// Closed when the channel is dropped, so the task processing received messages can stop.
    _open: mpsc::Sender<()>,
}

impl<Si: Sink<KvaserMessage, Error = std::io::Error> + Unpin + Send> AsyncTransmit for TransmitChannel<Si> {
    type Error = Error;

    fn transmit(&mut self, frame: &CanFrame) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.transmitter.transmit(frame)
    }
}

struct ReceiveChannel(mpsc::Receiver<ReceiveResult>);

impl ReceiveChannel {
    async fn receive(&mut self) -> ReceiveResult {
        self.0.recv().await.unwrap_or(Err(Error::DeviceClosed))
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

    type Messages = futures::channel::mpsc::UnboundedSender<Result<KvaserMessage, std::io::Error>>;

    /// Create flow-controlled channels over in-memory queues, returning the channels,
    /// a sender to inject received messages and a receiver for transmitted messages.
    fn test_channels(max_outstanding_tx: u16) -> (impl AsyncTransmit<Error = Error>, ReceiveChannel, Messages, futures::channel::mpsc::UnboundedReceiver<KvaserMessage>) {
        let (sink, sent) = futures::channel::mpsc::unbounded();
        let sink = sink.sink_map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe));
        let (messages, stream) = futures::channel::mpsc::unbounded();
        let (tx, rx, task) = flow_controlled(sink, stream, max_outstanding_tx);
        tokio::spawn(task);
        (tx, rx, messages, sent)
    }

    fn ack(transaction: u8) -> Result<KvaserMessage, std::io::Error> {
        Ok(KvaserMessage::TransmitAcknowledge { channel: 0, transaction, data: vec![] })
    }

    fn chip_state(bus_status: u8) -> Result<KvaserMessage, std::io::Error> {
        Ok(KvaserMessage::ChipState { transaction: 0, channel: 0, timestamp: 0, tx_errors: 0, rx_errors: 0, bus_status })
    }

    /// Returns the transaction id of a transmitted message.
    fn transaction(msg: Option<KvaserMessage>) -> u8 {
        match msg {
            Some(KvaserMessage::TransmitStandard(msg) | KvaserMessage::TransmitExtended(msg)) => msg.transaction(),
            other => panic!("expected a transmit message, got {other:?}"),
        }
    }

    const WAIT: Duration = Duration::from_millis(50);

    #[tokio::test]
    async fn test_transmit_waits_for_acknowledge() {
        let (mut tx, _rx, messages, sent) = test_channels(2);
        let frame = CanFrame::new(CanId::Standard(0x123), b"data").unwrap();
        tx.transmit(&frame).await.unwrap();
        tx.transmit(&frame).await.unwrap();
        assert!(tokio::time::timeout(WAIT, tx.transmit(&frame)).await.is_err(), "third transmit should wait");

        messages.unbounded_send(ack(0)).unwrap();
        tokio::time::timeout(WAIT, tx.transmit(&frame)).await.expect("transmit should proceed after acknowledge").unwrap();

        let transactions: Vec<_> = sent.take(3).map(|msg| transaction(Some(msg))).collect().await;
        assert_eq!(transactions, [0, 1, 2]);
    }

    #[tokio::test]
    async fn test_dropping_channels_releases_device() {
        let (tx, rx, _messages, sent) = test_channels(1);
        drop(rx);
        drop(tx);
        // The sink is dropped once the task processing received messages stops.
        let sent: Vec<_> = tokio::time::timeout(WAIT, sent.collect()).await.expect("task should stop when channels are dropped");
        assert!(sent.is_empty());
    }

    #[tokio::test]
    async fn test_unknown_and_duplicate_acknowledge_ignored() {
        let (mut tx, _rx, messages, _sent) = test_channels(1);
        let frame = CanFrame::new(CanId::Standard(0x123), b"data").unwrap();
        tx.transmit(&frame).await.unwrap();
        messages.unbounded_send(ack(5)).unwrap();
        assert!(tokio::time::timeout(WAIT, tx.transmit(&frame)).await.is_err(), "unknown acknowledge should not release");

        messages.unbounded_send(ack(0)).unwrap();
        messages.unbounded_send(ack(0)).unwrap();
        tokio::time::timeout(WAIT, tx.transmit(&frame)).await.unwrap().unwrap();
        assert!(tokio::time::timeout(WAIT, tx.transmit(&frame)).await.is_err(), "duplicate acknowledge should not release");
    }

    #[tokio::test]
    async fn test_bus_off_discards_outstanding() {
        let (mut tx, _rx, messages, mut sent) = test_channels(1);
        let frame = CanFrame::new(CanId::Standard(0x123), b"data").unwrap();
        tx.transmit(&frame).await.unwrap();
        assert_eq!(transaction(sent.next().await), 0);

        // Error passive does not discard.
        messages.unbounded_send(chip_state(bus_status::BUS_PASSIVE)).unwrap();
        assert!(tokio::time::timeout(WAIT, tx.transmit(&frame)).await.is_err());

        // Bus off flushes the device queue, then transmits can continue.
        messages.unbounded_send(chip_state(bus_status::BUS_OFF)).unwrap();
        tokio::time::timeout(WAIT, tx.transmit(&frame)).await.expect("transmit should proceed after bus off").unwrap();
        assert!(matches!(sent.next().await, Some(KvaserMessage::FlushQueue { .. })));
        assert_eq!(transaction(sent.next().await), 1);

        // A late acknowledgement for the discarded message does not release a permit.
        messages.unbounded_send(ack(0)).unwrap();
        assert!(tokio::time::timeout(WAIT, tx.transmit(&frame)).await.is_err());
    }

    #[test]
    fn test_allocate_skips_outstanding() {
        let mut outstanding = Outstanding { transactions: HashSet::from([1, 2]), next: 255 };
        assert_eq!(outstanding.allocate(), 255);
        assert_eq!(outstanding.allocate(), 0);
        assert_eq!(outstanding.allocate(), 3);
    }

    #[tokio::test]
    async fn test_full_receive_queue_does_not_block_acknowledge() {
        let (mut tx, mut rx, messages, _sent) = test_channels(1);
        let frame = CanFrame::new(CanId::Standard(0x123), b"data").unwrap();
        tx.transmit(&frame).await.unwrap();
        for _ in 0..RECEIVE_QUEUE_LEN + 10 {
            messages.unbounded_send(Ok(log_message(0, false, 0x123, 3))).unwrap();
        }
        messages.unbounded_send(ack(0)).unwrap();
        tokio::time::timeout(WAIT, tx.transmit(&frame)).await.expect("transmit should proceed after acknowledge").unwrap();

        // The queue holds the frames received before it filled.
        drop(messages);
        let mut received = 0;
        while rx.receive().await.is_ok() {
            received += 1;
        }
        assert_eq!(received, RECEIVE_QUEUE_LEN);
    }

    #[tokio::test]
    async fn test_stream_end_closes_channels() {
        let (mut tx, mut rx, messages, _sent) = test_channels(1);
        let frame = CanFrame::new(CanId::Standard(0x123), b"data").unwrap();
        tx.transmit(&frame).await.unwrap();
        messages.unbounded_send(Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))).unwrap();

        assert!(matches!(tokio::time::timeout(WAIT, tx.transmit(&frame)).await.unwrap(), Err(Error::DeviceClosed)));
        assert!(matches!(rx.receive().await, Err(Error::IoError(_))));
        assert!(matches!(rx.receive().await, Err(Error::DeviceClosed)));
    }

    #[test]
    fn test_received_frame_skips_error_and_remote() {
        assert!(received_frame(log_message(flags::ERROR_FRAME, false, 0x123, 3)).is_none());
        assert!(received_frame(log_message(flags::OVERRUN, false, 0x123, 3)).is_none());
        assert!(received_frame(log_message(flags::REMOTE_FRAME, false, 0x123, 3)).is_none());
        assert!(received_frame(KvaserMessage::ResetCard { transaction: 0 }).is_none());
    }
}
