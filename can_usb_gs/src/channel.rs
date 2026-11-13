//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! A started channel: frames out on bulk OUT, and on bulk IN the frames
//! received, the echoes of frames sent and error frames.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use can_hal::async_channel::{AsyncReceive, AsyncReceiveFd, AsyncTransmit, AsyncTransmitFd};
use can_hal::{BusState, BusStatus, CanFdFrame, CanFrame, CanId, ErrorCounters, Frame, SamplePoint, Timestamped};
use gs_usb_protocol::{ECHO_ID_RX, FD_TS_LEN, HostFrame, can_err, can_id, fd_len, feature, frame_flag, tx_failed};
use nusb::transfer::{Buffer, Bulk, In, Out, TransferError};
use nusb::{Endpoint, Interface, MaybeFuture};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::Error;

/// How many frames may be sent and not yet echoed, as Linux's `gs_usb`
/// allows.
pub const ECHO_SLOTS: usize = 10;

/// How many events wait for the [`Receiver`] before more are dropped.
const RECEIVE_QUEUE_LEN: usize = 1024;

/// How many bulk IN transfers are kept submitted.
const IN_TRANSFERS: usize = 8;

/// How long bulk IN must stay quiet, after a reset, for what came to be all
/// the device had queued from before.
const DRAIN_QUIET: Duration = Duration::from_millis(20);

/// How a channel is to be started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(clippy::struct_excessive_bools, reason = "independent mode flags, as gs_usb's own")]
pub struct Settings {
    pub(crate) bitrate: u32,
    pub(crate) sample_point: SamplePoint,
    pub(crate) data: Option<(u32, SamplePoint)>,
    pub(crate) listen_only: bool,
    pub(crate) loopback: bool,
    pub(crate) one_shot: bool,
    pub(crate) bus_errors: bool,
}

impl Settings {
    /// Classic CAN at `bitrate`, sampled at can-hal's default (70%).
    #[must_use]
    pub const fn new(bitrate: u32) -> Self {
        Self {
            bitrate,
            sample_point: SamplePoint::NOMINAL_DEFAULT,
            data: None,
            listen_only: false,
            loopback: false,
            one_shot: false,
            bus_errors: false,
        }
    }

    /// The nominal sample point (NMEA 2000 and J1939 use 87.5%).
    #[must_use]
    pub const fn with_sample_point(mut self, sample_point: SamplePoint) -> Self {
        self.sample_point = sample_point;
        self
    }

    /// CAN FD, with this data bit rate, sampled at can-hal's default (80%).
    #[must_use]
    pub const fn with_data(self, bitrate: u32) -> Self {
        self.with_data_sample_point(bitrate, SamplePoint::DATA_DEFAULT)
    }

    /// CAN FD, with this data bit rate and sample point.
    #[must_use]
    pub const fn with_data_sample_point(mut self, bitrate: u32, sample_point: SamplePoint) -> Self {
        self.data = Some((bitrate, sample_point));
        self
    }

    /// Never transmit or acknowledge.
    #[must_use]
    pub const fn listen_only(mut self) -> Self {
        self.listen_only = true;
        self
    }

    /// Receive the frames sent, inside the controller.
    #[must_use]
    pub const fn loopback(mut self) -> Self {
        self.loopback = true;
        self
    }

    /// One attempt per frame, with no retransmission.
    #[must_use]
    pub const fn one_shot(mut self) -> Self {
        self.one_shot = true;
        self
    }

    /// Error frames for each bus error, not only for state changes.
    #[must_use]
    pub const fn bus_errors(mut self) -> Self {
        self.bus_errors = true;
        self
    }
}

/// When a frame was on the bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    device_micros: Option<u64>,
    received: Instant,
}

impl Timestamp {
    /// The device's microsecond timer at the frame, extended past its 32-bit
    /// wrap, if the device stamps frames. Its zero is the device's, and its
    /// rate the device's clock, which may drift from the host's.
    #[must_use]
    pub const fn device_micros(&self) -> Option<u64> {
        self.device_micros
    }

    /// When the host read the frame from USB.
    #[must_use]
    pub const fn received(&self) -> Instant {
        self.received
    }
}

/// An error frame from the device, as Linux's `linux/can/error.h` describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorFrame {
    /// The error classes ([`can_err`]'s identifier bits).
    pub class: u32,
    /// The details, by class.
    pub data: [u8; 8],
    /// When.
    pub timestamp: Timestamp,
}

/// What a [`Receiver`] gets.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Event {
    /// A frame from the bus.
    Received(Timestamped<Frame, Timestamp>),
    /// A remote frame from the bus, which can-hal's frames can't carry.
    Remote {
        /// Its identifier.
        id: CanId,
        /// Its DLC.
        dlc: u8,
        /// When.
        timestamp: Timestamp,
    },
    /// A change of bus state or a bus error.
    Error(ErrorFrame),
}

/// Why a frame was not sent, as a device with transmit confirmation
/// ([`feature::TX_CONFIRMATION`], ferrocan's) says on its echo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TxFailure {
    /// Given up after a single-shot attempt failed.
    GaveUp,
    /// Dropped on going bus-off.
    BusOff,
    /// Aborted.
    Aborted,
    /// The controller's report was lost: it may have been sent.
    Unconfirmed,
    /// A reason this driver doesn't know.
    Other(u8),
}

impl TxFailure {
    fn from_code(code: u8) -> Self {
        match code {
            tx_failed::GAVE_UP => Self::GaveUp,
            tx_failed::BUS_OFF => Self::BusOff,
            tx_failed::ABORTED => Self::Aborted,
            tx_failed::UNCONFIRMED => Self::Unconfirmed,
            other => Self::Other(other),
        }
    }
}

impl std::fmt::Display for TxFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GaveUp => write!(f, "given up after a single-shot attempt"),
            Self::BusOff => write!(f, "dropped on going bus-off"),
            Self::Aborted => write!(f, "aborted"),
            Self::Unconfirmed => write!(f, "its report was lost; it may have been sent"),
            Self::Other(code) => write!(f, "reason {code}"),
        }
    }
}

/// A frame sent and not yet echoed.
struct Slot {
    _permit: OwnedSemaphorePermit,
    done: oneshot::Sender<Result<Timestamp, TxFailure>>,
}

/// What the reader, [`Transmitter`] and [`Receiver`] share.
struct Shared {
    bus: Mutex<(BusState, ErrorCounters)>,
    /// One permit per free slot; closed once the reader has stopped.
    permits: Arc<Semaphore>,
    slots: Mutex<Vec<Option<Slot>>>,
    dropped: AtomicU64,
    overflows: AtomicU64,
    /// Whether the channel was started with transmit confirmation, so that
    /// echoes say when a frame wasn't sent.
    tx_confirmation: bool,
}

impl Shared {
    fn new(tx_confirmation: bool) -> Self {
        Self {
            bus: Mutex::new((BusState::ErrorActive, ErrorCounters { transmit: 0, receive: 0 })),
            permits: Arc::new(Semaphore::new(ECHO_SLOTS)),
            slots: Mutex::new((0..ECHO_SLOTS).map(|_| None).collect()),
            dropped: AtomicU64::new(0),
            overflows: AtomicU64::new(0),
            tx_confirmation,
        }
    }

    fn bus(&self) -> (BusState, ErrorCounters) {
        *self.bus.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// No more echoes will come: senders waiting for a slot, and frames
    /// waiting for their echo, get [`Error::Stopped`]. Closing before
    /// clearing means a slot taken after the clear sees the close.
    fn stop(&self) {
        self.permits.close();
        self.slots.lock().unwrap_or_else(PoisonError::into_inner).iter_mut().for_each(|slot| *slot = None);
    }
}

/// Stops the channel when both halves are gone.
struct Guard {
    interface: Interface,
    index: u16,
    channel: u8,
    reader: JoinHandle<()>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.reader.abort();
        let mut buf = [0; 8];
        let reset = gs_usb_protocol::DeviceMode { mode: gs_usb_protocol::mode::RESET, flags: 0 };
        let request = crate::control_out_request(gs_usb_protocol::Request::Mode, self.channel.into(), self.index, reset.encode(&mut buf));
        if let Err(e) = self.interface.control_out(request, crate::CONTROL_TIMEOUT).wait() {
            debug!("gs_usb: reset on stop failed: {e}");
        }
    }
}

/// Sends frames on a started channel.
pub struct Transmitter {
    out: Endpoint<Bulk, Out>,
    channel: u8,
    fd: bool,
    listen_only: bool,
    shared: Arc<Shared>,
    _guard: Arc<Guard>,
}

/// A frame's echo, which resolves with the echo's [`Timestamp`].
///
/// What the echo means is the firmware's choice. A device with transmit
/// confirmation ([`feature::TX_CONFIRMATION`], ferrocan's; see
/// [`Transmitter::confirms_transmission`]) echoes a frame once the controller
/// reports it done: the timestamp is its time on the bus, and a frame not
/// sent resolves to [`Error::TxFailed`] with the reason. candleLight echoes
/// as soon as the frame is queued in the controller, so the timestamp is the
/// time it was queued, and a frame lost after that (on bus-off, say) is
/// still confirmed.
///
/// Until it is echoed the frame holds one of the [`ECHO_SLOTS`]. A frame that
/// can't get onto the bus (no other node acknowledges it, or it keeps losing
/// arbitration) is retried by the controller and not echoed, so this can wait
/// indefinitely: time it out if that matters. The `gs_usb` protocol has no
/// request to take back one frame; only a mode reset (stopping the channel)
/// drops them, all at once.
#[must_use = "a confirmation does nothing unless awaited"]
pub struct Confirmation(oneshot::Receiver<Result<Timestamp, TxFailure>>);

impl Future for Confirmation {
    type Output = Result<Timestamp, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|result| match result {
            Ok(Ok(timestamp)) => Ok(timestamp),
            Ok(Err(failure)) => Err(Error::TxFailed(failure)),
            Err(_) => Err(Error::Stopped),
        })
    }
}

impl Transmitter {
    /// Whether the device confirms transmissions
    /// ([`feature::TX_CONFIRMATION`]): a [`Confirmation`] then means the
    /// frame was acknowledged on the bus, and a frame not sent fails with
    /// [`Error::TxFailed`]. Otherwise it may only mean the frame was queued.
    #[must_use]
    pub fn confirms_transmission(&self) -> bool {
        self.shared.tx_confirmation
    }

    /// Sends `frame`, waiting while [`ECHO_SLOTS`] frames are unconfirmed
    /// and then for the device to take it. Await the [`Confirmation`] for
    /// its echo, or drop it.
    ///
    /// # Errors
    ///
    /// [`Error::NotFd`] for an FD frame on a classic channel,
    /// [`Error::ListenOnly`] on a listen-only one, [`Error::Stopped`] if the
    /// channel stopped or the device went away (also while waiting for a
    /// slot), and [`Error::Transfer`] if the device didn't take it.
    pub async fn send(&mut self, frame: &Frame) -> Result<Confirmation, Error> {
        if self.listen_only {
            return Err(Error::ListenOnly);
        }
        if matches!(frame, Frame::Fd(_)) && !self.fd {
            return Err(Error::NotFd);
        }
        let permit = self.shared.permits.clone().acquire_owned().await.map_err(|_| Error::Stopped)?;
        let (done, confirmation) = oneshot::channel();
        let echo_id = {
            let mut slots = self.shared.slots.lock().unwrap_or_else(PoisonError::into_inner);
            if self.shared.permits.is_closed() {
                return Err(Error::Stopped);
            }
            let index = slots.iter().position(Option::is_none).ok_or(Error::Stopped)?;
            slots[index] = Some(Slot { _permit: permit, done });
            index
        };
        let mut buf = [0; FD_TS_LEN];
        #[expect(clippy::cast_possible_truncation, reason = "fewer than ECHO_SLOTS (10)")]
        let bytes = to_host(frame, echo_id as u32, self.channel).encode(&mut buf);
        self.out.submit(Buffer::from(bytes));
        let completion = self.out.next_complete().await;
        if let Err(e) = completion.status {
            self.shared.slots.lock().unwrap_or_else(PoisonError::into_inner)[echo_id] = None;
            return Err(Error::Transfer(e));
        }
        Ok(Confirmation(confirmation))
    }
}

/// Receives on a started channel.
pub struct Receiver {
    events: mpsc::Receiver<Event>,
    shared: Arc<Shared>,
    _guard: Arc<Guard>,
}

impl Receiver {
    /// The next frame or error frame.
    ///
    /// # Errors
    ///
    /// [`Error::Stopped`] once the device has gone.
    pub async fn next_event(&mut self) -> Result<Event, Error> {
        self.events.recv().await.ok_or(Error::Stopped)
    }

    /// Events dropped because this receiver fell more than 1,024 behind.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.shared.dropped.load(Ordering::Relaxed)
    }

    /// Frames the device reported losing (its receive queue overflowed).
    #[must_use]
    pub fn overflows(&self) -> u64 {
        self.shared.overflows.load(Ordering::Relaxed)
    }
}

/// Opens the endpoints, starts the reader and returns the two halves; the
/// caller then starts the channel.
pub(crate) fn halves(interface: &Interface, index: u16, channel: u8, endpoints: (u8, u8), settings: &Settings, flags: u32) -> Result<(Transmitter, Receiver), Error> {
    let timestamped = flags & feature::HW_TIMESTAMP != 0;
    let bulk_in = interface.endpoint::<Bulk, In>(endpoints.0)?;
    let out = interface.endpoint::<Bulk, Out>(endpoints.1)?;
    let shared = Arc::new(Shared::new(flags & feature::TX_CONFIRMATION != 0));
    let (events, receiver) = mpsc::channel(RECEIVE_QUEUE_LEN);
    let reader = tokio::spawn(read(bulk_in, events, shared.clone(), channel, timestamped));
    let guard = Arc::new(Guard {
        interface: interface.clone(),
        index,
        channel,
        reader,
    });
    Ok((
        Transmitter {
            out,
            channel,
            fd: settings.data.is_some(),
            listen_only: settings.listen_only,
            shared: shared.clone(),
            _guard: guard.clone(),
        },
        Receiver { events: receiver, shared, _guard: guard },
    ))
}

/// Reads and drops what the device had queued for the host before a reset:
/// echoes and error frames from an earlier session that ended without
/// reading them (its process killed, say), which would otherwise come first
/// in the next, an old echo settling a new frame's slot. Done once bulk IN
/// stays quiet for [`DRAIN_QUIET`]. If the endpoint can't be claimed (a
/// started channel holds it) nothing is dropped.
pub(crate) async fn drain_in(interface: &Interface, endpoint: u8) {
    let Ok(mut bulk_in) = interface.endpoint::<Bulk, In>(endpoint) else { return };
    let packet = bulk_in.max_packet_size();
    let len = FD_TS_LEN.div_ceil(packet) * packet;
    bulk_in.submit(Buffer::new(len));
    loop {
        match tokio::time::timeout(DRAIN_QUIET, bulk_in.next_complete()).await {
            Ok(completion) if completion.status.is_ok() => {
                debug!("gs_usb: dropped {} bytes queued before the reset", completion.buffer.len());
                let mut buffer = completion.buffer;
                buffer.clear();
                buffer.set_requested_len(len);
                bulk_in.submit(buffer);
            }
            Ok(_) => return,
            Err(_) => {
                bulk_in.cancel_all();
                while bulk_in.pending() > 0 {
                    let _ = bulk_in.next_complete().await;
                }
                return;
            }
        }
    }
}

/// Extends the device's 32-bit microsecond timer.
#[derive(Default)]
struct Clock {
    high: u64,
    last: Option<u32>,
}

impl Clock {
    fn extend(&mut self, micros: u32) -> u64 {
        if self.last.is_some_and(|last| micros < last) {
            self.high += 1 << 32;
        }
        self.last = Some(micros);
        self.high | u64::from(micros)
    }
}

/// Reads bulk IN until the device goes or the channel stops, then fails
/// what waits for an echo. Dropping `events` on return ends the
/// [`Receiver`] too.
async fn read(bulk_in: Endpoint<Bulk, In>, events: mpsc::Sender<Event>, shared: Arc<Shared>, channel: u8, timestamped: bool) {
    read_frames(bulk_in, &events, &shared, channel, timestamped).await;
    shared.stop();
}

async fn read_frames(mut bulk_in: Endpoint<Bulk, In>, events: &mpsc::Sender<Event>, shared: &Shared, channel: u8, timestamped: bool) {
    let packet = bulk_in.max_packet_size();
    let len = FD_TS_LEN.div_ceil(packet) * packet;
    for _ in 0..IN_TRANSFERS {
        bulk_in.submit(Buffer::new(len));
    }
    let mut clock = Clock::default();
    loop {
        let completion = bulk_in.next_complete().await;
        match completion.status {
            Ok(()) => {}
            Err(TransferError::Cancelled | TransferError::Disconnected) => return,
            Err(e) => {
                warn!("gs_usb: bulk IN failed: {e}");
                return;
            }
        }
        let received = Instant::now();
        match HostFrame::decode_in(&completion.buffer, timestamped) {
            Ok(host) if host.channel == channel => {
                let timestamp = Timestamp {
                    device_micros: host.timestamp_us.map(|us| clock.extend(us)),
                    received,
                };
                if let Some(event) = handle(&host, timestamp, shared)
                    && events.try_send(event).is_err()
                {
                    shared.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(host) => debug!("gs_usb: frame for channel {}", host.channel),
            Err(e) => warn!("gs_usb: undecodable IN transfer ({} bytes): {e:?}", completion.buffer.len()),
        }
        let mut buffer = completion.buffer;
        buffer.clear();
        buffer.set_requested_len(len);
        bulk_in.submit(buffer);
    }
}

/// Settles an echo or updates the bus state; returns what the receiver gets.
fn handle(host: &HostFrame, timestamp: Timestamp, shared: &Shared) -> Option<Event> {
    if host.flags & frame_flag::OVERFLOW != 0 {
        shared.overflows.fetch_add(1, Ordering::Relaxed);
    }
    if host.echo_id != ECHO_ID_RX {
        let slot = usize::try_from(host.echo_id).ok().and_then(|index| shared.slots.lock().unwrap_or_else(PoisonError::into_inner).get_mut(index)?.take());
        if let Some(slot) = slot {
            let outcome = if shared.tx_confirmation && host.flags & frame_flag::TX_FAILED != 0 {
                Err(TxFailure::from_code(host.data[0]))
            } else {
                Ok(timestamp)
            };
            // The sender may have dropped its confirmation.
            let _ = slot.done.send(outcome);
        } else {
            debug!("gs_usb: echo {} with nothing sent", host.echo_id);
        }
        return None;
    }
    if host.can_id & can_id::ERR != 0 {
        let class = host.can_id & !can_id::ERR & can_id::EFF_MASK;
        let mut data = [0; 8];
        data.copy_from_slice(&host.data[..8]);
        update_bus(class, data, shared);
        return Some(Event::Error(ErrorFrame { class, data, timestamp }));
    }
    let id = if host.can_id & can_id::EFF != 0 {
        CanId::new_extended(host.can_id & can_id::EFF_MASK)
    } else {
        CanId::new_standard(u16::try_from(host.can_id & can_id::SFF_MASK).expect("11 bits"))
    }
    .expect("masked to range");
    if host.can_id & can_id::RTR != 0 {
        return Some(Event::Remote { id, dlc: host.can_dlc, timestamp });
    }
    let frame = if host.is_fd() {
        Frame::Fd(CanFdFrame::new(id, &host.data[..fd_len(host.can_dlc)], host.flags & frame_flag::BRS != 0, host.flags & frame_flag::ESI != 0).expect("an FD length"))
    } else {
        Frame::Can(CanFrame::new(id, &host.data[..usize::from(host.can_dlc.min(8))]).expect("at most 8 bytes"))
    };
    Some(Event::Received(Timestamped::new(frame, timestamp)))
}

/// The bus state and counters an error frame reports.
fn update_bus(class: u32, data: [u8; 8], shared: &Shared) {
    let mut bus = shared.bus.lock().unwrap_or_else(PoisonError::into_inner);
    if class & can_err::CRTL != 0 {
        if data[1] & (can_err::crtl::RX_PASSIVE | can_err::crtl::TX_PASSIVE) != 0 {
            bus.0 = BusState::ErrorPassive;
        } else if data[1] & can_err::crtl::ACTIVE != 0 {
            bus.0 = BusState::ErrorActive;
        }
    }
    if class & can_err::BUSOFF != 0 {
        bus.0 = BusState::BusOff;
    }
    if class & can_err::RESTARTED != 0 {
        bus.0 = BusState::ErrorActive;
    }
    if class & can_err::CNT != 0 {
        bus.1 = ErrorCounters { transmit: data[6], receive: data[7] };
    }
}

/// A can-hal frame as `gs_usb` sends it.
fn to_host(frame: &Frame, echo_id: u32, channel: u8) -> HostFrame {
    let id = match frame.id() {
        CanId::Standard(id) => u32::from(id),
        CanId::Extended(id) => id | can_id::EFF,
    };
    let mut data = [0; 64];
    data[..frame.len()].copy_from_slice(frame.data());
    let (can_dlc, flags) = match frame {
        Frame::Can(f) => (u8::try_from(f.len()).expect("at most 8"), 0),
        Frame::Fd(f) => (fd_dlc(f.len()), frame_flag::FD | if f.brs() { frame_flag::BRS } else { 0 } | if f.esi() { frame_flag::ESI } else { 0 }),
    };
    HostFrame {
        echo_id,
        can_id: id,
        can_dlc,
        channel,
        flags,
        data,
        timestamp_us: None,
    }
}

/// The DLC of an FD data length (which can-hal keeps valid).
fn fd_dlc(len: usize) -> u8 {
    match len {
        0..=8 => u8::try_from(len).expect("at most 8"),
        9..=12 => 9,
        13..=16 => 10,
        17..=20 => 11,
        21..=24 => 12,
        25..=32 => 13,
        33..=48 => 14,
        _ => 15,
    }
}

impl AsyncTransmit for Transmitter {
    type Error = Error;

    async fn transmit(&mut self, frame: &CanFrame) -> Result<(), Error> {
        self.send(&Frame::Can(frame.clone())).await.map(drop)
    }
}

impl AsyncTransmitFd for Transmitter {
    type Error = Error;

    async fn transmit_fd(&mut self, frame: &CanFdFrame) -> Result<(), Error> {
        self.send(&Frame::Fd(frame.clone())).await.map(drop)
    }
}

impl AsyncReceive for Receiver {
    type Error = Error;
    type Timestamp = Timestamp;

    /// The next classic frame; FD and remote frames and error frames are
    /// skipped.
    async fn receive(&mut self) -> Result<Timestamped<CanFrame, Timestamp>, Error> {
        loop {
            if let Event::Received(received) = self.next_event().await? {
                let timestamp = *received.timestamp();
                if let Frame::Can(frame) = received.into_frame() {
                    return Ok(Timestamped::new(frame, timestamp));
                }
            }
        }
    }
}

impl AsyncReceiveFd for Receiver {
    type Error = Error;
    type Timestamp = Timestamp;

    /// The next classic or FD frame; remote frames and error frames are
    /// skipped.
    async fn receive_fd(&mut self) -> Result<Timestamped<Frame, Timestamp>, Error> {
        loop {
            if let Event::Received(received) = self.next_event().await? {
                return Ok(received);
            }
        }
    }
}

impl BusStatus for Transmitter {
    type Error = Error;

    fn bus_state(&self) -> Result<BusState, Error> {
        Ok(self.shared.bus().0)
    }

    fn error_counters(&self) -> Result<ErrorCounters, Error> {
        Ok(self.shared.bus().1)
    }
}

impl BusStatus for Receiver {
    type Error = Error;

    fn bus_state(&self) -> Result<BusState, Error> {
        Ok(self.shared.bus().0)
    }

    fn error_counters(&self) -> Result<ErrorCounters, Error> {
        Ok(self.shared.bus().1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> Shared {
        Shared::new(true)
    }

    fn stamp() -> Timestamp {
        Timestamp { device_micros: None, received: Instant::now() }
    }

    #[test]
    fn frames_both_ways() {
        let shared = shared();
        let frames = [
            Frame::Can(CanFrame::new(CanId::new_standard(0x123).unwrap(), &[1, 2, 3]).unwrap()),
            Frame::Can(CanFrame::new(CanId::new_extended(0x18EE_FF01).unwrap(), &[0; 8]).unwrap()),
            Frame::Fd(CanFdFrame::new(CanId::new_extended(0x1234).unwrap(), &[7; 12], true, false).unwrap()),
            Frame::Fd(CanFdFrame::new(CanId::new_standard(0x7FF).unwrap(), &[9; 64], false, true).unwrap()),
        ];
        for frame in frames {
            let host = HostFrame { echo_id: ECHO_ID_RX, ..to_host(&frame, 0, 0) };
            match handle(&host, stamp(), &shared) {
                Some(Event::Received(received)) => assert_eq!(received.into_frame(), frame),
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(fd_dlc(48), 14);
    }

    #[test]
    fn echoes_settle_their_slot() {
        let shared = shared();
        let permits = Arc::new(Semaphore::new(1));
        let (done, mut confirmation) = oneshot::channel();
        shared.slots.lock().unwrap()[3] = Some(Slot {
            _permit: permits.clone().try_acquire_owned().unwrap(),
            done,
        });
        assert_eq!(permits.available_permits(), 0);
        let frame = Frame::Can(CanFrame::new(CanId::new_standard(1).unwrap(), &[]).unwrap());
        let echo = to_host(&frame, 3, 0);
        assert!(handle(&echo, stamp(), &shared).is_none());
        assert!(confirmation.try_recv().is_ok());
        assert_eq!(permits.available_permits(), 1);
        // An unknown echo is ignored.
        assert!(handle(&echo, stamp(), &shared).is_none());
    }

    #[test]
    fn failed_echoes_say_why_with_tx_confirmation() {
        for (confirming, expected) in [(true, Err(TxFailure::BusOff)), (false, Ok(()))] {
            let shared = Shared::new(confirming);
            let (done, mut confirmation) = oneshot::channel();
            shared.slots.lock().unwrap()[1] = Some(Slot {
                _permit: shared.permits.clone().try_acquire_owned().unwrap(),
                done,
            });
            let frame = Frame::Can(CanFrame::new(CanId::new_standard(1).unwrap(), &[0x55]).unwrap());
            let mut echo = to_host(&frame, 1, 0);
            echo.flags |= frame_flag::TX_FAILED;
            echo.data[0] = tx_failed::BUS_OFF;
            assert!(handle(&echo, stamp(), &shared).is_none());
            assert_eq!(confirmation.try_recv().unwrap().map(drop), expected, "TX confirmation {confirming}");
        }
        assert_eq!(TxFailure::from_code(9), TxFailure::Other(9));
    }

    #[test]
    fn stopping_fails_what_waits() {
        let shared = shared();
        let (done, mut confirmation) = oneshot::channel();
        shared.slots.lock().unwrap()[0] = Some(Slot {
            _permit: shared.permits.clone().try_acquire_owned().unwrap(),
            done,
        });
        shared.stop();
        // The confirmation's sender is gone, and no slot can be taken.
        assert!(matches!(confirmation.try_recv(), Err(oneshot::error::TryRecvError::Closed)));
        assert!(shared.permits.clone().try_acquire_owned().is_err());
        assert!(shared.slots.lock().unwrap().iter().all(Option::is_none));
    }

    #[test]
    fn error_frames_set_the_bus_state() {
        let shared = shared();
        let error = |class: u32, data: [u8; 8]| {
            let mut host = to_host(&Frame::Can(CanFrame::new(CanId::new_standard(0).unwrap(), &data).unwrap()), ECHO_ID_RX, 0);
            host.can_id = can_id::ERR | class;
            host
        };
        let passive = error(can_err::CRTL | can_err::CNT, [0, can_err::crtl::TX_PASSIVE, 0, 0, 0, 0, 136, 0]);
        assert!(matches!(handle(&passive, stamp(), &shared), Some(Event::Error(ErrorFrame { class, .. })) if class == can_err::CRTL | can_err::CNT));
        assert_eq!(shared.bus(), (BusState::ErrorPassive, ErrorCounters { transmit: 136, receive: 0 }));
        handle(&error(can_err::BUSOFF, [0; 8]), stamp(), &shared);
        assert_eq!(shared.bus().0, BusState::BusOff);
        handle(&error(can_err::RESTARTED | can_err::CNT, [0; 8]), stamp(), &shared);
        assert_eq!(shared.bus(), (BusState::ErrorActive, ErrorCounters { transmit: 0, receive: 0 }));
    }

    #[test]
    fn remote_frames_and_overflow() {
        let shared = shared();
        let mut host = to_host(&Frame::Can(CanFrame::new(CanId::new_standard(0x10).unwrap(), &[]).unwrap()), ECHO_ID_RX, 0);
        host.can_id |= can_id::RTR;
        host.can_dlc = 4;
        host.flags = frame_flag::OVERFLOW;
        assert!(matches!(handle(&host, stamp(), &shared), Some(Event::Remote { dlc: 4, .. })));
        assert_eq!(shared.overflows.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn clock_extends_past_the_wrap() {
        let mut clock = Clock::default();
        assert_eq!(clock.extend(u32::MAX - 1), u64::from(u32::MAX - 1));
        assert_eq!(clock.extend(5), (1 << 32) + 5);
        assert_eq!(clock.extend(6), (1 << 32) + 6);
    }
}
