//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! A channel for can-hal's confirmed transmission: [`ConfirmedTransmit`]
//! and [`ReceiveEvents`], on a device with transmit confirmation
//! ([`feature::TX_CONFIRMATION`]).
//!
//! The device's echoes and the frames it receives come in on one endpoint,
//! in the order they were on the bus, and the reader puts both in one queue
//! of [`Event`]s, so each [`Event::Transmitted`] comes in its place among
//! the frames received, as can-hal's confirmed channels promise.

use core::time::Duration;
use std::collections::VecDeque;
use std::sync::mpsc::{Receiver as EventQueue, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, PoisonError};
use std::time::Instant;

use can_hal::{Abort, AbortError, BusState, BusStatus, CanFrame, ConfirmedTransmit, ErrorCounters, Event, Failure, Frame, Outcome, ReceiveEvents, Timestamped, TransmitId, TryTransmitError};
use gs_usb_protocol::{FD_TS_LEN, Request, encode_u32, feature};
use nusb::transfer::{Buffer, Bulk, In, Out};
use nusb::{Endpoint, Interface, MaybeFuture};
use tokio::sync::TryAcquireError;
use tracing::warn;

use crate::Error;
use crate::channel::{Done, ECHO_SLOTS, Guard, Settings, Shared, Sink, Slot, Timestamp, TxFailure, read, to_host};

/// How many events wait to be read before more are dropped.
const EVENT_QUEUE_LEN: usize = 1024;

/// A started channel with can-hal's confirmed transmission: frames handed
/// over with [`ConfirmedTransmit::try_transmit`] are reported, sent or not,
/// by an [`Event::Transmitted`] among the frames received. Made by
/// [`GsUsb::start_confirmed`](crate::GsUsb::start_confirmed); dropping it
/// stops the channel.
///
/// It implements the traits for [`Frame`] and, on a classic channel's
/// terms, for [`CanFrame`] (FD frames received are then skipped). The
/// traits are blocking; the channel needs a tokio runtime running, as the
/// device is read by a task of its own. Remote frames and error frames,
/// which can-hal's events don't carry, aren't passed on; changes of bus
/// state are, as [`Event::BusStateChanged`].
pub struct ConfirmedChannel {
    out: Endpoint<Bulk, Out>,
    channel: u8,
    fd: bool,
    listen_only: bool,
    shared: Arc<Shared>,
    guard: Arc<Guard>,
    events: EventQueue<Event<Frame, Timestamp>>,
    /// Reports made here, not by the device: frames whose OUT transfer
    /// failed.
    local: VecDeque<Event<Frame, Timestamp>>,
    /// The slots of the OUT transfers not yet reaped, in submission order.
    submitted: VecDeque<(usize, u64)>,
    next_id: u32,
}

/// Opens the endpoints and starts the reader; the caller then starts the
/// channel.
pub(crate) fn open(interface: &Interface, index: u16, channel: u8, endpoints: (u8, u8), settings: &Settings, flags: u32) -> Result<ConfirmedChannel, Error> {
    let timestamped = flags & feature::HW_TIMESTAMP != 0;
    let bulk_in = interface.endpoint::<Bulk, In>(endpoints.0)?;
    let out = interface.endpoint::<Bulk, Out>(endpoints.1)?;
    let shared = Arc::new(Shared::new(true));
    let (sender, events) = std::sync::mpsc::sync_channel(EVENT_QUEUE_LEN);
    let reader = tokio::spawn(read(bulk_in, Sink::Confirmed(sender), shared.clone(), channel, timestamped));
    let guard = Arc::new(Guard {
        interface: interface.clone(),
        index,
        channel,
        reader,
    });
    Ok(ConfirmedChannel {
        out,
        channel,
        fd: settings.data.is_some(),
        listen_only: settings.listen_only,
        shared,
        guard,
        events,
        local: VecDeque::new(),
        submitted: VecDeque::new(),
        next_id: 0,
    })
}

/// can-hal's outcome of a transmission, from its echo.
pub(crate) fn outcome(result: Result<Timestamp, TxFailure>) -> Outcome<Timestamp> {
    match result {
        Ok(timestamp) => Outcome::Sent(timestamp),
        Err(TxFailure::Aborted) => Outcome::Aborted(Abort::Confirmed),
        Err(TxFailure::AbortedUnconfirmed) => Outcome::Aborted(Abort::Unconfirmed),
        // It may have been sent: the device lost its report, or gave a reason
        // this driver doesn't know.
        Err(TxFailure::Unconfirmed | TxFailure::Other(_)) => Outcome::Failed(Failure::Unconfirmed),
        // Single-shot attempts that failed, and frames dropped on going
        // bus-off: given up, not sent.
        Err(_) => Outcome::Failed(Failure::GaveUp),
    }
}

/// How long to wait for an event.
#[derive(Clone, Copy)]
enum Wait {
    No,
    Until(Instant),
    Forever,
}

impl ConfirmedChannel {
    /// Collects the OUT transfers that have finished; a frame whose transfer
    /// failed never reached the device, and is reported given up.
    fn reap(&mut self) {
        while self.out.pending() > 0 {
            let Some(completion) = self.out.wait_next_complete(Duration::ZERO) else { break };
            let Some((index, seq)) = self.submitted.pop_front() else { continue };
            if let Err(e) = completion.status {
                warn!("gs_usb: frame {index} not sent to the device: {e}");
                let mut slots = self.shared.slots.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(entry) = slots.get_mut(index)
                    && let Some(slot) = entry.take_if(|slot| slot.seq == seq)
                    && let Some(Done::Confirmed(id)) = slot.done
                {
                    self.local.push_back(Event::Transmitted { id, outcome: Outcome::Failed(Failure::GaveUp) });
                }
            }
        }
    }

    /// The next event, waiting as `wait` says.
    fn next(&mut self, wait: Wait) -> Result<Option<Event<Frame, Timestamp>>, Error> {
        self.reap();
        if let Some(event) = self.local.pop_front() {
            return Ok(Some(event));
        }
        match wait {
            Wait::No => match self.events.try_recv() {
                Ok(event) => Ok(Some(event)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Disconnected) => Err(Error::Stopped),
            },
            Wait::Until(deadline) => match self.events.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(event) => Ok(Some(event)),
                Err(RecvTimeoutError::Timeout) => Ok(None),
                Err(RecvTimeoutError::Disconnected) => Err(Error::Stopped),
            },
            Wait::Forever => self.events.recv().map(Some).map_err(|_| Error::Stopped),
        }
    }

    /// The next event on a classic channel's terms, waiting as `wait` says:
    /// FD frames received are skipped.
    fn next_classic(&mut self, wait: Wait) -> Result<Option<Event<CanFrame, Timestamp>>, Error> {
        loop {
            let Some(event) = self.next(wait)? else { return Ok(None) };
            return Ok(Some(match event {
                Event::Received(received) => {
                    let timestamp = *received.timestamp();
                    match received.into_frame() {
                        Frame::Can(frame) => Event::Received(Timestamped::new(frame, timestamp)),
                        Frame::Fd(_) => continue,
                    }
                }
                Event::Transmitted { id, outcome } => Event::Transmitted { id, outcome },
                Event::BusStateChanged(state) => Event::BusStateChanged(state),
            }));
        }
    }

    fn transmit(&mut self, frame: &Frame) -> Result<TransmitId, TryTransmitError<Error>> {
        self.reap();
        if self.listen_only {
            return Err(TryTransmitError::Backend(Error::ListenOnly));
        }
        if matches!(frame, Frame::Fd(_)) && !self.fd {
            return Err(TryTransmitError::Backend(Error::NotFd));
        }
        let permit = match self.shared.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return Err(TryTransmitError::Full),
            Err(TryAcquireError::Closed) => return Err(TryTransmitError::Backend(Error::Stopped)),
        };
        let id = TransmitId(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        let seq = self.shared.sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let index = {
            let mut slots = self.shared.slots.lock().unwrap_or_else(PoisonError::into_inner);
            if self.shared.permits.is_closed() {
                return Err(TryTransmitError::Backend(Error::Stopped));
            }
            let index = slots.iter().position(Option::is_none).ok_or(TryTransmitError::Backend(Error::Stopped))?;
            slots[index] = Some(Slot::new(permit, seq, Done::Confirmed(id)));
            index
        };
        let mut buf = [0; FD_TS_LEN];
        #[expect(clippy::cast_possible_truncation, reason = "fewer than ECHO_SLOTS (10)")]
        let bytes = to_host(frame, index as u32, self.channel).encode(&mut buf);
        self.out.submit(Buffer::from(bytes));
        self.submitted.push_back((index, seq));
        Ok(id)
    }

    /// Asks the device to withdraw transmission `id`, as
    /// [`Transmitter::abort`](crate::Transmitter::abort) does, blocking until
    /// it has the request.
    fn request_abort(&mut self, id: TransmitId) -> Result<(), AbortError<Error>> {
        let found = {
            let mut slots = self.shared.slots.lock().unwrap_or_else(PoisonError::into_inner);
            slots.iter_mut().enumerate().find_map(|(index, entry)| {
                let slot = entry.as_mut().filter(|slot| matches!(slot.done, Some(Done::Confirmed(mine)) if mine == id))?;
                slot.aborting = true;
                Some((index, slot.seq))
            })
        };
        // Already reported, or unknown.
        let Some((index, seq)) = found else { return Ok(()) };
        let mut buf = [0; 4];
        #[expect(clippy::cast_possible_truncation, reason = "fewer than ECHO_SLOTS (10)")]
        let echo_id = index as u32;
        let request = crate::control_out_request(Request::Abort, self.channel.into(), self.guard.index, encode_u32(echo_id, &mut buf));
        let result = self.guard.interface.control_out(request, crate::CONTROL_TIMEOUT).wait();
        let mut slots = self.shared.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = slots.get_mut(index)
            && let Some(slot) = entry.as_mut()
            && slot.seq == seq
        {
            if slot.done.is_none() {
                // Reported meanwhile: the slot can go now.
                *entry = None;
            } else {
                slot.aborting = false;
            }
        }
        result.map_err(|e| AbortError::Backend(Error::Transfer(e)))
    }
}

impl ConfirmedTransmit<Frame> for ConfirmedChannel {
    type Error = Error;

    fn capacity(&self) -> usize {
        ECHO_SLOTS
    }

    /// Hands `frame` to the device without waiting for USB; it is reported
    /// given up if it doesn't reach the device.
    fn try_transmit(&mut self, frame: &Frame) -> Result<TransmitId, TryTransmitError<Error>> {
        self.transmit(frame)
    }

    fn abort(&mut self, id: TransmitId) -> Result<(), AbortError<Error>> {
        self.request_abort(id)
    }
}

impl ConfirmedTransmit<CanFrame> for ConfirmedChannel {
    type Error = Error;

    fn capacity(&self) -> usize {
        ECHO_SLOTS
    }

    fn try_transmit(&mut self, frame: &CanFrame) -> Result<TransmitId, TryTransmitError<Error>> {
        self.transmit(&Frame::Can(frame.clone()))
    }

    fn abort(&mut self, id: TransmitId) -> Result<(), AbortError<Error>> {
        self.request_abort(id)
    }
}

impl ReceiveEvents<Frame> for ConfirmedChannel {
    type Error = Error;
    type Timestamp = Timestamp;

    fn receive_event(&mut self) -> Result<Event<Frame, Timestamp>, Error> {
        self.next(Wait::Forever)?.ok_or(Error::Stopped)
    }

    fn try_receive_event(&mut self) -> Result<Option<Event<Frame, Timestamp>>, Error> {
        self.next(Wait::No)
    }

    fn receive_event_timeout(&mut self, timeout: Duration) -> Result<Option<Event<Frame, Timestamp>>, Error> {
        self.next(Wait::Until(Instant::now() + timeout))
    }
}

impl ReceiveEvents<CanFrame> for ConfirmedChannel {
    type Error = Error;
    type Timestamp = Timestamp;

    fn receive_event(&mut self) -> Result<Event<CanFrame, Timestamp>, Error> {
        self.next_classic(Wait::Forever)?.ok_or(Error::Stopped)
    }

    fn try_receive_event(&mut self) -> Result<Option<Event<CanFrame, Timestamp>>, Error> {
        self.next_classic(Wait::No)
    }

    fn receive_event_timeout(&mut self, timeout: Duration) -> Result<Option<Event<CanFrame, Timestamp>>, Error> {
        self.next_classic(Wait::Until(Instant::now() + timeout))
    }
}

impl BusStatus for ConfirmedChannel {
    type Error = Error;

    fn bus_state(&self) -> Result<BusState, Error> {
        Ok(self.shared.bus().0)
    }

    /// As of the last error frame that carried them.
    fn error_counters(&self) -> Result<ErrorCounters, Error> {
        Ok(self.shared.bus().1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp() -> Timestamp {
        Timestamp::for_test()
    }

    #[test]
    fn outcomes_from_echoes() {
        assert!(matches!(outcome(Ok(stamp())), Outcome::Sent(_)));
        assert!(matches!(outcome(Err(TxFailure::Aborted)), Outcome::Aborted(Abort::Confirmed)));
        assert!(matches!(outcome(Err(TxFailure::AbortedUnconfirmed)), Outcome::Aborted(Abort::Unconfirmed)));
        assert!(matches!(outcome(Err(TxFailure::Unconfirmed)), Outcome::Failed(Failure::Unconfirmed)));
        assert!(matches!(outcome(Err(TxFailure::Other(9))), Outcome::Failed(Failure::Unconfirmed)));
        for given_up in [TxFailure::GaveUp, TxFailure::BusOff, TxFailure::LostArbitration, TxFailure::TxError] {
            assert!(matches!(outcome(Err(given_up)), Outcome::Failed(Failure::GaveUp)));
        }
    }
}
