//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! An Encoder and Decoder for Kvaser USB messages.

use crate::message::KvaserMessage;
use bytes::{Buf, BufMut, BytesMut};
use deku::DekuContainerWrite;
use tokio_util::codec::{Decoder, Encoder};

// Implements Encoder and Decoder for Kvaser USB messages.
pub struct KvaserCodec {}

impl Decoder for KvaserCodec {
    type Item = KvaserMessage;
    type Error = std::io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            // Skip zero padding the device adds to fill out the remainder of a USB packet.
            let padding = src.iter().take_while(|&&b| b == 0).count();
            src.advance(padding);
            if src.is_empty() {
                return Ok(None);
            }

            // Have we received enough data yet?
            let len = src[0] as usize;
            if len > src.len() {
                src.reserve(len - src.len());
                return Ok(None);
            }
            let mut data = src.split_to(len);
            data.advance(1); // Advance past the length byte.
            if data.is_empty() {
                tracing::warn!("Skipping message with no command");
                continue;
            }
            tracing::debug!("Decoding: {:02X}", data);

            // Return messages that fail to decode as unknown rather than an error, as errors terminate a FramedRead.
            let message = KvaserMessage::try_from(data.as_ref()).unwrap_or_else(|e| {
                tracing::warn!("Failed to decode message {:02X}: {}", data, e);
                KvaserMessage::Unknown { command: data[0], data: data[1..].to_vec() }
            });
            tracing::debug!("Decoded: {:02X?}", message);
            return Ok(Some(message));
        }
    }
}

impl Encoder<KvaserMessage> for KvaserCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: KvaserMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let data = item.to_bytes().expect("Failed to encode message");
        dst.reserve(data.len() + 1);
        dst.put_u8((data.len() + 1) as u8);
        dst.put_slice(&data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::TransmitMessage;

    #[test]
    fn test_encode() {
        let mut codec = KvaserCodec {};
        let mut buf = BytesMut::new();
        codec.encode(KvaserMessage::Unknown { command: 1, data: vec![1, 2, 3] }, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[5, 1, 1, 2, 3]);

        buf.clear();
        codec.encode(KvaserMessage::GetCardInfo { transaction: 0x12, level: 3 }, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[4, 0x22, 0x12, 3]);

        buf.clear();
        codec
            .encode(
                KvaserMessage::SetBusParams {
                    transaction: 0x10,
                    channel: 0,
                    bit_rate: 250_000,
                    tseg1: 13,
                    tseg2: 2,
                    sjw: 1,
                    no_samp: 1,
                },
                &mut buf,
            )
            .unwrap();
        assert_eq!(buf.as_ref(), &[12, 0x10, 0x10, 0x00, 0x90, 0xD0, 0x03, 0x00, 0x0D, 0x02, 0x01, 0x01]);

        buf.clear();
        let msg = TransmitMessage::standard(0, 1, 0x123, &[1, 2, 3, 4, 5, 6, 7, 8], 0x40);
        codec.encode(KvaserMessage::TransmitStandard(msg), &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[0x14, 0x0D, 0x00, 0x01, 0x04, 0x23, 0x00, 0x00, 0x00, 0x08, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x40]);

        buf.clear();
        let msg = TransmitMessage::extended(0, 2, 0x12345, &[1, 2, 3, 4, 5, 6, 7, 8], 0x40);
        codec.encode(KvaserMessage::TransmitExtended(msg), &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[0x14, 0x0F, 0x00, 0x02, 0x00, 0x00, 0x04, 0x8D, 0x05, 0x08, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x40]);
    }

    #[test]
    fn test_decode_skips_padding() {
        let mut codec = KvaserCodec {};
        let mut buf = BytesMut::from(&[4, 0x22, 0x12, 3, 0, 0, 0, 0, 4, 0x22, 0x13, 1, 0, 0][..]);
        assert!(matches!(codec.decode(&mut buf).unwrap(), Some(KvaserMessage::GetCardInfo { transaction: 0x12, level: 3 })));
        assert!(matches!(codec.decode(&mut buf).unwrap(), Some(KvaserMessage::GetCardInfo { transaction: 0x13, level: 1 })));
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_malformed() {
        let mut codec = KvaserCodec {};
        // A GetCardInfo command missing its level byte, then a length with no command.
        let mut buf = BytesMut::from(&[3, 0x22, 0x12, 1, 4, 0x22, 0x13, 1][..]);
        assert!(matches!(codec.decode(&mut buf).unwrap(), Some(KvaserMessage::Unknown { command: 0x22, .. })));
        assert!(matches!(codec.decode(&mut buf).unwrap(), Some(KvaserMessage::GetCardInfo { transaction: 0x13, level: 1 })));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_only_padding() {
        let mut codec = KvaserCodec {};
        let mut buf = BytesMut::from(&[0u8; 16][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert!(buf.is_empty());
    }
}
