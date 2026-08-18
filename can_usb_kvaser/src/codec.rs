//  SPDX-FileCopyrightText: 2026-2026. Aeolionics, LLC
//
//  SPDX-License-Identifier: Apache-2.0

//! A Encoder and Decoder for Kvaser USB messages.

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
        // Check we have a header (length + control bytes)
        if src.len() < 2 {
            src.reserve(2);
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
        tracing::debug!("Decoding: {:02X}", data);
        let message = KvaserMessage::try_from(data.as_ref())?;
        tracing::debug!("Decoded: {:02X?}", message);
        Ok(Some(message))
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
}
