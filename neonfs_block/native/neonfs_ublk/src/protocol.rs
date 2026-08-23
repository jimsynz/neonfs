//! The wire between this helper and the BEAM node that owns the device.
//!
//! Mirrors `NeonFS.Block.Ublk.Protocol`. The two are separate artefacts a
//! partial upgrade can pair unevenly, so the version byte is first and is
//! checked on both sides — decoding one release's offsets with another's
//! layout lands writes at the wrong place rather than failing.

use std::io::{self, Read, Write};

pub const VERSION: u8 = 1;

pub const OP_READ: u8 = 0;
pub const OP_WRITE: u8 = 1;
pub const OP_FLUSH: u8 = 2;
pub const OP_DISCARD: u8 = 3;
pub const OP_WRITE_ZEROES: u8 = 4;

/// `<<version, op, tag::16, offset::64, length::32>>`
pub const REQUEST_HEADER_BYTES: usize = 16;

/// `<<version, status, tag::16, length::32>>`
pub const REPLY_HEADER_BYTES: usize = 8;

/// One IO, on its way to the node.
pub struct Request<'a> {
    pub op: u8,
    pub tag: u16,
    pub offset: u64,
    pub length: u32,
    pub data: &'a [u8],
}

impl Request<'_> {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.clear();
        out.reserve(REQUEST_HEADER_BYTES + self.data.len());
        out.extend_from_slice(&[VERSION, self.op]);
        out.extend_from_slice(&self.tag.to_be_bytes());
        out.extend_from_slice(&self.offset.to_be_bytes());
        out.extend_from_slice(&self.length.to_be_bytes());
        out.extend_from_slice(self.data);
    }
}

/// What the node answered.
pub struct Reply {
    pub status: u8,
    pub tag: u16,
    pub data: Vec<u8>,
}

impl Reply {
    pub fn decode(frame: &[u8]) -> io::Result<Self> {
        if frame.len() < REPLY_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "reply frame is {} bytes, shorter than its header",
                    frame.len()
                ),
            ));
        }

        if frame[0] != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "reply speaks version {}, this helper speaks {}",
                    frame[0], VERSION
                ),
            ));
        }

        let status = frame[1];
        let tag = u16::from_be_bytes([frame[2], frame[3]]);
        let length = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
        let payload = &frame[REPLY_HEADER_BYTES..];

        // The header's length is authoritative and the payload has to match
        // it. A short one would complete a read out of a buffer that does
        // not hold the bytes it claims.
        if payload.len() != length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "reply claims {} payload bytes and carries {}",
                    length,
                    payload.len()
                ),
            ));
        }

        Ok(Reply {
            status,
            tag,
            data: payload.to_vec(),
        })
    }
}

/// Length-prefixed framing, matching Erlang's `{packet, 4}`.
pub fn write_frame<W: Write>(sink: &mut W, frame: &[u8]) -> io::Result<()> {
    sink.write_all(&(frame.len() as u32).to_be_bytes())?;
    sink.write_all(frame)?;
    sink.flush()
}

/// Reads one whole frame, or fails. A partial frame is not a case a caller
/// has to handle: the length is read first and then exactly that many bytes,
/// so a frame is either whole or an error.
pub fn read_frame<R: Read>(source: &mut R, buffer: &mut Vec<u8>) -> io::Result<()> {
    let mut length = [0u8; 4];
    source.read_exact(&mut length)?;

    buffer.clear();
    buffer.resize(u32::from_be_bytes(length) as usize, 0);
    source.read_exact(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_encodes_its_header_then_its_payload() {
        let mut out = Vec::new();
        Request {
            op: OP_WRITE,
            tag: 7,
            offset: 4096,
            length: 3,
            data: b"abc",
        }
        .encode(&mut out);

        assert_eq!(out.len(), REQUEST_HEADER_BYTES + 3);
        assert_eq!(out[0], VERSION);
        assert_eq!(out[1], OP_WRITE);
        assert_eq!(u16::from_be_bytes([out[2], out[3]]), 7);
        assert_eq!(&out[REQUEST_HEADER_BYTES..], b"abc");
    }

    #[test]
    fn a_reply_round_trips_through_a_frame() {
        let mut frame = vec![VERSION, 0, 0, 9];
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(b"data");

        let reply = Reply::decode(&frame).expect("decodes");

        assert_eq!(reply.status, 0);
        assert_eq!(reply.tag, 9);
        assert_eq!(reply.data, b"data");
    }

    // A version this helper does not know must not be reinterpreted: the
    // offsets would be read with the wrong layout.
    #[test]
    fn a_reply_from_another_version_is_refused() {
        let frame = vec![VERSION + 1, 0, 0, 1, 0, 0, 0, 0];

        assert!(Reply::decode(&frame).is_err());
    }

    #[test]
    fn a_reply_whose_payload_belies_its_length_is_refused() {
        let mut frame = vec![VERSION, 0, 0, 1];
        frame.extend_from_slice(&8u32.to_be_bytes());
        frame.extend_from_slice(b"short");

        assert!(Reply::decode(&frame).is_err());
    }

    #[test]
    fn a_frame_shorter_than_its_header_is_refused() {
        assert!(Reply::decode(&[VERSION, 0, 0]).is_err());
    }

    #[test]
    fn framing_round_trips() {
        let mut sink = Vec::new();
        write_frame(&mut sink, b"hello").expect("writes");

        let mut source = std::io::Cursor::new(sink);
        let mut buffer = Vec::new();
        read_frame(&mut source, &mut buffer).expect("reads");

        assert_eq!(buffer, b"hello");
    }
}
