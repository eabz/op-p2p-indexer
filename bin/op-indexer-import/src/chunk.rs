//! The file of a verified chunk: each block in its consensus encoding, with its senders.
//!
//! ```text
//! file   = parent hash of the first block (32) | hash of the last block (32) | zstd(blocks)
//! block  = hash (32) | sender count (u32) | senders (20 each)
//!          | header length (u32) | header RLP
//!          | body length (u32)   | body RLP
//!          | receipts length (u32) | receipts RLP
//! ```
//!
//! Integers are little-endian. The three RLP values are an [`EncodedBlock`]'s: the bytes
//! `verify` checked against the block hash and the header's roots, which `load` hands to the
//! archive unchanged. The two leading hashes let the chain of chunks be checked without
//! decompressing anything.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use alloy_primitives::{Address, B256, Bytes};
use op_indexer_primitives::EncodedBlock;

use crate::state::write_atomic;

/// Compression level of the verified chunks: zstd's default.
const COMPRESSION_LEVEL: i32 = 3;

/// A verified block.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedBlock {
    /// Header, body and receipts as verified; the receipts are always present.
    pub(crate) encoded: EncodedBlock,
    /// Sender of each transaction, in block order; the zero address for a transaction signed
    /// with all zeros.
    pub(crate) senders: Vec<Address>,
}

/// How a chunk attaches to its neighbours.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Link {
    /// Parent hash in the chunk's first header.
    pub(crate) first_parent: B256,
    /// Hash of the chunk's last block.
    pub(crate) last_hash: B256,
}

/// Writes the verified chunk at `path`, atomically. Blocking.
///
/// # Errors
///
/// Returns the I/O error, or `InvalidInput` if a value is longer than `u32::MAX` bytes.
pub(crate) fn write(path: &Path, link: Link, blocks: &[VerifiedBlock]) -> io::Result<()> {
    write_atomic(path, |file| {
        file.write_all(link.first_parent.as_slice())?;
        file.write_all(link.last_hash.as_slice())?;
        let mut out = zstd::stream::Encoder::new(file, COMPRESSION_LEVEL)?;
        // A damaged file then fails to decompress instead of passing on wrong bytes. Files
        // written without it still read: the flag is per frame.
        out.include_checksum(true)?;
        for block in blocks {
            out.write_all(block.encoded.hash.as_slice())?;
            out.write_all(&length(block.senders.len())?)?;
            for sender in &block.senders {
                out.write_all(sender.as_slice())?;
            }
            let receipts = block
                .encoded
                .receipts
                .as_ref()
                .map_or(&[][..], |receipts| &receipts[..]);
            for value in [&block.encoded.header[..], &block.encoded.body[..], receipts] {
                out.write_all(&length(value.len())?)?;
                out.write_all(value)?;
            }
        }
        out.finish()?;
        Ok(())
    })
}

/// Reads only how the chunk at `path` attaches to its neighbours. Blocking.
///
/// # Errors
///
/// Returns the I/O error; `UnexpectedEof` if the file is too short.
pub(crate) fn read_link(path: &Path) -> io::Result<Link> {
    read_link_from(&mut File::open(path)?)
}

/// Reads the verified chunk at `path`. Blocking.
///
/// # Errors
///
/// Returns the I/O error; `UnexpectedEof` or `InvalidData` if the file is damaged.
pub(crate) fn read(path: &Path) -> io::Result<(Link, Vec<VerifiedBlock>)> {
    let mut file = File::open(path)?;
    let link = read_link_from(&mut file)?;
    // One buffer for the chunk: every value below is a slice of it, not a copy.
    let data = Bytes::from(zstd::stream::decode_all(file)?);
    let mut rest: &[u8] = &data;
    let mut blocks = Vec::new();
    while !rest.is_empty() {
        let hash = B256::from_slice(take(&mut rest, 32)?);
        let count = take_length(&mut rest)?;
        let senders = take(&mut rest, count.saturating_mul(20))?
            .as_chunks::<20>()
            .0
            .iter()
            .map(Address::from)
            .collect();
        let mut value = || -> io::Result<Bytes> {
            let len = take_length(&mut rest)?;
            Ok(data.slice_ref(take(&mut rest, len)?))
        };
        let (header, body, receipts) = (value()?, value()?, value()?);
        blocks.push(VerifiedBlock {
            encoded: EncodedBlock {
                hash,
                header,
                body,
                receipts: Some(receipts),
            },
            senders,
        });
    }
    Ok((link, blocks))
}

fn read_link_from(file: &mut File) -> io::Result<Link> {
    // One read for both hashes.
    let mut hashes = [0_u8; 64];
    file.read_exact(&mut hashes)?;
    let (first_parent, last_hash) = hashes.split_at(32);
    Ok(Link {
        first_parent: B256::from_slice(first_parent),
        last_hash: B256::from_slice(last_hash),
    })
}

fn length(len: usize) -> io::Result<[u8; 4]> {
    u32::try_from(len)
        .map(u32::to_le_bytes)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))
}

fn take<'a>(rest: &mut &'a [u8], len: usize) -> io::Result<&'a [u8]> {
    let (taken, after) = rest
        .split_at_checked(len)
        .ok_or(io::ErrorKind::UnexpectedEof)?;
    *rest = after;
    Ok(taken)
}

fn take_length(rest: &mut &[u8]) -> io::Result<usize> {
    let bytes: [u8; 4] = take(rest, 4)?
        .try_into()
        .map_err(|_short| io::ErrorKind::UnexpectedEof)?;
    usize::try_from(u32::from_le_bytes(bytes))
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}
