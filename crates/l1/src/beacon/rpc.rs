//! The request/response wire format of the beacon network, and the few messages this node
//! sends and answers.
//!
//! A request is one `ssz_snappy` payload: the SSZ length as a varint, then the SSZ in snappy
//! frames; some requests have no payload. A response is a sequence of chunks, each a result
//! byte, then (for light-client data) the four bytes of the fork digest its SSZ is of, then a
//! payload ([encoding strategies], [light-client request/response]).
//!
//! Lengths come from peers: a payload longer than the protocol's limit is refused before
//! anything is decompressed, and a response is read up to a fixed number of bytes.
//!
//! Does not decode light-client containers or verify anything: see `types` and `verify`.
//!
//! [encoding strategies]: https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/p2p-interface.md#encoding-strategies
//! [light-client request/response]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/p2p-interface.md#the-reqresp-domain

use std::io::{self, Read, Write};

use alloy_primitives::B256;
use futures_util::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::StreamProtocol;
use libp2p::request_response;

use super::spec::ForkDigest;

/// `Status`: the first exchange with a peer.
pub(super) const STATUS: &str = "/eth2/beacon_chain/req/status/1/ssz_snappy";
/// `Ping`: liveness, with the sender's metadata sequence number.
pub(super) const PING: &str = "/eth2/beacon_chain/req/ping/1/ssz_snappy";
/// `GetMetaData`, as extended in Altair.
pub(super) const METADATA_V2: &str = "/eth2/beacon_chain/req/metadata/2/ssz_snappy";
/// `GetMetaData`, as extended in Fulu.
pub(super) const METADATA_V3: &str = "/eth2/beacon_chain/req/metadata/3/ssz_snappy";
/// `Goodbye`: the reason a peer leaves.
pub(super) const GOODBYE: &str = "/eth2/beacon_chain/req/goodbye/1/ssz_snappy";
/// `GetLightClientBootstrap`.
pub(super) const BOOTSTRAP: &str = "/eth2/beacon_chain/req/light_client_bootstrap/1/ssz_snappy";
/// `LightClientUpdatesByRange`.
pub(super) const UPDATES_BY_RANGE: &str =
    "/eth2/beacon_chain/req/light_client_updates_by_range/1/ssz_snappy";
/// `GetLightClientFinalityUpdate`.
pub(super) const FINALITY_UPDATE: &str =
    "/eth2/beacon_chain/req/light_client_finality_update/1/ssz_snappy";
/// `GetLightClientOptimisticUpdate`.
pub(super) const OPTIMISTIC_UPDATE: &str =
    "/eth2/beacon_chain/req/light_client_optimistic_update/1/ssz_snappy";

/// Result byte of a successful response chunk.
pub(super) const SUCCESS: u8 = 0;
/// Largest SSZ of a light-client object accepted: an update with its sync committee is about
/// 26 KiB.
pub(super) const MAX_LIGHT_CLIENT_BYTES: usize = 64 * 1024;
/// Largest SSZ of the small messages (status, ping, metadata, goodbye, an error text).
pub(super) const MAX_SMALL_BYTES: usize = 1024;
/// Most updates asked for in one `LightClientUpdatesByRange`: one per period, about a week.
pub(super) const MAX_UPDATES: u64 = 8;
/// `CUSTODY_REQUIREMENT`: the fewest custody groups a node may report from Fulu on.
const CUSTODY_REQUIREMENT: u64 = 4;
/// Bytes a varint length may take.
const MAX_VARINT_BYTES: usize = 10;

/// One chunk of a response.
#[derive(Debug, Clone)]
pub(super) struct Chunk {
    /// The result byte: [`SUCCESS`], or an error code with a text as its SSZ.
    pub(super) code: u8,
    /// The fork digest the SSZ is of; zero where the protocol sends none.
    pub(super) context: ForkDigest,
    pub(super) ssz: Vec<u8>,
}

impl Chunk {
    /// A successful chunk without a fork digest.
    pub(super) const fn plain(ssz: Vec<u8>) -> Self {
        Self {
            code: SUCCESS,
            context: [0; 4],
            ssz,
        }
    }
}

/// How one protocol's messages are framed.
#[derive(Debug, Clone, Copy)]
pub(super) struct Codec {
    /// Whether the request carries a payload.
    request_payload: bool,
    /// Whether successful response chunks carry a fork digest.
    context: bool,
    /// Largest SSZ of a response chunk.
    max_chunk_bytes: usize,
    /// Most chunks in a response.
    max_chunks: usize,
}

impl Codec {
    /// The framing of `protocol`, one of this module's protocol names.
    pub(super) fn of(protocol: &str) -> Self {
        let light_client = protocol.contains("/light_client_");
        Self {
            request_payload: !matches!(
                protocol,
                METADATA_V2 | METADATA_V3 | FINALITY_UPDATE | OPTIMISTIC_UPDATE
            ),
            context: light_client,
            max_chunk_bytes: if light_client {
                MAX_LIGHT_CLIENT_BYTES
            } else {
                MAX_SMALL_BYTES
            },
            max_chunks: if protocol == UPDATES_BY_RANGE {
                usize::try_from(MAX_UPDATES).unwrap_or(1)
            } else {
                1
            },
        }
    }

    /// Bytes a whole response may take on the wire.
    fn max_response_bytes(&self) -> u64 {
        let chunk = snap::raw::max_compress_len(self.max_chunk_bytes).saturating_add(64);
        u64::try_from(chunk.saturating_mul(self.max_chunks)).unwrap_or(u64::MAX)
    }
}

impl request_response::Codec for Codec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = Vec<Chunk>;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Vec<u8>>
    where
        T: AsyncRead + Unpin + Send,
    {
        let limit = snap::raw::max_compress_len(MAX_SMALL_BYTES).saturating_add(64);
        let mut wire = Vec::new();
        io.take(u64::try_from(limit).unwrap_or(u64::MAX))
            .read_to_end(&mut wire)
            .await?;
        if wire.is_empty() {
            return Ok(Vec::new());
        }
        decode_payload(&mut wire.as_slice(), MAX_SMALL_BYTES)
    }

    async fn read_response<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Vec<Chunk>>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut wire = Vec::new();
        io.take(self.max_response_bytes())
            .read_to_end(&mut wire)
            .await?;
        let mut rest = wire.as_slice();
        let mut chunks = Vec::new();
        while let Some((&code, after)) = rest.split_first() {
            if chunks.len() >= self.max_chunks {
                return Err(invalid("more response chunks than asked for"));
            }
            rest = after;
            let mut context = [0; 4];
            let limit = if code == SUCCESS {
                if self.context {
                    let (digest, after) = rest
                        .split_first_chunk::<4>()
                        .ok_or_else(|| invalid("response chunk without its fork digest"))?;
                    context = *digest;
                    rest = after;
                }
                self.max_chunk_bytes
            } else {
                MAX_SMALL_BYTES
            };
            let ssz = decode_payload(&mut rest, limit)?;
            chunks.push(Chunk { code, context, ssz });
        }
        Ok(chunks)
    }

    async fn write_request<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        request: Vec<u8>,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        if self.request_payload {
            io.write_all(&encode_payload(&request)?).await?;
        }
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        chunks: Vec<Chunk>,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        for chunk in chunks {
            let mut wire = vec![chunk.code];
            if self.context && chunk.code == SUCCESS {
                wire.extend_from_slice(&chunk.context);
            }
            wire.extend(encode_payload(&chunk.ssz)?);
            io.write_all(&wire).await?;
        }
        io.close().await
    }
}

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

/// Encodes one payload: the SSZ length as a varint, then the SSZ in snappy frames.
fn encode_payload(ssz: &[u8]) -> io::Result<Vec<u8>> {
    let length = u64::try_from(ssz.len()).map_err(|_overflow| invalid("payload too long"))?;
    let mut buffer = unsigned_varint::encode::u64_buffer();
    let mut wire = unsigned_varint::encode::u64(length, &mut buffer).to_vec();
    let mut encoder = snap::write::FrameEncoder::new(&mut wire);
    encoder.write_all(ssz)?;
    encoder.flush()?;
    drop(encoder);
    Ok(wire)
}

/// Decodes one payload from the front of `wire`, which is left at the byte after it. The
/// length is checked against `max_bytes` before anything is decompressed.
fn decode_payload(wire: &mut &[u8], max_bytes: usize) -> io::Result<Vec<u8>> {
    let head = wire
        .get(..wire.len().min(MAX_VARINT_BYTES))
        .unwrap_or_default();
    let (length, after) =
        unsigned_varint::decode::u64(head).map_err(|_bad| invalid("invalid payload length"))?;
    let length = usize::try_from(length)
        .ok()
        .filter(|length| *length <= max_bytes)
        .ok_or_else(|| invalid("payload longer than the protocol allows"))?;
    let consumed = head.len().saturating_sub(after.len());
    *wire = wire.get(consumed..).unwrap_or_default();
    let mut ssz = Vec::with_capacity(length);
    if length > 0 {
        // The decoder reads whole frames and no further, so `wire` ends up after the payload.
        let decoder = snap::read::FrameDecoder::new(&mut *wire);
        decoder
            .take(u64::try_from(length).unwrap_or(u64::MAX))
            .read_to_end(&mut ssz)?;
    }
    if ssz.len() != length {
        return Err(invalid("payload shorter than its length"));
    }
    Ok(ssz)
}

/// What a node reports about its chain in a `Status` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StatusData {
    pub(super) finalized_root: B256,
    pub(super) finalized_epoch: u64,
    pub(super) head_root: B256,
    pub(super) head_slot: u64,
}

/// The SSZ of a `Status` message ([Status]).
///
/// [Status]: https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/p2p-interface.md#status-v1
pub(super) fn status(digest: ForkDigest, data: StatusData) -> Vec<u8> {
    let mut ssz = Vec::with_capacity(84);
    ssz.extend_from_slice(&digest);
    ssz.extend_from_slice(data.finalized_root.as_slice());
    ssz.extend_from_slice(&data.finalized_epoch.to_le_bytes());
    ssz.extend_from_slice(data.head_root.as_slice());
    ssz.extend_from_slice(&data.head_slot.to_le_bytes());
    ssz
}

/// The fork digest in the SSZ of a peer's `Status`.
pub(super) fn status_digest(ssz: &[u8]) -> Option<ForkDigest> {
    ssz.first_chunk::<4>().copied()
}

/// The SSZ of this node's metadata: sequence number 0 and no subnets; with `fulu`, also the
/// custody group count, [`CUSTODY_REQUIREMENT`] ([GetMetaData v3]). A light client attests
/// nothing and keeps no data columns, but peers hang up on a count below the minimum.
///
/// [GetMetaData v3]: https://github.com/ethereum/consensus-specs/blob/master/specs/fulu/p2p-interface.md#getmetadata-v3
pub(super) fn metadata(fulu: bool) -> Vec<u8> {
    // seq_number (8) | attnets, 64 bits (8) | syncnets, 4 bits (1) | custody_group_count (8)
    let mut ssz = vec![0; 17];
    if fulu {
        ssz.extend_from_slice(&CUSTODY_REQUIREMENT.to_le_bytes());
    }
    ssz
}

/// The SSZ of a `LightClientUpdatesByRange` request.
pub(super) fn updates_by_range(start_period: u64, count: u64) -> Vec<u8> {
    let mut ssz = start_period.to_le_bytes().to_vec();
    ssz.extend_from_slice(&count.to_le_bytes());
    ssz
}
