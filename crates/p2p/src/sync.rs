//! The `payload_by_number` server: serves blocks by number to peers that fill gaps in their
//! unsafe chain this way ([`payload_by_number`]).
//!
//! op-node deprecated the client side and keeps the server for older nodes
//! ([req/resp sync deprecation]); this node serves, never asks. It answers as op-node's server
//! does (`op-node/p2p/sync.go`), the protocol id included (without the trailing `/` the spec
//! text shows): a result byte, then for a block the payload version (little-endian `u32`) and
//! the payload as SSZ in snappy frames. Version 0 is the bare `ExecutionPayload` (V1, V2),
//! version 1 the `ExecutionPayloadEnvelope` from Ecotone on (V3, V4).
//!
//! Requests are rate limited as op-node's are, with `governor`: 4 a second per peer (bursts of
//! 15), then 20 a second overall (bursts of 40). The peer's own limit is waited for first, so
//! one peer cannot drain the shared budget; a request is answered with result 3 when both take
//! more than 20 seconds, or at once when 4 of the peer's requests, or 512 in all, already wait.
//! At most 32 answers are read, encoded or being written at once (bounding the memory they
//! hold), and an answer must be written within 10 seconds, as op-node's write deadline. A number before the chain's Bedrock block, or past the
//! block the wall clock implies, is an invalid request (result 2).
//!
//! [`payload_by_number`]: https://specs.optimism.io/protocol/rollup-node-p2p.html#payload_by_number
//! [req/resp sync deprecation]: https://docs.optimism.io/notices/archive/req-resp-cl-sync-deprecation

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{BlockNumber, Bytes};
use governor::clock::DefaultClock;
use governor::state::keyed::HashMapStateStore;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use libp2p::futures::future::BoxFuture;
use libp2p::futures::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use libp2p::request_response::{self, InboundRequestId, ProtocolSupport, ResponseChannel};
use libp2p::{PeerId, StreamProtocol};
use op_alloy_rpc_types_engine::{OpExecutionPayload, OpExecutionPayloadEnvelope};
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::{EncodedBlock, decode_block, encode_body, split_body};
use ssz::Encode as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{self, JoinError, JoinSet};

/// The canonical block at a height, or why it could not be read.
pub type BlockFuture<'a> =
    BoxFuture<'a, Result<Option<EncodedBlock>, Box<dyn Error + Send + Sync>>>;

/// The blocks the node can serve: the canonical block at a height, from the archive or the
/// unsafe store. Implemented by the binary, which holds the stores.
pub trait PayloadSource: Send + Sync + fmt::Debug + 'static {
    /// The canonical block at `number`, in its consensus encoding; `None` if it is not held.
    ///
    /// # Errors
    ///
    /// Returns the store's error if it cannot be read.
    fn canonical_block(&self, number: BlockNumber) -> BlockFuture<'_>;
}

/// The block was served: the version and payload follow.
const SUCCESS: u8 = 0;
/// A valid request for a block this node does not hold.
const NOT_FOUND: u8 = 1;
/// A request for a block that cannot exist.
const INVALID: u8 = 2;
/// Any other failure, rate limiting included.
const UNKNOWN: u8 = 3;
/// The most a response may hold, as the spec recommends.
const MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
/// How long a request may wait for the rate limits before it is answered with [`UNKNOWN`].
const MAX_THROTTLE_DELAY: Duration = Duration::from_secs(20);
/// How long a peer has to send its request, and the node its answer.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall: 20 requests a second, bursts of 40.
const GLOBAL_RATE: (u32, u32) = (20, 40);
/// Per peer: 4 requests a second, bursts of 15 (30 s of 2 s blocks).
const PEER_RATE: (u32, u32) = (4, 15);
/// Requests waiting for the limits at once; past it a request is answered with [`UNKNOWN`]
/// at once, so a flood cannot pile up waiting tasks.
const MAX_WAITING: usize = 512;
/// Requests of one peer waiting at once, so one peer cannot fill [`MAX_WAITING`].
const MAX_WAITING_PER_PEER: usize = 4;
/// Answers read, encoded or being written at once: each holds a block and its encoding, up to
/// 10 MiB, until the peer has it (or the write fails), so this bounds the memory answers hold.
const MAX_ENCODING: usize = 32;
/// How long writing an answer may take, as op-node's write deadline.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Peers whose rate is remembered; past it the peers whose limit has fully recovered are
/// forgotten.
const MAX_RATED_PEERS: usize = 1000;
/// The refusal of a call this server does not make.
const SERVER_ONLY: &str = "this node only serves payload_by_number";

/// The protocol id: `/opstack/req/payload_by_number/<chain id>/0`, as op-node registers it.
fn protocol(chain_id: u64) -> StreamProtocol {
    StreamProtocol::try_from_owned(format!("/opstack/req/payload_by_number/{chain_id}/0"))
        .unwrap_or(StreamProtocol::new("/opstack/req/payload_by_number/0/0"))
}

/// The request-response behaviour, answering only.
pub(crate) fn behaviour(chain_id: u64) -> request_response::Behaviour<Codec> {
    request_response::Behaviour::new(
        [(protocol(chain_id), ProtocolSupport::Inbound)],
        request_response::Config::default().with_request_timeout(REQUEST_TIMEOUT),
    )
}

/// Reads a request (a little-endian `u64`) and writes the encoded response as it is.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Codec;

impl request_response::Codec for Codec {
    type Protocol = StreamProtocol;
    type Request = BlockNumber;
    type Response = Vec<u8>;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<BlockNumber>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut number = [0; 8];
        io.read_exact(&mut number).await?;
        Ok(BlockNumber::from_le_bytes(number))
    }

    fn read_response<T>(
        &mut self,
        _: &StreamProtocol,
        _io: &mut T,
    ) -> impl Future<Output = io::Result<Vec<u8>>> + Send
    where
        T: AsyncRead + Unpin + Send,
    {
        std::future::ready(Err(io::Error::other(SERVER_ONLY)))
    }

    fn write_request<T>(
        &mut self,
        _: &StreamProtocol,
        _io: &mut T,
        _request: BlockNumber,
    ) -> impl Future<Output = io::Result<()>> + Send
    where
        T: AsyncWrite + Unpin + Send,
    {
        std::future::ready(Err(io::Error::other(SERVER_ONLY)))
    }

    async fn write_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        response: Vec<u8>,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let write = async {
            io.write_all(&response).await?;
            io.close().await
        };
        tokio::time::timeout(WRITE_TIMEOUT, write)
            .await
            .unwrap_or_else(|_elapsed| Err(io::ErrorKind::TimedOut.into()))
    }
}

/// A quota of `per_second` requests with bursts of `burst`.
fn quota((per_second, burst): (u32, u32)) -> Quota {
    let count = |n: u32| NonZeroU32::new(n).unwrap_or(NonZeroU32::MIN);
    Quota::per_second(count(per_second)).allow_burst(count(burst))
}

/// A rate limiter per peer.
type PeerLimiter = RateLimiter<PeerId, HashMapStateStore<PeerId>, DefaultClock>;

/// Whether `response` carries a block.
pub(crate) fn is_success(response: &[u8]) -> bool {
    response.first() == Some(&SUCCESS)
}

/// The answer to a request that could not wait for the rate limits.
pub(crate) fn throttled() -> Vec<u8> {
    vec![UNKNOWN]
}

/// An answer ready to send, with the channel it goes on.
#[derive(Debug)]
pub(crate) struct Answer {
    pub(crate) request_id: InboundRequestId,
    pub(crate) channel: ResponseChannel<Vec<u8>>,
    pub(crate) response: Vec<u8>,
    /// Its place among [`MAX_ENCODING`]: kept until the answer is written
    /// ([`Server::writing`]); `None` for an answer that was throttled.
    pub(crate) permit: Option<OwnedSemaphorePermit>,
}

/// The server's state: the source, the limits, and the answers being prepared.
pub(crate) struct Server {
    chain: &'static ChainSpec,
    source: Arc<dyn PayloadSource>,
    global: Arc<DefaultDirectRateLimiter>,
    peers: Arc<PeerLimiter>,
    /// Answers being read and encoded, each after its wait for the limits; at most
    /// [`MAX_WAITING`].
    answers: JoinSet<Answer>,
    /// The peer each task of [`Self::answers`] is for, so a task that panicked is counted out.
    tasks: HashMap<task::Id, PeerId>,
    /// Requests of each peer in [`Self::answers`].
    waiting: HashMap<PeerId, usize>,
    /// Answers handed to the swarm and not yet written, with their places among
    /// [`MAX_ENCODING`].
    writing: HashMap<InboundRequestId, OwnedSemaphorePermit>,
    /// Permits to read, encode and write an answer; [`MAX_ENCODING`] of them.
    encoding: Arc<Semaphore>,
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server")
            .field("chain_id", &self.chain.chain_id)
            .field("answers", &self.answers.len())
            .finish_non_exhaustive()
    }
}

impl Server {
    pub(crate) fn new(chain: &'static ChainSpec, source: Arc<dyn PayloadSource>) -> Self {
        Self {
            chain,
            source,
            global: Arc::new(RateLimiter::direct(quota(GLOBAL_RATE))),
            peers: Arc::new(RateLimiter::hashmap(quota(PEER_RATE))),
            answers: JoinSet::new(),
            tasks: HashMap::new(),
            waiting: HashMap::new(),
            writing: HashMap::new(),
            encoding: Arc::new(Semaphore::new(MAX_ENCODING)),
        }
    }

    /// Prepares the answer to `peer`'s request for `number`, once the rate limits allow it.
    /// Returns the channel when too many requests are already waiting, in all or from `peer`:
    /// the caller answers [`throttled`] at once.
    pub(crate) fn on_request(
        &mut self,
        peer: PeerId,
        request_id: InboundRequestId,
        number: BlockNumber,
        channel: ResponseChannel<Vec<u8>>,
    ) -> Option<ResponseChannel<Vec<u8>>> {
        let waiting = self.waiting.get(&peer).copied().unwrap_or_default();
        if self.answers.len() >= MAX_WAITING || waiting >= MAX_WAITING_PER_PEER {
            return Some(channel);
        }
        self.waiting.insert(peer, waiting.saturating_add(1));
        if self.peers.len() >= MAX_RATED_PEERS {
            self.peers.retain_recent();
        }
        let (chain, source) = (self.chain, Arc::clone(&self.source));
        let (global, peers) = (Arc::clone(&self.global), Arc::clone(&self.peers));
        let encoding = Arc::clone(&self.encoding);
        let task = self.answers.spawn(async move {
            // The peer's own limit first: a peer over it takes nothing from the others.
            let allowed = async {
                peers.until_key_ready(&peer).await;
                global.until_ready().await;
            };
            let (response, permit) = match tokio::time::timeout(MAX_THROTTLE_DELAY, allowed).await {
                Ok(()) => match encoding.acquire_owned().await {
                    Ok(permit) => (serve(chain, &*source, number).await, Some(permit)),
                    // The semaphore is never closed.
                    Err(_closed) => (throttled(), None),
                },
                Err(_elapsed) => (throttled(), None),
            };
            Answer {
                request_id,
                channel,
                response,
                permit,
            }
        });
        self.tasks.insert(task.id(), peer);
        None
    }

    /// The next answer ready; never resolves while none is being prepared.
    pub(crate) async fn next_answer(&mut self) -> Option<Result<Answer, JoinError>> {
        let joined = self.answers.join_next_with_id().await?;
        // A task that panicked is counted out of its peer's requests too.
        let id = joined.as_ref().map_or_else(JoinError::id, |(id, _)| *id);
        if let Some(peer) = self.tasks.remove(&id)
            && let Some(waiting) = self.waiting.get_mut(&peer)
        {
            *waiting = waiting.saturating_sub(1);
            if *waiting == 0 {
                self.waiting.remove(&peer);
            }
        }
        Some(joined.map(|(_, answer)| answer))
    }

    /// Keeps an answer's place among [`MAX_ENCODING`] until [`Self::written`].
    pub(crate) fn writing(&mut self, request_id: InboundRequestId, permit: OwnedSemaphorePermit) {
        self.writing.insert(request_id, permit);
    }

    /// The answer to `request_id` was written, or will never be: its place is free.
    pub(crate) fn written(&mut self, request_id: InboundRequestId) {
        self.writing.remove(&request_id);
    }
}

/// The answer to a request for `number`, read from `source`. Never fails: a failure is its
/// result byte.
async fn serve(chain: &ChainSpec, source: &dyn PayloadSource, number: BlockNumber) -> Vec<u8> {
    if number < chain.bedrock_block || number > expected_tip(chain) {
        return vec![INVALID];
    }
    let block = match source.canonical_block(number).await {
        Ok(Some(block)) => block,
        Ok(None) => return vec![NOT_FOUND],
        Err(err) => {
            tracing::debug!(number, %err, "payload_by_number: the stores cannot be read");
            return vec![UNKNOWN];
        }
    };
    // SSZ and snappy over a whole block: CPU work.
    task::spawn_blocking(move || encode(&block))
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| vec![UNKNOWN])
}

/// The block the wall clock implies: the newest that may exist.
fn expected_tip(chain: &ChainSpec) -> BlockNumber {
    let now = crate::network::unix_now_secs();
    chain
        .bedrock_block
        .saturating_add(chain.blocks_in(now.saturating_sub(chain.bedrock_time)))
}

/// A successful response for `block`; `None` if it does not decode or exceeds the limit.
///
/// The payload is built from the header and the body's transactions as held: only the header
/// is decoded (through a copy of the block with no transactions), and each transaction is
/// copied in its consensus encoding, which is what the payload carries.
fn encode(block: &EncodedBlock) -> Option<Vec<u8>> {
    let parts = split_body(&block.body)?;
    let shell = EncodedBlock {
        body: encode_body(&[] as &[&[u8]], parts.withdrawals),
        receipts: None,
        ..block.clone()
    };
    let (decoded, _receipts) = decode_block(&shell).ok()?;
    let (mut execution_payload, _sidecar) =
        OpExecutionPayload::from_block_unchecked(block.hash, &decoded);
    execution_payload.as_v1_mut().transactions = parts
        .transactions
        .iter()
        .map(|transaction| Bytes::copy_from_slice(transaction))
        .collect();
    let version: u32 = match execution_payload {
        OpExecutionPayload::V1(_) | OpExecutionPayload::V2(_) => 0,
        OpExecutionPayload::V3(_) | OpExecutionPayload::V4(_) => 1,
    };
    let envelope = OpExecutionPayloadEnvelope {
        parent_beacon_block_root: decoded.header.parent_beacon_block_root,
        execution_payload,
    };
    let mut response = vec![SUCCESS];
    response.extend_from_slice(&version.to_le_bytes());
    let mut framed = snap::write::FrameEncoder::new(response);
    io::Write::write_all(&mut framed, &envelope.as_ssz_bytes()).ok()?;
    let response = framed.into_inner().ok()?;
    (response.len() <= MAX_RESPONSE_BYTES).then_some(response)
}
