//! Opening a session: TCP, the encrypted transport, the p2p hello and the eth status.
//!
//! Each step is reth's and runs under its own timeout. Does not keep the session: it hands
//! back a handle and a driver.

use std::net::SocketAddr;
use std::time::Duration;

use alloy_eip2124::{ForkId, ValidationError};
use alloy_primitives::{B256, U256};
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::errors::{EthHandshakeError, EthStreamError, P2PHandshakeError, P2PStreamError};
use reth_eth_wire::protocol::Protocol;
use reth_eth_wire::{EthNetworkPrimitives, HelloMessage, UnauthedEthStream, UnauthedP2PStream};
use reth_eth_wire_types::{DisconnectReason, EthVersion, UnifiedStatus};
use reth_network_peers::{PeerId, pk2id};
use secp256k1::SECP256K1;
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::context::SessionContext;
use super::driver::{self, SessionDriver, SessionHandle};
use crate::discovery::Candidate;

/// Limit for opening the TCP connection.
const TCP_TIMEOUT: Duration = Duration::from_secs(8);

/// Limit for the encrypted handshake: a round trip, so short; a connection that holds a
/// pending inbound place without speaking frees it soon.
const ECIES_TIMEOUT: Duration = Duration::from_secs(5);

/// Limit for the hello exchange.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

/// Limit for the status exchange, short like the others: an inbound connection holds a
/// pending place until it ends.
const STATUS_TIMEOUT: Duration = Duration::from_secs(5);

/// What this node calls itself in the hello.
const CLIENT_VERSION: &str = concat!("op-indexer/", env!("CARGO_PKG_VERSION"));

/// Who opened the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Direction {
    /// We dialed the peer.
    Outbound,
    /// The peer dialed us.
    Inbound,
}

/// What a peer told us in the handshake.
#[derive(Debug, Clone)]
pub(crate) struct PeerStatus {
    pub(crate) peer_id: PeerId,
    pub(crate) addr: SocketAddr,
    pub(crate) direction: Direction,
    /// The client's name and version, as it reports them.
    pub(crate) client: String,
    pub(crate) fork_id: ForkId,
    /// First block the peer says it serves.
    pub(crate) earliest: Option<u64>,
    /// Last block the peer says it has.
    pub(crate) latest: Option<u64>,
    /// Hash of the peer's head.
    pub(crate) head_hash: B256,
    /// Whether the peer is a known op-p2p-indexer: blocks held back from other peers are
    /// shared with it.
    pub(crate) indexer: bool,
    /// The eth version the session speaks: 69, or 68 with a peer that does not speak 69. An
    /// eth/68 peer is served but never asked: see `SessionHandle::is_askable`.
    pub(crate) version: EthVersion,
}

/// Why a session could not be established.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SessionError {
    #[error("tcp connection failed")]
    Tcp(#[source] std::io::Error),
    /// The peer closed the connection during the encrypted handshake, which carries no reason.
    #[error("peer dropped the connection during the encrypted handshake")]
    Ecies,
    /// The hello exchange failed; with the peer's reason if it gave one.
    #[error("hello failed: {}", reason_text(*.0))]
    Hello(Option<DisconnectReason>),
    /// The peer does not speak eth/69.
    #[error("peer shares no eth version with us")]
    NoSharedEth,
    /// The status exchange failed; with the peer's reason if it gave one.
    #[error("status failed: {}", reason_text(*reason))]
    Status { reason: Option<DisconnectReason> },
    /// The peer is on another fork: it missed an upgrade, or this build did.
    #[error("fork id mismatch: remote {remote:?}")]
    ForkMismatch { remote: ForkId },
    /// The peer is on another chain.
    #[error("peer is on another chain")]
    WrongChain,
    #[error("{stage} timed out")]
    Timeout { stage: &'static str },
}

/// Dials `candidate` and performs the whole handshake, each step under its own timeout.
///
/// The caller must run the returned driver (`driver.run(cancel)`) for the session to live.
pub(crate) async fn connect(
    ctx: &SessionContext,
    candidate: &Candidate,
) -> Result<(SessionHandle, SessionDriver), SessionError> {
    let tcp = timeout(TCP_TIMEOUT, TcpStream::connect(candidate.addr))
        .await
        .map_err(|_elapsed| SessionError::Timeout { stage: "tcp" })?
        .map_err(SessionError::Tcp)?;
    // reth's handshake futures are tens of kilobytes; keep them off the caller's stack.
    let connect = Box::pin(ECIESStream::connect(tcp, *ctx.key(), candidate.peer_id));
    let ecies = timeout(ECIES_TIMEOUT, connect)
        .await
        .map_err(|_elapsed| SessionError::Timeout { stage: "ecies" })?
        .map_err(|_closed| SessionError::Ecies)?;
    Box::pin(handshake(ctx, ecies, candidate.addr, Direction::Outbound)).await
}

/// Performs the handshake on a connection a peer opened.
pub(super) async fn accept(
    ctx: &SessionContext,
    tcp: TcpStream,
    addr: SocketAddr,
) -> Result<(SessionHandle, SessionDriver), SessionError> {
    let ecies = timeout(
        ECIES_TIMEOUT,
        Box::pin(ECIESStream::incoming(tcp, *ctx.key())),
    )
    .await
    .map_err(|_elapsed| SessionError::Timeout { stage: "ecies" })?
    .map_err(|_closed| SessionError::Ecies)?;
    Box::pin(handshake(ctx, ecies, addr, Direction::Inbound)).await
}

/// The hello and status exchanges on an encrypted connection.
async fn handshake(
    ctx: &SessionContext,
    ecies: ECIESStream<TcpStream>,
    addr: SocketAddr,
    direction: Direction,
) -> Result<(SessionHandle, SessionDriver), SessionError> {
    let peer_id = ecies.remote_id();
    let hello = HelloMessage::builder(pk2id(&ctx.key().public_key(SECP256K1)))
        // eth/69; and eth/68 where we serve, so peers that do not speak 69 can sync from us.
        .protocols(
            std::iter::once(Protocol::eth(EthVersion::Eth69))
                .chain(ctx.serves().then(|| Protocol::eth(EthVersion::Eth68))),
        )
        .client_version(CLIENT_VERSION)
        .port(ctx.listen_port())
        .build();
    let (p2p, their_hello) = timeout(
        HELLO_TIMEOUT,
        Box::pin(UnauthedP2PStream::new(ecies).handshake(hello)),
    )
    .await
    .map_err(|_elapsed| SessionError::Timeout { stage: "hello" })?
    .map_err(|err| {
        if let P2PStreamError::HandshakeError(P2PHandshakeError::Disconnected(reason))
        | P2PStreamError::Disconnected(reason) = err
        {
            SessionError::Hello(Some(reason))
        } else if matches!(
            err,
            P2PStreamError::HandshakeError(P2PHandshakeError::NoSharedCapabilities)
        ) {
            SessionError::NoSharedEth
        } else {
            SessionError::Hello(None)
        }
    })?;
    // The highest version both sides speak (RLPx, message ID-based multiplexing).
    let version = p2p
        .shared_capabilities()
        .eth_version()
        .map_err(|_none| SessionError::NoSharedEth)?;

    let fork_filter = ctx.fork_filter();
    let indexer = ctx.is_indexer(&peer_id);
    // What is advertised: the held range as it is (from the Bedrock block on, for a peer that
    // is not an indexer), else the tip alone (see `AdvertisedRange`). Sessions open only once
    // a tip is known.
    let (serving, answers) = ctx.session_serving(indexer, version);
    let advertised = serving.advertised();
    let latest = advertised.map(|range| range.latest);
    // eth/68 carries a total difficulty (0; peers check only its size) and the head hash;
    // eth/69 carries the range.
    let status = UnifiedStatus {
        version,
        chain: ctx.spec().network_id.into(),
        genesis: ctx.spec().genesis_hash,
        forkid: fork_filter.current(),
        blockhash: latest.map_or(ctx.spec().genesis_hash, |latest| latest.hash),
        total_difficulty: Some(U256::ZERO),
        earliest_block: Some(advertised.map_or(0, |range| range.earliest)),
        latest_block: Some(latest.map_or(0, |latest| latest.number)),
    };
    let exchange = Box::pin(
        UnauthedEthStream::new(p2p).handshake::<EthNetworkPrimitives>(status, fork_filter),
    );
    let (eth, theirs) = timeout(STATUS_TIMEOUT, exchange)
        .await
        .map_err(|_elapsed| SessionError::Timeout { stage: "status" })?
        .map_err(|err| status_error(ctx, &err))?;

    // A peer on our chain announcing a fork we do not know, still ahead.
    if ctx.spec().is_unknown_next(theirs.forkid) {
        ctx.warn_build_behind(theirs.forkid, "eth status");
        ctx.horizon().announced(addr.ip(), theirs.forkid.next);
    }
    let peer = PeerStatus {
        peer_id,
        addr,
        direction,
        client: their_hello.client_version,
        fork_id: theirs.forkid,
        earliest: theirs.earliest_block,
        latest: theirs.latest_block,
        head_hash: theirs.blockhash,
        indexer,
        version,
    };
    Ok(driver::new(peer, eth.into_inner(), serving, answers))
}

/// Maps reth's status error, raising the "build is behind" warning where it applies.
fn status_error(ctx: &SessionContext, err: &EthStreamError) -> SessionError {
    if let EthStreamError::EthHandshakeError(EthHandshakeError::InvalidFork(mismatch)) = err {
        return match *mismatch {
            // The peer is on a fork we do not know, or has passed one we have not.
            ValidationError::LocalIncompatibleOrStale { remote, .. } => {
                ctx.warn_build_behind(remote, "eth status");
                SessionError::ForkMismatch { remote }
            }
            ValidationError::RemoteStale { remote, .. } => SessionError::ForkMismatch { remote },
        };
    }
    if matches!(
        err,
        EthStreamError::EthHandshakeError(
            EthHandshakeError::MismatchedGenesis(_) | EthHandshakeError::MismatchedChain(_)
        )
    ) {
        return SessionError::WrongChain;
    }
    let reason = if let EthStreamError::P2PStreamError(P2PStreamError::Disconnected(reason)) = err {
        Some(*reason)
    } else {
        None
    };
    SessionError::Status { reason }
}

fn reason_text(reason: Option<DisconnectReason>) -> String {
    reason.map_or_else(|| "no reason given".to_owned(), |reason| reason.to_string())
}
