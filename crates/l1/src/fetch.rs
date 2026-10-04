//! Verified reads from L1 execution peers: headers by hash, and a block's transactions and
//! receipts against the roots in its header.
//!
//! Everything returned hangs on a hash the caller trusts: a header must hash to the hash it
//! was asked by (or to its child's parent hash), the transactions must hash to the header's
//! transactions root, the receipts to its receipts root. A peer whose answer fails that is
//! reported and another one is asked. Does not decide which blocks to read (`watch`).

use std::time::Duration;

use alloy_consensus::proofs::calculate_receipt_root;
use alloy_consensus::{EthereumReceipt, Header, ReceiptEnvelope, Sealed};
use alloy_primitives::{B256, Bytes, keccak256};
use alloy_rlp::{Decodable, EMPTY_LIST_CODE};
use op_indexer_el::{Peers, Report, RequestError, SessionHandle};
use op_indexer_primitives::{rlp_list_items, transactions_root};
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Longest wait before the sessions are tried again after none of them gave a usable answer
/// (or none is open yet). A session opening or ending ends the wait early.
const RETRY: Duration = Duration::from_secs(5);
/// Rounds over the open sessions after which a block nobody served is given up: the hash may
/// be of a block that was replaced, which no peer keeps.
const MAX_ROUNDS: u32 = 6;

/// Why a read ended without the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
    /// The node is shutting down.
    Cancelled,
    /// No open session served the block in [`MAX_ROUNDS`] rounds.
    NotHeld,
}

/// Why one peer's answer was not used.
enum Unusable {
    /// The peer does not hold what was asked: no fault.
    NotHeld,
    /// The answer is not what was asked for: the peer is banned.
    Wrong,
    /// The answer cannot be decoded: possibly a kind of data this build does not know.
    Undecodable,
    /// The request failed.
    Request(RequestError),
}

impl From<RequestError> for Unusable {
    fn from(err: RequestError) -> Self {
        Self::Request(err)
    }
}

/// Reads from the open L1 sessions.
#[derive(Debug)]
pub(crate) struct Fetcher {
    peers: Peers,
}

impl Fetcher {
    pub(crate) const fn new(peers: Peers) -> Self {
        Self { peers }
    }

    /// Fetches up to `limit` headers going down from the block with hash `start`, inclusive,
    /// each one the parent of the one before it. Never empty.
    pub(crate) async fn headers(
        &self,
        start: B256,
        limit: u64,
        cancel: &CancellationToken,
    ) -> Result<Vec<Sealed<Header>>, Stop> {
        let request = |session: SessionHandle| async move {
            linked_headers(start, &session.headers(start, limit).await?)
        };
        self.ask("headers", cancel, request).await
    }

    /// Fetches the transactions of `block`, each in the encoding the transactions trie holds,
    /// checked against the header's transactions root.
    pub(crate) async fn transactions(
        &self,
        block: &Sealed<Header>,
        cancel: &CancellationToken,
    ) -> Result<Vec<Bytes>, Stop> {
        let (hash, root) = (block.hash(), block.transactions_root);
        let request = |session: SessionHandle| async move {
            let bodies = session.bodies(vec![hash]).await?;
            let body = bodies.into_iter().next().ok_or(Unusable::NotHeld)?;
            // Hashing a block's transactions is CPU work.
            spawn_blocking(move || transactions(&body, root))
                .await
                .unwrap_or(Err(Unusable::Undecodable))
        };
        self.ask("body", cancel, request).await
    }

    /// Fetches the receipts of `block`, which has `count` transactions, checked against the
    /// header's receipts root.
    pub(crate) async fn receipts(
        &self,
        block: &Sealed<Header>,
        count: usize,
        cancel: &CancellationToken,
    ) -> Result<Vec<ReceiptEnvelope>, Stop> {
        let (hash, root) = (block.hash(), block.receipts_root);
        let request = |session: SessionHandle| async move {
            let blocks = session.receipts(vec![hash]).await?;
            let item = blocks.into_iter().next().ok_or(Unusable::NotHeld)?;
            // Counted, decoded and hashed off the runtime.
            spawn_blocking(move || receipts(&item, count, root))
                .await
                .unwrap_or(Err(Unusable::Undecodable))
        };
        self.ask("receipts", cancel, request).await
    }

    /// Asks the open sessions one after another, in a random order so the load spreads over
    /// the peers, until one gives a usable answer. A peer at fault is reported. With no usable
    /// answer it waits and tries again, [`MAX_ROUNDS`] times over sessions that were open.
    async fn ask<T, F, Fut>(
        &self,
        what: &'static str,
        cancel: &CancellationToken,
        request: F,
    ) -> Result<T, Stop>
    where
        F: Fn(SessionHandle) -> Fut,
        Fut: Future<Output = Result<T, Unusable>>,
    {
        let mut rounds = 0_u32;
        loop {
            let mut sessions = self.peers.sessions().to_vec();
            fastrand::shuffle(&mut sessions);
            // Waiting for the first session is not a round.
            if !sessions.is_empty() {
                rounds = rounds.saturating_add(1);
            }
            for session in sessions {
                match request(session.clone()).await {
                    Ok(answer) => return Ok(answer),
                    Err(unusable) => self.note(&session, what, &unusable),
                }
            }
            if rounds >= MAX_ROUNDS {
                return Err(Stop::NotHeld);
            }
            let mut peers = self.peers.clone();
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(Stop::Cancelled),
                _ = peers.changed() => {}
                () = tokio::time::sleep(RETRY) => {}
            }
        }
    }

    /// Logs why `session`'s answer was not used and reports the peer if it is at fault.
    fn note(&self, session: &SessionHandle, what: &'static str, unusable: &Unusable) {
        let peer = session.peer_id();
        let report = match unusable {
            Unusable::NotHeld => {
                debug!(%peer, what, "L1 peer does not hold the block");
                return;
            }
            Unusable::Wrong | Unusable::Request(RequestError::Excess { .. }) => {
                Report::BadData(peer)
            }
            Unusable::Undecodable | Unusable::Request(RequestError::Malformed(_)) => {
                Report::Undecodable(peer)
            }
            Unusable::Request(RequestError::Timeout) => Report::Unresponsive(peer),
            // The session is gone already.
            Unusable::Request(RequestError::SessionClosed) => return,
        };
        debug!(%peer, what, ?report, "L1 peer's answer not used");
        self.peers.report(report);
    }
}

/// Checks that `items` are headers linked down from the block with hash `start`, and decodes
/// them.
fn linked_headers(start: B256, items: &[Bytes]) -> Result<Vec<Sealed<Header>>, Unusable> {
    if items.is_empty() {
        return Err(Unusable::NotHeld);
    }
    let mut expected = start;
    let mut headers = Vec::with_capacity(items.len());
    for item in items {
        let hash = keccak256(item);
        if hash != expected {
            return Err(Unusable::Wrong);
        }
        // It hashes to a trusted hash, so it is a header: one that does not decode is ours
        // to blame, not the peer's.
        let header: Header =
            alloy_rlp::decode_exact(item).map_err(|_undecodable| Unusable::Undecodable)?;
        expected = header.parent_hash;
        headers.push(Sealed::new_unchecked(header, hash));
    }
    Ok(headers)
}

/// Cuts a block body into its transactions, each as the transactions trie holds it, and
/// checks them against `root`. CPU work.
fn transactions(body: &Bytes, root: B256) -> Result<Vec<Bytes>, Unusable> {
    let cut = || {
        let fields = rlp_list_items(body)?;
        let items = rlp_list_items(fields.first()?)?;
        items
            .into_iter()
            .map(|item| {
                // A legacy transaction is its RLP list; a typed one is wrapped in an RLP
                // string, and the trie holds what is inside.
                let leaf = if item.first().is_some_and(|first| *first >= EMPTY_LIST_CODE) {
                    item
                } else {
                    alloy_rlp::Header::decode_bytes(&mut &*item, false).ok()?
                };
                Some(body.slice_ref(leaf))
            })
            .collect::<Option<Vec<Bytes>>>()
    };
    let leaves = cut().ok_or(Unusable::Wrong)?;
    if transactions_root(&leaves) != root {
        return Err(Unusable::Wrong);
    }
    Ok(leaves)
}

/// Decodes a block's receipts from an eth/69 answer, rebuilds their blooms and checks them
/// against `root`. The receipts are counted over their RLP headers before anything is decoded:
/// an answer of millions of tiny receipts must not become as many blooms. CPU work.
fn receipts(item: &Bytes, count: usize, root: B256) -> Result<Vec<ReceiptEnvelope>, Unusable> {
    let items = rlp_list_items(item).ok_or(Unusable::Wrong)?;
    if items.is_empty() && count > 0 {
        return Err(Unusable::NotHeld);
    }
    if items.len() != count {
        return Err(Unusable::Wrong);
    }
    let decoded = Vec::<EthereumReceipt>::decode(&mut item.as_ref())
        .map_err(|_undecodable| Unusable::Undecodable)?;
    let receipts: Vec<ReceiptEnvelope> = decoded.into_iter().map(ReceiptEnvelope::from).collect();
    if calculate_receipt_root(&receipts) != root {
        return Err(Unusable::Wrong);
    }
    Ok(receipts)
}
