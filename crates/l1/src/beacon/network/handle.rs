//! What the light client sees of the network: the requests it can make, the answers and
//! gossip it gets, and the handle it makes them through.

use alloy_primitives::{B256, Bytes};
use libp2p::PeerId;
use libp2p::gossipsub::{MessageAcceptance, MessageId};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::beacon::spec::ForkDigest;

/// Requests and reports waiting for the swarm task. The light client sends one at a time.
pub(super) const COMMAND_CAPACITY: usize = 16;

/// What the light client asks peers for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::beacon) enum Request {
    /// The bootstrap of the block with this root.
    Bootstrap(B256),
    /// The update of one sync-committee period (`LightClientUpdatesByRange` with a count of
    /// one, see `rpc::updates_by_range`).
    UpdatesByRange { period: u64 },
    /// The newest finality update.
    FinalityUpdate,
    /// The newest optimistic update.
    OptimisticUpdate,
}

/// A peer's answer.
#[derive(Debug)]
pub(in crate::beacon) struct Response {
    pub(in crate::beacon) peer: PeerId,
    /// The successful chunks in order, each with the fork digest its SSZ is of. Empty: the
    /// peer does not hold what was asked.
    pub(in crate::beacon) chunks: Vec<(ForkDigest, Bytes)>,
}

/// Why a request has no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(in crate::beacon) enum RequestError {
    /// No peer to ask turned up in time: for a bootstrap, none that has not already said it
    /// lacks it.
    #[error("no beacon peer to ask")]
    NoPeer,
    #[error("beacon peer {0} did not answer")]
    Unanswered(PeerId),
    /// The peer answered with an error code of the protocol.
    #[error("beacon peer {0} refused the request with code {1}")]
    Refused(PeerId, u8),
    /// The answer is not in the wire format, or longer than the protocol allows. The peer is
    /// dropped.
    #[error("beacon peer {0} sent a malformed answer")]
    Malformed(PeerId),
}

/// The gossip topics of light-client data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::beacon) enum Topic {
    Finality,
    Optimistic,
}

/// A gossip message, decompressed and otherwise as the peer sent it.
#[derive(Debug)]
pub(in crate::beacon) struct Gossip {
    /// What gossipsub knows the message by, for [`NetworkHandle::report_gossip`].
    pub(in crate::beacon) id: MessageId,
    /// The peer that forwarded it.
    pub(in crate::beacon) peer: PeerId,
    pub(in crate::beacon) topic: Topic,
    pub(in crate::beacon) data: Bytes,
}

/// What the swarm task is asked to do.
#[derive(Debug)]
pub(in crate::beacon) enum Command {
    Request {
        request: Request,
        reply: Reply,
    },
    Invalid(PeerId),
    Gossip {
        id: MessageId,
        peer: PeerId,
        verdict: Verdict,
    },
}

/// What the light client found a gossip message to be ([gossip validation]): accepted
/// (it verified and is newer than what was held, so it is forwarded to the mesh), rejected
/// (it does not verify, by the fault of who sent it) or ignored (a duplicate, older than
/// what is held, or not verifiable yet).
///
/// [gossip validation]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/p2p-interface.md#the-gossip-domain-gossipsub
pub(in crate::beacon) type Verdict = MessageAcceptance;

/// The light client's side of the network.
#[derive(Debug, Clone)]
pub(in crate::beacon) struct NetworkHandle {
    pub(in crate::beacon) commands: mpsc::Sender<Command>,
}

impl NetworkHandle {
    /// Sends `request` to one peer and waits for its answer, at most `REQUEST_TIMEOUT`.
    ///
    /// Without a peer to ask, the request waits for one for the same time. A bootstrap is not
    /// asked of a peer that already answered one without data.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`]; [`RequestError::NoPeer`] also once the network has stopped.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future gives up the answer; the request may still go out.
    pub(in crate::beacon) async fn request(
        &self,
        request: Request,
    ) -> Result<Response, RequestError> {
        let (reply, answer) = oneshot::channel();
        let command = Command::Request { request, reply };
        if self.commands.send(command).await.is_err() {
            return Err(RequestError::NoPeer);
        }
        answer.await.unwrap_or(Err(RequestError::NoPeer))
    }

    /// Reports what a gossip message was found to be. Until then it is not forwarded.
    pub(in crate::beacon) fn report_gossip(&self, id: MessageId, peer: PeerId, verdict: Verdict) {
        let command = Command::Gossip { id, peer, verdict };
        if self.commands.try_send(command).is_err() {
            debug!(%peer, "verdict on a gossip message dropped");
        }
    }

    /// Reports a peer whose data did not verify: it is disconnected and not dialed again.
    pub(in crate::beacon) fn report_invalid(&self, peer: PeerId) {
        if self.commands.try_send(Command::Invalid(peer)).is_err() {
            debug!(%peer, "report of an invalid beacon peer dropped");
        }
    }
}

/// Who gets the answer to a request.
pub(in crate::beacon) type Reply = oneshot::Sender<Result<Response, RequestError>>;
