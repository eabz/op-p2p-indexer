//! The balancer's gRPC service: `Register` for servers (6.2, 6.3) and `Locate` for clients
//! (D17).
//!
//! Each registration is a task that reads the server's heartbeats and keeps its [`Table`]
//! entry current; three missed heartbeats, the server leaving, or a newer registration of the
//! same id end it and remove the entry. Keys are checked here rather than by an interceptor,
//! since the two calls take different keys.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use op_indexer_chainspec::ChainSpec;
use op_indexer_stream::ApiKeys;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt as _};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::proto::balancer_server;
use crate::proto::{Heartbeat, LocateRequest, LocateResponse, Registered};
use crate::register::{HEARTBEAT_INTERVAL, is_valid_address};
use crate::table::{Server, Table};

/// A server is down once no heartbeat came for this long: three missed (6.3).
const DOWN_AFTER: Duration = HEARTBEAT_INTERVAL.saturating_mul(3);
/// Replies queued on a registration after its acknowledgement: the reason it ended.
const REPLIES: usize = 1;

/// Registration ids, so the end of a replaced registration does not remove its successor.
static CALLS: AtomicU64 = AtomicU64::new(0);

/// The balancer service.
#[derive(Debug)]
pub(crate) struct Service {
    pub(crate) chain: &'static ChainSpec,
    pub(crate) table: Table,
    /// Keys of `Locate`.
    pub(crate) users: ApiKeys,
    /// Keys of `Register`.
    pub(crate) servers: ApiKeys,
    pub(crate) tasks: TaskTracker,
    pub(crate) cancel: CancellationToken,
}

/// The replies of a registration.
type Replies = Pin<Box<dyn Stream<Item = Result<Registered, Status>> + Send>>;

#[tonic::async_trait]
impl balancer_server::Balancer for Service {
    type RegisterStream = Replies;

    async fn register(
        &self,
        request: Request<Streaming<Heartbeat>>,
    ) -> Result<Response<Self::RegisterStream>, Status> {
        self.servers.verify(request.metadata())?;
        let mut heartbeats = request.into_inner();
        let first = tokio::time::timeout(DOWN_AFTER, heartbeats.message())
            .await
            .map_err(|_elapsed| Status::deadline_exceeded("no heartbeat"))??
            .ok_or_else(|| Status::invalid_argument("no heartbeat"))?;
        let last_sealed = first.last_sealed;
        let (id, server) = checked(self.chain, first)?;
        let call = CALLS.fetch_add(1, Ordering::Relaxed);
        self.table.insert(&id, call, server.clone());
        info!(server = %id, address = %server.address, last_sealed, "server registered");
        let (replies, rx) = mpsc::channel(REPLIES);
        let registration = Registration {
            chain: self.chain,
            table: self.table.clone(),
            id,
            call,
            heartbeats,
            replies,
        };
        self.tasks.spawn(registration.run(self.cancel.clone()));
        let replies = tokio_stream::once(Ok(Registered {})).chain(ReceiverStream::new(rx));
        Ok(Response::new(Box::pin(replies)))
    }

    async fn locate(
        &self,
        request: Request<LocateRequest>,
    ) -> Result<Response<LocateResponse>, Status> {
        self.users.verify(request.metadata())?;
        let LocateRequest {
            chain_id,
            from_block,
        } = request.into_inner();
        if chain_id != self.chain.chain_id {
            return Err(Status::invalid_argument(other_chain(self.chain, chain_id)));
        }
        // A subscription reads history up to the server's head, then follows it: any healthy
        // server whose head reaches the block before `from_block` serves it.
        let endpoints: Vec<String> = self
            .table
            .picker()
            .pick(usize::MAX, |server| {
                server
                    .unsafe_head
                    .is_some_and(|head| head.saturating_add(1) >= from_block)
            })
            .into_iter()
            .map(|address| format!("http://{address}"))
            .collect();
        if endpoints.is_empty() {
            return Err(Status::unavailable(format!(
                "no healthy server has reached block {from_block}"
            )));
        }
        Ok(Response::new(LocateResponse { endpoints }))
    }
}

/// One server's open registration.
struct Registration {
    chain: &'static ChainSpec,
    table: Table,
    id: String,
    call: u64,
    heartbeats: Streaming<Heartbeat>,
    replies: mpsc::Sender<Result<Registered, Status>>,
}

impl Registration {
    /// Follows the heartbeats until the registration ends, then removes the server and tells
    /// it why, when the balancer ended it.
    async fn run(mut self, cancel: CancellationToken) {
        let ended = loop {
            let next = tokio::select! {
                biased;
                () = cancel.cancelled() => break Some(Status::unavailable("the balancer is shutting down")),
                next = tokio::time::timeout(DOWN_AFTER, self.heartbeats.message()) => next,
            };
            let heartbeat = match next {
                Ok(Ok(Some(heartbeat))) => heartbeat,
                // The server left (or its connection broke): it registers again.
                Ok(Ok(None) | Err(_)) => break None,
                Err(_elapsed) => break Some(Status::deadline_exceeded("missed heartbeats")),
            };
            match checked(self.chain, heartbeat) {
                Ok((id, _)) if id != self.id => {
                    break Some(Status::invalid_argument(
                        "a heartbeat changed the server id",
                    ));
                }
                Ok((_, server)) => {
                    if !self.table.update(&self.id, self.call, server) {
                        break Some(Status::already_exists(
                            "a newer registration took this server id",
                        ));
                    }
                }
                Err(status) => break Some(status),
            }
        };
        self.table.remove(&self.id, self.call);
        match ended {
            None => info!(server = %self.id, "server left"),
            Some(status) if status.code() == Code::Unavailable => {
                debug!(server = %self.id, "registration ended on shutdown");
            }
            Some(status) => {
                warn!(server = %self.id, reason = status.message(), "server removed");
                if let Err(_gone) = self.replies.try_send(Err(status)) {
                    debug!(server = %self.id, "server gone before it could be told why");
                }
            }
        }
    }
}

/// The server a heartbeat describes, with its id, if it is of `chain` and well formed.
fn checked(chain: &ChainSpec, heartbeat: Heartbeat) -> Result<(String, Server), Status> {
    let Heartbeat {
        id,
        chain_id,
        address,
        healthy,
        unsafe_head,
        safe_head,
        finalized_head,
        last_sealed: _,
        requests_in_flight,
        bytes_per_second,
    } = heartbeat;
    if chain_id != chain.chain_id {
        return Err(Status::failed_precondition(other_chain(chain, chain_id)));
    }
    if id.is_empty() {
        return Err(Status::invalid_argument("a heartbeat needs the server id"));
    }
    if !is_valid_address(&address) {
        return Err(Status::invalid_argument(
            "the address must be host:port, nothing else",
        ));
    }
    let server = Server {
        address,
        healthy,
        unsafe_head,
        safe_head,
        finalized_head,
        requests_in_flight,
        bytes_per_second,
    };
    Ok((id, server))
}

fn other_chain(chain: &ChainSpec, chain_id: u64) -> String {
    format!(
        "this balancer serves chain {} ({}), not {chain_id}",
        chain.chain_id, chain.name
    )
}
