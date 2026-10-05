//! The server's side of registration (`docs/serving.md` section 6.2): keeps a server in its
//! balancer's table.
//!
//! [`Registration::run`] connects, opens `Register` with the server key, sends a heartbeat at
//! once and then every [`HEARTBEAT_INTERVAL`] with the latest [`Report`], and waits until the
//! balancer ends the call or the connection breaks. Then it registers again, after a backoff
//! (1 s doubling to 30 s, with jitter; back to 1 s after a registration that was taken). It
//! never gives up: a balancer that is down, restarting or misconfigured is waited for, and
//! the server keeps serving meanwhile.
//!
//! Does not measure anything: the server publishes its state on a `watch` channel, and each
//! heartbeat sends what is there.

use std::fmt;
use std::time::Duration;

use alloy_primitives::{BlockNumber, ChainId};
use futures_util::StreamExt as _;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tonic::codegen::http::uri::Authority;
use tonic::metadata::{AsciiMetadataValue, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tracing::{error, info, warn};

use crate::proto::Heartbeat;
use crate::proto::balancer_client::BalancerClient;
use crate::{KEEPALIVE_INTERVAL, KEEPALIVE_TIMEOUT};

/// How often a heartbeat is sent; the balancer drops a server after three missed.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long connecting, and the balancer's acknowledgement, may take.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(10);
/// Backoff between registration attempts.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Who the server is, and where its balancer is.
#[derive(Clone)]
pub struct Registration {
    /// The balancer's gRPC URI, e.g. `http://balancer.internal:50060`.
    pub balancer: String,
    /// The server key the balancer accepts on `Register`. Never logged.
    pub key: String,
    /// The server's name, unique in the deployment.
    pub id: String,
    /// The L2 chain id it follows.
    pub chain_id: ChainId,
    /// `host:port` where clients reach its stream and Flight services
    /// ([`is_valid_address`]).
    pub address: String,
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The key never appears in a log.
        f.debug_struct("Registration")
            .field("balancer", &self.balancer)
            .field("id", &self.id)
            .field("chain_id", &self.chain_id)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// Whether `address` is what [`Registration::address`] must be: exactly `host:port`, no
/// scheme or path. The balancer refuses a registration with any other.
pub fn is_valid_address(address: &str) -> bool {
    address
        .parse::<Authority>()
        .is_ok_and(|authority| authority.port_u16().is_some() && authority.as_str() == address)
}

/// What a server reports in each heartbeat: its health, heads and load, as of now.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Report {
    /// `false` while it cannot serve (its reads fail): the balancer then gives it no work.
    pub healthy: bool,
    /// Its heads, by number, when known.
    pub unsafe_head: Option<BlockNumber>,
    /// The safe head.
    pub safe_head: Option<BlockNumber>,
    /// The finalized head.
    pub finalized_head: Option<BlockNumber>,
    /// The last block of the sealed chunks it has read from the manifest.
    pub last_sealed: Option<BlockNumber>,
    /// Requests in flight: subscriptions, Flight streams, lookups.
    pub requests_in_flight: u32,
    /// Bytes it sends to clients per second.
    pub bytes_per_second: u64,
}

/// Why registration cannot start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RegisterError {
    /// The balancer's URI does not parse.
    #[error("invalid balancer URI {uri}")]
    Uri {
        /// The URI given.
        uri: String,
        /// Why it does not parse.
        #[source]
        source: tonic::transport::Error,
    },
    /// The server key cannot be sent as gRPC metadata (it is not visible ASCII).
    #[error("the balancer server key is not visible ASCII")]
    Key,
}

impl Registration {
    /// Keeps the server registered until `cancel` fires, sending `report`'s latest value in
    /// each heartbeat. Returns once cancelled; the balancer sees the call end and drops the
    /// server at once.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Uri`] if [`Self::balancer`] is not a URI and
    /// [`RegisterError::Key`] if [`Self::key`] cannot be sent, both before connecting. Every
    /// later failure is logged and retried.
    pub async fn run(
        self,
        report: watch::Receiver<Report>,
        cancel: CancellationToken,
    ) -> Result<(), RegisterError> {
        let endpoint = Endpoint::from_shared(self.balancer.clone())
            .map_err(|source| RegisterError::Uri {
                uri: self.balancer.clone(),
                source,
            })?
            .connect_timeout(ANSWER_TIMEOUT)
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true);
        let key: AsciiMetadataValue = MetadataValue::try_from(format!("Bearer {}", self.key))
            .map_err(|_not_ascii| RegisterError::Key)?;
        let mut backoff = MIN_BACKOFF;
        loop {
            let (ended, taken) = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                ended = self.session(&endpoint, &key, report.clone()) => ended,
            };
            if taken {
                backoff = MIN_BACKOFF;
            }
            let reason = ended.message();
            // These do not fix themselves: the balancer or this server is misconfigured.
            let refused = matches!(
                ended.code(),
                Code::Unauthenticated | Code::InvalidArgument | Code::FailedPrecondition
            );
            if refused {
                error!(balancer = %self.balancer, reason, "the balancer refused the registration");
            } else if taken {
                warn!(balancer = %self.balancer, reason, "registration ended; registering again");
            } else {
                warn!(balancer = %self.balancer, reason, "failed to register; trying again");
            }
            let jitter = backoff.mul_f64(fastrand::f64() * 0.5);
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                () = tokio::time::sleep(backoff.saturating_add(jitter)) => {}
            }
            backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
        }
    }

    /// One registration, from connecting to its end. Returns why it ended, and whether the
    /// balancer took it first.
    async fn session(
        &self,
        endpoint: &Endpoint,
        key: &AsciiMetadataValue,
        mut report: watch::Receiver<Report>,
    ) -> (tonic::Status, bool) {
        let channel: Channel = match endpoint.connect().await {
            Ok(channel) => channel,
            Err(err) => {
                return (
                    tonic::Status::unavailable(format!("cannot connect: {err}")),
                    false,
                );
            }
        };
        let mut ticks = tokio::time::interval(HEARTBEAT_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The transport owns the request stream, and would keep sending heartbeats after this
        // session is dropped: the stream ends with the session instead.
        let ended = CancellationToken::new();
        let _end_on_drop = ended.clone().drop_guard();
        let template = Heartbeat {
            id: self.id.clone(),
            chain_id: self.chain_id,
            address: self.address.clone(),
            ..Heartbeat::default()
        };
        let heartbeats = IntervalStream::new(ticks)
            .map(move |_| heartbeat(template.clone(), *report.borrow_and_update()))
            .take_until(ended.cancelled_owned());
        let mut request = tonic::Request::new(heartbeats);
        request.metadata_mut().insert("authorization", key.clone());
        let mut client = BalancerClient::new(channel);
        let answer = async {
            let mut replies = client.register(request).await?.into_inner();
            replies
                .message()
                .await?
                .ok_or_else(|| tonic::Status::unavailable("the balancer ended the call at once"))?;
            Ok::<_, tonic::Status>(replies)
        };
        let mut replies = match tokio::time::timeout(ANSWER_TIMEOUT, answer).await {
            Ok(Ok(replies)) => replies,
            Ok(Err(status)) => return (status, false),
            Err(_elapsed) => {
                return (
                    tonic::Status::deadline_exceeded("no answer from the balancer"),
                    false,
                );
            }
        };
        info!(balancer = %self.balancer, id = %self.id, "registered with the balancer");
        // The balancer sends nothing more: the call stays open until one side ends it.
        loop {
            match replies.message().await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return (
                        tonic::Status::unavailable("the balancer ended the call"),
                        true,
                    );
                }
                Err(status) => return (status, true),
            }
        }
    }
}

/// `template` (the server's id, chain and address) with `report`.
fn heartbeat(template: Heartbeat, report: Report) -> Heartbeat {
    let Report {
        healthy,
        unsafe_head,
        safe_head,
        finalized_head,
        last_sealed,
        requests_in_flight,
        bytes_per_second,
    } = report;
    Heartbeat {
        healthy,
        unsafe_head,
        safe_head,
        finalized_head,
        last_sealed,
        requests_in_flight,
        bytes_per_second,
        ..template
    }
}
