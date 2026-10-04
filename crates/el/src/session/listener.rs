//! Accepting sessions peers open to us.
//!
//! Does not decide which inbound sessions are kept: completed ones go to the peer set.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use reth_eth_wire_types::DisconnectReason;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, warn};

use super::context::SessionContext;
use super::driver::{SessionDriver, SessionHandle};
use super::handshake;
use crate::warn_limit::WarnLimit;
use crate::{ElError, metrics};

/// Inbound connections whose handshake may be in progress at once; more are dropped.
const MAX_PENDING_INBOUND: usize = 8;
/// Of those, how many may come from one host (an address, or its /64 for IPv6), so one host
/// cannot hold every place.
const MAX_PENDING_PER_IP: usize = 2;
/// Pause after a failed `accept`. Its errors (no file descriptors left, mostly) do not go away
/// by asking again at once.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(250);
/// Shortest time between two warnings about dropped inbound connections.
const DROP_WARN_INTERVAL: Duration = Duration::from_mins(1);

/// An inbound session that completed its handshake, for the peer set to keep or refuse.
#[derive(Debug)]
pub(crate) struct Accepted {
    pub(crate) handle: SessionHandle,
    pub(crate) driver: SessionDriver,
}

/// Accepts connections on `addr` until `cancel` fires and sends each completed session to
/// `accepted`. A session the receiver has no room for is refused with "too many peers".
///
/// At most [`MAX_PENDING_INBOUND`] handshakes run at once, [`MAX_PENDING_PER_IP`] of them from
/// one host; further connections are dropped, counted and warned about at most once a minute.
///
/// # Errors
///
/// Returns [`ElError::Listen`] if the TCP port cannot be bound.
pub(crate) async fn listen(
    ctx: Arc<SessionContext>,
    addr: SocketAddr,
    accepted: mpsc::Sender<Accepted>,
    cancel: CancellationToken,
) -> Result<(), ElError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| ElError::Listen { addr, source })?;
    let mut handshakes: JoinSet<IpAddr> = JoinSet::new();
    // Handshakes in progress per address; an entry goes when its last handshake ends.
    let mut pending: HashMap<IpAddr, usize> = HashMap::new();
    let dropped = WarnLimit::default();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            Some(done) = handshakes.join_next() => {
                // A handshake that panicked leaves its address counted; the listener's
                // places are bounded either way.
                if let Ok(ip) = done
                    && let Some(count) = pending.get_mut(&ip)
                {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        pending.remove(&ip);
                    }
                }
            }
            connection = listener.accept() => {
                let (tcp, peer_addr) = match connection {
                    Ok(connection) => connection,
                    Err(err) => {
                        debug!(%err, "failed to accept execution peer connection");
                        tokio::select! {
                            biased;
                            () = cancel.cancelled() => break,
                            () = tokio::time::sleep(ACCEPT_ERROR_PAUSE) => continue,
                        }
                    }
                };
                let ip = host(peer_addr.ip());
                let from_ip = pending.get(&ip).copied().unwrap_or(0);
                // Without a tip the status would advertise genesis, which peers reject.
                if !ctx.has_tip() {
                    continue;
                }
                if handshakes.len() >= MAX_PENDING_INBOUND || from_ip >= MAX_PENDING_PER_IP {
                    metrics::inbound_dropped(ctx.spec().label);
                    if let Some(held_back) = dropped.allow(DROP_WARN_INTERVAL) {
                        warn!(
                            held_back,
                            pending = handshakes.len(),
                            "dropping inbound execution connections: the handshake places, \
                             or the host's, are taken"
                        );
                    }
                    continue;
                }
                pending.insert(ip, from_ip.saturating_add(1));
                let ctx = Arc::clone(&ctx);
                let accepted = accepted.clone();
                handshakes.spawn(async move {
                    match Box::pin(handshake::accept(&ctx, tcp, peer_addr)).await {
                        Ok((handle, driver)) => {
                            if let Err(refused) = accepted.try_send(Accepted { handle, driver }) {
                                let Accepted { driver, .. } = refused.into_inner();
                                metrics::inbound_refused(ctx.spec().label);
                                driver.reject(DisconnectReason::TooManyPeers).await;
                            }
                        }
                        Err(err) => {
                            metrics::inbound_handshake_failed(ctx.spec().label, err.stage());
                            debug!(addr = %peer_addr, %err, "inbound handshake failed");
                        }
                    }
                    ip
                }.in_current_span());
            }
        }
    }
    handshakes.shutdown().await;
    Ok(())
}

/// What sessions per host are counted by: the address, or its /64 for IPv6, where one host
/// holds a whole /64.
pub(crate) fn host(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => {
            let [a, b, c, d, ..] = v6.segments();
            IpAddr::V6(Ipv6Addr::new(a, b, c, d, 0, 0, 0, 0))
        }
    }
}
