//! Accepting sessions peers open to us.
//!
//! Does not decide which inbound sessions are kept: completed ones go to the peer set.

use std::net::SocketAddr;
use std::sync::Arc;

use reth_eth_wire_types::DisconnectReason;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::context::SessionContext;
use super::driver::{SessionDriver, SessionHandle};
use super::handshake::Session;
use crate::{ElError, metrics};

/// Inbound connections whose handshake may be in progress at once; more are dropped.
const MAX_PENDING_INBOUND: usize = 8;

/// An inbound session that completed its handshake, for the peer set to keep or refuse.
#[derive(Debug)]
pub(crate) struct Accepted {
    pub(crate) handle: SessionHandle,
    pub(crate) driver: SessionDriver,
}

/// Accepts connections on `addr` until `cancel` fires and sends each completed session to
/// `accepted`. A session the receiver has no room for is refused with "too many peers".
///
/// At most [`MAX_PENDING_INBOUND`] handshakes run at once; further connections are dropped.
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
    let mut handshakes = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            Some(_done) = handshakes.join_next() => {}
            connection = listener.accept() => {
                let (tcp, peer_addr) = match connection {
                    Ok(connection) => connection,
                    Err(err) => {
                        debug!(%err, "failed to accept execution peer connection");
                        continue;
                    }
                };
                // Without a tip the status would advertise genesis, which peers reject.
                if handshakes.len() >= MAX_PENDING_INBOUND || !ctx.has_tip() {
                    continue;
                }
                let ctx = Arc::clone(&ctx);
                let accepted = accepted.clone();
                handshakes.spawn(async move {
                    match Box::pin(Session::accept(&ctx, tcp, peer_addr)).await {
                        Ok((handle, driver)) => {
                            if let Err(refused) = accepted.try_send(Accepted { handle, driver }) {
                                let Accepted { driver, .. } = refused.into_inner();
                                metrics::inbound_refused();
                                driver.reject(DisconnectReason::TooManyPeers).await;
                            }
                        }
                        Err(err) => {
                            metrics::inbound_handshake_failed(err.stage());
                            debug!(addr = %peer_addr, %err, "inbound handshake failed");
                        }
                    }
                });
            }
        }
    }
    handshakes.shutdown().await;
    Ok(())
}
