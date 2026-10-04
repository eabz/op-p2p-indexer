//! One devp2p (`RLPx`) session with an execution peer, outbound or inbound.
//!
//! ```text
//! TCP ─▶ ECIES (encrypted transport) ─▶ p2p hello (eth/69) ─▶ eth status ─▶ SessionDriver
//! ```
//!
//! The transport, the hello and the status exchange are reth's. After the handshake a
//! [`SessionDriver`] owns the stream: it answers pings, passes the peer's requests to the server
//! (`serve`) and writes its answers, follows the block range the peer announces, and routes
//! responses to the requests made through a [`SessionHandle`].
//!
//! Does not decide which peers to dial or keep, and does not verify what a peer returns: that
//! is the peer set and the fetcher. Nothing a peer sends is trusted here beyond being framed.

mod context;
mod driver;
mod handshake;
mod listener;

pub(crate) use context::{SessionContext, unix_now};
pub(crate) use driver::{EndReason, RequestError, SessionDriver, SessionEnd, SessionHandle};
pub(crate) use handshake::{Direction, Session, SessionError};
pub(crate) use listener::{Accepted, listen};
