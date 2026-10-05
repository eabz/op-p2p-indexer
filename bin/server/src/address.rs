//! Where clients reach this server, as it registers with the balancer:
//! `OP_INDEXER_SERVER_ADDRESS`, else derived from the stream's listen address.
//!
//! - A listen address that names an IP (`127.0.0.1:50051` for a local deployment,
//!   `10.0.0.5:50051` for one interface) is the address, as it is.
//! - One on every interface (`0.0.0.0:50051`, `[::]:50051`) takes the public IP the
//!   execution network's discovery learns (`NodeView::public_ip`), with the listen port. The
//!   server waits for it before registering: tens of seconds after start, as discovery's peers
//!   agree on it. Past [`PATIENCE`] it warns, and keeps waiting.

use std::future;
use std::net::SocketAddr;
use std::time::Duration;

use op_indexer_node::NodeView;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// How long the public IP may take before the operator is told: discovery usually learns it
/// within half a minute.
const PATIENCE: Duration = Duration::from_secs(60);

/// The `host:port` to register: `given` (`OP_INDEXER_SERVER_ADDRESS`), else derived from
/// `listen`, the stream's listen address. `None` if `cancel` fires first, or the execution
/// network stops (the node is stopping).
///
/// # Errors
///
/// Returns an error if the address can only come from discovery and the execution network
/// does not run.
pub(crate) async fn resolve(
    given: Option<String>,
    listen: SocketAddr,
    view: &NodeView,
    cancel: &CancellationToken,
) -> eyre::Result<Option<String>> {
    if let Some(address) = given {
        return Ok(Some(address));
    }
    if !listen.ip().is_unspecified() {
        info!(address = %listen, "registering the stream's listen address");
        return Ok(Some(listen.to_string()));
    }
    let mut public_ip = view.public_ip().ok_or_else(|| {
        eyre::eyre!(
            "OP_INDEXER_SERVER_ADDRESS is required: the stream listens on every interface \
             ({listen}) and the execution network, which learns the public IP, is disabled"
        )
    })?;
    let warn_later = async {
        tokio::time::sleep(PATIENCE).await;
        warn!(
            waited = ?PATIENCE,
            "the public IP is not known yet; registration with the balancer waits for it \
             (set OP_INDEXER_SERVER_ADDRESS to skip the wait)"
        );
        future::pending::<()>().await;
    };
    let ip = tokio::select! {
        biased;
        () = cancel.cancelled() => return Ok(None),
        ip = public_ip.wait_for(Option::is_some) => ip.ok().and_then(|ip| *ip),
        () = warn_later => return Ok(None),
    };
    Ok(ip.map(|ip| {
        let address = SocketAddr::new(ip, listen.port());
        info!(%address, "registering the public IP execution discovery learned");
        address.to_string()
    }))
}
