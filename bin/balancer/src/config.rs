//! The balancer's configuration, from the environment (and `.env`, loaded before).

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use eyre::{WrapErr, eyre};
use op_indexer_balancer::BalancerConfig;
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use op_indexer_chunks::{R2Config, ReadOptions};
use op_indexer_runtime::config;
use op_indexer_runtime::setting as var;

/// Clear of the stream's default port (50051).
const DEFAULT_LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 50060);

/// Everything the balancer reads at startup.
#[derive(Debug)]
pub(crate) struct BalancerSettings {
    pub(crate) chain: &'static ChainSpec,
    pub(crate) balancer: BalancerConfig,
    /// The chain's bucket, read-only: the manifest.
    pub(crate) r2: R2Config,
    /// The chain's bucket with the presign key, if one is set: raw chunk plans.
    pub(crate) presign: Option<R2Config>,
    pub(crate) read: ReadOptions,
}

impl BalancerSettings {
    /// Reads the configuration:
    ///
    /// - `--env-file <path>` (command line): read by the env-file loader before this; no other
    ///   argument is taken.
    /// - `chain`: L2 chain id, one of [`ChainSpec::ALL`] (default 10, OP
    ///   Mainnet). One balancer per chain.
    /// - `balancer.listen_addr`: gRPC listen socket of the balancer service and
    ///   Flight (default `0.0.0.0:50060`).
    /// - `balancer.server_keys` (required): comma-separated keys servers register
    ///   with. Never logged.
    /// - `balancer.api_keys`: comma-separated user keys, the servers' own: a client
    ///   reads from the servers with the key it asked the balancer with (default: none, no
    ///   check). Never logged.
    /// - `r2.account_id`, `r2.access_key_id`,
    ///   `r2.secret_access_key` (required), `r2.bucket`,
    ///   `r2.prefix`, `r2.endpoint`: the chain's bucket, as for `server`;
    ///   a read-only key is enough.
    /// - `r2.presign_access_key_id`, `r2.presign_secret_access_key`: a
    ///   read-only R2 key the balancer signs raw chunk URLs with (`docs/serving.md`, raw chunk
    ///   download). Both or neither; without them raw plans are refused. Never logged; a URL
    ///   carries the key's id, never its secret.
    ///
    /// # Errors
    ///
    /// Returns an error if a required variable is missing or one is invalid, or an argument
    /// is unknown.
    pub(crate) fn from_config_and_args() -> eyre::Result<Self> {
        eyre::ensure!(
            config::other_args(env::args_os().skip(1)).is_empty(),
            "unknown balancer argument; use --config, --chain or --check-config"
        );
        let chain_id = var("OP_INDEXER_CHAIN_ID")
            .map(|id| {
                id.parse::<u64>()
                    .wrap_err_with(|| format!("chain is invalid: {id}"))
            })
            .transpose()?
            .unwrap_or(OP_MAINNET.chain_id);
        let chain = ChainSpec::by_chain_id(chain_id)
            .ok_or_else(|| eyre!("unsupported chain id {chain_id}"))?;
        let listen_addr = var("OP_INDEXER_BALANCER_LISTEN_ADDR")
            .map(|addr| {
                addr.parse()
                    .wrap_err_with(|| format!("balancer.listen_addr is invalid: {addr}"))
            })
            .transpose()?
            .unwrap_or(DEFAULT_LISTEN_ADDR);
        let server_keys = list("OP_INDEXER_BALANCER_SERVER_KEYS");
        if server_keys.is_empty() {
            return Err(eyre!("balancer.server_keys is required"));
        }
        let r2 = R2Config::from_lookup(chain, var)?;
        let presign = match (
            var("OP_INDEXER_R2_PRESIGN_ACCESS_KEY_ID"),
            var("OP_INDEXER_R2_PRESIGN_SECRET_ACCESS_KEY"),
        ) {
            (Some(access_key_id), Some(secret_access_key)) => Some(R2Config {
                access_key_id,
                secret_access_key,
                ..r2.clone()
            }),
            (None, None) => None,
            _ => {
                return Err(eyre!(
                    "set both r2.presign_access_key_id and \
                     r2.presign_secret_access_key, or neither"
                ));
            }
        };
        Ok(Self {
            chain,
            balancer: BalancerConfig {
                listen_addr,
                api_keys: list("OP_INDEXER_STREAM_API_KEYS"),
                server_keys,
            },
            r2,
            presign,
            read: ReadOptions::default(),
        })
    }
}

/// A comma-separated list, without empty items.
fn list(name: &str) -> Vec<String> {
    var(name)
        .map(|list| {
            list.split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}
