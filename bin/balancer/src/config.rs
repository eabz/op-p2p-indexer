//! The balancer's configuration, from the environment (and `.env`, loaded before).

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use eyre::{WrapErr, eyre};
use op_indexer_balancer::BalancerConfig;
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use op_indexer_chunks::{R2Config, ReadOptions};
use op_indexer_runtime::env_file;
use op_indexer_runtime::env_var as var;

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
    /// - `OP_INDEXER_CHAIN_ID`: L2 chain id, one of [`ChainSpec::ALL`] (default 10, OP
    ///   Mainnet). One balancer per chain.
    /// - `OP_INDEXER_BALANCER_LISTEN_ADDR`: gRPC listen socket of the balancer service and
    ///   Flight (default `0.0.0.0:50060`).
    /// - `OP_INDEXER_BALANCER_SERVER_KEYS` (required): comma-separated keys servers register
    ///   with. Never logged.
    /// - `OP_INDEXER_STREAM_API_KEYS`: comma-separated user keys, the servers' own: a client
    ///   reads from the servers with the key it asked the balancer with (default: none, no
    ///   check). Never logged.
    /// - `OP_INDEXER_R2_ACCOUNT_ID`, `OP_INDEXER_R2_ACCESS_KEY_ID`,
    ///   `OP_INDEXER_R2_SECRET_ACCESS_KEY` (required), `OP_INDEXER_R2_BUCKET`,
    ///   `OP_INDEXER_R2_PREFIX`, `OP_INDEXER_R2_ENDPOINT`: the chain's bucket, as for `server`;
    ///   a read-only key is enough.
    /// - `OP_INDEXER_R2_PRESIGN_ACCESS_KEY_ID`, `OP_INDEXER_R2_PRESIGN_SECRET_ACCESS_KEY`: a
    ///   read-only R2 key the balancer signs raw chunk URLs with (`docs/serving.md`, raw chunk
    ///   download). Both or neither; without them raw plans are refused. Never logged; a URL
    ///   carries the key's id, never its secret.
    ///
    /// # Errors
    ///
    /// Returns an error if a required variable is missing or one is invalid, or an argument
    /// is unknown.
    pub(crate) fn from_env_and_args() -> eyre::Result<Self> {
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == env_file::FLAG {
                args.next();
            } else if !arg
                .strip_prefix(env_file::FLAG)
                .is_some_and(|rest| rest.starts_with('='))
            {
                return Err(eyre!(
                    "unknown argument {arg}; the only argument is --env-file"
                ));
            }
        }
        let chain_id = var("OP_INDEXER_CHAIN_ID")
            .map(|id| {
                id.parse::<u64>()
                    .wrap_err_with(|| format!("OP_INDEXER_CHAIN_ID is invalid: {id}"))
            })
            .transpose()?
            .unwrap_or(OP_MAINNET.chain_id);
        let chain = ChainSpec::by_chain_id(chain_id)
            .ok_or_else(|| eyre!("unsupported chain id {chain_id}"))?;
        let listen_addr = var("OP_INDEXER_BALANCER_LISTEN_ADDR")
            .map(|addr| {
                addr.parse()
                    .wrap_err_with(|| format!("OP_INDEXER_BALANCER_LISTEN_ADDR is invalid: {addr}"))
            })
            .transpose()?
            .unwrap_or(DEFAULT_LISTEN_ADDR);
        let server_keys = list("OP_INDEXER_BALANCER_SERVER_KEYS");
        if server_keys.is_empty() {
            return Err(eyre!("OP_INDEXER_BALANCER_SERVER_KEYS is required"));
        }
        let r2 = R2Config::from_env(chain)?;
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
                    "set both OP_INDEXER_R2_PRESIGN_ACCESS_KEY_ID and \
                     OP_INDEXER_R2_PRESIGN_SECRET_ACCESS_KEY, or neither"
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
