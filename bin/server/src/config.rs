//! The server's own configuration, next to the node's (`op_indexer_node::Config`): R2, the
//! export mode and the registration with a balancer.

use std::env;
use std::path::PathBuf;

use eyre::eyre;
use op_indexer_balancer::register::{Registration, is_valid_address};
use op_indexer_chainspec::ChainSpec;
use op_indexer_chunks::{R2Config, ReadOptions};
use op_indexer_node::sizing;
use op_indexer_runtime::config;
use op_indexer_runtime::machine::Machine;
use op_indexer_runtime::setting as var;

/// The exporter's name in the manifest when the server has none: no `server.id`
/// and no host name.
const DEFAULT_EXPORTER_ID: &str = "server";
/// The exporter's name before it followed the server's, read for one more release.
const EXPORT_ID_VAR: &str = "OP_INDEXER_EXPORT_ID";

/// What the server needs beyond the node's configuration.
#[derive(Debug)]
pub(crate) struct ServerConfig {
    /// Where the chunks are.
    pub(crate) chunks: Chunks,
    pub(crate) read: ReadOptions,
    /// Bytes consumers' reads of sealed history may hold in all.
    pub(crate) read_budget: u64,
    /// With `--export` or `server.export=true`: the exporter's name in the manifest.
    pub(crate) export: Option<String>,
    /// With `server.balancer_url`: how this server registers with the balancer. Its
    /// `address` is filled in once the node runs ([`crate::address::resolve`]).
    pub(crate) balancer: Option<Registration>,
    /// `server.address`; unset, the registered address is derived.
    pub(crate) address: Option<String>,
}

/// Where the sealed chunks are read from.
#[derive(Debug)]
pub(crate) enum Chunks {
    /// The bucket. Only the exporter's key may write.
    R2(R2Config),
    /// A local directory, as `import verify` writes one for tests and the bench, with the
    /// key prefix inside it.
    Local { dir: PathBuf, prefix: String },
}

impl ServerConfig {
    /// Reads the configuration:
    ///
    /// - Shared TOML configuration and legacy environment files are loaded before this.
    /// - `server.export`: `true` to also run the exporter (default `false`); `--export` on
    ///   the command line does the same. Exactly one server per deployment, the one with the
    ///   bucket's write key.
    /// - `r2.account_id`, `r2.access_key_id`,
    ///   `r2.secret_access_key` (required): the R2 account and the key to the
    ///   bucket, read-only except on the exporter. The secret is never logged.
    /// - `r2.bucket`: the chain's bucket (default `<chain>-snapshot`:
    ///   `op-snapshot`, `unichain-snapshot`, `base-snapshot`).
    /// - `r2.prefix`: the key prefix of the chain's objects in the bucket (default
    ///   `archive`).
    /// - `r2.endpoint`: the S3 endpoint (default
    ///   `https://<account id>.r2.cloudflarestorage.com`).
    /// - `r2.public_url`: the bucket's public custom domain behind Cloudflare's
    ///   cache; sealed chunks are read from it, the S3 API taking over on errors
    ///   (`docs/serving.md` 6.9). Default: none, every read through the S3 API.
    /// - `server.read_budget_mb`: memory, in MiB, that streams and Flight reads of
    ///   sealed history may hold in all, whatever the number of readers: decoded blocks read
    ///   ahead and chunk streams open (default sized from the machine:
    ///   [`sizing::server_read_budget`]). Past it readers wait.
    /// - `server.chunks_dir`: read the chunks from this local directory instead of R2,
    ///   under `r2.prefix` (for local runs and the bench); the R2 variables are
    ///   then not needed.
    /// - `server.id`: this server's name, unique in the deployment, in the balancer
    ///   and, on the exporter, the manifest (default: the host name). `server.export_ID`,
    ///   the exporter's own name before, is still read for this release, with a warning.
    /// - `server.balancer_url`: the balancer's gRPC URL (e.g.
    ///   `http://balancer.internal:50060`); set, the server registers there and reports its
    ///   health, load and heads every few seconds. Unset (the default), it runs standalone.
    ///   With it:
    ///   - `server.balancer_server_key` (required): the key servers register with; never
    ///     logged;
    ///   - `server.address`: the public `host:port` clients reach this server's
    ///     stream and Flight at (its `server.stream.listen_addr`, as seen from outside).
    ///     Default: derived ([`crate::address::resolve`]).
    ///
    /// The API keys of the gRPC and Flight services are the node's `server.stream.api_keys`.
    ///
    /// # Errors
    ///
    /// Returns an error if a required variable is missing or one is invalid, or an argument
    /// is unknown.
    pub(crate) fn from_config_and_args(chain: &ChainSpec) -> eyre::Result<Self> {
        let mut export = var("OP_INDEXER_EXPORT")
            .map(|value| value.parse())
            .transpose()
            .map_err(|_err| eyre!("server.export must be true or false"))?
            .unwrap_or(false);
        for arg in config::other_args(env::args_os().skip(1)) {
            if arg == "--export" {
                export = true;
            } else {
                return Err(eyre!(
                    "unknown server argument; use --export, --config, --chain or --check-config"
                ));
            }
        }
        let prefix = var("OP_INDEXER_R2_PREFIX").unwrap_or_else(|| "archive".to_owned());
        let chunks = match var("OP_INDEXER_CHUNKS_DIR") {
            Some(dir) => Chunks::Local {
                dir: PathBuf::from(dir),
                prefix,
            },
            None => Chunks::R2(R2Config::from_lookup(chain, var)?),
        };
        let id = var("OP_INDEXER_SERVER_ID").or_else(|| Machine::get().hostname.clone());
        let export_id = var(EXPORT_ID_VAR);
        let balancer = var("OP_INDEXER_BALANCER_URL")
            .map(|url| {
                Ok::<_, eyre::Report>(Registration {
                    balancer: url,
                    key: required("OP_INDEXER_BALANCER_SERVER_KEY")?,
                    id: id.clone().ok_or_else(|| {
                        eyre!("server.id is required: the host name cannot be read")
                    })?,
                    chain_id: chain.chain_id,
                    address: String::new(),
                })
            })
            .transpose()?;
        Ok(Self {
            balancer,
            address: server_address()?,
            chunks,
            // How wide a chunk stream reads is the read budget's to decide (the server's
            // `feed`): the store's own options only set what every GET does.
            read: ReadOptions::default(),
            read_budget: var("OP_INDEXER_SERVER_READ_BUDGET_MB")
                .map(|mib| mib.parse::<u64>())
                .transpose()
                .map_err(|_err| eyre!("server.read_budget_mb must be a number of MiB"))?
                .map_or_else(
                    || sizing::server_read_budget(Machine::get()),
                    |mib| mib.saturating_mul(1 << 20),
                ),
            export: export.then(|| {
                export_id
                    .or(id)
                    .unwrap_or_else(|| DEFAULT_EXPORTER_ID.to_owned())
            }),
        })
    }
}

fn required(name: &str) -> eyre::Result<String> {
    var(name).ok_or_else(|| eyre!("{} is required", config::setting_name(name)))
}

/// `server.address`, when set, checked as the balancer checks it (exactly
/// `host:port`, no scheme): a wrong one stops startup instead of being refused by the
/// balancer at every retry.
fn server_address() -> eyre::Result<Option<String>> {
    let Some(address) = var("OP_INDEXER_SERVER_ADDRESS") else {
        return Ok(None);
    };
    eyre::ensure!(
        is_valid_address(&address),
        "server.address must be host:port with no scheme, e.g. server1.example:50051"
    );
    Ok(Some(address))
}
