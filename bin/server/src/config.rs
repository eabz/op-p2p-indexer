//! The server's own configuration, next to the node's (`op_indexer_node::Config`): R2, the
//! export mode and the registration with a balancer.

use std::env;
use std::path::PathBuf;

use eyre::eyre;
use op_indexer_balancer::register::{Registration, is_valid_address};
use op_indexer_chainspec::ChainSpec;
use op_indexer_chunks::{R2Config, ReadOptions};
use op_indexer_runtime::env_file;
use op_indexer_runtime::env_var as var;
/// Default memory for consumers' reads of sealed history, in MiB.
const DEFAULT_READ_BUDGET_MB: u64 = 1024;
/// The exporter's name in the manifest when none is configured.
const DEFAULT_EXPORTER_ID: &str = "server";

/// What the server needs beyond the node's configuration.
#[derive(Debug)]
pub(crate) struct ServerConfig {
    /// Where the chunks are.
    pub(crate) chunks: Chunks,
    pub(crate) read: ReadOptions,
    /// Bytes consumers' reads of sealed history may hold in all.
    pub(crate) read_budget: u64,
    /// With `--export` or `OP_INDEXER_EXPORT=true`: the exporter's name in the manifest.
    pub(crate) export: Option<String>,
    /// With `OP_INDEXER_BALANCER_URL`: how this server registers with the balancer.
    pub(crate) balancer: Option<Registration>,
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
    /// - `--env-file <path>` (command line): read by the env-file loader before this.
    /// - `OP_INDEXER_EXPORT`: `true` to also run the exporter (default `false`); `--export` on
    ///   the command line does the same. Exactly one server per deployment, the one with the
    ///   bucket's write key.
    /// - `OP_INDEXER_R2_ACCOUNT_ID`, `OP_INDEXER_R2_ACCESS_KEY_ID`,
    ///   `OP_INDEXER_R2_SECRET_ACCESS_KEY` (required): the R2 account and the key to the
    ///   bucket, read-only except on the exporter. The secret is never logged.
    /// - `OP_INDEXER_R2_BUCKET`: the chain's bucket (default `<chain>-snapshot`:
    ///   `op-snapshot`, `unichain-snapshot`, `base-snapshot`).
    /// - `OP_INDEXER_R2_PREFIX`: the key prefix of the chain's objects in the bucket (default
    ///   `archive`).
    /// - `OP_INDEXER_R2_ENDPOINT`: the S3 endpoint (default
    ///   `https://<account id>.r2.cloudflarestorage.com`).
    /// - `OP_INDEXER_R2_PUBLIC_URL`: the bucket's public custom domain behind Cloudflare's
    ///   cache; sealed chunks are read from it, the S3 API taking over on errors
    ///   (`docs/serving.md` 6.9). Default: none, every read through the S3 API.
    /// - `OP_INDEXER_SERVER_READ_BUDGET_MB`: memory, in MiB, that streams and Flight reads of
    ///   sealed history may hold in all, whatever the number of readers: decoded blocks read
    ///   ahead and chunk streams open (default 1024). Past it readers wait.
    /// - `OP_INDEXER_CHUNKS_DIR`: read the chunks from this local directory instead of R2,
    ///   under `OP_INDEXER_R2_PREFIX` (for local runs and the bench); the R2 variables are
    ///   then not needed.
    /// - `OP_INDEXER_EXPORT_ID`: the exporter's name in the manifest (default `server`).
    /// - `OP_INDEXER_BALANCER_URL`: the balancer's gRPC URL (e.g.
    ///   `http://balancer.internal:50060`); set, the server registers there and reports its
    ///   health, load and heads every few seconds. Unset (the default), it runs standalone.
    ///   With it, these are required:
    ///   - `OP_INDEXER_BALANCER_SERVER_KEY`: the key servers register with; never logged;
    ///   - `OP_INDEXER_SERVER_ID`: this server's name, unique in the deployment;
    ///   - `OP_INDEXER_SERVER_ADDRESS`: the public `host:port` clients reach this server's
    ///     stream and Flight at (its `OP_INDEXER_STREAM_LISTEN_ADDR`, as seen from outside).
    ///
    /// The API keys of the gRPC and Flight services are the node's `OP_INDEXER_STREAM_API_KEYS`.
    ///
    /// # Errors
    ///
    /// Returns an error if a required variable is missing or one is invalid, or an argument
    /// is unknown.
    pub(crate) fn from_env_and_args(chain: &ChainSpec) -> eyre::Result<Self> {
        let mut export = var("OP_INDEXER_EXPORT")
            .map(|value| value.parse())
            .transpose()
            .map_err(|_err| eyre!("OP_INDEXER_EXPORT must be true or false"))?
            .unwrap_or(false);
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--export" => export = true,
                // Read before anything else, by the env-file loader.
                flag if flag == env_file::FLAG => {
                    args.next();
                }
                other
                    if other
                        .strip_prefix(env_file::FLAG)
                        .is_some_and(|rest| rest.starts_with('=')) => {}
                other => {
                    return Err(eyre!(
                        "unknown argument {other}; the arguments are --export and --env-file"
                    ));
                }
            }
        }
        let prefix = var("OP_INDEXER_R2_PREFIX").unwrap_or_else(|| "archive".to_owned());
        let chunks = match var("OP_INDEXER_CHUNKS_DIR") {
            Some(dir) => Chunks::Local {
                dir: PathBuf::from(dir),
                prefix,
            },
            None => Chunks::R2(R2Config::from_env(chain)?),
        };
        let balancer = var("OP_INDEXER_BALANCER_URL")
            .map(|balancer| {
                Ok::<_, eyre::Report>(Registration {
                    balancer,
                    key: required("OP_INDEXER_BALANCER_SERVER_KEY")?,
                    id: required("OP_INDEXER_SERVER_ID")?,
                    chain_id: chain.chain_id,
                    address: server_address()?,
                })
            })
            .transpose()?;
        Ok(Self {
            balancer,
            chunks,
            // How wide a chunk stream reads is the read budget's to decide (the server's
            // `feed`): the store's own options only set what every GET does.
            read: ReadOptions::default(),
            read_budget: var("OP_INDEXER_SERVER_READ_BUDGET_MB")
                .map(|mib| mib.parse::<u64>())
                .transpose()
                .map_err(|_err| eyre!("OP_INDEXER_SERVER_READ_BUDGET_MB must be a number of MiB"))?
                .unwrap_or(DEFAULT_READ_BUDGET_MB)
                .saturating_mul(1 << 20),
            export: export.then(|| {
                var("OP_INDEXER_EXPORT_ID").unwrap_or_else(|| DEFAULT_EXPORTER_ID.to_owned())
            }),
        })
    }
}

fn required(name: &str) -> eyre::Result<String> {
    var(name).ok_or_else(|| eyre!("{name} is required"))
}

/// `OP_INDEXER_SERVER_ADDRESS`, checked as the balancer checks it (exactly `host:port`, no
/// scheme): a wrong one stops startup instead of being refused by the balancer at every retry.
fn server_address() -> eyre::Result<String> {
    let address = required("OP_INDEXER_SERVER_ADDRESS")?;
    eyre::ensure!(
        is_valid_address(&address),
        "OP_INDEXER_SERVER_ADDRESS must be host:port with no scheme, e.g. server1.example:50051"
    );
    Ok(address)
}
