//! The server's own configuration, next to the node's (`op_indexer_node::Config`): R2 and the
//! export mode.

use std::env;

use eyre::eyre;
use op_indexer_chainspec::ChainSpec;
use op_indexer_chunks::{R2Config, ReadOptions};
use op_indexer_node::env_file;
/// The exporter's name in the manifest when none is configured.
const DEFAULT_EXPORTER_ID: &str = "server";

/// What the server needs beyond the node's configuration.
#[derive(Debug)]
pub(crate) struct ServerConfig {
    /// The bucket. Only the exporter's key may write.
    pub(crate) r2: R2Config,
    pub(crate) read: ReadOptions,
    /// With `--export` or `OP_INDEXER_EXPORT=true`: the exporter's name in the manifest.
    pub(crate) export: Option<String>,
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
    /// - `OP_INDEXER_EXPORT_ID`: the exporter's name in the manifest (default `server`).
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
        let r2 = R2Config {
            account_id: required("OP_INDEXER_R2_ACCOUNT_ID")?,
            bucket: var("OP_INDEXER_R2_BUCKET")
                .unwrap_or_else(|| format!("{}-snapshot", chain.name)),
            prefix: var("OP_INDEXER_R2_PREFIX").unwrap_or_else(|| "archive".to_owned()),
            access_key_id: required("OP_INDEXER_R2_ACCESS_KEY_ID")?,
            secret_access_key: required("OP_INDEXER_R2_SECRET_ACCESS_KEY")?,
            endpoint: var("OP_INDEXER_R2_ENDPOINT"),
        };
        Ok(Self {
            r2,
            read: ReadOptions::default(),
            export: export.then(|| {
                var("OP_INDEXER_EXPORT_ID").unwrap_or_else(|| DEFAULT_EXPORTER_ID.to_owned())
            }),
        })
    }
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn required(name: &str) -> eyre::Result<String> {
    var(name).ok_or_else(|| eyre!("{name} is required"))
}
