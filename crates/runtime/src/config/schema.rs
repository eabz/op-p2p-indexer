//! The accepted TOML fields, their types and legacy names. No field values appear in errors.

use eyre::{bail, eyre};
use std::collections::BTreeMap;
use std::path::Path;
use toml::Value;

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Text,
    Path,
    List,
    Bool,
    Number,
}

pub(super) struct Field {
    pub(super) path: &'static str,
    pub(super) env: &'static str,
    pub(super) kind: Kind,
}

pub(super) const FIELDS: &[Field] = &[
    Field {
        path: "r2.public_url",
        env: "OP_INDEXER_R2_PUBLIC_URL",
        kind: Kind::Text,
    },
    Field {
        path: "log_filter",
        env: "RUST_LOG",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.data_dir",
        env: "OP_INDEXER_DATA_DIR",
        kind: Kind::Path,
    },
    Field {
        path: "indexer.profile",
        env: "OP_INDEXER_PROFILE",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.unsafe_max_bytes",
        env: "OP_INDEXER_UNSAFE_MAX_BYTES",
        kind: Kind::Number,
    },
    Field {
        path: "indexer.log_file",
        env: "OP_INDEXER_LOG_FILE",
        kind: Kind::Path,
    },
    Field {
        path: "indexer.stream.listen_addr",
        env: "OP_INDEXER_STREAM_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.stream.api_keys",
        env: "OP_INDEXER_STREAM_API_KEYS",
        kind: Kind::List,
    },
    Field {
        path: "indexer.stream.max_subscriptions",
        env: "OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS",
        kind: Kind::Number,
    },
    Field {
        path: "indexer.stream.max_flights",
        env: "OP_INDEXER_STREAM_MAX_FLIGHTS",
        kind: Kind::Number,
    },
    Field {
        path: "indexer.stream.max_builds",
        env: "OP_INDEXER_STREAM_MAX_BUILDS",
        kind: Kind::Number,
    },
    Field {
        path: "indexer.stream.flight_queue_ms",
        env: "OP_INDEXER_STREAM_FLIGHT_QUEUE_MS",
        kind: Kind::Number,
    },
    Field {
        path: "indexer.p2p.listen_addr",
        env: "OP_INDEXER_P2P_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.p2p.advertised_addr",
        env: "OP_INDEXER_P2P_ADVERTISED_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.p2p.bootnodes",
        env: "OP_INDEXER_P2P_BOOTNODES",
        kind: Kind::List,
    },
    Field {
        path: "indexer.p2p.max_peers",
        env: "OP_INDEXER_P2P_MAX_PEERS",
        kind: Kind::Number,
    },
    Field {
        path: "indexer.el.enabled",
        env: "OP_INDEXER_EL_ENABLED",
        kind: Kind::Bool,
    },
    Field {
        path: "indexer.el.sync",
        env: "OP_INDEXER_EL_SYNC",
        kind: Kind::Bool,
    },
    Field {
        path: "indexer.el.listen_addr",
        env: "OP_INDEXER_EL_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.el.advertised_addr",
        env: "OP_INDEXER_EL_ADVERTISED_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.el.bootnodes",
        env: "OP_INDEXER_EL_BOOTNODES",
        kind: Kind::List,
    },
    Field {
        path: "indexer.el.trusted_peers",
        env: "OP_INDEXER_EL_TRUSTED_PEERS",
        kind: Kind::List,
    },
    Field {
        path: "indexer.el.max_sessions",
        env: "OP_INDEXER_EL_MAX_SESSIONS",
        kind: Kind::Number,
    },
    Field {
        path: "indexer.l1.enabled",
        env: "OP_INDEXER_L1_ENABLED",
        kind: Kind::Bool,
    },
    Field {
        path: "indexer.l1.checkpoint",
        env: "OP_INDEXER_L1_CHECKPOINT",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.l1.listen_addr",
        env: "OP_INDEXER_L1_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.l1.advertised_addr",
        env: "OP_INDEXER_L1_ADVERTISED_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "indexer.l1.beacon_listen_addr",
        env: "OP_INDEXER_L1_BEACON_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.data_dir",
        env: "OP_INDEXER_DATA_DIR",
        kind: Kind::Path,
    },
    Field {
        path: "server.profile",
        env: "OP_INDEXER_PROFILE",
        kind: Kind::Text,
    },
    Field {
        path: "server.unsafe_max_bytes",
        env: "OP_INDEXER_UNSAFE_MAX_BYTES",
        kind: Kind::Number,
    },
    Field {
        path: "server.log_file",
        env: "OP_INDEXER_LOG_FILE",
        kind: Kind::Path,
    },
    Field {
        path: "server.stream.listen_addr",
        env: "OP_INDEXER_STREAM_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.stream.api_keys",
        env: "OP_INDEXER_STREAM_API_KEYS",
        kind: Kind::List,
    },
    Field {
        path: "server.stream.max_subscriptions",
        env: "OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS",
        kind: Kind::Number,
    },
    Field {
        path: "server.stream.max_flights",
        env: "OP_INDEXER_STREAM_MAX_FLIGHTS",
        kind: Kind::Number,
    },
    Field {
        path: "server.stream.max_builds",
        env: "OP_INDEXER_STREAM_MAX_BUILDS",
        kind: Kind::Number,
    },
    Field {
        path: "server.stream.flight_queue_ms",
        env: "OP_INDEXER_STREAM_FLIGHT_QUEUE_MS",
        kind: Kind::Number,
    },
    Field {
        path: "server.p2p.listen_addr",
        env: "OP_INDEXER_P2P_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.p2p.advertised_addr",
        env: "OP_INDEXER_P2P_ADVERTISED_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.p2p.bootnodes",
        env: "OP_INDEXER_P2P_BOOTNODES",
        kind: Kind::List,
    },
    Field {
        path: "server.p2p.max_peers",
        env: "OP_INDEXER_P2P_MAX_PEERS",
        kind: Kind::Number,
    },
    Field {
        path: "server.el.enabled",
        env: "OP_INDEXER_EL_ENABLED",
        kind: Kind::Bool,
    },
    Field {
        path: "server.el.sync",
        env: "OP_INDEXER_EL_SYNC",
        kind: Kind::Bool,
    },
    Field {
        path: "server.el.listen_addr",
        env: "OP_INDEXER_EL_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.el.advertised_addr",
        env: "OP_INDEXER_EL_ADVERTISED_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.el.bootnodes",
        env: "OP_INDEXER_EL_BOOTNODES",
        kind: Kind::List,
    },
    Field {
        path: "server.el.trusted_peers",
        env: "OP_INDEXER_EL_TRUSTED_PEERS",
        kind: Kind::List,
    },
    Field {
        path: "server.el.max_sessions",
        env: "OP_INDEXER_EL_MAX_SESSIONS",
        kind: Kind::Number,
    },
    Field {
        path: "server.l1.enabled",
        env: "OP_INDEXER_L1_ENABLED",
        kind: Kind::Bool,
    },
    Field {
        path: "server.l1.checkpoint",
        env: "OP_INDEXER_L1_CHECKPOINT",
        kind: Kind::Text,
    },
    Field {
        path: "server.l1.listen_addr",
        env: "OP_INDEXER_L1_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.l1.advertised_addr",
        env: "OP_INDEXER_L1_ADVERTISED_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.l1.beacon_listen_addr",
        env: "OP_INDEXER_L1_BEACON_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "server.export",
        env: "OP_INDEXER_EXPORT",
        kind: Kind::Bool,
    },
    Field {
        path: "server.chunks_dir",
        env: "OP_INDEXER_CHUNKS_DIR",
        kind: Kind::Path,
    },
    Field {
        path: "server.id",
        env: "OP_INDEXER_SERVER_ID",
        kind: Kind::Text,
    },
    Field {
        path: "server.address",
        env: "OP_INDEXER_SERVER_ADDRESS",
        kind: Kind::Text,
    },
    Field {
        path: "server.balancer_url",
        env: "OP_INDEXER_BALANCER_URL",
        kind: Kind::Text,
    },
    Field {
        path: "server.balancer_server_key",
        env: "OP_INDEXER_BALANCER_SERVER_KEY",
        kind: Kind::Text,
    },
    Field {
        path: "server.read_budget_mb",
        env: "OP_INDEXER_SERVER_READ_BUDGET_MB",
        kind: Kind::Number,
    },
    Field {
        path: "server.export_id",
        env: "OP_INDEXER_EXPORT_ID",
        kind: Kind::Text,
    },
    Field {
        path: "balancer.listen_addr",
        env: "OP_INDEXER_BALANCER_LISTEN_ADDR",
        kind: Kind::Text,
    },
    Field {
        path: "balancer.server_keys",
        env: "OP_INDEXER_BALANCER_SERVER_KEYS",
        kind: Kind::List,
    },
    Field {
        path: "balancer.api_keys",
        env: "OP_INDEXER_STREAM_API_KEYS",
        kind: Kind::List,
    },
    Field {
        path: "balancer.log_file",
        env: "OP_INDEXER_LOG_FILE",
        kind: Kind::Path,
    },
    Field {
        path: "importer.state_dir",
        env: "OP_INDEXER_IMPORT_STATE_DIR",
        kind: Kind::Path,
    },
    Field {
        path: "importer.api_token",
        env: "OP_INDEXER_IMPORT_API_TOKEN",
        kind: Kind::Text,
    },
    Field {
        path: "importer.rpc_endpoint",
        env: "OP_INDEXER_IMPORT_RPC_ENDPOINT",
        kind: Kind::Text,
    },
    Field {
        path: "importer.endpoint",
        env: "OP_INDEXER_IMPORT_ENDPOINT",
        kind: Kind::Text,
    },
    Field {
        path: "importer.l1_endpoint",
        env: "OP_INDEXER_IMPORT_L1_ENDPOINT",
        kind: Kind::Text,
    },
    Field {
        path: "importer.verify_threads",
        env: "OP_INDEXER_IMPORT_VERIFY_THREADS",
        kind: Kind::Number,
    },
    Field {
        path: "importer.verify_uploads",
        env: "OP_INDEXER_IMPORT_VERIFY_UPLOADS",
        kind: Kind::Number,
    },
    Field {
        path: "importer.first_block",
        env: "OP_INDEXER_IMPORT_FIRST_BLOCK",
        kind: Kind::Number,
    },
    Field {
        path: "importer.last_block",
        env: "OP_INDEXER_IMPORT_LAST_BLOCK",
        kind: Kind::Number,
    },
    Field {
        path: "importer.anchor_hash",
        env: "OP_INDEXER_IMPORT_ANCHOR_HASH",
        kind: Kind::Text,
    },
    Field {
        path: "importer.legacy_only",
        env: "OP_INDEXER_IMPORT_LEGACY_ONLY",
        kind: Kind::Bool,
    },
    Field {
        path: "importer.chunk_blocks",
        env: "OP_INDEXER_IMPORT_CHUNK_BLOCKS",
        kind: Kind::Number,
    },
    Field {
        path: "importer.fill_from",
        env: "OP_INDEXER_IMPORT_FILL_FROM",
        kind: Kind::Text,
    },
    Field {
        path: "importer.rpc_batch",
        env: "OP_INDEXER_IMPORT_RPC_BATCH",
        kind: Kind::Number,
    },
    Field {
        path: "importer.rpc_requests",
        env: "OP_INDEXER_IMPORT_RPC_REQUESTS",
        kind: Kind::Number,
    },
    Field {
        path: "importer.requests",
        env: "OP_INDEXER_IMPORT_REQUESTS",
        kind: Kind::Number,
    },
    Field {
        path: "importer.refetch_incomplete",
        env: "OP_INDEXER_IMPORT_REFETCH_INCOMPLETE",
        kind: Kind::Bool,
    },
    Field {
        path: "importer.refetch_requests",
        env: "OP_INDEXER_IMPORT_REFETCH_REQUESTS",
        kind: Kind::Number,
    },
    Field {
        path: "importer.balancer_url",
        env: "OP_INDEXER_BALANCER_URL",
        kind: Kind::Text,
    },
    Field {
        path: "importer.api_key",
        env: "OP_INDEXER_API_KEY",
        kind: Kind::Text,
    },
    Field {
        path: "r2.account_id",
        env: "OP_INDEXER_R2_ACCOUNT_ID",
        kind: Kind::Text,
    },
    Field {
        path: "r2.access_key_id",
        env: "OP_INDEXER_R2_ACCESS_KEY_ID",
        kind: Kind::Text,
    },
    Field {
        path: "r2.secret_access_key",
        env: "OP_INDEXER_R2_SECRET_ACCESS_KEY",
        kind: Kind::Text,
    },
    Field {
        path: "r2.bucket",
        env: "OP_INDEXER_R2_BUCKET",
        kind: Kind::Text,
    },
    Field {
        path: "r2.prefix",
        env: "OP_INDEXER_R2_PREFIX",
        kind: Kind::Text,
    },
    Field {
        path: "r2.endpoint",
        env: "OP_INDEXER_R2_ENDPOINT",
        kind: Kind::Text,
    },
    Field {
        path: "r2.presign_access_key_id",
        env: "OP_INDEXER_R2_PRESIGN_ACCESS_KEY_ID",
        kind: Kind::Text,
    },
    Field {
        path: "r2.presign_secret_access_key",
        env: "OP_INDEXER_R2_PRESIGN_SECRET_ACCESS_KEY",
        kind: Kind::Text,
    },
];

pub(super) fn flatten(
    table: &toml::Table,
    parent: &str,
    base: &Path,
    out: &mut BTreeMap<String, String>,
) -> eyre::Result<()> {
    for (name, value) in table {
        let path = if parent.is_empty() {
            name.clone()
        } else {
            format!("{parent}.{name}")
        };
        if path == "chain" {
            continue;
        }
        if let Some(field) = FIELDS.iter().find(|field| field.path == path) {
            out.insert(path, encode(field, value, base)?);
        } else if let Some(table) = value.as_table().filter(|_| {
            FIELDS
                .iter()
                .any(|field| field.path.starts_with(&format!("{path}.")))
        }) {
            flatten(table, &path, base, out)?;
        } else {
            // The unrecognized key itself may accidentally hold a secret; don't print it.
            bail!("configuration contains an unknown field or invalid section");
        }
    }
    Ok(())
}

fn encode(field: &Field, value: &Value, base: &Path) -> eyre::Result<String> {
    let invalid = || eyre!("configuration field {} has the wrong type", field.path);
    Ok(match field.kind {
        Kind::Text => value.as_str().ok_or_else(invalid)?.to_owned(),
        Kind::Path => {
            let path = Path::new(value.as_str().ok_or_else(invalid)?);
            if path.as_os_str().is_empty() {
                return Err(invalid());
            }
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                base.join(path)
            };
            path.to_string_lossy().into_owned()
        }
        Kind::Bool => value.as_bool().ok_or_else(invalid)?.to_string(),
        Kind::Number => value
            .as_integer()
            .filter(|v| *v >= 0)
            .ok_or_else(invalid)?
            .to_string(),
        Kind::List => {
            let items = value.as_array().ok_or_else(invalid)?;
            let strings = items
                .iter()
                .map(|item| {
                    item.as_str()
                        .filter(|s| !s.contains(','))
                        .ok_or_else(invalid)
                })
                .collect::<eyre::Result<Vec<_>>>()?;
            strings.join(",")
        }
    })
}

pub(super) fn migrate_value(field: &Field, value: &str, base: &Path) -> eyre::Result<Value> {
    let invalid = || eyre!("legacy variable {} has an invalid value", field.env);
    Ok(match field.kind {
        Kind::Text => Value::String(value.to_owned()),
        Kind::Path => Value::String(if Path::new(value).is_absolute() {
            value.to_owned()
        } else {
            base.join(value).to_string_lossy().into_owned()
        }),
        Kind::Bool => Value::Boolean(value.parse().map_err(|_err| invalid())?),
        Kind::Number => Value::Integer(
            value
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .ok_or_else(invalid)?,
        ),
        Kind::List => Value::Array(
            value
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(|v| Value::String(v.to_owned()))
                .collect(),
        ),
    })
}
