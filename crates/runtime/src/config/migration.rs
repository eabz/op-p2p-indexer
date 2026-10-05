//! Converts legacy dotenv settings to typed TOML without loading them into the environment.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::Path;

use eyre::{WrapErr, ensure, eyre};
use toml::Value;

use super::schema::{FIELDS, migrate_value};

pub(super) fn run(
    role: &str,
    input: &Path,
    output: &Path,
    chain: Option<&OsStr>,
) -> eyre::Result<()> {
    let text = std::fs::read_to_string(input).wrap_err("cannot read migration source")?;
    let mut values = BTreeMap::new();
    for entry in dotenvy::from_read_iter(text.as_bytes()) {
        let (name, value) =
            entry.map_err(|_err| eyre!("invalid legacy env file (contents redacted)"))?;
        values.entry(name).or_insert(value);
    }
    values.retain(|_, value| !value.is_empty());
    if let Some(old) = values.remove("ENVIO_API_TOKEN") {
        values
            .entry("OP_INDEXER_IMPORT_API_TOKEN".to_owned())
            .or_insert(old);
    }
    for key in values.keys() {
        ensure!(
            key == "OP_INDEXER_CHAIN_ID" || FIELDS.iter().any(|field| field.key == key),
            "legacy file contains an unsupported variable; migration refused to avoid dropping settings"
        );
    }
    let chain = chain
        .map(|value| value.to_str().ok_or_else(|| eyre!("chain must be Unicode")))
        .transpose()?
        .or_else(|| values.get("OP_INDEXER_CHAIN_ID").map(String::as_str))
        .unwrap_or("op");
    let mut table = toml::Table::new();
    let chain = super::chain_name(chain)?;
    table.insert("chain".to_owned(), Value::String(chain.to_owned()));
    let base = std::env::current_dir().wrap_err("cannot resolve legacy relative paths")?;
    let selected: Vec<_> = FIELDS
        .iter()
        .filter(|field| {
            field.path.starts_with(&format!("{role}."))
                || field.path.starts_with("r2.")
                || field.path == "log_filter"
        })
        .collect();
    for key in values.keys() {
        ensure!(
            key == "OP_INDEXER_CHAIN_ID" || selected.iter().any(|field| field.key == key),
            "legacy file contains settings for a different role; migrate a role-specific env file to avoid dropping settings"
        );
    }
    for field in selected {
        if let Some(value) = values.get(field.key) {
            insert(&mut table, field.path, migrate_value(field, value, &base)?);
        }
    }
    let (data_key, data_field, default_dir) = if role == "importer" {
        (
            "OP_INDEXER_IMPORT_STATE_DIR",
            "state_dir",
            "import-state".to_owned(),
        )
    } else {
        ("OP_INDEXER_DATA_DIR", "data_dir", format!("data-{chain}"))
    };
    if role != "balancer" && !values.contains_key(data_key) {
        let legacy_data = if role != "importer"
            && !base.join(&default_dir).exists()
            && (base.join("data/archive").exists() || base.join("data/node").exists())
        {
            base.join("data")
        } else {
            base.join(default_dir)
        };
        insert(
            &mut table,
            &format!("{role}.{data_field}"),
            Value::String(legacy_data.to_string_lossy().into_owned()),
        );
    }
    let rendered =
        toml::to_string_pretty(&table).wrap_err("cannot encode migrated configuration")?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(output)
        .wrap_err("cannot create migration output (must not exist)")?;
    file.write_all(rendered.as_bytes())
        .wrap_err("cannot write migrated configuration")?;
    file.sync_all()
        .wrap_err("cannot flush migrated configuration")?;
    Ok(())
}

fn insert(table: &mut toml::Table, path: &str, value: Value) {
    if let Some((first, rest)) = path.split_once('.') {
        let child = table
            .entry(first.to_owned())
            .or_insert_with(|| Value::Table(toml::Table::new()));
        if let Some(child) = child.as_table_mut() {
            insert(child, rest, value);
        }
    } else {
        table.insert(path.to_owned(), value);
    }
}
