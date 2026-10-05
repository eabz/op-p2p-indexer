//! Immutable TOML settings shared by the binaries. CLI chain selection overrides the file.

mod migration;
mod schema;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use eyre::{WrapErr, bail, ensure, eyre};

use crate::args;
use schema::FIELDS;

static SETTINGS: OnceLock<Settings> = OnceLock::new();

struct Settings {
    role: String,
    values: BTreeMap<String, String>,
    chain_override: Option<String>,
    path: Option<PathBuf>,
}

/// Loads configuration before startup, installing an immutable role-specific overlay.
/// Explicit `--config` wins over discovery (`./config.toml`, then `~/.op-indexer/<chain>/config.toml`).
/// Normal runs require a TOML file; help and version commands do not.
///
/// # Errors
/// Returns an error for missing explicit files, invalid TOML, unsupported chains, conflicting
/// file flags, or repeated initialization. Parse errors never include configuration contents.
pub fn initialize(binary: &str) -> eyre::Result<Option<PathBuf>> {
    let role = role(binary)?;
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let explicit = argument("--config", &args)?.map(PathBuf::from);
    ensure!(
        !args.iter().any(|arg| arg == "--env-file"
            || arg
                .to_str()
                .is_some_and(|arg| arg.starts_with("--env-file="))),
        "--env-file is no longer supported; convert with --migrate-env INPUT --config OUTPUT"
    );
    let chain_argument = argument("--chain", &args)?;
    // Numeric importer chain flags select the chain without changing file discovery.
    let selects_home = chain_argument
        .as_deref()
        .is_some_and(|chain| !matches!(chain.to_str(), Some("10" | "130" | "8453")));
    let chain_override = chain_argument
        .map(|chain| {
            chain
                .to_str()
                .ok_or_else(|| eyre!("--chain must be Unicode"))
                .and_then(chain_id)
        })
        .transpose()?;
    let discovery_chain = chain_override.clone().unwrap_or_else(|| "10".to_owned());
    let path = discover(explicit, &discovery_chain, selects_home)?;
    ensure!(
        path.is_some() || args.iter().any(|arg| arg == "--help" || arg == "-h"),
        "no TOML configuration found; use --config PATH or --chain NAME"
    );
    let (values, loaded) = if let Some(path) = &path {
        let mut table = read(path, &mut Vec::new())?;
        if matches!(role, "indexer" | "server") {
            table
                .entry(role.to_owned())
                .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        }
        if let Some(common) = table
            .remove("node")
            .and_then(|value| value.as_table().cloned())
        {
            for name in ["indexer", "server"] {
                if let Some(settings) = table.get_mut(name).and_then(toml::Value::as_table_mut) {
                    let mut combined = common.clone();
                    merge(&mut combined, std::mem::take(settings));
                    *settings = combined;
                }
            }
        }
        let base = path
            .parent()
            .ok_or_else(|| eyre!("configuration has no parent directory"))?;
        let mut fields = BTreeMap::new();
        schema::flatten(&table, "", base, &mut fields)?;
        let mut values: BTreeMap<String, String> = FIELDS
            .iter()
            .filter(|field| {
                field.path.starts_with(&format!("{role}."))
                    || field.path.starts_with("r2.")
                    || field.path == "log_filter"
            })
            .filter_map(|field| {
                fields
                    .get(field.path)
                    .map(|value| (field.key.to_owned(), value.clone()))
            })
            .collect();
        let chain = table
            .get("chain")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| eyre!("configuration needs a string chain: op, unichain or base"))?;
        values.insert("OP_INDEXER_CHAIN_ID".to_owned(), chain_id(chain)?);
        let data_key = if role == "importer" {
            "OP_INDEXER_IMPORT_STATE_DIR"
        } else {
            "OP_INDEXER_DATA_DIR"
        };
        values
            .entry(data_key.to_owned())
            .or_insert_with(|| base.join("data").join(role).to_string_lossy().into_owned());
        (values, Some(path.clone()))
    } else {
        (BTreeMap::new(), None)
    };
    SETTINGS
        .set(Settings {
            role: role.to_owned(),
            values,
            chain_override,
            path,
        })
        .map_err(|_err| eyre!("configuration was already initialized"))?;
    Ok(loaded)
}

/// Whether startup should validate configuration and exit without opening stores or networks.
pub fn check_requested() -> bool {
    std::env::args_os()
        .skip(1)
        .any(|arg| arg == "--check-config")
}

/// Handles `--migrate-env INPUT --config OUTPUT`, returning true when it wrote the new file.
///
/// # Errors
/// Returns an error for invalid legacy values, missing arguments, unknown settings or an
/// existing output file. The legacy source and process environment are unchanged.
pub fn command(binary: &str) -> eyre::Result<bool> {
    let role = role(binary)?;
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let Some(input) = argument("--migrate-env", &args)? else {
        return Ok(false);
    };
    let output =
        argument("--config", &args)?.ok_or_else(|| eyre!("--migrate-env needs --config OUTPUT"))?;
    let chain = argument("--chain", &args)?;
    migration::run(
        role,
        Path::new(&input),
        Path::new(&output),
        chain.as_deref(),
    )?;
    crate::say(format_args!(
        "wrote TOML configuration to {}",
        Path::new(&output).display()
    ));
    Ok(true)
}

/// Arguments for the binary's parser, excluding shared configuration and chain-selection flags.
pub fn other_args(args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    let args = args::without_flag("--config", args);
    args::without_flag("--chain", args)
        .into_iter()
        .filter(|arg| arg != "--check-config")
        .collect()
}

/// Returns the selected TOML path, if any, for service commands forwarding startup arguments.
pub fn path() -> Option<&'static Path> {
    SETTINGS.get().and_then(|settings| settings.path.as_deref())
}

pub(crate) fn value(name: &str) -> Option<String> {
    let settings = SETTINGS.get();
    if name == "OP_INDEXER_CHAIN_ID"
        && let Some(chain) = settings.and_then(|s| s.chain_override.as_ref())
    {
        return Some(chain.clone());
    }
    settings.and_then(|s| s.values.get(name)).cloned()
}

/// Returns the public TOML field name for an internal setting key, without any value.
pub fn setting_name(key: &str) -> &str {
    if key == "OP_INDEXER_CHAIN_ID" {
        return "chain";
    }
    let role = SETTINGS.get().map(|settings| settings.role.as_str());
    FIELDS
        .iter()
        .find(|field| {
            field.key == key
                && (role.is_some_and(|role| field.path.starts_with(&format!("{role}.")))
                    || field.path.starts_with("r2.")
                    || field.path == "log_filter")
        })
        .map_or(key, |field| field.path)
}

fn role(binary: &str) -> eyre::Result<&str> {
    match binary {
        "indexer" | "server" | "balancer" | "bench" => Ok(binary),
        "import" | "importer" => Ok("importer"),
        _ => bail!("unknown binary role"),
    }
}

fn chain_id(chain: &str) -> eyre::Result<String> {
    match chain {
        "op" | "10" => Ok("10".to_owned()),
        "unichain" | "130" => Ok("130".to_owned()),
        "base" | "8453" => Ok("8453".to_owned()),
        _ => bail!("chain must be op, unichain, base, 10, 130 or 8453"),
    }
}

fn chain_name(chain: &str) -> eyre::Result<&'static str> {
    match chain {
        "op" | "10" => Ok("op"),
        "unichain" | "130" => Ok("unichain"),
        "base" | "8453" => Ok("base"),
        _ => bail!("unsupported chain selection"),
    }
}

fn discover(
    explicit: Option<PathBuf>,
    chain: &str,
    selected: bool,
) -> eyre::Result<Option<PathBuf>> {
    if let Some(path) = explicit {
        return Ok(Some(
            path.canonicalize()
                .wrap_err("cannot open explicit configuration file")?,
        ));
    }
    let cwd = PathBuf::from("config.toml");
    if !selected && cwd.try_exists()? {
        return Ok(Some(cwd.canonicalize()?));
    }
    let chain = chain_name(chain)?;
    let home = std::env::var_os("HOME").map(PathBuf::from);
    if let Some(path) = home.map(|home| home.join(".op-indexer").join(chain).join("config.toml"))
        && path.try_exists()?
    {
        return Ok(Some(path.canonicalize()?));
    }
    ensure!(
        !selected,
        "--chain requires ~/.op-indexer/<chain>/config.toml; create it or provide --config PATH"
    );
    Ok(None)
}

/// Loads a bounded inheritance chain. Each file's paths are resolved before merging so an
/// inherited relative path keeps its original meaning. Child values replace parent values;
/// tables merge recursively and arrays replace as a whole.
fn read(path: &Path, stack: &mut Vec<PathBuf>) -> eyre::Result<toml::Table> {
    let path = path
        .canonicalize()
        .wrap_err("cannot open TOML configuration")?;
    ensure!(
        stack.len() < 8 && !stack.contains(&path),
        "configuration inheritance cycle or depth exceeds eight files"
    );
    stack.push(path.clone());
    let text = std::fs::read_to_string(&path).wrap_err("cannot read TOML configuration")?;
    let mut table = text
        .parse::<toml::Table>()
        .map_err(|_err| eyre!("invalid TOML configuration (contents redacted)"))?;
    let base = path
        .parent()
        .ok_or_else(|| eyre!("configuration has no parent"))?;
    let parent = table
        .remove("extends")
        .map(|value| {
            let value = value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| eyre!("extends must be a nonempty file path"))?;
            Ok::<_, eyre::Report>(base.join(value))
        })
        .transpose()?;
    let common = table.remove("node");
    if let Some(common) = &common {
        let common = common
            .as_table()
            .ok_or_else(|| eyre!("node must be a table"))?;
        validate_common(common)?;
        schema::flatten(common, "indexer", base, &mut BTreeMap::new())?;
    }
    // Validate each layer, so a child cannot hide a misspelled field in a parent.
    let mut fields = BTreeMap::new();
    schema::flatten(&table, "", base, &mut fields)?;
    if let Some(chain) = table.get("chain") {
        chain_id(
            chain
                .as_str()
                .ok_or_else(|| eyre!("chain must be a string"))?,
        )?;
    }
    for field in FIELDS
        .iter()
        .filter(|field| matches!(field.kind, schema::Kind::Path))
    {
        if let Some(value) = fields.get(field.path) {
            set_path(&mut table, field.path, value);
        }
    }
    if let Some(common) = common {
        table.insert("node".to_owned(), common);
    }
    let result = if let Some(parent) = parent {
        let mut inherited = read(&parent, stack)?;
        merge(&mut inherited, table);
        inherited
    } else {
        table
    };
    stack.pop();
    Ok(result)
}

fn validate_common(table: &toml::Table) -> eyre::Result<()> {
    for (key, value) in table {
        ensure!(
            !matches!(
                key.as_str(),
                "data_dir" | "log_file" | "listen_addr" | "beacon_listen_addr" | "advertised_addr"
            ),
            "node defaults cannot share state paths or network bindings; put them in each role"
        );
        if let Some(child) = value.as_table() {
            validate_common(child)?;
        }
    }
    Ok(())
}

fn merge(parent: &mut toml::Table, child: toml::Table) {
    for (key, value) in child {
        if let Some(existing) = parent.get_mut(&key).and_then(toml::Value::as_table_mut)
            && let Some(table) = value.as_table()
        {
            merge(existing, table.clone());
        } else {
            parent.insert(key, value);
        }
    }
}

fn set_path(table: &mut toml::Table, path: &str, value: &str) {
    if let Some((first, rest)) = path.split_once('.') {
        if let Some(child) = table.get_mut(first).and_then(toml::Value::as_table_mut) {
            set_path(child, rest, value);
        }
    } else {
        table.insert(path.to_owned(), toml::Value::String(value.to_owned()));
    }
}

fn argument(flag: &str, args: &[OsString]) -> eyre::Result<Option<OsString>> {
    let mut result = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let value = if arg == flag {
            Some(
                args.next()
                    .filter(|arg| !arg.to_string_lossy().starts_with("--"))
                    .ok_or_else(|| eyre!("{flag} needs a value"))?
                    .clone(),
            )
        } else {
            arg.to_str()
                .and_then(|arg| arg.strip_prefix(flag)?.strip_prefix('='))
                .map(OsString::from)
        };
        if let Some(value) = value {
            ensure!(!value.is_empty(), "{flag} needs a value");
            ensure!(result.is_none(), "{flag} was provided more than once");
            result = Some(value);
        }
    }
    Ok(result)
}
