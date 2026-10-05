//! Immutable TOML settings shared by the binaries, with legacy environment compatibility.
//! Process variables override the selected role's settings; CLI chain selection wins over both.

mod migration;
mod schema;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use eyre::{WrapErr, bail, ensure, eyre};

use crate::env_file;
use schema::FIELDS;

static SETTINGS: OnceLock<Settings> = OnceLock::new();

struct Settings {
    values: BTreeMap<String, String>,
    chain_override: Option<String>,
    path: Option<PathBuf>,
}

/// Loads configuration before startup, installing an immutable role-specific overlay.
/// Explicit `--config` wins over discovery (`./config.toml`, then `~/indexer/<chain>/config.toml`).
/// Without TOML, the legacy `.env` loader remains available for one release.
///
/// # Errors
/// Returns an error for missing explicit files, invalid TOML, unsupported chains, conflicting
/// file flags, or repeated initialization. Parse errors never include configuration contents.
pub fn initialize(binary: &str) -> eyre::Result<Option<PathBuf>> {
    let role = role(binary)?;
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let explicit = argument("--config", &args)?.map(PathBuf::from);
    let legacy = argument("--env-file", &args)?.is_some()
        || std::env::var_os("OP_INDEXER_ENV_FILE").is_some();
    ensure!(
        !(explicit.is_some() && legacy),
        "--config and legacy env-file selection cannot be combined"
    );
    let chain_argument = argument("--chain", &args)?;
    // Numeric importer chain flags predate TOML and must keep working without a file.
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
    let discovery_chain = chain_override
        .clone()
        .or_else(|| process_var("OP_INDEXER_CHAIN_ID"))
        .unwrap_or_else(|| "10".to_owned());
    let path = if legacy {
        None
    } else {
        discover(explicit, &discovery_chain, selects_home)?
    };
    let (values, loaded) = if let Some(path) = &path {
        let table = read(path)?;
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
                    .map(|value| (field.env.to_owned(), value.clone()))
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
        let loaded = env_file::load(args)?;
        if loaded.is_some() {
            crate::say(format_args!(
                "legacy .env configuration is deprecated; use --migrate-env to create TOML"
            ));
        }
        (BTreeMap::new(), loaded)
    };
    SETTINGS
        .set(Settings {
            values,
            chain_override,
            path,
        })
        .map_err(|_| eyre!("configuration was already initialized"))?;
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
    let args = env_file::without_flag(env_file::FLAG, args);
    let args = env_file::without_flag("--config", args);
    env_file::without_flag("--chain", args)
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
    process_var(name).or_else(|| settings.and_then(|s| s.values.get(name)).cloned())
}

fn process_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn role(binary: &str) -> eyre::Result<&str> {
    match binary {
        "indexer" | "server" | "balancer" => Ok(binary),
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
    if let Some(path) = home.map(|home| home.join("indexer").join(chain).join("config.toml")) {
        if path.try_exists()? {
            return Ok(Some(path.canonicalize()?));
        }
    }
    ensure!(
        !selected,
        "--chain requires ~/indexer/<chain>/config.toml; create it or provide --config PATH"
    );
    Ok(None)
}

fn read(path: &Path) -> eyre::Result<toml::Table> {
    let text = std::fs::read_to_string(path).wrap_err("cannot read TOML configuration")?;
    text.parse::<toml::Table>()
        .map_err(|_| eyre!("invalid TOML configuration (contents redacted)"))
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
