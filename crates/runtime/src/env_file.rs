//! The `.env` file a binary loads at startup, so its `OP_INDEXER_*` variables (and `RUST_LOG`)
//! can live in a file rather than in the shell; one file serves every binary.
//!
//! The file is `--env-file <path>` on the command line, else the path in `OP_INDEXER_ENV_FILE`,
//! else `.env` in the current directory, which may be missing. It holds `NAME=value` lines,
//! parsed by `dotenvy`; a variable already set in the process environment wins over the file.
//! Values are never logged nor put in an error: the file holds secrets (R2 keys, API keys).

use std::ffi::{OsStr, OsString};
use std::io;
use std::path::PathBuf;

use eyre::{WrapErr, eyre};

/// The command-line flag naming the file.
pub const FLAG: &str = "--env-file";
/// The variable naming the file when the flag is absent.
const VAR: &str = "OP_INDEXER_ENV_FILE";
/// The file read when neither names one, in the current directory.
const DEFAULT: &str = ".env";

/// Loads the env file into the process environment and returns its path, or `None` if no file
/// was named and `.env` does not exist.
///
/// `args` are the command-line arguments after the program name; only `--env-file <path>` and
/// `--env-file=<path>` are read from them. Call it first in `main`, before the tokio runtime
/// or any other thread starts (it sets environment variables) and before the command line or
/// the configuration is parsed (they read the variables it sets).
///
/// # Errors
///
/// Returns an error naming the file if a named file does not exist or cannot be read, and the
/// file and line if a line is not `NAME=value`.
pub fn load(args: impl IntoIterator<Item = OsString>) -> eyre::Result<Option<PathBuf>> {
    let named = flag_value(FLAG, args)
        .or_else(|| std::env::var_os(VAR))
        .map(PathBuf::from);
    let explicit = named.is_some();
    let path = named.unwrap_or_else(|| PathBuf::from(DEFAULT));
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound && !explicit => return Ok(None),
        Err(err) => {
            return Err(err).wrap_err_with(|| format!("failed to read {}", path.display()));
        }
    };
    match dotenvy::from_read(text.as_bytes()) {
        Ok(()) => Ok(Some(path)),
        // The error's text holds the line, which may hold a secret: report only its number.
        Err(dotenvy::Error::LineParse(line, _index)) => Err(match line_number(&text, &line) {
            Some(number) => eyre!("{}: line {number} is not NAME=value", path.display()),
            None => eyre!("{}: a line is not NAME=value", path.display()),
        }),
        Err(err) => Err(err).wrap_err_with(|| format!("failed to load {}", path.display())),
    }
}

/// The value of `flag` in `args`: the argument after `<flag>`, or the rest of `<flag>=<value>`.
pub fn flag_value(flag: &str, args: impl IntoIterator<Item = OsString>) -> Option<OsString> {
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == flag {
            return args.next();
        }
        if let Some(value) = inline(&arg, flag) {
            return Some(value.into());
        }
    }
    None
}

/// `args` without `flag` and its value (`<flag> <value>` or `<flag>=<value>`).
pub fn without_flag(flag: &str, args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    let mut kept = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == flag {
            args.next();
        } else if inline(&arg, flag).is_none() {
            kept.push(arg);
        }
    }
    kept
}

/// The arguments the binary itself reads: `args` up to any `--`, without the env file's flag
/// and its value.
pub fn other_args(args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    without_flag(FLAG, args.into_iter().take_while(|arg| arg != "--"))
}

/// The value in `arg` if it is `<flag>=<value>`.
fn inline<'a>(arg: &'a OsStr, flag: &str) -> Option<&'a str> {
    arg.to_str()?.strip_prefix(flag)?.strip_prefix('=')
}

/// The 1-based number of the line of `text` where `line` starts: `dotenvy` reports the line's
/// text but not its number.
fn line_number(text: &str, line: &str) -> Option<usize> {
    let start = text.find(line)?;
    Some(text.get(..start)?.matches('\n').count().saturating_add(1))
}
