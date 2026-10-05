//! Shared command-line flag extraction for startup and service commands.

use std::ffi::{OsStr, OsString};

/// The value of `flag` in `args`: the argument after `<flag>`, or the rest of `<flag>=<value>`.
pub(crate) fn flag_value(flag: &str, args: impl IntoIterator<Item = OsString>) -> Option<OsString> {
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
pub(crate) fn without_flag(flag: &str, args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
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

/// The value in `arg` if it is `<flag>=<value>`.
fn inline<'a>(arg: &'a OsStr, flag: &str) -> Option<&'a str> {
    arg.to_str()?.strip_prefix(flag)?.strip_prefix('=')
}
