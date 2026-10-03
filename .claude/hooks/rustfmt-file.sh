#!/usr/bin/env bash
# PostToolUse (Edit|Write): after a Rust file is written, format the workspace.
# `cargo fmt` formats from crate roots, so out-of-line modules resolve correctly; bare `rustfmt`
# on a non-root file treats it as a crate root and resolves `mod foo;` against the wrong directory.
# Exit 2 feeds rustfmt's error (usually a syntax error) back to Claude.
set -uo pipefail

file=$(jq -r '.tool_input.file_path // empty')
[[ "$file" == *.rs && -f "$file" ]] || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0
if ! out=$(cargo fmt --all 2>&1); then
    printf 'cargo fmt failed after editing %s:\n%s\n' "$file" "$out" >&2
    exit 2
fi
