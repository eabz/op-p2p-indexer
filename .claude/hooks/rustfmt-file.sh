#!/usr/bin/env bash
# PostToolUse (Edit|Write): format the Rust file that was just written.
# Exit 2 feeds rustfmt's error (usually a syntax error) back to Claude.
set -uo pipefail

file=$(jq -r '.tool_input.file_path // empty')
[[ "$file" == *.rs && -f "$file" ]] || exit 0

if ! out=$(rustfmt "$file" 2>&1); then
    printf 'rustfmt failed on %s:\n%s\n' "$file" "$out" >&2
    exit 2
fi
