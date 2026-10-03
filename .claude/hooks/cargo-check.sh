#!/usr/bin/env bash
# Stop: when Rust sources changed since the last clean run, run `cargo fmt`,
# `cargo clippy -D warnings`, and (if manifests changed) `cargo deny check`.
# A failure blocks the stop (exit 2) so Claude fixes it. If Claude is already
# continuing because of this hook, report to the user instead of looping.
set -uo pipefail

input=$(cat)
cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

paths=('*.rs' '*Cargo.toml' 'Cargo.lock' 'clippy.toml' 'rustfmt.toml' 'deny.toml')
changed=$(git status --porcelain -- "${paths[@]}" 2>/dev/null)
[[ -n "$changed" ]] || exit 0

# Skip if nothing changed since the last clean run (e.g. Q&A turns on a dirty tree).
stamp=target/.claude-check-ok
fingerprint() {
    { git diff HEAD -- "${paths[@]}"; git ls-files -oz --exclude-standard -- "${paths[@]}" | xargs -0 cat 2>/dev/null; } | shasum | cut -d' ' -f1
}
fp=$(fingerprint)
[[ -f "$stamp" && "$(cat "$stamp")" == "$fp" ]] && exit 0

cargo fmt --all >/dev/null 2>&1

fail() {
    if [[ "$(jq -r '.stop_hook_active // false' <<<"$input")" == "true" ]]; then
        jq -n --arg m "$1 still fails after a fix attempt; run it manually to see the errors." '{systemMessage: $m}'
        exit 0
    fi
    printf '%s failed. Fix these before finishing:\n%s\n' "$1" "$(tail -n 120 <<<"$2")" >&2
    exit 2
}

# Default features, so the build cache is shared with `cargo test` / rust-analyzer.
out=$(cargo clippy --workspace --all-targets --quiet -- -D warnings 2>&1) || fail "cargo clippy" "$out"

if grep -qE 'Cargo\.(toml|lock)|deny\.toml' <<<"$changed"; then
    out=$(cargo deny --log-level error check 2>&1) || fail "cargo deny check" "$out"
fi

mkdir -p target && fingerprint >"$stamp"
