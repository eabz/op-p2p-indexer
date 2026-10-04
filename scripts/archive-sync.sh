#!/usr/bin/env bash
# Mirrors a chain's block archive (a fjall database) to or from Cloudflare R2 with rclone.
#
#   scripts/archive-sync.sh push <op|unichain>   # local archive  -> R2
#   scripts/archive-sync.sh pull <op|unichain>   # R2             -> local archive
#
# Only the archive is synced, never the node store (`node/` in the data dir): it holds the
# node's private key.
#
# A fjall database is consistent only while nothing writes to it, so both directions refuse
# to run while a process has the archive open: stop the node or `import load` first. A push
# deletes the remote completion marker first and writes it last, so an interrupted push is
# never taken for a complete snapshot; a pull refuses a snapshot without the marker. Re-running
# either direction resumes: files that already match are skipped.
#
# Environment (all optional):
#   RCLONE_REMOTE      rclone remote for R2 (default: r2)
#   OP_ARCHIVE_DIR     default: /root/op-p2p-indexer/data-op/archive
#   UNICHAIN_ARCHIVE_DIR  default: /root/op-p2p-indexer/data-unichain/archive
#   OP_BUCKET          default: op-snapshot
#   UNICHAIN_BUCKET    default: unichain-snapshot
#   TRANSFERS          parallel file transfers (default: 64)
set -euo pipefail

usage() {
  echo "usage: $0 push|pull op|unichain" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
direction=$1
chain=$2

remote=${RCLONE_REMOTE:-r2}
transfers=${TRANSFERS:-64}
case $chain in
  op)
    dir=${OP_ARCHIVE_DIR:-/root/op-p2p-indexer/data-op/archive}
    bucket=${OP_BUCKET:-op-snapshot}
    ;;
  unichain)
    dir=${UNICHAIN_ARCHIVE_DIR:-/root/op-p2p-indexer/data-unichain/archive}
    bucket=${UNICHAIN_BUCKET:-unichain-snapshot}
    ;;
  *) usage ;;
esac

target="$remote:$bucket/archive"
marker="$remote:$bucket/archive.complete"
# `lock` is fjall's own lock file; it is recreated on open and must not travel.
flags=(--exclude /lock --transfers "$transfers" --checkers "$transfers" --fast-list
  --s3-no-check-bucket --stats 10s --stats-one-line)

# Refuses to touch an archive another process has open.
ensure_closed() {
  [[ -e $dir/lock ]] || return 0
  if ! command -v fuser >/dev/null; then
    echo "fuser not found (apt-get install -y psmisc): cannot check that $dir is closed" >&2
    exit 1
  fi
  if fuser -s "$dir/lock" 2>/dev/null; then
    echo "$dir is open in another process (the node or \`import load\`): stop it first" >&2
    exit 1
  fi
}

case $direction in
  push)
    [[ -f $dir/version ]] || { echo "$dir is not a fjall archive (no version file)" >&2; exit 1; }
    ensure_closed
    echo "push $dir -> $target"
    rclone deletefile "$marker" --s3-no-check-bucket 2>/dev/null || true
    rclone sync "$dir" "$target" "${flags[@]}"
    rclone check "$dir" "$target" --one-way --exclude /lock --fast-list --checkers "$transfers"
    printf 'pushed_at=%s\nhost=%s\nbytes=%s\nfiles=%s\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(hostname)" \
      "$(du -sb "$dir" | cut -f1)" "$(find "$dir" -type f ! -name lock | wc -l)" |
      rclone rcat "$marker" --s3-no-check-bucket
    echo "push complete: $target"
    ;;
  pull)
    if ! rclone cat "$marker" >/dev/null 2>&1; then
      echo "$target has no completion marker: the last push did not finish" >&2
      exit 1
    fi
    mkdir -p "$dir"
    ensure_closed
    echo "pull $target -> $dir"
    rclone cat "$marker"
    rclone sync "$target" "$dir" "${flags[@]}"
    rclone check "$target" "$dir" --one-way --exclude /lock --fast-list --checkers "$transfers"
    echo "pull complete: $dir"
    ;;
  *) usage ;;
esac
