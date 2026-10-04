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
# never taken for a complete snapshot; a pull refuses a snapshot without the marker, and refuses
# it again if the marker changed during the pull (a push ran meanwhile). A pull writes into
# `<dir>.partial` and renames it to `<dir>` only once the copy is checked, so an interrupted
# pull never leaves a half-copied archive where the node looks. Re-running either direction
# resumes: files that already match are skipped.
#
# A pull refuses to replace an existing archive unless FORCE=1 is set (it then reuses the
# files that match, so a refresh downloads only what changed), and refuses a directory that is
# not a fjall archive at all.
#
# Environment (all optional):
#   RCLONE_REMOTE      rclone remote for R2 (default: r2)
#   OP_ARCHIVE_DIR     default: /root/op-p2p-indexer/data-op/archive
#   UNICHAIN_ARCHIVE_DIR  default: /root/op-p2p-indexer/data-unichain/archive
#   OP_BUCKET          default: op-snapshot
#   UNICHAIN_BUCKET    default: unichain-snapshot
#   TRANSFERS          parallel file transfers (default: 64)
#   FORCE              1: let a pull replace an existing local archive
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
# `lock` is fjall's lock file, held by whichever process has the archive open. It is not
# synced; fjall needs it to exist when it opens an existing archive, so a pull creates it.
flags=(--exclude /lock --transfers "$transfers" --checkers "$transfers" --fast-list
  --s3-no-check-bucket --stats 10s --stats-one-line --stats-log-level NOTICE)

# Refuses to touch an archive another process has open.
ensure_closed() {
  local lock=${1:-$dir}/lock
  [[ -e $lock ]] || return 0
  if ! command -v fuser >/dev/null; then
    echo "fuser not found (apt-get install -y psmisc): cannot check that $dir is closed" >&2
    exit 1
  fi
  if fuser -s "$lock" 2>/dev/null; then
    echo "${1:-$dir} is open in another process (the node or \`import load\`): stop it first" >&2
    exit 1
  fi
}

case $direction in
  push)
    [[ -f $dir/version ]] || { echo "$dir is not a fjall archive (no version file)" >&2; exit 1; }
    ensure_closed
    echo "push $dir -> $target"
    # A marker that exists must be gone before any file changes; one that does not is fine.
    if [[ -n $(rclone lsf "$marker" 2>/dev/null) ]]; then
      rclone deletefile "$marker" --s3-no-check-bucket
    fi
    rclone sync "$dir" "$target" "${flags[@]}"
    rclone check "$dir" "$target" --one-way --exclude /lock --fast-list --checkers "$transfers"
    printf 'pushed_at=%s\nhost=%s\nbytes=%s\nfiles=%s\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(hostname)" \
      "$(du -sb "$dir" | cut -f1)" "$(find "$dir" -type f ! -name lock | wc -l)" |
      rclone rcat "$marker" --s3-no-check-bucket
    echo "push complete: $target"
    ;;
  pull)
    before=$(rclone cat "$marker" 2>/dev/null) || {
      echo "$target has no completion marker: the last push did not finish" >&2
      exit 1
    }
    partial="$dir.partial"
    if [[ -d $dir ]]; then
      if [[ -n $(ls -A "$dir") && ! -f $dir/version ]]; then
        echo "$dir is not empty and is not a fjall archive: refusing to overwrite it" >&2
        exit 1
      fi
      if [[ -f $dir/version && ${FORCE:-0} != 1 ]]; then
        echo "$dir already holds an archive: set FORCE=1 to replace it with the snapshot" >&2
        exit 1
      fi
      ensure_closed
      [[ -e $partial ]] && { echo "$partial exists: remove it or finish that pull first" >&2; exit 1; }
      mv "$dir" "$partial"
    fi
    mkdir -p "$partial"
    ensure_closed "$partial"
    echo "pull $target -> $dir"
    printf '%s\n' "$before"
    rclone sync "$target" "$partial" "${flags[@]}"
    rclone check "$target" "$partial" --one-way --exclude /lock --fast-list --checkers "$transfers"
    after=$(rclone cat "$marker" 2>/dev/null) || after=
    if [[ $after != "$before" ]]; then
      echo "the snapshot changed during the pull (a push ran): run the pull again" >&2
      exit 1
    fi
    touch "$partial/lock"
    mv "$partial" "$dir"
    echo "pull complete: $dir"
    ;;
  *) usage ;;
esac
