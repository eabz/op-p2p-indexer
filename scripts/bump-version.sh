#!/usr/bin/env bash
# Bumps the workspace version, commits it and tags it, ready to push (which starts the release
# workflow, .github/workflows/release.yml).
#
#   scripts/bump-version.sh <major|minor|patch|X.Y.Z>
#
# Sets `[workspace.package] version` in Cargo.toml, refreshes Cargo.lock offline (workspace
# packages only), commits `release: vX.Y.Z` and makes the annotated tag `vX.Y.Z`. Pushes
# nothing: it prints the push command. Refuses uncommitted changes to tracked files, a tag that
# already exists, and a version that does not go up.
#
# Written for bash 3.2 (macOS) and both BSD and GNU tools: no `sed -i`, no bash 4 features.
set -euo pipefail

usage() {
    echo "usage: $0 <major|minor|patch|X.Y.Z>" >&2
    exit 2
}

die() {
    echo "error: $*" >&2
    exit 1
}

# X.Y.Z, numbers only.
semver='^[0-9]+\.[0-9]+\.[0-9]+$'

# A version as one number that orders like it (each part below a million).
pack() {
    echo "$1" | awk -F. '{ printf "%d%06d%06d\n", $1, $2, $3 }'
}

[ $# -eq 1 ] || usage
cd "$(git rev-parse --show-toplevel)"

[ -z "$(git status --porcelain --untracked-files=no)" ] ||
    die "the working tree has uncommitted changes; commit or stash them first"

# The version in the [workspace.package] table.
current=$(awk '
    /^\[/ { in_package = ($0 == "[workspace.package]") }
    in_package && /^version *=/ { gsub(/^version *= *"|".*$/, ""); print; exit }
' Cargo.toml)
echo "$current" | grep -Eq "$semver" ||
    die "no X.Y.Z version under [workspace.package] in Cargo.toml (found '$current')"

IFS=. read -r major minor patch <<EOF
$current
EOF

case "$1" in
    major) new="$((major + 1)).0.0" ;;
    minor) new="$major.$((minor + 1)).0" ;;
    patch) new="$major.$minor.$((patch + 1))" ;;
    *)
        echo "$1" | grep -Eq "$semver" || usage
        # Without leading zeros: 0.03.0 is 0.3.0.
        new=$(echo "$1" | awk -F. '{ printf "%d.%d.%d\n", $1, $2, $3 }')
        ;;
esac

[ "$(pack "$new")" -gt "$(pack "$current")" ] || die "$new does not go up from $current"

tag="v$new"
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    die "tag $tag already exists"
fi

# Rewrite the version line of [workspace.package] only, through a temporary file (portable,
# unlike `sed -i`).
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
awk -v new="$new" '
    /^\[/ { in_package = ($0 == "[workspace.package]") }
    in_package && /^version *=/ && !done { print "version = \"" new "\""; done = 1; next }
    { print }
' Cargo.toml >"$tmp"
cat "$tmp" >Cargo.toml

# Only the workspace's own packages change in the lock file; no network.
cargo update --workspace --offline --quiet

git add Cargo.toml Cargo.lock
git commit --quiet -F - <<EOF
release: $tag

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
git tag -a "$tag" -m "$tag"

branch=$(git rev-parse --abbrev-ref HEAD)
echo "Version $current -> $new: committed and tagged $tag (nothing pushed)."
echo "To publish the release:"
echo "  git push origin $branch $tag"
