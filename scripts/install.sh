#!/usr/bin/env bash
# Downloads verified binaries, then optionally configures independent chain services.
# Download this script and run it with bash, or pipe it to bash -s -- [options].

main() (
    set -euo pipefail
    umask 022
    local version="" prefix="/usr/local" repo="https://github.com/eabz/op-p2p-indexer"
    local download_dir="" bin_stage="" share_stage=""
    local action="" non_interactive=0 binaries_only=0
    local setup_args=()
    step() { printf "\n==> %s\n" "$*" >&2; }
    die() { printf 'install: %s\n' "$*" >&2; exit 1; }
    cleanup() {
        if [[ -n "$bin_stage" ]]; then rm -rf -- "$bin_stage"; fi
        if [[ -n "$share_stage" ]]; then rm -rf -- "$share_stage"; fi
        if [[ -n "$download_dir" ]]; then rm -rf -- "$download_dir"; fi
    }
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --version|--prefix)
                [[ $# -ge 2 && -n "$2" ]] || die "$1 needs a value"
                if [[ "$1" == --version ]]; then version="$2"; else prefix="$2"; fi
                shift 2 ;;
            --action)
                [[ $# -ge 2 ]] || die '--action needs a value'
                action="$2"; shift 2 ;;
            --roles|--role|--chain|--user|--root|--port-base|--set)
                [[ $# -ge 2 ]] || die "$1 needs a value"
                setup_args+=("$1" "$2"); shift 2 ;;
            --non-interactive)
                non_interactive=1; setup_args+=("$1"); shift ;;
            --register|--enable|--start|--restart)
                setup_args+=("$1"); shift ;;
            --binaries-only)
                binaries_only=1; shift ;;
            --help|-h)
                printf '%s\n' 'Usage: bash install.sh [--version vX.Y.Z] [--prefix /usr/local] [options]'
                printf '%s\n' 'Default: guided setup using /dev/tty. --binaries-only installs verified binaries only.'
                printf '%s\n' '--action install|add-service|update|restart|remove'
                printf '%s\n' '--non-interactive --roles server,indexer,balancer,importer --chain op|unichain|base'
                printf '%s\n' '--user ACCOUNT --root /home/ACCOUNT/indexer --port-base ROLE=PORT'
                printf '%s\n' '--set SECTION.KEY=TOML_VALUE (prefer private config files for secrets)'
                printf '%s\n' '--register writes systemd units; --enable and --start are explicit opt-ins.'
                printf '%s\n' '--restart restarts selected services after update. Remove keeps config/data.'
                printf '%s\n' 'Linux x86_64, glibc >=2.35. Missing Ubuntu prerequisites are installed automatically when run as root.'
                exit 0 ;;
            *) die "unknown option: $1 (see --help)" ;;
        esac
    done
    [[ -z "$version" || "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die 'version must be vX.Y.Z'
    [[ "$prefix" == /* ]] || die 'prefix must be an absolute path'
    while [[ "$prefix" != / && "$prefix" == */ ]]; do prefix="${prefix%/}"; done
    [[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || die 'only Linux x86_64 is supported'
    local libc major minor
    libc=$(getconf GNU_LIBC_VERSION 2>/dev/null) || die 'glibc is required (musl is not supported)'
    [[ "$libc" =~ ^glibc\ ([0-9]+)\.([0-9]+)$ ]] || die "cannot determine glibc version: $libc"
    major="${BASH_REMATCH[1]}"; minor="${BASH_REMATCH[2]}"
    (( 10#$major > 2 || (10#$major == 2 && 10#$minor >= 35) )) || die 'glibc >= 2.35 is required (Ubuntu 22.04 or newer)'


    # The installer itself is the bootstrap: fresh Ubuntu needs no Python/pip preparation.
    # Only known distro packages are installed, and only when a required tool is missing.
    step "Checking this machine and prerequisites"
    local tool package
    local packages=()
    for tool in curl tar sha256sum mktemp install mv rm mkdir chmod grep; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            case "$tool" in curl|tar|grep) package="$tool" ;; *) package=coreutils ;; esac
            packages+=("$package")
        fi
    done
    [[ -s /etc/ssl/certs/ca-certificates.crt ]] || packages+=(ca-certificates)
    if (( ! binaries_only )); then
        if ! command -v python3 >/dev/null 2>&1; then
            packages+=(python3 python3-tomli)
        elif ! python3 -c 'try:
 import tomllib
except ImportError:
 import tomli' 2>/dev/null; then
            packages+=(python3-tomli)
        fi
    fi
    if (( ${#packages[@]} )); then
        command -v apt-get >/dev/null 2>&1 || die "missing prerequisites: ${packages[*]}; install them with your package manager"
        (( EUID == 0 )) || die "missing prerequisites: ${packages[*]}; rerun with sudo or install them first"
        printf 'Installing required Ubuntu packages: %s\n' "${packages[*]}"
        apt-get -o Acquire::Retries=2 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 update || die 'cannot refresh Ubuntu package metadata'
        DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=60 -o Acquire::Retries=2 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 install -y --no-install-recommends "${packages[@]}" || die 'cannot install prerequisites'
    fi
    for tool in uname getconf curl tar sha256sum mktemp install mv rm mkdir chmod grep; do
        command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
    done

    if (( ! binaries_only )); then
        command -v python3 >/dev/null || die 'setup requires python3'
        python3 -c 'try:
 import tomllib
except ImportError:
 import tomli' 2>/dev/null || die 'install TOML support: sudo apt-get install python3-tomli'
        if [[ -z "$action" && "$non_interactive" == 0 ]]; then
            local tty_fd
            { exec {tty_fd}<>/dev/tty; } 2>/dev/null || die 'no terminal; use --non-interactive or --binaries-only'
            local labels=("Install and configure" "Add a service" "Update binaries" "Restart services" "Remove services (keep data)")
            local actions=(install add-service update restart remove)
            local selected=0 index key sequence
            printf '\nOP Indexer setup\n' >&"$tty_fd"
            if [[ "${TERM:-dumb}" != dumb && -t "$tty_fd" ]]; then
                printf 'Use ↑/↓ to move, Enter to select, or q to cancel.\n\n' >&"$tty_fd"
                while :; do
                    for index in "${!labels[@]}"; do
                        if (( index == selected )); then
                            printf '\r\033[2K  > \033[7m%s\033[0m\n' "${labels[index]}" >&"$tty_fd"
                        else
                            printf '\r\033[2K    %s\n' "${labels[index]}" >&"$tty_fd"
                        fi
                    done
                    IFS= read -rsn1 key <&"$tty_fd" || die 'terminal closed; nothing selected'
                    case "$key" in
                        "") action="${actions[selected]}"; break ;;
                        q|Q) die 'cancelled' ;;
                        1|2|3|4|5) selected=$((key - 1)) ;;
                        $'\033')
                            sequence=""
                            IFS= read -rsn2 -t 0.2 sequence <&"$tty_fd" || true
                            case "$sequence" in
                                '[A'|'OA') selected=$(((selected + 4) % 5)) ;;
                                '[B'|'OB') selected=$(((selected + 1) % 5)) ;;
                            esac ;;
                    esac
                    printf '\033[5A' >&"$tty_fd"
                done
            else
                for index in "${!labels[@]}"; do
                    printf '  %s) %s\n' "$((index + 1))" "${labels[index]}" >&"$tty_fd"
                done
                while :; do
                    printf '\nChoose an action [1]: ' >&"$tty_fd"
                    IFS= read -r action <&"$tty_fd" || die 'terminal closed; nothing selected'
                    case "$action" in
                        ""|1|install) action=install; break ;;
                        2|add-service) action=add-service; break ;;
                        3|update) action=update; break ;;
                        4|restart) action=restart; break ;;
                        5|remove) action=remove; break ;;
                        *) printf 'Enter a number from 1 to 5, or an action name.\n' >&"$tty_fd" ;;
                    esac
                done
            fi
            exec {tty_fd}>&-
        fi
        action="${action:-install}"
        case "$action" in install|add-service|update|restart|remove) ;; *) die 'invalid action' ;; esac
        if [[ "$action" == add-service || "$action" == restart || "$action" == remove ]]; then
            [[ -f "$prefix/share/op-p2p-indexer/setup.py" && -f "$prefix/share/op-p2p-indexer/config.toml.example" ]] || die 'install a release with guided setup support first'
            python3 "$prefix/share/op-p2p-indexer/setup.py" --prefix "$prefix" --action "$action" "${setup_args[@]}"
            exit $?
        fi
    fi

    local effective
    if [[ -z "$version" ]]; then
        step "Finding the latest release on GitHub"
        effective=$(curl --proto '=https' --tlsv1.2 -fsSL --connect-timeout 15 --max-time 60 \
            -o /dev/null -w '%{url_effective}' "$repo/releases/latest") || die 'cannot resolve the latest GitHub release'
        [[ "$effective" == "$repo/releases/tag/"* ]] || die "unexpected release redirect: $effective"
        version="${effective##*/}"
        [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "unsupported release tag: $version"
    fi
    local name="op-p2p-indexer-$version-x86_64-linux" archive url
    archive="$name.tar.gz"
    url="$repo/releases/download/$version"
    download_dir=$(mktemp -d) || die 'cannot create download directory'
    step "Downloading $version (progress below; stalled transfers time out)"
    curl --proto '=https' --tlsv1.2 -fL --progress-bar --retry 3 --retry-max-time 600 --connect-timeout 15 --max-time 600 --speed-limit 1024 --speed-time 30 \
        -o "$download_dir/$archive" "$url/$archive" || die 'binary download failed'
    curl --proto '=https' --tlsv1.2 -fsSL --retry 3 --connect-timeout 15 --max-time 60 \
        -o "$download_dir/checksum" "$url/$archive.sha256" || die 'checksum download failed'
    step "Verifying the archive checksum"
    local checksum line="" hash="" listed="" count=0
    # Accept exactly one standard sha256sum record, tied to this release archive.
    while IFS= read -r line || [[ -n "$line" ]]; do
        count=$((count + 1))
        [[ "$line" =~ ^([[:xdigit:]]{64})\ [\ \*](.+)$ ]] || die 'invalid checksum record'
        hash="${BASH_REMATCH[1]}"; listed="${BASH_REMATCH[2]}"
        [[ "$listed" == "$archive" ]] || die 'checksum names an unexpected archive'
    done < "$download_dir/checksum"
    [[ "$count" == 1 ]] || die 'expected exactly one checksum record'
    checksum=$(sha256sum "$download_dir/$archive") || die 'cannot hash downloaded archive'
    [[ "${checksum%% *}" == "${hash,,}" ]] || die 'archive checksum mismatch; installed files unchanged'

    # Extract only known members. Older releases may omit the companion files.
    tar -tzf "$download_dir/$archive" > "$download_dir/members" || die 'invalid release archive'
    # Never probe legacy binaries with TOML flags: old versions may ignore them and start.
    if (( ! binaries_only )); then
        for item in setup.py config.toml.example; do
            grep -Fxq "$name/$item" "$download_dir/members" || die "release $version lacks guided setup; use --binaries-only for a legacy binary install"
        done
    fi
    local binary item
    local binaries=(indexer server import balancer) companions=() members=()
    # Native benchmarking is optional in older releases.
    if grep -Fxq "$name/bench" "$download_dir/members"; then
        binaries+=(bench)
    fi
    for binary in "${binaries[@]}"; do
        grep -Fxq "$name/$binary" "$download_dir/members" || die "archive missing $binary"
        members+=("$name/$binary")
    done
    for item in config.toml.example setup.py install.sh bench.py README.md LICENSE; do
        if grep -Fxq "$name/$item" "$download_dir/members"; then
            companions+=("$item")
            members+=("$name/$item")
        fi
    done
    step "Extracting verified binaries"
    tar -xzf "$download_dir/$archive" -C "$download_dir" --no-same-owner --no-same-permissions \
        -- "${members[@]}" || die 'cannot extract release files'
    for item in "${members[@]}"; do
        [[ -f "$download_dir/$item" && ! -L "$download_dir/$item" ]] || die "archive member is not a regular file: $item"
    done

    for binary in "${binaries[@]}"; do
        [[ -s "$download_dir/$name/$binary" ]] || die "archive contains an empty binary: $binary"
    done

    step "Installing binaries to $prefix/bin"
    # Preflight every destination before staging or replacing any installed file.
    for binary in "${binaries[@]}"; do
        [[ ! -d "$prefix/bin/$binary" ]] || die "binary destination is a directory: $prefix/bin/$binary"
    done
    for item in "${companions[@]}"; do
        [[ ! -d "$prefix/share/op-p2p-indexer/$item" ]] || die "companion destination is a directory: $item"
    done
    if grep -Fxq "$name/setup.py" "$download_dir/members" && grep -Fxq "$name/config.toml.example" "$download_dir/members"; then
        [[ ! -d "$prefix/share/op-p2p-indexer/setup-binaries.sha256" ]] || die 'capability checksum destination is a directory'
    fi
    # Each destination directory may be on its own mounted filesystem. Stage inside each
    # one so every final rename stays atomic, then finish all copies before replacing files.
    mkdir -p -- "$prefix/bin" "$prefix/share/op-p2p-indexer" || die "cannot create $prefix; use sudo or a writable --prefix"
    bin_stage=$(mktemp -d "$prefix/bin/.op-p2p-indexer-install.XXXXXX") || die "cannot stage files under $prefix/bin"
    share_stage=$(mktemp -d "$prefix/share/op-p2p-indexer/.install.XXXXXX") || die 'cannot stage companion files'
    for binary in "${binaries[@]}"; do
        install -m 0755 "$download_dir/$name/$binary" "$bin_stage/$binary"
    done
    for item in "${companions[@]}"; do
        install -m 0644 "$download_dir/$name/$item" "$share_stage/$item"
    done
    # Bind setup capability to these exact binaries, so a later legacy --binaries-only
    # downgrade cannot leave a helper that probes old binaries with unsupported flags.
    if grep -Fxq "$name/setup.py" "$download_dir/members" && grep -Fxq "$name/config.toml.example" "$download_dir/members"; then
        (cd "$bin_stage" && sha256sum indexer server import balancer) > "$share_stage/setup-binaries.sha256"
        chmod 0644 "$share_stage/setup-binaries.sha256"
        companions+=(setup-binaries.sha256)
    fi
    # Atomic per file: a running process keeps its old inode, avoiding ETXTBSY. Keep these
    # stable paths because generated service units record the executable's resolved path.
    for binary in "${binaries[@]}"; do
        mv -fT -- "$bin_stage/$binary" "$prefix/bin/$binary"
    done
    for item in "${companions[@]}"; do
        mv -fT -- "$share_stage/$item" "$prefix/share/op-p2p-indexer/$item"
    done
    printf 'Installed %s to %s/bin\n' "$version" "$prefix"
    printf 'Companion files: %s/share/op-p2p-indexer\n' "$prefix"
    if (( ! binaries_only )); then
        step "Configuring chains and services"
        python3 "$prefix/share/op-p2p-indexer/setup.py" --prefix "$prefix" --action "$action" "${setup_args[@]}"
    else
        printf 'Configuration and state are unchanged. Restart services explicitly when ready.\n'
    fi
    case ":$PATH:" in
        *":$prefix/bin:"*) ;;
        *) printf 'Add %s/bin to PATH to run the installed commands.\n' "$prefix" ;;
    esac
)

main "$@"
