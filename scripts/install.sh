#!/usr/bin/env bash
# Installs, updates and removes the op-p2p-indexer binaries and their systemd services.
#
#   curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh | sudo bash
#
# It asks which programs this machine runs (arrow keys, Space, Enter), downloads the latest
# release, checks its checksum, and installs them to /usr/local/bin. Each service (server,
# indexer, balancer) gets a systemd unit reading ~/indexer/config.toml. Run it again to
# update, or to add or remove programs: what is unticked is stopped and removed, and the
# configuration and data are kept. Takes no options.

main() (
    set -euo pipefail
    umask 022

    local repo="https://github.com/eabz/op-p2p-indexer"
    local bin_dir="/usr/local/bin" share_dir="/usr/local/share/op-p2p-indexer"
    local unit_dir="/etc/systemd/system"
    # name|kind|what it is
    local programs=(
        "server|service|Serves history from R2, behind a balancer"
        "indexer|service|Full node with its own local archive"
        "balancer|service|Directs clients to the healthy servers"
        "import|tool|Imports a chain's history into R2 (run by hand)"
        "bench|tool|Benchmarks reads from a balancer (run by hand)"
    )

    # --- Output --------------------------------------------------------------------------------
    local bold="" dim="" green="" cyan="" red="" yellow="" reset=""
    if [[ -t 2 ]]; then
        bold=$'\e[1m' dim=$'\e[2m' green=$'\e[32m' cyan=$'\e[36m' red=$'\e[31m'
        yellow=$'\e[33m' reset=$'\e[0m'
    fi
    say() { printf '%s\n' "$*" >&2; }
    ok() { printf '%s✔%s %s\n' "$green" "$reset" "$*" >&2; }
    die() { printf '\r\e[2K\n%s✖ %s%s\n' "$red" "$*" "$reset" >&2; exit 1; }
    # Runs a command in the background behind a spinner and `message`; its output goes to a
    # log, shown only if it fails.
    spin() {
        local message=$1 log frame=0 status=0
        local frames=(⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏)
        shift
        log=$(mktemp)
        "$@" >"$log" 2>&1 &
        local pid=$!
        printf '\e[?25l' >&2
        while kill -0 "$pid" 2>/dev/null; do
            printf '\r\e[2K%s%s%s %s' "$cyan" "${frames[frame++ % 10]}" "$reset" "$message" >&2
            sleep 0.1
        done
        wait "$pid" || status=$?
        printf '\r\e[2K\e[?25h' >&2
        if (( status )); then
            tail -n 20 "$log" >&2
            rm -f -- "$log"
            die "$message: failed"
        fi
        rm -f -- "$log"
        ok "$message"
    }

    local work=""
    # shellcheck disable=SC2329 # run by the EXIT trap
    cleanup() {
        printf '\e[?25h' >&2 # the cursor, if a menu hid it
        if [[ -n "$work" ]]; then rm -rf -- "$work"; fi
    }
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    [[ $# -eq 0 ]] || die "install.sh takes no options; run it and choose in the menu"

    # --- Checks --------------------------------------------------------------------------------
    [[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || die "only Linux x86-64 is supported"
    local libc
    libc=$(getconf GNU_LIBC_VERSION 2>/dev/null) || die "glibc is required (Ubuntu 22.04 or newer)"
    if ! [[ "$libc" =~ ^glibc\ 2\.([0-9]+)$ ]] || (( BASH_REMATCH[1] < 35 )); then
        die "glibc 2.35 or newer is required (Ubuntu 22.04 or newer); found $libc"
    fi
    (( EUID == 0 )) || die "run it with sudo: curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh | sudo bash"
    command -v systemctl >/dev/null || die "systemd is required"
    local tool missing=()
    for tool in curl tar sha256sum; do
        command -v "$tool" >/dev/null || missing+=("$tool")
    done
    [[ -s /etc/ssl/certs/ca-certificates.crt ]] || missing+=(ca-certificates)
    if (( ${#missing[@]} )); then
        command -v apt-get >/dev/null || die "missing: ${missing[*]}; install them first"
        missing=("${missing[@]/sha256sum/coreutils}")
        spin "Installing ${missing[*]}" env DEBIAN_FRONTEND=noninteractive bash -c \
            'apt-get -qq update && apt-get -qq install -y --no-install-recommends "$@"' _ "${missing[@]}"
    fi
    { exec 3<>/dev/tty; } 2>/dev/null || die "the installer needs a terminal to show its menu"

    # The account that runs the services and owns ~/indexer: whoever ran sudo.
    local user="${SUDO_USER:-root}" home
    home=$(getent passwd "$user" | cut -d: -f6)
    [[ -n "$home" && -d "$home" ]] || die "cannot find the home directory of $user"
    local config_dir="$home/indexer"
    local config="$config_dir/config.toml"

    # --- What is installed, and the latest release ---------------------------------------------
    local installed_version="" latest
    [[ -f "$share_dir/version" ]] && installed_version=$(<"$share_dir/version")
    work=$(mktemp -d)
    # The release page GitHub redirects "latest" to names the tag.
    # shellcheck disable=SC2016 # expanded by the inner bash
    spin "Finding the latest release" bash -c 'curl --proto =https --tlsv1.2 -fsSL \
        --connect-timeout 15 --max-time 60 -o /dev/null -w "%{url_effective}" "$1" >"$2"' \
        _ "$repo/releases/latest" "$work/latest"
    latest=$(<"$work/latest")
    latest="${latest##*/}"
    [[ "$latest" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "unexpected latest release: $latest"

    local names=() kinds=() descs=() had=() picked=() entry name kind
    for entry in "${programs[@]}"; do
        IFS='|' read -r name kind entry <<<"$entry"
        names+=("$name") kinds+=("$kind") descs+=("$entry")
        if [[ -x "$bin_dir/$name" && -f "$share_dir/version" ]] \
            && grep -Fxq "$name" "$share_dir/installed" 2>/dev/null; then
            had+=(1) picked+=(1)
        else
            had+=(0) picked+=(0)
        fi
    done
    local count=${#names[@]}

    # --- The menu ------------------------------------------------------------------------------
    # One line per program: a checkbox, its name, what it is, and what will happen to it.
    line() {
        local i=$1 pointer="  " box status=""
        (( i == cursor )) && pointer="${cyan}❯${reset} "
        if (( picked[i] )); then box="${green}◉${reset}"; else box="${dim}◯${reset}"; fi
        if (( had[i] && picked[i] )); then status="${dim}installed · update${reset}"
        elif (( had[i] )); then status="${red}remove${reset}"
        elif (( picked[i] )); then status="${green}install${reset}"
        fi
        local label
        label=$(printf '%-9s' "${names[i]}")
        (( i == cursor )) && label="${bold}${label}${reset}"
        printf '\r\e[2K%s%s %s %s%-8s%s %-52s %s\n' "$pointer" "$box" "$label" \
            "$dim" "${kinds[i]}" "$reset" "${descs[i]}" "$status" >&3
    }
    local cursor=0
    printf '\n%s◆ op-p2p-indexer installer%s\n' "$bold" "$reset" >&3
    if [[ -n "$installed_version" ]]; then
        printf '%s  installed %s · latest %s%s\n' "$dim" "$installed_version" "$latest" "$reset" >&3
    else
        printf '%s  latest release %s%s\n' "$dim" "$latest" "$reset" >&3
    fi
    printf '\n  Which programs run on this machine?\n\n' >&3
    printf '\e[?25l' >&3
    local i key rest
    for ((i = 0; i < count; i++)); do line "$i"; done
    printf '\n%s  ↑/↓ move · Space select · a all · Enter confirm · q quit%s\n' "$dim" "$reset" >&3
    while :; do
        IFS= read -rsn1 key <&3 || exit 1
        if [[ "$key" == $'\e' ]]; then
            rest=""
            IFS= read -rsn2 -t 0.05 rest <&3 || true
            key="$key$rest"
        fi
        case "$key" in
            $'\e[A'|k) cursor=$(( (cursor + count - 1) % count )) ;;
            $'\e[B'|j) cursor=$(( (cursor + 1) % count )) ;;
            ' ') picked[cursor]=$(( 1 - picked[cursor] )) ;;
            a)
                local all=1
                for ((i = 0; i < count; i++)); do (( picked[i] )) || all=0; done
                for ((i = 0; i < count; i++)); do picked[i]=$(( 1 - all )); done ;;
            '') break ;;
            q|$'\e') printf '\e[?25h' >&3; say ""; say "Nothing changed."; exit 0 ;;
            *) continue ;;
        esac
        printf '\e[%dA' $((count + 2)) >&3
        for ((i = 0; i < count; i++)); do line "$i"; done
        printf '\n\r\e[2K%s  ↑/↓ move · Space select · a all · Enter confirm · q quit%s\n' "$dim" "$reset" >&3
    done
    printf '\e[?25h' >&3

    local install=() remove=()
    for ((i = 0; i < count; i++)); do
        if (( picked[i] )); then
            install+=("${names[i]}")
        elif (( had[i] )); then
            remove+=("${names[i]}")
        fi
    done
    if (( ${#install[@]} == 0 && ${#remove[@]} == 0 )); then
        say ""; say "Nothing selected; nothing changed."; exit 0
    fi

    # --- Confirm -------------------------------------------------------------------------------
    printf '\n' >&3
    if (( ${#install[@]} )); then
        printf '  %sInstall or update%s  %s (%s)\n' "$green" "$reset" "${install[*]}" "$latest" >&3
    fi
    (( ${#remove[@]} == 0 )) || printf '  %sRemove%s             %s %s(configuration and data are kept)%s\n' "$red" "$reset" "${remove[*]}" "$dim" "$reset" >&3
    local yes=1
    confirm_line() {
        local y="  Yes  " n="  No  "
        if (( yes )); then y="${cyan}${bold}❯ Yes${reset}  "; else n="${cyan}${bold}❯ No${reset} "; fi
        printf '\r\e[2K\n\r\e[2K  Continue?  %s %s  %s(←/→, Enter)%s' "$y" "$n" "$dim" "$reset" >&3
    }
    printf '\e[?25l' >&3
    confirm_line
    while :; do
        IFS= read -rsn1 key <&3 || exit 1
        if [[ "$key" == $'\e' ]]; then
            rest=""
            IFS= read -rsn2 -t 0.05 rest <&3 || true
            key="$key$rest"
        fi
        case "$key" in
            $'\e[C'|$'\e[D'|$'\t'|h|l) yes=$(( 1 - yes )) ;;
            y|Y) yes=1; break ;;
            n|N|q|$'\e') yes=0; break ;;
            '') break ;;
            *) continue ;;
        esac
        printf '\e[1A' >&3
        confirm_line
    done
    printf '\e[1A' >&3
    confirm_line
    printf '\e[?25h\n\n' >&3
    (( yes )) || { say "Nothing changed."; exit 0; }

    # --- Remove --------------------------------------------------------------------------------
    for name in "${remove[@]}"; do
        local unit="op-indexer-$name.service"
        if [[ -f "$unit_dir/$unit" ]]; then
            systemctl disable --now "$unit" >/dev/null 2>&1 || true
            rm -f -- "$unit_dir/$unit"
        fi
        rm -f -- "$bin_dir/$name"
        ok "removed $name"
    done

    # --- Download, check and install -----------------------------------------------------------
    if (( ${#install[@]} )); then
        local archive="op-p2p-indexer-$latest-x86_64-linux"
        local url="$repo/releases/download/$latest/$archive.tar.gz"
        say "${dim}Downloading $latest${reset}"
        curl --proto '=https' --tlsv1.2 -fL --progress-bar --retry 3 --connect-timeout 15 \
            --max-time 900 --speed-limit 1024 --speed-time 30 -o "$work/release.tar.gz" "$url" \
            || die "the download failed: $url"
        curl --proto '=https' --tlsv1.2 -fsSL --retry 3 --connect-timeout 15 --max-time 60 \
            -o "$work/release.sha256" "$url.sha256" || die "the checksum download failed"
        local expected actual
        expected=$(cut -d' ' -f1 "$work/release.sha256")
        actual=$(sha256sum "$work/release.tar.gz" | cut -d' ' -f1)
        [[ -n "$expected" && "${expected,,}" == "$actual" ]] \
            || die "the checksum does not match; nothing was installed"
        ok "downloaded $latest, checksum verified"

        local members=() wanted
        tar -tzf "$work/release.tar.gz" >"$work/members" || die "the release archive is damaged"
        for name in "${install[@]}"; do
            members+=("$archive/$name")
        done
        if grep -Fxq "$archive/config.toml.example" "$work/members"; then
            members+=("$archive/config.toml.example")
        fi
        for wanted in "${members[@]}"; do
            grep -Fxq "$wanted" "$work/members" || die "release $latest has no ${wanted#"$archive/"}"
        done
        tar -xzf "$work/release.tar.gz" -C "$work" --no-same-owner -- "${members[@]}" \
            || die "cannot unpack the release"

        mkdir -p -- "$bin_dir" "$share_dir"
        for name in "${install[@]}"; do
            # Renamed into place: a running service keeps the old binary until it restarts.
            install -m 0755 "$work/$archive/$name" "$bin_dir/.$name.new"
            mv -fT -- "$bin_dir/.$name.new" "$bin_dir/$name"
        done
        if [[ -f "$work/$archive/config.toml.example" ]]; then
            install -m 0644 "$work/$archive/config.toml.example" "$share_dir/config.toml.example"
        fi
        ok "installed ${install[*]} to $bin_dir"
    fi
    if (( ${#install[@]} )); then
        printf '%s\n' "${install[@]}" >"$share_dir/installed"
        printf '%s\n' "$latest" >"$share_dir/version"
    else
        rm -f -- "$share_dir/installed" "$share_dir/version"
    fi

    # --- Services ------------------------------------------------------------------------------
    local services=()
    for name in "${install[@]}"; do
        case "$name" in server|indexer|balancer) services+=("$name") ;; esac
    done
    local new_config=0 why
    if (( ${#services[@]} )) && [[ ! -f "$config" ]]; then
        install -d -m 0700 -o "$user" -g "$(id -gn "$user")" "$config_dir"
        install -m 0600 -o "$user" -g "$(id -gn "$user")" "$share_dir/config.toml.example" "$config"
        new_config=1
        ok "created $config from the example"
    fi
    for name in "${services[@]}"; do
        local unit="op-indexer-$name.service"
        cat >"$unit_dir/$unit" <<EOF
[Unit]
Description=op-p2p-indexer $name
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$user
WorkingDirectory=$config_dir
ExecStart=$bin_dir/$name --config $config
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=150
StandardOutput=append:$config_dir/$name.log
StandardError=append:$config_dir/$name.log
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
    done
    systemctl daemon-reload
    for name in "${services[@]}"; do
        local unit="op-indexer-$name.service"
        systemctl enable "$unit" >/dev/null 2>&1
        if (( new_config )); then
            ok "$name: service installed, not started (edit the configuration first)"
        elif ! why=$(runuser -u "$user" -- "$bin_dir/$name" --config "$config" --check-config 2>&1); then
            say "${yellow}!${reset} $name: service installed, not started: $config is not valid for it"
            say "  ${dim}${why%%$'\n'*}${reset}"
        elif systemctl restart "$unit"; then
            ok "$name: running ($unit)"
        else
            say "${yellow}!${reset} $name: failed to start; see: systemctl status $unit"
        fi
    done

    # --- Next steps ----------------------------------------------------------------------------
    say ""
    say "${bold}Done.${reset}"
    if (( ${#services[@]} )); then
        if (( new_config )); then
            say "  1. Edit $config (chain, keys, the [${services[0]}] section)"
            say "  2. sudo systemctl start ${services[*]/#/op-indexer-}"
        fi
        say "  Status:  systemctl status ${services[*]/#/op-indexer-}"
        say "  Logs:    tail -f $config_dir/${services[0]}.log"
    fi
    say "  Change what is installed, or update: run this installer again."
)

main "$@"
