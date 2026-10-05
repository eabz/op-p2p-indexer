#!/usr/bin/env bash
# Installs, updates and removes the op-p2p-indexer programs, the chains they run, and their
# systemd services. Run as root:
#
#   sudo bash -c "$(curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh)"
#
# Two checklists (arrow keys, Space, Enter): the programs, then the chains. The latest release
# is downloaded, its checksum checked, and the programs installed to /usr/local/bin. Each
# service (server, indexer, balancer) runs once per chain, as op-indexer-<program>-<chain>,
# from its own directory:
#
#   ~/.op-indexer/config.toml            shared by every chain (keys): each chain's file extends it
#   ~/.op-indexer/<chain>/config.toml    the chain, and a section per program with its own ports
#   ~/.op-indexer/<chain>/data/<program> the program's state
#   ~/.op-indexer/<chain>/<program>.log
#
# Each chain has its own block of ports (recorded in its file), so chains never collide. Run
# it again to update, or to add or remove programs and chains: what is unticked is stopped and
# removed; configuration and data are always kept. Takes no options.

main() (
    set -euo pipefail
    umask 022

    local repo="https://github.com/eabz/op-p2p-indexer"
    local script_url="https://eabz.github.io/op-p2p-indexer/install.sh"
    local bin_dir="/usr/local/bin" share_dir="/usr/local/share/op-p2p-indexer"
    local unit_dir="/etc/systemd/system"
    local run_command="sudo bash -c \"\$(curl -fsSL $script_url)\""
    # The descriptions are read through the checklist's name references.
    local p_names=(server indexer balancer import bench)
    # shellcheck disable=SC2034
    local p_descs=(
        "Serves history from R2, behind a balancer"
        "Full node with its own local archive"
        "Directs clients to the healthy servers"
        "Imports a chain's history into R2 (run by hand)"
        "Benchmarks reads from a balancer (run by hand)"
    )
    local c_names=(op unichain base)
    # shellcheck disable=SC2034
    local c_descs=("OP Mainnet (chain 10)" "Unichain (chain 130)" "Base (chain 8453)")

    # --- Output --------------------------------------------------------------------------------
    local bold="" dim="" green="" cyan="" red="" yellow="" reset=""
    if [[ -t 2 ]]; then
        bold=$'\e[1m' dim=$'\e[2m' green=$'\e[32m' cyan=$'\e[36m' red=$'\e[31m'
        yellow=$'\e[33m' reset=$'\e[0m'
    fi
    say() { printf '%s\n' "$*" >&2; }
    ok() { printf '%s✔%s %s\n' "$green" "$reset" "$*" >&2; }
    warn() { printf '%s!%s %s\n' "$yellow" "$reset" "$*" >&2; }
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

    [[ $# -eq 0 ]] || die "install.sh takes no options; run it and choose in the menus"

    # --- Checks --------------------------------------------------------------------------------
    [[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || die "only Linux x86-64 is supported"
    local libc
    libc=$(getconf GNU_LIBC_VERSION 2>/dev/null) || die "glibc is required (Ubuntu 22.04 or newer)"
    if ! [[ "$libc" =~ ^glibc\ 2\.([0-9]+)$ ]] || (( BASH_REMATCH[1] < 35 )); then
        die "glibc 2.35 or newer is required (Ubuntu 22.04 or newer); found $libc"
    fi
    # Root writes /usr/local/bin and /etc/systemd/system, and runs systemctl and apt.
    (( EUID == 0 )) || die "run it as root: $run_command"
    command -v systemctl >/dev/null || die "systemd is required"
    { exec 3<>/dev/tty; } 2>/dev/null || die "the installer needs a terminal to show its menus"
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

    # The account that runs the services and owns ~/.op-indexer: whoever ran sudo, else root.
    local user="${SUDO_USER:-root}" home group
    home=$(getent passwd "$user" | cut -d: -f6)
    [[ -n "$home" && -d "$home" ]] || die "cannot find the home directory of $user"
    group=$(id -gn "$user")
    local root_dir="$home/.op-indexer"

    # --- What is installed, and the latest release ---------------------------------------------
    work=$(mktemp -d)
    local installed_version=""
    if [[ -f "$share_dir/version" ]]; then installed_version=$(<"$share_dir/version"); fi
    # shellcheck disable=SC2016 # expanded by the inner bash
    spin "Finding the latest release" bash -c 'curl --proto =https --tlsv1.2 -fsSL \
        --connect-timeout 15 --max-time 60 -o /dev/null -w "%{url_effective}" "$1" >"$2"' \
        _ "$repo/releases/latest" "$work/latest"
    local latest
    latest=$(<"$work/latest")
    latest="${latest##*/}"
    [[ "$latest" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "unexpected latest release: $latest"

    # Was installed (1) or not, for each program and chain.
    local p_had=() c_had=() name i
    for name in "${p_names[@]}"; do
        if [[ -x "$bin_dir/$name" ]] && grep -Fxq "$name" "$share_dir/installed" 2>/dev/null; then
            p_had+=(1)
        else
            p_had+=(0)
        fi
    done
    for name in "${c_names[@]}"; do
        if grep -Fxq "$name" "$share_dir/chains" 2>/dev/null; then c_had+=(1); else c_had+=(0); fi
    done
    local p_on=("${p_had[@]}") c_on=("${c_had[@]}")

    # --- Checklists ----------------------------------------------------------------------------
    # checklist TITLE NAMES DESCS HAD ON ADD KEEP DROP MIN: arrow keys move, Space ticks, `a`
    # ticks all, Enter confirms (once at least MIN are ticked), q or Esc quits. ON is updated.
    # ADD, KEEP and DROP say what happens to a line ticked now, ticked before and now, before
    # only.
    checklist() {
        local title=$1 add=$6 keep=$7 drop=$8 min=$9
        local -n names=$2 descs=$3 had=$4 on=$5
        local count=${#names[@]} cursor=0 key rest j all ticked
        local hint="↑/↓ move · Space select · a all · Enter confirm · q quit"
        draw_line() {
            local j=$1 pointer="  " box status="" label
            if (( j == cursor )); then pointer="${cyan}❯${reset} "; fi
            if (( on[j] )); then box="${green}◉${reset}"; else box="${dim}◯${reset}"; fi
            if (( had[j] && on[j] )); then status="${dim}${keep}${reset}"
            elif (( had[j] )); then status="${red}${drop}${reset}"
            elif (( on[j] )); then status="${green}${add}${reset}"
            fi
            label=$(printf '%-9s' "${names[j]}")
            if (( j == cursor )); then label="${bold}${label}${reset}"; fi
            printf '\r\e[2K%s%s %s %-48s %s\n' "$pointer" "$box" "$label" "${descs[j]}" "$status" >&3
        }
        draw() {
            for ((j = 0; j < count; j++)); do draw_line "$j"; done
            printf '\r\e[2K\n\r\e[2K%s  %s%s\n' "$dim" "$1" "$reset" >&3
        }
        printf '\n  %s%s%s\n\n' "$bold" "$title" "$reset" >&3
        printf '\e[?25l' >&3
        draw "$hint"
        while :; do
            IFS= read -rsn1 key <&3 || exit 1
            if [[ "$key" == $'\e' ]]; then
                rest=""
                IFS= read -rsn2 -t 0.05 rest <&3 || true
                key="$key$rest"
            fi
            local message=$hint
            case "$key" in
                $'\e[A'|k) cursor=$(( (cursor + count - 1) % count )) ;;
                $'\e[B'|j) cursor=$(( (cursor + 1) % count )) ;;
                ' ') on[cursor]=$(( 1 - on[cursor] )) ;;
                a)
                    all=1
                    for ((j = 0; j < count; j++)); do (( on[j] )) || all=0; done
                    for ((j = 0; j < count; j++)); do on[j]=$(( 1 - all )); done ;;
                '')
                    ticked=0
                    for ((j = 0; j < count; j++)); do ticked=$(( ticked + on[j] )); done
                    if (( ticked >= min )); then break; fi
                    message="${reset}${yellow}Select at least one: Space ticks the line under ❯" ;;
                q|$'\e') printf '\e[?25h' >&3; say ""; say "Nothing changed."; exit 0 ;;
                *) continue ;;
            esac
            printf '\e[%dA' $((count + 2)) >&3
            draw "$message"
        done
        printf '\e[%dA' $((count + 2)) >&3
        draw "$hint"
        printf '\e[?25h' >&3
    }

    printf '\n%s◆ op-p2p-indexer installer%s\n' "$bold" "$reset" >&3
    if [[ -n "$installed_version" ]]; then
        printf '%s  installed %s · latest %s%s\n' "$dim" "$installed_version" "$latest" "$reset" >&3
    else
        printf '%s  latest release %s%s\n' "$dim" "$latest" "$reset" >&3
    fi
    if [[ -n "${SUDO_USER:-}" && ! -t 0 ]]; then
        printf '\n%s  Keys not working? Run it this way: %s%s\n' "$yellow" "$run_command" "$reset" >&3
    fi
    checklist "Which programs run on this machine?" p_names p_descs p_had p_on \
        install "installed · update" remove 0

    # The chains, if a program runs per chain (all but bench).
    local per_chain=0
    for ((i = 0; i < 4; i++)); do per_chain=$(( per_chain | p_on[i] )); done
    if (( per_chain )); then
        checklist "Which chains?" c_names c_descs c_had c_on "set up" "kept" remove 1
    else
        for i in "${!c_names[@]}"; do c_on[i]=0; done
    fi

    # --- The plan ------------------------------------------------------------------------------
    local install=() remove=() chains=() dropped=()
    for i in "${!p_names[@]}"; do
        if (( p_on[i] )); then install+=("${p_names[i]}")
        elif (( p_had[i] )); then remove+=("${p_names[i]}")
        fi
    done
    for i in "${!c_names[@]}"; do
        if (( c_on[i] )); then chains+=("${c_names[i]}")
        elif (( c_had[i] )); then dropped+=("${c_names[i]}")
        fi
    done
    # Services wanted: each service program on each chain.
    local services=() program chain
    for program in server indexer balancer; do
        [[ " ${install[*]} " == *" $program "* ]] || continue
        for chain in "${chains[@]}"; do services+=("$program-$chain"); done
    done
    # Units to stop and remove: ours that are not wanted any more.
    local stale=() unit
    for unit in "$unit_dir"/op-indexer-*.service; do
        [[ -f "$unit" ]] || continue
        unit="${unit##*/op-indexer-}"
        unit="${unit%.service}"
        [[ " ${services[*]} " == *" $unit "* ]] || stale+=("$unit")
    done
    if (( ${#install[@]} == 0 && ${#remove[@]} == 0 && ${#stale[@]} == 0 )); then
        say ""; say "Nothing selected; nothing changed."; exit 0
    fi

    printf '\n' >&3
    if (( ${#install[@]} )); then
        printf '  %sInstall or update%s  %s %s(%s)%s\n' "$green" "$reset" "${install[*]}" "$dim" "$latest" "$reset" >&3
    fi
    if (( ${#remove[@]} )); then
        printf '  %sRemove%s             %s\n' "$red" "$reset" "${remove[*]}" >&3
    fi
    if (( ${#chains[@]} )); then
        printf '  %sChains%s             %s %s(%s/<chain>)%s\n' "$green" "$reset" "${chains[*]}" "$dim" "$root_dir" "$reset" >&3
    fi
    if (( ${#services[@]} )); then
        printf '  %sServices%s           %s\n' "$green" "$reset" "${services[*]/#/op-indexer-}" >&3
    fi
    if (( ${#stale[@]} )); then
        printf '  %sStop and remove%s    %s %s(configuration and data are kept)%s\n' "$red" "$reset" "${stale[*]/#/op-indexer-}" "$dim" "$reset" >&3
    fi

    local yes=1 key rest
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

    # --- Stop and remove -----------------------------------------------------------------------
    for unit in "${stale[@]}"; do
        systemctl disable --now "op-indexer-$unit.service" >/dev/null 2>&1 || true
        rm -f -- "$unit_dir/op-indexer-$unit.service"
        ok "stopped and removed op-indexer-$unit"
    done
    for name in "${remove[@]}"; do
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
        for wanted in "${members[@]}"; do
            grep -Fxq "$wanted" "$work/members" || die "release $latest has no ${wanted#"$archive/"}"
        done
        tar -xzf "$work/release.tar.gz" -C "$work" --no-same-owner -- "${members[@]}" \
            || die "cannot unpack the release"
        mkdir -p -- "$bin_dir"
        for name in "${install[@]}"; do
            # Renamed into place: a running service keeps the old binary until it restarts.
            install -m 0755 "$work/$archive/$name" "$bin_dir/.$name.new"
            mv -fT -- "$bin_dir/.$name.new" "$bin_dir/$name"
        done
        ok "installed ${install[*]} to $bin_dir"
    fi
    mkdir -p -- "$share_dir"
    printf '%s\n' "${install[@]}" >"$share_dir/installed"
    printf '%s\n' "${chains[@]}" >"$share_dir/chains"
    printf '%s\n' "$latest" >"$share_dir/version"

    # --- Configuration -------------------------------------------------------------------------
    # Ports of slot `slot`: the defaults, plus 1000 per slot; the indexer's 100 above the
    # server's. One slot per chain directory, recorded in its file.
    port() { printf '%d' $(( $1 + $2 * 1000 + ${3:-0} )); }
    section() { # section PROGRAM SLOT: the program's tables for the chain file
        local slot=$2
        case "$1" in
            server) cat <<EOF

[server]
data_dir = "data/server"
# id = "server-1"                       # this server's name (default: the host name)
# address = "203.0.113.7:$(port 50051 "$slot")"         # where the balancer sends clients
# balancer_url = "http://balancer.example:50060"
# balancer_server_key = "replace-me"
export = false                          # true on the one server that exports chunks to R2

[server.stream]
listen_addr = "0.0.0.0:$(port 50051 "$slot")"

[server.p2p]
listen_addr = "0.0.0.0:$(port 9222 "$slot")"

[server.el]
sync = true
listen_addr = "0.0.0.0:$(port 30303 "$slot")"

[server.l1]
# enabled = true
# checkpoint = "0x..."                  # a trusted recent finalized beacon block root
listen_addr = "0.0.0.0:$(port 30304 "$slot")"
beacon_listen_addr = "0.0.0.0:$(port 9001 "$slot")"
EOF
                ;;
            indexer) cat <<EOF

[indexer]
data_dir = "data/indexer"

[indexer.stream]
listen_addr = "127.0.0.1:$(port 50051 "$slot" 100)"

[indexer.p2p]
listen_addr = "0.0.0.0:$(port 9222 "$slot" 100)"

[indexer.el]
listen_addr = "0.0.0.0:$(port 30303 "$slot" 100)"

[indexer.l1]
listen_addr = "0.0.0.0:$(port 30304 "$slot" 100)"
beacon_listen_addr = "0.0.0.0:$(port 9001 "$slot" 100)"
EOF
                ;;
            balancer) cat <<EOF

[balancer]
listen_addr = "0.0.0.0:$(port 50060 "$slot")"
# server_keys = ["replace-me"]          # keys servers register with
EOF
                ;;
            import) cat <<EOF

[importer]
state_dir = "data/importer"
EOF
                ;;
        esac
    }
    local mine=(install -o "$user" -g "$group")
    if (( ${#chains[@]} )) && [[ ! -f "$root_dir/config.toml" ]]; then
        "${mine[@]}" -d -m 0700 "$root_dir"
        "${mine[@]}" -m 0600 /dev/stdin "$root_dir/config.toml" <<'EOF'
# Shared by every chain under this directory: each <chain>/config.toml extends this file and
# overrides it. Put what is the same for every chain here, once: keys, R2.
log_filter = "info"

[node.stream]
# api_keys = ["replace-me"]             # clients send `authorization: Bearer <key>`

[r2]
# account_id = "replace-me"
# access_key_id = "replace-me"
# secret_access_key = "replace-me"
EOF
        ok "created $root_dir/config.toml (shared settings)"
    fi
    # Slots taken by the chain directories already set up.
    local used=" " file slot
    for file in "$root_dir"/*/config.toml; do
        [[ -f "$file" ]] || continue
        slot=$(sed -n 's/^# installer port slot: \([0-9]*\)$/\1/p' "$file" | head -n 1)
        if [[ -n "$slot" ]]; then used+="$slot "; fi
    done
    # Programs whose configuration was written now: not started until it is edited.
    local fresh=" "
    for chain in "${chains[@]}"; do
        local dir="$root_dir/$chain" config="$root_dir/$chain/config.toml"
        "${mine[@]}" -d -m 0700 "$dir"
        if [[ ! -f "$config" ]]; then
            slot=0
            while [[ "$used" == *" $slot "* ]]; do slot=$(( slot + 1 )); done
            used+="$slot "
            "${mine[@]}" -m 0600 /dev/stdin "$config" <<EOF
# $chain: set up by the installer. Shared settings (keys, R2) are in ../config.toml.
# installer port slot: $slot
extends = "../config.toml"
chain = "$chain"
EOF
            ok "created $config"
        fi
        slot=$(sed -n 's/^# installer port slot: \([0-9]*\)$/\1/p' "$config" | head -n 1)
        for program in server indexer balancer import; do
            [[ " ${install[*]} " == *" $program "* ]] || continue
            local table=$program
            [[ $program == import ]] && table=importer
            grep -q "^\[$table\]" "$config" && continue
            if [[ -z "$slot" ]]; then
                warn "$config has no installer port slot: add a [$table] section yourself"
                continue
            fi
            section "$program" "$slot" >>"$config"
            fresh+="$program-$chain "
            ok "added [$table] to $config"
        done
    done

    # --- Services ------------------------------------------------------------------------------
    for unit in "${services[@]}"; do
        program="${unit%%-*}"
        chain="${unit#*-}"
        cat >"$unit_dir/op-indexer-$unit.service" <<EOF
[Unit]
Description=op-p2p-indexer $program ($chain)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$user
WorkingDirectory=$root_dir/$chain
ExecStart=$bin_dir/$program --config $root_dir/$chain/config.toml
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=150
StandardOutput=append:$root_dir/$chain/$program.log
StandardError=append:$root_dir/$chain/$program.log
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
    done
    if (( ${#services[@]} || ${#stale[@]} )); then
        systemctl daemon-reload
    fi
    local waiting=() why
    for unit in "${services[@]}"; do
        program="${unit%%-*}"
        chain="${unit#*-}"
        systemctl enable "op-indexer-$unit.service" >/dev/null 2>&1
        if [[ "$fresh" == *" $unit "* ]]; then
            waiting+=("$unit")
        elif ! why=$(runuser -u "$user" -- "$bin_dir/$program" \
            --config "$root_dir/$chain/config.toml" --check-config 2>&1); then
            warn "op-indexer-$unit: not started, its configuration does not check:"
            why=$(grep -m 1 'Error' <<<"$why" || printf '%s' "${why%%$'\n'*}")
            say "  ${dim}${why}${reset}"
            waiting+=("$unit")
        elif systemctl restart "op-indexer-$unit.service"; then
            ok "op-indexer-$unit running"
        else
            warn "op-indexer-$unit failed to start: systemctl status op-indexer-$unit"
        fi
    done

    # --- Next steps ----------------------------------------------------------------------------
    say ""
    say "${bold}Done.${reset}"
    if (( ${#waiting[@]} )); then
        say "  Not started yet: ${waiting[*]/#/op-indexer-}"
        say "  1. Shared keys and R2: $root_dir/config.toml"
        say "  2. Each chain:         $root_dir/<chain>/config.toml"
        say "  3. sudo systemctl start ${waiting[*]/#/op-indexer-}"
    fi
    if (( ${#services[@]} )); then
        say "  Status:  systemctl status 'op-indexer-*'"
        say "  Logs:    tail -f $root_dir/<chain>/<program>.log"
    fi
    say "  Change programs or chains, or update: run this installer again."
)

main "$@"
