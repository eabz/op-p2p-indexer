#!/usr/bin/env bash
# Installs and manages the op-p2p-indexer programs and their systemd services. Run as root:
#
#   sudo bash -c "$(curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh)"
#
# The first run asks for the programs and the chains (arrow keys, Space, Enter), installs the
# programs from the latest release (checksum verified) to /usr/local/bin and sets up each
# service once per chain, as the systemd unit <chain>-<program> (unichain-server), from its
# own directory:
#
#   ~/.op-indexer/config.toml             shared by every chain (keys): each chain extends it
#   ~/.op-indexer/<chain>/config.toml     the chain, and a section per program, its own ports
#   ~/.op-indexer/<chain>/data/<program>  the program's state
#   ~/.op-indexer/<chain>/<program>.log
#
# Each chain has its own block of ports (recorded in its file), so chains never collide. Once
# installed, a run offers: update the programs, add services, or remove services. Removing
# stops a service and deletes its unit only: its configuration and data stay where they are.
# Takes no options.

main() (
    set -euo pipefail
    umask 022

    local repo="https://github.com/eabz/op-p2p-indexer"
    local script_url="https://eabz.github.io/op-p2p-indexer/install.sh"
    local bin_dir="/usr/local/bin" share_dir="/usr/local/share/op-p2p-indexer"
    local unit_dir="/etc/systemd/system"
    # The first line of every unit the installer writes: it touches no other unit.
    local marker="# Managed by the op-p2p-indexer installer"
    local run_command="sudo bash -c \"\$(curl -fsSL $script_url)\""
    local programs=(server indexer balancer import bench)
    # Read through the checklists' name references.
    # shellcheck disable=SC2034
    local program_descs=(
        "Serves history from R2, behind a balancer"
        "Full node with its own local archive"
        "Directs clients to the healthy servers of a chain"
        "Imports a chain's history into R2 (run by hand)"
        "Benchmarks reads from a balancer (run by hand)"
    )
    local chains=(op unichain base)
    # shellcheck disable=SC2034
    local chain_descs=("OP Mainnet (chain 10)" "Unichain (chain 130)" "Base (chain 8453)")

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
    work=$(mktemp -d)

    # --- What is installed ---------------------------------------------------------------------
    local version="" name i
    if [[ -f "$share_dir/version" ]]; then version=$(<"$share_dir/version"); fi
    local installed=()
    for name in "${programs[@]}"; do
        if [[ -x "$bin_dir/$name" ]] && grep -Fxq "$name" "$share_dir/installed" 2>/dev/null; then
            installed+=("$name")
        fi
    done
    # Services set up: the units carrying the marker, as <chain>-<program>.
    local services=() file
    for file in "$unit_dir"/*.service; do
        if ! [[ -f "$file" ]] || ! head -n 1 "$file" | grep -Fxq "$marker"; then continue; fi
        file="${file##*/}"
        services+=("${file%.service}")
    done
    # Units of the installer's first versions (op-indexer-*): stopped and removed, their
    # configuration and data kept; add the services again to get the current names.
    for file in "$unit_dir"/op-indexer-*.service; do
        [[ -f "$file" ]] || continue
        file="${file##*/}"
        systemctl disable --now "$file" >/dev/null 2>&1 || true
        rm -f -- "$unit_dir/$file"
        warn "removed $file, a unit name of an older installer"
    done
    has() { # has WORD LIST...: whether LIST holds WORD
        local word=$1
        shift
        [[ " $* " == *" $word "* ]]
    }

    # --- Menus ---------------------------------------------------------------------------------
    read_key() { # one key press into `key`; arrows as their escape sequence
        IFS= read -rsn1 key <&3 || exit 1
        if [[ "$key" == $'\e' ]]; then
            local rest=""
            IFS= read -rsn2 -t 0.05 rest <&3 || true
            key="$key$rest"
        fi
    }
    quit() { printf '\e[?25h' >&3; say ""; say "Nothing changed."; exit 0; }
    # checklist TITLE NAMES DESCS STATUS ON MIN: ↑/↓ move, Space ticks, a ticks all, Enter
    # confirms once at least MIN are ticked, q or Esc quits. ON (0/1 per line) is updated.
    # STATUS is shown after a line: "label" always, or "label|label when ticked".
    checklist() {
        local title=$1 min=$6
        local -n names=$2 descs=$3 statuses=$4 on=$5
        local count=${#names[@]} cursor=0 key j all ticked
        local hint="↑/↓ move · Space select · a all · Enter confirm · q quit"
        draw() {
            local pointer box status label
            for ((j = 0; j < count; j++)); do
                pointer="  " status="${statuses[j]}"
                if (( j == cursor )); then pointer="${cyan}❯${reset} "; fi
                if (( on[j] )); then
                    box="${green}◉${reset}"
                    if [[ "$status" == *"|"* ]]; then status="${green}${status#*|}${reset}"; fi
                else
                    box="${dim}◯${reset}"
                    status="${dim}${status%%|*}${reset}"
                fi
                label=$(printf '%-18s' "${names[j]}")
                if (( j == cursor )); then label="${bold}${label}${reset}"; fi
                printf '\r\e[2K%s%s %s %-48s %s\n' "$pointer" "$box" "$label" "${descs[j]}" "$status" >&3
            done
            printf '\r\e[2K\n\r\e[2K%s  %s%s\n' "$dim" "$1" "$reset" >&3
        }
        printf '\n  %s%s%s\n\n' "$bold" "$title" "$reset" >&3
        printf '\e[?25l' >&3
        draw "$hint"
        while :; do
            read_key
            local message=$hint
            case "$key" in
                $'\e[A'|k) cursor=$(( (cursor + count - 1) % count )) ;;
                $'\e[B'|j) cursor=$(( (cursor + 1) % count )) ;;
                ' ') on[cursor]=$(( 1 - on[cursor] )) ;;
                a)
                    all=1
                    for ((j = 0; j < count; j++)); do all=$(( all & on[j] )); done
                    for ((j = 0; j < count; j++)); do on[j]=$(( 1 - all )); done ;;
                '')
                    ticked=0
                    for ((j = 0; j < count; j++)); do ticked=$(( ticked + on[j] )); done
                    if (( ticked >= min )); then break; fi
                    message="${reset}${yellow}Select at least one: Space ticks the line under ❯" ;;
                q|$'\e') quit ;;
                *) continue ;;
            esac
            printf '\e[%dA' $((count + 2)) >&3
            draw "$message"
        done
        printf '\e[%dA' $((count + 2)) >&3
        draw "$hint"
        printf '\e[?25h' >&3
    }
    # choose TITLE OPTION...: ↑/↓ move, Enter picks; the option's index goes into `chosen`.
    choose() {
        local title=$1 key j
        shift
        local options=("$@") cursor=0
        draw_options() {
            for j in "${!options[@]}"; do
                if (( j == cursor )); then
                    printf '\r\e[2K%s❯ %s%s\n' "$cyan$bold" "${options[j]}" "$reset" >&3
                else
                    printf '\r\e[2K  %s\n' "${options[j]}" >&3
                fi
            done
            printf '\r\e[2K\n\r\e[2K%s  ↑/↓ move · Enter select · q quit%s\n' "$dim" "$reset" >&3
        }
        printf '\n  %s%s%s\n\n' "$bold" "$title" "$reset" >&3
        printf '\e[?25l' >&3
        draw_options
        while :; do
            read_key
            case "$key" in
                $'\e[A'|k) cursor=$(( (cursor + ${#options[@]} - 1) % ${#options[@]} )) ;;
                $'\e[B'|j) cursor=$(( (cursor + 1) % ${#options[@]} )) ;;
                '') break ;;
                q|$'\e') quit ;;
                *) continue ;;
            esac
            printf '\e[%dA' $(( ${#options[@]} + 2 )) >&3
            draw_options
        done
        printf '\e[?25h' >&3
        chosen=$cursor
    }
    # confirm: Yes/No (←/→, Enter); returns whether Yes.
    confirm() {
        local yes=1 key
        line() {
            local y="  Yes  " n="  No  "
            if (( yes )); then y="${cyan}${bold}❯ Yes${reset}  "; else n="${cyan}${bold}❯ No${reset} "; fi
            printf '\r\e[2K\n\r\e[2K  Continue?  %s %s  %s(←/→, Enter)%s' "$y" "$n" "$dim" "$reset" >&3
        }
        printf '\e[?25l' >&3
        line
        while :; do
            read_key
            case "$key" in
                $'\e[C'|$'\e[D'|$'\t'|h|l) yes=$(( 1 - yes )) ;;
                y|Y) yes=1; break ;;
                n|N|q|$'\e') yes=0; break ;;
                '') break ;;
                *) continue ;;
            esac
            printf '\e[1A' >&3
            line
        done
        printf '\e[1A' >&3
        line
        printf '\e[?25h\n\n' >&3
        (( yes )) || { say "Nothing changed."; exit 0; }
    }
    plan() { printf '  %s%-18s%s %s\n' "$1" "$2" "$reset" "$3" >&3; }

    # --- Steps ---------------------------------------------------------------------------------
    latest_release() {
        # shellcheck disable=SC2016 # expanded by the inner bash
        spin "Finding the latest release" bash -c 'curl --proto =https --tlsv1.2 -fsSL \
            --connect-timeout 15 --max-time 60 -o /dev/null -w "%{url_effective}" "$1" >"$2"' \
            _ "$repo/releases/latest" "$work/latest"
        latest=$(<"$work/latest")
        latest="${latest##*/}"
        [[ "$latest" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "unexpected latest release: $latest"
    }
    # fetch VERSION PROGRAM...: downloads release VERSION, checks it, installs the programs.
    fetch() {
        local release=$1 archive url expected actual members=() wanted
        shift
        archive="op-p2p-indexer-$release-x86_64-linux"
        url="$repo/releases/download/$release/$archive.tar.gz"
        say "${dim}Downloading $release${reset}"
        curl --proto '=https' --tlsv1.2 -fL --progress-bar --retry 3 --connect-timeout 15 \
            --max-time 900 --speed-limit 1024 --speed-time 30 -o "$work/release.tar.gz" "$url" \
            || die "the download failed: $url"
        curl --proto '=https' --tlsv1.2 -fsSL --retry 3 --connect-timeout 15 --max-time 60 \
            -o "$work/release.sha256" "$url.sha256" || die "the checksum download failed"
        expected=$(cut -d' ' -f1 "$work/release.sha256")
        actual=$(sha256sum "$work/release.tar.gz" | cut -d' ' -f1)
        [[ -n "$expected" && "${expected,,}" == "$actual" ]] \
            || die "the checksum does not match; nothing was installed"
        ok "downloaded $release, checksum verified"
        tar -tzf "$work/release.tar.gz" >"$work/members" || die "the release archive is damaged"
        for name in "$@"; do members+=("$archive/$name"); done
        for wanted in "${members[@]}"; do
            grep -Fxq "$wanted" "$work/members" || die "release $release has no ${wanted#"$archive/"}"
        done
        tar -xzf "$work/release.tar.gz" -C "$work" --no-same-owner -- "${members[@]}" \
            || die "cannot unpack the release"
        mkdir -p -- "$bin_dir" "$share_dir"
        for name in "$@"; do
            # Renamed into place: a running service keeps the old binary until it restarts.
            install -m 0755 "$work/$archive/$name" "$bin_dir/.$name.new"
            mv -fT -- "$bin_dir/.$name.new" "$bin_dir/$name"
        done
        printf '%s\n' "$release" >"$share_dir/version"
        ok "installed $* ($release)"
    }
    record_installed() { mkdir -p -- "$share_dir"; printf '%s\n' "${installed[@]}" >"$share_dir/installed"; }
    # start SERVICE...: starts (or restarts) each service whose configuration checks; says why
    # not for the others.
    start() {
        local service chain program why
        for service in "$@"; do
            chain="${service%%-*}" program="${service#*-}"
            if ! why=$(runuser -u "$user" -- "$bin_dir/$program" \
                --config "$root_dir/$chain/config.toml" --check-config 2>&1); then
                why=$(grep -m 1 'Error' <<<"$why" || printf '%s' "${why%%$'\n'*}")
                warn "$service: not started, its configuration does not check: ${dim}$why${reset}"
                waiting+=("$service")
            elif systemctl restart "$service.service"; then
                ok "$service running"
            else
                warn "$service failed to start: systemctl status $service"
            fi
        done
    }
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
# balancer_url = "http://balancer.example:$(port 50060 "$slot")"
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
    # scaffold CHAIN PROGRAM: the chain's directory and file, and the program's section in it.
    # Sets `wrote` to 1 if the section was written now (the program is then not started until
    # its configuration is edited).
    scaffold() {
        local chain=$1 program=$2 dir="$root_dir/$1" config="$root_dir/$1/config.toml"
        local mine=(install -o "$user" -g "$group") table=$2 slot used=" " other
        wrote=0
        if [[ $program == import ]]; then table=importer; fi
        if [[ ! -f "$root_dir/config.toml" ]]; then
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
        "${mine[@]}" -d -m 0700 "$dir"
        if [[ ! -f "$config" ]]; then
            # The lowest slot no other chain directory has.
            for other in "$root_dir"/*/config.toml; do
                [[ -f "$other" ]] || continue
                used+="$(sed -n 's/^# installer port slot: \([0-9]*\)$/\1/p' "$other" | head -n 1) "
            done
            slot=0
            while [[ "$used" == *" $slot "* ]]; do slot=$(( slot + 1 )); done
            "${mine[@]}" -m 0600 /dev/stdin "$config" <<EOF
# $chain: set up by the installer. Shared settings (keys, R2) are in ../config.toml.
# installer port slot: $slot
extends = "../config.toml"
chain = "$chain"
EOF
            ok "created $config"
        fi
        if grep -q "^\[$table\]" "$config"; then return 0; fi
        slot=$(sed -n 's/^# installer port slot: \([0-9]*\)$/\1/p' "$config" | head -n 1)
        if [[ -z "$slot" ]]; then
            warn "$config has no installer port slot: add a [$table] section yourself"
            return 0
        fi
        section "$program" "$slot" >>"$config"
        ok "added [$table] to $config"
        wrote=1
    }
    unit() { # unit CHAIN PROGRAM: writes the service's systemd unit
        local chain=$1 program=$2
        cat >"$unit_dir/$chain-$program.service" <<EOF
$marker
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
    }

    # --- Header and action ---------------------------------------------------------------------
    local latest="" chosen=0 waiting=()
    printf '\n%s◆ op-p2p-indexer installer%s\n' "$bold" "$reset" >&3
    if [[ -n "${SUDO_USER:-}" && ! -t 0 ]]; then
        printf '%s  Keys not working? Run it this way: %s%s\n' "$yellow" "$run_command" "$reset" >&3
    fi
    local action=add
    if [[ -n "$version" ]] && (( ${#installed[@]} )); then
        printf '%s  installed %s: %s%s\n' "$dim" "$version" "${installed[*]}" "$reset" >&3
        if (( ${#services[@]} )); then
            printf '%s  services: %s%s\n' "$dim" "${services[*]}" "$reset" >&3
        fi
        choose "What do you want to do?" "Update the programs" "Add services" "Remove services"
        local actions=(update add remove)
        action=${actions[chosen]}
    fi

    case "$action" in
    # --- Update --------------------------------------------------------------------------------
    update)
        latest_release
        printf '\n' >&3
        plan "$green" "Update" "${installed[*]} ($version → $latest)"
        if (( ${#services[@]} )); then plan "$green" "Then restart" "${services[*]}"; fi
        confirm
        fetch "$latest" "${installed[@]}"
        start "${services[@]}"
        ;;

    # --- Add -----------------------------------------------------------------------------------
    add)
        # Programs: what is installed is shown as such; ticking it adds it on more chains.
        local add_statuses=() add_picked=()
        for name in "${programs[@]}"; do
            if has "$name" "${installed[@]}"; then add_statuses+=("installed|add on chains")
            else add_statuses+=("|add"); fi
            add_picked+=(0)
        done
        checklist "Which programs?" programs program_descs add_statuses add_picked 1
        local add_programs=() per_chain=0
        for i in "${!programs[@]}"; do
            if (( add_picked[i] )); then
                add_programs+=("${programs[i]}")
                [[ ${programs[i]} == bench ]] || per_chain=1
            fi
        done
        # Chains, if a program runs per chain (all but bench).
        local add_chains=()
        if (( per_chain )); then
            add_statuses=() add_picked=()
            for name in "${chains[@]}"; do
                if [[ -f "$root_dir/$name/config.toml" ]]; then add_statuses+=("set up|add")
                else add_statuses+=("|set up"); fi
                add_picked+=(0)
            done
            checklist "On which chains?" chains chain_descs add_statuses add_picked 1
            for i in "${!chains[@]}"; do
                if (( add_picked[i] )); then add_chains+=("${chains[i]}"); fi
            done
        fi
        # The plan: programs to install, services to set up, import sections to add.
        local new_programs=() new_services=() import_chains=() program chain
        for program in "${add_programs[@]}"; do
            has "$program" "${installed[@]}" || new_programs+=("$program")
            for chain in "${add_chains[@]}"; do
                case "$program" in
                    server|indexer|balancer)
                        has "$chain-$program" "${services[@]}" || new_services+=("$chain-$program") ;;
                    import)
                        grep -qs '^\[importer\]' "$root_dir/$chain/config.toml" \
                            || import_chains+=("$chain") ;;
                esac
            done
        done
        if (( ${#new_programs[@]} + ${#new_services[@]} + ${#import_chains[@]} == 0 )); then
            say ""; say "All of that is set up already; nothing changed."; exit 0
        fi
        printf '\n' >&3
        if (( ${#new_programs[@]} )); then plan "$green" "Install" "${new_programs[*]}"; fi
        if (( ${#new_services[@]} )); then
            plan "$green" "Set up services" "${new_services[*]} ($root_dir/<chain>)"
        fi
        if (( ${#import_chains[@]} )); then plan "$green" "Set up import on" "${import_chains[*]}"; fi
        confirm
        if (( ${#new_programs[@]} )); then
            # The installed release, so every program stays on one version.
            if [[ -z "$version" ]]; then latest_release; version=$latest; fi
            fetch "$version" "${new_programs[@]}"
            installed+=("${new_programs[@]}")
            record_installed
        fi
        local wrote=0 fresh=()
        for chain in "${import_chains[@]}"; do scaffold "$chain" import; done
        for service in "${new_services[@]}"; do
            chain="${service%%-*}" program="${service#*-}"
            scaffold "$chain" "$program"
            if (( wrote )); then fresh+=("$service"); fi
            unit "$chain" "$program"
        done
        if (( ${#new_services[@]} )); then
            systemctl daemon-reload
            for service in "${new_services[@]}"; do
                systemctl enable "$service.service" >/dev/null 2>&1
                if has "$service" "${fresh[@]}"; then waiting+=("$service"); else start "$service"; fi
            done
        fi
        ;;

    # --- Remove --------------------------------------------------------------------------------
    remove)
        # The services, then the tools installed.
        local items=() item_descs=() item_statuses=() item_picked=() tool
        for service in "${services[@]}"; do
            item_descs+=("service · $(systemctl is-active "$service.service" 2>/dev/null || true)")
            items+=("$service") item_statuses+=("|remove") item_picked+=(0)
        done
        for tool in import bench; do
            if has "$tool" "${installed[@]}"; then
                items+=("$tool") item_descs+=("tool") item_statuses+=("|remove") item_picked+=(0)
            fi
        done
        (( ${#items[@]} )) || { say ""; say "Nothing to remove."; exit 0; }
        checklist "Which services to remove?" items item_descs item_statuses item_picked 1
        local gone=() program chain
        for i in "${!items[@]}"; do
            if (( item_picked[i] )); then gone+=("${items[i]}"); fi
        done
        printf '\n' >&3
        plan "$red" "Stop and remove" "${gone[*]}"
        printf '  %sConfiguration and data are kept.%s\n' "$dim" "$reset" >&3
        confirm
        local kept=()
        for service in "${gone[@]}"; do
            if has "$service" import bench; then
                rm -f -- "$bin_dir/$service"
                ok "removed $service"
                continue
            fi
            chain="${service%%-*}" program="${service#*-}"
            systemctl disable --now "$service.service" >/dev/null 2>&1 || true
            rm -f -- "$unit_dir/$service.service"
            ok "stopped and removed $service"
            kept+=("$service")
        done
        systemctl daemon-reload
        # A program no service uses any more, and the tools removed, are uninstalled.
        local still=()
        for service in "${services[@]}"; do has "$service" "${gone[@]}" || still+=("${service#*-}"); done
        local remaining=()
        for program in "${installed[@]}"; do
            if has "$program" import bench; then
                has "$program" "${gone[@]}" || remaining+=("$program")
            elif has "$program" "${still[@]}"; then
                remaining+=("$program")
            else
                rm -f -- "$bin_dir/$program"
                ok "removed $program (no service uses it)"
            fi
        done
        installed=("${remaining[@]}")
        record_installed
        if (( ${#kept[@]} )); then
            say ""
            say "${bold}Kept, delete them yourself if you no longer need them:${reset}"
            for service in "${kept[@]}"; do
                chain="${service%%-*}" program="${service#*-}"
                say "  $service"
                say "    configuration  $root_dir/$chain/config.toml ${dim}([$program] section)${reset}"
                say "    data           $root_dir/$chain/data/$program"
                say "    log            $root_dir/$chain/$program.log"
            done
        fi
        ;;
    esac

    # --- Next steps ----------------------------------------------------------------------------
    say ""
    say "${bold}Done.${reset}"
    if (( ${#waiting[@]} )); then
        say "  Not started yet: ${waiting[*]}"
        say "  1. Shared keys and R2: $root_dir/config.toml"
        say "  2. Each chain:         $root_dir/<chain>/config.toml"
        say "  3. sudo systemctl start ${waiting[*]}"
    fi
    if [[ "$action" != remove ]] && (( ${#services[@]} + ${#waiting[@]} )); then
        say "  Status:  systemctl status <chain>-<program>   (e.g. unichain-server)"
        say "  Logs:    tail -f $root_dir/<chain>/<program>.log"
    fi
    say "  Update, add or remove services: run this installer again."
)

main "$@"
