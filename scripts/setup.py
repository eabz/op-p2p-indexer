#!/usr/bin/env python3
"""Configure chain roles and explicit systemd lifecycle operations."""
import argparse
import copy
import errno
import getpass
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import socket
import shlex
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        raise SystemExit("Install TOML support: sudo apt-get install python3-tomli")

ROLES = ("server", "indexer", "balancer", "importer")
CHAINS = ("op", "unichain", "base")
MARKER = "# Managed by op-p2p-indexer setup\n"
UNIT_DIR = Path("/etc/systemd/system")


def fail(message):
    raise SystemExit("setup: " + message)


def ask(prompt, default="", secret=False):
    with open("/dev/tty", "r+") as tty:
        label = prompt + (" [" + default + "]" if default and not secret else "") + ": "
        if secret:
            value = getpass.getpass(label, stream=tty)
        else:
            tty.write(label)
            tty.flush()
            value = tty.readline().strip()
        return value or default


def yes(prompt):
    return ask(prompt + " (y/N)").lower() in ("y", "yes")


def read_local(path):
    if path.is_symlink():
        fail("refusing symlink config " + str(path))
    if not path.exists():
        return {}
    with path.open("rb") as source:
        return tomllib.load(source)


def merge(base, overrides):
    result = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def read_config(path, local=None):
    """Resolve inheritance for checks without copying shared secrets into child files."""
    path_fields = {"indexer": ("data_dir", "log_file"),
                   "server": ("data_dir", "log_file", "chunks_dir"),
                   "balancer": ("log_file",), "importer": ("state_dir",), "bench": ("json",)}
    def load(current, supplied, seen):
        canonical = current.resolve()
        if canonical in seen:
            fail("configuration inheritance cycle")
        if len(seen) >= 8:
            fail("configuration inheritance exceeds eight files")
        values = copy.deepcopy(read_local(current) if supplied is None else supplied)
        parent = values.pop("extends", None)
        base = {}
        if parent is not None:
            if not isinstance(parent, str) or not parent:
                fail("extends must name a configuration file")
            inherited = current.parent / parent
            if not inherited.is_file():
                fail("inherited configuration file is missing")
            base = load(inherited, None, seen + [canonical])
        for role, fields in path_fields.items():
            for field in fields:
                value = values.get(role, {}).get(field)
                if isinstance(value, str) and value and not Path(value).is_absolute():
                    values[role][field] = str(current.parent / value)
        return merge(base, values)
    effective = load(path, local, [])
    common = effective.pop("node", {})
    if not isinstance(common, dict):
        fail("node must be a configuration table")
    def shared(node):
        for key, value in node.items():
            if key in ("data_dir", "log_file", "listen_addr", "beacon_listen_addr", "advertised_addr"):
                fail("node defaults cannot share state paths or listener/advertised addresses")
            if isinstance(value, dict):
                shared(value)
    shared(common)
    for role in ("server", "indexer"):
        if role in effective:
            effective[role] = merge(common, effective[role])
    return effective


def toml(data):
    lines = []
    def value(item):
        if isinstance(item, bool):
            return "true" if item else "false"
        if isinstance(item, (str, int, float)):
            return json.dumps(item, ensure_ascii=False)
        if isinstance(item, list):
            return "[" + ", ".join(value(v) for v in item) + "]"
        fail("unsupported TOML value; edit config manually")
    def table(items, path):
        if path:
            lines.extend(["", "[" + ".".join(path) + "]"])
        for key, item in items.items():
            if not re.fullmatch(r"[A-Za-z0-9_-]+", key):
                fail("unsupported TOML key")
            if not isinstance(item, dict):
                lines.append(key + " = " + value(item))
        for key, item in items.items():
            if isinstance(item, dict):
                table(item, path + [key])
    table(data, [])
    return "\n".join(lines) + "\n"


def atomic(path, text, mode, account=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        fail("refusing symlink destination " + str(path))
    fd, temporary = tempfile.mkstemp(prefix=".setup-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as out:
            os.fchmod(out.fileno(), mode)
            if os.geteuid() == 0 and account:
                os.fchown(out.fileno(), account.pw_uid, account.pw_gid)
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def ports(data):
    # Omitted fields still bind runtime defaults. Reserve them even when roles are stopped
    # or a capability is disabled, so enabling it later does not collide with another role.
    found = []
    for role in ("server", "indexer"):
        if role not in data:
            continue
        section = data[role]
        for group, key, default in (("stream", "listen_addr", 50051), ("p2p", "listen_addr", 9222),
                                    ("el", "listen_addr", 30303), ("l1", "listen_addr", 30304),
                                    ("l1", "beacon_listen_addr", 9001)):
            address = section.get(group, {}).get(key, "0.0.0.0:" + str(default))
            found.append(int(address.rsplit(":", 1)[1]))
    if "balancer" in data:
        found.append(int(data["balancer"].get("listen_addr", "0.0.0.0:50060").rsplit(":", 1)[1]))
    return found


def free(port):
    held = []
    try:
        for family in (socket.AF_INET, socket.AF_INET6):
            for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
                try:
                    sock = socket.socket(family, kind)
                    held.append(sock)
                    if family == socket.AF_INET6:
                        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                    sock.bind(("::" if family == socket.AF_INET6 else "0.0.0.0", port))
                except OSError as error:
                    if family == socket.AF_INET6 and error.errno in (errno.EAFNOSUPPORT, errno.EADDRNOTAVAIL, errno.EPROTONOSUPPORT):
                        continue
                    raise
        return True
    except OSError:
        return False
    finally:
        for sock in held:
            sock.close()


def allocate(reserved, base=None, role="server"):
    defaults = [50060] if role == "balancer" else [50051, 9222, 30303, 30304, 9001]
    candidates = ([list(range(base, base + len(defaults)))] if base is not None
                  else ([port + offset for port in defaults] for offset in range(0, 15000, 100)))
    for block in candidates:
        if min(block) < 1024 or max(block) > 65535:
            fail("ports must be 1024..65535")
        if not set(block).intersection(reserved) and all(free(port) for port in block):
            reserved.update(block)
            return block
    fail("port block occupied or reserved")


def quote(value, argument=False):
    value = str(value)
    if any(ord(c) < 32 for c in value):
        fail("control characters in service path")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return '"' + (escaped.replace("$", "$$") if argument else escaped) + '"'


def unit_name(role, chain):
    return role + "-" + chain + ".service"


def service(role, chain, user, directory, prefix):
    return (MARKER + "[Unit]\nDescription=OP indexer " + role + " (" + chain + ")\n"
            "After=network-online.target\nWants=network-online.target\nPartOf=indexer-chain-" + chain + ".target\n"
            "\n[Service]\nType=simple\nUser=" + user + "\nWorkingDirectory=" + quote(directory) + "\n"
            "ExecStart=" + quote(prefix / "bin" / role, argument=True) + " --config " + quote(directory / "config.toml", argument=True) + "\n"
            "Restart=on-failure\nRestartSec=5\nTimeoutStopSec=120\nUMask=0077\n"
            "\n[Install]\nWantedBy=multi-user.target\n")


def check_capability(prefix, role):
    stamp = prefix / "share/op-p2p-indexer/setup-binaries.sha256"
    if not stamp.is_file():
        fail("guided setup capability missing; install a supported release with install.sh")
    binary = "import" if role == "importer" else role
    expected = {}
    for line in stamp.read_text().splitlines():
        digest, name = line.split(None, 1)
        expected[name.lstrip("*")] = digest
    digest = hashlib.sha256()
    with (prefix / "bin" / binary).open("rb") as source:
        for part in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(part)
    if digest.hexdigest() != expected.get(binary):
        fail("binary changed since guided install; reinstall supported release before configuration")


def managed(path):
    if path.exists() and (path.is_symlink() or not path.read_text().startswith(MARKER)):
        fail("refusing unmanaged service " + str(path))


def systemctl(*args):
    if sys.platform != "linux" or os.geteuid() != 0 or not Path("/run/systemd/system").exists():
        fail("systemd registration/control requires root on a Linux systemd host")
    subprocess.run(["systemctl", *args], check=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--action", choices=("install", "add-service", "update", "restart", "remove"), default="install")
    p.add_argument("--non-interactive", action="store_true")
    p.add_argument("--roles", "--role", help="comma-separated server,indexer,balancer,importer")
    p.add_argument("--chain", choices=CHAINS)
    p.add_argument("--user")
    p.add_argument("--root", type=Path)
    p.add_argument("--prefix", type=Path, default=Path("/usr/local"))
    p.add_argument("--port-base", action="append", default=[], metavar="ROLE=PORT")
    p.add_argument("--set", action="append", default=[], metavar="SECTION.KEY=TOML_VALUE")
    for flag in ("register", "enable", "start", "restart"):
        p.add_argument("--" + flag, action="store_true")
    a = p.parse_args()
    interactive = not a.non_interactive
    username = a.user or os.environ.get("SUDO_USER") or getpass.getuser()
    if interactive:
        username = ask("Account owning data and running services", username)
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_-]*[$]?", username):
        fail("invalid account name")
    account = pwd.getpwnam(username)
    if os.geteuid() not in (0, account.pw_uid):
        fail("run as selected account or root")
    root = (a.root or Path(account.pw_dir) / "indexer").expanduser().absolute()
    prefix = a.prefix.absolute()
    chain = a.chain or (ask("Chain: op, unichain, base", "unichain") if interactive else None)
    if chain not in CHAINS:
        fail("--chain is required")
    raw_roles = a.roles or (ask("Roles, comma-separated: server,indexer,balancer,importer", "server") if interactive else "")
    roles = list(dict.fromkeys(role.strip() for role in raw_roles.split(",")))
    if any(role not in ROLES for role in roles):
        fail("--roles must name server,indexer,balancer,importer")
    directory = root / chain
    config = directory / "config.toml"
    if root.is_symlink() or directory.is_symlink():
        fail("root and chain directories must not be symlinks")
    data = read_local(config)
    if not config.exists() and (root / "config.toml").is_file():
        data["extends"] = "../config.toml"
    # Resolve extends before prompts/allocation; other overrides are applied after defaults.
    for item in a.set:
        key, separator, value = item.partition("=")
        if separator and (key == "extends" or key.startswith("node.") or key.startswith("r2.")):
            target = data
            pieces = key.split(".")
            for piece in pieces[:-1]:
                target = target.setdefault(piece, {})
            target[pieces[-1]] = tomllib.loads("value = " + value)["value"]
    effective = read_config(config, data)
    if effective.get("chain", chain) != chain:
        fail("config belongs to another chain")
    units = [unit_name(role, chain) for role in roles if role != "importer"]
    # A concrete role/chain unit is unique on this host. Never take another installation over.
    for role in ROLES:
        if role == "importer":
            continue
        installed = UNIT_DIR / unit_name(role, chain)
        if installed.exists():
            managed(installed)
            text = installed.read_text()
            if "User=" + username + "\n" not in text or "WorkingDirectory=" + quote(directory) + "\n" not in text:
                fail("chain service belongs to another user/root: " + installed.name)
    managed(UNIT_DIR / ("indexer-chain-" + chain + ".target"))
    if a.action in ("restart", "remove", "update"):
        for name in units:
            path = UNIT_DIR / name
            if a.action != "update" and not path.exists():
                fail("service not registered: " + name)
            managed(path)
        if a.action == "remove":
            if units:
                systemctl("disable", "--now", *units)
                for name in units:
                    (UNIT_DIR / name).unlink()
                target = UNIT_DIR / ("indexer-chain-" + chain + ".target")
                if target.exists():
                    managed(target)
                    lines = target.read_text().splitlines()
                    lines = ["Wants=" + " ".join(name for name in line[6:].split() if name not in units)
                             if line.startswith("Wants=") else line for line in lines]
                    atomic(target, "\n".join(lines) + "\n", 0o644)
                systemctl("daemon-reload")
            print("Registrations removed; config and data retained.")
        elif a.action == "restart" or a.restart or (interactive and units and yes("Restart selected services?")):
            if units:
                systemctl("restart", *units)
        print("Configuration and data preserved.")
        return
    reserved = set()
    other = set()
    saved_configs = set(root.glob("*/config.toml"))
    # Registered services may live under another user's/custom root and may be stopped.
    # Their private configs still reserve ports; ask for root rather than ignore unreadable ones.
    for installed in UNIT_DIR.glob("*.service"):
        if installed.is_symlink() or not installed.read_text().startswith(MARKER):
            continue
        for line in installed.read_text().splitlines():
            if line.startswith("WorkingDirectory="):
                values = shlex.split(line.partition("=")[2])
                if len(values) == 1:
                    saved_configs.add(Path(values[0].replace("%%", "%")) / "config.toml")
    for saved in saved_configs:
        saved_ports = ports(read_config(saved))
        reserved.update(saved_ports)
        if saved != config:
            other.update(saved_ports)
    reserved.update(ports(effective))
    original_ports = set(ports(read_config(config)))
    overrides = {}
    for item in a.port_base:
        role, equal, number = item.partition("=")
        if not equal or role not in roles:
            fail("--port-base requires selected ROLE=PORT")
        overrides[role] = int(number)
    data["chain"] = chain
    for role in roles:
        effective = read_config(config, data)
        if role in effective:
            if role in overrides:
                fail("existing role ports preserved; edit config explicitly to relocate")
            continue
        section = data.setdefault(role, {})
        inherited = read_config(config, data).get(role, {})
        if role == "importer":
            section["state_dir"] = "data/importer"
            continue
        requested = overrides.get(role)
        if interactive and requested is None:
            answer = ask(role + " custom base port (consecutive ports; blank uses standard ports +100 offsets)")
            requested = int(answer) if answer else None
        block = allocate(reserved, requested, role)
        base = block[0]
        if role == "balancer":
            section["listen_addr"] = "0.0.0.0:" + str(base)
            if interactive:
                section["server_keys"] = [ask("Balancer server registration key", secret=True)]
                section["api_keys"] = [ask("Balancer client API key", secret=True)]
        else:
            section["data_dir"] = "data/" + role
            if interactive:
                section["data_dir"] = ask(role + " data directory (existing path retains identity/history)", section["data_dir"])
            section["stream"] = {"listen_addr": "127.0.0.1:" + str(base)}
            section["p2p"] = {"listen_addr": "0.0.0.0:" + str(block[1])}
            section["el"] = {"listen_addr": "0.0.0.0:" + str(block[2])}
            if "enabled" not in inherited.get("el", {}) and "profile" not in inherited:
                section["el"]["enabled"] = True
            section["l1"] = {"listen_addr": "0.0.0.0:" + str(block[3]), "beacon_listen_addr": "0.0.0.0:" + str(block[4])}
            if "enabled" not in inherited.get("l1", {}) and "profile" not in inherited:
                section["l1"]["enabled"] = False
            if inherited.get("stream", {}).get("api_keys"):
                section["stream"]["listen_addr"] = "0.0.0.0:" + str(base)
            if interactive:
                if not inherited.get("stream", {}).get("api_keys"):
                    key = ask(role + " API key (blank keeps localhost API)", secret=True)
                    if key:
                        section["stream"].update(listen_addr="0.0.0.0:" + str(base), api_keys=[key])
                checkpoint = "" if inherited.get("l1", {}).get("checkpoint") else ask("L1 checkpoint (blank keeps configured L1 setting)")
                if checkpoint:
                    section["l1"].update(enabled=True, checkpoint=checkpoint)
                    section["el"]["sync"] = True
                if role == "server":
                    address = ask("Public Flight host:port (no scheme; listener port " + str(base) + "; blank skips registration)")
                    if address:
                        section.update(address=address, balancer_url=ask("Balancer URL"), balancer_server_key=ask("Registration key", secret=True))
                    section["export"] = yes("Make this server the single exporter for this chain?")
    if interactive and any(role in ("server", "balancer") for role in roles):
        inherited_r2 = read_config(config, data).get("r2", {})
        for key in ("account_id", "bucket", "access_key_id", "secret_access_key"):
            if not inherited_r2.get(key):
                data.setdefault("r2", {})[key] = ask("R2 " + key, secret="key" in key)
        if "prefix" not in inherited_r2:
            data.setdefault("r2", {})["prefix"] = "archive"
    for item in a.set:
        key, equal, value = item.partition("=")
        if not equal:
            fail("--set needs SECTION.KEY=TOML_VALUE")
        target = data
        parts = key.split(".")
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = tomllib.loads("value = " + value)["value"]
    if data["chain"] != chain:
        fail("--set cannot change chain")
    for role in ROLES:
        key = "state_dir" if role == "importer" else "data_dir"
        local_state = data.get(role, {}).get(key)
        if isinstance(local_state, str) and local_state.startswith("~/"):
            data[role][key] = str(Path(account.pw_dir) / local_state[2:])
    effective = read_config(config, data)
    final_ports = ports(effective)
    if len(final_ports) != len(set(final_ports)) or other.intersection(final_ports):
        fail("listen ports collide with this or another saved chain")
    if any(not free(port) for port in set(final_ports) - original_ports):
        fail("new listen port is occupied")
    server = effective.get("server", {})
    if server.get("address"):
        address = server["address"]
        parsed = urlsplit("//" + address)
        if not parsed.hostname or not parsed.port or parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.password:
            fail("server.address must be host:port without a scheme or path")
        listener = server.get("stream", {}).get("listen_addr", "127.0.0.1:50051")
        print("Server registration address: " + address + "; listener: " + listener)
    state_paths = []
    for role in ROLES:
        if role not in effective or role == "balancer":
            continue
        key = "state_dir" if role == "importer" else "data_dir"
        state = Path(effective[role].get(key, "data/" + role))
        state_paths.append((directory / state).resolve())
    if len(state_paths) != len(set(state_paths)):
        fail("each node/importer role needs a distinct state directory")
    content = toml(data)
    directory.mkdir(parents=True, exist_ok=True)
    if os.geteuid() == 0:
        for owned in (root, directory):
            os.chown(owned, account.pw_uid, account.pw_gid)
    fd, candidate = tempfile.mkstemp(prefix=".config-check-", suffix=".toml", dir=directory)
    try:
        with os.fdopen(fd, "w") as out:
            out.write(content)
        for role in ROLES:
            if role not in effective:
                continue
            check_capability(prefix, role)
            binary = prefix / "bin" / ("import" if role == "importer" else role)
            result = subprocess.run([str(binary), "--config", candidate, "--check-config"], capture_output=True, text=True)
            if result.returncode:
                fail(role + " config validation failed; check required settings (values withheld)")
        atomic(config, content, 0o600, account)
    finally:
        os.unlink(candidate)
    print("Saved " + str(config) + " (0600). Existing data retained.")
    register = a.register or a.enable or a.start or (interactive and units and yes("Register selected systemd services?"))
    if not register or not units:
        return
    systemctl("--version")
    target = UNIT_DIR / ("indexer-chain-" + chain + ".target")
    managed(target)
    for name in units:
        managed(UNIT_DIR / name)
    for role in roles:
        if role != "importer":
            atomic(UNIT_DIR / unit_name(role, chain), service(role, chain, username, directory, prefix), 0o644)
    members = [unit_name(role, chain) for role in ROLES if role != "importer"
               and (UNIT_DIR / unit_name(role, chain)).exists()
               and (UNIT_DIR / unit_name(role, chain)).read_text().startswith(MARKER)]
    atomic(target, MARKER + "[Unit]\nDescription=OP indexer chain " + chain + "\nWants=" + " ".join(members) + "\n\n[Install]\nWantedBy=multi-user.target\n", 0o644)
    systemctl("daemon-reload")
    if a.enable or (interactive and yes("Enable selected services at boot?")):
        systemctl("enable", *units)
    if a.start or (interactive and yes("Start selected services now?")):
        systemctl("start", *units)
    print("Registered " + ", ".join(units) + "; group " + target.name)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        fail(type(error).__name__ + ": check paths, permissions and configuration")
