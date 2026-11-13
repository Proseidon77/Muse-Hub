#!/usr/bin/env python3
"""Shared helper: connect to ddPC's MuseHub and hand back a joined Hub.

Transports (chosen by MUSEHUB_TRANSPORT, default "proxy"):

- "proxy" (default since 2026-11-10): the client connects straight to
  http://100.116.194.77:8767 through the runtime's Tailscale proxy (the
  HTTPS_PROXY endpoint with its port changed to 3130, per the Tailscale
  connector doc). This sandbox has no tailnet interface and raw connects
  to tailnet addresses are denied, so the proxy is the only road in --
  but it carries plain TCP, so no SSH tunnel is needed on top of it.
  hub_client needs no changes for this: urllib and websockets both read
  proxy settings from the environment. ALL of http_proxy / https_proxy /
  all_proxy (both cases) must be pointed at the 3130 proxy first --
  setting only http_proxy is not enough, because the libraries may pick
  the all_proxy entry (the :3128 public-egress proxy, which cannot reach
  the tailnet and just closes the connection; found the hard way during
  the 2026-11-10 feasibility probes). The hub processes exist only to
  talk to the hub, so rewriting the proxy env inside open_hub is safe.
  There is no tunnel process to die, so the Hub needs no on_reconnect
  hook in this mode: a reconnect is just negotiate + CONNECT again.

- "ssh" (fallback): the pre-2026-11-10 path -- an ssh -L tunnel through
  the same proxy, managed as a restartable unit whose restart is the
  Hub's on_reconnect hook (defect found 2026-11-10: tunnel died, the
  waiter kept reconnecting to the dead local port for 10 minutes).
  Kept for instant rollback: MUSEHUB_TRANSPORT=ssh.
"""
import os, socket, subprocess, sys, time

sys.path.insert(0, os.path.expanduser("~/workspace/tools"))
import hub_client  # noqa: E402

HOME = os.path.expanduser("~")
HUB_BASE = "http://100.116.194.77:8767"


def _tailscale_proxy_url():
    raw = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if not raw:
        raise SystemExit("HTTPS_PROXY not set; cannot derive the Tailscale proxy")
    return raw.rsplit(":", 1)[0] + ":3130"


def _use_tailscale_proxy():
    url = _tailscale_proxy_url()
    for key in ("http_proxy", "https_proxy", "all_proxy",
                "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        os.environ[key] = url
    return url


class ProxyLink:
    """Stand-in for the tunnel handle in proxy mode: nothing to stop."""

    def __init__(self):
        self.proc = None

    def terminate(self):
        pass


def _start_tunnel(local_port):
    cmd = [
        "ssh", "-i", f"{HOME}/.ssh/id_ed25519",
        "-o", f"ProxyCommand=python3 {HOME}/workspace/tools/tsconnect.py %h %p",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "ExitOnForwardFailure=yes",
        "-N", "-L", f"{local_port}:100.116.194.77:8767",
        "proseidon@100.116.194.77",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(60):
        try:
            socket.create_connection(("127.0.0.1", local_port), 0.5).close()
            return proc
        except OSError:
            if proc.poll() is not None:
                raise SystemExit("ssh tunnel process exited early")
            time.sleep(0.3)
    proc.terminate()
    raise SystemExit("ssh tunnel did not come up")


class Tunnel:
    """Handle for the current ssh process; terminate() kills the live one."""

    def __init__(self, local_port):
        self.local_port = local_port
        self.proc = None

    def start(self):
        self.proc = _start_tunnel(self.local_port)
        return self.proc

    def restart(self):
        self.terminate()
        try:
            self.start()
        except SystemExit as e:
            # Inside the wait reconnect loop a failed restart must count as
            # one failed attempt (backoff), not kill the waiter.
            raise ConnectionError(f"tunnel restart failed: {e}") from e

    def terminate(self):
        if self.proc is not None:
            try:
                self.proc.terminate()
            except Exception:
                pass
            self.proc = None


def open_hub(agent_id, secret_file, local_port=18767):
    """Returns (hub, link). Caller must link.terminate() when done (a no-op
    in proxy mode). Initial connect+Join is retried up to 3 times: a Join
    can be rejected spuriously when a connection is attempted mid-restart
    of this VM. In ssh mode the hub's reconnect hook restarts the tunnel
    before every reconnect, making "tunnel + Join" the recovery unit;
    local_port only applies to that mode."""
    secret = open(os.path.expanduser(secret_file)).read().strip()
    transport = os.environ.get("MUSEHUB_TRANSPORT", "proxy").lower()
    last_exc = None
    if transport == "ssh":
        tunnel = Tunnel(local_port)
        for attempt in range(3):
            tunnel.start()
            try:
                hub = hub_client.Hub(f"http://127.0.0.1:{local_port}", agent_id, secret)
                hub.on_reconnect = tunnel.restart
                return hub, tunnel
            except BaseException as e:  # noqa: BLE001 - retry any connect/Join failure
                last_exc = e
                tunnel.terminate()
                time.sleep(3)
        raise SystemExit(f"could not join hub after 3 attempts: {last_exc}")
    _use_tailscale_proxy()
    for attempt in range(3):
        try:
            hub = hub_client.Hub(HUB_BASE, agent_id, secret)
            return hub, ProxyLink()
        except BaseException as e:  # noqa: BLE001 - retry any connect/Join failure
            last_exc = e
            time.sleep(3)
    raise SystemExit(f"could not join hub after 3 attempts: {last_exc}")
