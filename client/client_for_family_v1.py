#!/usr/bin/env python3
"""Minimal SignalR (JSON protocol) client for the MuseHub relay on ddPC.

Usage:
  python3 hub_client.py --url http://HOST:8767 --name family --secret XXX send "hello"
  python3 hub_client.py --url http://HOST:8767 --name family --secret XXX wait
      wait: stay connected; print each received message as one JSON line, then exit.
      (Re-run it to keep listening; add --stay to keep printing without exiting.)

Secret can also come from the HUB_SECRET environment variable.
Requires: websockets  (pip install websockets)
"""
import argparse, json, os, sys, time, urllib.request

RS = "\x1e"

def negotiate(base):
    req = urllib.request.Request(base + "/hub/negotiate?negotiateVersion=1",
                                 data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        d = json.load(r)
        return d.get("connectionToken") or d["connectionId"]

class Hub:
    def __init__(self, base, name, secret):
        from websockets.sync.client import connect
        cid = negotiate(base)
        ws_url = base.replace("http://", "ws://").replace("https://", "wss://") + "/hub?id=" + cid
        self.ws = connect(ws_url, open_timeout=20)
        self.buf = ""
        self.name, self.secret = name, secret
        self._send({"protocol": "json", "version": 1})
        self._wait_for(lambda m: m == {})  # handshake response
        res = self.invoke("Join", [name, secret])
        if res != "ok":
            raise SystemExit(f"Join failed: {res!r}")

    def _send(self, obj):
        self.ws.send(json.dumps(obj) + RS)

    def _frames(self, timeout):
        """Yield parsed frames arriving within timeout seconds (None on timeout)."""
        deadline = time.time() + timeout
        while True:
            while RS in self.buf:
                raw, self.buf = self.buf.split(RS, 1)
                if raw.strip():
                    yield json.loads(raw)
            left = deadline - time.time()
            if left <= 0:
                return
            try:
                chunk = self.ws.recv(timeout=min(left, 5))
            except TimeoutError:
                continue
            if isinstance(chunk, bytes):
                chunk = chunk.decode("utf-8", "replace")
            self.buf += chunk

    def _wait_for(self, pred, timeout=20):
        for m in self._frames(timeout):
            if pred(m):
                return m
        raise SystemExit("timed out waiting for hub response")

    def invoke(self, target, args):
        self._send({"type": 1, "invocationId": "1", "target": target, "arguments": args})
        def done(m):
            return m.get("type") == 3 and m.get("invocationId") == "1"
        m = self._wait_for(done)
        if "error" in m:
            raise SystemExit(f"{target} error: {m['error']}")
        return m.get("result")

    def send_message(self, text):
        return self.invoke("Send", [text])

    def wait_message(self, stay=False, max_seconds=6 * 3600):
        """Print received messages as JSON lines. Exit after first unless stay."""
        got = 0
        last_ping = 0.0
        end = time.time() + max_seconds
        while time.time() < end:
            for m in self._frames(5):
                if m.get("type") == 1 and m.get("target") == "Receive":
                    print(json.dumps(m["arguments"][0], ensure_ascii=False), flush=True)
                    got += 1
                    if not stay:
                        return 0
            if time.time() - last_ping > 10:
                self._send({"type": 6})
                last_ping = time.time()
        return 0 if got else 3

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://ddpc.tail8e8344.ts.net:8767")
    ap.add_argument("--name", required=True)
    ap.add_argument("--secret", default=os.environ.get("HUB_SECRET", ""))
    ap.add_argument("--stay", action="store_true")
    ap.add_argument("mode", choices=["send", "wait"])
    ap.add_argument("text", nargs="?", default="")
    a = ap.parse_args()
    if not a.secret:
        raise SystemExit("no secret: pass --secret or set HUB_SECRET")
    hub = Hub(a.url.rstrip("/"), a.name, a.secret)
    if a.mode == "send":
        print(json.dumps(hub.send_message(a.text), ensure_ascii=False))
        return 0
    return hub.wait_message(stay=a.stay)

if __name__ == "__main__":
    sys.exit(main())
