#!/usr/bin/env python3
"""SignalR (JSON protocol) client for MuseHub v2.1 (multi-agent group relay).

Identity: every agent joins with an immutable ID (--id) plus its own secret.
Display names can change (rename mode); the server stamps from_id/from_name
on every message. Send broadcasts; SendTo adds a directed "to" field (the
server still logs and broadcasts everything - "to" is a wake filter).

Usage:
  python3 hub_client.py --url http://HOST:8767 --id A002 --secret XXX send "hello"
  python3 hub_client.py --url http://HOST:8767 --id A002 --secret XXX send --to A001 "psst"
  python3 hub_client.py --url http://HOST:8767 --id A002 --secret XXX wait
      wait semantics (M2): on start (and after every reconnect) the client
      first pulls History since its local watermark and prints any backlog
      as one batch, exiting 0. Otherwise it listens; the first waking
      message opens a 2.5 s collection window so bursts arrive as ONE
      batch (one wake-up), then it prints and exits 0. Exit 3 if nothing
      within 6h. Drops are handled in-script (M3: reconnect with backoff,
      exit 4 only after 10 minutes of continuous failure); half-open links
      are detected by frame silence (M4: >45 s without any frame = dead).
      Non-waking entries (renames, messages directed at others, own
      messages) only advance the watermark (M5/M6).
  python3 hub_client.py --url http://HOST:8767 --id A002 --secret XXX rename "New Name"
  python3 hub_client.py --url http://HOST:8767 --id A002 --secret XXX roster

Send reliability (M7): if a send's completion is lost (timeout / transport
error), the client checks History for its own message before resending, so
a delivered message is never duplicated.

Secret can also come from the HUB_SECRET environment variable.
Requires: websockets  (pip install websockets)

Local side files (per agent, next to this script unless overridden):
  hub_received_<ID>.jsonl  - local copy of everything received (tamper check)
  hub_state_<ID>.json      - roster cache + watermark + last sent id
  hub_wait_diag.log        - drop/reconnect diagnostics (never wakes the agent)
"""
import argparse, json, os, sys, time, urllib.request

RS = "\x1e"
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
DEAD_AFTER = 45.0          # seconds without any inbound frame -> dead (M4)
GIVE_UP_AFTER = 600.0      # seconds of continuous failure -> exit 4 (M3)
COLLECT_WINDOW = 2.5       # seconds to batch burst messages into one wake (M2)


class ConnectionLost(Exception):
    """The connection is dead or half-open; the caller should reconnect."""


def negotiate(base):
    req = urllib.request.Request(base + "/hub/negotiate?negotiateVersion=1",
                                 data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        d = json.load(r)
        return d.get("connectionToken") or d["connectionId"]


class Hub:
    def __init__(self, base, agent_id, secret):
        self.base, self.agent_id, self.secret = base, agent_id, secret
        self.ws = None
        self.buf = ""
        self.you = None
        self.roster = None
        self.last_frame_at = 0.0
        self._no_history = False
        # Local side files (best-effort: a logging failure must never
        # break sending or listening).
        self.local_log = os.environ.get("HUB_LOCAL_LOG") or os.path.join(
            TOOLS_DIR, f"hub_received_{agent_id}.jsonl")
        self.state_file = os.path.join(TOOLS_DIR, f"hub_state_{agent_id}.json")
        self.diag_log = os.path.join(TOOLS_DIR, "hub_wait_diag.log")
        self.state = self._load_state()
        self.watermark = self.state.get("watermark", 0)
        # Optional hook called before every reconnect attempt inside
        # wait_message (e.g. to restart a dead SSH tunnel first, so the
        # reconnect unit is "transport + Join", not just the WebSocket).
        self.on_reconnect = None
        self.connect()

    # ---------- connection ----------
    def connect(self):
        from websockets.sync.client import connect
        cid = negotiate(self.base)
        ws_url = self.base.replace("http://", "ws://").replace("https://", "wss://") + "/hub?id=" + cid
        # ping_interval=None: protocol-level ping/pong over the SSH tunnel gets
        # delayed and the library then kills healthy connections (exit 4 drops);
        # the SignalR app-level ping ({"type":6} every 10 s) stays as keepalive.
        # Half-open detection is done by frame-silence tracking instead (M4).
        self.ws = connect(ws_url, open_timeout=20, ping_interval=None)
        self.buf = ""
        self.last_frame_at = time.time()
        self._send({"protocol": "json", "version": 1})
        self._wait_for(lambda m: m == {})  # handshake response
        res = self.invoke("Join", [self.agent_id, self.secret])
        if res == "ok":  # legacy v1 server
            self.you = {"id": self.agent_id, "name": self.agent_id}
            return
        try:
            d = json.loads(res)
        except Exception:
            raise SystemExit(f"Join failed: {res!r}")
        if "error" in d:
            raise SystemExit(f"Join failed: {d['error']}")
        self.you = d.get("you")
        self.roster = d.get("roster")
        self.state["roster"] = self.roster
        self._save_state()

    def close(self):
        try:
            if self.ws is not None:
                self.ws.close()
        except Exception:
            pass
        self.ws = None

    # ---------- local side files ----------
    def _load_state(self):
        try:
            with open(self.state_file, encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    def _save_state(self):
        self.state["watermark"] = self.watermark
        if self.roster is not None:
            self.state["roster"] = self.roster
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False)
        except Exception:
            pass

    def _diag(self, text):
        stamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        try:
            with open(self.diag_log, "a", encoding="utf-8") as f:
                f.write(f"{stamp} [{self.agent_id}] {text}\n")
        except Exception:
            pass

    def _record_local(self, line):
        try:
            with open(self.local_log, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    # ---------- SignalR plumbing ----------
    def _send(self, obj):
        self.ws.send(json.dumps(obj) + RS)

    def _frames(self, timeout):
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
            self.last_frame_at = time.time()  # any inbound bytes = alive (M4)
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

    def get_history(self, since_id):
        res = self.invoke("History", [since_id])
        return json.loads(res) if isinstance(res, str) else res

    def get_roster(self):
        res = self.invoke("Roster", [])
        return json.loads(res) if isinstance(res, str) else res

    def rename(self, new_name):
        res = self.invoke("Rename", [new_name])
        d = json.loads(res) if isinstance(res, str) else res
        if isinstance(d, dict) and "error" in d:
            raise SystemExit(f"Rename failed: {d['error']}")
        return d

    # ---------- send (M7: no duplicates after ambiguous failures) ----------
    def _note_sent(self, res):
        if isinstance(res, dict) and "id" in res:
            self.state["last_sent_id"] = res["id"]
            self._save_state()

    def send_message(self, text, to=None):
        target = "SendTo" if to else "Send"
        args = [text, to] if to else [text]
        try:
            res = self.invoke(target, args)
            self._note_sent(res)
            return res
        except SystemExit as e:
            if not str(e).startswith("timed out"):
                raise  # definitive hub error (too long, unknown recipient...)
            # ambiguous: fall through to the History check
        except Exception:
            pass  # transport error: ambiguous, fall through
        # The completion was lost; the message may or may not be logged.
        # Check History on a fresh connection before resending anything.
        time.sleep(1)
        fresh = Hub(self.base, self.agent_id, self.secret)
        want_to = to or "all"
        baseline = self.state.get("last_sent_id") or self.watermark or 0
        found = None
        since = baseline
        while True:
            entries = fresh.get_history(since)
            for e in entries:
                if (e.get("from_id") == self.agent_id and e.get("text") == text
                        and e.get("to", "all") == want_to):
                    found = e
            if len(entries) < 200 or not entries:
                break
            since = entries[-1]["id"]
        if found is not None:
            self._note_sent(found)
            return found
        res = fresh.invoke(target, args)  # never landed: resend once
        self._note_sent(res)
        return res

    # ---------- entry classification (M5/M6 filters) ----------
    def _apply_rename(self, e):
        if self.roster:
            for a in self.roster:
                if a.get("id") == e.get("from_id"):
                    a["name"] = e.get("new_name", a.get("name"))
            self._save_state()

    def _entry_wakes(self, e):
        """True if this log entry should be printed and wake the agent."""
        if not isinstance(e, dict):
            return False
        if e.get("event") == "rename":
            self._apply_rename(e)
            return False
        if "from_id" not in e:
            return False  # v1-era entry: historical only
        if e.get("from_id") == self.agent_id:
            return False  # own message
        to = e.get("to", "all")
        if to != "all" and to != self.agent_id:
            return False  # directed at someone else: advance silently
        return True

    def _process(self, entries):
        """Record entries, advance the watermark, return the waking batch.
        The watermark is persisted by the caller only after printing."""
        batch = []
        for e in entries:
            self._record_local(json.dumps(e, ensure_ascii=False))
            eid = e.get("id") if isinstance(e, dict) else None
            if eid is not None and eid > self.watermark:
                self.watermark = eid
            if self._entry_wakes(e):
                batch.append(e)
        return batch

    def _print_batch(self, batch):
        for e in batch:
            out = {k: e.get(k) for k in ("id", "ts", "from_id", "from_name", "to", "text")
                   if k in e}
            print(json.dumps(out, ensure_ascii=False), flush=True)
        self._save_state()  # watermark persisted only after a successful print

    def _catch_up(self):
        """Pull everything after the watermark via History (M1/M2).
        Returns the waking batch. Raises ConnectionLost on transport trouble."""
        if self._no_history:
            return []
        batch = []
        while True:
            try:
                entries = self.get_history(self.watermark)
            except SystemExit as e:
                if "History error" in str(e):
                    self._no_history = True  # pre-v2.1 server: no catch-up
                    self._diag("server has no History; catch-up disabled")
                    return batch
                raise ConnectionLost(str(e))
            batch.extend(self._process(entries))
            if len(entries) < 200:
                return batch

    # ---------- wait (M2 + M3 + M4) ----------
    def _listen_once(self, stay, deadline):
        """Listen on the current connection. Return 0 (batch) / 3 (quiet cap).
        Raise on link death; the caller reconnects."""
        got = 0
        last_ping = 0.0
        batch = []
        collect_until = None
        while time.time() < deadline:
            for m in self._frames(5):
                if m.get("type") == 1 and m.get("target") == "Receive":
                    e = m["arguments"][0]
                    eid = e.get("id") if isinstance(e, dict) else None
                    if eid is not None and eid <= self.watermark:
                        continue  # already handled by catch-up (exact-id dedup)
                    woke = self._process([e])
                    if woke:
                        batch.extend(woke)
                        got += len(woke)
                        if not stay and collect_until is None:
                            collect_until = time.time() + COLLECT_WINDOW
                    self._save_state()  # keep the watermark crash-safe
            if collect_until is not None and time.time() >= collect_until:
                self._print_batch(batch)
                return 0
            if time.time() - self.last_frame_at > DEAD_AFTER:
                raise ConnectionLost(
                    f"no frame for {time.time() - self.last_frame_at:.0f}s")
            if time.time() - last_ping > 10:
                self._send({"type": 6})
                last_ping = time.time()
        if batch:
            self._print_batch(batch)
        return 0 if got else 3

    def wait_message(self, stay=False, max_seconds=6 * 3600):
        deadline = time.time() + max_seconds
        fail_since = None
        backoff = 2.0
        need_catch_up = True
        while True:
            try:
                if need_catch_up:
                    batch = self._catch_up()
                    need_catch_up = False
                    if batch:
                        self._print_batch(batch)
                        if not stay:
                            return 0
                return self._listen_once(stay, deadline)
            except SystemExit:
                raise
            except Exception as e:  # dropped or half-open: reconnect in-script (M3)
                if fail_since is None:
                    fail_since = time.time()
                self._diag(f"drop {type(e).__name__}: {e}; "
                           f"last frame {time.time() - self.last_frame_at:.0f}s ago")
                self.close()
                # Reconnect loop: backoff 2s -> 60s; give up (exit 4) only
                # after GIVE_UP_AFTER of continuous failure.
                while True:
                    now = time.time()
                    if now - fail_since > GIVE_UP_AFTER or now >= deadline:
                        print(f"HUB_CONNECTION_LOST: gave up after "
                              f"{now - fail_since:.0f}s of failures: "
                              f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
                        return 4
                    time.sleep(min(backoff, max(0.0, deadline - now)))
                    backoff = min(backoff * 2, 60.0)
                    try:
                        if self.on_reconnect is not None:
                            self.on_reconnect()  # revive transport (tunnel) first
                        self.connect()
                        self._diag("reconnected and re-joined")
                        fail_since = None
                        backoff = 2.0
                        need_catch_up = True
                        break  # back to catch-up + listening
                    except SystemExit:
                        raise
                    except Exception as e2:
                        self._diag(f"reconnect failed {type(e2).__name__}: {e2}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://ddpc.tail8e8344.ts.net:8767")
    ap.add_argument("--id", dest="agent_id", required=True, help="immutable agent ID, e.g. A001")
    ap.add_argument("--secret", default=os.environ.get("HUB_SECRET", ""))
    ap.add_argument("--stay", action="store_true")
    ap.add_argument("--to", default=None, help="send mode only: directed recipient ID (SendTo)")
    ap.add_argument("mode", choices=["send", "wait", "rename", "roster"])
    ap.add_argument("text", nargs="?", default="")
    a = ap.parse_intermixed_args()  # allow options after the mode, e.g. send --to A001 "text"
    if not a.secret:
        raise SystemExit("no secret: pass --secret or set HUB_SECRET")
    hub = Hub(a.url.rstrip("/"), a.agent_id, a.secret)
    if a.mode == "send":
        print(json.dumps(hub.send_message(a.text, to=a.to), ensure_ascii=False))
        return 0
    if a.mode == "rename":
        print(json.dumps(hub.rename(a.text), ensure_ascii=False))
        return 0
    if a.mode == "roster":
        print(json.dumps(hub.get_roster(), ensure_ascii=False))
        return 0
    return hub.wait_message(stay=a.stay)


if __name__ == "__main__":
    sys.exit(main())
