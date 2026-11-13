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

Database mode (per DB-UPGRADE-GUIDE.md, 2026-11-13; opt-in, default off):
  Enabled by env HUB_DB_MODE=1 or a flag file hub_db_<ID>.on next to this
  script. Entries go to table hub_msg in the local PostgreSQL database
  "musehub" (one row per server id; wake = pending/done/none separates
  "stored" from "printed", so a kill can only reprint, never lose).
  No watermark is kept in DB mode: catch-up over-pulls one page (200)
  before max(id) and dedups by primary key. If the database is
  unreachable the client prints HUB_STATE_DEGRADED once and falls back
  to the file behaviour above for the rest of the process.
  Extra CLI mode: backfill (DB mode only) pulls the full History into
  hub_msg without printing.
"""
import argparse, base64, json, os, subprocess, sys, time, urllib.request

RS = "\x1e"
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
DEAD_AFTER = 45.0          # seconds without any inbound frame -> dead (M4)
GIVE_UP_AFTER = 600.0      # seconds of continuous failure -> exit 4 (M3)
COLLECT_WINDOW = 2.5       # seconds to batch burst messages into one wake (M2)
OVERPULL = 200             # DB mode catch-up over-pull (one History page)
DB_PW_DEFAULT = os.path.expanduser("~/workspace/musehub/musehub_owner.pw")


def _pw_file():
    """Password file for the musehub db. HUB_DB_PW_FILE may override it,
    but an override pointing at a missing file (e.g. a stale value left
    in some shell's environment) must not defeat the real default."""
    p = os.environ.get("HUB_DB_PW_FILE")
    if p and os.path.exists(p):
        return p
    return DB_PW_DEFAULT
PGSH = os.path.expanduser("~/workspace/postgres/pg.sh")


class DBDown(Exception):
    """The local musehub database could not be reached or used."""


def _db_sql(sql, tuples=False):
    """Run SQL against db "musehub" as musehub_owner via pg.sh psql
    (the house pattern: no driver, password from the .pw file).
    Returns stdout text; raises DBDown on any failure."""
    try:
        with open(_pw_file(), encoding="utf-8") as f:
            pw = f.read().strip()
    except Exception as e:
        raise DBDown(f"password file unreadable: {e}")
    env = dict(os.environ)
    env["PGPASSWORD"] = pw
    args = [PGSH, "psql", "-U", "musehub_owner", "-d", "musehub",
            "-X", "-q", "-v", "ON_ERROR_STOP=1"]
    if tuples:
        args += ["-A", "-t", "-F", "\x1f"]
    try:
        r = subprocess.run(args, input=sql, capture_output=True, text=True,
                           env=env, timeout=30)
    except Exception as e:
        raise DBDown(str(e))
    if r.returncode != 0:
        raise DBDown((r.stderr or r.stdout or "psql failed").strip()[:200])
    return r.stdout


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
        # Database mode (opt-in; see module docstring). Probed once here;
        # any later DB failure degrades to file mode via _db_degrade().
        self.db_on = False
        self._deferred = []        # Receive frames buffered during invokes (F3)
        self._startup_sync = False
        if self._db_requested():
            try:
                _db_sql("SELECT 1")
                self.db_on = True
                self._startup_sync = True
            except DBDown as e:
                print(f"HUB_STATE_DEGRADED: database unavailable ({e}); "
                      f"using file state", file=sys.stderr, flush=True)
                self._diag(f"db mode requested but unavailable: {e}; file mode")
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

    # ---------- database mode ----------
    def _db_requested(self):
        if os.environ.get("HUB_DB_MODE", "").lower() in ("1", "true", "yes"):
            return True
        return os.path.exists(os.path.join(
            TOOLS_DIR, f"hub_db_{self.agent_id}.on"))

    def _db_degrade(self, e):
        if self.db_on:
            print(f"HUB_STATE_DEGRADED: database lost ({e}); "
                  f"using file state", file=sys.stderr, flush=True)
        self.db_on = False
        self._diag(f"db degraded: {e}")

    def _db_insert(self, entries, force_wake=None):
        """Insert entries into hub_msg (PK dedup). Returns the sublist of
        entries newly inserted as wake='pending'. Raises DBDown.
        During the startup sync, entries at or below the file watermark
        are marked 'done' (they were printed in a file-mode period)."""
        rows, pending = [], []
        for e in entries:
            if not isinstance(e, dict) or e.get("id") is None:
                continue
            if force_wake is not None:
                wake = force_wake
            elif self._startup_sync and e["id"] <= self.state.get("watermark", 0):
                wake = "done"
            else:
                wake = "pending" if self._entry_wakes(e) else "none"
            raw = json.dumps(e, ensure_ascii=False)
            b64 = base64.b64encode(raw.encode("utf-8")).decode()
            rows.append(f"({int(e['id'])}, convert_from(decode('{b64}',"
                        f"'base64'),'UTF8')::jsonb, '{wake}')")
            if wake == "pending":
                pending.append(e)
        if not rows:
            return []
        out = _db_sql("INSERT INTO hub_msg (id, entry, wake) VALUES "
                      + ", ".join(rows)
                      + " ON CONFLICT (id) DO NOTHING RETURNING id;", tuples=True)
        new_ids = {int(x) for x in out.split() if x.strip()}
        return [e for e in pending if e["id"] in new_ids]

    def _db_max_id(self):
        out = _db_sql("SELECT COALESCE(MAX(id), 0) FROM hub_msg;", tuples=True)
        return int(out.strip() or 0)

    def _db_count(self):
        out = _db_sql("SELECT count(*) FROM hub_msg;", tuples=True)
        return int(out.strip() or 0)

    def _db_pending(self):
        out = _db_sql("SELECT entry::text FROM hub_msg "
                      "WHERE wake='pending' ORDER BY id;", tuples=True)
        return [json.loads(line) for line in out.splitlines() if line.strip()]

    def _db_mark_done(self, ids):
        if not ids:
            return
        idlist = ", ".join(str(int(i)) for i in ids)
        _db_sql(f"UPDATE hub_msg SET wake='done', printed_at=now() "
                f"WHERE wake='pending' AND id IN ({idlist});")

    def _flush_deferred(self, batch):
        if self._deferred:
            pending, self._deferred = self._deferred, []
            batch.extend(self._process(pending))

    def backfill(self):
        """DB mode only: pull the full History into hub_msg, insert-only,
        never prints. Returns how many rows were newly inserted."""
        if not self.db_on:
            raise SystemExit("backfill needs database mode and a reachable "
                             "database (HUB_DB_MODE=1 or the flag file)")
        total_new, since = 0, 0
        while True:
            entries = self.get_history(since)
            if not entries:
                break
            before = self._db_count()
            self._db_insert(entries, force_wake="none")
            total_new += self._db_count() - before
            if len(entries) < 200:
                break
            since = entries[-1]["id"]
        return total_new

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
            if (self.db_on and m.get("type") == 1
                    and m.get("target") == "Receive"):
                # F3: never drop a push that arrives while an invoke is
                # in flight; it is processed after the invoke returns.
                try:
                    self._deferred.append(m["arguments"][0])
                except Exception:
                    pass
                continue
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
            if self.db_on:
                try:
                    self._db_insert([res], force_wake="none")
                except DBDown as e:
                    self._db_degrade(e)

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
        The watermark is persisted by the caller only after printing.
        In DB mode the entries go to hub_msg instead and the return value
        is the newly inserted pending entries."""
        if self.db_on:
            try:
                return self._db_insert(entries)
            except DBDown as e:
                self._db_degrade(e)  # fall through to the file path below
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
        if self.db_on:
            try:
                rows = self._db_pending()
            except DBDown as e:
                self._db_degrade(e)
                rows = None
            if rows is not None:
                for e in rows:
                    out = {k: e.get(k) for k in ("id", "ts", "from_id", "from_name", "to", "text")
                           if k in e}
                    print(json.dumps(out, ensure_ascii=False), flush=True)
                if rows:
                    try:
                        self._db_mark_done([e["id"] for e in rows])
                    except DBDown as e:
                        # Printed but unmarked: they will reprint next
                        # time (the safe direction, never a loss).
                        self._db_degrade(e)
                    # Shadow file watermark (printed-up-to) so a later
                    # file-mode fallback resumes at the right place.
                    self.watermark = max(
                        self.watermark, max(e["id"] for e in rows))
                    self._save_state()
                return
        for e in batch:
            out = {k: e.get(k) for k in ("id", "ts", "from_id", "from_name", "to", "text")
                   if k in e}
            print(json.dumps(out, ensure_ascii=False), flush=True)
        self._save_state()  # watermark persisted only after a successful print

    def _catch_up(self):
        """Pull everything after the watermark via History (M1/M2).
        Returns the waking batch. Raises ConnectionLost on transport trouble.
        In DB mode there is no watermark: pull from max(id) - OVERPULL and
        let the primary key dedup; any rows still pending afterwards (e.g.
        left by a killed run) become the batch so they get printed."""
        if self._no_history:
            return []
        batch = []
        if self.db_on:
            try:
                since = max(0, self._db_max_id() - OVERPULL)
            except DBDown as e:
                self._db_degrade(e)
                since = self.watermark
        else:
            since = self.watermark
        while True:
            try:
                entries = self.get_history(since)
            except SystemExit as e:
                if "History error" in str(e):
                    self._no_history = True  # pre-v2.1 server: no catch-up
                    self._diag("server has no History; catch-up disabled")
                    return batch
                raise ConnectionLost(str(e))
            batch.extend(self._process(entries))
            if self.db_on:
                self._flush_deferred(batch)
            if len(entries) < 200:
                if self.db_on:
                    self._startup_sync = False
                    try:
                        pend = self._db_pending()
                        if pend:
                            batch = pend
                    except DBDown:
                        pass
                return batch
            since = entries[-1]["id"] if self.db_on else self.watermark

    # ---------- wait (M2 + M3 + M4) ----------
    def _listen_once(self, stay, deadline):
        """Listen on the current connection. Return 0 (batch) / 3 (quiet cap).
        Raise on link death; the caller reconnects."""
        got = 0
        last_ping = 0.0
        batch = []
        collect_until = None
        if self.db_on:
            self._flush_deferred(batch)
            if batch:
                got += len(batch)
                if not stay and collect_until is None:
                    collect_until = time.time() + COLLECT_WINDOW
        while time.time() < deadline:
            for m in self._frames(5):
                if m.get("type") == 1 and m.get("target") == "Receive":
                    e = m["arguments"][0]
                    eid = e.get("id") if isinstance(e, dict) else None
                    if not self.db_on and eid is not None and eid <= self.watermark:
                        continue  # already handled by catch-up (exact-id dedup)
                    woke = self._process([e])
                    if woke:
                        batch.extend(woke)
                        got += len(woke)
                        if not stay and collect_until is None:
                            collect_until = time.time() + COLLECT_WINDOW
                    if not self.db_on:
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
    ap.add_argument("mode", choices=["send", "wait", "rename", "roster", "backfill"])
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
    if a.mode == "backfill":
        n = hub.backfill()
        print(json.dumps({"backfilled_new": n, "total": hub._db_count()},
                         ensure_ascii=False))
        return 0
    return hub.wait_message(stay=a.stay)


if __name__ == "__main__":
    sys.exit(main())
