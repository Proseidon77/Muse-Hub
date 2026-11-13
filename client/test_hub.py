#!/usr/bin/env python3
"""End-to-end test of MuseHub: both directions + impersonation rejection."""
import json, os, subprocess, sys, time

HOME = os.path.expanduser("~")
PY = f"{HOME}/workspace/tools/venv/bin/python"
CLI = f"{HOME}/workspace/tools/hub_client.py"
SW = open(f"{HOME}/workspace/tools/hub_secret_sword.txt").read().strip()
FM = open(f"{HOME}/workspace/tools/hub_secret_family.txt").read().strip()

sys.path.insert(0, f"{HOME}/workspace/tools")
from hub_tunnel import _start_tunnel  # noqa: E402

def cli(port, name, secret, *args):
    return subprocess.run([PY, CLI, "--url", f"http://127.0.0.1:{port}",
                           "--name", name, "--secret", secret, *args],
                          capture_output=True, text=True, timeout=90)

def cli_wait_start(port, name, secret):
    return subprocess.Popen([PY, CLI, "--url", f"http://127.0.0.1:{port}",
                             "--name", name, "--secret", secret, "wait"],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def main():
    t1 = _start_tunnel(18767)   # sword side
    t2 = _start_tunnel(18768)   # family side (simulated from here, one-time check)
    try:
        # 1) family waits, sword sends
        fw = cli_wait_start(18768, "family", FM)
        time.sleep(4)
        r = cli(18767, "sword", SW, "send", "联调测试一：sword 发给 family")
        sent1 = json.loads(r.stdout.strip())
        out, _ = fw.communicate(timeout=60)
        got1 = json.loads(out.strip().splitlines()[-1])
        assert sent1["from"] == "sword" and sent1["to"] == "family", sent1
        assert got1["id"] == sent1["id"] and got1["from"] == "sword", got1
        print(f"PASS1 sword->family id={sent1['id']} text={got1['text']}")

        # 2) sword waits, family sends
        swait = cli_wait_start(18767, "sword", SW)
        time.sleep(4)
        r = cli(18768, "family", FM, "send", "联调测试二：family 回给 sword")
        sent2 = json.loads(r.stdout.strip())
        out, _ = swait.communicate(timeout=60)
        got2 = json.loads(out.strip().splitlines()[-1])
        assert sent2["from"] == "family" and got2["from"] == "family", (sent2, got2)
        print(f"PASS2 family->sword id={sent2['id']} text={got2['text']}")

        # 3) impersonation: family secret trying to join as sword
        r = cli(18767, "sword", FM, "send", "我是假冒的")
        assert r.returncode != 0 and "Join failed" in (r.stdout + r.stderr), (r.returncode, r.stdout, r.stderr)
        print("PASS3 impersonation rejected (family secret cannot join as sword)")

        # 4) wrong secret rejected
        r = cli(18767, "family", "0" * 48, "send", "x")
        assert r.returncode != 0 and "Join failed" in (r.stdout + r.stderr)
        print("PASS4 wrong secret rejected")
        print("ALL TESTS PASSED")
    finally:
        t1.terminate(); t2.terminate()

if __name__ == "__main__":
    main()
