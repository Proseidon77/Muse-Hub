#!/usr/bin/env python3
"""One-off driver: run hub_client backfill for A001 (DB migration step).
Uses the standard tunnel/proxy transport like hub_wait.py/hub_send.py."""
import json, os, sys
os.environ.setdefault("HUB_DB_MODE", "1")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from hub_tunnel import open_hub

def main():
    hub, proc = open_hub("A001", "~/workspace/tools/hub_secret_sword.txt")
    try:
        if not hub.db_on:
            print("ERROR: db mode not active", file=sys.stderr)
            return 2
        n = hub.backfill()
        print(json.dumps({"backfilled_new": n, "total": hub._db_count()},
                         ensure_ascii=False))
        return 0
    finally:
        proc.terminate()

if __name__ == "__main__":
    sys.exit(main())
