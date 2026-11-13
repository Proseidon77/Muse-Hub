#!/usr/bin/env python3
"""Send one message to the family Muse via MuseHub. Usage: hub_send.py "text" """
import json, sys
from hub_tunnel import open_hub

def main():
    hub, proc = open_hub("A001", "~/workspace/tools/hub_secret_sword.txt")
    try:
        msg = hub.send_message(sys.argv[1])
        print(json.dumps(msg, ensure_ascii=False))
    finally:
        proc.terminate()

if __name__ == "__main__":
    main()
