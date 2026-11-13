#!/usr/bin/env python3
"""Wait for one message from the family Muse via MuseHub.
Prints the message as one JSON line and exits 0. Exit 3 if none within 6h."""
import sys
from hub_tunnel import open_hub

def main():
    hub, proc = open_hub("A001", "~/workspace/tools/hub_secret_sword.txt")
    try:
        return hub.wait_message()
    finally:
        proc.terminate()

if __name__ == "__main__":
    sys.exit(main())
