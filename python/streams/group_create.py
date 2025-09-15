from __future__ import annotations

import argparse
import sys

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create a consumer group if missing (XGROUP CREATE)")
    p.add_argument("--stream", required=True, help="Stream key")
    p.add_argument("--group", required=True, help="Group name")
    p.add_argument("--from", dest="from_id", default="$", help="Read from ID (e.g., 0-0 or $)")
    p.add_argument("--mkstream", action="store_true", help="Create stream if not exists (MKSTREAM)")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)
    try:
        r.xgroup_create(name=args.stream, groupname=args.group, id=args.from_id, mkstream=args.mkstream)
        print(f"[xgroup] created group={args.group} on stream={args.stream} from={args.from_id}")
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            print(f"[xgroup] group already exists: {args.group}")
        else:
            raise


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

