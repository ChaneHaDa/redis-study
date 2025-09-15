from __future__ import annotations

import argparse
import sys
import time
from typing import List, Tuple

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis Streams simple reader (XREAD)")
    p.add_argument("streams", nargs="+", help="Stream keys to read from")
    p.add_argument("--from", dest="from_id", default="$", help="Start ID (e.g., 0-0 or $)")
    p.add_argument("--block", type=int, default=5000, help="Block milliseconds (default: 5000)")
    p.add_argument("--count", type=int, default=None, help="Max entries per read")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)

    # XREAD expects list of stream names and corresponding IDs
    stream_ids: List[Tuple[str, str]] = [(s, args.from_id) for s in args.streams]
    print(f"[xread] streams={args.streams} from={args.from_id} block={args.block} count={args.count}")

    while True:
        data = r.xread(stream_ids=stream_ids, block=args.block, count=args.count)
        if not data:
            print("[xread] timeout (no new entries)")
            continue
        for stream_key, entries in data:
            for entry_id, fields in entries:
                print(f"[entry] stream={stream_key} id={entry_id} fields={fields}")
                # advance last-id for this stream to continue from next
                for i, (k, _) in enumerate(stream_ids):
                    if k == stream_key:
                        stream_ids[i] = (k, entry_id)
                        break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

