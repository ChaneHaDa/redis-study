from __future__ import annotations

import argparse
import sys
import time
from typing import Dict, List

from _common import get_redis_client


def parse_field_kv(pairs: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for p in pairs:
        if "=" not in p:
            raise ValueError(f"Invalid --field '{p}', expected key=value")
        k, v = p.split("=", 1)
        out[k] = v
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis Streams producer (XADD)")
    p.add_argument("--stream", required=True, help="Stream key (e.g., mystream)")
    p.add_argument("--count", type=int, default=1, help="Number of messages to produce (default: 1)")
    p.add_argument("--interval", type=float, default=0.0, help="Seconds between messages")
    p.add_argument("--id", default="*", help="Entry ID to use (default: '*')")
    p.add_argument("--maxlen", type=int, default=None, help="Approximate max length (XADD MAXLEN ~ N)")
    p.add_argument("--field", action="append", default=[], help="Field pair key=value (repeatable)")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)
    fields = parse_field_kv(args.field)

    for i in range(1, args.count + 1):
        payload = {**fields, "seq": str(i)} if fields else {"seq": str(i)}
        if args.maxlen and args.maxlen > 0:
            entry_id = r.xadd(args.stream, payload, id=args.id, maxlen=args.maxlen, approximate=True)
        else:
            entry_id = r.xadd(args.stream, payload, id=args.id)
        print(f"[xadd] #{i} stream={args.stream} id={entry_id} fields={payload}")
        if i < args.count and args.interval > 0:
            time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

