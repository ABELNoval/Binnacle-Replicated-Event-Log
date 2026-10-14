from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path

import ray

from binnacle_ray import Consumer, ConsumedRecord


def _partitions(value: str) -> list[int] | None:
    if not value:
        return None
    return [int(part) for part in value.split(",") if part]


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume records from a Ray prototype topic.")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument(
        "--partitions",
        type=_partitions,
        default=None,
        help="comma-separated static assignment; defaults to all partitions",
    )
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--max-records", type=int, default=100)
    parser.add_argument("--idle-timeout-s", type=float, default=10.0)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--instance", default="consumer")
    parser.add_argument(
        "--base64",
        action="store_true",
        help="emit keys and values as base64 instead of UTF-8 text",
    )
    args = parser.parse_args()

    address = os.environ.get("RAY_ADDRESS") or "auto"
    ray.init(address=address, include_dashboard=False, logging_level="ERROR")
    consumer = Consumer(
        args.topic,
        args.group,
        partitions=args.partitions,
    )
    output = args.output.open("a", encoding="utf-8") if args.output else None

    def handle(record: ConsumedRecord) -> None:
        if args.base64:
            key_out = base64.b64encode(record.key).decode("ascii")
            value_out = base64.b64encode(record.value).decode("ascii")
        else:
            key_out = record.key.decode("utf-8")
            value_out = record.value.decode("utf-8")
        row = {
            "consumer_instance": args.instance,
            "consumer_pid": os.getpid(),
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "key": key_out,
            "value": value_out,
            "timestamp": record.record.timestamp,
        }
        line = json.dumps(row, sort_keys=True)
        if output:
            output.write(line + "\n")
            output.flush()
        else:
            print(line, flush=True)

    try:
        processed = consumer.run(
            handle,
            max_messages=args.max_messages,
            max_records=args.max_records,
            idle_timeout_s=args.idle_timeout_s,
        )
        print(
            json.dumps(
                {
                    "consumer_instance": args.instance,
                    "consumer_pid": os.getpid(),
                    "processed": processed,
                    "committed": consumer.committed_offsets(),
                },
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        if output:
            output.close()
        ray.shutdown()


if __name__ == "__main__":
    main()
