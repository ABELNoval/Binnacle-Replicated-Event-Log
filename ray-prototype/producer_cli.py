from __future__ import annotations

import argparse
import base64
import json
import os
import time

import ray

from binnacle_ray import Producer


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce records to a Ray prototype topic.")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--key-count", type=int, default=3)
    parser.add_argument("--value-prefix", default="msg")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument(
        "--base64",
        action="store_true",
        help="emit keys and values as base64 instead of UTF-8 text",
    )
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be >= 1")
    if args.key_count < 1:
        parser.error("--key-count must be >= 1")

    address = os.environ.get("RAY_ADDRESS") or "auto"
    ray.init(address=address, include_dashboard=False, logging_level="ERROR")
    producer = Producer(args.topic)
    for i in range(args.count):
        # Mix keyed and empty-key records so both partitioning paths run.
        key = b"" if i % (args.key_count + 1) == args.key_count else f"key-{i % args.key_count}".encode()
        value = f"{args.value_prefix}-{i}".encode()
        result = producer.send(key, value)
        if args.base64:
            key_out = base64.b64encode(key).decode("ascii")
            value_out = base64.b64encode(value).decode("ascii")
        else:
            key_out = key.decode("utf-8")
            value_out = value.decode("utf-8")
        print(
            json.dumps(
                {
                    "producer_pid": os.getpid(),
                    "topic": result.topic,
                    "partition": result.partition,
                    "offset": result.offset,
                    "key": key_out,
                    "value": value_out,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.sleep_ms:
            time.sleep(args.sleep_ms / 1000)
    ray.shutdown()


if __name__ == "__main__":
    main()
