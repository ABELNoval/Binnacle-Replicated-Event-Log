from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import ray

from binnacle_ray import get_or_create_offset_store, get_or_create_registry

ROOT = Path(__file__).resolve().parent


def _json_lines(text: str) -> list[dict]:
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            # Ray can emit startup/log lines around the JSON payload.
            start = line.find("{")
            if start >= 0:
                rows.append(json.loads(line[start:]))
    return rows


def _read_rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _child_env(address: str) -> dict[str, str]:
    env = os.environ.copy()
    python_path = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(ROOT) if not python_path else f"{ROOT}{os.pathsep}{python_path}"
    env["RAY_ADDRESS"] = address
    env["RAY_USAGE_STATS_ENABLED"] = "0"
    return env


def _run_checked(cmd: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=90,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(cmd)}\n{result.stdout}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run separate producer and consumer processes, stop the consumer, "
            "then verify that a new consumer resumes from committed offsets."
        )
    )
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--messages", type=int, default=12)
    parser.add_argument("--first-run", type=int, default=4)
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("--partitions must be >= 1")
    if args.messages < 2:
        parser.error("--messages must be >= 2")
    if not 1 <= args.first_run < args.messages:
        parser.error("--first-run must be at least 1 and less than --messages")

    ray.init(num_cpus=max(4, args.partitions + 2), include_dashboard=False, logging_level="ERROR")
    try:
        address = ray.get_runtime_context().gcs_address
        registry = get_or_create_registry()
        offset_store = get_or_create_offset_store()
        topic = f"demo-{uuid.uuid4().hex[:12]}"
        group = "demo-group"
        ray.get(registry.create_topic.remote(topic, args.partitions))
        env = _child_env(address)

        with tempfile.TemporaryDirectory(prefix="binnacle-demo-") as tmp:
            first_output = Path(tmp) / "consumer-first.jsonl"
            second_output = Path(tmp) / "consumer-second.jsonl"

            consumer1 = subprocess.Popen(
                [
                    sys.executable,
                    "consumer_cli.py",
                    "--topic",
                    topic,
                    "--group",
                    group,
                    "--max-messages",
                    str(args.first_run),
                    "--max-records",
                    "1",
                    "--idle-timeout-s",
                    "20",
                    "--output",
                    str(first_output),
                    "--instance",
                    "first",
                ],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            producer = _run_checked(
                [
                    sys.executable,
                    "producer_cli.py",
                    "--topic",
                    topic,
                    "--count",
                    str(args.messages),
                    "--key-count",
                    "3",
                ],
                env,
            )
            consumer1_stdout, _ = consumer1.communicate(timeout=90)
            if consumer1.returncode != 0:
                raise RuntimeError(f"first consumer failed ({consumer1.returncode})\n{consumer1_stdout}")

            committed_after_restart = ray.get(offset_store.committed_offsets.remote(group, topic))
            consumer2 = _run_checked(
                [
                    sys.executable,
                    "consumer_cli.py",
                    "--topic",
                    topic,
                    "--group",
                    group,
                    "--max-messages",
                    str(args.messages - args.first_run),
                    "--max-records",
                    "10",
                    "--idle-timeout-s",
                    "20",
                    "--output",
                    str(second_output),
                    "--instance",
                    "second",
                ],
                env,
            )

            produced = _json_lines(producer.stdout)
            first = _read_rows(first_output)
            second = _read_rows(second_output)
            consumed = first + second

            produced_triplets = {
                (row["partition"], row["offset"], row["key"], row["value"]) for row in produced
            }
            consumed_triplets = {
                (row["partition"], row["offset"], row["key"], row["value"]) for row in consumed
            }
            assert len(produced) == args.messages, (len(produced), args.messages)
            assert len(first) == args.first_run, (len(first), args.first_run)
            assert len(consumed) == args.messages, (len(consumed), args.messages)
            assert produced_triplets == consumed_triplets
            assert len(consumed_triplets) == args.messages, "duplicate consumed records"

            producer_pid = produced[0]["producer_pid"]
            consumer_pids = {row["consumer_pid"] for row in consumed}
            assert producer_pid not in consumer_pids
            assert {row["consumer_instance"] for row in consumed} == {"first", "second"}

            per_partition_counts = {partition: 0 for partition in range(args.partitions)}
            for partition in range(args.partitions):
                partition_rows = sorted(
                    (row for row in consumed if row["partition"] == partition),
                    key=lambda row: row["offset"],
                )
                offsets = [row["offset"] for row in partition_rows]
                assert offsets == list(range(len(offsets))), (partition, offsets)
                produced_values = [
                    (row["key"], row["value"])
                    for row in sorted(
                        (row for row in produced if row["partition"] == partition),
                        key=lambda row: row["offset"],
                    )
                ]
                consumed_values = [(row["key"], row["value"]) for row in partition_rows]
                assert consumed_values == produced_values
                per_partition_counts[partition] = len(partition_rows)

            final_committed = ray.get(offset_store.committed_offsets.remote(group, topic))
            assert final_committed == per_partition_counts, (final_committed, per_partition_counts)

            print(
                json.dumps(
                    {
                        "topic": topic,
                        "group": group,
                        "producer_pid": producer_pid,
                        "consumer_pids": sorted(consumer_pids),
                        "first_run_messages": len(first),
                        "resumed_messages": len(second),
                        "committed_after_restart": committed_after_restart,
                        "final_committed": final_committed,
                        "consumer2_summary": _json_lines(consumer2.stdout)[-1],
                    },
                    sort_keys=True,
                    indent=2,
                )
            )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
