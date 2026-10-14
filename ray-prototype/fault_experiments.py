"""Fault-injection experiments for the initial Phase 1 experiment (issue #29).

Each experiment runs in its own Ray session, uses the prototype code unchanged,
crashes a process abruptly with `taskkill /F`, and writes one JSON line per
observed operation to experiments/evidence/<experiment>.jsonl.

Usage (from ray-prototype/):  python fault_experiments.py [1] [2] [3]
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import ray

from binnacle_ray import (
    Consumer,
    Producer,
    Record,
    TopicRegistry,
    get_or_create_offset_store,
    get_or_create_registry,
)

HANG_TIMEOUT_S = 10
EVIDENCE_DIR = Path(__file__).resolve().parent.parent / "experiments" / "evidence"


class Recorder:
    def __init__(self, experiment: str) -> None:
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        self.experiment = experiment
        self._file = (EVIDENCE_DIR / f"{experiment}.jsonl").open("w", encoding="utf-8")
        self.log(
            "environment",
            started_at=datetime.now(timezone.utc).isoformat(),
            python=platform.python_version(),
            ray=ray.__version__,
            os=platform.platform(),
            driver_pid=os.getpid(),
            hang_timeout_s=HANG_TIMEOUT_S,
        )

    def log(self, step: str, **fields) -> None:
        row = {"experiment": self.experiment, "t": round(time.time(), 3), "step": step, **fields}
        line = json.dumps(row, default=str, ensure_ascii=False)
        self._file.write(line + "\n")
        self._file.flush()
        print(line, flush=True)

    def run(self, step: str, fn):
        """Run fn, classifying it as ok / error / hung (> HANG_TIMEOUT_S)."""
        box: dict = {}

        def target() -> None:
            try:
                box["result"] = fn()
            except BaseException as exc:  # noqa: BLE001 - the error itself is the evidence
                box["error"] = exc

        # Daemon thread: a hung call must not keep the script alive at exit.
        worker = threading.Thread(target=target, daemon=True)
        start = time.monotonic()
        worker.start()
        worker.join(HANG_TIMEOUT_S)
        elapsed = round(time.monotonic() - start, 3)

        if worker.is_alive():
            self.log(step, outcome="hung", elapsed_s=elapsed)
            return None
        if "error" in box:
            exc = box["error"]
            self.log(
                step,
                outcome="error",
                elapsed_s=elapsed,
                error_type=type(exc).__name__,
                error_classes=[cls.__name__ for cls in type(exc).__mro__[:6]],
                error=" | ".join(str(exc).strip().splitlines()[:3])[:500],
            )
            return None
        self.log(step, outcome="ok", elapsed_s=elapsed, result=box.get("result"))
        return box.get("result")

    def close(self) -> None:
        self._file.close()


def process_alive(pid: int) -> bool:
    out = subprocess.run(
        ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
        capture_output=True,
        text=True,
        errors="replace",
    ).stdout
    return f" {pid} " in f" {out} "


def crash(rec: Recorder, step: str, pid: int) -> None:
    done = subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
    rec.log(step, pid=pid, taskkill_returncode=done.returncode, alive_after_kill=process_alive(pid))


def pid_of(actor) -> int:
    """PID of any actor without modifying its class."""
    return ray.get(actor.__ray_call__.remote(lambda self: os.getpid()), timeout=HANG_TIMEOUT_S)


def brief(records: list) -> list[dict]:
    return [{"offset": offset, "value": record.value.decode()} for offset, record in records]


def experiment_1() -> None:
    """F1: a partition process crashes while a producer is writing."""
    rec = Recorder("exp1")
    registry = TopicRegistry.remote()
    rec.run("create_topic", lambda: ray.get(registry.create_topic.remote("exp1", 3)))
    producer = Producer("exp1", registry=registry)

    def send(phase: str, i: int) -> None:
        target = producer._next_empty_key_partition  # round-robin target, logged as evidence
        rec.run(f"send_{phase}[{i}]->p{target}", lambda: asdict(producer.send(b"", f"{phase}-{i}".encode())))

    for i in range(30):
        send("before", i)

    partitions = ray.get(registry.get_partitions.remote("exp1"))
    placement = [ray.get(p.whereami.remote()) for p in partitions]
    rec.log("placement", partitions=placement)
    victim_pid = placement[1]["pid"]
    crash(rec, "kill_partition_1", victim_pid)

    for i in range(30):
        send("after", i)

    rec.run("read_p1_from_0", lambda: brief(ray.get(partitions[1].read.remote(0))))
    rec.run("read_p0_from_0", lambda: brief(ray.get(partitions[0].read.remote(0))))
    rec.run("read_p2_from_0", lambda: brief(ray.get(partitions[2].read.remote(0))))

    returned = rec.run("registry_get_partition_1", lambda: ray.get(registry.get_partition.remote("exp1", 1)))
    if returned is not None:
        rec.log("returned_handle_is_same_actor", same=returned._actor_id.hex() == partitions[1]._actor_id.hex())
        rec.run("use_returned_handle", lambda: ray.get(returned.whereami.remote()))

    time.sleep(5)
    rec.log("after_5s", victim_pid=victim_pid, victim_process_alive=process_alive(victim_pid))
    rec.run("p1_next_offset_after_5s", lambda: ray.get(partitions[1].next_offset.remote()))
    rec.run(
        "p1_append_after_5s",
        lambda: ray.get(partitions[1].append.remote([Record(key=b"", value=b"late")])),
    )
    rec.close()


def experiment_2() -> None:
    """The owner of the partitions (topic registry) crashes."""
    rec = Recorder("exp2")
    registry = get_or_create_registry()
    rec.run("create_topic", lambda: ray.get(registry.create_topic.remote("exp2", 3)))
    partitions = ray.get(registry.get_partitions.remote("exp2"))
    for p, handle in enumerate(partitions):
        batch = [Record(key=b"", value=f"p{p}-{i}".encode()) for i in range(5)]
        rec.run(f"append_p{p}", lambda handle=handle, batch=batch: ray.get(handle.append.remote(batch)))

    registry_pid = pid_of(registry)
    placement = [ray.get(p.whereami.remote()) for p in partitions]
    rec.log("placement", registry_pid=registry_pid, partitions=placement)
    crash(rec, "kill_registry", registry_pid)

    killed_at = time.monotonic()
    for wait in (1, 5):
        time.sleep(max(0.0, wait - (time.monotonic() - killed_at)))
        rec.log(
            f"partition_processes_after_{wait}s",
            alive={w["pid"]: process_alive(w["pid"]) for w in placement},
        )
        for p, handle in enumerate(partitions):
            rec.run(f"read_p{p}_saved_handle_after_{wait}s", lambda handle=handle: brief(ray.get(handle.read.remote(0))))

    rec.run("new_producer_exp2", lambda: Producer("exp2").num_partitions)
    rec.run(
        "lookup_registry_again_same_actor",
        lambda: get_or_create_registry()._actor_id.hex() == registry._actor_id.hex(),
    )
    rec.run("lookup_registry_again_list_topics", lambda: ray.get(get_or_create_registry().list_topics.remote()))
    rec.close()


def experiment_3() -> None:
    """Consumer restart after the committed-offset store crashes."""
    rec = Recorder("exp3")
    registry = TopicRegistry.remote()
    ray.get(registry.create_topic.remote("exp3", 1))
    partition = ray.get(registry.get_partition.remote("exp3", 0))
    batch = [Record(key=b"", value=f"m{i}".encode()) for i in range(10)]
    rec.run("append_10", lambda: ray.get(partition.append.remote(batch)))

    seen_a: list[int] = []
    consumer_a = Consumer("exp3", "g", registry=registry)
    rec.run("consumer_a_run_max_6", lambda: consumer_a.run(lambda r: seen_a.append(r.offset), max_messages=6))
    rec.log("consumer_a_processed", offsets=seen_a)
    rec.run("committed_before_kill", consumer_a.committed_offsets)

    store_pid = pid_of(get_or_create_offset_store())
    crash(rec, "kill_offset_store", store_pid)

    rec.run("consumer_a_commit_with_old_handle", lambda: consumer_a.commit_offset(0, 7))

    holder: dict = {}

    def create_b() -> dict:
        holder["b"] = Consumer("exp3", "g", registry=registry)
        return holder["b"].positions

    rec.run("consumer_b_create", create_b)
    if "b" in holder:
        seen_b: list[int] = []
        rec.run("consumer_b_run", lambda: holder["b"].run(lambda r: seen_b.append(r.offset), idle_timeout_s=2))
        rec.log(
            "consumer_b_processed",
            offsets=seen_b,
            processed_twice=sorted(set(seen_a) & set(seen_b)),
        )
        rec.run("committed_after_b", holder["b"].committed_offsets)
    rec.close()


EXPERIMENTS = {"1": experiment_1, "2": experiment_2, "3": experiment_3}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("experiments", nargs="*", choices=sorted(EXPERIMENTS), default=sorted(EXPERIMENTS))
    args = parser.parse_args()
    for name in args.experiments:
        print(f"\n=== experiment {name} ===", flush=True)
        ray.init(include_dashboard=False, logging_level="ERROR")
        try:
            EXPERIMENTS[name]()
        finally:
            ray.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
