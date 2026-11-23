"""Export timing evidence and a summary from completed timed replay directories."""

import argparse
import gzip
import json
import os
import platform
import subprocess
from collections import Counter
from pathlib import Path
from statistics import median, quantiles

from src.streaming.replay_timing import write_json


def read_json(path):
    return json.loads(path.read_text())


def read_events(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream]


def capture_environment():
    def output(*command):
        return subprocess.check_output(command, text=True).strip()

    composition = ("docker", "compose", "-f", "infra/compose.yaml")
    fields = "{{.NCPU}}|{{.MemTotal}}|{{.OperatingSystem}}|{{.ServerVersion}}"
    cpus, memory, os_name, version = output("docker", "info", "--format", fields).split("|")
    limits = {}
    for service in ("kafka", "cassandra", "dashboard", "replay-worker"):
        container = output(*composition, "ps", "-q", service)
        if not container:
            continue
        fields = "{{.HostConfig.NanoCpus}}|{{.HostConfig.Memory}}|{{.HostConfig.CpusetCpus}}"
        quota, mem, affinity = output("docker", "inspect", "--format", fields, container).split("|")
        limits[service] = {
            "cpu_limit": int(quota) / 1e9 or None, "memory_limit_bytes": int(mem) or None,
            "cpuset": affinity or None,
        }
    cpu = next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
               if line.startswith("model name"))
    memory_kib = int(Path("/proc/meminfo").read_text().splitlines()[0].split()[1])
    return {
        "cpu": cpu, "wsl_logical_cpus": len(os.sched_getaffinity(0)), "wsl_memory_kib": memory_kib,
        "kernel": platform.release(), "architecture": platform.machine(),
        "docker_logical_cpus": int(cpus), "docker_memory_bytes": int(memory), "docker_os": os_name,
        "docker_version": version, "service_limits": limits, "kafka": "apache/kafka:3.9.1",
        "cassandra": "cassandra:5.0.9", "kafka_partitions": 2,
    }


def summarize(directory, workload, evidence):
    run_id = directory.name
    producer = read_json(directory / "producer_timing.json")
    launch = read_json(directory / "spark_launch.json")
    spark = read_json(directory / "spark_timing.json")
    if producer["clock"] != launch["clock"] or spark["clock"] != launch["clock"]:
        raise ValueError(f"{run_id}: timestamps do not share a verified clock.")
    expected = {row["replay_id"] for row in read_events(directory / "input.jsonl")}
    sends = read_events(directory / "sends.jsonl")
    acks = [row for path in sorted(directory.glob("ack-batch-*.jsonl")) for row in read_events(path)]
    stored = [
        {"run_id": run_id, "event_id": row["replay_id"]}
        for row in read_events(directory / "cassandra-first/batch-0.jsonl")
    ]
    groups = {"send": sends, "ack": acks, "stored": stored}
    counts = {kind: Counter(row["event_id"] for row in rows) for kind, rows in groups.items()}
    for kind, rows in groups.items():
        for row in rows:
            if row["run_id"] != run_id:
                raise ValueError(f"{run_id}: mismatched event run ID in {kind} evidence.")
            print(json.dumps({"kind": kind, **row}), file=evidence)

    confirmed = expected.intersection(*counts.values())
    if len(confirmed) < 2:
        raise ValueError(f"{run_id}: too few confirmed events to calculate timing.")
    first_send = min(row["send_ns"] for row in sends)
    last_send = max(row["send_ns"] for row in sends)
    first_batch = min(row["batch_start_ns"] for row in acks)
    last_ack = max(row["ack_ns"] for row in acks if row["event_id"] in confirmed)
    sent_at, acknowledged_at = {}, {}
    for row in sends:
        event_id = row["event_id"]
        sent_at[event_id] = min(row["send_ns"], sent_at.get(event_id, row["send_ns"]))
    for row in acks:
        event_id = row["event_id"]
        acknowledged_at[event_id] = max(row["ack_ns"], acknowledged_at.get(event_id, row["ack_ns"]))
    latencies_ms = [(acknowledged_at[key] - sent_at[key]) / 1e6 for key in confirmed]
    if (min(latencies_ms) < 0 or not first_send < last_send <= producer["publish_complete_ns"]
            <= launch["spark_launch_ns"] <= first_batch <= last_ack):
        raise ValueError(f"{run_id}: invalid send, launch, batch or acknowledgement order.")

    elapsed_s = (last_ack - first_send) / 1e9
    return {
        "workload": workload, "run_id": run_id, "rows": len(expected),
        "confirmed_events": len(confirmed), "stored_unique_events": len(counts["stored"]),
        "missing_event_ids": sorted(expected - confirmed),
        "duplicate_event_ids": {
            kind: sorted(key for key, count in events.items() if count > 1)
            for kind, events in counts.items()
        },
        "unexpected_event_ids": {
            kind: sorted(events.keys() - expected) for kind, events in counts.items()
        },
        "target_send_eps": producer["target_events_per_second"],
        "actual_send_eps": (len(sends) - 1) * 1e9 / (last_send - first_send),
        "elapsed_s": elapsed_s, "throughput_eps": len(confirmed) / elapsed_s,
        "median_ms": median(latencies_ms),
        "p95_ms": quantiles(latencies_ms, n=100, method="inclusive")[94],
        "micro_batches": len({row["batch_id"] for row in acks}),
        "spark_launch_to_query_s": (spark["query_started_ns"] - launch["spark_launch_ns"]) / 1e9,
        "spark_launch_to_first_batch_s": (first_batch - launch["spark_launch_ns"]) / 1e9,
        "timing": {"producer": producer, "launch": launch, "spark": spark},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--environment", type=Path, help="Reuse a recorded Docker environment instead of inspecting it.")
    parser.add_argument("--smoke", type=Path, required=True)
    parser.add_argument("repetitions", type=Path, nargs="+")
    args = parser.parse_args()
    runs = [args.smoke, *args.repetitions]
    if len(args.repetitions) < 3 or len(set(runs)) != len(runs):
        parser.error("Supply one smoke run and at least three distinct measured repetitions.")
    args.output.mkdir(parents=True, exist_ok=True)
    raw_path = args.output / "streaming_performance_events.jsonl.gz"
    summary_path = args.output / "streaming_performance_summary.json"
    if raw_path.exists() or summary_path.exists():
        parser.error("Output already exists. Use a fresh output directory.")
    environment = read_json(args.environment) if args.environment else capture_environment()
    with gzip.open(raw_path, "xt", encoding="utf-8") as evidence:
        results = [summarize(directory, "Smoke fixture" if index == 0 else f"Trial {index}", evidence)
                   for index, directory in enumerate(runs)]
    write_json(summary_path, {"environment": environment, "runs": results})


if __name__ == "__main__":
    main()
