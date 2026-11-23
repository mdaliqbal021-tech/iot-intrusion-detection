"""Write timing evidence using the shared Linux monotonic clock."""

import json
import time
from pathlib import Path


def clock_identity():
    return {
        "clock": "CLOCK_MONOTONIC",
        "units": "nanoseconds",
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "time_namespace": Path("/proc/self/ns/time").readlink().as_posix(),
    }


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")


def write_jsonl(path, rows):
    with path.open("w") as stream:
        for row in rows:
            print(json.dumps(row, allow_nan=False), file=stream)


def launch_marker(directory):
    marker = {"clock": clock_identity(), "spark_launch_ns": time.monotonic_ns()}
    write_json(directory / "spark_launch.json", marker)


if __name__ == "__main__":
    import sys

    launch_marker(Path(sys.argv[1]))
