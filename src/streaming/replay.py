"""Prepare the fixed Kafka replay and check Spark predictions by replay ID."""

import argparse
import csv
import json
import math
from collections import Counter
from pathlib import Path

ROW_COUNT = 32
SCORE_TOLERANCE = 1e-7


def write_messages(input_path, output_path):
    with input_path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    if len(rows) != ROW_COUNT:
        raise ValueError(f"Replay requires {ROW_COUNT} input rows, got {len(rows)}.")
    with output_path.open("w") as output:
        for replay_id, row in enumerate(rows):
            features = {name: float(value) for name, value in row.items()}
            print(json.dumps({"replay_id": replay_id, **features}, allow_nan=False), file=output)


def compare(expected_path, output_dir):
    with expected_path.open(newline="") as source:
        expected = list(csv.DictReader(source))
    if len(expected) != ROW_COUNT:
        raise ValueError(f"Reference requires {ROW_COUNT} rows, got {len(expected)}.")
    actual = [
        json.loads(line)
        for path in sorted(output_dir.glob("batch-*.jsonl"))
        for line in path.read_text().splitlines()
    ]
    ids = [row["replay_id"] for row in actual]
    if any(type(replay_id) is not int for replay_id in ids):
        raise ValueError("Spark returned a replay ID that is not an integer.")
    counts = Counter(ids)
    missing = sorted(set(range(ROW_COUNT)) - counts.keys())
    extra = sorted(counts.keys() - set(range(ROW_COUNT)))
    duplicates = sorted(replay_id for replay_id, count in counts.items() if count > 1)
    if missing or extra or duplicates:
        raise ValueError(f"Replay ID mismatch. Missing: {missing}; unexpected: {extra}; duplicated: {duplicates}.")

    by_id = {row["replay_id"]: row for row in actual}
    mismatches = []
    max_difference = 0.0
    for replay_id, reference in enumerate(expected):
        prediction = by_id[replay_id]
        if prediction["predicted_class"] != int(reference["predicted_class"]):
            mismatches.append(replay_id)
        difference = abs(prediction["attack_score"] - float(reference["attack_score"]))
        if not math.isfinite(difference):
            raise ValueError(f"Nonfinite score difference for replay ID {replay_id}.")
        max_difference = max(max_difference, difference)
    if mismatches or max_difference > SCORE_TOLERANCE:
        raise ValueError(f"Class mismatches: {mismatches}; maximum absolute score difference: {max_difference:.12g}.")
    print(f"PASS: {ROW_COUNT} unique IDs, {ROW_COUNT} matching classes, "
          f"maximum absolute score difference {max_difference:.12g} (tolerance {SCORE_TOLERANCE:g}).")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("input", type=Path)
    prepare.add_argument("output", type=Path)
    check = commands.add_parser("compare")
    check.add_argument("expected", type=Path)
    check.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            write_messages(args.input, args.output)
        else:
            compare(args.expected, args.output_dir)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Replay failed: {error}\n")


if __name__ == "__main__":
    main()
