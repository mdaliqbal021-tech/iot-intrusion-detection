"""Prepare Kafka replay records and verify predictions by replay ID."""

import argparse
import csv
import json
import math
from collections import Counter
from itertools import islice
from pathlib import Path

SMOKE_ROWS = 32
SCORE_TOLERANCE = 1e-7
MARGIN_TOLERANCE = 1e-5


def verify_contributions(explanation, stored_score):
    """Check all terms against the raw margin and the original attack score."""
    margin = math.fsum([
        explanation["bias"], *(row["contribution"] for row in explanation["features"]),
    ])
    raw_margin = explanation["raw_margin"]
    if not math.isclose(margin, raw_margin, rel_tol=0, abs_tol=MARGIN_TOLERANCE):
        raise ValueError("Feature contributions do not reconstruct the raw margin.")
    # Native terms are float32. Apply sigmoid to the verified model margin,
    # so rounding in the individual contributions does not affect score parity.
    exp_term = math.exp(-abs(raw_margin))
    score = 1 / (1 + exp_term) if raw_margin >= 0 else exp_term / (1 + exp_term)
    if not math.isclose(score, stored_score, rel_tol=0, abs_tol=SCORE_TOLERANCE):
        raise ValueError("The reconstructed attack score does not match the stored score.")
    return score


def write_messages(input_path, output_path, count, metadata_path=None):
    with input_path.open(newline="") as source, output_path.open("w") as output:
        reader = csv.DictReader(source)
        features = json.loads(metadata_path.read_text())["feature_columns"] if metadata_path else reader.fieldnames
        if not features or set(features) - set(reader.fieldnames or []):
            raise ValueError("Replay CSV is missing required features.")
        written = 0
        for replay_id, row in enumerate(islice(reader, count)):
            values = {feature: float(row[feature]) for feature in features}
            print(json.dumps({"replay_id": replay_id, **values}, allow_nan=False), file=output)
            written += 1
        if written != count:
            raise ValueError(f"Replay requires {count} input rows, got {written}.")


def read_predictions(directory, count):
    rows = []
    for path in sorted(directory.glob("batch-*.jsonl")):
        with path.open() as stream:
            rows.extend(json.loads(line) for line in stream)
    ids = [row["replay_id"] for row in rows]
    if any(type(replay_id) is not int for replay_id in ids):
        raise ValueError("A replay ID is not an integer.")
    counts = Counter(ids)
    expected_ids = set(range(count))
    missing = sorted(expected_ids - counts.keys())
    extra = sorted(counts.keys() - expected_ids)
    duplicates = sorted(replay_id for replay_id, total in counts.items() if total > 1)
    if missing or extra or duplicates:
        raise ValueError(
            f"Replay ID mismatch. Missing: {len(missing)} {missing[:10]}; "
            f"unexpected: {len(extra)} {extra[:10]}; duplicated: {len(duplicates)} {duplicates[:10]}."
        )
    for row in rows:
        if row["predicted_class"] not in (0, 1) or not 0 <= row["attack_score"] <= 1:
            raise ValueError(f"Invalid class or score for replay ID {row['replay_id']}.")
    return sorted(rows, key=lambda row: row["replay_id"])


def compare(expected_path, output_dir, count=SMOKE_ROWS):
    if expected_path.is_dir():
        expected = read_predictions(expected_path, count)
    else:
        with expected_path.open(newline="") as source:
            expected = list(csv.DictReader(source))
        if len(expected) != count:
            raise ValueError(f"Reference requires {count} rows, got {len(expected)}.")
    actual = read_predictions(output_dir, count)

    mismatches = []
    max_difference = 0.0
    for replay_id, (prediction, reference) in enumerate(zip(actual, expected, strict=True)):
        if prediction["predicted_class"] != int(reference["predicted_class"]):
            mismatches.append(replay_id)
        difference = abs(prediction["attack_score"] - float(reference["attack_score"]))
        if not math.isfinite(difference):
            raise ValueError(f"Nonfinite score difference for replay ID {replay_id}.")
        max_difference = max(max_difference, difference)
    if mismatches or max_difference > SCORE_TOLERANCE:
        raise ValueError(f"Class mismatches: {len(mismatches)}; maximum absolute score difference: {max_difference:.12g}.")
    print(f"PASS: {count:,} unique IDs, {count:,} matching classes, "
          f"maximum absolute score difference {max_difference:.12g} (tolerance {SCORE_TOLERANCE:g}).")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("input", type=Path)
    prepare.add_argument("output", type=Path)
    prepare.add_argument("--metadata", type=Path)
    prepare.add_argument("--rows", type=int, default=SMOKE_ROWS)
    check = commands.add_parser("compare")
    check.add_argument("expected", type=Path)
    check.add_argument("output_dir", type=Path)
    check.add_argument("--rows", type=int, default=SMOKE_ROWS)
    validate = commands.add_parser("check-ids")
    validate.add_argument("output_dir", type=Path)
    validate.add_argument("--rows", type=int, required=True)
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be positive")
    try:
        if args.command == "prepare":
            write_messages(args.input, args.output, args.rows, args.metadata)
        elif args.command == "compare":
            compare(args.expected, args.output_dir, args.rows)
        else:
            rows = read_predictions(args.output_dir, args.rows)
            print(f"PASS: {len(rows):,} unique IDs with valid scores and classes.")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Replay failed: {error}\n")


if __name__ == "__main__":
    main()
