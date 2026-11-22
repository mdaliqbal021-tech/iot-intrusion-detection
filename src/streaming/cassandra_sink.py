"""Write or query one replay run in the local Cassandra prediction table."""

import argparse
import json
from datetime import datetime, timezone

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

INSERT = """
INSERT INTO iot_replay.predictions (run_id, replay_id, attack_score, predicted_class)
VALUES (?, ?, ?, ?)
"""
SELECT = """
SELECT replay_id, attack_score, predicted_class
FROM iot_replay.predictions WHERE run_id = ?
"""
COUNT = "SELECT count(*) AS prediction_count FROM iot_replay.predictions WHERE run_id = ?"
COMPLETE = """
INSERT INTO iot_replay.completed_runs (run_id, completed_at, prediction_count, display_number)
VALUES (?, ?, ?, ?) IF NOT EXISTS
"""
RUNS = "SELECT run_id, completed_at, prediction_count, display_number FROM iot_replay.completed_runs"


def write_predictions(session, insert, run_id, predictions):
    parameters = [
        (run_id, int(replay_id), float(score), int(predicted_class))
        for replay_id, score, predicted_class in predictions
    ]
    execute_concurrent_with_args(session, insert, parameters, concurrency=32)


def next_display_number(session):
    update = session.prepare(
        "UPDATE iot_replay.run_number_sequence SET last_number = ? "
        "WHERE sequence_name = 'completed_runs' IF last_number = ?"
    )
    while True:
        current = session.execute(
            "SELECT last_number FROM iot_replay.run_number_sequence WHERE sequence_name = 'completed_runs'"
        ).one().last_number
        if session.execute(update, (current + 1, current)).one().applied:
            return current + 1


def number_existing_runs(session, runs):
    unnumbered = sorted(
        (run for run in runs if run.display_number is None),
        key=lambda run: (run.completed_at, run.run_id),
    )
    if unnumbered:
        update = session.prepare(
            "UPDATE iot_replay.completed_runs SET display_number = ? WHERE run_id = ? IF display_number = null"
        )
        for run in unnumbered:
            session.execute(update, (next_display_number(session), run.run_id))
    return bool(unnumbered)


def main():
    parser = argparse.ArgumentParser(description="Query or register a completed Cassandra replay run.")
    parser.add_argument("run_id")
    parser.add_argument("--complete", action="store_true", help="Register a run after replay verification passes")
    parser.add_argument("--expected-count", type=int)
    args = parser.parse_args()
    with Cluster(["cassandra"], protocol_version=4) as cluster:
        session = cluster.connect()
        if args.complete:
            runs = list(session.execute(RUNS))
            number_existing_runs(session, runs)
            if any(run.run_id == args.run_id for run in runs):
                return
            count = session.execute(session.prepare(COUNT), (args.run_id,)).one().prediction_count
            if not count or (args.expected_count is not None and count != args.expected_count):
                parser.exit(1, f"Cannot register run with {count} predictions (expected {args.expected_count}).\n")
            display_number = next_display_number(session)
            result = session.execute(
                session.prepare(COMPLETE), (args.run_id, datetime.now(timezone.utc), count, display_number)
            ).one()
            if result.applied:
                print(f"Registered Run {display_number} ({args.run_id}) with {count:,} predictions.")
        else:
            query = session.prepare(SELECT)
            for row in session.execute(query, (args.run_id,)):
                print(json.dumps(row._asdict(), allow_nan=False))


if __name__ == "__main__":
    main()
