"""Write or query one replay run in the local Cassandra prediction table."""

import argparse
import json

from cassandra.cluster import Cluster

INSERT = """
INSERT INTO iot_replay.predictions (run_id, replay_id, attack_score, predicted_class)
VALUES (?, ?, ?, ?)
"""
SELECT = """
SELECT replay_id, attack_score, predicted_class
FROM iot_replay.predictions WHERE run_id = ?
"""


def write_predictions(run_id, predictions):
    with Cluster(["cassandra"], protocol_version=4) as cluster:
        session = cluster.connect()
        insert = session.prepare(INSERT)
        for replay_id, score, predicted_class in predictions:
            session.execute(insert, (run_id, int(replay_id), float(score), int(predicted_class)))


def main():
    parser = argparse.ArgumentParser(description="Print Cassandra predictions for one run as JSON lines.")
    parser.add_argument("run_id")
    args = parser.parse_args()
    with Cluster(["cassandra"], protocol_version=4) as cluster:
        session = cluster.connect()
        query = session.prepare(SELECT)
        for row in session.execute(query, (args.run_id,)):
            print(json.dumps(row._asdict(), allow_nan=False))


if __name__ == "__main__":
    main()
