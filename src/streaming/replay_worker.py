"""Run queued record selections through Kafka, Spark and Cassandra."""

import fcntl
import logging
import os
import subprocess
from pathlib import Path
from time import perf_counter, sleep

from cassandra.cluster import Cluster

from src.streaming.cassandra_sink import SELECT, RUNS, register_completed_run
from src.streaming.replay import SCORE_TOLERANCE, read_predictions
from src.streaming.selected_replay import read_json, utc_now, write_status

ROOT = Path(os.environ.get("IOT_REPLAY_JOBS", "/replay-jobs/selected"))
KAFKA = Path("/opt/kafka/bin")
BROKER = "kafka:9092"


def run_command(command, log, source=None, timeout=120):
    subprocess.run(command, stdin=source, stdout=log, stderr=subprocess.STDOUT,
                   check=True, timeout=timeout)


def process(directory, session, query, job):
    topic = f"iot-replay-{job['run_id']}"
    job.update(status="processing", stage="Publishing the selected event to Kafka", started_at=utc_now())
    write_status(directory, job)
    started = perf_counter()
    try:
        with (directory / "worker.log").open("ab", buffering=0) as log:
            run_command([
                KAFKA / "kafka-topics.sh", "--bootstrap-server", BROKER,
                "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1",
            ], log)
            before_publish = perf_counter()
            with (directory / "input.jsonl").open("rb") as source:
                run_command([
                    KAFKA / "kafka-console-producer.sh", "--bootstrap-server", BROKER,
                    "--topic", topic, "--sync", "--producer-property", "acks=all",
                ], log, source)
            job["timings_ms"]["kafka_publish"] = (perf_counter() - before_publish) * 1_000
            job.update(stage="Kafka acknowledged the event. Spark is processing it.", topic=topic)
            write_status(directory, job)

            before_spark = perf_counter()
            run_command([
                "/usr/local/bin/iot-spark-replay", topic, "--run-dir", directory,
                "--cassandra-run-id", job["run_id"], "--source-record-id", str(job["source_record_id"]),
            ], log, timeout=360)
            job["timings_ms"]["spark_job"] = (perf_counter() - before_spark) * 1_000

        job["stage"] = "Verifying the saved Cassandra prediction"
        write_status(directory, job)
        expected, = read_predictions(directory / "predictions", 1)
        actual = list(session.execute(query, (job["run_id"],), timeout=5))
        if (len(actual) != 1 or actual[0].replay_id != job["replay_id"]
                or actual[0].predicted_class != expected["predicted_class"]
                or abs(actual[0].attack_score - expected["attack_score"]) > SCORE_TOLERANCE):
            raise RuntimeError("Cassandra does not match the Spark prediction.")
        register_completed_run(session, job["run_id"], expected_count=1)
    except Exception:
        logging.exception("Replay %s failed during %s", job["run_id"], job["stage"])
        job.update(status="failed", finished_at=utc_now(),
                   error="The replay worker could not finish this stage. See worker.log and the replay-worker service logs.")
    else:
        job.update(status="completed", stage="Cassandra prediction verified", finished_at=utc_now())
    job["timings_ms"]["worker_total"] = (perf_counter() - started) * 1_000
    write_status(directory, job)


def main():
    logging.basicConfig(level=logging.INFO)
    ROOT.mkdir(parents=True, exist_ok=True)
    # Only one worker consumes this spool, including during container replacement.
    with (ROOT / ".worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with Cluster(["cassandra"], protocol_version=4) as cluster:
            session = cluster.connect()
            query = session.prepare(SELECT)
            completed = {row.run_id for row in session.execute(RUNS)}
            for status in ROOT.glob("selected.*/status.json"):
                job = read_json(status)
                if job["status"] == "processing":
                    if job["run_id"] in completed:
                        job.update(status="completed", stage="Cassandra prediction verified", finished_at=utc_now())
                    else:
                        job.update(status="failed", error="The worker restarted before the run was verified.",
                                   finished_at=utc_now())
                    write_status(status.parent, job)

            while True:
                pending = []
                for status in ROOT.glob("selected.*/status.json"):
                    job = read_json(status)
                    if job["status"] == "queued":
                        pending.append((status.parent, job))
                for directory, job in sorted(pending, key=lambda item: item[1]["queued_at"]):
                    process(directory, session, query, job)
                sleep(1)


if __name__ == "__main__":
    main()
