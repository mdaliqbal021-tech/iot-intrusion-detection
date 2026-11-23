"""Persist single-record replay requests alongside the existing CLI run files."""

import csv
import json
from datetime import datetime, timezone
from errno import EEXIST, ENOTEMPTY
from hashlib import sha256
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

TEST_CSV = "ciciot2023_test_stratified_25000_seed42.csv"
MODEL_NAME = "xgboost_final"
EXAMPLE_ROWS = (132, 0, 12)


class ReplayConflict(ValueError):
    """A replay request no longer matches its selection."""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def read_json(path):
    return json.loads(path.read_text())


def write_status(directory, job):
    temporary = directory / "status.tmp"
    temporary.write_text(json.dumps(job, allow_nan=False) + "\n")
    temporary.replace(directory / "status.json")


class TestRecords:
    def __init__(self, path, metadata_path):
        self.metadata = read_json(metadata_path)
        self.features = self.metadata["feature_columns"]
        if self.metadata["attack_threshold"] != 0.5 or self.metadata["class_mapping"] != {"0": "benign", "1": "attack"}:
            raise ValueError("Unexpected deployed threshold or class mapping.")
        contents = path.read_bytes()
        self.dataset_sha256 = sha256(contents).hexdigest()
        with StringIO(contents.decode("utf-8"), newline="") as source:
            reader = csv.DictReader(source)
            if not set([*self.features, "label"]).issubset(reader.fieldnames or ()):
                raise ValueError("The held-out CSV is missing the deployed features or label.")
            if set(self.features) & {"label", "source_record_id", "replay_id", "run_id", "record_id"}:
                raise ValueError("Labels and IDs must stay outside the deployed features.")
            self.rows = list(reader)
        if len(self.rows) != 25_000:
            raise ValueError("Expected the fixed 25,000-row held-out subset.")

    def record(self, source_record_id):
        if not 0 <= source_record_id < len(self.rows):
            raise IndexError(source_record_id)
        row = self.rows[source_record_id]
        return {
            "source_record_id": source_record_id,
            "recorded_label": row["label"],
            "features": {feature: float(row[feature]) for feature in self.features},
        }

    def catalog(self):
        return {
            "dataset": TEST_CSV,
            "dataset_sha256": self.dataset_sha256,
            "count": len(self.rows),
            "examples": [
                {"source_record_id": index, "recorded_label": self.rows[index]["label"]}
                for index in EXAMPLE_ROWS
            ],
        }

    def enqueue(self, root, source_record_id, request_id=None, dataset_sha256=None):
        if dataset_sha256 is not None and dataset_sha256 != self.dataset_sha256:
            raise ReplayConflict("The test subset changed. Refresh the record preview before replaying.")
        record = self.record(source_record_id)
        run_id = f"selected.{(request_id or uuid4()).hex}"
        destination = root / run_id

        def existing_job():
            existing = read_json(destination / "status.json")
            if (existing["source_record_id"] != source_record_id
                    or existing.get("dataset_sha256") != self.dataset_sha256):
                raise ReplayConflict("This request ID already belongs to another record or test subset.")
            return existing

        if destination.exists():
            return existing_job()
        job = {
            "run_id": run_id,
            "source_record_id": source_record_id,
            "dataset_sha256": self.dataset_sha256,
            "replay_id": 0,
            "recorded_label": record["recorded_label"],
            "model_version": f"{MODEL_NAME} (exported with XGBoost {self.metadata['xgboost_version']})",
            "threshold": self.metadata["attack_threshold"],
            "status": "queued",
            "stage": "Waiting for the replay worker",
            "queued_at": utc_now(),
            "timings_ms": {},
        }
        root.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=".enqueue-", dir=root) as temporary:
            directory = Path(temporary) / run_id
            directory.mkdir()
            payload = {"source_record_id": source_record_id, "replay_id": 0, **record["features"]}
            (directory / "input.jsonl").write_text(json.dumps(payload, allow_nan=False) + "\n")
            write_status(directory, job)
            try:
                directory.rename(destination)
            except OSError as error:
                if error.errno not in (EEXIST, ENOTEMPTY):
                    raise
                return existing_job()
        return job
