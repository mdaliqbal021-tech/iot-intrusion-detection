"""Score Kafka replay messages in finite Spark Structured Streaming microbatches."""

import argparse
from hashlib import sha256
import json
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
from xgboost import DMatrix, XGBClassifier, __version__ as xgboost_version
from xgboost.core import XGBoostError

from src.streaming.replay import verify_contributions
from src.streaming.replay_timing import clock_identity, write_json, write_jsonl

MAX_BATCH_ROWS = 1_000


def score_batch(frame, model, features, attack_index, threshold):
    if frame[["replay_id", *features]].isna().any().any():
        raise ValueError("Replay record has a missing or invalid ID or feature.")
    scores = model.predict_proba(frame[features])[:, attack_index]
    return frame[["replay_id"]].assign(
        attack_score=scores.astype("float64"),
        predicted_class=(scores >= threshold).astype(int),
    )


def explain_row(inputs, booster, score, model_sha256):
    if json.loads(booster.save_config())["learner"]["objective"]["name"] != "binary:logistic":
        raise ValueError("Raw log-odds explanations require the binary:logistic objective.")
    matrix = DMatrix(inputs, nthread=2)
    contributions = booster.predict(matrix, pred_contribs=True)[0]
    explanation = {
        "model_sha256": model_sha256,
        "xgboost_version": xgboost_version,
        "bias": float(contributions[-1]),
        "raw_margin": float(booster.predict(matrix, output_margin=True)[0]),
        "features": [
            {"feature": name, "value": float(value), "contribution": float(contribution)}
            for name, value, contribution in zip(
                inputs.columns, inputs.iloc[0], contributions[:-1], strict=True,
            )
        ],
    }
    verify_contributions(explanation, score)
    return explanation


def main():
    from pyspark.sql import SparkSession, functions as F, types as T

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("topic")
    parser.add_argument("--bootstrap-server", default="kafka:9092")
    parser.add_argument("--model-dir", type=Path, default=Path("/models"))
    parser.add_argument("--run-dir", type=Path, default=Path("/replay"))
    parser.add_argument("--cassandra-run-id")
    parser.add_argument("--source-record-id", type=int)
    parser.add_argument("--record-timing", action="store_true")
    args = parser.parse_args()
    if args.record_timing and not args.cassandra_run_id:
        parser.error("--record-timing requires --cassandra-run-id")
    if args.cassandra_run_id:
        from cassandra.cluster import Cluster

        from src.streaming.cassandra_sink import INSERT, write_predictions

    metadata = json.loads((args.model_dir / "xgboost_final_metadata.json").read_text())
    features = metadata["feature_columns"]
    threshold = metadata["attack_threshold"]
    if metadata["class_mapping"] != {"0": "benign", "1": "attack"}:
        raise ValueError("Unexpected class mapping in the exported metadata.")
    model = XGBClassifier(device="cpu", n_jobs=2)
    model_path = args.model_dir / "xgboost_final.ubj"
    model_sha256 = None
    if args.source_record_id is not None:
        model_bytes = model_path.read_bytes()
        model_sha256 = sha256(model_bytes).hexdigest()
        model.load_model(bytearray(model_bytes))
    else:
        model.load_model(model_path)
    if model.get_booster().feature_names != features:
        raise ValueError("Metadata feature order differs from the exported model.")
    attack_index = list(model.classes_).index(1)

    schema = T.StructType([
        T.StructField("replay_id", T.LongType()),
        T.StructField("source_record_id", T.LongType()),
        *[T.StructField(feature, T.DoubleType()) for feature in features],
    ])
    output_dir = args.run_dir / "predictions"
    output_dir.mkdir()
    spark = SparkSession.builder.appName("iot-replay").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    cluster = None

    def predict_microbatch(batch, batch_id):
        batch_start_ns = time.monotonic_ns() if args.record_timing else None
        records = batch.take(MAX_BATCH_ROWS + 1)
        if not records:
            return
        if len(records) > MAX_BATCH_ROWS:
            raise ValueError(f"A replay microbatch must contain at most {MAX_BATCH_ROWS} records.")
        rows = pd.DataFrame.from_records(records, columns=["replay_id", "source_record_id", *features])
        if args.source_record_id is not None:
            if (len(rows) != 1 or rows["replay_id"].iloc[0] != 0
                    or rows["source_record_id"].iloc[0] != args.source_record_id):
                raise ValueError("Kafka event does not match the selected source record.")
            print(f"Consumed source record {args.source_record_id} as event 0 from {args.topic}.", flush=True)
        predictions = score_batch(rows, model, features, attack_index, threshold)
        if args.cassandra_run_id:
            write_predictions(session, insert, args.cassandra_run_id, predictions.itertuples(index=False, name=None))
            if args.record_timing:
                ack_ns = time.monotonic_ns()
                write_jsonl(args.run_dir / f"ack-batch-{batch_id}.jsonl", [
                    {"run_id": args.cassandra_run_id, "event_id": int(event_id), "batch_id": batch_id,
                     "batch_start_ns": batch_start_ns, "ack_ns": ack_ns}
                    for event_id in predictions["replay_id"]
                ])
        if model_sha256 is not None:
            try:
                explanation = explain_row(
                    rows[features], model.get_booster(), float(predictions["attack_score"].iloc[0]), model_sha256,
                )
            except (ValueError, XGBoostError):
                logging.exception("Native contributions could not be verified for the selected row.")
                explanation = {"unavailable": "This run did not produce a verifiable native contribution explanation."}
            explanation.update(
                run_id=args.cassandra_run_id, replay_id=int(rows["replay_id"].iloc[0]),
                source_record_id=args.source_record_id,
                model_export_version=metadata["xgboost_version"], threshold=threshold,
            )
            temporary_explanation = args.run_dir / "explanation.tmp"
            temporary_explanation.write_text(json.dumps(explanation, allow_nan=False))
            os.replace(temporary_explanation, args.run_dir / "explanation.json")
        destination = output_dir / f"batch-{batch_id}.jsonl"
        temporary = destination.with_suffix(".tmp")
        predictions.to_json(temporary, orient="records", lines=True, double_precision=15)
        os.replace(temporary, destination)
        print(f"Spark batch {batch_id}: scored {len(predictions)} predictions.", flush=True)

    try:
        if args.cassandra_run_id:
            cluster = Cluster(["cassandra"], protocol_version=4)
            session = cluster.connect()
            insert = session.prepare(INSERT)
        driver = (sys.executable, sys.version.split()[0])
        workers = spark.sparkContext.parallelize(range(2), 2).map(
            lambda _: (sys.executable, sys.version.split()[0])
        ).collect()
        print(f"Spark driver Python: {driver[0]} ({driver[1]})", flush=True)
        for worker, (executable, version) in enumerate(workers):
            print(f"Spark worker probe {worker}: {executable} ({version})", flush=True)
        if any(version.split(".")[:2] != ["3", "10"] for _, version in [driver, *workers]):
            raise RuntimeError("The Spark driver and workers must use Python 3.10.")

        messages = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_server)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", MAX_BATCH_ROWS)
            .load()
            .select(F.from_json(F.col("value").cast("string"), schema, {"mode": "FAILFAST"}).alias("message"))
            .select("message.*")
        )
        query = (
            messages.writeStream.foreachBatch(predict_microbatch)
            .option("checkpointLocation", str(args.run_dir / "checkpoint"))
            .trigger(availableNow=True)
            .start()
        )
        if args.record_timing:
            query_started_ns = time.monotonic_ns()
            write_json(args.run_dir / "spark_timing.json", {
                "clock": clock_identity(), "query_started_ns": query_started_ns,
                "spark_version": spark.version, "spark_master": spark.sparkContext.master,
                "python_version": driver[1], "xgboost_runtime": xgboost_version,
                "xgboost_export": metadata["xgboost_version"], "threshold": threshold,
                "model_sha256": sha256(model_path.read_bytes()).hexdigest(),
                "trigger": "availableNow", "starting_offsets": "earliest",
                "max_offsets_per_trigger": MAX_BATCH_ROWS, "cassandra_write_concurrency": 32,
                "cassandra_consistency": str(session.default_consistency_level),
                "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
                "memory_max": Path("/sys/fs/cgroup/memory.max").read_text().strip(),
            })
        query.awaitTermination()
    finally:
        if cluster is not None:
            cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
