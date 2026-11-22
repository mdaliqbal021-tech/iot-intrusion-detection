"""Score Kafka replay messages in finite Spark Structured Streaming microbatches."""

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd
from xgboost import XGBClassifier

MAX_BATCH_ROWS = 1_000


def score_batch(frame, model, features, attack_index, threshold):
    if frame[["replay_id", *features]].isna().any().any():
        raise ValueError("Replay record has a missing or invalid ID or feature.")
    scores = model.predict_proba(frame[features])[:, attack_index]
    return frame[["replay_id"]].assign(
        attack_score=scores.astype("float64"),
        predicted_class=(scores >= threshold).astype(int),
    )


def main():
    from pyspark.sql import SparkSession, functions as F, types as T

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("topic")
    parser.add_argument("--bootstrap-server", default="kafka:9092")
    parser.add_argument("--model-dir", type=Path, default=Path("/models"))
    parser.add_argument("--run-dir", type=Path, default=Path("/replay"))
    parser.add_argument("--cassandra-run-id")
    args = parser.parse_args()
    if args.cassandra_run_id:
        from cassandra.cluster import Cluster

        from src.streaming.cassandra_sink import INSERT, write_predictions

    metadata = json.loads((args.model_dir / "xgboost_final_metadata.json").read_text())
    features = metadata["feature_columns"]
    threshold = metadata["attack_threshold"]
    if metadata["class_mapping"] != {"0": "benign", "1": "attack"}:
        raise ValueError("Unexpected class mapping in the exported metadata.")
    model = XGBClassifier(device="cpu", n_jobs=2)
    model.load_model(args.model_dir / "xgboost_final.ubj")
    if model.get_booster().feature_names != features:
        raise ValueError("Metadata feature order differs from the exported model.")
    attack_index = list(model.classes_).index(1)

    schema = T.StructType([
        T.StructField("replay_id", T.LongType()),
        *[T.StructField(feature, T.DoubleType()) for feature in features],
    ])
    output_dir = args.run_dir / "predictions"
    output_dir.mkdir()
    spark = SparkSession.builder.appName("iot-replay").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    cluster = None

    def predict_microbatch(batch, batch_id):
        records = batch.take(MAX_BATCH_ROWS + 1)
        if not records:
            return
        if len(records) > MAX_BATCH_ROWS:
            raise ValueError(f"A replay microbatch must contain at most {MAX_BATCH_ROWS} records.")
        rows = pd.DataFrame.from_records(records, columns=["replay_id", *features])
        predictions = score_batch(rows, model, features, attack_index, threshold)
        if args.cassandra_run_id:
            write_predictions(session, insert, args.cassandra_run_id, predictions.itertuples(index=False, name=None))
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
        query.awaitTermination()
    finally:
        if cluster is not None:
            cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
