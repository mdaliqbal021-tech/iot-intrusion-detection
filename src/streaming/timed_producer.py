"""Publish prepared replay events at a fixed rate with Kafka send timestamps."""

import argparse
import json
import time
from pathlib import Path

from pyspark.java_gateway import launch_gateway

from src.streaming.replay_timing import clock_identity, write_json, write_jsonl


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("topic")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rate", type=int, required=True)
    args = parser.parse_args()
    if args.rate <= 0:
        parser.error("--rate must be positive")

    directory = Path("/replay")
    payloads = (directory / "input.jsonl").read_text().splitlines()
    event_ids = [json.loads(payload)["replay_id"] for payload in payloads]
    jvm = launch_gateway().jvm
    config = jvm.java.util.Properties()
    for key, value in {
        "bootstrap.servers": "kafka:9092",
        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer",
        "acks": "all",
        "enable.idempotence": "true",
    }.items():
        config.setProperty(key, value)
    producer = jvm.org.apache.kafka.clients.producer.KafkaProducer(config)
    record = jvm.org.apache.kafka.clients.producer.ProducerRecord
    try:
        # Resolve metadata and build the JVM records before starting the paced loop.
        producer.partitionsFor(args.topic)
        packets = [record(args.topic, None, payload) for payload in payloads]
        sends = []
        period_ns = 1_000_000_000 / args.rate
        next_send = time.monotonic_ns()
        for event_id, packet in zip(event_ids, packets, strict=True):
            wait_ns = next_send - time.monotonic_ns()
            if wait_ns > 0:
                time.sleep(wait_ns / 1_000_000_000)
            send_ns = time.monotonic_ns()
            future = producer.send(packet)
            sends.append((event_id, send_ns, future))
            next_send += period_ns
        producer.flush()
        for _, _, future in sends:
            future.get(30, jvm.java.util.concurrent.TimeUnit.SECONDS)
        publish_complete_ns = time.monotonic_ns()
    finally:
        producer.close()

    write_jsonl(directory / "sends.jsonl", [
        {"run_id": args.run_id, "event_id": event_id, "send_ns": send_ns}
        for event_id, send_ns, _ in sends
    ])
    write_json(directory / "producer_timing.json", {
        "clock": clock_identity(), "target_events_per_second": args.rate,
        "publish_complete_ns": publish_complete_ns,
    })
    print(f"Kafka acknowledged {len(sends)} sends for {args.run_id}.", flush=True)


if __name__ == "__main__":
    main()
