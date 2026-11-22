#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if (( $# > 1 )) || [[ ${1:-} != "" && $1 != "--cassandra" ]]; then
    printf 'Usage: %s [--cassandra]\n' "$0" >&2
    exit 2
fi

python=${REPLAY_PYTHON:-.venv/replay-310/bin/python}
"$python" -c 'import sys; print(f"Host Python: {sys.executable} ({sys.version.split()[0]})"); sys.exit("Replay requires host Python 3.10") if sys.version_info[:2] != (3, 10) else None'

mkdir -p data/replay
REPLAY_RUN_DIR=$(mktemp -d "$PWD/data/replay/run.XXXXXXXX")
export REPLAY_RUN_DIR
run_id=${REPLAY_RUN_DIR##*/}
topic="iot-replay-${run_id#run.}"
compose=(docker compose -f infra/compose.yaml)
models=data/models
expected="$models/xgboost_final_check_expected.csv"
services=(kafka)
sink_args=()
if [[ ${1:-} == "--cassandra" ]]; then
    services+=(cassandra)
    sink_args=(--cassandra-run-id "$run_id")
fi

"$python" -m src.streaming.replay prepare "$models/xgboost_final_check_input.csv" "$REPLAY_RUN_DIR/input.jsonl"
"${compose[@]}" up -d --wait "${services[@]}"
if [[ ${1:-} == "--cassandra" ]]; then
    "${compose[@]}" exec -T cassandra cqlsh 127.0.0.1 -f /schema/init.cql
fi
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --topic "$topic" --partitions 2 --replication-factor 1
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
    --topic "$topic" --producer-property acks=all < "$REPLAY_RUN_DIR/input.jsonl"
"${compose[@]}" run --rm --build --no-deps spark "$topic" "${sink_args[@]}"
printf 'Spark output for %s\n' "$run_id"
"$python" -m src.streaming.replay compare "$expected" "$REPLAY_RUN_DIR/predictions"

if [[ ${1:-} == "--cassandra" ]]; then
    for attempt in first retry; do
        if [[ $attempt == retry ]]; then
            mkdir "$REPLAY_RUN_DIR/retry"
            "${compose[@]}" run --rm --no-deps spark "$topic" "${sink_args[@]}" --run-dir /replay/retry
            "$python" -m src.streaming.replay compare "$expected" "$REPLAY_RUN_DIR/retry/predictions"
        fi
        snapshot="$REPLAY_RUN_DIR/cassandra-$attempt"
        mkdir "$snapshot"
        "${compose[@]}" run -T --rm --no-deps --entrypoint python3.10 spark \
            -m src.streaming.cassandra_sink "$run_id" > "$snapshot/batch-0.jsonl"
        printf 'Cassandra %s write for %s\n' "$attempt" "$run_id"
        "$python" -m src.streaming.replay compare "$expected" "$snapshot"
    done
fi
printf 'Replay files: %s\n' "$REPLAY_RUN_DIR"
