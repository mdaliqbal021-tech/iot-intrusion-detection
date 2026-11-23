#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

cassandra=false
mode=demo
timing_rate=""
for argument in "$@"; do
    case "$argument" in
        --cassandra) cassandra=true ;;
        --timing-rate=) printf 'Specify a positive timing rate.\n' >&2; exit 2 ;;
        --timing-rate=*) timing_rate=${argument#*=} ;;
        --smoke|--full)
            if [[ $mode != demo ]]; then
                printf 'Select only one replay size.\n' >&2
                exit 2
            fi
            mode=${argument#--}
            ;;
        *) printf 'Usage: %s [--cassandra] [--smoke | --full] [--timing-rate=EVENTS_PER_SECOND]\n' "$0" >&2; exit 2 ;;
    esac
done

if [[ -n $timing_rate ]] && { ! $cassandra || [[ ! $timing_rate =~ ^[1-9][0-9]*$ ]]; }; then
    printf 'Timing requires --cassandra and a positive integer rate.\n' >&2
    exit 2
fi

python=${REPLAY_PYTHON:-.venv/replay-310/bin/python}
"$python" -c 'import sys; print(f"Host Python: {sys.executable} ({sys.version.split()[0]})"); sys.exit("Replay requires host Python 3.10") if sys.version_info[:2] != (3, 10) else None'

models=data/models
input=data/subsets/ciciot2023_test_stratified_25000_seed42.csv
rows=1000
case "$mode" in
    smoke) input="$models/xgboost_final_check_input.csv"; rows=32 ;;
    full) rows=25000 ;;
esac

mkdir -p data/replay
REPLAY_RUN_DIR=$(mktemp -d "$PWD/data/replay/run.XXXXXXXX")
export REPLAY_RUN_DIR
run_id=${REPLAY_RUN_DIR##*/}
topic="iot-replay-${run_id#run.}"
compose=(docker compose -f infra/compose.yaml)
services=(kafka)
sink_args=()
if $cassandra; then
    services+=(cassandra)
    sink_args=(--cassandra-run-id "$run_id")
fi

printf 'Replay mode: %s (%s rows)\n' "$mode" "$rows"
"$python" -m src.streaming.replay prepare "$input" "$REPLAY_RUN_DIR/input.jsonl" \
    --rows "$rows" --metadata "$models/xgboost_final_metadata.json"
"${compose[@]}" up -d --wait "${services[@]}"
if $cassandra; then
    "${compose[@]}" exec -T cassandra cqlsh 127.0.0.1 -f /schema/init.cql
fi
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --topic "$topic" --partitions 2 --replication-factor 1
if [[ -n $timing_rate ]]; then
    # Prepare the image before publication so the paced measurements exclude build time.
    "${compose[@]}" build spark
    "${compose[@]}" run --rm --no-deps spark --timing-producer "$topic" --run-id "$run_id" --rate "$timing_rate"
    "$python" -m src.streaming.replay_timing "$REPLAY_RUN_DIR"
    "${compose[@]}" run --rm --no-deps spark "$topic" "${sink_args[@]}" --record-timing
else
    "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
        --topic "$topic" --producer-property acks=all < "$REPLAY_RUN_DIR/input.jsonl"

    "${compose[@]}" run --rm --build --no-deps spark "$topic" "${sink_args[@]}"
fi
spark_output="$REPLAY_RUN_DIR/predictions"
expected="$spark_output"
if [[ $mode == smoke ]]; then
    expected="$models/xgboost_final_check_expected.csv"
    "$python" -m src.streaming.replay compare "$expected" "$spark_output" --rows "$rows"
else
    "$python" -m src.streaming.replay check-ids "$spark_output" --rows "$rows"
fi

if $cassandra; then
    attempts=(first)
    if [[ $mode == smoke ]]; then
        attempts+=(retry)
    fi
    for attempt in "${attempts[@]}"; do
        if [[ $attempt == retry ]]; then
            mkdir "$REPLAY_RUN_DIR/retry"
            "${compose[@]}" run --rm --no-deps spark "$topic" "${sink_args[@]}" --run-dir /replay/retry
            "$python" -m src.streaming.replay compare "$expected" "$REPLAY_RUN_DIR/retry/predictions" --rows "$rows"
        fi
        snapshot="$REPLAY_RUN_DIR/cassandra-$attempt"
        mkdir "$snapshot"
        "${compose[@]}" run -T --rm --no-deps --entrypoint python3.10 spark \
            -m src.streaming.cassandra_sink "$run_id" > "$snapshot/batch-0.jsonl"
        printf 'Cassandra %s write for %s\n' "$attempt" "$run_id"
        "$python" -m src.streaming.replay compare "$expected" "$snapshot" --rows "$rows"
    done
    "${compose[@]}" run -T --rm --no-deps --entrypoint python3.10 spark \
        -m src.streaming.cassandra_sink "$run_id" --complete --expected-count "$rows"
fi
printf 'Replay files: %s\n' "$REPLAY_RUN_DIR"
