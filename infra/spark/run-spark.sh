#!/bin/sh
set -eu
entry=/app/src/streaming/spark_predict.py
if [ "${1:-}" = "--timing-producer" ]; then
    entry=/app/src/streaming/timed_producer.py
    shift
fi
jars=$(find /opt/iot-kafka-jars -name '*.jar' -type f -printf '%p\n' | sort | paste -sd, -)
test -n "$jars"
exec spark-submit --master 'local[2]' --conf spark.ui.enabled=false \
    --jars "$jars" "$entry" "$@"
