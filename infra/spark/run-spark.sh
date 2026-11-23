#!/bin/sh
set -eu
jars=$(find /opt/iot-kafka-jars -name '*.jar' -type f -printf '%p\n' | sort | paste -sd, -)
test -n "$jars"
exec spark-submit --master 'local[2]' --conf spark.ui.enabled=false \
    --jars "$jars" /app/src/streaming/spark_predict.py "$@"
