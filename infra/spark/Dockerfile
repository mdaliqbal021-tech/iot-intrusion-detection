FROM apache/kafka:3.9.1 AS kafka
FROM python:3.10-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless libgomp1 procps \
    && rm -rf /var/lib/apt/lists/*
RUN python3.10 --version \
    && python3.10 -m pip install --no-cache-dir pyspark==3.5.7 numpy==2.2.6 pandas==2.3.3 xgboost-cpu==3.2.0 scikit-learn==1.7.2
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.10 \
    PYSPARK_PYTHON=/usr/local/bin/python3.10 \
    PYTHONPATH=/app
RUN python3.10 -m pip install --no-cache-dir cassandra-driver==3.29.3

# Resolve the connector and its dependencies once while building the image.
RUN printf 'pass\n' > /tmp/resolve-kafka.py \
    && SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[1]' \
        --conf spark.jars.ivy=/tmp/spark-ivy \
        --repositories https://repo.maven.apache.org/maven2 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /tmp/resolve-kafka.py \
    && mv /tmp/spark-ivy/jars /opt/iot-kafka-jars \
    && rm -rf /tmp/spark-ivy /tmp/resolve-kafka.py
COPY --from=kafka /opt/kafka /opt/kafka
WORKDIR /app
COPY src/streaming/ /app/src/streaming/
COPY --chmod=755 infra/spark/run-spark.sh /usr/local/bin/iot-spark-replay
ENTRYPOINT ["/usr/local/bin/iot-spark-replay"]
