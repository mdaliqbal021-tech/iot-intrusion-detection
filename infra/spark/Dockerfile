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
WORKDIR /app
COPY src/streaming/spark_predict.py src/streaming/cassandra_sink.py /app/src/streaming/
ENTRYPOINT ["spark-submit", "--master", "local[2]", "--conf", "spark.ui.enabled=false", "--conf", "spark.jars.ivy=/tmp/spark-ivy", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7", "/app/src/streaming/spark_predict.py"]
