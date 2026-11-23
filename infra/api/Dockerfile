FROM python:3.12-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN grep -E '^(cassandra-driver|fastapi|uvicorn)==' requirements.txt > /tmp/serving-requirements.txt \
    && python -m pip install --no-cache-dir -r /tmp/serving-requirements.txt
COPY src/serving/api.py /app/src/serving/api.py
COPY src/streaming/cassandra_sink.py /app/src/streaming/cassandra_sink.py
COPY src/streaming/selected_replay.py /app/src/streaming/selected_replay.py
CMD ["python", "-m", "uvicorn", "src.serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
