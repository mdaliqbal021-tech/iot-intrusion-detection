"""Read stored IoT replay predictions over HTTP."""

import os
from contextlib import asynccontextmanager
from threading import Lock

from cassandra import OperationTimedOut, ReadTimeout, Unavailable
from cassandra.cluster import Cluster, NoHostAvailable, UnresolvableContactPoints
from cassandra.connection import ConnectionException
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

SELECT_PREDICTIONS = """
SELECT replay_id, attack_score, predicted_class
FROM iot_replay.predictions WHERE run_id = ? ORDER BY replay_id ASC
"""


class Prediction(BaseModel):
    replay_id: int
    attack_score: float
    predicted_class: int


class RunPredictions(BaseModel):
    run_id: str
    predictions: list[Prediction]


class PredictionStore:
    def __init__(self, host):
        self.host = host
        self.lock = Lock()
        self.cluster = None
        self.session = None
        self.select_predictions = None

    def connect(self):
        with self.lock:
            if self.session is None:
                cluster = Cluster([self.host], protocol_version=4, connect_timeout=3)
                try:
                    session = cluster.connect()
                except Exception:
                    cluster.shutdown()
                    raise
                self.cluster, self.session = cluster, session
            return self.session

    def health(self):
        self.connect().execute("SELECT release_version FROM system.local", timeout=3)

    def predictions(self, run_id):
        session = self.connect()
        with self.lock:
            if self.select_predictions is None:
                self.select_predictions = session.prepare(SELECT_PREDICTIONS)
        return [row._asdict() for row in session.execute(self.select_predictions, (run_id,), timeout=3)]

    def close(self):
        if self.cluster is not None:
            self.cluster.shutdown()


@asynccontextmanager
async def lifespan(app):
    app.state.store = PredictionStore(os.environ.get("CASSANDRA_HOST", "cassandra"))
    try:
        yield
    finally:
        app.state.store.close()


app = FastAPI(title="IoT replay predictions", lifespan=lifespan)


@app.exception_handler(NoHostAvailable)
@app.exception_handler(UnresolvableContactPoints)
@app.exception_handler(ConnectionException)
@app.exception_handler(OperationTimedOut)
@app.exception_handler(ReadTimeout)
@app.exception_handler(Unavailable)
async def cassandra_unavailable(request: Request, exc: Exception):
    return JSONResponse(status_code=503, content={"detail": "Cassandra unavailable"})


@app.get("/health")
def health(request: Request):
    request.app.state.store.health()
    return {"status": "ok"}


@app.get("/runs/{run_id}/predictions", response_model=RunPredictions)
def run_predictions(run_id: str, request: Request):
    predictions = request.app.state.store.predictions(run_id)
    if not predictions:
        raise HTTPException(status_code=404, detail="Replay run not found")
    return {"run_id": run_id, "predictions": predictions}
