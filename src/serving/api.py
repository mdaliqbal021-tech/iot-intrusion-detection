"""Select held-out IoT records and read stored replay predictions over HTTP."""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from re import fullmatch
from threading import Lock

from cassandra import OperationTimedOut, ReadTimeout, Unavailable
from cassandra.cluster import Cluster, NoHostAvailable, UnresolvableContactPoints
from cassandra.connection import ConnectionException
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, UUID4

from src.streaming.cassandra_sink import RUNS, number_existing_runs
from src.streaming.replay import verify_contributions
from src.streaming.selected_replay import MODEL_NAME, TEST_CSV, ReplayConflict, TestRecords, read_json

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


class CompletedRun(BaseModel):
    run_id: str
    display_number: int
    completed_at: datetime
    prediction_count: int


class ReplaySelection(BaseModel):
    source_record_id: int = Field(ge=0, strict=True)
    request_id: UUID4 | None = None
    dataset_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


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

    def completed_runs(self):
        session = self.connect()
        rows = list(session.execute(RUNS, timeout=3))
        if number_existing_runs(session, rows):
            rows = list(session.execute(RUNS, timeout=3))
        ordered = sorted(rows, key=lambda row: (row.completed_at, row.run_id), reverse=True)
        return [
            CompletedRun(
                run_id=row.run_id,
                display_number=row.display_number,
                completed_at=row.completed_at.replace(tzinfo=timezone.utc),
                prediction_count=row.prediction_count,
            )
            for row in ordered
        ]

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
    app.state.jobs = Path(os.environ.get("IOT_REPLAY_JOBS", "data/replay/selected"))
    app.state.test_records = TestRecords(
        Path(os.environ.get("IOT_TEST_CSV", f"data/subsets/{TEST_CSV}")),
        Path(os.environ.get("IOT_MODEL_METADATA", "data/models/xgboost_final_metadata.json")),
    )
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


@app.get("/runs", response_model=list[CompletedRun])
def completed_runs(request: Request):
    return request.app.state.store.completed_runs()


@app.get("/test-records")
def test_records(request: Request):
    return request.app.state.test_records.catalog()


@app.get("/test-records/{source_record_id}")
def test_record(source_record_id: int, request: Request):
    try:
        return request.app.state.test_records.record(source_record_id)
    except IndexError:
        raise HTTPException(status_code=404, detail="Test record ID is outside the held-out subset.")


@app.post("/replays", status_code=202)
def start_replay(selection: ReplaySelection, request: Request):
    try:
        return request.app.state.test_records.enqueue(
            request.app.state.jobs, selection.source_record_id, selection.request_id, selection.dataset_sha256,
        )
    except IndexError:
        raise HTTPException(status_code=404, detail="Test record ID is outside the held-out subset.")
    except ReplayConflict as error:
        raise HTTPException(status_code=409, detail=str(error))


@app.get("/history")
def prediction_history(request: Request):
    store = request.app.state.store
    history = []
    for run in store.completed_runs()[:10]:
        entry = run.model_dump()
        if fullmatch(r"selected\.[0-9a-f]{32}", run.run_id):
            try:
                job = read_json(request.app.state.jobs / run.run_id / "status.json")
            except FileNotFoundError:
                job = {}
            predictions = store.predictions(run.run_id)
            if len(predictions) == 1:
                entry.update(
                    source_record_id=job.get("source_record_id"), recorded_label=job.get("recorded_label"),
                    prediction=predictions[0],
                )
        history.append(entry)
    return history


@app.get("/replays/{run_id}")
def replay_status(run_id: str, request: Request):
    if not fullmatch(r"selected\.[0-9a-f]{32}", run_id):
        raise HTTPException(status_code=404, detail="Replay status is unavailable for this run.")
    try:
        return read_json(request.app.state.jobs / run_id / "status.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Replay run not found.")


@app.get("/runs/{run_id}/explanation")
def run_explanation(run_id: str, request: Request):
    if not fullmatch(r"selected\.[0-9a-f]{32}", run_id):
        raise HTTPException(status_code=404, detail="This older batch run lacks the captured row and model provenance needed for a faithful explanation.")
    job = replay_status(run_id, request)
    if job["status"] != "completed":
        raise HTTPException(status_code=409, detail="An explanation is available after Cassandra verifies the completed replay.")
    try:
        explanation = read_json(request.app.state.jobs / run_id / "explanation.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="This replay predates verified explanations or its captured explanation is missing. Replay the record to capture one.")
    except ValueError:
        raise HTTPException(status_code=409, detail="The captured explanation could not be read.")
    if explanation.get("unavailable"):
        raise HTTPException(status_code=409, detail=explanation["unavailable"])

    predictions = request.app.state.store.predictions(run_id)
    try:
        prediction, = predictions
        if (explanation["run_id"] != run_id or explanation["replay_id"] != prediction["replay_id"]
                or explanation["source_record_id"] != job["source_record_id"]
                or explanation["threshold"] != job["threshold"]
                or int(prediction["attack_score"] >= explanation["threshold"]) != prediction["predicted_class"]):
            raise ValueError("Explanation keys do not match the stored prediction.")
        reconstructed = verify_contributions(explanation, prediction["attack_score"])
        model_version = f"{MODEL_NAME} (exported with XGBoost {explanation['model_export_version']})"
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=409, detail="The captured explanation could not be verified against the stored prediction.")
    return {
        **explanation, **prediction, "reconstructed_attack_score": reconstructed,
        "recorded_label": job.get("recorded_label"), "model_version": model_version,
    }
