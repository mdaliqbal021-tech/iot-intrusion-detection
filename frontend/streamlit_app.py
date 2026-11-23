"""Select recorded IoT traffic for replay and inspect Cassandra results."""

import json
import os
import shlex
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests
import streamlit as st

API_BASE_URL = os.environ.get("IOT_API_BASE_URL", "http://127.0.0.1:18080").rstrip("/")
FIGURES = Path(__file__).resolve().parents[1] / "figures"
PAGE_SIZE = 25
CLASS_LABELS = {0: "Benign", 1: "Suspected attack"}
ACTIVE_STATUSES = {"queued", "processing"}


def request_json(path, payload=None):
    try:
        response = requests.request(
            "POST" if payload is not None else "GET", f"{API_BASE_URL}{path}",
            json=payload, timeout=(2, 4),
        )
        if response.status_code in (404, 422):
            st.warning(response.json()["detail"] if response.status_code == 404 else "The replay request is invalid.")
            return None
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        st.error("The replay API or Cassandra is unavailable. Check the IoT services and try Refresh.")
    except (ValueError, KeyError):
        st.error("The replay API returned an invalid response.")
    return None


def cassandra_proof(run_id, event_id):
    escaped_run = run_id.replace("'", "''")
    query = (
        "SELECT run_id, replay_id, attack_score, predicted_class FROM iot_replay.predictions "
        f"WHERE run_id = '{escaped_run}' AND replay_id = {int(event_id)};"
    )
    with st.expander("Cassandra proof"):
        st.markdown("**Keyspace** `iot_replay` · **Table** `predictions`")
        st.markdown(f"**Partition key** `run_id = {run_id}` · **Clustering key** `replay_id = {event_id}`")
        st.caption("Run from the repository root in WSL.")
        st.code(f"docker compose -f infra/compose.yaml exec -T cassandra cqlsh -e {shlex.quote(query)}", language="bash")


def show_record_prediction(job, row):
    score = row["attack_score"]
    threshold = job["threshold"]
    is_attack = row["predicted_class"] == 1
    status, prediction, score_card, threshold_card = st.columns(4)
    status.metric("Replay status", "Completed", border=True)
    prediction.metric("Predicted class", CLASS_LABELS[row["predicted_class"]], border=True)
    score_card.metric("Attack score", f"{score:.8f}", border=True)
    threshold_card.metric("Decision threshold", f"{threshold:g}", border=True)

    st.write(
        f"The score of {score:.8f} was "
        f"{'at or above' if is_attack else 'below'} {threshold:g}, "
        f"so the model classified this record as {'a suspected attack' if is_attack else 'benign'}."
    )
    st.markdown(
        f"**Recorded label** `{job['recorded_label']}` · **Model** {job['model_version']}"
    )
    with st.expander("Replay identifiers and timing"):
        st.table({
            "Identifier": ["Source record ID", "Run ID", "Within-run event ID"],
            "Value": [str(job["source_record_id"]), job["run_id"], str(row["replay_id"])],
        }, hide_index=True)
        timings = job["timings_ms"]
        for key, description in {
            "kafka_publish": "Kafka producer process through acknowledged publish",
            "spark_job": "Spark process startup, Kafka read, scoring, Cassandra write and shutdown",
            "worker_total": "Worker processing through Cassandra read-back and run registration",
        }.items():
            if key in timings:
                st.write(f"**{timings[key] / 1_000:.3f} s** · {description}")
    cassandra_proof(job["run_id"], row["replay_id"])


def show_saved_batch(rows, run_id):
    benign = sum(row["predicted_class"] == 0 for row in rows)
    for column, label, value in zip(
        st.columns(3), ("Predictions", "Predicted benign", "Suspected attacks"),
        (len(rows), benign, len(rows) - benign), strict=True,
    ):
        column.metric(label, f"{value:,}", border=True)
    pages = (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE
    page = st.number_input("Page", 1, pages, key=f"page_{run_id}") if pages > 1 else 1
    start = (page - 1) * PAGE_SIZE
    shown = rows[start:start + PAGE_SIZE]
    st.caption(f"Showing predictions {start + 1:,}–{start + len(shown):,} of {len(rows):,}. "
               "The original CLI run stores within-run event IDs.")
    with st.expander("Attack scores on this page"):
        st.vega_lite_chart({
            "height": 220,
            "layer": [
                {
                    "data": {"values": shown},
                    "mark": {"type": "line", "point": True},
                    "encoding": {
                        "x": {"field": "replay_id", "type": "quantitative", "title": "Within-run event ID",
                              "scale": {"zero": False}, "axis": {"tickMinStep": 1}},
                        "y": {"field": "attack_score", "type": "quantitative", "title": "Attack score",
                              "scale": {"domain": [0, 1]}},
                    },
                },
                {
                    "data": {"values": [{"threshold": 0.5}]},
                    "mark": {"type": "rule", "strokeDash": [6, 4], "color": "#e68175"},
                    "encoding": {"y": {"field": "threshold", "type": "quantitative"}},
                },
            ],
        })
        st.caption("The dashed line shows the deployed 0.5 threshold.")
    st.dataframe([
        {"Within-run event ID": row["replay_id"], "Attack score": row["attack_score"],
         "Predicted class": CLASS_LABELS[row["predicted_class"]]} for row in shown
    ], hide_index=True)
    event_ids = [row["replay_id"] for row in shown]
    event = st.selectbox("Event for Cassandra verification", event_ids, key=f"event_{run_id}")
    cassandra_proof(run_id, event)


def show_model_comparison():
    st.subheader("Model comparison")
    with st.expander("Validation ROC and model results"):
        st.caption("25,000 validation rows · threshold 0.5 for class predictions · notebooks 05 and 08")
        try:
            rows = json.loads((FIGURES / "validation_model_comparison.json").read_text())
            st.dataframe(rows, hide_index=True, column_config={
                field: st.column_config.NumberColumn(format="percent") for field in
                ("Accuracy", "Balanced accuracy", "Attack recall", "Benign false-positive rate")
            } | {"ROC-AUC": st.column_config.NumberColumn(format="%.6f")})
            st.image(str(FIGURES / "validation_roc.png"), width="stretch")
        except OSError:
            st.warning("The saved notebook 05 comparison artifacts are unavailable.")
    with st.expander("Recorded inference benchmark"):
        st.caption("Notebook 06 · median time per whole batch on two CPU threads · local probability prediction")
        image = FIGURES / "inference_speed.png"
        if image.is_file():
            st.image(str(image), width="stretch")
        else:
            st.warning("The saved notebook 06 speed figure is unavailable.")


st.set_page_config(page_title="IoT traffic replay", layout="wide")
st.title("IoT traffic replay")
st.write("Replay a recorded CICIoT2023 traffic record and see whether the deployed XGBoost model flags it as a suspected attack.")

with st.sidebar:
    st.header("Replay controls")
    refresh = st.button("Refresh", width="stretch")
    if refresh:
        st.session_state.pop("completed_runs", None)
    catalog = request_json("/test-records")
    if catalog is not None:
        source_record_id = st.number_input(
            "Test source record ID", min_value=0, max_value=catalog["count"] - 1, step=1,
            help="Zero-based row position in the fixed seed-42 held-out CSV.",
        )
        record = request_json(f"/test-records/{source_record_id}")
        if record is not None:
            st.write(f"Recorded label **{record['recorded_label']}**")
            with st.expander("Preview model inputs"):
                st.dataframe(
                    [{"Feature": name, "Value": value} for name, value in record["features"].items()],
                    hide_index=True, height=250,
                )
            current = st.session_state.get("active_replay")
            if st.button("Start replay", type="primary", width="stretch",
                         disabled=bool(current and current["status"] in ACTIVE_STATUSES)):
                submitted = request_json("/replays", {"source_record_id": source_record_id})
                if submitted is not None:
                    st.session_state.active_replay = submitted
                    st.rerun()

    st.divider()
    st.subheader("Saved results")
    if "completed_runs" not in st.session_state:
        runs = request_json("/runs")
        if runs is not None:
            st.session_state.completed_runs = runs
    runs = st.session_state.get("completed_runs", [])
    saved_labels = {
        row["run_id"]: (
            f"Run {row['display_number']} · "
            f"{datetime.fromisoformat(row['completed_at']):%d %b %H:%M UTC} · "
            f"{row['prediction_count']:,} predictions"
        )
        for row in runs
    }
    saved_run = st.selectbox("Completed run", list(saved_labels), index=None,
                            format_func=saved_labels.__getitem__, placeholder="Select a completed run")


def load_result(run_id):
    cached = st.session_state.get("loaded_result")
    if cached is None or cached["run_id"] != run_id or refresh:
        cached = request_json(f"/runs/{quote(run_id, safe='')}/predictions")
        if cached is not None:
            st.session_state.loaded_result = cached
    return cached


current = st.session_state.get("active_replay")
poll_interval = 2 if current and current["status"] in ACTIVE_STATUSES else None


@st.fragment(run_every=poll_interval)
def show_current_replay():
    st.subheader("Selected record replay")
    job = st.session_state.get("active_replay")
    if job is None:
        st.info("Choose a held-out test record in the sidebar and start a replay.")
        return

    if job["status"] in ACTIVE_STATUSES:
        updated = request_json(f"/replays/{quote(job['run_id'], safe='')}")
        if updated is None:
            st.caption(f"Last known status · {job['status'].title()} · {job['run_id']}")
            return
        st.session_state.active_replay = job = updated
        if job["status"] not in ACTIVE_STATUSES:
            st.session_state.pop("completed_runs", None)
            st.rerun()

    st.caption(f"Source record {job['source_record_id']:,} · event {job['replay_id']} · run {job['run_id']}")
    if job["status"] == "completed":
        result = load_result(job["run_id"])
        if result is not None:
            show_record_prediction(job, result["predictions"][0])
    elif job["status"] == "failed":
        st.error(f"Failed · {job['stage']}. {job['error']}")
    else:
        st.info(f"{job['status'].title()} · {job['stage']}")


show_current_replay()
if saved_run and (current is None or saved_run != current["run_id"]):
    st.subheader("Completed run")
    result = load_result(saved_run)
    if result is not None:
        if saved_run.startswith("selected."):
            job = request_json(f"/replays/{quote(saved_run, safe='')}")
            if job is not None:
                st.caption(f"Source record {job['source_record_id']:,} · event {job['replay_id']} · run {saved_run}")
                show_record_prediction(job, result["predictions"][0])
            else:
                show_saved_batch(result["predictions"], saved_run)
        else:
            st.caption(saved_run)
            show_saved_batch(result["predictions"], saved_run)
elif st.session_state.get("completed_runs") == []:
    st.caption("No completed runs yet. New runs appear here once their Cassandra results are verified.")
show_model_comparison()
