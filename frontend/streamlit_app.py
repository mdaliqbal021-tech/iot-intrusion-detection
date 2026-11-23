"""Select recorded IoT traffic for replay and inspect Cassandra results."""

import os
import shlex
from datetime import datetime
from urllib.parse import quote
from uuid import uuid4

import requests
import streamlit as st

from frontend.results_views import show_explanation, show_model_results

API_BASE_URL = os.environ.get("IOT_API_BASE_URL", "http://127.0.0.1:18080").rstrip("/")
PAGE_SIZE = 25
CLASS_LABELS = {0: "Benign", 1: "Suspected attack"}
ACTIVE_STATUSES = {"queued", "processing"}


def request_json(path, payload=None, on_rejected=None):
    try:
        response = requests.request(
            "POST" if payload is not None else "GET", f"{API_BASE_URL}{path}",
            json=payload, timeout=(2, 4),
        )
        if response.status_code in (404, 409, 422):
            if on_rejected is not None:
                on_rejected()
            st.warning(response.json()["detail"] if response.status_code != 422 else "The replay request is invalid.")
            return None
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        st.error("The replay API or Cassandra is unavailable. Check the IoT services and try Refresh.")
    except (ValueError, KeyError):
        st.error("The replay API returned an invalid response.")
    return None


def technical_details(run_id, event_id, job=None):
    escaped_run = run_id.replace("'", "''")
    query = (
        "SELECT run_id, replay_id, attack_score, predicted_class FROM iot_replay.predictions "
        f"WHERE run_id = '{escaped_run}' AND replay_id = {int(event_id)};"
    )
    with st.expander("Technical details"):
        if job is not None:
            st.markdown(f"**Source-record ID** `{job['source_record_id']}`")
            if job.get("dataset_sha256"):
                st.caption("Held-out test CSV SHA-256")
                st.code(job["dataset_sha256"], language=None)
            timings = job.get("timings_ms", {})
            for key, description in {
                "kafka_publish": "Kafka producer through acknowledged publish",
                "spark_job": "Spark startup, Kafka read, scoring, Cassandra write and shutdown",
                "worker_total": "Worker processing through Cassandra verification and run registration",
            }.items():
                if key in timings:
                    st.write(f"**{timings[key] / 1_000:.3f} s** · {description}")
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
    label = job.get("recorded_label")
    if label:
        agrees = is_attack == (label != "BenignTraffic")
        agreement = "Agrees with the recorded label" if agrees else "Differs from the recorded label"
        st.markdown(f"**Recorded label** `{label}` · **{agreement}**")
    st.caption(f"Model · {job['model_version']}")
    technical_details(job["run_id"], row["replay_id"], job)


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
    technical_details(run_id, event)


def traffic_label(recorded_label):
    family = recorded_label.split("-", 1)[0]
    return {"BenignTraffic": "Benign traffic", "DDoS": "DDoS traffic", "Mirai": "Mirai traffic"}.get(
        family, recorded_label,
    )


def select_input_record():
    st.session_state.saved_run_choice = None


def schedule_replay(source_record_id, dataset_sha256):
    current = st.session_state.get("active_replay")
    if current and current["status"] in ACTIVE_STATUSES:
        return
    if "pending_submission" not in st.session_state:
        st.session_state.pending_submission = {
            "source_record_id": source_record_id, "dataset_sha256": dataset_sha256,
            "request_id": str(uuid4()),
        }
    select_input_record()
    st.session_state.send_pending = True


def show_record_selector(catalog):
    examples = {
        example["source_record_id"]: f"Example {i} · {traffic_label(example['recorded_label'])}"
        for i, example in enumerate(catalog["examples"], start=1)
    }
    pending = st.session_state.get("pending_submission")
    example = st.selectbox(
        "Choose a test record", list(examples), format_func=examples.__getitem__,
        disabled=pending is not None, on_change=select_input_record,
    )
    st.caption("Example names describe recorded labels.")
    with st.expander(f"Browse all {catalog['count']:,} test records", expanded=False):
        use_browser = st.checkbox("Use the record number below", disabled=pending is not None,
                                  on_change=select_input_record)
        record_number = st.number_input(
            "Record number", min_value=1, max_value=catalog["count"], step=1,
            disabled=pending is not None, on_change=select_input_record,
        )
    source_id = record_number - 1 if use_browser else example
    if pending is not None:
        source_id = pending["source_record_id"]
    selection = "Full-record browser" if use_browser else examples[example]
    st.markdown(f"**Will replay Record {source_id + 1:,}** · {selection}")

    record = request_json(f"/test-records/{source_id}")
    if record is None:
        return
    st.caption(f"Recorded label · {record['recorded_label']}")
    current = st.session_state.get("active_replay")
    st.button(
        "Retry submission" if pending else "Start replay", type="primary", width="stretch",
        disabled=bool(current and current["status"] in ACTIVE_STATUSES),
        on_click=schedule_replay, args=(source_id, catalog["dataset_sha256"]),
    )
    if pending:
        st.caption("Retry checks the same submission and keeps its original run ID.")
    return record


def invalidate_history():
    st.session_state.pop("completed_runs", None)
    st.session_state.pop("prediction_history", None)


def show_prediction_history():
    st.subheader("Prediction history")
    if "prediction_history" not in st.session_state:
        history = request_json("/history")
        if history is None:
            return
        st.session_state.prediction_history = history
    history = st.session_state.prediction_history
    if not history:
        st.info("No completed runs yet. Verified Cassandra results will appear here.")
        return

    rows = []
    for entry in history:
        prediction = entry.get("prediction", {})
        source_id = entry.get("source_record_id")
        rows.append({
            "Run": entry["display_number"],
            "Completed (UTC)": datetime.fromisoformat(entry["completed_at"]).strftime("%d %b %H:%M:%S"),
            "Test record": f"Record {source_id + 1:,}" if source_id is not None else "—",
            "Recorded label": entry.get("recorded_label") or "—",
            "Prediction": CLASS_LABELS.get(prediction.get("predicted_class"), "Open completed run"),
            "Attack score": prediction.get("attack_score"),
            "Events": entry["prediction_count"],
        })
    st.caption("Latest 10 completed runs. Use Completed run in the sidebar to open any saved result.")
    st.dataframe(rows, hide_index=True, column_config={
        "Attack score": st.column_config.NumberColumn(format="%.8f"),
    })


st.set_page_config(page_title="IoT traffic replay", layout="wide")
st.title("IoT traffic replay")
st.write("This system passively screens recorded CICIoT2023 network flows for suspected attacks.")

with st.sidebar:
    view = st.radio("View", ["Replay", "Explain prediction", "Model results"])
    st.header("Replay controls")
    if st.session_state.pop("send_pending", False):
        submitted = request_json(
            "/replays", st.session_state.pending_submission,
            on_rejected=lambda: st.session_state.pop("pending_submission", None),
        )
        if submitted is not None:
            st.session_state.pop("pending_submission")
            st.session_state.active_replay = submitted

    refresh = st.button("Refresh", width="stretch", on_click=invalidate_history)
    catalog = request_json("/test-records")
    record = show_record_selector(catalog) if catalog is not None else None
    st.divider()
    st.subheader("Saved results")
    if catalog is not None and "completed_runs" not in st.session_state:
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
                            format_func=saved_labels.__getitem__, placeholder="Select a completed run",
                            key="saved_run_choice")


def show_run_explanation(run_id):
    explanation = request_json(f"/runs/{quote(run_id, safe='')}/explanation")
    if explanation is not None:
        show_explanation(explanation, CLASS_LABELS)


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
    job = st.session_state.get("active_replay")
    show_current = view != "Model results" and not saved_run
    matches_selection = job and record and record["source_record_id"] == job["source_record_id"]
    if show_current and view == "Replay":
        st.subheader("Selected record replay")
    if job and job["status"] in ACTIVE_STATUSES:
        updated = request_json(f"/replays/{quote(job['run_id'], safe='')}")
        if updated is None:
            if show_current and matches_selection:
                st.caption(f"Last known status · {job['status'].title()}")
            return
        st.session_state.active_replay = job = updated
        if job["status"] not in ACTIVE_STATUSES:
            invalidate_history()
            st.rerun()

    if not show_current:
        return
    if not matches_selection:
        st.info("Start a replay for the selected flow, or choose a completed run in the sidebar.")
        return

    st.caption(f"Record {job['source_record_id'] + 1:,}")
    if job["status"] == "completed":
        if view == "Explain prediction":
            show_run_explanation(job["run_id"])
            return
        result = load_result(job["run_id"])
        if result is not None:
            show_record_prediction(job, result["predictions"][0])
    elif job["status"] == "failed":
        st.error(f"Failed · {job['stage']}. {job['error']}")
    else:
        st.info(f"{job['status'].title()} · {job['stage']}")
    if job["status"] != "completed":
        with st.expander("Technical details"):
            st.write(f"Source-record ID · {job['source_record_id']}  |  Run ID · {job['run_id']}")


if view == "Replay" and record is not None:
    st.subheader("Selected flow")
    st.write(f"**Record {record['source_record_id'] + 1:,}** · Recorded label **{record['recorded_label']}**")
    preview = record["features"]
    st.caption(" · ".join(f"{key} {preview[key]:.5g}" for key in ("Protocol Type", "Duration", "Rate")))
    with st.expander(f"Ordered model features ({len(preview)})"):
        st.dataframe(
            [{"Feature": name, "Value": value} for name, value in preview.items()],
            hide_index=True, height=260,
        )

if view == "Explain prediction":
    st.header("Explain prediction")
    st.caption("Individual replay · native XGBoost feature contributions")
show_current_replay()
if view == "Model results":
    show_model_results()
elif view == "Explain prediction" and saved_run:
    st.caption("Completed run selected in the sidebar")
    show_run_explanation(saved_run)
elif view == "Replay":
    if saved_run:
        st.subheader("Completed run")
        result = load_result(saved_run)
        if result is not None:
            job = request_json(f"/replays/{quote(saved_run, safe='')}") if saved_run.startswith("selected.") else None
            if job is not None:
                st.caption(f"Record {job['source_record_id'] + 1:,}")
                show_record_prediction(job, result["predictions"][0])
            else:
                st.caption(saved_run)
                show_saved_batch(result["predictions"], saved_run)
    if catalog is not None:
        show_prediction_history()
