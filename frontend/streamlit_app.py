"""Review predictions from a saved IoT replay run."""

import os
from datetime import datetime
from urllib.parse import quote

import requests
import streamlit as st

API_BASE_URL = os.environ.get("IOT_API_BASE_URL", "http://127.0.0.1:18080").rstrip("/")
THRESHOLD = 0.5
PAGE_SIZE = 25
CLASS_LABELS = {0: "Benign (0)", 1: "Attack (1)"}


def request_json(path):
    try:
        response = requests.get(f"{API_BASE_URL}{path}", timeout=5)
        if response.status_code == 404 and path != "/runs":
            st.warning("No predictions found for this run ID.")
            return None
        response.raise_for_status()
    except requests.RequestException:
        st.error("The predictions API is unavailable. Try again when the IoT services are running.")
        return None

    try:
        return response.json()
    except ValueError:
        st.error("The predictions API returned an invalid response.")
        return None


def show_predictions(rows, run_id):
    benign = sum(row["predicted_class"] == 0 for row in rows)
    attack = sum(row["predicted_class"] == 1 for row in rows)
    total_column, benign_column, attack_column = st.columns(3)
    total_column.metric("Total predictions", len(rows), border=True)
    benign_column.metric("Predicted benign", benign, border=True)
    attack_column.metric("Predicted attack", attack, border=True)

    total = len(rows)
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page = st.number_input("Page", min_value=1, max_value=pages, step=1, key=f"page_{run_id}") if pages > 1 else 1
    start = (page - 1) * PAGE_SIZE
    rows = rows[start:start + PAGE_SIZE]
    st.caption(f"Showing rows {start + 1:,}–{start + len(rows):,} of {total:,} predictions.")

    st.subheader("Attack scores on this page")
    st.caption("Replay ID is row order. The dashed line marks the 0.5 attack threshold.")
    st.vega_lite_chart({
        "height": 260,
        "data": {"values": rows},
        "layer": [
            {
                "mark": {"type": "line", "point": True},
                "encoding": {
                    "x": {
                        "field": "replay_id", "type": "quantitative", "title": "Replay ID",
                        "axis": {"tickMinStep": 1},
                        "scale": {"zero": False},
                    },
                    "y": {
                        "field": "attack_score", "type": "quantitative", "title": "Attack score",
                        "scale": {"domain": [0, 1]},
                    },
                    "tooltip": [
                        {"field": "replay_id", "type": "quantitative", "title": "Replay ID"},
                        {"field": "attack_score", "type": "quantitative", "title": "Attack score", "format": ".10f"},
                    ],
                },
            },
            {
                "data": {"values": [{"threshold": THRESHOLD}]},
                "mark": {"type": "rule", "color": "#d44b39", "strokeDash": [6, 4], "strokeWidth": 2},
                "encoding": {"y": {"field": "threshold", "type": "quantitative"}},
            },
        ],
    })

    st.subheader("Predictions")
    st.table({
        "Replay ID": [row["replay_id"] for row in rows],
        "Attack score": [f"{row['attack_score']:.10f}" for row in rows],
        "Predicted class": [CLASS_LABELS[row["predicted_class"]] for row in rows],
    }, hide_index=True, hide_header=False)


st.set_page_config(page_title="IoT replay predictions", layout="wide")
st.title("IoT replay predictions")


def refresh_runs():
    st.session_state.pop("completed_runs", None)
    st.session_state.pop("loaded_predictions", None)


st.button("Refresh", on_click=refresh_runs)
if "completed_runs" not in st.session_state:
    runs = request_json("/runs")
    if runs is None:
        st.stop()
    st.session_state.completed_runs = runs
    if runs:
        st.session_state.selected_run = runs[0]["run_id"]

runs = st.session_state.completed_runs
if not runs:
    st.info("No completed replays yet. Run a replay with Cassandra, then select Refresh.")
    st.stop()

labels = {}
for run in runs:
    when = datetime.fromisoformat(run["completed_at"]).strftime("%Y-%m-%d %H:%M:%S UTC")
    labels[run["run_id"]] = f"Run {run['display_number']} · {when} · {run['prediction_count']:,} predictions"
run_id = st.selectbox("Completed run", list(labels), format_func=labels.__getitem__, key="selected_run")
loaded = st.session_state.get("loaded_predictions")
if loaded is None or loaded["run_id"] != run_id:
    loaded = request_json(f"/runs/{quote(run_id, safe='')}/predictions")
    if loaded is None:
        st.stop()
    loaded["predictions"].sort(key=lambda row: row["replay_id"])
    st.session_state.loaded_predictions = loaded
show_predictions(loaded["predictions"], run_id)
