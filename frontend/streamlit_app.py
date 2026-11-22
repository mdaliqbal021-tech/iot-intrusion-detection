"""Review predictions from a saved IoT replay run."""

import os
from urllib.parse import quote

import requests
import streamlit as st

API_BASE_URL = os.environ.get("IOT_API_BASE_URL", "http://127.0.0.1:18080").rstrip("/")
THRESHOLD = 0.5
CLASS_LABELS = {0: "Benign (0)", 1: "Attack (1)"}


def load_predictions(run_id):
    url = f"{API_BASE_URL}/runs/{quote(run_id, safe='')}/predictions"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 404:
            st.warning("No predictions found for this run ID.")
            return None
        response.raise_for_status()
    except requests.RequestException:
        st.error("The predictions API is unavailable. Try again when the IoT services are running.")
        return None

    try:
        return sorted(response.json()["predictions"], key=lambda row: row["replay_id"])
    except (ValueError, KeyError, TypeError):
        st.error("The predictions API returned an invalid response.")
        return None


def show_predictions(rows):
    benign = sum(row["predicted_class"] == 0 for row in rows)
    attack = sum(row["predicted_class"] == 1 for row in rows)
    total_column, benign_column, attack_column = st.columns(3)
    total_column.metric("Total predictions", len(rows), border=True)
    benign_column.metric("Predicted benign", benign, border=True)
    attack_column.metric("Predicted attack", attack, border=True)

    st.subheader("Attack scores")
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

with st.form("load_run", border=False):
    run_id = st.text_input("Run ID", placeholder="run.XXXXXXXX").strip()
    submitted = st.form_submit_button("Load predictions", type="primary")

if submitted:
    if not run_id:
        st.warning("Enter a replay run ID.")
    else:
        rows = load_predictions(run_id)
        if rows is not None:
            show_predictions(rows)
