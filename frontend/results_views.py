"""Render captured per-flow explanations and saved evaluation artifacts."""

import json
import math
from pathlib import Path

import streamlit as st

FIGURES = Path(__file__).resolve().parents[1] / "figures"


def show_explanation(result, class_labels):
    st.caption(
        f"Record {result['source_record_id'] + 1:,} · Run `{result['run_id']}` · "
        f"{result['model_version']} · XGBoost runtime {result['xgboost_version']}"
    )
    score, threshold, predicted = st.columns(3)
    score.metric("Attack score", f"{result['attack_score']:.8f}", border=True)
    threshold.metric("Deployed threshold", f"{result['threshold']:g}", border=True)
    predicted.metric("Predicted class", class_labels[result["predicted_class"]], border=True)
    if result["recorded_label"]:
        st.caption(f"Recorded label · {result['recorded_label']}")

    rows = result["features"]
    positive = sorted((row for row in rows if row["contribution"] > 0),
                      key=lambda row: row["contribution"], reverse=True)[:4]
    negative = sorted((row for row in rows if row["contribution"] < 0),
                      key=lambda row: row["contribution"])[:4]
    def table_row(row):
        value = row["contribution"]
        direction = "Raises attack score" if value > 0 else "Lowers attack score" if value < 0 else "No change"
        return {
            "Feature": row["feature"], "Observed value": row["value"],
            "Contribution (log-odds)": value, "Direction": direction,
        }

    chart_rows = [table_row(row) for row in positive + negative]
    if chart_rows:
        st.vega_lite_chart({
            "height": {"step": 29},
            "data": {"values": chart_rows},
            "mark": {"type": "bar", "tooltip": True},
            "encoding": {
                "x": {"field": "Contribution (log-odds)", "type": "quantitative"},
                "y": {"field": "Feature", "type": "nominal", "title": None,
                      "sort": {"field": "Contribution (log-odds)", "order": "descending"},
                      "axis": {"labelLimit": 175}},
                "color": {"field": "Direction", "type": "nominal",
                          "scale": {"domain": ["Raises attack score", "Lowers attack score"],
                                    "range": ["#E58D66", "#52C7BB"]},
                          "legend": {"orient": "bottom", "title": None}},
                "tooltip": [
                    {"field": "Feature"},
                    {"field": "Observed value", "type": "quantitative", "format": ".6g"},
                    {"field": "Contribution (log-odds)", "type": "quantitative", "format": "+.5f"},
                ],
            },
        }, width="stretch")
        st.caption("Largest positive and negative contributions. Bars are on the raw-margin (log-odds) scale.")
    st.caption(f"All {len(rows)} feature contributions, ordered by absolute contribution.")
    st.dataframe(
        [table_row(row) for row in sorted(rows, key=lambda row: abs(row["contribution"]), reverse=True)],
        hide_index=True, width="stretch", height=270, column_config={
            "Observed value": st.column_config.NumberColumn(format="%.6g"),
            "Contribution (log-odds)": st.column_config.NumberColumn(format="%+.5f"),
        },
    )

    effects = []
    if positive:
        effects.append(f"**{positive[0]['feature']}** pushed the model score up the most")
    if negative:
        effects.append(f"**{negative[0]['feature']}** pushed it down the most")
    if effects:
        st.markdown(f"For this flow, {' and '.join(effects)}. These contributions describe how the model scored the row.")
    else:
        st.write("The model assigned zero contribution to every feature in this row.")

    with st.expander("Verify the score reconstruction"):
        reconstructed_margin = math.fsum([result["bias"], *(row["contribution"] for row in rows)])
        st.markdown(
            f"**Bias** {result['bias']:+.8f} · **Bias + all {len(rows)} contributions** {reconstructed_margin:+.8f}  \n"
            f"**Model raw margin** {result['raw_margin']:+.8f} · "
            f"**Sigmoid of the verified margin** {result['reconstructed_attack_score']:.8f}"
        )
        st.caption("The API checks the log-odds sum within 1e-5 and the attack score against Cassandra within 1e-7.")
        st.caption("Captured model SHA-256")
        st.code(result["model_sha256"], language=None)


def read_artifact(name):
    with (FIGURES / name).open() as source:
        return json.load(source)


def show_figure(title, name, caption):
    st.markdown(f"#### {title}")
    path = FIGURES / name
    if not path.is_file():
        st.warning(f"Saved figure unavailable · {name}")
        return
    st.image(str(path), width="stretch")
    st.caption(caption)


def percent_columns(names):
    return {name: st.column_config.NumberColumn(format="%.4f%%") for name in names}


def display_rates(row, names):
    # The verified JSON stores rates in [0, 1]. Streamlit's printf display takes percent units.
    return {name: 100 * row[name] for name in names}


def show_streaming_performance():
    st.subheader("Measured streaming replay performance")
    try:
        runs = read_artifact("streaming_performance_summary.json")["runs"]
        trials = [run for run in runs if run["workload"] != "Smoke fixture"]
        rows = [{
            "Trial": run["workload"], "Unique stored": run["stored_unique_events"],
            "Send (events/s)": run["actual_send_eps"], "Median (s)": run["median_ms"] / 1_000,
            "P95 (s)": run["p95_ms"] / 1_000, "Throughput (events/s)": run["throughput_eps"],
        } for run in trials]
    except (OSError, ValueError, KeyError, TypeError):
        st.warning("Saved streaming measurements unavailable.")
        return
    st.caption(
        "Three 1,000-row replays. Kafka publication finished before each fresh Spark launch. "
        "Latency runs from each send to its shared Cassandra micro-batch acknowledgement."
    )
    st.dataframe(rows, hide_index=True, width="stretch", column_config={
        name: st.column_config.NumberColumn(format="%.2f") for name in
        ("Send (events/s)", "Median (s)", "P95 (s)", "Throughput (events/s)")
    })
    st.caption("Throughput is unique Cassandra-confirmed events divided by seconds from first send to last acknowledgement.")


def show_model_results():
    st.header("Model results")
    st.subheader("Validation results")
    st.caption("Same 25,000 validation rows across all three models. Classification results use threshold 0.5.")
    rates = ("Accuracy", "Balanced accuracy", "Attack precision", "Attack recall")
    extra_rates = ("Benign precision", "Benign recall", "Benign false-positive rate")
    try:
        rows = read_artifact("validation_model_comparison.json")
        summary = [{
            "Model": row["Model"], **display_rates(row, rates),
            "False alarms": int(row["FP"]), "Missed attacks": int(row["FN"]), "ROC-AUC": row["ROC-AUC"],
        } for row in rows]
        detailed = [{
            "Model": row["Model"], **{key: int(row[key]) for key in ("Rows", "TN", "FP", "FN", "TP")},
            **display_rates(row, extra_rates),
        } for row in rows]
    except (OSError, ValueError, KeyError, TypeError):
        st.warning("Saved validation table unavailable · validation_model_comparison.json")
    else:
        st.dataframe(summary, hide_index=True, width="stretch", column_config={
            **percent_columns(rates), "ROC-AUC": st.column_config.NumberColumn(format="%.6f"),
        })
        with st.expander("Detailed validation metrics and class counts"):
            st.dataframe(detailed, hide_index=True, width="stretch", column_config=percent_columns(extra_rates))

    show_figure(
        "Validation ROC", "validation_roc.png",
        "Each curve compares attack recall with the benign false-positive rate across thresholds. The diamond marks deployed XGBoost at 0.5.",
    )
    show_figure(
        "Validation confusion matrices", "validation_confusion_matrices.png",
        "Rows are recorded classes and columns are predicted classes. The off-diagonal cells count false alarms and missed attacks.",
    )

    st.subheader("Final test results: deployed XGBoost")
    try:
        test = read_artifact("final_test_results.json")
        tn, fp, fn, tp = (test[key] for key in ("TN", "FP", "FN", "TP"))
        n = test["rows"]
        if (n, tn, fp, fn, tp) != (25_000, 579, 10, 116, 24_295) or tn + fp + fn + tp != n:
            raise ValueError("Unexpected full-test counts.")
    except (OSError, ValueError, KeyError, TypeError):
        st.warning("Verified full-test results unavailable · final_test_results.json")
    else:
        st.caption(f"{test['scope']} · threshold {test['threshold']:g} · {test['source']}")
        st.dataframe([{
            "Model": test["model"], "Rows": n, "TN": tn, "FP": fp, "FN": fn, "TP": tp,
            "Accuracy": 100 * (tn + tp) / n,
            "Balanced accuracy": 50 * (tn / (tn + fp) + tp / (tp + fn)),
            "Attack precision": 100 * tp / (tp + fp), "Attack recall": 100 * tp / (tp + fn),
        }], hide_index=True, width="stretch", column_config=percent_columns(rates))
        st.caption("The deployed model raised 10 false alarms and missed 116 attacks on the held-out test sample.")

    st.subheader("Inference benchmark")
    show_figure(
        "Local CPU prediction time per batch", "inference_speed.png",
        "The x-axis is rows per batch. The y-axis is median milliseconds for one local probability prediction call on two CPU threads.",
    )
    show_streaming_performance()
