"""Evaluate the saved binary baseline on the two validation scopes."""

import pickle

import numpy as np
import pandas as pd
from sklearn.metrics import (
    balanced_accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
)

from src.batch.baseline_data import (
    ATTACK_THRESHOLD,
    MODEL_PATH,
    TRAINING_PATH,
    VALIDATION_PATH,
    binary_targets,
    load_features,
    validation_overlap,
)

CONFUSION_COUNTS = ["TN", "FP", "FN", "TP"]
CLASSIFICATION_RATES = ["Accuracy", "Attack precision", "Attack recall", "Benign precision", "Benign recall"]


def confusion_report(scores):
    """Verify evaluation counts and derive rates in benign/attack matrix order."""
    report = scores.copy()
    count_columns = ["Rows", *CONFUSION_COUNTS]
    counts = report[count_columns]
    integer_counts = counts.astype("int64")
    if not counts.eq(integer_counts).all(axis=None):
        raise ValueError("The sample size and confusion counts must be integers.")
    report[count_columns] = integer_counts
    if not report[CONFUSION_COUNTS].sum(axis=1).eq(report["Rows"]).all():
        raise ValueError("Confusion counts do not sum to the stated sample size.")
    tn, fp, fn, tp = (report[column] for column in CONFUSION_COUNTS)
    denominators = (report["Rows"], tp + fp, tp + fn, tn + fn, tn + fp)
    numerators = (tn + tp, tp, tp, tn, tn)
    for column, numerator, denominator in zip(CLASSIFICATION_RATES, numerators, denominators, strict=True):
        rate = numerator / denominator.where(denominator.ne(0))
        if column in report:
            defined = rate.notna()
            np.testing.assert_allclose(report.loc[defined, column], rate[defined], rtol=0, atol=1e-15)
        report[column] = rate
    return report


def format_confusion_report(report, extra_columns=()):
    """Format notebook results while preserving unrounded rates in the source frame."""
    formatted = report.reset_index().rename(columns={"Validation": "Scope", "Rows": "N"})
    for column in [*CLASSIFICATION_RATES, *extra_columns]:
        if column == "ROC-AUC":
            formatted[column] = formatted[column].map("{:.6f}".format)
        else:
            formatted[column] = formatted[column].map("{:.4%}".format, na_action="ignore").fillna("N/A")
    return formatted[["Model", "Scope", "N", *CONFUSION_COUNTS, *CLASSIFICATION_RATES, *extra_columns]]


def score_predictions(y_validation, predictions, evaluation_masks):
    results = []
    for name, predicted in predictions.items():
        for scope, mask in evaluation_masks.items():
            actual, selected = y_validation[mask], predicted[mask]
            tn, fp, fn, tp = confusion_matrix(actual, selected, labels=[0, 1]).ravel()
            precision, recall, f1, _ = precision_recall_fscore_support(
                actual, selected, labels=[0, 1], zero_division=0,
            )
            results.append({
                "Model": name, "Validation": scope, "Rows": len(actual),
                "TN": tn, "FP": fp, "FN": fn, "TP": tp,
                "Benign precision": precision[0], "Benign recall": recall[0], "Benign F1": f1[0],
                "Attack precision": precision[1], "Attack recall": recall[1], "Attack F1": f1[1],
                "Balanced accuracy": balanced_accuracy_score(actual, selected),
                "Benign false positives": fp,
                "Actual benign": tn + fp,
                "Benign false-positive rate": fp / (tn + fp),
            })

    return pd.DataFrame(results).set_index(["Model", "Validation"])


def main():
    with MODEL_PATH.open("rb") as model_file:
        artifact = pickle.load(model_file)

    training, _ = load_features(TRAINING_PATH)
    validation, labels = load_features(VALIDATION_PATH)
    y_validation = binary_targets(labels, "Validation").to_numpy()
    shared_patterns, matches = validation_overlap(training, validation)
    pipeline = artifact["pipeline"]
    feature_columns = artifact["feature_columns"]
    attack_index = list(pipeline.classes_).index(1)
    attack_probabilities = pipeline.predict_proba(validation[feature_columns])[:, attack_index]
    predictions = {
        "Always attack": np.ones_like(y_validation),
        "Logistic regression": (attack_probabilities >= ATTACK_THRESHOLD).astype(int),
    }
    evaluation_masks = {
        "All validation": np.ones(len(y_validation), dtype=bool),
        "Nonmatching only": ~matches,
    }
    print(f"Shared patterns: {shared_patterns:,}")
    print(f"Matching validation rows: {matches.sum():,}")
    print(f"Nonmatching validation rows: {(~matches).sum():,}")
    scores = score_predictions(y_validation, predictions, evaluation_masks)
    formatters = {
        column: "{:.4%}".format
        for column in scores.columns
        if pd.api.types.is_float_dtype(scores[column])
    }
    print(scores.to_string(formatters=formatters))


if __name__ == "__main__":
    main()
