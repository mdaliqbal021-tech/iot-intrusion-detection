"""Evaluate the saved binary baseline on the two validation scopes."""

import pickle

import numpy as np
import pandas as pd
from sklearn.metrics import (
    balanced_accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
)

from Batch.baseline_data import (
    ATTACK_THRESHOLD,
    MODEL_PATH,
    TRAINING_PATH,
    VALIDATION_PATH,
    binary_targets,
    load_features,
    validation_overlap,
)


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
