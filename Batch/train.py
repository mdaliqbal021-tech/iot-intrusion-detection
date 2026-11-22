"""Fit and save the binary logistic regression baseline."""

import pickle
from pathlib import Path
from tempfile import NamedTemporaryFile

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from Batch.baseline_data import MODEL_PATH, TRAINING_PATH, binary_targets, load_features, select_model_features


def main():
    training, labels = load_features(TRAINING_PATH)
    targets = binary_targets(labels, "Training")
    feature_columns = select_model_features(training)
    pipeline = make_pipeline(
        StandardScaler(),
        LogisticRegression(solver="lbfgs", class_weight="balanced", max_iter=2000),
    )
    pipeline.fit(training[feature_columns], targets)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=MODEL_PATH.parent, delete=False) as model_file:
        temporary_path = Path(model_file.name)
    try:
        with temporary_path.open("wb") as model_file:
            pickle.dump({"pipeline": pipeline, "feature_columns": feature_columns}, model_file)
        temporary_path.replace(MODEL_PATH)
    finally:
        temporary_path.unlink(missing_ok=True)

    iterations = pipeline.named_steps["logisticregression"].n_iter_.max()
    print(f"Fitted on {len(training):,} rows with {len(feature_columns)} features ({iterations:,} solver iterations).")
    print(f"Saved fitted pipeline and ordered feature list to {MODEL_PATH}")


if __name__ == "__main__":
    main()
