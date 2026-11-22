"""Predict attack probability and class for each row of a local CSV."""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from xgboost.core import XGBoostError

from src.batch.baseline_data import MODEL_PATH

DEFAULT_MODEL = MODEL_PATH.with_name("xgboost_final.ubj")
DEFAULT_METADATA = MODEL_PATH.with_name("xgboost_final_metadata.json")


def predict_csv(input_path, model_path, metadata_path):
    metadata = json.loads(metadata_path.read_text())
    features = metadata["feature_columns"]
    threshold = float(metadata["attack_threshold"])
    if not 0 <= threshold <= 1:
        raise ValueError("Metadata attack threshold must be between 0 and 1.")
    if metadata["class_mapping"] != {"0": "benign", "1": "attack"}:
        raise ValueError("Metadata class mapping must be 0 = benign and 1 = attack.")

    frame = pd.read_csv(input_path, low_memory=False)
    missing = [feature for feature in features if feature not in frame.columns]
    if missing:
        raise ValueError(f"Missing required feature columns: {', '.join(missing)}")

    model = XGBClassifier(device="cpu", n_jobs=2)
    model.load_model(model_path)
    if model.get_booster().feature_names != features:
        raise ValueError("Metadata feature order differs from the exported model.")

    attack_index = list(model.classes_).index(1)
    scores = model.predict_proba(frame[features])[:, attack_index]
    return pd.DataFrame({
        "attack_score": scores.astype(np.float64),
        "predicted_class": (scores >= threshold).astype(int),
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path, help="Input CSV with one row per prediction")
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--metadata", type=Path, default=DEFAULT_METADATA)
    args = parser.parse_args()
    try:
        predictions = predict_csv(args.csv, args.model, args.metadata)
    except (OSError, ValueError, KeyError, XGBoostError) as error:
        parser.exit(1, f"Prediction failed: {error}\n")
    predictions.to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    main()
