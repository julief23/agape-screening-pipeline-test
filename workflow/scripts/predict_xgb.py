import pandas as pd
import pickle
import numpy as np


def load_model(model_file):
    with open(model_file, "rb") as f:
        return pickle.load(f)


def predict(model, df):
    meta_cols = ["CID", "SMILES", "canonical_smiles"]
    X = df.drop(columns=[c for c in meta_cols if c in df.columns])

    probs = model.predict_proba(X)[:, 1]

    THRESHOLD = 0.5
    preds = (probs >= THRESHOLD).astype(int)

    confidence = probs

    confidence_level = np.where(
        probs >= 0.85, "High ACTIVE",
        np.where(probs <= 0.15, "High INACTIVE", "Moderate")
    )

    result = df[[c for c in meta_cols if c in df.columns]].copy()

    result["probability_active"] = probs
    result["prediction"] = ["ACTIVE" if p == 1 else "INACTIVE" for p in preds]
    result["model_confidence"] = confidence
    result["confidence_level"] = confidence_level

    print(f"[PREDICT] Rows: {len(result)}")

    return result