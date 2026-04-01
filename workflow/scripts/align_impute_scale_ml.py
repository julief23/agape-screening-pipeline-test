import pandas as pd
import numpy as np
import pickle
import joblib


def preprocess_features(df, feature_file, imputer_file, scaler_file):
    """
    Input: df with metadata + descriptors
    Output: df with metadata + scaled features
    """

    # Load feature list
    if str(feature_file).endswith(".joblib"):
        feature_list = joblib.load(feature_file)
    else:
        with open(feature_file, "rb") as f:
            feature_list = pickle.load(f)

    imputer = joblib.load(imputer_file)
    scaler = joblib.load(scaler_file)

    feature_list = list(feature_list)

    # Ensure all features exist
    for col in feature_list:
        if col not in df.columns:
            df[col] = np.nan

    descriptor_df = df[feature_list].copy()

    # Safety check
    assert list(descriptor_df.columns) == feature_list

    # Clean numeric values
    descriptor_df = descriptor_df.apply(pd.to_numeric, errors="coerce")
    descriptor_df = descriptor_df.replace([np.inf, -np.inf], np.nan)

    # Impute + scale
    X_imputed = imputer.transform(descriptor_df.values)
    X_scaled = scaler.transform(X_imputed)

    X_scaled = pd.DataFrame(
        X_scaled,
        columns=feature_list,
        index=df.index
    )

    # Safe metadata extraction
    meta_cols = [c for c in ["CID", "SMILES", "canonical_smiles"] if c in df.columns]
    meta_df = df[meta_cols].reset_index(drop=True)

    final_df = pd.concat([meta_df, X_scaled], axis=1)

    print(f"[PREPROCESS] Rows: {len(final_df)}")

    return final_df