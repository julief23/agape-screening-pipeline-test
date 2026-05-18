import sys
import os
import pandas as pd
import logging

from logging.handlers import RotatingFileHandler
from pathlib import Path

from clean_smiles import clean_dataframe
from compute_mordred_selected import compute_descriptors
from align_impute_scale_ml import preprocess_features
from predict_xgb import predict, load_model


# =========================
# INPUT ARGUMENTS
# =========================

if len(sys.argv) != 7:
    raise ValueError(
        "Usage: python worker.py INPUT OUT_ALL OUT_ACTIVE OUT_INACTIVE "
        "OUT_HIGH_ACTIVE OUT_HIGH_INACTIVE"
    )

INPUT = sys.argv[1]
OUT_ALL = sys.argv[2]
OUT_ACTIVE = sys.argv[3]
OUT_INACTIVE = sys.argv[4]
OUT_HIGH_ACTIVE = sys.argv[5]
OUT_HIGH_INACTIVE = sys.argv[6]


# =========================
# LOGGING SETUP
# =========================

chunk_name = Path(INPUT).stem
log_file = Path("logs") / f"{chunk_name}.log"
log_file.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(chunk_name)
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=5_000_000,
    backupCount=2
)

formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


# =========================
# FILES
# =========================

FEATURE_FILE = "models/xgb_feature_list.pkl"
IMPUTER_FILE = "models/xgb_final_imputer.pkl"
SCALER_FILE = "models/xgb_final_scaler.pkl"
MODEL_FILE = "models/xgb_final_model.pkl"

OUTPUT_FILES = [
    OUT_ALL,
    OUT_ACTIVE,
    OUT_INACTIVE,
    OUT_HIGH_ACTIVE,
    OUT_HIGH_INACTIVE,
]

for f in OUTPUT_FILES:
    os.makedirs(os.path.dirname(f), exist_ok=True)
    if os.path.exists(f):
        os.remove(f)


# =========================
# LOAD MODEL
# =========================

model = load_model(MODEL_FILE)


# =========================
# PARAMETERS
# =========================

BATCH_SIZE = 1000
ACTIVE_THRESHOLD = 0.5
HIGH_ACTIVE_THRESHOLD = 0.85
HIGH_INACTIVE_THRESHOLD = 0.15


# =========================
# FLUSH FUNCTION
# =========================

def write_csv(df, output_path, first_write):
    df.to_csv(
        output_path,
        mode="a",
        header=first_write,
        index=False
    )


def flush_batch(batch_df, first_write):
    if batch_df.empty:
        return 0

    df = batch_df[["CID", "SMILES"]].copy()

    df = clean_dataframe(df)

    if df.empty:
        logger.info("[BATCH] 0 valid molecules after cleaning")
        return 0

    desc = compute_descriptors(df, FEATURE_FILE)

    if desc.empty:
        logger.info("[BATCH] 0 molecules after descriptor computation")
        return 0

    X = preprocess_features(
        desc,
        FEATURE_FILE,
        IMPUTER_FILE,
        SCALER_FILE
    )

    results = predict(model, X)

    if results.empty:
        logger.info("[BATCH] 0 molecules after prediction")
        return 0

    active = results[
        results["probability_active"] >= ACTIVE_THRESHOLD
    ].copy()

    inactive = results[
        results["probability_active"] < ACTIVE_THRESHOLD
    ].copy()

    high_active = results[
        results["probability_active"] >= HIGH_ACTIVE_THRESHOLD
    ].copy()

    high_inactive = results[
        results["probability_active"] <= HIGH_INACTIVE_THRESHOLD
    ].copy()

    write_csv(results, OUT_ALL, first_write)
    write_csv(active, OUT_ACTIVE, first_write)
    write_csv(inactive, OUT_INACTIVE, first_write)
    write_csv(high_active, OUT_HIGH_ACTIVE, first_write)
    write_csv(high_inactive, OUT_HIGH_INACTIVE, first_write)

    logger.info(
        f"[BATCH] processed={len(results)} | "
        f"active={len(active)} | "
        f"inactive={len(inactive)} | "
        f"high_active={len(high_active)} | "
        f"high_inactive={len(high_inactive)}"
    )

    return len(results)


# =========================
# PROCESS CHUNK FILE
# =========================

logger.info(f"Starting processing: {INPUT}")

chunk_df = pd.read_csv(INPUT, dtype=str)

required_cols = {"CID"}

if not required_cols.issubset(chunk_df.columns):
    raise ValueError(
        f"Missing required columns in {INPUT}. "
        f"Found columns: {chunk_df.columns.tolist()}"
    )

if "canonical_smiles" in chunk_df.columns:
    smiles_col = "canonical_smiles"
elif "SMILES" in chunk_df.columns:
    smiles_col = "SMILES"
else:
    raise ValueError(
        f"No SMILES column found in {INPUT}. "
        f"Found columns: {chunk_df.columns.tolist()}"
    )

logger.info(f"Using SMILES column: {smiles_col}")
logger.info(f"Input rows: {len(chunk_df)}")

chunk_df = chunk_df[["CID", smiles_col]].copy()
chunk_df = chunk_df.rename(columns={smiles_col: "SMILES"})
chunk_df = chunk_df.dropna(subset=["CID", "SMILES"]).copy()

processed = 0
first_write = True

for start in range(0, len(chunk_df), BATCH_SIZE):
    batch_df = chunk_df.iloc[start:start + BATCH_SIZE].copy()

    n_processed = flush_batch(batch_df, first_write)

    if n_processed > 0:
        first_write = False

    processed += n_processed

    logger.info(f"[PROGRESS] {processed} molecules")

logger.info(f"DONE: {INPUT} → {processed} molecules")