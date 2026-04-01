import sys
import os
import pandas as pd

from clean_smiles import clean_dataframe
from compute_mordred_selected import compute_descriptors
from align_impute_scale_ml import preprocess_features
from predict_xgb import predict, load_model

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


# =========================
# INPUT ARGUMENTS
# =========================

INPUT = sys.argv[1]
OUT_ALL = sys.argv[2]
OUT_ACTIVE = sys.argv[3]
OUT_INACTIVE = sys.argv[4]


# =========================
# LOGGING SETUP (AFTER INPUT)
# =========================

chunk_name = Path(INPUT).stem
log_file = Path("logs") / f"{chunk_name}.log"
log_file.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(chunk_name)
logger.setLevel(logging.INFO)

# avoid duplicate handlers (important)
if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=2)

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

os.makedirs(os.path.dirname(OUT_ALL), exist_ok=True)
os.makedirs(os.path.dirname(OUT_ACTIVE), exist_ok=True)
os.makedirs(os.path.dirname(OUT_INACTIVE), exist_ok=True)

# remove old outputs
for f in [OUT_ALL, OUT_ACTIVE, OUT_INACTIVE]:
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

batch = []
processed = 0


# =========================
# FLUSH FUNCTION
# =========================

def flush_batch(batch, first_write):
    df = pd.DataFrame(batch, columns=["CID", "SMILES"])

    df = clean_dataframe(df)
    if df.empty:
        return 0

    desc = compute_descriptors(df, FEATURE_FILE)
    if desc.empty:
        return 0

    X = preprocess_features(
        desc,
        FEATURE_FILE,
        IMPUTER_FILE,
        SCALER_FILE
    )

    results = predict(model, X)

    # WRITE ALL
    results.to_csv(
        OUT_ALL,
        mode="a",
        header=first_write,
        index=False
    )

    # HIGH CONF FILTER
    high_active = results[results["probability_active"] >= 0.85]
    high_inactive = results[results["probability_active"] <= 0.15]

    high_active.to_csv(
        OUT_ACTIVE,
        mode="a",
        header=first_write,
        index=False
    )

    high_inactive.to_csv(
        OUT_INACTIVE,
        mode="a",
        header=first_write,
        index=False
    )

    logger.info(f"[BATCH] {len(results)} processed")

    return len(results)


# =========================
# PROCESS CHUNK FILE
# =========================

logger.info(f"Starting processing: {INPUT}")

with open(INPUT, "r") as f:
    first_write = True

    for line in f:
        try:
            cid, smiles = line.strip().split()
        except:
            continue

        batch.append((cid, smiles))

        if len(batch) == BATCH_SIZE:
            processed += flush_batch(batch, first_write)
            first_write = False
            batch = []

            logger.info(f"[PROGRESS] {processed} molecules")

    if batch:
        processed += flush_batch(batch, first_write)


logger.info(f"DONE: {INPUT} → {processed} molecules")