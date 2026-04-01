import numpy as np

if not hasattr(np, "float"):
    np.float = float
if not hasattr(np, "int"):
    np.int = int
if not hasattr(np, "bool"):
    np.bool = bool

import pandas as pd
from rdkit import Chem
from mordred import Calculator, descriptors
import pickle
import joblib

# =========================
# GLOBAL CACHE
# =========================

CALC = None
FEATURE_LIST = None


def compute_descriptors(df, feature_file):
    global CALC, FEATURE_LIST

    # =========================
    # INITIALIZE ONCE
    # =========================

    if FEATURE_LIST is None:
        if str(feature_file).endswith(".joblib"):
            FEATURE_LIST = list(joblib.load(feature_file))
        else:
            with open(feature_file, "rb") as f:
                FEATURE_LIST = list(pickle.load(f))

        # 👉 CORRECT WAY
        calc_full = Calculator(descriptors, ignore_3D=True)

        selected_descriptors = [
            d for d in calc_full.descriptors if str(d) in FEATURE_LIST
        ]

        if not selected_descriptors:
            raise ValueError("No descriptors match feature list")

        CALC = Calculator(selected_descriptors, ignore_3D=True)

        print(f"[MORDRED] Initialized with {len(selected_descriptors)} descriptors")

    # =========================
    # COMPUTE DESCRIPTORS
    # =========================

    results = []
    smiles_list = df["canonical_smiles"].values

    for smiles in smiles_list:
        mol = Chem.MolFromSmiles(smiles)

        if mol is None:
            results.append([np.nan] * len(FEATURE_LIST))
            continue

        try:
            values = CALC(mol)

            row = []
            for val in values:
                try:
                    row.append(float(val))
                except:
                    row.append(np.nan)

        except:
            row = [np.nan] * len(FEATURE_LIST)

        results.append(row)

    desc_df = pd.DataFrame(results, columns=FEATURE_LIST)

    # =========================
    # MERGE METADATA
    # =========================

    meta_df = df.iloc[:len(desc_df)].reset_index(drop=True)
    final_df = pd.concat([meta_df, desc_df], axis=1)

    print(f"[MORDRED] Rows: {len(final_df)}")

    return final_df