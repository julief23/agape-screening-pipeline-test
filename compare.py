import pandas as pd
from rdkit import Chem
from rdkit import RDLogger

RDLogger.DisableLog("rdApp.*")

EXPERIMENTAL_CSV = "results/agape_dataset.csv"
SCREENED_CSV = "results/ALL_high_active_clean.csv"

EXPERIMENTAL_SMILES_COLUMN = "Canonical_SMILES"
SCREENED_SMILES_COLUMN = "canonical_smiles"

LABEL_COLUMN = "coded_label"   # active = 1, inactive = 0
ACTIVE_VALUE = 1

OUTPUT_PRESENT = "results/experimental_actives_present_in_screened.csv"
OUTPUT_ABSENT = "results/experimental_actives_absent_from_screened.csv"


def canonicalize(smiles):
    if pd.isna(smiles):
        return None

    mol = Chem.MolFromSmiles(str(smiles).strip())

    if mol is None:
        return None

    return Chem.MolToSmiles(mol, canonical=True)


experimental = pd.read_csv(EXPERIMENTAL_CSV)
screened = pd.read_csv(SCREENED_CSV)

# Keep only experimentally active molecules
experimental_active = experimental[experimental[LABEL_COLUMN] == ACTIVE_VALUE].copy()

experimental_active["match_smiles"] = experimental_active[EXPERIMENTAL_SMILES_COLUMN].apply(canonicalize)
screened["match_smiles"] = screened[SCREENED_SMILES_COLUMN].apply(canonicalize)

experimental_active = experimental_active.dropna(subset=["match_smiles"])
screened = screened.dropna(subset=["match_smiles"])

experimental_active = experimental_active.drop_duplicates(subset=["match_smiles"])
screened = screened.drop_duplicates(subset=["match_smiles"])

matched = experimental_active.merge(
    screened,
    on="match_smiles",
    how="left",
    indicator=True,
    suffixes=("_experimental", "_screened")
)

present = matched[matched["_merge"] == "both"].copy()
absent = matched[matched["_merge"] == "left_only"].copy()

present.to_csv(OUTPUT_PRESENT, index=False)
absent.to_csv(OUTPUT_ABSENT, index=False)

print(f"Experimentally active molecules: {len(experimental_active)}")
print(f"Predicted active screened molecules: {len(screened)}")
print(f"Experimental actives present in screened set: {len(present)}")
print(f"Experimental actives absent from screened set: {len(absent)}")

print(f"Saved present molecules to: {OUTPUT_PRESENT}")
print(f"Saved absent molecules to: {OUTPUT_ABSENT}")