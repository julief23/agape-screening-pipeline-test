import pandas as pd
from rdkit import Chem


def clean_dataframe(df):
    """
    Clean a dataframe of CID/SMILES:
    - removes invalid SMILES
    - canonicalizes SMILES
    """

    # --- sanity check ---
    required_cols = {"CID", "SMILES"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    valid_rows = []

    # --- iterate efficiently ---
    for cid, smiles in zip(df["CID"], df["SMILES"]):
        smiles = str(smiles).strip()

        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            continue

        canonical_smiles = Chem.MolToSmiles(mol, canonical=True)

        valid_rows.append({
            "CID": cid,
            "SMILES": smiles,
            "canonical_smiles": canonical_smiles
        })

    clean_df = pd.DataFrame(valid_rows)

    print(f"[CLEAN] Input rows: {len(df)}")
    print(f"[CLEAN] Valid molecules: {len(clean_df)}")

    return clean_df