import argparse
import pandas as pd
from rdkit import Chem, RDLogger
from pathlib import Path

RDLogger.DisableLog("rdApp.*")


def alpha_suffix(n: int) -> str:
    result = ""
    while True:
        n, rem = divmod(n, 26)
        result = chr(97 + rem) + result
        if n == 0:
            break
        n -= 1
    return result


def safe_mol_from_smiles(smiles):
    if pd.isna(smiles):
        return None

    smiles = str(smiles).strip()
    if not smiles:
        return None

    try:
        return Chem.MolFromSmiles(smiles)
    except Exception:
        return None


def canonicalize_smiles(smiles):
    mol = safe_mol_from_smiles(smiles)
    if mol is None:
        return None

    try:
        return Chem.MolToSmiles(mol, canonical=True)
    except Exception:
        return None


def split_disconnected_smiles(smiles):
    if pd.isna(smiles):
        return []

    return [frag.strip() for frag in str(smiles).split(".") if frag.strip()]


def is_true_metal_atomic_number(z, z_threshold=21):
    excluded = {
        1, 5, 6, 7, 8, 9,
        14, 15, 16, 17,
        33, 34, 35,
        52, 53,
    }

    return z is not None and z >= z_threshold and z not in excluded


def get_fragment_info(fragment_smiles, z_threshold=21):
    mol = safe_mol_from_smiles(fragment_smiles)

    if mol is None:
        return {
            "valid": False,
            "is_single_atom_light_ion": False,
            "is_single_atom_heavy_metal_ion": False,
        }

    atomic_numbers = [atom.GetAtomicNum() for atom in mol.GetAtoms()]
    is_single_atom = len(atomic_numbers) == 1
    single_z = atomic_numbers[0] if is_single_atom else None

    return {
        "valid": True,
        "is_single_atom_light_ion": (
            is_single_atom
            and single_z is not None
            and single_z < z_threshold
        ),
        "is_single_atom_heavy_metal_ion": (
            is_single_atom
            and is_true_metal_atomic_number(single_z, z_threshold)
        ),
    }


def canonicalize_fragment_list(fragments):
    canonical = []
    seen = set()

    for frag in fragments:
        can = canonicalize_smiles(frag)

        if can is None:
            continue

        if can not in seen:
            seen.add(can)
            canonical.append(can)

    return canonical


def process_smiles(smiles, z_threshold=21, max_fragments=3):
    fragments = split_disconnected_smiles(smiles)

    if not fragments:
        return []

    infos = [get_fragment_info(frag, z_threshold) for frag in fragments]

    if any(not info["valid"] for info in infos):
        return []

    kept = []

    for frag, info in zip(fragments, infos):
        if not info["is_single_atom_light_ion"]:
            kept.append(frag)

    if not kept:
        return []

    kept_canonical = canonicalize_fragment_list(kept)

    if not kept_canonical:
        return []

    # Reject large reaction-like mixtures
    if len(kept_canonical) > max_fragments:
        return []

    kept_infos = [
        get_fragment_info(frag, z_threshold)
        for frag in kept_canonical
    ]

    has_heavy_metal = any(
        info["is_single_atom_heavy_metal_ion"]
        for info in kept_infos
    )

    # Keep small metal complexes grouped
    if has_heavy_metal:
        return [".".join(kept_canonical)]

    # Split normal multi-fragment organic systems
    return kept_canonical


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--cleaned", required=True)
    parser.add_argument("--z_threshold", type=int, default=21)
    parser.add_argument("--max_fragments", type=int, default=4)
    args = parser.parse_args()

    df = pd.read_csv(
        args.input,
        sep=r"\s+",
        header=None,
        names=["CID", "canonical_smiles"],
        dtype=str
    )

    output_rows = []

    for _, row in df.iterrows():
        cid = str(row["CID"])
        smiles = row["canonical_smiles"]

        output_fragments = process_smiles(
            smiles,
            z_threshold=args.z_threshold,
            max_fragments=args.max_fragments
        )

        if len(output_fragments) == 0:
            continue

        if len(output_fragments) == 1:
            output_rows.append({
                "CID": cid,
                "canonical_smiles": output_fragments[0],
            })

        else:
            for i, frag in enumerate(output_fragments):
                suffix = alpha_suffix(i)

                output_rows.append({
                    "CID": f"{cid}_{suffix}",
                    "canonical_smiles": frag,
                })

    cleaned = pd.DataFrame(
        output_rows,
        columns=["CID", "canonical_smiles"]
    )

    Path(args.cleaned).parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_csv(args.cleaned, index=False)

    print(f"Input rows: {len(df)}")
    print(f"Output rows: {len(cleaned)}")
    print(f"Saved: {args.cleaned}")


if __name__ == "__main__":
    main()