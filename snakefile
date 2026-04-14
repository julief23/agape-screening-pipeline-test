import glob
import os
from pathlib import Path

# =========================
# AUTO-DETECT CHUNKS
# =========================

CHUNKS = [
    os.path.basename(f).replace(".txt", "")
    for f in glob.glob("chunks/chunk_*.txt")
]

print(f"[SNAKEMAKE] Found {len(CHUNKS)} chunks")

# =========================
# FINAL TARGET (ONLY ACTIVES)
# =========================

rule all:
    input:
        "results/ALL_high_active.csv"


# =========================
# STEP 1 — FETCH + SPLIT
# =========================

rule fetch_pubchem:
    output:
        "data/raw/CID-SMILES.gz"
    shell:
        """
        mkdir -p logs data/raw chunks
        python workflow/scripts/fetch_pubchem.py
        """


# =========================
# STEP 2 — PROCESS CHUNKS
# =========================

rule process_chunk:
    input:
        "chunks/{chunk}.txt"
    output:
        all="results/all/{chunk}.csv",
        active="results/active/{chunk}.csv",
        inactive="results/inactive/{chunk}.csv"
    conda:
        "workflow/envs/full_pipeline.yaml"
    shell:
        """
        mkdir -p results/all results/active results/inactive logs
        python workflow/scripts/worker.py {input} \
            {output.all} \
            {output.active} \
            {output.inactive}
        """


# =========================
# STEP 3 — MERGE ONLY ACTIVES
# =========================
rule merge_predictions:
    input:
        lambda wildcards: sorted(glob.glob("results/active/*.csv"))
    output:
        "results/ALL_high_active.csv"
    script:
        "workflow/scripts/merge_predictions.py"