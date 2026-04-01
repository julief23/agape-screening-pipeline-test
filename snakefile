import glob
import os

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
        expand("results/active/{chunk}.csv", chunk=CHUNKS)
    output:
        "results/ALL_high_active.csv"
    shell:
        """
        python workflow/scripts/merge.py
        """