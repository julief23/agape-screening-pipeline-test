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
# FINAL TARGET
# =========================

rule all:
    input:
        "results/ALL_high_active.csv",
        "results/ALL_high_inactive.csv"


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
        all="results/{chunk}_results.csv",
        active="results/{chunk}_high_active.csv",
        inactive="results/{chunk}_high_inactive.csv"
    conda:
        "workflow/envs/full_pipeline.yaml"
    shell:
        """
        mkdir -p results logs
        python workflow/scripts/worker.py {input} \
            {output.all} \
            {output.active} \
            {output.inactive}
        """


# =========================
# STEP 3 — MERGE RESULTS
# =========================

rule merge_predictions:
    input:
        expand("results/{chunk}_high_active.csv", chunk=CHUNKS),
        expand("results/{chunk}_high_inactive.csv", chunk=CHUNKS)
    output:
        active="results/ALL_high_active.csv",
        inactive="results/ALL_high_inactive.csv"
    shell:
        """
        python workflow/scripts/merge_predictions.py
        """