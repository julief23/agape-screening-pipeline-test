import glob
import os

CHUNKS = [
    os.path.basename(f).replace(".txt", "")
    for f in glob.glob("data/chunks/chunk_*.txt")
]

rule all:
    input:
        "results/ALL_predictions.csv",
        "results/ALL_active.csv",
        "results/ALL_inactive.csv",
        "results/ALL_high_active.csv",
        "results/ALL_high_inactive.csv"

rule clean_chunk:
    input:
        "data/chunks/{chunk}.txt"
    output:
        "data/cleaned_chunks/{chunk}.csv"
    conda:
        "workflow/envs/full_pipeline.yaml"
    shell:
        """
        mkdir -p data/cleaned_chunks logs

        python workflow/scripts/clean_pubchem_chunk.py \
            --input {input} \
            --cleaned {output}
        """

rule process_chunk:
    input:
        "data/cleaned_chunks/{chunk}.csv"
    output:
        all="results/all/{chunk}.csv",
        active="results/active/{chunk}.csv",
        inactive="results/inactive/{chunk}.csv",
        high_active="results/high_active/{chunk}.csv",
        high_inactive="results/high_inactive/{chunk}.csv"
    conda:
        "workflow/envs/full_pipeline.yaml"
    shell:
        """
        mkdir -p results/all results/active results/inactive results/high_active results/high_inactive logs

        python workflow/scripts/worker.py {input} \
            {output.all} \
            {output.active} \
            {output.inactive} \
            {output.high_active} \
            {output.high_inactive}
        """

rule merge_all_predictions:
    input:
        expand("results/all/{chunk}.csv", chunk=CHUNKS)
    output:
        "results/ALL_predictions.csv"
    shell:
        "python workflow/scripts/merge_csv.py {output} {input}"

rule merge_active:
    input:
        expand("results/active/{chunk}.csv", chunk=CHUNKS)
    output:
        "results/ALL_active.csv"
    shell:
        "python workflow/scripts/merge_csv.py {output} {input}"

rule merge_inactive:
    input:
        expand("results/inactive/{chunk}.csv", chunk=CHUNKS)
    output:
        "results/ALL_inactive.csv"
    shell:
        "python workflow/scripts/merge_csv.py {output} {input}"

rule merge_high_active:
    input:
        expand("results/high_active/{chunk}.csv", chunk=CHUNKS)
    output:
        "results/ALL_high_active.csv"
    shell:
        "python workflow/scripts/merge_csv.py {output} {input}"

rule merge_high_inactive:
    input:
        expand("results/high_inactive/{chunk}.csv", chunk=CHUNKS)
    output:
        "results/ALL_high_inactive.csv"
    shell:
        "python workflow/scripts/merge_csv.py {output} {input}"