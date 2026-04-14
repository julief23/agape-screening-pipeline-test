from pathlib import Path

# =========================
# INPUTS FROM SNAKEMAKE
# =========================

input_files = [Path(f) for f in snakemake.input]

# keep only files that truly exist
input_files = [f for f in input_files if f.exists()]

# sort by chunk number when possible
def chunk_key(path):
    try:
        return int(path.stem.split("_")[-1])
    except Exception:
        return path.stem

input_files = sorted(input_files, key=chunk_key)

output_file = Path(snakemake.output[0])

# =========================
# CHECK INPUTS
# =========================

if not input_files:
    raise ValueError("No active CSV files found to merge.")

# =========================
# MERGE
# =========================

header_written = False
total_rows = 0
files_used = 0

output_file.parent.mkdir(parents=True, exist_ok=True)

with open(output_file, "w") as out:

    for path in input_files:

        # skip empty files
        if path.stat().st_size == 0:
            continue

        print(f"Merging {path.name}")
        files_used += 1

        with open(path, "r") as infile:

            for i, line in enumerate(infile):

                # first line = header
                if i == 0:
                    if not header_written:
                        out.write(line)
                        header_written = True
                    continue

                out.write(line)
                total_rows += 1

                if total_rows % 1_000_000 == 0:
                    print(f"{total_rows:,} rows merged...")

# =========================
# FINAL CHECK
# =========================

if not header_written:
    raise ValueError("All input files were empty. No header written.")

print(f"Files merged : {files_used}")
print(f"Rows merged  : {total_rows:,}")
print(f"Output file  : {output_file}")