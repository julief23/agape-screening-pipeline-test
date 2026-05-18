import sys
from pathlib import Path


def chunk_key(path):
    try:
        return int(path.stem.split("_")[-1])
    except Exception:
        return path.stem


def main():
    if len(sys.argv) < 3:
        raise ValueError(
            "Usage: python merge_csv.py OUTPUT INPUT1 INPUT2 ..."
        )

    output_file = Path(sys.argv[1])
    input_files = [
        Path(f)
        for f in sys.argv[2:]
        if Path(f).exists()
    ]

    input_files = sorted(input_files, key=chunk_key)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    header_written = False
    total_rows = 0
    files_used = 0

    with open(output_file, "w") as out:
        for path in input_files:
            if path.stat().st_size == 0:
                continue

            files_used += 1

            with open(path, "r") as infile:
                for i, line in enumerate(infile):
                    if i == 0:
                        if not header_written:
                            out.write(line)
                            header_written = True
                        continue

                    out.write(line)
                    total_rows += 1

                    if total_rows % 1_000_000 == 0:
                        print(f"{total_rows:,} rows merged...")

    if not header_written:
        raise ValueError("All input files were empty. No header written.")

    print(f"Files merged: {files_used}")
    print(f"Rows merged : {total_rows:,}")
    print(f"Output file : {output_file}")


if __name__ == "__main__":
    main()