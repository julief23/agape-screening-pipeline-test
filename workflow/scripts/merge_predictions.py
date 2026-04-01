# =========================
# INPUT FILES
# =========================

active_files = sorted(
    glob.glob("results/active/*.csv"),
    key=lambda x: int(Path(x).stem.split("_")[-1])
)

if not active_files:
    logger.warning("No active files found. Exiting merge.")
    sys.exit(0)

logger.info(f"Found {len(active_files)} active files")


# =========================
# MERGE FUNCTION
# =========================

def merge_files(file_list, output_file):

    if output_file.exists():
        logger.warning(f"{output_file} already exists — overwriting")

    header_written = False
    total_rows = 0

    with open(output_file, "w") as out:

        for f in file_list:
            path = Path(f)

            if not path.exists() or path.stat().st_size == 0:
                continue

            logger.info(f"Merging {path.name}")

            try:
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
                            logger.info(f"{total_rows:,} rows merged...")

            except Exception as e:
                logger.warning(f"Skipping {path.name}: {e}")

    logger.info(f"{output_file.name}: {total_rows} rows")