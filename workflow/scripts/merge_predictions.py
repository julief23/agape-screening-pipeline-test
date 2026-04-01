from pathlib import Path
import glob
import logging
from logging.handlers import RotatingFileHandler
import sys

# =========================
# LOGGING SETUP
# =========================

log_file = Path("logs/merge.log")
log_file.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("merge")
logger.setLevel(logging.INFO)

# prevent duplicate handlers
if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=2)

formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


# =========================
# OUTPUT FILES
# =========================

output_active = Path("results/ALL_high_active.csv")
output_inactive = Path("results/ALL_high_inactive.csv")

output_active.parent.mkdir(parents=True, exist_ok=True)
output_inactive.parent.mkdir(parents=True, exist_ok=True)


# =========================
# INPUT FILES
# =========================

active_files = sorted(glob.glob("results/*_high_active.csv"))
inactive_files = sorted(glob.glob("results/*_high_inactive.csv"))

logger.info(f"Found {len(active_files)} active files")
logger.info(f"Found {len(inactive_files)} inactive files")


# =========================
# MERGE FUNCTION
# =========================

def merge_files(file_list, output_file):

    header_written = False
    total_rows = 0

    with open(output_file, "w") as out:

        for f in file_list:
            path = Path(f)

            if not path.exists() or path.stat().st_size == 0:
                continue

            with open(path, "r") as infile:
                lines = infile.readlines()

                if not lines:
                    continue

                if not header_written:
                    out.write(lines[0])
                    header_written = True

                data_lines = lines[1:]
                out.writelines(data_lines)

                total_rows += len(data_lines)

    logger.info(f"{output_file.name}: {total_rows} rows")


# =========================
# RUN MERGE
# =========================

logger.info("Starting merge...")

merge_files(active_files, output_active)
merge_files(inactive_files, output_inactive)

logger.info("Merge completed successfully")