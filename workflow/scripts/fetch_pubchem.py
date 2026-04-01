from pathlib import Path
import urllib.request
import os
import gzip
import logging
from logging.handlers import RotatingFileHandler
import sys

# =========================
# LOGGING SETUP
# =========================

log_file = Path("logs/fetch_pubchem.log")
log_file.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("fetch")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
file_handler = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=3)

formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# =========================
# CONFIG
# =========================

url = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-SMILES.gz"

output = Path("data/raw/CID-SMILES.gz")
chunks_dir = Path("chunks")

CHUNK_SIZE = 100000

output.parent.mkdir(parents=True, exist_ok=True)
chunks_dir.mkdir(parents=True, exist_ok=True)

logger.info("Starting PubChem download...")

# =========================
# DOWNLOAD
# =========================

if not output.exists():
    tmp_output = output.with_suffix(".gz.tmp")

    try:
        urllib.request.urlretrieve(url, tmp_output)
        tmp_output.rename(output)
        logger.info(f"Saved to {output}")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        if tmp_output.exists():
            os.remove(tmp_output)
        raise
else:
    logger.info(f"File already exists: {output}")

# =========================
# SPLIT INTO CHUNKS
# =========================

logger.info("Starting chunking...")

# remove old chunks
for old_chunk in chunks_dir.glob("chunk_*.txt"):
    old_chunk.unlink()

chunk_index = 0
current_chunk_lines = 0
total_lines = 0
chunk_file = None


def open_new_chunk(idx):
    filename = chunks_dir / f"chunk_{idx:04d}.txt"
    logger.info(f"Creating {filename}")
    return open(filename, "w")


with gzip.open(output, "rt") as f:

    chunk_file = open_new_chunk(chunk_index)

    for line in f:
        chunk_file.write(line)

        current_chunk_lines += 1
        total_lines += 1

        if current_chunk_lines == CHUNK_SIZE:
            chunk_file.close()
            chunk_index += 1
            current_chunk_lines = 0
            chunk_file = open_new_chunk(chunk_index)

            if chunk_index % 50 == 0:
                logger.info(f"{total_lines:,} lines processed")

    if chunk_file:
        chunk_file.close()

# remove last chunk if empty
last_chunk = chunks_dir / f"chunk_{chunk_index:04d}.txt"
if last_chunk.exists() and last_chunk.stat().st_size == 0:
    last_chunk.unlink()
else:
    chunk_index += 1

logger.info(f"Chunking complete. Total chunks: {chunk_index}")