import glob
from pathlib import Path

# =========================
# DETECT CHUNKS
# =========================

all_chunks = sorted(Path("chunks").glob("chunk_*.txt"))
done_chunks = set(Path(f).stem for f in glob.glob("results/active/*.csv"))

# =========================
# CLASSIFY
# =========================

done = []
remaining = []

for chunk in all_chunks:
    name = chunk.stem
    if name in done_chunks:
        done.append(name)
    else:
        remaining.append(name)

# =========================
# PRINT
# =========================

print(f"\nTotal chunks: {len(all_chunks)}")
print(f"Done: {len(done)}")
print(f"Remaining: {len(remaining)}")

print("\n--- DONE (last 10) ---")
for c in done[-10:]:
    print(c)

print("\n--- REMAINING (next 10) ---")
for c in remaining[:10]:
    print(c)
