#!/usr/bin/env bash
# Chunked, resumable plan_master build.
#
# A full run is hours, and this sandbox restarted mid-build once already and took
# both runs with it — no error, just a truncated log and no output. So the roster
# is built in chunks via the builder's own --offset/--limit (which is what those
# flags are for), each chunk written to its own part file. Re-running skips every
# part that already exists, so a restart costs one chunk, not the whole run.
#
#   ./build_chunked.sh live   tsv       /tmp/parts_live
#   ./build_chunked.sh pts    tsv/pts   /tmp/parts_pts
#
# Then merge_parts.py stitches the parts back into one plan_master.json.
set -u
CHANNEL="$1"; DATA_DIR="$2"; OUT_DIR="$3"
CHUNK="${CHUNK:-600}"
cd "$(dirname "$0")"
mkdir -p "$OUT_DIR"

# Roster size: the builder prints it, but asking the BOOK export directly avoids
# a two-minute index load just to count rows.
TOTAL=$(python3 - "$DATA_DIR" <<'PY'
import csv, glob, os, sys
sys.path.insert(0, "src")
import tsv_source
f = tsv_source.newest(os.path.join(sys.argv[1], "BOOK_Export_*.tsv"), exclude="Locations", required=False)
n = 0
with open(f, encoding="utf-8", errors="replace") as fh:
    for r in csv.DictReader(fh, delimiter="\t"):
        full = (r.get("FULL") or "").strip()
        if full.startswith("Plan: ") or full.startswith("Recipe: "):
            n += 1
print(n)
PY
)
echo "[$CHANNEL] roster $TOTAL, chunk $CHUNK"

off=0
while [ "$off" -lt "$TOTAL" ]; do
  part="$OUT_DIR/part_$(printf '%06d' "$off").json"
  if [ -s "$part" ]; then
    echo "[$CHANNEL] $off skip (already built)"
  else
    echo "[$CHANNEL] $off building ..."
    python3 -u src/build_plan_obtain_json.py \
      --data-dir "$DATA_DIR" --offset "$off" --limit "$CHUNK" \
      --out "$part.tmp" >/dev/null 2>"$OUT_DIR/err_$off.log"
    if [ -s "$part.tmp" ]; then
      mv "$part.tmp" "$part"           # atomic: a half-written part is never "done"
      echo "[$CHANNEL] $off done"
    else
      echo "[$CHANNEL] $off FAILED (see $OUT_DIR/err_$off.log)"
      exit 1
    fi
  fi
  off=$((off + CHUNK))
done
echo "[$CHANNEL] all chunks present"
