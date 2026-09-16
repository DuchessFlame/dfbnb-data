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

# --- roster fingerprint -------------------------------------------------------
# Chunks address the roster by OFFSET, so every chunk has to see the same roster
# in the same order. If the BOOK export is replaced part-way through a run, the
# later chunks index into a different list: plans shift position, and one plan
# lands in two chunks at two different offsets. After the merge those show up as
# exact duplicates INTERLEAVED through the file rather than in a block, which is
# why the 73 duplicates in the 16 Sept live build did not look like a chunking
# fault and were written up as a roster bug. A clean rebuild had none.
#
# That is not hypothetical here: the June-September exports were swapped out of
# tsv/ while that very build was running.
#
# So the roster identity is pinned at the start and re-checked before each
# chunk. A change stops the run instead of quietly producing a file that is
# wrong in a way nothing downstream can see.
FP_FILE="$OUT_DIR/.roster"
BOOK=$(python3 - "$DATA_DIR" <<'PY2'
import os, sys
sys.path.insert(0, "src")
import tsv_source
f = tsv_source.newest(os.path.join(sys.argv[1], "BOOK_Export_*.tsv"),
                      exclude="Locations", required=False)
print(os.path.basename(f) if f else "")
PY2
)
FP="$BOOK|$TOTAL"
if [ -s "$FP_FILE" ]; then
  PREV=$(cat "$FP_FILE")
  if [ "$PREV" != "$FP" ]; then
    echo "[$CHANNEL] *** ROSTER CHANGED SINCE THE PARTS IN $OUT_DIR WERE BUILT ***"
    echo "[$CHANNEL]     was: $PREV"
    echo "[$CHANNEL]     now: $FP"
    echo "[$CHANNEL] Resuming would mix two rosters and produce duplicate rows."
    echo "[$CHANNEL] Delete $OUT_DIR and start again."
    exit 2
  fi
else
  printf '%s' "$FP" > "$FP_FILE"
fi
echo "[$CHANNEL] roster pinned: $FP"

check_roster() {
  local now
  now=$(python3 - "$DATA_DIR" <<'PY3'
import csv, os, sys
sys.path.insert(0, "src")
import tsv_source
f = tsv_source.newest(os.path.join(sys.argv[1], "BOOK_Export_*.tsv"),
                      exclude="Locations", required=False)
n = 0
with open(f, encoding="utf-8", errors="replace") as fh:
    for r in csv.DictReader(fh, delimiter="\t"):
        full = (r.get("FULL") or "").strip()
        if full.startswith("Plan: ") or full.startswith("Recipe: "):
            n += 1
print(f"{os.path.basename(f)}|{n}")
PY3
)
  if [ "$now" != "$FP" ]; then
    echo "[$CHANNEL] *** THE ROSTER CHANGED MID-BUILD ***"
    echo "[$CHANNEL]     started with: $FP"
    echo "[$CHANNEL]     now sees    : $now"
    echo "[$CHANNEL] Every chunk after this point would index a different list."
    echo "[$CHANNEL] Stopping. Delete $OUT_DIR and start again once tsv/ is settled."
    exit 2
  fi
}

off=0
while [ "$off" -lt "$TOTAL" ]; do
  part="$OUT_DIR/part_$(printf '%06d' "$off").json"
  if [ -s "$part" ]; then
    echo "[$CHANNEL] $off skip (already built)"
  else
    check_roster
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
