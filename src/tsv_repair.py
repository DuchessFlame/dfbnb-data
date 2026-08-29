#!/usr/bin/env python3
r"""
tsv_repair.py — the one place that repairs quote-split rows in an xEdit export.

WHY THIS EXISTS
===============
Every `!!!Wordpress - Export*ToTSV.pas` packs a record into one delimited string,
stores it, then unpacks it with:

    parts.Delimiter := FS;  parts.StrictDelimiter := True;
    parts.DelimitedText := rec;

`TStringList.DelimitedText` applies QUOTE processing. `StrictDelimiter := True`
only stops whitespace acting as a delimiter — it does **not** disable
`QuoteChar`, which defaults to `"`. So a field whose value STARTS with a double
quote is re-parsed as a quoted token: the opening quote is consumed, the closing
quote terminates the token, and whatever followed becomes an extra list entry.
The output line is then assembled by fixed index (`parts[2] + TAB + parts[3]…`),
so every column after that field shifts one place right and the tail drops off
the end of the row.

Ground truth — the same ENTM record in two exports:

    March : FULL = '"Gulper" Player Title Suffix'
    July  : FULL = 'Gulper'   DESC = ' Player Title Suffix'   (rest +1)

None of the 31 export scripts that use `DelimitedText` set `QuoteChar`. The
permanent fix is one line in each `.pas` (`parts.QuoteChar := #0;`). This module
repairs the exports already committed, which CI still builds from.

WHAT IS ACTUALLY AFFECTED
=========================
The bug can only bite a value that *starts* with a quote, so most record types
are immune — their names never do. Measured across every committed export
(tools/repair_export_quote_splits.py --dry-run), the newest of each family:

    ENTM   219 rows live, 242 on PTS   <- the one that matters
    BOOK     3 rows
    ACTI     3 rows
    COBJ     1 row
    MISC     1 row
    NOTE     1 row
    everything else — nothing, at any version

Names like `"Improved" Slot Machine`, `"Megasloth" Pelt Rug` and
`"Excavator Suit" - Final Steps` are the whole exposure: a quoted word at the
very start of a display name. ENTM dominates because the Atom Shop names
hundreds of player titles and icons that way.

HOW THE REPAIR WORKS
====================
1. Find the ANCHOR — the modal column index of a distinctive value pattern
   (":KYWD", a "Textures/" path, a ".dds" filename). Header names are not
   trusted: ALCH has a `Keywords_Flat` column holding a list with no ":KYWD" in
   it and the real keywords one column later.
2. A row whose anchor sits N places right of the mode was split N times.
3. Choose which N boundaries are spurious by scoring every combination. A
   genuine split leaves a recognisable shape: the right-hand fragment starts
   with a space (the value was `"A" B`) — proof, since nothing else writes a
   field that begins blank — or it is empty (the value was `"A"`), which is
   merely consistent and scores far lower, because every unused trailing column
   looks the same. The repaired row must also keep a valid flags value in the
   flags column, keep the ALL-CAPS boilerplate trailer inside DESC, and leave no
   field starting with a space. Ties go to the rightmost merge. If the winning
   combination shows no split shape at all the row is left untouched, so a
   misfiring anchor can never invent a merge.
4. Join the fragments back together WITHOUT re-inserting the quote characters.
   The newer exports strip quotes from other fields anyway (July's NNAM is
   `Gulper` where March's was `"Gulper"`), so re-adding them here would invent
   punctuation that the rest of the row no longer carries.

ACCURACY
========
Validated against `ENTM_Export_March_2026.tsv`, the last export taken before the
regression. Of the 219 rows the repair re-joined, 192 also exist in March and
**191 reproduce it exactly** on FULL/DESC/NNAM. The single difference is a real
game-data change — Bethesda renamed "N.C.R. Trooper" to "NCR Trooper", which two
untouched sibling records confirm.

Across the whole file, 5,936 of the 5,961 records present in both agree; the 25
that differ are ordinary drift between March and July (DESC rewrites, `T45` to
`T-45`, `Beckly` to `Beckley`) and none of them is a row the repair touched.
Re-run that check any time with:

    python src/tsv_repair.py --verify

It reads the file as it stands, so it keeps working after the corpus has been
repaired, and fails loudly if the drift ever jumps.

USAGE
=====
    import tsv_repair

    rows = tsv_repair.read_dicts(path)            # list[dict], repaired
    header, rows = tsv_repair.read(path)          # positional, repaired
    rows = tsv_repair.repair_dicts(header, rows)  # already-parsed rows

`read_dicts` is a drop-in for `list(csv.DictReader(f, delimiter="\t"))`. It also
handles the encodings in the corpus (a few older exports are cp1252) and reads
with QUOTE_NONE, so a mid-string quote never confuses Python's csv either.
"""

from __future__ import annotations

import csv
import itertools
import os
import re
import sys

TAG = "[tsv-repair]"

# Encodings present in the export corpus, in order.
_ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")

# Distinctive value patterns, most specific first. Position comes from the data
# (modal index), never from the header name.
_ANCHORS = (
    ("keywords", lambda v: ":KYWD" in v),
    ("texture", lambda v: v[:9].lower() == "textures/"),
    ("dds", lambda v: v[-4:].lower() == ".dds"),
    # Last resort, and it earns its place: BOOK's three damaged rows have no
    # keywords and no texture, so the only stable landmark left is the small
    # integer in KeywordCount. Kept last because plenty of columns hold small
    # integers and the earlier patterns are far more specific.
    ("count", lambda v: v.isdigit() and len(v) <= 4),
)

# ALL-CAPS store boilerplate that ends nearly every DESC:
#   "  - UNLOCKS A PLAYER ICON. -"   "  - C.A.M.P. ITEMS APPEAR WHILE... -"
_TRAILER = re.compile(r"-\s*[A-Z0-9][A-Z0-9 ,.'&/\-]{6,}\.\s*-")

# XALG_Flags is a tiny closed vocabulary; anything else in that column means the
# row is still misaligned.
_FLAG_VALUES = {"", "Premium", "Fallout 1st"}

# A merge is only made when the evidence includes at least one space-led
# fragment, which is what this threshold encodes (see _shape_score).
#
# The guarantee is arithmetic, not aspirational: the most a combination can
# score without a single space-led fragment is _MAX_SHIFT merges x 3 points. If
# that ever reaches _SPLIT_PROOF, rows with nothing but empty fragments start
# passing the veto and the false-repair bug that hit CHAL, NPC and WEAP comes
# straight back. The assert below keeps the two constants honest.
_SPLIT_PROOF = 12

_MIN_ROWS_FOR_ANCHOR = 20
_MODE_DOMINANCE = 0.6

_stats = {"files": 0, "rows": 0, "repaired": 0}


def stats() -> dict:
    """Counters since import — handy for a builder to print what it fixed."""
    return dict(_stats)


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------
def _read_raw(path: str):
    last = None
    for enc in _ENCODINGS:
        try:
            with open(path, encoding=enc, newline="") as f:
                rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
                header = next(rd)
                return header, [r for r in rd if r]
        except UnicodeDecodeError as e:
            last = e
    raise UnicodeDecodeError(*last.args) if last else RuntimeError(path)


def _pick_anchors(rows):
    """Every anchor that dominates, most specific first, with its modal index.

    A list rather than one winner: BOOK rows carrying keywords are judged by the
    ":KYWD" landmark, and the keyword-less rows — which is exactly where its
    damaged rows live — fall through to the next anchor instead of going
    unjudged. Each anchor keeps its own modal index.
    """
    sample = rows[:3000]
    found = []
    for name, test in _ANCHORS:
        positions = []
        for r in sample:
            at = next((i for i, v in enumerate(r) if test(v or "")), None)
            if at is not None:
                positions.append(at)
        if len(positions) < max(_MIN_ROWS_FOR_ANCHOR, len(sample) // 4):
            continue
        counts = {}
        for p in positions:
            counts[p] = counts.get(p, 0) + 1
        mode, n = max(counts.items(), key=lambda kv: kv[1])
        if n >= _MODE_DOMINANCE * len(positions):
            found.append((name, mode, test))
    return found


def _pick_anchor(rows):
    """Back-compat single-anchor view, used by the self-check."""
    found = _pick_anchors(rows)
    return found[0] if found else None


# A row is split once per quote-led field, and the head only holds three or four
# text fields. Two is the most ever observed. Anything beyond this is an anchor
# misfiring on a row whose landmark column happens to be empty — BOOK's Bo-Peep
# note reports a shift of 7 that way — so fall through to the next anchor
# instead of trusting it.
_MAX_SHIFT = 3

# See the _SPLIT_PROOF note: without this, a row of empty fragments could clear
# the veto on arithmetic alone.
assert _MAX_SHIFT * 3 < _SPLIT_PROOF, (
    "a combination of empty fragments must never be able to reach _SPLIT_PROOF"
)


def _shift_of(row, anchors):
    """How far right this row has slipped, by the first anchor that can tell."""
    for _name, expected, test in anchors:
        at = next((i for i, v in enumerate(row) if test(v or "")), None)
        if at is None:
            continue
        shift = at - expected
        if shift <= 0:
            return at, 0
        if shift <= _MAX_SHIFT:
            return at, shift
    return None, 0


# ---------------------------------------------------------------------------
# Repair
# ---------------------------------------------------------------------------
def _merge(row, boundaries):
    out = list(row)
    for i in sorted(boundaries, reverse=True):
        out[i - 1] = out[i - 1] + out[i]
        del out[i]
    return out


def _shape_score(original, boundaries):
    """Does each proposed merge actually look like an undone quote-split?

    This is the veto, not a preference. A real split leaves the right-hand
    fragment empty (the value was `"A"`) or space-led (it was `"A" B`), and the
    left-hand fragment non-empty. Anything else means the anchor misfired and
    the two fields are simply unrelated — which is what happens on CHAL, whose
    only usable landmark is a small integer and whose rows would otherwise get
    `10000` + `Event` + `Sub Challenge (Unsorted)` welded into one cell.

    The two shapes are not equally telling. A space-led fragment can only have
    come from a split — no exporter writes a field that starts with a space —
    so it scores high. An empty fragment is merely *consistent* with a split and
    is also what every unused trailing column looks like, so it scores low.
    Weighting them the same let an ordinary empty COBJ column outrank the real
    break and "fix" the row in the wrong place.
    """
    s = 0
    for i in boundaries:
        left, right = original[i - 1], original[i]
        if not left.strip():
            s -= 20                     # nothing to hang a quoted value on
        elif right[:1] == " ":
            s += 12                     # the  "A" B  shape — unambiguous
        elif right == "":
            s += 3                      # the  "A"  shape — plausible, not proof
        else:
            s -= 20                     # two unrelated fields
    return s


def _score(candidate, original, boundaries, cols):
    """Higher is more likely to be the true reconstruction."""
    s = _shape_score(original, boundaries)
    flags = cols.get("flags")
    if flags is not None and candidate[flags] in _FLAG_VALUES:
        s += 10
    desc = cols.get("desc")
    if desc is not None and _TRAILER.search(candidate[desc] or ""):
        s += 6
    for key in ("full", "desc", "nnam"):
        i = cols.get(key)
        if i is not None and candidate[i][:1] == " ":
            s -= 8                      # a field never legitimately starts blank
    return s


def _columns(header):
    """Map the scoring roles onto this export's header, where they exist."""
    upper = [h.upper() for h in header]

    def find(*names):
        for n in names:
            if n in upper:
                return upper.index(n)
        return None

    return {
        "full": find("FULL"),
        "desc": find("DESC"),
        "nnam": find("NNAM"),
        "flags": find("XALG_FLAGS", "XALG"),
    }


def repair_rows(header, rows, path_hint=""):
    """Return positional rows with every quote-split row re-joined."""
    anchors = _pick_anchors(rows)
    if not anchors:
        return rows, 0
    primary = anchors[0]
    cols = _columns(header)
    width = len(header)
    head_start = min(2, primary[1])     # FormID + EDID never split
    fixed_rows = []
    fixed = 0

    for row in rows:
        _n, expected, test = primary
        if expected < len(row) and test(row[expected] or ""):
            fixed_rows.append(row)      # fast path: already aligned
            continue
        # Pad short rows, but never SLICE an over-wide one on a path that is not
        # repairing it. `row[:width]` here would silently drop the tail of a row
        # the export wrote wider than its own header — a quiet data loss in
        # read()/read_dicts(), which builders are meant to adopt as a drop-in.
        # Nothing in the corpus is currently over-wide; keep it that way by
        # returning such a row untouched rather than trimming it.
        row = row + [""] * (width - len(row))
        at, shift = _shift_of(row, anchors)
        if shift <= 0:
            fixed_rows.append(row)
            continue
        best = None
        for combo in itertools.combinations(range(head_start + 1, at + 1), shift):
            cand = _merge(row, combo) + [""] * shift
            # Ties go to the rightmost merge: an orphaned fragment belongs to
            # the field it directly follows.
            key = (_score(cand, row, combo, cols), sum(combo))
            if best is None or key > best[0]:
                best = (key, cand, combo)
        # Veto: require at least one space-led fragment in the winning
        # combination — the only shape that is proof rather than coincidence.
        # An empty fragment alone is not enough: an NPC with no outfit and a
        # WEAP row with no ammo both look exactly like `"A"` followed by
        # nothing, and taking that as evidence "repaired" ten rows that were
        # never broken. Anything short of proof is left exactly as it was.
        if best is None or _shape_score(row, best[2]) < _SPLIT_PROOF:
            fixed_rows.append(row)          # untouched: never trim (see above)
            continue
        fixed_rows.append(best[1][:width])
        fixed += 1

    if fixed and path_hint:
        print(f"{TAG} {os.path.basename(path_hint)}: re-joined {fixed} quote-split row(s)")
    return fixed_rows, fixed


def read(path):
    """(header, positional rows) with the repair applied."""
    header, rows = _read_raw(path)
    rows, fixed = repair_rows(header, rows, path)
    _stats["files"] += 1
    _stats["rows"] += len(rows)
    _stats["repaired"] += fixed
    return header, rows


def read_dicts(path):
    """Drop-in for list(csv.DictReader(f, delimiter='\\t')), but repaired."""
    header, rows = read(path)
    width = len(header)
    return [dict(zip(header, r + [""] * (width - len(r)))) for r in rows]


def repair_dicts(header, dict_rows, path_hint=""):
    """Repair rows that some other reader already turned into dicts."""
    positional = [[r.get(h, "") or "" for h in header] for r in dict_rows]
    fixed_rows, _ = repair_rows(header, positional, path_hint)
    return [dict(zip(header, r)) for r in fixed_rows]


# ---------------------------------------------------------------------------
# Self-check
# ---------------------------------------------------------------------------
def _verify():
    """Diff the newest ENTM against the last clean export, record by record.

    This runs on the file as it stands, repaired or not, so it stays meaningful
    after tools/repair_export_quote_splits.py has been over the corpus — the
    earlier version only inspected rows the repair changed in memory, so once
    the file on disk was fixed it had nothing left to compare and reported a
    vacuous 0 of 0.

    `ENTM_Export_March_2026.tsv` predates the regression. FULL/DESC/NNAM sit at
    indices 2/3/4 in both schemas even though the columns diverge after that, so
    those three are comparable. Quotes are ignored: the newer export strips them
    everywhere, which is a separate change from the split.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    tsv = os.path.join(here, "..", "tsv")
    new = os.path.join(tsv, "ENTM_Export_July_2026.tsv")
    ref = os.path.join(tsv, "ENTM_Export_March_2026.tsv")
    if not (os.path.exists(new) and os.path.exists(ref)):
        print(f"{TAG} verify: reference exports not present, skipping")
        return 0

    new_header, new_rows = read(new)
    ref_header, ref_rows = _read_raw(ref)
    reference = {r[0]: dict(zip(ref_header, r)) for r in ref_rows}

    def norm(v):
        return " ".join((v or "").replace('"', "").split())

    fields = ("FULL", "DESC", "NNAM")
    checked = exact = 0
    misses = []
    for row in new_rows:
        ref_row = reference.get(row[0])
        if not ref_row:
            continue
        checked += 1
        got = dict(zip(new_header, row))
        if all(norm(got.get(f)) == norm(ref_row.get(f)) for f in fields):
            exact += 1
        elif len(misses) < 40:
            misses.append(row[1] if len(row) > 1 else row[0])

    drift = checked - exact
    print(f"{TAG} verify: {checked} records present in both the newest and the "
          f"clean March export; {exact} agree on FULL/DESC/NNAM")
    print(f"{TAG} verify: {drift} differ — expected to be real game-data changes")
    for m in misses:
        print(f"{TAG}   differs: {m}")
    # Around two dozen records legitimately drift between March and July
    # (renames, DESC rewrites). A sudden jump means the repair has regressed.
    if drift > 40:
        print(f"{TAG} [FAIL] far more drift than the ~25 expected — "
              f"check the repair before trusting this export")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(_verify() if "--verify" in sys.argv else _verify())
