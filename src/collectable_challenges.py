#!/usr/bin/env python3
r"""
collectable_challenges.py — join the CHAL export onto a collectable spawn set.

WHY
===
Every "{Item} Spawn Locations" page under /df/collectables/<set>/spawn-locations/
has a matching set of in-game challenges ("Collect Pint-Sized Slasher Masks",
tiers 1-4). Those challenges live in the CHAL export with their required counts
and reward names, so the page can carry them without a word being typed by hand,
and a new tier added by Bethesda appears on the next build.

This module is the join. It is imported by build_collectable_spawns_json.py
(so a full rebuild emits the key) and by add_collectable_challenges.py (so the
committed dist can be stamped without a Mappalachia DB — the same split as
split_chance_spawns.py).

MATCHING — CNDF is the authority, EDID stem is the fallback
===========================================================
A CHAL row does NOT name the item it counts. Its condition is

    IsTrueForConditionForm(... [CNDF:008FB7C3])

and only the CNDF record holds the leaf check that names the item:

    GetIsID(SDOW_SlasherClue "Pint-Sized Slasher Mask" [MISC:008E069E])

So the primary match resolves every [CNDF:x] reference against the CNDF export
and compares the FormIDs it finds with the set's own item FormIDs (which come
free from the MISC2/ACTI2 export's item_formid column). Nothing is hardcoded.

The fallback exists because the two exports do not always arrive together. The
September 2026 live CHAL export carries the four Slasher challenges; the newest
live CNDF export is May 2026 and cannot possibly know them. Rather than publish
an empty block until someone re-exports CNDF, an UNRESOLVABLE reference falls
back to matching the item's EditorID stem (SDOW_SlasherClue -> "SlasherClue")
inside the condition text, which is how Bethesda names its condition forms
(SDOW_Challenge_Lifetime_Collect_Condition_SlasherClue03).

The fallback is deliberately narrow:
  * it only fires for a [CNDF:x] the export cannot resolve — a CNDF row that IS
    present and does NOT name the item is a real "no", never guessed over;
  * the stem must be >= MIN_STEM chars, so a short or generic EditorID can never
    sweep in unrelated challenges.
Each entry records which path matched it in `match`, and build_report() prints
the split, so a page quietly running on the fallback says so instead of looking
identical to a clean CNDF join.
"""

from __future__ import annotations

import csv
import io
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import tsv_source  # noqa: E402  — one resolver for every export selection

CHAL_GLOB = "CHAL_Export_*.tsv"
CNDF_GLOB = "CNDF_Export_*.tsv"

# The item-locations export (same file build_collectable_spawns_json.py reads):
# its `set` / `item_formid` / `edid` columns are where a set's items come from.
ITEMS_GLOB = os.environ.get("INPUT_GLOB", os.path.join(REPO, "tsv", "*2_Export_*.tsv"))

# Shortest EditorID stem allowed to match by name. "SlasherClue" is 11; anything
# under this is too generic to be evidence.
MIN_STEM = 8

# How deep to follow a condition form that references another condition form.
MAX_CNDF_DEPTH = 3

_REC_REF_RE = re.compile(r"\[([A-Z_]{4}):([0-9A-Fa-f]{6,8})\]")
_SCOPE_PREFIX_RE = re.compile(r"^\s*\([^)]*\)\s*")
# Leading mod/DLC code on an EditorID: SDOW_SlasherClue -> SlasherClue
_EDID_PREFIX_RE = re.compile(r"^[A-Z0-9]{2,6}_")

csv.field_size_limit(10_000_000)


# ── export readers ───────────────────────────────────────────────────────────

def _read_tsv(path):
    """Rows from an export TSV. Mirrors build_challenges_json_v3.read_tsv: some
    exports carry cp1252 bytes and stray NULs, and a hard decode error there
    would take the whole build down over one em-dash."""
    def _decode(enc):
        with open(path, encoding=enc, errors="replace", newline="") as fh:
            return fh.read().replace("\x00", "")
    try:
        raw = _decode("utf-8-sig")
    except UnicodeDecodeError:
        raw = _decode("cp1252")
    return list(csv.DictReader(io.StringIO(raw), delimiter="\t"))


def _cond_cells(row):
    """Every populated Cond1..CondN cell on a CHAL or CNDF row."""
    out = []
    for key, val in row.items():
        if key and key.startswith("Cond") and key[4:].isdigit() and (val or "").strip():
            out.append(val.strip())
    return out


def load_chal(channel="live", path=None):
    """(rows, path) for the newest CHAL export on this channel. ([], None) if absent."""
    p = path or tsv_source.newest(CHAL_GLOB, channel=channel, required=False)
    if not p:
        return [], None
    return list(_read_tsv(p)), p


def load_cndf(channel="live", path=None):
    """{FormID: {"edid": str, "refs": [...cond cells...]}} from the newest CNDF export."""
    p = path or tsv_source.newest(CNDF_GLOB, channel=channel, required=False)
    if not p:
        return {}, None
    idx = {}
    for row in _read_tsv(p):
        fid = (row.get("FormID") or "").strip().upper()
        if fid:
            idx[fid] = {"edid": (row.get("EDID") or "").strip(),
                        "conds": _cond_cells(row)}
    return idx, p


def load_set_items(items_glob=None):
    """{set-slug: {"formids": set[str], "edids": set[str]}} from the locations export.

    Read straight off the export's own `set` column — the same grouping
    build_collectable_spawns_json.py uses — so a new set needs no entry here.
    """
    import crossref_mappalachia_markers as xref
    paths = xref.newest_per_section(tsv_source.all_matching(items_glob or ITEMS_GLOB))
    sets = {}
    for p in paths:
        for row in _read_tsv(p):
            slug = (row.get("set") or "").strip()
            if not slug:
                continue
            bucket = sets.setdefault(slug, {"formids": set(), "edids": set()})
            fid = (row.get("item_formid") or "").strip().upper()
            edid = (row.get("edid") or "").strip()
            if fid:
                bucket["formids"].add(fid)
            if edid:
                bucket["edids"].add(edid)
    return sets


# ── matching ─────────────────────────────────────────────────────────────────

def _stems(edids):
    """Distinctive name fragments for a set's items, longest first."""
    out = set()
    for edid in edids:
        edid = (edid or "").strip()
        if not edid:
            continue
        for cand in (edid, _EDID_PREFIX_RE.sub("", edid)):
            if len(cand) >= MIN_STEM:
                out.add(cand.lower())
    return sorted(out, key=len, reverse=True)


def _resolve_targets(cells, cndf, depth=0, seen=None):
    """(FormIDs named by these conditions, unresolved CNDF refs, text walked).

    Follows [CNDF:x] references through the CNDF export. A reference the export
    does not carry is reported rather than swallowed, so the caller can decide
    whether to fall back.
    """
    seen = seen if seen is not None else set()
    found, missing, text = set(), set(), []
    for cell in cells:
        text.append(cell)
        for sig, fid in _REC_REF_RE.findall(cell):
            fid = fid.upper().zfill(8)
            if sig == "CNDF":
                if fid in seen or depth >= MAX_CNDF_DEPTH:
                    continue
                seen.add(fid)
                node = cndf.get(fid)
                if not node:
                    missing.add(fid)
                    continue
                text.append(node["edid"])
                sub_f, sub_m, sub_t = _resolve_targets(node["conds"], cndf, depth + 1, seen)
                found |= sub_f
                missing |= sub_m
                text.extend(sub_t)
            else:
                found.add(fid)
    return found, missing, text


def _clean_name(full):
    """Drop the leading scope tag the game carries: "(Seasonal) Collect X" -> "Collect X"."""
    return _SCOPE_PREFIX_RE.sub("", (full or "").strip()).strip()


def _as_int(val):
    val = (val or "").strip()
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def build_for_items(formids, edids, chal_rows, cndf):
    """The `challenges` block for one set, or None when nothing matches."""
    want = {f.upper().zfill(8) for f in formids if f}
    stems = _stems(edids)
    items = []

    for row in chal_rows:
        cells = _cond_cells(row)
        if not cells:
            continue
        found, missing, text = _resolve_targets(cells, cndf)

        match = ""
        if found & want:
            match = "cndf"
        elif missing and stems:
            # Only a reference the CNDF export cannot resolve earns the fallback —
            # a CNDF row that resolved and didn't name the item is a real "no".
            blob = " ".join(text + [row.get("EDID") or ""]).lower()
            if any(stem in blob for stem in stems):
                match = "edid"
        if not match:
            continue

        full = (row.get("FULL") or "").strip()
        items.append({
            "form_id": (row.get("FormID") or "").strip().upper(),
            "edid": (row.get("EDID") or "").strip(),
            "full": full,
            "name": _clean_name(full) or full,
            "required": _as_int(row.get("TNAM")),
            "counter": (row.get("SNAM") or "").strip(),
            "scope": (row.get("CNAM") or "").strip(),
            "reward": (row.get("MNAM") or "").strip(),
            "reward_icon": (row.get("RNAM") or "").strip(),
            "match": match,
        })

    if not items:
        return None

    items.sort(key=lambda c: (c["required"] if c["required"] is not None else 10 ** 9,
                              c["name"].lower()))

    scopes = {c["scope"] for c in items if c["scope"]}
    heading = f"{scopes.pop()} Challenges" if len(scopes) == 1 else "Challenges"
    return {"heading": heading, "items": items}


_CTX = {}


def context(channel="live"):
    """(chal_rows, cndf_index, set_items) for a channel, read once per process.

    The CHAL export is ~5,700 rows and the locations export is every placement in
    the game; build_set() is called once per set, so this is cached rather than
    re-read per page.
    """
    if channel not in _CTX:
        chal, chal_path = load_chal(channel)
        cndf, cndf_path = load_cndf(channel)
        _CTX[channel] = (chal, cndf, load_set_items(), chal_path, cndf_path)
    chal, cndf, sets, _, _ = _CTX[channel]
    return chal, cndf, sets


def context_paths(channel="live"):
    """(chal_path, cndf_path) actually consumed — print them, don't assume them."""
    if channel not in _CTX:
        context(channel)
    return _CTX[channel][3], _CTX[channel][4]


def challenges_for_set(slug, chal_rows=None, cndf=None, set_items=None, channel="live"):
    """Convenience wrapper: everything loaded (and cached) for you, for one set slug."""
    if chal_rows is None or cndf is None or set_items is None:
        c_rows, c_cndf, c_sets = context(channel)
        chal_rows = c_rows if chal_rows is None else chal_rows
        cndf = c_cndf if cndf is None else cndf
        set_items = c_sets if set_items is None else set_items
    bucket = set_items.get(slug)
    if not bucket or not chal_rows:
        return None
    return build_for_items(bucket["formids"], bucket["edids"], chal_rows, cndf)


def build_report(block):
    """One line describing how a block was matched — print it, don't guess later."""
    if not block or not block.get("items"):
        return "no challenges matched"
    by = {}
    for it in block["items"]:
        by[it["match"]] = by.get(it["match"], 0) + 1
    bits = ", ".join(f"{n} by {k}" for k, n in sorted(by.items()))
    warn = "  [!] CNDF export predates these challenges" if by.get("edid") else ""
    return f"{len(block['items'])} challenge(s) ({bits}){warn}"
