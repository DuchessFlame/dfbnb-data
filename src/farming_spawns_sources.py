#!/usr/bin/env python3
r"""
farming_spawns_sources.py — resolve, per farming item, the FULL set of world sources
that can yield it, straight from the committed game-file exports in tsv/.

Adapted from nuka_cola_spawns_sources.py.  Handles both ALCH items (Cream, Deathclaw
Egg) and MISC items (Cracked Deathclaw Egg).

For each item set this yields:
  * lvli_closure  — every LVLI FormID that can roll any item in the set
  * placed_bases  — {base_formid: {sig, edid, source_type, via}} for CONT/ACTI/FURN/
                    NPC_/MSTT that hold any list in the closure (query Mappalachia
                    Position by these to get coords)
  * direct_refrs  — {refr_formid: {edid}} the item/list is placed directly (already a
                    placement; look up its coords by instanceFormID)

source_type is a friendly bucket for the page: vendor · collectron · nest · container ·
npc · direct · quest-reward · loot-list.

This module only READS tsv/ — it needs no Mappalachia DB.  The build scripts import
get_sources() and feed the FormIDs to the Position lookup.
"""

import os, re, csv, glob

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV = os.path.join(REPO, "tsv")

# Signatures we treat as a real world holder/placement of a list.
PLACED_SIGS = {"CONT", "ACTI", "FURN", "NPC_", "MSTT", "REFR", "FLOR"}


# ── file helpers ─────────────────────────────────────────────────────────────────
def _newest(patterns, tsv_root=None):
    root = tsv_root or TSV
    for pat in patterns:
        hits = sorted(glob.glob(os.path.join(root, pat)), key=os.path.getmtime, reverse=True)
        if hits:
            return hits[0]
    return None


def _fid(token):
    """'0059A5A6:LL_Drink_...:LVLI' or bare FormID -> ('0059A5A6', 'LL_Drink_...', 'LVLI')."""
    if not token:
        return "", "", ""
    parts = token.split(":")
    fid = parts[0].strip().upper()
    sig = parts[-1].strip() if len(parts) >= 2 else ""
    edid = parts[1].strip() if len(parts) >= 3 else ""
    return fid, edid, sig


# ── TSV loaders ──────────────────────────────────────────────────────────────────
def _load_child_to_parents(entries_path):
    """child FormID -> set(parent LVLI FormID), from LVLI_Entries (col LVLO_Reference)."""
    c2p = {}
    parent_edid = {}
    with open(entries_path, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            parent = (r.get("LVLI_FormID") or "").strip().upper()
            child, _, _ = _fid(r.get("LVLO_Reference") or "")
            if parent and child:
                c2p.setdefault(child, set()).add(parent)
                parent_edid[parent] = (r.get("LVLI_EDID") or "").strip()
    return c2p, parent_edid


def _load_lvli_refs(refs_path):
    """LVLI FormID -> list of (ref_formid, ref_edid, ref_sig)."""
    out = {}
    with open(refs_path, encoding="utf-8", errors="replace") as f:
        rd = csv.DictReader(f, delimiter="\t")
        refcols = [c for c in rd.fieldnames if re.fullmatch(r"Ref\d+", c or "")]
        for r in rd:
            lv = (r.get("LVLI_FormID") or "").strip().upper()
            if not lv:
                continue
            refs = []
            for c in refcols:
                tok = r.get(c)
                if tok:
                    refs.append(_fid(tok))
            if refs:
                out[lv] = refs
    return out


def _load_alch_refs(alch_path):
    """ALCH FormID -> list of (ref_formid, ref_edid, ref_sig), from Ref_1..N."""
    out = {}
    with open(alch_path, encoding="utf-8", errors="replace") as f:
        rd = csv.DictReader(f, delimiter="\t")
        refcols = [c for c in rd.fieldnames if re.fullmatch(r"Ref_\d+", c or "")]
        for r in rd:
            fid = (r.get("ALCH_FormID") or "").strip().upper()
            if not fid:
                continue
            refs = [_fid(r.get(c)) for c in refcols if r.get(c)]
            if refs:
                out[fid] = refs
    return out


def _load_misc_refs(misc_path):
    """MISC FormID -> list of (ref_formid, ref_edid, ref_sig), from Ref1..N."""
    out = {}
    with open(misc_path, encoding="utf-8", errors="replace") as f:
        rd = csv.DictReader(f, delimiter="\t")
        # MISC_Export uses Ref1, Ref2, ... (no underscore, unlike ALCH's Ref_1, Ref_2)
        refcols = [c for c in rd.fieldnames if re.fullmatch(r"Ref\d+", c or "")]
        for r in rd:
            fid = (r.get("FormID") or "").strip().upper()
            if not fid:
                continue
            refs = [_fid(r.get(c)) for c in refcols if r.get(c)]
            if refs:
                out[fid] = refs
    return out


# ── classify source type ─────────────────────────────────────────────────────────
def classify(sig, edid, via_edid):
    e = (edid or "").lower() + " " + (via_edid or "").lower()
    if "collectron" in e or "slowroaster" in e:
        return "collectron"
    if "vend" in e or "vendor" in e:
        return "vendor"
    if "nest" in e:
        return "nest"
    if "dispenser" in e:
        return "dispenser"
    if "questreward" in e or "quest_reward" in e or "_reward" in e:
        return "quest-reward"
    if sig == "NPC_":
        return "npc"
    if sig == "REFR":
        return "direct"
    if sig == "CONT":
        return "container"
    return "loot-list"


# ── LVLI closure walk ────────────────────────────────────────────────────────────
def _closure(seeds, c2p):
    """All LVLI FormIDs reachable UP from the seeds (lists that can roll the item)."""
    seen, stack = set(), list(seeds)
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        for p in c2p.get(n, ()):
            if p not in seen:
                stack.append(p)
    return seen


# ── public API ───────────────────────────────────────────────────────────────────
def load_tables(tsv_root=None):
    """Load all TSV tables needed for source resolution."""
    entries = _newest(["LVLI_Export_*_LVLI_Entries.tsv"], tsv_root)
    refs    = _newest(["LVLI_Export_*_LVLI_Refs.tsv"], tsv_root)
    alch    = _newest(["ALCH_Export_*.tsv"], tsv_root)
    misc    = _newest(["MISC_Export_*.tsv"], tsv_root)
    missing = [n for n, p in [("LVLI_Entries", entries), ("LVLI_Refs", refs)] if not p]
    if missing:
        raise FileNotFoundError(f"missing tsv export(s): {missing} in {tsv_root or TSV}")
    c2p, parent_edid = _load_child_to_parents(entries)
    return {
        "c2p": c2p, "parent_edid": parent_edid,
        "lvli_refs": _load_lvli_refs(refs),
        "alch_refs": _load_alch_refs(alch) if alch else {},
        "misc_refs": _load_misc_refs(misc) if misc else {},
    }


def get_sources(items_cfg, tables, extra_world_bases=None):
    """Return {'lvli_closure', 'placed_bases', 'direct_refrs'} for a list of item configs.

    Each item_cfg dict has: formid, edid, full, sig (ALCH or MISC).
    Multiple items (e.g. both deathclaw egg types) are combined into one result.

    extra_world_bases: optional list of {formid, edid, sig, source_type} dicts for
    world-placed records that won't be found through the LVLI closure or ALCH/MISC
    refs (e.g. harvestable ACTIs, FLOR via LPI).  These are injected directly into
    placed_bases for Mappalachia Position lookup.
    """
    c2p = tables["c2p"]
    parent_edid = tables["parent_edid"]
    lvli_refs = tables["lvli_refs"]

    # Seed from all item FormIDs
    seeds = {item["formid"].upper() for item in items_cfg}

    # Walk LVLI closure upward
    closure = _closure(seeds, c2p)

    placed_bases, direct_refrs = {}, {}

    # Holders of every list in the closure
    for lv in closure:
        for rf, redid, rsig in lvli_refs.get(lv, ()):
            if rsig == "REFR":
                direct_refrs.setdefault(rf, {"edid": redid})
            elif rsig in PLACED_SIGS:
                placed_bases.setdefault(rf, {
                    "sig": rsig, "edid": redid,
                    "source_type": classify(rsig, redid, parent_edid.get(lv, "")),
                    "via": parent_edid.get(lv, lv)})

    # Each item's own direct references (from ALCH/MISC export)
    for item in items_cfg:
        fid = item["formid"].upper()
        if item["sig"] == "ALCH":
            item_refs = tables["alch_refs"].get(fid, [])
        else:  # MISC
            item_refs = tables["misc_refs"].get(fid, [])

        for rf, redid, rsig in item_refs:
            if rsig == "REFR":
                direct_refrs.setdefault(rf, {"edid": redid})
            elif rsig in PLACED_SIGS:
                placed_bases.setdefault(rf, {
                    "sig": rsig, "edid": redid,
                    "source_type": classify(rsig, redid, ""), "via": "item"})

    # Inject extra world bases (harvestable ACTIs, LPI flora, etc.) that aren't
    # discoverable through the LVLI closure or item refs.
    if extra_world_bases:
        for eb in extra_world_bases:
            fid = eb["formid"].upper()
            placed_bases.setdefault(fid, {
                "sig": eb.get("sig", "ACTI"), "edid": eb.get("edid", ""),
                "source_type": eb.get("source_type", "loot-list"),
                "via": "extra_world_bases"})

    return {"lvli_closure": closure, "placed_bases": placed_bases, "direct_refrs": direct_refrs}
