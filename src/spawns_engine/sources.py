#!/usr/bin/env python3
r"""
spawns_engine.sources — resolve, per item set, the FULL set of world sources that
can yield it, straight from the committed game-file exports in tsv/.

Shared by every "{Item} Spawn Locations" page. Handles ALCH items (drinks, Cream,
Deathclaw Egg) and MISC items (Cracked Deathclaw Egg). No xEdit run and no FormID
hunting needed: everything is already exported.

  * LVLI_Export_*_LVLI_Entries.tsv  — parent list -> child. Inverted into
                                       child -> parents, then walked UP so we
                                       collect EVERY list that can ever roll the item.
  * LVLI_Export_*_LVLI_Refs.tsv     — each list's ReferencedBy: the CONT/ACTI/FURN/
                                       NPC_/MSTT/FLOR/REFR that place or hold it.
  * ALCH_Export_*.tsv / MISC_Export_*.tsv — the item's own ReferencedBy.

For each item set get_sources() yields:
  * lvli_closure  — every LVLI FormID that can roll the item
  * placed_bases  — {base_formid: {sig, edid, source_type, via}} for CONT/ACTI/FURN/
                    NPC_/MSTT/FLOR that hold any list in the closure
  * direct_refrs  — {refr_formid: {edid, via, dedicated}} the item/list is placed
                    directly. `dedicated` is the FIXED-SPAWN test (see
                    dedicated_lists): True when the placed list can only ever hand
                    out this item, False when it is a shared loot pool. Only the
                    dedicated ones are Fixed Spawn Locations; the rest are chance
                    points and render under Chance to Spawn Locations.

classify() is passed IN (see spawns_engine.classify) so the routing vocabulary is
per-family while this walker stays generic. This module only READS tsv/ — it needs
no Mappalachia DB.
"""

import os, re, csv, glob
import tsv_source          # one resolver for every export selection

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))   # .../spawns_engine -> src -> repo
TSV = os.path.join(REPO, "tsv")

# Signatures we treat as a real world holder/placement of a list. FLOR is only
# relevant to harvestable-flora items (the egg/farming sets); drinks never place a
# FLOR base, so the default set is passed per family to preserve exact behaviour.
PLACED_SIGS_DEFAULT = {"CONT", "ACTI", "FURN", "NPC_", "MSTT", "REFR"}
PLACED_SIGS_FLORA   = {"CONT", "ACTI", "FURN", "NPC_", "MSTT", "REFR", "FLOR"}


# ── file helpers ─────────────────────────────────────────────────────────────
def _newest(patterns, tsv_root=None):
    root = tsv_root or TSV
    for pat in patterns:
        hit = tsv_source.newest(os.path.join(root, pat), required=False)
        if hit:
            return hit
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


# ── TSV loaders ──────────────────────────────────────────────────────────────
def _load_child_to_parents(entries_path):
    """child FormID -> set(parent LVLI FormID), from LVLI_Entries (col LVLO_Reference).

    Also returns the DOWN edges (parent -> [(child, sig)]) in the same pass, because
    the dedication test (see `dedicated_lists`) has to walk the tree downwards."""
    c2p = {}
    p2c = {}
    parent_edid = {}
    with open(entries_path, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            parent = (r.get("LVLI_FormID") or "").strip().upper()
            child, _, csig = _fid(r.get("LVLO_Reference") or "")
            if parent and child:
                c2p.setdefault(child, set()).add(parent)
                p2c.setdefault(parent, []).append((child, csig))
                parent_edid[parent] = (r.get("LVLI_EDID") or "").strip()
    return c2p, p2c, parent_edid


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


# ── LVLI closure walk ────────────────────────────────────────────────────────
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


def _leaves(root, p2c, cache):
    """Every non-list leaf FormID a list can ultimately hand out (down-closure)."""
    if root in cache:
        return cache[root]
    out, seen, stack = set(), set(), [root]
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        kids = p2c.get(n)
        if not kids:                      # not a list (or an empty one) -> a leaf item
            out.add(n)
            continue
        for child, csig in kids:
            if csig == "LVLI" and child in p2c:
                stack.append(child)
            else:
                out.add(child)
    cache[root] = out
    return out


def dedicated_lists(seeds, closure, p2c):
    """The subset of `closure` that can ONLY ever produce this item set.

    THE FIXED-SPAWN RULE. A world REFR that places a leveled list is a *fixed spawn*
    for this item only when that list is DEDICATED to it — i.e. every leaf the list
    can hand out is one of the page's own items (`LPI_Chems_Addictol` -> Addictol,
    `LPI_Food_DeathclawEgg` -> Deathclaw Egg). If it spawns, it is this item.

    A SHARED loot pool (`LPI_Chems_Prewar` rolls nine different chems, `LPI_Chems_
    Prewar` -> `LL_Chems_Prewar_Same`) is NOT a fixed spawn: the point exists, but
    what appears there is a gamble. Those become source_type "chance" and render in
    the Chance to Spawn Locations expand instead.

    A list's own ChanceNone is deliberately NOT part of the test — the Deathclaw Egg
    world spawn is 50% and is still a fixed Deathclaw Egg spawn, because nothing else
    can ever be standing there. Dedication is about WHAT spawns, not HOW OFTEN.
    """
    seeds = {str(s).upper() for s in seeds}
    cache = {}
    return {lv for lv in closure if _leaves(lv, p2c, cache) <= seeds}


# ── public API ───────────────────────────────────────────────────────────────
def load_tables(tsv_root=None):
    """Load all TSV tables needed for source resolution. Requires LVLI_Entries +
    LVLI_Refs; ALCH / MISC are optional (empty dict if absent)."""
    entries = _newest(["LVLI_Export_*_LVLI_Entries.tsv"], tsv_root)
    refs    = _newest(["LVLI_Export_*_LVLI_Refs.tsv"], tsv_root)
    alch    = _newest(["ALCH_Export_*.tsv"], tsv_root)
    misc    = _newest(["MISC_Export_*.tsv"], tsv_root)
    missing = [n for n, p in [("LVLI_Entries", entries), ("LVLI_Refs", refs)] if not p]
    if missing:
        raise FileNotFoundError(f"missing tsv export(s): {missing} in {tsv_root or TSV}")
    c2p, p2c, parent_edid = _load_child_to_parents(entries)
    return {
        "entries": entries, "refs": refs, "alch": alch, "misc": misc,
        "c2p": c2p, "p2c": p2c, "parent_edid": parent_edid,
        "lvli_refs": _load_lvli_refs(refs),
        "alch_refs": _load_alch_refs(alch) if alch else {},
        "misc_refs": _load_misc_refs(misc) if misc else {},
    }


def get_sources(item_records, tables, classify, extra_closure_seeds=None,
                extra_world_bases=None, placed_sigs=PLACED_SIGS_DEFAULT,
                place_item_bases=False, dedication_seeds=None):
    """Return {'lvli_closure', 'placed_bases', 'direct_refrs'} for a set of items.

    item_records : list of {"formid": hex, "sig": "ALCH"|"MISC", ...}. Each formid
                   seeds the LVLI up-closure AND gets its own ALCH/MISC direct-ref
                   pass.
    classify     : the per-family source_type router (spawns_engine.classify).
    extra_closure_seeds : optional bare hex FormIDs added to the closure seed set
                   ONLY (no direct-ref pass) — used by drinks for forced leveled items.
    extra_world_bases   : optional [{formid, edid, sig, source_type}] injected
                   directly into placed_bases (harvestable ACTIs, LPI flora, etc.).
    placed_sigs  : record signatures treated as a world holder/placement.
    place_item_bases : when True, add each item's OWN FormID to placed_bases so the
                   build resolves Position rows by the item base (like Mappalachia,
                   which plots by base FormID). This catches items placed straight
                   into the world as their own base — e.g. the single raw Deathclaw
                   Egg in Vault 63 — that the xEdit "ReferencedBy" ref columns miss.
                   Each item's source_type comes from its record's `world_source_type`
                   (default "static", i.e. a guaranteed 100% direct spawn). Requires a
                   local geo-cache reseed with the Mappalachia DB to take effect.
    dedication_seeds : optional wider FormID set for the FIXED-SPAWN dedication test
                   only (never for the closure). Use it on a whole-category page whose
                   world list also carries category siblings the page's item list
                   doesn't enumerate — see the magazines note inline below.
    """
    c2p = tables["c2p"]
    parent_edid = tables["parent_edid"]
    lvli_refs = tables["lvli_refs"]

    seeds = {rec["formid"].upper() for rec in item_records}
    for fid in (extra_closure_seeds or []):
        seeds.add(str(fid).upper())

    closure = _closure(seeds, c2p)          # every list that can roll the item

    # Which of those lists are DEDICATED to this item (see dedicated_lists). A REFR
    # placing a dedicated list is a fixed spawn; a REFR placing a shared loot pool is
    # a chance point. Without this every closure REFR read as a guaranteed spawn —
    # Addictol shipped 332 "fixed" spawns when only 25 were its own.
    # `dedication_seeds` widens ONLY the dedication test, never the closure — for a
    # whole-CATEGORY page. The magazine guide covers 96 numbered issues, but
    # LPI_Loot_Magazines also hands out the four holotape magazines, so measured
    # against the page's own items the list looks "shared" and all 717 magazine
    # spawn points would be demoted to chance. They are magazines on a magazines
    # page — the widened set says so, keyword-driven, no FormIDs typed.
    ded = dedicated_lists(dedication_seeds or seeds, closure, tables.get("p2c") or {})

    placed_bases, direct_refrs = {}, {}

    def _mark_direct(rf, redid, dedicated, via=""):
        cur = direct_refrs.setdefault(rf, {"edid": redid, "dedicated": False, "via": via})
        if dedicated:                       # one dedicated placer is enough
            cur["dedicated"] = True
            cur["via"] = via or cur.get("via", "")
        if redid and not cur.get("edid"):
            cur["edid"] = redid

    # holders of every list in the closure
    for lv in closure:
        for rf, redid, rsig in lvli_refs.get(lv, ()):
            if rsig == "REFR":
                _mark_direct(rf, redid, lv in ded, parent_edid.get(lv, lv))
            elif rsig in placed_sigs:
                placed_bases.setdefault(rf, {
                    "sig": rsig, "edid": redid,
                    "source_type": classify(rsig, redid, parent_edid.get(lv, "")),
                    "via": parent_edid.get(lv, lv)})

    # each item's own direct references (from the ALCH/MISC export)
    for rec in item_records:
        fid = rec["formid"].upper()
        if rec.get("sig") == "MISC":
            item_refs = tables["misc_refs"].get(fid, [])
        else:
            item_refs = tables["alch_refs"].get(fid, [])
        for rf, redid, rsig in item_refs:
            if rsig == "REFR":
                # the item's OWN base placed in the world — guaranteed by definition
                _mark_direct(rf, redid, True, "item")
            elif rsig in placed_sigs:
                placed_bases.setdefault(rf, {
                    "sig": rsig, "edid": redid,
                    "source_type": classify(rsig, redid, ""), "via": "item"})

    # pull-by-base: resolve Position rows keyed on each item's OWN FormID, so a
    # directly world-placed item base (not listed in any ref column) is still found.
    # setdefault means a closure/item-ref hit keeps its richer source_type.
    if place_item_bases:
        for rec in item_records:
            fid = rec["formid"].upper()
            placed_bases.setdefault(fid, {
                "sig": rec.get("sig", "ALCH"), "edid": rec.get("edid", ""),
                "source_type": rec.get("world_source_type", "static"),
                "via": "item-base"})

    # inject extra world bases (harvestable ACTIs, LPI flora, …) not discoverable
    # through the LVLI closure or item refs.
    #
    # This is hand-curated, so it OVERRIDES anything the automatic passes above
    # produced for the same base — a straight assignment, never setdefault.
    # Reason: a harvestable ACTI reached through the item's own ReferencedBy column
    # has no EDID keyword classify() recognises, so it falls through to "loot-list"
    # and is then stripped from Fixed Spawn Locations by chem_loot_collapse. That is
    # what deleted all 68 MirelurkEgg_Harvestable points while the (unharvestable)
    # hatching ACTI, injected here, survived. If a base is listed here, the config
    # author has already decided what it is — do not let the fallback win.
    if extra_world_bases:
        for eb in extra_world_bases:
            fid = eb["formid"].upper()
            placed_bases[fid] = {
                "sig": eb.get("sig", "ACTI"), "edid": eb.get("edid", ""),
                "source_type": eb.get("source_type", "loot-list"),
                "via": "extra_world_bases"}

    return {"lvli_closure": closure, "placed_bases": placed_bases, "direct_refrs": direct_refrs}
