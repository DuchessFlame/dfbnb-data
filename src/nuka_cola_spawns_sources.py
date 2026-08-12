#!/usr/bin/env python3
r"""
nuka_cola_spawns_sources.py — resolve, per Nuka-Cola flavour, the FULL set of world
sources that can yield it, straight from the committed game-file exports in tsv/.

No xEdit run and no FormID hunting needed: everything is already exported.
  * LVLI_Export_*_LVLI_Entries.tsv  — parent list -> child (item/sub-list). Inverted here
                                       into child -> parents, then walked UP so we collect
                                       EVERY leveled list that can ever roll the drink
                                       (vendor pools, mystery machines, collectrons, assorted
                                       nuka lists, quest-reward lists, …).
  * LVLI_Export_*_LVLI_Refs.tsv     — each list's ReferencedBy: the CONT / ACTI / FURN / NPC_
                                       / REFR that actually place or hold it in the world.
  * ALCH_Export_*.tsv               — the drink's own ReferencedBy: direct world REFRs +
                                       the lists that contain the raw ALCH.

For each flavour this yields:
  * lvli_closure  — every LVLI FormID that can roll the drink
  * placed_bases  — {base_formid: {sig, edid, source_type, via}} for CONT/ACTI/FURN/NPC_/MSTT
                    that hold any list in the closure (query Mappalachia Position by these to
                    get coords)
  * direct_refrs  — {refr_formid: {edid}} the drink/list is placed as directly (already a
                    placement; look up its coords by instanceFormID)

source_type is a friendly bucket for the page: vending-machine · collectron · dispenser ·
container · npc · direct · quest-reward · loot-list.

This module only READS tsv/ — it needs no Mappalachia DB. build_nuka_cola_spawns_json.py
imports get_sources() and feeds the FormIDs to the Position lookup.
"""

import os, re, csv, glob, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV = os.path.join(REPO, "tsv")

# Base drink ALCH FormIDs (8-hex, load order) — from ALCH_Export. Seeds for the up-walk,
# alongside each flavour's leveled items in nuka_cola_spawns_config.
DRINK_ALCH = {
    "nuka-cola":            ["0004835D"],
    "nuka-cola-cherry":     ["00048360"],
    "nuka-cola-cranberry":  ["00598DCD"],
    "nuka-cola-dark":       ["00113294"],
    "nuka-cola-grape":      ["00113292"],
    "nuka-cola-orange":     ["00113298"],
    "nuka-cola-quantum":    ["0004835F"],
    "nuka-cola-wild":       ["0011329B"],
    "nuka-cola-twist":      ["00660864"],
    "nukashine":            ["0047BC14", "0047BC08"],           # fresh + vintage
    "nuka-cola-vaccinated": [],                                  # quest activator, not placed
    "sunset-sarsaparilla":  ["00832CA7", "00837E07", "00837E08"],
}

# Signatures we treat as a real world holder/placement of a list (query these in Position).
PLACED_SIGS = {"CONT", "ACTI", "FURN", "NPC_", "MSTT", "REFR"}


def _newest(patterns):
    for pat in patterns:
        hits = sorted(glob.glob(os.path.join(TSV, pat)), key=os.path.getmtime, reverse=True)
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
    """drink ALCH FormID -> list of (ref_formid, ref_edid, ref_sig), from Ref_1..N."""
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


def classify(sig, edid, via_edid):
    """Bucket a placed source into a renderer expand. EDID-driven, no hardcoded FormIDs.

    Buckets -> expand (routed in df-bnb-nuka-cola-spawns.js):
      direct / machine      -> Fixed Spawn Locations (guaranteed world source)
      container / loot-list  -> Containers            (chance loot)
      npc                    -> Creatures             (chance death drop)
      vendor                 -> Vendors               (merchant stock; incl. dedicated)
      collectron             -> Collectrons           (ATX/Season, no map coords)
      resource-generator     -> Resource Generators   (ATX dispenser, no map coords)
      quest-reward           -> excluded from the map (guaranteed but not a location)

    Order matters: check the most specific EDID keyword first. Note a vendor's stock
    chest can also read as a "vending machine" (e.g. NWOT_VendingMachine_VendorChest_
    NukaCola) — VendorChest wins so dedicated vendors are never mislabelled as machines.
    """
    e = (edid or "").lower() + " " + (via_edid or "").lower()
    # Collectron stations — a CAMP bot that gathers the drink.
    if "collectron" in e:
        return "collectron"
    # Merchant vendor stock chests — dedicated + faction merchants (VendorChest wins
    # over the VendingMachine token so NWOT's dedicated Nuka vendor lands in Vendors).
    if "vendorchest" in e:
        return "vendor"
    # ATX in-place dispensers that PRODUCE the drink — Mystery Machines / ATX vending.
    if "mysterymachine" in e:
        return "resource-generator"
    # Lootable world dispensing machines (ice / soda machines) — a fixed world object.
    if "icemachine" in e or "sodamachine" in e:
        return "machine"
    # Other ATX vending machines / dispensers -> Resource Generators.
    if "vendingmachine" in e or "dispenser" in e:
        return "resource-generator"
    # Generic vendor stock not named *VendorChest* (e.g. BS02_SpecialVendor_Brahmin).
    if "vendor" in e:
        return "vendor"
    if "questreward" in e or "quest_reward" in e or "_reward" in e:
        return "quest-reward"
    if sig == "NPC_":
        return "npc"
    if sig == "REFR":
        return "direct"
    if sig == "CONT":
        return "container"
    return "loot-list"


def _closure(seeds, c2p):
    """All LVLI FormIDs reachable UP from the seeds (lists that can roll the drink)."""
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


def get_sources(flavour_slug, extra_seed_formids=None, tables=None):
    """Return {'lvli_closure', 'placed_bases', 'direct_refrs'} for one flavour."""
    t = tables or load_tables()
    c2p, parent_edid, lvli_refs, alch_refs = (
        t["c2p"], t["parent_edid"], t["lvli_refs"], t["alch_refs"])

    seeds = set(DRINK_ALCH.get(flavour_slug, []))
    for fid in (extra_seed_formids or []):
        seeds.add(str(fid).upper())

    closure = _closure(seeds, c2p)          # every list that can roll the drink
    lvli_closure = {f for f in closure if f in lvli_refs or f in parent_edid or f in c2p.values()}

    placed_bases, direct_refrs = {}, {}

    # holders of every list in the closure
    for lv in closure:
        for rf, redid, rsig in lvli_refs.get(lv, ()):
            if rsig == "REFR":
                direct_refrs.setdefault(rf, {"edid": redid})
            elif rsig in PLACED_SIGS:
                placed_bases.setdefault(rf, {
                    "sig": rsig, "edid": redid,
                    "source_type": classify(rsig, redid, parent_edid.get(lv, "")),
                    "via": parent_edid.get(lv, lv)})

    # the drink's own direct references (REFRs placed in the world; lists already in closure)
    for fid in DRINK_ALCH.get(flavour_slug, []):
        for rf, redid, rsig in alch_refs.get(fid, ()):
            if rsig == "REFR":
                direct_refrs.setdefault(rf, {"edid": redid})
            elif rsig in PLACED_SIGS:
                placed_bases.setdefault(rf, {
                    "sig": rsig, "edid": redid,
                    "source_type": classify(rsig, redid, ""), "via": "drink"})

    return {"lvli_closure": closure, "placed_bases": placed_bases, "direct_refrs": direct_refrs}


def load_tables():
    entries = _newest(["LVLI_Export_*_LVLI_Entries.tsv"])
    refs    = _newest(["LVLI_Export_*_LVLI_Refs.tsv"])
    alch    = _newest(["ALCH_Export_*.tsv"])
    missing = [n for n, p in (("LVLI_Entries", entries), ("LVLI_Refs", refs), ("ALCH_Export", alch)) if not p]
    if missing:
        raise FileNotFoundError(f"missing tsv export(s): {missing} in {TSV}")
    c2p, parent_edid = _load_child_to_parents(entries)
    return {"entries": entries, "refs": refs, "alch": alch,
            "c2p": c2p, "parent_edid": parent_edid,
            "lvli_refs": _load_lvli_refs(refs), "alch_refs": _load_alch_refs(alch)}


def main(argv):
    t = load_tables()
    print(f"[nuka-sources] entries={os.path.basename(t['entries'])}  "
          f"refs={os.path.basename(t['refs'])}  alch={os.path.basename(t['alch'])}")
    from collections import Counter
    flavours = argv[1:] or list(DRINK_ALCH)
    for fl in flavours:
        s = get_sources(fl, tables=t)
        by_type = Counter(v["source_type"] for v in s["placed_bases"].values())
        print(f"\n{fl}")
        print(f"  lists that can roll it : {len(s['lvli_closure'])}")
        print(f"  placed holder bases    : {len(s['placed_bases'])}  {dict(by_type)}")
        print(f"  direct drink REFRs     : {len(s['direct_refrs'])}")


if __name__ == "__main__":
    main(sys.argv)
