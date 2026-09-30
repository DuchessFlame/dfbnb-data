#!/usr/bin/env python3
r"""
build_plant_combo_guides.py — one farming-guide page for several plant items that
grow from the SAME node, rendered by the Deathclaw Egg renderer
(df-bnb-farming-non-perishable-guide.js), the baseline for every farming page.

First page: Strangler Pod, Strangler Bloom & Swamp Plant (Sept 2026).
  The game places ONE leveled flora list, LPI_FloraSwampPod01, at every spot. A
  spot grows a Strangler Bloom when its REFR carries SFM04_Organic_ChosenPodsKeyword
  and a Strangler Pod when it doesn't (in nuke zones a Glow Pod can swap in).
  Harvesting a Strangler Bloom rolls LL_Flora_SwampPodFlower01, which can also hand
  out a Swamp Plant (LL_Flora_SwampPlant). So all three share one set of spawns.

  in : dist/plants/<plant>.json for each plant on the page   (build_spawns.py plants)
  out: dist/farming_spawns/plant-<page>_spawns.json          (default = live)
       dist/pts/farming_spawns/plant-<page>_spawns.json       (--pts)

Page shape (read by the renderer's multi-item path):
  regions / chance_spawns / map_base / full_map  — shared by every item, because
                                                  the spawns ARE shared
  sub_items[]  — one farming doc per item: name, used_for, farming_tips,
                 drop_rates (collectrons / resource generators / containers),
                 vendor_list, events_activities, treasure_maps. The renderer draws
                 every root expand with one sub-expand per item inside it.

Nothing is typed per item: the items come from each plant's FLOR `Produce`, walked
down through its harvest lists (so Swamp Plant is found through the Bloom's list),
and every section is filled by the same shared functions the other farming pages
use (build_farming_used_for, perk_ranks, rng76). Hand-authored photos and
directions already in the page are kept by `ref`.

  python src/build_plant_combo_guides.py            # live
  python src/build_plant_combo_guides.py --pts      # PTS
"""
import collections
import csv
import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import build_farming_used_for as B
import farming_spawns_sources as sources
import perk_ranks
import rng76
import tsv_source

csv.field_size_limit(10 ** 9)

ALL_REGIONS = ["Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog",
               "Forest", "Savage Divide", "Skyline Valley", "The Mire",
               "The Pitt", "Toxic Valley"]
PLANT_MAPS = "/wp-content/uploads/guide-images/farming-plants/"

# ── The combined pages ──────────────────────────────────────────────────────
# key = the page's URL slug (/bnb/farming/plants/<key>/<key>-guide/), which is
# also the plants doc whose spawns the page shows and its map folder.
#   plants  — the plant docs (dist/plants/<slug>.json) whose fixed spawns are
#             merged (deduped by ref). Their FLOR records seed the items, and a
#             UseLPI_ flora pulls in every flora its LPI_ node list can grow
#             (that is how the Strangler Bloom joins the Strangler Pod page, even
#             though the Bloom has no plants doc of its own any more)
#   skip    — produce EDIDs to leave off (the nuke-zone Glow Pod swap is its own
#             plant, not this page)
COMBOS = {
    "strangler-pod": {
        "title": "Strangler Pod, Strangler Bloom & Swamp Plant",
        "plants": ["strangler-pod"],
        "skip_edids": ["FloraSpecimenJarPurple"],
        "spot_label": "Strangler Pod or Strangler Bloom",
        "blurb": ("Strangler Pods and Strangler Blooms grow from the same spots in "
                  "Fallout 76, and picking a Strangler Bloom can also give you a "
                  "Swamp Plant. Each spot grows either a Pod or a Bloom."),
    },
}


def r2(x):
    return round(x, 2)


# ── Items: FLOR produce -> harvest lists -> ALCH ────────────────────────────
def _flor_rows(tsv_root):
    cands = [p for p in glob.glob(os.path.join(tsv_root, "FLOR_Export_*.tsv"))
             if not p.endswith("_Refs.tsv")]
    path = max(cands, key=tsv_source.export_key) if cands else None
    return {(r.get("FLOR_FormID") or "").strip().upper(): r
            for r in (B._read_tsv(path) if path else [])}


def _alch_rows(tsv_root):
    path = B._resolve_alch(tsv_root, effects=False)
    return {(r.get("ALCH_FormID") or "").strip().upper(): r
            for r in (B._read_tsv(path) if path else [])}


_REF = re.compile(r"([0-9A-Fa-f]{8}):([^:\t]*):([A-Za-z]{4})")


def _leaves(fid, sig, lvli, alch, out, depth=0):
    """Walk a produce reference down to the ALCH items it can hand out."""
    fid = fid.upper()
    if sig == "ALCH":
        if fid in alch and fid not in out:
            out.append(fid)
        return
    if sig != "LVLI" or depth > 6:
        return
    for e in lvli.entries_by_list.get(fid, []):
        m = _REF.match((e.get("LVLO_Reference") or "").strip())
        if m:
            _leaves(m.group(1), m.group(3).upper(), lvli, alch, out, depth + 1)


def _lpi_lists(lvli):
    """LVLI EDID (lower) -> FormID, read off the entries table."""
    out = {}
    for fid, rows in lvli.entries_by_list.items():
        for e in rows[:1]:
            ed = (e.get("LVLI_EDID") or "").strip().lower()
            if ed:
                out[ed] = fid
    return out


def page_flora(plant_docs, flor, lvli):
    """Every FLOR on the page, in order: the plants' own flora, then — for a
    UseLPI_<X> flora — the flora its LPI_<X> node list places (the game's own
    contract, spawn-guide 9m: exact name first, then a prefix match)."""
    out = []
    edid_to_list = None
    for doc in plant_docs:
        for fl in doc.get("flora") or []:
            fid = (fl.get("form_id") or "").upper()
            if fid and fid not in out:
                out.append(fid)
    for fid in list(out):
        ed = ((flor.get(fid) or {}).get("FLOR_EDID") or "")
        if not ed.lower().startswith("uselpi_"):
            continue
        if edid_to_list is None:
            edid_to_list = _lpi_lists(lvli)
        want = "lpi_" + ed[len("UseLPI_"):].lower()
        lid = edid_to_list.get(want) or next(
            (v for k, v in sorted(edid_to_list.items()) if k.startswith(want.rstrip("0123456789"))), None)
        for e in lvli.entries_by_list.get(lid or "", []):
            m = _REF.match((e.get("LVLO_Reference") or "").strip())
            if m and m.group(3).upper() == "FLOR" and m.group(1).upper() not in out:
                out.append(m.group(1).upper())
    return out


def page_items(combo, plant_docs, flor, alch, lvli):
    """[{form_id, name, edid, parent}] in the plants' own order.

    `parent` is set on a harvest-only item: one that is never the MAIN produce of
    any of the page's flora (the first item its produce list hands out) but turns
    up further down a harvest list — Swamp Plant, found inside the Strangler
    Bloom's list. It names the item whose harvest gives it."""
    order, main_of, came_from = [], set(), {}
    skip = {s.lower() for s in combo.get("skip_edids") or []}
    for fl_id in page_flora(plant_docs, flor, lvli):
        row = flor.get(fl_id)
        m = _REF.match(((row or {}).get("Produce") or "").strip())
        if not m:
            continue
        found = []
        _leaves(m.group(1), m.group(3).upper(), lvli, alch, found)
        found = [f for f in found if (alch[f].get("ALCH_EDID") or "").lower() not in skip]
        if not found:
            continue
        main_of.add(found[0])
        for fid in found:
            came_from.setdefault(fid, found[0])
            if fid not in order:
                order.append(fid)
    items = []
    for fid in order:
        r = alch[fid]
        parent = came_from.get(fid) if fid not in main_of else None
        items.append({"form_id": fid, "edid": r.get("ALCH_EDID") or "",
                      "name": (r.get("FULL") or "").strip(), "parent": parent,
                      "via": None})
    # The harvest list a harvest-only item comes out of, for its chance note.
    for it in items:
        if not it["parent"]:
            continue
        for fl_id in page_flora(plant_docs, flor, lvli):
            m = _REF.match(((flor.get(fl_id) or {}).get("Produce") or "").strip())
            if m and m.group(3).upper() == "LVLI":
                found = []
                _leaves(m.group(1), "LVLI", lvli, alch, found)
                if it["form_id"] in found:
                    it["via"] = m.group(1).upper()
                    break
    return items


# ── Farming tips (plant items: Green Thumb, weight perks) ───────────────────
def farming_tips(cons, keywords, spawn_note, channel):
    if not cons:
        return None
    w = cons.get("weight") or 0
    obj = cons.get("object_type") or "Food"
    spoils = "ObjectTypeNonPerishable" not in keywords
    weight_perk = "traveling_pharmacy" if obj == "Chem" else "thru_hiker"
    slugs = ["green_thumb", weight_perk] + (["good_with_salt"] if spoils else [])
    ft = collections.OrderedDict()
    if spawn_note:
        ft["spawn_note"] = spawn_note
    ft.update([
        ("spoils", spoils),
        ("spoil_duration_hours", None),
        ("base_weight", w),
        ("object_type", obj),
        ("yield_perk", "green_thumb"),
        ("weight_perk", weight_perk),
        ("weight_perk_ranks", [
            {"rank": 1, "reduction": "45%", "weight": r2(w * 0.55)},
            {"rank": 2, "reduction": "90%", "weight": r2(w * 0.10)},
        ]),
        ("backpack_mod", "chemists" if obj == "Chem" else "grocers"),
        ("backpack_weight", r2(w * 0.10)),
        ("armour_mod", "thru_hikers"),
        ("armour_mod_per_piece", 0.20),
        ("armour_mod_weights", [
            {"pieces": 1, "reduction": "20%", "weight": r2(w * 0.80)},
            {"pieces": 2, "reduction": "40%", "weight": r2(w * 0.60)},
            {"pieces": 3, "reduction": "60%", "weight": r2(w * 0.40)},
            {"pieces": 4, "reduction": "80%", "weight": r2(w * 0.20)},
            {"pieces": 5, "reduction": "90% cap", "weight": r2(w * 0.10)},
        ]),
        ("magazines_affect_yield", False),
        ("good_with_salt", spoils),
        ("perk_cards", perk_ranks.cards(slugs, channel)),
    ])
    if spoils:
        fr = perk_ranks.refrigerated_mod(channel)
        if fr:
            ft["refrigerated"] = fr
    return ft


# ── One item's sections ─────────────────────────────────────────────────────
def item_doc(item, ctx, spawn_note):
    fid, name = item["form_id"], item["name"]
    targets = {fid}
    cons = B.build_consumption(fid, ctx["tsv"], name)
    used_for = collections.OrderedDict([
        ("consumption", cons),
        ("modifiers", ctx["modifiers"]),
        ("challenges", B.build_challenges(fid, ctx["dist"])),
        ("recipes", B.build_recipes(name, ctx["recipe_guide"], ctx["bench_cat"],
                                    ctx["tsv"], ctx["guide_urls"])),
    ])
    src = sources.get_sources([{"formid": fid, "sig": "ALCH"}], ctx["tables"])
    closure = src["lvli_closure"]
    doc = collections.OrderedDict()
    doc["name"] = name
    doc["form_id"] = fid
    doc["used_for"] = used_for
    kw = (ctx["alch"].get(fid) or {}).get("Keywords_Flat") or ""
    tips = farming_tips(cons, kw, spawn_note, ctx["channel"])
    if tips:
        doc["farming_tips"] = tips
    doc["drop_rates"] = collections.OrderedDict([
        ("creatures", None), ("collectrons", None), ("resource_generators", None)])
    B._patch_camp_producers(doc, targets, ctx["dist"])
    B._patch_containers(doc, closure, targets, ctx["rates"], ctx["cont_names"], ctx["tables"])
    cont = doc["drop_rates"].get("containers")
    if isinstance(cont, dict) and isinstance(cont.get("types"), list):
        cont["types"] = [t for t in cont["types"]
                         if not re.match(r"(?i)^(test|qa|debug)\b", t.get("name") or "")]
    doc["vendor_list"] = B.build_vendor_list(
        [], {"items": [{"formid": fid}], "name": name},
        item_closure={f.upper() for f in closure},
        vendor_master=ctx["vendors"], rates=ctx["rates"])
    doc["events_activities"] = []
    B._patch_events_activities(doc, ctx["rates"], targets,
                               closure_lists=closure, tables=ctx["tables"])
    # One row per event + chance: an event with several reward lists at the same
    # rate (Project Paradise habitats) would otherwise repeat the same line.
    seen, evs = set(), []
    for e in doc["events_activities"]:
        if e.get("rate_value") is not None and e["rate_value"] <= 0:
            continue
        k = (e.get("name"), e.get("type"), e.get("rate_display"))
        if k not in seen:
            seen.add(k)
            evs.append(e)
    doc["events_activities"] = evs
    B._patch_treasure_maps(doc, ctx["rates"], targets, ctx["dist"])
    doc.setdefault("treasure_maps", {"maps": []})
    return doc


# ── Shared spawns ───────────────────────────────────────────────────────────
def _existing_slots(path):
    keep, markers = {}, {}
    if not os.path.exists(path):
        return keep, markers
    try:
        doc = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep, markers
    for reg in doc.get("regions", []):
        for loc in reg.get("locations", []):
            markers[(reg.get("region"), loc.get("marker"))] = {
                k: loc.get(k, "") for k in ("image_top", "directions", "image_bottom")}
            for sp in loc.get("spawns", []):
                if sp.get("ref"):
                    keep[sp["ref"]] = {k: sp.get(k, "") for k in
                                       ("image_top", "directions", "image_bottom")}
    return keep, markers


def _slot(a, b, key):
    return (a or {}).get(key) or (b or {}).get(key) or ""


def merge_regions(plant_docs, keep, keep_markers, label):
    """Fixed spawns of every plant on the page, merged per region + marker and
    deduped by ref (the plants share one node, so most refs repeat)."""
    by = collections.OrderedDict((r, collections.OrderedDict()) for r in ALL_REGIONS)
    for doc in plant_docs:
        for reg in (doc.get("fixed_spawns") or {}).get("regions", []):
            bucket = by.setdefault(reg["region"], collections.OrderedDict())
            for loc in reg.get("locations", []):
                cur = bucket.setdefault(loc.get("marker"), {
                    "refs": [], "spawns": {}, "coords": loc.get("coords"),
                    "compacted": False, "src": loc})
                for ref in loc.get("refs") or []:
                    if ref not in cur["refs"]:
                        cur["refs"].append(ref)
                for sp in loc.get("spawns") or []:
                    if sp.get("ref") and sp["ref"] not in cur["spawns"]:
                        cur["spawns"][sp["ref"]] = sp
                cur["compacted"] = cur["compacted"] or bool(loc.get("spawns_compacted"))
    out = []
    for region, markers in by.items():
        locs = []
        for marker in sorted(markers, key=lambda m: (m or "").lower()):
            cur = markers[marker]
            n = max(len(cur["refs"]), len(cur["spawns"]))
            km = keep_markers.get((region, marker))
            loc = collections.OrderedDict()
            loc["marker"] = marker
            loc["count"] = n
            loc["sources"] = {"static": n}
            for key in ("image_top", "directions", "image_bottom"):
                loc[key] = _slot(cur["src"], km, key)
            loc["refs"] = cur["refs"]
            loc["coords"] = cur["coords"]
            loc["breakdown"] = [{"label": label, "count": n, "source_type": "static"}]
            spawns = []
            if not cur["compacted"]:
                for i, (ref, sp) in enumerate(cur["spawns"].items(), 1):
                    k = keep.get(ref)
                    spawns.append(collections.OrderedDict([
                        ("label", f"{label} #{i}" if n > 1 else label),
                        ("ref", ref), ("source_type", "static"),
                        ("coords", sp.get("coords")),
                        ("image_top", _slot(sp, k, "image_top")),
                        ("directions", _slot(sp, k, "directions")),
                        ("image_bottom", _slot(sp, k, "image_bottom")),
                    ]))
            loc["spawns"] = spawns
            if cur["compacted"]:
                loc["spawns_compacted"] = True
            locs.append(loc)
        out.append({"region": region, "locations": locs})
    return out


def merge_chance(plant_docs):
    """Weighted flora points, names only, by region (spawn-guide 9k). Plant
    chance points carry no map refs, so there are no chance maps (no_maps)."""
    by = collections.OrderedDict()
    for doc in plant_docs:
        for loc in (doc.get("chance_spawns") or {}).get("locations", []):
            reg = loc.get("region") or "Unknown"
            names = by.setdefault(reg, collections.OrderedDict())
            m = loc.get("marker")
            names[m] = names.get(m, 0) + (loc.get("count") or 1)
    regions = [{"region": r, "placements": sum(m.values()),
                "markers": [{"name": k, "placements": v}
                            for k, v in sorted(m.items(), key=lambda x: (x[0] or "").lower())]}
               for r, m in sorted(by.items())]
    return {"regions": regions,
            "total_markers": sum(len(r["markers"]) for r in regions),
            "total": sum(r["placements"] for r in regions),
            "no_maps": True}


# ── One page ────────────────────────────────────────────────────────────────
def build_one(key, combo, ctx):
    plant_docs = []
    for p in combo["plants"]:
        path = os.path.join(ctx["plants_dir"], p + ".json")
        if not os.path.exists(path):
            path = os.path.join(ctx["repo_dist"], "plants", p + ".json")
        if os.path.exists(path):
            plant_docs.append(json.load(open(path, encoding="utf-8")))
    if not plant_docs:
        print(f"  [skip] {key}: no plant docs")
        return None
    items = page_items(combo, plant_docs, ctx["flor"], ctx["alch"], ctx["lvli"])
    if not items:
        print(f"  [skip] {key}: no produce resolved")
        return None

    slug = "plant-" + key
    out_path = os.path.join(ctx["out_dir"], f"{slug}_spawns.json")
    keep, keep_markers = _existing_slots(out_path)

    # A harvest-only item (Swamp Plant) has no spot of its own: say where it comes
    # from, with the chance read from the harvest list that hands it out.
    by_fid = {it["form_id"]: it for it in items}
    subs = []
    for it in items:
        note = None
        parent = by_fid.get(it.get("parent") or "")
        if parent:
            p = ctx["rates"].appearance([it["via"]], {it["form_id"]}) if it.get("via") else 0
            pct = B._fmt_rate(p) if p else ""
            note = (f"{it['name']} comes from picking a {parent['name']}"
                    + (f" \u2014 a {pct} chance each time." if pct else "."))
        subs.append(item_doc(it, ctx, note))

    doc = collections.OrderedDict()
    doc["_meta"] = {"generated": max(((d.get("_meta") or {}).get("generated") or "")
                                     for d in plant_docs),
                    "source": "dist/plants/{" + ",".join(combo["plants"]) + "}.json + "
                              "farming pipeline (src/build_plant_combo_guides.py)"}
    doc["set"] = "plants"
    doc["slug"] = slug
    doc["name"] = combo["title"]
    doc["page_title"] = combo["title"] + " Location Guide"
    doc["blurb"] = combo.get("blurb", "")
    doc["map_base"] = PLANT_MAPS + key + "/"
    doc["map_ext"] = ".jpg"
    doc["full_map"] = doc["map_base"] + key + ".jpg"
    doc["breakdown_lead"] = "Grows here:"
    doc["items"] = [{"name": it["name"], "form_id": it["form_id"]} for it in items]
    doc["sub_items"] = subs
    doc["regions"] = merge_regions(plant_docs, keep, keep_markers, combo["spot_label"])
    doc["chance_spawns"] = merge_chance(plant_docs)
    n = doc["chance_spawns"]["total_markers"]
    if n:
        doc["chance_spawns"]["lead"] = (
            f"These plants can also grow at {n} location{'' if n == 1 else 's'} where "
            f"the spot is shared with other plants, so they are not guaranteed there. "
            f"They are listed by name only.")

    os.makedirs(ctx["out_dir"], exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=1)
    n_fixed = sum(l["count"] for r in doc["regions"] for l in r["locations"])
    print(f"  {slug:<24} fixed:{n_fixed:>4}  chance:{n:>3}  items: "
          + ", ".join(f"{s['name']} (v{len(s['vendor_list'])} e{len(s['events_activities'])}"
                      f" m{len(s['treasure_maps']['maps'])})" for s in subs))
    return out_path


def load_ctx(pts):
    channel = "pts" if pts else "live"
    dist = os.path.join(REPO, "dist", "pts") if pts else os.path.join(REPO, "dist")
    tsv = os.path.join(REPO, "tsv", "pts") if pts else os.path.join(REPO, "tsv")
    data = rng76.Rng76Data.from_tsv_root(tsv)

    def dist_file(name):
        p = os.path.join(dist, name)
        return p if os.path.exists(p) else os.path.join(REPO, "dist", name)

    return {
        "channel": channel,
        "dist": dist,
        "repo_dist": os.path.join(REPO, "dist"),
        "tsv": tsv,
        "plants_dir": os.path.join(dist, "plants"),
        "out_dir": os.path.join(dist, "farming_spawns"),
        "recipe_guide": json.load(open(dist_file("recipe_guide.json"), encoding="utf-8")),
        "bench_cat": B._bench_category_map(dist_file("cobj-recipes.json")),
        "guide_urls": B._load_guide_urls(),
        "modifiers": B.build_modifiers(tsv),
        "tables": sources.load_tables(tsv),
        "rates": B.VendorRates(data),
        "lvli": data.lvli,
        "cont_names": B._load_cont_names(tsv),
        "vendors": B._load_vendor_master(dist),
        "flor": _flor_rows(tsv),
        "alch": _alch_rows(tsv),
    }


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    pts = "--pts" in argv
    want = [a.lower() for a in argv if not a.startswith("-")]
    ctx = load_ctx(pts)
    keys = [k for k in COMBOS if not want or k in want]
    print(f"[plant-combo-guides] {len(keys)} page(s) — {ctx['channel']}")
    for k in keys:
        build_one(k, COMBOS[k], ctx)


if __name__ == "__main__":
    main()
