#!/usr/bin/env python3
r"""
build_meat_guide_docs.py — turn each Farming - Meat doc into a farming-guide doc,
so the meat guide pages render through the SAME renderer as the Deathclaw Egg
guide (df-bnb-farming-non-perishable-guide.js), which is the baseline for every
farming page (Duchess, Sept 2026).

  in : dist/meat/<slug>.json                      (built by build_farming_meat_json.py)
  out: dist/farming_spawns/meat-<slug>_spawns.json (+ the dist/pts/ copy)

What the meat pages were missing next to the Deathclaw Egg page, and where each
part now comes from — nothing is typed, everything is read from the game data
through the same shared functions the farming pages use:

  Used For            build_farming_used_for.build_consumption / build_modifiers /
                      build_recipes / build_challenges (+ the creature's own
                      challenges from the meat doc)
  Farming Tips        weights from the ALCH record; perk cards from perk_ranks.py
                      (Butcher's Bounty, Good With Salt, Thru-Hiker); Refrigerated
                      backpack % from its ENCH record
  Collectrons /       build_farming_used_for._patch_camp_producers
  Resource Generators
  Containers          build_farming_used_for.container_types (rng76)
  Creatures           the meat doc's death-drop lists, cleaned: empty pools are
                      dropped, and the extra-meat pool is named for the perk that
                      turns it on (Butcher's Bounty)
  Events & Activities the creature's own events + the meat's reward pools (rng76)
  Treasure Maps       treasure_map_sources (rng76)
  Vendors             build_farming_used_for.build_vendor_list (NPC2 vendor master)
  Fixed Spawn         the meat doc's fixed spawns, all ten regions, one photo slot
                      per spawn, labelled with the creature variant
  Chance to Spawn     the meat doc's weighted spawn points, names only
  map_base            /wp-content/uploads/guide-images/farming-meat/<slug>/

Hand-authored photos and directions already on a spawn are kept by `ref`.

  python src/build_meat_guide_docs.py                 # every meat page
  python src/build_meat_guide_docs.py radstag wolf    # named pages only
"""
import collections
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

DIST = os.path.join(REPO, "dist")
TSV = os.path.join(REPO, "tsv")
OUT_DIR = os.path.join(DIST, "farming_spawns")
PTS_OUT_DIR = os.path.join(DIST, "pts", "farming_spawns")
MAP_ROOT = "/wp-content/uploads/guide-images/farming-meat/"

ALL_REGIONS = ["Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog",
               "Forest", "Savage Divide", "Skyline Valley", "The Mire",
               "The Pitt", "Toxic Valley"]

# The GLOB that gates the extra-meat roll on a creature's death list. It sits at
# 100 ChanceNone, so without the perk the roll never pays out; Butcher's Bounty is
# the card that opens it. Read from the list entry, never assumed per creature.
EXTRA_MEAT_GLOB = "ExtraMeatChance"


def r2(x):
    return round(x, 2)


# ── Farming tips ────────────────────────────────────────────────────────────
def _alch_keywords(formid):
    path = B._resolve_alch(TSV, effects=False)
    if not path:
        return ""
    fid = formid.upper()
    for r in B._read_tsv(path):
        if (r.get("ALCH_FormID") or "").strip().upper() == fid:
            return r.get("Keywords_Flat") or ""
    return ""


def farming_tips(cons, formid, creature):
    if not cons:
        return None
    w = cons.get("weight") or 0
    obj = cons.get("object_type") or "Food"
    spoils = "ObjectTypeNonPerishable" not in _alch_keywords(formid)
    slugs = ["butchers_bounty", "traveling_pharmacy" if obj == "Chem" else "thru_hiker"]
    if spoils:
        slugs.append("good_with_salt")
    ft = collections.OrderedDict([
        # Creature meat is not a pick-up spawn, so the page's opening line says
        # how the meat is actually got rather than the loose-item wording.
        ("spawn_note", f"{cons.get('_name')} comes from killing {creature}s and "
                       f"looting the body."),
        ("spoils", spoils),
        ("spoil_duration_hours", None),
        ("base_weight", w),
        ("object_type", obj),
        ("yield_perk", "butchers_bounty"),
        ("weight_perk", "traveling_pharmacy" if obj == "Chem" else "thru_hiker"),
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
        ("perk_cards", perk_ranks.cards(slugs)),
    ])
    if spoils:
        fr = perk_ranks.refrigerated_mod()
        if fr:
            ft["refrigerated"] = fr
    return ft


# ── Creatures (death drops) ─────────────────────────────────────────────────
def _is_extra_meat_pool(lvli, list_id):
    for e in lvli.entries_by_list.get((list_id or "").upper(), []):
        if EXTRA_MEAT_GLOB.lower() in (e.get("LVOG_ChanceNoneGlobal") or "").lower():
            return True
    return False


def clean_drops(drops, lvli, bb_card):
    """The meat doc's death lists, made readable:
      - a pool with no items at all (an empty leveled list — LLE_Creature_Small
        points at LLE_Creature_Small_Ammo, which has no entries) is dropped
      - the extra-meat pool is renamed and its 0% explained: it only pays out
        with Butcher's Bounty, at the perk's own per-rank chance."""
    out = {"lists": []}
    pcts = [r.get("percent") for r in (bb_card or {}).get("ranks", []) if r.get("percent")]
    for lst in (drops or {}).get("lists", []):
        rows = []
        for r in lst.get("rows", []):
            if r.get("kind") == "pool":
                if not r.get("item_count"):
                    continue
                if lvli is not None and _is_extra_meat_pool(lvli, r.get("sub_id")):
                    r = dict(r)
                    r["name"] = "Extra meat (Butcher's Bounty)"
                    need = ("Butcher's Bounty " + " / ".join(pcts)) if pcts else "Butcher's Bounty"
                    r["top_items"] = [{"name": t.get("name"), "rate_display": "needs " + need}
                                      for t in r.get("top_items", [])]
            rows.append(r)
        if rows:
            l2 = dict(lst)
            l2["rows"] = rows
            out["lists"].append(l2)
    return out


def _load_quest_names():
    """Quest EDID -> display name, from the QUEST export ("Event: X" -> "X")."""
    import csv
    import tsv_source
    csv.field_size_limit(10 ** 9)
    out = {}
    # Newest export by its name's date, through tsv_source (never by mtime).
    path = tsv_source.newest("QUEST_Export_*.tsv", required=False)
    if not path:
        return out
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        r = csv.reader(f, delimiter="\t")
        h = next(r)
        ie, iname = h.index("EDID"), h.index("FULL - Name")
        for row in r:
            if len(row) > max(ie, iname):
                name = re.sub(r"^Event:\s*", "", row[iname].strip())
                if name and not name.startswith("["):
                    out[row[ie].strip()] = name
    return out


def flat_drops(drops, quests):
    """One flat list of what the creature drops — item + % — with no pools and
    no per-list headings. A drop from an event-only death list (its EDID starts
    with the event quest's EDID, e.g. E07A_Mothman_LLD_...) carries a note naming
    the event, read from the QUEST export."""
    out, seen = [], {}
    for lst in (drops or {}).get("lists", []):
        edid = lst.get("edid") or ""
        note = ""
        m = re.match(r"^(.*?)_LLD_", edid)
        if m:
            parts = m.group(1).split("_")
            for i in range(len(parts), 0, -1):
                q = quests.get("_".join(parts[:i]))
                if q:
                    note = f"only drops during {q}"
                    break
        for r in lst.get("rows", []):
            if r.get("kind") != "item":
                continue
            key = (r.get("name"), note)
            if key in seen:
                continue
            seen[key] = True
            out.append({"name": r.get("name"), "qty": r.get("qty") or 1,
                        "rate_display": r.get("rate_display", ""), "note": note})
    return out


# ── Fixed spawns ────────────────────────────────────────────────────────────
def _existing_slots(path):
    """{ref: {image_top, directions, image_bottom}} from the last build, so
    photos and directions typed into the farming doc survive a rebuild."""
    keep = {}
    if not os.path.exists(path):
        return keep, {}
    try:
        doc = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep, {}
    markers = {}
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


def convert_regions(meat_doc, keep, keep_markers, creature):
    by_region = {r["region"]: r for r in (meat_doc.get("fixed_spawns") or {}).get("regions", [])}
    out = []
    for name in ALL_REGIONS:
        reg = by_region.get(name) or {"region": name, "locations": []}
        locs = []
        for loc in reg.get("locations", []):
            spawns = loc.get("spawns") or []
            variants = collections.Counter(
                (sp.get("variant") or creature) for sp in spawns)
            seen = collections.Counter()
            new_spawns = []
            for sp in spawns:
                v = sp.get("variant") or creature
                seen[v] += 1
                label = f"{v} #{seen[v]}" if variants[v] > 1 else v
                k = keep.get(sp.get("ref"))
                new_spawns.append(collections.OrderedDict([
                    ("label", label), ("ref", sp.get("ref")),
                    ("source_type", sp.get("source_type") or "placement"),
                    ("coords", sp.get("coords")),
                    ("image_top", _slot(sp, k, "image_top")),
                    ("directions", _slot(sp, k, "directions")),
                    ("image_bottom", _slot(sp, k, "image_bottom")),
                ]))
            km = keep_markers.get((name, loc.get("marker")))
            new = collections.OrderedDict()
            new["marker"] = loc.get("marker")
            new["count"] = loc.get("count") or len(spawns)
            new["sources"] = loc.get("sources") or {"placement": new["count"]}
            for key in ("image_top", "directions", "image_bottom"):
                new[key] = _slot(loc, km, key)
            new["refs"] = loc.get("refs") or [s.get("ref") for s in spawns if s.get("ref")]
            new["coords"] = loc.get("coords")
            new["breakdown"] = [{"label": v, "count": n, "source_type": "placement"}
                                for v, n in variants.items()] or \
                               [{"label": creature, "count": new["count"],
                                 "source_type": "placement"}]
            new["spawns"] = new_spawns
            if loc.get("spawns_compacted"):
                new["spawns_compacted"] = True
            locs.append(new)
        out.append({"region": name, "locations": locs})
    return out


def convert_chance(meat_doc):
    """Weighted spawn points -> names only, grouped by region (spawn-guide 9k).
    Meat chance points carry no map refs, so there are no chance map tiles and
    the page must not link to one (no_maps)."""
    by = collections.OrderedDict()
    for loc in (meat_doc.get("chance_spawns") or {}).get("locations", []):
        reg = loc.get("region") or "Unknown"
        by.setdefault(reg, []).append({"name": loc.get("marker"),
                                       "placements": loc.get("count") or 1})
    regions = [{"region": r, "markers": sorted(m, key=lambda x: (x["name"] or "").lower()),
                "placements": sum(x["placements"] for x in m)}
               for r, m in sorted(by.items())]
    return {"regions": regions,
            "total_markers": sum(len(r["markers"]) for r in regions),
            "total": sum(r["placements"] for r in regions),
            "no_maps": True}


# ── One page ────────────────────────────────────────────────────────────────
def build_one(meat_path, ctx):
    md = json.load(open(meat_path, encoding="utf-8"))
    slug = md.get("slug") or os.path.basename(meat_path)[:-5]
    creature = md.get("name") or slug
    meats = [m for m in (md.get("meat_items") or []) if m.get("form_id")]
    if not meats:
        print(f"  [skip] {slug}: no meat item")
        return None
    main = meats[0]
    formid = main["form_id"].upper()
    item = main.get("name") or f"{creature} Meat"
    targets = {m["form_id"].upper() for m in meats}
    out_path = os.path.join(OUT_DIR, f"meat-{slug}_spawns.json")
    keep, keep_markers = _existing_slots(out_path)

    # Used For
    cons = B.build_consumption(formid, TSV, item)
    challenges = list(md.get("used_for") or [])
    have = {c.get("edid") for c in challenges}
    for c in B.build_challenges(formid, DIST):
        if c.get("edid") not in have:
            challenges.append(c)
            have.add(c.get("edid"))
    used_for = collections.OrderedDict([
        ("consumption", cons),
        ("modifiers", ctx["modifiers"]),
        ("challenges", challenges),
        ("recipes", B.build_recipes(item, ctx["recipe_guide"], ctx["bench_cat"],
                                    TSV, ctx["guide_urls"])),
    ])
    if cons is not None:
        cons["_name"] = item
    tips = farming_tips(cons, formid, creature)
    if cons is not None:
        cons.pop("_name", None)

    # Closure (vendors, containers, events, treasure maps)
    src = sources.get_sources([{"formid": m["form_id"], "sig": "ALCH"} for m in meats],
                              ctx["tables"])
    closure = src["lvli_closure"]
    item_closure = {f.upper() for f in closure}

    doc = collections.OrderedDict()
    doc["_meta"] = {"generated": (md.get("_meta") or {}).get("generated"),
                    "source": "dist/meat/" + slug + ".json + farming pipeline "
                              "(src/build_meat_guide_docs.py)"}
    doc["set"] = "meat"
    doc["slug"] = "meat-" + slug
    doc["name"] = item
    doc["page_title"] = f"{creature} Location Guide"
    doc["blurb"] = md.get("blurb", "")
    # Region map links only once that page's tiles are on the site — otherwise
    # every meat page would link to maps that 404. Add the slug to
    # data/meat_spawn_maps_live.txt after uploading its tiles.
    doc["map_base"] = (MAP_ROOT + slug + "/") if slug in ctx["maps_live"] else ""
    # Only regions that HAVE a tile get a link: a region whose spawns are all
    # inside an interior cell has no region tile, and its link would 404.
    if slug in ctx["maps_live"]:
        doc["map_regions"] = ctx["maps_live"][slug]

    bb = (tips or {}).get("perk_cards", {}).get("butchers_bounty")
    drops = clean_drops(md.get("drops"), ctx["lvli"], bb)
    s = md.get("npc_summary") or {}
    bits = []
    if s.get("npc_count"):
        bits.append(f"{s['npc_count']} variant" + ("" if s["npc_count"] == 1 else "s"))
    if s.get("level_min") is not None and s.get("level_max") is not None:
        bits.append(f"level {s['level_min']}" if s["level_min"] == s["level_max"]
                    else f"levels {s['level_min']}–{s['level_max']}")
    if s.get("health_max"):
        bits.append(f"up to {s['health_max']:,} HP")
    yields = ", ".join(f"{m['name']} ({m['rate_display']})" if m.get("rate_display")
                       else m["name"] for m in meats)
    # Just the drop line — the variant / level / HP summary was dropped on request.
    note = f"Every {creature} you kill drops {yields}."
    doc["drop_rates"] = collections.OrderedDict([
        ("creatures", {"note": "", "items": flat_drops(drops, ctx["quests"])}),
        ("collectrons", None),
        ("resource_generators", None),
    ])
    B._patch_camp_producers(doc, targets, DIST)
    B._patch_containers(doc, closure, targets, ctx["rates"], ctx["cont_names"], ctx["tables"])
    # QA containers ("Test Cooler") are dev-only and never placed for players.
    cont = doc["drop_rates"].get("containers")
    if isinstance(cont, dict) and isinstance(cont.get("types"), list):
        cont["types"] = [t for t in cont["types"]
                         if not re.match(r"(?i)^(test|qa|debug)\b", t.get("name") or "")]
    if tips is not None:
        doc["farming_tips"] = tips
    doc["used_for"] = used_for
    doc["vendor_list"] = B.build_vendor_list([], {"items": [{"formid": formid}], "name": item},
                                             item_closure=item_closure,
                                             vendor_master=ctx["vendors"], rates=ctx["rates"])

    # Events: the creature's own (from the meat doc) + reward pools that pay the meat.
    item_events = {"events_activities": []}
    B._patch_events_activities(item_events, ctx["rates"], targets,
                               closure_lists=closure, tables=ctx["tables"])
    evs = list(md.get("events_activities") or [])
    seen = {(e.get("edid") or e.get("name")) for e in evs}
    for e in item_events.get("events_activities") or []:
        k = e.get("edid") or e.get("name")
        if k not in seen and (e.get("rate_value") is None or e.get("rate_value") > 0):
            evs.append(e)
            seen.add(k)
    doc["events_activities"] = evs

    doc["random_encounters"] = list(md.get("random_encounters") or [])
    B._patch_random_encounters(doc)

    doc["regions"] = convert_regions(md, keep, keep_markers, creature)
    doc["chance_spawns"] = convert_chance(md)
    n = doc["chance_spawns"]["total_markers"]
    if n:
        doc["chance_spawns"]["lead"] = (
            f"{creature}s can spawn at {n} location{'' if n == 1 else 's'} that share a "
            f"spawn pool with other creatures, so a {creature} is not guaranteed there. "
            f"They are listed by name only.")
    doc["breakdown_lead"] = "Spawns here:"
    B._patch_treasure_maps(doc, ctx["rates"], targets, DIST)
    doc.setdefault("treasure_maps", {"maps": []})

    os.makedirs(OUT_DIR, exist_ok=True)
    txt = json.dumps(doc, ensure_ascii=False, indent=1)
    open(out_path, "w", encoding="utf-8").write(txt)
    os.makedirs(PTS_OUT_DIR, exist_ok=True)
    open(os.path.join(PTS_OUT_DIR, os.path.basename(out_path)), "w",
         encoding="utf-8").write(txt)
    n_fixed = sum(l["count"] for r in doc["regions"] for l in r["locations"])
    print(f"  meat-{slug:<20} fixed:{n_fixed:>4}  recipes:{len(used_for['recipes']):>2}"
          f"  challenges:{len(challenges):>2}  vendors:{len(doc['vendor_list']):>2}"
          f"  events:{len(evs):>2}  containers:{len(doc['drop_rates']['containers']['types']) if isinstance(doc['drop_rates'].get('containers'), dict) else 0:>2}"
          f"  maps:{len(doc['treasure_maps']['maps']):>2}")
    return out_path


def load_ctx():
    rg = json.load(open(os.path.join(DIST, "recipe_guide.json"), encoding="utf-8"))
    data = rng76.Rng76Data.from_tsv_root(TSV)
    return {
        "recipe_guide": rg,
        "bench_cat": B._bench_category_map(os.path.join(DIST, "cobj-recipes.json")),
        "guide_urls": B._load_guide_urls(),
        "modifiers": B.build_modifiers(TSV),
        "tables": sources.load_tables(TSV),
        "rates": B.VendorRates(data),
        "lvli": data.lvli,
        "cont_names": B._load_cont_names(TSV),
        "vendors": B._load_vendor_master(DIST),
        "maps_live": _maps_live(),
        "quests": _load_quest_names(),
    }


def _maps_live():
    """{slug: [region names that have an uploaded tile]} from
    data/meat_spawn_maps_live.txt — `slug  region-slug region-slug ...`."""
    path = os.path.join(REPO, "data", "meat_spawn_maps_live.txt")
    out = {}
    if not os.path.exists(path):
        return out
    by_slug = {re.sub(r"[^a-z0-9]+", "-", r.lower()).strip("-"): r for r in ALL_REGIONS}
    for ln in open(path, encoding="utf-8"):
        parts = ln.split("#")[0].split()
        if parts:
            out[parts[0].lower()] = [by_slug[x] for x in parts[1:] if x in by_slug]
    return out


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    want = {a.lower() for a in argv if not a.startswith("-")}
    paths = sorted(p for p in glob.glob(os.path.join(DIST, "meat", "*.json"))
                   if os.path.basename(p) != "meat.json")
    if want:
        paths = [p for p in paths if os.path.basename(p)[:-5] in want]
    ctx = load_ctx()
    print(f"[meat-guides] {len(paths)} page(s)")
    for p in paths:
        try:
            build_one(p, ctx)
        except Exception as e:
            print(f"  [fail] {os.path.basename(p)}: {e}")
            raise


if __name__ == "__main__":
    main()
