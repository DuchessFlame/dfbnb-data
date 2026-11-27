#!/usr/bin/env python3
r"""
build_cryptid_guide_docs.py — turn each Cryptids doc into a farming-guide doc, so
the cryptid pages (/df/cryptids/<slug>/) render through the SAME renderer as the
Deathclaw Egg guide (df-bnb-farming-non-perishable-guide.js), the baseline for
every spawn / farming page (Duchess, Nov 2026). The old df-bnb-cryptids.js layout
is retired for the cryptid pages; the hub still uses the category-hub template.

  in : dist/cryptids/<slug>.json                       (spawns_configs/cryptids.py)
  out: dist/farming_spawns/cryptid-<slug>_spawns.json

Run once per channel. In CI the PTS workflow has tsv/ normalised to the PTS
exports and moves dist/ -> dist/pts/ at the end, so the same plain command
covers both. Locally, `--pts` reads dist/pts/cryptids/ and writes
dist/pts/farming_spawns/ (its rate joins still read tsv/, so CI's PTS run is
the one to trust for PTS percentages).

Where each part comes from — nothing typed, the same shared functions the meat
and NPC pages use:

  Used For            the cryptid's own challenges (recipes: null — nothing cooks)
  Inventory           what the cryptid drops when killed, one column per variant
                      (creature_inventory.build_inventory, rng76)
  Creatures           every creature that drops this cryptid's own parts
                      (creature_inventory.build_creatures, rng76)
  Collectrons /       build_farming_used_for joins, on the cryptid's own parts
  Resource Gen. /
  Containers / Vendors / Treasure Maps
  Events & Activities the cryptid's own events + reward pools that pay its parts
  Random Encounters   the cryptid doc, linked to their type guides
  Fixed Spawn         the cryptid doc's fixed spawns, all ten regions, one photo
                      slot per spawn; hand-authored slots kept by `ref`
  Chance to Spawn     the cryptid doc's weighted spawn points, names only
  map_base            /wp-content/uploads/guide-images/cryptids/<slug>/

  python src/build_cryptid_guide_docs.py                  # every cryptid (live)
  python src/build_cryptid_guide_docs.py --pts            # every cryptid (PTS)
  python src/build_cryptid_guide_docs.py mothman wendigo  # named pages only
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
import build_meat_guide_docs as M
import creature_inventory as CI
import farming_spawns_sources as sources
from spawns_configs import cryptids as C
import rng76

DIST = os.path.join(REPO, "dist")
TSV = os.path.join(REPO, "tsv")
MAP_ROOT = "/wp-content/uploads/guide-images/cryptids/"
APPALACHIA_SPACE = 2480661
GEO_CACHE = os.path.join(REPO, "data", "cryptid_spawns", "geo_cache.json")
EXTRAS = os.path.join(REPO, "data", "cryptid_guide_extras.json")
# Where the rendered maps live (render_spawn_maps.py --source cryptid --out ...).
# Only read for interior_cells.csv; CI has no such folder, so the last doc's
# interior_maps list is carried forward there.
GUIDES_ROOTS = [os.environ.get("GUIDES_ROOT") or "",
                os.path.expanduser(r"~/mnt/Guides and Stuff"),
                r"C:\Users\Duche\OneDrive\Guides and Stuff"]
# Cryptid folders whose name isn't the page name.
FOLDER_ALIASES = {"jersey-lesser-devils": "Jersey and Lesser Devils"}
ALL_REGIONS = M.ALL_REGIONS

# Fixed Spawn count list labels per source type. "spawn" = an Encounter Spawn
# System point whose pool only holds this cryptid; "ambush" = a placed ambush actor.
COUNT_LABELS = {"ambush": ["Ambush point", "Ambush points"],
                "spawn": ["Spawn point", "Spawn points"],
                "quest": ["Scripted spawn point", "Scripted spawn points"],
                "placement": ["Fixed spawn", "Fixed spawns"]}


def _plural(name):
    """'Aliens', 'Jersey and Lesser Devils' read as plural; 'Mothman' does not."""
    return name.endswith("s") or " and " in name


def _the(name):
    return "" if _plural(name) or name in ("Bigfoot", "Mothman", "Ogua") else "the "


def _own_items(inv, slug, md, creature):
    """The cryptid's own parts: creature_parts.tsv owners plus any direct drop named
    after the creature (Snallygaster Hide, Torn Mothman Wing, Wendigo Teeth)."""
    own = set(CI.own_items(inv, slug, []))
    words = {w.lower() for w in re.findall(r"[A-Za-z]+", creature) if len(w) > 3}
    words.discard("lesser")
    for lst in (md.get("drops") or {}).get("lists", []):
        for r in lst.get("rows", []):
            if r.get("kind") != "item" or not r.get("form_id"):
                continue
            name = (r.get("name") or "").lower()
            if name.startswith(("plan:", "recipe:", "player title")):
                continue
            if any(w in name for w in words):
                own.add(r["form_id"].upper())
    return own


def _label(sp, creature):
    """What is standing there. Cryptid spawns carry a label like
    'Snallygaster spawn #3'; drop the number (it is re-counted per marker)."""
    st = sp.get("source_type") or "placement"
    if st == "ambush":
        return f"{creature} ambush"
    if st == "spawn":
        return f"{creature} spawn"
    if st == "quest":
        return f"{creature} scripted spawn"
    return re.sub(r"\s*#\d+$", "", sp.get("label") or "") or creature


def convert_regions(md, keep, keep_markers, creature, route):
    by_region = {r["region"]: r for r in (md.get("fixed_spawns") or {}).get("regions", [])}
    out = []
    for name in ALL_REGIONS:
        reg = by_region.get(name) or {"region": name, "locations": []}
        locs = []
        for loc in M._route_sort(reg.get("locations", []), route["markers"].get(name),
                                 lambda l: l.get("marker")):
            spawns = M._route_sort(loc.get("spawns") or [],
                                   route["spawns"].get((name, loc.get("marker"))),
                                   lambda s: s.get("ref"))
            labels = collections.Counter(_label(sp, creature) for sp in spawns)
            seen = collections.Counter()
            new_spawns = []
            for sp in spawns:
                v = _label(sp, creature)
                seen[v] += 1
                k = keep.get(sp.get("ref"))
                new_spawns.append(collections.OrderedDict([
                    ("label", f"{v} #{seen[v]}" if labels[v] > 1 else v),
                    ("ref", sp.get("ref")),
                    ("source_type", sp.get("source_type") or "placement"),
                    ("coords", sp.get("coords")),
                    ("image_top", M._slot(sp, k, "image_top")),
                    ("directions", M._slot(sp, k, "directions")),
                    ("image_bottom", M._slot(sp, k, "image_bottom")),
                ]))
            km = keep_markers.get((name, loc.get("marker")))
            new = collections.OrderedDict()
            new["marker"] = loc.get("marker")
            new["count"] = loc.get("count") or len(spawns)
            new["sources"] = loc.get("sources") or {"placement": new["count"]}
            for key in ("image_top", "directions", "image_bottom"):
                new[key] = M._slot(loc, km, key)
            new["refs"] = loc.get("refs") or [s.get("ref") for s in spawns if s.get("ref")]
            new["coords"] = loc.get("coords")
            new["breakdown"] = [{"label": v, "count": n,
                                 "source_type": (spawns[0].get("source_type") if spawns
                                                 else "placement")}
                                for v, n in labels.items()] or \
                               [{"label": creature, "count": new["count"],
                                 "source_type": "placement"}]
            new["spawns"] = new_spawns
            if loc.get("spawns_compacted"):
                new["spawns_compacted"] = True
            locs.append(new)
        out.append({"region": name, "locations": locs})
    return out


def tile_regions(regions, geo):
    """Regions with at least one EXTERIOR fixed spawn — the only ones that get a
    region tile. An interior-only region would link to a map that doesn't exist."""
    out = []
    for reg in regions:
        for loc in reg["locations"]:
            if any((geo.get(str(int(r, 16))) or {}).get("space") == APPALACHIA_SPACE
                   for r in loc.get("refs") or []):
                out.append(reg["region"])
                break
    return out


def _key(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def cryptid_folder(slug, creature):
    """Guides and Stuff\\.Cryptids\\<folder> — matched, never invented."""
    want = {_key(FOLDER_ALIASES.get(slug, "")), _key(creature), _key(slug)} - {""}
    for root in GUIDES_ROOTS:
        for sub in (".Cryptids", "Cryptids Guide"):
            base = os.path.join(root, sub) if root else ""
            if not base or not os.path.isdir(base):
                continue
            for name in os.listdir(base):
                if _key(name) in want and os.path.isdir(os.path.join(base, name)):
                    return os.path.join(base, name)
    return None


def interior_maps(slug, creature, map_base, prev):
    """[{name, url, desc}] for every interior cell map the renderer wrote. The page
    HEAD-checks each one, so a map that isn't uploaded yet simply doesn't show."""
    import csv
    folder = cryptid_folder(slug, creature)
    path = os.path.join(folder, "04 Interior Maps", "interior_cells.csv") if folder else ""
    if not path or not os.path.exists(path):
        return prev or []
    out = []
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            if r.get("file"):
                n = int(r.get("total_spawns") or 0)
                out.append({"name": f"{r.get('display_name')} ({r.get('region')})",
                            "url": map_base + r["file"],
                            "desc": f"{n} spawn{'' if n == 1 else 's'} inside"})
    return out


def intro_line(md, creature):
    s = md.get("npc_summary") or {}
    bits = []
    if s.get("npc_count"):
        bits.append(f"{s['npc_count']} variant" + ("" if s["npc_count"] == 1 else "s"))
    if s.get("level_min") is not None and s.get("level_max") is not None:
        bits.append(f"level {s['level_min']}" if s["level_min"] == s["level_max"]
                    else f"levels {s['level_min']}–{s['level_max']}")
    if s.get("health_max"):
        bits.append(f"up to {s['health_max']:,} HP")
    return " · ".join(bits)


def _owning_quest(edid):
    """The quest a reward list belongs to: a quest EDID inside its own EDID
    (E07A_Mothman_CultistHighPriestReward_Loot -> E07A_Mothman), else the one
    named, typed quest of its prefix family (HTO_* -> HTO_HostileTakeOver_Master_Quest,
    "Infestation")."""
    if not edid:
        return None
    C._evq_tables()
    quests = C._EVQ["t"][5]
    q = C._quest_named_in(edid, quests)
    if q and q["full"] and not q["full"].startswith("["):
        return q
    head = edid.split("_")[0].lower() + "_"
    cands = [q for k, q in quests.items() if k.startswith(head) and q["full"]
             and not q["full"].startswith("[") and not C._CUT_RE.search(q["edid"])
             and (q["type"] or "").lower() not in ("", "none", "server")]
    return cands[0] if len(cands) == 1 else None


def build_one(src_path, ctx, out_dir):
    md = json.load(open(src_path, encoding="utf-8"))
    slug = md.get("slug") or os.path.basename(src_path)[:-5]
    creature = md.get("name") or slug
    out_path = os.path.join(out_dir, f"cryptid-{slug}_spawns.json")
    keep, keep_markers = M._existing_slots(out_path)
    try:
        prev_doc = json.load(open(out_path, encoding="utf-8")) if os.path.exists(out_path) else {}
    except Exception:
        prev_doc = {}
    ex = ctx["extras"].get(slug) or {}
    inv = ctx["inv"]

    own = _own_items(inv, slug, md, creature)
    if own:
        src = sources.get_sources([{"formid": f, "sig": "MISC"} for f in sorted(own)],
                                  ctx["tables"])
        closure = src["lvli_closure"]
    else:
        closure = set()

    doc = collections.OrderedDict()
    doc["_meta"] = {"generated": (md.get("_meta") or {}).get("generated"),
                    "source": "dist/cryptids/" + slug + ".json + farming pipeline "
                              "(src/build_cryptid_guide_docs.py)"}
    doc["set"] = "cryptid"
    doc["slug"] = "cryptid-" + slug
    doc["name"] = creature
    doc["page_title"] = creature
    the = _the(creature)
    verb = "spawn" if _plural(creature) else "spawns"
    doc["blurb"] = (f"Where {the}{creature} {verb} in Fallout 76, grouped by region, "
                    f"and everything {'they drop' if _plural(creature) else 'it drops'}.")
    doc["intro_note"] = ex.get("intro_note") or intro_line(md, creature)
    doc["map_base"] = MAP_ROOT + slug + "/"
    doc["map_ext"] = ".jpg"
    doc["breakdown_lead"] = "Spawns here:"
    # Every map in one closed Maps root expand above the gallery (guide-maps-expand).
    doc["maps_expand"] = ex.get("maps_expand", True)
    if ex.get("gallery"):
        doc["gallery"] = [dict({"url": doc["map_base"] + g["file"],
                                "caption": g.get("caption", "")},
                               **({"credit": g["credit"]} if g.get("credit") else {}))
                          for g in ex["gallery"] if g.get("file")]

    lists = [{"form_id": l.get("form_id"), "edid": l.get("edid")}
             for l in (md.get("drops") or {}).get("lists", []) if l.get("form_id")]
    doc["inventory"] = CI.build_inventory(
        inv, lists, base_name=creature,
        event_note=lambda e: ("only during " + M.event_of(e, ctx["quests"]))
        if M.event_of(e, ctx["quests"]) else "")
    doc["drop_rates"] = collections.OrderedDict([
        ("creatures", {"note": "", "rows": CI.build_creatures(inv, own) if own else [],
                       "items": []}),
        ("collectrons", None),
        ("resource_generators", None),
    ])
    B._patch_camp_producers(doc, own, DIST)
    if own:
        B._patch_containers(doc, closure, own, ctx["rates"], ctx["cont_names"], ctx["tables"])
    if not isinstance(doc["drop_rates"].get("containers"), dict):
        doc["drop_rates"]["containers"] = {"types": []}
    cont = doc["drop_rates"]["containers"]
    cont["types"] = [t for t in cont.get("types") or []
                     if not re.match(r"(?i)^(test|qa|debug)\b", t.get("name") or "")]
    loot = CI.container_loot(inv, creature)
    if loot:
        loot["own_ids"] = sorted(own)
        cont["loot"] = loot

    doc["used_for"] = collections.OrderedDict([
        ("challenges", list(md.get("used_for") or [])),
        ("recipes", None),
    ])
    doc["vendor_list"] = (B.build_vendor_list(
        [], {"items": [{"formid": f} for f in sorted(own)], "name": creature},
        item_closure={f.upper() for f in closure},
        vendor_master=ctx["vendors"], rates=ctx["rates"]) if own else [])

    evs = list(md.get("events_activities") or [])
    if own:
        item_events = {"events_activities": []}
        B._patch_events_activities(item_events, ctx["rates"], own,
                                   closure_lists=closure, tables=ctx["tables"])
        seen = {(e.get("edid") or e.get("name")) for e in evs}
        have_q = {e.get("edid") for e in evs if e.get("edid")}
        for e in item_events.get("events_activities") or []:
            # Reward pools come named from their list EDID ("HTO cr LLS Rewards
            # Bespoke Mob"). Name them after the event that pays them out, and drop
            # the row when that event is already listed (27 Nov 2026).
            q = _owning_quest(e.get("edid") or "")
            if q:
                if q["edid"] in have_q:
                    continue
                e = dict(e, name=q["full"], edid=q["edid"],
                         type=(q["type"] if q["type"] not in ("", "None") and not q["type"].isdigit()
                               else e.get("type")))
                have_q.add(q["edid"])
            k = e.get("edid") or e.get("name")
            if k not in seen and (e.get("rate_value") is None or e.get("rate_value") > 0):
                evs.append(e)
                seen.add(k)
    doc["events_activities"] = evs
    doc["random_encounters"] = list(md.get("random_encounters") or [])
    B._patch_random_encounters(doc)

    from spawns_engine import route_order
    doc["regions"] = convert_regions(md, keep, keep_markers, creature,
                                     route_order.load("cryptid-" + slug))
    doc["fixed_counts"] = M._fixed_counts(doc["regions"], creature, COUNT_LABELS)
    if ex.get("fixed_note"):
        doc["fixed_note"] = ex["fixed_note"]
    n_fixed = sum(l["count"] for r in doc["regions"] for l in r["locations"])
    doc["map_regions"] = tile_regions(doc["regions"], ctx["geo"])
    doc["full_map"] = (doc["map_base"] + slug + ".jpg") if doc["map_regions"] else ""
    doc["interior_maps"] = interior_maps(slug, creature, doc["map_base"],
                                         prev_doc.get("interior_maps"))

    doc["chance_spawns"] = M.convert_chance(md)
    # Chance maps (opt-in, extras "chance_maps": true): the night strange-encounter
    # spots Mothman and Flatwoods share. convert_chance drops refs (meat has none),
    # so put them back for render_spawn_maps and let the page show the maps.
    if ex.get("chance_maps"):
        refs = {}
        for loc in (md.get("chance_spawns") or {}).get("locations", []):
            refs.setdefault((loc.get("region") or "Unknown", loc.get("marker")), []).extend(
                loc.get("refs") or [])
        for reg in doc["chance_spawns"]["regions"]:
            for m in reg["markers"]:
                m["refs"] = sorted(set(refs.get((reg["region"], m["name"]), [])))
        if any(m["refs"] for r in doc["chance_spawns"]["regions"] for m in r["markers"]):
            doc["chance_spawns"].pop("no_maps", None)
            doc["chance_spawns"]["show_maps"] = True
    n = doc["chance_spawns"]["total_markers"]
    if n:
        doc["chance_spawns"]["lead"] = (
            f"{creature} can spawn at {n} location{'' if n == 1 else 's'} that share a "
            f"spawn pool with other creatures, so {'they are' if _plural(creature) else 'it is'} "
            f"not guaranteed there."
            + ("" if doc["chance_spawns"].get("show_maps") else " They are listed by name only."))
    note = (md.get("chance_spawns") or {}).get("note")
    doc["info_notes"] = [{"title": "Spawn Notes", "body": note}] if note else []

    B._patch_treasure_maps(doc, ctx["rates"], own, DIST)
    doc.setdefault("treasure_maps", {"maps": []})

    os.makedirs(out_dir, exist_ok=True)
    open(out_path, "w", encoding="utf-8").write(
        json.dumps(doc, ensure_ascii=False, indent=1))
    ok = all(len(l.get("spawns") or []) == l["count"] or l.get("spawns_compacted")
             for r in doc["regions"] for l in r["locations"])
    inv_rows = sum(len(g.get("rows") or []) for g in (doc["inventory"] or {}).get("groups", []))
    print(f"  cryptid-{slug:<22} fixed:{n_fixed:>4} chance-markers:{n:>4} "
          f"tiles:{len(doc['map_regions']):>2} challenges:{len(doc['used_for']['challenges']):>2} "
          f"inventory:{inv_rows:>4} own:{len(own)} creatures:{len(doc['drop_rates']['creatures']['rows']):>2} "
          f"events:{len(evs):>2} res:{len(doc['random_encounters']):>2} "
          f"vendors:{len(doc['vendor_list']):>2} slots-ok:{ok}")
    return out_path


def load_ctx():
    data = rng76.Rng76Data.from_tsv_root(TSV)
    raw = json.load(open(GEO_CACHE, encoding="utf-8")) if os.path.exists(GEO_CACHE) else {}
    # The cache is keyed "<page>:<ref>" (older copies used the bare ref); index both.
    geo = {}
    for k, v in raw.items():
        geo[str(k)] = v
        geo[str(k).rsplit(":", 1)[-1]] = v
    extras = {}
    if os.path.exists(EXTRAS):
        extras = {k: v for k, v in json.load(open(EXTRAS, encoding="utf-8")).items()
                  if not k.startswith("_")}
    return {
        "tables": sources.load_tables(TSV),
        "rates": B.VendorRates(data),
        "cont_names": B._load_cont_names(TSV),
        "vendors": B._load_vendor_master(DIST),
        "quests": M._load_quest_names(),
        "inv": CI.Ctx(data),
        "geo": geo,
        "extras": extras,
    }


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    pts = "--pts" in argv
    want = {a.lower() for a in argv if not a.startswith("-")}
    src_dir = os.path.join(DIST, "pts", "cryptids") if pts else os.path.join(DIST, "cryptids")
    out_dir = os.path.join(DIST, "pts" if pts else "", "farming_spawns")
    paths = sorted(glob.glob(os.path.join(src_dir, "*.json")))
    if want:
        paths = [p for p in paths if os.path.basename(p)[:-5] in want]
    ctx = load_ctx()
    print(f"[cryptid-guides] {len(paths)} page(s) -> {os.path.relpath(out_dir, REPO)}")
    for p in paths:
        build_one(p, ctx, os.path.normpath(out_dir))


if __name__ == "__main__":
    main()
