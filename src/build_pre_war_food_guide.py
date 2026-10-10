#!/usr/bin/env python3
r"""
build_pre_war_food_guide.py — the Pre-War Food Location Guide (Oct 2026).

    /bnb/farming/non-perishable/pre-war-food-location-guide/

One farming-guide page for EVERY pre-war food, drawn by the Deathclaw Egg
renderer (df-bnb-farming-non-perishable-guide.js), the baseline for every
farming page.

  out: dist/farming_spawns/pre-war-food_spawns.json        (default = live)
       dist/pts/farming_spawns/pre-war-food_spawns.json     (--pts)
  geo: data/farming_spawns/geo_cache_pre_war_food.json     (seeded locally with
       the Mappalachia DB; CI rebuilds from it with no DB)

WHICH ITEMS — Bethesda's own list, nothing typed
  The "Eat Pre-war Food" / "Collect Pre-war Food" challenges (CHAL) test
  `HasKeyword <kw> "Pre-war Food"`. That keyword (MealTypePackaged [0013485D] as
  of Oct 2026) is read off the CHAL export, and every named ALCH carrying it is a
  pre-war food. A new pre-war food joins the page on the next build with no edit.
  Records with the same name AND the same effects (the Lucky Strike copies of Gum
  Drops / Cotton Candy Bites) are one food on the page. Same name but different
  effects (the *_PreWar_Clean copies have no radiation) are shown apart, the
  rad-free one marked "(no rads)".

PAGE SHAPE (read by the renderer's multi-item path)
  sub_items[]   one farming doc per food (Used For, Farming Tips, Collectrons,
                Containers, Creatures, Events, Resource Generators, Treasure Maps,
                Vendors). A food that already has its own guide carries only
                `page_url`, and the page links to it instead of repeating it.
  fixed_items[] one Fixed Spawn Locations sub-expand per food that has physical
                fixed spawns: the item's own placed base, or a leveled list that
                can ONLY hand out that food (spawn-guide 9k dedication rule).
                Each has its own map folder (map_base / full_map).
  chance_spawns the MIXED pre-war food lists — a world-placed leveled list whose
                every leaf is a pre-war food but which can roll more than one of
                them (LPI_Food_Packaged as of Oct 2026). `pools[].contents` is the
                chance per spawn point for each food (rng76). Names only by
                region, with a chance map per region (show_maps).
  regions       left empty on purpose: the per-food regions live in fixed_items.

Hand-authored photos / directions are kept across rebuilds by food + ref.

  python src/build_pre_war_food_guide.py            # live
  python src/build_pre_war_food_guide.py --pts      # PTS

A full run resolves ~40 foods through rng76 and takes about half an hour. To do a
local run in chunks (each chunk under a time limit):
  python src/build_pre_war_food_guide.py --state pwf_state.json --budget-seconds 120
  ...repeat until it stops exiting with code 3.
"""
import collections
import csv
import datetime
import json
import os
import re
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import build_farming_used_for as B
import perk_ranks
import rng76
import tsv_source
from farming_spawns_config import ALL_SETS, ALL_REGIONS
from spawns_engine import sources as esources
from spawns_engine import build as ebuild
from spawns_engine import route_order
from spawns_engine.classify import make_farming_classify

csv.field_size_limit(10 ** 9)

SLUG = "pre-war-food"
NAME = "Pre-War Food"
MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
GEO_CACHE = os.path.join(REPO, "data", "farming_spawns", "geo_cache_pre_war_food.json")
# The page's guide-images folder. Each food's fixed-spawn maps sit in a sub-folder
# (<base><food-slug>/); the mixed-list chance maps sit in the page folder itself.
MAP_BASE = "/wp-content/uploads/guide-images/farming-non-perishable/pre-war-food/"
# Fallback only — used if the CHAL export carries no "Pre-war Food" keyword test.
FALLBACK_KEYWORD = "MealTypePackaged"
FIXED_TYPES = ("direct", "static")
# Bump when item_doc() changes, so a saved --state file is not reused.
STATE_VERSION = 2
RAD_EFFECT = "DamageRadiationEating"


def r2(x):
    return round(x, 2)


def slugify(s):
    return re.sub(r"-+$", "", re.sub(r"[^a-z0-9]+", "-", str(s or "").lower()).lstrip("-"))


# ── Items: CHAL keyword -> ALCH ──────────────────────────────────────────────
_KW_RE = re.compile(r'HasKeyword\|[^|]*\|[^|]*\|(\w+) "Pre-war Food" \[KYWD:([0-9A-Fa-f]{8})\]')


def prewar_keywords(tsv):
    """{EDID: FormID} of every keyword the challenges call "Pre-war Food"."""
    path = tsv_source.newest(os.path.join(tsv, "CHAL_Export_*.tsv"), required=False)
    out = {}
    if path:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                for edid, fid in _KW_RE.findall(line):
                    out[edid] = fid.upper()
    if not out:
        print(f"  [warn] no 'Pre-war Food' keyword found in CHAL — falling back to {FALLBACK_KEYWORD}")
        out[FALLBACK_KEYWORD] = ""
    return out


def _effects(tsv):
    """ALCH FormID -> [MGEF EDID, ...] from the Effects export."""
    path = B._resolve_alch(tsv, effects=True)
    out = collections.defaultdict(list)
    for r in (B._read_tsv(path) if path else []):
        out[(r.get("ALCH_FormID") or "").upper()].append(
            ((r.get("MGEF_EDID") or "").strip(), (r.get("EFIT_Magnitude") or "").strip(),
             (r.get("EFIT_Duration") or "").strip()))
    return out


def discover_items(tsv):
    """[{key, name, full, form_ids, edids, weight}] — one entry per pre-war food."""
    kws = prewar_keywords(tsv)
    path = B._resolve_alch(tsv, effects=False)
    rows = [r for r in (B._read_tsv(path) if path else [])
            if (r.get("FULL") or "").strip()
            and any(f"{k}[" in (r.get("Keywords_Flat") or "") for k in kws)]
    effects = _effects(tsv)

    groups = collections.OrderedDict()
    for r in rows:
        fid = (r.get("ALCH_FormID") or "").upper()
        full = r["FULL"].strip()
        sig = tuple(sorted(effects.get(fid, [])))
        k = (full.lower(), sig, (r.get("Weight") or "").strip())
        g = groups.setdefault(k, {"full": full, "form_ids": [], "edids": [],
                                  "weight": B._safe_num(r.get("Weight")),
                                  "rads": any(e[0] == RAD_EFFECT for e in sig),
                                  "kw": r.get("Keywords_Flat") or ""})
        g["form_ids"].append(fid)
        g["edids"].append(r.get("ALCH_EDID") or "")

    # Same name, different food: name the rad-free one.
    by_name = collections.defaultdict(list)
    for g in groups.values():
        by_name[g["full"].lower()].append(g)
    items = []
    for g in groups.values():
        name = g["full"]
        twins = by_name[name.lower()]
        if len(twins) > 1 and not g["rads"] and any(t["rads"] for t in twins):
            name += " (no rads)"
        g["name"] = name
        items.append(g)
    seen = collections.Counter()
    for g in items:
        seen[g["name"].lower()] += 1
        if seen[g["name"].lower()] > 1:                      # still a clash: number it
            g["name"] += f" ({seen[g['name'].lower()]})"
        g["key"] = slugify(g["name"])
    items.sort(key=lambda g: g["name"].lower())
    print(f"  pre-war food keyword(s): {', '.join(f'{k} [{v}]' for k, v in kws.items())}"
          f" -> {len(rows)} records, {len(items)} foods")
    return items


# ── Foods that already have their own guide ─────────────────────────────────
def own_pages():
    """{ALCH FormID: guide URL} for every farming page that is about ONE item.

    Driven by farming_spawns_config.ALL_SETS + tsv/guide_index.tsv, so a food
    that gets its own page later is linked here with no edit."""
    urls = {}
    path = os.path.join(REPO, "tsv", "guide_index.tsv")
    rows = []
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh, delimiter="\t"))
    out = {}
    for cfg in ALL_SETS:
        if cfg.get("multi_item") or len(cfg.get("items") or []) != 1:
            continue
        slug = cfg["slug"]
        url = next((r.get("url") for r in rows
                    if f"/{slug}/" in (r.get("url") or "")
                    and (r.get("url") or "").rstrip("/").endswith("-guide")
                    and (r.get("status") or "published") == "published"), None)
        if url:
            out[cfg["items"][0]["formid"].upper()] = url
    return out


# ── Farming tips ────────────────────────────────────────────────────────────
def farming_tips(item, cons, can_do, has_fixed, channel):
    if not cons:
        return None
    w = cons.get("weight") if cons.get("weight") is not None else (item["weight"] or 0)
    obj = cons.get("object_type") or "Food"
    weight_perk = "traveling_pharmacy" if obj == "Chem" else "thru_hiker"
    slugs = [weight_perk] + (["can_do"] if can_do else [])
    ft = collections.OrderedDict()
    if not has_fixed:
        ft["spawn_note"] = f"{item['name']} has no fixed world spawns."
    ft.update([
        ("spoils", False),                       # no pre-war food has a spoiled form
        ("spoil_duration_hours", None),
        ("base_weight", w),
        ("object_type", obj),
        ("yield_perk", "can_do" if can_do else None),
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
        ("good_with_salt", False),
        ("perk_cards", perk_ranks.cards(slugs, channel)),
    ])
    return ft


def can_do_items(tables):
    """FormIDs the Can Do! perk can find — the leaves of LL_Perk_CanDo_Items."""
    lid = next((fid for fid, ed in tables["parent_edid"].items()
                if ed.lower() == "ll_perk_cando_items"), None)
    return esources._leaves(lid, tables["p2c"], {}) if lid else set()


# ── One food's sections ─────────────────────────────────────────────────────
def item_doc(item, ctx, has_fixed):
    fids = item["form_ids"]
    targets = set(fids)
    cons = B.build_consumption(fids[0], ctx["tsv"], item["full"])
    chal, seen = [], set()
    for f in fids:
        for c in B.build_challenges(f, ctx["dist"]):
            k = c.get("edid") or c.get("name")
            if k not in seen:
                seen.add(k)
                chal.append(c)
    used_for = collections.OrderedDict([
        ("consumption", cons),
        ("modifiers", ctx["modifiers"]),
        ("challenges", chal),
        ("recipes", B.build_recipes(item["full"], ctx["recipe_guide"], ctx["bench_cat"],
                                    ctx["tsv"], ctx["guide_urls"])),
    ])
    recs = [{"formid": f, "sig": "ALCH"} for f in fids]
    closure = esources.get_sources(recs, ctx["tables"], ctx["classify"])["lvli_closure"]
    # LL_Perk_CanDo / LL_Perk_CanDo_Items are the EXTRA roll a container makes only
    # when you have the Can Do! perk (HasPerk CanDo01 + Luck bands on every entry).
    # Every food in it is pre-war, so left in it reads as "Cooler 100%" for the whole
    # page. It is a perk bonus, not the container's loot — Farming Tips covers it.
    closure = {L for L in closure
               if not ctx["tables"]["parent_edid"].get(L, "").lower().startswith("ll_perk_cando")}

    doc = collections.OrderedDict()
    doc["name"] = item["name"]
    doc["form_ids"] = fids
    doc["used_for"] = used_for
    tips = farming_tips(item, cons, bool(targets & ctx["can_do"]), has_fixed, ctx["channel"])
    if tips:
        doc["farming_tips"] = tips
    doc["drop_rates"] = collections.OrderedDict([
        ("creatures", None), ("collectrons", None), ("resource_generators", None)])
    B._patch_camp_producers(doc, targets, ctx["dist"], ctx["rates"])
    B._patch_containers(doc, closure, targets, ctx["rates"], ctx["cont_names"], ctx["tables"],
                        placed_bases=ctx["cont_bases"], corpse_bodies=True,
                        npc_dir=ctx["tsv"])
    cont = doc["drop_rates"].get("containers")
    if isinstance(cont, dict) and isinstance(cont.get("types"), list):
        cont["types"] = [t for t in cont["types"]
                         if not re.match(r"(?i)^(test|qa|debug)\b", t.get("name") or "")
                         # FlipCardSignManager's hidden counter box — not lootable
                         and not re.search(r"(?i)\bcounts items\b", t.get("name") or "")]
    B._patch_creatures(doc, {"creatures_per_type": True}, closure, targets,
                       ctx["rates"], ctx["tables"], ctx["tsv"])
    doc["vendor_list"] = B.build_vendor_list(
        [], {"items": recs, "name": item["name"]},
        item_closure={f.upper() for f in closure},
        vendor_master=ctx["vendors"], rates=ctx["rates"])
    doc["events_activities"] = []
    B._patch_events_activities(doc, ctx["rates"], targets,
                               closure_lists=closure, tables=ctx["tables"])
    ev_seen, evs = set(), []
    for e in doc["events_activities"]:
        if e.get("rate_value") is not None and e["rate_value"] <= 0:
            continue
        k = (e.get("name"), e.get("type"), e.get("rate_display"))
        if k not in ev_seen:
            ev_seen.add(k)
            evs.append(e)
    doc["events_activities"] = evs
    B._patch_treasure_maps(doc, ctx["rates"], targets, ctx["dist"])
    doc.setdefault("treasure_maps", {"maps": []})
    return doc


# ── Hand-authored slots from the previous build ─────────────────────────────
def load_keep(path):
    """{food key: {(region, marker): {...marker slots, 'spawns': {ref: slots}}}}"""
    out = {}
    try:
        old = json.load(open(path, encoding="utf-8"))
    except Exception:
        return out
    for fi in old.get("fixed_items") or []:
        keep = {}
        for reg in fi.get("regions") or []:
            for loc in reg.get("locations") or []:
                sp = {}
                for s in loc.get("spawns") or []:
                    saved = {k: s.get(k) for k in ("image_top", "directions", "image_bottom")
                             if s.get(k)}
                    if s.get("ref") and saved:
                        sp[s["ref"]] = saved
                keep[(reg.get("region", ""), loc.get("marker", ""))] = {
                    "image_top": loc.get("image_top", ""),
                    "directions": loc.get("directions", ""),
                    "image_bottom": loc.get("image_bottom", ""),
                    "spawns": sp}
        out[fi.get("key")] = keep
    return out


# ── Fixed spawns, one food at a time ────────────────────────────────────────
def fixed_spawns(item, ctx, keep):
    """(regions, total) — only the points that can ONLY ever be this food."""
    recs = [{"formid": f, "sig": "ALCH", "edid": e, "world_source_type": "static"}
            for f, e in zip(item["form_ids"], item["edids"])]
    src = esources.get_sources(recs, ctx["tables"], ctx["classify"], place_item_bases=True)
    only = {
        "lvli_closure": src["lvli_closure"],
        # the food's own base placed straight into the world
        "placed_bases": {f.upper(): {"sig": "ALCH", "edid": "", "source_type": "static",
                                     "via": "item-base"} for f in item["form_ids"]},
        # REFRs of the food itself, or of a leveled list dedicated to it
        "direct_refrs": {k: v for k, v in src["direct_refrs"].items() if v.get("dedicated")},
    }
    seen, _n = ebuild.resolve_placements(only, ctx["geo"], ctx["cur"], ctx["cache"], ctx["db_ok"])
    seen = {k: v for k, v in seen.items() if v[4] in FIXED_TYPES}
    route = route_order.load(f"{SLUG}:{item['key']}")
    regions, _st, unresolved, _total, placements = ebuild.group_regions(
        seen, ALL_REGIONS, keep, route=route)
    for reg in regions:
        for loc in reg["locations"]:
            n = len(loc["spawns"])
            for i, sp in enumerate(loc["spawns"], 1):
                sp["label"] = f"{item['name']} #{i}" if n > 1 else item["name"]
            loc["breakdown"] = [{"label": item["name"], "count": loc["count"],
                                 "rate_key": "static", "note": ""}]
    if unresolved:
        print(f"    {item['name']}: {sum(unresolved.values())} unresolved placement(s)")
    return regions, placements


# ── Mixed pre-war food lists (Chance to Spawn) ──────────────────────────────
def mixed_lists(items, ctx):
    """World-placed leveled lists whose every leaf is a pre-war food, but which
    can hand out more than one food. -> [(list FormID, [item, ...])]"""
    t = ctx["tables"]
    by_fid = {f: it for it in items for f in it["form_ids"]}
    allf = set(by_fid)
    closure = esources._closure(allf, t["c2p"])
    cache, out = {}, []
    for lv in sorted(closure):
        if not any(s == "REFR" for _rf, _e, s in t["lvli_refs"].get(lv, ())):
            continue
        leaves = esources._leaves(lv, t["p2c"], cache)
        if not leaves or not leaves <= allf:
            continue
        foods = []
        for f in sorted(leaves):
            if by_fid[f] not in foods:
                foods.append(by_fid[f])
        if len(foods) >= 2:
            out.append((lv, foods))
    return out


def chance_spawns(items, ctx, page_urls):
    pools, seen_all = [], {}
    lists = mixed_lists(items, ctx)
    for i, (lv, foods) in enumerate(lists, 1):
        refrs = {rf: {"edid": e, "dedicated": False, "via": lv}
                 for rf, e, s in ctx["tables"]["lvli_refs"].get(lv, ()) if s == "REFR"}
        seen, _n = ebuild.resolve_placements(
            {"lvli_closure": {lv}, "placed_bases": {}, "direct_refrs": refrs},
            ctx["geo"], ctx["cur"], ctx["cache"], ctx["db_ok"])
        seen_all.update(seen)
        contents = []
        for it in foods:
            p = ctx["rates"].appearance([lv], set(it["form_ids"]))
            if p <= 0:
                continue
            row = collections.OrderedDict([("name", it["name"]), ("rate", round(p, 6)),
                                           ("rate_display", B._fmt_rate(p))])
            url = next((page_urls[f] for f in it["form_ids"] if f in page_urls), "")
            if url:
                row["page_url"] = url
            contents.append(row)
        contents.sort(key=lambda r: (-r["rate"], r["name"].lower()))
        block = ebuild.group_chance(seen, ALL_REGIONS)
        pools.append(collections.OrderedDict([
            ("name", "Mixed pre-war food spawn" + (f" {i}" if len(lists) > 1 else "")),
            ("list_id", lv),
            ("total_markers", block["total_markers"]),
            ("total", block["total"]),
            ("contents", contents),
        ]))
        print(f"    mixed list {ctx['tables']['parent_edid'].get(lv, lv)}: "
              f"{block['total']} points at {block['total_markers']} locations, "
              f"{len(contents)} foods")
    cs = ebuild.group_chance(seen_all, ALL_REGIONS)
    out = collections.OrderedDict()
    out["pools"] = pools
    out["regions"] = cs["regions"]
    out["total_markers"] = cs["total_markers"]
    out["total"] = cs["total"]
    out["show_maps"] = True
    out["map_base"] = MAP_BASE
    if cs["total_markers"]:
        out["lead"] = (f"These {cs['total_markers']} locations have mixed pre-war food "
                       f"spots. A spot can give one food from the list below, or nothing. "
                       f"The table shows the chance per spot. The locations are listed by "
                       f"name only — open a region map to see where they are.")
    return out


# ── Merged page-level sections (Oct 2026) ───────────────────────────────────
# One sub-expand per food in Used For / Farming Tips was 41 near-identical blocks.
# The data says why: apart from weight and Can Do!, every food's tips are the same,
# and most foods have no recipe and no challenge of their own. So the page gets ONE
# Farming Tips block (weights grouped by base weight) and ONE Used For (an effects
# table, the shared "Pre-war Food" challenges, the food-specific ones, and every
# recipe that uses a pre-war food).
def _weight_row(w, foods):
    return collections.OrderedDict([
        ("base", w), ("foods", foods),
        ("thru_hiker", [r2(w * 0.55), r2(w * 0.10)]),
        ("grocers", r2(w * 0.10)),
        ("armour", [r2(w * 0.80), r2(w * 0.60), r2(w * 0.40), r2(w * 0.20), r2(w * 0.10)]),
    ])


def merged_sections(items, subs, ctx, page_urls):
    kws = prewar_keywords(ctx["tsv"])
    shared, shared_keys = [], set()
    for fid in kws.values():
        for c in (B.build_challenges(fid, ctx["dist"]) if fid else []):
            k = c.get("edid") or c.get("name")
            if k not in shared_keys:
                shared_keys.add(k)
                shared.append(c)

    rows, food_chal, recipes, rec_keys = [], [], [], set()
    by_weight = collections.OrderedDict()
    can_do = []
    for it, s in zip(items, subs):
        uf = s.get("used_for") or {}
        url = s.get("page_url") or ""
        cons = uf.get("consumption") or B.build_consumption(it["form_ids"][0], ctx["tsv"], it["full"])
        if uf:
            chal = uf.get("challenges") or []
            recs = uf.get("recipes") or []
        else:                                   # food with its own guide — still list it here
            chal = [c for f in it["form_ids"] for c in B.build_challenges(f, ctx["dist"])]
            recs = B.build_recipes(it["full"], ctx["recipe_guide"], ctx["bench_cat"],
                                   ctx["tsv"], ctx["guide_urls"])
        row = collections.OrderedDict([("name", it["name"])])
        if url:
            row["page_url"] = url
        row["effects"] = [{"display": e.get("display"), "duration": e.get("duration")}
                          for e in (cons or {}).get("effects") or [] if e.get("display")]
        row["weight"] = (cons or {}).get("weight", it["weight"])
        row["value"] = (cons or {}).get("value")
        rows.append(row)
        seen = set()
        for c in chal:
            k = c.get("edid") or c.get("name")
            if k in shared_keys or k in seen:
                continue
            seen.add(k)
            food_chal.append(collections.OrderedDict([
                ("food", it["name"]), ("type", c.get("type")), ("name", c.get("name")),
                ("required", c.get("required"))]))
        for r in recs:
            k = (r.get("name"), r.get("workbench"))
            if k not in rec_keys:
                rec_keys.add(k)
                recipes.append(r)
        w = row["weight"] if row["weight"] is not None else 0
        by_weight.setdefault(w, []).append(it["name"])
        if set(it["form_ids"]) & ctx["can_do"]:
            can_do.append(it["name"])

    recipes.sort(key=lambda r: (r.get("name") or "").lower())
    food_chal.sort(key=lambda c: (c["food"].lower(), c.get("type") or "", c.get("name") or ""))
    tips = collections.OrderedDict([
        ("merged", True),
        ("spoils", False),
        ("object_type", "Food"),
        ("perk_cards", perk_ranks.cards(["thru_hiker", "can_do"], ctx["channel"])),
        ("can_do_foods", can_do),
        ("weights", [_weight_row(w, by_weight[w]) for w in sorted(by_weight)]),
    ])
    used_for = collections.OrderedDict([
        ("consumption_table", rows),
        ("challenges", shared),
        ("food_challenges", food_chal),
        ("recipes", recipes),
        ("modifiers", ctx["modifiers"]),
    ])
    return tips, used_for


# ── The page ────────────────────────────────────────────────────────────────
def _load_state(path):
    try:
        return json.load(open(path, encoding="utf-8"), object_pairs_hook=collections.OrderedDict)
    except Exception:
        return {}


def build(ctx, state_path=None, budget=None):
    """`state_path` + `budget` (seconds) let a slow local run be done in chunks:
    each food's sections are saved to the state file as they finish, and a run
    that hits the budget stops and exits 3 so it can simply be run again. CI
    passes neither and builds the lot in one go."""
    t0 = time.time()
    out_path = os.path.join(ctx["out_dir"], f"{SLUG}_spawns.json")
    keep_all = load_keep(out_path)
    items = discover_items(ctx["tsv"])
    page_urls = own_pages()
    state = _load_state(state_path) if state_path else {}

    fixed, subs = [], []
    for it in items:
        url = next((page_urls[f] for f in it["form_ids"] if f in page_urls), "")
        regions, n = fixed_spawns(it, ctx, keep_all.get(it["key"], {}))
        base = MAP_BASE + it["key"] + "/"
        if n:
            fi = collections.OrderedDict([
                ("name", it["name"]), ("key", it["key"]), ("form_ids", it["form_ids"]),
                ("total", n)])
            if url:
                fi["page_url"] = url
            fi["map_base"] = base
            fi["full_map"] = base + it["key"] + ".jpg"
            fi["regions"] = regions
            fixed.append(fi)
        if url:
            subs.append(collections.OrderedDict([
                ("name", it["name"]), ("form_ids", it["form_ids"]), ("page_url", url)]))
        else:
            sig = ",".join(it["form_ids"]) + f"|{bool(n)}|{STATE_VERSION}"
            hit = state.get(it["key"])
            if hit and hit.get("sig") == sig:
                subs.append(hit["doc"])
            else:
                if budget and time.time() - t0 > budget:
                    print(f"[{SLUG}] time budget reached — run again to carry on "
                          f"({len(state)} foods saved in {state_path}).")
                    return None
                sub = item_doc(it, ctx, bool(n))
                subs.append(sub)
                if state_path:
                    state[it["key"]] = {"sig": sig, "doc": sub}
                    with open(state_path, "w", encoding="utf-8") as fh:
                        json.dump(state, fh, ensure_ascii=False)
        print(f"  {it['name']:<34} fixed:{n:>4}" + (f"  -> {url}" if url else ""))

    # Every pre-war food at once: the chance a container / vendor / event / creature /
    # treasure map gives ANY pre-war food, and one collectron / generator card per
    # station listing every food it makes. This is the main list in each of those
    # sections; the per-food detail sits under it in one "By food" sub-expand.
    all_item = {"name": "Pre-war food", "full": "Pre-war food", "key": "__all__",
                "form_ids": [f for it in items for f in it["form_ids"]],
                "edids": [e for it in items for e in it["edids"]], "weight": None, "kw": ""}
    sig = ",".join(sorted(all_item["form_ids"])) + f"|{STATE_VERSION}"
    hit = state.get("__all__")
    if hit and hit.get("sig") == sig:
        combined = hit["doc"]
    else:
        if budget and time.time() - t0 > budget:
            print(f"[{SLUG}] time budget reached — run again to carry on.")
            return None
        combined = item_doc(all_item, ctx, True)
        if state_path:
            state["__all__"] = {"sig": sig, "doc": combined}
            with open(state_path, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False)
    combined = collections.OrderedDict((k, v) for k, v in combined.items()
                                       if k not in ("used_for", "farming_tips", "form_ids"))
    combined["name"] = "Pre-war food"
    subs_and_all = subs + [combined]

    # The treasure-map `sources` audit trail is never rendered, and across ~40 foods
    # it was the biggest thing in the file. Drop it here (after the state cache, so
    # a cached food is trimmed too).
    for s in subs_and_all:
        for m in (s.get("treasure_maps") or {}).get("maps") or []:
            m.pop("sources", None)
        # same container filter as item_doc(), applied here too so a food read
        # back from a --state file made before the filter is cleaned as well
        cont = (s.get("drop_rates") or {}).get("containers")
        if isinstance(cont, dict) and isinstance(cont.get("types"), list):
            cont["types"] = [t for t in cont["types"]
                             if not re.search(r"(?i)\bcounts items\b", t.get("name") or "")]

    farming_tips_page, used_for_page = merged_sections(items, subs, ctx, page_urls)
    # The per-food Used For / Farming Tips now live in the merged page blocks.
    for s in subs:
        s.pop("used_for", None)
        s.pop("farming_tips", None)

    doc = collections.OrderedDict()
    doc["_meta"] = {"generated": datetime.date.today().isoformat(),
                    "source": "CHAL 'Pre-war Food' keyword -> ALCH; LVLI + Mappalachia "
                              "Position (cached for CI) — src/build_pre_war_food_guide.py"}
    doc["set"] = SLUG
    doc["slug"] = SLUG
    doc["name"] = NAME
    doc["page_title"] = "Pre-War Food Location Guide"
    doc["blurb"] = ("Every pre-war food in Fallout 76: where each one spawns, what it "
                    "does, and where else you can get it.")
    doc["map_base"] = MAP_BASE
    doc["map_ext"] = ".jpg"
    # Renderer: one shared Used For + Farming Tips, and the other sections group
    # foods whose content is identical into one sub-expand.
    doc["merge_sections"] = True
    doc["farming_tips"] = farming_tips_page
    doc["used_for"] = used_for_page
    doc["combined"] = combined
    doc["items"] = [collections.OrderedDict(
        [("name", it["name"]), ("form_ids", it["form_ids"])]
        + ([("page_url", s["page_url"])] if s.get("page_url") else []))
        for it, s in zip(items, subs)]
    doc["sub_items"] = subs
    doc["fixed_items"] = fixed
    doc["regions"] = [{"region": r, "locations": []} for r in ALL_REGIONS]
    doc["chance_spawns"] = chance_spawns(items, ctx, page_urls)

    os.makedirs(ctx["out_dir"], exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=1)
    total = sum(f["total"] for f in fixed)
    print(f"[{SLUG}] {len(items)} foods, {len(fixed)} with fixed spawns ({total} points), "
          f"{doc['chance_spawns']['total']} mixed-list points -> {os.path.relpath(out_path, REPO)}")
    return out_path


def load_ctx(pts):
    channel = "pts" if pts else "live"
    dist = os.path.join(REPO, "dist", "pts") if pts else os.path.join(REPO, "dist")
    tsv = os.path.join(REPO, "tsv", "pts") if pts else os.path.join(REPO, "tsv")
    data = rng76.Rng76Data.from_tsv_root(tsv)

    def dist_file(name):
        p = os.path.join(dist, name)
        return p if os.path.exists(p) else os.path.join(REPO, "dist", name)

    tables = esources.load_tables(tsv)
    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = cur = None
    if db_ok:
        from spawns_engine.geo import Geo
        geo = Geo(MAPPALACHIA_DB)
        cur = sqlite3.connect(MAPPALACHIA_DB).cursor()
    cache = ebuild.load_cache(GEO_CACHE)
    if not db_ok and not cache:
        sys.exit(f"[{SLUG}] No Mappalachia DB and no geo cache — run once locally with "
                 f"MAPPALACHIA_DB set to seed {os.path.relpath(GEO_CACHE, REPO)}.")
    print(f"[{SLUG}] {'Mappalachia DB found' if db_ok else 'no DB — using the committed geo cache'}"
          f" ({channel})")
    return {
        "channel": channel, "dist": dist, "tsv": tsv,
        "out_dir": os.path.join(dist, "farming_spawns"),
        "recipe_guide": json.load(open(dist_file("recipe_guide.json"), encoding="utf-8")),
        "bench_cat": B._bench_category_map(dist_file("cobj-recipes.json")),
        "guide_urls": B._load_guide_urls(),
        "modifiers": B.build_modifiers(tsv),
        "tables": tables,
        "classify": make_farming_classify(nests=False),
        "rates": B.VendorRates(data),
        "cont_names": B._load_cont_names(tsv),
        "cont_bases": B.live_cont_bases(tsv),
        "vendors": B._load_vendor_master(dist),
        "can_do": can_do_items(tables),
        "db_ok": db_ok, "geo": geo, "cur": cur, "cache": cache,
    }


def _arg(argv, name):
    if name in argv:
        i = argv.index(name)
        return argv[i + 1] if i + 1 < len(argv) else None
    return None


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    state_path = _arg(argv, "--state")
    budget = _arg(argv, "--budget-seconds")
    ctx = load_ctx("--pts" in argv)
    done = build(ctx, state_path, float(budget) if budget else None)
    if ctx["db_ok"]:
        ebuild.save_cache(ctx["cache"], GEO_CACHE)
        print(f"[{SLUG}] geo cache saved ({len(ctx['cache'])} placements) for DB-free CI rebuilds.")
    if done is None:
        sys.exit(3)


if __name__ == "__main__":
    main()
