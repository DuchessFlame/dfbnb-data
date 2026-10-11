#!/usr/bin/env python3
r"""
build_pre_war_food_guide.py — one location guide PER pre-war food (Oct 2026).

    /bnb/farming/non-perishable/<food>/<food>-guide/

Until 11 Oct 2026 every pre-war food sat on one page
(/bnb/farming/non-perishable/pre-war-food-location-guide/). Duchess did not like
it, so each food now gets its own page, drawn by the Deathclaw Egg renderer
(df-bnb-farming-non-perishable-guide.js), the baseline for every farming page.
The URL shape is the normal non-perishable one (same as Canned Coffee), so the
renderer finds <food>_spawns.json with no special case.

ONE PAGE PER FOOD — the only sharing rule
  A rad-free copy ("<Food> (no rads)", the *_PreWar_Clean records) goes on the
  same page as its food. Those pages use the multi-item shape (sub_items[] +
  fixed_items[], one sub-expand per version). Every other page is a plain
  single-item farming doc.
  A food that already has its own guide (Canned Coffee, Sugar Bombs, ...) gets
  no page here; nor does its "(no rads)" copy (it belongs on that page).

  out: dist/farming_spawns/<food>_spawns.json            (default = live)
       dist/pts/farming_spawns/<food>_spawns.json         (--pts)
       dist/farming_spawns/pre_war_food_pages.json        the page list (slug,
                                                          name, url, foods) —
                                                          read by the map script
  geo: data/farming_spawns/geo_cache_pre_war_food.json   (seeded locally with
       the Mappalachia DB; CI rebuilds from it with no DB)

MAPS (render_pre_war_food_maps.py)
  site:  guide-images/farming-non-perishable/<food>/            fixed-spawn maps
         guide-images/farming-non-perishable/<food>/no-rads/    the (no rads) copy
         guide-images/farming-non-perishable/pre-war-food/      the mixed-list
                                                                chance maps — ONE
                                                                set, shared by
                                                                every page
  Docs carry `own_map_renderer`, so add_spawn_map_base.py and render_all_maps.py
  leave them alone.

WHICH ITEMS — Bethesda's own list, nothing typed
  The "Eat Pre-war Food" / "Collect Pre-war Food" challenges (CHAL) test
  `HasKeyword <kw> "Pre-war Food"`. That keyword (MealTypePackaged [0013485D] as
  of Oct 2026) is read off the CHAL export, and every named ALCH carrying it is a
  pre-war food. A new pre-war food joins the page on the next build with no edit.
  Records with the same name AND the same effects (the Lucky Strike copies of Gum
  Drops / Cotton Candy Bites) are one food on the page. Same name but different
  effects (the *_PreWar_Clean copies have no radiation) are shown apart, the
  rad-free one marked "(no rads)".

PAGE SHAPE
  Single food: the normal farming doc — used_for, farming_tips, drop_rates,
                vendor_list, events_activities, treasure_maps, regions (fixed
                spawns: the item's own placed base, or a leveled list that can
                ONLY hand out that food — spawn-guide 9k dedication rule).
  Food + (no rads): sub_items[] (one full farming doc per version, rendered as a
                sub-expand in every section) and fixed_items[] (one Fixed Spawn
                sub-expand per version, each with its own map folder).
  chance_spawns (both shapes) the MIXED pre-war food lists — a world-placed
                leveled list whose every leaf is a pre-war food but which can roll
                more than one (LPI_Food_Packaged as of Oct 2026). `pools[].contents`
                holds only THIS page's food(s), with the chance per spot (rng76).
                Names only by region, with the shared chance maps (show_maps).

Hand-authored photos / directions are kept across rebuilds by food + ref.

  python src/build_pre_war_food_guide.py            # live
  python src/build_pre_war_food_guide.py --pts      # PTS

A full run resolves ~40 foods through rng76 and takes about half an hour. To do a
local run in chunks (each chunk under a time limit):
  python src/build_pre_war_food_guide.py --state pwf_state.json --budget-seconds 120
  ...repeat until it stops exiting with code 3.
The state file only holds the slow part (the source sections); Used For and
Farming Tips are worked out fresh every run.
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
UPLOADS = "/wp-content/uploads/guide-images/farming-non-perishable/"
# Each page's fixed-spawn maps: UPLOADS<page-slug>/ (the "(no rads)" copy in
# its no-rads/ sub-folder). The mixed-list chance maps are the same for every
# page, so there is ONE set, in the old pre-war-food folder.
CHANCE_MAP_BASE = UPLOADS + "pre-war-food/"
NO_RADS = " (no rads)"
NO_RADS_DIR = "no-rads/"
PAGE_URL = "/bnb/farming/non-perishable/{slug}/{slug}-guide/"
MANIFEST = "pre_war_food_pages.json"
# Fallback only — used if the CHAL export carries no "Pre-war Food" keyword test.
FALLBACK_KEYWORD = "MealTypePackaged"
FIXED_TYPES = ("direct", "static")
# Bump when item_doc() changes, so a saved --state file is not reused.
# 3 (11 Oct 2026): the state holds item_doc() WITHOUT used_for / farming_tips.
STATE_VERSION = 3
RAD_EFFECT = "DamageRadiationEating"


def r2(x):
    return round(x, 2)


def slugify(s):
    # apostrophes just go ("Rudy's" -> "rudys", not "rudy-s")
    s = re.sub(r"['\u2019]", "", str(s or "").lower())
    return re.sub(r"-+$", "", re.sub(r"[^a-z0-9]+", "-", s).lstrip("-"))


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
def shared_challenges(ctx):
    """The challenges that count EVERY pre-war food ("Eat Pre-war Food" ...),
    matched by the keyword FormID in challenges.json."""
    if "_shared_chal" not in ctx:
        out, keys = [], set()
        for fid in prewar_keywords(ctx["tsv"]).values():
            for c in (B.build_challenges(fid, ctx["dist"]) if fid else []):
                k = c.get("edid") or c.get("name")
                if k not in keys:
                    keys.add(k)
                    out.append(c)
        ctx["_shared_chal"] = out
    return ctx["_shared_chal"]


def uses_and_tips(item, ctx, has_fixed):
    """(used_for, farming_tips) for one food. Cheap, so never cached in --state."""
    fids = item["form_ids"]
    cons = B.build_consumption(fids[0], ctx["tsv"], item["full"])
    if cons:
        # Mystery Candy / Nuka-Cola Candy: the effect casts ONE spell at random.
        effs = ctx.setdefault("_alch_effects", _effects(ctx["tsv"]))
        rb = random_spell_buffs([e[0] for e in effs.get(fids[0], [])], ctx["tsv"])
        if rb:
            cons["random_buffs"] = rb
            # the script effect's own line is just its name ("Mystery Treat")
            cons["effects"] = [e for e in cons.get("effects") or []
                               if re.search(r"\d", e.get("display") or "")]
    chal, seen = [], set()
    for c in shared_challenges(ctx) + [c for f in fids for c in B.build_challenges(f, ctx["dist"])]:
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
    tips = farming_tips(item, cons, bool(set(fids) & ctx["can_do"]), has_fixed, ctx["channel"])
    return used_for, tips


def item_doc(item, ctx, has_fixed):
    """The slow part of a food's doc — every source section (rng76). Used For and
    Farming Tips are added by the caller (uses_and_tips), so a --state file can
    hold this and still get fresh recipes / challenges."""
    fids = item["form_ids"]
    targets = set(fids)
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
    doc["drop_rates"] = collections.OrderedDict([
        ("creatures", None), ("collectrons", None), ("resource_generators", None)])
    B._patch_camp_producers(doc, targets, ctx["dist"], ctx["rates"], data_dir=ctx["tsv"])
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
def _keep_regions(regions):
    keep = {}
    for reg in regions or []:
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
    return keep


def load_keep(paths):
    """{food key: {(region, marker): {...marker slots, 'spawns': {ref: slots}}}}

    Reads every page doc from the last build, plus the old all-in-one
    pre-war-food_spawns.json, so photos / directions follow a food to its page.
    A page's own doc wins over the old one."""
    out = {}
    for path in paths:
        try:
            old = json.load(open(path, encoding="utf-8"))
        except Exception:
            continue
        found = {}
        for fi in old.get("fixed_items") or []:
            found[fi.get("key")] = _keep_regions(fi.get("regions"))
        if old.get("food_key") and not old.get("fixed_items"):
            found[old["food_key"]] = _keep_regions(old.get("regions"))
        for k, v in found.items():
            if k and k not in out:          # paths come page docs first
                out[k] = v
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


def mixed_data(items, ctx, page_urls):
    """Every mixed list once: [{lv, seen, rates: {food name: (p, url)}}]."""
    out = []
    for lv, foods in mixed_lists(items, ctx):
        refrs = {rf: {"edid": e, "dedicated": False, "via": lv}
                 for rf, e, s in ctx["tables"]["lvli_refs"].get(lv, ()) if s == "REFR"}
        seen, _n = ebuild.resolve_placements(
            {"lvli_closure": {lv}, "placed_bases": {}, "direct_refrs": refrs},
            ctx["geo"], ctx["cur"], ctx["cache"], ctx["db_ok"])
        rates = collections.OrderedDict()
        for it in foods:
            p = ctx["rates"].appearance([lv], set(it["form_ids"]))
            if p > 0:
                url = next((page_urls[f] for f in it["form_ids"] if f in page_urls), "")
                rates[it["name"]] = (p, url)
        out.append({"lv": lv, "seen": seen, "rates": rates})
        print(f"    mixed list {ctx['tables']['parent_edid'].get(lv, lv)}: "
              f"{len(seen)} points, {len(rates)} foods")
    return out


def chance_spawns(page_items, mixed):
    """Chance to Spawn for ONE page: the mixed lists that can give this page's
    food(s), with only those foods in the table. The region maps are the shared
    set in CHANCE_MAP_BASE (as of Oct 2026 there is one mixed list, so every
    page's chance spots are the same spots)."""
    names = [it["name"] for it in page_items]
    use = [m for m in mixed if any(n in m["rates"] for n in names)]
    pools, seen_all = [], {}
    for i, m in enumerate(use, 1):
        seen_all.update(m["seen"])
        block = ebuild.group_chance(m["seen"], ALL_REGIONS)
        contents = []
        for n in names:
            if n in m["rates"]:
                p, _url = m["rates"][n]
                contents.append(collections.OrderedDict([
                    ("name", n), ("rate", round(p, 6)), ("rate_display", B._fmt_rate(p))]))
        pools.append(collections.OrderedDict([
            ("name", "Mixed pre-war food spawn" + (f" {i}" if len(use) > 1 else "")),
            ("list_id", m["lv"]),
            ("total_markers", block["total_markers"]),
            ("total", block["total"]),
            ("contents", contents),
        ]))
    cs = ebuild.group_chance(seen_all, ALL_REGIONS)
    out = collections.OrderedDict()
    out["pools"] = pools
    out["regions"] = cs["regions"]
    out["total_markers"] = cs["total_markers"]
    out["total"] = cs["total"]
    out["show_maps"] = True
    out["map_base"] = CHANCE_MAP_BASE
    if cs["total_markers"]:
        what = " or ".join(names)
        out["lead"] = (f"These {cs['total_markers']} locations have mixed pre-war food "
                       f"spots. Each spot gives one pre-war food, or nothing. The table "
                       f"shows the chance per spot that it is {what}. The locations are "
                       f"listed by name only — open a region map to see where they are.")
    return out



# ── Script effects that cast a random spell (Mystery Candy) ─────────────────
# Spooky_MysteryTreat's only effect is HalloweenCandyRandomEffect ("Mystery Treat"),
# a SCRIPT effect: its Papyrus script casts one of several SPELs. Which spells is a
# script property, so it is read from the MGEF export's VMAD_Scripts column
# (!!!Wordpress - ExportMGEFToTSV.pas, Oct 2026). Until the MGEF export is re-run
# with that column, fall back to the spells named after the effect
# (HalloweenCandyRandomEffect -> HalloweenCandy_*). The pick is made in the script,
# so the game files carry no odds — the page lists the outcomes, no percentages.
_REF_RE = re.compile(r"([0-9A-Fa-f]{8}):([^:|#]*):SPEL")


def _spell_tables(tsv):
    head = tsv_source.newest(os.path.join(tsv, "SPEL_Export_*_HEADER.tsv"), required=False)
    eff = tsv_source.newest(os.path.join(tsv, "SPEL_Export_*_EFFECTS.tsv"), required=False)
    spells = collections.OrderedDict()
    for r in (B._read_tsv(head) if head else []):
        spells[(r.get("SPEL_FormID") or "").upper()] = {
            "edid": r.get("SPEL_EDID") or "", "name": (r.get("SPEL_FULL") or "").strip(),
            "effects": []}
    for r in (B._read_tsv(eff) if eff else []):
        sp = spells.get((r.get("SPEL_FormID") or "").upper())
        if sp is not None:
            sp["effects"].append((r.get("EFID_MGEF_EDID") or "", (r.get("EFID_MGEF_FULL") or "").strip(),
                                  B._safe_num(r.get("EFIT_Magnitude")) or 0,
                                  B._safe_num(r.get("EFIT_Duration")) or 0))
    return spells


def _mgef_vmad(tsv):
    path = tsv_source.newest(os.path.join(tsv, "MGEF_Export_*.tsv"), required=False)
    out = {}
    for r in (B._read_tsv(path) if path else []):
        v = r.get("VMAD_Scripts")
        if v is None:
            return None                    # export predates the VMAD column
        out[(r.get("EDID") or "")] = v
    return out


def _effect_text(full, mag, dur):
    label = re.sub(r"^(Fortify|Restore)\s+", "", full)
    label = re.sub(r"\s+Food$", "", label)
    m = re.match(r"^(Reduce|Damage)\s+(.+)$", label)
    txt = label
    if mag:
        txt = (f"-{mag:g} {m.group(2)}" if m else f"+{mag:g} {label}")
    t = B._fmt_minutes(dur) if dur and dur >= 60 else (f"{dur:g}s" if dur and mag else None)
    return txt + (f" ({t})" if t else "")


def random_spell_buffs(mgef_edids, tsv, cache={}):
    """[{name, effects:[text]}] for a food whose effect casts a random spell."""
    if "spells" not in cache:
        cache["spells"] = _spell_tables(tsv)
        cache["vmad"] = _mgef_vmad(tsv)
    spells, vmad = cache["spells"], cache["vmad"]
    picked = []
    for ed in mgef_edids:
        if vmad is not None and vmad.get(ed):
            fids = [m.group(1).upper() for m in _REF_RE.finditer(vmad[ed])]
            picked += [spells[f] for f in fids if f in spells]
        elif vmad is None and ed.endswith("RandomEffect"):
            stem = ed[:-len("RandomEffect")].lower() + "_"
            picked += [sp for sp in spells.values() if sp["edid"].lower().startswith(stem)]
    out, seen = [], set()
    for sp in picked:
        # a spell made only of a "Duration" helper (the blackout timer) is not an outcome
        if not sp["effects"] or all("duration" in e[0].lower() for e in sp["effects"]):
            continue
        if sp["edid"] in seen:
            continue
        seen.add(sp["edid"])
        out.append(collections.OrderedDict([
            ("name", sp["name"]),
            ("effects", [_effect_text(e[1], e[2], e[3]) for e in sp["effects"]])]))
    return out

# ── The pages ───────────────────────────────────────────────────────────────
def _load_state(path):
    try:
        return json.load(open(path, encoding="utf-8"), object_pairs_hook=collections.OrderedDict)
    except Exception:
        return {}


def base_name(name):
    return name[:-len(NO_RADS)] if name.endswith(NO_RADS) else name


def page_groups(items, page_urls):
    """-> ([(page slug, page name, [item, ...])], [skipped item names])

    One page per food. A "(no rads)" copy joins its food's page. When the food
    (or its copy) already has its own guide, neither gets a page here."""
    groups = collections.OrderedDict()
    for it in items:
        groups.setdefault(base_name(it["name"]).lower(), []).append(it)
    pages, skipped = [], []
    for foods in groups.values():
        foods.sort(key=lambda it: (it["name"].endswith(NO_RADS), it["name"].lower()))
        if any(f in page_urls for it in foods for f in it["form_ids"]):
            skipped += [it["name"] for it in foods
                        if not any(f in page_urls for f in it["form_ids"])]
            continue
        name = base_name(foods[0]["name"])
        pages.append((slugify(name), name, foods))
    return pages, skipped


def _clean_sub(s):
    """Trim a food's source sections (also for docs read back from --state)."""
    for m in (s.get("treasure_maps") or {}).get("maps") or []:
        m.pop("sources", None)          # audit trail, never rendered
    cont = (s.get("drop_rates") or {}).get("containers")
    if isinstance(cont, dict) and isinstance(cont.get("types"), list):
        cont["types"] = [t for t in cont["types"]
                         if not re.match(r"(?i)^(test|qa|debug)\b", t.get("name") or "")
                         and not re.search(r"(?i)\bcounts items\b", t.get("name") or "")]
    return s


def _rejoin_producers(s, fids, ctx, page_name):
    """Collectron / generator cards are cheap, so re-joined every build instead of
    read back from a --state file (toggle notes / workshop names came later)."""
    if not fids or not isinstance(s.get("drop_rates"), dict):
        return
    for k in ("collectrons", "resource_generators"):
        node = s["drop_rates"].get(k)
        if isinstance(node, dict):
            node.pop("entries", None)
    B._patch_camp_producers(s, set(fids), ctx["dist"], ctx["rates"], data_dir=ctx["tsv"])
    # card rows use the in-game FULL name; use the page's name so a rad-free copy
    # reads "Salisbury Steak (no rads)", matching the rest of the page
    for k in ("collectrons", "resource_generators"):
        for e in ((s["drop_rates"].get(k) or {}).get("entries") or []):
            for row in e.get("items") or []:
                row["name"] = page_name.get(row.get("form_id"), row.get("name"))


def build(ctx, state_path=None, budget=None):
    """`state_path` + `budget` (seconds) let a slow local run be done in chunks:
    each food's source sections are saved to the state file as they finish, and a
    run that hits the budget stops and exits 3 so it can simply be run again. CI
    passes neither and builds the lot in one go."""
    t0 = time.time()
    out_dir = ctx["out_dir"]
    items = discover_items(ctx["tsv"])
    page_urls = own_pages()
    pages, skipped = page_groups(items, page_urls)
    old_paths = [os.path.join(out_dir, f"{slug}_spawns.json") for slug, _n, _f in pages]
    keep_all = load_keep(old_paths + [os.path.join(out_dir, f"{SLUG}_spawns.json")])
    state = _load_state(state_path) if state_path else {}
    page_name = {f: it["name"] for it in items for f in it["form_ids"]}

    # Pass 1 — every food's fixed spawns and source sections (the slow part).
    food = {}
    for slug, _name, foods in pages:
        for it in foods:
            regions, n = fixed_spawns(it, ctx, keep_all.get(it["key"], {}))
            sig = ",".join(it["form_ids"]) + f"|{bool(n)}|{STATE_VERSION}"
            hit = state.get(it["key"])
            if hit and hit.get("sig") == sig:
                sub = hit["doc"]
            else:
                if budget and time.time() - t0 > budget:
                    print(f"[{SLUG}] time budget reached — run again to carry on "
                          f"({len(state)} foods saved in {state_path}).")
                    return None
                sub = item_doc(it, ctx, bool(n))
                if state_path:
                    state[it["key"]] = {"sig": sig, "doc": sub}
                    with open(state_path, "w", encoding="utf-8") as fh:
                        json.dump(state, fh, ensure_ascii=False)
            sub = _clean_sub(collections.OrderedDict(sub))
            _rejoin_producers(sub, it["form_ids"], ctx, page_name)
            used_for, tips = uses_and_tips(it, ctx, bool(n))
            food[it["key"]] = (sub, used_for, tips, regions, n)
            print(f"  {it['name']:<38} fixed:{n:>4}  -> {slug}")

    mixed = mixed_data(items, ctx, page_urls)
    os.makedirs(out_dir, exist_ok=True)
    meta = {"generated": datetime.date.today().isoformat(),
            "source": "CHAL 'Pre-war Food' keyword -> ALCH; LVLI + Mappalachia "
                      "Position (cached for CI) — src/build_pre_war_food_guide.py"}
    manifest = []
    for slug, name, foods in pages:
        base = UPLOADS + slug + "/"
        doc = collections.OrderedDict()
        doc["_meta"] = meta
        doc["set"] = slug
        doc["slug"] = slug
        doc["name"] = name
        doc["page_title"] = f"{name} Location Guide"
        if len(foods) == 1:
            it = foods[0]
            sub, used_for, tips, regions, n = food[it["key"]]
            doc["blurb"] = (f"Every {name} spawn in Fallout 76: where it spawns, what it "
                            f"does, and where else you can get it.")
            doc["food_key"] = it["key"]
            doc["form_ids"] = it["form_ids"]
            doc["used_for"] = used_for
            if tips:
                doc["farming_tips"] = tips
            for k, v in sub.items():
                if k not in ("name", "form_ids"):
                    doc[k] = v
            doc["regions"] = regions
            doc["total"] = n
        else:
            doc["blurb"] = (f"Every {name} spawn in Fallout 76, with and without "
                            f"radiation: where each spawns, what it does, and where "
                            f"else you can get it.")
            subs, fixed = [], []
            for it in foods:
                sub, used_for, tips, regions, n = food[it["key"]]
                s = collections.OrderedDict([("name", it["name"]), ("form_ids", it["form_ids"]),
                                             ("used_for", used_for)])
                if tips:
                    s["farming_tips"] = tips
                for k, v in sub.items():
                    if k not in ("name", "form_ids"):
                        s[k] = v
                subs.append(s)
                if n:
                    fbase = base + (NO_RADS_DIR if it["name"].endswith(NO_RADS) else "")
                    fixed.append(collections.OrderedDict([
                        ("name", it["name"]), ("key", it["key"]), ("form_ids", it["form_ids"]),
                        ("total", n), ("map_base", fbase),
                        ("full_map", fbase + it["key"] + ".jpg"), ("regions", regions)]))
            doc["items"] = [collections.OrderedDict(
                [("name", it["name"]), ("form_ids", it["form_ids"])]) for it in foods]
            doc["sub_items"] = subs
            doc["fixed_items"] = fixed
            doc["regions"] = [{"region": r, "locations": []} for r in ALL_REGIONS]
        doc["map_base"] = base
        doc["map_ext"] = ".jpg"
        if len(foods) == 1 and doc["total"]:
            doc["full_map"] = base + foods[0]["key"] + ".jpg"
        # render_pre_war_food_maps.py draws these; add_spawn_map_base.py and
        # render_all_maps.py skip any doc that names its own renderer.
        doc["own_map_renderer"] = "render_pre_war_food_maps.py"
        doc["chance_spawns"] = chance_spawns(foods, mixed)
        path = os.path.join(out_dir, f"{slug}_spawns.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=1)
        manifest.append(collections.OrderedDict([
            ("slug", slug), ("name", name), ("url", PAGE_URL.format(slug=slug)),
            ("foods", [collections.OrderedDict([
                ("name", it["name"]), ("key", it["key"]), ("form_ids", it["form_ids"]),
                ("fixed", food[it["key"]][4]),
                ("map_base", base + (NO_RADS_DIR if len(foods) > 1
                                     and it["name"].endswith(NO_RADS) else ""))])
                for it in foods])]))

    with open(os.path.join(out_dir, MANIFEST), "w", encoding="utf-8") as fh:
        json.dump({"_meta": meta, "chance_map_base": CHANCE_MAP_BASE, "pages": manifest},
                  fh, ensure_ascii=False, indent=1)
    if skipped:
        print(f"  no page (their food already has its own guide): {', '.join(skipped)}")
    print(f"[{SLUG}] {len(items)} foods -> {len(pages)} pages "
          f"({sum(1 for p in pages if len(p[2]) > 1)} with a no-rads copy) in "
          f"{os.path.relpath(out_dir, REPO)}")
    return out_dir


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
