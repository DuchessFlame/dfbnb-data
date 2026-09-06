#!/usr/bin/env python3
r"""
spawns_configs.meat — the Farming MEAT family driver (BNB brand, /bnb/farming/meat/).

One creature-seeded page per raw meat, exactly like the cryptid pages: the page is
seeded by the CREATURE that drops the meat, and shows where that creature spawns, its
Drops (incl. the meat with rng76 rates), Used For, Activities/Events/Quests, Random
Encounters and Fixed Spawn Locations (spawn-guide §9k). It reuses
spawns_configs.cryptids.compute_bundle verbatim — same engine, same rules — so there is
no parallel pipeline. The Angler page reuses that same bundle logic (the doc built for
its future Meat home).

Meat = ALCH items keyworded MealTypeRaw + IngredientTypeMeat that actually appear in a
creature's death-drop list (LLD_Creature_*); no hardcoded FormIDs for routing — each
page selects its creature by a per-page regex on the death-list EDID, and the meat items
are tagged by keyword.

Two special pages:
  * Glowing Meat — per game data ONLY the Rad Frog (LLD_Creature_Radfrog) drops Glowing
    Meat (30%), so this page is seeded from FrogRace and flagged as glowing Rad Frogs.
  * Iguana — there is NO iguana creature in Fallout 76; Iguana Bits is a legacy loot /
    vendor meat. Built editorially: sourced from the raw-meat loot pools, the Whitespring
    meat vendor and the Rad Turkey Field-Dressing Station (read from the item's refs),
    with empty-state Fixed Spawn Locations.

Output: dist/meat.json (hub) + dist/meat/<slug>.json per page. Committed geo caches under
data/meat_spawns/<slug>.json so CI (no Mappalachia DB) reproduces the spawns.

Usage:
    python src/build_spawns.py meat [slug ...]
    python src/build_farming_meat_json.py
"""

import os, re, csv, sys, glob, json, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from spawns_configs import cryptids as C
from prune_outputs import prune_outputs

DIST = os.path.join(REPO, "dist")
OUT_DIR = os.path.join(DIST, "meat")
HUB_FILE = os.path.join(DIST, "meat.json")
GEO_DIR = os.path.join(REPO, "data", "meat_spawns")
URL_BASE = "/bnb/farming/meat/"
SOURCE_TAG = ("Game-file exports (NPC/LVLI/ALCH) + challenges.json + Mappalachia "
              "Position/Entity (cached for CI)")

# ── roster ───────────────────────────────────────────────────────────────────
# Each page: slug, name, lld (regex on INAM_EDID to pick the creature's NPCs),
# tokens (challenge + death-list + default ambush match), mappalachia (NPC-table
# names for world spawns), optional ambush_tokens / ambush_exclude for markers.
# Order: alphabetical by display name.
MEAT = [
    {"slug": "angler", "name": "Angler", "lld": r"^LLD_Creature_Angler",
     "tokens": ["angler"], "mappalachia": ["Angler"]},
    {"slug": "brahmin", "name": "Brahmin", "lld": r"^LLD_Creature_Brahmin",
     "tokens": ["brahmin"], "mappalachia": []},
    {"slug": "cat", "name": "Cat", "lld": r"^LLD_Creature_Cat(?![A-Za-z])",
     "tokens": ["cat"], "ambush_tokens": ["cat"], "ambush_exclude": ["fabricat"],
     "mappalachia": ["Cat"], "promote_unique_placements": True},
    {"slug": "chicken", "name": "Chicken", "lld": r"^LLD_Creature_Chicken",
     "tokens": ["chicken"], "mappalachia": ["Chicken"]},
    {"slug": "deathclaw", "name": "Deathclaw", "lld": r"^LLD_Creature_Deathclaw",
     "tokens": ["deathclaw"], "mappalachia": ["Deathclaw"]},
    {"slug": "fox", "name": "Fox", "lld": r"^LLD_Creature_Fox",
     "tokens": ["fox"], "mappalachia": ["Fox"]},
    {"slug": "frog", "name": "Frog", "lld": r"^LLD_Creature_Radfrog",
     "tokens": ["radfrog", "frog"], "mappalachia": ["Frog"]},
    {"slug": "glowing-meat", "name": "Glowing Meat", "lld": r"^LLD_Creature_Radfrog",
     "tokens": ["radfrog", "frog"], "mappalachia": ["Frog"],
     "note": ("Glowing Meat is dropped only by glowing Rad Frogs (30% chance) per the "
              "game data — this page is seeded from the Rad Frog. It overlaps the Frog "
              "meat page, which shares the same creature.")},
    {"slug": "gulper", "name": "Gulper", "lld": r"^LLD_Creature_Gulper",
     "tokens": ["gulper"], "mappalachia": ["Gulper"]},
    {"slug": "hermit-crab", "name": "Hermit Crab",
     "lld": r"^LLD_Creature_(HermitCrab|ThunderCrab)",
     "tokens": ["hermitcrab", "hermit crab", "thundercrab"], "mappalachia": []},
    {"slug": "megasloth", "name": "Megasloth", "lld": r"^LLD_Creature_Megasloth",
     "tokens": ["megasloth", "mega sloth"], "mappalachia": ["Mega Sloth"]},
    {"slug": "mirelurk", "name": "Mirelurk", "lld": r"^LLD_Creature_Mirelurk(Hunter)?(_|$)",
     "tokens": ["mirelurk"], "ambush_tokens": ["mirelurk"],
     "ambush_exclude": ["queen", "king"], "mappalachia": ["Mirelurk"]},
    {"slug": "mirelurk-queen", "name": "Mirelurk Queen",
     "lld": r"^LLD_Creature_MirelurkQueen",
     "tokens": ["mirelurkqueen", "mirelurk queen", "queen mirelurk"],
     "ambush_tokens": ["mirelurkqueen"], "mappalachia": []},
    {"slug": "mole-rat", "name": "Mole Rat", "lld": r"^LLD_Creature_Molerat",
     "tokens": ["molerat", "mole rat"], "mappalachia": ["Mole Rat"]},
    {"slug": "mongrel-dog", "name": "Mongrel Dog", "lld": r"^LLD_Creature_Dog",
     "tokens": ["mongrel dog", "creature_dog"], "ambush_tokens": ["_dog"],
     "mappalachia": ["Vicious Dog"]},
    {"slug": "mutant-hound", "name": "Mutant Hound", "lld": r"^LLD_Creature_MutantHound",
     "tokens": ["mutanthound", "mutant hound"], "mappalachia": []},
    {"slug": "opossum", "name": "Opossum", "lld": r"^LLD_Creature_Opossum",
     "tokens": ["opossum", "opposum"], "mappalachia": ["Opposum"]},
    {"slug": "owlet", "name": "Owlet", "lld": r"^LLD_Creature_Owlet",
     "tokens": ["owlet"], "mappalachia": []},
    {"slug": "rabbit", "name": "Rabbit", "lld": r"^LLD_Creature_Rabbit",
     "tokens": ["rabbit"], "mappalachia": ["Rabbit"]},
    {"slug": "radhog", "name": "Radhog", "lld": r"^LLD_Creature_RadHog",
     "tokens": ["radhog", "rad hog"], "mappalachia": []},
    {"slug": "radrat", "name": "Radrat", "lld": r"^LLD_Creature_Radrat",
     "tokens": ["radrat", "rad rat"], "mappalachia": ["Rad Rat"]},
    {"slug": "radstag", "name": "Radstag", "lld": r"^LLD_Creature_Radstag",
     "tokens": ["radstag", "rad stag"], "mappalachia": ["Radstag"]},
    {"slug": "radtoad", "name": "Radtoad", "lld": r"^LLD_Creature_Radtoad",
     "tokens": ["radtoad", "rad toad"], "mappalachia": ["Rad Toad"]},
    {"slug": "radturkey", "name": "Rad Turkey", "lld": r"^LLD_Creature_RadTurkey",
     "tokens": ["radturkey", "rad turkey"], "mappalachia": []},
    {"slug": "pheasant", "name": "Pheasant", "lld": r"^LLD_Creature_Pheasant",
     "tokens": ["pheasant"], "mappalachia": []},
    {"slug": "scorchbeast", "name": "Scorchbeast", "lld": r"^LLD_Creature_Scorchbeast",
     "tokens": ["scorchbeast"], "nest_tokens": ["scorchbeast"], "mappalachia": []},
    {"slug": "sheepsquatch", "name": "Sheepsquatch", "lld": r"^LLD_Creature_Sheepsquatch",
     "tokens": ["sheepsquatch"], "mappalachia": []},
    {"slug": "squirrel", "name": "Squirrel", "lld": r"^LLD_Creature_RadSquirrel",
     "tokens": ["radsquirrel", "squirrel"], "mappalachia": ["Squirrel"]},
    {"slug": "wolf", "name": "Wolf", "lld": r"^LLD_Creature_Wolf",
     "tokens": ["creature_wolf", "wolf meat"], "ambush_tokens": ["wolf"],
     "mappalachia": ["Wolf"]},
    {"slug": "yao-guai", "name": "Yao Guai", "lld": r"^LLD_Creature_YaoGuai",
     "tokens": ["yaoguai", "yao guai"], "mappalachia": ["Yao Guai"]},
    # editorial (no creature)
    {"slug": "iguana", "name": "Iguana", "editorial": "iguana",
     "tokens": ["iguana"], "meat_fid": "000330FD"},
]

# Naming convention (Aug 2026): each creature is a sub-category CARD named by the
# creature, and the guide PAGE lives under it at <slug>/<slug>-location-guide/ titled
# "{name} Location Guide". URL_OF returns the guide-page URL (doc.url + internal links).
URL_OF = lambda slug: f"{URL_BASE}{slug}/{slug}-location-guide/"


# ── raw-meat item set (MealTypeRaw ∩ IngredientTypeMeat) ─────────────────────
def _load_meat_items():
    """Raw meat = MealTypeRaw ∩ IngredientTypeMeat, EXCLUDING eggs (they have their own
    farming category)."""
    path = C._newest("ALCH_Export_*.tsv", exclude=["_Effects"])
    out = {}
    if path:
        with open(path, encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                kw = r.get("Keywords_Flat", "")
                name = r.get("FULL", "")
                if "MealTypeRaw" in kw and "IngredientTypeMeat" in kw \
                        and not re.search(r"\begg\b", name, re.I):
                    out[(r.get("ALCH_FormID") or "").upper()] = {
                        "name": name, "edid": r.get("ALCH_EDID", ""),
                        "refs": [r.get(f"Ref_{i}") for i in range(1, 60) if r.get(f"Ref_{i}")],
                    }
    return out


def _meats_from_death_lists(drops, resolver, meat_set):
    """The raw meats a creature drops, resolved from its death lists via rng76
    (catches meats nested inside loot pools, which the display rows don't surface).
    Returns [{name, form_id, rate_display}] sorted by rate desc."""
    best = {}
    if resolver is None:
        return []
    for lst in drops.get("lists", []):
        fid = lst.get("form_id")
        if not fid:
            continue
        try:
            leaves = resolver.resolve_simple(fid)
        except Exception:
            leaves = {}
        for lf, ch in leaves.items():
            lf = lf.upper()
            if lf in meat_set and ch and ch > 0:
                if lf not in best or ch > best[lf]:
                    best[lf] = ch
    out = [{"name": meat_set[fid]["name"], "form_id": fid,
            "rate_display": C.pct(round(rate, 6))}
           for fid, rate in best.items()]
    out.sort(key=lambda m: m["name"])
    return out


def _derive_races(lld_regex, by_fid):
    """RNAM_EDIDs of live NPCs whose INAM death list matches lld_regex (from the
    already-loaded NPC roster in ctx — no per-page file re-read)."""
    rx = re.compile(lld_regex, re.I)
    races = set()
    for row in by_fid.values():
        ed = row.get("inam_edid") or ""
        edd = row.get("edid") or ""
        if ed.startswith("zzz_") or re.match(r"(?i)(test|audiotemplate)", edd):
            continue
        if rx.match(ed) and row.get("race"):
            races.add(row["race"])
    return sorted(races)


def _meat_items_in_drops(doc, meat_set):
    """The raw-meat items this creature actually drops (tagged from the resolved
    Drops), for the page header + hub — derived, never hand-listed."""
    seen, out = set(), []
    for lst in doc.get("drops", {}).get("lists", []):
        for row in lst.get("rows", []):
            if row.get("kind") != "item":
                continue
            fid = (row.get("form_id") or "").upper()
            if fid in meat_set and fid not in seen:
                seen.add(fid)
                out.append({"name": row["name"], "form_id": fid,
                            "rate_display": row.get("rate_display", "")})
    return out


# ── editorial Iguana page ────────────────────────────────────────────────────
def _iguana_sources(meat_set):
    """Read Iguana Bits' refs to name where it actually comes from (vendors / loot
    pools / resource station), so the editorial note is data-derived, not invented."""
    info = meat_set.get("000330FD", {})
    vendors, loot, stations = [], [], []
    for ref in info.get("refs", []):
        parts = (ref or "").split(":")
        edid = parts[1] if len(parts) >= 3 else ""
        low = edid.lower()
        if not edid:
            continue
        if "vendor" in low:
            vendors.append(edid)
        elif "resource" in low or "dressingstation" in low or "field" in low:
            stations.append(edid)
        elif low.startswith(("ll_", "container_", "lls_")) and ("meat" in low or "food" in low):
            loot.append(edid)
    return vendors, loot, stations


def _build_iguana(pg, meat_set):
    generated = datetime.date.today().isoformat()
    vendors, loot, stations = _iguana_sources(meat_set)
    resolver, appearance_fn = C._load_rng76()
    # Used For (challenges naming iguana) via the shared challenge join.
    uf = C.used_for(pg, URL_OF(pg["slug"]))
    parts = []
    if loot:
        parts.append("the raw-meat loot pools (" + ", ".join(sorted(set(loot))[:4]) + ")")
    if vendors:
        parts.append("meat vendors (" + ", ".join(sorted(set(vendors))[:3]) + ")")
    if stations:
        parts.append("the " + ", ".join(sorted(set(stations))[:2]) + " resource station")
    note = ("There is no iguana creature in Fallout 76 — Iguana Bits is a legacy loot / "
            "vendor meat. It is sourced from " + ("; ".join(parts) if parts else
            "raw-meat loot and vendors") + ". It has no world creature spawns.")
    doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG, "editorial": True},
        "set": "meat", "slug": pg["slug"], "name": pg["name"],
        "page_title": f"Farming - Meat - {pg['name']}", "url": URL_OF(pg["slug"]),
        "blurb": ("Iguana Bits — a legacy raw meat with no creature source in Appalachia. "
                  "Found in raw-meat loot and sold by meat vendors."),
        "editorial_note": note,
        "npc_summary": {"npc_count": 0},
        "meat_items": [{"name": meat_set.get("000330FD", {}).get("name", "Iguana Bits"),
                        "form_id": "000330FD", "rate_display": ""}],
        "used_for": uf,
        "farming_tips": None,
        "events_activities": [],
        "drops": {"lists": [], "inherited_only": False, "empty_reason": "editorial"},
        "random_encounters": [],
        "fixed_spawns": {"regions": [{"region": r, "locations": []} for r in C.ALL_REGIONS],
                         "total_markers": 0, "total_placements": 0},
    }
    return doc


# ── entry point ──────────────────────────────────────────────────────────────
def run(argv=None):
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(GEO_DIR, exist_ok=True)
    generated = datetime.date.today().isoformat()
    slug_filter = {a for a in (argv or []) if not a.startswith("-") and a != "meat"}
    pages = [pg for pg in MEAT if not slug_filter or pg["slug"] in slug_filter]

    meat_set = _load_meat_items()
    print(f"[meat] {len(meat_set)} raw-meat items (MealTypeRaw ∩ IngredientTypeMeat); "
          f"building {len(pages)} pages")

    # Load rng76 / NPC roster / LVLI tables / DB ONCE and reuse across all pages
    # (the rng76 load is ~15s; per-page reload would be minutes for 38 pages).
    ctx = C.build_ctx()
    print("[meat] shared engine loaded (rng76 + tables + NPCs + DB) — reused per page.")

    hub = []
    for pg in pages:
        if pg.get("editorial") == "iguana":
            doc = _build_iguana(pg, meat_set)
            placements = 0
        else:
            pg = dict(pg)  # don't mutate the module config
            pg["races"] = _derive_races(pg["lld"], ctx["by_fid"])
            pg["used_for_url"] = URL_OF(pg["slug"])
            cache = os.path.join(GEO_DIR, pg["slug"] + ".json")
            keep = _keep_from_doc(os.path.join(OUT_DIR, pg["slug"] + ".json"))
            # promote_placements: a directly placed creature IS a fixed spawn here —
            # every base behind one is this page's own species. See
            # cryptids.tier_spawns for why the leveled ones count too.
            bundle = C.compute_bundle(pg, cache, keep=keep, ctx=ctx,
                                      promote_placements=True)
            placements = bundle["fixed_spawns"]["total_placements"]
            doc = {
                "_meta": {"generated": generated, "source": SOURCE_TAG,
                          "races": [{"edid": r} for r in pg["races"]],
                          "source_totals": bundle["_meta"]["source_totals"],
                          "unresolved": bundle["_meta"]["unresolved"]},
                "set": "meat", "slug": pg["slug"], "name": pg["name"],
                "page_title": f"Farming - Meat - {pg['name']}", "url": URL_OF(pg["slug"]),
                "npc_summary": bundle["npc_summary"],
                "meat_items": _meats_from_death_lists(bundle["drops"], ctx["resolver"], meat_set),
                "used_for": bundle["used_for"],
                "farming_tips": None,
                "events_activities": bundle["events_activities"],
                "drops": bundle["drops"],
                "random_encounters": bundle["random_encounters"],
                "fixed_spawns": bundle["fixed_spawns"],
                "chance_spawns": bundle["chance_spawns"],
            }
            doc["blurb"] = _blurb(pg, bundle, meat_set, doc["meat_items"])
            if pg.get("note"):
                doc["editorial_note"] = pg["note"]

        # assertion: spawns[] == count on every location
        bad = [(r["region"], l["marker"]) for r in doc["fixed_spawns"]["regions"]
               for l in r["locations"]
               if not l.get("spawns_compacted")
               and len(l.get("spawns") or []) != l["count"]]
        if bad:
            raise AssertionError(f"[{pg['slug']}] spawns/count mismatch at {bad[:5]}")

        out_path = os.path.join(OUT_DIR, pg["slug"] + ".json")
        json.dump(doc, open(out_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

        meats = ", ".join(m["name"] for m in doc.get("meat_items", [])) or "—"
        print(f"  {pg['slug']:<15} npcs:{doc['npc_summary'].get('npc_count', 0):>3} "
              f"meat:[{meats[:40]}] uf:{len(doc['used_for']):>2} "
              f"ev:{len(doc['events_activities']):>2} re:{len(doc['random_encounters']):>2} "
              f"fixed:{placements:>4}")
        hub.append({"slug": pg["slug"], "name": pg["name"], "url": URL_OF(pg["slug"]),
                    "meat_items": [m["name"] for m in doc.get("meat_items", [])],
                    "counts": {"npcs": doc["npc_summary"].get("npc_count", 0),
                               "fixed_spawns": placements,
                               "challenges": len(doc["used_for"])}})

    hub_doc = {"_meta": {"generated": generated, "source": SOURCE_TAG},
               "name": "Farming - Meat", "page_title": "Farming - Meat", "url": URL_BASE,
               "blurb": ("Every farmable meat in Fallout 76 — the creature that drops it, "
                         "where it spawns, and the drop rates. Pick a meat below."),
               "meats": hub}
    # Prune before the hub is written: an output directory that is only ever
    # written to keeps serving whatever it was last given. dist/meat/ still held
    # eight insect pages from before insects moved to their own family, and
    # nothing anywhere reported it. `pages` is the filtered roster, so the prune
    # is skipped whenever a slug filter narrowed the run.
    prune_outputs(OUT_DIR, [pg["slug"] for pg in MEAT],
                  tag="[meat]", skip=bool(slug_filter), also_keep=())

    json.dump(hub_doc, open(HUB_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"[meat] wrote {HUB_FILE} ({len(hub)} pages) + per-page docs in {OUT_DIR}")
    C.close_ctx(ctx)

    try:
        from patchlog_utils import write_empty_patchlog_feed
        write_empty_patchlog_feed("dist", "patchlog_latest_bnb_farming_meat.json",
                                  current_count=len(hub))
    except Exception:
        pass


def _keep_from_doc(path):
    try:
        return C.ebuild.load_existing(path)
    except Exception:
        return {}


def _blurb(pg, bundle, meat_set, meat_items):
    n = bundle["npc_summary"].get("npc_count", 0)
    pl = bundle["fixed_spawns"]["total_placements"]
    meats = ", ".join(m["name"] for m in meat_items)
    tail = []
    if meats:
        tail.append("drops " + meats)
    if n:
        tail.append(f"{n} NPC variant" + ("" if n == 1 else "s"))
    if pl:
        tail.append(f"{pl} known spawn point" + ("" if pl == 1 else "s"))
    head = f"The {pg['name']} in Fallout 76"
    return head + (" — " + ", ".join(tail) + "." if tail else ".")


def main(argv=None):
    run(argv)


if __name__ == "__main__":
    run(sys.argv)
