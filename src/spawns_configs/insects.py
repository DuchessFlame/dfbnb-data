#!/usr/bin/env python3
r"""
spawns_configs.insects — the Farming INSECTS family driver (BNB, /bnb/farming/insects/).

Insects are the creatures the game tags ActorTypeBug. This is the SAME creature-seeded
engine as meat (it reuses spawns_configs.cryptids.compute_bundle verbatim) — the only
difference is the category, the URL base and the roster. Eight creatures moved here out
of Farming - Meat (they carry ActorTypeBug): Bloatfly, Bloodbug, Cave Cricket, Fog
Crawler, Rad Ant, Radroach, Radscorpion, Stingwing. Plus one new page, Firefly
(drops Bioluminescent Fluid).

Naming convention (Aug 2026, shared with meat + plants): each creature is a sub-category
CARD named by the creature; the guide PAGE sits under it at
<slug>/<slug>-location-guide/, titled "{name} Location Guide". URL_OF returns that
guide-page URL.

Note: Tick is NOT built here — Tick's spawns live on the existing Tick Blood
non-perishable page, so the Insects index carries a Tick card that redirects to it
(guide_index alias), never a duplicated data page.

Output: dist/insects.json (hub) + dist/insects/<slug>.json per page. Geo caches under
data/insect_spawns/<slug>.json for DB-free CI.

Usage:
    python src/build_spawns.py insects [slug ...]
"""

import os, sys, json, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from spawns_configs import cryptids as C
from spawns_configs import meat as M   # reuse meat's item/meat helpers verbatim
from prune_outputs import prune_outputs

DIST = os.path.join(REPO, "dist")
OUT_DIR = os.path.join(DIST, "insects")
HUB_FILE = os.path.join(DIST, "insects.json")
GEO_DIR = os.path.join(REPO, "data", "insect_spawns")
URL_BASE = "/bnb/farming/insects/"
SOURCE_TAG = ("Game-file exports (NPC/LVLI/ALCH) + challenges.json + Mappalachia "
              "Position/Entity (cached for CI)")

# roster — the 8 ActorTypeBug creatures moved out of Meat + Firefly (new). A-Z.
INSECTS = [
    {"slug": "bloatfly", "name": "Bloatfly", "lld": r"^LLD_Creature_Bloatfly",
     "tokens": ["bloatfly"], "mappalachia": ["Bloatfly"]},
    {"slug": "bloodbug", "name": "Bloodbug", "lld": r"^LLD_Creature_Bloodbug",
     "tokens": ["bloodbug"], "mappalachia": ["Bloodbug"]},
    {"slug": "cave-cricket", "name": "Cave Cricket", "lld": r"^LLD_Creature_CaveCricket",
     "tokens": ["cavecricket", "cave cricket"], "mappalachia": ["Cave Cricket"]},
    {"slug": "firefly", "name": "Firefly", "lld": r"^LLD_Creature_Firefly",
     "tokens": ["firefly"], "mappalachia": ["Firefly"]},
    {"slug": "fog-crawler", "name": "Fog Crawler", "lld": r"^LLD_Creature_FogCrawler",
     "tokens": ["fogcrawler", "fog crawler"], "mappalachia": ["Fog Crawler"]},
    {"slug": "rad-ant", "name": "Rad Ant", "lld": r"^LLD_Creature_RadAnt",
     "tokens": ["radant", "rad ant"], "mappalachia": ["Rad Ant"]},
    {"slug": "radroach", "name": "Radroach", "lld": r"^LLD_Creature_Radroach",
     "tokens": ["radroach"], "mappalachia": ["Radroach"]},
    {"slug": "radscorpion", "name": "Radscorpion", "lld": r"^LLD_Creature_Radscorpion",
     "tokens": ["radscorpion", "rad scorpion"], "mappalachia": ["Rad Scorpion"]},
    {"slug": "stingwing", "name": "Stingwing", "lld": r"^LLD_Creature_Stingwing",
     "tokens": ["stingwing"], "mappalachia": ["Stingwing"]},
]

URL_OF = lambda slug: f"{URL_BASE}{slug}/{slug}-location-guide/"


def run(argv=None):
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(GEO_DIR, exist_ok=True)
    generated = datetime.date.today().isoformat()
    slug_filter = {a for a in (argv or []) if not a.startswith("-") and a != "insects"}
    pages = [pg for pg in INSECTS if not slug_filter or pg["slug"] in slug_filter]

    meat_set = M._load_meat_items()
    print(f"[insects] building {len(pages)} pages (creature-seeded, ActorTypeBug)")

    ctx = C.build_ctx()
    print("[insects] shared engine loaded (rng76 + tables + NPCs + DB) — reused per page.")

    hub = []
    for pg in pages:
        pg = dict(pg)
        pg["races"] = M._derive_races(pg["lld"], ctx["by_fid"])
        pg["used_for_url"] = URL_OF(pg["slug"])
        cache = os.path.join(GEO_DIR, pg["slug"] + ".json")
        keep = M._keep_from_doc(os.path.join(OUT_DIR, pg["slug"] + ".json"))
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
            "set": "insects", "slug": pg["slug"], "name": pg["name"],
            "page_title": f"Farming - Insects - {pg['name']}", "url": URL_OF(pg["slug"]),
            "npc_summary": bundle["npc_summary"],
            "meat_items": M._meats_from_death_lists(bundle["drops"], ctx["resolver"], meat_set),
            "used_for": bundle["used_for"],
            "farming_tips": None,
            "events_activities": bundle["events_activities"],
            "drops": bundle["drops"],
            "random_encounters": bundle["random_encounters"],
            "fixed_spawns": bundle["fixed_spawns"],
            "chance_spawns": bundle["chance_spawns"],
        }
        doc["blurb"] = M._blurb(pg, bundle, meat_set, doc["meat_items"])

        bad = [(r["region"], l["marker"]) for r in doc["fixed_spawns"]["regions"]
               for l in r["locations"]
               if not l.get("spawns_compacted")
               and len(l.get("spawns") or []) != l["count"]]
        if bad:
            raise AssertionError(f"[{pg['slug']}] spawns/count mismatch at {bad[:5]}")

        out_path = os.path.join(OUT_DIR, pg["slug"] + ".json")
        json.dump(doc, open(out_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        meats = ", ".join(m["name"] for m in doc.get("meat_items", [])) or "—"
        print(f"  {pg['slug']:<14} npcs:{doc['npc_summary'].get('npc_count', 0):>3} "
              f"drops:[{meats[:36]}] fixed:{placements:>4} "
              f"chance:{doc['chance_spawns'].get('total', 0):>3}")
        hub.append({"slug": pg["slug"], "name": pg["name"], "url": URL_OF(pg["slug"]),
                    "meat_items": [m["name"] for m in doc.get("meat_items", [])],
                    "counts": {"npcs": doc["npc_summary"].get("npc_count", 0),
                               "fixed_spawns": placements,
                               "challenges": len(doc["used_for"])}})

    hub_doc = {"_meta": {"generated": generated, "source": SOURCE_TAG},
               "name": "Farming - Insects", "page_title": "Farming - Insects", "url": URL_BASE,
               "blurb": ("Every farmable insect in Fallout 76 — the bug that drops it, where "
                         "it spawns, and the drop rates. Pick an insect below."),
               "insects": hub}
    # Prune before the hub is written: an output directory that is only ever
    # written to keeps serving whatever it was last given. dist/meat/ still held
    # eight insect pages from before insects moved to their own family, and
    # nothing anywhere reported it. `pages` is the filtered roster, so the prune
    # is skipped whenever a slug filter narrowed the run.
    prune_outputs(OUT_DIR, [pg["slug"] for pg in INSECTS],
                  tag="[insects]", skip=bool(slug_filter), also_keep=())

    json.dump(hub_doc, open(HUB_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"[insects] wrote {HUB_FILE} ({len(hub)} pages) + per-page docs in {OUT_DIR}")
    C.close_ctx(ctx)


def main(argv=None):
    run(argv)


if __name__ == "__main__":
    run(sys.argv)
