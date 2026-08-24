#!/usr/bin/env python3
r"""
spawns_configs.consumable_items — single-page "{Item} Location Guide" driver for
CAMP/party consumables that have NO fixed world spawns (Scout's Banner, Lunchbox).

PAGES (DF brand, under /df/farming/consumables/)
    /df/farming/consumables/scouts-banner/location-guide/   <- single page
    /df/farming/consumables/lunchbox/location-guide/        <- single page

These items are Season Scoreboard reward consumables (ALCH `SCORE_*_Consumable`),
not placed in the world, so Fixed Spawn Locations renders its honest empty state
(no markers). Every OTHER obtain route is populated: Used For (the ALCH effect via
build_consumption + any naming challenges), How to Obtain (used_for.obtain.note —
the Scoreboard/Atom source), Vendors, Containers (container-TYPE model) and Events
& Activities, all resolved through the shared spawns engine + rng76 where present.

RENDERER: no new JS logic — reuses df-bnb-farming-non-perishable-guide.js. Each
item is added to that file's DF_GUIDES map so parseDF routes its path to the JSON
here. Doc shape is the farming doc shape with regions=[] (fetchData requires
`regions` to be an array).

Usage:
    python src/build_spawns.py consumable-items            # both
    python src/build_spawns.py consumable-items lunchbox   # one
"""

import os, re, sys, json, sqlite3, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from spawns_engine import sources as esources
from spawns_engine import events as eevents
from spawns_engine.classify import nuka_classify

try:
    from build_farming_used_for import build_consumption
except Exception:
    build_consumption = None

TSV = esources.TSV
DIST = os.path.join(REPO, "dist")
URL_TMPL = "/df/farming/consumables/{slug}/location-guide/"

# Per-item config. `obtain_note` is grounded in the datamined SCORE_ EDID prefix
# (Season Scoreboard reward). It is deliberately hedged on any Atom-shop offering,
# which should be confirmed against the Atom Shop guide before asserting.
ITEMS = {
    "scouts-banner": {
        "formid": "00653FCD",
        "name": "Scout's Banner",
        "obtain_note": (
            "Scout's Banner is a <b>Season Scoreboard reward consumable</b> "
            "(datamined editor ID <code>SCORE_Banner_Consumable</code>) — it is "
            "handed out on the free reward track of the seasons that feature it, "
            "not found in the world. Deploy it in your CAMP or on an event to apply "
            "the Battle Banner team perks to nearby players. Check the current "
            "Season Scoreboard (and the Atom Shop, when a bundle includes it) for "
            "the live source."),
    },
    "lunchbox": {
        "formid": "003DF247",
        "name": "Lunchbox",
        "obtain_note": (
            "Lunchbox is a <b>Season Scoreboard reward consumable</b> (datamined "
            "editor ID <code>SCORE_Lunchbox_Consumable</code>) and is also a "
            "long-standing Atom Shop item. Open it near friends to apply the Party "
            "Favor buff (bonus XP that stacks per player nearby). Check the current "
            "Season Scoreboard and the Atom Shop for the live source."),
    },
}


def r6(v):
    return None if v is None else round(float(v), 6)


def pct(v, places=2):
    if v is None:
        return ""
    s = f"{v * 100:.{places}f}".rstrip("0").rstrip(".")
    return (s or "0") + "%"


def _prettify(edid):
    if not edid:
        return ""
    s = re.sub(r"^(?:(?:LL[SVEDIC]?|LPI|LLD|SCORE|ATX|NWOT|RE)_)+", "", edid)
    s = re.sub(r"_+", " ", s)
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s).strip()


def build_one(slug, cfg, appearance_fn, tbls):
    fid = cfg["formid"].upper()
    name = cfg["name"]
    item_records = [{"formid": fid, "sig": "ALCH", "edid": ""}]
    targets = {fid}
    src = esources.get_sources(item_records, tbls, nuka_classify)

    # Used For — the item's own ALCH effect + any naming challenges.
    consumption = None
    if build_consumption is not None:
        try:
            consumption = build_consumption(fid, TSV, item_name=name)
        except Exception:
            consumption = None
    used_for = {
        "consumption": consumption,
        "challenges": (consumption or {}).get("challenges", []) if consumption else [],
        "recipes": [],
        "obtain": {"recipes": [], "note": cfg["obtain_note"]},
    }

    # Containers (container-TYPE model) — any lootable container type that can hold it.
    parent_edid = tbls["parent_edid"]
    edid_to_fid = {}
    for f, e in parent_edid.items():
        if e:
            edid_to_fid.setdefault(e, f)

    def rate_of(list_ref):
        if not appearance_fn or not list_ref:
            return None
        lid = list_ref if re.fullmatch(r"[0-9A-Fa-f]{8}", str(list_ref)) else edid_to_fid.get(list_ref)
        if not lid:
            return None
        try:
            p = appearance_fn(lid, targets)
            return r6(p) if p and p > 0 else None
        except Exception:
            return None

    boxes = []
    seen_names = set()
    for lv in sorted(src["lvli_closure"]):
        edid = parent_edid.get(lv) or ""
        if not edid or re.search(r"(^qa|^test|debug|zzz_|babylon)", edid, re.I):
            continue
        if not re.search(r"(box|scavenger|cache|loot_?bag)", edid, re.I):
            continue
        nm = _prettify(edid)
        if nm in seen_names:
            continue
        seen_names.add(nm)
        boxes.append({"name": nm, "rate": r6(rate_of(lv)), "rate_display": pct(rate_of(lv))})
    boxes.sort(key=lambda b: (-(b["rate"] or 0), b["name"].lower()))
    containers = {"types": boxes} if boxes else None

    # Vendors.
    vendors = []
    for f, meta in sorted(src["placed_bases"].items()):
        if meta.get("source_type") != "vendor":
            continue
        via = meta.get("via") or ""
        r = rate_of(via) if via else None
        vendors.append({"name": _prettify(meta.get("edid") or via), "marker": "", "region": "",
                        "vendor_type": "Vendor",
                        "rate_lines": [pct(r)] if r else [], "rate_display": pct(r) or "",
                        "rate_value": r or 0, "count": 1, "stock_list": via})
    vendors.sort(key=lambda v: (-v["rate_value"], v["name"].lower()))

    # Events & Activities that award it.
    events_activities = eevents.detect(src["lvli_closure"], parent_edid, c2p=tbls["c2p"])
    eevents.resolve_event_rates(events_activities, targets, appearance_fn)

    generated = datetime.date.today().isoformat()
    doc = {
        "_meta": {"generated": generated,
                  "source": "Game-file exports (ALCH/LVLI) — no world placements (reward consumable)",
                  "item": {"form_id": fid, "edid_prefix": "SCORE"}},
        "set": slug,
        "name": name,
        "page_title": f"{name} Location Guide",
        "blurb": (f"{name} has no fixed world spawns — it is a Season Scoreboard reward "
                  "consumable. This guide covers what it does, how to obtain it, and any "
                  "vendors, containers or events that hand it over."),
        "drop_rates": {
            "world_spawns": None,
            "containers": containers,
            "collectrons": None,
            "resource_generators": None,
            "creatures": None,
        },
        "used_for": used_for,
        "vendor_list": vendors,
        "events_activities": events_activities,
        # regions empty -> Fixed Spawn Locations shows its honest empty state (no markers).
        "regions": [],
    }

    out_dir = os.path.join(DIST, slug.replace("-", "_") + "_spawns")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, slug.replace("-", "_") + "_spawns.json")
    json.dump(doc, open(out_file, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"[consumable-items] {name:<16} -> {os.path.relpath(out_file, REPO)}  "
          f"(containers: {len(boxes)} · vendors: {len(vendors)} · events: {len(events_activities)}"
          f" · consumption: {'yes' if consumption else 'no'})")
    print(f"    URL: {URL_TMPL.format(slug=slug)}")
    return out_file


def run(argv=None):
    argv = argv or []
    which = [a.lower() for a in argv if a in ITEMS]
    slugs = which or list(ITEMS)
    tbls = esources.load_tables()
    appearance_fn = None
    try:
        import rng76
        _res = rng76.Rng76Data.from_tsv_root(TSV).resolver
        appearance_fn = lambda lid, t: _res.appearance_prob(lid, t)
        print("[consumable-items] rng76 loaded — source rates computed.")
    except Exception as e:
        print(f"[consumable-items] [warn] rng76 unavailable ({e}); source rates blank.")
    for slug in slugs:
        build_one(slug, ITEMS[slug], appearance_fn, tbls)


def main(argv=None):
    run(argv)


if __name__ == "__main__":
    run(sys.argv[1:])
