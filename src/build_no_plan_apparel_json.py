#!/usr/bin/env python3
r"""
build_no_plan_apparel_json.py — Collectables: Apparel Without Plans.

Page: /df/collectables/apparel-without-plans/checklist/
Data: dist/no_plan_apparel.json  (PTS: dist/pts/no_plan_apparel.json)
Renderer: df-bnb-plan-checklists.js -> renderSubPage (SUBPAGE_DATASETS)

WHAT IS ON THE PAGE
===================
Outfits, hats, masks and bandanas a player can own but can never CRAFT —
there is no plan for them. The Forest Camo Jumpsuit and the VTU Baseball Cap
are the examples Duchess gave. They only come from drops, containers, vendors,
quests, events and challenges.

The roster is read off the game files, never a hand list:

  1. an ARMO record with a name, that is not dev / cut (plan_sources.is_dev_record),
     not an Atom Shop / scoreboard record (they have a COBJ, so test 2 excludes
     them anyway; ATX_/SCORE_ is belt and braces) and not a NONPLAYABLE copy or
     a C.A.M.P. pet skin (NOT_PLAYER_COPY)
  2. NO COBJ creates it (COBJ.CNAM). A plan teaches a COBJ, so "no COBJ" is
     also "no plan book". This is Duchess's rule: "the apparel I want has no
     cobj or book".
  3. it is apparel, not armour: no resistance ladder and no durability
     (plan_apparel_class.has_stats — the same test the Apparel plan page uses)
  4. something in the game files actually gives it out. Creature outfits,
     mannequin copies and NPC-only clothing resolve no route and drop off by
     themselves; nothing is excluded by name.
     resolve_routes(skip_worn=True) is what makes that true: a list that only
     an OTFT holds is what an NPC spawns WEARING, not loot (7 Oct 2026 — Mr.
     Claus' Suit, Executioner Outfit, Dog Armor, Super Mutant armour pieces
     were all published off "Outfit ..." rows). See bpo.worn_only.

Records sharing one in-game name are ONE row (a re-issued copy of the same
hat is still the same hat to a reader); every record is listed in Technical.

HOW TO OBTAIN
=============
Exactly the plan checklists' machinery, pointed at the ARMO FormID instead of
a plan BOOK FormID — so the ledger rows, route names, rng76 rates, Drop
Conditions, dead-route pruning and the source pill are the same code and
cannot drift from the plan pages:

    build_plan_obtain_json.resolve_routes   drop routes + rng76 rate
    plan_route_kinds                        quest / event / enemy / corpse
    plan_sources.UnlockIndex.unlocks_for    GMRW quest & challenge rewards
    plan_conditions                         Drop Conditions per source
    prune_dead_routes                       lists nothing in the game rolls
    plan_sources.obtain_ledger              the fixed route table
    plan_source_pill                        the one-word row pill

The ledger rows are plan_sources.LEDGER_ROWS — Caps, Stamps, Scoreboard, Gold
Bullion, Atom Shop, Limited Time Bundle, Containers, Enemies, Scrap to Learn,
Events & Activities, Quests, Challenges. Scrap to Learn is always N/A here; it
stays so the table reads the same on every checklist.

This module owns NO rate maths (drop-rate-engine owns that, via rng76).

USAGE
=====
    python src/build_no_plan_apparel_json.py --data-dir tsv --outdir dist
    python src/build_no_plan_apparel_json.py --data-dir tsv --limit 20   # quick look
"""
import argparse
import collections
import json
import os
import re
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import build_plan_obtain_json as bpo
import plan_sources
import plan_source_pill
import plan_route_kinds
import plan_conditions
import prune_dead_routes
import plan_apparel_class
import plan_images

SCHEMA = 1
PAGE_SLUG = "apparel-without-plans"
IMAGE_FOLDER = "apparel-without-plans"   # /guide-images/plan-checklist/<this>/
OUT_NAME = "no_plan_apparel.json"

ATOM_SHOP = re.compile(r"^ATX_", re.I)

# Copies of a record that a player never holds, named as such by Bethesda:
#   NONPLAYABLE / NotPlayable   the NPC- or mannequin-worn twin of an outfit
#   _ATX_ / TakenFromAtx / SCORE_   Atom Shop and scoreboard records whose
#                               playable copy HAS a COBJ (so is not this page);
#                               these leftovers are the non-craftable shells
#   CAMPPets                    C.A.M.P. pet skins are ARMO records too
# Left in, they either merge NPC outfit routes into the real item's row or
# publish a scoreboard reward as "no plan" on the strength of its EditorID.
NOT_PLAYER_COPY = re.compile(
    r"NON_?PLAYABLE|NOT_?PLAYABLE|_ATX_|TakenFromAtx|^SCORE_|CAMPPets", re.I)

# unlocks_for() sentences that come from an EditorID convention, not from a
# record that gives the item out ("Named as a Season 14 reward in the game
# files."). Fine as a hint beside a plan; on its own it is not a source.
INFERRED = re.compile(r"^named (as|in) ", re.I)

# Keywords that stop a player handing the item to someone else.
UNTRADEABLE_KW = ("nonplayertradable", "nondroppable")

LEAD = ("There is no plan for this apparel — it cannot be crafted. "
        "You get it ready-made from the sources below.")
LEAD_ROUTES = LEAD + " Each drop source shows its resolved chance."


def _q(v):
    return str(v or "").strip()


def load_cobj_created():
    """FormIDs of every record some COBJ creates (CNAM)."""
    out = set()
    f = bpo.newest("COBJ_Export_*.tsv")
    for r in bpo.read_rows(f):
        c = _q(r.get("CNAM_FormID")).split(":")[0].upper()
        if c:
            out.add(c)
    return out


def load_armo_keywords():
    """ARMO FormID -> [(keyword EDID, keyword FormID)], off the KYWD
    ReferencedBy export. The FormID is kept for the Technical "Keywords"
    section, which prints them the way xEdit does: EDID [KYWD:FormID]."""
    out = collections.defaultdict(list)
    f = bpo.newest("KYWD_Export_*_Refs.tsv")
    if not f:
        return out
    for r in bpo.read_rows(f):
        if _q(r.get("RefSignature")) != "ARMO":
            continue
        fid = _q(r.get("RefFormID")).upper()
        kw = _q(r.get("KeywordEDID"))
        kfid = _q(r.get("KeywordFormID")).upper()
        if fid and kw and (kw, kfid) not in out[fid]:
            out[fid].append((kw, kfid))
    return out


def load_slots():
    """ARMO FormID -> biped slot labels ("Hair Top|Headband")."""
    out = {}
    f = bpo.newest("ARMO_Export_*_SLOTS.tsv")
    if not f:
        return out
    for r in bpo.read_rows(f):
        fid = _q(r.get("ARMO_FormID")).upper()
        if fid:
            out[fid] = _q(r.get("BOD2_FirstPersonFlagLabels"))
    return out


def roster(cobj_created, verbose=True):
    """name -> [ARMO record dicts], the candidate apparel. See docstring 1-3."""
    f = bpo.newest("ARMO_Export_*_ARMOUR.tsv")
    stats = collections.Counter()
    by_name = collections.OrderedDict()
    for r in bpo.read_rows(f):
        fid = _q(r.get("ARMO_FormID")).upper()
        edid = _q(r.get("ARMO_EDID"))
        name = _q(r.get("ARMO_FULL"))
        if not fid or not name:
            stats["no_name"] += 1
            continue
        if plan_sources.is_dev_record(edid):
            stats["dev_record"] += 1
            continue
        if ATOM_SHOP.match(edid):
            stats["atom_shop"] += 1
            continue
        if NOT_PLAYER_COPY.search(edid):
            stats["not_player_copy"] += 1
            continue
        if fid in cobj_created:
            stats["has_cobj"] += 1
            continue
        curve = any(_q(r.get(c)) for c in plan_apparel_class.CURVES)
        try:
            health = float(_q(r.get("DATA_Health")) or 0)
        except ValueError:
            health = 0.0
        if plan_apparel_class.has_stats((curve, health)):
            stats["armour"] += 1
            continue
        stats["candidate"] += 1
        by_name.setdefault(name, []).append({
            "formid": fid, "edid": edid, "name": name,
            "value": _q(r.get("DATA_Value")), "weight": _q(r.get("DATA_Weight")),
        })
    if verbose:
        print(f"[no-plan-apparel] roster from {os.path.basename(f)}: "
              + ", ".join(f"{k} {v}" for k, v in sorted(stats.items()))
              + f" -> {len(by_name)} names")
    return by_name


def _tradeable(kws, unlocks):
    blob = " ".join(kws).lower()
    if any(k in blob for k in UNTRADEABLE_KW):
        return False
    return True


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.path.join(REPO, "tsv"))
    ap.add_argument("--outdir", default=os.path.join(REPO, "dist"))
    ap.add_argument("--limit", type=int, default=0, help="first N names only (testing)")
    ap.add_argument("--only", default="", help="names containing this text only")
    ap.add_argument("--report", default="", help="write dropped/unresolved names here")
    args = ap.parse_args(argv)

    # build_plan_obtain_json reads its export root from module globals.
    bpo.TSV = args.data_dir
    bpo.DIST = args.outdir
    bpo.SIG_INDEX = bpo.build_sig_index()

    cobj_created = load_cobj_created()
    names = roster(cobj_created)
    if args.only:
        names = collections.OrderedDict((k, v) for k, v in names.items()
                                        if args.only.lower() in k.lower())
    if args.limit:
        names = collections.OrderedDict(list(names.items())[:args.limit])
    kw_by_fid = load_armo_keywords()
    slots = load_slots()

    print("[no-plan-apparel] loading route tables ...")
    tables = bpo.ssrc.load_tables(args.data_dir)
    rates = bpo.bfu.VendorRates(bpo.rng76.Rng76Data.from_tsv_root(args.data_dir))
    cont_names = bpo.bfu._load_cont_names(args.data_dir)
    npc_names = {}
    npcf = bpo.newest("NPC_Export_*.tsv")
    if npcf:
        for r in bpo.read_rows(npcf):
            nfid = _q(r.get("FormID")).upper()
            full = _q(r.get("FULL"))
            if nfid and full and nfid not in npc_names:
                npc_names[nfid] = full
    unlock_idx = plan_sources.UnlockIndex(args.data_dir,
                                          lambda pat, root: bpo.newest(pat, root))
    bpo.QUEST_NAMES = unlock_idx.quest_names
    bpo.GMRW_QUESTS.clear()
    bpo.GMRW_QUESTS.update(unlock_idx.gmrw_quests)
    bpo.QUEST_TITLES.update(getattr(unlock_idx, "_quests_by_fid", {}))
    try:
        route_kinds = plan_route_kinds.Ctx(args.data_dir)
    except Exception as exc:                       # noqa: BLE001 - never fatal
        print(f"  WARNING: route kinds unavailable: {exc}", file=sys.stderr)
        route_kinds = None

    items, dropped = [], []
    t0 = datetime.now()
    for i, (name, recs) in enumerate(names.items()):
        routes, unlocks = [], []
        for rec in recs:
            rr = bpo.resolve_routes(rec["formid"], tables, rates, cont_names, npc_names,
                                    skip_worn=True)
            if rr and route_kinds is not None:
                rr = plan_route_kinds.apply_to_routes(rr, route_kinds)
            for r in rr:
                r["_target"] = rec["formid"]
            routes += rr
            for u in unlock_idx.unlocks_for(rec["formid"], rec["edid"], [],
                                            has_routes=bool(rr)):
                if INFERRED.match(u):
                    continue
                if u not in unlocks:
                    unlocks.append(u)
        if len(recs) > 1:
            routes = bpo.collapse_routes(routes)
            routes.sort(key=lambda r: (-(r["rate"] or 0), r["source_type"],
                                       r["route"].lower()))
            routes = routes[:12]
        if not routes and not unlocks:
            dropped.append(f"{name} [{', '.join(r['edid'] for r in recs)}]")
            continue

        main_rec = recs[0]
        kws = sorted({k for r in recs for k, _f in kw_by_fid.get(r["formid"], [])})
        item = {
            "kind": "apparel", "brand": "df", "type": PAGE_SLUG,
            "id": f"NPA_{main_rec['formid']}",
            "name": name,
            "no_plan": True,
            "has_image_box": True,
            "obtain": LEAD_ROUTES if routes else LEAD,
            "category_label": "Apparel",
            "obtain_routes": routes,
            "obtain_unlocks": unlocks,
            "plan_item": None,
            "cobj": None,
            # The record the page is a picture of. plan_images reads `cnam`
            # for hosted art and staged stems, exactly as on the plan pages.
            "cnam": {"formid": main_rec["formid"], "edid": main_rec["edid"], "sig": "ARMO"},
            # Each record's own keywords (KYWD export order), shown in
            # Technical > Keywords. Kept per record: a merged row's copies can
            # differ (one Tradeable, one not), and that is what to look at.
            "records": [{"formid": r["formid"], "edid": r["edid"],
                         "slots": slots.get(r["formid"], ""),
                         "keywords": [{"edid": k, "formid": f}
                                      for k, f in kw_by_fid.get(r["formid"], [])]}
                        for r in recs],
            "keywords": kws,
            "slots": slots.get(main_rec["formid"], ""),
            "tradeable": _tradeable(kws, unlocks),
            "stops_dropping": None,
            "cut": False,
        }
        items.append(item)
        if (i + 1) % 50 == 0:
            el = (datetime.now() - t0).total_seconds()
            print(f"   ... {i+1}/{len(names)}  ({el:.0f}s)")

    # Same post passes, same order, as build_plan_obtain_json.
    print("[no-plan-apparel] drop conditions:")
    cidx = plan_conditions.ConditionIndex(args.data_dir, bpo.newest)
    cstats = collections.Counter()
    for it in items:
        own = {"recipe": "", "book": it["cnam"]["formid"], "title": it["name"]}
        for r in it["obtain_routes"]:
            lists = r.get("lvli") or []
            if not lists:
                r["conditions"] = []
                continue
            lines, skipped = cidx.for_lists(lists, r.get("_target") or it["cnam"]["formid"], own)
            if skipped and not lines:
                lines = [plan_conditions.UNREADABLE_LINE]
            r["conditions"] = lines
            cstats["with_conditions" if lines else "none"] += 1
    print(f"    {dict(cstats)}")

    for it in items:
        it["obtain_ledger"] = plan_sources.obtain_ledger(it)

    print("[no-plan-apparel] dead routes:")
    prune_dead_routes.report(prune_dead_routes.attach(items, args.data_dir))
    # A row left with nothing once the dead routes are gone has no live source.
    keep = []
    for it in items:
        if it["obtain_routes"] or it["obtain_unlocks"]:
            keep.append(it)
        else:
            dropped.append(f"{it['name']} [only retired routes]")
    items = keep

    plan_sources.apply_tradeable_rules(items)
    plan_sources.tag_slasher_routes(items)
    for it in items:
        it["obtain_ledger"] = plan_sources.obtain_ledger(it)
        for r in it["obtain_routes"]:
            r.pop("_target", None)

    print("[no-plan-apparel] source tags:")
    plan_source_pill.report(plan_source_pill.attach(items))

    try:
        idx, staged = plan_images.load(args.outdir, args.data_dir)
        plan_images.report(plan_images.attach(items, idx, staged,
                                              folder_override=IMAGE_FOLDER))
    except Exception as exc:                       # noqa: BLE001 - never fatal
        print(f"  WARNING: image resolve skipped: {exc}", file=sys.stderr)
        for it in items:
            it.setdefault("image_dir", IMAGE_FOLDER)
            it.setdefault("images", [])

    items.sort(key=lambda it: it["name"].lower())
    out = {
        "version": SCHEMA,
        "generated": datetime.now(timezone.utc).isoformat(),
        "title": "Apparel Without Plans",
        "h1": "Collectables - Apparel Without Plans Checklist",
        "sub": ("Outfits, hats, masks and bandanas that have no plan — you can "
                "only get them from drops, containers, vendors, quests and events. "
                "Track which ones you have found."),
        "noun": {"one": "apparel item", "many": "apparel items"},
        "count": len(items),
        "source_files": {
            "ARMO": os.path.basename(bpo.newest("ARMO_Export_*_ARMOUR.tsv") or ""),
            "COBJ": os.path.basename(bpo.newest("COBJ_Export_*.tsv") or ""),
            "LVLI": os.path.basename(bpo.newest("LVLI_Export_*_LVLI_Entries.tsv") or ""),
        },
        "groups": [{"label": "", "items": items}],
    }
    os.makedirs(args.outdir, exist_ok=True)
    path = os.path.join(args.outdir, OUT_NAME)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f"[no-plan-apparel] wrote {path}  ({len(items)} items, "
          f"{len(dropped)} dropped with no source)")
    print("  source tags:", dict(collections.Counter(it.get("source_tag") for it in items)))
    print("  ledger rows:", dict(collections.Counter(
        r["label"] for it in items for r in it["obtain_ledger"])))
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump({"dropped_no_source": dropped}, f, ensure_ascii=False, indent=1)
    return out


if __name__ == "__main__":
    main()
