#!/usr/bin/env python3
r"""
src/build_fishing_all_rewards_json.py  ->  dist/fishing_all_rewards.json

The Fishing All Rewards Checklist (/df/fishing/all-rewards-checklist/).
Every UNIQUE reward fishing hands out, from the two sources Duchess picked
(Nov 2026):

  1. Fishing challenges   every recipe / plan / title a fishing challenge
                          unlocks. Read straight from the game files:
                            COBJ  GNAM -> the challenge that gates the recipe
                                  (plushies, floats, bobbers, rods, displays,
                                  cooking recipes, player titles)
                            GMRW  ChallengeReward_* records whose Ref columns
                                  name a fishing challenge and hand out a BOOK
                                  (the Fish Quest I-IV reel plans)
                            PLYT  titles unlocked by HasCompletedChallenge
  2. Big Fish daily       the plan pools of Daily: Big Fish in a Small Pond
                          (dist/fishing_big_fish.json, built by
                          build_fishing_daily_rewards_json.py from the LVLIs).

Consumables (bait, caps, modules, treasury notes, score, regional weapon
mods, aid and ammo) are left out: they are not things you tick off.

The challenge list itself (names, counts, which are fishing) comes from
dist/fishing_challenges_checklist.json so this page and the Fishing
Challenges Checklist always agree. Names are the game's own (CNAM_FULL /
plan_master names); nothing is typed by hand.

Images resolve through image_index (the shared library), then the fishing
equipment folder, then the challenge page's own reward image - only files the
server listing says exist. Missing art is printed as the upload list.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
REPO = os.path.dirname(HERE)

import tsv_source                      # noqa: E402
import image_index as ii               # noqa: E402

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
TAG = "[fishing_all_rewards]"

CONSUMABLE = re.compile(r"\b(bait|caps|legendary module|treasury note|score|scrip)\b", re.I)


def _rows(pattern, tsv_dir):
    p = tsv_source.newest(os.path.join(tsv_dir, pattern), required=False)
    if not p:
        print(f"{TAG} missing {pattern}", file=sys.stderr)
        return []
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            with open(p, encoding=enc) as f:
                return list(csv.DictReader(f, delimiter="\t"))
        except UnicodeDecodeError:
            continue
    return []


def _fid(v):
    m = re.match(r"\s*([0-9A-Fa-f]{8})", str(v or ""))
    return m.group(1).upper() if m else ""


def _camel(s):
    return re.sub(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", s or "").strip()


def _load(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def cobj_kind(row):
    """What a challenge-gated COBJ makes, in the words the page shows."""
    edid = row.get("COBJ_EDID") or ""
    bnam = row.get("BNAM_EDID") or ""
    kw = row.get("FNAM_Keywords") or ""
    if re.search(r"PlayerTitle_co_", edid):
        return "Title"
    if re.search(r"CAMPTitle_co_", edid):
        return "CAMP Title"
    if "RodBobber" in edid and "_Display_" not in edid:
        return "Float" if edid.endswith("Float") else "Bobber"
    if "RodBase" in edid:
        return "Rod Skin"
    if "Souvenir" in kw or "_Display_" in edid or "Tinkers_Fishing" in edid:
        return "Display"
    if "Cooking" in bnam or "Cooking" in kw:
        return "Recipe"
    if "Armor" in bnam or "Backpack" in edid:
        return "Apparel"
    return "C.A.M.P."


def title_from_cobj(edid):
    m = re.search(r"(?:PlayerTitle|CAMPTitle)_co_CondProxy_(?:Prefix|Suffix|Both)_([A-Za-z0-9]+)", edid or "")
    return _camel(m.group(1)) if m else ""


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args(argv)
    tsv_dir = os.path.join(REPO, "tsv", "pts") if a.pts else os.path.join(REPO, "tsv")
    dist = os.path.join(REPO, "dist", "pts") if a.pts else os.path.join(REPO, "dist")
    out_path = os.path.join(dist, "fishing_all_rewards.json")

    chal = _load(os.path.join(dist, "fishing_challenges_checklist.json"), {}) or {}
    bigfish = _load(os.path.join(dist, "fishing_big_fish.json"), {}) or {}
    equip = _load(os.path.join(dist, "fishing_equipment.json"), {}) or {}
    pm = _load(os.path.join(REPO, "dist", "plan_master.json"), {}) or {}
    titles = (_load(os.path.join(REPO, "dist", "titles_player.json"), {}) or {}).get("items") or []
    idx = ii.load(verbose=False)

    pm_by_plan, pm_by_cobj = {}, {}
    for it in pm.get("items") or []:
        f = _fid((it.get("plan_item") or {}).get("formid"))
        if f:
            pm_by_plan[f] = it
        c = _fid((it.get("cobj") or {}).get("formid"))
        if c:
            pm_by_cobj.setdefault(c, it)
    title_by_cobj = {}
    for t in titles:
        c = _fid(((t.get("debug") or {}).get("cobjFormId")))
        if c:
            title_by_cobj[c] = t
    equip_by_fid = {}
    for k in ("rodSkins", "bobbersAndFloats"):
        for e in equip.get(k) or []:
            equip_by_fid[_fid(e.get("formId"))] = (k, e)
    eq_base = (equip.get("imageBases") or {}).get("bobber") or \
        "/wp-content/uploads/guide-images/fishing/rods-bobbers-float-skins/"

    # fishing challenges: id -> challenge dict (trackable ones only - the
    # daily/weekly score challenges hand out Score, nothing to tick)
    challenges = {}
    def walk(items):
        for it in items or []:
            challenges[_fid(it.get("form_id"))] = it
            walk(it.get("children"))
    for g in chal.get("groups") or []:
        if g.get("key") in ("lifetime", "bigfish"):
            walk(g.get("items"))

    def challenge_route(cid):
        c = challenges.get(cid) or {}
        req = c.get("required")
        nm = c.get("name") or ""
        try:
            req = int(float(req))
        except (TypeError, ValueError):
            req = 0
        return {"row": "Challenges",
                "name": nm + (f" x{req}" if req > 1 else ""),
                "rate": None, "challengeFormId": cid,
                "guides": c.get("guides") or []}, c

    # Some challenge recipes make a FURN whose name is not in the COBJ export
    # (CNAM_FULL blank). Fall back to the plan BOOK that shares the recipe's
    # last EDID token ("..._Fishermans" -> "Plan: Fishermans Cooking Station"),
    # then to the EDID token itself.
    book_by_tail = {}
    for row in _rows("BOOK_Export_*.tsv", tsv_dir):
        full = (row.get("FULL") or "").strip()
        tail = (row.get("EDID") or "").split("_")[-1].lower()
        if full and tail and re.match(r"^(Plan|Recipe):", full):
            book_by_tail.setdefault(tail, re.sub(r"^(Plan|Recipe):\s*", "", full))

    def cobj_name(row):
        full = (row.get("CNAM_FULL") or "").strip()
        if full:
            return full
        pmi = pm_by_cobj.get(_fid(row.get("COBJ_FormID"))) or {}
        if pmi.get("name"):
            return re.sub(r"^(Plan|Recipe):\s*", "", pmi["name"])
        tail = (row.get("COBJ_EDID") or "").split("_")[-1]
        return book_by_tail.get(tail.lower()) or _camel(tail)

    items = {}

    def image_for(fids_edids, chal_img=None):
        for fid, edid in fids_edids:
            e = idx.entry(fid, edid) if (fid or edid) else None
            if e and e.get("url"):
                return e["url"]
        for fid, _ in fids_edids:
            hit = equip_by_fid.get(_fid(fid))
            if hit:
                u = idx.present(eq_base + hit[1].get("imageFilename", ""))
                if u:
                    return u
        if chal_img:
            u = idx.present(chal_img)
            if u:
                return u
        return ""

    def add(key, rec, route):
        cur = items.get(key)
        if cur:
            if route["name"] not in [r["name"] for r in cur["routes"]]:
                cur["routes"].append(route)
            for r in rec.get("records") or []:
                if r not in cur["records"]:
                    cur["records"].append(r)
            if not cur["image"] and rec.get("image"):
                cur["image"] = rec["image"]
            return
        rec["routes"] = [route]
        items[key] = rec

    # ---- 1a. COBJ recipes gated by a fishing challenge -----------------------
    for row in _rows("COBJ_Export_*.tsv", tsv_dir):
        cid = _fid(row.get("GNAM_FormID"))
        if cid not in challenges or (row.get("COBJ_EDID") or "").startswith("zzz_"):
            continue
        route, c = challenge_route(cid)
        kind = cobj_kind(row)
        cnam_fid, cnam_edid = _fid(row.get("CNAM_FormID")), row.get("CNAM_EDID") or ""
        cobj_fid, cobj_edid = _fid(row.get("COBJ_FormID")), row.get("COBJ_EDID") or ""
        if kind in ("Title", "CAMP Title"):
            t = title_by_cobj.get(cobj_fid)
            name = (t or {}).get("title") or title_from_cobj(cobj_edid)
            if not name:
                continue
            key = "title:" + name.lower()
            rec = {"id": "FISHAR_" + ((t or {}).get("formId") or cobj_fid),
                   "name": name, "kind": kind, "group": "challenges",
                   "tradeable": False, "desc": "",
                   "image": (t or {}).get("imageUrl") or "",
                   "records": [{"label": "Title unlock (COBJ)", "formid": cobj_fid, "edid": cobj_edid}]
                              + ([{"label": "Title record (PLYT)", "formid": t.get("formId"), "edid": t.get("edid")}] if t else [])}
            add(key, rec, route)
            continue
        name = cobj_name(row)
        if not name:
            continue
        # A float's Tinker's souvenir copy rides on the same challenge as the
        # float itself; it is one reward, so it joins the float's row.
        base_kind = "Display" if kind == "Display" else kind
        key = "item:" + name.lower()
        pmi = pm_by_cobj.get(cobj_fid) or {}
        rec = {"id": "FISHAR_" + cnam_fid, "name": name, "kind": base_kind,
               "group": "challenges",
               "tradeable": bool(pmi.get("tradeable")) if pmi else False,
               "desc": "",
               "image": image_for([(cnam_fid, cnam_edid), (cobj_fid, cobj_edid)], c.get("image_url")),
               "records": [{"label": "Recipe (COBJ)", "formid": cobj_fid, "edid": cobj_edid},
                           {"label": "Creates", "formid": cnam_fid, "edid": cnam_edid}]}
        if key in items and base_kind == "Display" and items[key]["kind"] != "Display":
            items[key]["records"] += rec["records"]
            items[key]["alsoDisplay"] = True
            continue
        if key in items and items[key]["kind"] == "Display" and base_kind != "Display":
            rec["records"] += items[key]["records"]
            rec["alsoDisplay"] = True
            rec["routes"] = items[key]["routes"]
            items[key] = rec
            continue
        add(key, rec, route)

    # ---- 1b. GMRW challenge rewards that hand out a plan (BOOK) ----------------
    for row in _rows("GMRW_Export_*.tsv", tsv_dir):
        edid = row.get("EDID") or ""
        if "ChallengeReward" not in edid or edid.startswith("zzz"):
            continue
        item = row.get("RewardedItem") or ""
        if not item.endswith(":BOOK"):
            continue
        cids = [_fid(v) for k, v in row.items() if k and k.startswith("Ref") and ":CHAL" in (v or "")]
        cids = [c for c in cids if c in challenges]
        if not cids:
            continue
        bfid = _fid(item)
        pmi = pm_by_plan.get(bfid) or {}
        name = pmi.get("name") or ("Plan: " + _camel(item.split(":")[1].split("_")[-1]))
        route, c = challenge_route(cids[0])
        rec = {"id": "FISHAR_" + bfid, "name": name,
               "kind": "Rod Mod" if "FishingRod" in item else "Plan",
               "group": "challenges", "tradeable": bool(pmi.get("tradeable")),
               "desc": "",
               "image": image_for([(bfid, item.split(":")[1])], c.get("image_url")),
               "records": [{"label": "Plan (BOOK)", "formid": bfid, "edid": item.split(":")[1]},
                           {"label": "Challenge reward (GMRW)", "formid": _fid(row.get("FormID") or row.get("GMRW_FormID")), "edid": edid}]}
        add("item:" + name.lower(), rec, route)

    # ---- 1c. PLYT titles unlocked directly by a fishing challenge --------------
    for row in _rows("PLYT_Export_*.tsv", tsv_dir):
        title = (row.get("ANAM - Male Title") or row.get("ANAM") or "").strip()
        if not title:
            continue
        for v in row.values():
            for m in re.finditer(r"HasCompletedChallenge\([^)]*\[CHAL:([0-9A-Fa-f]{8})\]", str(v or "")):
                cid = m.group(1).upper()
                if cid not in challenges:
                    continue
                route, c = challenge_route(cid)
                rec = {"id": "FISHAR_" + _fid(row.get("FormID") or ""), "name": title,
                       "kind": "Title", "group": "challenges", "tradeable": False, "desc": "",
                       "image": "",
                       "records": [{"label": "Title record (PLYT)", "formid": _fid(row.get("FormID")), "edid": row.get("EDID")}]}
                add("title:" + title.lower(), rec, route)

    # ---- 2. Big Fish daily plan pools ------------------------------------------
    q = bigfish.get("quest") or {}
    qname = q.get("gameName") or "Daily: Big Fish in a Small Pond"
    for pool in bigfish.get("pools") or []:
        for it in pool.get("items") or []:
            if not it.get("isPlan"):
                continue
            bfid = _fid(it.get("formId"))
            pmi = pm_by_plan.get(bfid) or {}
            name = it.get("name") or pmi.get("name")
            kind = "Recipe" if name.lower().startswith("recipe") else \
                   ("Rod Mod" if pool.get("key") == "hookMods" else "C.A.M.P.")
            stops = None
            notes = pool.get("legends") or []
            if any("Stops dropping" in n for n in notes) or "leaves the pool" in (pool.get("note") or "") \
                    or "stops dropping" in (pool.get("note") or "").lower():
                stops = True
            if any("Keeps dropping" in n for n in notes) or "KEEPS dropping" in (pool.get("note") or ""):
                stops = False
            route = {"row": "Quests", "name": qname, "rate": it.get("dropRate"),
                     "pool": pool.get("title"), "poolChance": pool.get("poolChance"),
                     "stopsDropping": stops,
                     "guides": [{"title": "Big Fish in a Small Pond", "url": "/df/fishing/big-fish-small-pond/"}]}
            rec = {"id": "FISHAR_" + bfid, "name": name, "kind": kind, "group": "bigfish",
                   "tradeable": bool(pool.get("tradeable")) if pool.get("tradeable") is not None else bool(pmi.get("tradeable")),
                   "desc": it.get("buff") or "",
                   "image": image_for([(bfid, (pmi.get("plan_item") or {}).get("edid") or "")]
                                      + ([(_fid((pmi.get("cnam") or {}).get("formid")), (pmi.get("cnam") or {}).get("edid") or "")] if pmi.get("cnam") else [])),
                   "records": [{"label": "Plan (BOOK)", "formid": bfid, "edid": (pmi.get("plan_item") or {}).get("edid") or ""}]
                              + ([{"label": "Reward list (LVLI)", "formid": pool.get("lvliFormID") or pool.get("bookFormID"),
                                   "edid": pool.get("lvliEdid") or pool.get("bookEdid")}] if (pool.get("lvliFormID") or pool.get("bookFormID")) else [])}
            add("item:" + name.lower(), rec, route)

    out_items = sorted(items.values(), key=lambda r: (0 if r["group"] == "challenges" else 1,
                                                       r["name"].lower()))
    # an item from both sources keeps every route; its group is its first source
    groups = [
        {"key": "challenges", "label": "Fishing Challenge Rewards",
         "blurb": "Unlocked by finishing a fishing challenge. Once a challenge is done the reward is yours on that character."},
        {"key": "bigfish", "label": "Big Fish Daily Rewards",
         "blurb": "Plans from the reward pools of Daily: Big Fish in a Small Pond, handed in to Captain Raymond at Fisherman's Rest."},
    ]
    missing = [r["name"] for r in out_items if not r["image"]]
    out = {
        "version": 1,
        "generated": datetime.now(timezone.utc).isoformat(),
        "isPts": bool(a.pts),
        "count": len(out_items),
        "groups": groups,
        "items": out_items,
    }
    os.makedirs(dist, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
        f.write("\n")
    by = {}
    for r in out_items:
        by[r["group"]] = by.get(r["group"], 0) + 1
    print(f"{TAG} {len(out_items)} rewards {by} -> {os.path.relpath(out_path, REPO)}")
    if missing:
        print(f"{TAG} no image yet ({len(missing)}): " + ", ".join(missing))


if __name__ == "__main__":
    main()
