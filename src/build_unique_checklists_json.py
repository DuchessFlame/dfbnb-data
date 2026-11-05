#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_unique_checklists_json.py - the "Unique Rewards Checklist" at the
bottom of every non-activity All Rewards page (Nov 2026).

The Activities pages already have it (activities_checklist.py writes
activityData.checklist, df-bnb-activities.js draws it). This builder writes
the same rows, in the same shape, for the other All Rewards pages, so one
shared renderer (assets/df-bnb-unique-checklist.js + .css) draws them all in
the A Real Blast design:

  Expos           atlantic-city-expos-all-rewards, pitt-expos-all-rewards
  Mutated Events  mutated-events-all-rewards
  Daily Ops       daily-ops-all-rewards
  Seasonal Events fasnacht-day-parade, grahms-meat-cook, halloween-scorched,
                  holiday-scorched, invaders-from-beyond, mischief-night,
                  mothman-equinox, primal-cuts, the-big-bloom, the-slasher,
                  treasure-hunter  (all "-all-rewards")

Output, one small file per page (+ an index):
    <dist>/unique_checklists/<slug>.json
    <dist>/unique_checklists/_index.json

Nothing here is typed in by hand and there is no new rate maths. Every row,
rate, condition, image and tradeable flag is read from the page's own
generated JSON (which its own builder made from the xEdit TSVs):

  seasonal   <dist>/seasonal_events/seasonal_events_rewards_by_page.json
             (eventRewardTree isUniqueReward lists; The Slasher: page.uniques)
  mutated    <dist>/mutated_events/mutated_events_all_rewards.json (checklist)
  daily ops  <dist>/daily_ops/daily_ops_all_rewards.json (checklist.groups)
  expos      <dist>/expos/expos_rewards.json (pools) + <dist>/plan_master.json

WHAT GOES ON THE CHECKLIST - the same rule the activities checklist uses:
plans, recipes, titles, weapons, armour and apparel, weapon mod boxes and
other collectable MISC items (Holiday toys, legendary shards). Never ammo,
food, drink, alcohol, chems, currency or random legendary rolls (Duchess's
rule). The seasonal routing rules that used to live in
df-bnb-seasonal-events.js (Gamma Green Tea and the three Big Bloom base
flowers live in their own tables, Meat-Cook alcohol / cooked food) are
applied here too, so the checklist lists exactly what it listed before.

Usage:
    python src/build_unique_checklists_json.py              # live  (dist/)
    python src/build_unique_checklists_json.py --pts        # local PTS (dist/pts/)
    python src/build_unique_checklists_json.py --dist dist --label pts
                                   # CI PTS run: tsv/ is already PTS there and
                                   # the relocate step moves dist/ -> dist/pts/
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent

UPLOADS = "/wp-content/uploads/"
PLAN_IMG_BASE = UPLOADS + "guide-images/plan-checklist/"
EXPOS_IMG_BASE = UPLOADS + "guide-images/expos/"

CATEGORY_ORDER = ["Titles", "Plans & Recipes", "Weapons", "Armour & Apparel",
                  "Weapon Mods", "Miscellaneous"]

_TITLE_RE = re.compile(r"^\s*(player|camp)\s+title\s*:", re.I)
_PLAN_RE = re.compile(r"^\s*(plan|recipe|schematic)\s*:", re.I)
_STOPS_RE = re.compile(r"won.t drop if you.ve already (learned|earned)", re.I)

# Never unique rewards (ammo, aid / food / alcohol / chems, currency,
# random legendary rolls, keys and holotapes the event hands out).
NEVER_SIGS = {"AMMO", "ALCH", "CNCY", "LGDI", "KEYM", "NOTE", "LVLI"}
NEVER_EDID_RE = re.compile(r"^(Treasury_Note|LegendaryTokens|Caps001)$", re.I)

# Seasonal routing (was hard-coded in df-bnb-seasonal-events.js):
# these leave the Unique list for their own tables.
SEASONAL_NOT_UNIQUE = {
    "007B2736",   # Gamma Green Tea -> Default Event Rewards (The Big Bloom)
    "007ACA49",   # Carnal Weeper   -> Base Flowers (The Big Bloom)
    "007ACA4A",   # Crystalcup      -> Base Flowers
    "007ACA4B",   # Radlily         -> Base Flowers
}

SLASHER_SECTION = "Slasher Season Rewards"


# ── per-page settings (wording only - every number comes from the data) ────

def _the(name):
    """'the X' - but never 'the The Big Bloom'."""
    return name if re.match(r"^the\s", name or "", re.I) else "the " + name


def _event(name):
    return "when you complete " + _the(name) + " event"


PAGES = {
    # slug: (kind, url, where-phrase builder, rate note)
    "atlantic-city-expos-all-rewards": ("expos", "/df/expos/atlantic-city/atlantic-city-expos-all-rewards/"),
    "pitt-expos-all-rewards": ("expos", "/df/expos/the-pitt/pitt-expos-all-rewards/"),
    "mutated-events-all-rewards": ("mutated", "/df/mutated-events/mutated-events-all-rewards/"),
    "daily-ops-all-rewards": ("daily_ops", "/df/daily-ops/daily-ops-all-rewards/"),
    "fasnacht-day-parade-all-rewards": ("seasonal", "/df/seasonal-events/fasnacht-day-parade/fasnacht-day-parade-all-rewards/"),
    "grahms-meat-cook-all-rewards": ("seasonal", "/df/seasonal-events/meat-week/grahms-meat-cook-all-rewards/"),
    "halloween-scorched-all-rewards": ("seasonal", "/df/seasonal-events/halloween-scorched/halloween-scorched-all-rewards/"),
    "holiday-scorched-all-rewards": ("seasonal", "/df/seasonal-events/holiday-scorched/holiday-scorched-all-rewards/"),
    "invaders-from-beyond-all-rewards": ("seasonal", "/df/seasonal-events/invaders-from-beyond/invaders-from-beyond-all-rewards/"),
    "mischief-night-all-rewards": ("seasonal", "/df/seasonal-events/mischief-night/mischief-night-all-rewards/"),
    "mothman-equinox-all-rewards": ("seasonal", "/df/seasonal-events/mothman-equinox/mothman-equinox-all-rewards/"),
    "primal-cuts-all-rewards": ("seasonal", "/df/seasonal-events/meat-week/primal-cuts-all-rewards/"),
    "the-big-bloom-all-rewards": ("seasonal", "/df/seasonal-events/the-big-bloom/the-big-bloom-all-rewards/"),
    "the-slasher-all-rewards": ("slasher", "/df/seasonal-events/the-slasher/the-slasher-all-rewards/"),
    "treasure-hunter-all-rewards": ("seasonal", "/df/seasonal-events/hunt-for-the-treasure-hunter/treasure-hunter-all-rewards/"),
}

# Where the reward comes from, for the How to Obtain lead line and the note
# under the rate table. Pages not listed are normal "complete the event" ones.
WHERE = {
    "treasure-hunter-all-rewards": ("from a Mole Miner Pail (found or crafted) during Hunt for the Treasure Hunter",
                                    "Chance per pail opened."),
    "holiday-scorched-all-rewards": ("from a Holiday Gift (found or crafted) during Holiday Scorched",
                                     "Chance per gift opened."),
    "halloween-scorched-all-rewards": ("from a Spooky Treat Bag during Halloween Scorched",
                                       "Chance per bag opened, or per masked Spooky Scorched looted."),
    "mutated-events-all-rewards": ("from a Mutated Package or Mutated Party Pack, earned by completing a Mutated Public Event",
                                   "Chance per package opened."),
    "daily-ops-all-rewards": ("when you clear a Daily Op at level 50 or above",
                              "Chance per clear. Ranks stack - an Elder clear also rolls the Paladin and Knight lists."),
    "the-slasher-all-rewards": ("during the Slasher season, from the sources below",
                                "Best chance from each source."),
}
WHERE_SLASHER_SECTION = "from a Slasher-masked Spooky Scorched while the Slasher season is running"


# ── helpers ────────────────────────────────────────────────────────────────

def _load(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        print("  [unique_checklists] {} not read: {}".format(path, e))
        return None


def _num(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(str(v).rstrip("%"))
    except (TypeError, ValueError):
        return default


def _fid(v):
    s = str(v or "").strip().upper()
    return s[-8:].zfill(8) if re.fullmatch(r"[0-9A-F]{1,8}", s) else s


def _sig_for(it):
    sig = str(it.get("sig") or "").upper()
    if sig:
        return sig
    name = it.get("name") or ""
    if _PLAN_RE.match(name) or _TITLE_RE.match(name):
        return "BOOK"
    kind = str(it.get("kind") or "").lower()
    return {"plan": "BOOK", "recipe": "BOOK", "title": "BOOK", "apparel": "ARMO",
            "armour": "ARMO", "armor": "ARMO", "weapon": "WEAP"}.get(kind, "")


def category_for(name, sig, edid):
    sig = (sig or "").upper()
    edid = (edid or "").lower()
    if sig == "BOOK":
        return "Titles" if _TITLE_RE.match(name or "") else "Plans & Recipes"
    if sig == "WEAP":
        return "Weapons"
    if sig == "ARMO":
        return "Armour & Apparel"
    if sig == "MISC":
        return "Weapon Mods" if edid.startswith("miscmod_") else "Miscellaneous"
    return None


def _keep(it, sig):
    if sig in NEVER_SIGS or it.get("consumable"):
        return False
    if NEVER_EDID_RE.match(str(it.get("edid") or "")):
        return False
    return True


def _clean_conds(conds):
    out = []
    for c in conds or []:
        s = str(c or "").strip()
        if s and not re.search(r"GetRandomPercent", s, re.I) and s not in out:
            out.append(s)
    return out


def _obtain_sentence(cat, name, where):
    if cat == "Titles":
        return "A title that can be awarded " + where + "."
    if cat == "Plans & Recipes":
        noun = "A recipe" if re.match(r"^\s*recipe\s*:", name or "", re.I) else "A plan"
        return noun + " that can drop " + where + "."
    if cat == "Weapons":
        return "A weapon that can drop " + where + "."
    if cat == "Weapon Mods":
        return "A weapon mod box that can drop " + where + "."
    return "Can drop " + where + "."


def _new_row(it, name, sig, cat, conds):
    row = {
        "formid": _fid(it.get("formid") or it.get("formId") or it.get("id")),
        "edid": it.get("edid") or "",
        "name": name,
        "sig": sig,
        "category": cat,
        "conditions": conds,
    }
    for k in ("tradeable", "unsellable", "customModName", "customModDescription", "minLevel"):
        if it.get(k) is not None:
            row[k] = it[k]
    if it.get("modSlots"):
        row["modSlots"] = it["modSlots"]
    if it.get("isNew"):
        row["isNew"] = True
    if it.get("releaseYear"):
        row["releaseYear"] = it["releaseYear"]
    return row


def _finish(rows, where, sections=None):
    """Stops-dropping, obtain line, key, best rate, sort."""
    seen_keys = {}
    for r in rows:
        r["stopsDropping"] = bool(r.pop("_stops", False)) or any(_STOPS_RE.search(c) for c in r["conditions"])
        r["obtain"] = _obtain_sentence(r["category"], r["name"],
                                       WHERE_SLASHER_SECTION if r.get("section") == SLASHER_SECTION and where != WHERE["the-slasher-all-rewards"][0] else where)
        r["dropRate"] = max([s["rate"] for s in r.get("sources") or [] if not s.get("notDropped")] or [0])
        r["images"] = [u for u in (r.get("images") or []) if u][:4]
        # Tick key: the FormID, same as every older checklist on these pages,
        # so ticks people already made carry straight over.
        k = r["formid"]
        if k in seen_keys and r.get("customModName"):
            k = k + "-" + re.sub(r"[^a-z0-9]+", "-", r["customModName"].lower()).strip("-")
        seen_keys[k] = True
        r["key"] = k
    rank = {c: i for i, c in enumerate(CATEGORY_ORDER)}
    srank = {s: i for i, s in enumerate(sections or [])}
    rows.sort(key=lambda r: (srank.get(r.get("section"), -1) if r.get("section") else -1,
                             rank.get(r["category"], 99), r["name"].lower()))
    return rows


# ── seasonal events ────────────────────────────────────────────────────────

def _seasonal_rows(slug, page, pm):
    name = page.get("name") or slug
    where = WHERE.get(slug, (_event(name),))[0]
    images = {}
    for rw in page.get("rewards") or []:
        f = _fid(rw.get("formId") or rw.get("formid"))
        imgs = list(rw.get("images") or []) or ([rw["imageUrl"]] if rw.get("imageUrl") else [])
        if f and imgs:
            images.setdefault(f, imgs)
    season_tiers = set(page.get("seasonTiers") or [])

    # Tier order as the page draws it (first seen across the unique lists).
    tier_order = []
    for node in page.get("eventRewardTree") or []:
        if not node.get("isUniqueReward"):
            continue
        for it in node.get("items") or []:
            for t in it.get("tiers") or []:
                tn = t.get("tier")
                if tn and tn not in tier_order:
                    tier_order.append(tn)

    rows, by_fid = [], {}
    for node in page.get("eventRewardTree") or []:
        if not node.get("isUniqueReward"):
            continue
        for it in node.get("items") or []:
            fid = _fid(it.get("formid") or it.get("formId"))
            if not fid or fid in SEASONAL_NOT_UNIQUE:
                continue
            # Meat-Cook alcohol / cooked food live in their own tables.
            if it.get("group") in ("alcohol", "cooked"):
                continue
            sig = _sig_for(it)
            if not _keep(it, sig):
                continue
            iname = (it.get("name") or "").strip()
            cat = category_for(iname, sig, it.get("edid"))
            if not cat or not iname:
                continue
            row = by_fid.get(fid)
            if row is None:
                row = _new_row(it, iname, sig, cat, _clean_conds(it.get("conditions")))
                row["_src"] = {}
                row["images"] = images.get(fid) or []
                p = pm.get(fid) or {}
                if sig == "BOOK" and isinstance(p.get("tradeable"), bool) and not isinstance(row.get("tradeable"), bool):
                    row["tradeable"] = p["tradeable"]
                if p.get("stops_dropping") is True:
                    row["_stops"] = True
                by_fid[fid] = row
                rows.append(row)

            tiers = [t for t in (it.get("tiers") or []) if t.get("tier")]
            if tiers:
                for t in tiers:
                    r = _num(t.get("rate"), 0.0)
                    prev = row["_src"].get(t["tier"])
                    if prev is None or r > prev:
                        row["_src"][t["tier"]] = r
                # Slasher-season-only items (Halloween: the masked Spooky
                # Scorched) are listed last, under their own line.
                if season_tiers and all(t["tier"] in season_tiers for t in tiers):
                    row["section"] = SLASHER_SECTION
            elif it.get("bratsnacht") and (it.get("rateOn") is not None or it.get("rateOff") is not None):
                # Fasnacht: normal rate and the Bratsnacht rate.
                row["_src"][name] = _num(it.get("rateOff"), 0.0)
                row["_src"]["During Bratsnacht"] = _num(it.get("rateOn"), 0.0)
                b = str(it.get("bratsnacht"))
                if b == "only-on":
                    row["note"] = "Bratsnacht only"
                elif b == "only-off":
                    row["note"] = "Not during Bratsnacht"
            else:
                label = name
                r = _num(it.get("dropRate"), 0.0)
                prev = row["_src"].get(label)
                if prev is None or r > prev:
                    row["_src"][label] = r

    src_order = list(tier_order)
    for r in rows:
        srcs = r.pop("_src")
        out = []
        if tier_order and any(n in srcs for n in tier_order):
            for n in tier_order:
                if r.get("section") != SLASHER_SECTION and n in season_tiers:
                    if n not in srcs:
                        continue
                s = {"name": n, "rate": round(srcs.get(n, 0.0), 6)}
                if n not in srcs or srcs[n] <= 0:
                    s["notDropped"] = True
                out.append(s)
        for n, v in srcs.items():
            if n in src_order:
                continue
            s = {"name": n, "rate": round(v, 6)}
            if v <= 0:
                s["notDropped"] = True
            out.append(s)
        r["sources"] = out
    sections = [SLASHER_SECTION] if any(r.get("section") for r in rows) else []
    return _finish(rows, where, sections), sections


def _slasher_rows(slug, page, pm):
    where = WHERE[slug][0]
    rows = []
    for u in page.get("uniques") or []:
        sig = _sig_for(u)
        if not _keep(u, sig):
            continue
        iname = (u.get("name") or "").strip()
        cat = category_for(iname, sig, u.get("edid"))
        if not cat or not iname:
            continue
        conds = []
        sources = []
        for s in u.get("sources") or []:
            conds += [c for c in _clean_conds(s.get("conditions")) if c not in conds]
            r = _num(s.get("rate"), 0.0)
            src = {"name": s.get("name") or s.get("route") or "", "rate": round(r, 6)}
            if s.get("route"):
                src["route"] = s["route"]
            if r <= 0:
                src["notDropped"] = True
            sources.append(src)
        row = _new_row(u, iname, sig, cat, conds)
        row["sources"] = sources
        row["images"] = list(u.get("pics") or [])
        if u.get("stopsDropping") is True:
            row["_stops"] = True
        rows.append(row)
    return _finish(rows, where), []


# ── mutated events ─────────────────────────────────────────────────────────

def _mutated_rows(slug, page, pm):
    where = WHERE[slug][0]
    live_packs = [p.get("name") for p in page.get("packs") or []
                  if p.get("name") and not (p.get("cut") or p.get("isCut")
                                            # Double mutations are cut content (Duchess)
                                            or p.get("mutationType") == "double")]
    rows = []
    for it in page.get("checklist") or []:
        sig = _sig_for(it)
        if not _keep(it, sig):
            continue
        iname = (it.get("name") or "").strip()
        cat = category_for(iname, sig, it.get("edid"))
        if not cat or not iname:
            continue
        row = _new_row(it, iname, sig, cat, _clean_conds(it.get("conditions")))
        got = {t.get("tier"): _num(t.get("rate"), 0.0) for t in it.get("tiers") or [] if t.get("tier")}
        row["sources"] = []
        for pn in live_packs:
            s = {"name": pn, "rate": round(got.get(pn, 0.0), 6)}
            if got.get(pn, 0.0) <= 0:
                s["notDropped"] = True
            row["sources"].append(s)
        row["images"] = list(it.get("images") or []) or ([it["imageUrl"]] if it.get("imageUrl") else [])
        if it.get("dropPersistence") == "stops":
            row["_stops"] = True
        rows.append(row)
    return _finish(rows, where), []


# ── daily ops ──────────────────────────────────────────────────────────────

def _daily_ops_rows(slug, data, pm):
    where = WHERE[slug][0]
    cl = data.get("checklist") or {}
    rows, sections = [], []
    for g in cl.get("groups") or []:
        section = None
        if g.get("id") != "dailyops":
            section = g.get("label") or g.get("id")
            if section not in sections:
                sections.append(section)
        for it in g.get("items") or []:
            sig = _sig_for(it)
            if not _keep(it, sig):
                continue
            iname = (it.get("name") or "").strip()
            cat = category_for(iname, sig, it.get("edid"))
            if not cat or not iname:
                continue
            row = _new_row(it, iname, sig, cat, _clean_conds(it.get("conditions")))
            row["sources"] = []
            for rr in it.get("rates") or []:
                r = _num(rr.get("rate"), 0.0)
                s = {"name": rr.get("label") or "", "rate": round(r, 6)}
                if r <= 0:
                    s["notDropped"] = True
                row["sources"].append(s)
            row["images"] = list(it.get("images") or [])
            if it.get("stopsDropping") is True:
                row["_stops"] = True
            if section:
                row["section"] = section
            rows.append(row)
    return _finish(rows, where, sections), sections


# ── expos ──────────────────────────────────────────────────────────────────

def _expo_source_name(pool):
    t = " ".join([pool.get("title") or "", pool.get("lvliEdid") or ""]).lower()
    if "repeatable" in t:
        return "Repeatable Mission Reward"
    return "Mission Reward"


def _expos_rows(slug, page, pm, idx):
    name = re.sub(r"\s+Expos?$", "", page.get("name") or slug, flags=re.I)
    where = "when you complete a mission on " + _the(name) + " Expedition"
    rows, by_fid, src_order = [], {}, []
    for pool in page.get("pools") or []:
        sname = _expo_source_name(pool)
        if sname not in src_order:
            src_order.append(sname)
        pchance = _num(pool.get("poolChance"), 100.0) / 100.0
        pconds = _clean_conds(str(pool.get("conditionSummary") or "").split(" | "))
        for it in pool.get("items") or []:
            fid = _fid(it.get("formid"))
            iname = (it.get("name") or "").strip()
            p = pm.get(fid) or {}
            if not iname or re.fullmatch(r"[0-9A-F]{8}", iname):
                continue
            sig = _sig_for(it) or ("BOOK" if p else "")
            if not _keep(it, sig):
                continue
            cat = category_for(iname, sig, it.get("edid") or "")
            if not cat:
                continue
            row = by_fid.get(fid)
            if row is None:
                conds = []
                if p.get("stops_dropping") is True:
                    conds.append("Won’t drop if you’ve already learned " + iname)
                row = _new_row(dict(it, edid=(p.get("plan_item") or {}).get("edid", "") if isinstance(p.get("plan_item"), dict) else ""),
                               iname, sig, cat, conds)
                if isinstance(p.get("tradeable"), bool):
                    row["tradeable"] = p["tradeable"]
                row["images"] = [im if str(im).startswith("/") else PLAN_IMG_BASE + (p.get("image_dir") or "") + "/" + im + ".avif"
                                 for im in (p.get("images") or []) if im]
                row["_src"] = {}
                row["_pconds"] = None
                by_fid[fid] = row
                rows.append(row)
            r = _num(it.get("dropRate"), 0.0) * pchance
            prev = row["_src"].get(sname)
            if prev is None or r > prev:
                row["_src"][sname] = r
            # Drops if EITHER pool's conditions are met: keep only what both ask.
            row["_pconds"] = pconds if row["_pconds"] is None else [c for c in row["_pconds"] if c in pconds]
    for r in rows:
        srcs = r.pop("_src")
        r["conditions"] = r["conditions"] + [c for c in (r.pop("_pconds") or []) if c not in r["conditions"]]
        r["sources"] = []
        # The first-run mission reward reads before the repeatable one.
        for n in sorted(src_order, key=lambda x: x != "Mission Reward"):
            s = {"name": n, "rate": round(srcs.get(n, 0.0), 6)}
            if n not in srcs or srcs[n] <= 0:
                s["notDropped"] = True
            r["sources"].append(s)
    return _finish(rows, where), []


# ── main ───────────────────────────────────────────────────────────────────

def _plan_master(dist):
    pm = _load(dist / "plan_master.json") or _load(REPO_ROOT / "dist" / "plan_master.json") or {}
    out = {}
    for it in pm.get("items") or []:
        fid = str(it.get("id") or "").split("_")[-1].upper()
        if fid:
            out[fid] = it
    return out


def _image_index():
    try:
        if str(SRC_DIR) not in sys.path:
            sys.path.insert(0, str(SRC_DIR))
        import image_index
        return image_index.load(verbose=False)
    except Exception as e:                # noqa: BLE001
        print("  [unique_checklists] image index not loaded: {}".format(e))
        return None


SEASONAL_IMG_BASE = UPLOADS + "guide-images/seasonal-events/"
MUTATED_IMG_BASE = UPLOADS + "guide-images/mutated-events/reward-images/"
DAILY_OPS_IMG_BASE = UPLOADS + "guide-images/daily-ops/"


def _event_dirs(slug, kind, page):
    if kind == "expos":
        return [EXPOS_IMG_BASE + slug.replace("-expos-all-rewards", "") + "/"]
    if kind == "mutated":
        return [MUTATED_IMG_BASE]
    if kind == "daily_ops":
        return [DAILY_OPS_IMG_BASE]
    ev = (page or {}).get("eventSlug") or slug.replace("-all-rewards", "")
    dirs = [SEASONAL_IMG_BASE + ev + "/"]
    # the page's own folders (meat-week/meat-cook/rewards/, treasure-hunters/ ...)
    for r in (page or {}).get("rewards") or []:
        for u in (r.get("images") or []) + ([r["imageUrl"]] if r.get("imageUrl") else []):
            d = str(u).rsplit("/", 1)[0] + "/"
            if d.startswith(SEASONAL_IMG_BASE) and d not in dirs:
                dirs.append(d)
    return dirs


def _images_and_trade(rows, slug, kind, page, pm, idx, stats):
    """Plan-checklist art as a fallback, then the shared image index (keeps
    only files the server has, finds library / other-page art, placeholders),
    then tradeable for any row still without it (activities_checklist's
    NonPlayerTradable rule) - the pill is on every row or none."""
    for r in rows:
        p = pm.get(r["formid"]) or {}
        extra = [im if str(im).startswith("/") else PLAN_IMG_BASE + (p.get("image_dir") or "") + "/" + im + ".avif"
                 for im in (p.get("images") or []) if im]
        r["images"] = list(r.get("images") or []) + [u for u in extra if u not in (r.get("images") or [])]
        if isinstance(p.get("tradeable"), bool) and not isinstance(r.get("tradeable"), bool):
            r["tradeable"] = p["tradeable"]
    if idx is not None:
        try:
            idx.apply(rows, event_dirs=_event_dirs(slug, kind, page), page=slug, stats=stats)
        except Exception as e:            # images are never fatal
            print("  [unique_checklists] image index skipped on {}: {}".format(slug, e))
    try:
        if str(SRC_DIR) not in sys.path:
            sys.path.insert(0, str(SRC_DIR))
        import activities_checklist
        activities_checklist.add_tradeable(rows, stats=stats)
    except Exception as e:                # noqa: BLE001
        print("  [unique_checklists] tradeable lookup skipped on {}: {}".format(slug, e))
    for r in rows:
        r["images"] = [u for u in (r.get("images") or []) if u][:4]
    return rows


def build(dist, channel):
    pm = _plan_master(dist)
    idx = _image_index()
    seasonal = ((_load(dist / "seasonal_events" / "seasonal_events_rewards_by_page.json") or {}).get("byPage") or {})
    mutated = ((_load(dist / "mutated_events" / "mutated_events_all_rewards.json") or {}).get("byPage") or {})
    daily = _load(dist / "daily_ops" / "daily_ops_all_rewards.json") or {}
    expos = ((_load(dist / "expos" / "expos_rewards.json") or {}).get("byPage") or {})

    out_dir = dist / "unique_checklists"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    index = {"generated": stamp, "channel": channel, "pages": {}}
    failed = []
    img_stats = {}

    for slug, (kind, url) in PAGES.items():
        try:
            if kind == "seasonal":
                page = seasonal.get(slug)
                rows, sections = _seasonal_rows(slug, page, pm) if page else (None, [])
                ev = (page or {}).get("name")
            elif kind == "slasher":
                page = seasonal.get(slug)
                rows, sections = _slasher_rows(slug, page, pm) if page else (None, [])
                ev = (page or {}).get("name")
            elif kind == "mutated":
                page = mutated.get(slug)
                rows, sections = _mutated_rows(slug, page, pm) if page else (None, [])
                ev = "Mutated Events"
            elif kind == "daily_ops":
                rows, sections = _daily_ops_rows(slug, daily, pm) if daily else (None, [])
                ev = daily.get("name") or "Daily Ops"
            else:
                page = expos.get(slug)
                rows, sections = _expos_rows(slug, page, pm, idx) if page else (None, [])
                ev = (page or {}).get("name")
        except Exception as e:            # one bad page never stops the rest
            print("  [unique_checklists] {} FAILED: {}".format(slug, e))
            failed.append(slug)
            continue
        if rows is None:
            print("  [unique_checklists] {}: source page missing - left as it was".format(slug))
            failed.append(slug)
            continue
        src_page = daily if kind == "daily_ops" else (expos.get(slug) if kind == "expos" else
                                                      (mutated.get(slug) if kind == "mutated" else seasonal.get(slug)))
        _images_and_trade(rows, slug, kind, src_page, pm, idx, img_stats)
        ev = ev or slug
        payload = {
            "version": 1,
            "generated": stamp,
            "channel": channel,
            "slug": slug,
            "url": url,
            "kind": kind,
            "eventName": ev,
            "title": "Unique Rewards Checklist",
            "posterTitle": ev + " Unique Rewards",
            "rateNote": WHERE.get(slug, (None, "Chance per event completion."))[1],
            "sections": sections,
            "count": len(rows),
            "rows": rows,
        }
        p = out_dir / (slug + ".json")
        tmp = p.with_suffix(".json.part")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        with open(tmp, encoding="utf-8") as f:
            json.load(f)
        os.replace(tmp, p)
        index["pages"][url.rstrip("/")] = slug + ".json"
        index["pages"][slug] = slug + ".json"
        cats = {}
        for r in rows:
            cats[r["category"]] = cats.get(r["category"], 0) + 1
        print("  {:<36} {:>4} rows  {}".format(slug, len(rows),
              ", ".join("{} {}".format(v, k) for k, v in cats.items()) or "-"))

    print("  images / tradeable: " + ", ".join("{} {}".format(v, k) for k, v in sorted(img_stats.items())))
    with open(out_dir / "_index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=1)
    return failed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist", default=None, help="dist folder to read the page JSONs from and write into")
    ap.add_argument("--pts", action="store_true", help="local PTS run: read and write dist/pts/")
    ap.add_argument("--label", default=None, help="channel label written into the JSON (CI PTS run: pts)")
    a = ap.parse_args()
    if a.dist:
        dist = Path(a.dist)
        if not dist.is_absolute():
            dist = REPO_ROOT / dist
    else:
        dist = REPO_ROOT / "dist" / ("pts" if a.pts else "")
    channel = a.label or ("pts" if (a.pts or os.environ.get("DFBNB_CHANNEL", "").strip().lower() == "pts") else "live")
    print("[unique_checklists] {} -> {}".format(channel, dist / "unique_checklists"))
    failed = build(dist, channel)
    if failed:
        print("[unique_checklists] not written: {}".format(", ".join(failed)))
    if len(failed) == len(PAGES):
        sys.exit(1)


if __name__ == "__main__":
    main()
