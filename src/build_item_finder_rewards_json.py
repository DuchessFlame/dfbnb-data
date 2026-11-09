#!/usr/bin/env python3
r"""
build_item_finder_rewards_json.py - "Rewards by Event or Quest" on the Item Finder.

Writes dist/item_finder/rewards.json, which the site's Item Finder endpoint
(inc/item-finder.php) fetches with the other item_finder files. The page shows
two dropdowns (type, then the event / activity / quest) and one answer at a time,
laid out like Brewmaster's 🎁 reward lists in Discord.

    { "schema": 1, "generated": "<content hash>",
      "types":   [ { "t": "Public Event", "label": "Public Events",
                     "items": [ { "k": "e:dangerous-pastimes-all-rewards", "n": "Dangerous Pastimes" } ] } ],
      "entries": { "<k>": { "t", "n", "quest": bool,
                            "link": { "text", "url" },
                            "parts": [ [ "<part name or null>", [ [ "<reward>", <rate %>, [ "<condition>" ] ] ] ] ] } } }

Sources (the same ones Brewmaster's reward lists read):
  dist/activities/by_page/*   activity / public event / seasonal ... pages: activityData.uniqueEventRewards,
                              kept when it is a plan or recipe, a title, apparel (dist/apparel_formids.json)
                              or a unique weapon / armour (name starts with a name in
                              dist/unique_weapons_armour/unique_weapons_armour.json, not cut).
                              Everyday loot: an item found on more than 5 pages (any pool) is left out.
  dist/unique_checklists/*    seasonal events, Daily Ops, Mutated Events and Expos: the Unique Rewards
                              Checklist is used as it is, minus its Miscellaneous section, and replaces
                              the activity-page version when both exist.
  dist/quest_unique_rewards.json   quests and questlines, part by part (skipped if not built yet).

Conditions use Brewmaster's wording: "Won't drop once you've learned it", the lowest
"Level N+", website Toggle notes dropped. The same item at several chances keeps the best.

Usage:  python src/build_item_finder_rewards_json.py
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
DIST = os.path.join(REPO, "dist")
OUT = os.path.join(DIST, "item_finder", "rewards.json")
SITE = "https://theduchessflame.com"
SPREAD_LIMIT = 5

TYPES = [  # (type, dropdown label, the folder in the page URL)
    ("Activity", "Activities", "activities"),
    ("Public Event", "Public Events", "public-events"),
    ("Seasonal Event", "Seasonal Events", "seasonal-events"),
    ("Quest", "Quests", None),
    ("Daily Ops", "Daily Ops", "daily-ops"),
    ("Expedition", "Expeditions", "expos"),
    ("Raid", "Raids", "raids"),
    ("Hideout", "Hideouts", "hideouts"),
    ("Infestation", "Infestations", "infestations"),
    ("Mutated Event", "Mutated Events", "mutated-events"),
]
TYPE_OF_FOLDER = {f: t for t, _l, f in TYPES if f}


def load(*parts, default=None):
    p = os.path.join(DIST, *parts)
    if not os.path.exists(p):
        return default
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def all_formids(node, out=None):
    out = set() if out is None else out
    if isinstance(node, dict):
        for k, v in node.items():
            if k.lower() == "formid" and isinstance(v, str):
                out.add(v.upper())
            else:
                all_formids(v, out)
    elif isinstance(node, list):
        for v in node:
            all_formids(v, out)
    return out


def conditions(raw):
    out, levels = [], []
    for c in raw or []:
        c = str(c)
        if c.startswith("Toggle"):
            continue
        m = re.match(r"Requires player level (\d+)\+", c)
        if m:
            levels.append(int(m.group(1)))
            continue
        if "already learned" in c or "already earned" in c:
            c = "Won’t drop once you’ve learned it"
        if c not in out:
            out.append(c)
    if levels:
        out.insert(0, "Level {}+".format(min(levels)))
    return out


def rate_of(v):
    v = 100.0 if v is None else float(v)
    v = round(v, 4)
    return int(v) if v == int(v) else v


def best_rows(items):
    """[(name, rate, conds, formid)] -> rows A-Z, best chance per item."""
    best = {}
    for name, rate, conds, fid in items:
        k = fid or name
        if k not in best or rate > best[k][1]:
            best[k] = (name, rate, conds)
    return [[n, r, c] for n, r, c in sorted(best.values(), key=lambda x: x[0].lower())]


def slug_of(url):
    return url.strip("/").split("/")[-1]


def main():
    entries, order = {}, []

    # ---- activity pages ----
    idx = (load("activities", "by_page", "_index.json") or {}).get("pages", {})
    pages = {}
    for key, fn in idx.items():
        if key.startswith("/"):
            pages.setdefault(fn, key)
    apparel = set((load("apparel_formids.json") or {}).get("formids", []))
    uwa = load("unique_weapons_armour", "unique_weapons_armour.json") or {}
    uniques = [x["name"] for x in uwa.get("items", []) if x.get("name") and not x.get("isCut")]

    raw, spread = {}, Counter()
    for fn, key in pages.items():
        pg = load("activities", "by_page", fn)
        if not pg:
            continue
        for f in all_formids(pg):
            spread[f] += 1
        page = pg.get("page") or pg
        ad = page.get("activityData") or {}
        name = page.get("title") or page.get("name") or ad.get("name") or fn
        raw[fn] = (key, name, ad.get("uniqueEventRewards") or [])

    def unique(x):
        n, k = x.get("name") or "", x.get("kind")
        if k in ("plan", "player_title", "camp_title") or n.startswith(("Plan:", "Recipe:", "Player Title:", "Camp Title:")):
            return True
        if (x.get("formid") or "").upper() in apparel:
            return True
        low = n.lower()
        return any(low.startswith(u.lower()) for u in uniques)

    for fn, (key, name, uer) in raw.items():
        folder = key.split("/")[2] if key.count("/") >= 2 else ""
        items = []
        for x in uer:
            fid = (x.get("formid") or "").upper()
            if spread[fid] > SPREAD_LIMIT or not unique(x):
                continue
            items.append((x["name"], rate_of(x.get("dropRate")), conditions(x.get("conditions")), fid))
        k = "e:" + slug_of(key)
        entries[k] = {"t": TYPE_OF_FOLDER.get(folder, "Activity"), "n": name, "quest": False,
                      "link": {"text": name + " All Rewards", "url": SITE + key.rstrip("/") + "/"},
                      "parts": [[None, best_rows(items)]]}

    # ---- unique rewards checklists (replace the activity-page version) ----
    cidx = (load("unique_checklists", "_index.json") or {}).get("pages", {})
    for key, fn in cidx.items():
        if not key.startswith("/"):
            continue
        cl = load("unique_checklists", fn)
        if not cl:
            continue
        folder = key.split("/")[2]
        items = []
        for r in cl.get("rows") or []:
            if (r.get("category") or "") == "Miscellaneous":
                continue
            items.append((r["name"], rate_of(r.get("dropRate")), conditions(r.get("conditions")),
                          (r.get("formid") or "").upper()))
        name = cl.get("eventName") or cl.get("posterTitle") or fn
        k = "e:" + slug_of(key)
        entries[k] = {"t": TYPE_OF_FOLDER.get(folder, "Seasonal Event"), "n": name, "quest": False,
                      "link": {"text": name + " All Rewards", "url": SITE + key.rstrip("/") + "/"},
                      "parts": [[None, best_rows(items)]]}

    # ---- quests ----
    qf = load("quest_unique_rewards.json") or {}
    for q in qf.get("quests", []):
        k = "q:" + re.sub(r"[^a-z0-9]+", "-", q["name"].lower()).strip("-")
        parts = [[p["name"], [[r["name"], rate_of(r.get("dropRate")), conditions(r.get("conditions"))]
                              for r in p.get("rewards", [])]] for p in q.get("parts", [])]
        link = {"text": q["name"] + " Guide", "url": q["url"]} if q.get("url") else \
               {"text": "Quests", "url": SITE + "/df/quests/"}
        entries[k] = {"t": "Quest", "n": q["name"], "quest": True, "link": link,
                      "parts": [p for p in parts if p[1]]}

    # ---- dropdowns ----
    types = []
    for t, label, _f in TYPES:
        items = sorted(({"k": k, "n": e["n"]} for k, e in entries.items() if e["t"] == t),
                       key=lambda x: x["n"].lower())
        if items:
            types.append({"t": t, "label": label, "items": items})

    body = {"types": types, "entries": dict(sorted(entries.items()))}
    stamp = hashlib.sha1(json.dumps(body, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
    out = {"schema": 1, "generated": stamp, **body}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    old = None
    if os.path.exists(OUT):
        try:
            with open(OUT, encoding="utf-8") as f:
                old = json.load(f).get("generated")
        except (OSError, ValueError):
            old = None
    if old == stamp:
        print("[item finder rewards] no change")
        return
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    n_q = sum(1 for e in entries.values() if e["quest"])
    print("[item finder rewards] wrote {}: {} entries ({} quests), {} types".format(
        OUT, len(entries), n_q, len(types)))


if __name__ == "__main__":
    main()
