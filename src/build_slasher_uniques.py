#!/usr/bin/env python3
r"""
build_slasher_uniques.py - the Unique Rewards checklist on The Slasher All Rewards page.

    python src/build_slasher_uniques.py                 # live  (dist/)
    python src/build_slasher_uniques.py --dist dist/pts # PTS   (dist/pts/)

Duchess, 1 Nov 2026: the page lists ONLY the event's unique rewards, in ONE root
expand, A to Z, each with its picture, How to Obtain and Technical. No XP, no
caps, no legendary gear rolls, no generic magazines.

Nothing here is typed in by hand except the route wording for each Slasher
activity (SOURCES below). Everything else is read from JSON the other builders
already wrote, so the list follows the game files on every live AND PTS build:

  seasonal_events/seasonal_events_rewards_by_page.json
      the-slasher-all-rewards  eventRewardTree  every activity's reward pool,
                                                resolved by rng76 (rates +
                                                drop conditions per item)
      the-slasher-guide        phases / quests  quest steps, what starts each
                                                quest, challenge rewards and
                                                the finale's paint plans
  plan_master.json            plan rows for challenge-reward plans (the bobber,
                              the radio) that no reward pool hands out
  titles_player.json / titles_camp.json   title records (Laughing)
  unique_weapons_armour/unique_weapons_armour.json
                              each named weapon's OWN picture (the reward pools
                              only know the base weapon - Old Guard is a 10mm
                              SMG to them)
  image_index.json            which pictures are really on the server

Writes page["uniques"] onto every byPage key of the-slasher-all-rewards (the
file stores the page under its slug and both URL spellings). Item pictures go
in "pics", not "images", so apply_image_index.py (which re-points every
"images" list at the shared library by FormID) cannot swap a named weapon back
to its base weapon's picture.

Runs after build_seasonal_events_json.py, the plan / titles builders and
build_unique_weapons_armour_json.py in dfbnb-patch-build.yml and
dfbnb-pts-build.yml, and again in build-images.yml so new uploads show up.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
REPO = os.path.dirname(HERE)

TAG = "[slasher_uniques]"
SLUG = "the-slasher-all-rewards"
GUIDE_SLUG = "the-slasher-guide"
SEASONAL = os.path.join("seasonal_events", "seasonal_events_rewards_by_page.json")
UPLOADS = "/wp-content/uploads/"

# ── Each reward pool on the page -> How to Obtain ledger row ────────────────
# key    = the activity label build_seasonal_events_json.py gives the pool
# route  = the ledger row (checklist-style-guide route rules)
# name   = the source line the reader sees (the in-game name, untouched)
# quest  = EDID of the quest whose steps explain the route (slasher guide)
# short  = the row's source pill
SOURCES = {
    "Masked Truth (Week 1)": {
        "route": "Quests", "name": "The Slasher: Masked Truth", "quest": "SDOW_MQ01_Bodies", "short": "Quest"},
    "Secrets to the Grave (Week 2)": {
        "route": "Quests", "name": "The Slasher: Secrets to the Grave", "quest": "SDOW_MQ02_Graves", "short": "Quest"},
    "Out of the Shadows (Week 3)": {
        "route": "Quests", "name": "The Slasher: Out of the Shadows", "quest": "SDOW_MQ04_Infestations", "short": "Quest"},
    "Blood Will Have Blood (Week 4)": {
        "route": "Quests", "name": "The Slasher: Blood Will Have Blood", "quest": "SDOW_MQ05_Headhunt", "short": "Quest"},
    "Infestations — Pint-Sized Phantom Boss": {
        "route": "Events & Activities", "name": "Infestations - Pint-Sized Phantom boss", "short": "Event"},
    "Head Hunt — The Reborn Slasher": {
        "route": "Enemies", "name": "The Reborn Pint-Sized Slasher (Head Hunts)", "short": "Enemy"},
    "Disturbed Grave (Repeatable)": {
        "route": "Events & Activities", "name": "Repeatable: Disturbed Grave", "quest": "SDOW_SQ01_Graves_Repeatable", "short": "Event"},
    "The Way of the Wicked (Daily Ops)": {
        "route": "Events & Activities", "name": "Daily Ops (while The Slasher is running)", "short": "Ops",
        "dailyOps": True},
    "Slasher Masked Spooky Scorched": {
        "route": "Enemies", "name": "Slasher-masked Spooky Scorched", "short": "Enemy"},
    "Mischief Night — Uninvited Guest": {
        "route": "Enemies", "name": "Uninvited Pint-Sized Phantom (Mischief Night)", "short": "Enemy"},
    "Pint-Sized Phantom Pack (Enemy Drops)": {
        "route": "Enemies", "name": "Pint-Sized Phantoms", "short": "Enemy"},
}
FINALE_DROP = {"route": "Enemies", "short": "Enemy"}
CHALLENGE = {"route": "Challenges", "short": "Challenge"}

# Not unique rewards: legendary gear rolls, caps, and the generic magazines the
# Phantoms share with every other enemy list.
SKIP_SIG = {"LGDI", "CNCY"}
SKIP_EDID = re.compile(r"^(Magazine_|LegendaryItems_|Caps\d*$)", re.I)
SKIP_NAME = re.compile(r"^(\d★ Legendary|Caps$)", re.I)

# Pictures for rewards the shared library cannot place by FormID (a named
# weapon shares its base weapon's FormID; a chem / shard has no plan page).
# Used only once the file is on the server listing.
EXTRA_PICS = {
    "super slasher auto axe": "guide-images/plan-checklist/weapons/super-slasher-auto-axe.avif",
    "ghost boy": "guide-images/seasonal-events/the-slasher/ghost-boy.avif",
}
# Every legendary mod (shard / mod box) without its own picture uses the
# site's one green legendary mod box.
LEGENDARY_MOD_BOX = "guide-images/category-images/legendary-mods-category-image.avif"


def log(msg):
    print(f"{TAG} {msg}")


def load(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def key_of(name):
    n = re.sub(r"\s*\(\d+\)\s*$", "", str(name or "")).strip().lower()
    return re.sub(r"\s+", " ", n)


def sort_key(name):
    return re.sub(r"^(plan|recipe):\s*", "", str(name or ""), flags=re.I).lower()


def kind_of(name, sig, edid):
    n = str(name or "")
    if re.match(r"^plan:", n, re.I):
        return "Plan"
    if re.match(r"^recipe:", n, re.I):
        return "Recipe"
    if re.match(r"^player title:", n, re.I):
        return "Player Title"
    if re.match(r"^(camp|c\.a\.m\.p\.) title:", n, re.I):
        return "C.A.M.P. Title"
    if sig == "WEAP":
        return "Weapon"
    if sig == "ARMO":
        return "Apparel"
    if "LegendaryShard" in (edid or ""):
        return "Legendary Shard"
    if sig == "ALCH":
        return "Chem"
    return "Item"


class Pics:
    """Server-listing checks + named-weapon pictures."""

    def __init__(self, dist):
        try:
            import image_index as ii
            self.idx = ii.load(os.path.join(dist, "image_index.json") if os.path.exists(
                os.path.join(dist, "image_index.json")) else ii.INDEX_PATH, verbose=False)
        except Exception as e:                                  # pragma: no cover
            log(f"image index unavailable ({e}) - keeping the pictures the pages already use")
            self.idx = None
        self.uwa = {}
        self.uwa_base = {}      # named weapon with no picture of its own -> base weapon's
        u = load(os.path.join(dist, "unique_weapons_armour", "unique_weapons_armour.json"), {}) or {}
        for it in u.get("items") or []:
            if it.get("image"):
                (self.uwa_base if it.get("imageSameAsBase") else self.uwa)[key_of(it.get("name"))] = it["image"]

    def present(self, url):
        if not url:
            return ""
        if self.idx is None:
            return url
        return self.idx.present(url)

    def library(self, fid, edid, name):
        """The shared library's picture for this FormID (what every other page
        shows for it), or "" - used when this channel's pools carry none (PTS)."""
        if self.idx is None or not fid:
            return ""
        try:
            urls, _src = self.idx.resolve(fid, edid or "", name or "", (), ())
            return urls[0] if urls else ""
        except Exception:
            return ""

    def pick(self, name, current, fid="", edid=""):
        out = []
        k = key_of(name)
        own = self.uwa.get(k)
        if own and self.present(own):
            # A named weapon's own picture. The pools' picture is its base
            # weapon (same FormID), so it is not shown as a second view.
            return [self.present(own)]
        for u in ([(UPLOADS + EXTRA_PICS[k]) if k in EXTRA_PICS else ""]
                  + list(current or []) + [self.uwa_base.get(k), self.library(fid, edid, name)]):
            p = self.present(u) if u else ""
            if p and p not in out:
                out.append(p)
        return out[:4]


def guide_quests(guide):
    """EDID -> {name, steps, requires, availability} from the slasher guide."""
    out = {}
    for ph in (guide or {}).get("phases") or []:
        for q in [ph.get("quest")] + list(ph.get("sideQuests") or []):
            if q and q.get("edid"):
                out[q["edid"]] = {k: q[k] for k in ("name", "steps", "requires", "availability") if q.get(k)}
    return out


def daily_ops_tiers(dist):
    """The Slasher tier of the Daily Ops page (build_daily_ops_rewards_json.py):
    each clear that rolls the Slasher list and the chance it does
    ([{label, chance}]), the ops it runs on (plan_master routes that reach
    those lists) and the tier's own explanation."""
    d = load(os.path.join(dist, "daily_ops", "daily_ops_all_rewards.json"), {}) or {}

    def find(o):
        if isinstance(o, dict):
            if o.get("type") == "tier" and o.get("tier") == "slasher":
                return o
            for v in o.values():
                r = find(v)
                if r:
                    return r
        elif isinstance(o, list):
            for v in o:
                r = find(v)
                if r:
                    return r
        return None

    t = find(d) or {}
    tiers, lists = [], set()
    for c in t.get("children") or []:
        if c.get("type") != "lvli":
            continue
        ch = c.get("listSelfChance", c.get("entryRate", 100.0))
        try:
            ch = float(ch)
        except (TypeError, ValueError):
            ch = 100.0
        tiers.append({"label": c.get("label") or "", "chance": ch})
        if c.get("formid"):
            lists.add(str(c["formid"]).upper())
    ops = set()
    if lists:
        pm = load(os.path.join(dist, "plan_master.json"), {}) or {}
        for p in pm.get("items") or []:
            for r in p.get("obtain_routes") or []:
                if lists & {str(x).upper() for x in r.get("lvli") or []} and r.get("route"):
                    ops.add(r["route"])
    return sorted(ops), tiers, (t.get("note") or "")


def new_item(name, formid="", edid="", sig=""):
    return {
        "id": (formid or "").upper() or re.sub(r"[^a-z0-9]+", "-", key_of(name)).strip("-"),
        "name": name, "formid": (formid or "").upper(), "edid": edid or "", "sig": sig or "",
        "kind": kind_of(name, sig, edid), "tradeable": None, "isNew": False, "releaseYear": None,
        "pics": [], "sources": [], "modSlots": [], "customModName": "",
    }


def add_source(item, src):
    for s in item["sources"]:
        if s["route"] == src["route"] and s["name"] == src["name"]:
            return
    item["sources"].append(src)


def build(dist):
    path = os.path.join(dist, SEASONAL)
    data = load(path)
    if not data or "byPage" not in data:
        log(f"no seasonal data at {path} - nothing to do")
        return None
    by_page = data["byPage"]
    page = by_page.get(SLUG)
    if not page:
        log(f"{SLUG} not in {path} - nothing to do")
        return None
    guide = by_page.get(GUIDE_SLUG) or {}
    quests = guide_quests(guide)
    pics = Pics(dist)

    flat = {}
    for r in page.get("rewards") or []:
        fid = str(r.get("formId") or r.get("formid") or "").upper()
        if fid:
            flat[fid] = r

    items = {}
    unknown_pools = []
    ops_names, ops_tiers, ops_note = daily_ops_tiers(dist)

    # 1. Every reward pool the page resolves (rng76 rates + conditions)
    for node in page.get("eventRewardTree") or []:
        if not node.get("isUniqueReward"):
            continue
        spec = SOURCES.get(node.get("label") or "")
        if not spec:
            unknown_pools.append(node.get("label"))
            spec = {"route": "Events & Activities", "name": node.get("label") or "", "short": "Event"}
        for it in node.get("items") or []:
            name = it.get("name") or ""
            sig = (it.get("sig") or "").upper()
            edid = it.get("edid") or ""
            # Ghost Boy (a chem) IS listed here - Duchess, 1 Nov 2026: it is
            # one of the event's own rewards.
            if sig in SKIP_SIG or SKIP_EDID.match(edid) or SKIP_NAME.match(name):
                continue
            k = key_of(name)
            row = items.get(k)
            if row is None:
                row = items[k] = new_item(name, it.get("formid"), edid, sig)
            fr = flat.get(row["formid"]) or {}
            if row["tradeable"] is None and it.get("tradeable") in (True, False):
                row["tradeable"] = it["tradeable"]
            row["isNew"] = row["isNew"] or bool(it.get("isNew") or fr.get("isNew"))
            row["releaseYear"] = row["releaseYear"] or it.get("releaseYear") or fr.get("releaseYear")
            if it.get("modSlots") and not row["modSlots"]:
                row["modSlots"] = it["modSlots"]
            if it.get("customModName") and not row["customModName"]:
                row["customModName"] = it["customModName"]
            cur = list(it.get("images") or fr.get("images") or [])
            if it.get("imageUrl") or fr.get("imageUrl"):
                cur.append(it.get("imageUrl") or fr.get("imageUrl"))
            row["_cur"] = row.get("_cur", []) + cur
            conds = [re.sub(r"\bthe The\b", "The", c) for c in (it.get("conditions") or []) if c]
            if spec.get("dailyOps") and ops_tiers:
                # One line per op and per clear (house rule: one source per
                # line, each with its own rate).
                first = True
                for op in ops_names or ["Daily Ops"]:
                    for t in ops_tiers:
                        r = (it.get("dropRate") or 0) * t["chance"] / 100.0
                        src = {"route": spec["route"], "short": spec["short"],
                               "name": (f"Daily Ops: {op} - " if op != "Daily Ops" else "Daily Ops - ") + t["label"],
                               "rate": round(r, 6), "conditions": conds}
                        if first and ops_note:
                            src["note"] = ops_note
                            first = False
                        add_source(row, src)
                continue
            src = {"route": spec["route"], "name": spec["name"], "short": spec["short"],
                   "rate": it.get("dropRate"), "conditions": conds}
            if spec.get("quest") and spec["quest"] in quests:
                src["quest"] = quests[spec["quest"]]
            add_source(row, src)

    # 2. The finale's paint plans (guaranteed quest reward or the Reborn
    #    Slasher's own death list) - slasher_guide.py resolved these.
    finale_q = quests.get("SDOW_MQ05_Headhunt")
    for fr in guide.get("finaleRewards") or []:
        name = fr.get("name") or ""
        k = key_of(name)
        row = items.get(k) or new_item(name)
        items[k] = row
        if row["tradeable"] is None:
            row["tradeable"] = fr.get("tradeable")
        if fr.get("image"):
            row["_cur"] = row.get("_cur", []) + [fr["image"]]
        for s in fr.get("sources") or []:
            rate = (s.get("rate") or 0) * 100 if s.get("rate") is not None else None
            if s.get("how") == "quest":
                src = {"route": "Quests", "name": s.get("quest") or "", "short": "Quest", "rate": rate, "conditions": []}
                if finale_q:
                    src["quest"] = finale_q
            else:
                who = s.get("from") or "The Reborn Pint-Sized Slasher"
                src = dict(FINALE_DROP, name=f"{who} (Head Hunts)", rate=rate, conditions=[])
            add_source(row, src)

    # 3. Lifetime challenge rewards (slasher guide -> CHAL MNAM), matched to
    #    the reward's own record: a reward pool item by name, a plan whose
    #    unlock line names the challenge, or a title by its title text.
    pm = load(os.path.join(dist, "plan_master.json"), {}) or {}
    plans = pm.get("items") or []
    titles = []
    for fn in ("titles_player.json", "titles_camp.json"):
        t = load(os.path.join(dist, fn), {}) or {}
        arr = (t.get("titles") or t.get("items") or []) if isinstance(t, dict) else t
        for x in arr or []:
            x = dict(x, _camp=(fn == "titles_camp.json"))
            titles.append(x)
    for ph in guide.get("phases") or []:
        for c in ph.get("challenges") or []:
            rew = (c.get("reward") or "").strip()
            if not rew:
                continue
            chal = re.sub(r"^\(Seasonal\)\s*", "", c.get("name") or "").strip()
            line = f"Complete the Lifetime Challenge: {chal} ({c.get('required')})" if c.get("required") else \
                f"Complete the Lifetime Challenge: {chal}"
            src = dict(CHALLENGE, name=line, rate=100.0, conditions=[], challenge=True)
            row = items.get(key_of(rew))
            if row is None:
                p = next((p for p in plans if not p.get("cut") and any(
                    chal.lower() in str(u).lower() for u in (p.get("obtain_unlocks") or []))), None)
                if p:
                    nm = p.get("name") or rew
                    row = items.get(key_of(nm))
                    if row is None:
                        pi = p.get("plan_item") or {}
                        row = items[key_of(nm)] = new_item(nm, pi.get("formid"), pi.get("edid"), "BOOK")
                        row["tradeable"] = p.get("tradeable")
                        row["_cur"] = list(p.get("images") or [])
            if row is None:
                m = re.match(r"^title:\s*(.+)$", rew, re.I)
                if m:
                    want = m.group(1).strip().lower()
                    t = next((t for t in titles if str(t.get("title") or "").lower() == want
                              and not t.get("cutContent") and "SDOW" in str(t.get("edid") or "")), None)
                    if t:
                        nm = ("C.A.M.P. Title: " if t["_camp"] else "Player Title: ") + t["title"]
                        row = items.get(key_of(nm))
                        if row is None:
                            row = items[key_of(nm)] = new_item(nm, t.get("formId"), t.get("edid"), "")
                            row["kind"] = "C.A.M.P. Title" if t["_camp"] else "Player Title"
                            row["tradeable"] = t.get("tradeable")
                            row["releaseYear"] = t.get("releaseYear")
                            row["_cur"] = list(t.get("images") or [])
            if row is None:
                log(f"challenge reward not matched to a record: {rew!r} ({chal})")
                continue
            add_source(row, src)

    # 4. Pictures (uwa / extra / what the pools already had), placeholders for
    #    titles, and the A-Z order.
    title_ph = (load(os.path.join(dist, "image_index.json"), {}) or {}).get("placeholders", {}).get("title", {})
    out = []
    for row in items.values():
        row["pics"] = pics.pick(row["name"], row.pop("_cur", []), row["formid"], row["edid"])
        if not row["pics"] and row["kind"] == "Legendary Shard":
            row["pics"] = pics.pick("", [UPLOADS + LEGENDARY_MOD_BOX])
        if not row["pics"] and row["kind"] in ("Player Title", "C.A.M.P. Title"):
            affix = "prefix" if re.search(r"prefix", row["edid"], re.I) else "suffix" if re.search(r"suffix", row["edid"], re.I) else "both"
            kind = "camp" if row["kind"].startswith("C.A.M.P") else "player"
            ph = (title_ph.get(kind) or {}).get(affix) or (title_ph.get("player") or {}).get(affix)
            if ph:
                row["pics"] = [ph]
        stops = any(re.search(r"already (learned|earned)", c, re.I) for s in row["sources"] for c in s.get("conditions") or [])
        # No "already learned" gate in any source = it keeps dropping (same
        # reading as sevDropPersistence on the other seasonal checklists).
        row["stopsDropping"] = bool(stops)
        order = {"Quests": 0, "Events & Activities": 1, "Enemies": 2, "Challenges": 3}
        row["sources"].sort(key=lambda s: (order.get(s["route"], 9), -(s.get("rate") or 0), s["name"]))
        row["source"] = row["sources"][0]["short"] if row["sources"] else ""
        if not row["modSlots"]:
            row.pop("modSlots")
        if not row["customModName"]:
            row.pop("customModName")
        out.append(row)
    out.sort(key=lambda r: sort_key(r["name"]))

    if unknown_pools:
        log(f"WARNING pools with no SOURCES entry (shown as Events & Activities): {unknown_pools}")
    missing = [r["name"] for r in out if not r["pics"]]
    log(f"{len(out)} unique rewards, {len(missing)} without a picture: {missing}")

    n = 0
    for k, v in by_page.items():
        if isinstance(v, dict) and (k == SLUG or str(k).rstrip("/").endswith("/" + SLUG)):
            v["uniques"] = out
            n += 1
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log(f"wrote {path} ({n} page keys)")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dist", default=os.path.join(REPO, "dist"))
    a = ap.parse_args()
    dist = a.dist if os.path.isabs(a.dist) else os.path.join(REPO, a.dist)
    build(dist)


if __name__ == "__main__":
    main()
