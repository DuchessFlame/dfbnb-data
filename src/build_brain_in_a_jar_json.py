#!/usr/bin/env python3
r"""
build_brain_in_a_jar_json.py - dist JSON for the Brain in a Jar pages (Return to
Skyline Valley, quest "A Matter of Matter", RTSV_SQ01_Collectible).

  /df/collectables/brain-in-a-jar/guide/                      guide  (dist/brain_in_a_jar_guide.json)
  /df/collectables/brain-in-a-jar/checklist/                  reward checklist
  /df/collectables/brain-in-a-jar/brain-in-a-jar-locations-<a>-<b>/   30 per page

Same document shape as dist/abraxodyne_intel_briefcases.json (pages[], locations[]
with n / fast_travel / directions / image_top / image_bottom / exists) so the
renderer pattern carries over, plus a data-driven rewards block.

Inputs (nothing typed - every name comes from the exports):
  data/brain_in_a_jar/placements_<channel>.tsv   esm_scan_placements.py (local, DB)
  data/brain_in_a_jar/numbering.tsv              fixed numbers; new refs appended here
  CHAL_Export_*.tsv     the collect-brain challenge tiers -> milestone + reward text
  GMRW_Export_*.tsv     the per-brain hand-in reward (QuestReward_RTSV_SQ01_*)
  GLOB_Export_*.tsv     caps value of that reward
  NOTE_Export_*.tsv     the Scan holotapes (RTSV_SQ01_Collectible_Holotape_##)

Hand-authored fields (fast_travel, directions, image_top, image_bottom) live in
data/brain_in_a_jar/hand.json (written by brain_photo.py), keyed by the brain's REF
FormID, never by its number; older ones are kept from the last build.
Page breaks: data/brain_in_a_jar/pages.tsv (pages can run past 30).

Channel: DFBNB_CHANNEL=pts (or --pts) reads tsv/pts + placements_pts.tsv. Output goes
to OUT_DIR (default dist/); the PTS workflow relocates dist/ -> dist/pts/.
"""
import csv, json, math, os, re, sys, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import tsv_source
from shared_images import HOLOTAPE_IMAGE

csv.field_size_limit(10 ** 9)
DATA = os.path.join(REPO, "data", "brain_in_a_jar")
OUT_DIR = os.environ.get("OUT_DIR", os.path.join(REPO, "dist"))
OUT_NAME = "brain_in_a_jar.json"
PAGE_SIZE = 30
BRAIN_FORMID = "008ABB18"                 # RTSV_SQ01_BrainInJar  "Brain in a Jar"
QUEST_EDID = "RTSV_SQ01_Collectible"
CHAL_PREFIX = "Challenge_Lifetime_RTSV_Collect_Brain_Tier"
HOLO_PREFIX = "RTSV_SQ01_Collectible_Holotape_"
IMAGE_BASE = "/wp-content/uploads/guide-images/collectables/Brain-in-a-Jar-Locations/"
# Server layout (Duchess, 8 Nov 2026): guide photos loose in IMAGE_BASE, per-brain photos in
# jars-<from>-<to>/, every map (full, numbered, region tiles, page maps) in full-maps/,
# every gallery picture in gallery-images/.
MAPS_BASE = IMAGE_BASE + "full-maps/"
GALLERY_BASE = IMAGE_BASE + "gallery-images/"
URL_BASE = "/df/collectables/brain-in-a-jar/"

# CHAL RNAM (the reward icon) -> the reward type the checklist shows.
ICON_TYPE = {"IconCR_Recipe": "CAMP item", "IconCR_WeaponMod": "Weapon mod",
             "IconCR_Weapon": "Plan", "IconCR_Armor": "Apparel", "IconCR_Caps": "Caps"}


def _clean(s):
    return re.sub(r"\s+", " ", (s or "").replace("¬", "")).strip()


def _rows(pattern, channel):
    p = tsv_source.newest(pattern, channel=channel, required=False)
    if not p:
        print(f"  (no {pattern} on {channel})")
        return []
    with open(p, encoding="utf-8", errors="replace") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def load_numbering():
    path = os.path.join(DATA, "numbering.tsv")
    head, num = [], {}
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    head = [l for l in lines if l.startswith("#")]
    for r in csv.DictReader([l for l in lines if not l.startswith("#")], delimiter="\t"):
        num[r["RefFormID"]] = int(r["n"])
    return path, head, num


def load_placements(channel):
    path = os.path.join(DATA, f"placements_{channel}.tsv")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return [r for r in csv.DictReader(fh, delimiter="\t") if r["BaseFormID"] == BRAIN_FORMID]


def assign_numbers(place, num_path, head, num):
    """Keep every existing number. New refs are appended in nearest-neighbour order
    from the last numbered brain, so the route keeps going rather than restarting."""
    new = [r for r in place if r["RefFormID"] not in num]
    if not new:
        return num
    xy = {r["RefFormID"]: (float(r["X"]), float(r["Y"])) for r in place}
    last = max(num, key=num.get) if num else None
    cur = xy.get(last, xy[new[0]["RefFormID"]])
    left = {r["RefFormID"] for r in new}
    n = max(num.values(), default=0)
    while left:
        k = min(left, key=lambda k: math.hypot(xy[k][0] - cur[0], xy[k][1] - cur[1]))
        n += 1; num[k] = n; cur = xy[k]; left.remove(k)
    with open(num_path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(head) + "\nn\tRefFormID\n")
        for k, v in sorted(num.items(), key=lambda kv: kv[1]):
            fh.write(f"{v}\t{k}\n")
    print(f"  numbered {len(new)} new brain(s)")
    return num


def build_rewards(channel):
    tiers = []
    for r in _rows("CHAL_Export_*.tsv", channel):
        eid = r.get("EDID", "")
        if not eid.startswith(CHAL_PREFIX):          # zzz = cut tier, skipped by prefix
            continue
        try:
            need = int(float(r.get("TNAM") or 0))
        except ValueError:
            continue
        if need <= 0 or need >= 9999999:
            continue
        name = _clean(r.get("MNAM"))
        tiers.append({"id": "rw-" + re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-"),
                      "name": name, "milestone": need,
                      "type": ICON_TYPE.get(r.get("RNAM", ""), "Reward"),
                      "challenge": _clean(r.get("FULL")), "form_id": r.get("FormID", ""),
                      "edid": eid, "image": "", "note": ""})
    tiers.sort(key=lambda t: t["milestone"])

    globs = {g.get("EDID"): g.get("FLTV") for g in _rows("GLOB_Export_*.tsv", channel)}
    hand_in = None
    for g in _rows("GMRW_Export_*.tsv", channel):
        if QUEST_EDID in (g.get("Ref1") or "") and g.get("EDID", "").startswith("QuestReward_"):
            caps_glob = (g.get("NAM8_CapsGlobal") or "").split(":")[1:2]
            caps = globs.get(caps_glob[0]) if caps_glob else None
            item = (g.get("RewardedItem") or "").split(":")
            hand_in = {"edid": g["EDID"],
                       "caps": int(float(caps)) if caps else None,
                       "xp_curve": (g.get("XPCT_XPCurveTable") or "").split(":")[1:2] or None,
                       "loot_list": item[1] if len(item) > 1 else None}
            break
    if hand_in is not None:
        hand_in["unlocks"] = quest_unlocks(channel)
    reward_pictures(tiers, channel)
    return tiers, hand_in


def quest_unlocks(channel):
    """Recipes the finished quest teaches: COBJ2 condition GetNumTimesCompletedQuest on
    the quest (e.g. the Brain Jar CAMP item). Name + picture from the COBJ export."""
    names = {r.get("COBJ_FormID"): r for r in _rows("COBJ_Export_*.tsv", channel)}
    out = []
    for r in _rows("COBJ2_Export_*.tsv", channel):
        if "GetNumTimesCompletedQuest" in (r.get("Conditions") or "") and r.get("Quest_EDID") == QUEST_EDID:
            c = names.get(r.get("COBJ_FormID"), {})
            name = _clean(c.get("CNAM_FULL")) or r.get("CNAM_EDID", "")
            bench = c.get("BNAM_EDID", "")
            kind = "CAMP item" if bench.startswith("Workshop") else "Recipe"
            out.append({"name": name, "kind": kind, "cobj": r.get("COBJ_FormID", ""),
                        "cobj_edid": r.get("COBJ_EDID", ""), "item": r.get("CNAM_FormID", ""),
                        "item_edid": r.get("CNAM_EDID", ""),
                        "image": next((u for u in (IMAGE_BASE + "brain-in-a-jar.avif",)
                                       if "BrainInJar" in r.get("CNAM_EDID", "") and on_server(u)), "")})
    return out


CAPS_IMAGE = "/wp-content/uploads/season_images/utility/score_currency_caps.avif"
LEGENDARY_MOD_IMAGE = "/wp-content/uploads/guide-images/category-images/legendary-mods-category-image.avif"


def _json(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def reward_pictures(tiers, channel):
    """Each milestone gets the picture of what it gives and a 'crafts' line (generative):
      Caps                      -> the site's caps icon
      '<Weapon> - <Mod> Recipe' -> the weapon (Unique Weapons & Armour data; its base
                                   weapon's picture when the unique has none) + weaponMod
      'Plan: <Legendary mod>'   -> the legendary star pages entry (star + picture)
    Only pictures on the server are used."""
    base = os.path.join(REPO, "dist", "pts") if channel == "pts" else os.path.join(REPO, "dist")
    uwa = _json(os.path.join(base, "unique_weapons_armour", "unique_weapons_armour.json")) or {}
    by_name = {it.get("name"): it for it in uwa.get("items", [])}
    legmods = {}
    for n in range(1, 6):
        d = _json(os.path.join(base, "legendary_star_pages", f"{n}-star.json")) or {}
        for it in d.get("items", []):
            legmods.setdefault(it.get("name"), it)
    for t in tiers:
        nm = t["name"]
        m = re.match(r"^(.+?) - (.+?) Recipe$", nm)
        p = re.match(r"^Plan: (.+)$", nm)
        if t["type"] == "Caps":
            if on_server(CAPS_IMAGE):
                t["image"] = CAPS_IMAGE
        elif m and m.group(1) in by_name:
            w = by_name[m.group(1)]
            img = w.get("image") or (by_name.get(w.get("base")) or {}).get("image") or ""
            if img and on_server(img):
                t["image"] = img
                if not w.get("image"):
                    t["image_note"] = f"Picture: {w.get('base')} (the {w['name']} has no picture of its own yet)"
            t["crafts"] = f"{m.group(2)} (weapon mod for the {w['name']})"
            mod = next((u for u in w.get("uniqueMods", []) if u.get("kind") == "unique"), None)
            if mod:
                t["weapon_mod"] = mod.get("formId", "")
        elif p and p.group(1) in legmods:
            lm = legmods[p.group(1)]
            img = lm.get("image") or LEGENDARY_MOD_IMAGE
            if on_server(img):
                t["image"] = img
            t["crafts"] = f"{lm['name']} ({lm.get('star')}\u2605 legendary mod)"
            if lm.get("headline"):
                t["note"] = lm["headline"]


def build_holotapes(channel):
    out = []
    for r in _rows("NOTE_Export_*.tsv", channel):
        eid = r.get("EDID") or r.get("NOTE_EDID") or ""          # NOTE export prefixes its columns
        if eid.startswith(HOLO_PREFIX):
            out.append({"form_id": r.get("FormID") or r.get("NOTE_FormID", ""), "edid": eid,
                        "name": _clean(r.get("FULL"))})
    return sorted(out, key=lambda h: h["name"])


def page_ranges(total):
    """Location page breaks. data/brain_in_a_jar/pages.tsv (from<TAB>to, one row per
    page, in order) lets a page run past 30 so it never cuts the route in half
    (Duchess, 5 Nov 2026); anything after the last listed page falls back to 30s."""
    out, nxt = [], 1
    path = os.path.join(DATA, "pages.tsv")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if not ln or ln.startswith("#") or ln.lower().startswith("from"):
                    continue
                a, b = (int(x) for x in ln.split("\t")[:2])
                if a != nxt or b < a:
                    raise SystemExit(f"pages.tsv: page {a}-{b} doesn't follow on from {nxt - 1}")
                out.append((a, min(b, total))); nxt = b + 1
                if nxt > total:
                    return out
    for a in range(nxt, total + 1, PAGE_SIZE):
        out.append((a, min(a + PAGE_SIZE - 1, total)))
    return out


SERVER_LISTING = os.path.join(REPO, "data", "server_listing.tsv")
_LISTING = None


def on_server(url):
    """True when url's file is on data/server_listing.tsv (Duchess's FileZilla uploads).
    The Maps expand only shows maps that are up, so the page never shows a 404."""
    global _LISTING
    if _LISTING is None:
        _LISTING = set()
        if os.path.exists(SERVER_LISTING):
            with open(SERVER_LISTING, encoding="utf-8") as fh:
                _LISTING = {ln.split("\t", 1)[0].strip() for ln in fh if not ln.startswith("#")}
    return url.replace("/wp-content/uploads/", "", 1) in _LISTING


def page_maps(page, doc):
    """Cards for one location page's Maps expand (guide-maps-expand rule, 5 Nov 2026):
    this page's route map, Full 4K map, Numbered 4K map, then region maps A-Z.
    Only maps on the server are listed."""
    cards = [{"url": page["map"], "name": f"Route map {page['from']} \u2013 {page['to']}",
              "desc": "This page's brains, in order", "file": f"{page['slug']}-map.jpg"},
             {"url": doc["full_map"], "name": "Full 4K map", "desc": "All 150 brains",
              "file": "brain-in-a-jar-full-map.jpg"},
             {"url": doc["numbered_map"], "name": "Numbered 4K map", "desc": "Matches the pages",
              "file": "brain-in-a-jar-numbered-map.jpg"}]
    for reg, u in sorted(doc["region_maps"].items(), key=lambda kv: re.sub(r"^the ", "", kv[0].lower())):
        cards.append({"url": u, "name": reg, "desc": "", "file": "brain-in-a-jar-" + u.rsplit("/", 1)[1]})
    return [c for c in cards if on_server(c["url"])]


GALLERY_TSV = os.path.join(REPO, "data", "brain_in_a_jar", "gallery.tsv")


def page_gallery(page):
    """Gallery cards for one location page (Duchess, 8 Nov 2026): data/brain_in_a_jar/gallery.tsv,
    files in gallery-images/. Only pictures on the server are listed."""
    if not os.path.exists(GALLERY_TSV):
        return []
    rows = [ln.rstrip("\n").split("\t") for ln in open(GALLERY_TSV, encoding="utf-8")
            if ln.strip() and not ln.startswith("#") and not ln.startswith("page_from")]
    cards = []
    for r in rows:
        if len(r) < 3 or not r[0].strip().isdigit() or int(r[0]) != page["from"]:
            continue
        url = f"{GALLERY_BASE}{r[1].strip()}"
        cards.append({"url": url, "name": r[2].strip(), "desc": (r[3].strip() if len(r) > 3 else ""),
                      "file": "brain-in-a-jar-" + r[1].strip(), "credit": "Image credit: DuchessFlame"})
    return [c for c in cards if on_server(c["url"])]


def _index(d):
    return {l["ref"]: l for l in (d or {}).get("locations", []) if l.get("ref")}


def load_existing(out_path, channel):
    """Hand-authored fields from the last build. The PTS workflow starts with
    `rm -rf dist`, so on CI the previous file is only in git - fall back to
    `git show HEAD:<path>` or every rebuild would wipe the photos/directions."""
    rel = f"dist/{'pts/' if channel == 'pts' else ''}{OUT_NAME}"
    for path in (out_path, os.path.join(REPO, rel)):
        try:
            with open(path, encoding="utf-8") as fh:
                got = _index(json.load(fh))
            if got:
                return got
        except (OSError, ValueError):
            pass
    try:
        import subprocess
        raw = subprocess.run(["git", "-C", REPO, "show", f"HEAD:{rel}"], capture_output=True,
                             text=True, encoding="utf-8", check=True).stdout
        return _index(json.loads(raw))
    except Exception:
        return {}


def main():
    channel = tsv_source.channel_of()
    place = load_placements(channel)
    if place is None:
        print(f"No data/brain_in_a_jar/placements_{channel}.tsv - nothing to build on this channel.")
        return
    num_path, head, num = load_numbering()
    num = assign_numbers(place, num_path, head, num)

    out_path = os.path.join(OUT_DIR, OUT_NAME)
    old = load_existing(out_path, channel)
    # Photos + directions as Duchess sends them (5 Nov 2026), keyed by REF FormID:
    # data/brain_in_a_jar/hand.json {ref: {directions, image_top, image_bottom, fast_travel}}.
    # Wins over the last build's copy, and lives in data/ so no rebuild can lose it.
    hand_path = os.path.join(DATA, "hand.json")
    hand = {}
    if os.path.exists(hand_path):
        with open(hand_path, encoding="utf-8") as fh:
            hand = {k.upper(): v for k, v in json.load(fh).items()}

    total = len(place)
    pages = []
    for a, b in page_ranges(total):
        slug = f"brain-in-a-jar-locations-{a}-{b}"
        pages.append({"slug": slug, "title": f"Brain in a Jar Locations {a} - {b}",
                      "from": a, "to": b, "url": f"{URL_BASE}{slug}/",
                      "map": f"{MAPS_BASE}{slug}-map.jpg"})

    # Fast travel points (Duchess, 7 Nov 2026): data/brain_in_a_jar/fast_travel.tsv {ref: marker}.
    # Those brains get the "FT <marker>" pill; the rest of a page that has any entry walk on
    # from the brain before ("Spawn above" pill). Pages with no entries keep the marker pill.
    ft_path = os.path.join(DATA, "fast_travel.tsv"); ft = {}
    if os.path.exists(ft_path):
        for ln in open(ft_path, encoding="utf-8"):
            if ln.startswith("#") or ln.startswith("ref\t") or not ln.strip(): continue
            a, b = ln.rstrip("\n").split("\t")[:2]; ft[a.strip().upper()] = b.strip()
    ft_pages = {next(p["slug"] for p in pages if p["from"] <= num[ref] <= p["to"]) for ref in ft if ref in num}

    locs = []
    for r in place:
        ref = r["RefFormID"]; n = num[ref]
        prev = {**old.get(ref, {}), **hand.get(ref, {})}
        page = next(p for p in pages if p["from"] <= n <= p["to"])
        locs.append({
            "n": n, "ref": ref, "region": r["region"], "marker": r["marker"],
            "fast_travel": ft.get(ref) or prev.get("fast_travel") or r["marker"],
            "travel": "ft" if ref in ft else ("walk" if page["slug"] in ft_pages else ""),
            "directions": prev.get("directions", ""),
            "image_top": prev.get("image_top", ""),
            # close-ups are no longer used on the location pages (Duchess, 7 Nov 2026):
            # only one set in hand.json shows, never one left over from an old build
            "image_bottom": hand.get(ref, {}).get("image_bottom", ""),
            "image_map": prev.get("image_map", ""),
            # Duchess 7 Nov 2026: a locked door on the way gets "You will need Lockpick
            # Skill N ..." where the directions go, plus her photo of the lock
            "lockpick": hand.get(ref, {}).get("lockpick", 0),
            "image_lock": hand.get(ref, {}).get("image_lock", ""),
            # Duchess 7 Nov 2026: a short how-to-get-there line (e.g. Vault 63 Immortality Sector)
            "note": hand.get(ref, {}).get("note", ""),
            # Duchess 8 Nov 2026: screenshot of the fast-travel choice (e.g. Dark Hollow Manor: Exterior)
            "image_ft": hand.get(ref, {}).get("image_ft", ""),
            "exists": True,
            "coords": [round(float(r["X"]), 1), round(float(r["Y"]), 1), round(float(r["Z"]), 1)],
            "page": page["slug"],
        })
    locs.sort(key=lambda l: l["n"])
    # Duchess 7 Nov 2026: the last brain before a fast travel says where to go next
    # ("After this one, fast travel back to Three Ponds for Brain 7.")
    # 9 Nov 2026: "back" for the same marker only when this brain really is near it (~110 m),
    # not just counted in its group (Camp Liberty's Riprap Overlook brains)
    _mk = {}
    try:
        _ld = json.load(open(os.path.join(REPO, "dist", "pts" if channel == "pts" else "", "locations", "all_locations.json"), encoding="utf-8"))
        _mk = {x["name"]: (x["x"], x["y"]) for r_ in _ld["regions"] for x in r_["locations"] if x.get("x") is not None}
    except Exception:
        pass
    def _near_marker(l, name):
        if name not in _mk or not l.get("coords"): return False
        return math.hypot(l["coords"][0] - _mk[name][0], l["coords"][1] - _mk[name][1]) <= 8000
    # "back" = fast travelled there before, or this brain is at that same marker
    # (46 -> "fast travel back to Big Meadows Gas Well for Brain 47")
    # Duchess 8 Nov 2026: a page always starts with a fast travel, never "Spawn above"
    # (nothing is above the first brain). Not for Milepost Zero (out of the game).
    for i, l in enumerate(locs):
        if (i == 0 or locs[i - 1]["page"] != l["page"]) and l.get("travel") != "ft" \
                and l.get("fast_travel") and l["fast_travel"] != "Milepost Zero":
            l["travel"] = "ft"
    seen = set()
    for i, l in enumerate(locs):
        if l.get("travel") == "ft": seen.add(l["fast_travel"])
        nxt = locs[i + 1] if i + 1 < len(locs) else None
        l["next_ft"] = ({"n": nxt["n"], "marker": nxt["fast_travel"],
                         "back": nxt["fast_travel"] in seen or _near_marker(l, nxt["fast_travel"])}
                        if nxt and nxt.get("travel") == "ft" and nxt["page"] == l["page"] else None)

    tiers, hand_in = build_rewards(channel)
    quest = next((q.get("FULL") for q in _rows("QUST2_Export_*_Quests.tsv", channel)
                  if q.get("EDID") == QUEST_EDID), None) or "A Matter of Matter"

    doc = {
        "_meta": {"generated": datetime.date.today().isoformat(), "channel": channel,
                  "source": "SeventySix.esm placements + CHAL/GMRW/GLOB/NOTE exports",
                  "note": "***Subject to change pending porting to live servers***" if channel == "pts" else ""},
        "set": "brain-in-a-jar", "name": "Brain in a Jar", "category": "Collectables",
        "quest": quest,
        "blurb": f"Every Brain in a Jar for the {quest} collectable quest, in route order. "
                 "Most of them are in Skyline Valley; follow the numbers and the page map.",
        "image_base": IMAGE_BASE,
        "full_map": MAPS_BASE + "brain_in_a_jar.jpg",
        "numbered_map": MAPS_BASE + "brain_in_a_jar_numbered.jpg",
        "region_maps": {reg: f"{MAPS_BASE}{re.sub(r'[^a-z0-9]+', '-', reg.lower()).strip('-')}-spawn-map.jpg"
                        for reg in sorted({l['region'] for l in locs if l['region']})},
        "total_slots": total, "total_real": total,
        "pages": pages, "locations": locs,
        "rewards": {"milestones": tiers, "hand_in": hand_in},
        "holotapes": build_holotapes(channel),
        "holotape_image": HOLOTAPE_IMAGE,
    }
    for pg in pages:
        pg["maps"] = page_maps(pg, doc)
        pg["gallery"] = page_gallery(pg)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=False)
    print(f"{OUT_NAME}: {total} brains, {len(pages)} pages, {len(tiers)} reward tiers, "
          f"{len(doc['holotapes'])} holotapes -> {out_path}")
    build_guide(doc, channel)
    print("brain_in_a_jar_guide.json written")


GUIDE_IMG_BASE = IMAGE_BASE   # guide photos loose in the root (8 Nov 2026)


def _quest_facts(channel):
    """Numbers the guide quotes, all from the exports (nothing typed):
    batches  - CollectablesNeededPerBatch on RTSV_SQ01_Collectible (QUST2 _ScriptProps), in order
    xp       - RTSV_SQ01_CollectablePickUp_XP (GLOB), base XP per brain picked up
    camp     - the Brain Jar CAMP item (COBJ RTSV_Workshop_CO_BrainInJar): name + components."""
    f = {"batches": [], "xp": None, "camp_name": "", "camp_cost": ""}
    for r in _rows("QUST2_Export_*_ScriptProps.tsv", channel):
        if r.get("QuestEDID") == QUEST_EDID and r.get("Property") == "CollectablesNeededPerBatch":
            try:
                f["batches"].append(int(float(r.get("Value") or 0)))
            except ValueError:
                pass
    for r in _rows("GLOB_Export_*.tsv", channel):
        vals = list(r.values())
        if len(vals) > 2 and vals[1] == "RTSV_SQ01_CollectablePickUp_XP":
            try:
                f["xp"] = int(float(vals[2]))
            except ValueError:
                pass
    names = {}
    for pat, ek, fk in (("MISC_Export_*.tsv", "EDID", "FULL"), ("ALCH_Export_*.tsv", "ALCH_EDID", "FULL")):
        for r in _rows(pat, channel):
            if r.get(ek) and r.get(fk):
                names.setdefault(r[ek], r[fk])
    for r in _rows("COBJ_Export_*.tsv", channel):
        if r.get("COBJ_EDID") == "RTSV_Workshop_CO_BrainInJar":
            f["camp_name"] = r.get("CNAM_FULL") or "Brain Jar"
            parts = []
            for c in (r.get("FVPA") or "").split("|"):
                bits = c.split(":")
                if len(bits) >= 2 and bits[0]:
                    nm = names.get(bits[0]) or re.sub(r"^c_", "", bits[0])
                    parts.append(f"{bits[1]} {nm}")
            f["camp_cost"] = ", ".join(parts)
    return f


def password_block(doc, channel):
    """The bunker password, as a redacted block the reader clicks to show (Duchess,
    5 Nov 2026: greyed out every time the page opens). The password is the lyrics on
    the last Scan holotape (Marcs: "exactly the length of this song without spaces or
    punctuation"), read from the ALLYDLG _Lore export: that tape's verses in line
    order, stage directions (*...*) and Iggy's one-line asides left out."""
    import html as _h
    if not doc.get("holotapes"):
        return ""
    tape = doc["holotapes"][-1]["form_id"]
    verses = {}
    order = []
    for r in _rows("ALLYDLG_Export_*_Lore.tsv", channel):
        if r.get("HolotapeFormID") != tape:
            continue
        t = (r.get("Text") or "").strip()
        if not t or re.fullmatch(r"\*.*\*", t):
            continue
        k = r.get("InfoFormID")
        if k not in verses:
            verses[k] = []; order.append((int(r.get("LineOrder") or 0), k))
        verses[k].append((int(r.get("LineOrder") or 0), t))
    vs = [[t for _, t in sorted(verses[k])] for _, k in sorted(order) if len(verses[k]) >= 4]
    if not vs:
        return ""
    E = _h.escape
    body = "".join("<p>" + "<br>".join(E(l) for l in v) + "</p>" for v in vs)
    return ('<div class="abg-redact" role="button" tabindex="0" aria-expanded="false" '
            'aria-label="Hidden: click to show the bunker password">'
            '<span class="abg-redact__label">Click to show the password</span>'
            f'<div class="abg-redact__text">{body}</div></div>')


def build_guide(doc, channel):
    """dist/brain_in_a_jar_guide.json - the shape df-bnb-brain-in-a-jar.js mounts
    (title / subtitle / sections[{title, html, images}]). Prose from
    data/brain_in_a_jar/guide.json (written 5 Nov 2026 from the quest's own logs,
    objectives and dialogue); everything countable is generated here.
    Order: intro, the hand-written story sections, Locations, Milestone Rewards,
    Scan Holotapes, FAQ. A section's pictures are listed only once they are on the
    server (data/server_listing.tsv), so the page never shows a broken picture."""
    import html as _h
    with open(os.path.join(DATA, "guide.json"), encoding="utf-8") as fh:
        g = json.load(fh)
    by_reg = {}
    for l in doc["locations"]:
        by_reg[l["region"]] = by_reg.get(l["region"], 0) + 1
    regions = ", ".join(f"{n} in {r}" for r, n in sorted(by_reg.items(), key=lambda kv: -kv[1]))
    hi = (doc.get("rewards") or {}).get("hand_in") or {}
    caps = f"{hi['caps']:,}" if hi.get("caps") is not None else "an unknown number of"
    qf = _quest_facts(channel)
    pw = password_block(doc, channel)
    batches = qf["batches"]
    need = sum(batches)
    E = _h.escape
    rows, run = [], 0
    for i, n in enumerate(batches, 1):
        run += n
        rows.append(f"<tr><td>{i}</td><td>{n}</td><td>{run}</td></tr>")
    table = ("<table class=\"abg-table\"><thead><tr><th>Hand-in</th><th>Brains</th><th>Running total</th></tr></thead>"
             f"<tbody>{''.join(rows)}</tbody></table>") if rows else ""
    fill = lambda t: (t.replace("{total}", str(doc["total_real"])).replace("{quest}", doc["quest"])
                      .replace("{caps}", caps).replace("{regions}", regions)
                      .replace("{batch_count}", str(len(batches))).replace("{quest_brains}", str(need))
                      .replace("{batch_table}", table)
                      .replace("{pickup_xp}", f"{qf['xp']:,}" if qf["xp"] is not None else "some")
                      .replace("{camp_name}", E(qf["camp_name"] or "Brain Jar"))
                      .replace("{camp_cost}", E(qf["camp_cost"] or "a few components"))
                      .replace("{holotape_count}", str(len(doc["holotapes"])))
                      .replace("{password_block}", pw))

    def pics(names):
        return [n for n in (names or []) if on_server(GUIDE_IMG_BASE + n + ".avif")]

    def sec(s):
        out = {"title": fill(s["title"]), "html": fill(s["html"])}
        im = pics(s.get("images"))
        if im:
            out["images"] = im
        return out

    locs = "".join(f'<li><a href="{E(p["url"])}">{E(p["title"])}</a></li>' for p in doc["pages"])
    miles = "".join(f'<li><strong>{m["milestone"]}</strong> {"brain" if m["milestone"] == 1 else "brains"}: {E(m["name"])}</li>'
                    for m in doc["rewards"]["milestones"])
    tapes = "".join(f"<li>{E(h['name'])}</li>" for h in doc["holotapes"])
    secs = [sec(g["intro"])] + [sec(s) for s in g.get("sections", [])]
    secs.append({"title": "Brain in a Jar Locations",
                 "html": f"<p>{fill(g.get('locations_html', ''))}</p><ul>{locs}</ul>"})
    secs.append({"title": "Lifetime Milestone Rewards",
                 "html": f"<p>{fill(g.get('milestones_html', ''))}</p><ul>{miles}</ul>"
                         f"<p>Tick them off on the <a href=\"{URL_BASE}checklist/\">Brain in a Jar Reward Checklist</a>.</p>"})
    if pics(g.get("milestones_images")):
        secs[-1]["images"] = pics(g.get("milestones_images"))
    if tapes:
        secs.append({"title": "The Scan Holotapes",
                     "html": f"<p>{fill(g.get('holotapes_html', ''))}</p><ol>{tapes}</ol>"})
    secs.append(sec(g["faq"]))
    out = {"_meta": doc["_meta"], "title": fill(g["title"]), "subtitle": fill(g["subtitle"]), "sections": secs}
    if g.get("image_dims"):               # [w, h] per guide image, so the page reserves the space
        out["image_dims"] = g["image_dims"]
    with open(os.path.join(OUT_DIR, "brain_in_a_jar_guide.json"), "w", encoding="utf-8", newline="\n") as fh:
        json.dump(out, fh, indent=1, ensure_ascii=False)


if __name__ == "__main__":
    main()
