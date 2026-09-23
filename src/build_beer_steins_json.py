#!/usr/bin/env python3
r"""
build_beer_steins_json.py — /df/plan-checklists/beer-steins/

WHAT "BEER STEINS" IS
---------------------
A checklist of every beer stein in Fallout 76. Membership is fully generative:
the game marks a MISC item as a beer stein with the ``BeerSteinsKeyword``
keyword (the same keyword the in-game beer-stein display racks pull from), so
any item carrying it is on the page and a new stein appears on the next TSV
drop with no edit here.

Cut/dev records are EXCLUDED entirely (not shown, not counted) — a stein whose
EditorID carries a dev prefix (zzz / CUT_ / DEL_ / DEBUG / POST_) never reaches
the list or the progress bar. This matches Duchess's Sept-2026 call for the
plan checklists (a row you can never obtain is not a checklist item).

WHERE THE DATA COMES FROM (generative)
--------------------------------------
* MISC export — membership + name + FormID/EDID + tradeable flag + description
  fallback. Selected on ``BeerSteinsKeyword``.
* tsv/season_rewards.tsv — for the SCORE_S<n> "Scoreboard" steins: the rank,
  the flavour description, and the ALREADY-HOSTED scoreboard artwork URL.
* tsv/fallout76_seasons.tsv — season name (for the obtain line) and season
  start date (for the ★ NEW pill `added` month).

IMAGES — scoreboard-first (no duplicate copies)
-----------------------------------------------
Every SCORE_S<n> stein reuses the artwork the scoreboard / season pages already
serve under /wp-content/uploads/season_images/… (matched to its season's stein
reward row in season_rewards.tsv by season number + theme token, then by name).
Nothing is copied into guide-images for those. Only the non-season event/deco
steins, which have no scoreboard artwork, fall back to a
/wp-content/uploads/guide-images/plan-checklist/beer-steins/<slug>.avif slot and
are reported as "needs art".

    python3 src/build_beer_steins_json.py          -> dist/beer_steins.json
    python3 src/build_beer_steins_json.py --pts    -> dist/pts/beer_steins.json
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

SCHEMA = 2
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import tsv_source                                   # one export resolver
try:
    import asset_paths
except ImportError:                                  # pragma: no cover
    asset_paths = None

PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
DIST_DIR = os.path.join(REPO, "dist", "pts") if PTS else os.path.join(REPO, "dist")
OUT = os.path.join(DIST_DIR, "beer_steins.json")

# season_rewards.tsv / fallout76_seasons.tsv are curated on the live side only;
# a PTS build reads the live copies (same rule the scoreboard-art builder uses).
SEASONS_REWARDS_TSV = os.path.join(REPO, "tsv", "pts", "season_rewards.tsv") if PTS else ""
if not SEASONS_REWARDS_TSV or not os.path.exists(SEASONS_REWARDS_TSV):
    SEASONS_REWARDS_TSV = os.path.join(REPO, "tsv", "season_rewards.tsv")
SEASONS_TSV = os.path.join(REPO, "tsv", "fallout76_seasons.tsv")

csv.field_size_limit(10 ** 9)

# The keyword that defines a beer stein in the game files.
STEIN_KEYWORD = "BeerSteinsKeyword"
# Dev / cut prefixes — an EDID that starts with one of these is excluded
# entirely (shared with the plan pipeline's HARD_DEV_CODES intent).
DEV = re.compile(r"^(zzz|CUT_|DEL_|DEBUG|POST_)", re.I)
SCORE_RE = re.compile(r"^SCORE_S(\d+)_", re.I)

_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def route(url):
    if not url or asset_paths is None:
        return url
    try:
        return asset_paths.asset_url(url)
    except Exception:                                # noqa: BLE001
        return url


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def slug(s):
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", (s or "").lower())).strip("_")


def clean_desc(s):
    s = re.sub(r"\s*-\s*C\.A\.M\.P\. ITEMS APPEAR.*$", "", s or "", flags=re.I)
    # A couple of season_rewards rows are quote-wrapped in the TSV.
    return s.strip().strip('"').strip()


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def load_steins():
    """Every MISC item carrying BeerSteinsKeyword, minus dev/cut records."""
    path = tsv_source.newest("MISC_Export_*.tsv", channel=CHANNEL)
    out = []
    with open(path, encoding="latin1", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            kw = r.get("Keywords") or ""
            if STEIN_KEYWORD not in kw:
                continue
            edid = (r.get("EDID") or "").strip()
            if not edid or DEV.search(edid):
                continue
            out.append({
                "formid": (r.get("FormID") or "").strip(),
                "edid": edid,
                "full": (r.get("FULL") or "").strip(),
                "keywords": kw,
                "value": (r.get("Value") or "").strip(),
            })
    return path, out


def load_season_steins():
    """season_rewards.tsv stein rows -> {season:int -> [row...]} and a name index.

    Each row carries the hosted scoreboard imageUrl, rank and flavour text."""
    by_season = {}
    by_name = {}
    with open(SEASONS_REWARDS_TSV, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            ent = (r.get("storefrontEntitlement") or "").strip()
            name = (r.get("name") or "").strip()
            img = (r.get("imageUrl") or "").strip()
            if not (name and img and "tein" in name.lower()):
                # not a stein reward row
                pass
            row = {
                "name": name,
                "img": img,
                "rank": (r.get("rank") or "").strip(),
                "desc": (r.get("description") or "").strip(),
                "ent": ent,
            }
            m = re.match(r"SCORE_S(\d+)_ENTM_CAMP_FloorDecor_Stein_(.+)$", ent, re.I)
            if m:
                row["theme"] = m.group(2).lower()
                by_season.setdefault(int(m.group(1)), []).append(row)
            if name and img and "tein" in name.lower():
                by_name.setdefault(norm(name), row)
    return by_season, by_name


def load_seasons():
    """fallout76_seasons.tsv -> {season:int -> {name, added 'MMM YYYY'}}."""
    out = {}
    if not os.path.exists(SEASONS_TSV):
        return out
    with open(SEASONS_TSV, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            num = (r.get("SeasonNumber") or "").strip()
            if not num.isdigit():
                continue
            name = (r.get("SeasonName") or "").strip()
            added = ""
            start = (r.get("StartDate") or "").strip()   # d/m/YYYY
            md = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", start)
            if md:
                mi = int(md.group(2))
                if 1 <= mi <= 12:
                    added = f"{_MONTHS[mi - 1]} {md.group(3)}"
            out[int(num)] = {"name": name, "added": added}
    return out


# ---------------------------------------------------------------------------
# Seasonal-events image hook (image priority 2) — FORWARD-COMPATIBLE
# ---------------------------------------------------------------------------
# The seasonal EVENT pages (Fasnacht, Meat Week, Invaders from Beyond, Mothman
# Equinox, Mischief Night, …) are not built yet, so no event reward dataset
# carries an image path today and this resolves NOTHING — every non-scoreboard
# stein falls through cleanly to the placeholder. It is wired generatively, the
# same way the scoreboard reuse is: when those pages ship and their reward JSON
# grows an image field, the next build picks the art up with no edit here.
#
# What it reads (channel-aware — PTS dist/pts/ copy wins, then live dist/):
EVENT_IMG_REL = [
    "events_rewards.json",                                   # public/seasonal event reward pools
    os.path.join("seasonal_events", "seasonal_events_rewards.json"),
    os.path.join("events", "events_rewards.json"),
    "seasonal_events_images.json",                           # optional future image manifest
]
# Image field names it will accept on a reward object — extend if the event
# pages adopt another name.
EVENT_IMG_FIELDS = ("images", "image", "imageUrl", "img", "icon", "thumb", "art")


def _name_key(s):
    """Normalise a reward name for matching: drop a 'Plan:'/'Recipe:' prefix and
    the word 'beer' so 'Plan: Scorchbeast Queen Beer Stein' == 'Scorchbeast
    Queen Stein'."""
    s = re.sub(r"^\s*(plan|recipe)\s*:\s*", "", s or "", flags=re.I)
    s = re.sub(r"\bbeer\b", "", s, flags=re.I)
    return norm(s)


def _first_image(obj):
    for k in EVENT_IMG_FIELDS:
        v = obj.get(k)
        if isinstance(v, list) and v:
            v = v[0]
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def load_event_images():
    """{key -> imageUrl} indexed by token.formid / token.edid / formid / edid and
    normalised name, for any seasonal-event reward that carries an image. Empty
    (so every stein falls through to placeholder) until the event pages ship
    image data. Best-effort: missing/oldshape files are skipped, never fatal."""
    idx = {}
    dist_dirs = []
    if PTS:
        dist_dirs.append(os.path.join(REPO, "dist", "pts"))
    dist_dirs.append(os.path.join(REPO, "dist"))

    def add(obj):
        img = _first_image(obj)
        if not img:
            return
        img = route(img)
        tok = obj.get("token") or {}
        keys = [tok.get("formid"), tok.get("edid"), obj.get("formid"),
                obj.get("edid"), _name_key(obj.get("name") or obj.get("full") or "")]
        for key in keys:
            if key:
                idx.setdefault(str(key).lower(), img)

    def walk(o):
        if isinstance(o, dict):
            if (o.get("name") or o.get("token")) and any(k in o for k in EVENT_IMG_FIELDS):
                add(o)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    for dd in dist_dirs:
        for rel in EVENT_IMG_REL:
            p = os.path.join(dd, rel)
            if not os.path.exists(p):
                continue
            try:
                with open(p, encoding="utf-8", errors="replace") as fh:
                    walk(json.load(fh))
            except (OSError, ValueError):
                continue
    return idx


def event_image_for(idx, formid, edid, full):
    """Priority-2 lookup: by FormID, then EditorID, then normalised name."""
    if not idx:
        return ""
    for key in (str(formid or "").lower(), str(edid or "").lower(), _name_key(full)):
        if key and key in idx:
            return idx[key]
    return ""


# ---------------------------------------------------------------------------
# Per-stein resolution
# ---------------------------------------------------------------------------
def match_season_image(season, full, edid, season_steins, by_name):
    """Return (imageUrl, rank, reward_desc) reusing the scoreboard artwork.

    Priority: single stein reward that season -> theme-token match among that
    season's stein rewards -> name match against any stein reward row."""
    cands = season_steins.get(season, [])
    row = None
    if len(cands) == 1:
        row = cands[0]
    elif cands:
        # theme token near "Stein" in the MISC EDID
        m = re.search(r"SCORE_S\d+_(?:Stein_)?(.+?)(?:_?Stein)?(?:_Misc)?$", edid)
        tok = (m.group(1).lower() if m else edid.lower())
        for c in cands:
            th = c.get("theme", "")
            if th and (th in tok or tok in th):
                row = c
                break
            if norm(c["name"]) == norm(full):
                row = c
                break
        if row is None:
            row = cands[0]
    if row is None:
        row = by_name.get(norm(full))
    if not row:
        return "", "", ""
    return route(row["img"]), row.get("rank", ""), row.get("desc", "")


# Non-season obtain classification. EDID-convention inference (see
# plan-obtain-pipeline: conventions are inference, not resolution) — worded
# plainly and flagged obtain_verified:false so Duchess can confirm the wording.
def classify_nonseason(edid, full):
    e = edid
    if e.startswith("ATX_"):
        return ("Atom Shop", "Available from the Atom Shop for Atoms.", False)
    if "MothmanEquinox" in e:
        return ("Seasonal Event", "Reward from the Mothman Equinox seasonal event.", False)
    if e.startswith("MN2_") or "MischiefNight" in e:
        return ("Seasonal Event", "Reward from the Mischief Night (Halloween) seasonal event.", False)
    if "VeggieMan" in e or "Fasnacht" in e:
        return ("Seasonal Event", "Reward from the Fasnacht Day seasonal event.", False)
    if "Chally" in e:
        return ("Seasonal Event", "Reward from the Meat Week seasonal event.", False)
    if "AlienCollectable" in e:
        return ("Seasonal Event", "Reward from the Invaders from Beyond seasonal event.", False)
    if "BigBloom" in e:
        return ("Seasonal Event", "Reward from a seasonal event.", False)
    if "ScorchbeastQueen" in e:
        return ("Event", "Associated with the Scorchbeast Queen public event.", False)
    if e.startswith("Helv_Deco_"):
        return ("Craftable", "A craftable Fasnacht C.A.M.P. decoration.", False)
    return ("Other", "Source not resolved from the game files — see Technical.", False)


def tradeable_of(keywords):
    if "NonPlayerTradable" in keywords or "UnsellableObject" in keywords:
        return False
    return None   # unresolved -> renders as the muted "unknown" trade pill


def build():
    misc_path, steins = load_steins()
    season_steins, by_name = load_season_steins()
    seasons = load_seasons()
    event_images = load_event_images()   # image priority 2 — empty until event pages ship art

    items = []
    reused = []        # (name, imageUrl) — priority 1: scoreboard/season art
    from_events = []   # (name, imageUrl) — priority 2: seasonal-events art
    needs_art = []     # (edid, name, obtain line) — priority 3: placeholder
    verify = []        # (name, obtain line) — inferred, not resolved

    # Order: season steins by season (then FormID), then non-season A-Z.
    def sort_key(s):
        m = SCORE_RE.match(s["edid"])
        if m:
            return (0, int(m.group(1)), int(s["formid"] or "0", 16))
        return (1, 0, norm(s["full"]))

    for s in sorted(steins, key=sort_key):
        edid, full, fid = s["edid"], s["full"], s["formid"]
        m = SCORE_RE.match(edid)
        images = []
        image_source = ""
        added = ""
        desc = ""
        scoreboard_img = ""
        if m:
            season = int(m.group(1))
            sinfo = seasons.get(season, {})
            theme = sinfo.get("name", "")
            added = sinfo.get("added", "")
            scoreboard_img, rank, rdesc = match_season_image(season, full, edid, season_steins, by_name)
            desc = clean_desc(rdesc)
            # Wording mirrors the scoreboard-art builder: S1-15 were claimed,
            # S16+ are bought with season tickets.
            board = f"Season {season}" + (f" — {theme}" if theme else "")
            if season <= 15:
                line = f"Claim from the Season {season} Scoreboard"
            else:
                line = f"Purchase with tickets from the Season {season} Scoreboard"
            if theme:
                line = line.replace(
                    f"Season {season} Scoreboard",
                    f"{re.sub(r'^the ', '', theme, flags=re.I)} Scoreboard (Season {season})")
            line += (f", rank {rank}." if rank else ".")
            source = "Scoreboard"
            category = "Beer Stein — Scoreboard reward"
            rank_i = int(rank) if str(rank).isdigit() else 0
        else:
            season = 0
            board = ""
            rank_i = 0
            source, line, ok = classify_nonseason(edid, full)
            category = f"Beer Stein — {source}"
            if not ok:
                verify.append((full, line))

        # Image priority chain:
        #   (1) scoreboard / season-reward art (hosted, reused, no copy)
        #   (2) seasonal-events art (forward-compatible hook — empty until built)
        #   (3) placeholder / needs-art
        if scoreboard_img:
            img, image_source = scoreboard_img, "season"
            reused.append((full, img))
        else:
            ev = event_image_for(event_images, fid, edid, full)
            if ev:
                img, image_source = ev, "seasonal-event"
                from_events.append((full, img))
            else:
                img, image_source = "", ""
                needs_art.append((edid, full, line))
        images = [img] if img else []

        items.append({
            "kind": "plan", "brand": "df", "type": "beer-steins",
            "id": f"STEIN_{edid}",
            "name": full or edid,
            "display_name": full or edid,
            "has_image_box": True,
            "image_dir": "beer-steins",
            "obtain": line,
            "category_label": category,
            "obtain_routes": [],
            "obtain_unlocks": [line],
            "obtain_ledger": [{"label": source, "unlocks": [0], "drop": "N/A"}],
            "plan_item": None, "cobj": None, "cnam": None,
            "tradeable": tradeable_of(s["keywords"]),
            "stops_dropping": None, "effects": None,
            "cut": False, "cut_reason": None,
            "images": images,
            "image_source": image_source,
            # Emit both keys: makeCanonicalPlanRow reads `desc`, the schema-v2
            # sibling datasets carry `description` — keep them in sync.
            "desc": desc,
            "description": desc,
            "added": added,
            "season": season,
            "season_name": (seasons.get(season, {}) or {}).get("name", "") if season else "",
            "board": board,
            "rank": rank_i,
            "source": source,
            "entitlement": edid,
            "formid": fid,
            "texture": "",
        })

    doc = {
        "schemaVersion": SCHEMA,
        "generated": datetime.now(timezone.utc).isoformat(),
        "isPts": PTS,
        "title": "Beer Steins",
        "sub": "Track which beer steins you have collected.",
        "noun": {"one": "beer stein", "many": "beer steins"},
        "keep_order": True,   # season order first, then non-season A-Z
        "note": [
            "Every beer stein in the game.",
            "Membership is read from the game's own BeerSteinsKeyword, so a new "
            "stein appears here on the next data update. Images resolve "
            "scoreboard/season art first, then seasonal-event art, then a "
            "placeholder — all reused, never copied.",
        ],
        "source_files": [
            os.path.basename(misc_path),
            os.path.basename(SEASONS_REWARDS_TSV),
            os.path.basename(SEASONS_TSV),
        ],
        "count": len(items),
        "groups": [{"key": "all", "label": "", "items": items}],
    }
    os.makedirs(DIST_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)

    print(f"[beer-steins] wrote {OUT} — {len(items)} steins "
          f"(images: {len(reused)} scoreboard, {len(from_events)} seasonal-event, "
          f"{len(needs_art)} need art)")
    print(f"  sources: {', '.join(doc['source_files'])}")
    print(f"  seasonal-events image hook: {len(event_images)} indexed image(s) "
          f"{'— NONE yet, hook is in place for when the event pages ship art' if not event_images else 'available'}")
    if from_events:
        print("  RESOLVED FROM SEASONAL-EVENTS ART:")
        for name, img in from_events:
            print(f"    - {name}  <- {img}")
    if needs_art:
        print("  NEEDS ART (no scoreboard OR seasonal-event image):")
        for edid, name, line in needs_art:
            print(f"    - {name}  [{edid}]")
    if verify:
        print("  VERIFY OBTAIN (inferred from EditorID, confirm wording):")
        for name, line in verify:
            print(f"    - {name}: {line}")


if __name__ == "__main__":
    build()
