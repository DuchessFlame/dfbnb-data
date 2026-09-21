#!/usr/bin/env python3
r"""
build_wallpapers_json.py
------------------------
Builds the data feed for the DF/BNB Wallpaper checklist at
/df/atom-shop/wallpaper/ (rendered by df-bnb-atom-shop.js).

Two modes:
  (default / live)  reads tsv/      -> dist/wallpapers.json
  --pts             reads tsv/pts/  -> dist/pts/wallpapers.json

GENERATIVE — no hand-kept list. Membership is Bethesda's own store filter:
every ENTM record carrying ATX_Entitlement_Filter_Store_CAMP_Wallpapers is a
wallpaper, whichever route it comes from (Atom Shop, Scoreboard, Fallout 1st,
seasonal events...). A new wallpaper appears on the next TSV drop with no edit.
Cut records (zzz / ZZZ / REUSE / TEMPLATE EDIDs) are dropped.

IMAGES
======
Scoreboard wallpapers REUSE the tile the site already serves from
/wp-content/uploads/season_images/season-N/ — looked up through
reusable_images (the season upload manifests in dist/), so the same art is
never uploaded twice. Everything else resolves to the wallpaper folder, named
after its DDS texture, lowercased, with the _l suffix dropped:

    ETDI  "ATX_CAMP_WallPaper_Tavern.dds"
      ->  /wp-content/uploads/guide-images/atom-shop/wallpaper/atx_camp_wallpaper_tavern.avif

A scoreboard wallpaper whose season tile was never uploaded falls back to the
wallpaper folder too, so it is still one file in one place.

USAGE
=====
    python src/build_wallpapers_json.py
    python src/build_wallpapers_json.py --pts
    python src/build_wallpapers_json.py --report-missing-images
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta

import reusable_images
import tsv_repair
import tsv_source

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
LIVE_TSV_DIR = os.path.join(SCRIPT_DIR, "..", "tsv")
LIVE_DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist")
DIST_DIR = os.path.join(LIVE_DIST_DIR, "pts" if PTS else "")
DIST_FILE = os.path.join(DIST_DIR, "wallpapers.json")

IMAGE_BASE = "/wp-content/uploads/guide-images/atom-shop/wallpaper/"
FILTER_KYWD = "ATX_Entitlement_Filter_Store_CAMP_Wallpapers"
TAG = "[wallpapers]"

# Local AVIF library — used only by --report-missing-images, never by the build.
LOCAL_AVIF_DIR = os.environ.get(
    "DFBNB_WALLPAPER_AVIF_DIR",
    os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff",
                 ".Atom Shop", "Wallpaper"),
)
for _a in sys.argv:
    if _a.startswith("--avif-dir="):
        LOCAL_AVIF_DIR = _a.split("=", 1)[1]

# Season 16 (Duel with the Devil) replaced claim-as-you-rank with tickets.
TICKETS_FROM_SEASON = 16

_FIRST_SEEN_FILENAME = "wallpapers_first_seen.json"
_NEW_CUTOFF_DAYS = 31

_CUT_PREFIXES = ("ZZZ", "ZZ_", "DEBUG", "TEMPLATE", "DEL_", "CUT_", "DONOTUSE", "REUSE_")
_RE_SEASON = re.compile(r"\bSCORE_S(\d+)_", re.IGNORECASE)
_RE_MINISEASON = re.compile(r"SCORE_MiniSeason_(?:(\d{4})_)?([A-Za-z0-9]+)_", re.IGNORECASE)
_RE_CHAL_REWARD = re.compile(r"ChallengeReward_([A-Za-z0-9_]+)")
_RE_RARITY = re.compile(r"ATX_ItemRarity_([A-Za-z]+)")
_RE_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_DESC_TAIL = re.compile(r"\s*-\s*C\.A\.M\.P\. ITEMS APPEAR.*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def is_cut(edid: str) -> bool:
    u = (edid or "").upper()
    return u.startswith(_CUT_PREFIXES) or bool(re.search(r"_REUSE\d*$|_REUSE_", u))


def prettify(token: str) -> str:
    parts = []
    for chunk in (token or "").replace("_", " ").split():
        split = _RE_CAMEL.sub(" ", chunk).split()
        if len(split) > 1 and len(split[-1]) == 1:
            split = split[:-2] + ["".join(split[-2:])]
        parts.extend(split)
    return " ".join(parts)


def clean_desc(desc: str) -> str:
    d = " ".join((desc or "").split())
    return _DESC_TAIL.sub("", d).strip(" -").strip()


def display_name(full: str, edid: str) -> str:
    name = (full or "").strip() or prettify(edid.split("WallPaper_")[-1])
    # "Starter Wallpaper Wallpaper" -> "Starter Wallpaper"
    name = re.sub(r"\b(Wallpaper)\s+\1\b", r"\1", name, flags=re.I)
    # The page is called Wallpaper, so the trailing word is noise in every row.
    short = re.sub(r"\s+Wallpaper$", "", name, flags=re.I).strip()
    return short or name


def texture_file(etdi: str) -> str:
    """DDS basename -> lowercase .avif, _l dropped. "" when there is no texture."""
    name = os.path.basename((etdi or "").strip().replace("\\", "/"))
    if not name.lower().endswith(".dds"):
        return ""
    stem = re.sub(r"_l$", "", name[:-4], flags=re.I).lower()
    return stem + ".avif"


def read_tsv(path: str) -> list[dict]:
    # tsv_repair re-joins rows the xEdit export split on a leading quote.
    return tsv_repair.read_dicts(path)


def season_names() -> dict:
    path = os.path.join(LIVE_TSV_DIR, "fallout76_seasons.tsv")
    out = {}
    if os.path.exists(path):
        for r in read_tsv(path):
            try:
                out[int(r.get("SeasonNumber") or 0)] = (r.get("SeasonName") or "").strip()
            except ValueError:
                pass
    return out


def challenge_names() -> dict:
    try:
        path = tsv_source.newest("CHAL_Export_*.tsv", channel=CHANNEL, required=False)
    except Exception:
        path = None
    if not path:
        return {}
    return {(r.get("EDID") or "").strip().upper(): (r.get("FULL") or "").strip()
            for r in read_tsv(path) if (r.get("EDID") or "").strip()}


def scoreboard_text(num: int, names: dict, premium: bool) -> str:
    theme = names.get(num, "")
    theme = re.sub(r"^the\s+", "", theme, flags=re.I)
    board = f"the {theme} Scoreboard (Season {num})" if theme else f"the Season {num} Scoreboard"
    verb = "Purchase with tickets from" if num >= TICKETS_FROM_SEASON else "Claim from"
    text = f"{verb} {board}."
    if premium:
        text += " Requires the paid Season Pass."
    return text


def route(edid, source, text, **extra):
    r = {"method": edid, "source": source, "text": text,
         "season": None, "seasonName": "", "premium": False}
    r.update(extra)
    return r


def resolve_obtain(row: dict, names: dict, chal: dict) -> dict:
    edid = row["EDID"]
    ref = row.get("ReferencedBy") or ""
    premium = (row.get("XALG_Flags") or "").strip().lower() == "premium"

    m = _RE_SEASON.search(edid)
    if m:
        num = int(m.group(1))
        return route("scoreboard", "Scoreboard", scoreboard_text(num, names, premium),
                     season=num, seasonName=names.get(num, ""), premium=premium)

    m = _RE_MINISEASON.search(edid)
    if m:
        nm = prettify(m.group(2))
        return route("mini-season", "Mini-Season", f"Purchase with tickets from the {nm} Mini-Season.",
                     seasonName=nm, premium=premium)

    cm = _RE_CHAL_REWARD.search(ref)
    if cm:
        chal_edid = cm.group(1)
        name = chal.get(chal_edid.upper(), "")
        de = re.match(r"ATX_DE(20\d\d)_([A-Za-z0-9]+)_", chal_edid)
        if de:
            year, event = de.group(1), prettify(de.group(2))
            if not event.lower().endswith("event"):
                event += " Event"
            text = f"Rewarded during the {year} {event}"
            text += f" for completing \"{name}\"." if name else "."
            text += " Only obtainable while that event is running."
            return route("seasonal-event", "Seasonal Event", text, seasonName=f"{year} {event}")
        return route("challenge", "Challenge",
                     f"Complete the challenge \"{name}\"." if name else "Awarded by completing a challenge.")

    if edid.startswith("ATX_TP_"):
        return route("twitch-prime", "Prime Gaming",
                     "Awarded through a Twitch / Prime Gaming promotion. No longer obtainable.")
    if edid.startswith(("ATX_F1_", "F1_")) or "_F1_" in edid:
        return route("fallout-1st", "Fallout 1st", "Included with a Fallout 1st membership.", premium=True)
    if edid.startswith("ATX_NPE_"):
        return route("starter", "Starter", "Unlocked for every player as part of the new player experience.")
    return route("atom-shop", "Atom Shop",
                 "Purchased from the Atom Shop, on its own or as part of a bundle.", premium=premium)


# ---------------------------------------------------------------------------
# first-seen ledger (31-day NEW pill) — same scheme as player icons
# ---------------------------------------------------------------------------
def load_first_seen() -> tuple[dict, bool]:
    path = os.path.join(LIVE_TSV_DIR, _FIRST_SEEN_FILENAME)
    if not os.path.exists(path):
        return {}, True
    try:
        with open(path, encoding="utf-8") as f:
            return (json.load(f) or {}).get("byFormId", {}), False
    except Exception as e:  # pragma: no cover
        print(f"{TAG} [WARN] could not read {_FIRST_SEEN_FILENAME}: {e}", file=sys.stderr)
        return {}, False


def save_first_seen(first_seen: dict) -> None:
    path = os.path.join(LIVE_TSV_DIR, _FIRST_SEEN_FILENAME)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"schema": 1, "byFormId": dict(sorted(first_seen.items()))}, f, indent=2)
        f.write("\n")


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------
def build() -> dict:
    entm_path = tsv_source.newest("ENTM_Export_*.tsv", channel=CHANNEL)
    print(f"{TAG} ENTM: {os.path.basename(entm_path)}")
    rows = read_tsv(entm_path)
    names = season_names()
    chal = challenge_names()
    hosted = reusable_images.build_index(LIVE_DIST_DIR)
    print(f"{TAG} {hosted.summary()}")

    out, dropped_cut, reused = [], 0, 0
    for r in rows:
        if FILTER_KYWD not in (r.get("KEYWORDS") or ""):
            continue
        edid = (r.get("EDID") or "").strip()
        if is_cut(edid):
            dropped_cut += 1
            continue

        etdi = (r.get("ETDI") or "").strip()
        filename = texture_file(etdi)
        obtain = resolve_obtain(r, names, chal)

        # Scoreboard art already lives in season_images — reuse it rather than
        # uploading the same tile again into the wallpaper folder.
        image_url, extra = "", []
        if obtain["source"] == "Scoreboard":
            hit = hosted.find(edid=edid, texture=etdi)
            if hit and "/season_images/" in hit:
                image_url = hit
                reused += 1
                extra = [u for u in hosted.find_all(texture=etdi)
                         if u != hit and "/season_images/" in u][:3]
        if not image_url and filename:
            image_url = IMAGE_BASE + filename

        kw = r.get("KEYWORDS") or ""
        rm = _RE_RARITY.search(kw)
        rarity = rm.group(1) if rm and rm.group(1).lower() != "none" else ""

        out.append({
            "formId": (r.get("FormID") or "").strip().upper(),
            "edid": edid,
            "name": display_name(r.get("FULL"), edid),
            "fullName": (r.get("FULL") or "").strip(),
            "desc": clean_desc(r.get("DESC")),
            "rarity": rarity,
            "premium": (r.get("XALG_Flags") or "").strip().lower() == "premium",
            "texture": os.path.basename(etdi.replace("\\", "/")),
            "imageFilename": filename,
            "imageUrl": image_url,
            "images": [image_url] + extra if image_url else [],
            "imageReused": bool(image_url) and "/season_images/" in image_url,
            "source": obtain["source"],
            "howToObtain": obtain,
            "isNew": False,
        })

    out.sort(key=lambda i: (i["name"].lower(), i["edid"].lower()))

    new_count = 0
    if not PTS:
        first_seen, bootstrap = load_first_seen()
        stamp = "2020-01-01" if bootstrap else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for i in out:
            first_seen.setdefault(i["formId"], stamp)
        save_first_seen(first_seen)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=_NEW_CUTOFF_DAYS)).strftime("%Y-%m-%d")
        for i in out:
            i["isNew"] = first_seen.get(i["formId"], "2020-01-01") >= cutoff
            new_count += i["isNew"]

    by_source: dict[str, int] = {}
    for i in out:
        by_source[i["source"]] = by_source.get(i["source"], 0) + 1
    print(f"{TAG} wallpapers: {len(out)} (dropped {dropped_cut} cut)  "
          f"season art reused: {reused}  NEW: {new_count}")
    for k in sorted(by_source, key=lambda k: -by_source[k]):
        print(f"{TAG}   {by_source[k]:4d}  {k}")

    return {
        "generated": datetime.now(timezone.utc).isoformat(),
        "schemaVersion": 1,
        "isPts": PTS,
        "source": os.path.basename(entm_path),
        "observed": tsv_source.observed(),
        "newCutoffDays": _NEW_CUTOFF_DAYS,
        "imageBase": IMAGE_BASE,
        "count": len(out),
        "countsBySource": by_source,
        "wallpapers": out,
    }


def report_missing_images(payload: dict) -> int:
    """Wallpaper-folder images not present in the local AVIF library."""
    if not os.path.isdir(LOCAL_AVIF_DIR):
        print(f"{TAG} local AVIF library not found: {LOCAL_AVIF_DIR}")
        return 0
    have = {n.lower() for n in os.listdir(LOCAL_AVIF_DIR) if n.lower().endswith(".avif")}
    need = [w for w in payload["wallpapers"] if not w["imageReused"]]
    missing = [w for w in need if w["imageFilename"] not in have]
    print(f"{TAG} wallpaper-folder coverage: {len(need) - len(missing)}/{len(need)} "
          f"(+{payload['count'] - len(need)} reusing season art)")
    for w in missing:
        print(f"{TAG}   MISSING  {w['imageFilename']:60} {w['edid']}")
    used = {w["imageFilename"] for w in need}
    spare = sorted(have - used)
    print(f"{TAG} AVIFs in the folder the page does not use: {len(spare)}")
    for n in spare:
        print(f"{TAG}   UNUSED   {n}")
    return len(missing)


def main() -> None:
    payload = build()
    os.makedirs(DIST_DIR, exist_ok=True)
    with open(DIST_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"{TAG} wrote {os.path.relpath(DIST_FILE, os.path.join(SCRIPT_DIR, '..'))}")
    if "--report-missing-images" in sys.argv:
        report_missing_images(payload)


if __name__ == "__main__":
    main()
