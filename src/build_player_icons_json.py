#!/usr/bin/env python3
r"""
build_player_icons_json.py
--------------------------
Builds the data feed for the DF/BNB Player Icons page at
/df/atom-shop/player-icons/.

Two modes:
  (default / live)  reads tsv/      -> dist/player_icons.json
  --pts             reads tsv/pts/  -> dist/pts/player_icons.json

The global PTS toggle (df-bnb-pts.js) redirects fetches from dist/ to dist/pts/,
so the renderer loads the right twin automatically.

GENERATIVE — there is no hand-maintained icon list anywhere. Every icon is
enumerated from the newest ENTM export by the AvatarIcons entitlement-category
keyword (with a PlayerIcon EDID fallback), so a new icon in the game files
appears on both the live and PTS pages with no JS edit. See the
"generative-not-hand-maintained" rule.

WHAT IT READS
=============
  ENTM_Export_*.tsv          the icon records themselves (newest, chronological)
  season_rewards.tsv         curated scoreboard rows -> season / rank / page
  fallout76_seasons.tsv      season numbers -> season names
  CHAL_Export_*.tsv          challenge EDID -> challenge name, and cut status
  player_icons_first_seen.json   first-seen ledger for the 31-day NEW pill

Curated metadata (season_rewards.tsv, fallout76_seasons.tsv) is always read from
the live tsv/ directory. First-seen persistence is skipped in PTS mode so
preview content never pollutes the live ledger or shows a NEW pill.

IMAGES
======
Every icon resolves to ONE avif named after its DDS texture, lowercased:

    ETDI  "ATX_PlayerIcon_Cappy.dds"
      ->  atx_playericon_cappy.avif
      ->  /wp-content/uploads/guide-images/atom-shop/player-icons/atx_playericon_cappy.avif

This mirrors the player-titles convention (icons live in their own wp-content
folder, addressed by texture filename rather than display name) and is the same
name the scoreboard rows now use — one folder, one filename rule, no guesswork.

QUOTE-SPLIT ROWS
================
Every read goes through tsv_repair, which re-joins rows the xEdit export broke
in half. A value that starts with a double quote gets split by the export's
TStringList.DelimitedText round-trip, shifting every later column right by one,
so a naive read finds the texture PATH in ETDI and no filename at all. The
S10-S12 scoreboard icons are the visible casualties — exactly the set that had
been falling back to season art. Do not switch to a plain DictReader because
"the export looks fine"; it looks fine because that module is running.

USAGE
=====
    python src/build_player_icons_json.py
    python src/build_player_icons_json.py --pts
    python src/build_player_icons_json.py --report-missing-images
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta

import tsv_repair
import tsv_source

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
LIVE_TSV_DIR = os.path.join(SCRIPT_DIR, "..", "tsv")
DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist", "pts" if PTS else "")
DIST_FILE = os.path.join(DIST_DIR, "player_icons.json")

IMAGE_BASE = ("https://www.buffsnbrew.com/wp-content/uploads/"
              "guide-images/atom-shop/player-icons/")

TAG = "[player-icons]"

# Local library of converted AVIFs. Used only for the coverage report — the
# build never depends on it, so CI (which has no OneDrive mount) still works.
# Override with --avif-dir=<path> or DFBNB_PLAYER_ICON_AVIF_DIR.
LOCAL_AVIF_DIR = os.environ.get(
    "DFBNB_PLAYER_ICON_AVIF_DIR",
    os.path.join(os.path.expanduser("~"), "OneDrive", "Guides and Stuff",
                 ".Atom Shop", "Player Icons"),
)
for _a in sys.argv:
    if _a.startswith("--avif-dir="):
        LOCAL_AVIF_DIR = _a.split("=", 1)[1]

# ---------------------------------------------------------------------------
# First-seen persistence — 31-day rolling NEW pill.
# Schema for tsv/player_icons_first_seen.json:
#   { "schema": 1, "byFormId": { "0057AE07": "2026-08-28", ... } }
# ---------------------------------------------------------------------------
_FIRST_SEEN_FILENAME = "player_icons_first_seen.json"
_NEW_CUTOFF_DAYS = 31


def load_first_seen() -> dict:
    path = os.path.join(LIVE_TSV_DIR, _FIRST_SEEN_FILENAME)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return (json.load(f) or {}).get("byFormId", {})
    except Exception as e:  # pragma: no cover - defensive
        print(f"{TAG} [WARN] could not read {_FIRST_SEEN_FILENAME}: {e}", file=sys.stderr)
        return {}


def save_first_seen(first_seen: dict) -> None:
    path = os.path.join(LIVE_TSV_DIR, _FIRST_SEEN_FILENAME)
    payload = {"schema": 1, "byFormId": dict(sorted(first_seen.items()))}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def update_first_seen(first_seen: dict, form_ids, bootstrap: bool) -> None:
    """Stamp today's date on any FormID we have not seen before.

    On the very first run there is no ledger, so everything would look new at
    once. `bootstrap` back-dates that first population so the page does not
    open with 400 NEW pills.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    stamp = "2020-01-01" if bootstrap else today
    for fid in form_ids:
        first_seen.setdefault(fid, stamp)


def compute_new_cutoff() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=_NEW_CUTOFF_DAYS)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# TSV reading + the shifted-row repair
# ---------------------------------------------------------------------------
def read_tsv(path: str, repair: bool = True) -> list[dict]:
    """Read an export, re-joining any quote-split rows.

    The repair lives in tsv_repair, shared with every builder that reads an
    export with free-text name/description columns — see that module for the
    TStringList.DelimitedText bug it undoes. About 219 ENTM rows are affected in
    the newest live export, the S10-S12 scoreboard icons among them, whose
    texture filename otherwise lands in the wrong column entirely.
    """
    if repair:
        return tsv_repair.read_dicts(path)
    with open(path, encoding="utf-8-sig", newline="") as f:
        rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(rd)
        return [dict(zip(header, r + [""] * (len(header) - len(r))))
                for r in rd if r]


# ---------------------------------------------------------------------------
# Icon selection
# ---------------------------------------------------------------------------
ICON_CATEGORY_KYWD = "ATX_Entitlement_Category_Store_AvatarIcons"

# Placeholder / dev / reused shells. These carry no texture and no real name —
# "cut ones dropped" per the page brief.
_CUT_PREFIXES = ("ZZZ", "ZZ_", "DEBUG", "TEMPLATE", "DEL_", "CUT_", "DONOTUSE", "REUSE_")
_RE_REUSE_SUFFIX = re.compile(r"_REUSE\d*$|_REUSE_", re.IGNORECASE)
_RE_RARITY = re.compile(r"ATX_ItemRarity_([A-Za-z]+)")
_RE_SEASON = re.compile(r"\bSCORE_S(\d+)(?:_|\b)", re.IGNORECASE)
_RE_MINISEASON = re.compile(r"SCORE_MiniSeason_(?:(\d{4})_)?([A-Za-z0-9]+)_", re.IGNORECASE)
_RE_CHAL_REWARD = re.compile(r"ChallengeReward_([A-Za-z0-9_]+)")
_RE_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")

# DESC boilerplate the game appends to every icon record.
_DESC_TRAILERS = (
    "- UNLOCKS A PLAYER ICON. -",
    "- UNLOCKS A PLAYER ICON.-",
)


def is_cut(edid: str) -> bool:
    u = (edid or "").upper()
    if u.startswith(_CUT_PREFIXES):
        return True
    if _RE_REUSE_SUFFIX.search(u):
        return True
    return False


def clean_desc(desc: str) -> str:
    d = " ".join((desc or "").split())
    for t in _DESC_TRAILERS:
        idx = d.upper().find(t.upper())
        if idx >= 0:
            d = d[:idx]
    return d.strip(" -").strip()


def rarity_from_keywords(keywords: str) -> str:
    m = _RE_RARITY.search(keywords or "")
    if not m:
        return ""
    r = m.group(1)
    return "" if r.lower() == "none" else r


def image_filename(etdi: str) -> str:
    """DDS basename -> lowercase .avif. Returns "" when there is no texture."""
    name = os.path.basename((etdi or "").strip().replace("\\", "/"))
    if not name.lower().endswith(".dds"):
        return ""
    return name[:-4].lower() + ".avif"


def prettify(token: str) -> str:
    """CamelCase / snake_case EDID fragment -> readable words.

    The camel split is suppressed when it would strand a single trailing
    capital, so "BoS" stays "BoS" instead of becoming "Bo S".
    """
    parts = []
    for chunk in (token or "").replace("_", " ").split():
        split = _RE_CAMEL.sub(" ", chunk).split()
        if len(split) > 1 and len(split[-1]) == 1:
            split = split[:-2] + ["".join(split[-2:])]
        parts.extend(split)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# How to obtain
# ---------------------------------------------------------------------------
def build_season_lookup() -> tuple[dict, dict]:
    """storefrontEntitlement -> curated scoreboard row, and season num -> name."""
    rewards_path = os.path.join(LIVE_TSV_DIR, "season_rewards.tsv")
    seasons_path = os.path.join(LIVE_TSV_DIR, "fallout76_seasons.tsv")

    by_ent: dict[str, dict] = {}
    if os.path.exists(rewards_path):
        for r in read_tsv(rewards_path):
            if (r.get("kind") or "").strip() != "playerIcon":
                continue
            ent = (r.get("storefrontEntitlement") or "").strip()
            if ent:
                by_ent[ent] = r

    names: dict[int, str] = {}
    if os.path.exists(seasons_path):
        for r in read_tsv(seasons_path):
            try:
                names[int(r.get("SeasonNumber") or 0)] = (r.get("SeasonName") or "").strip()
            except ValueError:
                continue
    return by_ent, names


def build_challenge_lookup() -> dict:
    """Challenge EDID -> (display name, is_cut)."""
    try:
        path = tsv_source.newest("CHAL_Export_*.tsv", channel=CHANNEL, required=False)
    except Exception:
        path = None
    if not path:
        return {}
    out = {}
    for r in read_tsv(path):
        edid = (r.get("EDID") or "").strip()
        if not edid:
            continue
        out[edid.upper()] = ((r.get("FULL") or "").strip(), is_cut(edid))
    return out


def season_label(num: int, names: dict) -> str:
    nm = names.get(num, "")
    return f"Season {num} - {nm}" if nm else f"Season {num}"


def resolve_obtain(row: dict, season_rows: dict, season_names: dict, chal: dict) -> dict:
    """Classify one icon into a single obtain route.

    Order matters: the curated scoreboard sheet wins over EDID guessing, and
    Twitch/Fallout 1st are checked before the generic ATX_ catch-all because
    those records are ATX_ too.
    """
    edid = row["EDID"]
    desc = (row.get("DESC") or "")
    ref = (row.get("ReferencedBy") or "")
    premium = (row.get("XALG_Flags") or "").strip().lower() == "premium"

    # 1. Curated scoreboard row — the richest source (season + rank/page).
    sr = season_rows.get(edid)
    if sr:
        num = int(sr.get("seasonNumber") or 0)
        rank = (sr.get("rank") or "").strip()
        page = (sr.get("page") or "").strip()
        label = season_label(num, season_names)
        if rank:
            where = f"Rank {rank}"
        elif page:
            where = f"Page {page}" if not page.startswith("B") else f"Bonus Page {page[1:]}"
        else:
            where = ""
        text = f"Reward from the {label} Scoreboard"
        if where:
            text += f" - {where}"
        text += "."
        if premium:
            text += " Requires the paid Season Pass."
        return {
            "method": "scoreboard",
            "source": "Scoreboard",
            "text": text,
            "season": num,
            "seasonName": season_names.get(num, ""),
            "rank": int(rank) if rank.isdigit() else None,
            "page": page or None,
            "premium": premium,
        }

    # 2. Mini-season.
    m = _RE_MINISEASON.search(edid)
    if m:
        nm = prettify(m.group(2))
        text = f"Reward from the {nm} Mini-Season."
        if premium:
            text += " Requires the paid Mini-Season Pass."
        return {"method": "mini-season", "source": "Mini-Season", "text": text,
                "season": None, "seasonName": nm, "rank": None, "page": None,
                "premium": premium}

    # 3. Twitch Prime / promotional drops.
    if edid.startswith("ATX_TP_") or "twitch prime" in desc.lower():
        return {"method": "twitch-prime", "source": "Twitch Prime",
                "text": "Awarded through a Twitch Prime promotion. No longer obtainable.",
                "season": None, "seasonName": "", "rank": None, "page": None,
                "premium": False}

    # 4. Fallout 1st.
    if (edid.startswith(("ATX_F1_", "F1_")) or "_F1_" in edid
            or "fallout 1st" in desc.lower()):
        return {"method": "fallout-1st", "source": "Fallout 1st",
                "text": "Included with a Fallout 1st membership.",
                "season": None, "seasonName": "", "rank": None, "page": None,
                "premium": True}

    # 5. Scoreboard record with no curated row yet (new season mid-datamine).
    m = _RE_SEASON.search(edid)
    if m:
        num = int(m.group(1))
        text = f"Reward from the {season_label(num, season_names)} Scoreboard."
        if premium:
            text += " Requires the paid Season Pass."
        return {"method": "scoreboard", "source": "Scoreboard", "text": text,
                "season": num, "seasonName": season_names.get(num, ""),
                "rank": None, "page": None, "premium": premium}

    # 6. Challenge reward. Nuclear Winter (Babylon) and the removed Vault
    #    raids both reward icons through challenges that are themselves cut,
    #    so say "no longer obtainable" rather than sending people looking.
    cm = _RE_CHAL_REWARD.search(ref)
    if cm:
        chal_edid = cm.group(1)
        name, cut = chal.get(chal_edid.upper(), ("", is_cut(chal_edid)))
        if "OverseerRank" in chal_edid or edid.startswith("Babylon_"):
            return {"method": "legacy", "source": "Legacy",
                    "text": "Earned through Nuclear Winter Overseer Rank progression. "
                            "Nuclear Winter was removed from the game - no longer obtainable.",
                    "season": None, "seasonName": "", "rank": None, "page": None,
                    "premium": False}
        if cut:
            return {"method": "legacy", "source": "Legacy",
                    "text": (f"Rewarded by the challenge \"{name}\", which is no longer "
                             "in the game - no longer obtainable." if name else
                             "Rewarded by a challenge that is no longer in the game - "
                             "no longer obtainable."),
                    "season": None, "seasonName": "", "rank": None, "page": None,
                    "premium": False}
        # ATX_DE2021_BoS_... / ATX_DE2023_BirthdayEvent_... — limited-time
        # seasonal-event challenges. The challenge FULL alone reads as if the
        # icon is still earnable today, so name the event and the year.
        de = re.match(r"ATX_DE(20\d\d)_([A-Za-z0-9]+)_", chal_edid)
        if de:
            year, event = de.group(1), prettify(de.group(2))
            if not event.lower().endswith("event"):
                event += " Event"
            text = f"Rewarded during the {year} {event}"
            text += f" for completing \"{name}\"." if name else "."
            text += " Only obtainable while that event is running."
            return {"method": "seasonal-event", "source": "Seasonal Event",
                    "text": text, "season": None, "seasonName": f"{year} {event}",
                    "rank": None, "page": None, "premium": False}
        return {"method": "challenge", "source": "Challenge",
                "text": f"Complete the challenge \"{name}\"." if name
                        else "Awarded by completing a challenge.",
                "season": None, "seasonName": "", "rank": None, "page": None,
                "premium": False}

    # 7. World Pets — earned by levelling a pet. These arrived on PTS with a
    #    WorldPets_Reward_PetLevelling_* GMRW rather than a ChallengeReward_,
    #    so they fall past the challenge branch above.
    wp = re.match(r"WorldPets_ENTM_PlayerIcon_PetIcon_([A-Za-z]+?)(\d*)$", edid)
    if wp or edid.startswith("WorldPets_"):
        pet = prettify(wp.group(1)) if wp else ""
        text = (f"Unlocked by levelling up your {pet} pet." if pet
                else "Unlocked by levelling up a pet.")
        return {"method": "world-pets", "source": "World Pets", "text": text,
                "season": None, "seasonName": "", "rank": None, "page": None,
                "premium": False}

    # 8. Community events / promotions.
    if re.search(r"Community\s?20\d\d|_PCA_", edid) or "community" in desc.lower():
        return {"method": "community", "source": "Community",
                "text": "Awarded through a Bethesda community event or promotion.",
                "season": None, "seasonName": "", "rank": None, "page": None,
                "premium": False}

    # 8. Vault raid icons (Vault 94 / 96) — the raids were removed in 2020.
    if edid.startswith("Vaults_"):
        return {"method": "legacy", "source": "Legacy",
                "text": "Earned from the Vault 94 / Vault 96 raids, which were removed "
                        "from the game - no longer obtainable.",
                "season": None, "seasonName": "", "rank": None, "page": None,
                "premium": False}

    # 9. Everything else in the ATX_ space is Atom Shop stock.
    if edid.startswith("ATX_"):
        return {"method": "atom-shop", "source": "Atom Shop",
                "text": "Purchased from the Atom Shop, on its own or as part of a bundle.",
                "season": None, "seasonName": "", "rank": None, "page": None,
                "premium": False}

    return {"method": "unknown", "source": "Unknown",
            "text": "Obtain route not recorded in the game files.",
            "season": None, "seasonName": "", "rank": None, "page": None,
            "premium": False}


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
def build() -> dict:
    entm_path = tsv_source.newest("ENTM_Export_*.tsv", channel=CHANNEL)
    print(f"{TAG} ENTM: {os.path.basename(entm_path)}")

    rows = read_tsv(entm_path, repair=True)
    season_rows, season_names = build_season_lookup()
    chal = build_challenge_lookup()

    icons: list[dict] = []
    dropped_cut = 0
    dropped_no_texture = 0

    for r in rows:
        edid = (r.get("EDID") or "").strip()
        keywords = r.get("KEYWORDS") or ""
        if ICON_CATEGORY_KYWD not in keywords and "playericon" not in edid.lower():
            continue
        if is_cut(edid):
            dropped_cut += 1
            continue

        filename = image_filename(r.get("ETDI") or "")
        if not filename:
            # No texture at all — these are the ..._Reuse0NN placeholder
            # shells named "Player Icon" with no art. Nothing to show.
            dropped_no_texture += 1
            continue

        full = (r.get("FULL") or "").strip()
        short = (r.get("NNAM") or "").strip()
        # FULL is the shop-facing name for nearly everything, but the World
        # Pets records put a dev label there ("Player Icon Cat Paw") and the
        # real name in NNAM ("Cat Paw Icon"). Prefer NNAM when FULL leads with
        # the category instead of naming the thing.
        if full.lower().startswith("player icon") and short:
            full = short
        name = full or short or prettify(edid.split("PlayerIcon_")[-1])
        # "Cappy Player Icon" / "Abduction Icon" -> "Cappy" / "Abduction".
        # The page is called Player Icons; repeating it in every row is noise,
        # and it wrecks the ABC sort's usefulness. FULL is inconsistent about
        # which suffix it uses, so strip either, plus a leading category label.
        display = re.sub(r"\s+(?:Player\s+)?Icon$", "", name, flags=re.I).strip()
        display = re.sub(r"^Player\s+Icon[:\s]+", "", display, flags=re.I).strip()
        display = display or name

        obtain = resolve_obtain(r, season_rows, season_names, chal)

        icons.append({
            "formId": (r.get("FormID") or "").strip().upper(),
            "edid": edid,
            "name": display,
            "fullName": name,
            "shortName": short,
            "desc": clean_desc(r.get("DESC")),
            "rarity": rarity_from_keywords(keywords),
            "premium": (r.get("XALG_Flags") or "").strip().lower() == "premium",
            "imageFilename": filename,
            "imageUrl": IMAGE_BASE + filename,
            "source": obtain["source"],
            "howToObtain": obtain,
            "avtr": next((p for p in (r.get("ReferencedBy") or "").split("|")
                          if ":AVTR" in p), ""),
            "isNew": False,
        })

    # ABC order, case-insensitive, on the displayed name.
    icons.sort(key=lambda i: (i["name"].lower(), i["edid"].lower()))

    # ---- First-seen persistence & isNew (31-day NEW pill) ----
    new_count = 0
    boot_note = ""
    if not PTS:
        fs_path = os.path.join(LIVE_TSV_DIR, _FIRST_SEEN_FILENAME)
        bootstrap = not os.path.exists(fs_path)
        first_seen = load_first_seen()
        update_first_seen(first_seen, [i["formId"] for i in icons], bootstrap)
        save_first_seen(first_seen)
        cutoff = compute_new_cutoff()
        for i in icons:
            i["isNew"] = first_seen.get(i["formId"], "2020-01-01") >= cutoff
            if i["isNew"]:
                new_count += 1
        boot_note = " [bootstrap: seeded existing as not-new]" if bootstrap else ""

    by_source: dict[str, int] = {}
    for i in icons:
        by_source[i["source"]] = by_source.get(i["source"], 0) + 1

    print(f"{TAG} icons: {len(icons)}  (dropped {dropped_cut} cut, "
          f"{dropped_no_texture} textureless)  NEW: {new_count}{boot_note}")
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
        "count": len(icons),
        "countsBySource": by_source,
        "icons": icons,
    }


def report_missing_images(payload: dict) -> int:
    """List icons whose AVIF is not in the local library. Local-only helper."""
    if not os.path.isdir(LOCAL_AVIF_DIR):
        print(f"{TAG} local AVIF library not found: {LOCAL_AVIF_DIR}")
        return 0
    have = {n.lower() for n in os.listdir(LOCAL_AVIF_DIR) if n.lower().endswith(".avif")}
    missing = [i for i in payload["icons"] if i["imageFilename"] not in have]
    print(f"{TAG} image coverage: {payload['count'] - len(missing)}/{payload['count']}")
    for i in missing:
        print(f"{TAG}   MISSING  {i['imageFilename']:52} {i['edid']}")
    unused = sorted(have - {i["imageFilename"] for i in payload["icons"]})
    print(f"{TAG} AVIFs with no live ENTM record: {len(unused)}")
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
