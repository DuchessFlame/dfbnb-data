#!/usr/bin/env python3
"""
build_pts_season_scoreboard.py
------------------------------
Generative PTS season scoreboard builder.

Reads the ENTM export TSV and discovers all SCORE_S{N}_ENTM entries
for seasons that lack curated data in season_rewards.tsv, then produces
provisional season_tickets_s{N}.json files in the same format consumed
by df-bnb-seasons.js.

Page assignments and ticket costs are auto-generated:
  - Items are grouped by category and spread across pages of ~8 items.
  - Costs are set to 0 (unknown until the season goes live).
  - isFirst is derived from the XALG_Flags column ("Premium" → True).

When the season goes live and gets curated in season_rewards.tsv,
build_season_rewards.py takes over and this script's output is
superseded.

STATUS: active
INPUT:  ENTM export TSV  (tsv/ENTM_Export_*.tsv)
        fallout76_seasons.tsv  (tsv/fallout76_seasons.tsv)
        season_rewards.tsv  (tsv/season_rewards.tsv — to detect curated seasons)
OUTPUT: dist/calculators/season_tickets_s{N}.json  (PTS-only seasons)
USAGE:  python src/build_pts_season_scoreboard.py
        python src/build_pts_season_scoreboard.py --season 26
"""

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist" / "calculators"

SEASONS_TSV = TSV_DIR / "fallout76_seasons.tsv"
REWARDS_TSV = TSV_DIR / "season_rewards.tsv"

TAG = "[build_pts_season_scoreboard]"

# ---------------------------------------------------------------------------
# Regex / constants
# ---------------------------------------------------------------------------
# Extract season number and suffix from ENTM EDID
# Handles zzz_ / ZZZ_ disabled prefix
EDID_RE = re.compile(
    r"^(?:zzz_?|ZZZ_?)?SCORE_S(\d+)_ENTM_(.+)$", re.IGNORECASE
)

# Items starting with zzz/ZZZ are disabled in-game — skip them
DISABLED_RE = re.compile(r"^(?:zzz|ZZZ)_")

ITEMS_PER_PAGE = 8

# ---------------------------------------------------------------------------
# Category classification
# ---------------------------------------------------------------------------
# (edid_suffix_prefix, category_key, display_sort_order)
# Order matters — first match wins.
CATEGORY_RULES = [
    ("Account_ScoreBoost",          "score_boost",     0),
    ("Account_PremiumBattlePass",   "account",         1),
    ("PlayerIcon_",                 "player_icon",     2),
    ("PlayerTitles_Prefix_",        "player_title",    3),
    ("PlayerTitles_Suffix_",        "player_title",    3),
    ("CAMPTitles_Prefix_",          "camp_title",      4),
    ("CAMPTitles_Suffix_",          "camp_title",      4),
    ("Emotes_",                     "emote",           5),
    ("Apparel_",                    "apparel",         6),
    ("Skin_PowerArmor_Model_",      "pa_model",        7),
    ("Skin_PowerArmor_Paint_",      "pa_paint",        8),
    ("Skin_PowerArmor_",            "pa_paint",        8),
    ("Skin_WeaponSkin_",            "weapon_paint",    9),
    ("Weapons_",                    "weapon_mod",     10),
    ("CAMP_Kit_",                   "camp_kit",       11),
    ("CAMP_Structure_",             "camp_structure",  12),
    ("CAMP_Floor_",                 "camp_structure",  12),
    ("CAMP_Workbench_",             "camp_workbench",  13),
    ("CAMP_Machinery_",             "camp_workbench",  13),
    ("CAMP_Utility_",               "camp_utility",    14),
    ("CAMP_Collector_",             "camp_utility",    14),
    ("CAMP_Door_",                  "camp_door",       15),
    ("CAMP_Bed_",                   "camp_furniture",  16),
    ("CAMP_Furniture_",             "camp_furniture",  16),
    ("CAMP_FloorDecor_",            "camp_decor",      17),
    ("CAMP_WallDecor_",             "camp_walldecor",  18),
]

# Shared utility images (reused across every season)
UTILITY_IMAGES = {
    "Account_ScoreBoost_1": "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_1.webp",
    "Account_ScoreBoost_2": "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_2.webp",
    "Account_ScoreBoost_3": "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_2.webp",
}

# ---------------------------------------------------------------------------
# Static _notes block
# ---------------------------------------------------------------------------
NOTES = {
    "purpose": (
        "PTS Season Scoreboard data for TheDuchessFlame.com. "
        "Auto-generated from PTS ENTM exports by build_pts_season_scoreboard.py. "
        "Used by df-bnb-seasons.js (Scoreboards module)."
    ),
    "pts_notice": (
        "This is PTS datamined data. Page assignments are provisional "
        "(grouped by category). Ticket costs are set to 0 (unknown). "
        "Items may be added, removed, or rearranged before the season "
        "goes live. When the season is curated in season_rewards.tsv, "
        "build_season_rewards.py output supersedes this file."
    ),
    "generated_by": "build_pts_season_scoreboard.py — do not hand-edit.",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_tsv(path: Path) -> list[dict]:
    """Read a TSV and return list of dicts."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader)


def parse_date_dmy(s: str) -> str:
    """Convert DD/MM/YYYY → YYYY-MM-DD."""
    if not s:
        return ""
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return s


def safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def find_entm_tsv(tsv_dir: Path) -> Path | None:
    """Find the most recent ENTM export TSV (by filename sort)."""
    candidates = sorted(tsv_dir.glob("ENTM_Export_*.tsv"))
    return candidates[-1] if candidates else None


def classify(edid_suffix: str) -> tuple[str, int]:
    """Classify an EDID suffix into (category_key, sort_order)."""
    for prefix, cat, order in CATEGORY_RULES:
        if edid_suffix.startswith(prefix):
            return cat, order
    return "other", 99


def derive_kind_value_tally(
    edid_suffix: str, full_name: str, nnam: str
) -> tuple[str | None, str | None, str | None]:
    """Derive kind, value, and tallyCategory from EDID + names."""
    kind = value = tally = None

    if "PlayerIcon_" in edid_suffix:
        kind = "playerIcon"
        value = re.sub(r"\s*Player Icon\s*$", "", full_name).strip()
        tally = "player_icon"
    elif "PlayerTitles_Prefix_" in edid_suffix:
        kind = "playerTitlePrefix"
        value = (nnam or full_name).strip().strip('"')
        tally = "camp_player_title"
    elif "PlayerTitles_Suffix_" in edid_suffix:
        kind = "playerTitleSuffix"
        value = (nnam or full_name).strip().strip('"')
        tally = "camp_player_title"
    elif "CAMPTitles_Prefix_" in edid_suffix:
        kind = "campTitlePrefix"
        value = (nnam or full_name).strip().strip('"')
        tally = "camp_player_title"
    elif "CAMPTitles_Suffix_" in edid_suffix:
        kind = "campTitleSuffix"
        value = (nnam or full_name).strip().strip('"')
        tally = "camp_player_title"
    elif "Account_ScoreBoost" in edid_suffix:
        tally = "score_booster"

    return kind, value, tally


def format_display_name(
    edid_suffix: str, full_name: str, kind: str | None, value: str | None
) -> str:
    """Format the display name to match curated conventions."""
    if kind == "playerIcon" and value:
        return f"Player Icon: {value}"
    if kind == "playerTitlePrefix" and value:
        return f'Player Title Prefix: "{value}"'
    if kind == "playerTitleSuffix" and value:
        return f'Player Title Suffix: "{value}"'
    if kind == "campTitlePrefix" and value:
        return f'C.A.M.P. Title Prefix: "{value}"'
    if kind == "campTitleSuffix" and value:
        return f'C.A.M.P. Title Suffix: "{value}"'
    return full_name


def make_item_id(season_num: int, page: str, edid_suffix: str) -> str:
    """Generate a stable item ID from the EDID suffix."""
    slug = re.sub(r"[^a-z0-9]+", "_", edid_suffix.lower()).strip("_")
    return f"S{season_num}_P{page}_{slug}"


def resolve_utility_image(edid_suffix: str) -> str | None:
    """Return a shared utility image URL if the EDID suffix maps to one."""
    for key, url in UTILITY_IMAGES.items():
        if edid_suffix.startswith(key) or edid_suffix == key:
            return url
    return None


# ---------------------------------------------------------------------------
# Season metadata
# ---------------------------------------------------------------------------

def load_season_metadata(path: Path) -> dict[int, dict]:
    """Load fallout76_seasons.tsv → dict keyed by season number."""
    if not path.exists():
        return {}
    rows = read_tsv(path)
    meta = {}
    for row in rows:
        num = safe_int(row.get("SeasonNumber", ""), -1)
        if num < 1:
            continue
        meta[num] = {
            "seasonName": row.get("SeasonName", ""),
            "startDate": parse_date_dmy(row.get("StartDate", "")),
            "endDate": parse_date_dmy(row.get("EndDate", "")),
            "days": safe_int(row.get("Days", ""), 0),
            "unlockRequiredCount": safe_int(row.get("UnlockRequiredCount") or "", 0),
            "unlockRankRequired": safe_int(row.get("UnlockRankRequired") or "", 0),
            "unlockLineText": (row.get("UnlockLineText") or "").strip(),
        }
    return meta


def curated_seasons(path: Path) -> set[int]:
    """Return set of season numbers that already have curated data."""
    if not path.exists():
        return set()
    rows = read_tsv(path)
    return {safe_int(r.get("seasonNumber", ""), -1) for r in rows} - {-1}


# ---------------------------------------------------------------------------
# ENTM extraction
# ---------------------------------------------------------------------------

def extract_season_entm(
    entm_tsv: Path, target_season: int | None = None
) -> dict[int, list[dict]]:
    """
    Read the ENTM export TSV and extract all SCORE_S{N}_ENTM entries.

    Returns a dict mapping season number → list of parsed item dicts.
    Disabled entries (zzz/ZZZ prefix) are excluded.
    """
    rows = read_tsv(entm_tsv)
    seasons: dict[int, list[dict]] = defaultdict(list)

    for row in rows:
        edid = (row.get("EDID") or "").strip()
        m = EDID_RE.match(edid)
        if not m:
            continue

        # Skip disabled items
        if DISABLED_RE.match(edid):
            continue

        snum = int(m.group(1))
        suffix = m.group(2)

        # Filter to target season if specified
        if target_season is not None and snum != target_season:
            continue

        full_name = (row.get("FULL") or "").strip()
        desc = (row.get("DESC") or "").strip()
        nnam = (row.get("NNAM") or "").strip()
        xalg = (row.get("XALG_Flags") or "").strip()
        form_id = (row.get("FormID") or "").strip()

        # Classify
        cat, sort_order = classify(suffix)

        # Derive kind / value / tally
        kind, value, tally = derive_kind_value_tally(suffix, full_name, nnam)

        # Format display name
        display_name = format_display_name(suffix, full_name, kind, value)

        # Premium → isFirst
        is_first = xalg.lower() == "premium"

        # Utility image
        img_url = resolve_utility_image(suffix)

        seasons[snum].append({
            "_edid": edid,
            "_suffix": suffix,
            "_sort_order": sort_order,
            "_category": cat,
            "_form_id": form_id,
            "name": display_name,
            "description": desc,
            "is_first": is_first,
            "kind": kind,
            "value": value,
            "tally": tally,
            "image_url": img_url,
            "storefront": edid,
        })

    return dict(seasons)


# ---------------------------------------------------------------------------
# Page assignment
# ---------------------------------------------------------------------------

def assign_pages(items: list[dict], items_per_page: int = ITEMS_PER_PAGE) -> None:
    """
    Assign provisional page numbers to items.

    Items are sorted by category order then EDID, and distributed
    across pages of `items_per_page` items each.  Category boundaries
    are respected — a new category always starts on the current page
    (no mid-category page breaks) unless the page is already full.
    """
    # Sort by category order, then by FormID (preserves game-data order)
    items.sort(key=lambda x: (x["_sort_order"], x["_form_id"]))

    page = 1
    count = 0
    prev_cat = None

    for item in items:
        cat = item["_category"]

        # Start a new page if the current one is full
        if count >= items_per_page:
            page += 1
            count = 0

        # Optionally break to a new page on category change, but only
        # if we've already placed items on this page (don't leave
        # empty pages) and the category boundary is "major"
        if (
            prev_cat is not None
            and cat != prev_cat
            and count > 0
            and count >= items_per_page // 2  # only break if page is ≥ half full
        ):
            page += 1
            count = 0

        item["_page"] = str(page)
        count += 1
        prev_cat = cat


# ---------------------------------------------------------------------------
# JSON assembly
# ---------------------------------------------------------------------------

def build_season_json(
    season_num: int, items: list[dict], meta: dict[int, dict]
) -> dict:
    """Build the full season_tickets_s{N}.json structure."""
    sm = meta.get(season_num, {})

    output: dict = {
        "_notes": NOTES,
        "seasonNumber": season_num,
        "seasonName": sm.get("seasonName") or f"Season {season_num}",
    }

    # Unlock metadata
    urc = sm.get("unlockRequiredCount", 0)
    urr = sm.get("unlockRankRequired", 0)
    ult = sm.get("unlockLineText", "")
    if urc:
        output["unlockRequiredCount"] = urc
    if urr:
        output["unlockRankRequired"] = urr
    if ult:
        output["unlockLineText"] = ult

    # Build item list
    json_items = []
    for item in items:
        page = item["_page"]
        ji: dict = {
            "id": make_item_id(season_num, page, item["_suffix"]),
            "page": page,
            "name": item["name"],
            "cost": 0,  # Unknown for PTS
        }

        if item["is_first"]:
            ji["isFirst"] = True

        if item["kind"]:
            ji["kind"] = item["kind"]
        if item["value"]:
            ji["value"] = item["value"]

        ji["storefrontEntitlement"] = item["storefront"]

        if item["image_url"]:
            ji["imageUrl"] = item["image_url"]

        if item["description"]:
            ji["description"] = item["description"]

        if item["tally"]:
            ji["tallyCategory"] = item["tally"]

        json_items.append(ji)

    output["items"] = json_items
    return output


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate PTS season scoreboard JSON from ENTM exports."
    )
    parser.add_argument(
        "--season", type=int, default=None,
        help="Only build a specific season number (default: auto-discover all).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Build even if the season already has curated data in season_rewards.tsv.",
    )
    parser.add_argument(
        "--out-dir", type=str, default=None,
        help="Output directory (default: dist/calculators/).",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DIST_DIR

    print(f"{TAG} Starting PTS season scoreboard build...")

    # --- Find ENTM TSV ---
    entm_tsv = find_entm_tsv(TSV_DIR)
    if entm_tsv is None:
        print(f"{TAG} [WARN] No ENTM_Export_*.tsv found in {TSV_DIR} — nothing to do.")
        return
    print(f"{TAG} Using ENTM: {entm_tsv.name}")

    # --- Load season metadata ---
    meta = load_season_metadata(SEASONS_TSV)
    print(f"{TAG} Loaded metadata for {len(meta)} season(s)")

    # --- Detect curated seasons (to avoid overwriting) ---
    curated = curated_seasons(REWARDS_TSV)
    if curated:
        print(f"{TAG} Curated seasons in season_rewards.tsv: "
              f"{', '.join(f'S{n}' for n in sorted(curated))}")

    # --- Extract ENTM entries ---
    season_items = extract_season_entm(entm_tsv, target_season=args.season)
    if not season_items:
        print(f"{TAG} [WARN] No SCORE_S*_ENTM entries found — nothing to do.")
        return

    print(f"{TAG} Found ENTM entries for: "
          f"{', '.join(f'S{n} ({len(items)} items)' for n, items in sorted(season_items.items()))}")

    # --- Build per-season JSON ---
    out_dir.mkdir(parents=True, exist_ok=True)
    built = 0

    for snum in sorted(season_items):
        # Skip curated seasons unless --force
        if snum in curated and not args.force:
            print(f"{TAG} S{snum}: skipped (curated data exists; use --force to override)")
            continue

        items = season_items[snum]

        # Assign provisional pages
        assign_pages(items)

        # Build JSON
        output = build_season_json(snum, items, meta)
        out_path = out_dir / f"season_tickets_s{snum}.json"

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        page_counts = defaultdict(int)
        for it in items:
            page_counts[it["_page"]] += 1
        page_summary = ", ".join(
            f"P{p}={c}" for p, c in sorted(page_counts.items(), key=lambda x: int(x[0]))
        )

        print(f"{TAG} S{snum}: written {out_path.name}  "
              f"({len(items)} items across {len(page_counts)} pages: {page_summary})")
        built += 1

    if built == 0:
        print(f"{TAG} No seasons built (all are curated or no data found).")
    else:
        print(f"{TAG} Done — built {built} season(s).")


if __name__ == "__main__":
    main()
