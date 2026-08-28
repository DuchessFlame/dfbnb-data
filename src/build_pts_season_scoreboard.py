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
  - isFirst is derived from the XALG_Flags column ("Premium" -> True).

When the season goes live and gets curated in season_rewards.tsv,
build_season_rewards.py takes over and this script's output is
superseded.

STATUS: active
INPUT:  ENTM export TSV  (tsv/ENTM_Export_*.tsv)
        fallout76_seasons.tsv  (tsv/fallout76_seasons.tsv)
        season_rewards.tsv  (tsv/season_rewards.tsv -- to detect curated seasons)
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
import tsv_source          # one resolver for every export selection

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist" / "calculators"
SEASONS_TSV = TSV_DIR / "fallout76_seasons.tsv"
REWARDS_TSV = TSV_DIR / "season_rewards.tsv"
TAG = "[build_pts_season_scoreboard]"

EDID_RE = re.compile(r"^(?:zzz_?|ZZZ_?)?SCORE_S(\d+)_ENTM_(.+)$", re.IGNORECASE)
DISABLED_RE = re.compile(r"^(?:zzz|ZZZ)_")
ITEMS_PER_PAGE = 8

# Entitlements that exist in ENTM but are NOT board rewards, so they must never be
# rendered as ones. Account_PremiumBattlePass is the real-money Season Pass that
# unlocks the board for players without Fallout 1st - it is a purchase, not a reward
# you claim off a page.
SKIP_EDID_SUBSTRINGS = (
    "Account_PremiumBattlePass",
)

CATEGORY_RULES = [
    ("Account_ScoreBoost",          "score_boost",     0),
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

UTILITY_IMAGES = {
    "Account_ScoreBoost_1": "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_1.webp",
    "Account_ScoreBoost_2": "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_2.webp",
    "Account_ScoreBoost_3": "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_2.webp",
}

NOTES = {
    "purpose": "PTS Season Scoreboard data for TheDuchessFlame.com. Auto-generated from PTS ENTM exports by build_pts_season_scoreboard.py. Used by df-bnb-seasons.js (Scoreboards module).",
    "pts_notice": "This is PTS datamined data. Page assignments are provisional (grouped by category). Ticket costs are set to 0 (unknown). Items may be added, removed, or rearranged before the season goes live. When the season is curated in season_rewards.tsv, build_season_rewards.py output supersedes this file.",
    "generated_by": "build_pts_season_scoreboard.py -- do not hand-edit.",
}


def read_tsv(path):
    for enc in ("utf-8-sig", "latin-1"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
                return list(reader)
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Cannot decode {path} with any known encoding")


def parse_date_dmy(s):
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


def find_entm_tsv(tsv_dir):
    hit = tsv_source.newest(str(Path(tsv_dir) / "ENTM_Export_*.tsv"), required=False)
    return Path(hit) if hit else None


def classify(edid_suffix):
    for prefix, cat, order in CATEGORY_RULES:
        if edid_suffix.startswith(prefix):
            return cat, order
    return "other", 99


def derive_kind_value_tally(edid_suffix, full_name, nnam):
    kind = value = tally = None
    if "PlayerIcon_" in edid_suffix:
        kind = "playerIcon"
        value = re.sub(r"\s*Player Icon\s*$", "", full_name).strip()
        tally = "player_icon"
    elif "PlayerTitles_Prefix_" in edid_suffix:
        kind = "playerTitlePrefix"
        # Extract from EDID — FULL/NNAM unreliable due to CSV quoting
        value = edid_suffix.split("PlayerTitles_Prefix_", 1)[1]
        tally = "camp_player_title"
    elif "PlayerTitles_Suffix_" in edid_suffix:
        kind = "playerTitleSuffix"
        value = edid_suffix.split("PlayerTitles_Suffix_", 1)[1]
        tally = "camp_player_title"
    elif "CAMPTitles_Prefix_" in edid_suffix:
        kind = "campTitlePrefix"
        value = edid_suffix.split("CAMPTitles_Prefix_", 1)[1]
        tally = "camp_player_title"
    elif "CAMPTitles_Suffix_" in edid_suffix:
        kind = "campTitleSuffix"
        value = edid_suffix.split("CAMPTitles_Suffix_", 1)[1]
        tally = "camp_player_title"
    elif "Account_ScoreBoost" in edid_suffix:
        tally = "score_booster"
    return kind, value, tally


def format_display_name(edid_suffix, full_name, kind, value):
    if kind == "playerIcon" and value:
        return "Player Icon: " + value
    if kind == "playerTitlePrefix" and value:
        return 'Player Title Prefix: "' + value + '"'
    if kind == "playerTitleSuffix" and value:
        return 'Player Title Suffix: "' + value + '"'
    if kind == "campTitlePrefix" and value:
        return 'C.A.M.P. Title Prefix: "' + value + '"'
    if kind == "campTitleSuffix" and value:
        return 'C.A.M.P. Title Suffix: "' + value + '"'
    return full_name


def make_item_id(season_num, page, edid_suffix):
    slug = re.sub(r"[^a-z0-9]+", "_", edid_suffix.lower()).strip("_")
    return "S" + str(season_num) + "_P" + str(page) + "_" + slug


def resolve_utility_image(edid_suffix):
    for key, url in UTILITY_IMAGES.items():
        if edid_suffix.startswith(key) or edid_suffix == key:
            return url
    return None


def load_season_metadata(path):
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


def curated_seasons(path):
    if not path.exists():
        return set()
    rows = read_tsv(path)
    return {safe_int(r.get("seasonNumber", ""), -1) for r in rows} - {-1}


def extract_season_entm(entm_tsv, target_season=None):
    rows = read_tsv(entm_tsv)
    seasons = defaultdict(list)

    for row in rows:
        edid = (row.get("EDID") or "").strip()
        m = EDID_RE.match(edid)
        if not m:
            continue
        if DISABLED_RE.match(edid):
            continue
        # Not a board reward — see SKIP_EDID_SUBSTRINGS.
        if any(s.lower() in edid.lower() for s in SKIP_EDID_SUBSTRINGS):
            continue

        snum = int(m.group(1))
        suffix = m.group(2)

        if target_season is not None and snum != target_season:
            continue

        full_name = (row.get("FULL") or "").strip()
        desc = (row.get("DESC") or "").strip()
        nnam = (row.get("NNAM") or "").strip()
        xalg = (row.get("XALG_Flags") or "").strip()
        form_id = (row.get("FormID") or "").strip()

        cat, sort_order = classify(suffix)
        kind, value, tally = derive_kind_value_tally(suffix, full_name, nnam)
        display_name = format_display_name(suffix, full_name, kind, value)
        is_first = xalg.lower() == "premium"
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


def assign_pages(items, items_per_page=ITEMS_PER_PAGE):
    items.sort(key=lambda x: (x["_sort_order"], x["_form_id"]))
    page = 1
    count = 0
    prev_cat = None

    for item in items:
        cat = item["_category"]
        if count >= items_per_page:
            page += 1
            count = 0
        if (prev_cat is not None and cat != prev_cat
                and count > 0 and count >= items_per_page // 2):
            page += 1
            count = 0
        item["_page"] = str(page)
        count += 1
        prev_cat = cat


def build_season_json(season_num, items, meta):
    sm = meta.get(season_num, {})
    output = {
        "_notes": NOTES,
        "seasonNumber": season_num,
        "seasonName": sm.get("seasonName") or ("Season " + str(season_num)),
    }
    urc = sm.get("unlockRequiredCount", 0)
    urr = sm.get("unlockRankRequired", 0)
    ult = sm.get("unlockLineText", "")
    if urc:
        output["unlockRequiredCount"] = urc
    if urr:
        output["unlockRankRequired"] = urr
    if ult:
        output["unlockLineText"] = ult

    json_items = []
    for item in items:
        page = item["_page"]
        ji = {
            "id": make_item_id(season_num, page, item["_suffix"]),
            "page": page,
            "name": item["name"],
            "cost": 0,
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


def main():
    parser = argparse.ArgumentParser(
        description="Generate PTS season scoreboard JSON from ENTM exports."
    )
    parser.add_argument("--season", type=int, default=None,
        help="Only build a specific season number (default: auto-discover all).")
    parser.add_argument("--force", action="store_true",
        help="Build even if the season already has curated data in season_rewards.tsv.")
    parser.add_argument("--entm-tsv", type=str, default=None,
        help="Path to the ENTM export TSV (default: auto-detect in tsv/).")
    parser.add_argument("--out-dir", type=str, default=None,
        help="Output directory (default: dist/calculators/).")
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DIST_DIR

    print(TAG + " Starting PTS season scoreboard build...")

    if args.entm_tsv:
        entm_tsv = Path(args.entm_tsv)
        if not entm_tsv.exists():
            raise SystemExit(TAG + " [ERROR] Specified ENTM TSV not found: " + str(entm_tsv))
    else:
        entm_tsv = find_entm_tsv(TSV_DIR)
        if entm_tsv is None:
            pts_dir = TSV_DIR / "pts"
            if pts_dir.exists():
                entm_tsv = find_entm_tsv(pts_dir)
        if entm_tsv is None:
            print(TAG + " [WARN] No ENTM_Export_*.tsv found -- nothing to do.")
            return
    print(TAG + " Using ENTM: " + entm_tsv.name)

    meta = load_season_metadata(SEASONS_TSV)
    print(TAG + " Loaded metadata for " + str(len(meta)) + " season(s)")

    curated = curated_seasons(REWARDS_TSV)
    if curated:
        clist = ", ".join("S" + str(n) for n in sorted(curated))
        print(TAG + " Curated seasons in season_rewards.tsv: " + clist)

    season_items = extract_season_entm(entm_tsv, target_season=args.season)
    if not season_items:
        print(TAG + " [WARN] No SCORE_S*_ENTM entries found -- nothing to do.")
        return

    found_list = ", ".join(
        "S" + str(n) + " (" + str(len(items)) + " items)"
        for n, items in sorted(season_items.items())
    )
    print(TAG + " Found ENTM entries for: " + found_list)

    out_dir.mkdir(parents=True, exist_ok=True)
    built = 0

    for snum in sorted(season_items):
        if snum in curated and not args.force:
            print(TAG + " S" + str(snum) + ": skipped (curated data exists; use --force to override)")
            continue

        items = season_items[snum]
        assign_pages(items)
        output = build_season_json(snum, items, meta)
        out_path = out_dir / ("season_tickets_s" + str(snum) + ".json")

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        page_counts = defaultdict(int)
        for it in items:
            page_counts[it["_page"]] += 1
        page_summary = ", ".join(
            "P" + str(p) + "=" + str(c)
            for p, c in sorted(page_counts.items(), key=lambda x: int(x[0]))
        )

        print(TAG + " S" + str(snum) + ": written " + out_path.name + "  ("
              + str(len(items)) + " items across " + str(len(page_counts))
              + " pages: " + page_summary + ")")
        built += 1

    if built == 0:
        print(TAG + " No seasons built (all are curated or no data found).")
    else:
        print(TAG + " Done -- built " + str(built) + " season(s).")


if __name__ == "__main__":
    main()
