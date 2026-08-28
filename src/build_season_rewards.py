#!/usr/bin/env python3
"""
build_season_rewards.py
-----------------------
Reads the master season_rewards.tsv and fallout76_seasons.tsv, then outputs:
  - dist/calculators/season_tickets_s{N}.json   (one per season in the TSV)
  - dist/calculators/all_seasons.json            (full season index)

STATUS: active
INPUT:  tsv/season_rewards.tsv, tsv/fallout76_seasons.tsv
OUTPUT: dist/calculators/season_tickets_s*.json, dist/calculators/all_seasons.json
USAGE:  python src/build_season_rewards.py
"""

import csv
import json
import re
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist" / "calculators"

REWARDS_TSV = TSV_DIR / "season_rewards.tsv"
SEASONS_TSV = TSV_DIR / "fallout76_seasons.tsv"

TAG = "[build_season_rewards]"

# ---------------------------------------------------------------------------
# Static _notes block embedded in every per-season JSON
# ---------------------------------------------------------------------------
NOTES = {
    "purpose": (
        "Season Ticket Calculator data for TheDuchessFlame.com. "
        "Used by df-bnb-df-calculator.js (mountSeasonTickets) "
        "and df-bnb-seasons.js (Scoreboards module)."
    ),
    "how_to_update_each_season": [
        "1. Add new item rows to tsv/season_rewards.tsv with the correct seasonNumber.",
        "2. If this is a brand-new season, add a row to tsv/fallout76_seasons.tsv with "
        "UnlockRequiredCount, UnlockRankRequired, and UnlockLineText filled in.",
        "3. Run: python src/build_season_rewards.py",
        "4. The script outputs per-season JSON and updates all_seasons.json automatically.",
        "5. IDs must follow the pattern: S{seasonNum}_P{pageNum}_{snake_case_name} for "
        "regular pages, S{seasonNum}_B{bonusPageNum}_{snake_case_name} for bonus pages.",
        "6. Pages are '1' through '10' for regular pages, 'B1' for Bonus Page 1, "
        "'B2' for Bonus Page 2.",
        "7. The tally table in the JS counts items from all regular pages and B1 only. "
        "B2 is intentionally EXCLUDED from the tally.",
        "8. Even though B2 items are excluded from the tally display, still add "
        "tallyCategory to B2 items for data completeness.",
    ],
    "item_schema": {
        "id": "STRING - Unique ID. Pattern: S{N}_P{page}_{snake_name} or S{N}_B{bonus}_{snake_name}",
        "page": "STRING - Page number: '1'-'10' for regular, 'B1'/'B2' for bonus. Blank on board-game seasons, which use rank instead.",
        "rank": "NUMBER (optional) - Board rank 1-100. Present only on the old board-game seasons (S1-S8), where the scoreboard had no pages. When a season has ranks the JSON carries layout:'rank' and df-bnb-seasons.js renders one flat ordered list instead of page groups.",
        "name": "STRING - Display name.",
        "cost": "NUMBER - Ticket cost (0 if free).",
        "isFirst": "BOOLEAN (optional) - True if Fallout First exclusive.",
        "tallyCategory": "STRING (optional) - Category key for the utility tally table.",
        "kind": "STRING (optional) - playerIcon, campTitlePrefix, campTitleSuffix, "
                "playerTitlePrefix, playerTitleSuffix, playerTitlePrefixSuffix.",
        "value": "STRING (optional) - The title/icon label value when kind is set.",
        "storefrontEntitlement": "STRING (optional) - Internal Bethesda entitlement code.",
        "reappearances": "STRING (optional, pipe-separated) - Sources where this item is now also obtainable (Atom Shop, Gold Bullion vendor, etc.). Populated by check_season_reward_reappearances.py; exported as a JSON array.",
        "addedInRerun": "STRING (optional) - The legacy re-release this reward was ADDED in, e.g. 'Aug 2026'. Bethesda adds rewards to legacy scoreboards on re-release; setting this emits isNew:true and renders a PERMANENT gold NEW pill on the card. It never expires - unlike the ~31-day date-driven NEW pill on checklist pages - because the point is to mark forever which rewards were not on the original board.",
        "isNew": "BOOLEAN (derived, do not put in the TSV) - Emitted automatically when addedInRerun is set. df-bnb-seasons.js keys the NEW pill off this.",
        "imageUrl": "STRING (optional) - URL to item image.",
        "description": "STRING (optional) - Item description.",
    },
    "tally_category_valid_values": {
        "atoms": "Atoms",
        "camp_player_title": "Camp & Player Titles",
        "caps": "Caps",
        "carry_weight_booster": "Carry Weight Booster",
        "fireworks": "Fireworks",
        "gold_bullion": "Gold Bullion",
        "improved_bait": "Improved Bait",
        "legendary_core": "Legendary Core",
        "legendary_module": "Legendary Module",
        "legendary_scrip": "Legendary Scrip",
        "liquid_courage": "Liquid Courage",
        "lunchbox": "Lunchbox",
        "mystery_bobblehead": "Mystery Bobblehead",
        "mystery_magazine": "Mystery Magazine",
        "nuclear_keycard": "Nuclear Keycard",
        "nukashine": "Nukashine",
        "perfect_bubblegum": "Perfect Bubblegum",
        "perk_card_pack": "Perk Card Pack",
        "perk_coins": "Perk Coins",
        "player_icon": "Player Icon",
        "re_roller": "Re-Roller",
        "repair_kit": "Repair Kits",
        "score_booster": "S.C.O.R.E. Boosters",
        "scouts_banner": "Scout's Banner",
        "scrap_kit": "Scrap Kit",
        "stamps": "Stamps",
        "superb_bait": "Superb Bait",
        "supply_package": "Vault-Tec Supply Package",
        "tadpole_badge": "Tadpole Badge",
    },
    "tally_behaviour": (
        "The tally table counts how many visible items (respecting the Fallout First "
        "toggle) have each tallyCategory, excluding Bonus Page 2 items. It counts "
        "occurrences (line items), NOT quantities. Counts update live as the user "
        "toggles 'Fallout First Member'."
    ),
    "js_file": "df-bnb-df-calculator.js — see mountSeasonTickets() and the TALLY_DISPLAY constant.",
    "css_file": "df-bnb-df-calculator.css — tally table uses dfcalcTallyTable and dfcalcTallyRow classes.",
    "generated_by": "build_season_rewards.py — do not hand-edit this file.",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_tsv(path: Path) -> list[dict]:
    """Read a TSV file and return a list of dicts (one per row)."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader)


def parse_date_dmy(date_str: str) -> str:
    """Convert DD/MM/YYYY to YYYY-MM-DD. Returns empty string on failure."""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str


def safe_int(val: str, default: int = 0) -> int:
    """Parse a string to int, returning default on failure."""
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def to_bool(val: str) -> bool:
    """Parse a TSV boolean string to Python bool."""
    return val.strip().upper() == "TRUE"


def build_item(row: dict) -> dict:
    """Convert a TSV row dict into the JSON item schema expected by the JS modules."""
    item = {
        "id": row.get("id", ""),
        "page": row.get("page", ""),
        "name": row.get("name", ""),
        "cost": safe_int(row.get("cost", "0")),
    }

    # rank — Seasons 1-17 were the 100-rank board game, not a paged scoreboard.
    # A season whose rows carry ranks is rendered as one flat RANK 1..100 list
    # and its `page` column is blank; see build_season() for the layout flag.
    rank = (row.get("rank") or "").strip()
    if rank:
        item["rank"] = safe_int(rank)

    # isFirst: only include if True (matches original hand-built convention)
    if to_bool(row.get("isFirst", "")):
        item["isFirst"] = True

    # Optional string fields — only include if non-empty
    for field in ("kind", "value", "storefrontEntitlement", "imageUrl", "description"):
        val = row.get(field, "").strip()
        if val:
            item[field] = val

    # addedInRerun — set when Bethesda adds a reward to a season that has already
    # run, which happens every time a legacy scoreboard is re-released. The value is
    # the re-release it arrived in ("Aug 2026").
    #
    # This drives a PERMANENT ★ NEW pill, deliberately unlike the ~31-day date
    # trigger used on the checklist pages: the point is to mark, forever, which
    # rewards were not on the original board. A returning player comparing their
    # collection to the page needs that distinction long after the re-run ends.
    rerun = row.get("addedInRerun", "").strip()
    if rerun:
        item["addedInRerun"] = rerun
        item["isNew"] = True

    # tallyCategory — only include if non-empty
    tally = row.get("tallyCategory", "").strip()
    if tally:
        item["tallyCategory"] = tally

    # reappearances — populated by check_season_reward_reappearances.py. Stored
    # in the TSV as a pipe-separated string ("Atom Shop|Gold Bullion vendor")
    # and unpacked here into a JSON array so df-bnb-seasons.js can iterate
    # without re-parsing.
    reapp = row.get("reappearances", "").strip()
    if reapp:
        parts = [p.strip() for p in reapp.split("|") if p.strip()]
        if parts:
            item["reappearances"] = parts

    return item


# ---------------------------------------------------------------------------
# Season metadata from fallout76_seasons.tsv
# ---------------------------------------------------------------------------

def load_season_metadata(path: Path) -> dict:
    """
    Load fallout76_seasons.tsv and return a dict keyed by season number (int).
    Each value contains name, dates, unlock metadata, etc.
    """
    rows = read_tsv(path)
    meta = {}
    for row in rows:
        num = safe_int(row.get("SeasonNumber", ""), -1)
        if num < 1:
            continue
        meta[num] = {
            "seasonKey": row.get("SeasonKey", ""),
            "seasonName": row.get("SeasonName", ""),
            "startDate": parse_date_dmy(row.get("StartDate", "")),
            "endDate": parse_date_dmy(row.get("EndDate", "")),
            "days": safe_int(row.get("Days", ""), 0),
            "unlockRequiredCount": safe_int(row.get("UnlockRequiredCount") or "", 0),
            "unlockRankRequired": safe_int(row.get("UnlockRankRequired") or "", 0),
            "unlockLineText": (row.get("UnlockLineText") or "").strip(),
        }
    return meta


# ---------------------------------------------------------------------------
# Build per-season JSON
# ---------------------------------------------------------------------------

def build_season_json(season_num: int, items: list[dict], meta: dict) -> dict:
    """
    Build the full JSON structure for one season, matching the schema
    consumed by df-bnb-seasons.js and df-bnb-calculators.js.
    """
    sm = meta.get(season_num, {})

    output = {
        "_notes": NOTES,
        "seasonNumber": season_num,
        "seasonName": sm.get("seasonName", f"Season {season_num}"),
    }

    # Unlock metadata — only include if present (older seasons may not have it)
    urc = sm.get("unlockRequiredCount", 0)
    urr = sm.get("unlockRankRequired", 0)
    ult = sm.get("unlockLineText", "")
    if urc:
        output["unlockRequiredCount"] = urc
    if urr:
        output["unlockRankRequired"] = urr
    if ult:
        output["unlockLineText"] = ult

    output["items"] = [build_item(row) for row in items]

    # layout — how df-bnb-seasons.js should group this season.
    #
    #   "rank"  the old board game: one flat, ordered RANK 1..100 list, no page
    #           headers. Used by every season whose rows carry a rank.
    #   "pages" the modern scoreboard: Page 1..10 plus Bonus Pages.
    #
    # Grouping a board season by page was the bug this replaced - the page
    # numbers on S1-S8 were invented by the datamined backfill, which bucketed
    # rewards by category, so "Page 1" of Season 1 was nothing but player icons.
    ranked = [it for it in output["items"] if "rank" in it]
    output["layout"] = "rank" if ranked else "pages"
    if ranked:
        output["maxRank"] = max(it["rank"] for it in ranked)

        # Mark the leftovers explicitly rather than leaving them defined by
        # absence. A row with no page, no rank and no flag is indistinguishable
        # from a broken one, so the CI sanity check cannot tell "this reward was
        # never on the board" from "this reward lost its place in a bad build".
        # These are kept so no curated artwork or description is lost, and are
        # rendered in their own section rather than mixed into the rank order.
        unplaced = 0
        for it in output["items"]:
            if "rank" in it or it.get("addedInRerun"):
                continue
            it["unplaced"] = True
            unplaced += 1
        output["unplacedCount"] = unplaced

    return output


# ---------------------------------------------------------------------------
# Build all_seasons.json
# ---------------------------------------------------------------------------

def load_legacy_runs(path: Path, meta: dict) -> list[dict]:
    """Load tsv/legacy_seasons.tsv — the re-runs of old seasons.

    Bethesda re-releases a past season alongside the current one, and does NOT
    announce the next one in advance, so this file is hand-maintained: add a row when
    a legacy season is announced, and nothing more. An empty file is the normal state
    between runs and simply means the hub shows no legacy season.

    EndDate is optional. Legacy runs track the concurrent season's window, so when it
    is blank the end date of whichever season is live at StartDate is used rather than
    a guessed date. `endDateEstimated` records which of the two happened, so the page
    can caveat honestly.
    """
    if not path.exists():
        return []

    runs = []
    for row in read_tsv(path):
        num = safe_int(row.get("SeasonNumber", ""), -1)
        if num < 1:
            continue
        start = parse_date_dmy(row.get("StartDate", ""))
        end = parse_date_dmy(row.get("EndDate", ""))
        estimated = False
        if not end and start:
            # Borrow the end date of the season running at StartDate.
            #
            # Seasons share their changeover day - S25 ends and S26 starts on
            # 2026-09-15 - so a plain "start <= date <= end" test matches the
            # OUTGOING season and borrows a window that closes the same day the
            # legacy run opens. A legacy season launches with the incoming season, so
            # prefer the one whose startDate is the legacy start, and otherwise take
            # the latest-ending match.
            exact = [m for m in meta.values() if m.get("startDate") == start
                     and m.get("endDate")]
            spans = [m for m in meta.values()
                     if m.get("startDate") and m.get("endDate")
                     and m["startDate"] <= start <= m["endDate"]]
            pick = exact or spans
            if pick:
                end = max(m["endDate"] for m in pick)
                estimated = True
        sm = meta.get(num, {})
        entry = {"number": num, "name": sm.get("seasonName", f"Season {num}")}
        if start:
            entry["startDate"] = start
        if end:
            entry["endDate"] = end
        entry["endDateEstimated"] = estimated
        note = (row.get("Note") or "").strip()
        if note:
            entry["note"] = note
        runs.append(entry)

    runs.sort(key=lambda r: r.get("startDate") or "")
    return runs


def build_all_seasons(meta: dict, legacy: list[dict]) -> dict:
    """Build the all_seasons.json index from season metadata."""
    seasons = []
    for num in sorted(meta.keys()):
        sm = meta[num]
        entry = {"number": num}
        if sm.get("seasonName"):
            entry["name"] = sm["seasonName"]
        if sm.get("startDate"):
            entry["startDate"] = sm["startDate"]
        if sm.get("endDate"):
            entry["endDate"] = sm["endDate"]
        if sm.get("days"):
            entry["days"] = sm["days"]
        seasons.append(entry)

    return {
        "generatedAt": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "kind": "all_seasons",
        "seasons": seasons,
        # Legacy re-runs ride along in the same file so the hub's countdown card
        # needs one fetch, not two.
        "legacy": legacy,
    }


# ---------------------------------------------------------------------------
# Reappearances cross-reference (aspirational)
# ---------------------------------------------------------------------------

def cross_reference_reappearances(rewards: list[dict]) -> None:
    """
    Cross-referencing is handled by the standalone script
    src/check_season_reward_reappearances.py — run that ahead of this build
    to refresh the 'reappearances' column in season_rewards.tsv. This
    function is kept as a documentation anchor; the actual values are
    already in `rewards` by the time this build runs.
    """
    pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"{TAG} Starting build...")

    # --- Validate inputs ---
    if not REWARDS_TSV.exists():
        raise SystemExit(f"{TAG} [ERROR] Missing: {REWARDS_TSV}")
    if not SEASONS_TSV.exists():
        raise SystemExit(f"{TAG} [ERROR] Missing: {SEASONS_TSV}")

    # --- Load data ---
    rewards = read_tsv(REWARDS_TSV)
    print(f"{TAG} Loaded {len(rewards)} reward rows from {REWARDS_TSV.name}")

    meta = load_season_metadata(SEASONS_TSV)
    print(f"{TAG} Loaded metadata for {len(meta)} seasons from {SEASONS_TSV.name}")

    # --- Cross-reference reappearances (no-op until data available) ---
    cross_reference_reappearances(rewards)

    # --- Group items by season ---
    seasons: dict[int, list[dict]] = {}
    for row in rewards:
        sn = safe_int(row.get("seasonNumber", ""), -1)
        if sn < 1:
            print(f"{TAG} [WARN] Skipping row with invalid seasonNumber: {row}")
            continue
        seasons.setdefault(sn, []).append(row)

    print(f"{TAG} Found items for {len(seasons)} season(s): "
          f"{', '.join(f'S{n}' for n in sorted(seasons))}")

    # --- Output per-season JSON ---
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    for sn in sorted(seasons):
        items = seasons[sn]
        output = build_season_json(sn, items, meta)
        out_path = DIST_DIR / f"season_tickets_s{sn}.json"

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        print(f"{TAG} Written: {out_path.name}  ({len(items)} items, "
              f"{out_path.stat().st_size:,} bytes)")

    # --- Output all_seasons.json ---
    legacy = load_legacy_runs(TSV_DIR / "legacy_seasons.tsv", meta)
    if legacy:
        print(f"{TAG} Legacy re-runs: "
              + ", ".join(f"S{r['number']} from {r.get('startDate','?')}" for r in legacy))
    else:
        print(f"{TAG} Legacy re-runs: none listed")

    all_seasons = build_all_seasons(meta, legacy)
    all_path = DIST_DIR / "all_seasons.json"

    with all_path.open("w", encoding="utf-8") as f:
        json.dump(all_seasons, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"{TAG} Written: {all_path.name}  ({len(all_seasons['seasons'])} seasons, "
          f"{all_path.stat().st_size:,} bytes)")

    print(f"{TAG} Done.")



if __name__ == "__main__":
    main()
