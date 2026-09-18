#!/usr/bin/env python3
"""
apply_legacy_rerun_board.py
---------------------------
Writes the RE-RUN board of a legacy season into tsv/season_rewards.tsv: the
paged, ticket-priced scoreboard Bethesda puts an old season back on.

WHY THIS EXISTS, AND WHY IT IS NOT apply_season_ticket_costs.py
    That script transcribes the costs of a season's ONE board and sets `cost`
    and `page` on the reward row in place. That is right for S18+, where the
    board it describes is the only board that season ever had.

    A legacy re-run is a second board. Season 4 ran as the 100-rank board game
    in 2021 (Ranks 2-100, ~115 rewards, no tickets) and came back in Sep 2026 as
    16 ticket pages holding 66 rewards - a re-sorted subset, priced, with a
    handful of rewards that were never on the original board. Both boards are
    real and the page shows both: Page 1..N at the top, the untouched
    Rank 2..100 list inside "Original Run".

    So a re-run card cannot overwrite the rank row it came from - that would
    destroy the original run. It gets its OWN row, carrying `page` + `cost` and
    no `rank`. df-bnb-seasons.js already reads it that way: groupByRank() fills
    its page stack from rows that have a page and no rank, and puts the ranked
    rows in Original Run. No renderer change is needed when this lands.

    The re-run also repeats consumables - S.C.O.R.E. Booster x 3 sits on pages
    1, 8 and 12 at three different prices. Matching by name, as the cost script
    does, would collapse those into one row and keep only the last price. One
    row per CARD is the only shape that survives that.

WHAT IT DOES WITH EACH CARD
    matchName (or name) is looked up in the season's existing rewards:

      - hits a row WITH a rank   -> the original board's row. Copied to a new
                                    re-run row (art, description, kind/value,
                                    entitlement) + page + cost. The rank row is
                                    left exactly as it was.
      - hits a row with NO rank   -> a reward added for the re-run, already
                                    filed by apply_legacy_additions.py. Updated
                                    IN PLACE with page + cost; no duplicate, and
                                    its addedInRerun NEW pill is kept.
      - hits nothing              -> a slot ENTM never carried (Stamps, Re-Roller,
                                    S.C.O.R.E. Booster). Inserted, with the shared
                                    utility icon for its tallyCategory.
      - matchName that hits nothing is a TYPO, not a new slot, and is a hard
        error: a bad pin would otherwise insert a silent duplicate reward.

    isFirst comes from the screenshot, which outranks the curated row - the
    in-game card is the only place the Fallout 1st marker is visible.

IDEMPOTENT
    Generated re-run rows are the ones whose id starts with "S{N}_L". They are
    dropped and rebuilt on every run, so re-running after a fixed price is safe.

TSV FORMAT (tsv/season_legacy_rerun_costs.tsv)
    seasonNumber  page  slot  name  cost  tallyCategory  isFirst  matchName  notes

    page/slot  Reading order in game: page 1..N, slot 1..k left to right.
    name       The card's own (often truncated) wording. Used for matching and
               for the inserted rows; a matched row keeps its curated name, so
               the site says "Hunting Lever Action Rifle Paint", not "HUNTING PAINT".
    matchName  The exact name in season_rewards.tsv, when the card differs.
    cost       Integer. BLANK means not yet transcribed - the card is skipped
               rather than written as a free reward.

STATUS: active
INPUT:  tsv/season_legacy_rerun_costs.tsv, tsv/season_rewards.tsv
OUTPUT: tsv/season_rewards.tsv (rewritten in place, .bak kept)
USAGE:  python src/apply_legacy_rerun_board.py --dry-run
        python src/apply_legacy_rerun_board.py
        python src/apply_legacy_rerun_board.py --season 4
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
REWARDS_TSV = TSV_DIR / "season_rewards.tsv"
RERUN_TSV = TSV_DIR / "season_legacy_rerun_costs.tsv"
LEGACY_TSV = TSV_DIR / "legacy_seasons.tsv"

TAG = "[apply_legacy_rerun_board]"

# Rows this script generates. Distinct from the S{N}_P{page}_ ids the datamined
# backfill left behind on rows that carry no page at all.
ID_PREFIX = "S%s_L%s_"

UTILITY_ROOT = "/wp-content/uploads/season_images/utility/"

# Shared icons for the slots ENTM never carried. Same table as
# apply_season_ticket_costs.TALLY_IMAGE - these files are already on the server,
# so an inserted row renders art instead of "No image".
TALLY_IMAGE = {
    "atoms":                "score_currency_atoms.avif",
    "gold_bullion":         "score_currency_bullion.avif",
    "caps":                 "score_currency_caps.avif",
    "perk_coins":           "score_currency_perkcoin.avif",
    "legendary_scrip":      "score_currency_scrip.avif",
    "stamps":               "score_currency_stamps.avif",
    "legendary_module":     "score_game_legendarymodule.avif",
    "carry_weight_booster": "score_utility_carryweight.avif",
    "improved_bait":        "score_utility_improvedbait.avif",
    "superb_bait":          "score_utility_superbait.avif",
    "re_roller":            "score_utility_reroller.avif",
    "score_booster":        "score_utility_scorebooster.avif",
    "lunchbox":             "atx_store_lunchbox001.avif",
    "scouts_banner":        "score_coen_utility_banner.avif",
    "mystery_magazine":     "score_utility_magazinebookbox.avif",
    "mystery_bobblehead":   "score_utility_mysterybobblehead.avif",
    "repair_kit":           "atx_utility_repairkit_basic.avif",
    "nuclear_keycard":      "score_utility_nuclearkeycard.avif",
    "nukashine":            "score_item_nukashine_sugarfree.avif",
}

# Copied onto the re-run row. Everything else - rank, origPage, origPageRank,
# addedInRerun - describes the ORIGINAL run and must not follow the card here.
CARRY = ("name", "kind", "value", "tallyCategory", "imageUrl",
         "description", "storefrontEntitlement", "reappearances")


def norm(s: str) -> str:
    s = (s or "").strip().lower().replace("&", "and")
    s = re.sub(r"[\"“”'`’]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def snake(s: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", norm(s))).strip("_")


def read_tsv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def legacy_seasons() -> set[str]:
    """Seasons with a re-run announced in tsv/legacy_seasons.tsv. A card for a
    season that is not in there is almost always a typed season number."""
    out = set()
    for r in read_tsv(LEGACY_TSV):
        n = (r.get("SeasonNumber") or "").strip()
        if n.isdigit():
            out.add(n)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, help="Only apply this season's re-run board.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not RERUN_TSV.exists():
        sys.exit(f"{TAG} [ERROR] Missing {RERUN_TSV}. Transcribe the re-run "
                 f"screenshots into it first - see the docstring for the format.")

    cards = read_tsv(RERUN_TSV)
    rewards = read_tsv(REWARDS_TSV)
    if not rewards:
        sys.exit(f"{TAG} [ERROR] Missing or empty {REWARDS_TSV}")

    known_legacy = legacy_seasons()
    seasons = {(c.get("seasonNumber") or "").strip() for c in cards}
    seasons = {s for s in seasons if s.isdigit()}
    if args.season:
        seasons = {s for s in seasons if int(s) == args.season}
    for s in sorted(seasons):
        if s not in known_legacy:
            sys.exit(f"{TAG} [ERROR] S{s} has no row in {LEGACY_TSV.name}. Add the "
                     f"re-run there first, or fix the season number in the card file.")

    print(f"{TAG} Re-run cards: {len(cards)} | reward rows: {len(rewards)}")

    # Drop the rows a previous run generated for these seasons, so this is a
    # rebuild rather than an append.
    kept, dropped = [], 0
    for r in rewards:
        sn = (r.get("seasonNumber") or "").strip()
        rid = (r.get("id") or "")
        if sn in seasons and re.match(r"^S%s_L\d" % re.escape(sn), rid):
            dropped += 1
            continue
        kept.append(r)
    if dropped:
        print(f"{TAG} Dropped {dropped} re-run rows from a previous run")

    # Index by (season, normalised name). Ranked rows are the original board;
    # everything else is a re-run addition or an unplaced curated row.
    index: dict[tuple, list[dict]] = {}
    for r in kept:
        key = ((r.get("seasonNumber") or "").strip(), norm(r.get("name", "")))
        index.setdefault(key, []).append(r)

    inserts: list[dict] = []
    repositioned: list[dict] = []
    copied = updated = created = blank = 0
    first_changed: list[tuple] = []

    for c in cards:
        season = (c.get("seasonNumber") or "").strip()
        if season not in seasons:
            continue

        raw_cost = (c.get("cost") or "").strip()
        if not raw_cost:
            blank += 1
            continue
        if not raw_cost.isdigit():
            print(f"{TAG} [WARN] Non-numeric cost {raw_cost!r} for {c.get('name')!r} "
                  f"(S{season} p{c.get('page')}) - skipped")
            continue

        page = (c.get("page") or "").strip()
        slot = (c.get("slot") or "").strip() or "0"
        name = (c.get("name") or "").strip()
        pin = (c.get("matchName") or "").strip()
        tally = (c.get("tallyCategory") or "").strip()
        is_first = "TRUE" if (c.get("isFirst") or "").strip().upper() == "TRUE" else ""

        hits = index.get((season, norm(pin or name))) or []

        if pin and not hits:
            sys.exit(f"{TAG} [ERROR] S{season} p{page} matchName {pin!r} matches no "
                     f"reward (card {name!r}). Fix the pin - inserting it would "
                     f"create a duplicate reward.")
        if len(hits) > 1:
            sys.exit(f"{TAG} [ERROR] S{season} p{page} {(pin or name)!r} matches "
                     f"{len(hits)} rewards. Pin it with matchName.")

        src = hits[0] if hits else None

        # A reward with no rank was never on the original board - it is one the
        # re-run added. Price it where it already sits instead of cloning it.
        if src is not None and not (src.get("rank") or "").strip():
            if (src.get("isFirst") or "").strip().upper() != is_first.upper():
                first_changed.append((season, src.get("name"),
                                      src.get("isFirst") or "(blank)", is_first or "(blank)"))
            src["page"] = page
            src["cost"] = raw_cost
            src["isFirst"] = is_first
            # Move it in with the rest of the re-run board. Row order in the TSV
            # is the order the season JSON carries, and the ticket calculator
            # puts its "Page N" separator in wherever the page CHANGES rather
            # than sorting - so an addition left at its old position printed a
            # stray "Page 3 / Page 9 / Page 16" run ahead of Page 1.
            repositioned.append(src)
            inserts.append(src)
            updated += 1
            continue

        row = {k: "" for k in rewards[0]}
        row["seasonNumber"] = season
        row["page"] = page
        row["cost"] = raw_cost
        row["isFirst"] = is_first
        if src is not None:
            for k in CARRY:
                if src.get(k):
                    row[k] = src[k]
            if tally:
                row["tallyCategory"] = tally
            copied += 1
        else:
            row["name"] = name
            row["tallyCategory"] = tally
            if tally in TALLY_IMAGE:
                row["imageUrl"] = UTILITY_ROOT + TALLY_IMAGE[tally]
            else:
                print(f"{TAG} [WARN] S{season} p{page} {name!r} is new to the re-run "
                      f"and has no icon for tallyCategory {tally!r} - it will render "
                      f"'No image'")
            created += 1
        # Slot keeps two cards on one page apart when their names collide.
        row["id"] = (ID_PREFIX % (season, page)) + (snake(row["name"]) or ("slot" + slot))
        inserts.append(row)

    print(f"{TAG} Re-run rows copied from the original board: {copied}")
    print(f"{TAG} Re-run additions priced in place:           {updated}")
    print(f"{TAG} Slots ENTM never carried, inserted:         {created}")
    print(f"{TAG} Blank costs left untranscribed:             {blank}")

    ids = [r["id"] for r in inserts]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        sys.exit(f"{TAG} [ERROR] Duplicate generated ids: {dupes}")

    for s, nm, was, now in first_changed:
        print(f"{TAG} [1st] S{s} {nm!r}: isFirst {was} -> {now} (from the screenshot)")

    if args.dry_run:
        print(f"{TAG} Dry run - nothing written.")
        return
    if not (inserts or updated or dropped):
        print(f"{TAG} Nothing to write.")
        return

    moved = {id(r) for r in repositioned}
    merged = [r for r in kept if id(r) not in moved] + inserts
    merged.sort(key=lambda r: int(r.get("seasonNumber") or 0))

    bak = REWARDS_TSV.with_suffix(
        REWARDS_TSV.suffix + ".bak-" + datetime.now().strftime("%Y%m%d-%H%M%S"))
    shutil.copy2(REWARDS_TSV, bak)
    print(f"{TAG} Backup written: {bak.name}")

    # Keep every column the file already has, in its existing order. Rebuilding
    # a fixed list here is what once wiped `rank` off all fifteen board seasons.
    fields = list(rewards[0].keys())
    for r in merged:
        for k in r:
            if k and k not in fields:
                fields.append(k)
    with REWARDS_TSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for r in merged:
            w.writerow({c: r.get(c, "") for c in fields})

    print(f"{TAG} Written: {REWARDS_TSV.name} ({len(merged)} rows)")
    print(f"{TAG} Done. Now run: python src/build_season_rewards.py")


if __name__ == "__main__":
    main()
