#!/usr/bin/env python3
"""
apply_season_ranks.py
---------------------
Rebuilds the Season 1-8 rows of tsv/season_rewards.tsv from the researched
rank-by-rank board list in tsv/season_ranks_s1_s8.tsv.

WHY THIS EXISTS
    Seasons 1-8 were the old 100-rank board game. They have no "pages" - the
    in-game reward viewer lists them as RANK 1 .. RANK 100. Before this script
    the S1-S8 rows carried synthetic page numbers invented by
    build_pts_season_scoreboard.py, which grouped items by category ~8 per page.
    That is why Page 1 of Season 1 was nothing but player icons.

    It also means roughly half of each board was missing entirely: Atoms, Caps,
    Lunchboxes, Perk Card Packs, Repair Kits, Scrap Kits and the rest are
    scoreboard rewards that never appear in ENTM, so the datamined backfill
    could not see them.

WHAT IT DOES
    - Reads the curated rank list (source: fallout.wiki, cross-checked against
      fallout.fandom.com - see docs/season_ranks_sources.md).
    - Matches each rank entry to the existing curated S1-S8 row by name so the
      artwork, in-game description and storefront entitlement survive.
    - Emits a new row for every reward the datamine never had, tagging the
      currency/consumable ones with a tallyCategory and the shared utility art.
    - Writes the `rank` column. `page` is left blank for S1-S8: the renderer
      switches to a flat rank list when a season has ranks.
    - Any existing curated row that the rank list does not account for is KEPT,
      with a blank rank, and listed in the report so it can be checked by hand.

STATUS: active
INPUT:  tsv/season_ranks_s1_s8.tsv, tsv/season_rewards.tsv
OUTPUT: tsv/season_rewards.tsv (backed up first), dist/season_ranks_report.txt
USAGE:  python src/apply_season_ranks.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import sys
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist"

RANKS_TSV = TSV_DIR / "season_ranks_s1_s8.tsv"
REWARDS_TSV = TSV_DIR / "season_rewards.tsv"
REPORT_TXT = DIST_DIR / "season_ranks_report.txt"

SEASONS = range(1, 9)
TAG = "[apply_season_ranks]"

UTILITY = "/wp-content/uploads/season_images/utility/"

# Reward-name pattern -> (tallyCategory, utility image filename or "").
# Order matters: first match wins, so put the specific patterns first.
UTILITY_RULES: list[tuple[str, str, str]] = [
    (r"^atoms$",                      "atoms",                "score_currency_atoms.avif"),
    (r"^caps$",                       "caps",                 "score_currency_caps.avif"),
    (r"^gold bullion$",               "gold_bullion",         "score_currency_bullion.avif"),
    (r"^legendary scrip$",            "legendary_scrip",      "score_currency_scrip.avif"),
    (r"^perk coins?$",                "perk_coins",           "score_currency_perkcoin.avif"),
    (r"^lunchboxe?s?$",               "lunchbox",             "atx_store_lunchbox001.avif"),
    (r"^legendary module$",           "legendary_module",     "score_game_legendarymodule.avif"),
    (r"^basic repair kits?$",         "repair_kit",           "atx_utility_repairkit_basic.avif"),
    (r"^carry weight booster$",       "carry_weight_booster", "score_utility_carryweight.avif"),
    (r"^nuclear keycards?$",          "nuclear_keycard",      "score_utility_nuclearkeycard.avif"),
    (r"nukashine",                    "nukashine",            "score_item_nukashine_sugarfree.avif"),
    # No shared artwork exists for these yet - they get a category but no image.
    (r"^perk card pack$",             "perk_card_pack",       ""),
    (r"^scrap kits?$",                "scrap_kit",            ""),
    # NB: patterns are matched against norm(), which has already stripped
    # punctuation - so "Vault-Tec" is "vault tec" by the time we get here.
    (r"vault tec supply (package|crate)", "supply_package",   ""),
    (r"^perfect bubblegum$",          "perfect_bubblegum",    ""),
    (r"^liquid courage$",             "liquid_courage",       ""),
    (r"fireworks$",                   "fireworks",            ""),
    (r"^legendary core$",             "legendary_core",       ""),
    (r"^tadpole badge$",              "tadpole_badge",        ""),
]

# (season, name on the board) -> name in season_rewards.tsv.
#
# The wiki names a reward the way the scoreboard displayed it; the TSV rows came
# out of ENTM, which names it the way the entitlement is written. Where the two
# diverge further than the fuzzy matcher can safely bridge, the mapping is
# spelled out here so nothing is joined on a guess.
ALIASES: dict[tuple[int, str], str] = {
    (1, "Ammo Converter"):                          "AmmoPoints Ammo Converter",
    (1, "Jangles the Moon Monkey Stein"):           "Jangles Beer Stein",
    (1, "Captain Cosmos Dark Matter Power Armor"):  "Captain Cosmos Dark Matter",
    (3, "10mm Pistol Settler Paint"):               "Settler's Special Paint (10mm)",
    (3, "Sportsman Paint"):                         "Sportsman Paint (Pump Action Shotgun)",
    (3, "Gold Bot"):                                "Scavenging Station with Gold Scavenge Bot",
    (3, "Vertiguard Paint"):                        "Vertiguard Power Armor Paint",
    (4, "Icebreaker Skin"):                         "Icebreaker (Power Fist)",
    (4, "Beekeeper's Beehive"):                     "Beehive",
    (4, "Double Tap Player Icon"):                  "Player Icon: Doubletap",
    (5, "MIND Power Armor Paint"):                  "Enlightened M.I.N.D. Power Armor Paint",
    (6, "Mistress Sidekick Mask"):                  "Mistress of Mystery Sidekick Mask",
    (6, "Judgement of Set Paint"):                  "Judgement of Set Paint (The Fixer)",
    (6, "T51 Helmet Backpack Flair"):               "Power Armor Helmet Backpack Flair",
    (7, "Opus or Obra Player Icon"):                "Player Icon: Opus and Obra",
    (7, "Zorbo T-51b Power Armor Paint"):           "Zorbo Power Armor T-51 Paint",
    (7, "Floating Face Flagon"):                    "Floating Face Farrah's Flagon",
    (7, "Ally: Xerxo"):                             "Lite Ally: Xerxo",
    (8, "First Responders CAMP Kit"):               "Responders Kit",
}

# Same-season matching runs first. Anything still unplaced then gets one pass
# across the other seven seasons at a stricter threshold, which is what recovers
# the rows the datamined backfill filed under the wrong season - S8 holds
# Season 7's "Dr. Zorbo's Magic Pose" and Season 5's Chronotron backpack, S3
# holds Season 4's checkered vault floors.
MATCH_THRESHOLD = 0.82
CROSS_SEASON_THRESHOLD = 0.90

# Words that carry no identity and only distort the similarity score.
NOISE = {
    "the", "a", "an", "of", "and", "paint", "skin", "set",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def norm(name: str) -> str:
    """Aggressively normalise a reward name for comparison."""
    s = name.lower()
    s = s.replace("&", " and ")
    s = re.sub(r"c\.a\.m\.p\.", "camp", s)
    s = re.sub(r"m\.i\.n\.d\.", "mind", s)
    s = re.sub(r"k\.d\.", "kd", s)
    s = re.sub(r"f\.e\.t\.c\.h\.", "fetch", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def tokens(name: str) -> set[str]:
    return {t for t in norm(name).split() if t not in NOISE}


def similarity(a: str, b: str) -> float:
    """Blend sequence ratio with token overlap so word order doesn't matter."""
    na, nb = norm(a), norm(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    seq = SequenceMatcher(None, na, nb).ratio()
    ta, tb = tokens(a), tokens(b)
    jac = len(ta & tb) / len(ta | tb) if (ta | tb) else 0.0
    return max(seq, (seq + jac) / 2, jac * 0.95)


def snake(name: str) -> str:
    s = norm(name).replace(" ", "_")
    return re.sub(r"_+", "_", s).strip("_")


# The wiki pluralises some stacked rewards in the early seasons and not in the
# later ones ("Lunchboxes x 3" in S1, "Lunchbox x 3" from S3). A checklist that
# spells the same item two ways reads as two items, so settle on the singular.
SINGULAR = {
    "lunchboxes": "Lunchbox",
    "scrap kits": "Scrap Kit",
    "basic repair kits": "Basic Repair Kit",
    "nuclear keycards": "Nuclear Keycard",
    "perk coins": "Perk Coin",
}


def display_name(name: str, qty: str) -> str:
    """Match the existing house convention for stacked rewards: 'Caps x 2500'."""
    name = SINGULAR.get(norm(name), name)
    if not qty:
        return name
    return f"{name} x {qty}"


def utility_for(name: str) -> tuple[str, str]:
    for pattern, category, image in UTILITY_RULES:
        if re.search(pattern, norm(name)):
            return category, (UTILITY + image if image else "")
    return "", ""


def read_tsv(path: Path) -> tuple[list[str], list[dict]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader.fieldnames or []), list(reader)


def write_tsv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fields, delimiter="\t",
            lineterminator="\n", extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would change without writing the TSV")
    args = parser.parse_args()

    _, rank_rows = read_tsv(RANKS_TSV)
    fields, reward_rows = read_tsv(REWARDS_TSV)

    if "rank" not in fields:
        fields = fields + ["rank"]

    # Split the master into the seasons we are rebuilding and everything else.
    old_by_season: dict[int, list[dict]] = {s: [] for s in SEASONS}
    untouched: list[dict] = []
    for row in reward_rows:
        try:
            num = int(row.get("seasonNumber", "0"))
        except ValueError:
            num = 0
        if num in old_by_season:
            old_by_season[num].append(row)
        else:
            untouched.append(row)

    report: list[str] = []
    new_by_season: dict[int, list[dict]] = {s: [] for s in SEASONS}
    matched_ids: dict[int, set[str]] = {s: set() for s in SEASONS}

    def best_match(target: str, pool: list[dict], season: int) -> tuple[dict | None, float]:
        best, best_score = None, 0.0
        for cand in pool:
            try:
                cand_season = int(cand.get("seasonNumber", "0"))
            except ValueError:
                cand_season = 0
            if cand["id"] in matched_ids[cand_season]:
                continue
            score = similarity(target, cand.get("name", ""))
            if score > best_score:
                best, best_score = cand, score
        return best, best_score

    # ---- pass 1: match each board entry inside its own season ----
    entries: list[dict] = []
    for entry in rank_rows:
        season = int(entry["season"])
        raw_name = entry["name"].strip()
        target = ALIASES.get((season, raw_name), raw_name)

        best, score = best_match(target, old_by_season[season], season)
        placed = None
        if best is not None and score >= MATCH_THRESHOLD:
            matched_ids[season].add(best["id"])
            placed = best
            if score < 0.95:
                report.append(
                    f"  S{season} rank {entry['rank']:>3}  matched {score:.2f}  "
                    f"'{raw_name}' -> '{best.get('name')}'"
                )
        entries.append({
            "season": season,
            "rank": int(entry["rank"]),
            "raw_name": raw_name,
            "target": target,
            "qty": (entry.get("qty") or "").strip(),
            "is_first": (entry.get("isFirst") or "").strip().upper() == "TRUE",
            "row": placed,
            "score": score,
        })

    # ---- pass 2: sweep the other seasons for rows filed under the wrong one ----
    everything = [r for s in SEASONS for r in old_by_season[s]]
    for item in entries:
        if item["row"] is not None:
            continue
        best, score = best_match(item["target"], everything, item["season"])
        if best is None or score < CROSS_SEASON_THRESHOLD:
            continue
        best_season = int(best.get("seasonNumber", "0"))
        matched_ids[best_season].add(best["id"])
        item["row"] = best
        item["score"] = score
        report.append(
            f"  S{item['season']} rank {item['rank']:>3}  moved  {score:.2f}  "
            f"'{item['raw_name']}' <- Season {best_season} row '{best.get('name')}'"
        )

    # ---- build the rebuilt rows ----
    for item in entries:
        season, rank = item["season"], item["rank"]
        raw_name, qty = item["raw_name"], item["qty"]

        if item["row"] is not None:
            row = dict(item["row"])
        else:
            category, image = utility_for(raw_name)
            row = {
                "seasonNumber": str(season),
                "name": display_name(raw_name, qty),
                "cost": "0",
                "kind": "",
                "value": "",
                "tallyCategory": category,
                "imageUrl": image,
                "description": "",
                "storefrontEntitlement": "",
                "reappearances": "",
                "addedInRerun": "",
            }
            closest = f" (closest {item['score']:.2f})" if item["score"] >= 0.5 else ""
            report.append(
                f"  S{season} rank {rank:>3}  new{closest:<18} "
                f"'{display_name(raw_name, qty)}'"
                + ("" if image else "   [no artwork]")
            )

        row["seasonNumber"] = str(season)
        row["rank"] = str(rank)
        row["page"] = ""
        # isFirst is taken ONLY from the board list, never carried over.
        #
        # The datamined backfill had set it on roughly half of every season -
        # Season 1's Clean Sink, Chicken Coop and Planetarium Lamp all carried a
        # "1st" pill. Season 1 had no Fallout 1st scoreboard rewards at all;
        # those bonuses did not start until Season 3. The published board is the
        # authority on which rewards were Fallout 1st bonuses, so anything it
        # does not flag gets cleared.
        row["isFirst"] = "TRUE" if item["is_first"] else ""
        row["id"] = f"S{season}_R{rank}_{snake(raw_name)}"
        new_by_season[season].append(row)

    # Keep anything curated that the board list did not account for.
    orphan_lines: list[str] = []
    for season in SEASONS:
        for cand in old_by_season[season]:
            if cand["id"] in matched_ids[season]:
                continue
            leftover = dict(cand)
            leftover["rank"] = ""
            leftover["page"] = ""
            # Fallout 1st scoreboard bonuses did not exist before Season 3, so
            # any 1st flag the backfill left on a Season 1 or 2 row is wrong no
            # matter whether the board list placed the row.
            if season <= 2:
                leftover["isFirst"] = ""
            new_by_season[season].append(leftover)
            orphan_lines.append(f"  S{season}  '{cand.get('name')}'  (id {cand['id']})")

    # De-duplicate ids (same reward name at the same rank, e.g. a base reward and
    # its Fallout 1st twin) so nothing collides in the JSON.
    for season in SEASONS:
        seen: dict[str, int] = {}
        for row in new_by_season[season]:
            base = row["id"]
            if base in seen:
                seen[base] += 1
                row["id"] = f"{base}_{seen[base]}"
            else:
                seen[base] = 1

    def sort_key(row: dict) -> tuple[int, int, str]:
        r = row.get("rank", "")
        return (0, int(r), row.get("name", "")) if r else (1, 0, row.get("name", ""))

    rebuilt: list[dict] = []
    for season in SEASONS:
        rebuilt.extend(sorted(new_by_season[season], key=sort_key))

    final_rows = rebuilt + untouched

    # ---- report ----
    lines = [
        f"{TAG} run {datetime.now().isoformat(timespec='seconds')}",
        "",
        "Per-season row counts (was -> now):",
    ]
    for season in SEASONS:
        lines.append(
            f"  Season {season:>2}: {len(old_by_season[season]):>3} -> "
            f"{len(new_by_season[season]):>3}"
        )
    lines += ["", "Fuzzy matches and newly created rows:"] + (report or ["  (none)"])
    lines += [
        "",
        "Curated rows the board list did not place at a rank "
        "(kept, rank left blank - check these by hand):",
    ] + (orphan_lines or ["  (none)"])
    report_text = "\n".join(lines)

    DIST_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_TXT.write_text(report_text + "\n", encoding="utf-8")
    print(report_text)

    if args.dry_run:
        print(f"\n{TAG} dry run - {REWARDS_TSV.name} not written")
        return 0

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    shutil.copy2(REWARDS_TSV, REWARDS_TSV.with_suffix(f".tsv.bak-{stamp}"))
    write_tsv(REWARDS_TSV, fields, final_rows)
    print(f"\n{TAG} wrote {REWARDS_TSV} ({len(final_rows)} rows)")
    print(f"{TAG} report at {REPORT_TXT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
