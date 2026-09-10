#!/usr/bin/env python3
"""
apply_season_ranks.py
---------------------
Rebuilds the board-game seasons in tsv/season_rewards.tsv from the researched
rank-by-rank board list in tsv/season_ranks_s1_s8.tsv.

WHICH SEASONS IT TOUCHES
    Exactly the ones present in the ranks TSV - nothing is hardcoded. Add a
    season's board to that file and it is rebuilt on the next run; every other
    season in season_rewards.tsv is left untouched. The filename still says
    s1_s8 because that is what it started as, and renaming it would only orphan
    the old copy in the repo; it now holds S1-S15 and will hold the rest of the
    board-game seasons (up to S17) as they are researched.

WHY THIS EXISTS
    Seasons 1-17 were the old 100-rank board game. They have no "pages" - the
    in-game reward viewer lists them as RANK 1 .. RANK 100. Before this script
    those rows carried synthetic page numbers invented by
    build_pts_season_scoreboard.py, which grouped items by category ~8 per page.
    That is why Page 1 of Season 1 was nothing but player icons.

    It also means roughly half of each board was missing entirely: Atoms, Caps,
    Lunchboxes, Perk Card Packs, Repair Kits, Scrap Kits and the rest are
    scoreboard rewards that never appear in ENTM, so the datamined backfill
    could not see them.

WHAT IT DOES
    - Reads the curated rank list (source: fallout.wiki, cross-checked against
      fallout.fandom.com - see docs/season_ranks_sources.md).
    - Matches each rank entry to the existing curated row in the same season by
      name so the artwork, in-game description and storefront entitlement
      survive.
    - Emits a new row for every reward the datamine never had, tagging the
      currency/consumable ones with a tallyCategory and the shared utility art.
    - Writes the `rank` column. `page` is left blank on a board season: the
      renderer switches to a flat rank list when a season has ranks.
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
    (r"^perk card pack$",             "perk_card_pack",       "score_item_perkcardpack.avif"),
    # The scrap kit's texture is named for what it does - scrap to stash - not for
    # what the scoreboard calls it. Same file for x3 / x6 / x15; the count-badged
    # variants (_6pack, _15pack) exist locally but are not uploaded.
    (r"^scrap kits?$",                "scrap_kit",            "atx_utility_repairkit_scraptostash.avif"),
    # NB: patterns are matched against norm(), which has already stripped
    # punctuation - so "Vault-Tec" is "vault tec" by the time we get here.
    #
    # The supply package art is named scrapball_<size> - one, two and three stars
    # on the Vault-Tec footlocker - so size has to be matched before the generic
    # pattern, and "Supply Crate (Level N)" is the same art as size N.
    (r"^small vault tec supply (package|crate)",  "supply_package", "score_utility_scrapball_small.avif"),
    (r"^medium vault tec supply (package|crate)", "supply_package", "score_utility_scrapball_medium.avif"),
    (r"^large vault tec supply (package|crate)",  "supply_package", "score_utility_scrapball_large.avif"),
    (r"vault tec supply crate .?level 1",         "supply_package", "score_utility_scrapball_small.avif"),
    (r"vault tec supply crate .?level 2",         "supply_package", "score_utility_scrapball_medium.avif"),
    (r"vault tec supply crate .?level 3",         "supply_package", "score_utility_scrapball_large.avif"),
    (r"vault tec supply (package|crate)",         "supply_package", ""),
    (r"^perfect bubblegum$",          "perfect_bubblegum",    "score_utility_bubblegumperfect.avif"),
    (r"^liquid courage$",             "liquid_courage",       "score_game_liquidcourage.avif"),
    # Only one fireworks texture was ever shipped; Crackle, Trail and the plain
    # S4 "Fireworks" all share it.
    (r"fireworks$",                   "fireworks",            "score_utility_fireworks_crackle.avif"),
    (r"^legendary core$",             "legendary_core",       "score_game_legendary_core.avif"),
    (r"^tadpole badge$",              "tadpole_badge",        "score_currency_tadpolebadge.avif"),
    # Shared scoreboard consumables that ENTM never carries, so every board
    # season creates them fresh. They all live in the utility folder; without
    # these rules each new season lands ~100 rows reading "No image".
    # No tallyCategory: the tally table's keys are a closed set in
    # build_season_rewards.py, and an unknown one renders as nothing.
    (r"^stamps$",                     "",                     "score_currency_stamps.avif"),
    # norm() has already turned "Scout's" into "scout s".
    (r"^scout s banner$",             "",                     "score_coen_utility_banner.avif"),
    (r"^re roller$",                  "",                     "score_utility_reroller.avif"),
    (r"^score booster$",              "",                     "score_utility_scorebooster.avif"),
    (r"^nuka cola 6 pack$",           "",                     "score_item_6pack_nuka-cola.avif"),
    (r"^nuka cola mix pack$",         "",                     "score_item_6pack_nukavariety.avif"),
    (r"^nuka cola twist 6 pack$",     "",                     "score_item_6pack_nuka-twist.avif"),
    (r"^mystery bobblehead box$",     "",                     "score_utility_mysterybobblehead.avif"),
    (r"^turbo fert fertilizer$",      "",                     "score_item_turbofertgrenade.avif"),
    (r"^perfectly preserved pie$",    "",                     "score_item_preservedpie.avif"),
    (r"^ghost boy$",                  "",                     "score_game_ghostboy.avif"),
    # No shared artwork exists for these yet - they get a category but no image.
    (r"^(health|rad) kit bundle$",    "",                     ""),
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
    # Without this the board's abbreviated "T-45 Paint" scores 0.85 against
    # "Mercenary Company Pip-Boy Paint" and takes it, which then leaves the
    # real Pip-Boy paint at rank 32 with no artwork.
    (9, "Mercenary Company T-45 Paint"):            "Mercenary Company T-45 Power Armor Paint",
    # The board drops the "and Foundations" the other two carpets keep.
    (9, "Yellow Moulded Carpet Floor"):             "Yellow Moulded Carpet Floor and Foundations",
    # --- Season 10: The City of Steel ---
    # The datamined row keeps Bethesda's typo, so similarity alone never
    # reaches it from the board's spelling.
    (10, "Fanatic Foreman Player Icon"):            "Player Icon: Fanatic Formean",
    (10, "Fanatic Player Icon"):                    "Player Icon: Fanatic",
    # --- Season 11: Nuka-World ---
    (11, "Nuka-Launcher"):                          "Nuka-Launcher Crafting",
    (11, "Herringbone Red Brick Floor"):            "Herringbone Red Brick Floor and Foundations",
    (11, "Abandoned Prison Car"):                   "Prison Car",
    (11, "Nuka-Quantum Collectron"):                "Nuka-Cola Quantum Collectron Station",
    # Three Mr. Fuzzy plushies in one season - Nuka-Cola, Nuka-Twist and
    # Nuka-Cola Quantum - so both of the board's abbreviated names are pinned
    # rather than left to scores that separate them by a hair.
    (11, "Nuka-Cola Mr. Fuzzy"):                    "Nuka-Cola Mr. Fuzzy Plushie",
    (11, "Nuka-Quantum Mr. Fuzzy Plushie"):         "Nuka-Cola Quantum Mr. Fuzzy Plushie",
    (11, "Framed Nuka-World Gameboard"):            "Nuka World On Tour Gameboard",
    # --- Season 12: Rip Daring and the Cryptid Hunt ---
    (12, "Smiling Man Bandana"):                    "Smile Bandana Mask",
    (12, "Rustic Bed with Furs"):                   "Rustic Fur Bed",
    (12, "On the Hunt Poster"):                     'Rip Daring "On The Hunt" Poster',
    (12, "'The Shot' Poster"):                      'Rip Daring "The Shot" Poster',
    (12, "Cryptid Teeth Trophy Flair"):             "Cryptid Teeth Backpack Flair",
    # --- Season 13: Shoot for the Stars ---
    # Without this the board's "(10mm Pistol)" scores 0.87 against
    # "Gilded Paint (.44 Pistol)" and takes it, which then leaves the real .44
    # paint at rank 46 with no artwork.
    (13, "Gilded Paint (10mm Pistol)"):             "Gilded Paint (10mm)",
    (13, "Racecar Driver Underarmor"):              "Racecar Driver Outfit",
    (13, "Luchador (Masked Wrestler) Underarmor"):  "Luchador Underarmor",
    (13, "The Bada-Boom"):                          "The Bada-Boom (Super Sledge)",
    # --- Season 14: Fight for Freedom ---
    # Four Season 13 rewards were held back and issued on the S14 board; their
    # curated rows are still filed under Season 13, so the cross-season pass
    # recovers them once the names line up.
    (14, "Flatwoods Monster Poster"):               "Flatwoods Poster",
    (14, "Revolution Painting"):                    "Fight For Freedom: Revolution Painting",
    (14, "Storming the Beach Painting"):            "Fight For Freedom: Storming the Beach Painting",
    (14, "Moon Mission Painting"):                  "Fight For Freedom: Moon Mission Painting",
    (14, "Alaska Liberation Painting"):             "Fight For Freedom: Alaska Liberation Painting",
    (14, "Assured Victory Painting"):               "Fight For Freedom: Assured Victory Painting",
    (14, "Wine Rack Display"):                      "Wine Rack",
    # --- Season 15: The Big Score ---
    # The board names the weapon family; the entitlement spells out every
    # weapon the paint covers.
    (15, "Poker Paint (Combat Rifle)"):             "Poker Paint (Combat Rifle + The Fixer)",
    (15, "Poker Paint (Plasma Gun)"):               "Poker Paint (Plasma Gun + Enclave Plasma Gun)",
    (15, "Poker Paint (Gatling Laser)"):            "Poker Paint (Gatling Laser + Ultracite Gatling Laser)",
    (15, "Company Tea Kettle"):                     "Company Tea Machine",
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

    # The board seasons are whichever ones the ranks TSV actually covers.
    # Nothing else in season_rewards.tsv is read, rewritten or reordered, so
    # researching one more legacy board is a data change, not a code change.
    SEASONS = sorted({int(e["season"]) for e in rank_rows if (e.get("season") or "").strip()})
    if not SEASONS:
        raise SystemExit(f"{TAG} [ERROR] no seasons found in {RANKS_TSV.name}")
    print(f"{TAG} rebuilding seasons: "
          + ", ".join(f"S{s}" for s in SEASONS))

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

    def best_match(targets: list[str], pool: list[dict],
                   season: int) -> tuple[dict | None, float]:
        """Best candidate row for a board entry, scored against every spelling.

        `targets` is the board name (or its alias) AND the display name this
        script would give the row - "Atoms" and "Atoms x 150". Both are needed
        or the script is not idempotent: on the first run it creates a row
        called "Atoms x 150", and on the second run the board's bare "Atoms"
        scores 0.63 against it, under the threshold, so it creates a SECOND
        one and orphans the first. That silently doubled every currency,
        lunchbox and repair-kit row on each re-run.
        """
        best, best_score = None, 0.0
        for cand in pool:
            try:
                cand_season = int(cand.get("seasonNumber", "0"))
            except ValueError:
                cand_season = 0
            if cand["id"] in matched_ids[cand_season]:
                continue
            name = cand.get("name", "")
            score = max(similarity(t, name) for t in targets)
            if score > best_score:
                best, best_score = cand, score
        return best, best_score

    # ---- pass 1: match each board entry inside its own season ----
    entries: list[dict] = []
    for entry in rank_rows:
        season = int(entry["season"])
        raw_name = entry["name"].strip()
        qty = (entry.get("qty") or "").strip()
        target = ALIASES.get((season, raw_name), raw_name)
        targets = [target]
        shown = display_name(raw_name, qty)
        if shown != target:
            targets.append(shown)

        best, score = best_match(targets, old_by_season[season], season)
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
            "targets": targets,
            "qty": qty,
            "is_first": (entry.get("isFirst") or "").strip().upper() == "TRUE",
            "row": placed,
            "score": score,
        })

    # ---- pass 2: sweep the other seasons for rows filed under the wrong one ----
    everything = [r for s in SEASONS for r in old_by_season[s]]
    for item in entries:
        if item["row"] is not None:
            continue
        best, score = best_match(item["targets"], everything, item["season"])
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
