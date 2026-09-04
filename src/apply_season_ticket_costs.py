#!/usr/bin/env python3
"""
apply_season_ticket_costs.py
----------------------------
Merges transcribed scoreboard ticket costs into the master tsv/season_rewards.tsv.

WHY THIS EXISTS
Ticket costs are not in the game files. ENTM records what an entitlement IS, not
what it costs on the board - so no amount of datamining recovers them. The only
authoritative source is the in-game scoreboard itself, captured in the page
screenshots under "Guides and Stuff\\Season Photos\\Season {N}\\".

Those screenshots also carry rewards that ENTM has no entitlement for at all -
the currency and consumable slots (Atoms, Caps, Perk Coins, Legendary Scrip,
Nuclear Keycard, bait, Lunchboxes). Seasons 1-23 are missing every one of those
rows because they were generated from ENTM only. This script adds them.

So costs and currency rows are transcribed by hand into tsv/season_ticket_costs.tsv
and merged here. Matching is by (seasonNumber, page, normalised name):

  - Row matches an existing reward  -> its cost is set.
  - Row matches nothing             -> it is INSERTED as a new reward, which is how
                                       the missing currency slots get filled in.

Seasons 1-17 were rank based and have no tickets; rows for those seasons are
rejected rather than silently written as cost 0.

TSV FORMAT (tsv/season_ticket_costs.tsv)
    seasonNumber  page  slot  name              cost  tallyCategory  isFirst  matchName  notes
    21            1     1     Superb Bait x 5   20    superb_bait
    21            1     6     200 Atoms         20    atoms          TRUE

  slot       Reading order on the page, 1-based. Only used to order inserted rows.
  cost       Integer ticket cost. Leave BLANK if not yet transcribed - a blank is
             recorded as unknown and never written as 0.
  matchName  Optional. The exact reward name as it appears in season_rewards.tsv,
             used to pin this card when the two disagree. Reward cards are
             truncated to fit ("RUBY REEL ROD" for "Ruby Reel Fishing Rod Paint",
             "ISOTOPE ISAAC FRAM..." for "Isotype Isaac Framed Vinyl Cover" -
             note Bethesda's "Isotype" spelling) and drop the word "Backpack"
             from flair names. Rather than guess with a fuzzy matcher, pin it.
             When blank, `name` is used for matching.

STATUS: active
INPUT:  tsv/season_ticket_costs.tsv, tsv/season_rewards.tsv
OUTPUT: tsv/season_rewards.tsv  (rewritten in place, .bak kept)
USAGE:  python src/apply_season_ticket_costs.py --dry-run
        python src/apply_season_ticket_costs.py
        python src/apply_season_ticket_costs.py --season 21
"""

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
COSTS_TSV = TSV_DIR / "season_ticket_costs.tsv"

TAG = "[apply_season_ticket_costs]"

# Tickets were introduced with Season 18 (Milepost Zero). Everything before that
# was the rank-based board, where a reward has no ticket price at all.
FIRST_TICKET_SEASON = 18

COLUMNS = [
    "seasonNumber", "id", "page", "name", "cost", "isFirst", "kind", "value",
    "tallyCategory", "imageUrl", "description", "storefrontEntitlement",
    "reappearances", "addedInRerun",
]

UTILITY_ROOT = "/wp-content/uploads/season_images/utility/"

# Reused by inserted currency rows so they render with the shared icons that are
# already uploaded, rather than pointing at a per-season texture that will never exist.
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

_QTY_TAIL_RE = re.compile(r"\s*x\s*\d+\s*$", re.IGNORECASE)
_LEADING_NUM_RE = re.compile(r"^\d[\d,]*\s+")
_PREFIXES = (
    "Player Icon:", "CAMP Title Prefix:", "CAMP Title Suffix:",
    "Player Title Prefix/Suffix:", "Player Title Prefix:", "Player Title Suffix:",
)


def norm(s: str) -> str:
    s = (s or "").strip().lower().replace("&", "and")
    s = re.sub(r"[\"“”'`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def match_key(name: str) -> str:
    """Normalise a reward name so the screenshot spelling matches the ENTM spelling.

    Three differences have to be absorbed:
      - In-game cards shout and append quantities: "SUPERB BAIT X 5" vs "Superb Bait".
      - Quantities lead as well as trail: "200 ATOMS" vs "Atoms".
      - Icons and titles are written in the opposite order. The card says
        "FISHER VAULT BOY PLAYER ICON"; the ENTM data says
        "Player Icon: Fisher Vault Boy". Stripping the label from whichever
        end it appears on makes both sides land on "fisher vault boy".
    """
    s = (name or "").strip()
    for p in _PREFIXES:
        if s.startswith(p):
            s = s[len(p):]
            break
    s = _QTY_TAIL_RE.sub("", s.strip()).strip()
    s = _LEADING_NUM_RE.sub("", s).strip()
    s = norm(s)
    s = re.sub(
        r"\s+(player icon|camp title prefix|camp title suffix|"
        r"player title prefix|player title suffix)$", "", s)
    return s.strip()


def snake(s: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", norm(s))).strip("_")


def read_tsv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, help="Only apply this season's costs.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not COSTS_TSV.exists():
        sys.exit(f"{TAG} [ERROR] Missing {COSTS_TSV}. Transcribe the scoreboard "
                 f"screenshots into it first - see the docstring for the format.")

    costs = read_tsv(COSTS_TSV)
    rewards = read_tsv(REWARDS_TSV)
    if not rewards:
        sys.exit(f"{TAG} [ERROR] Missing or empty {REWARDS_TSV}")

    print(f"{TAG} Cost rows: {len(costs)} | reward rows: {len(rewards)}")

    # Index existing rewards by (season, normalised name) - deliberately NOT by page.
    #
    # The page numbers on seasons 1-23 are synthetic. build_pts_season_scoreboard.py
    # generated them by grouping items into categories and spreading them ~8 per page,
    # so they do not correspond to the real board. Season 21's "Hanging Buoy" sits on
    # page 14 in the data and page 2 in the actual game. The screenshot is the only
    # authoritative source, so a transcribed row corrects the page as well as the cost.
    index: dict[tuple, list[dict]] = {}
    for r in rewards:
        key = ((r.get("seasonNumber") or "").strip(), match_key(r.get("name", "")))
        index.setdefault(key, []).append(r)

    updated = inserted = skipped_blank = rejected = 0
    inserts: list[dict] = []
    repaged: list[tuple] = []

    for c in costs:
        season = (c.get("seasonNumber") or "").strip()
        if not season.isdigit():
            continue
        if args.season and int(season) != args.season:
            continue

        if int(season) < FIRST_TICKET_SEASON:
            print(f"{TAG} [REJECT] S{season} predates tickets (first is "
                  f"S{FIRST_TICKET_SEASON}): {c.get('name')!r}")
            rejected += 1
            continue

        raw_cost = (c.get("cost") or "").strip()
        if not raw_cost:
            # Not yet transcribed. Leave the reward untouched so an unknown cost
            # never masquerades as a free (0 ticket) reward on the page.
            skipped_blank += 1
            continue
        if not raw_cost.isdigit():
            print(f"{TAG} [WARN] Non-numeric cost {raw_cost!r} for {c.get('name')!r} "
                  f"(S{season} p{c.get('page')}) - skipped")
            continue

        page = (c.get("page") or "").strip()
        name = (c.get("name") or "").strip()
        # matchName pins a truncated/differently-spelled card to its exact reward.
        pin = (c.get("matchName") or "").strip()
        key = (season, match_key(pin or name))

        hits = index.get(key)
        if hits:
            if len(hits) > 1:
                print(f"{TAG} [WARN] S{season} {name!r} matches {len(hits)} rewards - "
                      f"all updated; disambiguate by renaming if that is wrong")
            for h in hits:
                h["cost"] = raw_cost
                if page:
                    if h.get("page") and h["page"] != page:
                        repaged.append((season, name, h["page"], page))
                    h["page"] = page
            updated += len(hits)
            continue

        if pin:
            # A pin that resolves to nothing is a typo in matchName. Inserting a
            # duplicate reward would quietly corrupt the season, so refuse.
            print(f"{TAG} [ERROR] S{season} matchName {pin!r} matches no reward "
                  f"(card {name!r}, page {page}) - fix the pin; row skipped")
            rejected += 1
            continue

        # No existing reward - this is one of the currency/consumable slots that
        # ENTM never carried. Insert it.
        tally = (c.get("tallyCategory") or "").strip()
        img = UTILITY_ROOT + TALLY_IMAGE[tally] if tally in TALLY_IMAGE else ""
        slot = (c.get("slot") or "").strip() or "0"
        inserts.append({
            "seasonNumber": season,
            "id": f"S{season}_P{page}_{snake(name)}" or f"S{season}_P{page}_slot{slot}",
            "page": page,
            "name": name,
            "cost": raw_cost,
            "isFirst": "TRUE" if (c.get("isFirst") or "").strip().upper() == "TRUE" else "",
            "kind": "",
            "value": "",
            "tallyCategory": tally,
            "imageUrl": img,
            "description": (c.get("notes") or "").strip(),
            "storefrontEntitlement": "",
            "reappearances": "",
        })
        inserted += 1

    print(f"{TAG} Costs applied to existing rewards: {updated}")
    print(f"{TAG} New reward rows to insert:        {inserted}")
    print(f"{TAG} Blank costs left as unknown:      {skipped_blank}")
    if repaged:
        print(f"{TAG} Synthetic page numbers corrected: {len(repaged)}")
        for s, n, was, now in repaged[:10]:
            print(f"{TAG}    S{s} {n!r}: page {was} -> {now}")
        if len(repaged) > 10:
            print(f"{TAG}    ... and {len(repaged) - 10} more")
    if rejected:
        print(f"{TAG} Rejected (pre-ticket season):     {rejected}")

    merged = rewards + inserts
    merged.sort(key=lambda r: int(r.get("seasonNumber") or 0))

    if args.dry_run:
        print(f"{TAG} Dry run - nothing written.")
        return

    if not (updated or inserted):
        print(f"{TAG} Nothing to write.")
        return

    bak = REWARDS_TSV.with_suffix(
        REWARDS_TSV.suffix + ".bak-" + datetime.now().strftime("%Y%m%d-%H%M%S"))
    shutil.copy2(REWARDS_TSV, bak)
    print(f"{TAG} Backup written: {bak.name}")

    with REWARDS_TSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for r in merged:
            w.writerow({c: r.get(c, "") for c in COLUMNS})

    print(f"{TAG} Written: {REWARDS_TSV.name} ({len(merged)} rows)")
    print(f"{TAG} Done. Now run: python src/build_season_rewards.py")


if __name__ == "__main__":
    main()
