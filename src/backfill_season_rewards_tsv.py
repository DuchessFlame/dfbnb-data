#!/usr/bin/env python3
"""
backfill_season_rewards_tsv.py
------------------------------
ONE-TIME MIGRATION.

Seasons 1-23 lived only as hand-built JSON in dist/calculators/season_tickets_s*.json.
Nothing regenerated them: tsv/season_rewards.tsv only held S24 and S25, so a run of
build_season_rewards.py would silently leave S1-23 as orphaned, unreproducible files.

This script reverses those 23 JSONs back into TSV rows and merges them with the
existing S24/S25 rows, producing a single master tsv/season_rewards.tsv that
build_season_rewards.py can regenerate every season from.

While reversing it also enriches two fields that were largely blank on S1-23:

  imageUrl      Derived from storefrontEntitlement using the project's existing
                convention (see entitlement_to_webp_name in build_season_ticket_images.py):
                    SCORE_S12_ENTM_CAMP_AidBox_Alien -> score_s12_camp_aidbox_alien.avif
                Validated against all 59 S24 rows that carry both fields - 59/59 match.
                Utility/currency items resolve to the shared /utility/ icons instead.
                df-bnb-seasons.js resolveImageUrl() rewrites these to
                /season_images/season-{N}/*.avif at render time, so the data does not
                need to change again when images land.

  tallyCategory Derived from `kind` (playerIcon / playerTitle* / campTitle*) and from
                a name map learned from the S24+S25 rows that already carry the field.

STATUS: one-time migration (safe to re-run; output is deterministic)
INPUT:  dist/calculators/season_tickets_s1..s23.json, tsv/season_rewards.tsv
OUTPUT: tsv/season_rewards.tsv  (rewritten, all seasons)
USAGE:  python src/backfill_season_rewards_tsv.py [--dry-run]

A timestamped .bak of the existing TSV is written before any overwrite.
"""

import argparse
import csv
import json
import re
import shutil
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_PATH = REPO_ROOT / "tsv" / "season_rewards.tsv"
DIST_DIR = REPO_ROOT / "dist" / "calculators"

TAG = "[backfill_season_rewards]"

# Seasons reversed out of dist/. S24+ already live in the TSV and are passed through
# untouched so this migration can never regress the hand-maintained newer seasons.
BACKFILL_SEASONS = range(1, 24)

# Column order of tsv/season_rewards.tsv. Must stay in sync with build_season_rewards.py
# build_item(), which reads these names off the row dict.
COLUMNS = [
    "seasonNumber", "id", "page", "name", "cost", "isFirst", "kind", "value",
    "tallyCategory", "imageUrl", "description", "storefrontEntitlement",
    "reappearances", "addedInRerun",
]

UTILITY_ROOT = "/wp-content/uploads/season_images/utility/"
SEASON_ROOT = "/wp-content/uploads/season_images/"


# ---------------------------------------------------------------------------
# Name normalisation (mirrors norm()/strip_prefixes()/strip_quantity() in
# build_season_ticket_images.py so both scripts agree on what an item "is")
# ---------------------------------------------------------------------------

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


def strip_prefixes(name: str) -> str:
    s = (name or "").strip()
    for p in _PREFIXES:
        if s.startswith(p):
            return s[len(p):].strip().strip('"').strip()
    return s.strip().strip('"').strip()


def strip_quantity(name: str) -> str:
    s = _QTY_TAIL_RE.sub("", (name or "").strip()).strip()
    return _LEADING_NUM_RE.sub("", s).strip()


def base_name(name: str) -> str:
    """'500 Atoms' / 'Re-Roller x5' / 'Player Icon: Radhog' -> comparable key."""
    return norm(strip_quantity(strip_prefixes(name)))


# ---------------------------------------------------------------------------
# Utility / currency items: shared season-agnostic icons + tally categories.
# Keys are base_name() output. Values are (tallyCategory, utility filename).
# ---------------------------------------------------------------------------

UTILITY_MAP = {
    "atoms":                    ("atoms",                 "score_currency_atoms.avif"),
    "atom":                     ("atoms",                 "score_currency_atoms.avif"),
    "bullion":                  ("gold_bullion",          "score_currency_bullion.avif"),
    "gold bullion":             ("gold_bullion",          "score_currency_bullion.avif"),
    "caps":                     ("caps",                  "score_currency_caps.avif"),
    "bottle caps":              ("caps",                  "score_currency_caps.avif"),
    "perk coin":                ("perk_coins",            "score_currency_perkcoin.avif"),
    "perk coins":               ("perk_coins",            "score_currency_perkcoin.avif"),
    "scrip":                    ("legendary_scrip",       "score_currency_scrip.avif"),
    "legendary scrip":          ("legendary_scrip",       "score_currency_scrip.avif"),
    "stamps":                   ("stamps",                "score_currency_stamps.avif"),
    "stamp":                    ("stamps",                "score_currency_stamps.avif"),
    "legendary module":         ("legendary_module",      "score_game_legendarymodule.avif"),
    "legendary modules":        ("legendary_module",      "score_game_legendarymodule.avif"),
    "carry weight booster":     ("carry_weight_booster",  "score_utility_carryweight.avif"),
    "carryweight booster":      ("carry_weight_booster",  "score_utility_carryweight.avif"),
    "improved bait":            ("improved_bait",         "score_utility_improvedbait.avif"),
    "superb bait":              ("superb_bait",           "score_utility_superbait.avif"),
    "re roller":                ("re_roller",             "score_utility_reroller.avif"),
    "reroller":                 ("re_roller",             "score_utility_reroller.avif"),
    "score booster":            ("score_booster",         "score_utility_scorebooster.avif"),
    "s c o r e booster":        ("score_booster",         "score_utility_scorebooster.avif"),
    "scorebooster":             ("score_booster",         "score_utility_scorebooster.avif"),
    "lunchbox":                 ("lunchbox",              "atx_store_lunchbox001.avif"),
    "lunch box":                ("lunchbox",              "atx_store_lunchbox001.avif"),
    "lunchboxes":               ("lunchbox",              "atx_store_lunchbox001.avif"),
    "banner":                   ("scouts_banner",         "score_coen_utility_banner.avif"),
    "scouts banner":            ("scouts_banner",         "score_coen_utility_banner.avif"),
    "scout s banner":           ("scouts_banner",         "score_coen_utility_banner.avif"),
    "magazine and book box":    ("mystery_magazine",      "score_utility_magazinebookbox.avif"),
    "magazine book box":        ("mystery_magazine",      "score_utility_magazinebookbox.avif"),
    "mystery magazine":         ("mystery_magazine",      "score_utility_magazinebookbox.avif"),
    "mystery magazine package": ("mystery_magazine",      "score_utility_magazinebookbox.avif"),
    "mystery bobblehead":       ("mystery_bobblehead",    "score_utility_mysterybobblehead.avif"),
    "mysterybobblehead":        ("mystery_bobblehead",    "score_utility_mysterybobblehead.avif"),
    "repair kit":               ("repair_kit",            "atx_utility_repairkit_basic.avif"),
    "basic repair kit":         ("repair_kit",            "atx_utility_repairkit_basic.avif"),
    "repair kits":              ("repair_kit",            "atx_utility_repairkit_basic.avif"),
    "nuclear keycard":          ("nuclear_keycard",       "score_utility_nuclearkeycard.avif"),
    "nukashine":                ("nukashine",             "score_item_nukashine_sugarfree.avif"),
    "sugar free nukashine":     ("nukashine",             "score_item_nukashine_sugarfree.avif"),
    "sugarfree nukashine":      ("nukashine",             "score_item_nukashine_sugarfree.avif"),
}

# `kind` -> tallyCategory. Covers the icon/title rows, which are the bulk of the
# taggable S1-23 items and never need a name lookup.
KIND_TALLY = {
    "playerIcon":              "player_icon",
    "campTitlePrefix":         "camp_player_title",
    "campTitleSuffix":         "camp_player_title",
    "playerTitlePrefix":       "camp_player_title",
    "playerTitleSuffix":       "camp_player_title",
    "playerTitlePrefixSuffix": "camp_player_title",
}

# S.C.O.R.E. boosters are named by percentage ("10% S.C.O.R.E. Boost") so they miss
# the plain-name map. They also have dedicated per-tier utility art.
_BOOST_RE = re.compile(r"s\s*c\s*o\s*r\s*e\s*boost", re.IGNORECASE)


def entitlement_to_webp(edid: str) -> str:
    """SCORE_S12_ENTM_CAMP_AidBox -> score_s12_camp_aidbox.avif

    Same rule as entitlement_to_webp_name() in build_season_ticket_images.py, plus
    a strip of Bethesda's "zzz" deprecation prefix so cut records still point at the
    texture name the extractor produces.
    """
    k = (edid or "").strip().lower()
    k = re.sub(r"^zzz+", "", k)
    k = k.replace("_entm_", "_")
    return f"{k}.avif" if k else ""


def name_from_entitlement(edid: str) -> str:
    """Fallback display name for rows whose FULL was blank in the ENTM export.

    Only the handful of "zzz" (cut/deprecated) records hit this path.
        zzzSCORE_S21_ENTM_CAMP_FloorDecor_FishingCutout -> "Fishing Cutout"
    """
    k = re.sub(r"^zzz+", "", (edid or "").strip(), flags=re.IGNORECASE)
    k = re.sub(r"^SCORE_S\d+_ENTM_", "", k, flags=re.IGNORECASE)
    # Drop the leading category segment (CAMP, Skin, Apparel, ...) - it is a folder
    # name, not part of what the item is called in game.
    parts = k.split("_")
    if len(parts) > 1:
        parts = parts[1:]
    # Split CamelCase into words, then drop repeated words so
    # "Skin_WeaponSkin_RailwayRifle_BlueRidge" reads "Weapon Skin Railway Rifle Blue Ridge".
    words: list[str] = []
    for seg in parts:
        for w in re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", seg).split():
            if not words or words[-1].lower() != w.lower():
                words.append(w)
    return " ".join(words).strip()


def resolve_utility(item: dict) -> tuple[str, str]:
    """Return (tallyCategory, imageUrl) for shared utility/currency rows, else ('','')."""
    raw = item.get("name") or ""
    key = base_name(raw)

    if key in UTILITY_MAP:
        tally, fname = UTILITY_MAP[key]
        return tally, UTILITY_ROOT + fname

    if _BOOST_RE.search(norm(raw)):
        # Tier is encoded in the entitlement (…_Account_ScoreBoost_1/_2/_3); the art
        # is shared across seasons, so point every season at the S24 files that exist.
        m = re.search(r"scoreboost_(\d)", (item.get("storefrontEntitlement") or "").lower())
        tier = m.group(1) if m else "1"
        return "score_booster", f"{UTILITY_ROOT}score_s24_account_scoreboost_{tier}.avif"

    return "", ""


def enrich(item: dict) -> dict:
    """Fill in tallyCategory and imageUrl without ever overwriting existing values."""
    out = dict(item)

    # A blank name means the ENTM record had no FULL - only the "zzz" cut records.
    # Recover something readable so the row is identifiable in the TSV.
    if not (out.get("name") or "").strip():
        recovered = name_from_entitlement(out.get("storefrontEntitlement", ""))
        if recovered:
            out["name"] = recovered

    tally, util_img = resolve_utility(item)

    if not out.get("tallyCategory"):
        if tally:
            out["tallyCategory"] = tally
        elif out.get("kind") in KIND_TALLY:
            out["tallyCategory"] = KIND_TALLY[out["kind"]]

    if not out.get("imageUrl"):
        if util_img:
            out["imageUrl"] = util_img
        else:
            fname = entitlement_to_webp(out.get("storefrontEntitlement", ""))
            if fname:
                out["imageUrl"] = SEASON_ROOT + fname

    return out


# ---------------------------------------------------------------------------
# JSON item -> TSV row
# ---------------------------------------------------------------------------

def item_to_row(season_num: int, item: dict) -> dict:
    item = enrich(item)
    reapp = item.get("reappearances") or []
    if isinstance(reapp, str):
        reapp = [reapp]
    return {
        "seasonNumber":          str(season_num),
        "id":                    item.get("id", ""),
        "page":                  str(item.get("page", "")),
        "name":                  item.get("name", ""),
        "cost":                  str(item.get("cost", 0) or 0),
        "isFirst":               "TRUE" if item.get("isFirst") else "",
        "kind":                  item.get("kind", ""),
        "value":                 item.get("value", ""),
        "tallyCategory":         item.get("tallyCategory", ""),
        "imageUrl":              item.get("imageUrl", ""),
        "description":           item.get("description", ""),
        "storefrontEntitlement": item.get("storefrontEntitlement", ""),
        "reappearances":         "|".join(reapp),
    }


def read_existing_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would change without writing the TSV.")
    args = ap.parse_args()

    print(f"{TAG} Starting backfill...")

    existing = read_existing_rows(TSV_PATH)
    print(f"{TAG} Existing TSV rows: {len(existing)}")

    # Keep every row whose season is NOT being backfilled, in original order.
    keep = [r for r in existing if (r.get("seasonNumber") or "").strip()
            not in {str(n) for n in BACKFILL_SEASONS}]
    dropped = len(existing) - len(keep)
    if dropped:
        print(f"{TAG} Replacing {dropped} existing row(s) that fall in S1-23")

    new_rows: list[dict] = []
    for n in BACKFILL_SEASONS:
        src = DIST_DIR / f"season_tickets_s{n}.json"
        if not src.exists():
            print(f"{TAG} [WARN] Missing {src.name} - skipping S{n}")
            continue
        data = json.loads(src.read_text(encoding="utf-8"))
        items = data.get("items") or []
        rows = [item_to_row(n, it) for it in items]
        blank = sum(1 for r in rows if not r["name"].strip())
        imgs = sum(1 for r in rows if r["imageUrl"])
        tallies = sum(1 for r in rows if r["tallyCategory"])
        print(f"{TAG} S{n:<2} {len(rows):>3} rows | imageUrl {imgs:>3} | "
              f"tallyCategory {tallies:>3}" + (f" | [WARN] {blank} blank name(s)" if blank else ""))
        new_rows.extend(rows)

    merged = new_rows + keep
    merged.sort(key=lambda r: int(r.get("seasonNumber") or 0))

    print(f"{TAG} Total rows after merge: {len(merged)}")

    if args.dry_run:
        print(f"{TAG} Dry run - nothing written.")
        return

    if TSV_PATH.exists():
        bak = TSV_PATH.with_suffix(
            TSV_PATH.suffix + ".bak-" + datetime.now().strftime("%Y%m%d-%H%M%S"))
        shutil.copy2(TSV_PATH, bak)
        print(f"{TAG} Backup written: {bak.name}")

    with TSV_PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for r in merged:
            w.writerow({c: r.get(c, "") for c in COLUMNS})

    print(f"{TAG} Written: {TSV_PATH} ({TSV_PATH.stat().st_size:,} bytes)")
    print(f"{TAG} Done. Now run: python src/build_season_rewards.py")


if __name__ == "__main__":
    main()
