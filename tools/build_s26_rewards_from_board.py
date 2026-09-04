#!/usr/bin/env python3
"""
build_s26_rewards_from_board.py
-------------------------------
Turns the Season 26 board transcription into season_rewards.tsv rows.

WHY A MERGE
  Neither source is sufficient on its own:

    dist/pts/calculators/season_tickets_s26.json   (69 items)
      Canonical names, in-game descriptions, storefront entitlements, kind /
      value, image URLs - but its page numbers are PROVISIONAL (the PTS build
      buckets by category, not by board page) and every cost is 0 because the
      PTS export does not carry ticket prices.

    the board screenshots  (105 cards, transcribed to s26_board_transcript.psv)
      The only source for the things that matter to a player: which page a
      reward is on, what it actually costs in tickets, and whether it is behind
      Fallout 1st. But names are truncated on the small cards and there are no
      descriptions.

  So: page + cost + isFirst come from the board, everything else from the PTS
  export, joined on a normalised name. Currency and consumable rewards have no
  entitlement to join against, so they are matched to the shared utility icon
  and tally bucket by pattern instead - the same rows S25 uses.

OUTPUT
  A TSV fragment of season_rewards.tsv rows plus a review report naming every
  card that did not join. Nothing is written into season_rewards.tsv
  automatically - check the report, then append.

USAGE
  python tools/build_s26_rewards_from_board.py \
      --transcript s26_board_transcript.psv --out s26_rows.tsv
"""

import argparse
import csv
import json
import os
import re
import sys

SEASON = 26
PTS_JSON = "dist/pts/calculators/season_tickets_s26.json"
UTIL = "/wp-content/uploads/season_images/utility/"

# Currency / consumable rewards. These never appear in the PTS entitlement
# export, so they are recognised by name and given the shared icon + tally
# bucket, exactly as the curated S25 rows do.
# NOTE: patterns are matched against norm(), which lowercases and collapses all
# punctuation to single spaces - so "S.C.O.R.E. Booster x3" arrives here as
# "s c o r e booster x3" and "Re-Roller x5" as "re roller x5".
#   pattern -> (tallyCategory, image filename under UTIL)
CONSUMABLES = [
    (r"^\d+ atoms$",                  "atoms",                "score_currency_atoms.avif"),
    (r"^caps x",                      "caps",                 "score_currency_caps.avif"),
    (r"^gold bullion x",              "gold_bullion",         "score_currency_bullion.avif"),
    (r"^perk coins? x",               "perk_coins",           "score_currency_perkcoin.avif"),
    (r"^stamps x",                    "stamps",               "score_currency_stamps.avif"),
    (r"^legendary modules? x",        "legendary_module",     "score_game_legendarymodule.avif"),
    (r"^lunchbox x",                  "lunchbox",             "atx_store_lunchbox001.avif"),
    (r"^mystery bobblehead x",        "mystery_bobblehead",   "score_utility_mysterybobblehead.avif"),
    (r"^mystery magazine package x",  "mystery_magazine",     "score_utility_magazinebookbox.avif"),
    (r"^nuclear keycard x",           "nuclear_keycard",      "score_utility_nuclearkeycard.avif"),
    (r"^re roller x",                 "re_roller",            "score_utility_reroller.avif"),
    (r"^(basic )?repair kit",         "repair_kit",           "atx_utility_repairkit_basic.avif"),
    (r"^carry weight booster x",      "carry_weight_booster", "score_utility_carryweight.avif"),
    (r"^s c o r e booster x",         "score_booster",        "score_utility_scorebooster.avif"),
    (r"s c o r e boost$",             "score_booster",        "score_s24_account_scoreboost_1.avif"),
]

# Board caption -> PTS name, for the handful the loose join cannot bridge.
# Kept explicit rather than fuzzy-matched: a wrong auto-match here would put the
# wrong description and entitlement on a reward and nobody would notice.
ALIASES = {
    # The board prints a bare title; the PTS export prefixes the title type.
    # norm() already strips "player title prefix/suffix" but not the C.A.M.P.
    # variants, which use full stops.
    'haunted':                            'C.A.M.P. Title Prefix: "Haunted"',
    'eerie':                              'C.A.M.P. Title Prefix: "Eerie"',
    'mansion':                            'C.A.M.P. Title Suffix: "Mansion"',
    # PTS export has a typo in the outfit name.
    'batty skirt outfit':                 'Batty Skirtn Outfit',
    # Board shortens these; PTS carries the full object name.
    'weather station blood moon':         'Weather Control Station (Blood Moon)',
    'creepy cultist well':                'Creepy Cultist Well Collector',
    'fish of appalachia cranberry bog':   'Fish of Appalachia: Cranberry Bog Poster',
    'tales from the west virginia hills vol 1':
        'Tales from the West Virginia Hills Vol.1 Poster',
    'tales from the west virginia hills vol 4':
        'Tales from the West Virginia Hills Vol.4 Poster',
}


def norm(s):
    """Loose key for joining a board caption to a PTS name."""
    s = str(s or "").lower()
    s = s.replace("’", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"\b(player icon|camp title prefix|camp title suffix|"
               r"player title prefix|player title suffix)\b[: ]*", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def slug(s):
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")


def load_transcript(path):
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or line.startswith("–") or line.startswith("page|name"):
                continue
            parts = line.split("|")
            if len(parts) != 4:
                sys.exit("Malformed transcript line: %s" % line)
            page, name, cost, first = [p.strip() for p in parts]
            out.append({"page": page, "name": name, "cost": int(cost),
                        "isFirst": first.upper() == "FIRST"})
    return out


def consumable(name):
    n = norm(name)
    for pat, tally, img in CONSUMABLES:
        if re.search(pat, n):
            return tally, UTIL + img
    return None, None


# Rewards whose icon filename cannot be derived from the entitlement.
# Keyed on norm(board name) -> reward-image stem.
IMAGE_OVERRIDES = {
    # All five Cryolator mods ship as one shared icon.
    "blasted barrel mod cryolator":        "score_s26_weapons_cryolator_mods",
    "circuit chiller barrel mod cryolator": "score_s26_weapons_cryolator_mods",
    "polar lobber barrel mod cryolator":   "score_s26_weapons_cryolator_mods",
    "cold surge muzzle mod cryolator":     "score_s26_weapons_cryolator_mods",
    "hypothermic muzzle mod cryolator":    "score_s26_weapons_cryolator_mods",
    "cryolator magazine mods":             "score_s26_weapons_cryolator_mods",
    # Icon file hyphenates the model, the entitlement does not. The key is
    # post-norm(), which strips the "player icon" words.
    "t 51b":                               "score_s26_playericon_t-51b",
    # Entitlement orders the words the other way round.
    "bloodstained tinker s workbench":
        "score_s26_camp_machinery_workbench_tinker_bloodstained",
}


def load_reward_images(folder):
    """Filename stems of the converted per-season reward icons, e.g.
    'score_s26_apparel_headwear_bloodynurse_hat'."""
    if not folder or not os.path.isdir(folder):
        return set()
    return {os.path.splitext(f)[0].lower()
            for f in os.listdir(folder) if f.lower().endswith(".avif")}


def image_from_entitlement(ent, stems):
    """SCORE_S26_ENTM_Apparel_Headwear_BloodyNurse_Hat
         -> /wp-content/uploads/season_images/score_s26_apparel_headwear_bloodynurse_hat.avif

    The exported icon drops the ENTM segment for most rewards but keeps it for
    a few (the power armour paints), so both spellings are tried. The .webp
    extension is deliberate: resolveImageUrl() in df-bnb-seasons.js rewrites
    /season_images/score_s{N}_*.* to /season_images/season-{N}/*.avif, which is
    where the icons actually live."""
    if not ent:
        return ""
    low = ent.lower()
    for cand in (low.replace("_entm_", "_", 1), low):
        if cand in stems:
            return "/wp-content/uploads/season_images/%s.webp" % cand
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transcript", required=True)
    ap.add_argument("--pts", default=PTS_JSON)
    ap.add_argument("--out", required=True)
    ap.add_argument("--report", default=None)
    ap.add_argument("--reward-images", default=None,
                    help="folder of converted score_s26_*.avif reward icons, "
                         "used to fill imageUrl from the storefront entitlement")
    a = ap.parse_args()

    board = load_transcript(a.transcript)
    pts = json.load(open(a.pts, encoding="utf-8"))["items"]
    stems = load_reward_images(a.reward_images)

    by_name = {}
    for it in pts:
        by_name.setdefault(norm(it.get("name")), []).append(it)

    rows, matched, consumables, unmatched = [], 0, 0, []
    used_ids = set()
    seen_pts = set()

    for card in board:
        key = norm(card["name"])
        if key in ALIASES:
            key = norm(ALIASES[key])
        hit = None
        for cand in by_name.get(key, []):
            if id(cand) not in seen_pts:
                hit = cand
                seen_pts.add(id(cand))
                break

        tally, img = consumable(card["name"])

        if hit:
            matched += 1
            name = hit.get("name") or card["name"]
            kind = hit.get("kind", "")
            value = hit.get("value", "")
            desc = hit.get("description", "")
            ent = hit.get("storefrontEntitlement", "")
            override = IMAGE_OVERRIDES.get(norm(card["name"]))
            image = (("/wp-content/uploads/season_images/%s.webp" % override
                      if override and override in stems else "")
                     or hit.get("imageUrl", "")
                     or image_from_entitlement(ent, stems)
                     or (img or ""))
            tallyc = hit.get("tallyCategory", "") or (tally or "")
        elif tally:
            consumables += 1
            name, kind, value, desc, ent = card["name"], "", "", "", ""
            image, tallyc = img, tally
        else:
            unmatched.append(card)
            name, kind, value, desc, ent, image, tallyc = \
                card["name"], "", "", "", "", "", ""

        base = "S26_P%s_%s" % (card["page"], slug(name))
        rid, i = base, 2
        while rid in used_ids:
            rid = "%s_%d" % (base, i)
            i += 1
        used_ids.add(rid)

        rows.append([SEASON, rid, card["page"], name, card["cost"],
                     "TRUE" if card["isFirst"] else "FALSE",
                     kind, value, tallyc, image, desc, ent, "", "", ""])

    with open(a.out, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(["seasonNumber", "id", "page", "name", "cost", "isFirst",
                    "kind", "value", "tallyCategory", "imageUrl", "description",
                    "storefrontEntitlement", "reappearances", "addedInRerun", "rank"])
        w.writerows(rows)

    leftover = [it["name"] for it in pts if id(it) not in seen_pts]

    report = []
    report.append("Season 26 board -> season_rewards rows")
    report.append("  board cards          : %d" % len(board))
    report.append("  joined to PTS export : %d" % matched)
    report.append("  currency/consumable  : %d" % consumables)
    report.append("  UNJOINED             : %d" % len(unmatched))
    if unmatched:
        report.append("")
        report.append("  These board cards found no PTS entry and no consumable rule.")
        report.append("  Name/description/image are blank - check each before appending:")
        for c in unmatched:
            report.append("    p%-3s %-45s %3d tickets%s"
                          % (c["page"], c["name"], c["cost"],
                             "  [1st]" if c["isFirst"] else ""))
    if leftover:
        report.append("")
        report.append("  PTS entries never seen on the board (%d) - either a PTS-only" % len(leftover))
        report.append("  item that was cut, or a board caption read wrong:")
        for n in leftover:
            report.append("    %s" % n)

    text = "\n".join(report)
    print(text)
    if a.report:
        open(a.report, "w", encoding="utf-8").write(text + "\n")
    print("\nWrote %d rows -> %s" % (len(rows), a.out))


if __name__ == "__main__":
    main()
