#!/usr/bin/env python3
"""
apply_season_pages.py
---------------------
Rebuilds the ORIGINAL RUN of the paged legacy seasons (S16-S23) in
tsv/season_rewards.tsv from the researched page-by-page list in
tsv/season_pages_s16_s23.tsv.

WHICH SEASONS IT TOUCHES
    Exactly the ones present in the pages TSV - nothing is hardcoded. Every
    other season in season_rewards.tsv is left byte-for-byte alone.

WHY THIS EXISTS
    Season 16 (Duel with the Devil) was the first season on the ticket-based
    scoreboard: Page 1..N plus Bonus Pages 1 and 2, every reward with a ticket
    price. Our rows for S16-S23 were never built from those boards. They came
    out of the datamined backfill (build_pts_season_scoreboard.py), which
    INVENTS page numbers by bucketing rewards about eight to a page by
    category, writes cost 0 on everything, and cannot see the currency and
    consumable slots at all because those are not ENTM records. So every one
    of those pages listed the wrong rewards on the wrong page with no prices,
    and roughly a third of each board was simply missing.

    This is the paged-season twin of apply_season_ranks.py, which did the
    same job for the rank-based board-game seasons (S1-S15).

HOW A LEGACY PAGED SEASON IS STORED
    Two columns carry the original run and leave `page` alone:

      origPage       the page the reward sat on in the ORIGINAL run
                     ("1".."15", "B1", "B2")
      origPageRank   the S.C.O.R.E. rank that unlocked that page

    `page` is left BLANK. It is reserved for the re-run: when Bethesda puts the
    season back on the scoreboard as a legacy season, its new pages (with their
    own prices) go in `page`, exactly as the rank seasons do. The renderer puts
    `page` rows in the Page 1-9 stack at the top and `origPage` rows inside the
    Original Run expand, so the two runs never mix.

    `rank` stays blank - these seasons never had ranks.

MATCHING (most reliable first)
    1. Already rebuilt: a row that already carries this origPage and the same
       entitlement or display name. Makes the script idempotent - a second run
       changes nothing.
    2. Form ID: the wikitext's `|form =` resolves through ENTM_Export to an
       editor ID, compared to the row's storefrontEntitlement. Exact.
    3. Name: the same blended fuzzy score apply_season_ranks.py uses, >= 0.82,
       inside the same season only.
    4. Otherwise a NEW row: currency/consumable slots get the shared utility art
       and a tallyCategory; anything else is built from its ENTM record
       (name, description, entitlement, editor-ID-derived image).

    A matched row keeps its `id`. The Season Ticket Calculator stores ticked
    rewards in localStorage against those ids, and renaming them would wipe
    everyone's selections for no gain. New rows get the documented
    S{N}_P{page}_{name} / S{N}_B{n}_{name} pattern.

    isFirst and cost are taken ONLY from the page list, never carried over - the
    backfill set the 1st flag on roughly half of every season at random.

    Curated rows the page list does not place are KEPT with origPage blank and
    listed in the report. They render nowhere on the scoreboard (same contract
    as the rank seasons) but nothing curated is thrown away.

STATUS: active
INPUT:  tsv/season_pages_s16_s23.tsv, tsv/season_rewards.tsv,
        tsv/ENTM_Export_July_2026.tsv
OUTPUT: tsv/season_rewards.tsv (backed up first), dist/season_pages_report.txt
USAGE:  python src/apply_season_pages.py --dry-run
        python src/apply_season_pages.py
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from apply_season_ranks import norm, similarity, snake  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist"

PAGES_TSV = TSV_DIR / "season_pages_s16_s23.tsv"
REWARDS_TSV = TSV_DIR / "season_rewards.tsv"
ENTM_TSV = TSV_DIR / "ENTM_Export_July_2026.tsv"
REPORT_TXT = DIST_DIR / "season_pages_report.txt"

TAG = "[apply_season_pages]"
MATCH_THRESHOLD = 0.82

UTILITY = "/wp-content/uploads/season_images/utility/"
SEASON_IMAGES = "/wp-content/uploads/season_images/"
PLAYER_ICONS = "/wp-content/uploads/guide-images/atom-shop/player-icons/"
TITLES_PLAYER = "/wp-content/uploads/storefront/titles-player/"
TITLES_CAMP = "/wp-content/uploads/storefront/titles-camp/"

# norm(name) pattern -> (display name, tallyCategory, utility image).
# The display names follow the house convention the S21/S24-S26 boards already
# use ("200 Atoms", "Caps x 5000", "Lunchbox x 5"), so a stacked reward reads
# the same on every season. {q} is the quantity from the page list.
# tallyCategory values are all keys of TALLY_NAMES in df-bnb-seasons.js - an
# unknown key renders as nothing, so do not invent one.
CURRENCY_RULES: list[tuple[str, str, str, str]] = [
    (r"^atoms?$",                     "{q} Atoms",                    "atoms",                "score_currency_atoms.webp"),
    (r"^caps$",                       "Caps x {q}",                   "caps",                 "score_currency_caps.webp"),
    (r"^gold bullion$",               "Gold Bullion x {q}",           "gold_bullion",         "score_currency_bullion.webp"),
    (r"^legendary scrip$",            "Legendary Scrip x {q}",        "legendary_scrip",      "score_currency_scrip.webp"),
    (r"^perk coins?$",                "Perk Coin x {q}",              "perk_coins",           "score_currency_perkcoin.webp"),
    (r"^stamps$",                     "Stamps x {q}",                 "stamps",               "score_currency_stamps.webp"),
    (r"^lunchbox(es)?$",              "Lunchbox x {q}",               "lunchbox",             "atx_store_lunchbox001.webp"),
    (r"^legendary modules?$",         "Legendary Module x {q}",       "legendary_module",     "score_game_legendarymodule.webp"),
    (r"^legendary cores?$",           "Legendary Core x {q}",         "legendary_core",       "score_game_legendary_core.avif"),
    (r"^liquid courage$",             "Liquid Courage x {q}",         "liquid_courage",       "score_game_liquidcourage.avif"),
    (r"^basic repair kits?$",         "Basic Repair Kit x {q}",       "repair_kit",           "atx_utility_repairkit_basic.webp"),
    (r"^scrap kits?$",                "Scrap Kit x {q}",              "scrap_kit",            "atx_utility_repairkit_scraptostash.avif"),
    (r"^carry weight booster$",       "Carry Weight Booster x {q}",   "carry_weight_booster", "score_utility_carryweight.webp"),
    (r"^nuclear keycards?$",          "Nuclear Keycard x {q}",        "nuclear_keycard",      "score_utility_nuclearkeycard.webp"),
    (r"^perk card pack$",             "Perk Card Pack x {q}",         "perk_card_pack",       "score_item_perkcardpack.avif"),
    (r"^re roller$",                  "Re-Roller x {q}",              "re_roller",            "score_utility_reroller.webp"),
    (r"^s ?c ?o ?r ?e booster$",       "S.C.O.R.E. Booster x {q}",     "score_booster",        "score_utility_scorebooster.webp"),
    (r"^scout s banner$",             "Scout's Banner x {q}",         "scouts_banner",        "score_coen_utility_banner.webp"),
    (r"^mystery bobblehead$",         "Mystery Bobblehead x {q}",     "mystery_bobblehead",   "score_utility_mysterybobblehead.avif"),
    (r"^mystery magazine package$",   "Mystery Magazine Package x {q}", "mystery_magazine",   "score_utility_magazinebookbox.webp"),
    (r"^superb bait$",                "Superb Bait x {q}",            "superb_bait",          "score_utility_superbait.webp"),
    (r"^sugar free nukashine$",       "Sugar-Free Nukashine x {q}",   "nukashine",            "score_item_nukashine_sugarfree.webp"),
    (r"^large vault tec supply package$", "Large Vault-Tec Supply Package x {q}", "supply_package", "score_utility_scrapball_large.avif"),
    # Re-used from Season 15 - the texture lives in utility/, not season-15/.
    (r"^ultracite supply crate$",     "Ultracite Supply Crate x {q}", "",                     "score_s15_consumable_ultracitescrapsupply.avif"),
]

# Page-list names that differ from the curated row too much for the fuzzy
# matcher, keyed (season, name on the page list). Only needed where neither the
# form ID nor the name can bridge the gap.
ALIASES: dict[tuple[int, str], str] = {
    # The S21 screenshots called the Bonus Page 2 repair kits just "Repair Kit".
    (21, "Basic Repair Kit"): "Repair Kit",
}

COLUMNS_ADDED = ["origPage", "origPageRank"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_tsv(path: Path) -> tuple[list[str], list[dict]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader.fieldnames or []), list(reader)


def write_tsv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                                lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def load_entm(path: Path) -> dict[str, dict]:
    """FormID -> {edid, full, desc}. Read with quoting OFF: ENTM DESC fields
    carry unbalanced double quotes that would otherwise swallow whole rows."""
    csv.field_size_limit(10 ** 9)
    out: dict[str, dict] = {}
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(reader)
        i_form, i_edid = header.index("FormID"), header.index("EDID")
        i_full, i_desc = header.index("FULL"), header.index("DESC")
        for r in reader:
            if len(r) <= i_desc:
                continue
            out[r[i_form].strip().upper()] = {
                "edid": r[i_edid].strip(),
                "full": r[i_full].strip(),
                "desc": r[i_desc].strip(),
            }
    return out


def currency_for(name: str, qty: str) -> tuple[str, str, str] | None:
    n = norm(name)
    for pattern, display, tally, image in CURRENCY_RULES:
        if re.search(pattern, n):
            shown = display.format(q=qty) if qty else re.sub(r"\s*x \{q\}|\{q\} ", "", display)
            return shown, tally, UTILITY + image
    return None


def score_boost_name(name: str) -> str | None:
    """'5% SCORE Boost' / '10% S.C.O.R.E. Boost' -> the house spelling."""
    m = re.match(r"^(\d+)%\s*s\.?c\.?o\.?r\.?e\.?\s*boost$", name.strip(), re.I)
    return f"{m.group(1)}% S.C.O.R.E. Boost" if m else None


def page_sort_key(pk: str) -> tuple[int, int]:
    pk = pk.upper()
    return (1, int(pk[1:])) if pk.startswith("B") else (0, int(pk))


def title_kind(edid: str) -> tuple[str, str] | None:
    """ENTM editor ID -> (kind, name prefix) for title rewards."""
    e = edid.lower()
    if "playertitle" in e:
        if "prefix" in e and "suffix" in e:
            return "playerTitlePrefixSuffix", "Player Title Prefix/Suffix"
        if "suffix" in e:
            return "playerTitleSuffix", "Player Title Suffix"
        return "playerTitlePrefix", "Player Title Prefix"
    if "camptitle" in e:
        if "suffix" in e:
            return "campTitleSuffix", "C.A.M.P. Title Suffix"
        return "campTitlePrefix", "C.A.M.P. Title Prefix"
    return None


def wiki_file(icon: str) -> str:
    """Wiki icon filename -> our uploaded filename: lowercased, spaces to
    underscores, the trailing _l size suffix dropped, .avif."""
    base = re.sub(r"\.(webp|png|jpe?g)$", "", icon.strip(), flags=re.I)
    base = base.replace(" ", "_").lower()
    base = re.sub(r"_l$", "", base)
    return base + ".avif"


def image_from_edid(edid: str, season: int) -> str:
    """The rule from season-reward-images: lowercase the EDID, strip
    SCORE_S{N}_ENTM_, prefix score_s{N}_. Written as the flat .webp form -
    asset_paths.py routes it to season-{N}/*.avif at render time."""
    # Most are SCORE_S{N}_ENTM_<rest>. Fallout 1st rewards put F1_ in front of
    # ENTM_ (SCORE_S19_F1_ENTM_CAMP_AmmoStorageBox_FatmanCrate ->
    # score_s19_f1_camp_ammostoragebox_fatmancrate) and a few drop ENTM_
    # altogether (SCORE_S16_CAMP_Workbench_Chemistry_Autopsy).
    m = re.match(r"^SCORE_S0*(\d+)_(.+)$", edid, re.I)
    if not m:
        return ""
    rest = re.sub(r"(^|_)ENTM_", r"\1", m.group(2), flags=re.I).lower()
    return f"{SEASON_IMAGES}score_s{m.group(1)}_{rest}.webp"


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    _, page_rows = read_tsv(PAGES_TSV)
    fields, reward_rows = read_tsv(REWARDS_TSV)
    entm = load_entm(ENTM_TSV)

    seasons = sorted({int(r["season"]) for r in page_rows if r.get("season")})
    print(f"{TAG} rebuilding original runs: " + ", ".join(f"S{s}" for s in seasons))

    for col in COLUMNS_ADDED:
        if col not in fields:
            fields.append(col)

    old: dict[int, list[dict]] = {s: [] for s in seasons}
    untouched: list[dict] = []
    first_index = None
    for i, row in enumerate(reward_rows):
        try:
            num = int(row.get("seasonNumber", "0"))
        except ValueError:
            num = 0
        if num in old:
            old[num].append(row)
            if first_index is None:
                first_index = i
        else:
            untouched.append(row)

    report: list[str] = []
    used: dict[int, set[int]] = {s: set() for s in seasons}   # id(row) of consumed rows
    new_rows: dict[int, list[dict]] = {s: [] for s in seasons}
    ids_taken: set[str] = {r["id"] for r in untouched}
    stats = {s: {"formid": 0, "name": 0, "again": 0, "new": 0, "dup": 0} for s in seasons}

    for entry in page_rows:
        season = int(entry["season"])
        pk = entry["page"].strip().upper()
        raw = entry["name"].strip()
        qty = (entry.get("qty") or "").strip()
        form_ids = [f.strip().upper() for f in (entry.get("formId") or "").split(",") if f.strip()]
        edids = [entm[f]["edid"] for f in form_ids if f in entm]
        edid_set = {e.lower() for e in edids}

        cur = currency_for(raw, qty)
        boost = score_boost_name(raw)
        shown = boost or (cur[0] if cur else raw)
        target = ALIASES.get((season, raw), raw)
        pool = old[season]

        def free(r: dict) -> bool:
            return id(r) not in used[season]

        match, how = None, ""
        # 1. already rebuilt on an earlier run
        for r in pool:
            if free(r) and r.get("origPage", "").upper() == pk and (
                    (edid_set and r.get("storefrontEntitlement", "").lower() in edid_set)
                    or norm(r.get("name", "")) == norm(shown)):
                match, how = r, "again"
                break
        # 2. form ID -> entitlement
        if match is None and edid_set:
            for r in pool:
                if free(r) and r.get("storefrontEntitlement", "").lower() in edid_set:
                    match, how = r, "formid"
                    break
        # 2b. same form ID as a reward already placed on this board - the S21
        #     houseboat tent is a Page 9 Fallout 1st reward AND a Bonus Page 1
        #     reward. Checked before the name pass, or the second placement
        #     fuzzy-matches the nearest look-alike (S20's "Correct Toilet Paper
        #     Holder" would take the plain "Toilet Paper Holder").
        twin = None
        if match is None and edid_set:
            twin = next((r for r in new_rows[season]
                         if r.get("storefrontEntitlement", "").lower() in edid_set), None)
        # 3. name
        if match is None and twin is None:
            best, best_score = None, 0.0
            for r in pool:
                if not free(r):
                    continue
                score = max(similarity(t, r.get("name", "")) for t in {target, shown})
                if score > best_score:
                    best, best_score = r, score
            if best is not None and best_score >= MATCH_THRESHOLD:
                match, how = best, "name"
                if best_score < 0.95:
                    report.append(f"  S{season} {pk:>3}  name {best_score:.2f}  "
                                  f"'{raw}' -> '{best.get('name')}'")

        if match is not None:
            used[season].add(id(match))
            row = dict(match)
            stats[season][how] += 1
            # Currency slots take the house name, shared art and tally bucket
            # even when a curated row existed - S21's "Repair Kit" becomes
            # "Basic Repair Kit x 5" like every other season's.
            # Fill what the curated row is missing from ENTM - the S21 rows
            # built from screenshots carry no entitlement, which is also what
            # lets a second placement of the same reward find its twin.
            if edids and not row.get("storefrontEntitlement"):
                row["storefrontEntitlement"] = edids[0]
            if form_ids and form_ids[0] in entm and not row.get("description"):
                row["description"] = entm[form_ids[0]]["desc"]
            if boost:
                row["name"] = shown
                row["tallyCategory"] = "score_booster"
            elif cur:
                row["name"] = shown
                row["tallyCategory"] = cur[1]
                row["imageUrl"] = row.get("imageUrl") or cur[2]
        else:
            # A reward placed twice on the same board (the S21 houseboat tent is
            # a Page 9 Fallout 1st reward AND a Bonus Page 1 reward) matches the
            # curated row once; the second placement copies it.
            if twin is not None:
                row = dict(twin)
                row["id"] = ""
                stats[season]["dup"] += 1
            elif cur or boost:
                row = {
                    "seasonNumber": str(season), "id": "", "name": shown,
                    "kind": "", "value": "",
                    "tallyCategory": "score_booster" if boost else cur[1],
                    "imageUrl": (UTILITY + "score_s24_account_scoreboost_1.avif") if boost else cur[2],
                    "description": "", "storefrontEntitlement": edids[0] if (boost and edids) else "",
                    "reappearances": "", "addedInRerun": "",
                }
                if boost and form_ids and form_ids[0] in entm:
                    row["description"] = entm[form_ids[0]]["desc"]
                stats[season]["new"] += 1
            else:
                rec = entm.get(form_ids[0]) if form_ids else None
                edid = rec["edid"] if rec else ""
                kind = title_kind(edid) if edid else None
                row = {
                    "seasonNumber": str(season), "id": "", "name": raw,
                    "kind": "", "value": "", "tallyCategory": "",
                    "imageUrl": "", "description": rec["desc"] if rec else "",
                    "storefrontEntitlement": edid,
                    "reappearances": "", "addedInRerun": "",
                }
                icon = entry.get("wikiIcon", "")
                if kind:
                    value = (re.search(r'"([^"]+)"', raw) or [None, raw])[1]
                    row.update(kind=kind[0], value=value, tallyCategory="camp_player_title",
                               name=f'{kind[1]}: "{value}"')
                    row["imageUrl"] = (TITLES_CAMP if kind[0].startswith("camp") else TITLES_PLAYER) + wiki_file(icon)
                elif "playericon" in edid.lower() or "playericon" in icon.lower():
                    value = re.sub(r"\s*Player Icons?$", "", raw).strip()
                    row.update(kind="playerIcon", value=value, tallyCategory="player_icon",
                               name=f"Player Icon: {value}")
                    row["imageUrl"] = PLAYER_ICONS + wiki_file(icon)
                else:
                    row["imageUrl"] = image_from_edid(edid, season) or (
                        f"{SEASON_IMAGES}season-{season}/{wiki_file(icon)}" if icon else "")
                stats[season]["new"] += 1
                why = "no ENTM record" if not rec else edid
                report.append(f"  S{season} {pk:>3}  new    '{raw}'  ({why})"
                              + ("" if row["imageUrl"] else "   [no artwork]"))

        row["seasonNumber"] = str(season)
        row["origPage"] = pk
        row["origPageRank"] = (entry.get("unlockRank") or "").strip()
        row["page"] = ""
        row["rank"] = ""
        row["cost"] = str(int(entry["cost"] or 0))
        row["isFirst"] = "TRUE" if (entry.get("isFirst") or "").strip().upper() == "TRUE" else ""
        if not row.get("id"):
            base = (f"S{season}_{pk}_" if pk.startswith("B") else f"S{season}_P{pk}_") + snake(shown)
            row["id"] = base
        row["_slot"] = int(entry.get("slot") or 0)
        new_rows[season].append(row)

    # Curated rows the page list did not place - keep them, unplaced.
    orphan_lines: list[str] = []
    for season in seasons:
        for r in old[season]:
            if id(r) in used[season]:
                continue
            leftover = dict(r)
            leftover["page"] = ""
            leftover["origPage"] = ""
            leftover["origPageRank"] = ""
            leftover["rank"] = ""
            leftover["isFirst"] = ""
            leftover["_slot"] = 0
            new_rows[season].append(leftover)
            orphan_lines.append(f"  S{season}  '{r.get('name')}'  "
                                f"({r.get('storefrontEntitlement') or 'no entitlement'})")

    # Unique ids across the whole file.
    for season in seasons:
        for row in new_rows[season]:
            base, n = row["id"], 1
            while row["id"] in ids_taken:
                n += 1
                row["id"] = f"{base}_{n}"
            ids_taken.add(row["id"])

    # In-page order: the scoreboard shows the free-to-everyone rewards first,
    # most expensive first, then the Fallout 1st bonuses the same way.
    def sort_key(row: dict):
        op = row.get("origPage", "")
        placed = 0 if op else 1
        pos = page_sort_key(op) if op else (9, 0)
        return (placed, pos, 1 if row.get("isFirst") else 0,
                -int(row.get("cost") or 0), row.get("name", ""))

    rebuilt: list[dict] = []
    for season in seasons:
        rebuilt.extend(sorted(new_rows[season], key=sort_key))
    for r in rebuilt:
        r.pop("_slot", None)

    # Keep the file's season order: the rebuilt block goes where the first of
    # its rows used to sit.
    idx = first_index if first_index is not None else len(reward_rows)
    keep = {id(r) for r in untouched}
    before = [r for r in reward_rows[:idx] if id(r) in keep]
    after = [r for r in reward_rows[idx:] if id(r) in keep]
    final_rows = before + rebuilt + after

    # ---- report ----
    lines = [f"{TAG} run {datetime.now().isoformat(timespec='seconds')}", "",
             "Per-season rows (was -> now)  [matched by form ID / name / earlier run, new, duplicates]:"]
    for s in seasons:
        st = stats[s]
        placed = sum(1 for r in new_rows[s] if r.get("origPage"))
        lines.append(f"  Season {s}: {len(old[s]):>3} -> {len(new_rows[s]):>3}   "
                     f"placed {placed:>3}  [form {st['formid']:>3} / name {st['name']:>2} / "
                     f"again {st['again']:>3}, new {st['new']:>3}, dup {st['dup']}]")
    lines += ["", "Fuzzy name matches and newly created rows:"] + (report or ["  (none)"])
    lines += ["", "Curated rows the page list did not place (kept, origPage blank):"] + (orphan_lines or ["  (none)"])
    text = "\n".join(lines)
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_TXT.write_text(text + "\n", encoding="utf-8")
    print(text)

    if args.dry_run:
        print(f"\n{TAG} dry run - {REWARDS_TSV.name} not written")
        return 0

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    shutil.copy2(REWARDS_TSV, REWARDS_TSV.with_suffix(f".tsv.bak-{stamp}"))
    write_tsv(REWARDS_TSV, fields, final_rows)
    print(f"\n{TAG} wrote {REWARDS_TSV} ({len(final_rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
