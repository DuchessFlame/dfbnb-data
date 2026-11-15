#!/usr/bin/env python3
"""
build_bounty_hunting_rewards_json.py
Build Bounty Hunting reward JSON for buffsnbrew.com.

Reads GLOB values from TSV for numeric accuracy, structures the reward tree
manually based on confirmed xEdit data.  Output consumed by the upcoming
df-bnb-bounty-hunting.js (Phase 2).

Covers these pages:
  - grunt-hunt-rewards                (Burning Springs Grunt Hunt — daily, solo)
  - skyline-valley-grunt-hunt-rewards (Skyline Valley Grunt Hunt — PTS until it ships)
  - head-hunt-rewards                 (Burning Springs Head Hunt — public event, hand-checked tree)
  - skyline-valley-head-hunt-rewards  (Skyline Valley Head Hunt — generative, PTS until it ships)

GRUNT HUNTS ARE GENERATIVE (Nov 2026). Both regions share one reward list,
BountyHunt_GruntHunt_LL_QuestRewards [LVLI:007D6A6D]; on the Skyline Valley
PTS it switches on the bounty's region (LocationHierarchyHasKeyword). The
grunt pages are walked from the TSVs by bounty_trees.py through rng76, so the
Legendary Modules, Bounty Sniffer and Hideout Lead entries the PTS added show
up by themselves, and the Skyline Valley page only exists on a channel whose
tree has a Skyline Valley branch. The Outlaw's own loot is its death item list
(NPC INAM, LL_BountyDrop_Grunt) - the Outlaw is the only enemy the hunt spawns
with bounty loot; the support wave enemies keep their normal loot.
The Head Hunt page below is still the hand-checked tree it was.

KEY DIFFERENCE FROM INFESTATIONS (HTO):
  Bounty hunts have three separate reward tiers that fire:
    1. Event Rewards   — GMRW quest-completion LVLI (star-level gated)
    2. Boss Loot       — LL_BountyDrop_BIG (enemy death items from the target)
    3. Mob Loot        — LL_BountyDrop_REG (regular enemy death items)
    4. Support Loot    — LL_BountyDrop_SML (minor creature death items)

Output: dist/bounty-hunting/bounty_hunting_rewards.json
"""

import argparse, json, os, glob, csv, re, sys
from pathlib import Path
import tsv_source          # one resolver for every export selection

# ── Paths ───────────────────────────────────────────────────────────────────
# CI (both channels) runs it bare: tsv/ is already the channel's exports there
# and the PTS workflow relocates dist/ -> dist/pts/. --tsv-dir / --dist are for
# a local PTS run:  python src/build_bounty_hunting_rewards_json.py --pts
_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_DIR    = _REPO_ROOT / "tsv"
DIST_DIR   = _REPO_ROOT / "dist" / "bounty-hunting"

sys.path.insert(0, str(Path(__file__).resolve().parent))


def newest(pattern):
    """Pick the chronologically newest TSV matching a glob pattern."""
    return tsv_source.newest(str(TSV_DIR / pattern), required=False)


def read_tsv(path):
    """Read a TSV file, return list of dicts."""
    if not path or not os.path.exists(path):
        return []
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, encoding=enc, newline="") as f:
                return list(csv.DictReader(f, delimiter="\t"))
        except (UnicodeDecodeError, KeyError):
            continue
    return []


def load_globs():
    """Load GLOB FormID → FLTV mapping."""
    path = newest("GLOB_Export_*.tsv")
    globs = {}
    for row in read_tsv(path):
        fid = (row.get("FormID") or "").strip()
        fltv = row.get("FLTV", "0")
        try:
            globs[fid] = float(fltv)
        except (ValueError, TypeError):
            globs[fid] = 0.0
    return globs


# ── Constants ───────────────────────────────────────────────────────────────

TOGGLE_WARNING = (
    "This reward is enabled or disabled at Bethesda's discretion "
    "and may not always be active."
)


# ════════════════════════════════════════════════════════════════════════════
# GRUNT HUNTS — generative, one page per region (bounty_trees.py + rng76)
# ════════════════════════════════════════════════════════════════════════════

GRUNT_REWARD_ROOT = "007D6A6D"   # BountyHunt_GruntHunt_LL_QuestRewards (GMRW 007D6A81)
GRUNT_DEATH_FALLBACK = "007CFA78"  # LL_BountyDrop_Grunt, if the NPC export has no INAM

GRUNT_PAGES = {
    # slug: (region id, quest FormID, page name)
    "grunt-hunt-rewards": ("burning-springs", "007D6A80", "Burning Springs: Grunt Hunt"),
    "skyline-valley-grunt-hunt-rewards": ("skyline-valley", "008A8998", "Skyline Valley: Grunt Hunt"),
}


def _outlaws():
    """The Grunt Hunt targets (BountyHunt_GruntHunt_Grunt_*): names + death list."""
    rows = read_tsv(newest("NPC_Export_*.tsv"))
    names, death = [], {}
    for r in rows:
        ed = (r.get("EDID") or "").strip()
        # PTS: BountyHunt_GruntHunt_Grunt_*; live: Burn_BountyTarget_SML_* (same Outlaws)
        if not re.match(r"^(BountyHunt_GruntHunt_Grunt_|Burn_BountyTarget_SML_)", ed):
            continue
        full = (r.get("FULL") or "").strip()
        if not full.endswith("Outlaw"):          # skip the template / debug actors
            continue
        if full and full not in names:
            names.append(full)
        inam = (r.get("INAM_FormID") or "").strip().upper()[-8:]
        if inam:
            death[inam] = death.get(inam, 0) + 1
    death_fid = max(death, key=death.get) if death else GRUNT_DEATH_FALLBACK
    return sorted(names), death_fid


def _tree_has_region(walker, list_id, kw, depth=0):
    """Does this reward tree switch on region keyword `kw` anywhere near the top?"""
    for e in walker.L.entries_by_list.get(list_id, []):
        conds = walker.R._entry_conditions(e)
        if any("LocationHierarchyHasKeyword" in c and kw in c for c in conds):
            return True
        ref = (e.get("LVLO_Reference") or "")
        if depth < 2 and ref.upper().endswith(":LVLI"):
            if _tree_has_region(walker, ref.split(":")[0], kw, depth + 1):
                return True
    return False


HEAD_REWARD_ROOT = "007EBDF3"   # BountyHunt_HeadHunt_LL_QuestRewards (GMRW 007EBDEE)

# Generative Head Hunt pages. Burning Springs keeps the hand-checked tree
# (head-hunt-rewards, below); Skyline Valley is walked from the files and only
# exists on a channel that has the Skyline Valley bosses.
HEAD_PAGES = {
    "skyline-valley-head-hunt-rewards": ("skyline-valley", "008A8999", "Skyline Valley: Head Hunt",
                                         r"^BountyHunt_HeadHunt_Skyline_(Boss|Lackey)_"),
}


def _head_cast(edid_re):
    """{'Boss'|'Lackey': {death list FormID: [names]}} for one region's Head Hunt NPCs."""
    out = {"Boss": {}, "Lackey": {}}
    for r in read_tsv(newest("NPC_Export_*.tsv")):
        m = re.match(edid_re, (r.get("EDID") or "").strip())
        full = (r.get("FULL") or "").strip()
        if not m or not full:
            continue
        inam = (r.get("INAM_FormID") or "").strip().upper()[-8:]
        names = out[m.group(1)].setdefault(inam, [])
        if full not in names:
            names.append(full)
    return out


def build_head_pages(walker, head_xp, caps, ltt_status, seasonal_groups):
    pages = {}
    for slug, (region, quest_fid, name, edid_re) in HEAD_PAGES.items():
        cast = _head_cast(edid_re)
        if not cast["Boss"]:
            print(f"[Bounty] {slug}: no bosses on this channel - not written")
            continue
        boss_list = max(cast["Boss"], key=lambda k: len(cast["Boss"][k]))
        kick_list = max(cast["Lackey"], key=lambda k: len(cast["Lackey"][k])) if cast["Lackey"] else ""
        other_kicks = sorted(n for k, v in cast["Lackey"].items() if k != kick_list for n in v)
        pages[slug] = {
            "name": name, "region": region, "regionName": "Skyline Valley",
            "questFormID": quest_fid, "gmrwFormID": "007EBDEE", "rewardListFormID": HEAD_REWARD_ROOT,
            "type": "bountyhunt", "hunt": "head", "generative": True,
            "xp": head_xp, "caps": caps,
            "eventRewards": walker.grunt_event_pools(HEAD_REWARD_ROOT, region),
            "bossLoot": walker.death_pools(boss_list),
            "mobLoot": walker.death_pools(kick_list) if kick_list else [],
            "supportLoot": [],
            "bosses": sorted(n for v in cast["Boss"].values() for n in v),
            "sidekicks": sorted(cast["Lackey"].get(kick_list, [])),
            "sidekicksOwnLoot": other_kicks,
            "bossDeathList": boss_list, "sidekickDeathList": kick_list,
            "seasonalGroups": seasonal_groups,
            "lttStatus": ltt_status,
        }
    return pages


def build_grunt_pages(globs, grunt_xp, caps, ltt_status):
    import rng76
    import bounty_trees as bt
    data = rng76.Rng76Data.from_tsv_root(str(TSV_DIR))
    walker = bt.GruntTreeWalker(data)
    outlaws, death_fid = _outlaws()
    death = walker.death_pools(death_fid)
    pages = {}
    for slug, (region, quest_fid, name) in GRUNT_PAGES.items():
        kw = bt.REGION_KEYWORDS[region]
        if region != "burning-springs" and not _tree_has_region(walker, GRUNT_REWARD_ROOT, kw):
            print(f"[Bounty] {slug}: no {bt.REGION_NAMES[region]} branch on this channel - not written")
            continue
        pages[slug] = {
            "name": name,
            "region": region,
            "regionName": bt.REGION_NAMES[region],
            "questFormID": quest_fid,
            "gmrwFormID": "007D6A81",
            "rewardListFormID": GRUNT_REWARD_ROOT,
            "type": "bountyhunt",
            "hunt": "grunt",
            "generative": True,
            "description": (
                f"Solo {bt.REGION_NAMES[region]} bounty - pick up a Grunt Hunt, track down the "
                "Outlaw and kill it. The star level of the hunt decides the legendary rank "
                "of the reward. Only one Grunt Hunt can be active at a time."
            ),
            "xp": grunt_xp,
            "caps": caps,
            "eventRewards": walker.grunt_event_pools(GRUNT_REWARD_ROOT, region),
            "outlawLoot": death,
            "outlawDeathList": death_fid,
            "outlaws": outlaws,
            "lttStatus": ltt_status,
        }
    return pages, walker


# ── Build reward structure ──────────────────────────────────────────────────

def build_bounty_hunting_rewards():
    globs = load_globs()

    def g(fid, fallback=0.0):
        return globs.get(fid, fallback)

    # ────────────────────────────────────────────────────────────────────────
    # Shared GLOB lookups
    # ────────────────────────────────────────────────────────────────────────
    grunt_xp     = int(g("007D6A68", 1000))     # XP_Burn_BountyHuntDaily
    head_xp      = int(g("007EBDED", 1500))     # XP_Burn_BountyHuntPublic
    caps         = int(g("007D6A67", 5000))      # zzzBurn_BountyHunty_CapsPrice1Star

    # LTT toggle GLOBs (0 = OFF, 1 = ON)
    ltt_grunt_toggle = g("0085712D", 0.0)        # LTT_GruntHuntBonusStarDrops_Toggle
    ltt_head_toggle  = g("008553A3", 0.0)        # LTT_HeadHunt4StarDrops_Toggle

    # LTT ChanceNone GLOBs (100 = 0% drop when toggled on)
    ltt_grunt_cn = g("008553A2", 100.0)           # LTT_GruntHuntBonusStarDrops_ChanceNoneDropRate
    ltt_head_cn  = g("0085712E", 100.0)           # LTT_HeadHunt4StarDrops_ChanceNoneDropRate

    # Camp Title ChanceNone (75 → 25% drop)
    camp_title_cn = g("0089EA90", 75.0)           # SpawnChance_Cnone_ActivityCampTitle

    # Improved Repair Kit 3rd-entry ChanceNone (50 → 50% for the 3rd kit)
    repair_kit_cn = g("00421776", 50.0)           # Econ_ImprovedRepairKitLoot_3_ChanceNone

    # Computed rates
    ltt_grunt_active   = ltt_grunt_toggle >= 1.0
    ltt_grunt_drop_pct = round(100 - ltt_grunt_cn, 1) if ltt_grunt_active else 0
    ltt_head_active    = ltt_head_toggle >= 1.0
    ltt_head_drop_pct  = round(100 - ltt_head_cn, 1) if ltt_head_active else 0
    camp_title_pct     = round(100 - camp_title_cn, 1)  # 25%

    # ── SDOW / Slasher seasonal integration (Head Hunt only) ────────────────
    # Gated on the presence of the SDOW Head Hunt toggle GLOB. On the live
    # channel this GLOB is absent from the TSV, so the whole seasonal block is
    # omitted; on PTS (and on live once the records land) it is present, so the
    # Slasher party-crasher group is emitted.
    slasher_present      = "008E0671" in globs            # LCP_SDOW_LTC_HeadHuntsToggle
    slasher_headhunt_on  = g("008E0671", 0.0) >= 1.0      # master toggle state
    slasher_axe_override = g("0090FC28", 0.0) >= 1.0      # LCP_SDOW_LTC_SlasherAxeLootOverride
    slasher_axe_pct      = 5 if slasher_axe_override else 0
    slasher_spawn_rate   = g("008FADB5", 0.0)         # LCP_SDOW_HeadHuntPartyCrasherSpawnRate
    slasher_spawn_pct    = round(slasher_spawn_rate, 1)

    # NOTE: the SDOW records ship dormant in the live game files, so mere
    # presence isn't enough to decide visibility. Show the Slasher group when
    # the Head Hunt toggle is actually enabled (live activation) OR when we're
    # building the PTS preview channel (DFBNB_CHANNEL=pts).
    is_pts_channel = os.environ.get("DFBNB_CHANNEL", "").strip().lower() == "pts"
    show_slasher   = slasher_present and (slasher_headhunt_on or is_pts_channel)

    # GRUNT HUNT pages are built by build_grunt_pages() (generative, see top).

    # ════════════════════════════════════════════════════════════════════════
    # HEAD HUNT  (007EBDF3 — Burn_BountyHuntPublic_LL_QuestRewards)
    # ════════════════════════════════════════════════════════════════════════
    #
    # Root LVLI: UseAll (001), 6 entries, max_count=0 → independent.
    # All entries fire independently — no star-level gating.

    head_event_rewards = []

    # ── 1. Legendary Modules (3–5) ────────────────────────────────────────
    # UseAll (001), max_count=0, 3 entries → independent.
    # Entry 0: qty=3, cn=0  → 100% (guaranteed 3 modules)
    # Entry 1: qty=1, cn=40 → 60%  (chance of a 4th)
    # Entry 2: qty=1, cn=60 → 40%  (chance of a 5th)
    head_event_rewards.append({
        "label": "Legendary Modules",
        "formid": "00843A0F",
        "blurb": "Guaranteed 3 modules · 60% chance of a 4th · 40% chance of a 5th",
        "dropRate": 100,
        "items": [
            {"name": "Legendary Module",  "formid": "005652F9",
             "sig": "MISC", "qty": 3, "dropRate": 100},
            {"name": "Legendary Module",  "formid": "005652F9",
             "sig": "MISC", "qty": 1, "dropRate": 60},
            {"name": "Legendary Module",  "formid": "005652F9",
             "sig": "MISC", "qty": 1, "dropRate": 40},
        ],
        "mode": "useall",
    })

    # ── 2. 3★ BOUNTY Legendary ────────────────────────────────────────────
    # UseAll (001), max_count=0, 3 entries, all cn=0 → independent, all 100%.
    # You get ALL THREE — armour, melee weapon, and ranged weapon.
    head_event_rewards.append({
        "label": "3★ BOUNTY Legendary Items",
        "formid": "00833A16",
        "blurb": "Guaranteed drop of all 3 items · 3★ Legendary",
        "dropRate": 100,
        "items": [
            {"name": "3★ Legendary BOUNTY Armour",         "formid": "00853D20",
             "sig": "LGDI", "qty": 1, "dropRate": 100},
            {"name": "3★ Legendary BOUNTY Melee Weapon",   "formid": "0085DCD4",
             "sig": "LGDI", "qty": 1, "dropRate": 100},
            {"name": "3★ Legendary BOUNTY Ranged Weapon",  "formid": "0085DCD7",
             "sig": "LGDI", "qty": 1, "dropRate": 100},
        ],
        "mode": "useall",
    })

    # ── 3. LTT 4★ Legendary (toggled) ────────────────────────────────────
    # Toggle GLOB 008553A3 = 0 → currently OFF.
    # When ON, ChanceNone GLOB 0085712E controls drop rate.
    # Pick-one of 4 types (armour, PA, melee, ranged).
    if ltt_head_active:
        ltt_head_blurb = f"Currently active · {ltt_head_drop_pct}% chance per completion"
    else:
        ltt_head_blurb = "Currently disabled by Bethesda"

    head_event_rewards.append({
        "label": "LTT 4★ Legendary",
        "formid": "0085646D",
        "blurb": f"Bonus 4★ legendary from the Live Tuning Table · {ltt_head_blurb}",
        "dropRate": ltt_head_drop_pct,
        "warningNote": TOGGLE_WARNING,
        "items": [
            {"name": "4★ Legendary Armour",         "formid": "00884073",
             "sig": "LGDI", "qty": 1, "dropRate": 25},
            {"name": "4★ Legendary Power Armour",   "formid": "00884074",
             "sig": "LGDI", "qty": 1, "dropRate": 25},
            {"name": "4★ Legendary Melee Weapon",   "formid": "00884075",
             "sig": "LGDI", "qty": 1, "dropRate": 25},
            {"name": "4★ Legendary Ranged Weapon",  "formid": "00884076",
             "sig": "LGDI", "qty": 1, "dropRate": 25},
        ],
        "mode": "pickone",
        "toggleGlob": "008553A3",
        "chanceNoneGlob": "0085712E",
    })

    # ── 4. Improved Bait (1–3) ────────────────────────────────────────────
    # Pick-one (flags all-zero), 3 entries, each gives a different qty of
    # the same item. 33.3% chance of each qty tier.
    # Condition: must have completed Fishing MQ01 "Casting Off".
    head_event_rewards.append({
        "label": "Improved Bait",
        "formid": "0081137A",
        "blurb": "Guaranteed drop · 33% chance each of 1, 2, or 3 bait",
        "dropRate": 100,
        "conditions": ["Player has completed quest: Casting Off (Fishing MQ01)"],
        "items": [
            {"name": "Improved Bait", "formid": "007FDC33",
             "sig": "MISC", "qty": 1, "dropRate": round(100 / 3, 2)},
            {"name": "Improved Bait", "formid": "007FDC33",
             "sig": "MISC", "qty": 2, "dropRate": round(100 / 3, 2)},
            {"name": "Improved Bait", "formid": "007FDC33",
             "sig": "MISC", "qty": 3, "dropRate": round(100 / 3, 2)},
        ],
        "mode": "pickone",
    })

    # ── 5. Treasury Notes ×2 ─────────────────────────────────────────────
    # ForEach (11), 1 entry, qty=2 → guaranteed 2 Treasury Notes.
    # Condition: Gold_Treasury_Note_Loot_Enabled = 1 (currently ON).
    head_event_rewards.append({
        "label": "Treasury Notes",
        "formid": "005A5442",
        "blurb": "Guaranteed drop · 2 notes",
        "dropRate": 100,
        "items": [
            {"name": "Treasury Note", "formid": "005A5443",
             "sig": "MISC", "qty": 2, "dropRate": 100},
        ],
    })

    # ── 6. Camp Title: Hideout ────────────────────────────────────────────
    # ChanceNone GLOB 0089EA90 = 75 → 25% chance.
    # Condition: player has NOT already learned the recipe.
    head_event_rewards.append({
        "label": "Camp Title Recipe: Hideout",
        "formid": "0089EA74",
        "blurb": f"{camp_title_pct}% chance · only drops if you haven't learned it yet",
        "dropRate": camp_title_pct,
        "conditions": ["Player has NOT learned recipe: CAMPTitle Suffix Hideout"],
        "items": [
            {"name": "Camp Title: Hideout", "formid": "0089EA74",
             "sig": "BOOK", "qty": 1, "dropRate": camp_title_pct},
        ],
        "chanceNoneGlob": "0089EA90",
    })

    # ════════════════════════════════════════════════════════════════════════
    # ENEMY DEATH LOOT — shared across Grunt and Head Hunts
    # ════════════════════════════════════════════════════════════════════════
    #
    # Bounty hunt enemies use three tiered death-item lists:
    #   LL_BountyDrop_BIG (00823671) — the bounty target (boss)
    #   LL_BountyDrop_REG (007CFA7A) — regular enemies
    #   LL_BountyDrop_SML (007CFA78) — support creatures
    #
    # All are UseAll (001). Max counts are high (20/20/5) meaning all entries
    # fire independently. Contents are standard loot: caps, stimpaks, rad-x,
    # contextual ammo, and bounty-specific junk.

    # --- Caps High sub-list (003AE2A1) ---
    # Pick-one (01) of 10 entries, qty ranges 3-7 caps. Average ~4.6 caps.
    _caps_high_avg = 4.6

    # --- Stimpak sub-list (00307C9E) ---
    # UseAll (001) max_count=1 → waterfall of Rad-X (cn varies) + diluted.

    # --- Junk sub-lists ---
    # 00843A10: 3 entries (2× common 58 items, 1× rare 40 items w/ cn=20)
    # Common: pick-one of 58 junk items, Rare: pick-one of 40 junk (80% chance)

    def _build_boss_loot():
        """LL_BountyDrop_BIG (00823671) — UseAll, 7 entries, max_count=20."""
        return [
            {
                "label": "Caps",
                "formid": "003AE2A1",
                "blurb": "3 rolls on the caps table · average ~14 caps total",
                "dropRate": 100,
                "items": [
                    {"name": "Caps (roll 1)", "formid": "003AE2A1",
                     "sig": "LVLI", "qty": 2, "dropRate": 100,
                     "note": "Pick-one of 3–7 caps"},
                    {"name": "Caps (roll 2)", "formid": "003AE2A1",
                     "sig": "LVLI", "qty": 2, "dropRate": 100,
                     "note": "Pick-one of 3–7 caps"},
                    {"name": "Caps (roll 3)", "formid": "003AE2A1",
                     "sig": "LVLI", "qty": 2, "dropRate": 100,
                     "note": "Pick-one of 3–7 caps"},
                ],
                "mode": "useall",
            },
            {
                "label": "Rad-X",
                "formid": "002AD63F",
                "blurb": "Waterfall · Rad-X or Diluted Rad-X",
                "dropRate": 100,
                "items": [
                    {"name": "Rad-X",         "formid": "00024057",
                     "sig": "ALCH", "qty": 1, "dropRate": 100,
                     "note": "ChanceNone varies by tier GLOB"},
                    {"name": "Diluted Rad-X",  "formid": "0012D3F1",
                     "sig": "ALCH", "qty": 1, "dropRate": 100,
                     "note": "Fallback if Rad-X fails"},
                ],
                "mode": "waterfall",
            },
            {
                "label": "Contextual Ammo",
                "formid": "00621922",
                "blurb": "Guaranteed drop · 1 round matching your equipped weapon",
                "dropRate": 100,
                "items": [
                    {"name": "Contextual Ammo", "formid": "00621922",
                     "sig": "LVLI", "qty": 1, "dropRate": 100},
                ],
            },
            {
                "label": "Stimpaks",
                "formid": "00307C9E",
                "blurb": "Waterfall · Stimpak or Diluted Stimpak",
                "dropRate": 100,
                "items": [
                    {"name": "Stimpak",         "formid": "00307C9E",
                     "sig": "LVLI", "qty": 1, "dropRate": 100},
                ],
            },
            {
                "label": "Bounty Junk",
                "formid": "00843A10",
                "blurb": "3 junk items · 2 common rolls + 1 rare roll (80% chance)",
                "dropRate": 100,
                "children": [
                    {
                        "label": "Common Junk (roll 1)",
                        "formid": "00843A11",
                        "blurb": "Pick-one of 58 common junk items",
                        "dropRate": 100,
                        "mode": "pickone",
                    },
                    {
                        "label": "Common Junk (roll 2)",
                        "formid": "00843A11",
                        "blurb": "Pick-one of 58 common junk items",
                        "dropRate": 100,
                        "mode": "pickone",
                    },
                    {
                        "label": "Rare Junk",
                        "formid": "00843A12",
                        "blurb": "Pick-one of 40 rare junk items · 80% chance to drop",
                        "dropRate": 80,
                        "mode": "pickone",
                        "listChanceNone": 20,
                    },
                ],
                "mode": "useall",
            },
        ]

    def _build_mob_loot():
        """LL_BountyDrop_REG (007CFA7A) — UseAll, 6 entries, max_count=20."""
        return [
            {
                "label": "Caps",
                "formid": "003AE2A1",
                "blurb": "2 rolls on the caps table · average ~9 caps total",
                "dropRate": 100,
                "items": [
                    {"name": "Caps (roll 1)", "formid": "003AE2A1",
                     "sig": "LVLI", "qty": 2, "dropRate": 100},
                    {"name": "Caps (roll 2)", "formid": "003AE2A1",
                     "sig": "LVLI", "qty": 2, "dropRate": 100},
                ],
                "mode": "useall",
            },
            {
                "label": "Rad-X",
                "formid": "002AD63F",
                "blurb": "Waterfall · Rad-X or Diluted Rad-X",
                "dropRate": 100,
                "items": [
                    {"name": "Rad-X",         "formid": "00024057",
                     "sig": "ALCH", "qty": 1, "dropRate": 100},
                    {"name": "Diluted Rad-X",  "formid": "0012D3F1",
                     "sig": "ALCH", "qty": 1, "dropRate": 100},
                ],
                "mode": "waterfall",
            },
            {
                "label": "Contextual Ammo",
                "formid": "00621922",
                "blurb": "Guaranteed drop · 1 round matching your equipped weapon",
                "dropRate": 100,
                "items": [
                    {"name": "Contextual Ammo", "formid": "00621922",
                     "sig": "LVLI", "qty": 1, "dropRate": 100},
                ],
            },
            {
                "label": "Stimpaks",
                "formid": "00307C9E",
                "blurb": "Waterfall · Stimpak or Diluted Stimpak",
                "dropRate": 100,
                "items": [
                    {"name": "Stimpak",         "formid": "00307C9E",
                     "sig": "LVLI", "qty": 1, "dropRate": 100},
                ],
            },
            {
                "label": "Bounty Junk",
                "formid": "00843A10",
                "blurb": "3 junk items · 2 common rolls + 1 rare roll (80% chance)",
                "dropRate": 100,
                "children": [
                    {
                        "label": "Common Junk (roll 1)",
                        "formid": "00843A11",
                        "blurb": "Pick-one of 58 common junk items",
                        "dropRate": 100,
                        "mode": "pickone",
                    },
                    {
                        "label": "Common Junk (roll 2)",
                        "formid": "00843A11",
                        "blurb": "Pick-one of 58 common junk items",
                        "dropRate": 100,
                        "mode": "pickone",
                    },
                    {
                        "label": "Rare Junk",
                        "formid": "00843A12",
                        "blurb": "Pick-one of 40 rare junk items · 80% chance to drop",
                        "dropRate": 80,
                        "mode": "pickone",
                        "listChanceNone": 20,
                    },
                ],
                "mode": "useall",
            },
        ]

    def _build_support_loot():
        """LL_BountyDrop_SML (007CFA78) — UseAll, 5 entries, max_count=5."""
        return [
            {
                "label": "Caps",
                "formid": "003AE2A1",
                "blurb": "1 roll on the caps table · average ~5 caps",
                "dropRate": 100,
                "items": [
                    {"name": "Caps", "formid": "003AE2A1",
                     "sig": "LVLI", "qty": 2, "dropRate": 100},
                ],
                "mode": "useall",
            },
            {
                "label": "Rad-X",
                "formid": "002AD63F",
                "blurb": "Waterfall · Rad-X or Diluted Rad-X",
                "dropRate": 100,
                "items": [
                    {"name": "Rad-X",         "formid": "00024057",
                     "sig": "ALCH", "qty": 1, "dropRate": 100},
                    {"name": "Diluted Rad-X",  "formid": "0012D3F1",
                     "sig": "ALCH", "qty": 1, "dropRate": 100},
                ],
                "mode": "waterfall",
            },
            {
                "label": "Contextual Ammo",
                "formid": "00621922",
                "blurb": "Guaranteed drop · 1 round matching your equipped weapon",
                "dropRate": 100,
                "items": [
                    {"name": "Contextual Ammo", "formid": "00621922",
                     "sig": "LVLI", "qty": 1, "dropRate": 100},
                ],
            },
            {
                "label": "Stimpaks",
                "formid": "00307C9E",
                "blurb": "Waterfall · Stimpak or Diluted Stimpak",
                "dropRate": 100,
                "items": [
                    {"name": "Stimpak",         "formid": "00307C9E",
                     "sig": "LVLI", "qty": 1, "dropRate": 100},
                ],
            },
            {
                "label": "Bounty Junk",
                "formid": "00843A10",
                "blurb": "3 junk items · 2 common rolls + 1 rare roll (80% chance)",
                "dropRate": 100,
                "children": [
                    {
                        "label": "Common Junk (roll 1)",
                        "formid": "00843A11",
                        "blurb": "Pick-one of 58 common junk items",
                        "dropRate": 100,
                        "mode": "pickone",
                    },
                    {
                        "label": "Common Junk (roll 2)",
                        "formid": "00843A11",
                        "blurb": "Pick-one of 58 common junk items",
                        "dropRate": 100,
                        "mode": "pickone",
                    },
                    {
                        "label": "Rare Junk",
                        "formid": "00843A12",
                        "blurb": "Pick-one of 40 rare junk items · 80% chance to drop",
                        "dropRate": 80,
                        "mode": "pickone",
                        "listChanceNone": 20,
                    },
                ],
                "mode": "useall",
            },
        ]

    # ════════════════════════════════════════════════════════════════════════
    # SDOW / SLASHER SEASONAL PARTY CRASHER  (Head Hunt only)
    # ════════════════════════════════════════════════════════════════════════
    #
    # The Reborn Pint-Sized Slasher (008E06C5) can crash a Head Hunt. When you
    # loot it, its own drop list SDOW_LL_BountyDrop_BIG (008E071A) fires in place
    # of the normal boss list. That list is the standard boss loot PLUS two
    # Slasher-only pools:
    #   A — SDOW_LL_Slasher_RareRecipes (008E06F6) @ 20%, pick-one of 5 plans
    #   B — SDOW_LLS_Slasher_Rewards_LegendaryRewards (0090FC46):
    #         guaranteed 4★ legendary + rare Super Slasher Auto Axe (Severing)
    # The adds (mob/support) and quest-completion rewards are unchanged, so the
    # event/mob/support lists reuse the standard head-hunt pools.

    def _build_slasher_boss_loot():
        pools = _build_boss_loot()  # standard baseline (caps/rad-x/ammo/stimpak/junk)

        # Pool A — Slasher Rare Recipes (20% to roll · pick-one of 5 plans)
        pools.append({
            "label": "Slasher Rare Recipes",
            "formid": "008E06F6",
            "blurb": "20% chance to roll · pick-one of 5 Slasher plans",
            "dropRate": 20,
            "conditions": ["Only from The Reborn Pint-Sized Slasher"],
            "items": [
                {"name": "Plan: Slasher Power Armor Torso Paint",   "formid": "008DE08B", "sig": "BOOK", "qty": 1, "dropRate": 20},
                {"name": "Plan: Slasher Power Armor Arms Paint",    "formid": "008DE089", "sig": "BOOK", "qty": 1, "dropRate": 20},
                {"name": "Plan: Slasher Power Armor Legs Paint",    "formid": "008DE087", "sig": "BOOK", "qty": 1, "dropRate": 20},
                {"name": "Plan: Slasher Power Armor Jetpack Paint", "formid": "008DE088", "sig": "BOOK", "qty": 1, "dropRate": 20},
                {"name": "Plan: Slasher Auto Axe Paint",            "formid": "008E069A", "sig": "BOOK", "qty": 1, "dropRate": 20},
            ],
            "mode": "pickone",
        })

        # Pool B — Slasher Legendary Rewards (guaranteed 4★ + rare Auto Axe)
        pools.append({
            "label": "Slasher Legendary Rewards",
            "formid": "0090FC46",
            "blurb": ("Guaranteed 4★ legendary · rare chance at the Super Slasher Auto Axe"
                      if slasher_axe_override else
                      "Guaranteed 4★ legendary · Super Slasher Auto Axe currently disabled by Bethesda"),
            "dropRate": 100,
            "warningNote": TOGGLE_WARNING,
            "items": [
                {"name": "4★ Legendary Item", "formid": "00863A9D", "sig": "LGDI", "qty": 1,
                 "dropRate": 100, "note": "Standard 4-star legendary template"},
                {"name": "Super Slasher Auto Axe", "formid": "006361A2", "sig": "WEAP", "qty": 1,
                 "dropRate": slasher_axe_pct,
                 "note": "Auto Axe carrying the new Severing 4★ legendary effect",
                 "conditions": [
                     "Requires completing quest: Blood Will Have Blood (SDOW_MQ05)",
                     "5% roll · only when the SlasherAxeLootOverride toggle is ON",
                 ]},
            ],
            "mode": "useall",
        })
        return pools

    head_seasonal_groups = []
    if show_slasher:
        head_seasonal_groups.append({
            "id":   "slasher",
            "label": "Seasonal — Slasher Party Crasher",
            "blurb": (
                (f"{slasher_spawn_pct}% chance per Head Hunt for The Reborn "
                 f"Pint-Sized Slasher to replace the normal bounty target · "
                 f"Level 100 seasonal boss with its own loot pool · "
                 f"requires Shadows of the Dead of Winter")
                if slasher_spawn_pct > 0 else
                ("Seasonal Shadows of the Dead of Winter encounter · "
                 "when active, The Reborn Pint-Sized Slasher can replace the "
                 "normal bounty target with a Level 100 boss carrying its own "
                 "loot pool · spawn rate currently set to 0%")
            ),
            "open": False,
            "lists": [
                {"tier": "event",   "title": "Event Rewards",
                 "rosterBlurb": "Same quest-completion rewards as a standard Head Hunt.",
                 "pools": head_event_rewards},
                {"tier": "boss",    "title": "Boss Loot",
                 "pools": _build_slasher_boss_loot()},
                {"tier": "mob",     "title": "Mob Loot",
                 "rosterBlurb": "Same as the standard Head Hunt — the adds are unchanged.",
                 "pools": _build_mob_loot()},
                {"tier": "support", "title": "Support Loot",
                 "rosterBlurb": "Same as the standard Head Hunt — the support enemies are unchanged.",
                 "pools": _build_support_loot()},
            ],
        })

    # ════════════════════════════════════════════════════════════════════════
    # ASSEMBLE FINAL JSON
    # ════════════════════════════════════════════════════════════════════════

    output = {
        "byPage": {
            "head-hunt-rewards": {
                "name": "Bounty Hunting: Head Hunt",
                "questFormID": "007EBDF4",
                "gmrwFormID": "007EBDEE",
                "type": "bountyhunt",
                "description": (
                    "Public event bounty — a high-value target appears on the map "
                    "for all players. Defeat the boss and its support enemies to "
                    "earn 3★ BOUNTY legendary items, Legendary Modules, Improved "
                    "Bait, Treasury Notes, and more. Multiple players can "
                    "participate."
                ),
                "xp": head_xp,
                "caps": caps,
                "eventRewards": head_event_rewards,
                "bossLoot": _build_boss_loot(),
                "mobLoot": _build_mob_loot(),
                "supportLoot": _build_support_loot(),
                "seasonalGroups": head_seasonal_groups,
                "lttStatus": {
                    "toggleGlob": "008553A3",
                    "toggleValue": ltt_head_toggle,
                    "active": ltt_head_active,
                    "chanceNoneGlob": "0085712E",
                    "chanceNoneValue": ltt_head_cn,
                    "dropPct": ltt_head_drop_pct,
                },
            },
        }
    }

    grunt_ltt = {
        "toggleGlob": "0085712D",
        "toggleValue": ltt_grunt_toggle,
        "active": ltt_grunt_active,
        "chanceNoneGlob": "008553A2",
        "chanceNoneValue": ltt_grunt_cn,
        "dropPct": ltt_grunt_drop_pct,
    }
    grunt_pages, walker = build_grunt_pages(globs, grunt_xp, caps, grunt_ltt)
    head_ltt = output["byPage"]["head-hunt-rewards"]["lttStatus"]
    head_pages = build_head_pages(walker, head_xp, caps, head_ltt, head_seasonal_groups)
    output["byPage"] = {**grunt_pages, **output["byPage"], **head_pages}
    return output


# ── Entry point ─────────────────────────────────────────────────────────────

def main():
    global TSV_DIR, DIST_DIR
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv-dir", default=None, help="exports folder (default tsv/)")
    ap.add_argument("--dist", default=None, help="dist folder to write into (default dist/)")
    ap.add_argument("--pts", action="store_true", help="local PTS run: tsv/pts -> dist/pts")
    a = ap.parse_args()
    if a.pts:
        TSV_DIR = _REPO_ROOT / "tsv" / "pts"
        DIST_DIR = _REPO_ROOT / "dist" / "pts" / "bounty-hunting"
        os.environ.setdefault("DFBNB_CHANNEL", "pts")
    if a.tsv_dir:
        TSV_DIR = Path(a.tsv_dir).resolve()
    if a.dist:
        DIST_DIR = Path(a.dist).resolve() / "bounty-hunting"
    os.makedirs(DIST_DIR, exist_ok=True)
    data = build_bounty_hunting_rewards()
    out_path = DIST_DIR / "bounty_hunting_rewards.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"[Bounty] Wrote {out_path}")
    for slug, page in data["byPage"].items():
        print(
            f"[Bounty] {slug}: XP={page['xp']}, Caps={page['caps']}, "
            f"Event pools={len(page['eventRewards'])}, "
            + (f"Outlaw loot pools={len(page.get('outlawLoot') or [])}"
               if page.get("hunt") == "grunt" else
               f"Boss pools={len(page['bossLoot'])}, Mob pools={len(page['mobLoot'])}, "
               f"Support pools={len(page['supportLoot'])}")
        )


if __name__ == "__main__":
    main()
