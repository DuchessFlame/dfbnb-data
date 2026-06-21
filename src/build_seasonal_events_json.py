#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# REWRITE_VERSION: 2026-04-28-v5
"""
build_seasonal_events_json.py - Seasonal Events Rewards (Tree Rewrite April 2026)

Builds:
  dist/seasonal_events/seasonal_events_rewards_by_page.json

Uses the shared rng76 engine (Rng76Data.from_tsv_root) for LVLI resolution.

Output schema per event page (abbreviated):
  {
    "name":           "Fasnacht Day Parade",
    "slug":           "fasnacht-day-parade-all-rewards",
    "eventSlug":      "fasnacht-day-parade",
    "isContainerLoot": false,
    "xp":   { "value": int, "scalesWithLevel": bool, "breakdown": [...] } | null,
    "caps": { "value": int, "breakdown": [...] } | null,
    "eventRewardTree": [ { type, label, items[{name, qty, dropRate, tiers?[]}] } ],
    "rewards":  [ ... legacy flat list, used by Unique Event Rewards expand ... ],
    "groups":   null | [ ... ],
    "gallery":  []
  }

Tier semantics:
  - Container events (Halloween, Holiday Scorched, Treasure Hunter): each
    container becomes a tier label. An item appearing in N containers will
    have N tier rows; if all rows have identical qty + rate they collapse.
  - Quest events (Fasnacht, Invaders, Meat Week, Mischief, Mothman, Big Bloom,
    Radtoads): same-stem LVLIs (e.g. Mothman _Tier01/02/03, Meat Week
    _Best/Good/Bad) merge into one tier-aware node.

Excludes:
  - Drifter Activation Card and any EDID starting with zzz_, CUT_, POST_, DEL_, P62_
  - GLOBs containing "IgnoreMe" / "XPNone" sentinels

Usage: python build_seasonal_events_json.py
"""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Import shared drop-rate engine
# ---------------------------------------------------------------------------
_this_dir = Path(__file__).resolve().parent
for _p in [_this_dir, _this_dir / "src", _this_dir.parent / "src"]:
    if _p.exists() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from rng76 import (
    Rng76Data, Rng76Resolver,
    humanize_edid, fmt_pct, pick, read_tsv, newest, safe_float,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
TSV_ROOT   = str(_REPO_ROOT / "tsv")
DIST_DIR   = _REPO_ROOT / "dist" / "seasonal_events"
RELEASE_YEARS_PATH = _REPO_ROOT / "data" / "seasonal_events_release_years.json"

# Per-page gallery image strips (bottom-of-page). Keyed by page slug; filenames
# resolve under guide-images/seasonal-events/<eventSlug>/<page-slug>/ on the site.
EVENT_GALLERIES = {
    "primal-cuts-all-rewards": [
        {"src": "forest-location.avif",        "alt": "Forest Primal Cuts Location"},
        {"src": "toxic-valley-location.avif",  "alt": "Toxic Valley Primal Cuts Location"},
        {"src": "ash-heap-location.avif",      "alt": "Ash Heap Primal Cuts Location"},
        {"src": "savage-divide-location.avif", "alt": "Savage Divide Primal Cuts Location"},
        {"src": "the-mire-location.avif",      "alt": "The Mire Primal Cuts Location"},
        {"src": "cranberry-bog-location.avif", "alt": "Cranberry Bog Primal Cuts Location"},
        {"src": "primal-cut-drums.avif",       "alt": "Primal Cut Drums"},
        {"src": "turn-in.avif",                "alt": "Turn Primal Cuts In"},
        {"src": "meat-per-region.avif",        "alt": "Meat List Per Region"},
        {"src": "ash-heap-scenic.avif",        "alt": "Ash Heap Primal Cuts"},
    ],
}

# Meat Sweats buff (Prime Meat turn-in). EFFECT magnitudes are read GENERATIVELY
# from the SPEL EFFECTS TSV; only the MGEF -> friendly-text mapping and the
# per-tier turn-in amounts are fixed here.
_MEAT_SWEATS_TIERS = [
    ("MeatSweats_Lvl1", "5 (one stack)"),
    ("MeatSweats_Lvl2", "10 (two stacks)"),
    ("MeatSweats_Lvl3", "15 (three stacks)"),
]


def _meat_sweats_effect(mgef_edid, mag, poison=None):
    """Map a Meat Sweats MGEF + magnitude to a (sort_key, friendly_text) tuple,
    or None to skip (base food carrier / unknown effect). `poison` is an optional
    (damage, seconds) tuple describing the gas-discharge Noxious Gas sub-effect."""
    e = mgef_edid or ""
    if e == "MeatSweats_GasExplosion":
        txt = "Taking damage may result in gaseous discharge"
        if poison:
            txt += " (poisons nearby enemies for {} damage over {} seconds)".format(poison[0], poison[1])
        return (0, txt + ".")
    if e == "MeatSweats_RemoveHungerPerk":
        return (1, "Hunger does not decay (your hunger bar is paused).")
    if e == "MeatSweats_FortifyHealthFood":
        return (2, "+{} maximum HP.".format(int(round(mag))))
    if e == "MeatSweats_FortifyXPBonusFood":
        return (3, "+{}% bonus XP.".format(int(round(mag))))
    return None


def _build_prime_meat_buff(tsv_root):
    """Build the Meat Sweats turn-in buff (3 tiers) generatively from the SPEL
    HEADER + EFFECTS TSVs. Returns None if the records aren't present."""
    eff_path = newest(os.path.join(tsv_root, "SPEL_Export_*_EFFECTS.tsv"))
    hdr_path = newest(os.path.join(tsv_root, "SPEL_Export_*_HEADER.tsv"))
    if not eff_path or not hdr_path:
        return None
    full_by_edid = {}
    for r in read_tsv(hdr_path):
        full_by_edid[r.get("SPEL_EDID", "")] = r.get("SPEL_FULL", "")
    eff_by_edid = defaultdict(list)
    duration = 0
    poison = None  # (damage, seconds) for the gas-discharge Noxious Gas sub-effect
    for r in read_tsv(eff_path):
        se = r.get("SPEL_EDID", "")
        if se == "MeatSweats_PoisonSpell":
            if "DamageHealthPoison" in r.get("EFID_MGEF_EDID", ""):
                try:
                    pmag = int(round(float(r.get("EFIT_Magnitude") or 0)))
                except ValueError:
                    pmag = 0
                try:
                    pdur = int(float(r.get("EFIT_Duration") or 0))
                except ValueError:
                    pdur = 0
                if pmag and pdur:
                    poison = (pmag, pdur)
            continue
        if not se.startswith("MeatSweats_Lvl"):
            continue
        try:
            mag = float(r.get("EFIT_Magnitude") or 0)
        except ValueError:
            mag = 0.0
        try:
            d = int(float(r.get("EFIT_Duration") or 0))
        except ValueError:
            d = 0
        if d:
            duration = d
        eff_by_edid[se].append((r.get("EFID_MGEF_EDID", ""), mag))
    tiers = []
    for edid, amount in _MEAT_SWEATS_TIERS:
        rows = eff_by_edid.get(edid)
        if not rows:
            continue
        effs = [x for x in (_meat_sweats_effect(m, g, poison) for m, g in rows) if x]
        effs.sort(key=lambda x: x[0])
        tiers.append({
            "amount":     amount,
            "pipBoyName": full_by_edid.get(edid, edid),
            "effects":    [s for _, s in effs],
        })
    if not tiers:
        return None
    return {"name": "Meat Sweats", "durationSeconds": duration, "tiers": tiers}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_EXCLUDE_RE = re.compile(r"^(zzz_|CUT_|POST_|DEL_|P62_)", re.IGNORECASE)
_DRIFTER_EDID = "P62_LLS_Rewards_TheDrifter_ActivationKeyCard"

IMAGE_BASE = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/seasonal-events/"

XP_REFERENCE_LEVEL = 50
MIN_RATE_DECIMAL = 0.0001  # 0.01% as decimal

# Keyword FormIDs used to flag tradeability / unsellability on item refs.
# NonPlayerTradable [KYWD:00499F7A] → tradeable: false
# UnsellableObject  [KYWD:003D4327] → unsellable: true
_KW_NON_PLAYER_TRADABLE = "00499f7a"
_KW_UNSELLABLE_OBJECT   = "003d4327"

# Named-weapon display overrides. These weapons drop with a custom OMOD that
# changes their in-game display name (e.g. "Cursed Broadsider"), but the base
# WEAP record's FULL field only carries the generic name ("Broadsider").
# Keyed by FormID (lowercase).
_NAMED_WEAPON_OVERRIDES = {
    "000fd11b": "Cursed Broadsider",
    "00142fab": "Cursed Rolling Pin",
    "000b3293": "Cursed Sickle",
}

# ---------------------------------------------------------------------------
# Event Definitions
# ---------------------------------------------------------------------------

EVENTS = {
    "fasnacht-day-parade-all-rewards": {
        "name": "Fasnacht Day Parade",
        "eventSlug": "fasnacht-day-parade",
        "description": "Join the fun during the Fasnacht Day parade and earn a chance at a festive mask!",
        "isContainerLoot": False,
        "questFormIDs": ["0049886E"],
    },
    "halloween-scorched-all-rewards": {
        "name": "Halloween Scorched",
        "eventSlug": "halloween-scorched",
        "description": "Take down Spooky Scorched to earn Spooky Treat Bags filled with Halloween-themed rewards.",
        "isContainerLoot": True,
        "containers": [
            {"title": "Spooky Treat Bag", "lvliFormID": "0062038D"},
        ],
    },
    "holiday-scorched-all-rewards": {
        "name": "Holiday Scorched",
        "eventSlug": "holiday-scorched",
        "description": "Defeat Holiday Scorched enemies to collect Holiday Gifts containing rare plans and outfits.",
        "isContainerLoot": True,
        "containers": [
            {"title": "Large Holiday Gift (Found)",   "lvliFormID": "005DCA88"},
            {"title": "Medium Holiday Gift (Found)",  "lvliFormID": "005DCA8A"},
            {"title": "Small Holiday Gift (Found)",   "lvliFormID": "005DCA89"},
            {"title": "Large Holiday Gift (Crafted)",  "lvliFormID": "005DCA85"},
            {"title": "Medium Holiday Gift (Crafted)", "lvliFormID": "005DCA87"},
            {"title": "Small Holiday Gift (Crafted)",  "lvliFormID": "005DCA86"},
        ],
    },
    "invaders-from-beyond-all-rewards": {
        "name": "Invaders from Beyond",
        "eventSlug": "invaders-from-beyond",
        "description": "Defend against the Zetan invasion and earn unique alien-themed rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["00620F7B"],
    },
    "grahms-meat-cook-all-rewards": {
        "name": "Grahm's Meat-Cook",
        "eventSlug": "meat-week",
        "description": "Help Grahm cook up a feast at the grill during Meat Week and earn unique rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["0054B3FA"],
    },
    "primal-cuts-all-rewards": {
        "name": "Primal Cuts",
        "eventSlug": "meat-week",
        "description": "Hunt creatures across Appalachia and harvest prime cuts for Grahm during Meat Week to earn unique rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["0054B3F3"],
        # Per-region detail (difficulty / enemies / end boss / Prime Meat).
        # Confirmed by hand — the GMRW grants Prime Meat ×3/×4/×5 on one stage
        # condition with no region attached, and per-region creatures are
        # runtime/encounter-driven, so none of this is derivable from the TSVs.
        # Keys must match the regionLocations region names.
        "regionInfo": {
            "Forest":        {"difficulty": "Easy",   "enemies": ["Wolves", "Yao Guai"],                                        "boss": "Deathclaw",             "primeMeat": 3},
            "Toxic Valley":  {"difficulty": "Easy",   "enemies": ["Radtoads", "Mongrels", "Snallygasters"],                     "boss": "Grafton Monster",       "primeMeat": 3},
            "Ash Heap":      {"difficulty": "Medium", "enemies": ["Radrats", "Radscorpions", "Cave Crickets"],                  "boss": "Sheepsquatch",          "primeMeat": 4},
            "Savage Divide": {"difficulty": "Medium", "enemies": ["Yao Guai", "Honeybeasts", "Wolves"],                         "boss": "Hermit Crab",           "primeMeat": 4},
            "The Mire":      {"difficulty": "Hard",   "enemies": ["Radtoads", "Mirelurk Kings", "Gulpers"],                     "boss": "Mirelurk Queen",        "primeMeat": 5},
            "Cranberry Bog": {"difficulty": "Hard",   "enemies": ["Fog Crawlers", "Radscorpions", "Mirelurk Kings", "Insects"], "boss": "Super Mutant Behemoth", "primeMeat": 5},
        },
        # Direct ALCH reward the LVLI walk does not capture; injected into the
        # tree after the synthetic Caps node. Prime Meat qty scales by region
        # difficulty (3 / 4 / 5).
        "extraRewardNodes": [
            {
                "type": "lvli", "formid": "005527C2", "edid": "E02A_Meat_PrimeMeat",
                "label": "Prime Meat", "useAll": False, "entryRate": 100.0,
                "gmrwDropRate": 100.0, "tierLabel": None, "conditions": [],
                "items": [
                    {"name": "Prime Meat (Easy — Forest, Toxic Valley)", "formid": "005527C2", "edid": "E02A_Meat_PrimeMeat", "sig": "ALCH", "qty": 3, "dropRate": 100.0},
                    {"name": "Prime Meat (Medium — Ash Heap, Savage Divide)", "formid": "005527C2", "edid": "E02A_Meat_PrimeMeat", "sig": "ALCH", "qty": 4, "dropRate": 100.0},
                    {"name": "Prime Meat (Hard — The Mire, Cranberry Bog)", "formid": "005527C2", "edid": "E02A_Meat_PrimeMeat", "sig": "ALCH", "qty": 5, "dropRate": 100.0},
                ],
            },
        ],
    },
    "mischief-night-all-rewards": {
        "name": "Mischief Night",
        "eventSlug": "mischief-night",
        "description": "A night of mischief and mayhem at the Whitespring.",
        "isContainerLoot": False,
        "questFormIDs": ["005600A9", "0077FA14"],
    },
    "mothman-equinox-all-rewards": {
        "name": "Mothman Equinox",
        "eventSlug": "mothman-equinox",
        "description": "Participate in the Mothman Equinox event and earn unique Cultist-themed rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["006173A1"],
    },
    "the-big-bloom-all-rewards": {
        "name": "The Big Bloom",
        "eventSlug": "the-big-bloom",
        "description": "Investigate the strange blooming phenomenon and earn unique rewards.",
        "isContainerLoot": False,
        "questFormIDs": ["0079AA0F"],
    },
    "treasure-hunter-all-rewards": {
        "name": "Hunt for the Treasure Hunter",
        "eventSlug": "hunt-for-the-treasure-hunter",
        "description": "Hunt down Mole Miner Treasure Hunters and open their pails for rare rewards.",
        "isContainerLoot": True,
        "containers": [
            {"title": "Ornate Mole Miner Pail (Found)",   "lvliFormID": "005D805A"},
            {"title": "Regular Mole Miner Pail (Found)",   "lvliFormID": "005D8054"},
            {"title": "Dusty Mole Miner Pail (Found)",     "lvliFormID": "005D8056"},
            {"title": "Ornate Mole Miner Pail (Crafted)",  "lvliFormID": "005D8053"},
            {"title": "Regular Mole Miner Pail (Crafted)", "lvliFormID": "005D8059"},
            {"title": "Dusty Mole Miner Pail (Crafted)",   "lvliFormID": "005D8055"},
        ],
        # After the 6 pails are merged into a single tier-aware node, split
        # the items into per-category nodes. Each sub-LVLI FormID below maps
        # to a category. The renderer renders each non-unique category as its
        # own collapsible expand (Currency, Junk & Scrap, Goodies, Contextual
        # Ammo), and the unique categories (Rare + Player Titles) stay in the
        # Unique Event Rewards checklist.
        "splitByCategory": {
            "categories": [
                {"key": "currency",        "label": "Currency",        "isUnique": False,
                 "subLvliFormIDs": ["005D8058", "0075062D"]},
                {"key": "junk-scrap",      "label": "Junk & Scrap",    "isUnique": False,
                 "subLvliFormIDs": ["005A7544"]},
                {"key": "goodies",         "label": "Goodies",         "isUnique": False,
                 "subLvliFormIDs": ["0059CACE"]},
                {"key": "contextual-ammo", "label": "Contextual Ammo", "isUnique": False,
                 "subLvliFormIDs": ["006A2511"]},
                {"key": "unique",          "label": "Hunt for the Treasure Hunter Rewards",
                 "isUnique": True,
                 "subLvliFormIDs": ["005D8057", "007B2464"]},
            ],
        },
    },
    "night-of-the-radtoads-all-rewards": {
        "name": "Night of the Radtoads",
        "eventSlug": "night-of-the-radtoads",
        "description": "Survive the Night of the Radtoads and earn unique rewards.",
        "isContainerLoot": False,
        "questFormIDs": [],
    },
}

# ---------------------------------------------------------------------------
# Party Crasher / Invaders detection (generative from QUEST TSV)
# ---------------------------------------------------------------------------

def _load_quest_index(tsv_root):
    """Load QUEST TSV and index rows by FormID for fast lookup."""
    path = newest(os.path.join(tsv_root, "QUEST_Export_*.tsv"))
    if not path:
        print("[build_seasonal_events] WARNING: No QUEST_Export TSV found")
        return {}
    rows = read_tsv(path)
    print("[build_seasonal_events] Loaded {} QUEST rows from {}".format(
        len(rows), os.path.basename(path)))
    idx = {}
    for r in rows:
        fid = pick(r, "QUEST_FormID", "FormID")
        if fid:
            idx[fid.upper()] = r
    return idx


def _norm_event_name(s):
    """Normalise an event name for region/location lookup (mirrors the
    activities build's norm_name): lowercase, drop bracketed bits, keep
    only alphanumerics."""
    s = (s or "").lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"<.*?>", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.strip()


def _load_region_location_tsv(tsv_root):
    """Read events_region_location.tsv → { norm_name(event): [{region, location}] }.

    Same source and shape used by the activities build so seasonal pages
    rendered in the activity layout show matching Region/Location header
    lines. Columns: 'Activity / Event Name', 'Region', 'Location / LCTN'.
    """
    from collections import defaultdict
    out = defaultdict(list)
    path = os.path.join(tsv_root, "events_region_location.tsv")
    try:
        rows = read_tsv(path)
    except Exception as e:
        print("[build_seasonal_events] WARNING: could not read {}: {}".format(path, e))
        return {}
    for row in rows:
        name     = str(row.get("Activity / Event Name") or "").strip()
        region   = str(row.get("Region") or "").strip()
        location = str(row.get("Location / LCTN") or "").strip()
        if not name:
            continue
        bare = re.sub(r"^(enclave\s+)?activity:\s*", "", name, flags=re.IGNORECASE).strip()
        bare = re.sub(r"^event:\s*", "", bare, flags=re.IGNORECASE).strip()
        key = _norm_event_name(bare)
        if key:
            out[key].append({"region": region, "location": location})
    print("[build_seasonal_events] Loaded region/location rows for {} events".format(len(out)))
    return dict(out)


def _humanize_party_crasher_name(raw):
    """Convert a PartyCrasher NPC EDID into a readable name."""
    s = (raw or "").strip()
    if not s:
        return "Party Crasher"
    edid = s.split(":", 1)[1] if ":" in s else s
    edid = re.sub(r"^Lvl", "", edid)
    edid = re.sub(r"_?PartyCrasher$", "", edid)
    edid = re.sub(r"_", " ", edid).strip()
    edid = re.sub(r"(?<!^)(?=[A-Z])", " ", edid).strip()
    return edid if edid else "Party Crasher"


def _detect_event_flags(quest_fids, quest_index, globs):
    """Detect party crashers and invaders flag from QUEST TSV data.

    Returns dict with keys:
      partyCrashers  - str or None
      invadersEvent  - str or None
    """
    result = {"partyCrashers": None, "invadersEvent": None}
    if not quest_fids:
        return result

    pc_lines = []
    has_invaders = False

    for fid in quest_fids:
        q = quest_index.get(fid.upper())
        if not q:
            continue

        # Invaders flag
        if str(q.get("InvadersTakeOver") or "0").strip() == "1":
            has_invaders = True

        # Party Crashers - read PartyCrasherCount and iterate NPC/GLOB pairs
        pc_count = int(q.get("PartyCrasherCount") or 0)
        for i in range(pc_count):
            npc_raw  = q.get("PartyCrasher_NPC_{}".format(i))
            glob_raw = q.get("PartyCrasher_GLOB_{}".format(i))
            if not npc_raw or not glob_raw:
                continue
            glob_fid = glob_raw.split(":")[0] if ":" in str(glob_raw) else str(glob_raw)
            spawn_pct = globs.value(glob_fid)
            name = _humanize_party_crasher_name(npc_raw)
            if spawn_pct is not None:
                pct_val = round(max(0.0, spawn_pct) * 100, 6)
                pc_lines.append("{} \u2014 {}% chance to spawn at the end of the event.".format(
                    name, pct_val))
            else:
                pc_lines.append("{} can spawn at the end of the event.".format(name))

    if pc_lines:
        result["partyCrashers"] = " ".join(pc_lines)
    if has_invaders:
        result["invadersEvent"] = "Yes."

    return result


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def is_excluded(edid):
    if not edid:
        return False
    if _EXCLUDE_RE.match(edid):
        return True
    if _DRIFTER_EDID.lower() in edid.lower():
        return True
    return False


def is_trackable(name):
    lower = (name or "").lower()
    return lower.startswith("plan:") or lower.startswith("recipe:")


def slugify_item(name):
    s = (name or "").lower()
    s = re.sub(r"^(plan|recipe):\s*", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def build_image_url(event_slug, item_name):
    return IMAGE_BASE + event_slug + "/" + slugify_item(item_name) + ".avif"


def parse_ref(ref):
    parts = (ref or "").split(":")
    fid  = parts[0].strip() if len(parts) > 0 else ""
    edid = parts[1].strip() if len(parts) > 1 else ""
    sig  = parts[2].strip().upper() if len(parts) > 2 else ""
    return fid, edid, sig


# ---------------------------------------------------------------------------
# Label cleanup helpers
# ---------------------------------------------------------------------------

_TIER_SUFFIX_RE = re.compile(
    r"_+(Tier\d+|Best|Good|Bad|Bronze|Silver|Gold|"
    r"Easy|Medium|Hard|Common|Uncommon|Rare|Epic|Legendary|"
    r"Small|Large)$",
    re.IGNORECASE,
)

_LVLI_LABEL_OVERRIDES = {
    "ra_ll_rewards_publicevents":         "Public Event Rewards",
    "ra_ll_rewards_activities":           "Activity Rewards",
    "fishing_ll_rewards_improvedbait":    "Improved Bait",
    "ll_questreward_goldbullion":         "Gold Bullion",
    "legendaryitems_special_allitems":    "Legendary Items",
}

# Generic shared LVLIs that appear across many events (caps, gold bullion,
# public event rewards, etc.). These STAY in the Event Rewards group even
# though their EDID may match the Quest_Rewards pattern. Substring matched
# against lowercase EDID.
_GENERIC_SHARED_LVLI_SUBSTRINGS = (
    "ll_questreward_goldbullion",
    "ra_ll_rewards_publicevents",
    "ra_ll_rewards_activities",
    "fishing_ll_rewards_improvedbait",
)

# Pattern that flags an LVLI as the event's own "unique" reward pool.
# Matches: Quest_Rewards, Quest_Reward, QuestRewards, QuestReward,
#          Event_Rewards, Event_Reward, EventRewards, EventReward.
_UNIQUE_LVLI_RE = re.compile(r"(?:Quest|Event)_?Reward(s)?", re.IGNORECASE)


def _is_unique_event_lvli(edid):
    """
    Return True when an LVLI EDID looks like the event-specific Quest/Event
    Rewards pool (whose items belong in Unique Event Rewards), False when it's
    a generic shared LVLI like Gold Bullion or Public Event Rewards.
    """
    if not edid:
        return False
    el = edid.lower()
    for shared in _GENERIC_SHARED_LVLI_SUBSTRINGS:
        if shared in el:
            return False
    return bool(_UNIQUE_LVLI_RE.search(edid))


def _strip_event_prefix(edid):
    if not edid:
        return edid
    edid = re.sub(r"^E\d+[A-Z]?_", "", edid, flags=re.IGNORECASE)
    edid = re.sub(r"^[A-Z]{2,4}\d*_", "", edid)
    return edid


def _clean_pool_label(edid):
    if not edid:
        return ""
    tl = edid.lower()
    for substr, label in _LVLI_LABEL_OVERRIDES.items():
        if substr in tl:
            return label

    t = _strip_event_prefix(edid)

    glue = ["RA_LL", "RA_LLS", "LLS", "LL", "QuestReward", "Quest_Reward",
            "Quest_Rewards", "QuestRewards", "Event_Reward", "Event_Rewards",
            "EventReward", "EventRewards", "Quest", "Rewards", "Reward"]
    for token in glue:
        t = re.sub(r"(?:^|_)" + token + r"(?:_|$)", "_", t, flags=re.IGNORECASE)
    t = re.sub(r"^_+|_+$", "", t)
    t = re.sub(r"_+", "_", t)

    t = t.replace("_", " ")
    t = re.sub(r"([a-z])([A-Z])", r"\1 \2", t)
    t = re.sub(r"([A-Za-z])(\d)", r"\1 \2", t)
    t = re.sub(r"(\d)([A-Za-z])", r"\1 \2", t)
    t = re.sub(r"\s+", " ", t).strip()

    if not t:
        return _strip_event_prefix(edid).replace("_", " ").strip()

    words = t.split()
    out = " ".join(w.capitalize() if w.islower() else w for w in words)
    out = re.sub(r"\bTier\s*0?(\d+)\b", r"Tier \1", out, flags=re.IGNORECASE)
    return out.strip()


def _extract_tier_suffix(edid):
    if not edid:
        return edid, None
    m = _TIER_SUFFIX_RE.search(edid)
    if not m:
        return edid, None
    stem = edid[: m.start()].rstrip("_")
    tier = m.group(1)
    tier = re.sub(r"Tier\s*0?(\d+)", r"Tier \1", tier, flags=re.IGNORECASE)
    return stem, tier


def _clean_xp_caps_label(glob_ref, fallback="Event Completion"):
    if not glob_ref:
        return fallback
    edid = glob_ref.split(":")[1] if ":" in glob_ref else glob_ref
    if not edid:
        return fallback
    edid = re.sub(r"^(XP|Caps)_?", "", edid)
    edid = _strip_event_prefix(edid)
    edid = re.sub(r"_?Reward(s)?$", "", edid, flags=re.IGNORECASE)
    edid = re.sub(r"_+", "_", edid).strip("_")
    if not edid:
        return fallback
    parts = re.sub(r"_", " ", edid).strip()
    parts = re.sub(r"([a-z])([A-Z])", r"\1 \2", parts)
    parts = re.sub(r"([A-Za-z])(\d)", r"\1 \2", parts)
    parts = re.sub(r"(\d)([A-Za-z])", r"\1 \2", parts)
    parts = re.sub(r"\s+", " ", parts).strip()
    if not parts:
        return fallback
    words = parts.split()
    return " ".join(w.capitalize() if w.islower() else w for w in words)


# ---------------------------------------------------------------------------
# GMRW field resolvers
# ---------------------------------------------------------------------------

def _resolve_xp_value(xpct_ref, xp_glob_ref, data):
    """Resolve XP value at level XP_REFERENCE_LEVEL.
    Returns (xp_int, scales_bool, fid_or_None, label_or_None) or (None, ...)."""
    if xpct_ref:
        fid = xpct_ref.split(":")[0]
        y = data.curvs.interpolate(fid, XP_REFERENCE_LEVEL)
        if y is not None:
            label = _clean_xp_caps_label(xp_glob_ref) if xp_glob_ref else "Event Completion"
            return int(round(y)), True, fid, label
    if xp_glob_ref:
        fid = xp_glob_ref.split(":")[0]
        edid = xp_glob_ref.split(":")[1] if ":" in xp_glob_ref else ""
        if "IgnoreMe" in edid or "XPNone" in edid:
            return None, False, None, None
        v = data.globs.value(fid)
        if v is not None:
            return int(round(v)), False, fid, _clean_xp_caps_label(xp_glob_ref)
    return None, False, None, None


def _resolve_caps_value(caps_glob_ref, data):
    if not caps_glob_ref:
        return None, None, None
    fid = caps_glob_ref.split(":")[0]
    edid = caps_glob_ref.split(":")[1] if ":" in caps_glob_ref else ""
    if "IgnoreMe" in edid:
        return None, None, None
    v = data.globs.value(fid)
    if v is None:
        return None, None, None
    return int(round(v)), fid, _clean_xp_caps_label(caps_glob_ref)


def _legendary_rank_label(qrlr_str, qrri_str):
    qrlr = (qrlr_str or "").strip()
    qrri = (qrri_str or "").strip()
    min_rank = None
    max_rank = None
    try:
        if qrlr:
            min_rank = int(qrlr)
        if qrri:
            max_rank = int(qrri)
    except (ValueError, TypeError):
        pass
    if min_rank is None and max_rank is None:
        return "Legendary Items", None, None
    if min_rank is not None and max_rank is not None and min_rank != max_rank:
        return "{}–{}★ Legendary Items".format(min_rank, max_rank), min_rank, max_rank
    rank = max_rank if max_rank is not None else min_rank
    if rank == 1:
        return "1★ Legendary Items", rank, rank
    if rank is not None:
        return "{}★ Legendary Items".format(rank), rank, rank
    return "Legendary Items", None, None


# ---------------------------------------------------------------------------
# GMRW row processing
# ---------------------------------------------------------------------------

def _gmrw_rows_for_quests(quest_fids, gmrw_rows):
    if not quest_fids:
        return []
    qset = {q.upper() for q in quest_fids}
    out = []
    for row in gmrw_rows:
        parent_raw = pick(row, "ParentQuestLink", "Parent Quest", default="")
        parent_fid = parent_raw.split(":")[0].strip().upper() if parent_raw else ""
        if parent_fid in qset:
            out.append(row)
    return out


def _gmrw_extract_xp_caps_per_ri(gmrw_rows, data):
    seen = set()
    xp_entries = []
    caps_entries = []
    ri_to_tier = {}

    for row in gmrw_rows:
        gmrw_fid = (row.get("FormID") or "").strip()
        ri_raw = row.get("RewardIndex") or "0"
        try:
            ri = int(ri_raw)
        except (ValueError, TypeError):
            ri = 0
        key = (gmrw_fid, ri)
        if key in seen:
            continue
        seen.add(key)

        edid = (row.get("EDID") or "").strip()
        if is_excluded(edid):
            continue

        xpct_ref    = (row.get("XPCT_XPCurveTable") or "").strip()
        xp_glob_ref = (row.get("NAM7_XPGlobal") or "").strip()
        caps_ref    = (row.get("NAM8_CapsGlobal") or "").strip()

        cond_text = (row.get("Conditions") or "").strip()
        tier_fn   = (row.get("TierConditionFunc") or "").strip()
        is_conditional = bool(cond_text) or bool(tier_fn)

        xp_val, xp_scales, xp_fid, xp_label = _resolve_xp_value(xpct_ref, xp_glob_ref, data)
        if xp_val and xp_val > 0:
            label = xp_label or "Event Completion"
            xp_entries.append({
                "label":           label,
                "xp":              xp_val,
                "xpFormID":        xp_fid,
                "scalesWithLevel": xp_scales,
                "condition":       is_conditional,
            })
            ri_to_tier.setdefault(ri, label)

        caps_val, caps_fid, caps_label = _resolve_caps_value(caps_ref, data)
        if caps_val and caps_val > 0:
            label = caps_label or "Event Completion"
            caps_entries.append({
                "label":     label,
                "caps":      caps_val,
                "capsFormID": caps_fid,
                "condition": is_conditional,
            })
            ri_to_tier.setdefault(ri, label)

    return xp_entries, caps_entries, ri_to_tier


def _gmrw_iter_lvli_sources(gmrw_rows):
    """Yield (title, lvli_fid, lvli_edid, ri, parent_quest_fid)."""
    seen = set()
    for row in gmrw_rows:
        edid = pick(row, "EDID", "GMRW_EDID", default="")
        if is_excluded(edid):
            continue

        rewarded = pick(row, "RewardedItem", default="")
        if not rewarded:
            continue
        fid, ref_edid, sig = parse_ref(rewarded)
        if sig != "LVLI" or not fid:
            continue
        if is_excluded(ref_edid):
            continue
        if fid in seen:
            continue
        seen.add(fid)

        ri_raw = row.get("RewardIndex") or "0"
        try:
            ri = int(ri_raw)
        except (ValueError, TypeError):
            ri = 0

        parent_fid = pick(row, "ParentQuestLink", default="").split(":")[0].strip()

        title = _clean_pool_label(ref_edid) if ref_edid else _clean_pool_label(edid)
        yield title, fid, ref_edid, ri, parent_fid


def _gmrw_iter_legendary_sources(gmrw_rows):
    """Yield (title, lvli_fid, lvli_edid, ri) for each QRLI legendary list."""
    seen = set()
    for row in gmrw_rows:
        edid = pick(row, "EDID", "GMRW_EDID", default="")
        if is_excluded(edid):
            continue
        qrli = pick(row, "QRLI_LegendaryItemRewardList", default="")
        if not qrli:
            continue
        fid, ref_edid, sig = parse_ref(qrli)
        if not fid or is_excluded(ref_edid):
            continue
        if fid in seen:
            continue
        seen.add(fid)

        ri_raw = row.get("RewardIndex") or "0"
        try:
            ri = int(ri_raw)
        except (ValueError, TypeError):
            ri = 0

        rank_label, _min, _max = _legendary_rank_label(
            row.get("QRLR_LegendaryItemRewardRank"),
            row.get("QRRI_LegendaryRankRandom"),
        )
        yield rank_label, fid, ref_edid, ri


# ---------------------------------------------------------------------------
# Tradeable / unsellable keyword index (loaded once, cached)
# ---------------------------------------------------------------------------

_KYWD_TRADE_CACHE = {"loaded": False, "non_tradable": set(), "unsellable": set()}


def _load_kywd_flags():
    """Load NonPlayerTradable / UnsellableObject keyword refs once. Returns
    (non_tradable_fids, unsellable_fids). FormIDs are lowercase."""
    if _KYWD_TRADE_CACHE["loaded"]:
        return _KYWD_TRADE_CACHE["non_tradable"], _KYWD_TRADE_CACHE["unsellable"]
    non_tradable = set()
    unsellable = set()
    try:
        path = newest(str(_REPO_ROOT / "tsv" / "KYWD_Export_*_Refs.tsv"))
        for r in read_tsv(path):
            kw_fid = (r.get("KeywordFormID") or "").strip().lower()
            ref_sig = (r.get("RefSignature") or "").strip().upper()
            ref_fid = (r.get("RefFormID") or "").strip().lower()
            if not ref_fid:
                continue
            if ref_sig in ("ARMO", "BOOK"):
                if kw_fid == _KW_NON_PLAYER_TRADABLE:
                    non_tradable.add(ref_fid)
                elif kw_fid == _KW_UNSELLABLE_OBJECT:
                    unsellable.add(ref_fid)
    except (FileNotFoundError, Exception) as e:
        print("  [WARN] could not load KYWD_Refs: {}".format(e))
    _KYWD_TRADE_CACHE["non_tradable"] = non_tradable
    _KYWD_TRADE_CACHE["unsellable"]   = unsellable
    _KYWD_TRADE_CACHE["loaded"] = True
    return non_tradable, unsellable


# ---------------------------------------------------------------------------
# Condition simplification (minimal — handles common xEdit condition strings)
# ---------------------------------------------------------------------------

def _humanize_cobj_edid(edid):
    """Convert a COBJ EDID into a clean human-readable plan/recipe name.
    Handles seasonal-events naming patterns:
      SSE_co_Headwear_FlowerCrown_CarnalWeeper   → 'Flower Crown - Carnal Weeper'
      workshop_co_Tinkers_SSE_Tier2_HybridFlower → 'Tinkers Hybrid Flower'
      PlayerTitle_co_CondProxy_Suffix_Gardener   → 'Player Title: Gardener'
      Workshop_co_Condproxy_FloorDecor_BigBloomStein → 'Big Bloom Stein'
    Conservative — falls back to a CamelCase-spaced version of the trailing
    segment when no known pattern matches."""
    if not edid:
        return ""
    s = str(edid).strip()
    if not s:
        return ""

    # Player Title / Camp Title → "Player Title: <Name>"
    m = re.search(r'(Player|CAMP)Title_co_CondProxy_(?:(?:Prefix|Suffix|Both)_)+(\w+)',
                  s, re.IGNORECASE)
    if m:
        title_type = "Camp" if m.group(1).upper() == "CAMP" else "Player"
        name = re.sub(r"([a-z])([A-Z])", r"\1 \2", m.group(2))
        return "{} Title: {}".format(title_type, name.strip())

    # Strip leading event/quest/workshop/ATX prefixes. Some EDIDs nest these
    # (workshop_co_Tinkers_SSE_…) so we run the event-prefix regex twice —
    # once on the raw EDID, then again after the wrapper prefixes are removed.
    _EVENT_PREFIX_RE = re.compile(
        r"^(SSE|MTNZ|MTNS|SFS|CBZ|EN|FS|TWZ|RD|HTO|XPD|MQ|MTRZ|Storm_E)\w*?_"
    )
    s = _EVENT_PREFIX_RE.sub("", s)
    s = re.sub(r"^ATX_", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^[Ww]orkshop_", "", s)
    s = re.sub(r"^co_(?:CondProxy_)?", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^Condproxy_", "", s, flags=re.IGNORECASE)

    # Common category prefixes — strip or rewrite for readability
    s = re.sub(r"^Headwear_", "", s)
    s = re.sub(r"^FloorDecor_", "", s)
    s = re.sub(r"^Tinkers_", "", s)
    s = re.sub(r"^Mod_", "", s, flags=re.IGNORECASE)

    # Second pass — covers nested SSE_/MTNZ_ etc. after Tinkers_ was stripped
    s = _EVENT_PREFIX_RE.sub("", s)

    # Split underscores; the last two parts (or just last) are usually the
    # meaningful name.
    parts = [p for p in s.split("_") if p]
    if not parts:
        return ""
    # If the trailing chunk is a tier ("Tier1", "Tier2"), join with the
    # final name segment.
    if len(parts) >= 2 and re.match(r"^Tier\d+$", parts[0], re.IGNORECASE):
        parts = parts[1:]

    # Join remaining parts with " - " between major segments, then split
    # CamelCase within each segment.
    if len(parts) == 1:
        out = re.sub(r"([a-z])([A-Z])", r"\1 \2", parts[0])
    else:
        # Treat first part as category (e.g. "FlowerCrown") and last as the
        # specific item (e.g. "CarnalWeeper"); join with " - ".
        segs = [re.sub(r"([a-z])([A-Z])", r"\1 \2", p) for p in parts]
        out = " - ".join(segs)

    return out.strip()


def _simplify_condition_basic(cond_str):
    """Convert a raw xEdit condition string into a friendly display string.
    Returns "" for conditions that should be hidden (internal toggles, etc.).
    Mirrors the most common cases from build_activities_rewards_json.py's
    simplify_condition; falls through to "" for unrecognised conditions to
    avoid showing raw EDID noise to players."""
    s = (cond_str or "").strip()
    if not s:
        return ""

    # Already-translated strings — pass through unchanged
    if s.startswith(("Requires ", "Won’t drop", "Won't drop",
                     "Bethesda toggle", "Region: ", "Toggle: ",
                     "Tradeable", "Not Tradeable",
                     "Stops dropping", "Continues to drop",
                     "Cannot be sold")):
        return s

    # GetQuestCompleted → "Requires the <Quest Name> quest to be completed"
    if "GetQuestCompleted" in s:
        m = re.search(r'"([^"]+)"\s*\[QUST:', s)
        if m:
            return "Requires the {} quest to be completed".format(m.group(1))
        return ""

    # GetLevel → "Requires player level X+"
    if "GetLevel" in s:
        m = re.search(r'(\d+)\.0+\s*$', s)
        if m:
            return "Requires player level {}+".format(m.group(1))
        return ""

    # GetIsPlayerGhoul → character race restriction
    if "GetIsPlayerGhoul" in s:
        if re.search(r'0\.0+\s*$', s):
            return "Human character only"
        return "Ghoul character only"

    # HasLearnedRecipe → require/avoid recipe learned. Combines the CTDA
    # operator byte (first 8-digit hex token) with the comparison value
    # (trailing float). Per build_titles_json.py convention:
    #   flag 10000000 = AND (positive — the comparison must be TRUE)
    #   flag 00000000 = OR / exclusion gate (effectively NOT-equal)
    # XOR of (op==Equal, value==1.0) → "required" vs "exclusion":
    #   - (Eq, 1.0)  → required learned
    #   - (Eq, 0.0)  → required NOT learned  → exclusion
    #   - (NEq, 1.0) → not equal to 1.0       → exclusion
    #   - (NEq, 0.0) → not equal to 0.0       → required learned
    # Also extracts the COBJ EDID and humanises it so each condition
    # names its own recipe — distinct strings, no dedup collapse.
    if "HasLearnedRecipe" in s:
        op_match = re.search(r'(\d{8})\s+(\d+\.\d+)\s*$', s)
        if not op_match:
            return ""
        op_code = op_match.group(1)
        comp_val = float(op_match.group(2))
        is_equal_op = op_code.startswith("1")
        positive = (is_equal_op and comp_val >= 1.0) or \
                   (not is_equal_op and comp_val < 1.0)

        cobj_edid = ""
        edid_match = re.search(r'HasLearnedRecipe\(\s*(?:[^,]*,\s*){2}(\w+)\s*\[COBJ:', s)
        if edid_match:
            cobj_edid = edid_match.group(1)
        human_name = _humanize_cobj_edid(cobj_edid) if cobj_edid else ""

        # Avoid "Plan: Player Title: X" doubling — when the humanised name
        # already carries a Player/Camp Title prefix, drop the extra "Plan:".
        is_title = bool(re.match(r"^(Player|Camp) Title:", human_name))
        if positive:
            if human_name:
                if is_title:
                    return "Requires {} to be learned".format(human_name)
                return "Requires Plan: {} to be learned".format(human_name)
            return "Requires the base plan to be learned"
        else:
            if human_name:
                if is_title:
                    return "Won’t drop if you’ve already earned {}".format(human_name)
                return "Won’t drop if you’ve already learned Plan: {}".format(human_name)
            return "Won’t drop if you’ve already learned this plan"

    # GetInCurrentLocation → Region: <Name>
    if "GetInCurrentLocation" in s:
        m = re.search(r'"([^"]+)"\s*\[LCTN:', s)
        if m:
            return "Region: {}".format(m.group(1))
        return ""

    # Hide internal-only conditions
    HIDE_PREFIXES = (
        "GetRandomPercent", "HasEntitlement", "IsActivePlayer",
        "GetVMQuestVariable", "GetGlobalValue", "GetItemCount",
        "GetValue", "Subject.", "GetPublicEventHasMutation",
        "PlayerHasQuest", "GetNumTimesCompletedQuest",
        "GetStageDoneUniqueQuest", "GetStageDoneCurrentInstance",
    )
    for p in HIDE_PREFIXES:
        if p in s:
            return ""

    # Unknown — hide rather than show raw EDID noise
    return ""


def _simplify_conditions(conditions):
    out = []
    for c in (conditions or []):
        sub_conds = re.split(r'","?|",', c) if '","' in c or '",' in c else [c]
        for sc in sub_conds:
            sc = sc.strip().strip('"')
            simplified = _simplify_condition_basic(sc)
            if simplified and simplified not in out:
                out.append(simplified)
    return out


# ---------------------------------------------------------------------------
# Default Rewards / Legendary Module / Improved Bait split (May 2026 revision)
# ---------------------------------------------------------------------------
# Most events' unique Quest_Rewards LVLI contains a `_Default` child plus
# always-given siblings (LegendaryModule, ImprovedBait) alongside the event-
# specific pools (Headwear, Recipes, etc.). The split happens for display
# granularity:
#   - `_Default` children are unique event rewards (per-event "pick one"
#     pool: plans, recipes, titles, steins). They get cracked open and
#     keyword-grouped, then flagged isUniqueReward so the renderer routes
#     them to Unique Event Rewards.
#   - `LegendaryModule` and `Fishing_LL_Rewards_ImprovedBait` are deterministic
#     always-given rewards. They stay in Default Event Rewards (no
#     isUniqueReward flag).
#   - Everything else in the parent Quest_Rewards LVLI (the big common/rare
#     pool) is the remaining unique node — also flagged isUniqueReward.

_DEFAULT_CHILD_PATTERN = re.compile(r"_quest_?rewards?_default\b", re.IGNORECASE)
_LEGMODULE_CHILD_PATTERN = re.compile(r"legendarymodule", re.IGNORECASE)
_FISHING_BAIT_PATTERN = re.compile(r"fishing.*bait|improvedbait", re.IGNORECASE)
_BASE_FLOWERS_CHILD_PATTERN = re.compile(r"baseflowers", re.IGNORECASE)


def _build_flat_lvli_tiers(lvli_fid, data, resolver):
    """Walk a flat leveled list's entries directly (no recursion into
    sub-LVLIs) and return a per-FormID tiers map:

        {formid: [{"qty": int, "rate": float}, ...]}

    Used to enrich items whose underlying LVLI has multiple entries pointing
    at the same FormID with different qtys (e.g. Improved Bait ×1/×2/×3,
    Legendary Module ×3/×2/×1 with First-Match thresholds, Base Flowers
    ×5..×10 at uniform pick-one rates).

    Respects the list's LVLF flags:
      - First Match (bit 6) with GetRandomPercent thresholds → cascading
        thresholds (drop-rate-engine §3f).
      - Otherwise → uniform pick-one (1/N each). Per drop-rate-engine §3e
        this is correct for the three cases this helper targets; ChanceNone
        and Use-All waterfall logic are intentionally not implemented here
        because the targeted lists don't use them.
    """
    flags = data.lvli.flags_for(lvli_fid)
    entries = list(data.lvli.entries_by_list.get(lvli_fid, []))
    if not entries:
        return {}

    rows = []
    for e in entries:
        idx = e.get("EntryIndex")
        if idx is None:
            continue
        math = data.lvli.math_by_entry.get((lvli_fid, idx))
        if not math:
            continue
        qty = resolver._entry_qty(e)
        conds = resolver._entry_conditions(e)
        sub_lvli = (math.get("SubLVLI_FormID") or "").strip()
        # Resolve the leaf FormID for this entry. Direct-ref entries take
        # FormID from LVLO_Reference. Sub-LVLI entries (e.g. Legendary
        # Module 1-3 wrapping LegendaryModule_Single) dive into the sub
        # one level to pick up the leaf item. Multi-leaf sub-LVLIs aren't
        # expected here for the targeted lists; the first leaf FormID is
        # used as the row key.
        if sub_lvli:
            try:
                sub_items = resolver.resolve_deep(sub_lvli)
            except Exception:
                sub_items = []
            if not sub_items:
                continue
            fid = (sub_items[0].get("formid") or "").strip()
            if not fid:
                continue
        else:
            ref = (e.get("LVLO_Reference") or "").strip()
            if ":" not in ref:
                continue
            fid = ref.split(":")[0]
        rows.append({"fid": fid, "qty": qty, "conds": conds})

    if not rows:
        return {}

    rates = [0.0] * len(rows)
    is_first_match = flags["first_match"]

    if is_first_match:
        # Cascading GetRandomPercent thresholds (drop-rate-engine §3f).
        thresholds = [resolver.extract_grp_threshold(r["conds"]) for r in rows]
        if any(t is not None for t in thresholds):
            prev = 0.0
            for i, t in enumerate(thresholds):
                if t is not None:
                    rates[i] = max((t - prev) / 100.0, 0.0)
                    prev = t
                else:
                    rates[i] = max((100.0 - prev) / 100.0, 0.0)
        else:
            for i in range(len(rows)):
                rates[i] = 1.0 / len(rows)
    else:
        # Pick-one or Use-All without ChanceNone → uniform 1/N per entry.
        n = len(rows)
        for i in range(n):
            rates[i] = 1.0 / n

    tiers_by_fid = {}
    for i, r in enumerate(rows):
        tiers_by_fid.setdefault(r["fid"], []).append({
            "qty":  r["qty"],
            "rate": round(rates[i] * 100.0, 6),
        })
    return tiers_by_fid


def _apply_tiers_to_items(items, tiers_by_fid):
    """For every item in `items` whose formid has tiers in `tiers_by_fid`,
    replace `item["tiers"]` with the multi-row breakdown. Updates the
    top-level qty/dropRate to the first tier (renderer fallback path)."""
    if not items or not tiers_by_fid:
        return
    for it in items:
        fid = (it.get("formid") or "").strip()
        tiers = tiers_by_fid.get(fid)
        if not tiers or len(tiers) <= 1:
            continue
        it["tiers"] = [{"qty": t["qty"], "rate": t["rate"]} for t in tiers]
        # Sync the top-level qty/dropRate so the rendering fallback (no
        # tiers array) still makes sense if the renderer ever ignores tiers.
        first = tiers[0]
        it["qty"] = first["qty"]
        it["dropRate"] = first["rate"]

# Keyword patterns used to bucket items from the `_Default` LVLI into grouped
# sub-nodes. First match wins; items with no match become standalone nodes.
_DEFAULT_GROUPING = [
    (re.compile(r"stein",         re.IGNORECASE), "Steins"),
    (re.compile(r"playertitle",   re.IGNORECASE), "Player Titles"),
    (re.compile(r"banner|flag",   re.IGNORECASE), "Banners"),
    (re.compile(r"lantern",       re.IGNORECASE), "Lanterns"),
]


def _match_default_group(edid, name):
    """Return a group label (e.g. "Steins") if EDID/name matches a known
    grouping keyword, else None."""
    text = "{} {}".format(edid or "", name or "")
    for pat, label in _DEFAULT_GROUPING:
        if pat.search(text):
            return label
    return None


def _walk_direct_sub_lvlis(parent_lvli_fid, data):
    """Yield (sub_fid, sub_edid) for each direct child LVLI entry of the
    parent (skips entries that aren't sub-LVLI references)."""
    for entry in data.lvli.entries_by_list.get(parent_lvli_fid, []):
        idx = entry.get("EntryIndex")
        if idx is None:
            continue
        math = data.lvli.math_by_entry.get((parent_lvli_fid, idx))
        if not math:
            continue
        sub_fid = (math.get("SubLVLI_FormID") or "").strip()
        if not sub_fid:
            continue
        sub_edid = data.lvli.edid_for(sub_fid) or ""
        yield sub_fid, sub_edid


def _make_split_subnode(label, items, parent_node):
    """Build a tree node carrying the given items under the given label,
    inheriting the parent unique LVLI's metadata."""
    return {
        "type":         "lvli",
        "formid":       parent_node.get("formid", ""),
        "edid":         parent_node.get("edid", ""),
        "label":        label,
        "useAll":       parent_node.get("useAll", False),
        "entryRate":    parent_node.get("entryRate", 100.0),
        "gmrwDropRate": parent_node.get("gmrwDropRate", 100.0),
        "tierLabel":    parent_node.get("tierLabel"),
        "conditions":   list(parent_node.get("conditions") or []),
        "items":        items,
    }


def _split_unique_node(parent_lvli_fid, unique_node, data, resolver):
    """Pull `_Default` and `LegendaryModule` child LVLI items out of the
    parent unique LVLI's flat resolution.

    Returns (event_reward_nodes, remaining_unique_node):
      event_reward_nodes - extra tree nodes split out for display granularity.
                           Default-derived nodes carry isUniqueReward=True and
                           render under Unique Event Rewards; the Legendary
                           Module node has no flag and renders under Default
                           Event Rewards.
      remaining_unique_node - parent node with default/legmodule items
                              filtered out, or None if nothing left. The
                              caller flags this as isUniqueReward=True.
    """
    if not unique_node or not unique_node.get("items"):
        return [], unique_node

    default_fids = set()
    legmodule_fids = set()
    bait_fids = set()
    bait_sub_fid = None
    # Tier-enrichment sources. Each maps FormID → list of {qty, rate}; the
    # lead item per FormID picks these up so the renderer shows one row per
    # (qty, rate) tier instead of collapsing to a single row.
    legmodule_tiers = {}
    bait_tiers = {}
    base_flower_tiers = {}

    for sub_fid, sub_edid in _walk_direct_sub_lvlis(parent_lvli_fid, data):
        if _DEFAULT_CHILD_PATTERN.search(sub_edid):
            try:
                for it in resolver.resolve_deep(sub_fid):
                    fid = it.get("formid")
                    if fid:
                        default_fids.add(fid)
            except Exception as e:
                print("    [WARN] resolve_deep _Default {}: {}".format(sub_fid, e))
        elif _LEGMODULE_CHILD_PATTERN.search(sub_edid):
            try:
                for it in resolver.resolve_deep(sub_fid):
                    fid = it.get("formid")
                    if fid:
                        legmodule_fids.add(fid)
                # Build per-FormID tier breakdown for the lead item
                # (RESTRICTED_LL_LegendaryModule_1-3 is First Match — ×3@20%,
                # ×2@20%, ×1@60% — collapsed by resolve_deep to one row).
                legmodule_tiers = _build_flat_lvli_tiers(sub_fid, data, resolver)
            except Exception as e:
                print("    [WARN] resolve_deep LegendaryModule {}: {}".format(sub_fid, e))
        elif _FISHING_BAIT_PATTERN.search(sub_edid):
            try:
                bait_sub_fid = sub_fid
                for it in resolver.resolve_deep(sub_fid):
                    fid = it.get("formid")
                    if fid:
                        bait_fids.add(fid)
                # Build per-FormID tier breakdown — Improved Bait LVLI has
                # 3 entries all pointing at Fishing_Bait_Improved at qtys
                # 1/2/3, pick-one → 33.333% each.
                bait_tiers = _build_flat_lvli_tiers(sub_fid, data, resolver)
            except Exception as e:
                print("    [WARN] resolve_deep ImprovedBait {}: {}".format(sub_fid, e))
        elif _BASE_FLOWERS_CHILD_PATTERN.search(sub_edid):
            # Base Flowers stay in the unique-node items (renderer pulls them
            # out by FormID for the Base Flowers expand). We don't filter
            # them here; we only collect their per-FormID tier breakdown so
            # each flower shows its ×5..×10 rows. SSE_LL_Quest_Rewards_-
            # BaseFlowers (007AD25A) is pick-one over 18 entries → 5.555%
            # each (6 entries per flower across 3 FormIDs).
            try:
                base_flower_tiers = _build_flat_lvli_tiers(sub_fid, data, resolver)
            except Exception as e:
                print("    [WARN] _build_flat_lvli_tiers BaseFlowers {}: {}".format(sub_fid, e))

    if not default_fids and not legmodule_fids and not bait_fids and not base_flower_tiers:
        return [], unique_node

    default_items   = []
    legmodule_items = []
    bait_items      = []
    remaining_items = []
    for it in unique_node["items"]:
        fid = it.get("formid")
        if fid in default_fids:
            default_items.append(it)
        elif fid in legmodule_fids:
            legmodule_items.append(it)
        elif fid in bait_fids:
            bait_items.append(it)
        else:
            remaining_items.append(it)

    event_reward_nodes = []

    if default_items:
        groups = {}      # label → [items]
        standalone = []  # items with no group match
        for it in default_items:
            grp = _match_default_group(it.get("edid", ""), it.get("name", ""))
            if grp:
                groups.setdefault(grp, []).append(it)
            else:
                standalone.append(it)
        # Default-LVLI items are unique event rewards (the per-event "pick one"
        # pool: plans, recipes, titles, steins, etc.) — flag them so the
        # renderer routes them to Unique Event Rewards.
        for label, items in groups.items():
            sub = _make_split_subnode(label, items, unique_node)
            sub["isUniqueReward"] = True
            event_reward_nodes.append(sub)
        for it in standalone:
            label = it.get("name") or _clean_pool_label(it.get("edid", "")) or "Reward"
            sub = _make_split_subnode(label, [it], unique_node)
            sub["isUniqueReward"] = True
            event_reward_nodes.append(sub)

    if legmodule_items:
        # Apply tier breakdown (×3@20%, ×2@20%, ×1@60% via First Match
        # thresholds) so the renderer shows three rows instead of one.
        _apply_tiers_to_items(legmodule_items, legmodule_tiers)
        # Legendary Module is a deterministic always-given reward — stays in
        # the Default Event Rewards section (no isUniqueReward flag).
        event_reward_nodes.append(_make_split_subnode(
            "Legendary Module", legmodule_items, unique_node
        ))

    if bait_items:
        # Apply tier breakdown (×1/×2/×3 pick-one at 33.333% each).
        _apply_tiers_to_items(bait_items, bait_tiers)
        # Improved Bait is a deterministic always-given reward sibling — stays
        # in the Default Event Rewards section (no isUniqueReward flag).
        bait_node = _make_split_subnode("Improved Bait", bait_items, unique_node)
        # Attach the bait LVLI's own list-level conditions (e.g. "Requires the
        # Casting Off quest to be completed"). The split subnode wraps items
        # lifted from the parent pool, so the sub-list's conditions must be
        # pulled in explicitly here. Mirrors build_activities_rewards_json.py.
        if bait_sub_fid:
            for _c in _simplify_conditions(data.lvli.list_conditions_for(bait_sub_fid)):
                if _c not in bait_node["conditions"]:
                    bait_node["conditions"].append(_c)
        event_reward_nodes.append(bait_node)

    if remaining_items:
        # Enrich base-flower items (Carnal Weeper / Crystalcup / Radlily)
        # with their 6-tier ×5..×10 breakdowns. The renderer pulls them out
        # of the unique list by FormID and shows them in the Base Flowers
        # expand using the tiers array.
        _apply_tiers_to_items(remaining_items, base_flower_tiers)
        unique_node["items"] = remaining_items
        return event_reward_nodes, unique_node
    return event_reward_nodes, None


# ---------------------------------------------------------------------------
# Tree node construction
# ---------------------------------------------------------------------------

def _build_lvli_node(title, lvli_fid, lvli_edid, resolver, ev_slug,
                     tier_label_fn, group_key=None):
    """Resolve an LVLI deep and build a tree node (or None)."""
    try:
        items = resolver.resolve_deep(lvli_fid)
    except Exception as e:
        print("    [ERROR] resolve_deep({}): {}".format(lvli_fid, e))
        return None

    non_tradable_fids, unsellable_fids = _load_kywd_flags()

    out_items = []
    seen_fids = set()
    for it in items:
        fid  = it.get("formid", "")
        edid = it.get("edid", "")
        name = _NAMED_WEAPON_OVERRIDES.get(fid.lower(), it.get("name", ""))
        rate = it.get("dropRate", 0.0)
        qty  = it.get("qty", 1) or 1
        sig  = (it.get("sig") or "").upper()
        raw_conditions = it.get("conditions") or []

        if not fid or not name:
            continue
        if is_excluded(edid):
            continue
        if rate < MIN_RATE_DECIMAL:
            continue
        if fid in seen_fids:
            continue
        seen_fids.add(fid)

        fid_lc = fid.lower()
        item_dict = {
            "name":     name,
            "formid":   fid,
            "edid":     edid,
            "sig":      sig,
            "qty":      qty,
            "dropRate": round(rate * 100, 6),
            "tiers":    [{"tier": tier_label_fn({"formid": fid, "name": name}),
                          "qty":  qty,
                          "rate": round(rate * 100, 6)}],
        }
        # Tradeable / unsellable flags — only attach to ARMO and BOOK items
        # (where the JS Technical: section actually renders something useful).
        if sig in ("ARMO", "BOOK"):
            item_dict["tradeable"] = fid_lc not in non_tradable_fids
            if fid_lc in unsellable_fids:
                item_dict["unsellable"] = True
        # Conditions — simplified to friendly strings, empty list dropped.
        simplified = _simplify_conditions(raw_conditions)
        if simplified:
            item_dict["conditions"] = simplified
        out_items.append(item_dict)

    if not out_items:
        return None

    out_items.sort(key=lambda x: x["name"].lower())

    node = {
        "type":         "lvli",
        "formid":       lvli_fid,
        "edid":         lvli_edid,
        "label":        title,
        "useAll":       False,
        "entryRate":    100.0,
        "gmrwDropRate": 100.0,
        "tierLabel":    None,
        "conditions":   _simplify_conditions(resolver.lvli.list_conditions_for(lvli_fid)),
        "items":        out_items,
    }
    if group_key:
        node["group"] = group_key
    return node


def _merge_tier_nodes(nodes_per_tier):
    """Merge nodes-per-tier into a single node where items repeated across
    tiers get a stacked tier-row breakdown."""
    if not nodes_per_tier:
        return None

    merged_items = {}
    first_node = None

    for tier_label, node in nodes_per_tier:
        if not node:
            continue
        if first_node is None:
            first_node = node
        for it in node["items"]:
            fid = it["formid"]
            if fid not in merged_items:
                merged = {
                    "name":     it["name"],
                    "formid":   fid,
                    "edid":     it["edid"],
                    "sig":      it.get("sig", ""),
                    "qty":      it["qty"],
                    "dropRate": it["dropRate"],
                    "tiers":    [],
                }
                # Carry through Plan/Recipe/ARMO metadata so Technical + Drop
                # Conditions rows still render after a tier merge.
                if "tradeable" in it:
                    merged["tradeable"] = it["tradeable"]
                if "unsellable" in it:
                    merged["unsellable"] = it["unsellable"]
                if it.get("conditions"):
                    merged["conditions"] = list(it["conditions"])
                merged_items[fid] = merged
            for src_tier in it["tiers"]:
                merged_items[fid]["tiers"].append({
                    "tier": tier_label,
                    "qty":  src_tier.get("qty", it["qty"]),
                    "rate": src_tier.get("rate", it["dropRate"]),
                })

    if not merged_items:
        return None

    out_items = sorted(merged_items.values(), key=lambda x: x["name"].lower())

    for it in out_items:
        rates = [t["rate"] for t in it["tiers"]]
        qtys  = [t["qty"]  for t in it["tiers"]]
        it["dropRate"] = max(rates) if rates else it["dropRate"]
        it["qty"]      = max(qtys)  if qtys  else it["qty"]

    return {
        "type":         "lvli",
        "formid":       first_node["formid"],
        "edid":         first_node["edid"],
        "label":        first_node["label"],
        "useAll":       False,
        "entryRate":    100.0,
        "gmrwDropRate": 100.0,
        "tierLabel":    None,
        "conditions":   [],
        "items":        out_items,
    }


def _build_caps_node(caps_breakdown):
    if not caps_breakdown:
        return None
    items = []
    for entry in caps_breakdown:
        label = entry.get("label") or "Event Completion"
        val = int(entry.get("caps") or 0)
        if val <= 0:
            continue
        items.append({
            "name":     "Caps",
            "formid":   "0000000F",
            "edid":     "Caps001",
            "sig":      "CNCY",
            "qty":      val,
            "dropRate": 100.0,
            "tiers":    [{"tier": label, "qty": val, "rate": 100.0}],
        })
    if not items:
        return None
    if len(items) > 1:
        merged_tiers = []
        for it in items:
            merged_tiers.extend(it["tiers"])
        canonical_qty = max(t["qty"] for t in merged_tiers)
        items = [{
            "name":     "Caps",
            "formid":   "0000000F",
            "edid":     "Caps001",
            "sig":      "CNCY",
            "qty":      canonical_qty,
            "dropRate": 100.0,
            "tiers":    merged_tiers,
        }]
    return {
        "type":         "synthetic",
        "formid":       "",
        "edid":         "_synthetic_caps",
        "label":        "Caps",
        "useAll":       False,
        "entryRate":    100.0,
        "gmrwDropRate": 100.0,
        "tierLabel":    None,
        "conditions":   [],
        "items":        items,
    }


def _build_legendary_node(title, lvli_fid, lvli_edid):
    """Synthetic info-only legendary node (LGDI lists aren't in the LVLI index)."""
    return {
        "type":         "synthetic",
        "formid":       lvli_fid,
        "edid":         lvli_edid,
        "label":        title,
        "useAll":       False,
        "entryRate":    100.0,
        "gmrwDropRate": 100.0,
        "tierLabel":    None,
        "conditions":   [],
        "items": [{
            "name":     title,
            "formid":   lvli_fid,
            "edid":     lvli_edid,
            "sig":      "LGDI",
            "qty":      1,
            "dropRate": 100.0,
            "tiers":    [{"tier": title, "qty": 1, "rate": 100.0}],
        }],
    }


def _collapse_redundant_tiers(node):
    """Drop tiers[] when every tier has identical qty + rate so the JS shows one row."""
    if not node or not node.get("items"):
        return node
    for it in node["items"]:
        tiers = it.get("tiers") or []
        if len(tiers) <= 1:
            it.pop("tiers", None)
            continue
        first_qty = tiers[0].get("qty")
        first_rate = tiers[0].get("rate")
        if all(t.get("qty") == first_qty and t.get("rate") == first_rate for t in tiers):
            it.pop("tiers", None)
    return node


# ---------------------------------------------------------------------------
# Per-event processing
# ---------------------------------------------------------------------------

def _process_quest_event(event_def, slug, resolver, data, gmrw_rows):
    quest_fids = event_def.get("questFormIDs") or []
    rows = _gmrw_rows_for_quests(quest_fids, gmrw_rows)
    print("  Found {} GMRW rows for quest(s) {}".format(len(rows), quest_fids))

    xp_breakdown, caps_breakdown, ri_to_tier = _gmrw_extract_xp_caps_per_ri(rows, data)
    xp_block   = _build_xp_block(xp_breakdown)
    caps_block = _build_caps_block_summary(caps_breakdown)

    tree = []

    caps_node = _build_caps_node(caps_breakdown)
    if caps_node:
        tree.append(caps_node)

    groups = event_def.get("groups")
    quest_to_group = {}
    if groups:
        for g in groups:
            quest_to_group[g["questFormID"]] = g["key"]

    sources = list(_gmrw_iter_lvli_sources(rows))
    by_stem = {}
    stem_order = []
    for title, lvli_fid, lvli_edid, ri, parent_fid in sources:
        stem, tier = _extract_tier_suffix(lvli_edid or "")
        if stem not in by_stem:
            by_stem[stem] = []
            stem_order.append(stem)
        by_stem[stem].append((title, lvli_fid, lvli_edid, ri, parent_fid, tier))

    for stem in stem_order:
        entries = by_stem[stem]

        group_key = None
        if quest_to_group and entries:
            group_key = quest_to_group.get(entries[0][4])

        # Detect whether this stem represents the event's unique reward pool
        is_unique = _is_unique_event_lvli(stem)

        if len(entries) == 1:
            title, lvli_fid, lvli_edid, ri, _parent_fid, tier_suffix = entries[0]
            tier_label_for_items = tier_suffix or title
            node = _build_lvli_node(
                title, lvli_fid, lvli_edid, resolver, event_def["eventSlug"],
                tier_label_fn=lambda _it, _t=tier_label_for_items: _t,
                group_key=group_key,
            )
            if node:
                if ri in ri_to_tier:
                    node["tierLabel"] = ri_to_tier[ri]
                if is_unique:
                    # Split out _Default + LegendaryModule children — these
                    # render as STANDARD Event Rewards (cracked open / grouped
                    # for Default, single node for Legendary Module).
                    extra_nodes, remaining = _split_unique_node(
                        lvli_fid, node, data, resolver
                    )
                    for n in extra_nodes:
                        if group_key:
                            n["group"] = group_key
                        _collapse_redundant_tiers(n)
                        tree.append(n)
                    if remaining:
                        remaining["isUniqueReward"] = True
                        _collapse_redundant_tiers(remaining)
                        tree.append(remaining)
                else:
                    _collapse_redundant_tiers(node)
                    tree.append(node)
        else:
            nodes_per_tier = []
            for title, lvli_fid, lvli_edid, ri, _parent_fid, tier_suffix in entries:
                tlabel = tier_suffix or title
                sub_node = _build_lvli_node(
                    title, lvli_fid, lvli_edid, resolver, event_def["eventSlug"],
                    tier_label_fn=lambda _it, _t=tlabel: _t,
                )
                if sub_node:
                    nodes_per_tier.append((tlabel, sub_node))

            merged = _merge_tier_nodes(nodes_per_tier)
            if merged:
                merged["label"] = _clean_pool_label(stem)
                if group_key:
                    merged["group"] = group_key
                if is_unique:
                    merged["isUniqueReward"] = True
                _collapse_redundant_tiers(merged)
                tree.append(merged)

    for title, lvli_fid, lvli_edid, ri in _gmrw_iter_legendary_sources(rows):
        node = _build_legendary_node(title, lvli_fid, lvli_edid)
        if node:
            if ri in ri_to_tier:
                node["tierLabel"] = ri_to_tier[ri]
            tree.append(node)

    flat_rewards = _build_flat_rewards_from_tree(tree, event_def, groups)

    return {
        "xp":              xp_block,
        "caps":            caps_block,
        "eventRewardTree": tree,
        "rewards":         flat_rewards,
        "groups":          groups,
    }


def _process_container_event(event_def, slug, resolver, data):
    containers = event_def.get("containers") or []
    print("  Resolving {} container LVLIs".format(len(containers)))

    nodes_per_tier = []
    for c in containers:
        title = c.get("title", "")
        lvli_fid = c.get("lvliFormID", "")
        if not lvli_fid:
            continue
        node = _build_lvli_node(
            title, lvli_fid, "", resolver, event_def["eventSlug"],
            tier_label_fn=lambda _it, _t=title: _t,
        )
        if node:
            nodes_per_tier.append((title, node))

    merged = _merge_tier_nodes(nodes_per_tier)
    tree = []
    split_config = event_def.get("splitByCategory")
    if merged and split_config:
        # Split the merged node into per-category nodes (e.g. Treasure Hunter
        # → Currency / Junk & Scrap / Goodies / Contextual Ammo / Unique).
        # Each category node holds the items whose FormID belongs to one of
        # that category's sub-LVLIs. Items in non-unique categories will be
        # rendered as collapsible expands; isUniqueReward category items go
        # in the Unique Event Rewards checklist.
        for cat_node in _split_categories(merged, split_config, resolver):
            tree.append(cat_node)
    elif merged:
        merged["label"] = event_def["name"] + " Rewards"
        # Container events: the entire merged container content IS the unique
        # reward pool — Halloween/Holiday players collect these items as
        # their event prize, not as generic activity rewards.
        merged["isUniqueReward"] = True
        _collapse_redundant_tiers(merged)
        tree.append(merged)

    flat_rewards = _build_flat_rewards_from_tree(tree, event_def, None)

    return {
        "xp":              None,
        "caps":            None,
        "eventRewardTree": tree,
        "rewards":         flat_rewards,
        "groups":          event_def.get("groups"),
    }


def _split_categories(merged_node, split_config, resolver):
    """
    Split a merged container node's items into per-category sub-nodes.

    Each category in split_config["categories"] lists one or more
    sub-LVLI FormIDs. We resolve_deep each sub-LVLI to discover which
    FormIDs belong to that category, then bucket the merged node's items
    accordingly. Returns a list of category nodes in the order declared
    in split_config (so the renderer can rely on a stable layout).
    """
    categories = split_config.get("categories") or []
    if not categories:
        return [merged_node]

    # Build FormID → category_key map by resolving each sub-LVLI.
    fid_to_cat = {}
    for cat in categories:
        cat_key = cat.get("key", "")
        for sub_fid in cat.get("subLvliFormIDs") or []:
            try:
                sub_items = resolver.resolve_deep(sub_fid)
            except Exception as e:
                print("    [WARN] resolve_deep({}) failed: {}".format(sub_fid, e))
                continue
            for sub_it in sub_items:
                fid = (sub_it.get("formid") or "").lower()
                if fid and fid not in fid_to_cat:
                    fid_to_cat[fid] = cat_key

    # Bucket the merged node's items by category.
    items_by_cat = {c["key"]: [] for c in categories}
    unmatched = []
    for it in merged_node.get("items", []):
        fid = (it.get("formid") or "").lower()
        cat_key = fid_to_cat.get(fid)
        if cat_key and cat_key in items_by_cat:
            items_by_cat[cat_key].append(it)
        else:
            unmatched.append(it)

    if unmatched:
        # Surface items we couldn't categorize — usually a sign that a
        # sub-LVLI FormID in the config is wrong or out of date.
        sample = ", ".join(
            "{} ({})".format(it.get("name", "?"), it.get("formid", "?"))
            for it in unmatched[:5]
        )
        print("    [WARN] {} item(s) not categorized; sample: {}".format(
            len(unmatched), sample))

    # Build output nodes (preserve declaration order).
    out_nodes = []
    for cat in categories:
        cat_items = items_by_cat.get(cat["key"]) or []
        if not cat_items:
            continue
        node = {
            "type":         "lvli",
            "formid":       merged_node.get("formid", ""),
            "edid":         merged_node.get("edid", ""),
            "label":        cat.get("label", cat["key"]),
            "categoryKey":  cat["key"],
            "useAll":       merged_node.get("useAll", False),
            "entryRate":    merged_node.get("entryRate", 100.0),
            "gmrwDropRate": merged_node.get("gmrwDropRate", 100.0),
            "tierLabel":    None,
            "conditions":   [],
            "items":        cat_items,
        }
        if cat.get("isUnique"):
            node["isUniqueReward"] = True
        # Per-item tier collapse — drop the tiers array when every tier
        # has identical qty + rate so the table shows a single row instead
        # of 6 redundant tier rows.
        _collapse_redundant_tiers(node)
        out_nodes.append(node)

    return out_nodes


# ---------------------------------------------------------------------------
# Output builders
# ---------------------------------------------------------------------------

def _build_xp_block(xp_breakdown):
    visible = [e for e in xp_breakdown if e.get("xp", 0) > 0]
    if not visible:
        return None
    total = sum(e["xp"] for e in visible)
    scales = any(e.get("scalesWithLevel") for e in visible)
    return {
        "value":           total,
        "scalesWithLevel": scales,
        "breakdown":       visible,
    }


def _build_caps_block_summary(caps_breakdown):
    visible = [e for e in caps_breakdown if e.get("caps", 0) > 0]
    if not visible:
        return None
    total = sum(e["caps"] for e in visible)
    return {
        "value":     total,
        "breakdown": visible,
    }


def _build_flat_rewards_from_tree(tree, event_def, groups):
    """Produce the legacy flat rewards[] list from the event reward tree."""
    ev_slug = event_def["eventSlug"]
    by_fid = {}
    for node in tree:
        node_label = node.get("label", "")
        for it in node.get("items", []):
            fid = it["formid"]
            name = it["name"]
            edid = it["edid"]
            if fid not in by_fid:
                by_fid[fid] = {
                    "name":         name,
                    "formId":       fid,
                    "edid":         edid,
                    "imageUrl":     build_image_url(ev_slug, name),
                    "releaseYear":  None,
                    "tradeable":    None,
                    "isTrackable":  is_trackable(name),
                    "howToObtain":  "<strong>Source:</strong> " + node_label,
                    "group":        node.get("group"),
                    "dropRates":    [],
                }
            for tier in it.get("tiers") or [{"tier": node_label, "rate": it["dropRate"]}]:
                by_fid[fid]["dropRates"].append({
                    "tier": tier.get("tier") or node_label,
                    "rate": fmt_pct(tier.get("rate", it["dropRate"])),
                })
            if node.get("group") and not by_fid[fid].get("group"):
                by_fid[fid]["group"] = node.get("group")

    return sorted(by_fid.values(), key=lambda r: r["name"].lower())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _load_release_years():
    """Load the release-year tracking file. Returns a dict {formid: year}."""
    if RELEASE_YEARS_PATH.exists():
        with open(RELEASE_YEARS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_release_years(tracking):
    """Save the release-year tracking file back to disk."""
    RELEASE_YEARS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RELEASE_YEARS_PATH, "w", encoding="utf-8") as f:
        json.dump(dict(sorted(tracking.items())), f, indent=2)
        f.write("\n")


def _apply_release_years(output, tracking):
    """Assign releaseYear to every trackable reward. New FormIDs get the
    current year and are added to the tracking dict for next time."""
    current_year = datetime.now().year
    new_count = 0
    seen_slugs = set()
    for key, page_data in output.get("byPage", {}).items():
        slug = page_data.get("slug", "")
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        for reward in page_data.get("rewards", []):
            fid = reward.get("formId", "")
            if not reward.get("isTrackable"):
                continue
            if fid in tracking:
                reward["releaseYear"] = tracking[fid]
            else:
                reward["releaseYear"] = current_year
                tracking[fid] = current_year
                new_count += 1
    return new_count


def _stamp_release_years_on_tree(output, tracking):
    """Stamp releaseYear on eventRewardTree items so the JS renderer can
    display year pills without consulting the flat rewards[] list.

    The tracking dict is keyed by FormID and valued by year. Only items
    whose FormID appears in the tracking dict get stamped — this limits
    year pills to trackable items (Plans / Recipes) which is the same
    scope as the flat rewards[] list.
    """
    seen_slugs = set()
    stamped = 0
    for key, page_data in output.get("byPage", {}).items():
        slug = page_data.get("slug", "")
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        for node in page_data.get("eventRewardTree", []):
            for item in node.get("items", []):
                fid = item.get("formid", "")
                if fid and fid in tracking:
                    item["releaseYear"] = tracking[fid]
                    stamped += 1
    return stamped


# ---------------------------------------------------------------------------
# Meat Week Guide page (hand-written guide + data-driven plans & challenges)
# ---------------------------------------------------------------------------
# Grahm's vendor recipe stock. Flat pick-one list (For Each flag, no UseAll /
# FirstMatch), no per-entry or list ChanceNone, no conditions -> every recipe
# in it has an equal 100/N chance per drop-rate-engine section 3e. The guide
# tracks the furniture/decor subset below; the rate is computed from the live
# list length so it stays correct as Bethesda adds plans.
GRAHM_PLAN_VENDOR_LVLI = "003A0815"  # LLV_Vendor_Recipes_Workshop_GQ10

MEAT_WEEK_TRACKED_PLANS = [
    "Plan: Black Domestic Kitchen Tables",
    "Plan: Brown Domestic Kitchen Tables",
    "Plan: Clean Park Bench",
    "Plan: Domestic Kitchen Tables",
    "Plan: Metal Picnic Table",
    "Plan: Mirror Ball",
    "Plan: Mirror Ball - Blue",
    "Plan: Mirror Ball - Green",
    "Plan: Mirror Ball - Pink",
    "Plan: Mirror Ball - Red",
    "Plan: Park Bench",
    "Plan: Picnic Table - Blue",
    "Plan: Picnic Table - Green",
    "Plan: Picnic Table - Pink",
    "Plan: Picnic Table - Red",
    "Plan: Picnic Table - White",
    "Plan: Radiation Emitter",
    "Plan: Starburst Clock",
    "Plan: Stone Benches",
    "Plan: Suitcase - Black",
    "Plan: Suitcase - Blue",
    "Plan: Suitcase - Green",
    "Plan: Suitcase - Orange",
    "Plan: Suitcase - Pink",
    "Plan: Suitcase - Purple",
    "Plan: Suitcase - Red",
    "Plan: Suitcase - Yellow",
    "Plan: White Domestic Kitchen Tables",
]


def _grahm_plan_pool_size(tsv_root):
    """Count entries in Grahm's vendor recipe list (pick-one denominator N)."""
    entries_path = newest(os.path.join(tsv_root, "LVLI_Export_*_LVLI_Entries.tsv"))
    if not entries_path:
        return 0
    n = 0
    for row in read_tsv(entries_path):
        if (row.get("LVLI_FormID") or "").upper() == GRAHM_PLAN_VENDOR_LVLI:
            n += 1
    return n


def _meat_week_challenges(tsv_root):
    """Meat Week SCORE challenges from the CHAL TSV (reward = SCORE progress)."""
    chal_path = newest(os.path.join(tsv_root, "CHAL_Export_*.tsv"))
    out = []
    if not chal_path:
        return out
    for row in read_tsv(chal_path):
        edid = row.get("EDID") or ""
        if not edid.startswith("SCORE_Challenge_"):
            continue
        if "Event_Seasonal_Meat" not in edid:
            continue
        cadence = "Daily" if "_Daily_" in edid else ("Weekly" if "_Weekly_" in edid else "")
        try:
            required = int(float(row.get("TNAM") or 0))
        except (TypeError, ValueError):
            required = 0
        out.append({
            "cadence":  cadence,
            "name":     row.get("FULL") or "",
            "required": required,
            "reward":   "SCORE",
            "formid":   row.get("FormID") or "",
        })
    order = {"Daily": 0, "Weekly": 1, "": 2}
    out.sort(key=lambda c: (order.get(c["cadence"], 3), c["required"]))
    return out


def _build_meat_week_guide(tsv_root):
    """Page data for /df/seasonal-events/meat-week/meat-week-guide/.

    The hand-written prose / images live in the JS renderer; this carries only
    the data-driven pieces (Grahm's rare-plan rate + the Meat Week challenges).
    """
    n = _grahm_plan_pool_size(tsv_root)
    rate = round(100.0 / n, 2) if n else None
    plans = [{"name": nm, "rate": rate} for nm in MEAT_WEEK_TRACKED_PLANS]
    challenges = _meat_week_challenges(tsv_root)
    print("[build_seasonal_events] Meat Week Guide: Grahm plan pool N={} "
          "-> {}% each, {} challenges".format(n, rate, len(challenges)))
    return {
        "name":             "Meat Week Guide",
        "slug":             "meat-week-guide",
        "eventSlug":        "meat-week",
        "guidePage":        True,
        "description":      "Grahm's Meat-Cook walkthrough for Meat Week - task "
                            "breakdown, Chally's Feed recipe, challenges and "
                            "Grahm's rare plans.",
        "grahmPlanPoolSize": n,
        "rarePlans":        plans,
        "planRecipe":       {"name": "Recipe: Chally's Feed", "rate": 100.0},
        "challenges":       challenges,
        # carried for parity with other pages (renderer ignores when guidePage)
        "eventRewardTree":  [],
        "rewards":          [],
    }


def main():
    print("[build_seasonal_events] Loading rng76 engine...")
    data = Rng76Data.from_tsv_root(TSV_ROOT)
    resolver = data.resolver

    gmrw_path = newest(str(_REPO_ROOT / "tsv" / "GMRW_Export_*.tsv"))
    gmrw_rows = read_tsv(gmrw_path)
    print("[build_seasonal_events] Loaded {} GMRW rows from {}".format(
        len(gmrw_rows), os.path.basename(gmrw_path)))

    # Load QUEST TSV for party-crasher / invaders detection
    quest_index = _load_quest_index(TSV_ROOT)

    # Load region/location lookup (shared with the activities build) so pages
    # rendered in the activity layout can show Region/Location header lines.
    region_locations = _load_region_location_tsv(TSV_ROOT)

    # Load release-year tracking (persistent across builds)
    release_years = _load_release_years()
    print("[build_seasonal_events] Loaded {} release-year entries".format(
        len(release_years)))

    output = {"byPage": {}}

    for slug, event_def in EVENTS.items():
        ev_name = event_def["name"]
        ev_slug = event_def["eventSlug"]
        print("\n[build_seasonal_events] Processing: {} ({})".format(ev_name, slug))

        # Detect party-crasher / invaders flags from QUEST TSV
        quest_fids = event_def.get("questFormIDs") or []
        event_flags = _detect_event_flags(quest_fids, quest_index, data.globs)

        page_data = {
            "name":            ev_name,
            "description":     event_def["description"],
            "slug":            slug,
            "eventSlug":       ev_slug,
            "isContainerLoot": event_def["isContainerLoot"],
            "xp":              None,
            "caps":            None,
            "eventRewardTree": [],
            "rewards":         [],
            "groups":          event_def.get("groups"),
            "gallery":         list(EVENT_GALLERIES.get(slug, [])),
            "regionLocations": region_locations.get(_norm_event_name(ev_name), []),
            "regionInfo":      event_def.get("regionInfo"),
        }
        if event_flags["partyCrashers"]:
            page_data["partyCrashers"] = event_flags["partyCrashers"]
        if event_flags["invadersEvent"]:
            page_data["invadersEvent"] = event_flags["invadersEvent"]
        if slug == "primal-cuts-all-rewards":
            _pmb = _build_prime_meat_buff(TSV_ROOT)
            if _pmb:
                page_data["primeMeatBuff"] = _pmb

        try:
            if event_def["isContainerLoot"]:
                ev = _process_container_event(event_def, slug, resolver, data)
            else:
                ev = _process_quest_event(event_def, slug, resolver, data, gmrw_rows)
        except Exception as e:
            print("  [ERROR] processing failed: {}".format(e))
            ev = {"xp": None, "caps": None, "eventRewardTree": [], "rewards": [],
                  "groups": event_def.get("groups")}

        page_data["xp"]              = ev.get("xp")
        page_data["caps"]            = ev.get("caps")
        page_data["eventRewardTree"] = ev.get("eventRewardTree") or []
        page_data["rewards"]         = ev.get("rewards") or []
        page_data["groups"]          = ev.get("groups")

        # Inject any hand-authored extra reward nodes (e.g. direct ALCH rewards
        # the LVLI walk doesn't capture) after the synthetic Caps node.
        extra_nodes = event_def.get("extraRewardNodes") or []
        if extra_nodes:
            tree = page_data["eventRewardTree"]
            insert_at = next(
                (i + 1 for i, n in enumerate(tree)
                 if "synthetic_caps" in str(n.get("edid", "")).lower()),
                0,
            )
            for off, node in enumerate(extra_nodes):
                tree.insert(insert_at + off, node)

        tree_len = len(page_data["eventRewardTree"])
        rewards_len = len(page_data["rewards"])
        xp_summary = "{} XP".format(page_data["xp"]["value"]) if page_data["xp"] else "no XP"
        caps_summary = "{} caps".format(page_data["caps"]["value"]) if page_data["caps"] else "no caps"
        print("  -> {} tree nodes, {} flat rewards, {}, {}".format(
            tree_len, rewards_len, xp_summary, caps_summary))

        output["byPage"][slug] = page_data
        url_path = "/df/seasonal-events/" + ev_slug + "/" + slug + "/"
        output["byPage"][url_path] = page_data
        output["byPage"][url_path.rstrip("/")] = page_data

    # Meat Week Guide (hand-written guide page; data-driven plans + challenges)
    print("\n[build_seasonal_events] Processing: Meat Week Guide (meat-week-guide)")
    _mwg = _build_meat_week_guide(TSV_ROOT)
    _mwg_url = "/df/seasonal-events/meat-week/meat-week-guide/"
    output["byPage"]["meat-week-guide"] = _mwg
    output["byPage"][_mwg_url] = _mwg
    output["byPage"][_mwg_url.rstrip("/")] = _mwg

    # Assign release years to all trackable rewards
    new_items = _apply_release_years(output, release_years)
    print("\n[build_seasonal_events] Release years: {} existing, {} new".format(
        len(release_years) - new_items, new_items))
    _save_release_years(release_years)

    # Stamp release years onto eventRewardTree items (the JS renderer reads
    # from the tree, not from the flat rewards[] list).
    tree_stamped = _stamp_release_years_on_tree(output, release_years)
    print("[build_seasonal_events] Stamped releaseYear on {} tree items".format(
        tree_stamped))

    DIST_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DIST_DIR / "seasonal_events_rewards_by_page.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print("\n[build_seasonal_events] Written: {}".format(out_path))
    print("[build_seasonal_events] File size: {} bytes".format(out_path.stat().st_size))


if __name__ == "__main__":
    main()
