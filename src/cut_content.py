"""
cut_content.py
==============
Single source of truth for "is this game-data record cut content / test /
debug / never-released?". Shared across build_cobj_recipes_json.py,
build_bnb_item_categories_json.py, and build_bnb_menu_sync.py so the three
pipelines stay in lock-step — if an item is cut in one, it's cut everywhere.

Cut-content signals:
  - Standard cut prefixes: DEL, CUT, POST, ZZZ (matches zzz too, case-insensitive)
  - Test/debug prefixes:   TEST, DEBUG, PETS_, _Disease
  - Quest-scrapped prefixes and specific EDID patterns collected over time
    (Nukashine 2 / SFS01_Brew / Firecracker etc.)

is_cut(edid) returns True if the EDID should be filtered. The patterns are
regex-matched case-insensitively against the beginning (or a recognisable
substring) of the EDID. New patterns should be added here, not in individual
builders.
"""
from __future__ import annotations

import re
from typing import Iterable

# Case-insensitive, anchored-start unless noted otherwise.
CUT_PATTERNS = [
    # ── Standard cut-content prefixes ──────────────────────────────────
    re.compile(r"^DEL_?", re.IGNORECASE),
    re.compile(r"^CUT_?", re.IGNORECASE),
    re.compile(r"^POST_?", re.IGNORECASE),
    re.compile(r"^ZZZ+_?", re.IGNORECASE),   # ZZZ, ZZZZ, zzz — all cut
    re.compile(r"^zz", re.IGNORECASE),       # catch-all belt-and-suspenders

    # ── Test / debug items ─────────────────────────────────────────────
    re.compile(r"^TEST_?", re.IGNORECASE),
    re.compile(r"^DEBUG_?", re.IGNORECASE),
    re.compile(r"^test[A-Z]", re.IGNORECASE),   # testQA_*, testSURV_*, etc.
    re.compile(r"^PETS_", re.IGNORECASE),
    re.compile(r"^_Disease", re.IGNORECASE),    # disease chance test items

    # ── Prototype / scrapped ───────────────────────────────────────────
    re.compile(r"^PTS_", re.IGNORECASE),        # Potion of Experience/etc.
    re.compile(r"^DEPRECATED_", re.IGNORECASE),
    re.compile(r"^SpoiledFood_", re.IGNORECASE),
    re.compile(r"_Spoiled$", re.IGNORECASE),

    # ── Quest cut-content (historical) ─────────────────────────────────
    re.compile(r"^AC_SQ04_Reopening_", re.IGNORECASE),   # Nukashine 2 Electric Boogaloo etc.
    re.compile(r"^SFS01_Brew_", re.IGNORECASE),          # Tater/Muttberry/Sunday Shine
    re.compile(r"Firecracker", re.IGNORECASE),            # Firecracker Whiskey line
    re.compile(r"^W05_MQS_205P_Jen", re.IGNORECASE),     # Jen's stealth potion
    re.compile(r"^CUT_RefreshingBeverage", re.IGNORECASE),
    re.compile(r"^CUT_SFS09_FormulaQ", re.IGNORECASE),
    re.compile(r"^CUT_P01E_HeartNougat", re.IGNORECASE),
    re.compile(r"^POST_DetoxingSalve", re.IGNORECASE),

    # ── Game-system / infusion effect shells (not menu items) ──────────
    re.compile(r"SteelSkin", re.IGNORECASE),
    re.compile(r"HealingCloud", re.IGNORECASE),
    re.compile(r"FieryInfusion", re.IGNORECASE),
    re.compile(r"^PotionOf", re.IGNORECASE),
    re.compile(r"^Recalibrated", re.IGNORECASE),
    re.compile(r"^BS00_Contribution", re.IGNORECASE),    # ATLAS donor provisions
    re.compile(r"TreasureHunt_Chest", re.IGNORECASE),    # Mole Miner Pails
    re.compile(r"^E07A_Mothman", re.IGNORECASE),         # Cultist High Priest Pack
    re.compile(r"MutatedEvents_Package", re.IGNORECASE),
    re.compile(r"Challenge_Raids_", re.IGNORECASE),      # Gleaming Depths crate
    re.compile(r"^Festive_", re.IGNORECASE),             # Holiday/Waterlogged gifts
    re.compile(r"^v96_Metabolux", re.IGNORECASE),
    re.compile(r"^MTR01_Precursor", re.IGNORECASE),      # Precursor serums
    re.compile(r"^SCORE_", re.IGNORECASE),               # SCORE rewards
    re.compile(r"^Spooky_TreatBag", re.IGNORECASE),
    re.compile(r"^TrackingDart", re.IGNORECASE),
    re.compile(r"^MTNS05_VoxDart", re.IGNORECASE),
    re.compile(r"^DLC04_Calmex", re.IGNORECASE),
    re.compile(r"^MTNZ03_FormulaB", re.IGNORECASE),
    re.compile(r"^SURV_Innoculation", re.IGNORECASE),
    re.compile(r"^ResuscitationKit", re.IGNORECASE),
    re.compile(r"^SwarmMeat", re.IGNORECASE),
    re.compile(r"^FloraSpecimenJarYellow$", re.IGNORECASE),

    # ── No-COBJ / no-recipe item families ──────────────────────────────
    re.compile(r"^Fishing_.*_Glowing$", re.IGNORECASE),
    re.compile(r"^ZZZ_Fishing_.*_Glowing", re.IGNORECASE),
]


def is_cut(edid: str) -> bool:
    """True if the EDID matches any cut-content pattern."""
    if not edid:
        return False
    e = edid.strip()
    if not e:
        return False
    return any(p.search(e) for p in CUT_PATTERNS)


def purge_cut_rows(rows: Iterable[dict], edid_col: str = "edid") -> "tuple[list[dict], list[dict]]":
    """Split rows into (kept, removed) based on is_cut(row[edid_col]).

    Convenience for the sync script's startup cleanup pass.
    """
    kept: list[dict] = []
    removed: list[dict] = []
    for r in rows:
        if is_cut((r.get(edid_col) or "").strip()):
            removed.append(r)
        else:
            kept.append(r)
    return kept, removed
