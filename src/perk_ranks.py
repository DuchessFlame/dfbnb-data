#!/usr/bin/env python3
r"""
perk_ranks.py — resolve a perk card's SPECIAL, level, rank count and per-rank
magnitudes from the game's OWN records, so page copy never hardcodes something
Bethesda can move or rebalance.

WHY THIS EXISTS
===============
The farming "Farming Tips and Tricks" expand used to carry hand-typed strings:

    Good With Salt (Luck, 1 rank) slows spoilage by 45%.
    Thru-Hiker (Agility) will reduce the weight of ...
    Green Thumb (Luck, 1 rank) lets you harvest twice as much flora.

Every one of those was wrong, and every correct value was already in the exports:

  * Good With Salt has TWO ranks. GoodWithSaltCard (PCRD 0052415D) says
    ``RankCount = 2`` and the ability SPEL AbPerkGoodWithSalt (0052415A) carries
    one abPerkReduceFoodSpoilageEffect entry per rank — 0.45 then 0.90. Rank 2
    (90% slower) was missing from every perishable farming page.
  * Good With Salt is an INTELLIGENCE card, Thru-Hiker an ENDURANCE card, and
    Green Thumb a PERCEPTION card. The pages said Luck, Agility and Luck.

The rule this follows: when the game files carry Bethesda's own authoritative
value, build from it. The card record owns SPECIAL, level and rank count; the
ability SPEL owns what each rank does.

TRAP: THE LIFETIME-CHALLENGE FORMLISTS GO STALE WHEN A CARD MOVES
-----------------------------------------------------------------
A card references ``Challenge_Lifetime_Perks_RankUp_SUB_<SPECIAL>_Formlist``,
which looks like a second opinion on its SPECIAL. It is not — that formlist is
NOT re-pointed when Bethesda moves a card, so it preserves the card's OLD
SPECIAL. As of the Sept 2026 exports, GoodWithSaltCard sits in the *Luck*
formlist while ``DATA_Special`` reads Intelligence (Intelligence is what the
game shows), and ThruHikerCard sits in the *Agility* formlist while
``DATA_Special`` reads Endurance. Read ``DATA_Special``. A disagreement between
the two means the card moved, not that the column is wrong.

USAGE
-----
    import perk_ranks

    perk_ranks.card("good_with_salt")
    # -> {"name": "Good With Salt", "special": "Intelligence", "min_level": 9,
    #     "rank_count": 2,
    #     "ranks": [{"rank": 1, "reduction": "45%"},
    #               {"rank": 2, "reduction": "90%"}]}

    perk_ranks.card("thru_hiker", channel="pts")   # no ability SPEL -> no "ranks"

Returns ``None`` / ``[]`` (never raises) when an export is missing, so a builder
that can't see PCRD/SPEL degrades to "no numbers" instead of publishing wrong
ones. Add a perk by adding one row to CARDS — no new code.
"""

from __future__ import annotations

import csv
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import tsv_source

# slug -> (PCRD card EDID, display name, ability SPEL EDID or None).
# The SPEL is only needed for a perk whose per-rank MAGNITUDE the pages print.
# Butcher's Bounty / Green Thumb / Can Do! state their effect in words, so they
# carry the card only and their rank count comes from PCRD.
CARDS = {
    "good_with_salt":     ("GoodWithSaltCard",      "Good With Salt",      "AbPerkGoodWithSalt"),
    "thru_hiker":         ("ThruHikerCard",         "Thru-Hiker",          None),
    "traveling_pharmacy": ("TravelingPharmacyCard", "Traveling Pharmacy",  None),
    "butchers_bounty":    ("ButchersBountyCard",    "Butcher's Bounty",    None),
    "green_thumb":        ("GreenThumbCard",        "Green Thumb",         None),
    "can_do":             ("CanDoCard",             "Can Do!",             None),
}

_CACHE: dict = {}
_ROWS: dict = {}


def _rows(pattern, channel):
    """Resolved export -> list of dict rows. [] if the export isn't there.

    Cached per (pattern, channel): a farming run resolves six cards and would
    otherwise re-read the same two exports twelve times.
    """
    key = (pattern, channel)
    if key in _ROWS:
        return _ROWS[key]
    try:
        path = tsv_source.newest(pattern, channel=channel, required=False)
    except Exception:
        path = None
    if not path or not os.path.exists(path):
        _ROWS[key] = []
        return []
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        _ROWS[key] = list(csv.DictReader(fh, delimiter="\t"))
    return _ROWS[key]


def _pct(magnitude):
    """0.45 -> '45%'. Magnitudes are stored as a fraction of the base rate."""
    return f"{round(float(magnitude) * 100):g}%"


def _int(value):
    try:
        return int((value or "").strip() or 0)
    except (ValueError, AttributeError):
        return 0


_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def perk_descs(card_row, rank_count, channel="live"):
    """{rank: (edid, DESC)} for a card's ranks, via its Rank_N_MalePerk_EDID columns.

    The card lists its own rank perks, so this never guesses at an "01/02/03"
    naming convention — zzz_ThruHiker03 is exactly the kind of record that breaks.
    """
    wanted = {}
    for i in range(1, (rank_count or 0) + 1):
        edid = (card_row.get(f"Rank_{i}_MalePerk_EDID") or "").strip()
        if edid:
            wanted[edid] = i
    if not wanted:
        return {}
    out = {}
    for row in _rows("PERK_Export_*.tsv", channel):
        edid = (row.get("PERK_EDID") or "").strip()
        if edid in wanted:
            desc = (row.get("DESC") or "").strip()
            if desc:                      # a perk repeats per entry-point row
                out[wanted[edid]] = (edid, desc)
    return out


def spel_ranks(spel_edid, rank_count, channel="live"):
    """[{rank, reduction}] from an ability SPEL's effect entries.

    One effect entry per rank, matched by EffectIndex: index 0 is rank 1. A rank
    the card declares but the SPEL has no entry for is emitted with
    ``reduction: None`` rather than dropped, so a caller can see the gap.
    """
    by_index = {}
    for row in _rows("SPEL_Export_*_EFFECTS.tsv", channel):
        if (row.get("SPEL_EDID") or "").strip() != spel_edid:
            continue
        try:
            by_index[int((row.get("EffectIndex") or "").strip())] = \
                float((row.get("EFIT_Magnitude") or "").strip())
        except ValueError:
            continue
    if not by_index:
        return []
    # The card is the source of truth for the count; fall back to the SPEL's own
    # entry count when PCRD is missing from an older export set.
    n = rank_count or len(by_index)
    out = []
    for i in range(n):
        mag = by_index.get(i)
        out.append({"rank": i + 1, "reduction": _pct(mag) if mag is not None else None})
    return out


def card(slug, channel="live"):
    """{name, special, min_level, rank_count, ranks?} for a CARDS slug.

    ``special`` is PCRD ``DATA_Special`` — see the TRAP note above before
    reaching for the challenge formlist instead. ``ranks`` is present only for a
    perk with an ability SPEL in CARDS. Returns None for an unknown slug or when
    PCRD can't be read.
    """
    if slug not in CARDS:
        return None
    key = (slug, channel)
    if key in _CACHE:
        return _CACHE[key]

    card_edid, name, spel_edid = CARDS[slug]
    row = None
    for r in _rows("PCRD_Export_*.tsv", channel):
        if (r.get("PCRD_EDID") or "").strip() == card_edid:
            row = r
            break
    if row is None:
        _CACHE[key] = None
        return None

    rank_count = _int(row.get("RankCount"))
    info = {
        "name": name,
        "special": (row.get("DATA_Special") or "").strip() or None,
        "min_level": _int(row.get("DATA_MinLevel")) or None,
        "rank_count": rank_count or None,
    }
    # Per-rank detail. A magnitude the pages print comes from the ability SPEL;
    # everything else states its effect in the rank perk's own DESC, whose
    # headline percentage ("40% chance to find extra meat...") is the number the
    # pages were hardcoding.
    ranks = spel_ranks(spel_edid, rank_count, channel) if spel_edid else []
    descs = perk_descs(row, rank_count, channel)
    if not ranks and descs:
        ranks = [{"rank": i} for i in range(1, rank_count + 1)]
    for r in ranks:
        hit = descs.get(r["rank"])
        if not hit:
            continue
        r["desc"] = hit[1]
        m = _PCT_RE.search(hit[1])
        if m:
            r["percent"] = f"{float(m.group(1)):g}%"
    if ranks:
        info["ranks"] = ranks

    _CACHE[key] = info
    return info


def cards(slugs, channel="live"):
    """{slug: card(slug)} for the slugs that resolve. Unknown/missing are dropped."""
    out = {}
    for slug in slugs:
        info = card(slug, channel)
        if info:
            out[slug] = info
    return out


if __name__ == "__main__":
    ch = sys.argv[1] if len(sys.argv) > 1 else "live"
    for slug in CARDS:
        print(f"[{ch}] {slug:20}", card(slug, ch))
