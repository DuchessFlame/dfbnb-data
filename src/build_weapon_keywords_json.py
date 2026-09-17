#!/usr/bin/env python3
"""
build_weapon_keywords_json.py
=============================
Builds the Weapon Keyword Index for /df/score-challenges/weapon-keyword-index/.

Score challenges are written in keyword language -- "kill 20 enemies with an
Automatic weapon", "kill 10 with a Blunt weapon". Those words are literal
WeaponType* keywords attached to each WEAP record, so this script inverts the
WEAP export: instead of weapon -> its keywords, it produces keyword -> every
weapon carrying it. A weapon with several keywords appears under each one.

Reads from:
  - WEAP_Export_*_Base.tsv   (column `Keywords`, pipe-separated)
  - CHAL_Export_*.tsv        (columns `Cond1..CondN`) -- so each keyword can also
                             list the live challenges that test for it

Outputs:
  - dist/weapon_keywords.json

The weapon filter is IMPORTED from build_spin_wheel_json.py on purpose -- the
Challenge Roulette wheel and this page both answer "what counts as a weapon for
a challenge", and they must never disagree. Fix a bad weapon in one place.

Output shape:
  {
    "groups": [
      { "key": "automatic", "label": "Automatic", "keyword": "WeaponTypeAutomatic",
        "named": false, "count": 78, "weapons": ["10mm Submachine Gun", ...],
        "challengeCount": 5,
        "challenges": [ { "type": "Daily", "text": "Kill an Enemy with any ..." }, ... ] },
      ...
    ],
    "_meta": { "built": "YYYY-MM-DD", "source": "WEAP_Export_July_2026_Base.tsv",
               "groupCount": N, "weaponCount": N }
  }

Groups are sorted A-Z by label; weapons inside each group are sorted A-Z.

No external dependencies -- runs on stdlib only.

Usage:  python src/build_weapon_keywords_json.py
PTS:    python src/build_weapon_keywords_json.py --pts
"""

import json
import os
import re
import sys
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import build_spin_wheel_json as spin  # noqa: E402  (shared weapon filter)
import tsv_source              # noqa: E402  (one resolver for every export selection)

ROOT = os.path.dirname(HERE)
PTS = "--pts" in sys.argv
OUT_DIR = os.path.join(ROOT, "dist", "pts") if PTS else os.path.join(ROOT, "dist")

# ─────────────────────────────────────────────────────────────────────────────
#  The keyword set
# ─────────────────────────────────────────────────────────────────────────────
# The page covers EVERY WeaponType* keyword the exports actually use -- one on
# a live weapon, or named by a live challenge. That set is discovered from the
# data (see discover_groups), so a keyword Bethesda adds next patch turns up on
# its own instead of waiting for someone to hand-add it here.
#
# This list is the CURATED layer on top of that: the broad keywords a challenge
# usually names, where the EDID makes a poor heading or where several keywords
# belong under one heading. It does three things discovery can't:
#
#   1. Renames -- WeaponTypeHeavyGun reads "Big Gun", WeaponTypeUnarmed "Fist",
#      WeaponTypeBladed "Sharp", WeaponTypeAutomaticMelee "Power Tool".
#   2. Merges  -- Poison is one heading over two keywords.
#   3. Pins the `named` flag for keywords whose display string is inconsistent
#      in the export.
#
# Anything NOT listed here is auto-discovered and labelled from the export's own
# display string, or de-camel-cased from the EDID. Adding a row here overrides
# that; you never have to add a row just to make a keyword appear.
#
# label -> list of keywords that feed it (Poison ships under two keywords).
KEYWORD_GROUPS = [
    # ── named ────────────────────────────────────────────────────────────────
    ("1-Hand Melee", ["WeaponTypeMelee1H"],                          True),
    ("2-Hand Melee", ["WeaponTypeMelee2H"],                          True),
    ("Archaic",      ["WeaponTypeArchaic"],                          True),
    ("Ballistic",    ["WeaponTypeBallistic"],                        True),
    ("Big Gun",      ["WeaponTypeHeavyGun"],                         True),
    ("Blunt",        ["WeaponTypeBlunt"],                            True),
    ("Bow",          ["WeaponTypeBow"],                              True),
    ("Cryo",         ["WeaponTypeCryoDamage"],                       True),
    ("Energy",       ["WeaponTypeEnergy"],                           True),
    ("Explosive",    ["WeaponTypeExplosive"],                        True),
    ("Fire",         ["WeaponTypeFireDamage"],                       True),
    ("Fist",         ["WeaponTypeUnarmed"],                          True),
    ("Improvised",   ["WeaponTypeImprovised"],                       True),
    ("Pistol",       ["WeaponTypePistol"],                           True),
    ("Poison",       ["WeaponTypePoisonDamage", "WeaponTypeDmgPoison"], True),
    ("Power Tool",   ["WeaponTypeAutomaticMelee"],                   True),
    ("Radiation",    ["WeaponTypeRadiation"],                        True),
    ("Rifle",        ["WeaponTypeRifle"],                            True),
    ("Sharp",        ["WeaponTypeBladed"],                           True),
    ("Shotgun",      ["WeaponTypeShotgun"],                          True),
    ("Throwing",     ["WeaponTypeThrown"],                           True),
    # ── unnamed but challenge-relevant ───────────────────────────────────────
    ("Automatic",    ["WeaponTypeAutomatic"],                        False),
    ("Grenade",      ["WeaponTypeGrenade"],                          False),
    ("Laser",        ["WeaponTypeLaser"],                            False),
    ("Melee",        ["WeaponTypeMeleeGeneral"],                     False),
    ("Mine",         ["WeaponTypeMine"],                             False),
    ("Plasma",       ["WeaponTypePlasma"],                           False),
    ("Ranged",       ["WeaponTypeRanged"],                           False),
    ("Revolver",     ["WeaponTypeRevolver"],                         False),
    ("Sniper",       ["WeaponTypeSniper"],                           False),
]


# Engine plumbing: real WeaponType* keywords that no live challenge references
# and that mean nothing to a player reading a challenge. WeaponTypeWeird sits on
# 32 weapons and says nothing about any of them; DOT/Piercing/NonBullet are
# damage-system internals. Kept out of discovery so the list stays readable.
# Delete a line here and that keyword starts appearing -- nothing else to change.
_KEYWORD_SKIP = {
    "WeaponTypeWeird", "WeaponTypeDOT", "WeaponTypePiercing", "WeaponTypeGun",
    "WeaponTypeNonBullet", "WeaponTypeCult", "WeaponTypeNitro",
    "WeaponTypeUltracite", "WeaponTypeV63",
}

# Where de-camel-casing an EDID gets it wrong. Only for discovered keywords --
# a curated KEYWORD_GROUPS row already carries its own label.
_LABEL_OVERRIDE = {
    "WeaponTypeFatman":          "Fat Man",
    "WeaponTypeHandToHand":      "Hand to Hand",
    "WeaponTypeMeleeAndUnarmed": "Melee and Unarmed",
}


def decamel(edid):
    """WeaponTypeColdShoulder -> "Cold Shoulder".

    Splits lower/digit -> Upper, and the tail of an acronym run before a word
    (ABCWord -> ABC Word). Deliberately does NOT split letter -> digit, which
    would turn V63 into "V 63".
    """
    s = edid[len("WeaponType"):] if edid.startswith("WeaponType") else edid
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)
    return s.strip()


def slugify(label):
    return re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


def keywords_on_row(keywords_str):
    """Exact keyword EDIDs on a WEAP row.

    The export packs them as
        Anims1hmWeapon [KYWD:00023465]|WeaponTypeBlunt "Blunt" [KYWD:0033AB24]
    so split on the pipe and take the leading token. Matching the token exactly
    matters -- a substring test would let WeaponTypeGrenadeLauncher answer for
    WeaponTypeGrenade, and WeaponTypePlasmaPistol for WeaponTypePlasma.
    """
    out = set()
    for chunk in (keywords_str or "").split("|"):
        chunk = chunk.strip()
        if not chunk:
            continue
        out.add(chunk.split(" ")[0].split("[")[0].strip())
    return out


def collect_weapons():
    """Every player-obtainable weapon, as (display name, set of keyword EDIDs)."""
    tsv_path = spin.find_newest_tsv("WEAP_Export_*_Base.tsv")
    if not tsv_path:
        print("  [WARN] No WEAP_Export_*_Base.tsv found", file=sys.stderr)
        return [], None
    print(f"  Reading weapons from: {os.path.basename(tsv_path)}")
    rows = spin.read_tsv(tsv_path)
    print(f"  Total WEAP rows: {len(rows)}")

    # WeaponTypeBlunt "Blunt" -- the game's own display string for a keyword.
    # Harvested here rather than in a second pass so the export is read once.
    named_re = re.compile(r'(WeaponType[A-Za-z0-9_]+)\s+"([^"]+)"')
    display = {}

    seen = {}
    for row in rows:
        for m in named_re.finditer(row.get("Keywords") or ""):
            display.setdefault(m.group(1), m.group(2).strip())
        edid = (row.get("WEAP_EDID") or "").strip()
        name = (row.get("WEAP_FULL") or "").strip()
        kw_raw = row.get("Keywords") or ""
        if not name:
            continue
        if spin.is_cut(edid):
            continue
        if any(p.search(edid) for p in spin._WEAP_SKIP_EDID):
            continue
        if name.lower() in spin._WEAP_SKIP_NAMES:
            continue
        if re.match(r"^\d+\s*(DMG|DR|ER|Rad)", name):
            continue
        kws = keywords_on_row(kw_raw)
        if kws & spin._WEAP_SKIP_KEYWORDS:
            continue
        name = spin._WEAP_NAME_REMAP.get(edid, name)
        # Same display name can appear on several records (variants, DLC
        # duplicates). Union their keywords so the weapon lands in every group
        # any of its records qualifies for.
        seen.setdefault(name, set()).update(kws)

    print(f"  Weapons after filtering: {len(seen)}")
    return (sorted(seen.items(), key=lambda kv: kv[0].lower()),
            display, os.path.basename(tsv_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Challenges that test for a keyword
# ─────────────────────────────────────────────────────────────────────────────
# A CHAL record's conditions are where the keyword requirement actually lives:
#
#   Cond1 : 10000000|1.000000|WornHasKeyword|00 00 00|00 00|
#           WeaponTypeEnergy "Energy" [KYWD:0033A7C9]|00 00 00 00|0|-1|Subject|
#
# so "which challenges want an Energy weapon" is a scan of every Cond* cell for
# the keyword EDID. Pulled out with a leading-pipe anchor and a full-token match
# for the same reason keywords_on_row() is exact: a substring test would let
# WeaponTypePlasmaPistol answer for WeaponTypePlasma.
#
# Cut rows are dropped with the shared is_cut() -- CUT_/POST_/ZZZ_ challenges
# are still in the export and would otherwise put retired text on a live page.

_CHAL_KEYWORD_IN_COND = re.compile(r"\|(WeaponType[A-Za-z0-9_]+)\b")

# Display order for the type heading inside an expand. Daily and Weekly are the
# SCORE challenges the page exists for, so they lead; Lifetime is the long tail.
_CHAL_TYPE_ORDER = ["Daily", "Weekly", "Event", "Lifetime"]


def _chal_sort_key(text):
    """Sort "Epic - Kill X" next to "Kill X" instead of under E."""
    return re.sub(r"^\s*epic\s*-\s*", "", text.strip(), flags=re.I).lower()


def collect_challenges():
    """keyword EDID -> [ {type, text}, ... ] for every live challenge."""
    # Channel-explicit: a --pts build must not quietly read the live CHAL
    # export. spin.find_newest_tsv already picks the channel by TSV_DIR; this
    # one has to be told.
    tsv_path = tsv_source.newest(
        "CHAL_Export_*.tsv", channel="pts" if PTS else "live", required=False)
    if not tsv_path:
        print("  [WARN] No CHAL_Export_*.tsv found -- challenges omitted", file=sys.stderr)
        return {}, None
    print(f"  Reading challenges from: {os.path.basename(tsv_path)}")
    rows = spin.read_tsv(tsv_path)
    print(f"  Total CHAL rows: {len(rows)}")

    by_keyword = {}
    seen = set()
    for row in rows:
        edid = (row.get("EDID") or "").strip()
        text = (row.get("FULL") or "").strip()
        if not text or text.upper() == "NONE":
            continue
        if spin.is_cut(edid):
            continue
        ctype = (row.get("CNAM") or "").strip() or "Other"

        kws = set()
        for col, val in row.items():
            if not col or not col.startswith("Cond") or col == "CondCount" or not val:
                continue
            kws.update(_CHAL_KEYWORD_IN_COND.findall(val))
        if not kws:
            continue

        for kw in kws:
            # The same challenge text ships as a Daily and a Weekly, and
            # Lifetime tiers repeat one line per tier. One row per (kw, type,
            # text) is what a reader wants to see.
            sig = (kw, ctype, text)
            if sig in seen:
                continue
            seen.add(sig)
            by_keyword.setdefault(kw, []).append({"type": ctype, "text": text})

    total = sum(len(v) for v in by_keyword.values())
    print(f"  Challenge/keyword pairs: {total} across {len(by_keyword)} keywords")
    return by_keyword, os.path.basename(tsv_path)


def challenges_for(keywords, by_keyword):
    out = []
    seen = set()
    for kw in keywords:
        for c in by_keyword.get(kw, []):
            sig = (c["type"], c["text"])
            if sig in seen:
                continue
            seen.add(sig)
            out.append(c)
    order = {t: i for i, t in enumerate(_CHAL_TYPE_ORDER)}
    out.sort(key=lambda c: (order.get(c["type"], len(order)), _chal_sort_key(c["text"])))
    return out


def discover_groups(weapons, by_keyword, display):
    """Every WeaponType* keyword the exports use that KEYWORD_GROUPS doesn't claim.

    In scope: carried by at least one live weapon, or named by at least one live
    challenge. Out: anything a curated group already covers, the weapon filter's
    own skips (camera, binoculars, fishing rod...), and _KEYWORD_SKIP.

    Label priority: _LABEL_OVERRIDE, then the export's display string, then the
    de-camel-cased EDID. `named` reports which keywords the game itself names.
    """
    claimed = {kw for _, kws, _ in KEYWORD_GROUPS for kw in kws}

    seen = set()
    for _, kws in weapons:
        seen.update(k for k in kws if k.startswith("WeaponType"))
    seen.update(k for k in by_keyword if k.startswith("WeaponType"))

    out = []
    for kw in sorted(seen - claimed - spin._WEAP_SKIP_KEYWORDS - _KEYWORD_SKIP):
        override = _LABEL_OVERRIDE.get(kw)
        label = override or display.get(kw) or decamel(kw)
        out.append((label, [kw], bool(display.get(kw)) and not override))
    return out


def build_groups(weapons, by_keyword, display):
    discovered = discover_groups(weapons, by_keyword, display)
    print(f"  Curated groups: {len(KEYWORD_GROUPS)}, discovered: {len(discovered)}")

    groups = []
    for label, keywords, named in list(KEYWORD_GROUPS) + discovered:
        wanted = set(keywords)
        members = [name for name, kws in weapons if kws & wanted]
        if not members:
            # No live weapon carries it. For a curated keyword that means it was
            # probably renamed in the export and wants looking at; for a
            # discovered one it is routine -- WeaponTypeLaserMusket exists only
            # on zzz_LaserMusket, which the cut filter drops. Either way an
            # expand that answers "no weapons" is not worth rendering.
            where = "group" if (label, keywords, named) in KEYWORD_GROUPS else "discovered keyword"
            print(f"  [WARN] {where} '{label}' ({'/'.join(keywords)}) matched no live weapon"
                  f" -- skipped", file=sys.stderr)
            continue
        chals = challenges_for(keywords, by_keyword)
        groups.append({
            "key": slugify(label),
            "label": label,
            "keyword": " / ".join(keywords),
            "named": named,
            "count": len(members),
            "weapons": members,
            "challengeCount": len(chals),
            "challenges": chals,
        })
    groups.sort(key=lambda g: g["label"].lower())
    return groups


def main():
    print("=" * 60)
    print("  Building Weapon Keyword Index JSON")
    print(f"  Mode: {'PTS' if PTS else 'LIVE'}")
    print("=" * 60)

    weapons, display, source = collect_weapons()
    by_keyword, chal_source = collect_challenges()
    groups = build_groups(weapons, by_keyword, display)

    output = {
        "groups": groups,
        "_meta": {
            "built": date.today().isoformat(),
            "source": source,
            "challengeSource": chal_source,
            "groupCount": len(groups),
            "weaponCount": len(weapons),
            "challengeCount": sum(g["challengeCount"] for g in groups),
        },
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "weapon_keywords.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n  Written to: {out_path}")
    print(f"  Groups: {len(groups)}, Weapons: {len(weapons)}")
    for g in groups:
        w, c = g["count"], g["challengeCount"]
        print(f"    {g['label']:<22} {w:>4} weapon{'' if w == 1 else 's':<2}"
              f"  {c:>3} challenge{'' if c == 1 else 's'}")
    print("  Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
