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

Outputs:
  - dist/weapon_keywords.json

The weapon filter is IMPORTED from build_spin_wheel_json.py on purpose -- the
Challenge Roulette wheel and this page both answer "what counts as a weapon for
a challenge", and they must never disagree. Fix a bad weapon in one place.

Output shape:
  {
    "groups": [
      { "key": "automatic", "label": "Automatic", "keyword": "WeaponTypeAutomatic",
        "named": false, "count": 78, "weapons": ["10mm Submachine Gun", ...] },
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

ROOT = os.path.dirname(HERE)
PTS = "--pts" in sys.argv
OUT_DIR = os.path.join(ROOT, "dist", "pts") if PTS else os.path.join(ROOT, "dist")

# ─────────────────────────────────────────────────────────────────────────────
#  The keyword set
# ─────────────────────────────────────────────────────────────────────────────
# Every WeaponType* keyword that a player would recognise from a challenge or
# a Pip-Boy stat line. Two flavours:
#
#   named   -- the keyword carries an in-game display string in the export
#              (e.g. WeaponTypeBlunt "Blunt"). The label IS that string.
#   unnamed -- no display string, but the concept is used by challenge text or
#              perk cards (Automatic, Ranged, Melee...). Label written by hand.
#
# Deliberately excluded: the ~75 remaining WeaponType* keywords that name a
# single weapon family (WeaponTypeShishkebab, WeaponTypeFishingRod) or are
# engine plumbing (WeaponTypeNoAttack, WeaponTypeNonBullet). They'd each expand
# to a list of one and bury the useful groups.
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

    seen = {}
    for row in rows:
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
    return sorted(seen.items(), key=lambda kv: kv[0].lower()), os.path.basename(tsv_path)


def build_groups(weapons):
    groups = []
    for label, keywords, named in KEYWORD_GROUPS:
        wanted = set(keywords)
        members = [name for name, kws in weapons if kws & wanted]
        if not members:
            print(f"  [WARN] group '{label}' matched nothing -- keyword renamed?", file=sys.stderr)
            continue
        groups.append({
            "key": slugify(label),
            "label": label,
            "keyword": " / ".join(keywords),
            "named": named,
            "count": len(members),
            "weapons": members,
        })
    groups.sort(key=lambda g: g["label"].lower())
    return groups


def main():
    print("=" * 60)
    print("  Building Weapon Keyword Index JSON")
    print(f"  Mode: {'PTS' if PTS else 'LIVE'}")
    print("=" * 60)

    weapons, source = collect_weapons()
    groups = build_groups(weapons)

    output = {
        "groups": groups,
        "_meta": {
            "built": date.today().isoformat(),
            "source": source,
            "groupCount": len(groups),
            "weaponCount": len(weapons),
        },
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "weapon_keywords.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n  Written to: {out_path}")
    print(f"  Groups: {len(groups)}, Weapons: {len(weapons)}")
    for g in groups:
        print(f"    {g['label']:<14} {g['count']:>4}")
    print("  Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
