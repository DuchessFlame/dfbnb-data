#!/usr/bin/env python3
r"""
plan_display_names.py — the "prettify" rule for plan row titles.

Bethesda names a weapon paint either way round. Most read model-first, the way a
player says it:

    Plan: Overgrown Assault Rifle Paint
    Plan: Showstopper Shotgun Paint

but a slice of them lead with the weapon instead, which buries the part that
identifies the paint and sorts them under the gun:

    Plan: Gatling Laser Valkyrie Paint
    Plan: Laser Gun V63-OLGA Paint

This module flips that slice so every paint reads the same way -- model name
first, then the weapon, then Paint:

    Plan: Valkyrie Gatling Laser Paint
    Plan: V63-OLGA Laser Gun Paint

WHAT IT TOUCHES
---------------
Only rows whose name ends in "Paint" or "Skin", and only when the name STARTS
with a real weapon name. Everything else is left exactly as the game wrote it.

The original name is never lost: this writes `display_name`, and the renderer
already prints the untouched `name` as "Plan name" in Technical whenever the two
differ (df-bnb-plan-checklists.js). Nothing keys off the title, so no progress
tick moves.

WEAPON NAMES COME FROM THE GAME
-------------------------------
The list is the FULL of every WEAP record, not a hand-written list, so a weapon
added in a patch is recognised the moment its export lands. EXTRA_WEAPONS only
covers spellings the plan names use that no WEAP FULL carries ("10mm SMG" is
"10mm Submachine Gun" in WEAP).

THE TRAP, AND THE VETO
----------------------
A paint can be NAMED after a weapon. "Plan: Laser Chainsaw Paint" is the "Laser
Chainsaw" paint for a Chainsaw -- flipping it would produce "Chainsaw Laser
Paint", which is wrong twice over. So when the plan's EditorID names a weapon
(`Recipe_mod_Chainsaw_Paint_LaserChainsaw` -> Chainsaw) and that weapon is NOT
the prefix we matched, the flip is vetoed. Where the EditorID names no weapon at
all (`RD01_Recipe_Weapon_Ranged_ValkyriePaint`) there is nothing to contradict
the name, and the flip goes ahead.
"""

import csv
import glob
import os
import re

# Spellings the plan names use that no WEAP FULL carries.
EXTRA_WEAPONS = [
    "10mm SMG",        # WEAP calls it "10mm Submachine Gun"
    "MG42",            # WEAP: "MG42 Light Machine Gun"
]

# The word the game ends the title with is kept as it wrote it -- a couple of
# power armour paints are "Coating", not "Paint".
_TAIL_RE = re.compile(r"^(?P<body>.*\S)\s+(?P<tail>Paint|Skin|Coating)$")

# The weapon slot in a plan EditorID. The trailing underscore is required: it is
# what separates `..._Weapon_Ranged_CompoundBow_BurningLovePaint` (slot present,
# CompoundBow) from `..._Weapon_Ranged_TheFixerPaint` (no slot -- the whole tail
# is the paint's name).
# "<Weapon> Skin - <Name>" -- the one title in the roster written this way.
_DASH_RE = re.compile(
    r"^(?P<weapon>.+?)\s+(?P<tail>Paint|Skin|Coating)\s+-\s+(?P<model>\S.*)$")

# ARMOUR. Power armour and armour sets are named the same way round as weapons
# -- "T-60 BOS Elder Paint" is the BOS Elder paint for a T-60, "X-01 Military
# Paint" the Military paint for an X-01 -- so the same flip applies.
#
# The model is read from the EditorID slot, exactly as for weapons:
#   recipe_mod_PowerArmor_T60_Material_Paint_BOSElder   -> T60
#   RD01_Recipe_Mod_Armor_Scout_Material_Paint_...      -> Scout
# The slot word is only trusted when it is a real model: these fillers sit in
# the same position on paints that apply to every frame
# (`..._PowerArmor_Material_Paint_HotRod03_Flames`, `..._PowerArmor_ALL_...`).
_ARMOUR_SLOT_RE = re.compile(r"(?:PowerArmor|Armor|Armour)_([A-Za-z0-9\-]+)_", re.I)
_ARMOUR_SLOT_SKIP = {"material", "paint", "skin", "mod", "all", "misc", "recipe",
                     "weapon", "torso", "helmet", "arms", "legs", "jetpack"}

# Armour SETS, whose EditorIDs do not always carry the set in the slot
# ("Recipe_mod_MetalArmor_Paint_Snowflakes..."). Short, closed list: these are
# the game's armour sets, and a set only flips when the title STARTS with it, so
# "Atom Cats Leather Armor Paint" -- already model-first -- is untouched.
ARMOUR_SETS = [
    "Combat Armor", "Metal Armor", "Leather Armor", "Scout Armor",
    "Marine Armor", "Robot Armor", "Wood Armor", "Trapper Armor",
    "Raider Armor", "Forest Armor", "Urban Scout Armor",
]

# Words that belong to the make when they trail the model ("T-51" + "Power
# Armor"), so the flip keeps them together instead of stranding them in front of
# the paint name.
_MAKE_TAILS = ("Power Armor", "Power Armour", "Armor", "Armour")


# An EditorID that states outright that the row is a paint or skin.
_EDID_PAINT_RE = re.compile(r"_(?:Paint|Skin)_", re.I)

_SLOT_RE = re.compile(
    r"(?:^|_)(?:mod|Recipe_Weapon_Ranged|Recipe_Weapon_Melee|Weapon_Ranged|Weapon_Melee)"
    r"_([A-Za-z0-9\-\.]+)_", re.I)
_PLAN_RE = re.compile(r"^(Plan|Recipe):\s*", re.I)


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


class WeaponNames:
    """Weapon FULLs from the newest WEAP export, longest first."""

    def __init__(self, tsv_dir="tsv", newest=None):
        self.names = []
        path = None
        if newest:
            path = newest("WEAP_Export_*_Base.tsv", tsv_dir)
        if not path:
            found = sorted(glob.glob(os.path.join(tsv_dir, "WEAP_Export_*_Base.tsv")))
            path = found[-1] if found else None
        self.export = os.path.basename(path) if path else ""
        seen = set()
        if path:
            with open(path, encoding="utf-8", errors="replace", newline="") as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    edid = (row.get("WEAP_EDID") or "")
                    full = (row.get("WEAP_FULL") or "").strip()
                    low = edid.lower()
                    # Creature attacks, turrets and dev stubs are not things a
                    # plan is ever named after.
                    if not full or len(full) < 3:
                        continue
                    if low.startswith(("cr", "zzz", "test")) or "turret" in low or "dummy" in low:
                        continue
                    if full.lower() in seen:
                        continue
                    seen.add(full.lower())
                    self.names.append(full)
        for extra in EXTRA_WEAPONS:
            if extra.lower() not in seen:
                seen.add(extra.lower())
                self.names.append(extra)
        # Longest first: "Gatling Laser" has to win over "Gatling".
        self.names.sort(key=lambda s: (-len(s), s))
        self._by_key = {}
        for n in self.names:
            self._by_key.setdefault(_norm(n), n)

    def leading_weapon(self, body):
        """The weapon name `body` starts with, or None."""
        low = body.lower()
        for name in self.names:
            if low.startswith(name.lower() + " "):
                return name
        return None

    def weapon_in_edid(self, edid):
        """The weapon an EditorID names in its SLOT position, or None.

        Position is everything. A plan EditorID reads
        `<prefix>_mod_<Weapon>_Paint_<PaintName>` or
        `<prefix>_Recipe_Weapon_Ranged_<Weapon>_<PaintName>`, so the weapon --
        when it is stated at all -- sits in one place, between the marker and
        the next underscore.

        Scanning the whole string instead was tried and is wrong: paint names
        are themselves weapon names often enough to poison it. "Burning Love",
        "The Fixer" and "Laser Chainsaw" are all real WEAP records, so a
        substring scan read the paint's name as the weapon and refused to flip
        three titles that needed it.
        """
        m = _SLOT_RE.search(str(edid or ""))
        if not m:
            return None
        key = _norm(m.group(1))
        return self._by_key.get(key)


def _armour_make(core, edids):
    """(make, model) when `core` starts with the armour this row paints."""
    for set_name in ARMOUR_SETS:
        if core.lower().startswith(set_name.lower() + " "):
            model = core[len(set_name):].strip()
            return (set_name, model) if model else None

    token = None
    for edid in edids:
        for m in _ARMOUR_SLOT_RE.finditer(str(edid or "")):
            word = m.group(1)
            if _norm(word) and _norm(word) not in _ARMOUR_SLOT_SKIP:
                token = _norm(word)
                break
        if token:
            break
    if not token:
        return None

    words = core.split()
    for k in (1, 2, 3):
        if k > len(words):
            break
        cand = " ".join(words[:k])
        key = _norm(cand)
        # "T-51b" against a T51 slot: the game writes the b in the name only.
        if key != token and not (key.startswith(token) and len(key) - len(token) <= 1):
            continue
        rest = " ".join(words[k:])
        for tail in _MAKE_TAILS:
            if rest.lower().startswith(tail.lower() + " "):
                cand = f"{cand} {rest[:len(tail)]}"
                rest = rest[len(tail):].strip()
                break
        return (cand, rest) if rest else None
    return None


def prettify(name, plan_edid="", weapons=None, cnam_edid=""):
    """Model-first title for a weapon or armour paint, or None to leave it alone."""
    if not name or weapons is None:
        return None
    prefix = _PLAN_RE.match(name)
    lead = prefix.group(0) if prefix else ""
    body = name[len(lead):].strip()

    dash = _DASH_RE.match(body)
    m = _TAIL_RE.match(body)
    if dash:
        # "Hunting Rifle Skin - Union" -- the same title inside out.
        core, tail = f"{dash.group('weapon')} {dash.group('model')}", dash.group("tail")
    elif m:
        core, tail = m.group("body"), m.group("tail")
    elif _EDID_PAINT_RE.search(plan_edid or ""):
        # The game left the word off: "Plan: Laser Gun V63-BERTHA" is a paint --
        # its EditorID says `_Paint_` -- while every one of its siblings ends in
        # "Paint". Flip it and supply the missing word, so the set reads alike.
        core, tail = body, "Paint"
    else:
        return None

    weapon = weapons.leading_weapon(core)
    if weapon:
        model = core[len(weapon):].strip()
        if not model:
            # The whole title is the weapon ("Shock Baton Paint") -- nothing to move.
            return None
        # The veto: the EditorID gets the last word on which weapon this is.
        named = weapons.weapon_in_edid(plan_edid)
        if named and _norm(named) != _norm(weapon):
            return None
    else:
        armour = _armour_make(core, (plan_edid, cnam_edid))
        if not armour:
            return None
        weapon, model = armour

    out = f"{lead}{model} {weapon} {tail}"
    return out if out != name else None


def attach(items, tsv_dir="tsv", newest=None, stats=None):
    """Set display_name on every row the rule applies to. Idempotent."""
    stats = stats if stats is not None else {}
    weapons = WeaponNames(tsv_dir, newest)
    stats["weap_export"] = weapons.export
    stats["weapon_names"] = len(weapons.names)
    stats["changed"] = []
    stats["cleared"] = 0
    for it in items:
        name = it.get("name") or ""
        edid = (it.get("plan_item") or {}).get("edid") or ""
        cnam = (it.get("cnam") or {}).get("edid") or ""
        pretty = prettify(name, edid, weapons, cnam)
        if pretty:
            if it.get("display_name") != pretty:
                stats["changed"].append(f"{name}  ->  {pretty}")
            it["display_name"] = pretty
        elif it.get("display_name") and _TAIL_RE.match(re.sub(_PLAN_RE, "", name)):
            # A title this rule used to flip and no longer does (an export moved
            # under it) must lose the stale display_name, not keep it forever.
            it.pop("display_name", None)
            stats["cleared"] += 1
    return stats


def report(stats, stream=None):
    import sys as _sys
    stream = stream or _sys.stdout
    print(f"  [titles] weapons from {stats.get('weap_export','?')} "
          f"({stats.get('weapon_names',0)} names)", file=stream)
    changed = stats.get("changed") or []
    print(f"    flipped to model-first   {len(changed)}", file=stream)
    if stats.get("cleared"):
        print(f"    stale display_name cleared {stats['cleared']}", file=stream)
    for line in changed[:40]:
        print(f"      {line}", file=stream)
    if len(changed) > 40:
        print(f"      ... and {len(changed) - 40} more", file=stream)
