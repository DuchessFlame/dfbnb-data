#!/usr/bin/env python3
"""
add_weapon_groups.py — group the weapon plan checklist by weapon.

WHY THIS IS A SEPARATE ENRICHER, NOT PART OF THE MAIN BUILD
-----------------------------------------------------------
A full plan_master build is ~80 minutes per channel because the drop-rate
resolution walks huge shared loot pools. The weapon grouping needs none of
that: it is a pure join between the rows plan_master already has and three
xEdit exports (OMOD / WEAP / COBJ), and it runs in about a second. So it
follows the same shape as add_obtain_ledger.py and add_plan_images.py — a
post-pass over a finished plan_master that can be re-run on its own whenever a
new export lands, without touching rng76.py or the drop-rate pipeline.

WHAT IT ADDS
------------
Every row with type == "weapon" gains:

  weapon_role        "base" | "mod" | "skin" | "fishing"
  weapon_group       display name of the weapon the row belongs to
                     ("Laser Gun", "Sheepsquatch Club", ...)
  weapon_group_key   slug of the same, for DOM ids and stable sorting
  weapon_group_solo  true when the group is this one plan and nothing else
                     (grenades, mines, thrown traps — they have no mods and no
                     skins, and the page gives each its own root expand)

Rows that are fishing-rod equipment get weapon_role "fishing". They are NOT
deleted: the renderer skips them so they leave /df/plan-checklists/weapon/,
and they stay in the data so the fishing pages can pick them up later. Nothing
else in plan_master is touched — other pages read these rows unchanged.

HOW A ROW FINDS ITS WEAPON
--------------------------
plan_master already records what each plan's recipe creates, in `cnam`:

  cnam.sig == "WEAP"   the plan makes the weapon itself      -> base
  cnam.sig == "OMOD"   the plan makes a mod for a weapon     -> mod or skin

For an OMOD the weapon is not a field on the record, so it is resolved by
matching EditorID segments — OMOD EditorID first ("mod_LaserGun_SCOPE_..."),
then the COBJ's ("co_mod_PumpActionShotgun_Weapon_Paint_Tornado"), then the
plan's own, then the OMOD's MNAM target keyword (ma_LaserGun) as a last
resort. Segment matching, not substring: "recipe_mod_PipeGun_Receiver_
AmmoConv45" must not match the ".44" family, and it does not, because 45 and
44 are different segments.

The family names themselves come from the physical weapon plans wherever
possible ("Plan: Laser Gun" -> "Laser Gun"), because those are the names a
player actually reads. WEAP FULL is the fallback and is often abbreviated
("Laser", "Rod"), and a small alias table covers the places the game files
spell a weapon differently from its in-game name (HandMadeGun -> Handmade
Rifle, Meltdown -> V63 Laser Carbine, LightningGun -> Tesla Rifle).

Usage:
    python3 src/add_weapon_groups.py src/plan-system/plan_master.json
    python3 src/add_weapon_groups.py src/plan-system/plan_master.json dist/plan_master.json
    python3 src/add_weapon_groups.py --report-only src/plan-system/plan_master.json
"""

from __future__ import annotations

import argparse
import collections
import csv
import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TSV = os.path.join(ROOT, "tsv")

import plan_fishing_rod     # the one definition of what counts as rod gear

SCHEMA = 1  # bump when the emitted fields change shape


# ─── TSV pickup ──────────────────────────────────────────────────────────────
# Newest by mtime, same auto-pickup rule build_pennants_json.py uses: drop a
# fresh export in tsv/ and re-run, no code change.

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}

_STAMP = re.compile(r"_([A-Za-z]{3,9})_(\d{4})", re.I)


def _stamp(path: str):
    """(year, month) parsed out of "..._September_2026.tsv", else (0, 0)."""
    m = _STAMP.search(os.path.basename(path))
    if not m:
        return (0, 0)
    return (int(m.group(2)), MONTHS.get(m.group(1)[:3].lower(), 0))


def newest(pattern: str, exclude: str = "") -> str:
    """Newest export matching `pattern`.

    The month in the filename decides, not the mtime. Re-downloading an older
    export, or a sync touching every file at once, leaves several with the same
    mtime — which is exactly the state tsv/ is in: COBJ_Export_May_2026.tsv and
    COBJ_Export_July_2026.tsv carry an identical timestamp, so an mtime sort
    picked whichever glob returned first and silently used the May data. mtime
    is kept only as the tie-break for files with no month in the name.
    """
    hits = glob.glob(os.path.join(TSV, pattern))
    if exclude:
        rx = re.compile(exclude, re.I)
        hits = [h for h in hits if not rx.search(os.path.basename(h))]
    if not hits:
        # RuntimeError, not SystemExit: build_plan_obtain_json.py wraps attach()
        # in `except Exception` to keep an 80-minute build alive, and SystemExit
        # would sail straight through that and kill it.
        raise RuntimeError(f"[add_weapon_groups] no TSV matching {pattern} in {TSV}")
    return sorted(hits, key=lambda h: (_stamp(h), os.path.getmtime(h)), reverse=True)[0]


def read_tsv(path: str):
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            yield row


# ─── EditorID segmentation ───────────────────────────────────────────────────
# "Recipe_Weapon_Ranged_10mmSMG" -> ["recipe","weapon","ranged","10mm","smg"]
# Splitting on camelCase as well as underscores is what lets a family be
# matched as whole segments instead of a substring.

_SEG = re.compile(r"[A-Z]+(?![a-z])|[A-Z][a-z0-9]*|[a-z0-9]+")


def segs(s: str):
    return [x.lower() for x in _SEG.findall(s or "")]


def slug(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", (s or "").lower())).strip("-")


# Editor-only records: never let one name a family or win a lookup.
BAD_EDID = re.compile(r"^(zzz|DEL_|DELETED|CUT_|DEBUG)|NONPLAYABLE|_Test_", re.I)

# ma_ keywords that sit on many unrelated weapons ("can this take paint?",
# "is this legendary-craftable?"). Useful to the game, useless for grouping —
# ma_CanHavePaint alone resolves to whichever weapon happens to be first.
GENERIC_KW = re.compile(
    r"^ma_(CanHave|Gun_Appearance|Melee_Appearance|legendarycrafting|Misc_|UnModdable"
    r"|1hMelee|BallisticGun$|CombatGun$|Handmade_Nonshotgun$|PipeGun$|Flamer_CanHave"
    r"|ChainsawCanHave|CompoundBow_SightMods$|Knife_Blades$|SingleActionRevolver_44$"
    r"|GaussRifle_Presidential$|JunkJet$|LaserMusket$)", re.I)

# Where the game files and the in-game name disagree, or where the WEAP FULL is
# an abbreviation a player would not recognise. Each of these was read off the
# records, not guessed.
ALIASES = {
    "HandmadeGun": "Handmade Rifle", "HandMadeGun": "Handmade Rifle",
    "Handmade": "Handmade Rifle", "HandmadeRifle": "Handmade Rifle",
    "Meltdown": "V63 Laser Carbine", "Stormcutter": "V63 Zweihaender",
    "LightningGun": "Tesla Rifle", "ThirstZapper": "Thirst Zapper",
    "CommieWhacker": "Commie Whacker", "HarpoonGun": "Harpoon Gun",
    "MrHandyBuzzBlade": "Mr. Handy Buzz Blade", "LeverGun": "Lever Gun",
    "PoleHook": "Pole Hook", "AssaultronBlade": "Assaultron Blade",
    "AssaultronHead": "Salvaged Assaultron Head",
    "DeathclawGauntlet": "Deathclaw Gauntlet", "Enclave_PlasmaGun": "Enclave Plasma Gun",
    "LucaSwitchblade": "Switchblade", "Lucaswitchblade": "Switchblade",
    "CattleProd": "Cattle Prod", "Cattleprod": "Cattle Prod",
    "PaddleBall": "Paddle Ball", "MoleMinerGauntlet": "Mole Miner Gauntlet",
    "GaussShotgun": "Gauss Shotgun", "GaussMinigun": "Gauss Minigun",
    "GaussPistol": "Gauss Pistol", "GaussRifle": "Gauss Rifle",
    "CombatShotgun": "Combat Shotgun", "PumpActionShotgun": "Pump Action Shotgun",
    "DoubleBarrelShotgun": "Double-Barrel Shotgun", "SubmachineGun": "Submachine Gun",
    "LaserGun": "Laser Gun", "PlasmaGun": "Plasma Gun",
    "UltraciteLaserGun": "Ultracite Laser Gun", "Ultracite_GatlingLaser": "Ultracite Gatling Laser",
    "RadiumRifle": "Radium Rifle", "RailwayRifle": "Railway Rifle",
    "10mm": "10mm Pistol", "10mmSMG": "10mm Submachine Gun", "44": ".44 Revolver",
    "Minigun": "Minigun", "Minigun_Vertibird": "Minigun", "MissileLauncher": "Missile Launcher",
    "BOSRocketLauncher": "Hellstorm Missile Launcher", "bospistol": "Crusader Pistol",
    "CombatRifle": "Combat Rifle", "CombatRifle_Fixer": "The Fixer", "TheFixer": "The Fixer",
    "Shishkebab": "Shishkebab", "Knife": "Combat Knife", "BowieKnife": "Bowie Knife",
    "WoodCuttingAxe": "Wood Axe", "FireAxe": "Fire Axe", "GrognakAxe": "Grognak's Axe",
    "AutoAxe": "Auto Axe", "Hatchet": "Hatchet", "Pickaxe": "Pickaxe", "PickAxe": "Pickaxe",
    "FishingRod": "Fishing Rod", "PipeGun": "Pipe Gun", "GulperSmacker": "Gulper Smacker",
    "PipeBoltAction": "Pipe Bolt-Action", "PipeRevolver": "Pipe Revolver",
    "PipeSyringer": "Syringer", "PipeWrench": "Pipe Wrench",
    "AlienBlaster": "Alien Blaster", "AlienRifle": "Alien Disintegrator",
    "GammaGun": "Gamma Gun", "BoxingGlove": "Boxing Glove", "BaseballBat": "Baseball Bat",
    "ChineseOfficerSword": "Chinese Officer Sword", "RevolutionarySword": "Revolutionary War Sword",
    "SheepsquatchClub": "Sheepsquatch Club", "SheepSquatchClub": "Sheepsquatch Club",
    "SheepsquatchStaff": "Sheepsquatch Staff", "PepperShaker": "Pepper Shaker",
    "GatlingLaser": "Gatling Laser", "GatlingPlasma": "Gatling Plasma", "GatlingGun": "Gatling Gun",
    "HuntingRifle": "Hunting Rifle", "AssaultRifle": "Assault Rifle",
    "SingleActionRevolver": "Single Action Revolver",
    "BlackPowder_Rifle": "Black Powder Rifle", "Blackpowder_Pistol": "Black Powder Pistol",
    "BearArm": "Bear Arm", "WarGlaive": "War Glaive", "WarDrum": "War Drum",
    "WarShrike": "War Shrike", "PlasmaCaster": "Plasma Caster", "PlasmaSword": "Plasma Cutter",
    "TeslaCannon": "Tesla Cannon", "Cryolator": "Cryolator", "Broadsider": "Broadsider",
    "Fatman": "Fat Man", "Crossbow": "Crossbow", "CompoundBow": "Compound Bow",
    "RegularBow": "Bow", "Cremator": "Cremator", "M79": "M79 Grenade Launcher",
    "MG42": "MG42 Light Machine Gun", "Nitro": "Dom Pedro", "Chainsaw": "Chainsaw",
    "Chainsaw_76": "Chainsaw", "ElectroEnforcer": "Electro Enforcer",
    "AutoGrenadeLauncher": "Auto Grenade Launcher", "Camera_SnapMatic": "ProSnap Deluxe Camera",
    "SuperSledge": "Super Sledge", "Sledgehammer": "Sledgehammer",
    "PowerFist": "Power Fist", "Powerfist": "Power Fist", "Machete": "Machete",
    "MeatTenderizer": "Tenderizer", "MeatHook": "Meat Hook", "GolfClub": "Golf Club",
    "WalkingCane": "Walking Cane", "Switchblade": "Switchblade",
    "CultistBlade": "Cultist Blade", "CultistDagger": "Cultist Dagger",
    "DeathTambo": "Death Tambo", "Ripper": "Ripper", "Spear": "Spear",
    "Pitchfork": "Pitchfork", "Tomahawk": "Tomahawk", "Shovel": "Shovel", "Sickle": "Sickle",
    "Board": "Board", "LeadPipe": "Lead Pipe", "PoolCue": "Pool Cue",
    "RollingPin": "Rolling Pin", "TireIron": "Tire Iron", "Baton": "Baton",
    "Knuckles": "Knuckles", "BoneClub": "Bone Club", "BoneHammer": "Bone Hammer",
    "Gauntlet": "Gauntlet", "Drill": "Drill", "SkiSword": "Ski Sword",
    "GuitarSword": "Guitar Sword", "HogSplitter": "Hog Splitter",
    "ShepherdsCrook": "Shepherd's Crook", "CroquetMallet": "Croquet Mallet",
    "ATX_CroquetMallet": "Croquet Mallet", "binoculars": "Binoculars",
    "50CalMachineGun": ".50 Cal Machine Gun", "CircuitBreaker": "Circuit Breaker",
    "Flamer": "Flamer",
}

# A mod is a "skin" when it changes how the weapon looks and nothing else.
SKIN_ATTACH = {"ap_gun_Appearance", "ap_melee_Appearance"}
SKIN_WORDS = re.compile(r"(material|paint|skin)", re.I)

# Fishing-rod equipment (reels, drags, handles, bearings, hooks, gear ratios)
# leaves the weapon page for /df/plan-checklists/fishing-rod/. What counts is
# plan_fishing_rod's call, not a second copy of the rule here — one definition,
# so the weapon page and the fishing page can never disagree about a row.

# The plan's own EditorID prefix, so "Recipe_Weapon_Ranged_LaserGun" yields the
# family token "LaserGun".
PLAN_PREFIX = re.compile(r"^(?:POST_|zzz_?|ZZZ_?|CUT_|DEL_)*recipe_weapon_(?:ranged|melee|thrown)_", re.I)

# Tinkers recipes are grenades, mines and thrown traps - things you throw once,
# with no mods and no skins. Each gets its own root expand, so they must never
# be pulled into a weapon family by a word they happen to share ("Floater
# Flamer Grenade" is not a Flamer mod).
STANDALONE = re.compile(r"(^|_)recipe_tinkers_", re.I)

# Paints sold as a loadout bundle apply across several weapons. The OMOD the
# recipe happens to point at is only one of them, so filing the plan under that
# weapon would be a guess dressed up as a fact. They get their own group.
LOADOUT = re.compile(r"mod_Loadouts_", re.I)


class Resolver:
    def __init__(self, items):
        self.weap = {}
        self.kw2weap = collections.defaultdict(list)
        self.omod = {}
        self.plan2cobj = {}
        self.families = {}   # tuple(segments) -> display name
        self.sources = []

        self._load_weap()
        self._load_omod()
        self._load_cobj()
        self._seed_families(items)
        self._order = sorted(self.families, key=len, reverse=True)

    # -- exports --------------------------------------------------------
    def _load_weap(self):
        path = newest("WEAP_Export_*_Base.tsv")
        self.sources.append(os.path.basename(path))
        for r in read_tsv(path):
            fid = (r.get("WEAP_FormID") or "").strip()
            if not fid:
                continue
            self.weap[fid] = {
                "edid": (r.get("WEAP_EDID") or "").strip(),
                "full": (r.get("WEAP_FULL") or "").strip(),
            }
            for kw in re.findall(r"\b(ma_[A-Za-z0-9_]+)", r.get("Keywords") or ""):
                self.kw2weap[kw].append(fid)

    def _load_omod(self):
        # ..._Properties.tsv is a companion sheet, not the record list.
        path = newest("OMOD_Export_*.tsv", exclude=r"_Properties\.tsv$")
        self.sources.append(os.path.basename(path))
        for r in read_tsv(path):
            fid = (r.get("OMOD_FormID") or "").strip()
            if not fid:
                continue
            self.omod[fid] = {
                "edid": (r.get("OMOD_EDID") or "").strip(),
                "ap": (r.get("AttachPoint_EDID") or "").strip(),
                "mnam": (r.get("MNAM_TargetKWDs") or "").strip(),
            }

    def _load_cobj(self):
        path = newest("COBJ_Export_*.tsv")
        self.sources.append(os.path.basename(path))
        for r in read_tsv(path):
            gnam = (r.get("GNAM_EDID") or "").strip()
            if gnam:
                self.plan2cobj.setdefault(gnam, (r.get("COBJ_EDID") or "").strip())

    # -- family table ---------------------------------------------------
    def _add(self, token, display, force=False):
        key = tuple(segs(token))
        if not key:
            return
        if force or key not in self.families:
            self.families[key] = display

    def _seed_families(self, items):
        # 1. Physical weapon plans give the best display names, because they are
        #    the names the game prints on the plan a player picks up.
        for it in items:
            if (it.get("cnam") or {}).get("sig") != "WEAP":
                continue
            edid = (it.get("plan_item") or {}).get("edid", "")
            token = PLAN_PREFIX.sub("", edid)
            if token != edid and token and "_" not in token:
                self._add(token, re.sub(r"^Plan:\s*", "", it.get("name") or "").strip())
        # 2. ma_ keywords cover weapons with no learnable plan of their own.
        for kw, fids in self.kw2weap.items():
            if GENERIC_KW.search(kw):
                continue
            good = [self.weap[f] for f in fids
                    if not BAD_EDID.search(self.weap[f]["edid"]) and self.weap[f]["full"]]
            if good:
                self._add(kw[3:], good[0]["full"])
        # 3. Aliases win outright.
        for token, display in ALIASES.items():
            self._add(token, display, force=True)

    def find(self, text):
        """Earliest match wins, longest breaks a tie at the same position.

        Position matters more than length because an EditorID names the thing
        it belongs to before it names what it does:
        "recipe_mod_melee_Pitchfork_Flamer" is a Pitchfork mod, not a Flamer
        one, and "XPD_AC_mod_44_Ranged_Paint_RouletteRevolver" is a .44 paint,
        not a Revolver one. Scanning longest-first got both of those wrong.
        Length still decides within one position, so "Ultracite Laser Gun"
        beats the "Laser Gun" sitting inside it.
        """
        parts = segs(text)
        if not parts:
            return None
        for i in range(len(parts)):
            for key in self._order:              # already longest-first
                n = len(key)
                if i + n <= len(parts) and tuple(parts[i:i + n]) == key:
                    return key
        return None

    # -- per row --------------------------------------------------------
    def classify(self, item):
        cnam = item.get("cnam") or {}
        plan_edid = (item.get("plan_item") or {}).get("edid", "")
        omod = self.omod.get(cnam.get("formid", "")) if cnam.get("sig") == "OMOD" else None
        cobj_edid = (item.get("cobj") or {}).get("edid", "") or self.plan2cobj.get(plan_edid, "")
        attach = (omod or {}).get("ap", "")
        omod_edid = (omod or {}).get("edid", "")

        if plan_fishing_rod.role(plan_edid):
            return "fishing", None

        # A plan whose recipe creates a WEAP makes the weapon itself — unless
        # the builder already called it a mod/recipe, which is how the variant
        # weapons behave (Barbed Sheepsquatch Club is a mod you craft onto the
        # club, but the game stores each variant as its own WEAP record).
        label = item.get("category_label") or ""
        if cnam.get("sig") == "WEAP" and label == "Weapon (physical plan)":
            role = "base"
        elif SKIN_WORDS.search(plan_edid or "") or SKIN_WORDS.search(omod_edid or "") \
                or SKIN_WORDS.search(cobj_edid or "") or attach in SKIN_ATTACH:
            role = "skin"
        else:
            role = "mod"

        family = None
        if LOADOUT.search(plan_edid or ""):
            return "skin", "Weapon Loadout Paints"
        if STANDALONE.search(plan_edid or ""):
            return role, None
        if cnam.get("sig") == "WEAP":
            weapon = self.weap.get(cnam.get("formid", ""))
            for source in ((weapon or {}).get("edid", ""), plan_edid, cobj_edid):
                family = self.find(source)
                if family:
                    break
        else:
            for source in (omod_edid, cobj_edid, plan_edid):
                family = self.find(source)
                if family:
                    break
            if not family and omod:
                for kw in re.findall(r"\b(ma_[A-Za-z0-9_]+)", omod["mnam"]):
                    if GENERIC_KW.search(kw):
                        continue
                    key = tuple(segs(kw[3:]))
                    if key in self.families:
                        family = key
                        break
        return role, (self.families[family] if family else None)


def attach(items):
    """Write the weapon_* fields onto the weapon rows of a plan_master item list.

    Importable so build_plan_obtain_json.py can apply it in-process the same way
    it applies plan_images, which means a fresh 80-minute build comes out already
    grouped and no workflow has to learn a new step. Returns a stats dict, or an
    empty one when the list holds no weapon plans.
    """
    weapons = [i for i in items if (i.get("type") or "").lower() == "weapon"]
    if not weapons:
        return {}

    res = Resolver(weapons)
    tally = collections.Counter()
    # Pass 1: role + family for every row.
    for it in weapons:
        role, family = res.classify(it)
        # A plan with no family is a weapon that stands alone — a grenade, a
        # mine, a thrown trap. It becomes its own group, named after itself.
        if not family:
            family = re.sub(r"^Plan:\s*", "", it.get("name") or it.get("id") or "").strip()
            tally["solo"] += 1
        it["weapon_role"] = role
        it["weapon_group"] = None if role == "fishing" else family
        it["weapon_group_key"] = None if role == "fishing" else slug(family)
        tally[role] += 1

    # Pass 2: a group holding exactly one plan and no mods or skins is flagged,
    # so the page can render it without pretending it has sections.
    sizes = collections.Counter(i["weapon_group_key"] for i in weapons if i.get("weapon_group_key"))
    for it in weapons:
        it["weapon_group_solo"] = bool(it.get("weapon_group_key")) and sizes[it["weapon_group_key"]] == 1

    return {"groups": len(sizes), "tally": dict(tally), "sources": res.sources}


def report(stats, where=""):
    """One-line build log, in the same shape plan_images.report prints."""
    if not stats:
        print(f"[add_weapon_groups] {where}no weapon rows, nothing to do")
        return
    print(f"[add_weapon_groups] {where}{stats['groups']} weapon groups from "
          + ", ".join(stats["sources"]))
    print("  rows: " + ", ".join(f"{k}={v}" for k, v in sorted(stats["tally"].items())))


def enrich(path, report_only=False):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    stats = attach(doc.get("items") or [])
    if not stats:
        print(f"[add_weapon_groups] {path}: no weapon rows, nothing to do")
        return doc, {}

    doc["weapon_groups_schema"] = SCHEMA
    doc["weapon_groups_sources"] = stats["sources"]
    if not report_only:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=2)
    return doc, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+", help="plan_master.json file(s) to enrich in place")
    ap.add_argument("--report-only", action="store_true", help="resolve and print, write nothing")
    args = ap.parse_args()

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[add_weapon_groups] missing: {path}", file=sys.stderr)
            continue
        doc, stats = enrich(path, args.report_only)
        if not stats:
            continue
        print(f"[add_weapon_groups] {path}")
        print(f"   exports : {', '.join(stats['sources'])}")
        print(f"   groups  : {stats['groups']}")
        print(f"   rows    : " + ", ".join(f"{k}={v}" for k, v in sorted(stats["tally"].items())))
        if args.report_only:
            weapons = [i for i in doc["items"] if (i.get("type") or "").lower() == "weapon"]
            by = collections.defaultdict(lambda: collections.Counter())
            for i in weapons:
                if i["weapon_role"] == "fishing":
                    continue
                by[i["weapon_group"]][i["weapon_role"]] += 1
            for name in sorted(by):
                c = by[name]
                print(f"      {name:34s} base={c['base']} mods={c['mod']} skins={c['skin']}")


if __name__ == "__main__":
    main()
