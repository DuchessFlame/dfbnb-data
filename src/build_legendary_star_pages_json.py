#!/usr/bin/env python3
r"""
build_legendary_star_pages_json.py - the five BnB "N★ Legendary Mods" pages
(29 Oct 2026, Duchess).

    /bnb/legendary-mods/1-star-legendary-mods/  ...  /5-star-legendary-mods/

One root expand per legendary mod (A-Z), each with:
    picture            the mod box the game hands you (MISC LegendaryShard_*)
    How to Obtain      learn it by scrapping (the recipe's own learn method +
                       chance), the mod box drops (every leveled list that holds
                       the box, rated by rng76), the random roll on legendary
                       items (the star pools), the Purveyor, and the unique
                       items that carry it fixed
    Outputs & Buffs    every number the effect changes: the enchantment's magic
                       effects, actor values, perk entry points, curve tables
                       (input -> output points) and when each one works
    Crafting           the mod box recipe (Tinker's Workbench) and the attach
                       recipe per item type
    Technical          every record behind it

Everything is read from the xEdit / Python exports on every build. Nothing is
typed per mod: a new mod, a changed number or a moved drop shows up after the
next export + build.

Rates: rng76 only (drop-rate-engine). The random-roll and Purveyor numbers are
the legendary star pools (OMOD modcol "Random Legendary Mod" includes), one
pick from the mods that fit the item - 1 / pool size - not leveled-list maths.

Inputs (newest of each in --data-dir; --pts falls back to tsv/ for anything
the PTS folder doesn't have):
    MISC  COBJ  COBJ2  OMOD (+_Properties)  ENCH  MGEF  PERK  PERKEP  AVIF
    GLOB  GMST  CURV (_CURV + _POINTS)  LGDI  LVLI (List/Entries/Math/Refs)
    NPC  GMRW  BUFFCOND (tools/esm_buff_guides_extract.py: ENCH + MGEF
    conditions)  KYWD  dist/unique_weapons_armour/unique_weapons_armour.json
Outputs:
    dist/legendary_star_pages/{1..5}-star.json   (dist/pts/... with --pts)

Usage:
    python src/build_legendary_star_pages_json.py
    python src/build_legendary_star_pages_json.py --pts
"""
from __future__ import annotations

import argparse
import collections
import csv
import datetime
import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

import tsv_source                      # noqa: E402
import rng76                           # noqa: E402
import rng76_fast                      # noqa: E402  (skips branches that can't hold the mod box; bit-identical)
import build_buff_guides_json as BG    # noqa: E402  (condition words)
import plan_sources                    # noqa: E402  (raid encounter names)

TAG = "[legendary_star_pages]"
STAR = "★"
DEV_RE = re.compile(r"^(zzz|DEL_|CUT_|POST_|test|DEBUG|Babylon)", re.I)
FID_RE = re.compile(r"\[([A-Z_]{4}):([0-9A-Fa-f]{8})\]")
REF_RE = re.compile(r"^([0-9A-Fa-f]{8}):([^:]*):([A-Z_]{4})$")

# Where the item pictures live on the server (one per mod-box look).
IMG_DIR = "/wp-content/uploads/guide-images/legendary-mods/"

# Item kinds, from the mod's target keyword (OMOD MNAM).
KIND_BY_KW = {
    "ma_legendarycrafting_weapon": ("weapon", "Weapons"),
    "ma_legendarycrafting_weaponranged": ("ranged", "Ranged weapons"),
    "ma_legendarycrafting_weaponmelee": ("melee", "Melee weapons"),
    "ma_legendarycrafting_armor": ("armour", "Armour"),
    "ma_legendarycrafting_powerarmor": ("powerarmour", "Power Armour"),
}
# Which kinds a real item accepts (a ranged gun takes "weapon" and "ranged" mods).
ITEM_ACCEPTS = {
    "Ranged weapon": {"weapon", "ranged"},
    "Melee weapon": {"weapon", "melee"},
    "Armour": {"armour"},
    "Power Armour": {"powerarmour"},
}
POOL_ITEMS = {"Weapon": ("Ranged weapon", "Melee weapon"), "Armor": ("Armour",),
              "PowerArmor": ("Power Armour",)}
WORKBENCH = {
    "Workbench_Crafting_Tinkers": "Tinker's Workbench",
    "Workbench_Crafting_Weapon": "Weapons Workbench",
    "Workbench_Crafting_Armor": "Armour Workbench",
    "Workbench_Crafting_PowerArmor": "Power Armour Station",
}

# Perk entry points in plain words: (what it changes, how the number is used).
ENTRY_WORDS = {
    "Mod Weapon Attack Damage": "Your damage (its own multiplier, on top of your damage bonus)",
    "Mod Weapon DMG Bonus Mult": "Your damage bonus (adds to your other % damage bonuses)",
    "Mod Incoming Weapon Damage": "Damage you take from weapons",
    "Mod Incoming Damage": "Damage you take",
    "Mod Incoming Explosion Damage": "Damage you take from explosions",
    "Mod Incoming Limb Damage": "Limb damage you take",
    "Mod Incoming Spell Magnitude": "Strength of effects cast on you",
    "Mod Target Damage Resistance": "Your target's damage resistance",
    "Mod Weapon Reload Speed": "Reload speed",
    "Mod Attack Speed": "Attack speed",
    "Mod Action Point Cost": "Action Point cost",
    "Mod Weapon Attack Action Point Cost": "Action Point cost of attacks",
    "Mod VATS Hit Chance": "V.A.T.S. hit chance",
    "Mod Critical Charge Mult On Hit": "Critical meter fill per hit",
    "Mod Critical Damage": "Critical damage",
    "Mod Item Weight": "Item weight",
    "Mod Item Condition Loss": "How fast it wears down",
    "Mod Ammo Health Mult": "Ammo / fusion core life",
    "Mod Spell Magnitude": "Strength of effects you use",
    "Mod Spell Duration": "How long effects you use last",
    "Mod Player Explosion Damage": "Your explosion damage",
    "Mod Explosion Radius": "Explosion size",
    "Mod Sneak Attack Mult": "Sneak attack damage",
    "Mod Bash Damage": "Bash damage",
    "Mod Power Attack Damage": "Power attack damage",
    "Mod Gun Range Mult": "Weapon range",
    "Mod Bullet Count": "Bullets per shot",
    "Mod Ammo Count": "Ammo per shot",
    "Mod Recoil": "Recoil",
    "Mod Weapon Accuracy": "Accuracy",
    "Mod Lockpick Sweet Spot": "Lockpick sweet spot",
}
FUNC_WORDS = {
    "Multiply Value": "multiply",
    "Add Value": "add",
    "Set Value": "set",
    "Multiply 1 + Actor Value Mult": "multiply by (1 + value)",
    "Add Actor Value Mult": "add (value x actor value)",
    "Multiply Actor Value Mult": "multiply (value x actor value)",
    "Add Range To Value": "add a random amount",
    "Multiply Range To Value": "multiply by a random amount",
    "Absolute Value": "absolute",
}
# Actor values a reader knows by a better name than the game's.
AV_WORDS = {
    "HealthPercent": "Your health (%)",
    "STAT_DmgAll": "Damage bonus (all damage)",
    "ActionPointsRate": "Action Point regen",
    "ArmorPenetration": "Armour penetration (ignores target armour)",
    "Invisibility": "Cloaked (invisible)",
    "SpeedMult": "Move speed",
    "CarryWeight": "Carry weight",
}

# Holder labels for mod-box drop routes that are game systems, not a place.
SYSTEM_LABEL = {
    "LGDI": None,  # filled from the LGDI (legendary creature loot)
}


def warn(msg):
    print(f"{TAG} WARNING: {msg}", file=sys.stderr)


def q(v):
    return (v or "").strip().strip('"').strip()


def fnum(v, d=None):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return d


def fmt(v):
    if v is None:
        return ""
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return ("%.4f" % v).rstrip("0").rstrip(".")


def pct(p):
    """0..1 -> '12.3456%' (the site's 4-decimal style)"""
    return "%.4f%%" % (p * 100.0)


def fid_in(text, sig=None):
    for m in FID_RE.finditer(text or ""):
        if sig is None or m.group(1) == sig:
            return m.group(2).upper()
    return ""


def is_dev(edid):
    return bool(DEV_RE.match(edid or "")) or "_DONOTUSE" in (edid or "").upper() or "PTS_" in (edid or "")


def stars_of(full):
    return (full or "").count("¬")


def clean_name(full):
    return re.sub(r"^[¬\s]+", "", full or "").strip()


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

class Src:
    def __init__(self, data_dir, pts):
        self.data_dir, self.pts = data_dir, pts
        self.live_dir = os.path.join(REPO, "tsv")
        self.used = {}

    def path(self, pattern, exclude=None, required=True):
        def find(d):
            c = [p for p in glob.glob(os.path.join(d, pattern))
                 if not (exclude and any(x in os.path.basename(p) for x in
                                         ([exclude] if isinstance(exclude, str) else exclude)))]
            c = [p for p in c if os.path.isfile(p)]
            return max(c, key=tsv_source.export_key) if c else None
        p = find(self.data_dir)
        if not p and self.pts:
            p = find(self.live_dir)
        if not p and required:
            raise SystemExit(f"{TAG} FATAL: no {pattern} in {self.data_dir}")
        if p:
            self.used[pattern] = os.path.basename(p)
        return p

    def rows(self, pattern, exclude=None, required=True, cols=None, where=None):
        """rows as dicts holding only `cols` (the wide exports run to hundreds of
        reference columns; reading them whole runs the PC's Linux shell out of
        memory). `where(row_dict)` drops rows early."""
        p = self.path(pattern, exclude, required)
        if not p:
            return []
        out = []
        with open(p, encoding="utf-8-sig", errors="replace", newline="") as f:
            rd = csv.reader(f, delimiter="\t")
            head = next(rd, [])
            if cols is None:
                idx = list(enumerate(head))
            else:
                want = set(cols)
                idx = [(i, h) for i, h in enumerate(head) if h in want]
            for r in rd:
                d = {h: (r[i] if i < len(r) else "") for i, h in idx}
                if where is None or where(d):
                    out.append(d)
        return out

    def stream(self, pattern, exclude=None, required=True):
        """raw rows, one at a time (header first)"""
        p = self.path(pattern, exclude, required)
        if not p:
            return
        with open(p, encoding="utf-8-sig", errors="replace", newline="") as f:
            for r in csv.reader(f, delimiter="\t"):
                yield r

    def lists(self, pattern, exclude=None, required=True):
        return list(self.stream(pattern, exclude, required))


class Data:
    def __init__(self, src):
        S = self.S = src
        # names for anything with an EDID
        self.full_by_edid, self.full_by_fid = {}, {}
        for pat, ex, fcol, ecol, ncol in (
                ("MISC_Export_*.tsv", None, "FormID", "EDID", "FULL"),
                ("ALCH_Export_*.tsv", "_Effects", "ALCH_FormID", "ALCH_EDID", "ALCH_FULL"),
                ("AMMO_Export_*.tsv", None, "FormID", "EDID", "FULL"),
                ("BOOK_Export_*.tsv", "_Locations", "BOOK_FormID", "BOOK_EDID", "BOOK_FULL"),
                ("CMPO_Export_*.tsv", None, "CMPO_FormID", "CMPO_EDID", "FULL")):
            for r in S.rows(pat, ex, required=False, cols=[fcol, ecol, ncol, "FormID", "EDID", "FULL"]):
                ed = q(r.get(ecol) or r.get("EDID"))
                nm = q(r.get(ncol) or r.get("FULL"))
                fid = q(r.get(fcol) or r.get("FormID")).upper()
                if ed and nm:
                    self.full_by_edid.setdefault(ed, nm)
                if fid and nm:
                    self.full_by_fid.setdefault(fid, nm)
        # currencies
        self.full_by_edid.setdefault("LegendaryTokens", "Legendary Scrip")

        self.misc = {q(r["FormID"]).upper(): r for r in S.rows("MISC_Export_*.tsv", cols=["FormID", "EDID", "FULL", "Keywords"],
                                                                where=lambda d: "LegendaryShard" in d.get("EDID", ""))}
        self.cobj = S.rows("COBJ_Export_*.tsv", cols=["COBJ_FormID", "COBJ_EDID", "CNAM_FormID", "CNAM_EDID", "BNAM_EDID", "GNAM_EDID", "FNAM_Keywords", "FVPA"],
                           where=lambda d: "egendary" in d["COBJ_EDID"])
        self.cobj2 = {q(r["COBJ_FormID"]).upper(): r for r in S.rows("COBJ2_Export_*.tsv", required=False, cols=["COBJ_FormID", "COBJ_EDID", "CNAM_FormID", "CNAM_EDID", "LRNM_LearnMethod", "LRNC_LearnChance"])}
        self.omod = {q(r["OMOD_FormID"]).upper(): r for r in S.rows("OMOD_Export_*.tsv", "_Properties", cols=["OMOD_FormID", "OMOD_EDID", "FULL", "DESC", "AttachPoint_EDID", "Includes_Flat", "MNAM_TargetKWDs"])}
        self.omod_by_edid = {q(r["OMOD_EDID"]): f for f, r in self.omod.items()}
        self.props = collections.defaultdict(list)
        for r in S.rows("OMOD_Export_*_Properties.tsv", where=lambda d: "egendary" in d.get("OMOD_EDID", "")):
            self.props[q(r["OMOD_FormID"]).upper()].append(r)

        # ENCH: effects by position (the header names run one column out on old exports)
        self.ench = {}
        rows = S.lists("ENCH_Export_*.tsv")
        if rows:
            h = rows[0]
            ic = h.index("Effects_Count")
            for r in rows[1:]:
                if len(r) <= ic:
                    continue
                n = int(fnum(r[ic], 0) or 0)
                effs = []
                for k in range(n):
                    c = (r[ic + 1 + k * 8: ic + 1 + k * 8 + 8] + [""] * 8)
                    if not c[0]:
                        continue
                    effs.append({"mgef": c[0].upper(), "mgef_edid": c[1], "mag": fnum(c[2], 0.0),
                                 "area": fnum(c[3], 0.0), "dur": fnum(c[4], 0.0), "curve": c[5],
                                 "input_fid": c[6].upper(), "input": c[7]})
                self.ench[r[0].upper()] = {"edid": r[1], "full": q(r[2]), "cast": r[4] if len(r) > 4 else "",
                                           "target": r[5] if len(r) > 5 else "", "effects": effs}
        fx = S.rows("ENCHFX_Export_*.tsv", required=False)
        if fx:
            by = collections.defaultdict(list)
            for r in fx:
                by[r["ENCH_FormID"].upper()].append(r)
            for ef, rs in by.items():
                rec = self.ench.get(ef)
                if not rec:
                    continue
                effs = []
                for r in sorted(rs, key=lambda r: int(r["EffectIndex"] or 0)):
                    effs.append({"mgef": r["MGEF_FormID"].upper(), "mgef_edid": r["MGEF_EDID"],
                                 "mag": fnum(r["Magnitude"], 0.0), "area": fnum(r["Area"], 0.0),
                                 "dur": fnum(r["Duration"], 0.0), "curve": r["Curve_EDID"],
                                 "input_fid": r["CurveInput_FormID"].upper(), "input": r["CurveInput_EDID"],
                                 "mag_glob": r["MAGG_EDID"], "dur_glob": r["DURG_EDID"]})
                rec["effects"] = effs
        else:
            warn("no ENCHFX export - magnitudes kept in globals will read 0 (run tools/esm_buff_guides_extract.py)")
        self.spel = collections.defaultdict(list)
        for r in S.rows("SPEL_Export_*_EFFECTS.tsv", required=False):
            self.spel[q(r["SPEL_EDID"])].append(r)
        self.mgef = {q(r["MGEF_FormID"]).upper(): r for r in S.rows("MGEF_Export_*.tsv", cols=["MGEF_FormID", "EDID", "FULL", "DNAM_MagicItemDescription", "DATA_Flags", "Archetype", "ActorValue", "PerkToApply", "CastingType", "Delivery", "VMAD_Scripts"])}
        self.mgef_by_edid = {q(r["EDID"]): f for f, r in self.mgef.items()}
        self.perk = {}
        for r in S.rows("PERK_Export_*.tsv", "PERKEP", cols=["PERK_FormID", "PERK_EDID", "FormID", "EDID", "FULL", "DESC", "PERK_DESC"]):
            fid = q(r.get("PERK_FormID") or r.get("FormID")).upper()
            if fid and fid not in self.perk:
                self.perk[fid] = r
        self.perk_by_edid = {q(r.get("PERK_EDID") or r.get("EDID")): f for f, r in self.perk.items()}
        self.perkep = collections.defaultdict(list)
        for r in S.rows("PERKEP_Export_*.tsv", cols=["PERK_FormID", "PERK_EDID", "EffectIndex", "EffectType", "EntryPoint", "Function", "Value", "Ability_EDID", "Conditions", "Raw"]):
            self.perkep[q(r["PERK_FormID"]).upper()].append(r)
        self.avif = {q(r["FormID"]).upper(): r for r in S.rows("AVIF_Export_*.tsv", cols=["FormID", "EDID", "FULL", "DESC"])}
        self.avif_by_edid = {q(r["EDID"]): r for r in self.avif.values()}
        self.glob = {}
        for r in S.rows("GLOB_Export_*.tsv", cols=["FormID", "EDID", "FLTV", "Value"]):
            self.glob[q(r.get("FormID")).upper()] = (q(r.get("EDID")), fnum(r.get("FLTV") or r.get("Value")))
        self.glob_by_edid = {v[0]: v[1] for v in self.glob.values()}
        self.gmst = {q(r["EDID"]): q(r["Value"]) for r in S.rows("GMST_Export_*.tsv", required=False, cols=["EDID", "Value"])}
        # curves: EDID -> FormID, FormID -> points
        self.curv_fid = {}
        for r in S.stream("CURV_Export_*_CURV.tsv", required=False):
            if len(r) >= 2 and re.fullmatch(r"[0-9A-Fa-f]{8}", r[0]):
                self.curv_fid[r[1]] = r[0].upper()
        self.curv_pts = collections.defaultdict(list)
        for r in S.rows("CURV_Export_*_POINTS.tsv", required=False):
            x, y = fnum(r.get("X")), fnum(r.get("Y"))
            if x is not None and y is not None:
                self.curv_pts[q(r.get("FormID")).upper()].append((x, y))
        for k in self.curv_pts:
            self.curv_pts[k].sort()
        # conditions
        self.cond_ench = {}
        self.cond_mgef = {}
        for r in S.rows("BUFFCOND_Export_*.tsv"):
            conds = [r.get(f"Cond{i}", "") for i in range(1, int(r.get("CondCount") or 0) + 1)]
            if r["Scope"] == "ENCH":
                self.cond_ench[(r["FormID"].upper(), int(r["EffectIndex"] or 0))] = conds
            elif r["Scope"] == "MGEF":
                self.cond_mgef[r["FormID"].upper()] = conds
        if not self.cond_ench:
            warn("BUFFCOND has no ENCH rows - run tools/esm_buff_guides_extract.py (29 Oct 2026 version)")
        self.lgdi = {q(r["LGDI_FormID"]).upper(): r for r in S.rows("LGDI_Export_*.tsv", "_Mods", cols=["LGDI_FormID", "LGDI_EDID", "FULL", "ScripCost", "ValueCurrency", "ApplicableItemList_EDID", "LegMods_Flat"] + [f"Ref_{i}" for i in range(1, 31)])}
        self.vendor_currency = ""
        for r in S.rows("NPC2_Vendors_*_Factions.tsv", required=False, cols=["Faction_EDID", "Currency_EDID"]):
            if r["Faction_EDID"] == "LGV01_MoleMiner_VendorFaction":
                self.vendor_currency = self.full_by_edid.get(r["Currency_EDID"]) or BG.camel(r["Currency_EDID"])
        self.npc = {q(r["FormID"]).upper(): r for r in S.rows("NPC_Export_*.tsv", ["_PRPS", "_Refs"], cols=["FormID", "EDID", "FULL", "INAM_FormID"])}
        self.gmrw = {}
        for r in S.rows("GMRW_Export_*.tsv", cols=["FormID", "EDID", "ParentQuestDisplay"]):
            self.gmrw.setdefault(q(r["FormID"]).upper(), r)
        quests = [(q(r.get("EDID")), q(r.get("FULL - Name"))) for r in
                  S.rows("QUEST_Export_*.tsv", required=False, cols=["EDID", "FULL - Name"])]
        chals = [(q(r.get("EDID")), q(r.get("FULL"))) for r in
                 S.rows("CHAL_Export_*.tsv", required=False, cols=["EDID", "FULL"])]
        self.raid_enc = plan_sources.raid_encounter_names(quests, chals)
        # leveled lists (rng76)
        root = os.path.dirname(S.path("LVLI_Export_*_LVLI_List.tsv"))
        self.L = rng76.LvliIndex()
        self.L.load_from_tsv_root(root)
        g = rng76.GlobIndex()
        g.load_from_tsv_root(os.path.dirname(S.path("GLOB_Export_*.tsv")))
        names = rng76.ItemNameIndex()
        try:
            names.load_all_from_tsv_root(root)
        except Exception as e:     # names only label rows; rates don't need them
            warn(f"item names: {e}")
        c = rng76.CurvIndex()
        c.load_from_tsv_root(os.path.dirname(S.path("CURV_Export_*_POINTS.tsv", required=False) or root))
        self.R = rng76_fast.PrunedResolver(self.L, g, names, c)
        self.R.reach = rng76_fast.build_reach(self.L)
        # who holds each list (non-LVLI references)
        self.holders = collections.defaultdict(list)
        for r in S.stream("LVLI_Export_*_LVLI_Refs.tsv"):
            if not r or not re.fullmatch(r"[0-9A-Fa-f]{8}", r[0]):
                continue
            for c_ in r[3:]:
                m = REF_RE.match(c_.strip())
                if m and m.group(3) != "LVLI":
                    self.holders[r[0].upper()].append((m.group(3), m.group(2), m.group(1).upper()))
        # list -> direct item entries
        self.lists_with = collections.defaultdict(set)
        for lf, ents in self.L.entries_by_list.items():
            for e in ents:
                ref = e.get("LVLO_Reference") or ""
                m = re.match(r"([0-9A-Fa-f]{8}):", ref)
                if m:
                    self.lists_with[m.group(1).upper()].add(lf.upper())
        # unique items (fixed stars)
        self.uwa = []
        for cand in ("dist/pts/unique_weapons_armour/unique_weapons_armour.json" if S.pts else "",
                     "dist/unique_weapons_armour/unique_weapons_armour.json"):
            p = os.path.join(REPO, cand) if cand else ""
            if p and os.path.isfile(p):
                try:
                    self.uwa = json.load(open(p, encoding="utf-8")).get("items") or []
                    break
                except Exception as e:
                    warn(f"unique weapons: {e}")

    # --- small lookups -------------------------------------------------------
    def name(self, edid):
        return self.full_by_edid.get(edid) or BG.camel(edid)

    def curve_points(self, edid_or_fid):
        fid = edid_or_fid.upper() if re.fullmatch(r"[0-9A-Fa-f]{8}", edid_or_fid or "") else self.curv_fid.get(edid_or_fid or "")
        return self.curv_pts.get(fid or "", [])

    def curve_y(self, edid, x):
        pts = self.curve_points(edid)
        if not pts:
            return None
        if x <= pts[0][0]:
            return pts[0][1]
        if x >= pts[-1][0]:
            return pts[-1][1]
        for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
            if x0 <= x <= x1:
                return y0 if x1 == x0 else y0 + (x - x0) / (x1 - x0) * (y1 - y0)
        return pts[-1][1]

    def gval(self, edid):
        return self.glob_by_edid.get(edid)

    def av_name(self, edid):
        if edid in AV_WORDS:
            return AV_WORDS[edid]
        if edid in getattr(self, "counters", ()):
            return "Pieces worn with this effect"
        r = self.avif_by_edid.get(edid)
        nm = q(r.get("FULL")) if r else ""
        return nm or BG.camel(re.sub(r"^(LGND|BOUNTY_LGND|STAT)_", "", edid or ""))


# ---------------------------------------------------------------------------
# Conditions -> plain words
# ---------------------------------------------------------------------------

OPW = {"<=": "at or below", "<": "below", ">=": "at or above", ">": "above", "=": "", "!=": "not"}


def cond_line(D, c, who):
    """one parsed condition -> a short plain sentence (None = not worth saying)"""
    fn, op, val, p1 = c["fn"], c["op"], c["val"], c.get("p1", "")
    yes = BG.is_true(c)
    ed = BG.edid_of(p1)
    You = who[0].upper() + who[1:]
    are = "are" if who == "you" else "is"
    your = "your" if who == "you" else who + "'s"
    if fn == "GetValue" and ed in getattr(D, "counters", ()):
        n = fmt(val)
        if op == "=":
            return f"With {n} piece{'s' if val != 1 else ''} worn"
        return f"Pieces worn {op} {n}"
    if fn == "GetValue":
        nm = D.av_name(ed)
        if ed == "Invisibility":
            return f"{You} {are} cloaked (invisible)" if yes else f"{You} {are} not cloaked"
        if op in ("=", "!=") and val in (0, 1):
            return f"{nm}: {'on' if yes else 'off'} ({your})"
        return f"{your[0].upper() + your[1:]} {nm.lower()} is {OPW.get(op, op)} {fmt(val)}".replace("is  ", "is ")
    if fn == "GetHealthPercentage":
        return f"{your[0].upper() + your[1:]} health is {OPW.get(op, op)} {fmt(val * 100)}%".replace("is  ", "is ")
    if fn == "IsInCombat":
        return f"{You} {are} in combat" if yes else f"{You} {are} not in combat"
    if fn == "IsMoving":
        return f"{You} {are} moving" if yes else f"{You} {are} standing still"
    if fn == "IsSprinting":
        return f"{You} {are} sprinting" if yes else f"{You} {are} not sprinting"
    if fn == "IsBlocking":
        return f"{You} {are} blocking" if yes else f"{You} {are} not blocking"
    if fn == "IsPowerAttacking":
        return "It's a power attack" if yes else "It's not a power attack"
    if fn == "IsBleedingOut":
        return f"{You} {are} downed" if yes else f"{You} {are} not downed"
    if fn == "GetLastHitCritical":
        return "The hit is a critical hit" if yes else "The hit is not a critical hit"
    if fn == "GetIsID" and "Player" in p1:
        return None if yes else "Not on players"
    if fn in ("GetIsPlayer",):
        return None if yes else "Not on players"
    if fn == "IsInInterior":
        return f"{You} {are} indoors" if yes else f"{You} {are} outdoors"
    if fn in ("HasKeyword", "WornHasKeyword") and ed.startswith("WeaponType"):
        kind = BG.camel(ed[len("WeaponType"):]).lower().replace("general", "").strip()
        kind = {"melee1 h": "one-handed melee", "melee2 h": "two-handed melee", "melee 1h": "one-handed melee",
                "melee 2h": "two-handed melee"}.get(kind, kind)
        thing_ = f"{BG.article(kind)} {kind} weapon"
        if fn == "WornHasKeyword" or who == "your weapon":
            return f"You have {thing_} equipped" if yes else f"You don't have {thing_} equipped"
        return f"{You} {are} using {thing_}" if yes else f"{You} {are} not using {thing_}"
    if fn in ("HasKeyword", "WornHasKeyword") and ed.startswith("HasLegendary_"):
        return None
    if fn in ("HasKeyword",) and ed.startswith("ActorType"):
        kind = BG.camel(ed[len("ActorType"):]).lower()
        return f"{You} {are} {BG.article(kind)} {kind}" if yes else f"{You} {are} not {BG.article(kind)} {kind}"
    if fn == "GetIsRace":
        nm = BG.thing(p1)
        return f"{You} {are} {BG.article(nm)} {nm}" if yes else f"{You} {are} not {BG.article(nm)} {nm}"
    if fn == "GetVATSMode" or fn == "IsInVATS":
        return "In V.A.T.S." if yes else "Not in V.A.T.S."
    if fn == "GetRandomPercent":
        return f"{fmt(val)}% chance each time"
    if fn == "GetPlayerTeammateCount":
        n = fmt(val)
        if op == "=":
            return "You have no teammates" if val == 0 else f"You have {n} teammate{'s' if val != 1 else ''}"
        return f"Teammates: {OPW.get(op, op)} {n}"
    if fn == "GetDistance":
        return f"They are within {fmt(val)} units (about {fmt(round(val * 0.01428))} m)" if op in ("<", "<=") else None
    if fn == "IsMemberOfAPlayerTeam":
        return "You are in a team" if yes else "You are not in a team (solo)"
    try:
        return BG.cond_words(c, who, None)
    except Exception:
        return None


PERK_COND_RE = re.compile(r"\[tab (\d+)\] (\w+)\((.*?)\)\s+([01]+)\s+(.+?)\s*$")


def parse_perk_conds(D, text):
    """PERKEP 'Conditions' -> parsed conditions; a comparison against a GLOB
    ('GetHealthPercentage() <= LGND_ExecuteHealthThreshold') reads the GLOB"""
    out = []
    for c in (text or "").split(" | "):
        m = PERK_COND_RE.match(c.strip())
        if not m:
            continue
        typ, raw_v = m.group(4), m.group(5)
        v = fnum(raw_v)
        if v is None:
            g = re.match(r"(\S+) \[GLOB:", raw_v)
            v = D.gval(g.group(1)) if g else None
        if v is None:
            continue
        out.append({"tab": int(m.group(1)), "fn": m.group(2), "p1": m.group(3), "op": BG.OPS.get(typ[:3], "="),
                    "or": len(typ) > 3 and typ[3] == "1", "val": float(v), "raw": c.strip()})
    return out


def conds_words(D, conds, who):
    out, night = [], False
    for c in conds:
        if c["fn"] == "GetGlobalValue" and "GameHour" in c.get("p1", ""):
            night = True
            continue
        w = cond_line(D, c, who)
        if w and w not in out:
            out.append(w + (" OR" if c.get("or") else ""))
    if night:
        out.insert(0, "At night (9pm to 6am game time)")
    if out:
        out[-1] = re.sub(r" OR$", "", out[-1])
    return out


def who_for(D, mgef_fid, run_on):
    m = D.mgef.get(mgef_fid) or {}
    deliv = (m.get("Delivery") or "Self").lower()
    if (run_on or "").lower().startswith("target") or deliv in ("contact", "target actor", "target location"):
        return "your target"
    return "you"


# ---------------------------------------------------------------------------
# Effects ("Outputs & Buffs")
# ---------------------------------------------------------------------------
# Every output row is
#   {kind, text, math, curve, when, gameText, note, records}
# text      the plain sentence ("+50% damage while cloaked")
# math      how the number is used, in plain words ("adds to your damage bonus")
# curve     {input, inputIsPercent, outputIsPercent, points:[{x,y}]} when the number
#           depends on something (your health, caps, durability ...)
# when      plain conditions; gameText = the game's own description

PROP_WORDS = {
    "AmmoCapacity": "magazine size", "NumProjectiles": "projectiles per shot",
    "DamageBonusMult": "damage", "Speed": "attack speed", "ReloadSpeed": "reload speed",
    "WeightMult": "weight", "Weight": "weight", "CriticalDamageMult": "critical damage",
    "CriticalChargeBonus": "critical meter fill", "AttackActionPointCost": "AP cost",
    "SneakAttackMult": "sneak attack damage", "AttackDamage": "damage", "Rate of Fire": "fire rate",
    "AimModelMinConeDegrees": "spread (min)", "AimModelMaxConeDegrees": "spread (max)",
    "AimModelRecoilMinDegPerShot": "recoil (min)", "AimModelRecoilMaxDegPerShot": "recoil (max)",
    "MinRange": "range (min)", "MaxRange": "range (max)", "Value": "item value",
    "Health": "durability", "BashDamage": "bash damage", "OutOfRangeDamageMult": "damage beyond range",
}
PCT_EP_ADD = {"Mod Weapon DMG Bonus Mult", "Mod Critical Damage", "Mod Damage on Consecutive Hits"}


def _sign(v):
    return ("+" if v >= 0 else "") + fmt(v)


def _pct_change(v):
    """multiplier -> '+25%' / '-15%'"""
    return _sign(round((v - 1.0) * 100, 4)) + "%"


def fill_desc(text, mag=None, mag_max=None, dur=None, value=None, D=None, av="", mag_range=None):
    """the game's description with its tokens filled from the files; '' when a
    number can't be filled (never print '+0')"""
    s = q(text)
    if not s or s.startswith("<CALL="):
        return ""
    s = re.sub(r"\s*\(?\s*\.?\s*Currently\b[^)]*\)?\s*$", "", s, flags=re.I)
    if D is not None:
        def g(m):
            v = D.gval(m.group(1))
            return fmt(v) if v is not None else m.group(0)
        s = re.sub(r"<GLOBAL=([A-Za-z0-9_]+)>", g, s)
        if av:
            s = s.replace("<EFFECT_AV.NAME>", D.av_name(av).lower())
    if mag_max is not None:
        s = re.sub(r"<\+MAG\.MAX>", _sign(mag_max), s).replace("<MAG.MAX>", fmt(mag_max))
    if mag_range is not None:
        lo, hi = mag_range
        s = re.sub(r"<\+?MAG>", (fmt(lo) + " to " + fmt(hi)) if lo != hi else fmt(hi), s)
    if mag is not None and mag != 0:
        s = re.sub(r"<\+MAG>", _sign(mag), s).replace("<MAG>", fmt(mag))
    if value is not None:
        s = re.sub(r"<\+VALUE>", _sign(value), s).replace("<VALUE>", fmt(value))
    if dur:
        s = re.sub(r"<DUR>", fmt(dur) + " seconds", s, flags=re.I)
    if re.search(r"<[^>]*>", s):
        return ""
    s = re.sub(r"\s{2,}", " ", s).strip(" .,")
    return s[:1].upper() + s[1:] if s else s


def curve_block(D, curve_edid, input_edid, out_pct=False):
    pts = D.curve_points(curve_edid)
    if not pts:
        return None
    xs = [p[0] for p in pts]
    in_pct = (input_edid or "").endswith("Percent") or (max(xs) <= 1.0 and min(xs) >= 0.0 and len(xs) > 1)
    return {"input": D.av_name(input_edid) if input_edid else "Input", "inputIsPercent": bool(in_pct),
            "outputIsPercent": bool(out_pct), "points": [{"x": x, "y": y} for x, y in pts], "curve": curve_edid}


def av_is_pct(D, av):
    a = D.avif_by_edid.get(av) or {}
    desc = a.get("DESC") or ""
    return av.startswith("STAT_") or "%" in desc


def av_text(D, av, v, verb="+"):
    a = D.avif_by_edid.get(av) or {}
    d = fill_desc(a.get("DESC") or "", value=v) if re.search(r"<\+?VALUE>", a.get("DESC") or "") else ""
    if d:
        return d
    unit = "%" if av_is_pct(D, av) else ""
    return f"{_sign(v)}{unit} {D.av_name(av)}"


def skip_self_keyword(D, cond, mod_kws):
    """perk entries gated on 'the item has legendary effect X' - keep only this mod's"""
    ed = BG.edid_of(cond.get("p1", ""))
    if cond["fn"] in ("HasKeyword", "WornHasKeyword", "EPMagic_SpellHasKeyword") and ed.startswith("HasLegendary_"):
        return ed
    return None


def mgef_effect(D, ench_fid, idx, e, mod_kws):
    m = D.mgef.get(e["mgef"]) or {}
    arche = q(m.get("Archetype"))
    av = q(m.get("ActorValue"))
    mag = e["mag"]
    if e.get("mag_glob"):
        gv = D.gval(e["mag_glob"])
        if gv is not None:
            mag = gv
    dur = e["dur"]
    if e.get("dur_glob"):
        gv = D.gval(e["dur_glob"])
        if gv is not None:
            dur = gv
    curve, mag_max = None, None
    if e["curve"]:
        curve = curve_block(D, e["curve"], e["input"], out_pct=av_is_pct(D, av))
        if curve:
            mag_max = max(p["y"] for p in curve["points"])
    row = {"kind": "effect", "text": "", "math": "", "curve": curve, "when": [], "note": "",
           "gameText": fill_desc(m.get("DNAM_MagicItemDescription") or "", mag=None if curve else mag, mag_max=mag_max,
                                 dur=dur, D=D, av=e["input"] or av,
                                 mag_range=((min(p["y"] for p in curve["points"]), mag_max) if curve else None)),
           "records": {"enchantment": D.ench[ench_fid]["edid"] + " [ENCH:" + ench_fid + "]",
                       "magicEffect": q(m.get("EDID")) + " [MGEF:" + e["mgef"] + "]",
                       "archetype": arche, "actorValue": av, "magnitude": mag,
                       "magnitudeGlobal": e.get("mag_glob") or "", "duration": dur,
                       "durationGlobal": e.get("dur_glob") or "", "curve": e["curve"], "curveInput": e["input"],
                       "perk": q(m.get("PerkToApply")), "script": q(m.get("VMAD_Scripts"))}}
    detrimental = (q(m.get("DATA_Flags")) + "000")[2] == "1"
    if arche in ("Value Modifier", "Peak Value Modifier", "Dual Value Modifier") and av:
        if curve:
            unit = "%" if curve["outputIsPercent"] else ""
            row["text"] = f"Up to {_sign(mag_max)}{unit} {D.av_name(av)}"
            row["math"] = f"The number depends on {curve['input'].lower()} - see the table"
        elif mag:
            row["text"] = av_text(D, av, -mag if detrimental else mag)
            row["math"] = "Lowers" if detrimental else "Adds to your " + D.av_name(av).lower()
        if av.startswith("STAT_Dmg"):
            row["note"] = ("Goes into your damage bonus total, the same one perk cards use, so it adds to "
                           "them. No cap and no diminishing returns.")
        if dur and arche != "Peak Value Modifier" and (q(m.get("CastingType")) != "Constant Effect"):
            row["text"] += f" for {fmt(dur)} seconds" if row["text"] else ""
    elif arche in ("Chameleon", "Invisibility"):
        row["text"] = row["gameText"] or "Makes you invisible"
    elif arche == "Cloak":
        row["text"] = row["gameText"]
        row["math"] = "An aura: it reaches you and the people near you"
    elif arche == "Script":
        sc = q(m.get("VMAD_Scripts"))
        row["kind"] = "script"
        if curve:
            row["kind"] = "effect"
            row["text"] = row["gameText"]
            row["math"] = f"The number depends on {curve['input'].lower()} - see the table"
        if sc:
            row["math"] = "Handled by a game script (" + sc.split("::")[0] + ")"
    elif arche:
        row["text"] = row["gameText"]
        row["math"] = BG.camel(arche)
    conds, raw = [], []
    for cs in (D.cond_ench.get((ench_fid, idx), []), D.cond_mgef.get(e["mgef"], [])):
        for c in cs:
            pc = BG.parse_pas(c)
            if pc and not skip_self_keyword(D, pc, mod_kws):
                conds.append(pc)
            if pc:
                raw.append(c)
    if conds:
        run_on = conds[0].get("on")
        row["when"] = conds_words(D, conds, who_for(D, e["mgef"], run_on))
    if raw:
        row["records"]["conditions"] = raw
    out = [row]
    perk = q(m.get("PerkToApply"))
    pfid = fid_in(perk, "PERK") or D.perk_by_edid.get(perk.split(" ")[0] if perk else "", "")
    if pfid:
        out += perk_effects(D, pfid, mod_kws)
    return out


def spell_lines(D, spell_edid):
    out = []
    for r in D.spel.get(spell_edid, []):
        m = D.mgef.get(q(r["EFID_MGEF_FormID"]).upper()) or {}
        av = q(m.get("ActorValue"))
        mag = fnum(r.get("EFIT_Magnitude"), 0.0)
        dur = fnum(r.get("EFIT_Duration"), 0.0)
        mg = re.match(r"([0-9A-F]{8}) (\S+)", r.get("MAGG_Global") or "")
        if mg and D.gval(mg.group(2)) is not None:
            mag = D.gval(mg.group(2))
        dg = re.match(r"([0-9A-F]{8}) (\S+)", r.get("DURG_Global") or "")
        if dg and D.gval(dg.group(2)) is not None:
            dur = D.gval(dg.group(2))
        desc = fill_desc(m.get("DNAM_MagicItemDescription") or "", mag=mag, dur=dur)
        if av and mag:
            t = f"{_sign(mag)} {D.av_name(av)}" + (" per second" if dur and dur > 1 else "") + (f" for {fmt(dur)} seconds" if dur else "")
        else:
            t = desc or q(r.get("EFID_MGEF_FULL"))
        if t:
            out.append(t)
    return out


def ability_rows(D, spell_edid, when, rec):
    """a perk's Ability = a spell whose effects carry the numbers (Fencer's +12.5%
    per teammate ...); one row per effect, with that effect's own conditions"""
    out = []
    for r in D.spel.get(spell_edid, []):
        mf = q(r["EFID_MGEF_FormID"]).upper()
        m = D.mgef.get(mf) or {}
        av = q(m.get("ActorValue"))
        mag = fnum(r.get("EFIT_Magnitude"), 0.0)
        dur = fnum(r.get("EFIT_Duration"), 0.0)
        mg = re.match(r"([0-9A-F]{8}) (\S+)", r.get("MAGG_Global") or "")
        if mg and D.gval(mg.group(2)) is not None:
            mag = D.gval(mg.group(2))
        gt = fill_desc(m.get("DNAM_MagicItemDescription") or "", mag=mag, dur=dur, D=D, av=av)
        if q(m.get("Archetype")) in ("Value Modifier", "Peak Value Modifier") and av and mag:
            text = av_text(D, av, mag)
        else:
            text = gt or (f"Effect strength {fmt(mag)}" if mag else "")
        conds = []
        for c in (r.get("Conditions") or "").split(" | "):
            pc = BG.parse_spel_cond(c)
            if pc:
                conds.append(pc)
        w = list(when) + conds_words(D, conds, "you")
        out.append({"kind": "ability", "text": text, "math": "", "curve": None, "when": w, "note": "",
                    "gameText": "", "records": dict(rec, spellEffect=q(r.get("EFID_MGEF_EDID")) + " [MGEF:" + mf + "]",
                                                     magnitude=mag, conditions=[c for c in (r.get("Conditions") or "").split(" | ") if c])})
    return out


def perk_effects(D, perk_fid, mod_kws):
    out = []
    pr = D.perk.get(perk_fid) or {}
    pdesc = fill_desc(pr.get("DESC") or pr.get("PERK_DESC") or "")
    pname = q(pr.get("PERK_EDID") or pr.get("EDID"))
    for r in D.perkep.get(perk_fid, []):
        ep, fn, typ, raw = q(r.get("EntryPoint")), q(r.get("Function")), q(r.get("EffectType")), r.get("Raw") or ""
        conds = parse_perk_conds(D, r.get("Conditions") or "")
        # the Conditions column drops parameters ("GetValue()"); Raw has them in order
        segs = raw.split(";; Function=")[2:]
        for c, seg in zip(conds, segs):
            if not c.get("p1"):
                pm = re.search(r";; (?:Actor Value|Keyword|Race|Perk|Magic Effect|Form|Global|Spell|Faction|"
                               r"Weapon Type|Parameter #1)=([^;]+?) ;;", seg)
                if pm and not re.fullmatch(r"[0 ]+", pm.group(1)):
                    c["p1"] = pm.group(1).strip()
        gate = [skip_self_keyword(D, c, mod_kws) for c in conds]
        gate = [g for g in gate if g]
        if gate and mod_kws and not any(g in mod_kws for g in gate):
            continue                       # this entry belongs to another legendary effect
        conds = [c for c in conds if not skip_self_keyword(D, c, mod_kws)]
        tabs = BG.TABS.get(ep, ["you", "your weapon", "your target"])
        when = []
        for c in conds:
            who = tabs[c.get("tab", 0)] if c.get("tab", 0) < len(tabs) else "you"
            w = cond_line(D, c, who)
            if w and w not in when:
                when.append(w + (" OR" if c.get("or") else ""))
        if when:
            when[-1] = re.sub(r" OR$", "", when[-1])
        rec = {"perk": pname + " [PERK:" + perk_fid + "]", "entryPoint": ep, "function": fn,
               "value": r.get("Value") or "", "conditions": [c["raw"] for c in conds]}
        if typ == "Ability":
            ab = q(r.get("Ability_EDID"))
            rows_ = ability_rows(D, ab, when, dict(rec, ability=ab))
            out += rows_ or [{"kind": "ability", "text": "", "math": "Gives the ability " + BG.camel(ab), "curve": None,
                              "when": when, "note": "", "gameText": "", "records": dict(rec, ability=ab)}]
            continue
        if typ != "Entry Point":
            continue
        val = fnum(r.get("Value"))
        cm = re.search(r"Curve Table=(\S+) \[CURV:([0-9A-F]{8})\]", raw)
        sp = re.search(r"Spell=(\S+) \[SPEL:([0-9A-F]{8})\]", raw)
        label = ENTRY_WORDS.get(ep, BG.camel(re.sub(r"^Mod ", "", ep)))
        text, math, curve = "", "", None
        if cm:
            im = re.search(r"EPF4 - Actor value 2=([A-Za-z0-9_]+)", raw)
            curve = curve_block(D, cm.group(1), im.group(1) if im else "")
            rec["curve"] = cm.group(1)
            ys = [p["y"] for p in curve["points"]] if curve else []
            if ys and fn.startswith("Multiply"):
                best = min(ys) if "Incoming" in ep else max(ys)
                text = f"{label}: {_pct_change(best)} at best"
                math = f"Multiplies - the number depends on {curve['input'].lower()} - see the table"
            elif ys:
                text = f"{label}: up to {_sign(max(ys))}"
                math = "Adds - see the table"
        elif sp:
            lines = spell_lines(D, sp.group(1))
            text = "; ".join(lines) if lines else "Casts " + BG.camel(sp.group(1))
            math = {"Apply Combat Hit Spell": "On every hit",
                    "Apply Bash Hit Spell": "On every bash", "Apply Weapon Swing Spell": "On every swing"}.get(ep, ep)
            rec["spell"] = sp.group(1)
        elif val is not None:
            if fn == "Multiply Value":
                text = f"{label}: {_pct_change(val)}"
                math = f"Multiplies by {fmt(val)} (its own multiplier)"
            elif fn == "Add Value":
                if ep in PCT_EP_ADD and abs(val) <= 5:
                    text = f"{label}: {_sign(val * 100)}%"
                else:
                    text = f"{label}: {_sign(val)}"
                math = "Adds" + (" to the same total as your other % damage bonuses" if ep == "Mod Weapon DMG Bonus Mult" else "")
            elif fn == "Set Value":
                text = f"{label}: {fmt(val)}"
                math = "Sets the value"
            elif "Actor Value Mult" in fn:
                avm = re.search(r"Actor Value=([A-Za-z0-9_]+) \[AVIF", raw)
                avn = D.av_name(avm.group(1)) if avm else "a counter"
                if avm and avm.group(1).startswith("LGND_") and " " not in avn.strip():
                    avn = avn + " stack"
                text = f"{label}: {_sign(val * 100) if abs(val) < 1 else _sign(val)}{'%' if abs(val) < 1 else ''} per {avn.lower()}"
                math = FUNC_WORDS.get(fn, fn)
            else:
                text = f"{label}: {fmt(val)}"
                math = FUNC_WORDS.get(fn, fn)
        internal = (ep not in ENTRY_WORDS and not curve and not sp and not when
                    and (val is None or abs(val) in (0, 1)) and fn in ("Add Value", "Set Value"))
        out.append({"kind": "perk", "text": text, "math": math, "curve": curve, "when": when, "note": "",
                    "gameText": "", "internal": bool(internal), "records": rec})
    if pdesc and out:
        out[0]["gameText"] = pdesc
    return out


def prop_value(D, r, raw):
    """a property's number: its own value, or (when 0) its curve table, which for
    legendary mods is keyed on item level - flat across levels in the Oct 2026 files"""
    val = fnum(raw)
    cf = fid_in(r.get("CurveTable") or "", "CURV")
    if cf and not val:
        pts = D.curv_pts.get(cf, [])
        if pts:
            ys = {round(y, 6) for _x, y in pts}
            return (pts[-1][1] if len(ys) == 1 else max(y for _x, y in pts)), (None if len(ys) == 1 else
                    {"input": "Item level", "inputIsPercent": False, "outputIsPercent": False,
                     "points": [{"x": x, "y": y} for x, y in pts], "curve": re.sub(r"\s*\[.*$", "", r.get("CurveTable"))})
    return val, None


def omod_outputs(D, omod_fid):
    """every output of one legendary OMOD, in property order"""
    out = []
    props = D.props.get(omod_fid, [])
    kws = set()
    D.counters = set()
    for r in props:
        if q(r.get("PropertyName")) == "Keywords":
            kws.update(re.findall(r"(HasLegendary_\w+)", r.get("Value1") or ""))
        if q(r.get("PropertyName")) in ("Actor Values", "ActorValues") and fnum(r.get("Value2")) == 1:
            ed = q((D.avif.get(fid_in(r.get("Value1") or "", "AVIF")) or {}).get("EDID"))
            if re.search(r"Count|^LGND_|^BOUNTY_LGND_", ed):
                D.counters.add(ed)
    for r in props:
        prop, fn = q(r.get("PropertyName")), q(r.get("FunctionType"))
        v1, v2 = r.get("Value1") or "", r.get("Value2") or ""
        if prop == "Keywords":
            continue
        if prop == "Enchantments":
            ef = fid_in(v1, "ENCH")
            en = D.ench.get(ef)
            if en:
                for i, e in enumerate(en["effects"]):
                    out += mgef_effect(D, ef, i, e, kws)
        elif prop in ("Actor Values", "ActorValues"):
            af = fid_in(v1, "AVIF")
            a = D.avif.get(af) or {}
            ed = q(a.get("EDID"))
            val, pcurve = prop_value(D, r, v2)
            rec = {"actorValue": ed + " [AVIF:" + af + "]", "function": fn, "value": val,
                   "curve": q(r.get("CurveTable")) if fid_in(r.get("CurveTable") or "", "CURV") else ""}
            if ed in D.counters:
                out.append({"kind": "counter", "text": "", "math": "Counts this piece towards the full-set bonus",
                            "curve": None, "when": [], "note": "", "gameText": "", "records": rec})
                continue
            out.append({"kind": "av", "text": av_text(D, ed, val) if val else "", "math": "Adds to your " + D.av_name(ed).lower(),
                        "curve": pcurve, "when": [], "note": ("Goes into your damage bonus total, the same one perk cards use, "
                                                              "so it adds to them. No cap and no diminishing returns.")
                        if ed.startswith("STAT_Dmg") else "", "gameText": "", "records": rec})
        elif prop in ("Perk", "116"):
            out += perk_effects(D, fid_in(v1, "PERK"), kws)
        elif prop in ("DamageTypeValues", "Damage Type Value", "Damage Type Values"):
            nm = (re.search(r'""(.*?)""', v1) or [None, BG.camel(re.sub(r"\s.*$", "", v1).replace("dt", ""))])[1]
            val, _c = prop_value(D, r, v2)
            armour = "Armor" in q((D.omod.get(omod_fid) or {}).get("OMOD_EDID"))
            kind_ = re.sub(r"\s*Damage$", "", nm)
            text = (f"{_sign(val)} {kind_} Resistance" if armour else f"{_sign(val)} {kind_} damage") if val else ""
            out.append({"kind": "stat", "text": text, "math": "Adds to your resistance" if armour else "Adds damage of this type",
                        "curve": None, "when": [], "note": "", "gameText": "",
                        "records": {"property": prop, "function": fn, "value1": v1, "value2": v2}})
        else:
            val, pcurve = prop_value(D, r, v1)
            word = PROP_WORDS.get(prop, BG.camel(prop).lower())
            if val is None:
                continue
            if fn == "MUL+ADD":
                text, math = f"{_sign(round(val * 100, 4))}% {word}", f"Multiplies by {fmt(1 + val)}"
            elif fn == "ADD":
                pctish = prop in ("DamageBonusMult", "CriticalDamageMult", "WeightMult", "SneakAttackMult")
                text = f"{_sign(round(val * 100, 4))}% {word}" if pctish else f"{_sign(val)} {word}"
                math = "Adds"
            elif fn == "SET":
                text, math = f"{word[:1].upper() + word[1:]}: {fmt(val)}", "Sets the value"
            else:
                text, math = f"{word}: {fmt(val)}", fn
            out.append({"kind": "stat", "text": text, "math": math, "curve": None, "when": [], "note": "",
                        "gameText": "", "records": {"property": prop, "function": fn, "value": v1}})
    return out, kws


def headline(D, omod_fid, outputs):
    """one line: the mod's own description, else the game's effect text"""
    r = D.omod.get(omod_fid) or {}
    if q(r.get("DESC")):
        return fill_desc(r.get("DESC")) or q(r.get("DESC"))
    for o in outputs:
        if o.get("gameText"):
            return o["gameText"]
    for kinds in (("av", "effect", "stat"), ("perk",)):
        for o in outputs:
            if o["kind"] in kinds and o.get("text") and not o.get("internal"):
                return o["text"]
    return ""


# ---------------------------------------------------------------------------
# How to Obtain
# ---------------------------------------------------------------------------

def holder_label(D, sig, edid, fid, list_fid):
    if sig == "NPC_":
        n = D.npc.get(fid) or {}
        if q(n.get("INAM_FormID")).upper() != list_fid.upper():
            return None              # not its death list (inventory / outfit)
        full = q(n.get("FULL"))
        return ("Enemy", full or BG.camel(edid))
    if sig == "GMRW":
        g = D.gmrw.get(fid) or {}
        quest = q(g.get("ParentQuestDisplay"))
        quest = re.sub(r"\s*\[[A-Z]{4}:[0-9A-F]{8}\]\s*$", "", quest)
        quest = re.sub(r'^[^"]*"([^"]+)".*$', r"\1", quest) if '"' in quest else quest
        if "<" in quest or "[" in quest:
            quest = ""
        tag = ""
        if re.search(r"Failure", edid, re.I):
            tag = " (failed run)"
        elif re.search(r"Success", edid, re.I):
            tag = " (successful run)"
        label = quest or BG.camel(re.sub(r"^QuestReward_|_QuestReward.*$", "", edid))
        raid = plan_sources.raid_stage_of_gmrw(edid, D.raid_enc)
        if raid:
            return ("Raids", f"Raid: {raid[0]} - {raid[1]}" + tag)
        return ("Quest / event reward", label + tag)
    if sig == "LGDI":
        return ("Legendary enemies", "Any 4" + STAR + " legendary enemy")
    if sig == "MGEF":
        return ("Item you open / use", BG.camel(re.sub(r"_?Effect$", "", edid)))
    if sig == "CONT":
        return ("Container", BG.camel(edid))
    if sig == "QUST":
        return ("Quest / event reward", BG.camel(edid))
    return None


def tranche(edid):
    m = re.search(r"Tranche(\d+|XX)", edid or "")
    if not m:
        return ""
    return " - final tranche" if m.group(1) == "XX" else f" - tranche {int(m.group(1))}"


def box_drop_routes(D, shard_fid):
    """every list path from a holder down to the mod box, rated by rng76"""
    out, seen = [], set()
    start = D.lists_with.get(shard_fid, set())
    frontier = [(lf, 0) for lf in start]
    visited = set()
    while frontier:
        lf, depth = frontier.pop()
        if lf in visited or depth > 8:
            continue
        visited.add(lf)
        ed = D.L.edid_for(lf)
        if is_dev(ed):
            continue
        for sig, hed, hfid in D.holders.get(lf, []):
            if is_dev(hed):
                continue
            lab = holder_label(D, sig, hed, hfid, lf)
            if not lab:
                continue
            try:
                p = D.R.appearance_prob(lf, shard_fid)
            except Exception as e:
                warn(f"rate {ed} -> {shard_fid}: {e}")
                continue
            if p <= 0:
                continue
            group, label = lab
            label = label + tranche(ed)
            key = (group, label, round(p, 8))
            if key in seen:
                continue
            seen.add(key)
            note = ""
            if sig == "LGDI":
                note = ("Only 4" + STAR + " legendary enemies can drop a legendary mod box. Their legendary drop is "
                        "one of: armour, a melee weapon, a ranged weapon, a mod box or power armour.")
            out.append({"group": group, "source": label, "rate": p, "rateText": pct(p),
                        "note": note, "list": ed + " [LVLI:" + lf + "]", "holder": f"{hed} [{sig}:{hfid}]"})
        for parent in D.L.parent_map.get(lf.upper(), ()):
            frontier.append((parent, depth + 1))
    out.sort(key=lambda r: (r["group"], -r["rate"], r["source"]))
    return out


class Pools:
    """the legendary star pools ("Random Legendary Mod" collections)"""

    def __init__(self, D):
        self.D = D
        self.members = {}         # modcol fid -> [omod fids]
        self.meta = {}
        for fid, r in D.omod.items():
            ed = q(r["OMOD_EDID"])
            m = re.match(r"^(BOUNTY_)?modcol_Legendary_Crafting_(Weapon|Armor|PowerArmor)(\d)$", ed)
            if not m:
                continue
            mods = [x.group(2).upper() for x in FID_RE.finditer(r.get("Includes_Flat") or "") if x.group(1) == "OMOD"]
            self.members[fid] = mods
            self.meta[fid] = {"edid": ed, "bounty": bool(m.group(1)), "type": m.group(2), "star": int(m.group(3))}

    def kinds(self, omod_fid):
        r = self.D.omod.get(omod_fid) or {}
        ks = set()
        for x in FID_RE.finditer(r.get("MNAM_TargetKWDs") or ""):
            pass
        for kw in re.findall(r"(ma_legendarycrafting_\w+)", r.get("MNAM_TargetKWDs") or ""):
            if kw in KIND_BY_KW:
                ks.add(KIND_BY_KW[kw][0])
        return ks

    def fits(self, omod_fid, item):
        return bool(self.kinds(omod_fid) & ITEM_ACCEPTS[item])

    def chance(self, pool_fid, omod_fid, item):
        mods = [m for m in self.members.get(pool_fid, []) if self.fits(m, item)]
        if omod_fid not in mods:
            return None, len(mods)
        return 1.0 / len(mods), len(mods)


def random_roll_routes(D, P, omods):
    out = []
    for pool, meta in sorted(P.meta.items(), key=lambda kv: (kv[1]["bounty"], kv[1]["type"], kv[1]["star"])):
        for item in POOL_ITEMS[meta["type"]]:
            for om in omods:
                ch, n = P.chance(pool, om, item)
                if ch is None:
                    continue
                out.append({
                    "pool": ("Bounty" if meta["bounty"] else "Standard"),
                    "item": item,
                    "star": meta["star"],
                    "poolSize": n,
                    "rate": ch,
                    "rateText": pct(ch),
                    "text": (f"Any {meta['star']}{STAR} or higher legendary {item.lower()} from the "
                             f"{'Bounty' if meta['bounty'] else 'standard'} pool: 1 in {n}"),
                    "pool_edid": meta["edid"] + " [OMOD:" + pool + "]",
                })
                break
    return out


def purveyor_routes(D, P, omods):
    """the Purveyor's stock: LGDIs referenced by her vendor list"""
    vendor_lists = {f for f, ed in D.L.edid_by_formid.items() if ed.startswith("LGV01_LL_Vendor_LegendaryItems")}
    out = []
    for fid, r in D.lgdi.items():
        refs = " ".join(r.get(f"Ref_{i}") or "" for i in range(1, 31))
        if not any(v in refs for v in vendor_lists):
            continue
        name = clean_name(q(r.get("FULL")))
        if "Mystery" in name:
            continue
        cost = fnum(r.get("ScripCost"))
        item_list = q(r.get("ApplicableItemList_EDID"))
        item = ("Ranged weapon" if "Ranged" in item_list else "Melee weapon" if "Melee" in item_list
                else "Power Armour" if "PowerArmor" in item_list else "Armour" if "Armor" in item_list else None)
        if not item:
            continue
        slots = [x.group(2).upper() for x in FID_RE.finditer(r.get("LegMods_Flat") or "") if x.group(1) == "OMOD"]
        for k, pool in enumerate(slots, start=1):
            for om in omods:
                ch, n = P.chance(pool, om, item)
                if ch is None:
                    continue
                out.append({"item": f"{stars_of(q(r.get('FULL')))}{STAR} {name}",
                            "cost": (fmt(cost) + " " + (D.vendor_currency or "Legendary Scrip")) if cost else "",
                            "slot": k, "poolSize": n, "rate": ch, "rateText": pct(ch),
                            "lgdi": q(r.get("LGDI_EDID")) + " [LGDI:" + fid + "]"})
                break
    out.sort(key=lambda x: (x["item"]))
    return out


def fixed_on(D, omods):
    eds = {q(D.omod[o]["OMOD_EDID"]) for o in omods if o in D.omod}
    out = []
    for it in D.uwa:
        for s in it.get("starEffects") or []:
            if s.get("edid") in eds and s.get("fixed"):
                out.append({"name": it.get("name"), "kind": it.get("kind") or "", "star": s.get("star")})
    out.sort(key=lambda x: x["name"] or "")
    return out


# ---------------------------------------------------------------------------
# Crafting
# ---------------------------------------------------------------------------

def components(D, fvpa):
    out = []
    for part in (fvpa or "").split("|"):
        bits = part.split(":")
        if len(bits) < 2 or not bits[0]:
            continue
        ed, qty, curve = bits[0], fnum(bits[1], 0), (bits[2] if len(bits) > 2 else "")
        real = qty
        if curve:
            y = D.curve_y(curve, qty)
            if y is not None:
                real = y
        nm = D.name(ed)
        if ed == "c_LegendaryModule":
            nm = "Legendary Module"
        out.append({"name": nm, "qty": real, "edid": ed, "curve": curve, "curveX": qty if curve else None})
    return out


def learn_info(D, cobj_fid):
    c2 = D.cobj2.get(cobj_fid) or {}
    method = q(c2.get("LRNM_LearnMethod"))
    chance_ref = q(c2.get("LRNC_LearnChance"))
    gf = re.match(r"([0-9A-Fa-f]{8}):([^:]+):GLOB", chance_ref)
    chance = D.glob.get(gf.group(1).upper(), (None, None))[1] if gf else None
    return method, chance, (gf.group(2) if gf else "")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build(src, outdir, channel):
    D = Data(src)
    P = Pools(D)

    # mod box (shard) -> its recipes
    craft_by_shard, attach_by_shard = {}, collections.defaultdict(list)
    misc_by_edid = {q(m.get("EDID")): f for f, m in D.misc.items()}
    for r in D.cobj:
        ced = q(r.get("CNAM_EDID"))
        cfid = q(r.get("CNAM_FormID")).upper()
        if is_dev(q(r.get("COBJ_EDID"))):
            continue
        if cfid in D.misc and "LegendaryShard" in ced:
            craft_by_shard[cfid] = r
            continue
        if not cfid:
            c2 = D.cobj2.get(q(r.get("COBJ_FormID")).upper()) or {}
            cfid = q(c2.get("CNAM_FormID")).upper()
            if not cfid:
                # the COBJ export leaves CNAM blank on some recipes (P62_co_mod_*):
                # the recipe co_mod_X makes the mod mod_X
                guess = re.sub(r"(^|_)co_mod_", r"\1mod_", q(r.get("COBJ_EDID")))
                cfid = D.omod_by_edid.get(guess, "")
            r = dict(r, CNAM_FormID=cfid)
        if cfid in D.omod:
            for part in (r.get("FVPA") or "").split("|"):
                ed = part.split(":")[0]
                if "LegendaryShard" in ed:
                    sh = misc_by_edid.get(ed)
                    if sh:
                        attach_by_shard[sh].append(r)

    entries = {}
    for fid, m in D.misc.items():
        ed, full = q(m.get("EDID")), q(m.get("FULL"))
        if "LegendaryShard" not in ed or is_dev(ed) or not full:
            continue
        star = stars_of(full)
        if not star:
            continue
        name = clean_name(full)
        omods = sorted({q(a.get("CNAM_FormID")).upper() for a in attach_by_shard.get(fid, [])})
        entries[fid] = {"fid": fid, "edid": ed, "name": name, "star": star, "omods": omods}

    # pool mods nobody can learn (no mod box): still roll on items
    pooled = set()
    for mods in P.members.values():
        pooled.update(mods)
    covered = {o for e in entries.values() for o in e["omods"]}
    extra = collections.defaultdict(list)
    for om in sorted(pooled - covered):
        r = D.omod.get(om)
        if not r or is_dev(q(r["OMOD_EDID"])):
            continue
        star = int((re.search(r"ap_Legendary(\d)", r.get("AttachPoint_EDID") or "") or [0, 0])[1] or 0)
        if not star:
            continue
        extra[(star, q(r["FULL"]))].append(om)
    by_name = {(e["star"], e["name"].lower()): e for e in entries.values()}
    for (star, name), oms in list(extra.items()):
        if (star, name.lower()) in by_name:
            by_name[(star, name.lower())]["omods"] = sorted(set(by_name[(star, name.lower())]["omods"]) | set(oms))
            del extra[(star, name)]
    for (star, name), oms in extra.items():
        entries["NOBOX:" + str(star) + ":" + name] = {"fid": "", "edid": "", "name": name, "star": star,
                                                       "omods": sorted(oms), "noBox": True}

    # two different mods with one name (the Drifter's Satiated and the Infestations
    # one): say which is which
    PREFIX_WORDS = {"P62": "The Drifter", "HTO": "Infestations", "RD01": "Raids", "BOUNTY": "Bounty Hunting"}
    clash = collections.Counter((e["star"], e["name"].lower()) for e in entries.values())
    for e in entries.values():
        if clash[(e["star"], e["name"].lower())] > 1:
            pre = (re.match(r"^([A-Z0-9]+)_", e.get("edid") or "") or [None, ""])[1]
            if pre in PREFIX_WORDS:
                e["name"] = f"{e['name']} ({PREFIX_WORDS[pre]})"
    pages = {s: [] for s in range(1, 6)}
    for key, e in entries.items():
        omods = e["omods"]
        kinds = set()
        for o in omods:
            kinds |= P.kinds(o)
        kind_words = [KIND_BY_KW[k][1] for k in KIND_BY_KW if KIND_BY_KW[k][0] in kinds]
        kind_words = [KIND_BY_KW[k][1] for k in ("ma_legendarycrafting_weapon", "ma_legendarycrafting_weaponranged",
                                                 "ma_legendarycrafting_weaponmelee", "ma_legendarycrafting_armor",
                                                 "ma_legendarycrafting_powerarmor") if KIND_BY_KW[k][0] in kinds]
        # outputs: one block per item type (armour and power armour can differ)
        blocks, seen_sig = [], {}
        order = {"weapon": 0, "ranged": 1, "melee": 2, "armour": 3, "powerarmour": 4}
        for o in sorted(omods, key=lambda o: min([order.get(k, 9) for k in P.kinds(o)] or [9])):
            outs, _kws = omod_outputs(D, o)
            sig = json.dumps([(x["text"], x.get("curve") and x["curve"]["curve"], x["when"]) for x in outs])
            r = D.omod.get(o) or {}
            kws = [KIND_BY_KW[k][1] for k in re.findall(r"(ma_legendarycrafting_\w+)", r.get("MNAM_TargetKWDs") or "") if k in KIND_BY_KW]
            if sig in seen_sig:
                seen_sig[sig]["appliesTo"] = sorted(set(seen_sig[sig]["appliesTo"]) | set(kws))
                seen_sig[sig]["omods"].append(q(r.get("OMOD_EDID")) + " [OMOD:" + o + "]")
                continue
            b = {"appliesTo": kws, "outputs": outs, "omods": [q(r.get("OMOD_EDID")) + " [OMOD:" + o + "]"],
                 "headline": headline(D, o, outs)}
            seen_sig[sig] = b
            blocks.append(b)
        head = next((b["headline"] for b in blocks if b["headline"]), "")

        # how to obtain
        learn = None
        craft = None
        if e.get("fid"):
            cr = craft_by_shard.get(e["fid"])
            if cr:
                cfid = q(cr["COBJ_FormID"]).upper()
                method, chance, glob_ed = learn_info(D, cfid)
                learn = {"method": method, "chance": chance, "chanceText": (fmt(chance) + "%") if chance is not None else "",
                         "glob": glob_ed}
                craft = {"workbench": WORKBENCH.get(q(cr.get("BNAM_EDID")), BG.camel(q(cr.get("BNAM_EDID")))),
                         "components": components(D, cr.get("FVPA")),
                         "recipe": q(cr["COBJ_EDID"]) + " [COBJ:" + cfid + "]",
                         "learnMethod": method}
        attach = []
        for a in sorted(attach_by_shard.get(e.get("fid", ""), []), key=lambda r: q(r.get("BNAM_EDID"))):
            om = q(a.get("CNAM_FormID")).upper()
            kws = [KIND_BY_KW[k][1] for k in re.findall(r"(ma_legendarycrafting_\w+)", (D.omod.get(om) or {}).get("MNAM_TargetKWDs") or "") if k in KIND_BY_KW]
            attach.append({"workbench": WORKBENCH.get(q(a.get("BNAM_EDID")), BG.camel(q(a.get("BNAM_EDID")))),
                           "appliesTo": kws,
                           "cost": [c for c in components(D, a.get("FVPA")) if "LegendaryShard" not in c["edid"]],
                           "recipe": q(a["COBJ_EDID"]) + " [COBJ:" + q(a["COBJ_FormID"]).upper() + "]"})

        drops = box_drop_routes(D, e["fid"]) if e.get("fid") else []
        rolls = random_roll_routes(D, P, omods)
        purv = purveyor_routes(D, P, omods)
        fixed = fixed_on(D, omods)

        if not omods and not drops and not (learn and learn.get("method")):
            print(f"{TAG} skipped {e['star']}{STAR} {e['name']}: no mod record, recipe or drop", file=sys.stderr)
            continue
        # every mod box is the same crate; its colour swap goes by star
        # (MISC MODS -> MSWP_LegendaryShard_<N>Star, none on 1-star boxes)
        img = (IMG_DIR + f"legendary-mod-box-{e['star']}-star.avif") if e.get("fid") else ""

        item = {
            "id": slug(f"{e['star']}-{e['name']}"),
            "name": e["name"],
            "star": e["star"],
            "appliesTo": kind_words,
            "headline": head,
            "image": img,
            "noBox": bool(e.get("noBox")),
            "obtain": {"learn": learn, "boxDrops": drops, "randomRoll": rolls, "purveyor": purv, "fixedOn": fixed},
            "outputs": blocks,
            "crafting": {"box": craft, "attach": attach},
            "technical": {
                "modBox": (e["edid"] + " [MISC:" + e["fid"] + "]") if e.get("fid") else "",
                "modBoxName": q((D.misc.get(e.get("fid", "")) or {}).get("FULL")),
                "mods": [q((D.omod.get(o) or {}).get("OMOD_EDID")) + " [OMOD:" + o + "]" for o in omods],
                "attachPoint": "ap_Legendary" + str(e["star"]),
                "keywords": sorted({k for o in omods for k in re.findall(r"(HasLegendary_\w+)", " ".join(
                    (p.get("Value1") or "") for p in D.props.get(o, [])))}),
            },
        }
        pages[e["star"]].append(item)

    # 5th star slot: what (if anything) uses it
    star5_note = []
    for fid, r in D.omod.items():
        if (r.get("AttachPoint_EDID") or "") == "ap_Legendary5" and not is_dev(q(r["OMOD_EDID"])):
            star5_note.append({"name": q(r.get("FULL")) or BG.camel(re.sub(r"^RTSV_|Bounty_|mod_|Custom_", "", q(r["OMOD_EDID"]))),
                               "mod": q(r["OMOD_EDID"]) + " [OMOD:" + fid + "]",
                               "enemyOnly": bool(re.search(r"Bounty", q(r["OMOD_EDID"]), re.I))})

    # shared facts (one copy, every page)
    def g(ed):
        return D.glob_by_edid.get(ed)
    shard_chance = {s: g(f"LegendaryShards_DropChance_{s}Star") for s in range(1, 5)}
    shared = {
        "learnChance": {s: g(f"Learn_Scrap_Chance_LegendaryShard_{s}") for s in range(1, 5)},
        "boxStarChance": shard_chance,
        "gmst": {"fLegendaryModShardChance": fnum(D.gmst.get("fLegendaryModShardChance")),
                 "uLegendaryRarity": fnum(D.gmst.get("uLegendaryRarity"))},
        "exeOnly": _exe_settings(),
        "moduleCurve": [{"x": x, "y": y} for x, y in D.curve_points("COBJ_Legendary_Crafting_Module")],
        "scripCurve": [{"x": x, "y": y} for x, y in D.curve_points("COBJ_Legendary_Attach_Scrip")],
        "star5": star5_note,
    }

    os.makedirs(os.path.join(outdir, "legendary_star_pages"), exist_ok=True)
    stamp = datetime.date.today().isoformat()
    counts = {}
    for s, items in pages.items():
        items.sort(key=lambda x: (x["name"].lower().lstrip("'"), x["name"]))
        doc = {"version": 1, "generated": stamp, "channel": channel, "star": s, "count": len(items),
               "shared": shared, "items": items,
               "sources": dict(sorted(src.used.items()))}
        path = os.path.join(outdir, "legendary_star_pages", f"{s}-star.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, separators=(",", ":"))
        counts[s] = len(items)
    print(f"{TAG} {channel}: " + ", ".join(f"{s}{STAR} {n}" for s, n in counts.items()) + f" -> {outdir}/legendary_star_pages/")
    return pages


def _exe_settings():
    """exe-only settings about legendary mods (dist/exe_settings.json, src/exe_settings.py)"""
    p = os.path.join(REPO, "dist", "exe_settings.json")
    out = {}
    try:
        data = json.load(open(p, encoding="utf-8"))
        rows = data.get("settings") if isinstance(data, dict) else data
        for r in rows or []:
            n = r.get("name") or ""
            if n in ("fLegendaryCraftingModExtractPityValue", "uFourStarLegendaryRequiredMods",
                     "fLegendaryLootSharingDistanceMax", "fPlayerStashWeightLegendaryModule"):
                out[n] = {"exe": r.get("exe_default"), "esm": r.get("esm_value"),
                          "pcReaders": r.get("client_readers")}
    except Exception as e:
        warn(f"exe settings: {e}")
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="Build the BnB N-star Legendary Mods pages.")
    ap.add_argument("--pts", action="store_true", help="read tsv/pts, write dist/pts")
    ap.add_argument("--data-dir")
    ap.add_argument("--outdir")
    a = ap.parse_args(argv)
    pts = a.pts or os.environ.get("DFBNB_CHANNEL", "").lower() == "pts"
    data = a.data_dir or os.path.join(REPO, "tsv", "pts" if pts else "")
    out = a.outdir or os.path.join(REPO, "dist", "pts" if pts else "")
    build(Src(os.path.normpath(data), pts), os.path.normpath(out), "pts" if pts else "live")


if __name__ == "__main__":
    main()
