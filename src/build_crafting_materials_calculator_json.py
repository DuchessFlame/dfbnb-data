#!/usr/bin/env python3
"""
build_crafting_materials_calculator_json.py
===========================================
Builds dist/calculators/crafting_materials_calculator.json for the
Buffs n Brew "Crafting Materials Calculator" page
(/bnb/calculators/crafting-materials-calculator/), rendered by
df-bnb-crafting-materials-calculator.js.

The page lets a player pick body armour / power armour / weapons, the mods for
each attach point and a legendary mod per star, then totals every component,
junk item, module, currency and consumable needed to craft the lot.

FULLY GENERATIVE — nothing below is a hand-kept list of items or mods.
New armour sets, weapons, mods and legendary effects appear on the next build.

How the game data is joined
---------------------------
Items
  * Weapons       COBJ crafted at Workbench_Crafting_Weapon whose CNAM is a WEAP.
  * Body armour   COBJ crafted at Workbench_Crafting_Armor whose CNAM is an ARMO
                  that has a legendary attach point (ap_Legendary1). That keeps
                  real armour pieces and drops outfits / headwear / underarmour.
  * Power armour  COBJ crafted at a power-armour bench whose CNAM is a
                  Armor_PowerArmor_* ARMO.
  One COBJ = one pickable item, so Light / Sturdy / Heavy recipes that share
  one ARMO each get their own entry ("Leather Left Arm (Heavy)").

Mods  (engine rule: an OMOD attaches when EVERY one of its MNAM target
       keywords is on the item — verified against the paint data, where
       "Gunmetal Paint" targets ma_Gun_Appearance AND ma_10mm)
  * Item keywords = the item's own keywords (WEAP Keywords / ARMO SLOTS flat)
    + every target keyword of every mod in the item's object templates (a
    template mod must attach, so its targets are on the item).
  * Legendary-capable armour additionally gets the legendary-crafting
    keywords its attach-point set implies (armour exports omit them).
  * A mod is offered when it has a craft COBJ (non-cosmetic, non-cut), a
    non-zero cost, and targets ⊆ item keywords. Slots = attach points.

Legendary mods
  * Attach recipes  co_mod_Legendary_* COBJs (cost: Legendary Scrip + mod box).
  * Mod boxes       COBJ crafting the LegendaryShard_* item (Legendary Modules +
                    the effect's ingredient). The page can expand boxes into
                    their own cost or count the boxes as owned.
  * Random rolls    co_mod_Legendary_Crafting_{Armor|PowerArmor|Weapon}{N}.

Quantities
  FVPA entries are EDID:base:curve. The real count is the CURV point for
  x=base (linear interpolation between points, like the engine).

Material groups (for the results list)
  currency  CNCY records (FLST Collections_Currency / KYWD refs signature)
  modules   components whose name contains "Module"
  flux      c_NukeFlora_* components
  scrap     every other CMPO component
  junk      MISC items
  aid       ALCH records (chems, serums, food, drink)
  ammo      AMMO records
  weapons   WEAP records used as ingredients (grenades, mines)
  boxes     legendary mod boxes (LegendaryShard_*)
  other     anything unresolved

Inputs (newest export per type, via tsv_source)
  COBJ_Export_*.tsv, OMOD_Export_*.tsv,
  ARMO_Export_*_ARMOUR.tsv, ARMO_Export_*_SLOTS.tsv, ARMO_Export_*_ObjectTemplate.tsv,
  WEAP_Export_*_Base.tsv, WEAP_Export_*_ObjectTemplate.tsv,
  CURV_Export_*_POINTS.tsv, CMPO_Export_*.tsv, MISC_Export_*.tsv,
  ALCH_Export_*.tsv, FLST_Export_*_Entries.tsv,
  KYWD_Export_*.tsv, KYWD_Export_*_Refs.tsv (optional)

Usage
  python src/build_crafting_materials_calculator_json.py            # live
  python src/build_crafting_materials_calculator_json.py --pts      # reads tsv/pts, writes dist/pts/
  python src/build_crafting_materials_calculator_json.py --out path.json

The PTS workflow (dfbnb-pts-build.yml) normalises tsv/pts into tsv/ and runs
this WITHOUT --pts, then relocates dist/ -> dist/pts/. Both paths end up at
dist/pts/calculators/crafting_materials_calculator.json.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import os
import re
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tsv_source  # noqa: E402  one resolver for every export selection

try:
    from cut_content import is_cut  # noqa: E402
except Exception:  # pragma: no cover - keep the build alive without it
    def is_cut(edid):
        return bool(re.match(r"^(DEL|CUT|POST|ZZZ|zz|TEST|DEBUG|DEPRECATED)", edid or "", re.I))

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_NAME = "crafting_materials_calculator.json"

# ── workbench + category rules ────────────────────────────────────────────────
BENCH_WEAPON = {"Workbench_Crafting_Weapon"}
BENCH_ARMOUR = {"Workbench_Crafting_Armor"}
BENCH_PA = {"PowerArmorWorkbenchKeyword", "Workbench_Crafting_PowerArmor"}

CATEGORIES = [
    ("body-armour", "Body Armour"),
    ("power-armour", "Power Armour"),
    ("weapons", "Weapons"),
]

# Legendary-crafting keywords implied by a legendary attach point on armour.
# (ARMO exports carry no keyword list for most armour; weapons carry their own.)
LEG_KW_BODY = {"ma_legendarycrafting_armor", "ma_Misc_Legendarycrafting_Armor4"}
LEG_KW_PA = {"ma_legendarycrafting_powerarmor", "ma_PowerArmorMod",
             "ma_Misc_Legendarycrafting_PowerArmor4"}

RANDOM_ROLL_RE = re.compile(r"^co_mod_Legendary_Crafting_(Armor|PowerArmor|Weapon)(\d)$")
RANDOM_ROLL_CAT = {"Armor": "body-armour", "PowerArmor": "power-armour", "Weapon": "weapons"}

# COBJ / record names that are never a real, craftable player recipe.
SKIP_SUBSTR = ("REPAIRONLY", "RepairOnly", "_DUPLICATE", "NONPLAYABLE", "NotPlayable",
               "NPCONLY", "UNPLAYABLE", "DEPRECATED", "_TEMP_", "BOUNTY")
SKIP_PREFIX = ("SURVIVAL_", "Survival_", "zzz", "ZZZ", "CUT_", "DEL_", "DEBUG", "TEST_")
COSMETIC_RE = re.compile(r"(^|_)(ATX|SCORE)_")

KW_RE = re.compile(r"(\w+)\s+(?:\"\"[^\"]*\"\"\s*|\"[^\"]*\"\s*)?\[KYWD:[0-9A-Fa-f]+\]")
LEGEND_AP_RE = re.compile(r"^ap_Legendary(\d)$")

GROUPS = [
    ("currency", "Currency"),
    ("modules", "Modules"),
    ("flux", "Flux"),
    ("scrap", "Scrap Components"),
    ("junk", "Junk & Misc Items"),
    ("aid", "Aid & Consumables"),
    ("ammo", "Ammo"),
    ("weapons", "Weapons & Explosives"),
    ("boxes", "Legendary Mod Boxes"),
    ("other", "Other"),
]

PIECE_RE = re.compile(
    r"\s*\b(Left Arm|Right Arm|Left Leg|Right Leg|Chest Piece|Chest|Torso|Helmet|Helm|Mask)\b\s*",
    re.I)
PIECE_ORDER = {"helmet": 0, "helm": 0, "mask": 0, "chest piece": 1, "chest": 1, "torso": 1,
               "left arm": 2, "right arm": 3, "left leg": 4, "right leg": 5}
WEIGHT_RE = re.compile(r"_(Light|Medium|Sturdy|Heavy)(?:_|$)")

# Weapon type keywords → dropdown group, first match wins.
WEAPON_TYPE_GROUPS = [
    (("WeaponTypeUnarmed",), "Unarmed"),
    (("WeaponTypeMelee2H",), "Two-Handed Melee"),
    (("WeaponTypeMelee1H",), "One-Handed Melee"),
    (("WeaponTypeThrown", "WeaponTypeGrenade"), "Thrown"),
    (("WeaponTypeBow",), "Bows"),
    (("WeaponTypeHeavyGun", "WeaponTypeHeavy"), "Heavy Guns"),
    (("WeaponTypeShotgun",), "Shotguns"),
    (("WeaponTypeRifle", "WeaponTypeSniper"), "Rifles"),
    (("WeaponTypePistol",), "Pistols"),
]
WEAPON_OTHER_GROUP = "Other Guns (Pistol / Rifle by mods)"


# ── small helpers ─────────────────────────────────────────────────────────────
def read_tsv(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def g(row, *keys):
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v).strip().strip('"')
    return ""


def skip_edid(edid):
    e = (edid or "").strip()
    if not e:
        return True
    if is_cut(e) or e.startswith(SKIP_PREFIX):
        return True
    return any(s in e for s in SKIP_SUBSTR)


def is_cosmetic(edid):
    return bool(COSMETIC_RE.search(edid or ""))


def kw_list(text):
    """Keyword EDIDs from either 'ma_x "Name" [KYWD:..] | ...' or 'a|b c' styles."""
    text = text or ""
    found = KW_RE.findall(text)
    if found:
        return set(found)
    return {t for t in re.split(r"[|\s,]+", text) if t and not t.startswith("[")}


def lead_edid(cell):
    m = re.match(r"\s*([A-Za-z0-9_]+)", cell or "")
    return m.group(1) if m else ""


def pretty(edid):
    e = re.sub(r"^(?:c_|(?:ap|ma)_(?:gun_|melee_|armor_|PowerArmor_)?)", "", edid or "")
    e = re.sub(r"_scrap$", "", e)
    e = e.replace("_", " ")
    e = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", e)
    return re.sub(r"\s+", " ", e).strip() or edid


def star_text(name):
    """'¬¬ Rushing' → ('Rushing', 2). The game uses ¬ as the star glyph."""
    n = (name or "").strip()
    stars = n.count("¬")
    return n.replace("¬", "").strip(), stars


# ── curves ────────────────────────────────────────────────────────────────────
class Curves:
    def __init__(self, rows):
        pts = defaultdict(dict)
        for r in rows:
            e = g(r, "EDID")
            try:
                x = float(g(r, "X"))
                y = float(g(r, "Y"))
            except ValueError:
                continue
            if e:
                pts[e][x] = y
        self.pts = {k: sorted(v.items()) for k, v in pts.items()}
        self.missing = Counter()

    def resolve(self, base, curve):
        if not curve:
            return base
        p = self.pts.get(curve)
        if not p:
            self.missing[curve] += 1
            return base
        xs = [a for a, _ in p]
        x = float(base)
        i = bisect.bisect_left(xs, x)
        if i < len(xs) and xs[i] == x:
            y = p[i][1]
        elif i == 0:
            y = p[0][1]
        elif i >= len(xs):
            y = p[-1][1]
        else:
            (x0, y0), (x1, y1) = p[i - 1], p[i]
            y = y0 + (y1 - y0) * (x - x0) / (x1 - x0)
        return int(y + 0.5)


# ── main build ────────────────────────────────────────────────────────────────
def build(channel):
    def need(pattern, required=True, exclude=None):
        return tsv_source.newest(pattern, channel=channel, required=required, exclude=exclude)

    src = {
        "COBJ": need("COBJ_Export_*.tsv"),
        "OMOD": need("OMOD_Export_*.tsv", exclude="Properties"),
        "ARMO": need("ARMO_Export_*_ARMOUR.tsv"),
        "ARMO_SLOTS": need("ARMO_Export_*_SLOTS.tsv", required=False),
        "ARMO_TMPL": need("ARMO_Export_*_ObjectTemplate.tsv", required=False),
        "WEAP": need("WEAP_Export_*_Base.tsv"),
        "WEAP_TMPL": need("WEAP_Export_*_ObjectTemplate.tsv", required=False),
        "CURV": need("CURV_Export_*_POINTS.tsv"),
        "CMPO": need("CMPO_Export_*.tsv", required=False),
        "MISC": need("MISC_Export_*.tsv", required=False),
        "ALCH": need("ALCH_Export_*.tsv", required=False, exclude="Effects"),
        "FLST": need("FLST_Export_*_Entries.tsv", required=False),
        "KYWD": need("KYWD_Export_*.tsv", required=False, exclude="Refs"),
        "KYWD_REFS": need("KYWD_Export_*_Refs.tsv", required=False),
    }
    for k, v in src.items():
        print(f"  {k:<10} {os.path.basename(v) if v else '(none)'}")

    curves = Curves(read_tsv(src["CURV"]))

    # ---- names / signatures for every record we might need to label --------
    names, sigs = {}, {}

    def note(edid, name, sig=None, force=False):
        if not edid:
            return
        if name and (force or edid not in names):
            names[edid] = name
        if sig and edid not in sigs:
            sigs[edid] = sig

    for r in read_tsv(src["CMPO"]):
        note(g(r, "CMPO_EDID"), g(r, "FULL"), "CMPO", force=True)
    for r in read_tsv(src["MISC"]):
        note(g(r, "EDID"), g(r, "FULL"), "MISC")
    for r in read_tsv(src["ALCH"]):
        note(g(r, "ALCH_EDID", "EDID"), g(r, "FULL"), "ALCH")
    for r in read_tsv(src["FLST"]):
        note(g(r, "Entry_EDID"), g(r, "Entry_FULL"), g(r, "Entry_Sig"))
    for r in read_tsv(src["KYWD_REFS"]):
        note(g(r, "RefEDID"), g(r, "RefName"), g(r, "RefSignature"))

    kw_label = {}
    for r in read_tsv(src["KYWD"]):
        kw_label[g(r, "EDID")] = g(r, "NNAM_DisplayName", "FULL_Name")

    # ---- weapons --------------------------------------------------------------
    weap = {}
    for r in read_tsv(src["WEAP"]):
        e = g(r, "WEAP_EDID")
        if e:
            weap[e] = {"name": g(r, "WEAP_FULL"), "kw": kw_list(r.get("Keywords")),
                       "appr": kw_list(r.get("APPR_Slots"))}
            note(e, g(r, "WEAP_FULL"), "WEAP")
    weap_tmpl = defaultdict(set)
    for r in read_tsv(src["WEAP_TMPL"]):
        m = lead_edid(r.get("Include_Mod"))
        if m:
            weap_tmpl[g(r, "WEAP_EDID")].add(m)

    # ---- armour ---------------------------------------------------------------
    armo = {}
    for r in read_tsv(src["ARMO"]):
        e = g(r, "ARMO_EDID")
        if not e:
            continue
        appr = set()
        for i in range(1, 40):
            v = r.get(f"APPR_{i}")
            if v is None:
                break
            if v.strip():
                appr.add(v.split(":")[0].strip())
        armo[e] = {"name": g(r, "ARMO_FULL"), "appr": appr, "kw": set()}
    for r in read_tsv(src["ARMO_SLOTS"]):
        e = g(r, "ARMO_EDID")
        if e in armo:
            armo[e]["kw"] |= kw_list(r.get("Keywords_EDID_Flat"))
    armo_tmpl = defaultdict(set)
    for r in read_tsv(src["ARMO_TMPL"]):
        m = lead_edid(r.get("Include_Mod"))
        if m:
            armo_tmpl[g(r, "ARMO_EDID")].add(m)

    # ---- OMOD -----------------------------------------------------------------
    omod = {}
    for r in read_tsv(src["OMOD"]):
        e = g(r, "OMOD_EDID")
        if not e:
            continue
        omod[e] = {
            "name": g(r, "FULL"),
            "ap": g(r, "AttachPoint_EDID"),
            "apn": g(r, "AttachPoint_Name"),
            "tg": kw_list(r.get("MNAM_TargetKWDs")),
        }

    # ---- COBJ -----------------------------------------------------------------
    cobj_rows = read_tsv(src["COBJ"])
    by_cnam = defaultdict(list)
    for r in cobj_rows:
        c = g(r, "CNAM_EDID")
        if c:
            by_cnam[c].append(r)
            note(c, g(r, "CNAM_FULL"))  # ammo etc. are only named here

    used_mats = Counter()

    def parse_cost(row):
        out = {}
        for part in (row.get("FVPA") or "").split("|"):
            bits = part.strip().split(":")
            if len(bits) < 2 or not bits[0]:
                continue
            try:
                base = int(float(bits[1]))
            except ValueError:
                continue
            qty = curves.resolve(base, bits[2] if len(bits) > 2 else "")
            if qty > 0:
                out[bits[0]] = out.get(bits[0], 0) + qty
        cost = sorted(out.items(), key=lambda kv: kv[0].lower())
        return [[m, q] for m, q in cost]

    def use(cost):
        for m, _ in cost:
            used_mats[m] += 1
        return cost

    def craft_cobj(cnam, prefer=None):
        """First usable (non-cut, non-cosmetic) COBJ crafting `cnam`."""
        rows = [r for r in by_cnam.get(cnam, [])
                if not skip_edid(g(r, "COBJ_EDID")) and not is_cosmetic(g(r, "COBJ_EDID"))]
        if prefer:
            rows.sort(key=lambda r: 0 if g(r, "COBJ_EDID").startswith(prefer) else 1)
        return rows[0] if rows else None

    # ---- craftable (non-legendary) mods, indexed by target keyword ---------
    mods_out = {}
    mod_index = defaultdict(set)  # target kw → mod ids
    for e, o in omod.items():
        if not o["tg"] or (o["ap"] or "").startswith("ap_Legendary") or o["ap"] == "ap_customName":
            continue
        if skip_edid(e) or is_cosmetic(e):
            continue
        row = craft_cobj(e)
        if not row:
            continue
        cost = parse_cost(row)
        if not cost:
            continue
        label = kw_label.get(o["ap"]) or o["apn"] or pretty(o["ap"])
        if label.lower().startswith("no "):
            label = label[3:].strip().title() if label[3:].islower() else label[3:].strip()
        if label.lower().startswith("default "):
            label = label[8:].strip()
        if label.lower() in ("misc", ""):
            label = "Misc"
        name = o["name"] or g(row, "CNAM_FULL") or pretty(e)
        mods_out[e] = {"n": name.replace("¬", "★"), "ap": o["ap"], "s": label,
                       "c": cost, "_tg": o["tg"]}
        for t in o["tg"]:
            mod_index[t].add(e)

    # ---- legendary mods, boxes and random rolls ----------------------------
    legendary = {}
    crafts = {}
    for r in cobj_rows:
        ce = g(r, "COBJ_EDID")
        cn = g(r, "CNAM_EDID")
        o = omod.get(cn)
        if not o:
            continue
        m = LEGEND_AP_RE.match(o["ap"] or "")
        if not m or skip_edid(ce) or skip_edid(cn) or is_cosmetic(ce):
            continue
        cost = parse_cost(r)
        box = next((x for x, _ in cost if "LegendaryShard" in x), "")
        if not box:
            continue
        nm, _ = star_text(o["name"] or g(r, "CNAM_FULL"))
        legendary[cn] = {"n": nm, "star": int(m.group(1)), "c": cost, "box": box,
                         "_tg": o["tg"]}
        # the box's own recipe (Legendary Modules + effect ingredient)
        if box not in crafts:
            br = craft_cobj(box, prefer="co_LegendaryShard_")
            if br:
                bc = parse_cost(br)
                if bc:
                    crafts[box] = bc

    random_rolls = defaultdict(dict)
    for r in cobj_rows:
        m = RANDOM_ROLL_RE.match(g(r, "COBJ_EDID"))
        if m:
            cost = parse_cost(r)
            if cost:
                random_rolls[RANDOM_ROLL_CAT[m.group(1)]][m.group(2)] = use(cost)

    # ---- items ----------------------------------------------------------------
    def weapon_group(kws):
        for keys, label in WEAPON_TYPE_GROUPS:
            if any(k in kws for k in keys):
                return label
        return WEAPON_OTHER_GROUP

    def item_keywords(own, tmpl_mods):
        k = set(own)
        for mid in tmpl_mods:
            k |= omod.get(mid, {}).get("tg", set())
        return k

    items = {key: [] for key, _ in CATEGORIES}
    seen = set()
    for r in cobj_rows:
        ce, cn, bench = g(r, "COBJ_EDID"), g(r, "CNAM_EDID"), g(r, "BNAM_EDID")
        if ce in seen or skip_edid(ce) or is_cosmetic(ce) or skip_edid(cn):
            continue
        if bench in BENCH_WEAPON and cn in weap:
            cat = "weapons"
            w = weap[cn]
            kws = item_keywords(w["kw"], weap_tmpl.get(cn, ()))
            name = w["name"] or g(r, "CNAM_FULL")
            grp = weapon_group(w["kw"])
            legendary_ok = "ap_Legendary1" in w["appr"]
        elif cn in armo and (bench in BENCH_PA and "PowerArmor" in cn):
            cat = "power-armour"
            a = armo[cn]
            legendary_ok = "ap_Legendary1" in a["appr"]
            kws = item_keywords(a["kw"], armo_tmpl.get(cn, ()))
            if legendary_ok:
                kws |= LEG_KW_PA
            name = a["name"] or g(r, "CNAM_FULL")
            grp = PIECE_RE.sub(" ", name).strip() or "Power Armour"
        elif cn in armo and bench in BENCH_ARMOUR and "ap_Legendary1" in armo[cn]["appr"] \
                and "PowerArmor" not in cn:
            cat = "body-armour"
            a = armo[cn]
            legendary_ok = True
            kws = item_keywords(a["kw"], armo_tmpl.get(cn, ())) | LEG_KW_BODY
            name = a["name"] or g(r, "CNAM_FULL")
            grp = PIECE_RE.sub(" ", name).strip() or "Armour"
        else:
            continue
        if not name or "¬" in name:
            continue
        cost = parse_cost(r)
        if not cost:
            continue
        seen.add(ce)

        wm = WEIGHT_RE.search(ce)
        weight = wm.group(1) if wm else ""

        # mods: every target keyword present on the item
        cand = set()
        for t in kws:
            cand |= mod_index.get(t, set())
        slots = defaultdict(list)
        dedupe = set()
        for mid in sorted(cand):
            mo = mods_out[mid]
            if not mo["_tg"] <= kws:
                continue
            key = (mo["ap"], mo["n"], json.dumps(mo["c"]))
            if key in dedupe:
                continue
            dedupe.add(key)
            slots[(mo["ap"], mo["s"])].append(mid)

        slot_list = []
        for (ap, label), mids in sorted(slots.items(), key=lambda kv: (kv[0][1] == "Paint", kv[0][1])):
            mids.sort(key=lambda m: mods_out[m]["n"].lower())
            slot_list.append({"k": ap, "l": label, "m": mids})

        leg = {}
        if legendary_ok:
            per = defaultdict(list)
            for lid, lo in legendary.items():
                if lo["_tg"] and lo["_tg"] <= kws:
                    per[str(lo["star"])].append(lid)
            for s, ids in sorted(per.items()):
                # one entry per effect name (armour + PA copies can both match)
                byname = {}
                for lid in sorted(ids):
                    byname.setdefault(legendary[lid]["n"].lower(), lid)
                leg[s] = sorted(byname.values(), key=lambda x: legendary[x]["n"].lower())

        items[cat].append({
            "id": ce, "n": name, "w": weight, "g": grp, "cnam": cn,
            "c": use(cost), "slots": slot_list, "leg": leg,
        })

    # names: disambiguate same-name recipes (Light / Sturdy / Heavy, variants)
    for cat, lst in items.items():
        counts = Counter(i["n"] for i in lst)
        for it in lst:
            if counts[it["n"]] > 1 and it["w"]:
                it["n"] = f'{it["n"]} ({it["w"]})'
        counts = Counter(i["n"] for i in lst)
        dup_seen = Counter()
        for it in lst:
            if counts[it["n"]] > 1:
                dup_seen[it["n"]] += 1
                if dup_seen[it["n"]] > 1:
                    it["n"] = f'{it["n"]} (alt {dup_seen[it["n"]]})'
        weight_rank = {"Light": 0, "Medium": 1, "Sturdy": 1, "Heavy": 2, "": 3}

        def sort_key(it):
            base = PIECE_RE.search(it["n"])
            pr = PIECE_ORDER.get(base.group(1).lower(), 9) if base and cat != "weapons" else 9
            return (it["g"].lower(), pr, weight_rank.get(it["w"], 3), it["n"].lower())
        lst.sort(key=sort_key)

    # keep only mods / legendary entries something actually references
    ref_mods, ref_leg = set(), set()
    for lst in items.values():
        for it in lst:
            for s in it["slots"]:
                ref_mods.update(s["m"])
            for ids in it["leg"].values():
                ref_leg.update(ids)
    mods_final = {}
    for mid in sorted(ref_mods):
        mo = mods_out[mid]
        mods_final[mid] = {"n": mo["n"], "c": use(mo["c"])}
    leg_final = {}
    for lid in sorted(ref_leg):
        lo = legendary[lid]
        leg_final[lid] = {"n": lo["n"], "star": lo["star"], "c": use(lo["c"]), "box": lo["box"]}
    crafts_final = {}
    for lo in leg_final.values():
        b = lo["box"]
        if b in crafts and b not in crafts_final:
            crafts_final[b] = use(crafts[b])

    # ---- materials ------------------------------------------------------------
    currency = {g(r, "Entry_EDID") for r in read_tsv(src["FLST"])
                if g(r, "FLST_EDID") == "Collections_Currency"}

    def group_of(e):
        s = sigs.get(e, "")
        nm = names.get(e, "")
        if e in currency or s == "CNCY":
            return "currency"
        if "LegendaryShard" in e:
            return "boxes"
        if s == "CMPO" or e.startswith("c_"):
            if "module" in (nm + e).lower():
                return "modules"
            if e.startswith("c_NukeFlora"):
                return "flux"
            return "scrap"
        if s == "ALCH":
            return "aid"
        if s == "AMMO" or e.startswith("Ammo"):
            return "ammo"
        if s == "WEAP":
            return "weapons"
        if s == "MISC":
            return "junk"
        return "other"

    materials = {}
    for e in sorted(used_mats):
        nm = names.get(e) or pretty(e)
        grp = group_of(e)
        if grp == "boxes":
            base, stars = star_text(nm)
            nm = f"{base} {'★' * stars}".strip() if stars else base
            nm = f"Mod Box: {nm}"
        materials[e] = {"n": nm, "g": grp}

    # ---- pool identical slot layouts / legendary lists (keeps the JSON small:
    #      Light/Sturdy/Heavy and same-set pieces share them) -----------------
    slot_pool, leg_pool = [], []
    slot_ix, leg_ix = {}, {}

    def pooled(value, pool, index):
        key = json.dumps(value, sort_keys=True)
        if key not in index:
            index[key] = len(pool)
            pool.append(value)
        return index[key]

    cats_out = []
    for k, l in CATEGORIES:
        lst = []
        for it in items[k]:
            lst.append({
                "id": it["id"], "n": it["n"], "g": it["g"], "c": it["c"],
                "s": pooled(it["slots"], slot_pool, slot_ix),
                "l": pooled(it["leg"], leg_pool, leg_ix) if it["leg"] else -1,
            })
        cats_out.append({"key": k, "label": l, "items": lst})

    out = {
        "_meta": {
            "title": "Crafting Materials Calculator",
            "built_by": os.path.basename(__file__),
            "channel": channel,
            "generated": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data_observed": tsv_source.observed(),
            "sources": {k: os.path.basename(v) for k, v in src.items() if v},
            "counts": {
                **{k: len(v) for k, v in items.items()},
                "mods": len(mods_final),
                "legendary": len(leg_final),
                "mod_boxes_craftable": len(crafts_final),
                "materials": len(materials),
            },
        },
        "groups": [{"key": k, "label": l} for k, l in GROUPS],
        "categories": cats_out,
        "slotSets": slot_pool,
        "legSets": leg_pool,
        "mods": mods_final,
        "legendary": leg_final,
        "crafts": crafts_final,
        "random": {k: v for k, v in random_rolls.items()},
        "materials": materials,
    }
    if curves.missing:
        out["_meta"]["missing_curves"] = sorted(curves.missing)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--pts", action="store_true", help="read tsv/pts, write dist/pts/")
    ap.add_argument("--out", default="", help="explicit output path")
    args = ap.parse_args()
    channel = "pts" if args.pts else tsv_source.channel_of()

    print(f"[crafting-materials] channel={channel}")
    data = build(channel)

    if args.out:
        out_path = args.out
    elif channel == "pts":
        out_path = os.path.join(ROOT, "dist", "pts", "calculators", OUT_NAME)
    else:
        out_path = os.path.join(ROOT, "dist", "calculators", OUT_NAME)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    c = data["_meta"]["counts"]
    if not (c["body-armour"] and c["power-armour"] and c["weapons"]):
        # Never publish an empty calculator over a good one.
        print(f"[crafting-materials] ERROR: empty category in {c} — not writing.")
        sys.exit(1)

    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out_path)
    size = os.path.getsize(out_path) / 1024
    print(f"[crafting-materials] wrote {os.path.relpath(out_path, ROOT)} ({size:.0f} KB) {c}")


if __name__ == "__main__":
    main()
