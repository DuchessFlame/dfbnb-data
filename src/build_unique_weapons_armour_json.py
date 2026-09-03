#!/usr/bin/env python3
"""
build_unique_weapons_armour_json.py
===================================
Builds the DATA pipeline for the "Unique Weapons and Armour" page on
buffsnbrew.com (DF/BNB).

Rebuilds the union of unique named items from three independent sources and
dedupes them, because the old master leveled list LL_Weapon_Unique_All is gone
from the current exports.

  WEAPONS
    * Reuse foundation: dist/unique_weapons/unique_weapons.json (93 uniques).
    * Fresh pass over CURRENT July exports: every weapon OMOD whose _Properties
      carry a CustomItemName_/CustomItem_SpeciallyNamed keyword (~89). Each is
      resolved through its host WEAP ObjectTemplate combo -> base stats + fixed
      star effects. ATX_* paints/skins surface as cosmetic entries.
    * Union by custom-mod formId then normalised name. Reuse-only items kept
      with inCurrentExport=false.

  ARMOUR
    * LVLI LL_Armor_Unique_All -> 8 unique armours; 3 weight variants collapse
      into one item; fixed stars from the ARMO ObjectTemplate combo carrying
      the mod_Custom_<name>.
    * Cosmetic named armour: 6 Secret Settler name plates + Blue Ridge
      Guardsmen Paint (cosmeticOnly).

  POWER ARMOUR
    * None exist in the exports (no SpeciallyNamed PA OMODs, no PA ARMO with
      fixed-legendary custom combos); zero emitted.

  SOURCES: reward-pool aggregate JSONs + GMRW ParentQuestDisplay.

Reuses helpers from build_unique_weapons_json.py.

Output:
  dist/unique_weapons_armour/unique_weapons_armour.json         (LIVE)
  dist/pts/unique_weapons_armour/unique_weapons_armour.json     (PTS)
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
TSV_DIR = REPO_ROOT / "tsv"

sys.path.insert(0, str(SRC_DIR))
import tsv_source
from patchlog_utils import write_empty_patchlog_feed

import build_unique_weapons_json as buw
from build_unique_weapons_json import (
    strip_q, norm_formid, read_tsv, safe_int, safe_float, is_cut,
    parse_omod_ref, humanise_ammo, classify_weapon_type,
    resolve_base_weapon_name, camelcase_to_display, match_obtain_fallback,
)


def pick(pattern, channel, exclude=None):
    """Newest export for `pattern` on `channel`; PTS falls back to LIVE."""
    path = tsv_source.newest(pattern, channel=channel, exclude=exclude,
                             required=False)
    if path is None and channel == "pts":
        path = tsv_source.newest(pattern, channel="live", exclude=exclude,
                                 required=False)
    return path


_SLUG_SEEN = {}


def slugify(text):
    s = re.sub(r"[^a-z0-9]+", "-", str(text or "").lower()).strip("-")
    return s or "item"


def unique_slug(base):
    s = slugify(base)
    if s not in _SLUG_SEEN:
        _SLUG_SEEN[s] = 1
        return s
    _SLUG_SEEN[s] += 1
    return f"{s}-{_SLUG_SEEN[s]}"


def norm_name(text):
    s = str(text or "").strip().strip('"')
    s = re.sub(r"\s*(Custom Mod|Custom Name|Custom|Bounty)\s*$", "", s, flags=re.I)
    s = re.sub(r"\b(Vault ?63|V-?63)\b", "v63", s, flags=re.I)
    s = s.lower().replace("the ", "")
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def clean_display(full):
    s = strip_q(full)
    s = re.sub(r"\s*(Custom Mod|Custom Name|Custom Paint|Custom)\s*$", "", s, flags=re.I)
    return s.strip()


def classify_source(howto):
    h = (howto or "").strip()
    hl = h.lower()
    if not h or hl == "unknown":
        return "Unknown"
    if hl.startswith("quest") or "camden park daily" in hl:
        return "Quest"
    if hl.startswith("vendor") or "vendor:" in hl:
        return "Vendor"
    if "purveyor" in hl:
        return "Purveyor"
    if "daily ops" in hl:
        return "Daily Ops"
    if "score season" in hl or hl.startswith("score"):
        return "Score"
    if "expedition" in hl:
        return "Expedition"
    if "treasure map" in hl:
        return "Treasure Map"
    if "seasonal" in hl:
        return "Seasonal"
    if "atom" in hl:
        return "Atom Shop"
    if "world drop" in hl or "world-drop" in hl:
        return "World Drop"
    if "treasure hunter" in hl or "mutated" in hl or hl.startswith("event") \
            or "public event" in hl:
        return "Public Event" if "public" in hl else "Event"
    if "survival mode" in hl or "legacy" in hl:
        return "Unknown"
    return "Unknown"


_FID_RE = re.compile(r"\b([0-9A-Fa-f]{8})\b")


def _iter_events(obj):
    if isinstance(obj, dict):
        evs = obj.get("events")
        if isinstance(evs, list):
            return evs
    if isinstance(obj, list):
        return obj
    return []


def build_pool_index():
    files = [
        ("dist/activities/activities_rewards.json", "Public Event", "Event"),
        ("dist/events/events_rewards.json", "Public Event", "Event"),
        ("dist/seasonal_events/seasonal_events_rewards.json", "Seasonal", "Seasonal Event"),
        ("dist/daily_ops/daily_ops_rewards.json", "Daily Ops", "Daily Ops"),
        ("dist/expos/expos_rewards.json", "Expedition", "Expedition"),
        ("dist/bounty-hunting/bounty_hunting_rewards.json", "Vendor", "Bounty Hunting"),
    ]
    by_name, by_fid = {}, {}
    for rel, stype, prefix in files:
        p = REPO_ROOT / rel
        if not p.is_file():
            continue
        try:
            data = json.load(open(p, encoding="utf-8"))
        except Exception:
            continue
        for ev in _iter_events(data):
            if not isinstance(ev, dict):
                continue
            label = (ev.get("name") or ev.get("title") or ev.get("gameName")
                     or ev.get("slug") or "")
            if not label:
                continue
            blob = json.dumps(ev, ensure_ascii=False)
            entry = (stype, f"{prefix}: {label}", label)
            for fid in set(m.upper() for m in _FID_RE.findall(blob)):
                by_fid.setdefault(fid, entry)
            for nm in set(re.findall(r'"name"\s*:\s*"([^"]+)"', blob)):
                key = norm_name(nm)
                if key and len(key) >= 4:
                    by_name.setdefault(key, entry)
    return by_name, by_fid


def blank_item():
    return {
        "id": "", "name": "", "kind": "", "subType": "", "base": "",
        "image": "", "ddsHandle": "", "stats": {}, "starEffects": [],
        "inherentEffect": "", "buffs": [], "cosmeticOnly": False,
        "cosmeticNote": "", "howToObtain": "", "sourceType": "Unknown",
        "sourceRef": "", "questName": "", "craftable": False, "planName": "",
        "tradeable": None, "formId": "", "edid": "", "inCurrentExport": True,
        "isCut": False,
    }


# --------------------------------------------------------------------------
#  WEAPONS
# --------------------------------------------------------------------------

def load_current_omods(channel):
    omod_base = pick("OMOD_Export_*.tsv", channel, exclude="_Properties")
    omod_prop = pick("OMOD_Export_*_Properties.tsv", channel)
    by_fid, by_edid = {}, {}
    props = defaultdict(list)
    if omod_base:
        for r in read_tsv(omod_base):
            fid = norm_formid(r.get("OMOD_FormID", ""))
            ed = strip_q(r.get("OMOD_EDID", ""))
            if fid:
                by_fid[fid] = r
            if ed:
                by_edid[ed] = r
    if omod_prop:
        for r in read_tsv(omod_prop):
            fid = norm_formid(r.get("OMOD_FormID", ""))
            if fid:
                props[fid].append(r)
    cn_weapon = set()
    for fid, rows in props.items():
        for pr in rows:
            if strip_q(pr.get("PropertyName", "")) == "Keywords":
                v1 = pr.get("Value1", "")
                if "CustomItemName_" in v1 or "CustomItem_SpeciallyNamed" in v1:
                    if strip_q(by_fid.get(fid, {}).get("FormType", "")) == "Weapon":
                        cn_weapon.add(fid)
                    break
    return by_fid, by_edid, props, cn_weapon


def load_weap_tables(channel):
    base_p = pick("WEAP_Export_*_Base.tsv", channel)
    dnam_p = pick("WEAP_Export_*_DNAM.tsv", channel)
    objt_p = pick("WEAP_Export_*_ObjectTemplate.tsv", channel)
    base = {norm_formid(r.get("WEAP_FormID", "")): r for r in read_tsv(base_p)} if base_p else {}
    dnam = {norm_formid(r.get("WEAP_FormID", "")): r for r in read_tsv(dnam_p)} if dnam_p else {}
    combos = defaultdict(lambda: defaultdict(lambda: {"full": "", "mods": []}))
    host = defaultdict(list)
    if objt_p:
        for r in read_tsv(objt_p):
            wf = norm_formid(r.get("WEAP_FormID", ""))
            ci = strip_q(r.get("CombinationIndex", ""))
            cd = combos[wf][ci]
            cf = strip_q(r.get("Combination_FULL", ""))
            if cf and not cd["full"]:
                cd["full"] = cf
            m = strip_q(r.get("Include_Mod", ""))
            if m:
                me, mn, mf = parse_omod_ref(m)
                cd["mods"].append((me, mn, mf))
                if mf:
                    host[mf].append((wf, ci))
    return base, dnam, combos, host


STAR_RE = re.compile(r"(?:RA_)?mod_Legendary_Weapon(\d)_")


def combo_star_effects(combo, omod_by_fid):
    out, seen = [], set()
    for me, mn, mf in combo.get("mods", []):
        if not me:
            continue
        m = STAR_RE.match(me)
        if not m:
            continue
        star = int(m.group(1))
        if star in seen:
            continue
        seen.add(star)
        nm = strip_q(omod_by_fid.get(mf, {}).get("FULL", "")) or strip_q(mn) or me
        out.append({"star": star, "name": nm, "edid": me,
                    "formId": mf or "", "fixed": True})
    out.sort(key=lambda x: x["star"])
    return out


def weap_stats_from_dnam(base_row, dnam_row):
    return {
        "damage": safe_int(dnam_row.get("DNAM_BaseDamage")),
        "fireRate": safe_float(dnam_row.get("DNAM_Speed")),
        "range": safe_int(dnam_row.get("DNAM_MaxRange")),
        "weight": safe_float(dnam_row.get("DNAM_Weight")),
        "value": safe_int(dnam_row.get("DNAM_Value")),
        "ammoType": humanise_ammo(dnam_row.get("DNAM_Ammo")),
        "capacity": safe_int(dnam_row.get("DNAM_Capacity")),
        "apCost": safe_int(dnam_row.get("DNAM_ActionPointCost")),
        "critMult": safe_float(dnam_row.get("CRDT_CritDamageMult")),
        "minLevel": safe_int(base_row.get("EILV_Level")),
    }


APPEARANCE_APS = ("appearance", "skin", "paint", "modelswap")


def is_cosmetic_omod(omod_row):
    ap = strip_q(omod_row.get("AttachPoint_EDID", "")).lower()
    ed = strip_q(omod_row.get("OMOD_EDID", "")).lower()
    if any(t in ap for t in APPEARANCE_APS):
        return True
    if "paint" in ed or "skin" in ed or "modelswap" in ed:
        return True
    return False


SUPP_OBTAIN = {
    "ChaosEngine": "Quest: Skyline Valley (Vault 63)",
    "Tempest": "Quest: Skyline Valley (Vault 63)",
    "Splinter": "Quest: Skyline Valley (Vault 63)",
}


def obtain_for(edid, name, pool_name, pool_fid, formid=None):
    for frag, val in SUPP_OBTAIN.items():
        if frag.lower() in (edid or "").lower():
            return val, classify_source(val), (val.split(":", 1)[1].strip() if ":" in val else "")
    fb = match_obtain_fallback(edid)
    if fb:
        return fb, classify_source(fb), (fb.split(":", 1)[1].strip() if ":" in fb else "")
    if formid and formid.upper() in pool_fid:
        st, ho, ref = pool_fid[formid.upper()]
        return ho, st, ref
    key = norm_name(name)
    if key in pool_name:
        st, ho, ref = pool_name[key]
        return ho, st, ref
    return "Source unconfirmed", "Unknown", ""


def convert_reuse_weapon(w):
    it = blank_item()
    it["name"] = w["name"]
    it["kind"] = "Weapon"
    it["subType"] = w.get("weaponType") or "Unknown"
    it["base"] = w.get("baseWeapon") or ""
    s = w.get("stats", {})
    it["stats"] = {
        "damage": s.get("damage"), "fireRate": s.get("fireRate"),
        "range": s.get("range"), "weight": s.get("weight"),
        "value": s.get("value"), "ammoType": s.get("ammoType"),
        "capacity": s.get("capacity"), "apCost": s.get("apCost"),
        "critMult": s.get("critMult"), "minLevel": s.get("minLevel"),
    }
    it["starEffects"] = [
        {"star": e.get("star"), "name": e.get("name") or "",
         "edid": e.get("edid") or "", "formId": e.get("formId") or "",
         "fixed": bool(e.get("fixed", True))}
        for e in w.get("legendaryEffects", [])
    ]
    inh = ""
    for m in w.get("customMods", []):
        d = (m.get("description") or "").strip()
        if d:
            inh = d
            break
    it["inherentEffect"] = inh
    howto = w.get("howToObtain") or "Unknown"
    if howto == "Unknown":
        howto = ""
    it["howToObtain"] = howto
    it["sourceType"] = classify_source(howto)
    it["questName"] = w.get("questName") or ""
    it["sourceRef"] = it["questName"] or ""
    it["craftable"] = bool(w.get("craftable"))
    it["planName"] = w.get("planName") or ""
    it["tradeable"] = w.get("tradeable")
    it["formId"] = w.get("formId") or ""
    it["edid"] = w.get("edid") or ""
    it["isCut"] = bool(w.get("isCut"))
    if "paint" in w["name"].lower() or "skin" in w["name"].lower():
        it["cosmeticOnly"] = True
        it["cosmeticNote"] = "Visual skin/paint only - no combat effect"
        it["starEffects"] = []
        if not it["howToObtain"]:
            it["howToObtain"] = "Atom Shop"
            it["sourceType"] = "Atom Shop"
    it["_cmod_fids"] = [norm_formid(m["formId"]) for m in w.get("customMods", [])
                        if m.get("formId")]
    return it


def build_fresh_weapon(omod_fid, omod_row, host, combos, weap_base, weap_dnam,
                       omod_by_fid, pool_name, pool_fid):
    hosts = host.get(omod_fid, [])
    omod_edid = strip_q(omod_row.get("OMOD_EDID", ""))
    full = strip_q(omod_row.get("FULL", ""))
    name = clean_display(full) or camelcase_to_display(omod_edid)
    it = blank_item()
    it["kind"] = "Weapon"
    it["name"] = name
    it["edid"] = omod_edid
    it["formId"] = omod_fid
    cosmetic = is_cosmetic_omod(omod_row)
    cut = is_cut(omod_edid)
    weap_fid = ""
    if hosts:
        weap_fid, cidx = hosts[0]
        base_row = weap_base.get(weap_fid, {})
        dnam_row = weap_dnam.get(weap_fid, {})
        w_edid = strip_q(base_row.get("WEAP_EDID", ""))
        it["base"] = resolve_base_weapon_name(w_edid, strip_q(base_row.get("WEAP_FULL", "")))
        it["subType"] = classify_weapon_type(dnam_row, strip_q(base_row.get("Keywords", "")))
        it["stats"] = weap_stats_from_dnam(base_row, dnam_row)
        it["formId"] = weap_fid or omod_fid
        it["edid"] = w_edid or omod_edid
        if not cosmetic:
            it["starEffects"] = combo_star_effects(combos[weap_fid][cidx], omod_by_fid)
        cut = cut or is_cut(w_edid)
    else:
        it["subType"] = "Unknown"
    it["inherentEffect"] = strip_q(omod_row.get("DESC", "")) or ""
    if cosmetic:
        it["cosmeticOnly"] = True
        it["cosmeticNote"] = "Visual skin/paint only - no combat effect"
        it["starEffects"] = []
    howto, stype, ref = obtain_for(omod_edid, name, pool_name, pool_fid,
                                   formid=weap_fid or omod_fid)
    if cosmetic and howto == "Source unconfirmed":
        howto, stype = "Atom Shop", "Atom Shop"
    it["howToObtain"] = howto
    it["sourceType"] = stype
    it["sourceRef"] = ref
    if stype == "Quest":
        it["questName"] = ref
    it["tradeable"] = None
    it["isCut"] = cut
    it["inCurrentExport"] = True
    return it


# --------------------------------------------------------------------------
#  ARMOUR
# --------------------------------------------------------------------------

ARMOUR_UNIQUES = [
    ("Bulwark", "LL_Armor_SecretService_Torso_Bulwark", "Bulwark",
     "Secret Service Armor", "Secret Service Armor (Torso)"),
    ("Last Bastion", "LL_Armor_EnclaveScoutUniform_Torso_Urban_LastBastion",
     "LastBastion", "Enclave Scout Armor", "Urban Scout Armor (Torso)"),
    ("Last Stand", "LL_Armor_BOSInfantry_Torso_LastStand", "LastStand",
     "Brotherhood Recon Armor", "Brotherhood Recon Armor (Torso)"),
    ("Rage", "LL_Armor_RaiderMod_Torso_Rage", "Rage",
     "Raider Armor", "Raider Armor (Torso)"),
    ("Road Kill", "LL_Armor_DLC03_Trapper_ArmLeft_Roadkill", "Roadkill",
     "Trapper Armor", "Trapper Armor (Left Arm)"),
    ("Silver Lining", "LL_Armor_Leather_Torso_SilverLining", "SilverLining",
     "Leather Armor", "Leather Armor (Torso)"),
    ("Stand Fast", "LL_Armor_Combat_Torso_StandFast", "StandFast",
     "Combat Armor", "Combat Armor (Torso)"),
    ("Trail Warden", "LL_Armor_Metal_Torso_TrailWarden", "TrailWarden",
     "Metal Armor", "Metal Armor (Torso)"),
]

ARMO_LEG_RE = re.compile(
    r'mod_Legendary_Armor(\d)_\S+\s+"([^"]+)"\s+\[OMOD:([0-9A-Fa-f]+)\]')


def build_lvli_index(channel):
    p = pick("LVLI_Export_*_LVLI_Entries.tsv", channel)
    idx = defaultdict(list)
    if not p:
        return idx
    for r in read_tsv(p):
        e = strip_q(r.get("LVLI_EDID", ""))
        ref = strip_q(r.get("LVLO_Reference", ""))
        if e and ref:
            idx[e].append(ref)
    return idx


def resolve_lvli_to_armo(top_edid, lvli_idx, depth=0):
    out = []
    for ref in lvli_idx.get(top_edid, []):
        parts = ref.split(":")
        sig = parts[2] if len(parts) > 2 else ""
        edid = parts[1] if len(parts) > 1 else ""
        fid = norm_formid(parts[0])
        if sig == "ARMO":
            out.append((fid, edid))
        elif sig == "LVLI" and depth < 4:
            out += resolve_lvli_to_armo(edid, lvli_idx, depth + 1)
    return out


def load_armo_tables(channel):
    armour_p = pick("ARMO_Export_*_ARMOUR.tsv", channel)
    objt_p = pick("ARMO_Export_*_ObjectTemplate.tsv", channel)
    slots_p = pick("ARMO_Export_*_SLOTS.tsv", channel)
    armour = {norm_formid(r.get("ARMO_FormID", "")): r for r in read_tsv(armour_p)} if armour_p else {}
    combos = defaultdict(lambda: defaultdict(lambda: {"full": "", "mods": []}))
    if objt_p:
        for r in read_tsv(objt_p):
            fid = norm_formid(r.get("ARMO_FormID", ""))
            ci = strip_q(r.get("CombinationIndex", ""))
            cd = combos[fid][ci]
            cf = strip_q(r.get("Combination_FULL", ""))
            if cf and not cd["full"]:
                cd["full"] = cf
            m = strip_q(r.get("Include_Mod", ""))
            if m:
                cd["mods"].append(m)
    slots = {norm_formid(r.get("ARMO_FormID", "")): r for r in read_tsv(slots_p)} if slots_p else {}
    return armour, combos, slots


def armo_stats(row):
    return {
        "physical": safe_int(row.get("DAMA_Physical_Amount")),
        "energy": safe_int(row.get("DAMA_Energy_Amount")),
        "fire": safe_int(row.get("DAMA_Fire_Amount")),
        "cryo": safe_int(row.get("DAMA_Cryo_Amount")),
        "poison": safe_int(row.get("DAMA_Poison_Amount")),
        "rad": safe_int(row.get("DAMA_Rad_Amount")),
        "weight": safe_float(row.get("DATA_Weight")),
        "value": safe_int(row.get("DATA_Value")),
        "health": safe_int(row.get("DATA_Health")),
        "minLevel": None,
    }


def extract_armo_stars(armo_fid, cust_frag, combos):
    frag = cust_frag.replace(" ", "").lower()
    for ci, cd in combos.get(armo_fid, {}).items():
        modline = " ".join(cd["mods"])
        if "mod_Custom" in modline and frag in modline.replace(" ", "").lower():
            stars, seen = [], set()
            for m in cd["mods"]:
                mm = ARMO_LEG_RE.match(m)
                if mm:
                    star = int(mm.group(1))
                    if star in seen:
                        continue
                    seen.add(star)
                    stars.append({"star": star, "name": mm.group(2),
                                  "edid": m.split()[0], "formId": norm_formid(mm.group(3)),
                                  "fixed": True})
            stars.sort(key=lambda x: x["star"])
            if stars:
                return stars
    return []


def build_armour(channel, lvli_idx, armo_tbl, pool_name, pool_fid, gmrw_quest):
    armour_rows, combos, slots = armo_tbl
    items = []
    top_fid_map = {}
    for ref in lvli_idx.get("LL_Armor_Unique_All", []):
        parts = ref.split(":")
        if len(parts) >= 2:
            top_fid_map[parts[1]] = norm_formid(parts[0])
    for name, top_edid, cust_frag, sub, base_label in ARMOUR_UNIQUES:
        armos = resolve_lvli_to_armo(top_edid, lvli_idx)
        armo_fid = armos[0][0] if armos else ""
        row = armour_rows.get(armo_fid, {})
        it = blank_item()
        it["kind"] = "Armour"
        it["name"] = name
        it["subType"] = sub
        it["base"] = base_label
        it["stats"] = armo_stats(row) if row else {}
        it["stats"]["variants"] = "light/medium/heavy"
        it["starEffects"] = extract_armo_stars(armo_fid, cust_frag, combos)
        it["formId"] = armo_fid
        it["edid"] = strip_q(row.get("ARMO_EDID", "")) or top_edid
        it["tradeable"] = None
        it["inCurrentExport"] = True
        top_fid = top_fid_map.get(top_edid, "")
        howto = stype = ref_label = ""
        if top_fid and top_fid in gmrw_quest:
            qn = gmrw_quest[top_fid]
            howto, stype, ref_label = f"Quest: {qn}", "Quest", qn
            it["questName"] = qn
        elif top_fid and top_fid.upper() in pool_fid:
            stype, howto, ref_label = pool_fid[top_fid.upper()]
        else:
            key = norm_name(name)
            if key in pool_name:
                stype, howto, ref_label = pool_name[key]
        if not howto:
            howto, stype = "Source unconfirmed", "Unknown"
        it["howToObtain"] = howto
        it["sourceType"] = stype
        it["sourceRef"] = ref_label
        items.append(it)
    return items


def build_cosmetic_armour(channel, omod_by_fid):
    specs = [
        ("007FFF1C", "Secret Settler (Helmet)", "Secret Service Armor",
         "Secret Service Armor (Helmet)"),
        ("007FFF20", "Secret Settler (Torso)", "Secret Service Armor",
         "Secret Service Armor (Torso)"),
        ("007FFF1E", "Secret Settler (Left Arm)", "Secret Service Armor",
         "Secret Service Armor (Left Arm)"),
        ("007FFF1F", "Secret Settler (Right Arm)", "Secret Service Armor",
         "Secret Service Armor (Right Arm)"),
        ("007FFF1D", "Secret Settler (Left Leg)", "Secret Service Armor",
         "Secret Service Armor (Left Leg)"),
        ("007FFF19", "Secret Settler (Right Leg)", "Secret Service Armor",
         "Secret Service Armor (Right Leg)"),
        ("0069224A", "Blue Ridge Guardsmen Paint", "Armor Paint",
         "Blue Ridge Guardsmen Armor"),
    ]
    items = []
    for fid, name, sub, base_label in specs:
        row = omod_by_fid.get(fid, {})
        it = blank_item()
        it["kind"] = "Armour"
        it["name"] = name
        it["subType"] = sub
        it["base"] = base_label
        it["edid"] = strip_q(row.get("OMOD_EDID", "")) or ""
        it["formId"] = fid
        it["cosmeticOnly"] = True
        it["cosmeticNote"] = "Cosmetic name plate / paint only - no combat effect"
        it["tradeable"] = None
        it["inCurrentExport"] = bool(row)
        if "Blue Ridge" in name:
            it["howToObtain"] = "Purchased from Blue Ridge Caravan vendors"
            it["sourceType"] = "Vendor"
            it["sourceRef"] = "Blue Ridge Caravan"
        else:
            it["howToObtain"] = ("Secret Service armour customisation - applied at "
                                 "an armour workbench (Vault 79 / Gold Bullion plans)")
            it["sourceType"] = "Vendor"
            it["sourceRef"] = "Vault 79 (Gold Bullion)"
        items.append(it)
    return items


def load_gmrw_quest(channel):
    p = pick("GMRW_Export_*.tsv", channel)
    out = {}
    if not p:
        return out
    for r in read_tsv(p):
        rewarded = strip_q(r.get("RewardedItem", ""))
        disp = strip_q(r.get("ParentQuestDisplay", ""))
        if rewarded and disp:
            fid = norm_formid(rewarded.split(":")[0])
            if fid:
                out.setdefault(fid, disp)
    return out


def build_channel(channel, dist_dir):
    global _SLUG_SEEN
    _SLUG_SEEN = {}
    print(f"\n{'='*64}\n  Unique Weapons & Armour -- channel: {channel}\n{'='*64}")

    pool_name, pool_fid = build_pool_index()
    gmrw_quest = load_gmrw_quest(channel)
    omod_by_fid, omod_by_edid, omod_props, cn_weapon = load_current_omods(channel)
    weap_base, weap_dnam, weap_combos, weap_host = load_weap_tables(channel)
    omod_fids_present = set(omod_by_fid.keys())

    reuse_path = REPO_ROOT / "dist" / "unique_weapons" / "unique_weapons.json"
    reuse = json.load(open(reuse_path, encoding="utf-8"))
    reuse_items = [convert_reuse_weapon(w) for w in reuse["weapons"]]

    reuse_by_cmod, reuse_by_name = {}, {}
    for it in reuse_items:
        for cf in it["_cmod_fids"]:
            reuse_by_cmod[cf] = it
        reuse_by_name.setdefault(norm_name(it["name"]), it)

    for it in reuse_items:
        cmod_in = any(cf in omod_fids_present for cf in it["_cmod_fids"])
        base_in = norm_formid(it["formId"]) in weap_base
        no_cmod = len(it["_cmod_fids"]) == 0
        it["inCurrentExport"] = bool(cmod_in or (no_cmod and base_in))

    fresh_new = []
    matched_ct = 0
    for omod_fid in sorted(cn_weapon):
        row = omod_by_fid.get(omod_fid, {})
        full = strip_q(row.get("FULL", ""))
        nn = norm_name(full)
        matched = reuse_by_cmod.get(omod_fid) or reuse_by_name.get(nn)
        if matched:
            matched["inCurrentExport"] = True
            matched_ct += 1
            continue
        it = build_fresh_weapon(omod_fid, row, weap_host, weap_combos,
                                weap_base, weap_dnam, omod_by_fid, pool_name, pool_fid)
        fkey = norm_name(it["name"])
        if fkey in reuse_by_name:
            reuse_by_name[fkey]["inCurrentExport"] = True
            matched_ct += 1
            continue
        reuse_by_name[fkey] = it
        fresh_new.append(it)

    weapon_items = reuse_items + fresh_new

    lvli_idx = build_lvli_index(channel)
    armo_tbl = load_armo_tables(channel)
    armour_items = build_armour(channel, lvli_idx, armo_tbl, pool_name, pool_fid, gmrw_quest)
    armour_items += build_cosmetic_armour(channel, omod_by_fid)

    power_items = []

    all_items = weapon_items + armour_items + power_items

    # --- Final dedupe: merge items that share (kind, normalized name). The
    #     reuse + fresh union can produce two records for one item (e.g. a
    #     legacy reuse entry whose custom mod is gone, plus a freshly resolved
    #     one). Keep the more complete record and union their flags/fields. ---
    def _completeness(it):
        score = 0
        if it.get("inCurrentExport"):
            score += 4
        ob = (it.get("howToObtain") or "").strip().lower()
        if ob and ob not in ("source unconfirmed", "unknown", "atom shop", ""):
            score += 2
        score += min(len(it.get("starEffects") or []), 3)
        if it.get("inherentEffect"):
            score += 1
        return score

    _merged, _order = {}, []
    for it in all_items:
        key = (it["kind"], norm_name(it["name"]))
        if key not in _merged:
            _merged[key] = it
            _order.append(key)
            continue
        keep, drop = _merged[key], it
        if _completeness(drop) > _completeness(keep):
            keep, drop = drop, keep
        keep["inCurrentExport"] = bool(keep["inCurrentExport"] or drop["inCurrentExport"])
        if not keep.get("starEffects") and drop.get("starEffects"):
            keep["starEffects"] = drop["starEffects"]
        for f in ("howToObtain", "sourceType", "sourceRef", "planName",
                  "questName", "inherentEffect", "image", "ddsHandle"):
            kv = str(keep.get(f) or "").strip()
            if (not kv or kv.lower() in ("unknown", "source unconfirmed")) and str(drop.get(f) or "").strip():
                keep[f] = drop[f]
        if not keep.get("craftable") and drop.get("craftable"):
            keep["craftable"] = True
        _merged[key] = keep
    all_items = [_merged[k] for k in _order]

    # Never leave a blank How-to-Obtain: fall back honestly.
    for it in all_items:
        if not str(it.get("howToObtain") or "").strip():
            it["howToObtain"] = "Source unconfirmed"
        if not str(it.get("sourceType") or "").strip():
            it["sourceType"] = "Unknown"

    for it in all_items:
        it.pop("_cmod_fids", None)
        it["id"] = unique_slug(it.get("edid") or it.get("name") or "item")

    all_items.sort(key=lambda x: x["name"].lower())

    counts = {
        "weapon": sum(1 for i in all_items if i["kind"] == "Weapon"),
        "armour": sum(1 for i in all_items if i["kind"] == "Armour"),
        "powerArmour": sum(1 for i in all_items if i["kind"] == "Power Armour"),
    }
    output = {
        "version": 1,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "channel": channel,
        "count": len(all_items),
        "counts": counts,
        "items": all_items,
    }

    subdir = "unique_weapons_armour"
    out_path = dist_dir / ("pts" if channel == "pts" else "") / subdir / "unique_weapons_armour.json"
    os.makedirs(out_path.parent, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
        f.write("\n")

    reuse_only = sum(1 for i in weapon_items if not i["inCurrentExport"])
    unconfirmed = [i["name"] for i in all_items if i["howToObtain"] in ("", "Source unconfirmed")]
    print(f"  Wrote {out_path}")
    print(f"  Total: {len(all_items)}  counts={counts}")
    print(f"  Weapons: reuse={len(reuse_items)} fresh-new={len(fresh_new)} "
          f"fresh-matched-reuse={matched_ct} reuse-only(inCurrentExport=false)={reuse_only}")
    print(f"  Source unconfirmed: {len(unconfirmed)} -> {unconfirmed}")

    if channel == "live":
        write_empty_patchlog_feed(str(dist_dir),
                                  "patchlog_latest_bnb_unique_weapons_armour.json",
                                  current_count=len(all_items))
    return output


def main():
    ap = argparse.ArgumentParser(description="Build unique_weapons_armour.json")
    ap.add_argument("--channel", choices=["live", "pts", "both"], default="both")
    args = ap.parse_args()
    os.chdir(REPO_ROOT)
    dist_dir = REPO_ROOT / "dist"
    if args.channel in ("live", "both"):
        build_channel("live", dist_dir)
    if args.channel in ("pts", "both"):
        build_channel("pts", dist_dir)


if __name__ == "__main__":
    main()
