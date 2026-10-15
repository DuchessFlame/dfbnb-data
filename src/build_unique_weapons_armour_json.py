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
      star effects. ATX_* paints/skins surface as cosmetic entries and are
      dropped unless INCLUDE_COSMETICS is on.
    * Union by custom-mod formId then normalised name. Reuse-only items kept
      with inCurrentExport=false.

  ARMOUR
    * LVLI LL_Armor_Unique_All -> 8 unique armours; 3 weight variants collapse
      into one item; fixed stars from the ARMO ObjectTemplate combo carrying
      the mod_Custom_<name>.
    * Cosmetic named armour (Blue Ridge
      Guardsmen Paint) is built only when INCLUDE_COSMETICS is on.

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

# Paints, skins and name plates carry no stats and no star effects, so they
# are not "unique weapons and armour" in the sense this page lists. They are
# excluded (Sep 2026). Flip to True to bring all 19 back.
INCLUDE_COSMETICS = False

# Cut content — items flagged isCut (zzz_ / DEL_ prefixed EDIDs) exist in the
# files but cannot be obtained in game. Excluded (Sep 2026). NOTE: this is not
# the same as inCurrentExport=false, which marks items that WERE obtainable
# (legacy Survival rewards and the like) and are deliberately kept.
INCLUDE_CUT = False

# Uniques confirmed cut in game even though the files still reference them.
# Normalised names (norm_name). Keep this short — a file-based rule wins.
CUT_BY_HAND = {"crimsonsky"}

# In the files but not released in game yet (Duchess, 11 Oct 2026). Kept on the
# page with an "Unreleased" pill rather than hidden, so they are ready the day
# they ship. Normalised names.
# The Drifter's gear: in the files, not in the game yet (Duchess, 15 Oct 2026:
# "these are unreleased"). Unreleased pill, off the progress bar, no picture
# needed. Not cut — the Drifter is coming back.
UNRELEASED_BY_HAND = {n for n in (
    "Chaos Engine", "Splinter", "Tempest",
    "Secret Settler Helmet", "Secret Settler Torso",
    "Secret Settler Left Arm", "Secret Settler Right Arm",
    "Secret Settler Left Leg", "Secret Settler Right Leg",
)}

SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
TSV_DIR = REPO_ROOT / "tsv"

sys.path.insert(0, str(SRC_DIR))
import tsv_source
import uwa_mods
import uwa_effects
import uwa_images      # unique pictures, matched by name (Oct 2026)
import uwa_obtain
import uwa_discover
import uwa_source_links
import uwa_names       # in-game names from INNR (Oct 2026)
import uwa_trace       # sources traced from the game files (Oct 2026)
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
    if hl.startswith("challenge"):
        return "Challenge"
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
        ("dist/daily_ops/daily_ops_reward_checklist.json", "Daily Ops", "Daily Ops"),
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
    combos = defaultdict(lambda: defaultdict(lambda: {"full": "", "mods": [], "kw_tails": set()}))
    host = defaultdict(list)
    if objt_p:
        for r in read_tsv(objt_p):
            wf = norm_formid(r.get("WEAP_FormID", ""))
            ci = strip_q(r.get("CombinationIndex", ""))
            cd = combos[wf][ci]
            cf = strip_q(r.get("Combination_FULL", ""))
            if cf and not cd["full"]:
                cd["full"] = cf
            # OBTS_Keywords (export column added 13 Oct 2026): the
            # if_tmp_<Name> instance filter that a leveled list asks for.
            for kw in strip_q(r.get("OBTS_Keywords", "")).split("|"):
                m = re.match(r"\s*if_tmp_([A-Za-z0-9_-]+)", kw)
                if m:
                    cd["kw_tails"].add(m.group(1))
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


# Emptied 13 Oct 2026 (Duchess): no hand-written source lines.
SUPP_OBTAIN = {}


def obtain_for(edid, name, pool_name, pool_fid, formid=None):
    for frag, val in SUPP_OBTAIN.items():
        if frag.lower() in (edid or "").lower():
            return val, classify_source(val), (val.split(":", 1)[1].strip() if ":" in val else "")
    fb = match_obtain_fallback(edid)
    if fb:
        return fb, classify_source(fb), (fb.split(":", 1)[1].strip() if ":" in fb else "")
    # No lookup by the BASE weapon's FormID: every unique on that base would
    # inherit whatever pool hands out a plain one (The Pipe got Monster Mash).
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

def armour_uniques_from_roster(lvli_idx, armo_tbl):
    """Bethesda's own unique armour list (LL_Armor_Unique_All), read every build
    - no typed table (Duchess, 13 Oct 2026). Each entry is a leveled list that
    resolves to the armour; its mod_Custom_<X> names it ("Silver Lining Custom
    Mod" -> "Silver Lining"; uwa_names then applies the in-game INNR name).
    Returns (name, list edid, custom fragment, sub type, base label)."""
    armour_rows, combos, _slots = armo_tbl
    out = []
    for ref in lvli_idx.get("LL_Armor_Unique_All", []):
        parts = ref.split(":")
        if len(parts) < 3 or parts[2] != "LVLI" or _ARMO_DEAD_RE.search(parts[1]):
            continue
        edid = parts[1]
        armos = resolve_lvli_to_armo(edid, lvli_idx)
        # The custom mod that belongs to THIS list: one armour can carry
        # several sets' mods (the Secret Service chest has Bulwark's and the
        # Secret Settler paint), so prefer the mod named in the list's EditorID.
        frag = name = ""
        cands = []
        for fid, _e in armos:
            for _ci, cd in combos.get(fid, {}).items():
                for m in cd["mods"]:
                    mm = re.match(r'^(\S*mod_Custom_(\w+))\s+"([^"]*)"', m, re.I)
                    if mm and not _ARMO_DEAD_RE.search(mm.group(1)) and "_CustomName_" not in mm.group(1):
                        cands.append((mm.group(2), mm.group(3)))
        key = re.sub(r"[^a-z0-9]", "", edid.lower())
        own = [c for c in cands if re.sub(r"[^a-z0-9]", "", c[0].lower()) in key]
        if own:
            frag, name = own[0]
        elif len({c[0] for c in cands}) == 1:
            frag, name = cands[0]
        name = re.sub(r"\s*Custom (Mod|Name)$", "", name or "").strip()
        if not name:
            # no named mod: the list's own last part ("..._Torso_TrailWarden")
            name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", edid.split("_")[-1])
            frag = frag or edid.split("_")[-1]
        row = armour_rows.get(armos[0][0], {}) if armos else {}
        base = strip_q(row.get("ARMO_FULL", "")) or edid
        out.append((name, edid, frag, armour_family(base), base))
    return out


def armour_family(full):
    """'Raider Chest Piece' -> 'Raider Armor'; 'Combat Armor Chest Piece' ->
    'Combat Armor'; 'Trapper Left Arm' -> 'Trapper Armor'."""
    fam = re.sub(r"\s*\b(Chest Piece|Chest|Torso|Left Arm|Right Arm|Left Leg|Right Leg|Helmet|Arm|Leg)\b\s*$",
                 "", full or "", flags=re.I).strip()
    if not fam:
        return full
    return fam if re.search(r"\b(Armor|Armour|Uniform|Suit)\b", fam, re.I) else fam + " Armor"


_ARMO_DEAD_RE = re.compile(r"(^|_)(zzz|CUT|DEL|DONOTUSE|DEPRECATED)(_|$)|^zzz", re.I)


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
    for name, top_edid, cust_frag, sub, base_label in armour_uniques_from_roster(lvli_idx, armo_tbl):
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


_ARMO_SET_MOD = re.compile(
    r"^(?P<edid>\S*Mod_Custom_(?P<set>\w+?)_CustomName_(?P<slot>\w+))\s+\"(?P<full>[^\"]*)\"\s+\[OMOD:(?P<fid>[0-9A-Fa-f]{8})\]")
_ARMO_DEAD = re.compile(r"(^|_)(zzz|CUT|DEL|DONOTUSE|DEPRECATED)(_|$)|^zzz", re.I)


def discover_armour_sets(lvli_idx, armo_tbl, known_names, omod_by_fid=None):
    """Named armour pieces read from the files (Duchess, 13 Oct 2026: the
    Drifter is coming back, so its Secret Settler set is not cut). A piece is
    an ARMO ObjectTemplate combination that includes a
    Mod_Custom_<Set>_CustomName_<Piece> mod; its own leveled list is the
    <...>_LL_Armor_<Set>_<Piece> list, which uwa_trace walks up to whatever
    hands it out. Anything with a CUT / DEL / zzz tag stays out."""
    armour_rows, combos, _slots = armo_tbl
    items, seen = [], set()
    for armo_fid, cmap in combos.items():
        for _ci, cd in cmap.items():
            for m in cd["mods"]:
                mm = _ARMO_SET_MOD.match(m)
                if not mm or _ARMO_DEAD.search(mm.group("edid")) or mm.group("edid") in seen:
                    continue
                seen.add(mm.group("edid"))
                row = armour_rows.get(armo_fid, {})
                if _ARMO_DEAD.search(strip_q(row.get("ARMO_EDID", ""))):
                    continue
                name = (cd["full"] or "").strip()
                if not name:
                    # "Secret Settler Custom Name Helmet" -> "Secret Settler Helmet"
                    name = re.sub(r"\s*Custom Name\s*", " ", mm.group("full")).strip()
                if not name or norm_name(name) in known_names:
                    continue
                token, slot = mm.group("set"), mm.group("slot")
                own = [e for e in lvli_idx
                       if re.search(rf"(^|_)LL_Armor_{re.escape(token)}_{re.escape(slot)}$", e, re.I)
                       and not _ARMO_DEAD.search(e)]
                base_label = strip_q(row.get("ARMO_FULL", "")) or token
                it = blank_item()
                it["kind"] = "Armour"
                it["name"] = name
                it["subType"] = armour_family(base_label)
                it["base"] = base_label
                it["stats"] = armo_stats(row) if row else {}
                it["starEffects"] = extract_armo_stars(armo_fid, token, combos)
                it["formId"] = armo_fid
                it["edid"] = strip_q(row.get("ARMO_EDID", ""))
                it["tradeable"] = None
                it["inCurrentExport"] = True
                it["_cmod_fids"] = [norm_formid(mm.group("fid"))]
                it["_rosterLists"] = own
                # the set's own effect mod (Mod_Custom_<Set>_SpecialEffect) and its text
                for m2 in cd["mods"]:
                    em = re.match(r"^(\S*Mod_Custom_" + re.escape(token) + r"_\w+)\s.*\[OMOD:([0-9A-Fa-f]{8})\]", m2)
                    if not em or "_CustomName_" in em.group(1):
                        continue
                    orow = (omod_by_fid or {}).get(norm_formid(em.group(2)), {})
                    desc = strip_q(orow.get("DESC", ""))
                    if desc:
                        it["inherentEffect"] = it.get("inherentEffect") or desc
                        it.setdefault("uniqueMods", []).append({
                            "name": name, "edid": em.group(1), "formId": norm_formid(em.group(2)),
                            "kind": "unique", "desc": desc})
                it["howToObtain"] = "Source unconfirmed"
                it["sourceType"] = "Unknown"
                it["sourceRef"] = ""
                items.append(it)
    return items


def build_cosmetic_armour(channel, omod_by_fid):
    specs = [
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


CONTRADICTED = []     # (name, old line, own list) — filled by fill_sources, printed
_VAGUE_SCORE = re.compile(r"^\s*score season reward\s*(\([^)]*\))?\s*$", re.I)

# Uniques whose own leveled list is named differently from the unique
# (normalised unique name -> list EDID). Anchorage Ace's list is
# LL_Weapon_Ranged_Silenced10mmSMG, in the Neurological Warfare reward pool.
OWN_LIST_ALIAS = {"anchorageace": "LL_Weapon_Ranged_Silenced10mmSMG"}
_SCORE_WEAP_ENTM = re.compile(
    r"^SCORE_(?:S(\d+)|MiniSeason_(?:\d{4}_)?([A-Za-z]+?))_(?:ENTM_)?Weapons?_(?:.*_)?([A-Za-z0-9]+)$", re.I)


def _seasonal_drops():
    """{normalised weapon name: (event name, tradeable)} from the seasonal event
    reward pages (dist/seasonal_events/seasonal_events_rewards_by_page.json).
    A unique there carries its own name or its customModName."""
    out = {}
    path = REPO_ROOT / "dist" / "seasonal_events" / "seasonal_events_rewards_by_page.json"
    try:
        pages = json.load(open(path, encoding="utf-8")).get("byPage") or {}
    except (OSError, ValueError):
        return out

    def walk(o, event):
        if isinstance(o, dict):
            if (o.get("sig") or "") == "WEAP":
                for nm in (o.get("customModName"), o.get("name")):
                    if nm:
                        out.setdefault(norm_name(nm), (event, o.get("tradeable")))
            for v in o.values():
                walk(v, event)
        elif isinstance(o, list):
            for v in o:
                walk(v, event)

    for page in pages.values():
        if isinstance(page, dict) and page.get("name"):
            walk(page, page["name"])
    return out


def _former_name(it, weap_combos):
    """A named combination on the unique's own WEAP that starts like its name
    but is not it ("Xerxo's Gamma Ray Gun" for Xerxos) — the older name."""
    if not weap_combos:
        return None
    key = norm_name(it.get("name"))
    for combo in (weap_combos.get(norm_formid(it.get("formId") or "")) or {}).values():
        full = (combo or {}).get("full") or ""
        n = norm_name(full)
        if n and n != key and len(key) >= 5 and n.startswith(key[:5]):
            return full
    return None


def _score_weapon_seasons(channel):
    """{normalised name: "Season 17" | "Love Hurts mini-season"} from the
    SCORE_*_ENTM_Weapons_* entitlements, keyed on their last EditorID token
    (SCORE_S17_ENTM_Weapons_HeadHunter -> "headhunter")."""
    out = {}
    path = pick("ENTM_Export_*.tsv", channel)
    if not path:
        return out
    minis = {}
    try:
        ms = json.load(open(REPO_ROOT / "dist" / "mini_seasons" / "mini_seasons.json", encoding="utf-8"))
        for key, v in ms.items():
            if isinstance(v, dict) and (v.get("title") or v.get("name")):
                minis[key.replace("-", "").lower()] = v.get("title") or v.get("name")
    except (OSError, ValueError):
        pass
    for r in read_tsv(path):
        m = _SCORE_WEAP_ENTM.match(strip_q(r.get("EDID", "")))
        if not m or is_cut(strip_q(r.get("EDID", ""))):
            continue
        season, mini, tail = m.groups()
        where = (f"Season {int(season)}" if season
                 else f"{minis.get((mini or '').lower(), camelcase_to_display(mini or ''))} mini-season")
        out.setdefault(norm_name(tail), where)
        out.setdefault(norm_name(strip_q(r.get("FULL", ""))), where)
    return out


_DEAD_LIST = re.compile(r"(^|_)(zzz|CUT|DONOTUSE|DEL)(_|$)|^zzz", re.I)
_SKIP_PARENT = re.compile(r"^LL_Weapon_Unique_All$", re.I)


def _item_alias_quests(channel):
    """WEAP/ARMO FormID -> quest name, for every quest alias that creates the
    item (ALCO). {} when the export has no _ItemAliases sheet yet."""
    path = pick("QUST2_Export_*_ItemAliases.tsv", channel)
    out = {}
    if not path:
        return out
    for r in read_tsv(path):
        name = strip_q(r.get("QuestName", ""))
        if not name or "<" in name or "[" in name:
            continue
        name = re.sub(r"^\s*(Event|Daily|Activity):\s*", "", name)
        for m in re.finditer(r"ALCO=([0-9A-Fa-f]{8}):[^:;]*:(WEAP|ARMO)\b",
                             strip_q(r.get("Links", ""))):
            out.setdefault(norm_formid(m.group(1)), name)
    return out


_RAID_GMRW = re.compile(r"^RD\d+_QuestReward_", re.I)


def _event_lists(channel):
    """Walk from a unique's own leveled list (LL_Weapon_Ranged_RedTerror,
    SCORE_S7_LL_GammaGun_Xerxos ...) up to the event or quest that hands it
    out. Returns (own_lists, find) where own_lists maps the normalised list
    tail -> list EDID and find(edid) -> [event names].

    The quest is read from the GMRW's own QUST reference, NOT its
    ParentQuestDisplay: Dangerous Pastimes' reward row says "Neurological
    Warfare" there (the Skyline Valley region boss it is filed under)."""
    lv = pick("LVLI_Export_*_LVLI_Entries.tsv", channel)
    gm = pick("GMRW_Export_*.tsv", channel)
    qu = pick("QUST2_Export_*_Quests.tsv", channel)
    if not (lv and gm and qu):
        return {}, (lambda e: [])
    qname = {}
    for r in read_tsv(qu):
        full = strip_q(r.get("FULL", ""))
        if full and "<" not in full and "[" not in full:
            qname[norm_formid(r.get("FormID", ""))] = re.sub(r"^\s*Event:\s*", "", full)
    reward_of = defaultdict(set)              # list EDID -> quest names
    for r in read_tsv(gm):
        if _DEAD_LIST.search(strip_q(r.get("EDID", ""))):
            continue
        item = strip_q(r.get("RewardedItem", ""))
        if not item.endswith(":LVLI"):
            continue
        q = None
        for k, v in r.items():
            v = strip_q(v or "")
            if (k or "").startswith("Ref") and v.endswith(":QUST"):
                q = qname.get(norm_formid(v.split(":")[0]))
                if q:
                    break
        if not q:
            link = strip_q(r.get("ParentQuestLink", ""))
            q = qname.get(norm_formid(link.split(":")[0])) if link else None
        if not q and _RAID_GMRW.match(strip_q(r.get("EDID", ""))):
            # A raid stage reward has no QUST behind it; its ParentQuestDisplay
            # IS the stage ("Enclave Research Lab Module"). Same label the plan
            # pages print (build_plan_obtain_json.raid_stage_label).
            stage = strip_q(r.get("ParentQuestDisplay", ""))
            if stage and "<" not in stage and "[" not in stage:
                q = "Raids - " + stage
        if q:
            reward_of[item.split(":")[1]].add(q)
    parents = defaultdict(set)                # child EDID -> parent list EDIDs
    own = {}
    for r in read_tsv(lv):
        vals = list(r.values())
        par = strip_q(vals[1] if len(vals) > 1 else "")
        child = next((strip_q(v) for v in vals if strip_q(v or "").endswith(":LVLI")), "")
        if not par or not child:
            continue
        ced = child.split(":")[1]
        parents[ced].add(par)
        for ed in (ced, par):
            tail = ed.rsplit("_", 1)[-1]
            if not _DEAD_LIST.search(ed):
                own.setdefault(norm_name(tail), ed)
                # "TheVATSUnknown" has no space for norm_name to drop "the "
                # from, so key it the way the unique's name normalises too.
                own.setdefault(norm_name(re.sub(r"^The(?=[A-Z])", "", tail)), ed)

    def tops(edid):
        """[(event name, reward list EDID)] for every live way up from edid."""
        seen, out, frontier = {edid}, [], [edid]
        for _ in range(5):
            nxt = []
            for ed in frontier:
                for q in sorted(reward_of.get(ed, ())):
                    if (q, ed) not in out:
                        out.append((q, ed))
                for p in sorted(parents.get(ed, ())):   # sets: fixed order between builds
                    if p in seen or _DEAD_LIST.search(p) or _SKIP_PARENT.match(p):
                        continue
                    seen.add(p)
                    nxt.append(p)
            frontier = nxt
        return out
    def find(edid):
        seen, out, frontier = {edid}, [], [edid]
        for _ in range(5):
            nxt = []
            for ed in frontier:
                for q in sorted(reward_of.get(ed, ())):
                    if q not in out:
                        out.append(q)
                for p in sorted(parents.get(ed, ())):   # sets: fixed order between builds
                    if p in seen or _DEAD_LIST.search(p) or _SKIP_PARENT.match(p):
                        continue
                    seen.add(p)
                    nxt.append(p)
            frontier = nxt
        return out
    find.tops = tops
    return own, find


def fill_sources(items, channel, weap_combos=None):
    """Give a source to uniques the reuse foundation left unconfirmed or vague.

    * a seasonal event reward page lists the weapon -> "Event: <event>"
      (beats a bare "Score Season reward": Relic Reaper drops from the
      Slasher's Disturbed Graves, it is not a scoreboard item)
    * a bare "Score Season reward" -> the season named by its own
      SCORE_S<n>_ENTM_Weapons_* entitlement ("Scoreboard: Season 17")
    Read from the game files and the event pages every build, never a list.
    """
    events = _seasonal_drops()
    seasons = _score_weapon_seasons(channel)
    own_lists, find_events = _event_lists(channel)
    links = uwa_source_links.SourceLinks(lambda p: pick(p, channel), read_tsv,
                                         strip_q, norm_formid)
    for it in items:
        key = norm_name(it.get("name"))
        own = it.get("_rosterList") or own_lists.get(key) or OWN_LIST_ALIAS.get(key)
        how = str(it.get("howToObtain") or "").strip()
        # A hand line whose quest exists in the files but never hands out the
        # unique's own list is wrong, whatever else it looks like (12 Oct
        # 2026: both Abraxo guns, Elders Mark, Slug Buster, Peace Maker …).
        # It is dropped here; the own-list routes (uwa_obtain) name the real
        # source, and the build log lists every one.
        bad = links.check(how, own)["contradicted"] if own else []
        if bad:
            CONTRADICTED.append((it.get("name"), how, own))
            it["howToObtain"], it["sourceType"] = "Source unconfirmed", "Unknown"
            it["sourceRef"] = it["questName"] = ""
            how = "Source unconfirmed"
        if it.get("kind") != "Weapon":
            continue
        # Only sources that are missing, a bare "Score Season reward", or a
        # vague "World drop" are filled. Many uniques sit in shared unique
        # pools that several events roll, so walking up the lists for an item
        # that already has a checked source (Holy Fire, Gunther's Revolver,
        # the Treasure Hunter rewards) names the wrong event — tried and
        # reverted, 11 Oct 2026.
        weak = (not how or "unconfirmed" in how.lower() or _VAGUE_SCORE.match(how)
                or how.lower().startswith("world drop")
                # the old hand line from build_unique_weapons_json.py, which
                # never named the stage (and was wrong for The Kabloom)
                or "raids (enclave expeditions)" in how.lower())
        if not weak:
            continue
        # 1. the unique's own leveled list, walked up to the event that gives it.
        #    Bethesda's roster pairing (WeaponsUniqueNamedList) comes first.
        evs = find_events(own) if own else []
        if evs:
            it["_walkSource"] = True
            it["howToObtain"] = "Event: " + " / ".join(evs)
            it["sourceType"], it["sourceRef"] = "Event", evs[0]
            note = []
            m = re.match(r"^SCORE_S(\d+)_", own or "", re.I)
            if m:
                note.append(f"First released as a Season {int(m.group(1))} Scoreboard reward")
            old = _former_name(it, weap_combos)
            if old:
                note.append(f"it was called {old} back then" if note else f"It used to be called {old}")
            if note:
                it["obtainNote"] = ", ".join(note) + "."
            continue
        if key in events:
            ev, trade = events[key]
            it["howToObtain"], it["sourceType"], it["sourceRef"] = f"Event: {ev}", "Event", ev
            if it.get("tradeable") is None and trade is not None:
                it["tradeable"] = bool(trade)
            continue
        if not how or "unconfirmed" in how.lower() or _VAGUE_SCORE.match(how):
            where = seasons.get(key) or next(
                (w for k, w in seasons.items() if k.startswith(key) and len(key) >= 6), None)
            if where:
                it["howToObtain"] = f"Score: {where} Scoreboard"
                it["sourceType"], it["sourceRef"] = "Score", where


def report_source_changes(out_path, output):
    """Print every unique whose source, tradeable flag or presence changed
    against the file about to be replaced.

    A safety net, added 11 Oct 2026 after a source rule that looked right on
    five items quietly renamed the event on 35 others. A short list here is
    a change you meant; a long one is a rule that reached too far — read it
    before pushing."""
    try:
        old = {x["name"]: x for x in json.load(open(out_path, encoding="utf-8")).get("items") or []}
    except (OSError, ValueError):
        return
    new = {x["name"]: x for x in output.get("items") or []}

    def src(x):
        return [(r.get("route"), [l for l in r.get("lines") or []
                                  if l.startswith(("Source", "Note"))])
                for r in x.get("obtainRoutes") or [] if r.get("populated")]
    lines = []
    for name in sorted(set(old) | set(new)):
        a, b = old.get(name), new.get(name)
        if not a or not b:
            lines.append(f"    {'added' if b else 'REMOVED'}: {name}")
        elif src(a) != src(b) or a.get("tradeable") != b.get("tradeable"):
            lines.append(f"    changed: {name}: {src(a)} -> {src(b)}")
    print(f"  Source changes vs last build: {len(lines)}")
    for l in lines[:60]:
        print(l)
    if len(lines) > 15:
        print("  WARNING: more than 15 uniques changed source — check a rule did "
              "not reach further than meant before pushing.")


def craft_mod_routes(items, channel):
    """A unique that is a MOD you craft onto another unique gets that item's routes.

    Cosmic Knife Super-Heated is mod_custom_CosmicKnife_Superheated, made at a
    weapon workbench by co_Weapon_Melee_CombatKnife_CosmicKnife_Superheated.
    Its COBJ2 conditions say who can craft it: HasEntitlement(SCORE_S22_ENTM_
    Weapons_CosmicKnife "Cosmic Knife") or HasLearnedRecipe(the Cosmic Knife
    plan's recipe). So the way to get it is the way to get the Cosmic Knife.
    Only items with no route of their own are touched."""
    cobj2 = pick("COBJ2_Export_*.tsv", channel)
    if not cobj2:
        return
    by_cnam = defaultdict(list)
    for r in read_tsv(cobj2):
        by_cnam[norm_formid(r.get("CNAM_FormID", "")).upper()].append(r.get("Conditions") or "")
    by_name = {norm_name(i.get("name")): i for i in items}
    done = []
    for it in items:
        if any(r.get("populated") for r in it.get("obtainRoutes") or []):
            continue
        parents = []
        for m in it.get("uniqueMods") or []:
            if m.get("kind") != "unique":
                continue
            for cond in by_cnam.get((m.get("formId") or "").upper(), []):
                for full in re.findall(r'HasEntitlement\|[^|]*\|[^|]*\|\S+\s+"+([^"]+)"+\s*\[ENTM:', cond):
                    par = by_name.get(norm_name(full))
                    if par and par is not it and par not in parents:
                        parents.append(par)
        for par in parents:
            for src_r in par.get("obtainRoutes") or []:
                if not src_r.get("populated"):
                    continue
                dst = next(x for x in it["obtainRoutes"] if x["route"] == src_r["route"])
                lines = [f"Drops: Mod — craft it onto the {par['name']} at a weapon workbench"]
                lines += [l for l in src_r["lines"] if not l.startswith("Drops:")]
                dst.update({"populated": True, "lines": lines,
                            "tradeable": src_r.get("tradeable"), "dropRate": src_r.get("dropRate")})
                if src_r.get("paths"):
                    dst["paths"] = list(src_r["paths"])
            it["howToObtain"] = f"Craft: mod for the {par['name']} (" + par.get("howToObtain", "") + ")"
            done.append(it["name"])
    if done:
        print(f"  crafted-mod uniques given their item's routes: {done}")


def attach_conditions(items, channel):
    """Drop Conditions for every unique's live How to Obtain routes.

    Same reader the plan pages use (plan_conditions.ConditionIndex): level
    gates, "only after you learn X", quest-completed gates, region and event
    switches. Read on the exact leveled list each route came from
    (uwa_obtain records it as route["paths"] = [[list FormID, record], ...]),
    so a unique in a shared pool gets the conditions of the way in the route
    names, not another event's. Duchess, 11 Oct 2026: conditions are checked
    on every item.

    Routes with no list to read (a quest handing the item over directly, or a
    hand-written fallback line) are filled from the unique's own list walk
    when that walk reaches the event the route names; otherwise they are
    marked conditionsChecked False and the page says "Not checked" rather than
    a "None" nobody verified.
    """
    import plan_conditions
    tsv_dir = os.path.dirname(pick("LVLI_Export_*_LVLI_Entries.tsv", channel) or "")
    idx = plan_conditions.ConditionIndex(tsv_dir, lambda pat, d=None: pick(pat, channel))
    ed2fid = {}
    for r in read_tsv(pick("LVLI_Export_*_LVLI_Entries.tsv", channel)):
        ed2fid.setdefault(strip_q(r.get("LVLI_EDID", "")),
                          norm_formid(r.get("LVLI_FormID", "")).upper())
    own_lists, find_events = _event_lists(channel)
    stats = {"routes": 0, "with": 0, "unchecked": 0}
    implied = {f"Only from {plan_conditions._humanize(k)}" for k in uwa_trace.IMPLIED_KW}
    for it in items:
        key = norm_name(it.get("name"))
        own = it.get("_rosterList") or own_lists.get(key) or OWN_LIST_ALIAS.get(key)
        ways = find_events.tops(own) if own else []
        own_fid = ed2fid.get(own or "", "")
        for r in it.get("obtainRoutes") or []:
            paths = r.pop("paths", None) or []
            if not r.get("populated"):
                continue
            stats["routes"] += 1
            text = " ".join(r.get("lines") or []).lower()
            if "no longer obtainable" in text:
                r["conditions"], r["conditionsChecked"] = [], True
                continue
            lines, skipped, checked = [], 0, False
            if r.get("route") in ("Scoreboard", "Atom Shop") and not paths:
                # Claimed straight off the board / bought from the shop: no
                # leveled list, so no drop gate exists to read. That is a
                # checked "None", not a "Not checked".
                checked = True
            for rl in r.get("lines") or []:
                # Gold Bullion vendors: the reputation the vendor needs is
                # the gate (gold_vendor.py reads it from the files).
                m = re.match(r"^Reputation:\s*(.+?)\s+\u2014\s+(.+)$", rl)
                if m:
                    lines.append(f"Only at {m.group(2)} reputation with the {m.group(1)} or higher")
                    checked = True
                if re.search(r"rotating stock", rl, re.I):
                    lines.append("Only in the weeks the vendor has it in stock")
                    checked = True
            for list_fid, target in paths:
                if not list_fid:
                    checked = True          # handed over directly: no list gate
                    continue
                ln, sk = idx.for_lists([str(list_fid).upper()], str(target).upper(),
                                       {"title": it.get("name") or ""})
                lines += [l for l in ln if l not in lines]
                skipped += sk
                checked = True
            if not checked and own_fid and ways:
                match = [ed for ev, ed in ways if ev.lower() in text]
                title = {"title": it.get("name") or ""}
                if match:
                    ln, sk = idx.for_lists([ed2fid[e] for e in match if e in ed2fid],
                                           own_fid, title)
                    lines, skipped, checked = ln, sk, True
                else:
                    # The route is a hand-written line the walk cannot tie to
                    # one event. If EVERY live way into the unique's own list
                    # carries exactly the same conditions, those conditions
                    # hold whichever way the route means — so they can be
                    # stated. If the ways disagree, nothing can be said.
                    per_way = []
                    for _ev, ed in ways:
                        if ed in ed2fid:
                            ln, sk = idx.for_lists([ed2fid[ed]], own_fid, title)
                            per_way.append((tuple(ln), sk))
                    if per_way and len({w[0] for w in per_way}) == 1:
                        lines, skipped, checked = list(per_way[0][0]), per_way[0][1], True
            if any(l.startswith("Source: Enemy drop:") for l in r.get("lines") or []):
                # The enemy list was already cut to the enemies these keyword
                # gates allow (uwa_trace), so the gate itself says nothing more —
                # and its text is editor shorthand ("SDOW Actor Type Slasher Fan").
                lines = [l for l in lines if l not in implied]
            if skipped and not lines:
                lines = [plan_conditions.UNREADABLE_LINE]
            r["conditions"] = plan_conditions.tidy(lines)
            r["conditionsChecked"] = checked
            stats["with"] += bool(lines)
            stats["unchecked"] += not checked
    print(f"  Drop conditions: {stats['routes']} live route(s), {stats['with']} with "
          f"conditions, {stats['unchecked']} with nothing in the files to check")


def discover_and_add(all_items, channel, omod_by_fid, cn_weapon, weap_base, weap_dnam,
                     weap_combos, weap_host, pool_name, pool_fid):
    """Run uwa_discover, add uniques the page lacks, report the rest.
    Returns the set of keys Bethesda's own rosters confirm."""
    import add_weapon_groups as _awg
    words = set(_awg.ALIASES) | {v.replace(" ", "") for v in _awg.ALIASES.values()}
    acc, possible = uwa_discover.discover(
        lambda p, exclude=None: pick(p, channel, exclude=exclude),
        read_tsv, strip_q, norm_formid, omod_by_fid, cn_weapon,
        weap_base, weap_combos, weap_host, words)
    official = {k for k, e in acc.items() if "official" in e["src"]}

    def mods_of(it):
        return {m.get("formId") for m in it.get("uniqueMods") or []} | set(it.get("_cmod_fids") or [])

    # Paint / cut records are dropped from the page later, so they must not
    # count as "already there" (Nuka-Launcher matched its own paint record).
    real = [i for i in all_items if not i.get("cosmeticOnly") and not i.get("isCut")]
    page_keys = {uwa_discover.key(i["name"]): i for i in real}
    # A renamed item is still the roster's entry under its EditorID name
    # (Bethesda's list calls Meadow Breeze Sprayer "AbraxoGun").
    for i in real:
        for alt in i.get("_altNames") or []:
            page_keys.setdefault(uwa_discover.key(alt), i)
    page_mods = set().union(*[mods_of(i) for i in real]) if real else set()
    unconfirmed = []
    for it in all_items:
        if it.get("cosmeticOnly") or it.get("isCut"):
            continue                      # dropped later anyway
        k = uwa_discover.key(it["name"])
        hit = uwa_discover.match(k, acc)
        for alt in ([] if hit else it.get("_altNames") or []):
            hit = uwa_discover.match(uwa_discover.key(alt), acc)
            if hit:
                break
        if hit and acc[hit]["lists"] and not it.get("_rosterList"):
            it["_rosterList"] = sorted(acc[hit]["lists"])[0]
            it["_rosterLists"] = sorted(acc[hit]["lists"])
        if not hit and not any(mods_of(it) & e["omods"] for e in acc.values()):
            unconfirmed.append(it["name"])
            it["confirmedByGameFiles"] = False

    # leaf WEAP / ARMO of a roster leveled list (for items with no custom mod)
    leaf = {}
    lv = pick("LVLI_Export_*_LVLI_Entries.tsv", channel)
    for r in (read_tsv(lv) if lv else []):
        ref = strip_q(r.get("LVLO_Reference", "")).split(":")
        if len(ref) >= 3 and ref[2] in ("WEAP", "ARMO"):
            leaf.setdefault(strip_q(r.get("LVLI_EDID", "")), (norm_formid(ref[0]), ref[1], ref[2]))

    # A page item already tied to a roster list (OWN_LIST_ALIAS: Anchorage Ace
    # is LL_Weapon_Ranged_Silenced10mmSMG) is that list's unique.
    aliased_lists = set(OWN_LIST_ALIAS.values())
    added, unbuildable = [], []
    for k, e in sorted(acc.items()):
        # Linked by name, or by a shared custom mod. A shared mod is NOT
        # enough for an item on Bethesda's own lists: the old hand-built list
        # pinned the Nuka-Launcher's mod onto Peace Maker, which hid the
        # Nuka-Launcher entirely.
        if uwa_discover.match(k, page_keys) or (e["lists"] & aliased_lists):
            continue
        if (e["omods"] & page_mods) and "official" not in e["src"]:
            continue
        it = None
        hosted = [f for f in sorted(e["omods"]) if weap_host.get(f)]
        if hosted:
            it = build_fresh_weapon(hosted[0], omod_by_fid.get(hosted[0], {}), weap_host,
                                    weap_combos, weap_base, weap_dnam, omod_by_fid,
                                    pool_name, pool_fid)
            it["name"] = e["name"] or it["name"]
            # build_fresh_weapon looks sources up by the BASE weapon's formid,
            # which hands Nuka-Launcher Peace Maker's quest. Start it blank;
            # fill_sources reads its own roster list / entitlement instead.
            it.update({"howToObtain": "", "sourceType": "Unknown", "sourceRef": ""})
        else:
            wf, wed, sig = "", "", ""
            if e["weap"]:
                wf = e["weap"]; wed = strip_q(weap_base.get(wf, {}).get("WEAP_EDID", "")); sig = "WEAP"
            else:
                for lst in sorted(e["lists"]):
                    if lst in leaf:
                        wf, wed, sig = leaf[lst]
                        break
            if sig != "WEAP" or not wf:
                unbuildable.append(e["name"])
                continue
            it = blank_item()
            base_row, dnam_row = weap_base.get(wf, {}), weap_dnam.get(wf, {})
            lst = sorted(e["lists"])[0] if e["lists"] else wed
            it.update({"kind": "Weapon", "name": e["name"], "formId": wf, "edid": lst,
                       "base": resolve_base_weapon_name(wed, strip_q(base_row.get("WEAP_FULL", ""))),
                       "subType": classify_weapon_type(dnam_row, strip_q(base_row.get("Keywords", ""))),
                       "stats": weap_stats_from_dnam(base_row, dnam_row),
                       "isCut": is_cut(lst) or is_cut(wed)})
            # Left blank on purpose: fill_sources walks Bethesda's own roster
            # list up to its event (the Cursed weapons are A Colossal Problem /
            # Daily Ops) — a name lookup guessed other events for them.
            it.update({"howToObtain": "", "sourceType": "Unknown", "sourceRef": ""})
        it["discoveredBy"] = sorted(e["src"])
        if e["lists"]:
            it["_rosterList"] = sorted(e["lists"])[0]
            it["_rosterLists"] = sorted(e["lists"])
        if not it.get("id"):
            it["id"] = unique_slug(it["name"])
        all_items.append(it)
        added.append(it["name"])
        page_keys[k] = it

    print(f"  Discovery (game files): {len(acc)} uniques confirmed, "
          f"{len(official)} in Bethesda's own lists")
    print(f"    added (on the game's lists, missing from the page): {len(added)} {added}")
    if unconfirmed:
        print(f"    on the page but NOT confirmed by any game-file source — check: {unconfirmed}")
    if unbuildable:
        print(f"    in Bethesda's lists but no weapon record to build from: {unbuildable}")
    if possible:
        print(f"    possible (one weak signal only, not published): {len(possible)}")
    return official


_PLACEHOLDER_FULL = re.compile(r"(custom\s*mod|\bmod|paint|skin)\s*$", re.I)


def rename_from_own_mod(items, omod_by_fid):
    """Name an item after its own custom-name mod (Duchess, 12 Oct 2026).

    The old hand-built list named some uniques from EditorIDs (Abraxo Gun,
    Abraxolator). The game shows the FULL of the mod on the ap_customName
    slot instead (Meadow Breeze Sprayer, Prototype ABX03). Placeholder FULLs
    ("All Rise Custom Mod"), effect mods and paints never rename; spelling-only
    differences (The VATS Unknown) are left alone.
    """
    for it in items:
        if it.get("cosmeticOnly"):
            continue
        for fid in it.get("_cmod_fids") or []:
            row = omod_by_fid.get(fid) or {}
            ed = strip_q(row.get("OMOD_EDID", ""))
            full = strip_q(row.get("FULL", ""))
            # Only the item's own naming mod: mod_custom_[<Weapon>_]<Name>,
            # nothing after the name (TheKabloom_Effect is "Poison").
            m = re.match(r"mod_custom_(?:[A-Za-z0-9]+_)?([A-Za-z0-9]+)$", ed, re.I)
            if not m:
                continue
            if strip_q(row.get("AttachPoint_EDID", "")) != "ap_customName":
                continue
            if not full or _PLACEHOLDER_FULL.search(full):
                continue
            def _c(x):
                return re.sub(r"[^a-z0-9]", "", x.lower())
            # Rename only names rebuilt from that EditorID (AbraxoGun ->
            # "Abraxo Gun"); a hand-picked name is left alone.
            if _c(full) != _c(it["name"]) and _c(m.group(1)) == _c(it["name"]):
                print(f"  renamed from its custom-name mod: {it['name']} -> {full}")
                it["_altNames"] = [it["name"]]
                it["name"] = full
            break


def build_channel(channel, dist_dir):
    global _SLUG_SEEN
    _SLUG_SEEN = {}
    print(f"\n{'='*64}\n  Unique Weapons & Armour -- channel: {channel}\n{'='*64}")

    pool_name, pool_fid = build_pool_index()
    gmrw_quest = load_gmrw_quest(channel)
    omod_by_fid, omod_by_edid, omod_props, cn_weapon = load_current_omods(channel)
    weap_base, weap_dnam, weap_combos, weap_host = load_weap_tables(channel)
    omod_fids_present = set(omod_by_fid.keys())

    # Second discovery path (Oct 2026). Some uniques carry no CustomItemName_
    # keyword at all - "The Pipe" (Pipe Gun, random encounter) is a perk-only
    # custom mod - so the keyword pass above never saw them. The game still
    # names them: the mod is a mod_Custom_* that sits in a WEAP ObjectTemplate
    # combination carrying the SAME name ("The Pipe" combo on PipeGun). Both
    # tests together are narrow - on the Oct 2026 export they add only
    # The Pipe and Camden Whacker (already listed, so it just merges).
    for _fid, _row in omod_by_fid.items():
        if _fid in cn_weapon:
            continue
        _ed = strip_q(_row.get("OMOD_EDID", ""))
        _full = strip_q(_row.get("FULL", ""))
        if not _full or not re.search(r"(^|_)mod_Custom_", _ed, re.I):
            continue
        if strip_q(_row.get("FormType", "")) != "Weapon":
            continue
        for _wf, _ci in weap_host.get(_fid, []):
            if uwa_discover.combo_is_mods(weap_combos[_wf][_ci], _ed, _full,
                                          lambda a, b: norm_name(a) == norm_name(b)):
                cn_weapon.add(_fid)
                break

    # Combination labels that disagree with their own keyword - printed so a
    # shuffled label is seen, never trusted (Plasma Gun, 13 Oct 2026).
    _shuffled = []
    for _wf, _cs in weap_combos.items():
        for _ci, _cd in _cs.items():
            if not _cd.get("kw_tails"):
                continue
            for _me, _mn, _mf in _cd["mods"]:
                _row = omod_by_fid.get(_mf) or {}
                _mfull = strip_q(_row.get("FULL", ""))
                if _mfull and re.match(r"mod_custom_", _me or "", re.I) \
                        and uwa_discover.combo_is_mods(_cd, _me, _mfull) \
                        and _cd["full"] and norm_name(_cd["full"]) != norm_name(_mfull):
                    _shuffled.append(f"{strip_q(weap_base.get(_wf, {}).get('WEAP_EDID', _wf))} "
                                     f"#{_ci}: labelled '{_cd['full']}', builds '{_mfull}'")
    print(f"  combination labels that disagree with their keyword: {len(_shuffled)}")
    for _l in _shuffled:
        print("    " + _l)

    reuse_path = REPO_ROOT / "dist" / "unique_weapons" / "unique_weapons.json"
    reuse = json.load(open(reuse_path, encoding="utf-8"))
    reuse_items = [convert_reuse_weapon(w) for w in reuse["weapons"]]
    rename_from_own_mod(reuse_items, omod_by_fid)

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
    armour_items += discover_armour_sets(
        lvli_idx, armo_tbl, {norm_name(i["name"]) for i in armour_items}, omod_by_fid)
    if INCLUDE_COSMETICS:
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

    # ---- Generative discovery (uwa_discover.py, Oct 2026) -----------------
    # What counts as a unique is read from the game files every build:
    # Bethesda's own rosters, the CustomItemName_ keywords, custom mods that
    # sit on a real weapon, and named build-table combinations. Uniques found
    # there and not yet on the page are ADDED; page items no game-file signal
    # confirms are printed for a human (kept, flagged). See uwa_discover.
    disc_official = discover_and_add(all_items, channel, omod_by_fid, cn_weapon,
                                     weap_base, weap_dnam, weap_combos, weap_host,
                                     pool_name, pool_fid)

    # Drop paints / skins / name plates. Done after the dedupe so a cosmetic
    # record that merged into a real item can't take the real item with it.
    if not INCLUDE_COSMETICS:
        all_items = [i for i in all_items if not i.get("cosmeticOnly")]

    # Drop cut content — unobtainable in game, so it has no place on a list
    # of things to go and get.
    # Cut by hand where the files still look live. Crimson Sky's list still
    # sits in the Dangerous Pastimes unique pool, but it has no unique mod of
    # its own and does not drop in game (Duchess, 11 Oct 2026).
    for i in all_items:
        if norm_name(i.get("name")) in CUT_BY_HAND:
            i["isCut"] = True
        if norm_name(i.get("name")) in {norm_name(n) for n in UNRELEASED_BY_HAND}:
            i["unreleased"] = True
    if not INCLUDE_CUT:
        all_items = [i for i in all_items if not i.get("isCut")]

    # Quest aliases that CREATE the item (ALCO) - QUST2_*_ItemAliases.tsv, the
    # every-quest pass added to ExportQUST2ToTSV.pas on 12 Oct 2026. Read
    # from the files first; the SUPP_OBTAIN lines below are only the fallback
    # for an export made before that pass existed.
    made_by = _item_alias_quests(channel)
    for it in all_items:
        if str(it.get("howToObtain") or "").strip() in ("", "Source unconfirmed"):
            q = made_by.get(norm_formid(it.get("formId") or ""))
            if q:
                it["howToObtain"] = "Quest: " + q
                it["sourceType"] = classify_source(it["howToObtain"])
                it["questName"] = q

    # Items discover_and_add() brought in skip obtain_for(), so a SUPP_OBTAIN
    # line keyed on their own EditorID (Grant's Saber) is applied here, only
    # where nothing better was found.
    for it in all_items:
        if str(it.get("howToObtain") or "").strip() in ("", "Source unconfirmed"):
            for frag, val in SUPP_OBTAIN.items():
                if frag.lower() in str(it.get("edid") or "").lower():
                    it["howToObtain"] = val
                    it["sourceType"] = classify_source(val)
                    it["questName"] = val.split(":", 1)[1].strip() if ":" in val else ""
                    break

    # Never leave a blank How-to-Obtain: fall back honestly.
    for it in all_items:
        if not str(it.get("howToObtain") or "").strip():
            it["howToObtain"] = "Source unconfirmed"
        if not str(it.get("sourceType") or "").strip():
            it["sourceType"] = "Unknown"

    # Resolve each item's fixed star effects and its unique / pre-installed
    # mods from its OWN WEAP/ARMO record where it has one. The reuse foundation
    # stores the BASE weapon's formId (Commander's Charge -> RevolutionarySword),
    # so without this pass the fixed stars read "Random" and the mod that makes
    # the item unique never surfaces at all. See uwa_mods.py.
    mod_idx = uwa_mods.build_indexes_for_channel(pick, read_tsv, channel)
    mod_stats = uwa_mods.enrich(all_items, mod_idx)
    # What each unique mod actually does, read from its properties (perk,
    # enchantment, actor value, damage type…) when it has no DESC; items whose
    # custom mod only renames them are flagged "named only". See uwa_effects.py.
    eff_stats = uwa_effects.apply(all_items, pick, channel)
    print(f"  unique mod effects: {eff_stats}")
    _tr = uwa_effects.apply_tradeable(all_items, pick, channel)
    print(f"  tradeable set from the game files: {len(_tr)} changed")
    for _n, _o, _v in _tr:
        print(f"    {_n}: {_o} -> {_v}")

    # The nine How to Obtain routes. One free-text howToObtain line could not
    # say the thing a reader needs — whether a source hands over the made
    # weapon, its plan, or both — and the plan it named was inherited from the
    # BASE weapon, so every repair-only legacy item claimed to be craftable.
    # See uwa_obtain.py. howToObtain is kept as the one-line summary.
    CONTRADICTED.clear()
    fill_sources(all_items, channel, weap_combos)
    print(f"  hand sources the game files contradict (dropped): {len(CONTRADICTED)}")
    for name, how, own in CONTRADICTED:
        print(f"    {name}: {how!r} never reaches {own}")
    obtain_idx = uwa_obtain.index(channel)
    obtain_idx.apply(all_items)
    # A source fill_sources found by walking the item's own list must also
    # show in the routes, or the summary and the box
    # disagree (Grand Finale: "Seismic Activity" above, routes only showed
    # its bounty-target copy).
    for it in all_items:
        if not it.pop("_walkSource", None):
            continue
        how = str(it.get("howToObtain") or "")
        if not re.match(r"^\s*Event\s*:", how):
            continue
        shown = " ".join(l for r in it.get("obtainRoutes") or [] if r.get("populated")
                         for l in r.get("lines") or []).lower()
        for ev in [e.strip() for e in how.split(":", 1)[1].split(" / ") if e.strip()]:
            ev = re.sub(r"^\s*Event\s*:\s*", "", ev)
            if ev.lower() in shown:
                continue
            r = next(x for x in it["obtainRoutes"] if x["route"] == "Events & Activities")
            if r.get("populated"):
                at = max((i for i, l in enumerate(r["lines"]) if l.startswith("Source:")),
                         default=len(r["lines"]) - 1)
                r["lines"].insert(at + 1, f"Source: {ev}")
            else:
                r.update({"populated": True, "dropRate": "N/A", "tradeable": it.get("tradeable"),
                          "lines": [uwa_obtain._drops_line({"weapon"}, it.get("kind")),
                                    f"Source: {ev}"]})
            shown += " " + ev.lower()
    # History note under the route that is live today (Xerxos: first a
    # Season 7 Scoreboard reward, under another name).
    for it in all_items:
        note = it.get("obtainNote")
        if not note:
            continue
        for r in it.get("obtainRoutes") or []:
            if r.get("populated"):
                r["lines"].append("Note: " + note)
                break
    # The roster list's OWN name ties a unique to a second event the routes
    # do not show (Cursed Broadsider: E06_Colossus_... = A Colossal Problem,
    # while the routes only found the Treasure Hunter pool). Duchess confirmed
    # in game that the Cursed weapons drop from BOTH, so the second event is a
    # real second "Source:" line in the same route, not a side note — the How
    # to Obtain box then lists two entries.
    for it in all_items:
        shown = " ".join(l for r in it.get("obtainRoutes") or [] if r.get("populated")
                         for l in r.get("lines") or []).lower()
        for lst in it.get("_rosterLists") or []:
            origin = obtain_idx._quest_name(lst)
            named = bool(origin)
            if not origin and re.search(r"(^|_)DailyOps(_|$)", lst):
                origin = "Daily Ops"
            origin = re.sub(r"^\s*Event:\s*", "", origin or "").strip()
            if not origin or origin.lower() in shown:
                continue
            for r in it.get("obtainRoutes") or []:
                if not r.get("populated"):
                    continue
                lines = r["lines"]
                # Promote to a second Source only when it is the same kind of
                # thing: a NAMED event (from the list's quest, not a guessed
                # "Daily Ops" from the list name) on a live Events route.
                # Anything else (retired items, quest or vendor routes) keeps
                # the neutral note — the list name alone is not proof.
                live = not any("no longer obtainable" in l.lower() or "retired" in l.lower()
                               for l in lines)
                if not (named and live and r.get("route") == "Events & Activities"):
                    lines.append(f"Note: Also tied to {origin} in the game files.")
                    shown += " " + origin.lower()
                    break
                at = max((i for i, l in enumerate(lines)
                          if l.lower().startswith("source:")), default=len(lines) - 1)
                lines.insert(at + 1, f"Source: {origin}")
                if it.get("sourceRef") and origin not in it["sourceRef"]:
                    it["sourceRef"] = f"{it['sourceRef']} / {origin}"
                if it.get("howToObtain") and origin not in it["howToObtain"]:
                    it["howToObtain"] = f"{it['howToObtain']} / {origin}"
                shown += " " + origin.lower()
                break
    # In-game names from the naming rules (INNR), then every source the files
    # give, traced from each unique's own lists up to whatever hands them out.
    # Both read the game files each build — see uwa_names.py / uwa_trace.py.
    _pk = lambda p, exclude=None: pick(p, channel, exclude=exclude)
    uwa_names.apply(all_items, uwa_names.Names(_pk))
    uwa_trace.apply(all_items, uwa_trace.Tracer(_pk, read_tsv), uwa_names.Names(_pk),
                    obtain_idx, getattr(obtain_idx, "hand_only", ()))
    craft_mod_routes(all_items, channel)
    for _it in all_items:
        # A summary with nothing behind it would send readers somewhere the
        # files never say (The Pipe kept "Event: Monster Mash", inherited from
        # the plain Pipe Gun pool).
        if not any(r.get("populated") for r in _it.get("obtainRoutes") or []):
            _it["howToObtain"] = "No source found in the game files"
            _it["sourceType"], _it["sourceRef"], _it["questName"] = "Unknown", "", ""
    uwa_obtain.merge_package_lines(all_items)
    attach_conditions(all_items, channel)
    uwa_images.attach(all_items)   # pictures by name, from the server listing

    # Items Bethesda's own WeaponsUniqueNamedList has never heard of AND that
    # the override table names as mod records rather than weapons. Dropping
    # them here rather than in the discovery pass keeps the reasoning in one
    # place, in data/, where the next one can be added without a code change.
    # Bethesda's own rosters beat the hand "excluded" table: Minty Breather
    # sat in it as "a mod record" while WeaponsUniqueNamedList lists it.
    _excl = {n for n in obtain_idx.excluded if uwa_discover.key(n) not in disc_official}
    dropped = [i["name"] for i in all_items if i["name"] in _excl]
    if dropped:
        all_items[:] = [i for i in all_items if i["name"] not in _excl]
        print(f"  excluded {len(dropped)} non-item record(s): "
              f"{', '.join(sorted(dropped))}")

    # Ticks on the page are saved by id, so an item keeps the id it had in the
    # last build. Only a new item gets a new one, and never a used one (adding
    # Overkill must not hand it The Guarantee's fatman-2 and its ticks).
    prev_ids, used_ids = {}, set()
    _prev = REPO_ROOT / "dist" / ("pts" if channel == "pts" else "") / "unique_weapons_armour" / "unique_weapons_armour.json"
    try:
        for _p in json.loads(_prev.read_text(encoding="utf-8")).get("items", []):
            if _p.get("id"):
                for _n in [_p.get("name")] + list(_p.get("aka") or []):
                    if _n:
                        prev_ids.setdefault((_p.get("kind"), norm_name(_n)), _p["id"])
    except (OSError, ValueError, AttributeError):
        pass
    _taken = set(prev_ids.values())
    for it in all_items:
        it.pop("_cmod_fids", None)
        it.pop("_rosterList", None)
        it.pop("_rosterLists", None)
        it.pop("_altNames", None)
        it.pop("_nameKeywords", None)
        it.pop("_ownLists", None)
        it.pop("_howWas", None)
        nid = next((prev_ids[(it.get("kind"), norm_name(n))]
                    for n in [it.get("name")] + list(it.get("aka") or [])
                    if n and (it.get("kind"), norm_name(n)) in prev_ids
                    and prev_ids[(it.get("kind"), norm_name(n))] not in used_ids), None)
        if not nid:
            nid = unique_slug(it.get("edid") or it.get("name") or "item")
            while nid in _taken or nid in used_ids:
                nid = unique_slug(it.get("edid") or it.get("name") or "item")
        used_ids.add(nid)
        it["id"] = nid

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
    report_source_changes(out_path, output)
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
    print(f"  Mods: {mod_stats}")

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
