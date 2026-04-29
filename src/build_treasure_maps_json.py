#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_treasure_maps_json.py
===========================
Builds: dist/treasure_maps.json

Reads xEdit TSV exports and resolves the Treasure Map + U Mine It LVLI
hierarchies into structured JSON with drop rates.

Key LVLIs:
  LL_TreasureMap (0038B471)           - FirstMatch by region (5 entries)
  LLS_TreasureMap_{FF,TV,MTN,MTR,CB_SF} - pick-one map pools per region
  LL_TreasureMap_Reward (001A7220)    - UseAll reward chain
  MTRz05_LL_01_MinerMine (0032AD5F)   - UseAll Independent (max_count=0)
  MTRz05_LL_02_ProspectorMine (0032AD60)
  MTRz05_LL_03_ExcavatorMine (0032AD61)
  MTRZ05_Vendor_LuckyMaps (0086A8AF)  - pick-one lucky map vendor pool

Usage:
  python build_treasure_maps_json.py
"""

import csv, glob, json, os, re, sys
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone
from pathlib import Path

from patchlog_utils import write_empty_patchlog_feed

_REPO_ROOT = Path(__file__).resolve().parent.parent
DIST_DIR   = _REPO_ROOT / "dist"
TSV_DIR    = _REPO_ROOT / "tsv"

_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

def _filename_date_key(path):
    base = os.path.basename(path).lower()
    m = re.search(r'_([a-z]+)_(\d{4})', base)
    if m:
        month_num = _MONTH_ORDER.get(m.group(1), 0)
        if month_num:
            return (int(m.group(2)), month_num)
    return (0, 0)

def newest(pattern, exclude_substrings=None):
    full_pattern = str(TSV_DIR / pattern)
    files = glob.glob(full_pattern)
    if exclude_substrings:
        files = [f for f in files
                 if not any(s in os.path.basename(f) for s in exclude_substrings)]
    if not files:
        raise FileNotFoundError(f"No files matching {pattern} in {TSV_DIR}")
    files.sort(key=lambda x: (_filename_date_key(x), os.path.getmtime(x)))
    return files[-1]

def read_tsv(path):
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))

def pick(row, *keys, default=""):
    for k in keys:
        if k in row:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return v
    return default

def safe_float(v, default=0.0):
    try: return float(v)
    except (ValueError, TypeError): return default

def fmt_pct(rate):
    pct = rate * 100
    if pct == 0: return "0%"
    if pct >= 99.999: return "100%"
    if pct >= 10: return f"{pct:.1f}%"
    if pct >= 1: return f"{pct:.2f}%"
    return f"{pct:.3f}%"

def parse_formid_ref(ref_str):
    parts = ref_str.split(":")
    if len(parts) >= 3: return parts[0], parts[1], parts[2]
    if len(parts) == 2: return parts[0], parts[1], ""
    return ref_str, "", ""

def humanize_edid(edid):
    s = re.sub(r'^(?:c_|MTRZ05_|MTRz05_|LL_|LLS_|LLE_|RA_)', '', edid)
    s = re.sub(r'_scrap$', ' Scrap', s)
    s = re.sub(r'([a-z])([A-Z])', r'\1 \2', s)
    s = s.replace('_', ' ')
    return s.strip()

GLOB_FALLBACKS = {
    "0043B770": 75.0,
    "0089EA90": 75.0,
}

class GlobLookup:
    def __init__(self, rows):
        self._by_formid = {}
        fltv_key = None
        if rows:
            keys = list(rows[0].keys())
            for c in ("FLTV", "DATA"):
                if c in keys: fltv_key = c; break
            if fltv_key is None and len(keys) >= 3:
                fltv_key = keys[2]
        for r in rows:
            fid = pick(r, "FormID", "GLOB_FormID", default="").strip()
            fltv = safe_float(r.get(fltv_key, "0") if fltv_key else "0", 0.0)
            edid = pick(r, "EDID", default="")
            if fid: self._by_formid[fid] = {"fltv": fltv, "edid": edid}

    def fltv(self, formid):
        formid = formid.strip()
        if formid in self._by_formid: return self._by_formid[formid]["fltv"]
        if formid in GLOB_FALLBACKS: return GLOB_FALLBACKS[formid]
        return None

    def edid(self, formid):
        formid = formid.strip()
        return self._by_formid.get(formid, {}).get("edid", "")

class BookLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid: self._by_formid[fid] = {"full": pick(r, "FULL", default=""), "edid": pick(r, "EDID", default="")}

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]: return e["full"]
        if e and e["edid"]: return humanize_edid(e["edid"])
        return formid

    def edid(self, formid):
        e = self._by_formid.get(formid.strip())
        return e["edid"] if e else ""

class MiscLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid: self._by_formid[fid] = {"full": pick(r, "FULL", default=""), "edid": pick(r, "EDID", default="")}

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]: return e["full"]
        if e and e["edid"]: return humanize_edid(e["edid"])
        return formid

class AlchLookup:
    def __init__(self, rows):
        self._by_formid = {}
        for r in rows:
            fid = pick(r, "FormID", default="").strip()
            if fid: self._by_formid[fid] = {"full": pick(r, "FULL", default=""), "edid": pick(r, "EDID", default="")}

    def name(self, formid):
        e = self._by_formid.get(formid.strip())
        if e and e["full"]: return e["full"]
        if e and e["edid"]: return humanize_edid(e["edid"])
        return formid

class LvliListIndex:
    def __init__(self, rows):
        self._d = {}
        for r in rows:
            fid = pick(r, "LVLI_FormID", default="").strip()
            if not fid: continue
            self._d[fid] = {
                "edid": pick(r, "LVLI_EDID", default=""),
                "flags": pick(r, "LVLF_Flags", default=""),
                "count": int(safe_float(pick(r, "EntryCount", "LLCT_Count", default="0"))),
                "maxValue": safe_float(pick(r, "LVMV_MaxValue", default="0")),
                "maxGlob": pick(r, "LVMG_MaxGlobal", default=""),
            }

    def get(self, fid): return self._d.get(fid.strip())

    def parse_flags(self, flags_str):
        f = flags_str or ""
        return {
            "useAll": len(f) > 2 and f[2] == '1',
            "firstMatch": len(f) > 6 and f[6] == '1',
        }

    def max_count(self, fid, globs):
        info = self.get(fid)
        if not info: return 0
        mg = info["maxGlob"]
        if mg:
            mg_fid = mg.split(":")[0] if ":" in mg else mg
            val = globs.fltv(mg_fid)
            if val is not None: return int(val)
        mv = info["maxValue"]
        if mv: return int(mv)
        return 0

class LvliEntriesIndex:
    def __init__(self, rows):
        self._d = defaultdict(list)
        for r in rows:
            pfid = pick(r, "LVLI_FormID", default="").strip()
            ref_raw = pick(r, "LVLO_Reference", default="")
            if not pfid or not ref_raw: continue
            ref_fid, ref_edid, ref_sig = parse_formid_ref(ref_raw)
            conds = []
            for ci in range(1, 11):
                c = pick(r, f"Cond{ci}", default="")
                if c: conds.append(c)
            self._d[pfid].append({
                "index": int(safe_float(pick(r, "EntryIndex", default="0"))),
                "ref_formid": ref_fid, "ref_edid": ref_edid, "ref_sig": ref_sig,
                "chanceNoneValue": safe_float(pick(r, "LVOV_ChanceNoneValue", default="0")),
                "chanceNoneGlob": pick(r, "LVOG_ChanceNoneGlobal", default=""),
                "quantity": safe_float(pick(r, "LVIV_Quantity", default="1")),
                "condCount": int(safe_float(pick(r, "CondCount", default="0"))),
                "conditions": conds,
            })
        for fid in self._d:
            self._d[fid].sort(key=lambda e: e["index"])

    def entries(self, fid): return self._d.get(fid.strip(), [])

def resolve_chance_none(entry, globs):
    cn_glob = entry.get("chanceNoneGlob", "")
    if cn_glob:
        glob_fid = cn_glob.split(":")[0] if ":" in cn_glob else cn_glob
        glob_edid = globs.edid(glob_fid)
        if "MinLvl" in glob_edid: return 0.0
        val = globs.fltv(glob_fid)
        if val is not None: return val
    cn_val = entry.get("chanceNoneValue", 0.0)
    if cn_val: return cn_val
    return 0.0

def has_toggle_condition(entry):
    for c in entry.get("conditions", []):
        if "GetGlobalValue" in c and "Toggle" in c and "1.000000" in c: return True
    return False

def has_learned_recipe_condition(entry):
    for c in entry.get("conditions", []):
        if "HasLearnedRecipe" in c: return True
    return False

def get_toggle_glob_info(entry):
    for c in entry.get("conditions", []):
        m = re.search(r'(\w+_Toggle)\s*\[GLOB:([0-9A-Fa-f]{8})\]', c)
        if m: return m.group(1), m.group(2)
    return None, None

def get_quest_condition(entry):
    for c in entry.get("conditions", []):
        if "PlayerHasQuest" in c:
            m = re.search(r'"([^"]+)"\s*\[QUST:', c)
            if m: return f"Requires quest: {m.group(1)}"
            m2 = re.search(r'(\w+)\s*\[QUST:', c)
            if m2: return f"Requires quest: {humanize_edid(m2.group(1))}"
    return None

REGION_MAP = OrderedDict([
    ("LLS_TreasureMap_FF",    {"key": "forest",       "name": "The Forest",           "formid": "003D0CD5"}),
    ("LLS_TreasureMap_TV",    {"key": "toxic_valley",  "name": "Toxic Valley",         "formid": "003D0CD6"}),
    ("LLS_TreasureMap_MTR",   {"key": "ash_heap",      "name": "Ash Heap",             "formid": "003D0CD7"}),
    ("LLS_TreasureMap_CB_SF", {"key": "cranberry_bog", "name": "Cranberry Bog & Mire", "formid": "003D0CD8"}),
    ("LLS_TreasureMap_MTN",   {"key": "savage_divide", "name": "Savage Divide",        "formid": "003D0CD9"}),
])

REWARD_REGION_FORMIDS = {
    "forest":       "0050CC2E",
    "toxic_valley": "0050CC31",
    "ash_heap":     "0050CC30",
    "cranberry_bog":"0050CC2D",
    "savage_divide":"0050CC2F",
}

REWARD_REGION_NAMES = {
    "forest":       "The Forest",
    "toxic_valley": "Toxic Valley",
    "ash_heap":     "Ash Heap",
    "cranberry_bog":"Cranberry Bog & Mire",
    "savage_divide":"Savage Divide",
}

UMINE_TIERS = OrderedDict([
    ("miner",      {"formid": "0032AD5F", "edid": "MTRz05_LL_01_MinerMine",      "name": "Miner Mine"}),
    ("prospector", {"formid": "0032AD60", "edid": "MTRz05_LL_02_ProspectorMine",  "name": "Prospector Mine"}),
    ("excavator",  {"formid": "0032AD61", "edid": "MTRz05_LL_03_ExcavatorMine",   "name": "Excavator Mine"}),
])

def build_region_maps(entries_idx, books):
    regions = OrderedDict()
    for edid, rinfo in REGION_MAP.items():
        region_entries = entries_idx.entries(rinfo["formid"])
        count = len(region_entries)
        maps = []
        for e in region_entries:
            fid = e["ref_formid"]
            maps.append({
                "name": books.name(fid),
                "edid": books.edid(fid),
                "form_id": fid,
                "drop_rate": fmt_pct(1.0 / count) if count > 0 else "0%",
                "drop_rate_raw": round(1.0 / count, 6) if count > 0 else 0,
            })
        maps.sort(key=lambda m: m["name"])
        regions[rinfo["key"]] = {
            "name": rinfo["name"],
            "list_edid": edid,
            "list_formid": rinfo["formid"],
            "map_count": count,
            "maps": maps,
            "notes": [
                f"Pick-one list: each map has equal {fmt_pct(1.0/count)} chance" if count else "Empty list",
                "Region selection via FirstMatch on LL_TreasureMap [0038B471]",
            ],
        }
    return regions

def build_rewards(list_idx, entries_idx, globs):
    reward_formid = "001A7220"
    reward_info = list_idx.get(reward_formid)
    reward_entries = entries_idx.entries(reward_formid)
    max_c = list_idx.max_count(reward_formid, globs) if reward_info else 0

    pool_labels = {
        "LL_Recipes_All_Any_AllRegions": "Recipes",
        "LLS_Loot_Weapons_Any_Mods_Recipes_AllRegions": "Weapon Mod Plans",
        "LLS_Loot_Armor_Mods_Recipes_AllRegions": "Armour Mod Plans",
        "LLS_TreasureMap_Reward_Base": "Region-Specific Loot",
    }

    pools = []
    # Caps pool first
    caps_entries = entries_idx.entries("0050CC2A")
    if caps_entries:
        pools.append({"name": "Caps", "edid": "LL_Caps_TreasureMap", "form_id": "0050CC2A",
                       "sig": "LVLI", "drop_rate": "100%", "chance_none": 0,
                       "notes": ["Caps x5 from LLS_Caps_High"]})
    for e in reward_entries:
        cn = resolve_chance_none(e, globs)
        cn_factor = 1.0 - (cn / 100.0)
        pools.append({
            "name": pool_labels.get(e["ref_edid"], humanize_edid(e["ref_edid"])),
            "edid": e["ref_edid"], "form_id": e["ref_formid"], "sig": e["ref_sig"],
            "drop_rate": fmt_pct(cn_factor), "chance_none": cn,
            "notes": [f"ChanceNone: {cn}"] if cn > 0 else [],
        })

    region_rewards = OrderedDict()
    for rkey, rfid in REWARD_REGION_FORMIDS.items():
        r_entries = entries_idx.entries(rfid)
        items = []
        for re_ in r_entries:
            items.append({
                "name": humanize_edid(re_["ref_edid"]),
                "edid": re_["ref_edid"], "form_id": re_["ref_formid"],
                "sig": re_["ref_sig"], "quantity": int(re_["quantity"]),
            })
        region_rewards[rkey] = {
            "name": REWARD_REGION_NAMES[rkey], "list_formid": rfid, "items": items,
        }

    return {
        "list_formid": reward_formid, "list_edid": "LL_TreasureMap_Reward",
        "list_type": "UseAll", "max_count": max_c, "pools": pools,
        "region_rewards": region_rewards,
        "notes": [
            f"UseAll list with max_count={max_c} (from GLOB Container_MaxCount_Boss_Tier [0030843B])",
            "ChanceNone via GLOB LL_EpicChance_High_ECON [0014EC20]",
            "Each pool fires independently",
        ],
    }

def build_u_mine_it(list_idx, entries_idx, globs, books, misc, alch):
    def resolve_name(entry):
        fid, sig, edid = entry["ref_formid"], entry["ref_sig"], entry["ref_edid"]
        if sig == "BOOK": return books.name(fid)
        if sig == "MISC": return misc.name(fid)
        if sig == "ALCH": return alch.name(fid)
        return humanize_edid(edid) if edid else fid

    tiers = OrderedDict()
    for tier_key, tier_info in UMINE_TIERS.items():
        fid = tier_info["formid"]
        lvli_info = list_idx.get(fid)
        entries = entries_idx.entries(fid)
        flags = list_idx.parse_flags(lvli_info["flags"] if lvli_info else "")
        max_c = list_idx.max_count(fid, globs) if lvli_info else 0

        # These lists have UseAll + max_count=0 -> Independent mode (each entry fires at own rate)
        rewards = []
        for e in entries:
            cn = resolve_chance_none(e, globs)
            rate = 1.0 - (cn / 100.0)  # Independent: rate = cn_factor

            toggle_name, toggle_fid = get_toggle_glob_info(e)
            conditions, notes = [], []

            if has_toggle_condition(e) and toggle_name:
                conditions.append(f"Requires {humanize_edid(toggle_name)} = ON")
                if toggle_fid: notes.append(f"Toggle GLOB: {toggle_name} [{toggle_fid}]")
            if has_learned_recipe_condition(e):
                conditions.append("Won't drop if you've already learned this recipe")
            qc = get_quest_condition(e)
            if qc: conditions.append(qc)
            if cn > 0:
                notes.append(f"ChanceNone: {cn}")
                cn_glob_str = e.get("chanceNoneGlob", "")
                if cn_glob_str:
                    gfid = cn_glob_str.split(":")[0]
                    gedid = cn_glob_str.split(":")[1] if ":" in cn_glob_str else ""
                    notes.append(f"ChanceNone via GLOB {gedid} [{gfid}]")

            tradeable = True
            if "PlayerTitle" in e["ref_edid"] or "CampTitle" in e["ref_edid"]:
                tradeable = False

            rewards.append({
                "name": resolve_name(e), "edid": e["ref_edid"], "form_id": e["ref_formid"],
                "sig": e["ref_sig"], "quantity": int(e["quantity"]),
                "drop_rate": fmt_pct(rate), "drop_rate_raw": round(rate, 6),
                "tradeable": tradeable, "conditions": conditions, "notes": notes,
            })

        tiers[tier_key] = {
            "name": tier_info["name"], "edid": tier_info["edid"], "form_id": fid,
            "list_type": "UseAll Independent (max_count=0)",
            "entry_count": len(entries), "rewards": rewards,
            "notes": [
                f"UseAll list, max_count={max_c} -> Independent",
                "Each entry fires independently at its own rate",
                "rate = 1 - (ChanceNone / 100)",
            ],
        }
    return tiers

def build_lucky_maps(entries_idx, books):
    vendor_fid = "0086A8AF"
    vendor_entries = entries_idx.entries(vendor_fid)
    count = len(vendor_entries)
    items = []
    for e in vendor_entries:
        fid = e["ref_formid"]
        qc = get_quest_condition(e)
        items.append({
            "name": books.name(fid), "edid": books.edid(fid), "form_id": fid,
            "drop_rate": fmt_pct(1.0 / count) if count > 0 else "0%",
            "conditions": [qc] if qc else [],
        })
    drop_sources = [
        {"source": "Vendor (Purveyor Murmrgh)", "list": "MTRZ05_Vendor_LuckyMaps",
         "notes": "Pick-one from 3 maps, requires Lucky Strike quest not active"},
        {"source": "Activity Rewards", "list": "RA_LL_Rewards_Activities_UMineItMaps",
         "notes": "ChanceNone via GLOB LTT_RA_Rewards_Activities_UMineItMap_DropRate (runtime toggle)"},
        {"source": "Public Event Rewards", "list": "RA_LLS_Loot_UmineItMaps",
         "notes": "Requires Lucky Strike quest not active"},
        {"source": "Safe Containers", "list": "Various LLE_Safe_* lists",
         "notes": "Excavator's Map only; ChanceNone 70-95 depending on safe difficulty"},
        {"source": "Motherlode Container (Rare)", "list": "MTR05_LLI_MotherlodeContainer_Rare",
         "notes": "Excavator's Map; ChanceNone 70"},
    ]
    return {
        "vendor_list": "MTRZ05_Vendor_LuckyMaps", "vendor_formid": vendor_fid,
        "items": items, "drop_sources": drop_sources,
        "notes": [
            "Lucky Maps start the U Mine It questline (Lucky Strike)",
            "Three tiers: Miner's Map -> Prospector's Map -> Excavator's Map",
            "Each tier leads to progressively better mining rewards",
        ],
    }

def build_teammate_reward(entries_idx):
    fid = "001A721E"
    entries = entries_idx.entries(fid)
    count = len(entries)
    items = []
    for e in entries:
        items.append({
            "name": humanize_edid(e["ref_edid"]), "edid": e["ref_edid"],
            "form_id": e["ref_formid"], "sig": e["ref_sig"],
            "drop_rate": fmt_pct(1.0 / count) if count > 0 else "0%",
        })
    return {
        "list_edid": "LL_TreasureMap_TeammateReward", "list_formid": fid,
        "list_type": "Pick-one", "items": items,
        "notes": [
            "Teammates get a random reward when a team member digs a treasure mound",
            f"Pick-one from {count} entries (equal chance each)",
        ],
    }

def main():
    print("[build_treasure_maps_json.py] Starting build...")
    source_files = []

    lvli_list_path = newest("LVLI_Export_*_LVLI_List.tsv")
    lvli_entries_path = newest("LVLI_Export_*_LVLI_Entries.tsv")
    book_path = newest("BOOK_Export_*.tsv", exclude_substrings=["Locations"])
    glob_path = newest("GLOB_Export_*.tsv")
    source_files = [os.path.basename(p) for p in [lvli_list_path, lvli_entries_path, book_path, glob_path]]

    for label, p in [("LVLI List", lvli_list_path), ("LVLI Entries", lvli_entries_path),
                     ("BOOK", book_path), ("GLOB", glob_path)]:
        print(f"  {label}: {os.path.basename(p)}")

    list_idx = LvliListIndex(read_tsv(lvli_list_path))
    entries_idx = LvliEntriesIndex(read_tsv(lvli_entries_path))
    globs = GlobLookup(read_tsv(glob_path))
    books = BookLookup(read_tsv(book_path))

    misc, alch = MiscLookup([]), AlchLookup([])
    try:
        misc_path = newest("MISC_Export_*.tsv")
        misc = MiscLookup(read_tsv(misc_path))
        source_files.append(os.path.basename(misc_path))
    except FileNotFoundError: pass
    try:
        alch_path = newest("ALCH_Export_*.tsv")
        alch = AlchLookup(read_tsv(alch_path))
        source_files.append(os.path.basename(alch_path))
    except FileNotFoundError: pass

    print("  Building regions..."); regions = build_region_maps(entries_idx, books)
    print("  Building rewards..."); rewards = build_rewards(list_idx, entries_idx, globs)
    print("  Building U Mine It..."); tiers = build_u_mine_it(list_idx, entries_idx, globs, books, misc, alch)
    print("  Building Lucky Maps..."); lucky = build_lucky_maps(entries_idx, books)
    print("  Building teammate reward..."); teammate = build_teammate_reward(entries_idx)

    output = {
        "regions": regions,
        "rewards": rewards,
        "teammate_reward": teammate,
        "u_mine_it": {"tiers": tiers, "lucky_maps": lucky},
        "meta": {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "generator": "build_treasure_maps_json.py",
            "source_files": source_files,
            "notes": [
                "Drop rates resolved from xEdit LVLI TSV exports",
                "GLOB fallback values used for runtime-set globals (see drop rate engine skill S10)",
                "Toggle-gated rewards shown with conditions",
                "GMRW conditions NOT baked in - handled by website JS (see drop rate engine skill S7)",
            ],
        },
    }

    os.makedirs(str(DIST_DIR), exist_ok=True)
    out_path = DIST_DIR / "treasure_maps.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total_maps = sum(r["map_count"] for r in regions.values())
    print(f"\n  Output: {out_path}")
    print(f"  Regions: {len(regions)}, Maps: {total_maps}, Reward pools: {len(rewards['pools'])}")
    print(f"  U Mine It tiers: {len(tiers)}, Lucky Maps: {len(lucky['items'])}")
    print("[build_treasure_maps_json.py] Done.")

    patchlog_dir = DIST_DIR / "patchlogs"
    os.makedirs(str(patchlog_dir), exist_ok=True)
    write_empty_patchlog_feed(str(DIST_DIR), "patchlog_latest_df_treasure_maps.json", current_count=total_maps)

if __name__ == "__main__":
    main()
