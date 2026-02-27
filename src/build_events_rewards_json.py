# ==========================================================
# DF/BNB — Events Rewards Builder (Dynamic Layout Version)
# Follows: QUEST → GMRW → Reward Component → LVLI → Items
# Uses full RNG-76 LVLI math (no-globals supported)
# ==========================================================

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

TSV_DIR = Path("tsv")
OUT_JSON = Path("dist/events_rewards.json")
MAX_GMRW_REFS = 10

# -------------------- Helpers --------------------

def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def pick_latest(prefix):
    files = sorted(TSV_DIR.glob(f"{prefix}*.tsv"))
    if not files:
        raise FileNotFoundError(f"Missing {prefix}*.tsv")
    return files[-1]

def parse_float(x):
    try:
        return float(str(x).strip())
    except:
        return None

def parse_int(x):
    v = parse_float(x)
    return int(round(v)) if v is not None else None

def parse_ref_cell(cell):
    if not cell:
        return None
    parts = str(cell).split(":")
    if len(parts) >= 2 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return parts[0].upper()
    return None

def parse_item_token(token):
    if not token:
        return None
    parts = str(token).split(":")
    if len(parts) >= 3:
        return {
            "formid": parts[0].upper(),
            "edid": parts[1],
            "sig": parts[2]
        }
    return None

def load_rows(path):
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f, delimiter="\t"))

def load_map(path, key):
    m = {}
    for r in load_rows(path):
        fid = (r.get(key) or "").upper()
        if fid:
            m[fid] = r
    return m

# -------------------- LVLI Engine (RNG-76 math) --------------------

def parse_flags_int(x):
    try:
        return int(str(x).strip() or "0")
    except:
        return 0

class LvliEngine:
    def __init__(self, lvli_list, lvli_entries):
        self.lvli_list = lvli_list
        self.lvli_entries = lvli_entries
        self._empty_cache = {}
        self._stack = set()

    def list_empty_chance(self, fid):
        fid = (fid or "").upper()
        if not fid:
            return 1.0
        if fid in self._empty_cache:
            return self._empty_cache[fid]
        if fid in self._stack:
            return 0.0

        self._stack.add(fid)

        lrow = self.lvli_list.get(fid, {})
        flags = parse_flags_int(lrow.get("LVLF_Flags"))

        list_cn = parse_float(lrow.get("LVCV_ChanceNoneValue")) or 0
        list_self = 1 - (list_cn / 100)

        entries = self.lvli_entries.get(fid, [])
        if not entries:
            self._empty_cache[fid] = 1.0
            self._stack.remove(fid)
            return 1.0

        base_weight = list_self / len(entries) if entries else 0

        empty_after = 1.0
        for e in entries:
            entry_cn = parse_float(e.get("LVOV_ChanceNoneValue")) or 0
            entry_presence = 1 - (entry_cn / 100)

            token = parse_item_token(e.get("LVLO_Reference"))
            sub_nonempty = 1.0

            if token and token.get("sig") == "LVLI":
                sub_empty = self.list_empty_chance(token["formid"])
                sub_nonempty = 1 - sub_empty

            chance = base_weight * entry_presence * sub_nonempty
            empty_after *= (1 - chance)

        result = (1 - list_self) + list_self * empty_after
        self._empty_cache[fid] = result
        self._stack.remove(fid)
        return result

    def expand(self, fid):
        fid = (fid or "").upper()
        lrow = self.lvli_list.get(fid, {})
        list_cn = parse_float(lrow.get("LVCV_ChanceNoneValue")) or 0
        list_self = 1 - (list_cn / 100)

        entries = self.lvli_entries.get(fid, [])
        if not entries:
            return []

        base_weight = list_self / len(entries)

        out = []

        for e in entries:
            entry_cn = parse_float(e.get("LVOV_ChanceNoneValue")) or 0
            entry_presence = 1 - (entry_cn / 100)

            token = parse_item_token(e.get("LVLO_Reference"))
            sub_nonempty = 1.0
            sub_empty = None

            if token and token.get("sig") == "LVLI":
                sub_empty = self.list_empty_chance(token["formid"])
                sub_nonempty = 1 - sub_empty

            chance = base_weight * entry_presence * sub_nonempty

            out.append({
                "item": token,
                "chance": chance,
                "entryChanceNone": entry_cn,
                "sublistEmpty": sub_empty
            })

        return out

# -------------------- Group Name Logic --------------------

def group_from_lvli_name(edid):
    if not edid:
        return "Rewards"

    # Extract last meaningful segment
    parts = edid.split("_")
    if "Rewards" in parts:
        idx = parts.index("Rewards")
        if idx + 1 < len(parts):
            return parts[idx + 1].replace("LL", "").replace("LS", "").replace("L", "")
    return "Rewards"

def clean_group_label(x):
    x = re.sub(r"(?<!^)([A-Z])", r" \1", x)
    return x.strip()

# -------------------- Build --------------------

def main():

    QUEST = pick_latest("QUEST_Export_")
    GMRW = pick_latest("GMRW_Export_")
    LVLI_LIST = pick_latest("LVLI_Export_")
    LVLI_ENTRIES = pick_latest("LVLI_Export_")
    BOOK = pick_latest("BOOK_Export_")

    quests = load_rows(QUEST)
    gmrw = load_rows(GMRW)
    books = load_map(BOOK, "FormID")

    lvli_list = load_map(LVLI_LIST, "LVLI_FormID")
    lvli_entries = {}

    for r in load_rows(LVLI_ENTRIES):
        fid = (r.get("LVLI_FormID") or "").upper()
        if fid:
            lvli_entries.setdefault(fid, []).append(r)

    engine = LvliEngine(lvli_list, lvli_entries)

    out = {
        "generatedAt": now_iso(),
        "events": []
    }

    for q in quests:

        full = (q.get("FULL - Name") or "")
        if not full.lower().startswith(("event:", "activity:", "enclave activity:")):
            continue

        event_obj = {
            "quest": {
                "formid": q.get("FormID"),
                "edid": q.get("EDID"),
                "full": full,
                "desc": q.get("DESC - Description"),
                "type": q.get("Quest Type"),
                "location": q.get("LNAM - Location")
            },
            "groups": {}
        }

        # Collect GMRW rows
        gmrw_rows = []
        for i in range(MAX_GMRW_REFS):
            fid = parse_ref_cell(q.get(f"GMRWRef{i}"))
            if fid:
                gmrw_rows += [r for r in gmrw if r.get("FormID") == fid]

        for rr in gmrw_rows:

            conditions = rr.get("Conditions") or ""
            component_prob = 1.0
            m = re.search(r"GetRandomPercent\s*>=\s*(\d+)", conditions)
            if m:
                n = float(m.group(1))
                component_prob = (100 - n) / 100

            # BASE payouts
            xp = rr.get("NAM7_XPGlobal")
            if xp:
                event_obj["groups"].setdefault("Base Rewards", []).append({
                    "displayName": "XP",
                    "dropRate": component_prob * 100,
                    "technical": {"xpGlobal": xp}
                })

            # Rewarded item
            token = parse_item_token(rr.get("RewardedItem"))
            if not token:
                continue

            if token["sig"] == "LVLI":
                expanded = engine.expand(token["formid"])

                group_key = clean_group_label(group_from_lvli_name(token["edid"]))

                for e in expanded:
                    item = e["item"]
                    if not item:
                        continue

                    display = item["edid"]

                    if item["sig"] == "BOOK":
                        b = books.get(item["formid"])
                        if b:
                            display = b.get("FULL")

                    final_rate = component_prob * e["chance"] * 100

                    event_obj["groups"].setdefault(group_key, []).append({
                        "displayName": display,
                        "dropRate": final_rate,
                        "technical": {
                            "lvli": token["formid"],
                            "entryChanceNone": e["entryChanceNone"],
                            "sublistEmpty": e["sublistEmpty"]
                        }
                    })

        out["events"].append(event_obj)

    OUT_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("Wrote", OUT_JSON)

if __name__ == "__main__":
    main()