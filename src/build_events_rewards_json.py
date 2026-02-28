# ==========================================================
# DF/BNB — Build Events Rewards JSON (Fasnacht-style v1)
#
# Outputs:
#   dist/events_rewards.json
#   dist/events_rewards_by_page.json
#   dist/patchlog_latest_df_events.json
#
# Inputs (tsv/):
#   guide_index.tsv
#   QUEST_Export_*.tsv
#   GMRW_Export_*.tsv
#   GLOB_Export_*.tsv
#   LVLI_Export_*_LVLI_List.tsv
#   LVLI_Export_*_LVLI_Entries.tsv
#   LVLI_Export_*_LVLI_Refs.tsv
#   LVLI_Export_*_LVLI_Math.tsv
#   BOOK_Export_*.tsv
#   ARMO_Export_*.tsv
#
# Notes:
# - This builder focuses on “Fasnacht-style” event structure:
#   QUEST -> GMRW -> RewardedItem LVLI (*_LL_Quest_Rewards)
#   -> sublists: Default, Headwear, Plans(Recipes)
# - Legendary Modules must be bubbled into Base Rewards.
# - Party Crashers come from QUEST TSV (PartyCrasher_* columns).
# - Never allow “blank page”: if rewards can’t be resolved, we emit warnings and placeholders.
# ==========================================================

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set

TSV_DIR = Path("tsv")
DIST_DIR = Path("dist")

OUT_EVENTS = DIST_DIR / "events_rewards.json"
OUT_BY_PAGE = DIST_DIR / "events_rewards_by_page.json"
OUT_PATCHLOG = DIST_DIR / "patchlog_latest_df_events.json"

IGNORE_EDID_PREFIXES = ("DEL", "CUT", "POST", "ZZZ", "zzz", "Zzz")
IGNORE_SUBSTRINGS = ("TheDrifter",)  # treat like cut content


# -------------------- IO helpers --------------------

def now_iso_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def latest_match(glob_pattern: str) -> Optional[Path]:
    matches = sorted(TSV_DIR.glob(glob_pattern))
    return matches[-1] if matches else None


# -------------------- parsing helpers --------------------

def is_ignored_edid(edid: str) -> bool:
    s = (edid or "").strip()
    if not s:
        return False
    for p in IGNORE_EDID_PREFIXES:
        if s.startswith(p):
            return True
    for sub in IGNORE_SUBSTRINGS:
        if sub.lower() in s.lower():
            return True
    return False


def norm_path(p: str) -> str:
    s = str(p or "")
    s = s.split("#")[0].split("?")[0]
    if not s.startswith("/"):
        s = "/" + s
    s = re.sub(r"/{2,}", "/", s)
    if not s.endswith("/"):
        s += "/"
    return s


def slugify(s: str) -> str:
    t = (s or "").strip().lower()
    t = re.sub(r"[^a-z0-9]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-")
    return t


def event_title_from_full(full: str) -> str:
    s = (full or "").strip()
    low = s.lower()
    for prefix in ("event:", "activity:", "enclave activity:"):
        if low.startswith(prefix):
            return s[len(prefix):].strip()
    return s


def parse_token(cell: str) -> Optional[Dict[str, str]]:
    """
    TSV tokens look like:
      FORMID:EDID
      FORMID:EDID:SIG
    """
    if not cell:
        return None
    parts = str(cell).strip().split(":")
    if len(parts) >= 2 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        tok = {"formid": parts[0].upper(), "edid": parts[1]}
        if len(parts) >= 3:
            tok["sig"] = parts[2]
        return tok
    return None


def parse_formid_ref(cell: str) -> Optional[str]:
    tok = parse_token(cell)
    return tok["formid"] if tok else None


def as_float(x: Any) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return None


def percent_from_glob_fltv(fltv: Optional[float]) -> Optional[float]:
    """
    Heuristic:
    - If 0..1 => interpret as probability fraction
    - If 1..100 => interpret as percent
    - Otherwise unknown
    """
    if fltv is None:
        return None
    if 0.0 <= fltv <= 1.0:
        return fltv * 100.0
    if 1.0 < fltv <= 100.0:
        return fltv
    if fltv == 1.0:
        return 100.0
    return None


def chance_from_chance_none(chance_none_percent: float) -> float:
    # chance_none=0 => 100% drop
    return max(0.0, min(100.0, 100.0 - chance_none_percent))


def pretty_name_from_edid(edid: str) -> str:
    """
    Fallback humanizer when FULL isn't available.
    Keep it conservative (no “creative writing”).
    """
    s = (edid or "").strip()
    if not s:
        return "Unknown"
    # strip common technical suffixes/prefixes
    s = re.sub(r"_PartyCrasher$", "", s, flags=re.I)
    s = re.sub(r"^Lvl", "", s)
    s = s.replace("_", " ").strip()
    # titlecase without breaking acronyms too hard
    s = " ".join(w[:1].upper() + w[1:] if w else "" for w in s.split(" "))
    return s.strip() or edid


# -------------------- data indexes --------------------

@dataclass
class GlobRow:
    formid: str
    edid: str
    fltv: Optional[float]


def build_glob_index(rows: List[Dict[str, str]]) -> Dict[str, GlobRow]:
    out: Dict[str, GlobRow] = {}
    for r in rows:
        fid = (r.get("FormID") or "").upper()
        if not fid:
            continue
        out[fid] = GlobRow(
            formid=fid,
            edid=r.get("EDID") or "",
            fltv=as_float(r.get("FLTV"))
        )
    return out


@dataclass
class BookRow:
    formid: str
    edid: str
    full: str


def build_book_index(rows: List[Dict[str, str]]) -> Dict[str, BookRow]:
    out: Dict[str, BookRow] = {}
    for r in rows:
        fid = (r.get("FormID") or "").upper()
        if not fid:
            continue
        out[fid] = BookRow(formid=fid, edid=r.get("EDID") or "", full=r.get("FULL") or "")
    return out


@dataclass
class ArmoRow:
    formid: str
    edid: str
    full: str


def build_armo_index(rows: List[Dict[str, str]]) -> Dict[str, ArmoRow]:
    out: Dict[str, ArmoRow] = {}
    for r in rows:
        fid = (r.get("FormID") or "").upper()
        if not fid:
            continue
        out[fid] = ArmoRow(formid=fid, edid=r.get("EDID") or "", full=r.get("FULL") or "")
    return out


@dataclass
class LvliListRow:
    formid: str
    edid: str
    chance_none_value: Optional[float]
    flags: str
    count: Optional[int]


@dataclass
class LvliEntryRow:
    parent_formid: str
    reference: str
    chance_none_value: Optional[float]
    chance_none_global: Optional[str]
    quantity: Optional[float]
    quantity_global: Optional[str]
    conditions: str


def build_lvli_list_index(rows: List[Dict[str, str]]) -> Dict[str, LvliListRow]:
    out: Dict[str, LvliListRow] = {}
    for r in rows:
        fid = (r.get("LVLI_FormID") or "").upper()
        if not fid:
            continue
        out[fid] = LvliListRow(
            formid=fid,
            edid=r.get("EDID") or "",
            chance_none_value=as_float(r.get("LVCV_ChanceNoneValue")),
            flags=r.get("LVLF_Flags") or "",
            count=int(as_float(r.get("LLCT_Count") or "0") or 0),
        )
    return out


def build_lvli_entries_index(rows: List[Dict[str, str]]) -> Dict[str, List[LvliEntryRow]]:
    out: Dict[str, List[LvliEntryRow]] = {}
    for r in rows:
        pfid = (r.get("LVLI_FormID") or "").upper()
        if not pfid:
            continue
        ref = r.get("LVLO_Reference") or ""
        out.setdefault(pfid, []).append(
            LvliEntryRow(
                parent_formid=pfid,
                reference=ref,
                chance_none_value=as_float(r.get("LVOV_ChanceNoneValue")),
                chance_none_global=parse_formid_ref(r.get("LVOC_ChanceNoneGlobal") or ""),
                quantity=as_float(r.get("LVIV_Quantity")),
                quantity_global=parse_formid_ref(r.get("LVIG_QuantityGlobal") or ""),
                conditions=r.get("Conditions") or ""
            )
        )
    return out


# -------------------- guide index mapping --------------------

def load_guide_index() -> Tuple[Dict[str, Dict[str, str]], Dict[str, List[str]]]:
    """
    Returns:
      hubs_by_slug: slug -> row
      descendants_by_hub_id: hub_id -> [child_id...]
    """
    gi_path = TSV_DIR / "guide_index.tsv"
    if not gi_path.exists():
        return {}, {}

    rows = load_rows(gi_path)

    hubs_by_slug: Dict[str, Dict[str, str]] = {}
    children_by_parent: Dict[str, List[str]] = {}

    for r in rows:
        pid = (r.get("parentId") or "").strip()
        rid = (r.get("id") or "").strip()
        if pid and rid:
            children_by_parent.setdefault(pid, []).append(rid)

    # hubs: category-hub and topCategory in your event hubs
    for r in rows:
        if (r.get("template") or "").strip() != "category-hub":
            continue
        top = (r.get("topCategory") or "").strip().lower()
        # DF naming tends to be these (adjust later if needed)
        if top not in ("activities", "public-events", "seasonal-events", "events"):
            continue
        slug = (r.get("slug") or "").strip()
        if slug:
            hubs_by_slug[slug] = r

    # descendants: hub -> children + grandchildren
    descendants_by_hub_id: Dict[str, List[str]] = {}
    for slug, hub in hubs_by_slug.items():
        hub_id = (hub.get("id") or "").strip()
        if not hub_id:
            continue
        desc: Set[str] = set()
        for c in children_by_parent.get(hub_id, []):
            desc.add(c)
            for gc in children_by_parent.get(c, []):
                desc.add(gc)
        descendants_by_hub_id[hub_id] = sorted(desc)

    return hubs_by_slug, descendants_by_hub_id


# -------------------- Party Crashers --------------------

def extract_party_crashers(qrow: Dict[str, str], glob_index: Dict[str, GlobRow]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    cnt = int(as_float(qrow.get("PartyCrasherCount") or "0") or 0)
    if cnt <= 0:
        return out

    mesg_tok = parse_token(qrow.get("PartyCrasher_MESG") or "")

    for i in range(0, min(cnt, 10)):
        npc_tok = parse_token(qrow.get(f"PartyCrasher_NPC_{i}") or "")
        glob_tok = parse_token(qrow.get(f"PartyCrasher_GLOB_{i}") or "")
        if not npc_tok or not glob_tok:
            continue

        g = glob_index.get(glob_tok["formid"])
        pct = percent_from_glob_fltv(g.fltv if g else None)

        display_name = pretty_name_from_edid(npc_tok.get("edid") or "")
        chance_text = f"{pct:.0f}%" if pct is not None else "Unknown%"

        out.append({
            "type": "partyCrasher",
            "text": f"{chance_text} chance for {display_name} to spawn at the end of the event.",
            "npc": npc_tok,
            "chanceGlob": {
                "formid": glob_tok["formid"],
                "edid": glob_tok.get("edid", ""),
                "fltv": g.fltv if g else None,
                "percent": pct
            },
            "message": mesg_tok
        })

    return out


# -------------------- LVLI classification helpers --------------------

def classify_event_rewards_child(edid: str) -> Optional[str]:
    """
    Map sublist EDIDs under *_LL_Quest_Rewards to display expands.
    """
    s = (edid or "")
    low = s.lower()
    if "headwear" in low:
        return "Headwear Rewards"
    if "recipes" in low:
        return "Plan Rewards"
    if "_quest_rewards_default" in low or low.endswith("_default"):
        return "Default Rewards"
    # some events may name it slightly differently
    if "quest_rewards_default" in low:
        return "Default Rewards"
    return None


def is_quest_rewards_router(edid: str) -> bool:
    low = (edid or "").lower()
    return ("_ll_quest_rewards" in low) and ("default" not in low) and ("headwear" not in low) and ("recipes" not in low)


def is_legendary_module_list(edid: str) -> bool:
    return "legendarymodule" in (edid or "").lower()


# -------------------- Drop chance resolvers --------------------

def resolve_entry_drop_chance(entry: LvliEntryRow, glob_index: Dict[str, GlobRow]) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Returns (dropChancePercent, technicalParts)
    - If chance_none_value exists: drop = 100 - value
    - Else if chance_none_global exists: use glob FLTV => interpret percent => drop = 100 - fltvPercent
    - Else: None
    """
    tech: Dict[str, Any] = {}
    if entry.chance_none_value is not None:
        tech["chanceNoneValue"] = entry.chance_none_value
        return chance_from_chance_none(entry.chance_none_value), tech

    if entry.chance_none_global:
        g = glob_index.get(entry.chance_none_global)
        tech["chanceNoneGlobal"] = {"formid": entry.chance_none_global, "edid": g.edid if g else "", "fltv": g.fltv if g else None}
        pct = percent_from_glob_fltv(g.fltv if g else None)
        if pct is None:
            return None, tech
        return chance_from_chance_none(pct), tech

    return None, tech


def extract_toggle_condition_state(cond: str) -> Optional[Dict[str, Any]]:
    """
    Detect a toggle-like condition:
      Subject.GetGlobalValue(XXXX [GLOB:...]) == 1
      Subject.GetGlobalValue(...) <> 1
      ... == 0
    Returns dict with glob formid and operator/value.
    """
    s = cond or ""
    # try to catch [GLOB:00xxxxxx]
    m = re.search(r"GetGlobalValue\([^\[]*\[GLOB:([0-9A-Fa-f]{8})\]\)\s*(==|<>|!=)\s*([01])", s)
    if not m:
        return None
    return {
        "glob": m.group(1).upper(),
        "op": m.group(2),
        "val": int(m.group(3))
    }


def human_toggle_state(op: str, val: int) -> str:
    """
    Convert toggle condition to words (no 0/1).
    We intentionally avoid guessing “On/Off” meaning beyond truthy.
    """
    if op in ("==",):
        return "Enabled" if val == 1 else "Disabled"
    if op in ("<>", "!=",):
        return "Not Enabled" if val == 1 else "Not Disabled"
    return "Conditional"


# -------------------- Builders for reward groups --------------------

def resolve_display_name(token: Dict[str, str], book_index: Dict[str, BookRow], armo_index: Dict[str, ArmoRow]) -> str:
    sig = (token.get("sig") or "").upper()
    fid = token.get("formid") or ""
    edid = token.get("edid") or ""

    # BOOK => use FULL
    if sig == "BOOK":
        b = book_index.get(fid)
        if b and b.full.strip():
            return b.full.strip()
        return pretty_name_from_edid(edid)

    # ARMO => use FULL
    if sig == "ARMO":
        a = armo_index.get(fid)
        if a and a.full.strip():
            return a.full.strip()
        return pretty_name_from_edid(edid)

    # LVLI should not render as item name here
    if sig == "LVLI":
        return pretty_name_from_edid(edid)

    return pretty_name_from_edid(edid)


def build_plans_pool(
    pool_lvli_formid: str,
    pool_drop_chance: Optional[float],
    lvli_list: Dict[str, LvliListRow],
    lvli_entries: Dict[str, List[LvliEntryRow]],
    glob_index: Dict[str, GlobRow],
    book_index: Dict[str, BookRow],
) -> Dict[str, Any]:
    """
    Plan Rewards: common pattern is uniform list of BOOK items.
    """
    items: List[Dict[str, Any]] = []
    entries = lvli_entries.get(pool_lvli_formid, [])
    count = len(entries)
    pool_pct = pool_drop_chance if pool_drop_chance is not None else 100.0

    per_item = (pool_pct / count) if count > 0 else None

    for e in entries:
        tok = parse_token(e.reference)
        if not tok:
            continue
        if is_ignored_edid(tok.get("edid") or ""):
            continue

        name = ""
        if (tok.get("sig") or "").upper() == "BOOK":
            b = book_index.get(tok["formid"])
            name = b.full.strip() if (b and b.full) else pretty_name_from_edid(tok.get("edid") or "")
        else:
            name = pretty_name_from_edid(tok.get("edid") or "")

        items.append({
            "name": name,
            "token": tok,
            "dropRate": round(per_item, 4) if per_item is not None else None,
            "collectible": True,
            "technical": {
                "sourceLvli": pool_lvli_formid,
                "conditions": e.conditions or ""
            }
        })

    # alphabetical
    items.sort(key=lambda x: (x.get("name") or "").lower())

    return {
        "poolChance": round(pool_pct, 4) if pool_pct is not None else None,
        "planCount": count,
        "perItemRate": round(per_item, 4) if per_item is not None else None,
        "items": items,
        "summaryText": f"{count} Plans. 100% chance to receive 1 plan. Each plan: 100/{count}." if count else "Plans list is empty."
    }


def build_uniform_pool(
    pool_lvli_formid: str,
    pool_drop_chance: Optional[float],
    lvli_entries: Dict[str, List[LvliEntryRow]],
    glob_index: Dict[str, GlobRow],
    book_index: Dict[str, BookRow],
    armo_index: Dict[str, ArmoRow]
) -> Dict[str, Any]:
    """
    Default Rewards / Common headwear pools often behave like uniform lists.
    We compute per-item = poolChance / entryCount.
    """
    entries = lvli_entries.get(pool_lvli_formid, [])
    count = len(entries)
    pool_pct = pool_drop_chance if pool_drop_chance is not None else 100.0
    per_item = (pool_pct / count) if count > 0 else None

    items: List[Dict[str, Any]] = []
    for e in entries:
        tok = parse_token(e.reference)
        if not tok:
            continue
        if is_ignored_edid(tok.get("edid") or ""):
            continue

        name = resolve_display_name(tok, book_index, armo_index)

        items.append({
            "name": name,
            "token": tok,
            "dropRate": round(per_item, 4) if per_item is not None else None,
            "collectible": True,
            "technical": {
                "sourceLvli": pool_lvli_formid,
                "conditions": e.conditions or ""
            }
        })

    items.sort(key=lambda x: (x.get("name") or "").lower())

    return {
        "poolChance": round(pool_pct, 4) if pool_pct is not None else None,
        "itemCount": count,
        "perItemRate": round(per_item, 4) if per_item is not None else None,
        "items": items,
        "summaryText": f"{count} items. Uniform roll." if count else "List is empty."
    }


def build_headwear(
    headwear_lvli_formid: str,
    headwear_pool_chance: Optional[float],
    lvli_entries: Dict[str, List[LvliEntryRow]],
    glob_index: Dict[str, GlobRow],
    armo_index: Dict[str, ArmoRow],
    book_index: Dict[str, BookRow],
) -> Dict[str, Any]:
    """
    Headwear Rewards:
      Headwear LVLI usually routes to rarity sublists:
        *_Common, *_UnCommon, *_Rare
      Each entry may have chance-none globals and toggle conditions.
    """
    head_pct = headwear_pool_chance if headwear_pool_chance is not None else 100.0
    entries = lvli_entries.get(headwear_lvli_formid, [])

    # collect rarity entries by their referenced LVLI EDID
    rarity_nodes: List[Dict[str, Any]] = []
    for e in entries:
        tok = parse_token(e.reference)
        if not tok or (tok.get("sig") or "").upper() != "LVLI":
            continue
        if is_ignored_edid(tok.get("edid") or ""):
            continue

        drop_pct, tech = resolve_entry_drop_chance(e, glob_index)
        toggle = extract_toggle_condition_state(e.conditions or "")

        rarity_nodes.append({
            "rarityEdid": tok.get("edid") or "",
            "rarityLvli": tok.get("formid") or "",
            "rarityName": "Rare Headwear" if "rare" in (tok.get("edid") or "").lower()
                         else "Uncommon Headwear" if "uncommon" in (tok.get("edid") or "").lower()
                         else "Common Headwear" if "common" in (tok.get("edid") or "").lower()
                         else "Headwear",
            "poolChance": drop_pct,
            "toggle": toggle,
            "technical": {
                **tech,
                "conditions": e.conditions or ""
            }
        })

    # group rarity nodes into scenario buckets if they share a toggle glob
    # If no toggle present, it goes into base scenario only.
    scenarios: Dict[str, Dict[str, Any]] = {
        "base": {"label": "Base", "rarities": []}
    }

    for n in rarity_nodes:
        t = n.get("toggle")
        if not t:
            scenarios["base"]["rarities"].append(n)
            continue

        key = t["glob"]
        on_key = f"toggle_{key}_on"
        off_key = f"toggle_{key}_off"

        # classify based on operator/value
        if t["op"] == "==" and t["val"] == 1:
            scenarios.setdefault(on_key, {"label": f"Toggle Enabled ({key})", "rarities": []})["rarities"].append(n)
        elif t["op"] == "==" and t["val"] == 0:
            scenarios.setdefault(off_key, {"label": f"Toggle Disabled ({key})", "rarities": []})["rarities"].append(n)
        elif t["op"] in ("<>", "!=") and t["val"] == 1:
            scenarios.setdefault(off_key, {"label": f"Toggle Disabled ({key})", "rarities": []})["rarities"].append(n)
        else:
            scenarios.setdefault(f"toggle_{key}_other", {"label": f"Toggle Conditional ({key})", "rarities": []})["rarities"].append(n)

    # Build final rarity sections (ABC: Common, Rare, Uncommon as requested: Common, Rare, Uncommon would be A/C? User said ABC order,
    # but they earlier said "Rare, Uncommon, Common" then corrected to ABC. We'll do Common, Rare, Uncommon? That is C, R, U.
    # ABC by section title: Common, Rare, Uncommon is correct alphabetically.
    section_order = ["Common Headwear", "Rare Headwear", "Uncommon Headwear"]

    def build_rarity_section(rarity_node: Dict[str, Any]) -> Dict[str, Any]:
        lvli_id = rarity_node["rarityLvli"]
        pool_pct = rarity_node["poolChance"] if rarity_node["poolChance"] is not None else None
        # items: uniform split of pool across entries
        entries2 = lvli_entries.get(lvli_id, [])
        count2 = len(entries2)
        per_item = (pool_pct / count2) if (pool_pct is not None and count2 > 0) else None

        items: List[Dict[str, Any]] = []
        for e2 in entries2:
            tok2 = parse_token(e2.reference)
            if not tok2:
                continue
            if is_ignored_edid(tok2.get("edid") or ""):
                continue
            name = resolve_display_name(tok2, book_index, armo_index)
            items.append({
                "name": name,
                "token": tok2,
                "dropRate": round(per_item, 4) if per_item is not None else None,
                "collectible": True,
                "technical": {"conditions": e2.conditions or "", "sourceLvli": lvli_id}
            })

        items.sort(key=lambda x: (x.get("name") or "").lower())

        return {
            "name": rarity_node["rarityName"],
            "poolChance": round(pool_pct, 4) if pool_pct is not None else None,
            "itemCount": count2,
            "perItemRate": round(per_item, 4) if per_item is not None else None,
            "items": items,
            "technical": rarity_node.get("technical") or {}
        }

    # Build sections per scenario
    built_scenarios: List[Dict[str, Any]] = []
    for skey, sval in scenarios.items():
        rarities = sval.get("rarities", [])
        # group by rarity name and keep order
        sections: List[Dict[str, Any]] = []
        by_name: Dict[str, Dict[str, Any]] = {}
        for rnode in rarities:
            by_name[rnode["rarityName"]] = rnode

        for nm in section_order:
            if nm in by_name:
                sections.append(build_rarity_section(by_name[nm]))

        # if base scenario empty but we have toggle scenarios, still include it as placeholder
        if not sections and skey == "base":
            continue

        built_scenarios.append({
            "key": skey,
            "label": sval.get("label") or skey,
            "sections": sections
        })

    return {
        "poolChance": round(head_pct, 4) if head_pct is not None else None,
        "scenarios": built_scenarios
    }


# -------------------- Main build --------------------

def main() -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    guide_path = TSV_DIR / "guide_index.tsv"
    quest_path = latest_match("QUEST_Export_*.tsv")
    gmrw_path = latest_match("GMRW_Export_*.tsv")
    glob_path = latest_match("GLOB_Export_*.tsv")
    lvli_list_path = latest_match("LVLI_Export_*_LVLI_List.tsv")
    lvli_entries_path = latest_match("LVLI_Export_*_LVLI_Entries.tsv")
    book_path = latest_match("BOOK_Export_*.tsv")
    armo_path = latest_match("ARMO_Export_*.tsv")

    missing = []
    for p, label in [
        (quest_path, "QUEST"),
        (gmrw_path, "GMRW"),
        (glob_path, "GLOB"),
        (lvli_list_path, "LVLI_List"),
        (lvli_entries_path, "LVLI_Entries"),
        (guide_path if guide_path.exists() else None, "guide_index.tsv"),
    ]:
        if not p:
            missing.append(label)

    if missing:
        raise FileNotFoundError(f"Missing required TSV(s): {', '.join(missing)}")

    quests = load_rows(quest_path)
    gmrw_rows = load_rows(gmrw_path)
    glob_rows = load_rows(glob_path)
    lvli_list_rows = load_rows(lvli_list_path)
    lvli_entries_rows = load_rows(lvli_entries_path)

    book_rows = load_rows(book_path) if book_path and book_path.exists() else []
    armo_rows = load_rows(armo_path) if armo_path and armo_path.exists() else []

    glob_index = build_glob_index(glob_rows)
    book_index = build_book_index(book_rows)
    armo_index = build_armo_index(armo_rows)
    lvli_list_index = build_lvli_list_index(lvli_list_rows)
    lvli_entries_index = build_lvli_entries_index(lvli_entries_rows)

    hubs_by_slug, descendants_by_hub = load_guide_index()

    # GMRW index by FormID
    gmrw_by_id: Dict[str, List[Dict[str, str]]] = {}
    for r in gmrw_rows:
        fid = (r.get("FormID") or "").upper()
        if fid:
            gmrw_by_id.setdefault(fid, []).append(r)

    # guide id->url map
    guide_rows = load_rows(guide_path)
    guide_url_by_id: Dict[str, str] = {}
    for gr in guide_rows:
        gid = (gr.get("id") or "").strip()
        url = (gr.get("url") or "").strip()
        if gid and url:
            guide_url_by_id[gid] = norm_path(url)

    # output structures
    out_events: Dict[str, Any] = {
        "generatedAt": now_iso_utc(),
        "schemaVersion": 1,
        "events": []
    }

    out_by_page: Dict[str, Any] = {
        "generatedAt": now_iso_utc(),
        "pagesByUrl": {}  # "/df/activities/.../": ["0049...","..."]
    }

    # helper: add mapping
    def map_event_to_page_urls(event_id: str, hub_row: Dict[str, str]) -> List[str]:
        urls: List[str] = []
        hub_id = (hub_row.get("id") or "").strip()
        if hub_id:
            u = guide_url_by_id.get(hub_id)
            if u:
                urls.append(u)
            for cid in descendants_by_hub.get(hub_id, []):
                cu = guide_url_by_id.get(cid)
                if cu:
                    urls.append(cu)
        return urls

    def add_event_to_pages(event_formid: str, urls: List[str]) -> None:
        for u in urls:
            bucket = out_by_page["pagesByUrl"].setdefault(u, {"eventFormIds": []})
            bucket["eventFormIds"].append(event_formid)

    # build events
    for q in quests:
        full = (q.get("FULL - Name") or "").strip()
        edid = (q.get("EDID") or "").strip()
        if is_ignored_edid(edid):
            continue

        low = full.lower()
        if not (low.startswith("event:") or low.startswith("activity:") or low.startswith("enclave activity:")):
            continue

        title = event_title_from_full(full)
        slug = slugify(title)

        warnings: List[str] = []

        # find hub guide entry by slug
        hub = hubs_by_slug.get(slug)
        page_urls: List[str] = map_event_to_page_urls(q.get("FormID","").upper(), hub) if hub else []
        guide_url = norm_path(hub.get("url")) if hub and hub.get("url") else ""

        # Party Crasher notices
        notices: List[Dict[str, Any]] = extract_party_crashers(q, glob_index)

        # Mutated detection (best-effort placeholder)
        # If you later export a specific column, this will start working without rewriting JS.
        mutated_hint = ""
        if "mutat" in edid.lower() or "mutat" in full.lower():
            mutated_hint = "This event can be a Mutated Public Event."
        if mutated_hint:
            notices.insert(0, {"type": "mutated", "text": mutated_hint})

        # gather GMRW refs from QUEST
        gmrw_ids: List[str] = []
        for k, v in q.items():
            if str(k).startswith("GMRWRef"):
                fid = parse_formid_ref(v or "")
                if fid:
                    gmrw_ids.append(fid)
        gmrw_ids = [x for i, x in enumerate(gmrw_ids) if x and x not in gmrw_ids[:i]]

        if not gmrw_ids:
            warnings.append("No GMRWRef fields found on QUEST.")

        base_rewards_rows: List[Dict[str, Any]] = []
        event_rewards_router_lvli: Optional[Dict[str, str]] = None  # token for *_LL_Quest_Rewards
        event_rewards_source_from_gmrw: Optional[str] = None

        # parse GMRW reward blocks
        for gmrw_fid in gmrw_ids:
            rr_list = gmrw_by_id.get(gmrw_fid, [])
            if not rr_list:
                warnings.append(f"GMRW not found: {gmrw_fid}")
                continue

            for rr in rr_list:
                # skip cut content
                if is_ignored_edid(rr.get("EDID") or ""):
                    continue

                # Base rewards: XP, caps, legendary pick + rank
                xp_glob = parse_token(rr.get("NAM7_XPGlobal") or "")
                caps_glob = parse_token(rr.get("NAM8_CapsGlobal") or "")
                currency_obj = parse_token(rr.get("QRCO_CurrencyObject") or "")
                leg_list = parse_token(rr.get("QRLI_LegendaryItemRewardList") or "")
                leg_rank = rr.get("QRLR_LegendaryItemRewardRank") or ""
                leg_rand = rr.get("QRRI_LegendaryRankRandom") or ""

                if xp_glob:
                    base_rewards_rows.append({
                        "name": "XP",
                        "dropRate": None,
                        "technical": {
                            "gmrw": gmrw_fid,
                            "rewardIndex": rr.get("RewardIndex"),
                            "xpGlobal": xp_glob
                        }
                    })

                if caps_glob or currency_obj:
                    base_rewards_rows.append({
                        "name": "Caps",
                        "dropRate": None,
                        "technical": {
                            "gmrw": gmrw_fid,
                            "rewardIndex": rr.get("RewardIndex"),
                            "capsGlobal": caps_glob,
                            "currencyObject": currency_obj
                        }
                    })

                if leg_list:
                    star_txt = f"{{{leg_rank}*}}" if str(leg_rank).strip() else ""
                    rand_txt = " (random rank)" if str(leg_rand).strip().lower() == "true" else ""
                    base_rewards_rows.append({
                        "name": f"Legendary Item {star_txt}{rand_txt}".strip(),
                        "dropRate": None,
                        "technical": {
                            "gmrw": gmrw_fid,
                            "rewardIndex": rr.get("RewardIndex"),
                            "legendaryList": leg_list,
                            "legendaryRank": leg_rank,
                            "legendaryRankRandom": leg_rand
                        }
                    })

                # RewardedItem token
                tok = parse_token(rr.get("RewardedItem") or "")
                if not tok:
                    continue

                # skip drifter/cut
                if is_ignored_edid(tok.get("edid") or ""):
                    continue

                if (tok.get("sig") or "").upper() == "LVLI":
                    # identify the router list for Event Rewards
                    if is_quest_rewards_router(tok.get("edid") or ""):
                        event_rewards_router_lvli = tok
                        event_rewards_source_from_gmrw = gmrw_fid

        # Build Event Rewards from router LVLI (Fasnacht-style)
        event_rewards: Dict[str, Any] = {
            "source": event_rewards_router_lvli,
            "default": None,
            "plans": None,
            "headwear": None
        }

        if not event_rewards_router_lvli:
            warnings.append("No *_LL_Quest_Rewards LVLI found in GMRW RewardedItem.")
        else:
            router_id = event_rewards_router_lvli["formid"]
            router_edid = event_rewards_router_lvli.get("edid") or ""
            router_entries = lvli_entries_index.get(router_id, [])

            # Router entries: Default/Headwear/Plans + LegendaryModules (bubble up)
            for e in router_entries:
                tok = parse_token(e.reference)
                if not tok or (tok.get("sig") or "").upper() != "LVLI":
                    continue
                if is_ignored_edid(tok.get("edid") or ""):
                    continue

                # Legendary Modules get bubbled into Base Rewards
                if is_legendary_module_list(tok.get("edid") or ""):
                    # module list itself can have GetRandomPercent logic; we do best-effort summary here
                    base_rewards_rows.append({
                        "name": "Legendary Module(s)",
                        "dropRate": None,
                        "technical": {
                            "source": "eventRewardsRouter",
                            "routerLvli": router_id,
                            "moduleLvli": tok,
                            "conditions": e.conditions or ""
                        }
                    })
                    continue

                label = classify_event_rewards_child(tok.get("edid") or "")
                if not label:
                    continue

                pool_pct, pool_tech = resolve_entry_drop_chance(e, glob_index)
                # Router-level toggles: stored and handled in JS as “scenario note”
                toggle = extract_toggle_condition_state(e.conditions or "")

                # Default Rewards
                if label == "Default Rewards":
                    event_rewards["default"] = {
                        "label": "Default Rewards",
                        "poolChance": round(pool_pct, 4) if pool_pct is not None else 100.0,
                        "toggle": toggle,
                        "lvli": tok,
                        "data": build_uniform_pool(tok["formid"], pool_pct, lvli_entries_index, glob_index, book_index, armo_index),
                        "technical": {"conditions": e.conditions or "", **pool_tech}
                    }

                # Plan Rewards (recipes)
                if label == "Plan Rewards":
                    event_rewards["plans"] = {
                        "label": "Plan Rewards",
                        "poolChance": round(pool_pct, 4) if pool_pct is not None else 100.0,
                        "toggle": toggle,
                        "lvli": tok,
                        "data": build_plans_pool(tok["formid"], pool_pct, lvli_list_index, lvli_entries_index, glob_index, book_index),
                        "technical": {"conditions": e.conditions or "", **pool_tech}
                    }

                # Headwear Rewards
                if label == "Headwear Rewards":
                    event_rewards["headwear"] = {
                        "label": "Headwear Rewards",
                        "poolChance": round(pool_pct, 4) if pool_pct is not None else 100.0,
                        "toggle": toggle,
                        "lvli": tok,
                        "data": build_headwear(tok["formid"], pool_pct, lvli_entries_index, glob_index, armo_index, book_index),
                        "technical": {"conditions": e.conditions or "", **pool_tech}
                    }

            # placeholders if missing
            if not event_rewards.get("default"):
                warnings.append("Event Rewards router has no Default Rewards sublist.")
            if not event_rewards.get("plans"):
                warnings.append("Event Rewards router has no Plan Rewards sublist.")
            if not event_rewards.get("headwear"):
                warnings.append("Event Rewards router has no Headwear Rewards sublist.")

        # Build event object
        event_formid = (q.get("FormID") or "").upper()

        ev_obj: Dict[str, Any] = {
            "quest": {
                "formid": event_formid,
                "edid": edid,
                "full": full,
                "title": title,
                "desc": q.get("DESC - Description") or "",
                "type": q.get("Quest Type") or "",
                "location": q.get("LNAM - Location") or "",
            },
            "guideUrl": guide_url,
            "pageUrls": page_urls,
            "notices": notices,
            "baseRewards": {
                "label": "Base Rewards",
                "rows": base_rewards_rows
            },
            "eventRewards": event_rewards,
            "technical": {
                "warnings": warnings,
                "sources": {
                    "questTsv": str(quest_path.name),
                    "gmrwTsv": str(gmrw_path.name),
                    "globTsv": str(glob_path.name),
                    "lvliListTsv": str(lvli_list_path.name),
                    "lvliEntriesTsv": str(lvli_entries_path.name),
                }
            }
        }

        # guarantee renderable
        if not base_rewards_rows and not event_rewards_router_lvli:
            ev_obj["technical"]["warnings"].append("No rewards resolved (base + event rewards empty).")

        out_events["events"].append(ev_obj)

        # map to pagesByUrl
        if page_urls:
            add_event_to_pages(event_formid, page_urls)
        elif guide_url:
            add_event_to_pages(event_formid, [guide_url])

    # Sort and dedupe mappings
    for u, bucket in out_by_page["pagesByUrl"].items():
        ids = bucket.get("eventFormIds") or []
        dedup = []
        for x in ids:
            if x not in dedup:
                dedup.append(x)
        bucket["eventFormIds"] = dedup

    # stable sort events by title
    out_events["events"].sort(key=lambda e: (e.get("quest", {}).get("title") or "").lower())

    OUT_EVENTS.write_text(json.dumps(out_events, indent=2), encoding="utf-8")
    OUT_BY_PAGE.write_text(json.dumps(out_by_page, indent=2), encoding="utf-8")

    patchlog = {
        "generatedAt": now_iso_utc(),
        "notes": [
            "Events Rewards JSON rebuilt (Fasnacht-style v1).",
            "Includes pagesByUrl mapping to prevent blank pages.",
            "Party Crashers pulled from QUEST TSV (PartyCrasher_* columns)."
        ]
    }
    OUT_PATCHLOG.write_text(json.dumps(patchlog, indent=2), encoding="utf-8")

    print("Wrote:", OUT_EVENTS)
    print("Wrote:", OUT_BY_PAGE)
    print("Wrote:", OUT_PATCHLOG)


if __name__ == "__main__":
    main()