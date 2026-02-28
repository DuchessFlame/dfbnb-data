# ==========================================================
# DF/BNB — Events Rewards Builder
# - Builds:
#     dist/events_rewards.json
#     dist/events_rewards_by_page.json
#     dist/patchlog_latest_df_events.json
# - Maps QUEST (Event:/Activity:/Enclave Activity:) -> Guide hub in guide_index.tsv
# - Expands GMRW RewardedItem LVLI using simplified RNG-76 math already in this script
# ==========================================================

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

TSV_DIR = Path("tsv")
DIST_DIR = Path("dist")

OUT_EVENTS = DIST_DIR / "events_rewards.json"
OUT_BY_PAGE = DIST_DIR / "events_rewards_by_page.json"
OUT_PATCHLOG = DIST_DIR / "patchlog_latest_df_events.json"

# -------------------- Helpers --------------------

def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f, delimiter="\t"))

def safe_load_rows(path: Optional[Path]) -> List[Dict[str, str]]:
    if not path or not path.exists():
        return []
    return load_rows(path)

def find_latest_glob(pattern: str) -> Optional[Path]:
    files = sorted(TSV_DIR.glob(pattern))
    return files[-1] if files else None

def parse_float(x):
    try:
        return float(str(x).strip())
    except:
        return None

def parse_ref_cell(cell: str) -> Optional[str]:
    if not cell:
        return None
    parts = str(cell).split(":")
    if len(parts) >= 2 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return parts[0].upper()
    return None

def parse_item_token(token: str) -> Optional[Dict[str, str]]:
    if not token:
        return None
    parts = str(token).split(":")
    # Expect: FORMID:EDID:SIG
    if len(parts) >= 3 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return {"formid": parts[0].upper(), "edid": parts[1], "sig": parts[2]}
    return None

def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def event_title_from_full(full: str) -> str:
    s = (full or "").strip()
    low = s.lower()
    for prefix in ["event:", "activity:", "enclave activity:"]:
        if low.startswith(prefix):
            return s[len(prefix):].strip()
    return s.strip()

# -------------------- LVLI Engine (existing simplified RNG-76 math) --------------------

def parse_flags_int(x):
    try:
        return int(str(x).strip() or "0")
    except:
        return 0

class LvliEngine:
    def __init__(self, lvli_list: Dict[str, Dict[str, str]], lvli_entries: Dict[str, List[Dict[str, str]]]):
        self.lvli_list = lvli_list
        self.lvli_entries = lvli_entries
        self._empty_cache: Dict[str, float] = {}
        self._stack: Set[str] = set()

    def list_empty_chance(self, fid: str) -> float:
        fid = (fid or "").upper()
        if not fid:
            return 1.0
        if fid in self._empty_cache:
            return self._empty_cache[fid]
        if fid in self._stack:
            return 0.0

        self._stack.add(fid)

        lrow = self.lvli_list.get(fid, {})
        _flags = parse_flags_int(lrow.get("LVLF_Flags"))

        list_cn = parse_float(lrow.get("LVCV_ChanceNoneValue")) or 0.0
        list_self = 1.0 - (list_cn / 100.0)

        entries = self.lvli_entries.get(fid, [])
        if not entries:
            self._empty_cache[fid] = 1.0
            self._stack.remove(fid)
            return 1.0

        base_weight = list_self / len(entries)

        empty_after = 1.0
        for e in entries:
            entry_cn = parse_float(e.get("LVOV_ChanceNoneValue")) or 0.0
            entry_presence = 1.0 - (entry_cn / 100.0)

            token = parse_item_token(e.get("LVLO_Reference") or "")
            sub_nonempty = 1.0

            if token and token.get("sig") == "LVLI":
                sub_empty = self.list_empty_chance(token["formid"])
                sub_nonempty = 1.0 - sub_empty

            chance = base_weight * entry_presence * sub_nonempty
            empty_after *= (1.0 - chance)

        result = (1.0 - list_self) + list_self * empty_after
        self._empty_cache[fid] = result
        self._stack.remove(fid)
        return result

    def expand(self, fid: str) -> List[Dict]:
        fid = (fid or "").upper()
        lrow = self.lvli_list.get(fid, {})
        list_cn = parse_float(lrow.get("LVCV_ChanceNoneValue")) or 0.0
        list_self = 1.0 - (list_cn / 100.0)

        entries = self.lvli_entries.get(fid, [])
        if not entries:
            return []

        base_weight = list_self / len(entries)
        out: List[Dict] = []

        for e in entries:
            entry_cn = parse_float(e.get("LVOV_ChanceNoneValue")) or 0.0
            entry_presence = 1.0 - (entry_cn / 100.0)

            token = parse_item_token(e.get("LVLO_Reference") or "")
            sub_nonempty = 1.0
            sub_empty = None

            if token and token.get("sig") == "LVLI":
                sub_empty = self.list_empty_chance(token["formid"])
                sub_nonempty = 1.0 - sub_empty

            chance = base_weight * entry_presence * sub_nonempty

            out.append({
                "item": token,
                "chance": chance,
                "entryChanceNone": entry_cn,
                "sublistEmpty": sub_empty
            })

        return out

# -------------------- Guide index mapping --------------------

def load_guide_index():
    gi_path = TSV_DIR / "guide_index.tsv"
    if not gi_path.exists():
        return {}, {}, {}

    rows = load_rows(gi_path)

    # Find hubs that represent an Event/Activity container.
    hubs_by_slug = {}
    children_by_hub = {}

    # Index all rows by id for traversal
    by_id = {r.get("id",""): r for r in rows if r.get("id")}

    for r in rows:
        if (r.get("template") or "").strip() != "category-hub":
            continue

        top = (r.get("topCategory") or "").strip().lower()
        # These are the event-ish buckets you’re using on site
        if top not in ("activities", "events", "seasonal events"):
            continue

        slug = (r.get("slug") or "").strip()
        if slug:
            hubs_by_slug[slug] = r

    # Collect hub -> descendants (direct children + grandchildren)
    for hub_slug, hub_row in hubs_by_slug.items():
        hub_id = hub_row.get("id")
        if not hub_id:
            continue

        kids: Set[str] = set()
        # direct children
        for r in rows:
            if (r.get("parentId") or "").strip() == hub_id:
                cid = (r.get("id") or "").strip()
                if cid:
                    kids.add(cid)

        # grandchildren (one level deep)
        for cid in list(kids):
            for r in rows:
                if (r.get("parentId") or "").strip() == cid:
                    gid = (r.get("id") or "").strip()
                    if gid:
                        kids.add(gid)

        children_by_hub[hub_id] = sorted(kids)

    return hubs_by_slug, children_by_hub, by_id

# -------------------- Group label logic (basic) --------------------

def group_from_lvli_name(edid: str) -> str:
    if not edid:
        return "Rewards"
    parts = edid.split("_")
    if "Rewards" in parts:
        idx = parts.index("Rewards")
        if idx + 1 < len(parts):
            return parts[idx + 1].replace("LL", "").replace("LS", "").replace("L", "")
    return "Rewards"

def clean_group_label(x: str) -> str:
    x = re.sub(r"(?<!^)([A-Z])", r" \1", x or "")
    return x.strip() or "Rewards"

# -------------------- Build --------------------

def main():
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    # Latest TSVs (specific patterns, not the old “LVLI_Export_* anything” trap)
    quest_path = find_latest_glob("QUEST_Export_*.tsv")
    gmrw_path = find_latest_glob("GMRW_Export_*.tsv")
    lvli_list_path = find_latest_glob("LVLI_Export_*_LVLI_List.tsv")
    lvli_entries_path = find_latest_glob("LVLI_Export_*_LVLI_Entries.tsv")
    book_path = find_latest_glob("BOOK_Export_*.tsv")  # optional

    if not quest_path:
        raise FileNotFoundError("Missing tsv/QUEST_Export_*.tsv")
    if not gmrw_path:
        raise FileNotFoundError("Missing tsv/GMRW_Export_*.tsv")
    if not lvli_list_path:
        raise FileNotFoundError("Missing tsv/LVLI_Export_*_LVLI_List.tsv")
    if not lvli_entries_path:
        raise FileNotFoundError("Missing tsv/LVLI_Export_*_LVLI_Entries.tsv")

    quests = load_rows(quest_path)
    gmrw_rows = load_rows(gmrw_path)

    # Map GMRW by FormID for fast lookup
    gmrw_by_id: Dict[str, List[Dict[str, str]]] = {}
    for r in gmrw_rows:
        fid = (r.get("FormID") or "").upper()
        if fid:
            gmrw_by_id.setdefault(fid, []).append(r)

    # BOOK map (optional)
    books = {}
    for r in safe_load_rows(book_path):
        fid = (r.get("FormID") or "").upper()
        if fid:
            books[fid] = r

    # LVLI maps
    lvli_list: Dict[str, Dict[str, str]] = {}
    for r in load_rows(lvli_list_path):
        fid = (r.get("LVLI_FormID") or "").upper()
        if fid:
            lvli_list[fid] = r

    lvli_entries: Dict[str, List[Dict[str, str]]] = {}
    for r in load_rows(lvli_entries_path):
        fid = (r.get("LVLI_FormID") or "").upper()
        if fid:
            lvli_entries.setdefault(fid, []).append(r)

    engine = LvliEngine(lvli_list, lvli_entries)

    hubs_by_slug, children_by_hub, _by_id = load_guide_index()

    out_events = {
        "generatedAt": now_iso(),
        "events": []
    }

    out_by_page = {
        "generatedAt": now_iso(),
        "pages": {}
    }

    # helper to append an event to a page bucket
    def add_to_page(page_id: str, event_obj: Dict):
        if not page_id:
            return
        bucket = out_by_page["pages"].setdefault(page_id, {"events": []})
        bucket["events"].append(event_obj)

    for q in quests:
        full = (q.get("FULL - Name") or "").strip()
        low = full.lower()

        if not low.startswith(("event:", "activity:", "enclave activity:")):
            continue

        title = event_title_from_full(full)
        hub_slug = slugify(title)

        event_obj = {
            "quest": {
                "formid": (q.get("FormID") or "").upper(),
                "edid": q.get("EDID") or "",
                "full": full,
                "title": title,
                "desc": q.get("DESC - Description") or "",
                "type": q.get("Quest Type") or "",
                "location": q.get("LNAM - Location") or ""
            },
            "groups": {}
        }

        # Collect all GMRWRef* fields that exist (not limited to 10)
        gmrw_ids: List[str] = []
        for k, v in q.items():
            if not k:
                continue
            if str(k).startswith("GMRWRef"):
                fid = parse_ref_cell(v or "")
                if fid:
                    gmrw_ids.append(fid)

        # Also tolerate alternate column names (just in case)
        for k, v in q.items():
            if not k:
                continue
            if str(k).lower().startswith("gmrwref") and str(k) not in q:
                fid = parse_ref_cell(v or "")
                if fid:
                    gmrw_ids.append(fid)

        # Dedup preserve order
        seen = set()
        gmrw_ids = [x for x in gmrw_ids if not (x in seen or seen.add(x))]

        for gmrw_fid in gmrw_ids:
            for rr in gmrw_by_id.get(gmrw_fid, []):

                # Component probability from GetRandomPercent >= N
                conditions = rr.get("Conditions") or ""
                component_prob = 1.0
                m = re.search(r"GetRandomPercent\s*>=\s*(\d+)", conditions)
                if m:
                    n = float(m.group(1))
                    component_prob = (100.0 - n) / 100.0

                # BASE payouts: XP (keep existing behavior)
                xp = rr.get("NAM7_XPGlobal")
                if xp:
                    event_obj["groups"].setdefault("Base Rewards", []).append({
                        "displayName": "XP",
                        "dropRate": component_prob * 100.0,
                        "technical": {"xpGlobal": xp, "gmrw": gmrw_fid}
                    })

                token = parse_item_token(rr.get("RewardedItem") or "")
                if not token:
                    continue

                if token["sig"] == "LVLI":
                    expanded = engine.expand(token["formid"])
                    group_key = clean_group_label(group_from_lvli_name(token.get("edid") or ""))

                    for e in expanded:
                        item = e.get("item")
                        if not item:
                            continue

                        display = item.get("edid") or item.get("formid") or "—"

                        if item.get("sig") == "BOOK":
                            b = books.get((item.get("formid") or "").upper())
                            if b and b.get("FULL"):
                                display = b.get("FULL")

                        final_rate = component_prob * float(e.get("chance") or 0.0) * 100.0

                        event_obj["groups"].setdefault(group_key, []).append({
                            "displayName": display,
                            "dropRate": final_rate,
                            "technical": {
                                "gmrw": gmrw_fid,
                                "lvli": token["formid"],
                                "entryChanceNone": e.get("entryChanceNone"),
                                "sublistEmpty": e.get("sublistEmpty")
                            }
                        })

        out_events["events"].append(event_obj)

        # Map to hub + all descendant pages (checklist, guide, etc)
        hub_row = hubs_by_slug.get(hub_slug)
        if hub_row:
            hub_id = (hub_row.get("id") or "").strip()
            add_to_page(hub_id, event_obj)

            for child_id in children_by_hub.get(hub_id, []):
                add_to_page(child_id, event_obj)

    OUT_EVENTS.write_text(json.dumps(out_events, indent=2), encoding="utf-8")
    OUT_BY_PAGE.write_text(json.dumps(out_by_page, indent=2), encoding="utf-8")

    patchlog = {
        "generatedAt": now_iso(),
        "notes": [
            "Auto-generated from TSV exports.",
            "Includes per-page mapping from guide_index.tsv (hub + descendants)."
        ]
    }
    OUT_PATCHLOG.write_text(json.dumps(patchlog, indent=2), encoding="utf-8")

    print("Wrote", OUT_EVENTS)
    print("Wrote", OUT_BY_PAGE)
    print("Wrote", OUT_PATCHLOG)

if __name__ == "__main__":
    main()