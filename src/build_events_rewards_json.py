import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

# ----------------------------
# CONFIG
# ----------------------------
TSV_DIR = Path("tsv")
OUT_JSON = Path("dist/events_rewards.json")

MAX_GMRW_REFS = 10  # must match QUEST export

FALLBACK_LIST_CHANCENONE = 0.0   # when list chanceNone can't be resolved from LVLG/LVCT
FALLBACK_LIST_SELF_CHANCE = 1.0  # if list conditions aren't exported we can't reduce this

# ----------------------------
# Helpers
# ----------------------------
def pick_latest_tsv(prefix: str) -> Path:
    # deterministic: lexicographically last file
    files = sorted(TSV_DIR.glob(f"{prefix}*.tsv"))
    if not files:
        raise FileNotFoundError(f"Missing {prefix}*.tsv in {TSV_DIR}")
    return files[-1]

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_ref_cell(cell: str):
    cell = (cell or "").strip()
    if not cell:
        return None, None
    parts = cell.split(":")
    if len(parts) >= 2 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return parts[0].upper(), parts[1]
    if re.fullmatch(r"[0-9A-Fa-f]{8}", cell):
        return cell.upper(), None
    return None, None

def parse_item_token(token: str):
    token = (token or "").strip()
    if not token:
        return None
    parts = token.split(":")
    if len(parts) >= 3 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return {"formid": parts[0].upper(), "edid": parts[1], "sig": parts[2]}
    if len(parts) == 2 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return {"formid": parts[0].upper(), "edid": parts[1], "sig": None}
    return {"raw": token}

def best_bucket_from_edid(edid: str):
    t = (edid or "").lower()
    if "rare" in t:
        return "rare"
    if "uncommon" in t:
        return "uncommon"
    if "common" in t:
        return "common"
    return "default"

# ----------------------------
# TSV loaders
# ----------------------------
def load_tsv_dict(path: Path, key_col: str) -> Dict[str, dict]:
    d = {}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            k = (row.get(key_col) or "").upper()
            if not k:
                continue
            d[k] = row
    return d

def load_tsv_rows(path: Path) -> List[dict]:
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            rows.append(row)
    return rows

# ----------------------------
# Guide index
# ----------------------------
def load_guides(path: Path):
    guides = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            title = row.get("title") or ""
            menu = row.get("menuTitle") or ""
            url  = row.get("url") or ""
            slug = row.get("slug") or ""
            guides.append({
                "title": title,
                "menuTitle": menu,
                "slug": slug,
                "url": url,
                "k": norm(title) or norm(menu) or norm(slug)
            })
    return guides

def match_guide_url(guides, quest_full_name: str):
    name = quest_full_name.strip()
    name = re.sub(r"^(event|activity)\s*:\s*", "", name, flags=re.I).strip()
    key = norm(name)
    if not key:
        return ""

    for g in guides:
        if g["k"] == key:
            return g["url"]

    for g in guides:
        if g["k"] and (g["k"] in key or key in g["k"]):
            return g["url"]

    return ""

# ----------------------------
# GLOB + CURV resolution
# ----------------------------
def parse_float(s: str) -> Optional[float]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except:
        return None

def load_glob_values(path: Path) -> Dict[str, float]:
    # Expect columns including FormID and a value field.
    # We will try common names in order.
    rows = load_tsv_rows(path)
    out = {}
    for r in rows:
        fid = (r.get("FormID") or r.get("formid") or "").upper()
        if not fid:
            continue
        val = None
        for k in ("FLTV - Value", "Value", "GLOB_Value", "Float", "DATA - Value"):
            if k in r and r[k].strip():
                val = parse_float(r[k])
                break
        if val is None:
            # last-ditch: find any numeric-looking column
            for kk, vv in r.items():
                if vv and re.fullmatch(r"-?\d+(\.\d+)?", vv.strip()):
                    val = parse_float(vv)
                    break
        if val is None:
            continue
        out[fid] = val
    return out

@dataclass
class Curve:
    # list of (x, y) sorted by x
    pts: List[Tuple[float, float]]

def load_curves(path: Path) -> Dict[str, Curve]:
    rows = load_tsv_rows(path)
    curves: Dict[str, List[Tuple[float, float]]] = {}
    # We don’t assume a specific schema; we attempt two common layouts:
    # A) columns: CurveFormID, X, Y (multiple rows per curve)
    # B) columns: FormID + PointCount + X1 Y1 X2 Y2 ...
    headers = rows[0].keys() if rows else []
    has_xy = any(h.lower() in ("x", "y") for h in headers)

    for r in rows:
        fid = (r.get("FormID") or r.get("CurveFormID") or r.get("CURV_FormID") or "").upper()
        if not fid:
            continue

        if has_xy and (r.get("X") or r.get("x")) and (r.get("Y") or r.get("y")):
            x = parse_float(r.get("X") or r.get("x"))
            y = parse_float(r.get("Y") or r.get("y"))
            if x is None or y is None:
                continue
            curves.setdefault(fid, []).append((x, y))
        else:
            # flattened columns
            pts = []
            # Try PointCount or infer pairs X1/Y1...
            # Collect any columns like X1, Y1, X2, Y2
            xs = {}
            ys = {}
            for k, v in r.items():
                m = re.fullmatch(r"X(\d+)", (k or "").strip(), flags=re.I)
                if m:
                    xs[int(m.group(1))] = parse_float(v)
                m = re.fullmatch(r"Y(\d+)", (k or "").strip(), flags=re.I)
                if m:
                    ys[int(m.group(1))] = parse_float(v)
            for idx in sorted(set(xs.keys()) & set(ys.keys())):
                if xs[idx] is None or ys[idx] is None:
                    continue
                pts.append((xs[idx], ys[idx]))
            if pts:
                curves.setdefault(fid, []).extend(pts)

    out = {}
    for fid, pts in curves.items():
        pts2 = sorted(list(set(pts)), key=lambda t: t[0])
        out[fid] = Curve(pts=pts2)
    return out

def curve_lookup(curve: Curve, x: float) -> float:
    pts = curve.pts
    if not pts:
        return 0.0
    if x <= pts[0][0]:
        return pts[0][1]
    if x >= pts[-1][0]:
        return pts[-1][1]
    # linear interpolation between neighbors
    for i in range(1, len(pts)):
        x0, y0 = pts[i-1]
        x1, y1 = pts[i]
        if x <= x1:
            if x1 == x0:
                return y0
            t = (x - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)
    return pts[-1][1]

# ----------------------------
# Condition handling (best-effort)
# ----------------------------
RND_PATTERNS = [
    # >= / >
    (re.compile(r"GetRandomPercent\s*(>=|>)\s*(\d+)", re.I), "ge"),
    # <= / <
    (re.compile(r"GetRandomPercent\s*(<=|<)\s*(\d+)", re.I), "le"),
]

def condition_probability(conds: List[str]) -> Tuple[float, List[dict], List[str]]:
    """
    Returns:
      probMultiplier (0..1),
      probNotes: list of {type, expr, p},
      hardConditions: list of strings (non-probabilistic)
    We treat unknown conditions as hard gates (state dependent), prob multiplier only from GetRandomPercent.
    """
    p = 1.0
    prob_notes = []
    hard = []

    for c in (conds or []):
        cs = (c or "").strip()
        if not cs:
            continue

        matched = False
        for rx, kind in RND_PATTERNS:
            m = rx.search(cs)
            if m:
                n = float(m.group(2))
                if kind == "ge":
                    pc = max(0.0, min(1.0, (100.0 - n) / 100.0))
                else:
                    pc = max(0.0, min(1.0, n / 100.0))
                p *= pc
                prob_notes.append({"type": "GetRandomPercent", "expr": cs, "p": pc})
                matched = True
                break

        if not matched:
            # Hard gate we cannot convert to probability without player state
            hard.append(cs)

    return p, prob_notes, hard

def extract_entry_conditions(entry_row: dict) -> List[str]:
    conds = []
    for i in range(1, 11):
        k = f"Cond{i}"
        if k in entry_row and (entry_row[k] or "").strip():
            conds.append(entry_row[k].strip())
    return conds

# ----------------------------
# LVLI chance engine (best-effort based on TSV fields)
# ----------------------------
def resolve_percent_value(value_str: str) -> Optional[float]:
    v = parse_float(value_str)
    if v is None:
        return None
    # treat as percent already
    return max(0.0, min(100.0, v))

def resolve_chancenone_from_entry(row: dict, glob: Dict[str, float], curves: Dict[str, Curve]) -> float:
    """
    Entry chanceNone in percent.
    Priority: LVOG (global) -> LVOC (curve, indexed by LVOG global value) -> LVOV (value) -> 0
    """
    g = (row.get("LVOG_ChanceNoneGlobal") or "").strip()
    c = (row.get("LVOC_ChanceNoneCurve") or "").strip()
    v = (row.get("LVOV_ChanceNoneValue") or "").strip()

    # global
    if g and re.fullmatch(r"[0-9A-Fa-f]{8}", g):
        gv = glob.get(g.upper())
        if gv is not None:
            return max(0.0, min(100.0, float(gv)))

    # curve: indexer is LVOG global value per rng-76
    if c and re.fullmatch(r"[0-9A-Fa-f]{8}", c):
        curve = curves.get(c.upper())
        if curve:
            idx = 0.0
            if g and re.fullmatch(r"[0-9A-Fa-f]{8}", g):
                gv = glob.get(g.upper())
                if gv is not None:
                    idx = float(gv)
            return max(0.0, min(100.0, float(curve_lookup(curve, idx))))

    # value
    vv = resolve_percent_value(v)
    if vv is not None:
        return vv

    return 0.0

def resolve_list_chancenone_from_list(row: dict, glob: Dict[str, float], curves: Dict[str, Curve]) -> float:
    """
    List chanceNone in percent.
    Priority per your TSV: LVLG global -> LVCT curve (indexed by LVLG global value) -> fallback
    """
    g = (row.get("LVLG_ChanceNoneGlobal") or "").strip()
    c = (row.get("LVCT_ChanceNoneCurve") or "").strip()

    if g and re.fullmatch(r"[0-9A-Fa-f]{8}", g):
        gv = glob.get(g.upper())
        if gv is not None:
            return max(0.0, min(100.0, float(gv)))

    if c and re.fullmatch(r"[0-9A-Fa-f]{8}", c):
        curve = curves.get(c.upper())
        if curve:
            idx = 0.0
            if g and re.fullmatch(r"[0-9A-Fa-f]{8}", g):
                gv = glob.get(g.upper())
                if gv is not None:
                    idx = float(gv)
            return max(0.0, min(100.0, float(curve_lookup(curve, idx))))

    return FALLBACK_LIST_CHANCENONE

def parse_flags_int(s: str) -> int:
    try:
        return int((s or "").strip() or "0")
    except:
        return 0

@dataclass
class LvliEntryChance:
    entry_ref: str
    base_item_token: str
    chance: float  # 0..1 chance "drops at least once" from this entry path
    prob_notes: List[dict]
    hard_conditions: List[str]
    entry_chancenone_pct: float
    sublist_empty_chance: Optional[float]

class LvliEngine:
    def __init__(self, lvli_list: Dict[str, dict], lvli_entries: Dict[str, List[dict]], glob: Dict[str, float], curves: Dict[str, Curve]):
        self.lvli_list = lvli_list
        self.lvli_entries = lvli_entries
        self.glob = glob
        self.curves = curves
        self._empty_cache: Dict[str, float] = {}
        self._stack: Set[str] = set()

    def list_empty_chance(self, lvli_formid: str) -> float:
        lvli_formid = (lvli_formid or "").upper()
        if not lvli_formid:
            return 1.0
        if lvli_formid in self._empty_cache:
            return self._empty_cache[lvli_formid]
        if lvli_formid in self._stack:
            # cycle guard: assume empty chance 0 (non-empty) to avoid infinite recursion
            return 0.0

        self._stack.add(lvli_formid)

        lrow = self.lvli_list.get(lvli_formid, {})
        flags = parse_flags_int(lrow.get("LVLF_Flags") or "0")

        list_cn = resolve_list_chancenone_from_list(lrow, self.glob, self.curves) / 100.0
        list_self = FALLBACK_LIST_SELF_CHANCE * (1.0 - list_cn)  # best-effort

        entries = self.lvli_entries.get(lvli_formid, [])

        # Compute entry chances (not aggregated) depending on mode (best-effort)
        entry_chances = []
        if flags & (1 << 2):  # All
            # max not available -> assume max=0 (independent collection)
            for e in entries:
                ch = self._entry_path_chance(list_self, e)
                entry_chances.append(ch)
            # empty if none of the entries yield something
            empty = 1.0
            for ch in entry_chances:
                empty *= (1.0 - ch)
            out = max(0.0, min(1.0, empty))
        elif flags & (1 << 6):  # First entry where conditions match
            # We only model probabilistic gates; unknown conditions treated as "pass" but recorded.
            # Probability: P(first success occurs) => cascade of (fail prior) * (success current)
            empty = 1.0
            for e in entries:
                ch = self._entry_path_chance(list_self, e)
                # chance current selected = ch * (prob no previous selected)
                empty *= (1.0 - ch)
            out = max(0.0, min(1.0, empty))
        else:
            # Non-All uniform pick (best-effort)
            n = len(entries)
            if n == 0:
                out = 1.0
            else:
                uniform = list_self / n
                total = 0.0
                for e in entries:
                    total += self._entry_path_chance(uniform, e)
                out = max(0.0, min(1.0, 1.0 - total))

        self._stack.remove(lvli_formid)
        self._empty_cache[lvli_formid] = out
        return out

    def _entry_path_chance(self, base_weight: float, entry_row: dict) -> float:
        cn_pct = resolve_chancenone_from_entry(entry_row, self.glob, self.curves)
        entry_presence = 1.0 - (cn_pct / 100.0)

        conds = extract_entry_conditions(entry_row)
        cond_mult, _, _ = condition_probability(conds)

        # sublist handling: if entry item token is LVLI, incorporate its non-empty chance
        token = (entry_row.get("LVLO_Reference") or entry_row.get("LVLO") or entry_row.get("Item") or "").strip()
        sub_nonempty = 1.0
        if token:
            item = parse_item_token(token)
            if item and item.get("sig") == "LVLI":
                sub_empty = self.list_empty_chance(item["formid"])
                sub_nonempty = 1.0 - sub_empty

        ch = base_weight * entry_presence * cond_mult * sub_nonempty
        return max(0.0, min(1.0, ch))

    def expand_lvli(self, lvli_formid: str) -> List[LvliEntryChance]:
        lvli_formid = (lvli_formid or "").upper()
        lrow = self.lvli_list.get(lvli_formid, {})
        flags = parse_flags_int(lrow.get("LVLF_Flags") or "0")

        list_cn = resolve_list_chancenone_from_list(lrow, self.glob, self.curves) / 100.0
        list_self = FALLBACK_LIST_SELF_CHANCE * (1.0 - list_cn)

        entries = self.lvli_entries.get(lvli_formid, [])
        out: List[LvliEntryChance] = []

        if flags & (1 << 2):  # All
            base_weight = list_self
            for e in entries:
                out.append(self._expand_entry(base_weight, e))
        else:
            # Uniform pick best-effort for non-all and first modes
            n = len(entries) if entries else 1
            base_weight = list_self / n
            for e in entries:
                out.append(self._expand_entry(base_weight, e))

        return out

    def _expand_entry(self, base_weight: float, entry_row: dict) -> LvliEntryChance:
        cn_pct = resolve_chancenone_from_entry(entry_row, self.glob, self.curves)
        entry_presence = 1.0 - (cn_pct / 100.0)

        conds = extract_entry_conditions(entry_row)
        cond_mult, prob_notes, hard = condition_probability(conds)

        token = (entry_row.get("LVLO_Reference") or entry_row.get("LVLO") or entry_row.get("Item") or "").strip()
        item = parse_item_token(token) if token else None

        sub_empty = None
        sub_nonempty = 1.0
        if item and item.get("sig") == "LVLI":
            sub_empty = self.list_empty_chance(item["formid"])
            sub_nonempty = 1.0 - sub_empty

        chance = base_weight * entry_presence * cond_mult * sub_nonempty
        chance = max(0.0, min(1.0, chance))

        return LvliEntryChance(
            entry_ref=(entry_row.get("LVLO_Reference") or token or ""),
            base_item_token=(token or ""),
            chance=chance,
            prob_notes=prob_notes,
            hard_conditions=hard,
            entry_chancenone_pct=cn_pct,
            sublist_empty_chance=sub_empty
        )

# ----------------------------
# Load GMRW grouped by FormID
# ----------------------------
def load_gmrw(path: Path):
    g = {}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            fid = (row.get("FormID") or "").upper()
            if not fid:
                continue
            g.setdefault(fid, []).append(row)
    return g

# ----------------------------
# Load quests (Event/Activity only)
# ----------------------------
def load_event_quests(path: Path):
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            full = (row.get("FULL - Name") or "").strip()
            if not full:
                continue
            if not (full.lower().startswith("event:") or full.lower().startswith("activity:")):
                continue
            rows.append(row)
    return rows

# ----------------------------
# Load LVLI list + entries (FULL, not just needed)
# ----------------------------
def load_lvli_list(path: Path):
    d = {}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            fid = (row.get("LVLI_FormID") or row.get("FormID") or "").upper()
            if not fid:
                continue
            d[fid] = row
    return d

def load_lvli_entries_all(path: Path):
    entries: Dict[str, List[dict]] = {}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            fid = (row.get("LVLI_FormID") or "").upper()
            if not fid:
                continue
            entries.setdefault(fid, []).append(row)
    return entries

# ----------------------------
# Build JSON
# ----------------------------
def main():
    # auto-pick latest TSVs (no more hard-coded March)
    QUEST_TSV = pick_latest_tsv("QUEST_Export_")
    GMRW_TSV  = pick_latest_tsv("GMRW_Export_")
    LVLI_LIST_TSV    = pick_latest_tsv("LVLI_Export_")
    # explicitly prefer exact names if present
    lvli_list_candidates = sorted(TSV_DIR.glob("LVLI_Export_*_LVLI_List.tsv"))
    lvli_entries_candidates = sorted(TSV_DIR.glob("LVLI_Export_*_LVLI_Entries.tsv"))
    lvli_refs_candidates = sorted(TSV_DIR.glob("LVLI_Export_*_LVLI_Refs.tsv"))
    if not lvli_list_candidates or not lvli_entries_candidates:
        raise FileNotFoundError("Missing LVLI_Export_*_LVLI_List.tsv or LVLI_Export_*_LVLI_Entries.tsv in /tsv")

    LVLI_LIST_TSV = lvli_list_candidates[-1]
    LVLI_ENTRIES_TSV = lvli_entries_candidates[-1]
    LVLI_REFS_TSV = lvli_refs_candidates[-1] if lvli_refs_candidates else None

    GUIDE_INDEX_TSV  = (TSV_DIR / "guide_index.tsv")
    if not GUIDE_INDEX_TSV.exists():
        raise FileNotFoundError("Missing tsv/guide_index.tsv")

    GLOB_TSV = pick_latest_tsv("GLOB_Export_")
    CURV_TSV = pick_latest_tsv("CURV_Export_")

    guides = load_guides(GUIDE_INDEX_TSV)
    gmrw_by_id = load_gmrw(GMRW_TSV)
    quests = load_event_quests(QUEST_TSV)

    lvli_list = load_lvli_list(LVLI_LIST_TSV)
    lvli_entries = load_lvli_entries_all(LVLI_ENTRIES_TSV)

    glob_vals = load_glob_values(GLOB_TSV)
    curves = load_curves(CURV_TSV)

    engine = LvliEngine(lvli_list, lvli_entries, glob_vals, curves)

    out = {
        "generatedAt": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": {
            "questTsv": QUEST_TSV.name,
            "gmrwTsv": GMRW_TSV.name,
            "lvliListTsv": LVLI_LIST_TSV.name,
            "lvliEntriesTsv": LVLI_ENTRIES_TSV.name,
            "lvliRefsTsv": LVLI_REFS_TSV.name if LVLI_REFS_TSV else "",
            "globTsv": GLOB_TSV.name,
            "curvTsv": CURV_TSV.name,
            "guideIndexTsv": GUIDE_INDEX_TSV.name,
        },
        "events": []
    }

    for q in quests:
        q_formid = (q.get("FormID") or "").upper()
        q_edid   = q.get("EDID") or ""
        q_full   = q.get("FULL - Name") or ""
        q_desc   = q.get("DESC - Description") or ""
        q_type   = q.get("Quest Type") or ""
        q_lnam   = q.get("LNAM - Location") or ""

        guide_url = match_guide_url(guides, q_full)

        event_obj = {
            "quest": {
                "formid": q_formid,
                "edid": q_edid,
                "full": q_full,
                "desc": q_desc,
                "type": q_type,
                "location": q_lnam,
            },
            "guideUrl": guide_url,
            "groups": { "default": [], "common": [], "uncommon": [], "rare": [], "bonus": [] }
        }

        for i in range(MAX_GMRW_REFS):
            cell = q.get(f"GMRWRef{i}") or ""
            gmrw_id, gmrw_edid = parse_ref_cell(cell)
            if not gmrw_id:
                continue

            rows = gmrw_by_id.get(gmrw_id, [])
            if not rows:
                continue

            for rrow in rows:
                rewarded_item = parse_item_token(rrow.get("RewardedItem") or "")
                bucket_hint = best_bucket_from_edid((rewarded_item or {}).get("edid") or gmrw_edid)

                base_reward = {
                    "source": {
                        "gmrwFormid": gmrw_id,
                        "gmrwEdid": gmrw_edid or (rrow.get("EDID") or ""),
                    },
                    "rewardIndex": rrow.get("RewardIndex"),
                    "rewardedItemIndex": rrow.get("RewardedItemIndex"),
                    "item": rewarded_item,
                    "count": rrow.get("RewardedItemCount"),
                    "xpGlobal": rrow.get("NAM7_XPGlobal") or "",
                    "capsGlobal": rrow.get("NAM8_CapsGlobal") or "",
                    "conditions": (rrow.get("Conditions") or "").strip(),
                    "conditionGlobs": (rrow.get("ConditionGlobs") or "").strip(),

                    # UI fields
                    "imageUrl": "",
                    "releaseYear": "",
                    "tradeable": "",

                    # will be filled for LVLI expansions
                    "dropRate": "",
                    "dropConditions": [],
                    "dropProbNotes": [],
                    "lvli": None,
                }

                is_bonus = False
                if (base_reward["conditions"] or base_reward["conditionGlobs"]) and (base_reward["xpGlobal"] or base_reward["capsGlobal"]):
                    is_bonus = True

                if rewarded_item and rewarded_item.get("sig") == "LVLI":
                    fid = rewarded_item["formid"]

                    expanded = engine.expand_lvli(fid)

                    # Expand into separate reward rows (Titles-style will show these as rows)
                    for idx, e in enumerate(expanded):
                        rr = dict(base_reward)
                        rr["lvli"] = {
                            "lvliFormid": fid,
                            "lvliFlags": lvli_list.get(fid, {}).get("LVLF_Flags", ""),
                            "entryRef": e.entry_ref,
                            "entryChanceNonePct": e.entry_chancenone_pct,
                            "sublistEmptyChance": e.sublist_empty_chance,
                        }
                        rr["dropRate"] = round(e.chance * 100.0, 6)
                        rr["dropProbNotes"] = e.prob_notes
                        rr["dropConditions"] = e.hard_conditions
                        # Replace the "item" with the actual entry item token (if any)
                        rr["item"] = parse_item_token(e.base_item_token) or rr["item"]
                        event_obj["groups"]["bonus" if is_bonus else bucket_hint].append(rr)
                else:
                    # fixed reward (xp/caps/item etc)
                    event_obj["groups"]["bonus" if is_bonus else bucket_hint].append(base_reward)

        # prune empty groups
        event_obj["groups"] = {k: v for k, v in event_obj["groups"].items() if v}
        out["events"].append(event_obj)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_JSON}")

if __name__ == "__main__":
    main()
