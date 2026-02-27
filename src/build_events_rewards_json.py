import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Any

# ----------------------------
# CONFIG
# ----------------------------
TSV_DIR = Path("tsv")
OUT_EVENTS = Path("dist/events_rewards.json")
OUT_BY_PAGE = Path("dist/events_rewards_by_page.json")
OUT_PATCHLOG = Path("dist/patchlog_latest_df_events.json")

MAX_GMRW_REFS = 10  # must match QUEST export

FALLBACK_LIST_CHANCENONE = 0.0
FALLBACK_LIST_SELF_CHANCE = 1.0

# ----------------------------
# Helpers
# ----------------------------
def pick_latest_tsv(prefix: str) -> Path:
    files = sorted(TSV_DIR.glob(f"{prefix}*.tsv"))
    if not files:
        raise FileNotFoundError(f"Missing {prefix}*.tsv in {TSV_DIR}")
    return files[-1]

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

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
def load_tsv_rows(path: Path) -> List[dict]:
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            rows.append(row)
    return rows

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
# Guide index loader + matching
# ----------------------------
def load_guides(path: Path):
    guides = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            url  = (row.get("url") or "").strip()
            if not url:
                continue
            guides.append({
                "title": (row.get("title") or "").strip(),
                "menuTitle": (row.get("menuTitle") or "").strip(),
                "slug": (row.get("slug") or "").strip(),
                "url": url,
                "template": (row.get("template") or "").strip(),
                "nodeType": (row.get("nodeType") or "").strip(),
                "k": norm(row.get("title") or "") or norm(row.get("menuTitle") or "") or norm(row.get("slug") or "")
            })
    return guides

def _strip_kind_prefix(full_name: str) -> str:
    # remove Event:/Activity:/Enclave Activity:
    s = (full_name or "").strip()
    s = re.sub(r"^(event|activity|enclave\s+activity)\s*:\s*", "", s, flags=re.I).strip()
    return s

def _score_base(g: dict) -> int:
    """
    Base scoring: checklist beats guide, guide beats category-hub.
    Also prefer reward-checklist URLs specifically.
    """
    t = (g.get("template") or "").lower()
    url = (g.get("url") or "").lower()
    title = norm(g.get("title") or "")
    menu = norm(g.get("menuTitle") or "")
    slug = norm(g.get("slug") or "")

    if "reward-checklist" in url or "rewards-checklist" in url:
        return 500
    if "checklist" in t:
        return 420

    if (
        ("reward checklist" in title) or ("rewards checklist" in title) or
        ("reward checklist" in menu)  or ("rewards checklist" in menu)  or
        ("reward checklist" in slug)  or ("rewards checklist" in slug)
    ):
        return 380

    if "guide" in t or (" guide" in title) or (" guide" in menu) or slug.endswith(" guide"):
        return 220

    return 100

def _route_bonus(route: str, url: str) -> int:
    """
    Routing preference layer.
    IMPORTANT: Seasonal Events wins if it exists, even though they are Public Events in game.
    """
    u = (url or "").lower()

    if route == "seasonal_event":
        if "/df/seasonal-events/" in u:
            return 1000
        # don't hard-penalize others, just lower preference
        return 0

    if route == "public_event":
        if "/df/public-events/" in u:
            return 800
        # seasonal-events still valid for public events if that’s where the page lives
        if "/df/seasonal-events/" in u:
            return 700
        return 0

    if route == "activity":
        if "/df/activities/" in u:
            return 800
        return 0

    # generic event fallback
    if route == "event":
        # allow seasonal/public/events/activities as fallback; no special boost
        return 0

    return 0

def classify_route(quest_full_name: str, quest_type: str, guides: List[dict]) -> str:
    """
    Decide which URL family we should prefer.
    - Seasonal: detected via guide_index availability (since QUEST doesn't reliably label Seasonal)
    - Public: quest type contains "Public Event"
    - Activity: FULL starts with Activity: or Enclave Activity:
    - Else: event
    """
    full = (quest_full_name or "").strip()
    qtype = (quest_type or "").lower()

    is_activity = bool(re.match(r"^(activity|enclave\s+activity)\s*:", full, flags=re.I))
    is_event = bool(re.match(r"^event\s*:", full, flags=re.I))
    is_public = ("public event" in qtype)

    name_key = norm(_strip_kind_prefix(full))
    # Seasonal detection: if we have a matching checklist under /df/seasonal-events/
    seasonal_exists = False
    if name_key:
        for g in guides:
            if not g.get("k"):
                continue
            if g["k"] != name_key and (g["k"] not in name_key) and (name_key not in g["k"]):
                continue
            url = (g.get("url") or "").lower()
            if "/df/seasonal-events/" in url and ("checklist" in (g.get("template") or "").lower() or "reward-checklist" in url):
                seasonal_exists = True
                break

    if seasonal_exists:
        return "seasonal_event"
    if is_public and is_event:
        return "public_event"
    if is_activity:
        return "activity"
    if is_event:
        return "event"
    return "event"

def match_guide_url(guides: List[dict], quest_full_name: str, route: str) -> str:
    """
    Match by quest name, but enforce routing preferences:
    - Seasonal Events if possible (route bonus)
    - Public Events pages if public_event
    - Activities pages if activity
    """
    name = _strip_kind_prefix(quest_full_name)
    key = norm(name)
    if not key:
        return ""

    candidates = [g for g in guides if g.get("k") and (g["k"] == key or g["k"] in key or key in g["k"])]
    if not candidates:
        return ""

    def final_score(g: dict) -> int:
        base = _score_base(g)
        bonus = _route_bonus(route, g.get("url") or "")
        exact = 50 if (g.get("k") == key) else 0
        # prefer actual pages
        is_page = 20 if (g.get("nodeType") or "").lower() == "page" else 0
        return base + bonus + exact + is_page

    candidates.sort(key=final_score, reverse=True)
    return candidates[0].get("url") or ""

# ----------------------------
# LVLI engine (unchanged from your current script)
# (Kept intact so we don't break your existing chance logic)
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
    rows = load_tsv_rows(path)
    out = {}
    for r in rows:
        fid = (r.get("FormID") or r.get("formid") or "").upper()
        if not fid:
            continue
        val = None
        for k in ("FLTV - Value", "Value", "GLOB_Value", "Float", "DATA - Value"):
            if k in r and str(r[k]).strip():
                val = parse_float(r[k])
                break
        if val is None:
            for kk, vv in r.items():
                if vv and re.fullmatch(r"-?\d+(\.\d+)?", str(vv).strip()):
                    val = parse_float(vv)
                    break
        if val is None:
            continue
        out[fid] = val
    return out

@dataclass
class Curve:
    pts: List[Tuple[float, float]]

def load_curves(path: Path) -> Dict[str, Curve]:
    rows = load_tsv_rows(path)
    curves: Dict[str, List[Tuple[float, float]]] = {}
    headers = rows[0].keys() if rows else []
    has_xy = any(str(h).lower() in ("x", "y") for h in headers)

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
            pts = []
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
    for i in range(1, len(pts)):
        x0, y0 = pts[i-1]
        x1, y1 = pts[i]
        if x <= x1:
            if x1 == x0:
                return y0
            t = (x - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)
    return pts[-1][1]

RND_PATTERNS = [
    (re.compile(r"GetRandomPercent\s*(>=|>)\s*(\d+)", re.I), "ge"),
    (re.compile(r"GetRandomPercent\s*(<=|<)\s*(\d+)", re.I), "le"),
]

def condition_probability(conds: List[str]) -> Tuple[float, List[dict], List[str]]:
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
            hard.append(cs)
    return p, prob_notes, hard

def extract_entry_conditions(entry_row: dict) -> List[str]:
    conds = []
    for i in range(1, 11):
        k = f"Cond{i}"
        if k in entry_row and (entry_row[k] or "").strip():
            conds.append(entry_row[k].strip())
    return conds

def resolve_int_value(value_str: str) -> Optional[int]:
    v = parse_float(value_str)
    if v is None:
        return None
    return int(round(v))

def resolve_percent_value(value_str: str) -> Optional[float]:
    v = parse_float(value_str)
    if v is None:
        return None
    return max(0.0, min(100.0, float(v)))

def resolve_from_global(formid: str, glob: Dict[str, float]) -> Optional[float]:
    if not formid:
        return None
    if re.fullmatch(r"[0-9A-Fa-f]{8}", formid):
        return glob.get(formid.upper())
    return None

def resolve_from_curve(curve_id: str, indexer: float, curves: Dict[str, Curve]) -> Optional[float]:
    if not curve_id:
        return None
    if not re.fullmatch(r"[0-9A-Fa-f]{8}", curve_id):
        return None
    c = curves.get(curve_id.upper())
    if not c or not c.pts:
        return None
    return float(curve_lookup(c, float(indexer)))

def resolve_list_chancenone_pct(lrow: dict, glob: Dict[str, float], curves: Dict[str, Curve]) -> float:
    g = (lrow.get("LVLG_ChanceNoneGlobal") or "").strip()
    c = (lrow.get("LVCT_ChanceNoneCurve") or "").strip()
    v = (lrow.get("LVCV_ChanceNoneValue") or "").strip()

    gv = resolve_from_global(g, glob)
    if gv is not None:
        return max(0.0, min(100.0, float(gv)))

    if c:
        idx = float(gv) if gv is not None else 0.0
        cv = resolve_from_curve(c, idx, curves)
        if cv is not None:
            return max(0.0, min(100.0, float(cv)))

    vv = resolve_percent_value(v)
    if vv is not None:
        return vv

    return 0.0

def resolve_entry_chancenone_pct(erow: dict, glob: Dict[str, float], curves: Dict[str, Curve]) -> float:
    g = (erow.get("LVOG_ChanceNoneGlobal") or "").strip()
    c = (erow.get("LVOC_ChanceNoneCurve") or "").strip()
    v = (erow.get("LVOV_ChanceNoneValue") or "").strip()

    gv = resolve_from_global(g, glob)
    if gv is not None:
        return max(0.0, min(100.0, float(gv)))

    if c:
        idx = float(gv) if gv is not None else 0.0
        cv = resolve_from_curve(c, idx, curves)
        if cv is not None:
            return max(0.0, min(100.0, float(cv)))

    vv = resolve_percent_value(v)
    if vv is not None:
        return vv

    return 0.0

def resolve_list_maximum(lrow: dict, glob: Dict[str, float], curves: Dict[str, Curve]) -> int:
    g = (lrow.get("LVMG_MaxGlobal") or "").strip()
    c = (lrow.get("LVMT_MaxCurve") or "").strip()
    v = (lrow.get("LVMV_MaxValue") or "").strip()

    gv = resolve_from_global(g, glob)
    if gv is not None:
        return max(0, int(round(gv)))

    if c:
        idx = float(gv) if gv is not None else 0.0
        cv = resolve_from_curve(c, idx, curves)
        if cv is not None:
            return max(0, int(round(cv)))

    vv = resolve_int_value(v)
    if vv is not None:
        return max(0, vv)

    return 0

def parse_flags_int(s: str) -> int:
    try:
        return int((s or "").strip() or "0")
    except:
        return 0

@dataclass
class LvliEntryChance:
    entry_ref: str
    base_item_token: str
    chance: float
    prob_notes: List[dict]
    hard_conditions: List[str]
    entry_chancenone_pct: float
    sublist_empty_chance: Optional[float]
    apriori_chance: float

class Rng76LvliEngine:
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
            return 0.0

        self._stack.add(lvli_formid)

        lrow = self.lvli_list.get(lvli_formid, {})
        flags = parse_flags_int(lrow.get("LVLF_Flags") or "0")

        list_cn = resolve_list_chancenone_pct(lrow, self.glob, self.curves) / 100.0
        list_self = (1.0 - list_cn)

        entries = self.lvli_entries.get(lvli_formid, [])
        if not entries:
            out = 1.0
            self._stack.remove(lvli_formid)
            self._empty_cache[lvli_formid] = out
            return out

        is_all = bool(flags & (1 << 2))
        is_first = bool(flags & (1 << 6))

        entry_self = []
        entry_sub_nonempty = []
        for erow in entries:
            cn_pct = resolve_entry_chancenone_pct(erow, self.glob, self.curves)
            entry_presence = 1.0 - (cn_pct / 100.0)

            conds = extract_entry_conditions(erow)
            cond_mult, _, _ = condition_probability(conds)

            apriori = list_self * entry_presence * cond_mult

            token = (erow.get("LVLO_Reference") or "").strip()
            sub_nonempty = 1.0
            if token:
                it = parse_item_token(token)
                if it and it.get("sig") == "LVLI":
                    sub_empty = self.list_empty_chance(it["formid"])
                    sub_nonempty = 1.0 - sub_empty

            entry_self.append(apriori)
            entry_sub_nonempty.append(sub_nonempty)

        if is_all or is_first:
            empty_after = 1.0
            for i in range(len(entries)):
                ch = entry_self[i] * entry_sub_nonempty[i]
                empty_after *= (1.0 - ch)
            out = max(0.0, min(1.0, (1.0 - list_self) + list_self * empty_after))
        else:
            n = len(entries)
            uniform = list_self / n if n else 0.0
            total = 0.0
            for i in range(n):
                total += uniform * (entry_self[i] / list_self if list_self > 0 else 0.0) * entry_sub_nonempty[i]
            empty_after = max(0.0, min(1.0, 1.0 - total))
            out = max(0.0, min(1.0, (1.0 - list_self) + list_self * empty_after))

        self._stack.remove(lvli_formid)
        self._empty_cache[lvli_formid] = out
        return out

    def expand_lvli(self, lvli_formid: str) -> List[LvliEntryChance]:
        lvli_formid = (lvli_formid or "").upper()
        lrow = self.lvli_list.get(lvli_formid, {})
        flags = parse_flags_int(lrow.get("LVLF_Flags") or "0")

        list_cn = resolve_list_chancenone_pct(lrow, self.glob, self.curves) / 100.0
        list_self = (1.0 - list_cn)

        entries = self.lvli_entries.get(lvli_formid, [])
        out: List[LvliEntryChance] = []
        if not entries:
            return out

        is_all = bool(flags & (1 << 2))
        is_first = bool(flags & (1 << 6))

        base_weight = list_self if (is_all or is_first) else (list_self / len(entries))

        for erow in entries:
            cn_pct = resolve_entry_chancenone_pct(erow, self.glob, self.curves)
            entry_presence = 1.0 - (cn_pct / 100.0)
            conds = extract_entry_conditions(erow)
            cond_mult, prob_notes, hard = condition_probability(conds)

            apriori = base_weight * entry_presence * cond_mult

            token = (erow.get("LVLO_Reference") or "").strip()
            item = parse_item_token(token) if token else None

            sub_empty = None
            sub_nonempty = 1.0
            if item and item.get("sig") == "LVLI":
                sub_empty = self.list_empty_chance(item["formid"])
                sub_nonempty = 1.0 - sub_empty

            chance = max(0.0, min(1.0, apriori * sub_nonempty))

            out.append(LvliEntryChance(
                entry_ref=(erow.get("LVLO_Reference") or token or ""),
                base_item_token=token,
                chance=chance,
                prob_notes=prob_notes,
                hard_conditions=hard,
                entry_chancenone_pct=cn_pct,
                sublist_empty_chance=sub_empty,
                apriori_chance=apriori
            ))

        return out

# ----------------------------
# LVLI loaders
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
# QUEST loader (Event/Activity including Enclave Activity)
# ----------------------------
def load_event_quests(path: Path):
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            full = (row.get("FULL - Name") or "").strip()
            if not full:
                continue
            if not (
                full.lower().startswith("event:")
                or full.lower().startswith("activity:")
                or full.lower().startswith("enclave activity:")
            ):
                continue
            rows.append(row)
    return rows

# ----------------------------
# Patchlog (simple, page-safe)
# ----------------------------
def _git_show_json(path: str) -> Optional[dict]:
    import subprocess
    try:
        out = subprocess.check_output(["git", "show", f"HEAD^:{path}"], stderr=subprocess.DEVNULL)
        return json.loads(out.decode("utf-8"))
    except Exception:
        return None

def build_patchlog(prev: Optional[dict], curr: dict) -> dict:
    prev_events = { (e.get("quest") or {}).get("formid",""): e for e in (prev.get("events") if prev else []) }
    curr_events = { (e.get("quest") or {}).get("formid",""): e for e in (curr.get("events") or []) }

    added = [k for k in curr_events.keys() if k and k not in prev_events]
    removed = [k for k in prev_events.keys() if k and k not in curr_events]
    changed: List[str] = []

    for k in curr_events.keys():
        if k in prev_events:
            a = prev_events[k]
            b = curr_events[k]
            # compare key routing/display fields only (keeps patchlog stable)
            fields = ("guideUrl", "route")
            aq = a.get("quest") or {}
            bq = b.get("quest") or {}
            qfields = ("edid","full","type","location")
            if any(a.get(f) != b.get(f) for f in fields) or any(aq.get(f) != bq.get(f) for f in qfields):
                changed.append(k)

    return {
        "generatedAt": now_iso(),
        "counts": {
            "prev": len(prev_events),
            "curr": len(curr_events),
            "added": len(added),
            "removed": len(removed),
            "changed": len(changed),
        },
        "addedFormIds": added[:500],
        "removedFormIds": removed[:500],
        "changedFormIds": changed[:500],
    }

# ----------------------------
# Build JSON
# ----------------------------
def main():
    QUEST_TSV = pick_latest_tsv("QUEST_Export_")
    GMRW_TSV  = pick_latest_tsv("GMRW_Export_")

    lvli_list_candidates = sorted(TSV_DIR.glob("LVLI_Export_*_LVLI_List.tsv"))
    lvli_entries_candidates = sorted(TSV_DIR.glob("LVLI_Export_*_LVLI_Entries.tsv"))
    lvli_refs_candidates = sorted(TSV_DIR.glob("LVLI_Export_*_LVLI_Refs.tsv"))
    if not lvli_list_candidates or not lvli_entries_candidates:
        raise FileNotFoundError("Missing LVLI_Export_*_LVLI_List.tsv or LVLI_Export_*_LVLI_Entries.tsv in /tsv")

    LVLI_LIST_TSV = lvli_list_candidates[-1]
    LVLI_ENTRIES_TSV = lvli_entries_candidates[-1]
    LVLI_REFS_TSV = lvli_refs_candidates[-1] if lvli_refs_candidates else None

    GUIDE_INDEX_TSV = (TSV_DIR / "guide_index.tsv")
    if not GUIDE_INDEX_TSV.exists():
        raise FileNotFoundError("Missing tsv/guide_index.tsv")

    GLOB_TSV = pick_latest_tsv("GLOB_Export_")

    curv_points_candidates = sorted(TSV_DIR.glob("CURV_Export_*POINTS*.tsv"))
    if curv_points_candidates:
        CURV_TSV = curv_points_candidates[-1]
    else:
        CURV_TSV = pick_latest_tsv("CURV_Export_")

    guides = load_guides(GUIDE_INDEX_TSV)
    gmrw_by_id = load_gmrw(GMRW_TSV)
    quests = load_event_quests(QUEST_TSV)

    lvli_list = load_lvli_list(LVLI_LIST_TSV)
    lvli_entries = load_lvli_entries_all(LVLI_ENTRIES_TSV)

    glob_vals = load_glob_values(GLOB_TSV)
    curves = load_curves(CURV_TSV)

    engine = Rng76LvliEngine(lvli_list, lvli_entries, glob_vals, curves)

    out = {
        "generatedAt": now_iso(),
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

    by_page: Dict[str, str] = {}  # url -> questFormId

    for q in quests:
        q_formid = (q.get("FormID") or "").upper()
        q_edid   = q.get("EDID") or ""
        q_full   = q.get("FULL - Name") or ""
        q_desc   = q.get("DESC - Description") or ""
        q_type   = q.get("Quest Type") or ""
        q_lnam   = q.get("LNAM - Location") or ""

        route = classify_route(q_full, q_type, guides)
        guide_url = match_guide_url(guides, q_full, route)

        if guide_url:
            by_page[guide_url] = q_formid

        event_obj = {
            "route": route,          # seasonal_event | public_event | activity | event
            "guideUrl": guide_url,   # exact page url from guide_index
            "quest": {
                "formid": q_formid,
                "edid": q_edid,
                "full": q_full,
                "desc": q_desc,
                "type": q_type,
                "location": q_lnam,
            },
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

                    # UI fields (filled later by site pipeline if you want)
                    "imageUrl": "",
                    "releaseYear": "",
                    "tradeable": "",

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

                    for e in expanded:
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
                        rr["item"] = parse_item_token(e.base_item_token) or rr["item"]
                        event_obj["groups"]["bonus" if is_bonus else bucket_hint].append(rr)
                else:
                    event_obj["groups"]["bonus" if is_bonus else bucket_hint].append(base_reward)

        event_obj["groups"] = {k: v for k, v in event_obj["groups"].items() if v}
        out["events"].append(event_obj)

    # diagnostics
    events_out = out.get("events", [])
    cnt_total = len(events_out)
    cnt_blank = sum(1 for e in events_out if not (e.get("guideUrl") or "").strip())
    print(f"guideUrl stats: total={cnt_total} blank={cnt_blank}")

    OUT_EVENTS.parent.mkdir(parents=True, exist_ok=True)
    OUT_EVENTS.write_text(json.dumps(out, indent=2), encoding="utf-8")

    OUT_BY_PAGE.write_text(json.dumps({
        "generatedAt": out["generatedAt"],
        "byPage": by_page
    }, indent=2), encoding="utf-8")

    # patchlog feed (so your patch log loader has something real to fetch)
    prev = _git_show_json("dist/events_rewards.json")
    patchlog = build_patchlog(prev, out)
    OUT_PATCHLOG.write_text(json.dumps(patchlog, indent=2), encoding="utf-8")

    print(f"Wrote {OUT_EVENTS}")
    print(f"Wrote {OUT_BY_PAGE}")
    print(f"Wrote {OUT_PATCHLOG}")

if __name__ == "__main__":
    main()