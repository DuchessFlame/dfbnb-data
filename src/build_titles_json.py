from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

# ============================================================
# DF/BNB Titles JSON Builder (Camp + Player) — v2
#
# Implements the agreed rules:
# - Challenges:
#   - HasCompletedChallenge -> 100%, "Complete the {CNAM} Challenge {FULL}"
#   - IsTrueForConditionForm(Challenge_*_ConditionForm) -> resolve to CHAL by EDID, same output
# - Quests:
#   - GetQuestCompleted -> 100%, Complete the quest "{QuestName}".
#   - GetNumTimesCompletedQuest -> 100%, if N>1 add "{N} times."
# - Entitlements:
#   - Community_* -> "Awarded through a Bethesda community event or promotion." (DropRate N/A)
#   - SCORE_MiniSeason_* -> 100%, "Claim from the Mini Season - {Name}" (drop leading YYYY_)
#   - SCORE_S#:
#       Camp: Framed Art if EndOfSeasonArt (or "Framed" in FULL), else Gameboard (includes CorkBoard)
#       Player: "Unlock via the Season {#} - {SeasonName} Scoreboard."
#   - ATX_* -> "Can be purchased with certain bundles from the Atom Shop." (DropRate N/A)
# - COBJ proxy (any condition with [COBJ:]):
#   - how: "Complete the Event: X" / "Complete the Activity: Y" using GMRW.ParentQuest quoted label
#   - drop: GLOB first (100 - FLTV), LVLI fallback (100 - LVOV_ChanceNone)
# - Tradeable:
#   - Default non-tradeable; BOOK row containing keyword NonPlayerTradeable => non-tradeable; else tradeable
#
# Outputs:
#   titles_camp.json, titles_player.json, titles_patchlog.json, titles_manifest.json
# ============================================================

CUT_PREFIXES = ("DEL", "POST", "CUT", "ZZZ", "ZZZZ")
CUT_SUFFIXES = ("_COPY01",)

RE_HAS_ENTITLEMENT = re.compile(r"\bHasEntitlement\(", re.IGNORECASE)
RE_HAS_COMPLETED_CHAL = re.compile(r"\bHasCompletedChallenge\(", re.IGNORECASE)
RE_IS_TRUE_CNDF = re.compile(r"\bIsTrueForConditionForm\(", re.IGNORECASE)

RE_QUEST_COMPLETED = re.compile(r"\bGetQuestCompleted\(", re.IGNORECASE)
RE_NUM_TIMES_COMPLETED = re.compile(r"\bGetNumTimesCompletedQuest\(", re.IGNORECASE)

RE_SCORE_SEASON = re.compile(r"\bSCORE[_-]?S(\d+)(?:\b|_)", re.IGNORECASE)
RE_MINISEASON = re.compile(r"\bSCORE_MiniSeason(?:\b|_)", re.IGNORECASE)
RE_ATX = re.compile(r"\bATX_", re.IGNORECASE)
RE_COMMUNITY = re.compile(r"\bCommunity_", re.IGNORECASE)

RE_FORM_REF = re.compile(r"\[([A-Z]{4}):([0-9A-F]{8})\]", re.IGNORECASE)
RE_QUOTED = re.compile(r'"([^"]+)"')
RE_COBJ_REF = re.compile(r"(?:\[COBJ:|COBJ:)([0-9A-F]{8})(?:\]?)", re.IGNORECASE)


def now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def today_ymd_utc() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")


def load_release_overrides(tsv_root: Optional[str]) -> Dict[str, str]:
    """
    Optional overrides file:
      tsv/title_release_overrides.json

    Accepts either:
      - "YYYY" (interpreted as "YYYY-01-01")
      - "YYYY-MM-DD"

    Format examples:
      { "00ABCDEF": "2024", "00112233": "2020-04-14" }

    Keys are FormID (8 hex). Values become YYYY-MM-DD.
    """
    if not tsv_root:
        return {}
    path = os.path.join(tsv_root, "title_release_overrides.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        out: Dict[str, str] = {}
        for k, v in (data or {}).items():
            kk = (str(k) or "").strip().upper()
            vv = (str(v) or "").strip()

            if not re.fullmatch(r"[0-9A-F]{8}", kk):
                continue

            # Allow YYYY -> YYYY-01-01
            if re.fullmatch(r"\d{4}", vv):
                out[kk] = f"{vv}-01-01"
                continue

            # Allow full date
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", vv):
                out[kk] = vv
                continue

        return out
    except Exception:
        return {}


def load_previous_release_dates(dist_path: str) -> Dict[str, str]:
    """
    Read an existing dist JSON (titles_camp.json / titles_player.json) and return:
      { FORMID8: "YYYY-MM-DD" }
    """
    if not dist_path or not os.path.exists(dist_path):
        return {}
    try:
        with open(dist_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items") or []
        out: Dict[str, str] = {}
        for it in items:
            fid = (str(it.get("formId") or "")).strip().upper()
            rd = (str(it.get("releaseDate") or "")).strip()
            if re.fullmatch(r"[0-9A-F]{8}", fid) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", rd):
                out[fid] = rd
        return out
    except Exception:
        return {}


def safe_int(s: str, default: int = 0) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return default


def safe_float(s: str, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(str(s).strip())
    except Exception:
        return default


def read_tsv_rows(path: str) -> List[Dict[str, str]]:
    """
    Read TSV rows and add 'alias' keys so downstream logic can use stable names.

    Your March 2026 exports use prefixed headers like COBJ_FormID, LVLI_FormID, etc.
    The generator logic expects plain 'FormID'/'EDID' in many places.

    This function preserves original keys AND adds these aliases when missing:
      - FormID: from <TYPE>_FormID (COBJ_FormID, LVLI_FormID, BOOK_FormID, etc)
      - EDID:   from <TYPE>_EDID (COBJ_EDID, LVLI_EDID, etc)
      - FULL:   from <TYPE>_FULL when present
    """
    alias_formid_keys = (
        "CMPT_FormID", "PLYT_FormID", "BOOK_FormID", "COBJ_FormID",
        "GLOB_FormID", "GMRW_FormID", "LVLI_FormID", "CHAL_FormID",
        "ENTM_FormID",
    )
    alias_edid_keys = (
        "CMPT_EDID", "PLYT_EDID", "BOOK_EDID", "COBJ_EDID",
        "GLOB_EDID", "GMRW_EDID", "LVLI_EDID", "CHAL_EDID",
        "ENTM_EDID",
    )
    alias_full_keys = (
        "CMPT_FULL", "PLYT_FULL", "BOOK_FULL", "COBJ_FULL",
        "GLOB_FULL", "GMRW_FULL", "LVLI_FULL", "CHAL_FULL",
        "ENTM_FULL",
    )

    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        out: List[Dict[str, str]] = []
        for row in reader:
            r = dict(row)

            # FormID alias
            if not (r.get("FormID") or "").strip():
                for k in alias_formid_keys:
                    v = (r.get(k) or "").strip()
                    if v:
                        r["FormID"] = v
                        break

            # EDID alias
            if not (r.get("EDID") or "").strip():
                for k in alias_edid_keys:
                    v = (r.get(k) or "").strip()
                    if v:
                        r["EDID"] = v
                        break

            # FULL alias (not always used, but handy)
            if not (r.get("FULL") or "").strip():
                for k in alias_full_keys:
                    v = (r.get(k) or "").strip()
                    if v:
                        r["FULL"] = v
                        break

                                    # Condition aliases (CondCount, Cond1..CondN)
            # Some exports use prefixed headers like PLYT_CondCount / PLYT_Cond1 etc.
            if not (r.get("CondCount") or "").strip():
                for k, v in r.items():
                    if k.endswith("_CondCount"):
                        vv = (v or "").strip()
                        if vv:
                            r["CondCount"] = vv
                            break

            # Alias any <TYPE>_CondN -> CondN
            for k, v in list(r.items()):
                m = re.match(r"^[A-Z]+_Cond(\d+)$", str(k))
                if not m:
                    continue
                n = m.group(1)
                base_key = f"Cond{n}"
                if not (r.get(base_key) or "").strip():
                    vv = (v or "").strip()
                    if vv:
                        r[base_key] = vv

            out.append(r)
        return out

def merge_rows_by_key(row_sets: List[List[Dict[str, str]]], key_field: str) -> List[Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}
    for rows in row_sets:
        for r in rows:
            k = (r.get(key_field) or "").strip()
            if not k:
                continue
            if k not in merged:
                merged[k] = dict(r)
            else:
                merged[k].update({kk: vv for kk, vv in r.items() if vv is not None})
    return list(merged.values())


def _autofill_paths(tsv_root: Optional[str], provided: Optional[List[str]], patterns: List[str]) -> List[str]:
    if provided:
        return provided
    if not tsv_root:
        return []
    hits: List[str] = []
    for pat in patterns:
        hits.extend(glob.glob(os.path.join(tsv_root, pat), recursive=True))
    return sorted(set(hits))


def starts_cut(edid: str) -> bool:
    e = (edid or "").strip().upper()
    if any(e.startswith(p) for p in CUT_PREFIXES):
        return True
    return any(e.endswith(s) for s in CUT_SUFFIXES)


def extract_conditions(row: Dict[str, str]) -> List[str]:
    c = safe_int(row.get("CondCount", "0"))
    out: List[str] = []
    for i in range(1, c + 1):
        v = (row.get(f"Cond{i}") or "").strip()
        if v:
            out.append(v)
    return out

def seasons_map(seasons_path: Optional[str]) -> Dict[int, str]:
    if not seasons_path or not os.path.exists(seasons_path):
        return {}

    # Robust parse: handle TSV, CSV, and UTF-8 BOM.
    # Your file SHOULD be TSV, but CI or editors sometimes save it differently.
    try:
        with open(seasons_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
            head = f.readline()
            f.seek(0)

            # Detect delimiter from header line
            delim = "\t" if "\t" in head else ("," if "," in head else "\t")

            reader = csv.DictReader(f, delimiter=delim)
            rows = [dict(r) for r in reader]
    except Exception:
        # Fallback to existing TSV reader
        rows = read_tsv_rows(seasons_path)

    m: Dict[int, str] = {}
    for r in rows:
        sn = (r.get("SeasonNumber") or r.get("Season") or r.get("Number") or "").strip()
        name = (r.get("SeasonName") or r.get("Name") or r.get("ScoreboardName") or "").strip()
        n = safe_int(sn, 0)
        if n and name:
            m[n] = name

    return m

def _norm_dds_path(p: str) -> str:
    """
    Normalize FO76 archive paths for case-insensitive matching later.
    Output: lowercase + forward slashes, no duplicate slashes.
    """
    if not p:
        return ""
    p = str(p).strip().replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    return p.lower()

def _join_dds_path(folder: str, filename: str) -> str:
    folder = _norm_dds_path(folder)
    filename = _norm_dds_path(filename)
    if not folder and not filename:
        return ""
    if folder and not folder.endswith("/"):
        folder += "/"
    return _norm_dds_path(folder + filename)

def entm_storefront_dds_index(entm_rows: List[Dict[str, str]]) -> Dict[str, List[str]]:
    """
    Map ENTM EDID (lowercased) -> list of resolved DDS paths (lowercased).
    Uses:
      ETIP = folder
      ETDI and/or ETUI = filename (.dds)
    """
    idx: Dict[str, List[str]] = {}
    for r in entm_rows:
        edid = (r.get("EDID") or "").strip()
        if not edid:
            continue

        etip = (r.get("ETIP") or "").strip()
        etui = (r.get("ETUI") or "").strip()
        etdi = (r.get("ETDI") or "").strip()

        paths: List[str] = []

        # Preferred: ETIP + ETDI (FO76 Storefront Preview Image), but keep ETUI as fallback
        if etip and etdi:
            paths.append(_join_dds_path(etip, etdi))
        if etip and etui and _norm_dds_path(etui) != _norm_dds_path(etdi):
            paths.append(_join_dds_path(etip, etui))

        # Fallback: if exporter already included folders in ETDI/ETUI
        if not etip:
            if etdi:
                paths.append(_norm_dds_path(etdi))
            if etui and _norm_dds_path(etui) != _norm_dds_path(etdi):
                paths.append(_norm_dds_path(etui))

        # De-dupe preserving order
        seen = set()
        out: List[str] = []
        for p in paths:
            p = _norm_dds_path(p)
            if not p or p in seen:
                continue
            seen.add(p)
            out.append(p)

        if out:
            idx[edid.lower()] = out

    return idx

def _formid8_lower(s: str) -> str:
    s = (s or "").strip().lower().replace("0x", "")
    if len(s) > 8:
        s = s[-8:]
    return s.zfill(8)

def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s


def book_tradeable_map(book_rows: List[Dict[str, str]]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    for r in book_rows:
        edid = (r.get("EDID") or "").strip()
        full = (r.get("FULL") or "").strip()

        row_blob = " ".join(str(v) for v in r.values() if v)
        blob = row_blob.lower()

        # Bethesda naming is inconsistent across exports:
        # - NonPlayerTradeable (common)
        # - NonPlayerTradable (also common)
        # Also treat UnsellableObject as non-tradeable
        non_trade = (
            ("nonplayertradeable" in blob) or
            ("nonplayertradable" in blob) or
            ("unsellableobject" in blob)
        )
        is_tradeable = not non_trade

        if edid:
            out[_norm_key(edid)] = is_tradeable
        if full:
            out[_norm_key(full)] = is_tradeable
    return out

def _gmrw_parentquest_from_row(row: Dict[str, str]) -> str:
    """
    STRICT RULE:
      - Ignore ANAM / Record / ParentQuest / ParentQuestDisplay entirely.
      - ONLY scan Ref* columns (Ref1, Ref2, Ref3...) IN ORDER.
      - Prefer quoted labels:
            "Event: ..."
            "Activity: ..."
            "Bounty Hunting: ..."
      - Fallback: if a ref is a QUST ref and has a quoted FULL, allow it,
        BUT SKIP cut-content quests (EditorID starts with ZZZ/ZZZZ/CUT/DEL/POST).
    """

    quoted_label_re = re.compile(
        r'"(?P<label>(Event|Activity|Bounty\s*Hunting)\s*:\s*[^"]+)"',
        re.IGNORECASE
    )

    quoted_any_re = re.compile(r'"(?P<label>[^"]+)"')

    def _ref_keys_in_order(d: Dict[str, str]) -> List[str]:
        keys = [k for k in d.keys() if k.startswith("Ref")]
        def _n(k: str) -> int:
            m = re.match(r"Ref(\d+)$", k)
            return int(m.group(1)) if m else 10**9
        keys.sort(key=_n)
        return keys

    for k in _ref_keys_in_order(row):
        s = (row.get(k) or "").strip()
        if not s:
            continue

        # 1) Preferred: quoted "Event:" / "Activity:" / "Bounty Hunting:"
        m = quoted_label_re.search(s)
        if m:
            return m.group(0).strip()

        # 2) Fallback: QUST ref with quoted FULL (but skip cut-content quest EDIDs)
        # Expected shape: 00824A46:zzzBurn_BountyHunt_Public_Test:"Event: ...":QUST
        if s.endswith(":QUST"):
            parts = s.split(":", 3)  # FormID, EDID, "FULL", QUST
            quest_edid = parts[1].strip() if len(parts) >= 2 else ""
            if quest_edid and starts_cut(quest_edid):
                continue  # ignore zzz/cut quest refs

            m2 = quoted_any_re.search(s)
            if m2:
                return f"\"{m2.group('label').strip()}\""

    return ""

def gmrw_parentquest_map(gmrw_rows: List[Dict[str, str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for r in gmrw_rows:
        edid = (r.get("EDID") or "").strip()
        if not edid:
            continue
        token = edid.split("_", 1)[0]
        pq = _gmrw_parentquest_from_row(r)
        if token and pq:
            out[token] = pq
    return out


def gmrw_parentquest_by_any_ref_formid_map(gmrw_rows: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Map any 8-hex FormID mentioned anywhere in a GMRW row -> ParentQuest label.
    Used for resolving COBJ ReferencedBy LVLI chains.
    """
    out: Dict[str, str] = {}
    hex_re = re.compile(r"\b[0-9A-F]{8}\b", re.IGNORECASE)

    for r in gmrw_rows:
        pq = _gmrw_parentquest_from_row(r)
        if not pq:
            continue

        for v in r.values():
            if not v:
                continue
            for m in hex_re.findall(str(v)):
                out[m.upper()] = pq

    return out

def _extract_formids_from_ref_fields(row: Dict[str, str], suffix: str) -> List[str]:
    """
    Rows store refs like: "006313AF:QuestReward_...:GMRW" or "0004718C:Something:LVLI"
    This returns the leading 8-hex FormID for matching suffix, preserving order.
    """
    out: List[str] = []
    for k, v in row.items():
        if not k.startswith("Ref"):
            continue
        s = (v or "").strip()
        if not s:
            continue
        if not s.endswith(suffix):
            continue
        m = re.match(r"^([0-9A-Fa-f]{8}):", s)
        if not m:
            continue
        out.append(m.group(1).upper())
    return out


def _find_row_by_formid(rows: List[Dict[str, str]], formid: str) -> Optional[Dict[str, str]]:
    fid = (formid or "").strip().upper()
    if not fid:
        return None
    for r in rows:
        if (r.get("FormID") or "").strip().upper() == fid:
            return r
    return None

def lvli_drop_rate_from_cobj_lvli(
    cobj_rows: List[Dict[str, str]],
    lvli_entry_rows: List[Dict[str, str]],
    lvli_list_rows: List[Dict[str, str]],
    glob_rows: List[Dict[str, str]],
    cobj_formid: str,
    prefer_lvli_formid: Optional[str] = None,
) -> Optional[str]:
    """
    COBJ(FormID) -> GNAM_FormID (BOOK FormID)
      -> find LVLI entry rows (LVLI_Entries.tsv) where LVLO_Reference contains that BOOK
      -> apply global-first rule using entry-level globals/curves first
      -> if no entry global, allow list-level globals/curves (LVLI_List.tsv) by LVLI_FormID
      -> else fallback to LVOV_ChanceNoneValue (LVLI_Entries.tsv)

    PLUS: Tier-family support:
      - Tier_01/02/03
      - Reward_1/2/3
      - Bad/Good/Best
    Outputs multi-line tier text using:
      Tier 1 - 5%
      Tier 2 - 10%
      Tier 3 - 0%
    """
    cobj_formid = (cobj_formid or "").strip().upper()
    if not cobj_formid:
        return None

    # 1) Find exact COBJ row
    cand = None
    for r in cobj_rows:
        if (r.get("FormID") or "").strip().upper() == cobj_formid:
            cand = r
            break
    if not cand:
        return None

    # 2) BOOK FormID from GNAM_FormID
    book_formid = (cand.get("GNAM_FormID") or "").strip().upper()
    if not book_formid or not re.fullmatch(r"[0-9A-F]{8}", book_formid):
        return None

    # 3) Find LVLI entry row(s) referencing that BOOK
    matches_all = [r for r in lvli_entry_rows if book_formid in ((r.get("LVLO_Reference") or "").upper())]
    if not matches_all:
        return None

    # ------------------------------------------------------------
    # Tier-family support (Tier_01/02/03, Reward_1/2/3, Bad/Good/Best)
    # ------------------------------------------------------------

    def _lvli_edid_for(fid8: str) -> str:
        f = (fid8 or "").strip().upper()
        if not f:
            return ""
        for lr in lvli_list_rows:
            rr_f = (lr.get("LVLI_FormID") or lr.get("FormID") or "").strip().upper()
            if rr_f == f:
                return (lr.get("LVLI_EDID") or lr.get("EDID") or "").strip()
        return ""

    def _tier_info_from_edid(edid: str) -> Optional[Tuple[str, str, int]]:
        """
        Returns (family_key, tier_label, tier_order)

        family_key: EDID normalized with the tier token stripped, for grouping.
        tier_label: "Tier 1" / "Bad" / etc.
        tier_order: sorting key
        """
        e = (edid or "").strip()
        if not e:
            return None
        el = e.lower()

        # Bad/Good/Best style
        if "bad" in el or "good" in el or "best" in el:
            for lab, order in (("bad", 1), ("good", 2), ("best", 3)):
                if re.search(rf"(?:^|[_\-]){lab}(?:$|[_\-])", el):
                    fam = re.sub(rf"([_\-]){lab}([_\-]|$)", r"\1", el)
                    fam = re.sub(r"[_\-]+$", "", fam)
                    fam = re.sub(r"[_\-]+", "_", fam).strip("_")
                    return (fam, lab.capitalize(), order)

        # Numeric tier/reward style (Tier_01, Tier02, Reward_3, Quest_Reward_3, etc)
        m_all = list(re.finditer(r"(tier|reward)[_\-]*(0?\d{1,2})\b", el))
        if m_all:
            m = m_all[-1]  # use the last tier token
            n = int(m.group(2).lstrip("0") or "0")
            if n <= 0:
                return None
            fam = el[:m.start()] + el[m.end():]
            fam = re.sub(r"[_\-]+", "_", fam).strip("_")
            return (fam, f"Tier {n}", n)

        return None

    def _compute_rate_for_entry_row(entry_row: Dict[str, str], lvli_fid: str) -> Optional[str]:
        # Helper: list row lookup
        list_row = None
        for lr in lvli_list_rows:
            rr_f = (lr.get("LVLI_FormID") or lr.get("FormID") or "").strip().upper()
            if rr_f == lvli_fid:
                list_row = lr
                break

        # Global-first rule (entry-level then list-level)
        candidates: List[str] = []

        lvog = (entry_row.get("LVOG_ChanceNoneGlobal") or "").strip()
        if lvog:
            candidates.append(lvog)

        lvoc = (entry_row.get("LVOC_ChanceNoneCurve") or "").strip()
        if lvoc and ":GLOB" in lvoc.upper():
            candidates.append(lvoc)

        if list_row:
            lvlg = (list_row.get("LVLG_ChanceNoneGlobal") or "").strip()
            if lvlg:
                candidates.append(lvlg)

            lvct = (list_row.get("LVCT_ChanceNoneCurve") or "").strip()
            if lvct and ":GLOB" in lvct.upper():
                candidates.append(lvct)

        for glob_field in candidates:
            gfid = _glob_formid_from_lvli_global_field(glob_field)
            if not gfid:
                continue
            dr2 = glob_drop_rate_by_formid(glob_rows, gfid)
            if dr2:
                return dr2

        # Fallback: LVOV ChanceNoneValue on the entry row
        chance_none = safe_float(entry_row.get("LVOV_ChanceNoneValue") or "", None)
        if chance_none is None:
            return None
        if abs(chance_none) < 1e-9:
            return "100%"

        pct = 100.0 - chance_none
        if pct < 0:
            return None
        if abs(pct - round(pct)) < 1e-6:
            return f"{int(round(pct))}%"
        return f"{pct:.3f}%"

    # Group matches by LVLI FormID (only those LVLIs that contain THIS BOOK)
    by_lvli: Dict[str, List[Dict[str, str]]] = {}
    for r in matches_all:
        fid = (r.get("LVLI_FormID") or r.get("FormID") or "").strip().upper()
        if not fid or not re.fullmatch(r"[0-9A-F]{8}", fid):
            continue
        by_lvli.setdefault(fid, []).append(r)

    # Detect tier family among matched LVLIs
    tier_hits: List[Tuple[str, str, int, str]] = []
    for fid in by_lvli.keys():
        ed = _lvli_edid_for(fid)
        info = _tier_info_from_edid(ed)
        if not info:
            continue
        fam, lab, order = info
        tier_hits.append((fam, lab, order, fid))

    if tier_hits:
        # Choose the family key that appears most among the matched LVLIs
        fam_counts: Dict[str, int] = {}
        for fam, _lab, _ord, _fid in tier_hits:
            fam_counts[fam] = fam_counts.get(fam, 0) + 1
        best_family = sorted(fam_counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

        # Discover all tiers in that family from LVLI_List
        family_tiers: Dict[int, Tuple[str, str]] = {}
        family_named: Dict[str, str] = {}

        for lr in lvli_list_rows:
            fid = (lr.get("LVLI_FormID") or lr.get("FormID") or "").strip().upper()
            ed = (lr.get("LVLI_EDID") or lr.get("EDID") or "").strip()
            info = _tier_info_from_edid(ed)
            if not info:
                continue
            fam, lab, order = info
            if fam != best_family:
                continue

            if lab.lower().startswith("tier "):
                family_tiers[order] = (lab, fid)
            else:
                family_named[lab] = fid

        parts: List[str] = []

        # Bad/Good/Best format
        if family_named:
            for lab in ("Bad", "Good", "Best"):
                fid = family_named.get(lab)
                if not fid:
                    continue
                if fid not in by_lvli:
                    parts.append(f"{lab} - 0%")
                    continue
                entry_row = by_lvli[fid][0]
                dr = _compute_rate_for_entry_row(entry_row, fid) or "N/A"
                parts.append(f"{lab} - {dr}")
            if parts:
                return "\n".join(parts)

        # Numeric tiers
        if family_tiers:
            for order in sorted(family_tiers.keys()):
                lab, fid = family_tiers[order]
                if fid not in by_lvli:
                    parts.append(f"{lab} - 0%")
                    continue
                entry_row = by_lvli[fid][0]
                dr = _compute_rate_for_entry_row(entry_row, fid) or "N/A"
                parts.append(f"{lab} - {dr}")
            if parts:
                return "\n".join(parts)

    # ------------------------------------------------------------
    # Original single-rate logic (unchanged)
    # ------------------------------------------------------------

    prefer = (prefer_lvli_formid or "").strip().upper()
    if prefer and re.fullmatch(r"[0-9A-F]{8}", prefer):
        matches = []
        for r in matches_all:
            fid = (r.get("LVLI_FormID") or r.get("FormID") or "").strip().upper()
            if fid == prefer:
                matches.append(r)
        if not matches:
            matches = matches_all
    else:
        matches = matches_all

    # Prefer a match that has an entry-level global override
    def _rank(row: Dict[str, str]) -> int:
        eg = (row.get("LVOG_ChanceNoneGlobal") or "").strip()
        return 0 if eg else 1

    matches.sort(key=_rank)
    best = matches[0]

    # Helper: list row by LVLI_FormID (for LVLG/LVCT fallback)
    lvli_fid = (best.get("LVLI_FormID") or best.get("FormID") or "").strip().upper()
    list_row = None
    if lvli_fid:
        for lr in lvli_list_rows:
            if (lr.get("LVLI_FormID") or lr.get("FormID") or "").strip().upper() == lvli_fid:
                list_row = lr
                break

    # Global override first (global-first rule, order matters)
    candidates: List[str] = []

    lvog = (best.get("LVOG_ChanceNoneGlobal") or "").strip()
    if lvog:
        candidates.append(lvog)

    lvoc = (best.get("LVOC_ChanceNoneCurve") or "").strip()
    if lvoc and ":GLOB" in lvoc.upper():
        candidates.append(lvoc)

    if list_row:
        lvlg = (list_row.get("LVLG_ChanceNoneGlobal") or "").strip()
        if lvlg:
            candidates.append(lvlg)

        lvct = (list_row.get("LVCT_ChanceNoneCurve") or "").strip()
        if lvct and ":GLOB" in lvct.upper():
            candidates.append(lvct)

    for glob_field in candidates:
        gfid = _glob_formid_from_lvli_global_field(glob_field)
        if not gfid:
            continue
        dr = glob_drop_rate_by_formid(glob_rows, gfid)
        if dr:
            return dr

    # Fallback: LVOV_ChanceNoneValue
    chance_none = safe_float(best.get("LVOV_ChanceNoneValue") or "", None)
    if chance_none is None:
        return None
    if abs(chance_none) < 1e-9:
        return "100%"

    pct = 100.0 - chance_none
    if pct < 0:
        return None
    if abs(pct - round(pct)) < 1e-6:
        return f"{int(round(pct))}%"
    return f"{pct:.3f}%"

def _glob_formid_from_lvli_global_field(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None

    # Format A: "0089EA90:Something:GLOB"
    m = re.match(r"^([0-9A-Fa-f]{8}):", s)
    if m:
        return m.group(1).upper()

    # Format B: "Name [GLOB:0085AD24]"
    m2 = re.search(r"\[GLOB:([0-9A-Fa-f]{8})\]", s)
    if m2:
        return m2.group(1).upper()

    return None


def glob_drop_rate_by_formid(glob_rows: List[Dict[str, str]], glob_formid: str) -> Optional[str]:
    """
    Your rule:
      pct = 100 - FLTV
    (So FLTV 98 => 2%, FLTV 95 => 5%, etc)
    """
    glob_formid = (glob_formid or "").strip().upper()
    if not glob_formid:
        return None

    for r in glob_rows:
        if (r.get("FormID") or "").strip().upper() != glob_formid:
            continue
        fv = safe_float(r.get("FLTV") or "", None)
        if fv is None:
            return None
        pct = 100.0 - fv
        if pct < 0:
            return None
        if abs(pct - round(pct)) < 1e-6:
            return f"{int(round(pct))}%"
        return f"{pct:.3f}%"
    return None

def prettify_token_words(token: str) -> str:
    s = token.replace("_", " ").strip()
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def clean_full(full: Optional[str]) -> str:
    s = (full or "").strip()

    # Remove surrounding quotes if present
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_entitlement_edid_from_condition(cond: str) -> Optional[str]:
    m = re.search(r"HasEntitlement\(\s*([^\s\)]+)", cond, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip()


def storefront_webp_url_from_extra(kind: str, extra: Dict[str, Any]) -> Optional[str]:
    """
    Storefront WEBP URL for Titles pages.

    Uploads:
      /wp-content/uploads/storefront/titles-camp/<ent>.webp
      /wp-content/uploads/storefront/titles-player/<ent>.webp

    NOTE:
      We normalize entitlement filenames by removing "_ENTM_" token so they match your
      uploaded storefront filenames (no entm in the webp filename).
    """
    img_ent = (extra.get("imageEntitlementEdid") or "").strip()
    if not img_ent:
        return None

    if kind == "camp":
        folder = "titles-camp"
    elif kind == "player":
        folder = "titles-player"
    else:
        return None

    ent = img_ent.lower().replace("_entm_", "_")
    return "/wp-content/uploads/storefront/" + folder + "/" + ent + ".webp"

def parse_quest_name_from_condition(cond: str) -> Optional[str]:
    m = RE_QUOTED.search(cond)
    return m.group(1).strip() if m else None


def parse_rhs_number(cond: str) -> Optional[float]:
    m = re.search(r"=\s*([0-9]+(?:\.[0-9]+)?)", cond)
    return safe_float(m.group(1), None) if m else None

    def chal_maps(chal_rows: List[Dict[str, str]]) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    """
    Build lookups:
      - by_id:  FormID (8-hex uppercase) -> row
      - by_edid: EDID (lowercase) -> row
    """
    by_id: Dict[str, Dict[str, str]] = {}
    by_edid: Dict[str, Dict[str, str]] = {}

    for r in (chal_rows or []):
        fid = (r.get("FormID") or "").strip().upper()
        edid = (r.get("EDID") or "").strip()
        if fid and re.fullmatch(r"[0-9A-F]{8}", fid):
            by_id[fid] = r
        if edid:
            by_edid[edid.lower()] = r

    return by_id, by_edid

def parse_chal_formid_from_condition(cond: str) -> Optional[str]:
    for typ, fid in RE_FORM_REF.findall(cond):
        if typ.upper() == "CHAL":
            return fid.upper()
    return None

def parse_cobj_formid_from_condition(cond: str) -> Optional[str]:
    # 1) Prefer [COBJ:XXXXXXXX] style (RE_FORM_REF)
    for typ, fid in RE_FORM_REF.findall(cond):
        if typ.upper() == "COBJ":
            return fid.upper()

    # 2) Support COBJ:XXXXXXXX without brackets
    m = re.search(r"\bCOBJ:([0-9A-Fa-f]{8})\b", cond or "")
    if m:
        return m.group(1).upper()

    return None


def parse_cndf_formid_from_condition(cond: str) -> Optional[str]:
    m = re.search(r"\[CNDF:([0-9A-Fa-f]{8})\]", cond or "")
    if not m:
        return None
    return m.group(1).upper()


def extract_cndf_conditions_and_refs(cndf_row: Dict[str, str]) -> Tuple[List[str], List[str]]:
    """
    CNDF TSV format:
      ConditionCount, Cond01..Cond25
      ReferencedByCount, Ref01..Ref25
    Values are already pretty human-readable in your fixed export.
    """
    conds: List[str] = []
    refs: List[str] = []

    n_cond = safe_int((cndf_row.get("ConditionCount") or "").strip(), 0)
    for i in range(1, min(n_cond, 25) + 1):
        v = (cndf_row.get(f"Cond{i:02d}") or "").strip()
        if v:
            conds.append(v)

    n_ref = safe_int((cndf_row.get("ReferencedByCount") or "").strip(), 0)
    for i in range(1, min(n_ref, 25) + 1):
        v = (cndf_row.get(f"Ref{i:02d}") or "").strip()
        if v:
            refs.append(v)

    return conds, refs

def compute_unlock_and_rates(
    kind: str,
    title_display: str,
    edid: str,
    conds: List[str],
    seasons: Dict[int, str],
    gmrw_by_token: Dict[str, str],
    gmrw_by_formid: Dict[str, str],
    book_rows: List[Dict[str, str]],
    lvli_refby_rows: List[Dict[str, str]],
    glob_rows: List[Dict[str, str]],
    cobj_rows: List[Dict[str, str]],
    lvli_entry_rows: List[Dict[str, str]],
    lvli_list_rows: List[Dict[str, str]],
    chal_by_id: Dict[str, Dict[str, str]],
    chal_by_edid: Dict[str, Dict[str, str]],
    cndf_by_id: Dict[str, Dict[str, str]],
) -> Tuple[str, str, Optional[int], str, Dict[str, Any]]:

    extra: Dict[str, Any] = {}

    if not conds:
        return "Unlocked by Default", "N/A", None, "default", extra

    joined = " ".join(conds)

    # Expand CNDF if present in any condition line (attach into debug/extra)
    cndf_formid = None
    for c in conds:
        cndf_formid = parse_cndf_formid_from_condition(c)
        if cndf_formid:
            break

    if cndf_formid:
        extra["cndfFormId"] = cndf_formid
        row = cndf_by_id.get(cndf_formid)
        if row:
            extra["cndfEdid"] = (row.get("EDID") or "").strip()
            cc, rr = extract_cndf_conditions_and_refs(row)
            extra["cndfConditions"] = cc
            extra["cndfRefs"] = rr

            # If CNDF expands to multiple HasCompletedChallenge requirements,
            # generate a human-readable "Complete these challenges" list.
            chal_names: List[str] = []
            for s in cc:
                if "HasCompletedChallenge" not in s:
                    continue
                m = RE_QUOTED.search(s)
                if m:
                    chal_names.append(m.group(1).strip())

            # De-dupe while preserving order
            if chal_names:
                seen = set()
                ordered: List[str] = []
                for n in chal_names:
                    if n in seen:
                        continue
                    seen.add(n)
                    ordered.append(n)

                # Only use list-style output when it’s actually a list
                if len(ordered) >= 2:
                    how = "Complete the following challenges to unlock this title:\n" + "\n".join(f"- {n}" for n in ordered)
                    return how, "N/A", None, "challenge", extra

    # --- Challenges: HasCompletedChallenge -> CHAL by FormID ---
    if RE_HAS_COMPLETED_CHAL.search(joined):
        chal_fid = None
        for c in conds:
            if "HasCompletedChallenge" not in c:
                continue
            chal_fid = parse_chal_formid_from_condition(c) or chal_fid

        if chal_fid and chal_fid in chal_by_id:
            row = chal_by_id[chal_fid]

            chal_edid = (row.get("EDID") or "").strip()
            full = clean_full(row.get("FULL")) or clean_full(chal_edid) or "Challenge"
            cnam = (row.get("CNAM") or "").strip() or "Challenge"

            # Drill Complex counters: Enc02_01 / Enc02_05 / Enc02_76 => x1/x5/x76
            mcount = re.search(r"_Enc\d+_(\d+)\b", chal_edid, flags=re.IGNORECASE)
            if mcount:
                full = f"{full} x{mcount.group(1)}"
            else:
                # Generic challenge target count (Lifetime kills etc) => xN
                target = None
                for k in ("TargetCount", "Target Count", "DATA - Target Count", "CTDA - Comparison Value"):
                    v = (row.get(k) or "").strip() if isinstance(row.get(k), str) else row.get(k)
                    if v is None:
                        continue
                    try:
                        # allow "5.000000" etc
                        target = int(float(str(v).strip()))
                        break
                    except Exception:
                        pass

                if target and target > 1:
                    full = f"{full} x{target}"

            extra.update({
                "chalFormId": chal_fid,
                "chalEdid": chal_edid,
                "chalCNAM": cnam,
                "chalFULL": full
            })

            if cnam.lower() == "challenge":
                return f"Complete the Challenge:\n{full}", "N/A", None, "challenge", extra
            return f"Complete the {cnam} Challenge:\n{full}", "N/A", None, "challenge", extra

        return "Complete the Challenge:", "N/A", None, "challenge", extra

    # --- CNDF-based challenge: IsTrueForConditionForm(Challenge_*_ConditionForm) -> CHAL by EDID ---
    if RE_IS_TRUE_CNDF.search(joined):
        for c in conds:
            if "IsTrueForConditionForm" not in c:
                continue

            m = re.search(r"IsTrueForConditionForm\(\s*([^\s\)]+)", c, flags=re.IGNORECASE)
            if not m:
                continue

            arg = m.group(1).strip()
            if not arg.endswith("_ConditionForm"):
                continue

            chal_edid = arg[:-len("_ConditionForm")]
            row = chal_by_edid.get(chal_edid.lower())
            if not row:
                continue

            full = clean_full(row.get("FULL")) or clean_full(chal_edid) or chal_edid

            mcount = re.search(r"_Enc\d+_(\d+)\b", chal_edid, flags=re.IGNORECASE)
            if mcount:
                full = f"{full} x{mcount.group(1)}"
            else:
                target = None
                for k in ("TargetCount", "Target Count", "DATA - Target Count", "CTDA - Comparison Value"):
                    v = (row.get(k) or "").strip() if isinstance(row.get(k), str) else row.get(k)
                    if v is None:
                        continue
                    try:
                        target = int(float(str(v).strip()))
                        break
                    except Exception:
                        pass

                if target and target > 1:
                    full = f"{full} x{target}"
            cnam = (row.get("CNAM") or "").strip() or "Challenge"
            extra.update({
                "chalEdid": chal_edid,
                "chalCNAM": cnam,
                "chalFULL": full
            })

            if cnam.lower() == "challenge":
                return f"Complete the Challenge:\n{full}", "N/A", None, "challenge", extra
            return f"Complete the {cnam} Challenge:\n{full}", "N/A", None, "challenge", extra
        # else: fall through (IsTrueForConditionForm used for other things)

    # --- Quests ---
    if RE_NUM_TIMES_COMPLETED.search(joined):
        for c in conds:
            if "GetNumTimesCompletedQuest" not in c:
                continue
            qname = clean_full(parse_quest_name_from_condition(c)) or "Unknown Quest"
            qname = qname.replace('"', "").strip()
            n = parse_rhs_number(c)
            if n is None:
                return f"Complete the Quest:\n{qname}", "N/A", None, "quest", extra
            n_int = int(round(n))
            if n_int <= 1:
                return f"Complete the Quest:\n{qname}", "N/A", None, "quest", extra
            return f"Complete the Quest:\n{qname} ({n_int} times)", "N/A", None, "quest", extra

    if RE_QUEST_COMPLETED.search(joined):
        qname = clean_full(parse_quest_name_from_condition(joined)) or "Unknown Quest"
        qname = qname.replace('"', "").strip()
        return f"Complete the Quest:\n{qname}", "N/A", None, "quest", extra

    # --- Entitlements ---
    if RE_HAS_ENTITLEMENT.search(joined):
        ent_edids: List[str] = []
        for c in conds:
            if "HasEntitlement" not in c:
                continue
            ee = parse_entitlement_edid_from_condition(c)
            if ee:
                ent_edids.append(ee)
        extra["entitlementEdids"] = ent_edids

                # Pick ONLY real title entitlements.
        # Must contain camptitle or playertitle.
        # Reject gameboards, framed art, plushies, bundles, icons.

        def _is_valid_title_entitlement(edid: str, kind: str) -> bool:
            if not edid:
                return False

            e = edid.lower()

            # normalize common noise
            e = e.replace("_entm_", "_")

            # must contain correct title keyword
            if kind == "camp":
                if "camptitle" not in e:
                    return False
            elif kind == "player":
                if "playertitle" not in e:
                    return False
            else:
                return False

            # reject non-title storefront items
            blacklist = [
                "gameboard",
                "corkboard",
                "framed",
                "plushie",
                "icon",
                "bundle"
            ]

            for bad in blacklist:
                if bad in e:
                    return False

            return True

        img_ent = ""
        for e in ent_edids:
            if _is_valid_title_entitlement(e, kind):
                img_ent = e
                break

        extra["imageEntitlementEdid"] = img_ent

        # Priority: Community -> MiniSeason -> SCORE season -> ATX -> other

        # Bethesda data issue: House Finalist should be treated as Community acquisition
        if any("HOUSEFINALIST" in (e or "").upper() for e in ent_edids):
            return "Awarded through a Bethesda community event or promotion.", "N/A", None, "community", extra

        if any(RE_COMMUNITY.search(e) for e in ent_edids):
            return "Awarded through a Bethesda community event or promotion.", "N/A", None, "community", extra

        # Mini Season
        ms = next((e for e in ent_edids if RE_MINISEASON.search(e)), None)
        if ms:
            tok = ms
            idx = tok.lower().find("score_miniseason_")
            tok2 = tok[idx + len("SCORE_MiniSeason_"):] if idx != -1 else tok
            cut_idx = tok2.upper().find("_ENTM_")
            if cut_idx != -1:
                tok2 = tok2[:cut_idx]
            # Capture year BEFORE stripping it
            year = None
            my = re.match(r"^(\d{4})_", tok2)
            if my:
                year = my.group(1)

            tok2 = re.sub(r"^\d{4}_", "", tok2)  # drop leading year
            name = prettify_token_words(tok2)
            extra.update({"miniSeasonRaw": tok2, "miniSeasonName": name, "miniSeasonYear": year})

            if year:
                return f"Purchase with tickets from the {name} Mini Season board.", "N/A", None, "miniseason", extra

            return f"Purchase with tickets from the {name} Mini Season board.", "N/A", None, "miniseason", extra

        # SCORE season
        season_num: Optional[int] = None
        season_edid: Optional[str] = None

        def _is_player_title_entitlement(ed: str) -> bool:
            s = (ed or "").lower()
            return ("playertitle" in s) and ("gameboard" not in s) and ("corkboard" not in s) and ("endofseasonart" not in s)

        def _is_camp_title_entitlement(ed: str) -> bool:
            s = (ed or "").lower()
            return ("camptitle" in s) and ("gameboard" not in s) and ("corkboard" not in s) and ("endofseasonart" not in s)

        # Prefer the entitlement that is actually the title, not the gameboard/framed art gate.
        preferred = None
        for e in ent_edids:
            if kind == "player" and _is_player_title_entitlement(e):
                preferred = e
                break
            if kind == "camp" and _is_camp_title_entitlement(e):
                preferred = e
                break

        search_list = [preferred] if preferred else []
        search_list += [e for e in ent_edids if e and e != preferred]

        for e in search_list:
            m = RE_SCORE_SEASON.search(e)
            if m:
                season_num = safe_int(m.group(1), 0)
                season_edid = e
                break

        if season_num:
            sname = seasons.get(season_num, f"Season {season_num}")
            extra.update({"seasonNumber": season_num, "seasonName": sname})

            if kind == "player":
                claimed = None
                for c in conds:
                    if "HasEntitlement" in c and (season_edid or "") in c:
                        mq = RE_QUOTED.search(c)
                        if mq:
                            claimed = clean_full(mq.group(1))
                            break

                se = (season_edid or "").lower()
                cl = (claimed or "").lower()

                is_gameboard_gate = (
                    ("gameboard" in se) or ("corkboard" in se) or
                    ("gameboard" in cl) or ("corkboard" in cl)
                )

                is_framed_art_gate = (
                    ("endofseasonart" in se) or
                    (("framed art" in cl) and (not is_gameboard_gate))
                )

                if is_framed_art_gate:
                    return (
                        f"Unlocked if you have claimed the {sname} (Season {season_num}) Framed Art.",
                        "N/A",
                        season_num,
                        "season_score",
                        extra
                    )

                if is_gameboard_gate:
                    return (
                        f"Unlocked if you have claimed the {sname} (Season {season_num}) Gameboard.",
                        "N/A",
                        season_num,
                        "season_score",
                        extra
                    )

                return (
                    f"Purchase with tickets from the {sname} Scoreboard (Season {season_num})",
                    "N/A",
                    season_num,
                    "season_score",
                    extra
                )

            # Camp titles
            e_upper = (season_edid or "").upper()
            framed = ("ENDOFSEASONART" in e_upper)

            if framed:
                return f"Unlocked if you have claimed the {sname} (Season {season_num}) Framed Art.", "N/A", season_num, "season_score", extra

            if "CAMPTITLES" in e_upper and "GAMEBOARD" not in e_upper and "CORKBOARD" not in e_upper:
                return f"Unlocked if you have claimed the {sname} (Season {season_num}) Scoreboard.", "N/A", season_num, "season_score", extra

            return f"Unlocked if you have claimed the {sname} (Season {season_num}) Gameboard.", "N/A", season_num, "season_score", extra

        # ATX standard
        if any(RE_ATX.search(e) for e in ent_edids):

            # Fallout 1st titles: free claim for members
            if any(("ATX_F1_" in (e or "").upper()) for e in ent_edids):
                return "Free to claim from the Atom Shop\nfor Fallout 1st members.", "N/A", None, "atx", extra

            return "Can be purchased with certain bundles from the Atom Shop.", "N/A", None, "atx", extra

        # PTS titles: special rule
        if any(str(e).upper().startswith("PTS_") for e in ent_edids):
            return "Log into the PTS and play for 15 minutes.", "N/A", None, "pts", extra

        return "Unlocked via account entitlement.", "N/A", None, "entitlement", extra

    # --- COBJ proxy (can mean: event/activity BOOK drop OR challenge unlock via GNAM) ---
    if RE_COBJ_REF.search(joined):
        token = cobj_token_from_condition(conds)
        extra["cobjToken"] = token

        # Strict BOOK -> LVLI -> GMRW ParentQuest resolution (no token guessing)
        how_event = "Complete the Event/Activity: (unknown)"
        how_from_parentquest = None

        # Pull COBJ FormID from the condition text
        cobj_formid = None
        for c in conds:
            if ("[COBJ:" in c) or ("COBJ:" in c):
                cobj_formid = parse_cobj_formid_from_condition(c)
                if cobj_formid:
                    break

        if not cobj_formid:
            return how_event, "N/A", None, "event_activity", extra

        extra["cobjFormId"] = cobj_formid

        # Locate the COBJ row so we can inspect GNAM_* (BOOK vs CHAL)
        cobj_row = None
        for rr in cobj_rows:
            if (rr.get("FormID") or "").strip().upper() == cobj_formid:
                cobj_row = rr
                break

        if cobj_row:
            gnam_edid = (cobj_row.get("GNAM_EDID") or "").strip()
            gnam_full = (cobj_row.get("GNAM_FULL") or "").strip()
            gnam_form = (cobj_row.get("GNAM_FormID") or "").strip().upper()

            # If GNAM is a BOOK FormID, resolve Event/Activity via:
            #   (1) BOOK -> LVLI_Entries (LVLO_Reference) -> LVLI -> GMRW
            #   (2) fallback to BOOK row Ref* -> LVLI -> GMRW (older shape)
            if gnam_form and re.fullmatch(r"[0-9A-F]{8}", gnam_form):

                pq = None

                # (1) Preferred: scan LVLI_Entries for BOOK refs
                pq_a, picked_lvli_a, dbg_a = book_to_gmrw_parentquest_via_lvli_entries(
                    gnam_form,
                    lvli_entry_rows,
                    lvli_refby_rows,
                    gmrw_by_formid,
                    lvli_list_rows
                )
                extra["bookLvliGmrwViaEntries"] = dbg_a
                if pq_a:
                    pq = pq_a
                    extra["bookLvliPickedViaEntries"] = picked_lvli_a

                # (2) Fallback: older BOOK.Ref* -> LVLI links
                if not pq:
                    pq_b, dbg_b = book_lvli_gmrw_parentquest(
                        book_rows,
                        lvli_refby_rows,
                        gmrw_by_formid,
                        gnam_form,
                        lvli_list_rows
                    )
                    extra["bookLvliGmrw"] = dbg_b
                    pq = pq_b

                if pq:
                    parsed = parse_parentquest_label(pq)
                    if parsed:
                        lk, ln = parsed
                        how_from_parentquest = f"Complete the {lk}: {ln}"
                        how_event = how_from_parentquest

                                        # ------------------------------------------------------------
                # Party Crasher creature drops (no GMRW parent quest)
                # If the BOOK is only referenced by LLD_Creature_*_PartyCrasher lists,
                # show: "Drops from Party Crasher Creatures."
                # ------------------------------------------------------------
                if not how_from_parentquest:
                    try:
                        # Candidate LVLIs come from the LVLI_Entries scan debug
                        cand_ids = []
                        dbg_a = extra.get("bookLvliGmrwViaEntries") or {}
                        for k in ("candidateLvliFormIdsFiltered", "candidateLvliFormIdsAll"):
                            v = dbg_a.get(k)
                            if isinstance(v, list):
                                cand_ids.extend([str(x).strip().upper() for x in v if str(x).strip()])

                        # De-dupe preserving order
                        seen = set()
                        cand_ids = [x for x in cand_ids if not (x in seen or seen.add(x))]

                        # Map FormID -> EDID via LVLI list rows
                        edids = []
                        for fid in cand_ids:
                            for lr in lvli_list_rows:
                                rr_f = (lr.get("LVLI_FormID") or lr.get("FormID") or "").strip().upper()
                                if rr_f == fid:
                                    ed = (lr.get("LVLI_EDID") or lr.get("EDID") or "").strip()
                                    if ed:
                                        edids.append(ed)
                                    break

                        blob = " ".join(edids).lower()
                        if ("partycrasher" in blob) and ("lld_creature" in blob):
                            how_event = "Drops from Party Crasher Creatures."
                    except Exception:
                        pass

                                    # Fallback: resolve via LVLI that references this COBJ (COBJ "Referenced By" list),
            # then map LVLI FormID -> GMRW ParentQuestDisplay.
            if not how_from_parentquest:
                # Collect all LVLI candidates and skip cut-content lists (DEL/CUT/POST/ZZZ/ZZZZ)
                lvli_candidates: List[str] = []

                # Helper: try to get LVLI EDID from:
                #  1) the string itself (often "SomeEdid [LVLI:DEADBEEF]")
                #  2) lvli_list_rows lookup by FormID
                def _lvli_edid_for(fid8: str, s: str = "") -> str:
                    # (1) parse "EDID [LVLI:XXXXXXXX]" if present
                    if s:
                        m_ed = re.search(r"\b([A-Za-z0-9_]+)\s*\[LVLI:[0-9A-Fa-f]{8}\]", s)
                        if m_ed:
                            return (m_ed.group(1) or "").strip()

                    # (2) lookup in LVLI list rows
                    f = (fid8 or "").strip().upper()
                    if not f:
                        return ""
                    for rr in lvli_list_rows:
                        rr_f = (rr.get("LVLI_FormID") or rr.get("FormID") or "").strip().upper()
                        if rr_f == f:
                            return (rr.get("LVLI_EDID") or rr.get("EDID") or "").strip()
                    return ""

                # Pull candidates from any field containing [LVLI:XXXXXXXX]
                for k, v in cobj_row.items():
                    if not v:
                        continue
                    s = str(v)

                    # Match [LVLI:XXXXXXXX] (preferred)
                    for m in re.finditer(r"\[LVLI:([0-9A-Fa-f]{8})\]", s):
                        fid = m.group(1).upper()
                        lvli_candidates.append(fid)

                    # Also support odd legacy shapes like "[XXXXXXXX]" when LVLI: token is present
                    if "LVLI:" in s:
                        m2 = re.search(r"\[([0-9A-Fa-f]{8})\]", s)
                        if m2:
                            lvli_candidates.append(m2.group(1).upper())

                # De-dupe preserving order
                seen = set()
                lvli_candidates = [x for x in lvli_candidates if not (x in seen or seen.add(x))]

                # Filter out cut-content LVLIs by EDID
                filtered: List[str] = []
                for fid in lvli_candidates:
                    ed = _lvli_edid_for(fid)
                    if ed and starts_cut(ed):
                        continue
                    filtered.append(fid)

                extra["cobjLvliCandidatesAll"] = lvli_candidates
                extra["cobjLvliCandidatesFiltered"] = filtered

                # Pick the first LVLI that resolves a ParentQuest label; otherwise keep first filtered
                lvli_formid = None
                pq2 = None

                for fid in filtered:
                    pq_try = lvli_to_gmrw_parentquest(lvli_refby_rows, gmrw_by_formid, fid, lvli_list_rows)
                    if not pq_try:
                        pq_try = gmrw_by_formid.get(fid)
                    if pq_try:
                        lvli_formid = fid
                        pq2 = pq_try
                        break

                if not lvli_formid and filtered:
                    lvli_formid = filtered[0]

                extra["cobjReferencedByLvli"] = lvli_formid
                if pq2:
                    extra["cobjLvliToGmrwParentQuest"] = pq2

                if lvli_formid:
                    pq2 = lvli_to_gmrw_parentquest(
                        lvli_refby_rows,
                        gmrw_by_formid,
                        lvli_formid,
                        lvli_list_rows
                    )

                    # Extra fallback: if LVLI_Refs has no :GMRW refs, try resolving directly from the LVLI FormID
                    # using the "any ref formid -> parentquest" map.
                    if not pq2:
                        pq2 = gmrw_by_formid.get(lvli_formid)

                    extra["cobjLvliToGmrwParentQuest"] = pq2

                    if pq2:
                        parsed2 = parse_parentquest_label(pq2)
                        if parsed2:
                            lk2, ln2 = parsed2
                            how_from_parentquest = f"Complete the {lk2}: {ln2}"
                            how_event = how_from_parentquest

            extra.update({
                "cobjGNAM_EDID": gnam_edid,
                "cobjGNAM_FULL": gnam_full,
                "cobjGNAM_FormID": gnam_form,
            })

            # --- COBJ GNAM -> CHALLENGE unlock ---
            # Example: GNAM_EDID = Challenge_Lifetime_... and GNAM_FULL = "Build decorative furnishings..."
            if gnam_edid.startswith("Challenge_") or (gnam_form and gnam_full and "CHAL:" in gnam_full):
                               # Resolve CHAL strictly by EDID as exported (no prefix stripping)
                chal_key = gnam_edid
                row = chal_by_edid.get(chal_key)

                if row:
                    chal_edid2 = (row.get("EDID") or "").strip()
                    full = clean_full(row.get("FULL")) or clean_full(gnam_full) or clean_full(chal_edid2) or clean_full(chal_key) or "Challenge"
                    cnam = (row.get("CNAM") or "").strip() or "Challenge"

                    # Drill Complex counters for GNAM-routed challenges too
                    mcount = re.search(r"_Enc\d+_(\d+)\b", chal_edid2, flags=re.IGNORECASE)
                    if mcount:
                        full = f"{full} x{mcount.group(1)}"

                    extra.update({"chalEdid": chal_edid2, "chalCNAM": cnam, "chalFULL": full})
                    if cnam.lower() == "challenge":
                        return f"Complete the Challenge:\n{full}", "N/A", None, "challenge", extra
                    return f"Complete the {cnam} Challenge:\n{full}", "N/A", None, "challenge", extra

                # If CHAL row not found, still treat as challenge unlock
                fallback_full = clean_full(gnam_full) or clean_full(gnam_edid) or "Challenge"
                return f"Complete the Challenge:\n{fallback_full}", "N/A", None, "challenge", extra

        # --- Otherwise: treat as BOOK-drop event/activity title recipe ---
        prefer_lvli = None
        if isinstance(extra.get("bookLvliPickedViaEntries"), str):
            prefer_lvli = extra.get("bookLvliPickedViaEntries")

        dr = lvli_drop_rate_from_cobj_lvli(
            cobj_rows, lvli_entry_rows, lvli_list_rows, glob_rows, cobj_formid,
            prefer_lvli_formid=prefer_lvli
        )
        return how_event, (dr or "N/A"), None, "event_activity", extra

    # --- HasLearnedRecipe without [COBJ:] ---
    if "HasLearnedRecipe(" in joined:
        return "Unlocks after learning the required plan.", "N/A", None, "learned", extra

    return "Unlock condition present (unclassified).", "N/A", None, "other", extra


def git_show_json(rev: str, path: str) -> Optional[dict]:
    try:
        out = subprocess.check_output(["git", "show", f"{rev}:{path}"], stderr=subprocess.DEVNULL)
        return json.loads(out.decode("utf-8"))
    except Exception:
        return None


def build_patchlog(prev: Optional[dict], curr: dict) -> dict:
    def index_by_id(items: List[dict]) -> Dict[str, dict]:
        return {str(x.get("formId")): x for x in items if x.get("formId")}

    prev_items = index_by_id(prev.get("items", [])) if prev else {}
    curr_items = index_by_id(curr.get("items", []))

    added = [k for k in curr_items.keys() if k not in prev_items]
    removed = [k for k in prev_items.keys() if k not in curr_items]
    changed: List[str] = []

    for k in curr_items.keys():
        if k in prev_items:
            a = prev_items[k]
            b = curr_items[k]
            fields = ("edid", "title", "titleMale", "titleFemale", "isPrefix", "isSuffix", "howToObtain", "dropRate", "tradeable", "cutContent", "unlockType")
            if any(a.get(f) != b.get(f) for f in fields):
                changed.append(k)

    return {
        "generatedAt": now_iso(),
        "counts": {
            "prev": len(prev_items),
            "curr": len(curr_items),
            "added": len(added),
            "removed": len(removed),
            "changed": len(changed),
        },
        "addedFormIds": added[:500],
        "removedFormIds": removed[:500],
        "changedFormIds": changed[:500],
    }

def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--tsv-root", required=False, default=None)

    ap.add_argument("--cmpt", action="append", required=False)
    ap.add_argument("--plyt", action="append", required=False)
    ap.add_argument("--book", action="append", required=False)
    ap.add_argument("--cobj", action="append", required=False)
    ap.add_argument("--glob", action="append", required=False)
    ap.add_argument("--gmrw", action="append", required=False)
    ap.add_argument("--lvli", action="append", required=False)
    ap.add_argument("--chal", action="append", required=False)
    ap.add_argument("--cndf", action="append", required=False)
    ap.add_argument("--entm", action="append", required=False)

    ap.add_argument("--seasons", required=False, default=None)
    ap.add_argument("--outdir", required=True)

    args = ap.parse_args()

    args.cmpt = _autofill_paths(args.tsv_root, args.cmpt, ["**/*CMPT*.tsv"])
    args.plyt = _autofill_paths(args.tsv_root, args.plyt, ["**/*PLYT*.tsv", "**/*Player*Title*.tsv", "**/*PlayerTitles*.tsv"])
    args.book = _autofill_paths(args.tsv_root, args.book, ["**/*BOOK*.tsv"])
    args.cobj = _autofill_paths(args.tsv_root, args.cobj, ["**/*COBJ*.tsv"])
    args.glob = _autofill_paths(args.tsv_root, args.glob, ["**/*GLOB*.tsv"])
    args.gmrw = _autofill_paths(args.tsv_root, args.gmrw, ["**/*GMRW*.tsv"])
    args.lvli = _autofill_paths(args.tsv_root, args.lvli, ["**/*LVLI*.tsv"])
    args.chal = _autofill_paths(args.tsv_root, args.chal, ["**/*CHAL*.tsv"])
    args.cndf = _autofill_paths(args.tsv_root, args.cndf, ["**/*CNDF*.tsv"])
    args.entm = _autofill_paths(args.tsv_root, args.entm, ["**/*ENTM*.tsv"])

    missing = []
    if not args.cmpt: missing.append("--cmpt (or auto via --tsv-root)")
    if not args.plyt: missing.append("--plyt (or auto via --tsv-root)")
    if not args.book: missing.append("--book (or auto via --tsv-root)")
    if not args.cobj: missing.append("--cobj (or auto via --tsv-root)")
    if not args.glob: missing.append("--glob (or auto via --tsv-root)")
    if not args.gmrw: missing.append("--gmrw (or auto via --tsv-root)")
    if not args.lvli: missing.append("--lvli (or auto via --tsv-root)")
    if not args.chal: missing.append("--chal (or auto via --tsv-root)")
    if not args.cndf: missing.append("--cndf (or auto via --tsv-root)")
    # ENTM is optional for pure title JSON, but required for images manifest
    if not args.entm: print("[WARN] No ENTM TSV provided; titles_images_manifest.json will be empty.", file=sys.stderr)
    if missing:
        raise SystemExit("Missing required TSV inputs: " + ", ".join(missing))

    os.makedirs(args.outdir, exist_ok=True)

    # ------------------------------------------------------------
    # Release dates
    # - If item already existed in dist, keep its releaseDate
    # - Else if override exists, use override
    # - Else assign today's date (UTC)
    # ------------------------------------------------------------
    overrides = load_release_overrides(args.tsv_root)

    prev_camp_release = load_previous_release_dates(os.path.join(args.outdir, "titles_camp.json"))
    prev_player_release = load_previous_release_dates(os.path.join(args.outdir, "titles_player.json"))

    today_str = today_ymd_utc()

    seasons = {}
    if args.seasons and os.path.isfile(args.seasons):
        seasons = seasons_map(args.seasons)

    cmpt_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.cmpt], "FormID")
    plyt_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.plyt], "FormID")
    book_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.book], "FormID")
    cobj_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.cobj], "FormID")
    glob_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.glob], "FormID")
    gmrw_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.gmrw], "FormID")
    chal_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.chal], "FormID")
    cndf_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.cndf], "FormID")

    # ------------------------------------------------------------
    # ENTM storefront DDS index (safe even if ENTM TSV missing)
    # ------------------------------------------------------------
    entm_dds_by_edid: Dict[str, List[str]] = {}
    if args.entm:
        entm_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.entm], "FormID")
        entm_dds_by_edid = entm_storefront_dds_index(entm_rows)

    # LVLI split (List vs Entries vs Referenced-by).
    lvli_list_rows: List[Dict[str, str]] = []
    lvli_entry_rows: List[Dict[str, str]] = []
    lvli_refby_rows: List[Dict[str, str]] = []

    for p in args.lvli:
        rows = read_tsv_rows(p)
        if not rows:
            continue

        headers = set(rows[0].keys())

        # Refs file: LVLI_Refs.tsv
        if "ReferencedByCount" in headers:
            lvli_refby_rows.extend(rows)
            continue

        # Entries file: LVLI_Entries.tsv
        if ("EntryIndex" in headers) or ("LVLO_Reference" in headers):
            lvli_entry_rows.extend(rows)
            continue

        # List file: LVLI_List.tsv (everything else LVLI-ish)
        lvli_list_rows.extend(rows)

    # build lookup maps AFTER all TSVs are loaded
    tradeable_by_book = book_tradeable_map(book_rows)
    gmrw_by_token = gmrw_parentquest_map(gmrw_rows)
    gmrw_by_formid = gmrw_parentquest_by_any_ref_formid_map(gmrw_rows)
    gmrw_by_ref_formid = gmrw_by_formid
    chal_by_id, chal_by_edid = chal_maps(chal_rows)

    # CNDF
    cndf_by_id: Dict[str, Dict[str, str]] = {}
    for r in cndf_rows:
        fid = (r.get("FormID") or "").strip().upper()
        if fid:
            cndf_by_id[fid] = r

    # CAMP
    camp_items: List[Dict[str, Any]] = []
    for r in cmpt_rows:
        form_id = (r.get("FormID") or "").strip()
        edid = (r.get("EDID") or "").strip()
        title = (r.get("ANAM - Title") or "").strip()

        is_prefix_s = (r.get("PTPR - Is Prefix") or "").strip()
        is_suffix_s = (r.get("PTSU - Is Suffix") or "").strip()
        is_prefix = (is_prefix_s == "1" or is_prefix_s.lower() == "true")
        is_suffix = (is_suffix_s == "1" or is_suffix_s.lower() == "true")

        conds = extract_conditions(r)

        how, dr, sn, unlock_type, extra = compute_unlock_and_rates(
            kind="camp",
            title_display=title,
            edid=edid,
            conds=conds,
            seasons=seasons,
            gmrw_by_token=gmrw_by_token,
            gmrw_by_formid=gmrw_by_formid,
            book_rows=book_rows,
            lvli_refby_rows=lvli_refby_rows,
            glob_rows=glob_rows,
            cobj_rows=cobj_rows,
            lvli_entry_rows=lvli_entry_rows,
            lvli_list_rows=lvli_list_rows,
            chal_by_id=chal_by_id,
            chal_by_edid=chal_by_edid,
            cndf_by_id=cndf_by_id,
        )

        tradeable = False  # camp default
        k_edid = _norm_key(edid)
        k_title = _norm_key(title)
        if k_edid in tradeable_by_book:
            tradeable = tradeable_by_book[k_edid]
        elif k_title in tradeable_by_book:
            tradeable = tradeable_by_book[k_title]

        image_url = storefront_webp_url_from_extra("camp", extra)

        fid8 = (form_id or "").strip().upper()

        if fid8 in prev_camp_release:
            release_date = prev_camp_release[fid8]
        elif fid8 in overrides:
            release_date = overrides[fid8]
        else:
            release_date = today_str

        release_year = int(release_date[:4])

        camp_items.append({
            "formId": form_id,
            "edid": edid,
            "title": title,
            "imageUrl": image_url,
            "isPrefix": is_prefix,
            "isSuffix": is_suffix,
            "affixType": ("Prefix/Suffix" if (is_prefix and is_suffix) else "Prefix" if is_prefix else "Suffix" if is_suffix else "-"),
            "conditions": conds,
            "condCount": len(conds),
            "howToObtain": how,
            "dropRate": dr,
            "releaseDate": release_date,
            "releaseYear": release_year,
            "tradeable": tradeable,
            "unlockType": unlock_type,
            "seasonNumber": sn,
            "cutContent": starts_cut(edid),
            "debug": extra,
        })

    # PLAYER
    player_items: List[Dict[str, Any]] = []
    for r in plyt_rows:
        form_id = (r.get("FormID") or "").strip()
        edid = (r.get("EDID - Editor ID") or r.get("EDID") or "").strip()
        title_m = (r.get("ANAM - Male Title") or "").strip()
        title_f = (r.get("BNAM - Female Title") or "").strip()
        title_display = title_m or title_f

        is_prefix_s = (r.get("PTPR - Is Prefix") or "").strip()
        is_suffix_s = (r.get("PTSU - Is Suffix") or "").strip()
        is_prefix = (is_prefix_s == "1" or is_prefix_s.lower() == "true")
        is_suffix = (is_suffix_s == "1" or is_suffix_s.lower() == "true")

        conds = extract_conditions(r)

        how, dr, sn, unlock_type, extra = compute_unlock_and_rates(
            kind="player",
            title_display=title_display,
            edid=edid,
            conds=conds,
            seasons=seasons,
            gmrw_by_token=gmrw_by_token,
            gmrw_by_formid=gmrw_by_formid,
            book_rows=book_rows,
            lvli_refby_rows=lvli_refby_rows,
            glob_rows=glob_rows,
             cobj_rows=cobj_rows,
            lvli_entry_rows=lvli_entry_rows,
            lvli_list_rows=lvli_list_rows,
            chal_by_id=chal_by_id,
            chal_by_edid=chal_by_edid,
            cndf_by_id=cndf_by_id,
        )

        tradeable = False  # player default
        k_edid = _norm_key(edid)
        k_title = _norm_key(title_display)
        if k_edid in tradeable_by_book:
            tradeable = tradeable_by_book[k_edid]
        elif k_title in tradeable_by_book:
            tradeable = tradeable_by_book[k_title]

        image_url = storefront_webp_url_from_extra("player", extra)

        fid8 = (form_id or "").strip().upper()

        if fid8 in prev_player_release:
            release_date = prev_player_release[fid8]
        elif fid8 in overrides:
            release_date = overrides[fid8]
        else:
            release_date = today_str

        release_year = int(release_date[:4])

        player_items.append({
            "formId": form_id,
            "edid": edid,
            "titleMale": title_m,
            "imageUrl": image_url,
            "titleFemale": title_f,
            "title": title_display,
            "isPrefix": is_prefix,
            "isSuffix": is_suffix,
            "affixType": ("Prefix/Suffix" if (is_prefix and is_suffix) else "Prefix" if is_prefix else "Suffix" if is_suffix else "-"),
            "conditions": conds,
            "condCount": len(conds),
            "howToObtain": how,
            "dropRate": dr,
            "releaseDate": release_date,
            "releaseYear": release_year,
            "tradeable": tradeable,
            "unlockType": unlock_type,
            "seasonNumber": sn,
            "cutContent": starts_cut(edid),
            "debug": extra,
        })

    camp_items.sort(key=lambda x: (x.get("cutContent", False), (x.get("title") or "").lower()))
    player_items.sort(key=lambda x: (x.get("cutContent", False), (x.get("title") or "").lower()))

    camp_json = {"generatedAt": now_iso(), "type": "camp_titles", "items": camp_items}
    player_json = {"generatedAt": now_iso(), "type": "player_titles", "items": player_items}

    camp_path = os.path.join(args.outdir, "titles_camp.json")
    player_path = os.path.join(args.outdir, "titles_player.json")
    data_path = os.path.join(args.outdir, "titles_data.json")

    with open(camp_path, "w", encoding="utf-8") as f:
        json.dump(camp_json, f, ensure_ascii=False, separators=(",", ":"), indent=2)
    with open(player_path, "w", encoding="utf-8") as f:
        json.dump(player_json, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    # Back-compat: combined file for older pages that still fetch titles_data.json
    combined_items = []
    for it in camp_items:
        x = dict(it)
        x["titleType"] = "camp"
        combined_items.append(x)
    for it in player_items:
        x = dict(it)
        x["titleType"] = "player"
        combined_items.append(x)

    combined_json = {
        "generatedAt": now_iso(),
        "type": "titles_combined",
        "items": combined_items,
    }
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(combined_json, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    prev_camp = git_show_json("HEAD^", "dist/titles_camp.json")
    prev_player = git_show_json("HEAD^", "dist/titles_player.json")

    patchlog = {
        "generatedAt": now_iso(),
        "camp": build_patchlog(prev_camp, camp_json),
        "player": build_patchlog(prev_player, player_json),
    }

    # ============================================================
    # NEW: titles_images_manifest.json (ENTM storefront DDS tasks)
    # ============================================================
    images_tasks: List[Dict[str, Any]] = []

    def _add_image_tasks(title_type: str, items: List[Dict[str, Any]]) -> None:
        for it in items:
            formid = _formid8_lower(it.get("formId") or "")
            edid = (it.get("edid") or "").strip()

            dbg = it.get("debug") or {}

            # Only use the selected title image entitlement
            img_ent = dbg.get("imageEntitlementEdid")
            if not img_ent:
                continue

            ent_edids = [img_ent]

            dds_paths: List[str] = []
            for e in ent_edids:
                e2 = (str(e) or "").strip()
                if not e2:
                    continue
                dds_paths.extend(entm_dds_by_edid.get(e2.lower(), []))

            # de-dupe paths
            seen = set()
            dds_out: List[str] = []
            for p in dds_paths:
                p2 = _norm_dds_path(p)
                if not p2 or p2 in seen:
                    continue
                seen.add(p2)
                dds_out.append(p2)

            if not dds_out:
                continue

            images_tasks.append({
                "titleType": title_type,
                "formId": formid,
                "edid": edid,
                "entitlementEdids": [str(x) for x in ent_edids if str(x).strip()],
                "ddsPaths": dds_out,
            })

    _add_image_tasks("camp", camp_items)
    _add_image_tasks("player", player_items)

    images_manifest = {
        "generatedAt": now_iso(),
        "tasks": images_tasks,
    }

    images_manifest_path = os.path.join(args.outdir, "titles_images_manifest.json")
    with open(images_manifest_path, "w", encoding="utf-8") as f:
        json.dump(images_manifest, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    patchlog_path = os.path.join(args.outdir, "titles_patchlog.json")
    with open(patchlog_path, "w", encoding="utf-8") as f:
        json.dump(patchlog, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    manifest = {
        "generatedAt": now_iso(),
        "outputs": {
            "camp": {"file": "titles_camp.json", "count": len(camp_items)},
            "player": {"file": "titles_player.json", "count": len(player_items)},
            "patchlog": {"file": "titles_patchlog.json"},
            "imagesManifest": {"file": "titles_images_manifest.json", "count": len(images_tasks)},
        },
        "sources": {
            "cmpt": [os.path.basename(p) for p in args.cmpt],
            "plyt": [os.path.basename(p) for p in args.plyt],
            "book": [os.path.basename(p) for p in args.book],
            "cobj": [os.path.basename(p) for p in args.cobj],
            "glob": [os.path.basename(p) for p in args.glob],
            "gmrw": [os.path.basename(p) for p in args.gmrw],
            "lvli": [os.path.basename(p) for p in args.lvli],
            "chal": [os.path.basename(p) for p in args.chal],
            "cndf": [os.path.basename(p) for p in args.cndf],
            "entm": [os.path.basename(p) for p in args.entm] if args.entm else [],
            "seasons": os.path.basename(args.seasons) if args.seasons else None,
        },
    }

    # ------------------------------------------------------------
    # Storefront index (derived from images_tasks instead of filesystem)
    # CI-safe: does not depend on export/storefront existing.
    # ------------------------------------------------------------

    titles_player_webps: List[str] = []
    titles_camp_webps: List[str] = []

    for task in images_tasks:
        ent_ids = task.get("entitlementEdids") or []
        if not ent_ids:
            continue

        # Normalize entitlement -> filename
        ent = str(ent_ids[0]).strip().lower().replace("_entm_", "_")
        filename = f"{ent}.webp"

        tt = (task.get("titleType") or "").strip().lower()
        if tt == "player":
            titles_player_webps.append(filename)
        elif tt == "camp":
            titles_camp_webps.append(filename)

    manifest["storefront"] = {
        "titlesPlayer": sorted(set(titles_player_webps)),
        "titlesCamp": sorted(set(titles_camp_webps)),
    }
    
    manifest_path = os.path.join(args.outdir, "titles_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

