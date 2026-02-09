#!/usr/bin/env python3
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

RE_HAS_ENTITLEMENT = re.compile(r"\bHasEntitlement\(", re.IGNORECASE)
RE_HAS_COMPLETED_CHAL = re.compile(r"\bHasCompletedChallenge\(", re.IGNORECASE)
RE_IS_TRUE_CNDF = re.compile(r"\bIsTrueForConditionForm\(", re.IGNORECASE)

RE_QUEST_COMPLETED = re.compile(r"\bGetQuestCompleted\(", re.IGNORECASE)
RE_NUM_TIMES_COMPLETED = re.compile(r"\bGetNumTimesCompletedQuest\(", re.IGNORECASE)

RE_SCORE_SEASON = re.compile(r"\bSCORE[_-]?S(\d+)(?:\b|_)", re.IGNORECASE)
RE_MINISEASON = re.compile(r"\bSCORE_MiniSeason\b", re.IGNORECASE)
RE_ATX = re.compile(r"\bATX_", re.IGNORECASE)
RE_COMMUNITY = re.compile(r"\bCommunity_", re.IGNORECASE)

RE_FORM_REF = re.compile(r"\[([A-Z]{4}):([0-9A-F]{8})\]", re.IGNORECASE)
RE_QUOTED = re.compile(r'"([^"]+)"')
RE_COBJ_REF = re.compile(r"\[COBJ:[0-9A-F]{8}\]", re.IGNORECASE)


def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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
    )
    alias_edid_keys = (
        "CMPT_EDID", "PLYT_EDID", "BOOK_EDID", "COBJ_EDID",
        "GLOB_EDID", "GMRW_EDID", "LVLI_EDID", "CHAL_EDID",
    )
    alias_full_keys = (
        "CMPT_FULL", "PLYT_FULL", "BOOK_FULL", "COBJ_FULL",
        "GLOB_FULL", "GMRW_FULL", "LVLI_FULL", "CHAL_FULL",
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
    return any(e.startswith(p) for p in CUT_PREFIXES)


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
    rows = read_tsv_rows(seasons_path)
    m: Dict[int, str] = {}
    for r in rows:
        sn = r.get("SeasonNumber") or r.get("Season") or r.get("Number") or ""
        name = r.get("SeasonName") or r.get("Name") or r.get("ScoreboardName") or ""
        n = safe_int(sn, 0)
        if n and name:
            m[n] = name
    return m


def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s


def book_tradeable_map(book_rows: List[Dict[str, str]]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    for r in book_rows:
        edid = (r.get("EDID") or "").strip()
        full = (r.get("FULL") or "").strip()

        row_blob = " ".join(str(v) for v in r.values() if v)
        non_trade = "nonplayertradeable" in row_blob.lower()
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

def gmrw_parentquest_by_formid_map(gmrw_rows: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Strict: map GMRW FormID -> ParentQuest (display text when available)
    Used for BOOK -> LVLI -> (ReferencedBy) -> GMRW resolution.

    IMPORTANT:
      - Ignore CUT/ZZZ/etc GMRW rows here so ref-path resolution doesn't pick cut content.
    """
    out: Dict[str, str] = {}
    for r in gmrw_rows:
        edid = (r.get("EDID") or "").strip()
        if edid and starts_cut(edid):
            continue

        fid = (r.get("FormID") or "").strip().upper()
        pq = _gmrw_parentquest_from_row(r)
        if fid and pq:
            out[fid] = pq
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

def book_lvli_gmrw_parentquest(
    book_rows: List[Dict[str, str]],
    lvli_refby_rows: List[Dict[str, str]],
    gmrw_by_formid: Dict[str, str],
    book_formid: str
) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Strict path:
      BOOK(FormID) -> BOOK.Ref* :LVLI -> LVLI_ReferencedBy.Ref* :GMRW -> GMRW.Ref* contains "Event:"/"Activity:"
    Returns (parentquest_string_or_None, debug_dict)
    """
    dbg: Dict[str, Any] = {
        "bookFormId": (book_formid or "").strip().upper(),
        "bookFound": False,
        "lvliIds": [],
        "lvliPicked": None,
        "lvliRefByFound": False,
        "gmrwIds": [],
        "gmrwPicked": None,
        "gmrwLabelFound": False,
    }

    book_row = _find_row_by_formid(book_rows, book_formid)
    if not book_row:
        return None, dbg
    dbg["bookFound"] = True

    lvli_ids = _extract_formids_from_ref_fields(book_row, ":LVLI")
    dbg["lvliIds"] = lvli_ids
    if not lvli_ids:
        return None, dbg

    lvli_id = lvli_ids[0]
    dbg["lvliPicked"] = lvli_id

    lvli_refby = None
    for r in lvli_refby_rows:
        if (r.get("LVLI_FormID") or "").strip().upper() == lvli_id:
            lvli_refby = r
            break
    if not lvli_refby:
        return None, dbg
    dbg["lvliRefByFound"] = True

    gmrw_ids = _extract_formids_from_ref_fields(lvli_refby, ":GMRW")
    dbg["gmrwIds"] = gmrw_ids
    if not gmrw_ids:
        return None, dbg

    gmrw_id = gmrw_ids[0]
    dbg["gmrwPicked"] = gmrw_id

    pq = gmrw_by_formid.get(gmrw_id)
    dbg["gmrwLabelFound"] = bool(pq)
    return pq, dbg

def chal_maps(chal_rows: List[Dict[str, str]]) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    by_id: Dict[str, Dict[str, str]] = {}
    by_edid: Dict[str, Dict[str, str]] = {}
    for r in chal_rows:
        fid = (r.get("FormID") or "").strip().upper()
        edid = (r.get("EDID") or "").strip()
        if fid:
            by_id[fid] = r
        if edid:
            by_edid[edid] = r
    return by_id, by_edid


def cobj_token_from_condition(conds: List[str]) -> Optional[str]:
    for s in conds:
        if "[COBJ:" not in s:
            continue
        m = re.search(r"\(([^)\s]+)", s)
        if not m:
            continue
        arg = m.group(1)
        token = arg.split("_", 1)[0]
        if token:
            return token
    return None

def parse_parentquest_label(pq: str) -> Optional[Tuple[str, str]]:
    # Preferred format: quoted label somewhere in the string
    m = RE_QUOTED.search(pq)
    if m:
        label = m.group(1).strip()  # "Event: X" / "Activity: Y"
    else:
        # Fallback: plain text contains Event:/Activity: without quotes
        m2 = re.search(r"\b(Event|Activity|Bounty\s*Hunting)\s*:\s*([^\r\n|]+)", pq)
        if not m2:
            return None
        label = f"{m2.group(1)}: {m2.group(2).strip()}"

    # Normal: "Event: X" / "Activity: Y"
    if ":" in label:
        left, right = label.split(":", 1)
        kind = left.strip()
        name = right.strip()
        if not kind or not name:
            return None
        return kind, name

    # Fallback: quoted FULL from a QUST ref, e.g. "Lucky Strike"
    # Treat it as a quest name.
    label = label.strip()
    if not label:
        return None
    return "Quest", label

def glob_drop_rate_by_edid(glob_rows: List[Dict[str, str]], glob_edid: str) -> Optional[str]:
    """Strict: match GLOB.EDID exactly, DropRate = 100 - FLTV"""
    glob_edid = (glob_edid or "").strip()
    if not glob_edid:
        return None

    for r in glob_rows:
        if (r.get("EDID") or "").strip() != glob_edid:
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

def _glob_formid_from_lvli_global_field(s: str) -> Optional[str]:
    # Example field: "0089EA90:SpawnChance_Cnone_ActivityCampTitle:GLOB"
    s = (s or "").strip()
    if not s:
        return None
    m = re.match(r"^([0-9A-Fa-f]{8}):", s)
    return m.group(1).upper() if m else None


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


def lvli_drop_rate_from_cobj_lvli(
    cobj_rows: List[Dict[str, str]],
    lvli_rows: List[Dict[str, str]],
    glob_rows: List[Dict[str, str]],
    cobj_formid: str
) -> Optional[str]:
    """
    BOOK-type COBJ path for titles:
      COBJ(FormID) -> GNAM_FormID (BOOK FormID) -> find LVLI entries referencing that BOOK in LVLO_Reference

    Drop-rate priority (your rule):
      1) if LVOG_ChanceNoneGlobal (entry) exists OR LVLG_ChanceNoneGlobal (list) exists:
           look up that GLOB FormID -> pct = 100 - FLTV
      2) else use LVOV_ChanceNone:
           if 0 => 100%
           else pct = 100 - LVOV
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

    # 3) Find LVLI row(s) referencing that BOOK
    matches = [r for r in lvli_rows if book_formid in (r.get("LVLO_Reference") or "")]
    if not matches:
        return None

    # Prefer a match that has a global override (entry global first, then list global)
    def _rank(row: Dict[str, str]) -> Tuple[int, str]:
        eg = (row.get("LVOG_ChanceNoneGlobal") or "").strip()
        lg = (row.get("LVLG_ChanceNoneGlobal") or "").strip()
        if eg:
            return (0, eg)
        if lg:
            return (1, lg)
        return (2, "")

    matches.sort(key=_rank)
    best = matches[0]

    # 4) Global override first (global-first rule, order matters)
    # Priority:
    #   1) LVOG_ChanceNoneGlobal
    #   2) LVOC_ChanceNoneCurve containing a :GLOB reference
    #   3) LVLG_ChanceNoneGlobal
    #   4) LVCT_ChanceNoneCurve containing a :GLOB reference
    candidates: List[str] = []

    lvog = (best.get("LVOG_ChanceNoneGlobal") or "").strip()
    if lvog:
        candidates.append(lvog)

    lvoc = (best.get("LVOC_ChanceNoneCurve") or "").strip()
    if lvoc and ":GLOB" in lvoc:
        candidates.append(lvoc)

    lvlg = (best.get("LVLG_ChanceNoneGlobal") or "").strip()
    if lvlg:
        candidates.append(lvlg)

    lvct = (best.get("LVCT_ChanceNoneCurve") or "").strip()
    if lvct and ":GLOB" in lvct:
        candidates.append(lvct)

    for glob_field in candidates:
        gfid = _glob_formid_from_lvli_global_field(glob_field)
        if not gfid:
            continue
        dr = glob_drop_rate_by_formid(glob_rows, gfid)
        if dr:
            return dr

    # 5) Fallback: LVOV_ChanceNone
    chance_none = safe_float(best.get("LVOV_ChanceNone") or "", None)
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

def prettify_token_words(token: str) -> str:
    s = token.replace("_", " ").strip()
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    s = re.sub(r"\\s+", " ", s).strip()
    return s


def parse_entitlement_edid_from_condition(cond: str) -> Optional[str]:
    m = re.search(r"HasEntitlement\(\s*([^\s\)]+)", cond, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip()


def parse_quest_name_from_condition(cond: str) -> Optional[str]:
    m = RE_QUOTED.search(cond)
    return m.group(1).strip() if m else None


def parse_rhs_number(cond: str) -> Optional[float]:
    m = re.search(r"=\\s*([0-9]+(?:\\.[0-9]+)?)", cond)
    return safe_float(m.group(1), None) if m else None


def parse_chal_formid_from_condition(cond: str) -> Optional[str]:
    for typ, fid in RE_FORM_REF.findall(cond):
        if typ.upper() == "CHAL":
            return fid.upper()
    return None

def parse_cobj_formid_from_condition(cond: str) -> Optional[str]:
    for typ, fid in RE_FORM_REF.findall(cond):
        if typ.upper() == "COBJ":
            return fid.upper()
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
    lvli_rows: List[Dict[str, str]],
    chal_by_id: Dict[str, Dict[str, str]],
    chal_by_edid: Dict[str, Dict[str, str]],
    cndf_by_id: Dict[str, Dict[str, str]],
) -> Tuple[str, str, Optional[int], str, Dict[str, Any]]:

    extra: Dict[str, Any] = {}

    if not conds:
        return "Unlocked by Default", "100%", None, "default", extra

    joined = " ".join(conds)

        # Expand CNDF if present in any condition line (attach into debug/extra)
    cndf_formid = None
    for c in conds:
        if "[CNDF:" in c:
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
                    how = "Complete the following challenges to unlock this camp title:\n" + "\n".join(f"- {n}" for n in ordered)
                    return how, "100%", None, "challenge", extra

    # --- Challenges: HasCompletedChallenge -> CHAL by FormID ---
    if RE_HAS_COMPLETED_CHAL.search(joined):
        chal_fid = None
        for c in conds:
            if "HasCompletedChallenge" not in c:
                continue
            chal_fid = parse_chal_formid_from_condition(c) or chal_fid
        if chal_fid and chal_fid in chal_by_id:
            row = chal_by_id[chal_fid]
            full = (row.get("FULL") or "").strip() or (row.get("EDID") or "").strip()
            cnam = (row.get("CNAM") or "").strip() or "Challenge"
            extra.update({"chalFormId": chal_fid, "chalEdid": (row.get("EDID") or "").strip(), "chalCNAM": cnam, "chalFULL": full})
            if cnam.lower() == "challenge":
                return f"Complete the Challenge {full}", "100%", None, "challenge", extra
            return f"Complete the {cnam} Challenge {full}", "100%", None, "challenge", extra
        return "Complete the Challenge.", "100%", None, "challenge", extra

    # --- CNDF-based challenge: IsTrueForConditionForm(Challenge_*_ConditionForm) -> CHAL by EDID ---
    if RE_IS_TRUE_CNDF.search(joined):
        for c in conds:
            if "IsTrueForConditionForm" not in c:
                continue
            m = re.search(r"IsTrueForConditionForm\(\s*([^\s\)]+)", c, flags=re.IGNORECASE)
            if not m:
                continue
            arg = m.group(1).strip()
            if arg.endswith("_ConditionForm"):
                chal_edid = arg[:-len("_ConditionForm")]
                if chal_edid in chal_by_edid:
                    row = chal_by_edid[chal_edid]
                    full = (row.get("FULL") or "").strip() or chal_edid
                    cnam = (row.get("CNAM") or "").strip() or "Challenge"
                    extra.update({"chalEdid": chal_edid, "chalCNAM": cnam, "chalFULL": full})
                    if cnam.lower() == "challenge":
                        return f"Complete the Challenge {full}", "100%", None, "challenge", extra
                    return f"Complete the {cnam} Challenge {full}", "100%", None, "challenge", extra
        # else: fall through (IsTrueForConditionForm used for other things)

    # --- Quests ---
    if RE_NUM_TIMES_COMPLETED.search(joined):
        for c in conds:
            if "GetNumTimesCompletedQuest" not in c:
                continue
            qname = parse_quest_name_from_condition(c) or "Unknown Quest"
            n = parse_rhs_number(c)
            if n is None:
                return f'Complete the quest "{qname}".', "100%", None, "quest", extra
            n_int = int(round(n))
            if n_int <= 1:
                return f'Complete the quest "{qname}".', "100%", None, "quest", extra
            return f'Complete the quest "{qname}" {n_int} times.', "100%", None, "quest", extra

    if RE_QUEST_COMPLETED.search(joined):
        qname = parse_quest_name_from_condition(joined) or "Unknown Quest"
        return f'Complete the quest "{qname}".', "100%", None, "quest", extra

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

        # Priority: Community -> MiniSeason -> SCORE season -> ATX -> other
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
            tok2 = re.sub(r"^\\d{4}_", "", tok2)  # drop leading year
            name = prettify_token_words(tok2)
            extra.update({"miniSeasonRaw": tok2, "miniSeasonName": name})
            return f"Claim from the Mini Season - {name}", "100%", None, "miniseason", extra

        # SCORE season
        season_num: Optional[int] = None
        season_edid: Optional[str] = None
        for e in ent_edids:
            m = RE_SCORE_SEASON.search(e)
            if m:
                season_num = safe_int(m.group(1), 0)
                season_edid = e
                break

        if season_num:
            sname = seasons.get(season_num, f"Season {season_num}")
            extra.update({"seasonNumber": season_num, "seasonName": sname})

            if kind == "player":
                return f"Unlock via the Season {season_num} - {sname} Scoreboard.", "100%", season_num, "season", extra

            e_upper = (season_edid or "").upper()
            framed = ("ENDOFSEASONART" in e_upper)
            quoted_name = None
            if not framed:
                # quoted fallback (do NOT treat "Framed ... Gameboard" as Framed Art)
                for c in conds:
                    if "HasEntitlement" in c and (season_edid or "") in c:
                        m = RE_QUOTED.search(c)
                        if m:
                            quoted_name = m.group(1)
                            q = quoted_name.lower()
                            # Only count as Framed Art if it explicitly says "framed art"
                            # and is NOT a gameboard/corkboard item.
                            if ("framed art" in q) and ("gameboard" not in q) and ("corkboard" not in q):
                                framed = True
                            break

            if framed:
                return f"Unlocks when you claim the Framed Art from Season {season_num} - {sname}.", "100%", season_num, "season", extra

            # If the entitlement is the title itself (CAMPTitles_*), it's a normal season-board claim,
            # not "claim the Gameboard" item.
            if "CAMPTITLES" in e_upper and "GAMEBOARD" not in e_upper and "CORKBOARD" not in e_upper:
                return f"Unlocks when you claim this reward from Season {season_num} - {sname}.", "100%", season_num, "season", extra

            # Gameboard bucket (includes CorkBoard etc)
            return f"Unlocks when you claim the Gameboard from Season {season_num} - {sname}.", "100%", season_num, "season", extra

        # ATX standard
        if any(RE_ATX.search(e) for e in ent_edids):
            return "Can be purchased with certain bundles from the Atom Shop.", "100%", None, "atx", extra

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
            if "[COBJ:" in c:
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

                        # If GNAM is a BOOK FormID, resolve Event/Activity via BOOK -> LVLI -> GMRW
            if gnam_form and re.fullmatch(r"[0-9A-F]{8}", gnam_form):
                pq, pq_dbg = book_lvli_gmrw_parentquest(book_rows, lvli_refby_rows, gmrw_by_formid, gnam_form)
                extra["bookLvliGmrw"] = pq_dbg

                if pq:
                    parsed = parse_parentquest_label(pq)
                    if parsed:
                        lk, ln = parsed
                        how_from_parentquest = f"Complete the {lk}: {ln}"
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
                    full = (row.get("FULL") or "").strip() or gnam_full or chal_key
                    cnam = (row.get("CNAM") or "").strip() or "Challenge"
                    extra.update({"chalEdid": (row.get("EDID") or "").strip(), "chalCNAM": cnam, "chalFULL": full})
                    if cnam.lower() == "challenge":
                        return f"Complete the Challenge {full}", "100%", None, "challenge", extra
                    return f"Complete the {cnam} Challenge {full}", "100%", None, "challenge", extra

                # If CHAL row not found, still treat as challenge unlock
                return f"Complete the Challenge {gnam_full or gnam_edid}", "100%", None, "challenge", extra

        # --- Otherwise: treat as BOOK-drop event/activity title recipe ---
        dr = lvli_drop_rate_from_cobj_lvli(cobj_rows, lvli_rows, glob_rows, cobj_formid)
        return how_event, (dr or "N/A"), None, "event_activity", extra

    # --- HasLearnedRecipe without [COBJ:] ---
    if "HasLearnedRecipe(" in joined:
        return "Unlocks after learning the required plan.", "100%", None, "learned", extra

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
    if missing:
        raise SystemExit("Missing required TSV inputs: " + ", ".join(missing))

    os.makedirs(args.outdir, exist_ok=True)

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

    # LVLI split (defs vs referenced-by).
    lvli_rows: List[Dict[str, str]] = []
    lvli_refby_rows: List[Dict[str, str]] = []
    for p in args.lvli:
        rows = read_tsv_rows(p)
        if not rows:
            continue
        headers = set(rows[0].keys())
        if "ReferencedByCount" in headers:
            lvli_refby_rows.extend(rows)
            continue
        lvli_rows.extend(rows)

    # build lookup maps AFTER all TSVs are loaded
    tradeable_by_book = book_tradeable_map(book_rows)
    gmrw_by_token = gmrw_parentquest_map(gmrw_rows)
    gmrw_by_formid = gmrw_parentquest_by_formid_map(gmrw_rows)
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
            lvli_rows=lvli_rows,
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

        camp_items.append({
            "formId": form_id,
            "edid": edid,
            "title": title,
            "isPrefix": is_prefix,
            "isSuffix": is_suffix,
            "affixType": ("Prefix/Suffix" if (is_prefix and is_suffix) else "Prefix" if is_prefix else "Suffix" if is_suffix else "-"),
            "conditions": conds,
            "condCount": len(conds),
            "howToObtain": how,
            "dropRate": dr,
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
        edid = (r.get("EDID - Editor ID") or "").strip()
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
            lvli_rows=lvli_rows,
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

        player_items.append({
            "formId": form_id,
            "edid": edid,
            "titleMale": title_m,
            "titleFemale": title_f,
            "title": title_display,
            "isPrefix": is_prefix,
            "isSuffix": is_suffix,
            "affixType": ("Prefix/Suffix" if (is_prefix and is_suffix) else "Prefix" if is_prefix else "Suffix" if is_suffix else "-"),
            "conditions": conds,
            "condCount": len(conds),
            "howToObtain": how,
            "dropRate": dr,
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
    patchlog_path = os.path.join(args.outdir, "titles_patchlog.json")
    with open(patchlog_path, "w", encoding="utf-8") as f:
        json.dump(patchlog, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    manifest = {
        "generatedAt": now_iso(),
        "outputs": {
            "camp": {"file": "titles_camp.json", "count": len(camp_items)},
            "player": {"file": "titles_player.json", "count": len(player_items)},
            "patchlog": {"file": "titles_patchlog.json"},
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
            "seasons": os.path.basename(args.seasons) if args.seasons else None,
        },
    }
    manifest_path = os.path.join(args.outdir, "titles_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
