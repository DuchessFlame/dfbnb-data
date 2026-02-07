#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple

CUT_PREFIXES = ("DEL", "POST", "CUT", "ZZZ", "ZZZZ")

RE_HAS_ENTITLEMENT = re.compile(r"\bHasEntitlement\(", re.IGNORECASE)
# Season markers appear in multiple shapes in conditions/EDIDs:
#   SCORE_S7, Score_S7, score_s7
#   S7 (bare)
RE_SCORE_SEASON = re.compile(r"\bSCORE[_-]?S(\d+)\b", re.IGNORECASE)
RE_BARE_SEASON = re.compile(r"\bS(\d{1,2})\b", re.IGNORECASE)
RE_ATX = re.compile(r"\bATX_", re.IGNORECASE)
RE_QUEST_COMPLETED = re.compile(r"\bGetQuestCompleted\(", re.IGNORECASE)
RE_QUEST_NAME_IN_QUOTES = re.compile(r'"([^"]+)"')
RE_COBJ_REF = re.compile(r"\[COBJ:[0-9A-F]{8}\]", re.IGNORECASE)

def read_tsv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return [row for row in reader]

def safe_int(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default

def safe_float(s: str, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(s)
    except Exception:
        return default

def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

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
    # accept flexible headers
    for r in rows:
        sn = r.get("SeasonNumber") or r.get("Season") or r.get("Number") or ""
        name = r.get("SeasonName") or r.get("Name") or r.get("ScoreboardName") or ""
        n = safe_int(sn, 0)
        if n and name:
            m[n] = name
    return m

def _norm_key(s: str) -> str:
    # aggressive normalize so "Zen", "zen", "Zen " all match
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s

def book_tradeable_map(book_rows: List[Dict[str, str]]) -> Dict[str, bool]:
    """
    Keys:
      - BOOK.EDID (normalized)
      - BOOK.FULL (normalized)  <-- this is what lets "Zen"/"Boiled"/etc work
    Value:
      - False if keyword contains NonPlayerTradeable
      - True otherwise
    """
    out: Dict[str, bool] = {}
    for r in book_rows:
        edid = (r.get("EDID") or "").strip()
        full = (r.get("FULL") or "").strip()

        kw_count = safe_int(r.get("KeywordCount", "0"))
        kws: List[str] = []
        for i in range(1, kw_count + 1):
            kws.append((r.get(f"KW{i}") or "").strip())

        non_trade = any("nonplayertradeable" in (k or "").lower() for k in kws if k)
        is_tradeable = (not non_trade)

        if edid:
            out[_norm_key(edid)] = is_tradeable
        if full:
            out[_norm_key(full)] = is_tradeable

    return out

def gmrw_parentquest_map(gmrw_rows: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Key = token like MTNM03 (prefix before first _ in EDID)
    Value = ParentQuest string (already human-readable)
    """
    out: Dict[str, str] = {}
    for r in gmrw_rows:
        edid = (r.get("EDID") or "").strip()
        if not edid:
            continue

        token = edid.split("_", 1)[0]
        pq = (r.get("ParentQuest") or "").strip()
        if token and pq:
            out[token] = pq
    return out

def cobj_token_from_condition(conds: List[str]) -> Optional[str]:
    # Find the first COBJ mention and return the token before first underscore inside the function args if present.
    # Condition lines look like: Top:Subject.IsTrueForConditionForm(MTNM03_PlayerTitle_co_CondProxy... [COBJ:...]) = 1.000000
    for s in conds:
        if "[COBJ:" not in s:
            continue
        # extract the argument right after "(" up to first space or ")"
        m = re.search(r"\(([^)\s]+)", s)
        if not m:
            continue
        arg = m.group(1)
        token = arg.split("_", 1)[0]
        if token:
            return token
    return None

def how_to_obtain_from_parentquest(pq: str) -> Optional[str]:
    # pq example: FFZ16_Swatter "Activity: Fly Swatter" [QUST:00029183]
    m = RE_QUEST_NAME_IN_QUOTES.search(pq)
    if not m:
        return None
    label = m.group(1).strip()
    # keep exactly "Event: X" or "Activity: X"
    return label

def find_glob_drop_rate(glob_rows: List[Dict[str, str]], token: Optional[str], title_words: List[str]) -> Optional[str]:
    """
    GLOB TSV has: FormID, EDID, XALG, FNAM, FLTV, ReferencedByCount, Ref1..RefN
    Drop rate rule: value shown is like 95 => 100-95 = 5%
    We search by token in EDID/Refs, else title words in refs.
    """
    needles = []
    if token:
        needles.append(token)
    for w in title_words:
        if w and len(w) >= 3:
            needles.append(w)
    if not needles:
        return None

    for r in glob_rows:
        edid = r.get("EDID", "") or ""
        refs_count = safe_int(r.get("ReferencedByCount", "0"))
        hay = [edid]
        for i in range(1, min(refs_count, 30) + 1):  # cap scan
            hay.append(r.get(f"Ref{i}", "") or "")
        blob = " ".join(hay)
        if not any(n in blob for n in needles):
            continue

        v = r.get("FLTV") or ""
        fv = safe_float(v, None)
        if fv is None:
            continue
        # fv is like 95 -> 5%
        pct = 100.0 - fv
        if pct < 0:
            continue
        if abs(pct - round(pct)) < 1e-6:
            return f"{int(round(pct))}%"
        return f"{pct:.3f}%"
    return None

def lvli_drop_rate_from_cobj_lvli(cobj_rows: List[Dict[str, str]], lvli_rows: List[Dict[str, str]], cobj_token: str) -> Optional[str]:
    """
    Fallback: COBJ -> Ref1/Ref2 includes QuestReward_Titles LVLI -> LVLI LVOV_ChanceNone
    Your COBJ export has lots of columns; the safest fallback is:
      - find COBJ row whose COBJ_EDID starts with token + "_"
      - scan all its fields for something that looks like LVLI EDID "QuestReward_Titles"
      - then in LVLI export find matching LVLI_EDID and read LVOV_ChanceNone
    """
    # 1) find candidate COBJ row
    cand = None
    for r in cobj_rows:
        edid = (r.get("COBJ_EDID") or "").strip()
        if edid.startswith(cobj_token + "_"):
            cand = r
            break
    if not cand:
        return None

    # 2) locate LVLI EDID mention in any cell
    lvli_edid = None
    for k, v in cand.items():
        if not v:
            continue
        if "QuestReward_Titles" in v:
            # try to extract EDID token before first space/quote
            m = re.search(r"(QuestReward_Titles[^ \t\"]+)", v)
            lvli_edid = (m.group(1) if m else "QuestReward_Titles").strip()
            break
    if not lvli_edid:
        lvli_edid = "QuestReward_Titles"

    # 3) find LVLI row and read LVOV_ChanceNone
    for r in lvli_rows:
        if (r.get("LVLI_EDID") or "").strip() != lvli_edid:
            continue
        chance_none = safe_float(r.get("LVOV_ChanceNone") or "", None)
        if chance_none is None:
            return None
        if chance_none == 0:
            return "100%"
        pct = 100.0 - chance_none
        if pct < 0:
            return None
        if abs(pct - round(pct)) < 1e-6:
            return f"{int(round(pct))}%"
        return f"{pct:.3f}%"
    return None

def compute_unlock_and_rates(
    kind: str,
    title_display: str,
    edid: str,
    conds: List[str],
    seasons: Dict[int, str],
    gmrw_by_token: Dict[str, str],
    glob_rows: List[Dict[str, str]],
    cobj_rows: List[Dict[str, str]],
    lvli_rows: List[Dict[str, str]],
) -> Tuple[str, str, Optional[int], Optional[str]]:
    """
    Returns:
      how_to_obtain, drop_rate, season_number, unlock_type
    """
    if not conds:
        return "Unlocked by Default", "100%", None, "default"

    joined = " ".join(conds)

    # QUEST
    if RE_QUEST_COMPLETED.search(joined):
        # Example: Subject.GetQuestCompleted(BURN_SQ02_OutroP2 "When the Rust Settles" [QUST:...]) = 1.000000
        m = RE_QUEST_NAME_IN_QUOTES.search(joined)
        qname = m.group(1) if m else "Unknown Quest"
        return f'Complete the quest "{qname}".', "100%", None, "quest"

    # ENTITLEMENT (SCORE season vs ATX)
    if RE_HAS_ENTITLEMENT.search(joined):
        # Season
        # Season can appear as SCORE_S7 / Score_S7 or as a bare S7 token
        sm = RE_SCORE_SEASON.search(joined)
        if sm:
            season_num = safe_int(sm.group(1), 0)
        else:
            bm = RE_BARE_SEASON.search(joined)
            season_num = safe_int(bm.group(1), 0) if bm else 0

        if season_num:
            sname = seasons.get(season_num, "Unknown")

            # Your rules:
            # - Player titles: "Unlock via the Season {#} - {season name} Scoreboard"
            # - Camp titles:  "Unlocks when you claim the Gameboard or Framed Art from Season {#} - {season name}"
            if kind == "player":
                return f"Unlock via the Season {season_num} - {sname} Scoreboard.", "100%", season_num, "season"

            return f"Unlocks when you claim the Gameboard or Framed Art from Season {season_num} - {sname}.", "100%", season_num, "season"

        # ATX
        if RE_ATX.search(joined):
            return "Can be purchased with certain bundles from the Atom Shop.", "N/A", None, "atx"

        # Other entitlement
        return "Unlocked via account entitlement.", "N/A", None, "entitlement"

    # COBJ proxy -> event/activity (via GMRW token -> ParentQuest label)
    if RE_COBJ_REF.search(joined):
        token = cobj_token_from_condition(conds)
        label = None
        if token and token in gmrw_by_token:
            label = how_to_obtain_from_parentquest(gmrw_by_token[token])
        if not label:
            label = "Event/Activity reward"

        # drop rate: first GLOB, then LVLI fallback
        title_words = [w for w in re.split(r"[^A-Za-z0-9]+", title_display) if w]
        dr = find_glob_drop_rate(glob_rows, token, title_words)
        if not dr and token:
            dr = lvli_drop_rate_from_cobj_lvli(cobj_rows, lvli_rows, token)

        return label, (dr or "N/A"), None, "event_activity"

    # Fallback
    return "Unlock condition present (unclassified).", "N/A", None, "other"

def build_patchlog(prev: Optional[dict], curr: dict) -> dict:
    # minimal: counts + changed IDs lists
    def index_by_id(items: List[dict]) -> Dict[str, dict]:
        return {str(x.get("formId")): x for x in items if x.get("formId")}

    prev_items = index_by_id(prev.get("items", [])) if prev else {}
    curr_items = index_by_id(curr.get("items", []))

    added = [k for k in curr_items.keys() if k not in prev_items]
    removed = [k for k in prev_items.keys() if k not in curr_items]
    changed: List[str] = []

    for k in curr_items.keys():
        if k in prev_items:
            # compare a stable subset
            a = prev_items[k]
            b = curr_items[k]
            fields = ("edid", "title", "titleMale", "titleFemale", "isPrefix", "isSuffix", "howToObtain", "dropRate", "tradeable", "cutContent")
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
        "notes": "Patch log is generated by diffing previous dist JSON in git against the newly built JSON.",
    }

def git_show_json(rev: str, path: str) -> Optional[dict]:
    try:
        out = subprocess.check_output(["git", "show", f"{rev}:{path}"], stderr=subprocess.DEVNULL)
        return json.loads(out.decode("utf-8"))
    except Exception:
        return None

def main() -> int:
   def merge_rows_by_key(row_sets: List[List[Dict[str, str]]], key_field: str) -> List[Dict[str, str]]:
    """
    Merge multiple TSV exports for the same record type.
    Later files win on field values, but we keep one row per key.
    """
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cmpt", action="append", required=True)
    ap.add_argument("--plyt", action="append", required=True)
    ap.add_argument("--book", action="append", required=True)
    ap.add_argument("--cobj", action="append", required=True)
    ap.add_argument("--glob", action="append", required=True)
    ap.add_argument("--gmrw", action="append", required=True)
    ap.add_argument("--lvli", action="append", required=True)
    ap.add_argument("--seasons", required=False, default=None)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    seasons = seasons_map(args.seasons)

    cmpt_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.cmpt], "FormID")
    plyt_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.plyt], "FormID")
    book_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.book], "FormID")
    cobj_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.cobj], "FormID")
    glob_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.glob], "FormID")
    gmrw_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.gmrw], "FormID")
    lvli_rows = merge_rows_by_key([read_tsv_rows(p) for p in args.lvli], "FormID")

    tradeable_by_book_edid = book_tradeable_map(book_rows)
    gmrw_by_token = gmrw_parentquest_map(gmrw_rows)

    # CAMP
    camp_items: List[Dict[str, Any]] = []
    for r in cmpt_rows:
        form_id = (r.get("FormID") or "").strip()
        edid = (r.get("EDID") or "").strip()
        title = (r.get("ANAM - Title") or "").strip()
        is_prefix = (r.get("PTPR - Is Prefix") or "").strip()
        is_suffix = (r.get("PTSU - Is Suffix") or "").strip()

        conds = extract_conditions(r)

        how, dr, sn, unlock_type = compute_unlock_and_rates(
            kind="camp",
            title_display=title,
            edid=edid,
            conds=conds,
            seasons=seasons,
            gmrw_by_token=gmrw_by_token,
            glob_rows=glob_rows,
            cobj_rows=cobj_rows,
            lvli_rows=lvli_rows,
        )

        # Camp default NON-tradeable unless BOOK proves otherwise (by EDID or display title)
        tradeable = False
        k_edid = _norm_key(edid)
        k_title = _norm_key(title)
        if k_edid in tradeable_by_book_edid:
            tradeable = tradeable_by_book_edid[k_edid]
        elif k_title in tradeable_by_book_edid:
            tradeable = tradeable_by_book_edid[k_title]

        camp_items.append({
            "formId": form_id,
            "edid": edid,
            "title": title,
            "isPrefix": (is_prefix == "1" or is_prefix.lower() == "true"),
            "isSuffix": (is_suffix == "1" or is_suffix.lower() == "true"),
            "affixType": (
                "Prefix/Suffix"
                if ((is_prefix == "1" or is_prefix.lower() == "true") and (is_suffix == "1" or is_suffix.lower() == "true"))
                else "Prefix"
                if (is_prefix == "1" or is_prefix.lower() == "true")
                else "Suffix"
                if (is_suffix == "1" or is_suffix.lower() == "true")
                else "—"
            ),
            "conditions": conds,
            "condCount": len(conds),
            "howToObtain": how,
            "dropRate": dr,
            "tradeable": tradeable,
            "unlockType": unlock_type,
            "seasonNumber": sn,
            "cutContent": starts_cut(edid),
        })

    # PLAYER
    player_items: List[Dict[str, Any]] = []
    for r in plyt_rows:
        form_id = (r.get("FormID") or "").strip()
        edid = (r.get("EDID - Editor ID") or "").strip()
        title_m = (r.get("ANAM - Male Title") or "").strip()
        title_f = (r.get("BNAM - Female Title") or "").strip()
        is_prefix = (r.get("PTPR - Is Prefix") or "").strip()
        is_suffix = (r.get("PTSU - Is Suffix") or "").strip()

        conds = extract_conditions(r)

        title_display = title_m or title_f

        how, dr, sn, unlock_type = compute_unlock_and_rates(
            kind="player",
            title_display=title_display,
            edid=edid,
            conds=conds,
            seasons=seasons,
            gmrw_by_token=gmrw_by_token,
            glob_rows=glob_rows,
            cobj_rows=cobj_rows,
            lvli_rows=lvli_rows,
        )

        # Player default Tradeable unless BOOK says NonPlayerTradeable (by EDID or display title)
        tradeable = True
        k_edid = _norm_key(edid)
        k_title = _norm_key(title_display)
        if k_edid in tradeable_by_book_edid:
            tradeable = tradeable_by_book_edid[k_edid]
        elif k_title in tradeable_by_book_edid:
            tradeable = tradeable_by_book_edid[k_title]

        player_items.append({
            "formId": form_id,
            "edid": edid,
            "titleMale": title_m,
            "titleFemale": title_f,
            "title": title_display,
            "isPrefix": (is_prefix == "1" or is_prefix.lower() == "true"),
            "isSuffix": (is_suffix == "1" or is_suffix.lower() == "true"),
            "affixType": (
                "Prefix/Suffix"
                if ((is_prefix == "1" or is_prefix.lower() == "true") and (is_suffix == "1" or is_suffix.lower() == "true"))
                else "Prefix"
                if (is_prefix == "1" or is_prefix.lower() == "true")
                else "Suffix"
                if (is_suffix == "1" or is_suffix.lower() == "true")
                else "—"
            ),
            "conditions": conds,
            "condCount": len(conds),
            "howToObtain": how,
            "dropRate": dr,
            "tradeable": tradeable,
            "unlockType": unlock_type,
            "seasonNumber": sn,
            "cutContent": starts_cut(edid),
        })

    camp_items.sort(key=lambda x: (x.get("cutContent", False), (x.get("title") or "").lower()))
    player_items.sort(key=lambda x: (x.get("cutContent", False), (x.get("title") or "").lower()))

    camp_json = {"generatedAt": now_iso(), "type": "camp_titles", "items": camp_items}
    player_json = {"generatedAt": now_iso(), "type": "player_titles", "items": player_items}

    camp_path = os.path.join(args.outdir, "titles_camp.json")
    player_path = os.path.join(args.outdir, "titles_player.json")

    with open(camp_path, "w", encoding="utf-8") as f:
        json.dump(camp_json, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    with open(player_path, "w", encoding="utf-8") as f:
        json.dump(player_json, f, ensure_ascii=False, separators=(",", ":"), indent=2)

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
            "seasons": os.path.basename(args.seasons) if args.seasons else None,
        },
    }

    manifest_path = os.path.join(args.outdir, "titles_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
