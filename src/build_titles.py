from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root
TSV_DIR = os.path.join(ROOT, "tsv")
DIST_DIR = os.path.join(ROOT, "dist")

def _read_tsv(filename: str) -> pd.DataFrame:
    path = os.path.join(TSV_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing TSV: {path}")
    return pd.read_csv(path, sep="\t", dtype=str, encoding_errors="replace").fillna("")

def _season_map() -> dict[str, str]:
    path = os.path.join(TSV_DIR, "Seasons_Map.tsv")
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path, sep="\t", dtype=str, encoding_errors="replace").fillna("")
    out: dict[str, str] = {}
    for _, r in df.iterrows():
        sid = str(r.get("SeasonID", "")).strip()
        sname = str(r.get("SeasonName", "")).strip()
        if sid:
            out[sid.upper()] = sname
    return out

def _cond_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if re.match(r"^Cond\d+$", str(c))]

def _join_conds(row: pd.Series, cols: list[str]) -> str:
    parts: list[str] = []
    for c in cols:
        v = str(row.get(c, "")).strip()
        if v:
            v = re.sub(r"^[A-Za-z]+:\s*", "", v)  # strip "Top:" / "OR:" etc
            parts.append(v)
    return " | ".join(parts)

RE_HASENT = re.compile(r"HasEntitlement\(", re.IGNORECASE)
RE_SCORE = re.compile(r"\bSCORE_(S\d+)\b", re.IGNORECASE)
RE_ATX = re.compile(r"\bATX\b|ATOM\s*SHOP|ATOMIC\s*SHOP", re.IGNORECASE)
RE_QUEST = re.compile(r"\bQUST:|GetQuestCompleted\(", re.IGNORECASE)
RE_COBJ = re.compile(r"\bCOBJ:([0-9A-Fa-f]{6,8})\b")

def _classify(conds: str, seasons: dict[str, str]) -> dict:
    c = (conds or "").strip()
    if not c:
        return {
            "bucket": "No conditions",
            "howTo": "Unlocked by Default",
            "dropRate": "100%",
        }

    if RE_HASENT.search(c):
        m = RE_SCORE.search(c)
        sid = (m.group(1).upper() if m else "")
        sname = seasons.get(sid, "")
        season_num = sid.replace("S", "") if sid else "{#}"
        how = f'Claim the {{Reward Name}} from Season {season_num}'
        if sname:
            how += f" - {sname}"
        return {
            "bucket": "Seasons",
            "howTo": how,
            "dropRate": "100%",
            "seasonId": sid,
            "seasonName": sname,
        }

    if RE_ATX.search(c):
        return {
            "bucket": "ATX",
            "howTo": "Unlocks with the purchase of certain bundles from the Atom Shop",
            "dropRate": "100%",
        }

    if RE_QUEST.search(c):
        return {
            "bucket": "Quests",
            "howTo": "Complete the quest {Quest Name}",
            "dropRate": "100%",
        }

    if RE_COBJ.search(c):
        return {
            "bucket": "COBJ chain",
            "howTo": "(COBJ chain unresolved until COBJ/LVLI/GMRW/GLOB TSVs are added)",
            "dropRate": "",
        }

    return {
        "bucket": "Other",
        "howTo": "(Needs mapping)",
        "dropRate": "100%",
    }

def main() -> None:
    os.makedirs(DIST_DIR, exist_ok=True)

    seasons = _season_map()

    cmpt = _read_tsv("CMPT_Export_March_2026.tsv")
    plyt = _read_tsv("PLYT_Export_March_2026.tsv")

    cmpt_cc = _cond_cols(cmpt)
    plyt_cc = _cond_cols(plyt)

    def make_records(df: pd.DataFrame, kind: str, cc: list[str]) -> list[dict]:
        recs: list[dict] = []
        for _, r in df.iterrows():
            conds = _join_conds(r, cc)
            meta = _classify(conds, seasons)
            recs.append({
                "kind": kind,  # "camp" or "player"
                "formId": str(r.get("FormID", "")).strip(),
                "edid": str(r.get("EDID - Editor ID", "")).strip(),
                "name": str(r.get("ANAM - Male Title", "") or r.get("ANAM", "")).strip(),
                "conditions": conds,
                "source": meta,
            })
        return recs

    data = {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "records": make_records(cmpt, "camp", cmpt_cc) + make_records(plyt, "player", plyt_cc),
    }

    with open(os.path.join(DIST_DIR, "titles_data.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    with open(os.path.join(DIST_DIR, "titles_manifest.json"), "w", encoding="utf-8") as f:
        json.dump({"generatedAtUtc": data["generatedAtUtc"], "total": len(data["records"])}, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
