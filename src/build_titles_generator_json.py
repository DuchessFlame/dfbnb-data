#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]  # repo root (src/..)
TSV_DIR = ROOT / "tsv"
DIST_DIR = ROOT / "dist"

OUT_CAMP = DIST_DIR / "titles_camp_generator.json"
OUT_PLAYER = DIST_DIR / "titles_player_generator.json"


# Always remove cut content, no toggles.
CUT_PATTERNS = [
    r"\bCUT\b", r"CUT_", r"_CUT",
    r"\bPOST\b", r"POST_", r"_POST",
    r"\bDEL\b", r"DEL_", r"_DEL",
    r"ZZZZ", r"ZZZ",
]
CUT_RE = re.compile("|".join(CUT_PATTERNS), re.IGNORECASE)


def is_cut(edid: str, text: str) -> bool:
    hay = f"{edid or ''} {text or ''}"
    return bool(CUT_RE.search(hay))


def read_tsv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        headers = reader.fieldnames or []
        rows: List[Dict[str, str]] = []
        for r in reader:
            # normalize None values to ""
            rows.append({k: (v if v is not None else "") for k, v in r.items()})
        return headers, rows


def find_latest_file(prefix: str) -> Optional[Path]:
    """
    Picks the latest TSV by modified time for files matching:
      tsv/<prefix>_Export_*.tsv
    Example prefix: "CMPT" or "PLYT"
    """
    candidates = sorted(TSV_DIR.glob(f"{prefix}_Export_*.tsv"))
    if not candidates:
        return None
    # Use mtime to avoid month-name parsing edge cases
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def truthy(v: str) -> bool:
    return str(v).strip().lower() in ("true", "1", "yes", "y")


def camp_extract(rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    CMPT_Export_March_2026.tsv headers:
      FormID, EDID, ANAM - Title, PTPR - Is Prefix, PTSU - Is Suffix, ...
    """
    prefixes: List[Dict[str, str]] = []
    suffixes: List[Dict[str, str]] = []

    for r in rows:
        formid = (r.get("FormID", "") or "").strip()
        edid = (r.get("EDID", "") or "").strip()
        text = (r.get("ANAM - Title", "") or "").strip()

        if not text:
            continue
        if is_cut(edid, text):
            continue

        is_prefix = truthy(r.get("PTPR - Is Prefix", ""))
        is_suffix = truthy(r.get("PTSU - Is Suffix", ""))

        # Some entries are both prefix and suffix.
        item = {"id": formid or edid, "text": text}

        if is_prefix:
            prefixes.append(item)
        if is_suffix:
            suffixes.append(item)

    # De-dupe while preserving order (just in case)
    prefixes = dedupe_items(prefixes)
    suffixes = dedupe_items(suffixes)
    return prefixes, suffixes


def player_extract(headers: List[str], rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Supports both:
    - PLYT_Export_March_2026.tsv headers:
        Plugin, FormID, EDID - Editor ID, ANAM - Male Title, BNAM - Female Title,
        PTPR - Is Prefix, PTSU - Is Suffix, ...
    - PLYT_Export_Dec_2025.tsv headers:
        Plugin, EditorID, MaleTitle, FemaleTitle, IsPrefix, IsSuffix, Conditions
    """
    prefixes: List[Dict[str, str]] = []
    suffixes: List[Dict[str, str]] = []

    is_new_format = "EDID - Editor ID" in headers or any("EDID - Editor ID" == h for h in headers)

    for r in rows:
        if is_new_format:
            formid = (r.get("FormID", "") or "").strip()
            edid = (r.get("EDID - Editor ID", "") or "").strip()

            male = (r.get("ANAM - Male Title", "") or "").strip()
            female = (r.get("BNAM - Female Title", "") or "").strip()
            text = male or female

            is_prefix = truthy(r.get("PTPR - Is Prefix", ""))
            is_suffix = truthy(r.get("PTSU - Is Suffix", ""))
        else:
            formid = ""  # older export has no FormID
            edid = (r.get("EditorID", "") or "").strip()

            male = (r.get("MaleTitle", "") or "").strip()
            female = (r.get("FemaleTitle", "") or "").strip()
            text = male or female

            is_prefix = truthy(r.get("IsPrefix", ""))
            is_suffix = truthy(r.get("IsSuffix", ""))

        if not text:
            continue
        if is_cut(edid, text):
            continue

        item = {"id": formid or edid, "text": text}

        if is_prefix:
            prefixes.append(item)
        if is_suffix:
            suffixes.append(item)

    prefixes = dedupe_items(prefixes)
    suffixes = dedupe_items(suffixes)
    return prefixes, suffixes


def dedupe_items(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out: List[Dict[str, str]] = []
    for it in items:
        key = (it.get("id", ""), it.get("text", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def build_payload(kind: str, prefixes: List[Dict[str, str]], suffixes: List[Dict[str, str]]) -> Dict:
    return {
        "meta": {
            "type": kind,
            "updated": datetime.utcnow().strftime("%Y-%m-%d"),
            "prefixCount": len(prefixes),
            "suffixCount": len(suffixes),
        },
        "prefixes": prefixes,
        "suffixes": suffixes,
    }


def ensure_dist_dir():
    DIST_DIR.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Dict):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    ensure_dist_dir()

    camp_path = find_latest_file("CMPT")
    player_path = find_latest_file("PLYT")

    if not camp_path:
        raise SystemExit("No CMPT_Export_*.tsv found in tsv/")
    if not player_path:
        raise SystemExit("No PLYT_Export_*.tsv found in tsv/")

    camp_headers, camp_rows = read_tsv(camp_path)
    player_headers, player_rows = read_tsv(player_path)

    camp_prefixes, camp_suffixes = camp_extract(camp_rows)
    player_prefixes, player_suffixes = player_extract(player_headers, player_rows)

    camp_payload = build_payload("camp", camp_prefixes, camp_suffixes)
    player_payload = build_payload("player", player_prefixes, player_suffixes)

    write_json(OUT_CAMP, camp_payload)
    write_json(OUT_PLAYER, player_payload)

    print(f"Wrote {OUT_CAMP} ({len(camp_prefixes)} prefixes, {len(camp_suffixes)} suffixes)")
    print(f"Wrote {OUT_PLAYER} ({len(player_prefixes)} prefixes, {len(player_suffixes)} suffixes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
