from __future__ import annotations

"""
build_scrap_breakdown_json.py
=============================
Generates dist/scrap-breakdown.json — what each junk item scraps into and
how much (e.g. Toothbrush -> 1x Plastic, Teapot -> 3x Ceramic).

The scrap system is a two-table join, resolved WITHOUT walking any
ReferencedBy graph (Steel alone has 8000+ refs):

  MISC.MCQP  = item -> {component, tier keyword}   (e.g. c_Plastic + ComponentQuantityLow)
  CMPO.CVPA  = component -> {tier keyword: amount}  (e.g. c_Plastic Low = 1)

The amount is per-component, so the same tier differs between components
(Steel Bulk=30 vs Plastic Bulk=10). Join on (component EDID, tier keyword).

Input files (place in tsv/ or pass --data-dir):
  CMPO_Export_*.tsv   (xEdit export — ExportCMPOToTSV.pas)
  MISC_Export_*.tsv   (xEdit export — ExportMISCToCSV.pas, MUST include the MCQP column)

Output:
  dist/scrap-breakdown.json -> {
    "version": "YYYY-MM-DD",
    "generated": "<ISO-8601 UTC>",
    "cmpo_source": "CMPO_Export_July_2026.tsv",
    "misc_source": "MISC_Export_July_2026.tsv",
    "component_count": N,
    "item_count": M,
    "items": [ { "formid", "edid", "name",
                 "components": [ {"component","component_name","scrap_item","tier","amount"} ] } ],
    "by_component": { "c_Plastic": {"name","scrap_item","items":[{"item","name","tier","amount"}]} }
  }

If the latest MISC export predates the MCQP column, the script writes a
placeholder file with "needs_misc_reexport": true and prints how to fix it,
rather than silently producing an empty breakdown.

Usage:
  python build_scrap_breakdown_json.py
  python build_scrap_breakdown_json.py --data-dir ../tsv --outdir ../dist
"""

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple

# Shared cut-content filter (single source of truth in src/cut_content.py).
# Fallback keeps the script runnable when staged outside src/.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from cut_content import is_cut  # noqa: E402
except Exception:  # pragma: no cover - fallback only
    _CUT_FALLBACK = re.compile(
        r"^(zz|zzz|del_|cut_|post_|test_|debug_|deprecated_|pts_)", re.IGNORECASE
    )

    def is_cut(edid: str) -> bool:
        return bool(edid) and bool(_CUT_FALLBACK.match(edid))


# CSV fields can be very wide (MISC rows carry thousands of ref columns).
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

_MONTH_TOKEN = {
    "Jan": 1, "Feb": 2, "Mar": 3, "March": 3, "Apr": 4, "April": 4,
    "May": 5, "Jun": 6, "June": 6, "Jul": 7, "July": 7, "Aug": 8,
    "Sep": 9, "Sept": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _rank(fname: str) -> Tuple[int, int]:
    m = re.search(r"_([A-Za-z]+)_(\d{4})", os.path.basename(fname))
    if not m:
        return (0, 0)
    return (int(m.group(2)), _MONTH_TOKEN.get(m.group(1), 0))


def find_latest(data_dir: str, prefix: str) -> Optional[str]:
    """Newest LIVE monthly TSV for a record type (skips PTS exports)."""
    files = [
        f for f in glob.glob(os.path.join(data_dir, f"{prefix}_Export_*.tsv"))
        if "_PTS_" not in os.path.basename(f)
    ]
    if not files:
        return None
    return max(files, key=_rank)


def _parse_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def load_cmpo(path: str) -> Dict[str, Dict[str, Any]]:
    """component EDID -> {name, scrap_item_edid, scrap_item_name, scalar_edid, tiers{tier:amount}}."""
    comps: Dict[str, Dict[str, Any]] = {}
    with open(path, encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        for row in rd:
            edid = (row.get("CMPO_EDID") or "").strip()
            if not edid or is_cut(edid):
                continue
            tiers: Dict[str, int] = {}
            for ent in (row.get("CVPA") or "").strip().split("|"):
                if not ent:
                    continue
                parts = ent.split(":")  # tier : count : curve
                if len(parts) < 2:
                    continue
                tier = parts[0].strip()
                cnt = _parse_int(parts[1])
                if tier and cnt is not None:
                    tiers[tier] = cnt
            comps[edid] = {
                "name": (row.get("FULL") or "").strip() or edid,
                "scrap_item_edid": (row.get("ScrapItem_EDID") or "").strip(),
                "scrap_item_name": (row.get("ScrapItem_FULL") or "").strip(),
                "scalar_edid": (row.get("ModScrapScalar_EDID") or "").strip(),
                "tiers": tiers,
            }
    return comps


def load_misc(
    path: str, comps: Dict[str, Dict[str, Any]], warnings: List[str]
) -> Optional[List[Dict[str, Any]]]:
    """List of item breakdowns, or None if the export has no MCQP column."""
    items: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        if "MCQP" not in (rd.fieldnames or []):
            return None
        for row in rd:
            mcqp = (row.get("MCQP") or "").strip()
            if not mcqp:
                continue
            edid = (row.get("EDID") or "").strip()
            if is_cut(edid):
                continue

            comp_list: List[Dict[str, Any]] = []
            for pair in mcqp.split("|"):
                p = pair.split(":", 1)  # component : tier
                if len(p) < 2:
                    continue
                comp_edid = p[0].strip()
                tier = p[1].strip()
                if not comp_edid:
                    continue

                cinfo = comps.get(comp_edid)
                amount: Optional[int] = None
                comp_name = ""
                scrap_item = ""
                if cinfo is None:
                    warnings.append(
                        f"component '{comp_edid}' not in CMPO export (item {edid})"
                    )
                else:
                    comp_name = cinfo["name"]
                    scrap_item = cinfo["scrap_item_name"]
                    amount = cinfo["tiers"].get(tier)
                    if amount is None:
                        warnings.append(
                            f"tier '{tier}' missing from CVPA of {comp_edid} (item {edid})"
                        )

                comp_list.append({
                    "component": comp_edid,
                    "component_name": comp_name,
                    "scrap_item": scrap_item,
                    "tier": tier,
                    "amount": amount,
                })

            if not comp_list:
                continue

            items.append({
                "formid": (row.get("FormID") or "").strip(),
                "edid": edid,
                "name": (row.get("FULL") or "").strip() or edid,
                "components": comp_list,
            })
    return items


def build_by_component(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for it in items:
        for c in it["components"]:
            slot = idx.setdefault(c["component"], {
                "name": c["component_name"],
                "scrap_item": c["scrap_item"],
                "items": [],
            })
            slot["items"].append({
                "item": it["edid"],
                "name": it["name"],
                "tier": c["tier"],
                "amount": c["amount"],
            })
    return idx


def write_json_atomic(path: str, data: Any) -> None:
    """Write via tempfile + os.replace, with a round-trip parse so a
    truncated write can never reach dist/."""
    out_dir = os.path.dirname(path) or "."
    os.makedirs(out_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=".json", dir=out_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        with open(tmp, encoding="utf-8") as fh:
            json.load(fh)  # verify
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="tsv", help="TSV folder (default: tsv)")
    parser.add_argument("--outdir", default="dist", help="Output folder (default: dist)")
    args = parser.parse_args()

    cmpo_path = find_latest(args.data_dir, "CMPO")
    misc_path = find_latest(args.data_dir, "MISC")
    if not cmpo_path:
        print(f"ERROR: no CMPO_Export_*.tsv in {args.data_dir}", file=sys.stderr)
        return 1
    if not misc_path:
        print(f"ERROR: no MISC_Export_*.tsv in {args.data_dir}", file=sys.stderr)
        return 1

    print(f"CMPO: {os.path.basename(cmpo_path)}")
    print(f"MISC: {os.path.basename(misc_path)}")

    comps = load_cmpo(cmpo_path)
    print(f"Components loaded: {len(comps)}")

    now = dt.datetime.now(dt.timezone.utc)
    out_path = os.path.join(args.outdir, "scrap-breakdown.json")

    warnings: List[str] = []
    items = load_misc(misc_path, comps, warnings)

    if items is None:
        # MISC export predates the MCQP column — cannot build the join yet.
        print(
            "\nNOTE: the latest MISC export has no MCQP column.\n"
            "      Re-run '!!!Wordpress - ExportMISCToCSV.pas' on the MISC branch\n"
            "      (it now emits MCQP), drop the new TSV in the tsv/ folder, and\n"
            "      re-run this builder.\n",
            file=sys.stderr,
        )
        payload = {
            "version": now.strftime("%Y-%m-%d"),
            "generated": now.isoformat(),
            "cmpo_source": os.path.basename(cmpo_path),
            "misc_source": os.path.basename(misc_path),
            "needs_misc_reexport": True,
            "component_count": len(comps),
            "item_count": 0,
            "items": [],
            "by_component": {},
        }
        write_json_atomic(out_path, payload)
        print(f"Wrote placeholder: {out_path}")
        return 0

    items.sort(key=lambda it: it["name"].lower())
    payload = {
        "version": now.strftime("%Y-%m-%d"),
        "generated": now.isoformat(),
        "cmpo_source": os.path.basename(cmpo_path),
        "misc_source": os.path.basename(misc_path),
        "component_count": len(comps),
        "item_count": len(items),
        "items": items,
        "by_component": build_by_component(items),
    }
    write_json_atomic(out_path, payload)

    unresolved = sum(
        1 for it in items for c in it["components"] if c["amount"] is None
    )
    print(f"Items with scrap data: {len(items)}")
    print(f"Unresolved component/tier pairs: {unresolved}")
    if warnings:
        print(f"Warnings: {len(warnings)} (showing up to 10)")
        for w in warnings[:10]:
            print(f"  - {w}")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
