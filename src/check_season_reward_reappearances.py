#!/usr/bin/env python3
"""
check_season_reward_reappearances.py
-----------------------------------
Cross-references items in tsv/season_rewards.tsv against:

  * dist/atom_shop.json     — current Atom Shop catalogue (authoritative)
  * tsv/ENTM_Export_*.tsv   — storefront entitlements (Gold Bullion vendor,
                              Legendary Module Exchange, Stamps vendor are
                              all encoded in the DESC text)
  * tsv/BOOK_Export_*.tsv   — plans/recipes that may have re-appeared
  * tsv/FURN_Export_*.tsv,
    tsv/LVLI_Export_*.tsv,
    tsv/RESO_Export_*.tsv,
    tsv/MISC_Export_*.tsv,
    tsv/NOTE_Export_*.tsv   — other record types scanned by EDID/name match

Writes back to the `reappearances` column of tsv/season_rewards.tsv as a
pipe-separated list of labels, e.g.:

    reappearances = "Atom Shop|Gold Bullion vendor"

build_season_rewards.py picks this up and writes it into the per-season
JSON. df-bnb-seasons.js then renders it under "How to Obtain" on each
reward card so a player who missed the season can see where else the
item is still obtainable.

Run:
    python src/check_season_reward_reappearances.py

Idempotent — re-run any time the xEdit TSVs or atom_shop.json update.

Cross-season SCORE_S{N}_ matches are intentionally NOT labelled — items
like "5% S.C.O.R.E. Boost" share an EDID stem across 25+ seasons because
they're reused templates, not actual reappearances. If you ever need
those back, add a label inside classify_entm().
"""

from __future__ import annotations

import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = ROOT / "tsv"
DIST_DIR = ROOT / "dist"
SEASON_REWARDS_TSV = TSV_DIR / "season_rewards.tsv"
ATOM_SHOP_JSON = DIST_DIR / "atom_shop.json"

TAG = "[check_reappearances]"


# ---------------------------------------------------------------------------
# Latest-export discovery — picks the newest TSV per record type by parsing
# the {Month}_{Year} fragment in the filename. mtime is unreliable on this
# repo because the TSVs sit in OneDrive and timestamps drift on sync.
# ---------------------------------------------------------------------------

MONTH_MAP = {
    "Jan": 1, "January": 1, "Feb": 2, "February": 2,
    "Mar": 3, "March": 3, "Apr": 4, "April": 4, "May": 5,
    "Jun": 6, "June": 6, "Jul": 7, "July": 7, "Aug": 8, "August": 8,
    "Sep": 9, "Sept": 9, "September": 9, "Oct": 10, "October": 10,
    "Nov": 11, "November": 11, "Dec": 12, "December": 12,
}


def parse_export_date(filename: str):
    m = re.search(r"_Export_([A-Za-z]+)_(\d{4})", filename)
    if not m:
        return (0, 0)
    return (int(m.group(2)), MONTH_MAP.get(m.group(1), 0))


def find_latest(pattern: str):
    candidates = list(TSV_DIR.glob(pattern))
    if not candidates:
        return None
    return max(candidates, key=lambda p: parse_export_date(p.name))


# (label, glob, edid_col, full_col, desc_col)
TSV_SOURCES = [
    ("ENTM", "ENTM_Export_*.tsv",           "EDID",      "FULL",      "DESC"),
    ("BOOK", "BOOK_Export_*[!s].tsv",       "EDID",      "FULL",      "DESC"),
    ("FURN", "FURN_Export_*_FURN.tsv",      "FURN_EDID", "FURN_FULL", None),
    ("LVLI", "LVLI_Export_*_LVLI_List.tsv", "LVLI_EDID", "LVLI_FULL", None),
    ("RESO", "RESO_Export_*.tsv",           "EDID",      None,        None),
    ("MISC", "MISC_Export_*.tsv",           "EDID",      "FULL",      "DESC"),
    ("NOTE", "NOTE_Export_*.tsv",           "EDID",      "FULL",      "DESC"),
]


# ---------------------------------------------------------------------------
# EDID stem extraction — strip the source-encoding prefix so two records that
# represent the same item under different sources collide on the same stem.
# ---------------------------------------------------------------------------

PREFIX_STRIP_RE = re.compile(
    r"^(?:"
    r"(?:ZZZ_?|zzz_?|REUSE_?|CUT_?|DEL_?|DEBUG_?|TEMPLATE_?|TEST_?|DevUtility_?)*"
    r"(?:"
    r"SCORE_S\d+_|"
    r"SCORE_MiniSeason_\d{4}_[A-Za-z]+_|"
    r"ATX_F\d+_|ATX_|"
    r"MILE_|MS_|MTX_|"
    r"F1_|"
    r"PTS_|DE\d{4}_|Babylon_|CAMPPets_|Shelters_|Vaults_|"
    r"Community(?:_\d{4})?_|"
    r"Anniversary_|QC\d+_|TP_|WST_|Upgrade\d+_|NPE_|Event_"
    r")"
    r"(?:ENTM_|ATX_|SCORE_)?"
    r")+",
    flags=re.IGNORECASE,
)


def edid_stem(edid: str) -> str:
    if not edid:
        return ""
    return PREFIX_STRIP_RE.sub("", edid).strip("_")


# ---------------------------------------------------------------------------
# Classification from xEdit data — only labels that mean "you can still
# obtain this elsewhere". Cross-season SCORE_S{N}_ matches deliberately
# excluded; see module docstring for rationale.
# ---------------------------------------------------------------------------

GOLD_BULLION_RE = re.compile(r"GOLD\s*BULLION", re.IGNORECASE)
LMOD_RE = re.compile(r"LEGENDARY\s*MODULE", re.IGNORECASE)
STAMPS_BUY_RE = re.compile(r"\b(?:PURCHASE|AVAILABLE|BOUGHT|BUY)[^.]*\bSTAMPS?\b", re.IGNORECASE)


def classify_entm(edid: str, desc: str):
    """Return source labels for an xEdit record matched to a season reward."""
    labels = []
    edid_u = (edid or "").upper()
    desc = desc or ""

    if edid_u.startswith("ATX_") or edid_u.startswith("ZZZATX_") or "_ATX_" in edid_u:
        labels.append("Atom Shop")
    elif edid_u.startswith("MILE_") or "_MILE_" in edid_u:
        labels.append("Milestone Reward")
    elif edid_u.startswith("MS_") or "SCORE_MINISEASON_" in edid_u or "_MS_" in edid_u:
        labels.append("Mini Season")
    elif edid_u.startswith("VAULTS_"):
        labels.append("Vault Reward")
    elif edid_u.startswith("BABYLON_"):
        labels.append("Skyline Valley reward")

    if GOLD_BULLION_RE.search(desc):
        labels.append("Gold Bullion vendor")
    if LMOD_RE.search(desc):
        labels.append("Legendary Module Exchange")
    if STAMPS_BUY_RE.search(desc):
        labels.append("Stamps vendor")

    return labels


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_xedit_tsv(path: Path, edid_col: str, full_col, desc_col):
    """Stream an xEdit TSV. xEdit dumps sometimes contain Windows-1252
    bytes (e.g. 0x95 bullets), so try UTF-8 then fall back to cp1252."""
    rows = []
    for encoding in ("utf-8", "cp1252"):
        rows = []
        try:
            with path.open("r", encoding=encoding, newline="") as fh:
                reader = csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
                for r in reader:
                    edid = (r.get(edid_col) or "").strip()
                    if not edid:
                        continue
                    rows.append({
                        "edid": edid,
                        "full": (r.get(full_col) or "").strip() if full_col else "",
                        "desc": (r.get(desc_col) or "").strip() if desc_col else "",
                    })
            return rows
        except UnicodeDecodeError:
            continue
        except (OSError, csv.Error) as e:
            print("{0} [WARN] {1}: {2}".format(TAG, path.name, e), file=sys.stderr)
            return rows
    print("{0} [WARN] Could not decode {1}".format(TAG, path.name), file=sys.stderr)
    return rows


def load_atom_shop(path: Path):
    """Return list of {edid, name} for everything in the Atom Shop, INCLUDING
    bundle child items so a season reward that's only sold as part of a
    bundle still gets flagged. Bundle parent rows often have empty edid/name
    so we don't lose anything by adding their children."""
    out = []
    if not path.exists():
        print("{0} [INFO] no atom_shop.json, skipping Atom Shop cross-ref".format(TAG))
        return out
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print("{0} [WARN] could not load atom_shop.json: {1}".format(TAG, e), file=sys.stderr)
        return out
    for item in data.get("items", []):
        edid = (item.get("edid") or "").strip()
        name = (item.get("name") or "").strip()
        if edid or name:
            out.append({"edid": edid, "name": name})
        for bi in item.get("bundleItems") or []:
            be = (bi.get("edid") or "").strip()
            bn = (bi.get("name") or "").strip()
            if be or bn:
                out.append({"edid": be, "name": bn})
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    if not SEASON_REWARDS_TSV.exists():
        print("{0} [ERROR] missing {1}".format(TAG, SEASON_REWARDS_TSV), file=sys.stderr)
        return 1

    with SEASON_REWARDS_TSV.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        fieldnames = reader.fieldnames or []
        season_rows = list(reader)

    if "reappearances" not in fieldnames:
        print("{0} [ERROR] no 'reappearances' column in TSV".format(TAG), file=sys.stderr)
        return 1

    print("{0} Loaded {1} season reward rows.".format(TAG, len(season_rows)))

    # --- Build xEdit indexes ---------------------------------------------
    entm_by_stem = defaultdict(list)
    entm_by_name = defaultdict(list)
    for label, pattern, ecol, fcol, dcol in TSV_SOURCES:
        path = find_latest(pattern)
        if not path:
            print("{0} [INFO] no {1} match for {2!r}".format(TAG, label, pattern))
            continue
        rows = load_xedit_tsv(path, ecol, fcol, dcol)
        print("{0} {1}: indexed {2:>6} from {3}".format(TAG, label, len(rows), path.name))
        for r in rows:
            stem = edid_stem(r["edid"])
            if stem:
                entm_by_stem[stem.lower()].append(r)
            if r["full"]:
                entm_by_name[r["full"].lower()].append(r)

    # --- Build Atom Shop indexes -----------------------------------------
    atom_by_stem = defaultdict(list)
    atom_by_name = defaultdict(list)
    atom_rows = load_atom_shop(ATOM_SHOP_JSON)
    print("{0} ATOM_SHOP: indexed {1:>6} items".format(TAG, len(atom_rows)))
    for r in atom_rows:
        stem = edid_stem(r["edid"]) if r["edid"] else ""
        if stem:
            atom_by_stem[stem.lower()].append(r)
        if r["name"]:
            atom_by_name[r["name"].lower()].append(r)

    # --- Match each season reward ----------------------------------------
    source_counts = defaultdict(int)
    rows_with_hits = 0
    rows_changed = 0

    for row in season_rows:
        own_edid = (row.get("storefrontEntitlement") or "").strip()
        own_name = (row.get("name") or "").strip()
        own_stem = edid_stem(own_edid).lower()
        own_name_lc = own_name.lower()
        own_edid_lc = own_edid.lower()

        labels = []
        seen_labels = set()

        # --- Atom Shop: exact EDID, stem, or display-name match ----------
        atom_hit = False
        if own_edid:
            for a in atom_by_stem.get(own_stem, []):
                if a["edid"].lower() == own_edid_lc:
                    atom_hit = True
                    break
        if not atom_hit and own_name_lc and atom_by_name.get(own_name_lc):
            atom_hit = True
        if not atom_hit and own_stem and atom_by_stem.get(own_stem):
            atom_hit = True
        if atom_hit:
            labels.append("Atom Shop")
            seen_labels.add("Atom Shop")

        # --- xEdit ENTM matches by stem and by display name --------------
        candidates = []
        if own_stem:
            candidates.extend(entm_by_stem.get(own_stem, []))
        if own_name_lc:
            candidates.extend(entm_by_name.get(own_name_lc, []))

        seen_edids = set()
        for c in candidates:
            if c["edid"] in seen_edids:
                continue
            seen_edids.add(c["edid"])
            # Same ENTM record as the season reward — still inspect its
            # description text since Bethesda flags Gold Bullion / Stamps /
            # Legendary Module availability there.
            if own_edid and c["edid"].lower() == own_edid_lc:
                for lab in classify_entm("", c["desc"]):
                    if lab not in seen_labels:
                        seen_labels.add(lab)
                        labels.append(lab)
                continue
            for lab in classify_entm(c["edid"], c["desc"]):
                if lab not in seen_labels:
                    seen_labels.add(lab)
                    labels.append(lab)

        new_value = "|".join(labels)
        if labels:
            rows_with_hits += 1
            for lab in labels:
                source_counts[lab] += 1
        if (row.get("reappearances") or "") != new_value:
            rows_changed += 1
            row["reappearances"] = new_value

    # --- Write back ------------------------------------------------------
    with SEASON_REWARDS_TSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=fieldnames, delimiter="\t",
            quoting=csv.QUOTE_NONE, escapechar="\\",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(season_rows)

    print("{0} Rows scanned:   {1}".format(TAG, len(season_rows)))
    print("{0} Rows with hits: {1}".format(TAG, rows_with_hits))
    print("{0} Rows updated:   {1}".format(TAG, rows_changed))
    if source_counts:
        print("{0} Hits per source:".format(TAG))
        for src, cnt in sorted(source_counts.items(), key=lambda kv: -kv[1]):
            print("{0}   {1:<32} {2}".format(TAG, src, cnt))
    print("{0} Done. Re-run build_season_rewards.py to refresh the JSON.".format(TAG))
    return 0


if __name__ == "__main__":
    sys.exit(main())
