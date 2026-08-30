#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rng76.py — Shared Fallout 76 Drop Rate Engine

Centralises all LVLI tree walking, GLOB lookups, ChanceNone math,
tier detection, item-name resolution, and adaptive precision formatting
so that every page-specific build script can import from ONE place.

Used by:
  build_activities_rewards_json.py (activity reward pages)
  build_events_rewards_json.py     (event reward pages)
  build_drop_rates.py              (pre-computed drop-rate JSON)
  build_reho_json.py               (Raid · Expo · Hunts · Ops pages)
  build_titles_json.py             (Player & Camp title checklists)

Core formula (rng-76):
  Use All mode (bit 2), max_count = 0:
    each entry fires independently at its own ChanceNone rate
  Use All mode (bit 2), max_count = 1:
    waterfall — entries checked in order, first that passes its
    ChanceNone wins, remaining probability cascades to next entry
  Use All mode (bit 2), max_count > 1:
    combinatorial pick (approximated as independent for display)
  Pick-one mode (non-Use-All):
    rate = 100% / N items   (uniform random pick)
  First Match mode (bit 6):
    waterfall with cascading GetRandomPercent thresholds

  Where:
    pick_weight = apriori with ChanceNone stripped out
    cn_factor   = (1 - actual_list_CN/100) × (1 - actual_entry_CN/100)
    ChanceNone values resolved through GLOB/CURV lookups (0-100 scale)
"""

from __future__ import annotations

import csv
import glob as _glob
import os
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import tsv_source          # one resolver for every export selection


# ============================================================
# 1. TSV LOADING UTILITIES
# ============================================================

_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

def _filename_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)

def newest(pattern: str, exclude_substrings=None) -> str:
    """Return the most recent file matching *pattern*.
    Primary sort: parsed year+month from filename (reliable on GitHub Actions
    where git checkout mtimes vary by checkout order, not commit date).
    Tiebreaker: file mtime (useful on local machines).

    *exclude_substrings* is an optional iterable of substrings; any file
    whose basename contains one of them is dropped before sorting. Used to
    keep the CURV records glob ("CURV_Export_*.tsv") from accidentally
    matching the POINTS file ("CURV_Export_<month>_<year>_POINTS.tsv").
    """
    files = _glob.glob(pattern)
    if exclude_substrings:
        files = [f for f in files
                 if not any(s in os.path.basename(f) for s in exclude_substrings)]
    if not files:
        raise FileNotFoundError(pattern)
    files.sort(key=lambda x: (_filename_date_key(x), os.path.basename(x)))
    return files[-1]


def read_tsv(path: str) -> List[Dict[str, str]]:
    """
    Read a tab-separated file exported from xEdit.

    Tries UTF-8-SIG first (BOM-aware), falls back to cp1252 on decode
    errors.  Returns a list of row dicts.
    """
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))


def read_tsv_columns(path: str, wanted: Iterable[str]) -> List[Dict[str, str]]:
    """
    Read a TSV keeping only the named columns.

    Same as ``read_tsv`` but streams the file and drops every other column, so
    a very wide export doesn't have to be held in memory in full. The GLOB
    export is the reason this exists: it carries one "RefN" column per
    referencing record (5,500+ columns), of which the engine needs exactly
    three — reading it whole costs ~1.6 GB, reading these three costs a few MB.
    """
    keep = set(wanted)

    def _rows(handle):
        reader = csv.reader(handle, delimiter="\t")
        try:
            header = next(reader)
        except StopIteration:
            return []
        idx = [(i, name) for i, name in enumerate(header) if name in keep]
        out = []
        for row in reader:
            out.append({name: (row[i] if i < len(row) else "") for i, name in idx})
        return out

    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return _rows(f)
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return _rows(f)


def pick(row: Dict[str, str], *keys: str, default: str = "") -> str:
    """Return the first non-blank value among *keys* in *row*."""
    for k in keys:
        if k in row:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return v
    return default


def safe_float(s, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(str(s).strip())
    except Exception:
        return default


def safe_int(s, default: int = 0) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return default


# ============================================================
# 2. ITEM-NAME RESOLUTION
# ============================================================

# Well-known FormIDs that appear across many reward lists
KNOWN_FID_NAMES: Dict[str, str] = {
    "0000000F": "Caps",
    "005652F9": "Legendary Module",
    "005A5443": "Treasury Note",
    "007FDC33": "Improved Bait",
    "003F7410": "Legendary Scrip",
    "0072D4FC": "Bobblehead Crate",
}


class ItemNameIndex:
    """
    Loads BOOK / MISC / KEYM / ARMO / WEAP / ALCH / AMMO / CREA TSVs
    and provides a single ``resolve(formid, edid)`` lookup.
    """

    def __init__(self) -> None:
        self.by_formid: Dict[str, str] = {}
        self.by_edid: Dict[str, str] = {}

    # -- loaders for each record type --------------------------------

    def load_book(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "BOOK_FormID", "FormID")
            full = pick(r, "BOOK_FULL", "FULL")
            edid = pick(r, "BOOK_EDID", "EDID")
            if fid and full:
                self.by_formid[fid] = full
            if edid and full:
                self.by_edid[edid] = full

    def load_misc(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "MISC_FormID", "FormID", "FormId")
            full = pick(r, "MISC_FULL", "FULL - Name", "FULL", "Name")
            edid = pick(r, "MISC_EDID", "EDID")
            if fid and full:
                self.by_formid[fid] = full
            if edid and full:
                self.by_edid[edid] = full

    def load_keym(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "KEYM_FormID", "FormID")
            full = pick(r, "KEYM_FULL", "FULL")
            edid = pick(r, "KEYM_EDID", "EDID")
            if fid and full:
                self.by_formid[fid] = full
            if edid and full:
                self.by_edid[edid] = full

    def load_armo(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "ARMO_FormID", "FormID")
            full = pick(r, "ARMO_FULL", "FULL")
            edid = pick(r, "ARMO_EDID", "EDID")
            if fid and full:
                self.by_formid[fid] = full
            if edid and full:
                self.by_edid[edid] = full

    def load_weap(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "WEAP_FormID", "FormID")
            full = pick(r, "WEAP_FULL", "FULL - Name", "FULL")
            edid = pick(r, "WEAP_EDID", "EDID")
            if fid and full:
                self.by_formid[fid] = full
            if edid and full:
                self.by_edid[edid] = full

    def load_alch(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "ALCH_FormID", "FormID")
            full = pick(r, "ALCH_FULL", "FULL - Name", "FULL")
            edid = pick(r, "ALCH_EDID", "EDID")
            if fid and full:
                self.by_formid[fid] = full
            if edid and full:
                self.by_edid[edid] = full

    def load_ammo(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "AMMO_FormID", "FormID")
            full = pick(r, "AMMO_FULL", "FULL - Name", "FULL")
            edid = pick(r, "AMMO_EDID", "EDID")
            if fid and full:
                self.by_formid[fid] = full
            if edid and full:
                self.by_edid[edid] = full

    def load_crea(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "CREA_FormID", "FormID")
            full = pick(r, "CREA_FULL", "FULL")
            edid = pick(r, "CREA_EDID", "EDID")
            if fid and full:
                self.by_formid[fid] = full
            if edid and full:
                self.by_edid[edid] = full

    # -- convenience: load everything from a tsv/ folder -------------

    @staticmethod
    def _name_columns(sig: str) -> Tuple[str, ...]:
        """Every column name the per-record loaders read, for one signature.

        The name index only ever wants FormID / FULL / EDID, so the loaders
        take those columns and skip the rest of the export. Several of these
        tables are very wide (hundreds of stat and back-reference columns) and
        reading them whole is what used to push the engine past 1.5 GB.
        """
        return (f"{sig}_FormID", "FormID", "FormId",
                f"{sig}_FULL", "FULL - Name", "FULL", "Name",
                f"{sig}_EDID", "EDID")

    def load_all_from_tsv_root(self, tsv_root: str) -> None:
        """Auto-discover and load all item-name TSVs from *tsv_root*."""
        def _safe_load(pattern, loader, sig, exclude_suffixes=None):
            files = _glob.glob(os.path.join(tsv_root, pattern))
            if exclude_suffixes:
                files = [f for f in files
                         if not any(suf in os.path.basename(f)
                                    for suf in exclude_suffixes)]
            if not files:
                return
            files.sort(key=tsv_source.export_key)
            loader(read_tsv_columns(files[-1], self._name_columns(sig)))

        _safe_load("BOOK_Export_*.tsv", self.load_book, "BOOK",
                   exclude_suffixes=["_Locations"])
        _safe_load("MISC_Export_*.tsv", self.load_misc, "MISC")
        _safe_load("KEYM_Export_*.tsv", self.load_keym, "KEYM",
                   exclude_suffixes=["_Locations", "_Refs", "_KYWD"])
        _safe_load("ARMO_Export_*.tsv", self.load_armo, "ARMO",
                   exclude_suffixes=["_SLOTS", "_ObjectTemplate"])
        _safe_load("WEAP_Export_*.tsv", self.load_weap, "WEAP",
                   exclude_suffixes=["_ObjectTemplate", "_DNAM"])
        _safe_load("ALCH_Export_*.tsv", self.load_alch, "ALCH",
                   exclude_suffixes=["_Effects"])
        _safe_load("AMMO_Export_*.tsv", self.load_ammo, "AMMO")
        _safe_load("CREA_Export_*.tsv", self.load_crea, "CREA")

    # -- public API ---------------------------------------------------

    def resolve(self, formid: str, edid: str = "") -> str:
        """
        Return the best human-readable name for an item.

        Priority:
          1. KNOWN_FID_NAMES (hardcoded well-known items)
          2. by_formid (FULL from any loaded TSV)
          3. by_edid (FULL from any loaded TSV)
          4. humanized EDID
          5. raw FormID
        """
        if not formid:
            return formid or ""
        name = KNOWN_FID_NAMES.get(formid) or self.by_formid.get(formid)
        if name:
            return name
        if edid:
            name = self.by_edid.get(edid)
            if name:
                return name
            return humanize_edid(edid)
        return formid


def humanize_edid(edid: str) -> str:
    """Convert ``DLC04_HandMadeGun`` → ``Hand Made Gun``."""
    if not edid:
        return edid or ""
    s = edid
    for pfx in ("LL_Weapon_", "LL_Armor_", "LPI_Weapon_", "LPI_Armor_",
                 "LL_", "LPI_", "DLC04_", "DLC05_", "DLC06_", "POST_"):
        if s.startswith(pfx):
            s = s[len(pfx):]
    s = re.sub(r"_", " ", s)
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    return re.sub(r"\s+", " ", s).strip()


# ============================================================
# 3. GLOB INDEX
# ============================================================

class GlobIndex:
    """
    Loads GLOB TSV and exposes:
      - ``value(formid)`` → float FLTV
      - ``drop_rate(formid)`` → ``100 - FLTV`` (formatted string or None)
    """

    def __init__(self) -> None:
        self.vals: Dict[str, float] = {}
        self.edids: Dict[str, str] = {}

    def load(self, rows: List[Dict[str, str]]) -> None:
        for r in rows:
            fid = pick(r, "GLOB_FormID", "FormID")
            fltv = pick(r, "GLOB_FLTV", "FLTV")
            edid = pick(r, "GLOB_EDID", "EDID")
            if fid and fltv:
                try:
                    self.vals[fid] = float(fltv)
                except ValueError:
                    pass
            if fid and edid:
                self.edids[fid] = edid

    #: The only GLOB columns the engine reads. Everything else in that export
    #: is a "RefN" back-reference column — thousands of them — so the loader
    #: takes just these three rather than the whole (very wide) table.
    COLUMNS = ("GLOB_FormID", "FormID", "GLOB_FLTV", "FLTV", "GLOB_EDID", "EDID")

    def load_from_tsv_root(self, tsv_root: str) -> None:
        try:
            self.load(read_tsv_columns(
                newest(os.path.join(tsv_root, "GLOB_Export_*.tsv")), self.COLUMNS))
        except FileNotFoundError:
            pass

    def value(self, formid: str) -> Optional[float]:
        return self.vals.get(formid)

    def drop_rate_pct(self, formid: str) -> Optional[float]:
        """Return ``100 − FLTV`` or None."""
        v = self.vals.get(formid)
        if v is None:
            return None
        pct = 100.0 - v
        return pct if pct >= 0 else None

    def drop_rate_str(self, formid: str) -> Optional[str]:
        """Return ``"10%"`` / ``"5.5%"`` or None."""
        p = self.drop_rate_pct(formid)
        if p is None:
            return None
        return fmt_pct(p)


# ============================================================
# 3b. CURVE TABLE INDEX
# ============================================================

class CurvIndex:
    """
    Loads CURV TSV exports and provides:
      - ``interpolate(curv_formid, x)`` → Y value from curve points
      - ``curv_for_lvli(lvli_formid)`` → CURV FormID (or None)

    The CURV main TSV lists which LVLIs reference each curve.
    The CURV points TSV has (FormID, EDID, X, Y) rows.
    """

    def __init__(self) -> None:
        # curv_formid → sorted list of (x, y) points
        self.points: Dict[str, List[Tuple[float, float]]] = {}
        # lvli_formid → curv_formid
        self.lvli_to_curv: Dict[str, str] = {}

    def load_points(self, tsv_path: str) -> None:
        """Load the CURV points TSV (FormID, EDID, X, Y, JsonPath)."""
        for r in read_tsv(tsv_path):
            fid = pick(r, "FormID", "\ufeffFormID")
            x = safe_float(pick(r, "X"))
            y = safe_float(pick(r, "Y"))
            if fid and x is not None and y is not None:
                self.points.setdefault(fid, []).append((x, y))
        # Sort each curve by X
        for fid in self.points:
            self.points[fid].sort(key=lambda p: p[0])

    def load_main(self, tsv_path: str) -> None:
        """
        Load the CURV main TSV to build lvli→curv mapping.

        Format: FormID, EDID, JsonRelPath, Filename, RefCount, Ref1, Ref2, ...
        Each Ref is ``LVLI_FormID:EDID:LVLI``.
        """
        try:
            with open(tsv_path, encoding="utf-8-sig", newline="") as f:
                for line in f:
                    parts = line.strip().split("\t")
                    if len(parts) < 6:
                        continue
                    curv_fid = parts[0].strip()
                    if not re.fullmatch(r"[0-9A-Fa-f]{8}", curv_fid):
                        continue
                    for ref in parts[5:]:
                        ref = ref.strip()
                        if not ref:
                            continue
                        lvli_fid = ref.split(":")[0]
                        if re.fullmatch(r"[0-9A-Fa-f]{8}", lvli_fid):
                            self.lvli_to_curv[lvli_fid] = curv_fid
        except (FileNotFoundError, UnicodeDecodeError):
            try:
                with open(tsv_path, encoding="cp1252", errors="replace",
                          newline="") as f:
                    for line in f:
                        parts = line.strip().split("\t")
                        if len(parts) < 6:
                            continue
                        curv_fid = parts[0].strip()
                        if not re.fullmatch(r"[0-9A-Fa-f]{8}", curv_fid):
                            continue
                        for ref in parts[5:]:
                            ref = ref.strip()
                            if not ref:
                                continue
                            lvli_fid = ref.split(":")[0]
                            if re.fullmatch(r"[0-9A-Fa-f]{8}", lvli_fid):
                                self.lvli_to_curv[lvli_fid] = curv_fid
            except FileNotFoundError:
                pass

    def load_from_tsv_root(self, tsv_root: str) -> None:
        """Auto-discover and load CURV TSVs from *tsv_root*."""
        try:
            self.load_points(newest(os.path.join(tsv_root, "CURV_Export_*_POINTS.tsv")))
        except FileNotFoundError:
            pass
        # Main CURV file (without _POINTS suffix)
        try:
            candidates = _glob.glob(os.path.join(tsv_root, "CURV_Export_*.tsv"))
            main_files = [f for f in candidates
                          if "_POINTS" not in f and "CurvePoints" not in f]
            if main_files:
                main_files.sort(key=tsv_source.export_key)
                self.load_main(main_files[-1])
        except FileNotFoundError:
            pass

    def curv_for_lvli(self, lvli_formid: str) -> Optional[str]:
        """Return the CURV FormID associated with an LVLI, or None."""
        return self.lvli_to_curv.get(lvli_formid)

    def interpolate(self, curv_formid: str, x: float) -> Optional[float]:
        """
        Linearly interpolate the curve at *x*.

        Returns the Y value, or None if the curve has no points.
        Clamps to the first/last Y for out-of-range X values.
        """
        pts = self.points.get(curv_formid)
        if not pts:
            return None
        if x <= pts[0][0]:
            return pts[0][1]
        if x >= pts[-1][0]:
            return pts[-1][1]
        # Find bracketing points
        for i in range(len(pts) - 1):
            x0, y0 = pts[i]
            x1, y1 = pts[i + 1]
            if x0 <= x <= x1:
                if abs(x1 - x0) < 1e-9:
                    return y0
                t = (x - x0) / (x1 - x0)
                return y0 + t * (y1 - y0)
        return pts[-1][1]


# ============================================================
# 4. LVLI INDEX
# ============================================================

class LvliIndex:
    """
    Loads the three LVLI TSV files (List, Entries, Math) and builds the
    indexes every resolver needs.

    Also builds the **parent map** (child LVLI → set of parent LVLIs)
    used by the titles tier resolver.
    """

    def __init__(self) -> None:
        self.list_by_formid: Dict[str, Dict[str, str]] = {}
        self.edid_by_formid: Dict[str, str] = {}
        self.entries_by_list: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        self.math_by_entry: Dict[Tuple[str, str], Dict[str, str]] = {}
        self.parent_map: Dict[str, Set[str]] = {}

        # Raw row lists (some consumers still need them)
        self.list_rows: List[Dict[str, str]] = []
        self.entry_rows: List[Dict[str, str]] = []
        self.math_rows: List[Dict[str, str]] = []

    def load_list(self, rows: List[Dict[str, str]]) -> None:
        self.list_rows = rows
        for r in rows:
            fid = pick(r, "LVLI_FormID", "FormID")
            edid = pick(r, "LVLI_EDID", "EDID")
            if fid:
                self.list_by_formid[fid] = r
                if edid:
                    self.edid_by_formid[fid] = edid

    def load_entries(self, rows: List[Dict[str, str]]) -> None:
        self.entry_rows = rows
        for r in rows:
            fid = pick(r, "LVLI_FormID", "FormID")
            if fid:
                self.entries_by_list[fid].append(r)

    def load_math(self, rows: List[Dict[str, str]]) -> None:
        self.math_rows = rows
        for r in rows:
            try:
                key = (r["LVLI_FormID"], r["EntryIndex"])
            except KeyError:
                continue
            self.math_by_entry[key] = r

    def build_parent_map(self) -> None:
        """
        Build child→parent mapping from entry rows.

        If LVLO_Reference contains ``:LVLI`` the referenced FormID is a
        child, and the entry's own LVLI_FormID is a parent.
        """
        hex_re = re.compile(r"([0-9A-F]{8})[^:]*:[^:]+:LVLI", re.IGNORECASE)
        self.parent_map = {}
        for r in self.entry_rows:
            ref = (r.get("LVLO_Reference") or "").strip()
            if not ref or ":LVLI" not in ref.upper():
                continue
            parent_fid = pick(r, "LVLI_FormID", "FormID").upper()
            if not parent_fid or not re.fullmatch(r"[0-9A-F]{8}", parent_fid):
                continue
            for m in hex_re.finditer(ref):
                child_fid = m.group(1).upper()
                self.parent_map.setdefault(child_fid, set()).add(parent_fid)

    def load_from_tsv_root(self, tsv_root: str) -> None:
        """Auto-discover List / Entries / Math TSVs and load them."""
        p = os.path.join
        try:
            self.load_list(read_tsv(newest(p(tsv_root, "LVLI_Export_*_LVLI_List.tsv"))))
        except FileNotFoundError:
            pass
        try:
            self.load_entries(read_tsv(newest(p(tsv_root, "LVLI_Export_*_LVLI_Entries.tsv"))))
        except FileNotFoundError:
            pass
        try:
            self.load_math(read_tsv(newest(p(tsv_root, "LVLI_Export_*_LVLI_Math.tsv"))))
        except FileNotFoundError:
            pass
        self.build_parent_map()

    def edid_for(self, formid: str) -> str:
        return self.edid_by_formid.get(formid, "")

    def flags_for(self, formid: str) -> Dict[str, bool]:
        row = self.list_by_formid.get(formid)
        if not row:
            return {"use_all": False, "for_each": False,
                    "level_filter": False, "first_match": False}
        return parse_lvlf_flags(pick(row, "LVLF_Flags", default=""))

    def list_conditions_for(self, formid: str) -> List[str]:
        """Return the raw list-level condition strings (ListCond1..N) attached
        to the LVLI itself, in order. These come from the LVLI_List TSV's
        ListCondCount / ListCond1.. columns (populated by the xEdit export) and
        are SEPARATE from per-entry conditions. Returns [] when the list has no
        conditions or the columns are absent. Mirrors the LVLI_LIST_CONDITIONS
        builder in build_activities_rewards_json.py."""
        row = self.list_by_formid.get(formid)
        if not row:
            return []
        lcc = (row.get("ListCondCount") or "").strip()
        if not lcc or lcc == "0":
            return []
        out: List[str] = []
        for ci in range(1, 26):  # up to ListCond25
            cv = (row.get("ListCond{}".format(ci)) or "").strip()
            if cv:
                out.append(cv)
        return out

    def max_count_for(
        self,
        formid: str,
        globs: Optional["GlobData"] = None,
        curvs: Optional["CurvData"] = None,
    ) -> int:
        """
        Resolve the maximum entry count for a UseAll list (rng-76 rules).

        The max count determines UseAll behaviour:
          0  → no limit, every entry fires independently
          1  → waterfall cascading (first entry that passes wins)
          >1 → combinatorial pick (complex)

        Resolution priority (per rng-76):
          1. LVMT curve + LVMG global as X index → curve Y
          2. LVMG global value directly
          3. LVMV static value
          4. 0 (no limit)
        """
        row = self.list_by_formid.get(formid)
        if not row:
            return 0

        max_val = safe_float(row.get("LVMV_MaxValue"), 0.0)
        max_glob_ref = (row.get("LVMG_MaxGlobal") or "").strip()
        max_curv_ref = (row.get("LVMT_MaxCurve") or "").strip()

        # Resolve GLOB value
        glob_fltv = None
        if max_glob_ref and globs:
            gfid = glob_formid_from_lvli_field(max_glob_ref)
            if gfid:
                glob_fltv = globs.value(gfid)

        # Try curve-based resolution (GLOB as X into curve)
        if max_curv_ref and glob_fltv is not None and curvs:
            cfid = glob_formid_from_lvli_field(max_curv_ref)
            if cfid and cfid in curvs.points:
                y = curvs.interpolate(cfid, glob_fltv)
                if y is not None:
                    return int(round(y))

        # GLOB value directly
        if glob_fltv is not None:
            return int(round(glob_fltv))

        # Static value
        return int(round(max_val))


# ============================================================
# 5. FLAG PARSING
# ============================================================

def parse_lvlf_flags(flags_str: str) -> Dict[str, bool]:
    """
    Parse LVLF_Flags positional bit string from xEdit export.

    xEdit's GetEditValue on a flags field returns a bit string where
    character position N (left-to-right, 0-indexed) corresponds to bit N:
      position 0 = Calculate from all levels <= PC's level (Level Filter)
      position 1 = Calculate for each item in count (For Each)
      position 2 = Use All
      position 6 = First Match

    Examples: "001" → Use All, "11" → Level Filter + For Each,
              "0000001" → First Match

    Returns: ``{use_all, for_each, level_filter, first_match}``
    """
    flags_str = (flags_str or "").strip()
    if not flags_str:
        return {"use_all": False, "for_each": False,
                "level_filter": False, "first_match": False}

    def bit_set(pos):
        return pos < len(flags_str) and flags_str[pos] == '1'

    return {
        "level_filter": bit_set(0),
        "for_each":     bit_set(1),
        "use_all":      bit_set(2),
        "first_match":  bit_set(6),
    }


# ============================================================
# 6. GetRandomPercent CONDITION PARSING
# ============================================================

# ---- CTDA comparison operators -------------------------------------------
# The TSV export writes the CTDA "Type" byte as an 8-character bit string with
# bit 0 FIRST (left-most).  The comparison operator lives in the low three bits,
# so the operator value is  bit0 + 2*bit1 + 4*bit2  — i.e. the first three
# characters of the string.  Everything from bit 3 up is a flag (bit 5 = "use
# global", which is why those rows carry a ``[GLOB:xxxxxxxx]`` instead of a
# literal number).
#
#   "101....." → 5 → <=      "110....." → 3 → >=      "010....." → 2 → >
#
# Verified against known data: RA_LL_Rewards_LegendaryItems (0086A8BD) exports
# "10100000 20.000000" and is documented as ``<= 20``; the Auto-Miner Collectron
# (006C6DC3) exports "11000000 98.000000" and must mean ``>= 98`` (2% ultracite),
# and GetActorValueForCurrentLocation "01000000 0.000000" is plainly ``> 0``.
GRP_OP_EQ = 0
GRP_OP_NE = 1
GRP_OP_GT = 2
GRP_OP_GE = 3
GRP_OP_LT = 4
GRP_OP_LE = 5

_GRP_SYMBOL_OPS = {
    "<=": GRP_OP_LE, "<": GRP_OP_LT, ">=": GRP_OP_GE, ">": GRP_OP_GT,
    "==": GRP_OP_EQ, "=": GRP_OP_EQ, "!=": GRP_OP_NE, "<>": GRP_OP_NE,
}

_RE_GRP_SYMBOL = re.compile(
    r"GetRandomPercent\s*(<=|>=|<>|!=|==|<|>|=)\s*(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
_RE_GRP_RAW = re.compile(
    r"GetRandomPercent[^)]*\)\s*([01]{8})\s*(.*)$",
    re.IGNORECASE,
)


def parse_grp_condition(cond: str) -> Optional[Tuple[int, str]]:
    """
    Parse one ``GetRandomPercent`` condition string.

    Returns ``(operator_code, value_token)`` or None when the string isn't a
    GetRandomPercent condition.  ``value_token`` is the raw right-hand side —
    either a literal number or an xEdit ``EDID [GLOB:xxxxxxxx]`` reference —
    and is resolved by the caller (only the resolver has the GLOB table).

    Handles both export formats:
      - ``GetRandomPercent <= 50``                    (canonical / synthesised)
      - ``GetRandomPercent(...) 10100000 50.000000``  (raw xEdit flags format)
    """
    if not cond or "GetRandomPercent" not in cond:
        return None
    m = _RE_GRP_SYMBOL.search(cond)
    if m:
        return (_GRP_SYMBOL_OPS[m.group(1)], m.group(2))
    m = _RE_GRP_RAW.search(cond)
    if m:
        bits = m.group(1)
        op = int(bits[0]) + 2 * int(bits[1]) + 4 * int(bits[2])
        return (op, (m.group(2) or "").strip())
    return None


def grp_span_for_value(op: int, value: float) -> Tuple[float, float]:
    """
    The slice of the 0–100 GetRandomPercent roll that satisfies ``op value``.

    ``<=`` / ``<``  → ``(0, N)``      ``>=`` / ``>`` → ``(N, 100)``
    ``!=``          → ``(0, 100)``    ``==``         → a single 1-point slice

    Keeping this as an interval (rather than a bare "threshold") is what makes
    a First Match cascade work in either direction: the entries of an ascending
    ``<=`` list and a descending ``>=`` list both carve non-overlapping slices
    out of the same roll, and the slices still total 100%.
    """
    v = max(0.0, min(100.0, float(value)))
    if op in (GRP_OP_LE, GRP_OP_LT):
        return (0.0, v)
    if op in (GRP_OP_GE, GRP_OP_GT):
        return (v, 100.0)
    if op == GRP_OP_NE:
        return (0.0, 100.0)
    if op == GRP_OP_EQ:
        return (v, min(100.0, v + 1.0))
    return (0.0, 100.0)


def span_union(covered: List[Tuple[float, float]],
               span: Tuple[float, float]) -> List[Tuple[float, float]]:
    """Merge ``span`` into the sorted, disjoint interval list ``covered``."""
    out: List[Tuple[float, float]] = []
    lo, hi = span
    for c_lo, c_hi in sorted(list(covered) + [(lo, hi)]):
        if out and c_lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], c_hi))
        else:
            out.append((c_lo, c_hi))
    return out


def span_uncovered_width(span: Tuple[float, float],
                         covered: List[Tuple[float, float]]) -> float:
    """Width (0–100) of ``span`` that is NOT already inside ``covered``."""
    lo, hi = span
    width = max(0.0, hi - lo)
    for c_lo, c_hi in covered:
        overlap = min(hi, c_hi) - max(lo, c_lo)
        if overlap > 0:
            width -= overlap
    return max(0.0, width)


def parse_randompercent_multiplier(conditions_text: str) -> float:
    """
    Combined ``GetRandomPercent`` multiplier (0.0 – 1.0) for a condition blob.

    Operator-aware: ``<= 40`` is a 40% gate, ``>= 40`` is a 60% gate. Literal
    values only — GLOB-valued conditions need the resolver's GLOB table, so use
    ``Rng76Resolver.extract_grp_chance()`` for those.

    Handles both xEdit export formats:
      - ``GetRandomPercent <= 50``                    (standard)
      - ``GetRandomPercent(...) 10100000 50.000000``  (raw GMRW flags format)
    """
    mult = 1.0
    for chunk in re.split(r"(?=GetRandomPercent)", conditions_text or ""):
        parsed = parse_grp_condition(chunk)
        if not parsed:
            continue
        op, tok = parsed
        tok = (tok or "").split()[0] if (tok or "").split() else ""
        try:
            value = float(tok)
        except ValueError:
            continue
        lo, hi = grp_span_for_value(op, value)
        mult *= max(0.0, hi - lo) / 100.0
    return mult


# ============================================================
# 7. ADAPTIVE PRECISION FORMATTING
# ============================================================

def pct(x: float) -> float:
    """Convert a 0-1 probability to a 0-100 percentage, 6 dp."""
    return round(max(0.0, float(x)) * 100, 6)


def fmt_pct(value: float) -> str:
    """
    Format a percentage value as a clean string — NO rounding.

    Shows up to 6 decimal places, trailing zeros stripped.
    - Integer values:  ``"10%"``
    - Fractional:      ``"14.851485%"``  (not rounded to 15%)
    - Tiny values:     ``"0.0012%"``
    """
    if value == 0:
        return "0%"
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value))}%"
    # Always use 6 dp, strip trailing zeros
    s = f"{value:.6f}".rstrip("0").rstrip(".")
    return f"{s}%"


# ============================================================
# 8. CORE RNG-76 RESOLVER
# ============================================================

class Rng76Resolver:
    """
    The main engine.  Walks LVLI trees using the rng-76 formula and
    returns resolved leaf items with drop rates, quantities, conditions.

    Two modes:
      - ``resolve_deep(lvli_formid)``
            Full walk → list of leaf items with drop rates.
            Used by events rewards and REHO pages.
      - ``resolve_simple(lvli_formid)``
            Quick walk → ``{formid: total_chance}`` dict (cached).
            Used for summary probability lookups.
    """

    def __init__(
        self,
        lvli: LvliIndex,
        globs: GlobIndex,
        names: ItemNameIndex,
        curvs: Optional[CurvIndex] = None,
    ) -> None:
        self.lvli = lvli
        self.globs = globs
        self.names = names
        self.curvs = curvs
        self._cache: Dict[str, Dict[str, float]] = {}

    # ---- pick-weight / ChanceNone helpers ----------------------------

    def _entry_pick_and_cn(
        self,
        math: Dict[str, str],
        entry: Dict[str, str],
        list_id: str,
    ) -> Tuple[float, float]:
        """
        Compute (pick_weight, cn_factor) for a single LVLI entry.

        *pick_weight*: pure selection weight with ChanceNone stripped out.
          For equal-weight lists every entry gets ~1.0; for conditioned
          entries it equals ``condChance``.

        *cn_factor*: ``(1 − actualListCN/100) × (1 − actualEntryCN/100)``
          resolved through GLOB/CURV where needed.

        The effective drop rate for a Use-All entry is
        ``pick_weight × cn_factor``.
        For a pick-one (non-Use-All) entry it is
        ``(pick_weight / Σpick_weights) × cn_factor``.
        """
        _ecn_glob = (math.get("EntryChanceNoneGlobal") or "")
        _is_minlvl = "MinLvl" in _ecn_glob

        apriori = (
            1.0 if _is_minlvl
            else float(math.get("EntryAprioriChance_NoSublist") or 1)
        )
        resolved_list_cn = float(math.get("ListChanceNoneResolved") or 0)       # 0-100
        resolved_entry_cn = (
            0.0 if _is_minlvl
            else float(math.get("EntryChanceNoneResolved") or 0)                # 0-100
        )

        # Strip resolved ChanceNone out of apriori → pure pick weight.
        # xEdit bakes (1-listCN/100)*(1-entryCN/100)*condChance into apriori.
        # We undo the CN parts so normalisation uses ChanceNone-free weights.
        pick_weight = apriori
        if 0 < resolved_list_cn < 100:
            pick_weight /= (1.0 - resolved_list_cn / 100.0)
        if 0 < resolved_entry_cn < 100:
            pick_weight /= (1.0 - resolved_entry_cn / 100.0)

        # Resolve actual ChanceNone via GLOB/CURV (both return 0-100).
        actual_list_cn = self.resolve_chance_none(math, "List")
        actual_entry_cn = (
            0.0 if _is_minlvl
            else self.resolve_chance_none(math, "Entry")
        )

        cn_factor = (
            (1.0 - actual_list_cn / 100.0)
            * (1.0 - actual_entry_cn / 100.0)
        )

        return (pick_weight, cn_factor)

    # ---- quick (cached) resolve ------------------------------------

    def resolve_simple(self, list_id: str) -> Dict[str, float]:
        """
        Resolve LVLI → ``{leaf_formid: total_chance}``.

        Cached.  Used by ``compute_lvli()`` in the events script.

        Modes (rng-76 rules):
          - Use All (bit 2), no ChanceNone on entries:
                each entry independent, rate = 100%.
          - Use All (bit 2), with ChanceNone on entries:
                waterfall — entries checked in order, first that passes
                its ChanceNone wins, remaining probability cascades.
          - First Match (bit 6): cascading condition thresholds.
          - Non-Use-All:      pick one at random,
                              rate = (pw / Σpw) × cn_factor.
        """
        if not list_id:
            return {}
        if list_id in self._cache:
            return self._cache[list_id]

        flags = self.lvli.flags_for(list_id)
        is_use_all = flags["use_all"]
        is_first_match = flags["first_match"]

        # ── gather entries ──
        raw: List[Dict[str, Any]] = []
        for e in self.lvli.entries_by_list.get(list_id, []):
            idx = e.get("EntryIndex")
            if idx is None:
                continue
            math = self.lvli.math_by_entry.get((list_id, idx))
            if not math:
                continue

            pw, cn = self._entry_pick_and_cn(math, e, list_id)
            sub = (math.get("SubLVLI_FormID") or "").strip()
            ref = (e.get("LVLO_Reference") or "").strip()
            conditions = self._entry_conditions(e)

            # UseAll + GetRandomPercent condition → use threshold as rate
            if is_use_all and conditions:
                grp = self.extract_grp_chance(conditions)
                if grp is not None:
                    pw = grp
                    cn = 1.0

            raw.append({"pw": pw, "cn": cn, "sub": sub, "ref": ref,
                        "conditions": conditions})

        # ── compute per-entry rate (rng-76 rules) ──
        if is_first_match:
            # First Match (bit 6): cascading GetRandomPercent thresholds.
            # Entries checked in order, first match wins.
            fm_rates = self.first_match_rates([r["conditions"] for r in raw])
            if fm_rates is not None:
                for i, r in enumerate(raw):
                    r["rate"] = fm_rates[i]
            else:
                # Fallback: cascading with cn_factor
                cum_fail = 1.0
                for r in raw:
                    s = r["pw"] * r["cn"]
                    r["rate"] = s * cum_fail
                    cum_fail *= (1.0 - s)

        elif is_use_all:
            # UseAll (bit 2): behaviour depends on max_count attribute.
            #   max=0 → no limit, each entry fires independently
            #   max=1 → waterfall: entries checked in order, first that
            #           passes its ChanceNone wins, probability cascades
            #   max>1 → combinatorial (entries compete for limited slots)
            max_count = self.lvli.max_count_for(
                list_id, self.globs, self.curvs
            )
            if max_count == 1:
                # Waterfall: cascading ChanceNone
                cum_fail = 1.0
                for r in raw:
                    drop = r["pw"] * r["cn"]
                    r["rate"] = drop * cum_fail
                    cum_fail *= (1.0 - drop)
            else:
                # max=0 (independent) or max>1 (each entry fires on its own
                # ChanceNone roll; max>1 just caps total results which we
                # approximate as independent for drop rate display purposes)
                for r in raw:
                    r["rate"] = r["pw"] * r["cn"]

        else:
            # Pick one at random — 100% / N items
            total_pw = sum(r["pw"] for r in raw)
            if total_pw > 0:
                for r in raw:
                    r["rate"] = (r["pw"] / total_pw) * r["cn"]
            else:
                for r in raw:
                    r["rate"] = 0.0

        # ── recurse / accumulate ──
        results: Dict[str, float] = {}
        for r in raw:
            rate = r["rate"]
            if r["sub"]:
                for k, v in self.resolve_simple(r["sub"]).items():
                    results[k] = results.get(k, 0) + v * rate
            else:
                ref = r["ref"]
                if ":" in ref:
                    fid = ref.split(":")[0]
                    results[fid] = results.get(fid, 0) + rate

        self._cache[list_id] = results
        return results

    # ---- deep resolve (full items) ---------------------------------

    def resolve_deep(
        self,
        list_id: str,
        depth: int = 0,
        seen: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Resolve LVLI → list of leaf items with full metadata.

        Each item dict:
          ``{formid, name, qty, dropRate, edid, sig, conditions}``

        Uses rng-76 rules:
          - Use All (bit 2), max_count=0: each entry fires independently.
          - Use All (bit 2), max_count=1: waterfall — entries checked in
                order, first that passes its ChanceNone wins, cascading.
          - Use All (bit 2), max_count>1: combinatorial (approx. independent).
          - First Match (bit 6): cascading condition thresholds —
                              first entry whose conditions pass wins.
          - Non-Use-All:      pick ONE entry at uniform random,
                              rate = (pw / Σpw) × cn_factor.
                              The ``for_each`` flag (bit 1) only affects
                              condition pruning order, NOT independence.
        """
        if seen is None:
            seen = set()
        if list_id in seen or depth > 50:
            return []
        seen = seen | {list_id}

        flags = self.lvli.flags_for(list_id)
        is_use_all = flags["use_all"]
        is_first_match = flags["first_match"]

        # ── gather entries with pick-weight and cn-factor ──
        raw: List[Dict[str, Any]] = []
        for entry in self.lvli.entries_by_list.get(list_id, []):
            idx = entry.get("EntryIndex")
            if idx is None:
                continue
            math = self.lvli.math_by_entry.get((list_id, idx))
            if not math:
                continue

            pw, cn = self._entry_pick_and_cn(math, entry, list_id)
            qty = self._entry_qty(entry)
            conditions = self._entry_conditions(entry)
            sub_lvli = (math.get("SubLVLI_FormID") or "").strip()
            ref = (entry.get("LVLO_Reference") or "").strip()

            # UseAll + GetRandomPercent condition → override with threshold
            if is_use_all and conditions:
                grp = self.extract_grp_chance(conditions)
                if grp is not None:
                    pw = grp
                    cn = 1.0

            raw.append({
                "entry": entry, "math": math,
                "pw": pw, "cn": cn,
                "sub_lvli": sub_lvli, "ref": ref,
                "qty": qty, "conditions": conditions,
            })

        if not raw:
            return []

        # ── compute per-entry drop rate based on mode ──
        if is_first_match:
            # First Match: cascading GetRandomPercent thresholds.
            fm_rates = self.first_match_rates([r["conditions"] for r in raw])
            if fm_rates is not None:
                for i, r in enumerate(raw):
                    r["rate"] = fm_rates[i]
            else:
                cum_fail = 1.0
                for r in raw:
                    s = r["pw"] * r["cn"]
                    r["rate"] = s * cum_fail
                    cum_fail *= (1.0 - s)

        elif is_use_all:
            # UseAll (bit 2): behaviour depends on max_count attribute.
            #   max=0 → no limit, each entry fires independently
            #   max=1 → waterfall: entries checked in order, first that
            #           passes its ChanceNone wins, probability cascades
            #   max>1 → combinatorial (approximated as independent)
            max_count = self.lvli.max_count_for(
                list_id, self.globs, self.curvs
            )
            if max_count == 1:
                # Waterfall: cascading ChanceNone
                cum_fail = 1.0
                for r in raw:
                    drop = r["pw"] * r["cn"]
                    r["rate"] = drop * cum_fail
                    cum_fail *= (1.0 - drop)
            else:
                # max=0 (independent) or max>1
                for r in raw:
                    r["rate"] = r["pw"] * r["cn"]

        else:
            # Pick one at random — 100% / N items
            total_pw = sum(r["pw"] for r in raw)
            if total_pw > 0:
                for r in raw:
                    r["rate"] = (r["pw"] / total_pw) * r["cn"]
            else:
                for r in raw:
                    r["rate"] = 0.0

        # ── build resolved items list ──
        items: List[Dict[str, Any]] = []
        for r in raw:
            dr = r["rate"]
            ref = r["ref"]
            ref_sig = ref.split(":")[-1].upper() if ref.count(":") >= 2 else ""

            if r["sub_lvli"]:
                # Propagate the parent entry's quantity into child items.
                # Per drop-rate-engine §3g:
                #   - With For Each (bit 1) CLEAR: the sub-list is evaluated
                #     once and parent_qty multiplies through into each sub
                #     item's qty.
                #   - With For Each SET: the sub-list is rolled parent_qty
                #     times. For a single-entry sub-list this still yields
                #     parent_qty × sub_qty total items per drop. For a
                #     multi-entry sub-list the expected count per item is
                #     `parent_qty × sub_chance × sub_qty` — multiplying qty
                #     by parent_qty gives the same expected count when the
                #     single-roll probability cascade isn't applied here.
                # Either way, multiplication is the correct display value
                # for "items received per drop". Without it, Treasury Note
                # inside Gold_Treasury_Note_QuestReward_Medium (outer qty=3
                # via GLOB, inner qty=1) was showing as ×1 instead of ×3.
                parent_qty = r["qty"] if r["qty"] > 0 else 1
                for sub_item in self.resolve_deep(r["sub_lvli"], depth + 1, seen):
                    out_qty = sub_item["qty"] * parent_qty
                    items.append({
                        "formid":     sub_item["formid"],
                        "name":       sub_item["name"],
                        "qty":        out_qty,
                        "dropRate":   sub_item["dropRate"] * dr,
                        "edid":       sub_item["edid"],
                        "sig":        sub_item.get("sig", ""),
                        "conditions": r["conditions"] + (sub_item.get("conditions") or []),
                    })
            else:
                if ":" in ref:
                    fid = ref.split(":")[0]
                    edid = ref.split(":")[1] if len(ref.split(":")) > 1 else ""
                    name = self.names.resolve(fid, edid)
                    items.append({
                        "formid":     fid,
                        "name":       name,
                        "qty":        r["qty"],
                        "dropRate":   dr,
                        "edid":       edid,
                        "sig":        ref_sig,
                        "conditions": r["conditions"],
                    })

        return items

    # ---- appearance probability (inventory "is it there?" question) ----

    def appearance_prob(
        self,
        list_id: str,
        target_formid: str,
        depth: int = 0,
        seen: Optional[Set[str]] = None,
    ) -> float:
        """P(``target_formid`` appears at least once when ``list_id`` is rolled ONCE).

        This answers the "will a bottle of X be in the vendor's inventory when
        I open the trade window?" question — the true per-reset appearance
        probability, NOT the per-roll pick rate that ``resolve_deep`` returns.

        It mirrors ``resolve_deep``'s per-entry selection maths exactly (waterfall
        / independent / pick-one / first-match, ChanceNone, GetRandomPercent
        conditions) but folds child results into an at-least-once probability and
        applies the For Each (bit 1) repeated-roll rule from the drop-rate-engine
        skill §3g: a For Each sub-list referenced with quantity N is rolled N
        times, so its appearance chance becomes ``1 - (1 - single)^N``.

        Nested lists compose the same way. UseAll ``max_count > 1`` is treated as
        independent (the documented display approximation, §3d) — identical to
        ``resolve_deep`` — so results stay consistent with the rest of the engine.

        ``target_formid`` may be a single FormID string or an iterable of them; in
        the iterable case this returns P(ANY of them appears), evaluated exactly
        within each list roll (they share the same pick), which is the right
        maths for multi-FormID items (e.g. raw + cracked egg).
        """
        if isinstance(target_formid, (list, set, tuple, frozenset)):
            targets = {str(t).upper() for t in target_formid if t}
        else:
            targets = {(target_formid or "").upper()}
        if seen is None:
            seen = set()
        if list_id in seen or depth > 50:
            return 0.0
        seen = seen | {list_id}

        flags = self.lvli.flags_for(list_id)
        is_use_all = flags["use_all"]
        is_first_match = flags["first_match"]

        raw: List[Dict[str, Any]] = []
        for entry in self.lvli.entries_by_list.get(list_id, []):
            idx = entry.get("EntryIndex")
            if idx is None:
                continue
            math = self.lvli.math_by_entry.get((list_id, idx))
            if not math:
                continue
            pw, cn = self._entry_pick_and_cn(math, entry, list_id)
            qty = self._entry_qty(entry)
            conditions = self._entry_conditions(entry)
            sub_lvli = (math.get("SubLVLI_FormID") or "").strip()
            ref = (entry.get("LVLO_Reference") or "").strip()
            if is_use_all and conditions:
                grp = self.extract_grp_chance(conditions)
                if grp is not None:
                    pw = grp
                    cn = 1.0
            raw.append({"pw": pw, "cn": cn, "qty": qty,
                        "sub": sub_lvli, "ref": ref, "conditions": conditions})

        if not raw:
            return 0.0

        # ── per-entry SELECTION probability (same maths as resolve_deep) ──
        if is_first_match:
            fm_rates = self.first_match_rates([r["conditions"] for r in raw])
            if fm_rates is not None:
                for i, r in enumerate(raw):
                    r["sel"] = fm_rates[i]
            else:
                cum_fail = 1.0
                for r in raw:
                    s = r["pw"] * r["cn"]
                    r["sel"] = s * cum_fail
                    cum_fail *= (1.0 - s)
            independent = False
        elif is_use_all:
            max_count = self.lvli.max_count_for(list_id, self.globs, self.curvs)
            if max_count == 1:
                cum_fail = 1.0
                for r in raw:
                    drop = r["pw"] * r["cn"]
                    r["sel"] = drop * cum_fail
                    cum_fail *= (1.0 - drop)
                independent = False
            else:
                for r in raw:
                    r["sel"] = r["pw"] * r["cn"]
                independent = True
        else:
            total_pw = sum(r["pw"] for r in raw)
            for r in raw:
                r["sel"] = (r["pw"] / total_pw) * r["cn"] if total_pw > 0 else 0.0
            independent = False

        # ── each entry's contribution to "target appears" ──
        contribs: List[float] = []
        for r in raw:
            if r["sub"]:
                a = self.appearance_prob(r["sub"], targets, depth + 1, seen)
                if self.lvli.flags_for(r["sub"])["for_each"] and r["qty"] > 1:
                    single = r["sel"] * a
                    contribs.append(1.0 - (1.0 - single) ** r["qty"])
                else:
                    contribs.append(r["sel"] * a)
            else:
                fid = r["ref"].split(":")[0].upper() if ":" in r["ref"] else ""
                hit = bool(fid) and any(fid.endswith(t) for t in targets)
                contribs.append(r["sel"] if hit else 0.0)

        if independent:
            prod = 1.0
            for c in contribs:
                prod *= (1.0 - c)
            return 1.0 - prod
        return sum(contribs)

    def pick_rate(self, list_id: str, target_formid) -> float:
        """Single per-roll pick rate for ``target_formid`` in ``list_id``.

        This is the per-entry drop rate ``resolve_deep`` reports (the number the
        rng76 harness table shows) — i.e. "when this list is rolled once, what is
        the chance THIS item is the pick?", INCLUDING the entry's own ChanceNone
        gate but NOT compounded across repeated rolls. When the item is reachable
        by more than one leaf path, the highest (least-gated) path is returned, so
        e.g. a guaranteed drink-pool entry reports the raw 1-of-N pick rather than
        a 50%-gated copy of it.

        ``target_formid`` may be a single FormID or an iterable of them.
        """
        if isinstance(target_formid, (list, set, tuple, frozenset)):
            targets = {str(t).upper() for t in target_formid if t}
        else:
            targets = {(target_formid or "").upper()}
        best = 0.0
        for it in self.resolve_deep(list_id):
            fid = (it.get("formid") or "").upper()
            if fid and any(fid.endswith(t) for t in targets):
                best = max(best, float(it.get("dropRate", 0.0)))
        return best

    def entry_appearances(self, list_id: str, target_formid, depth: int = 0,
                          seen: Optional[Set[str]] = None) -> List[float]:
        """Per-SOURCE appearance probabilities for ``target_formid`` — one number
        per contributing entry, the way the rng76 harness lists them.

        For each entry that can yield the item this returns its own chance of
        producing at least one, so an item stocked by two drink-pool entries comes
        back as e.g. ``[0.2778, 0.7824]`` (which SUM to the harness's 106% figure).
        The maths per source:
          * entry ChanceNone is applied ONCE (entry fires or not),
          * a For Each sub-list rolled qty N contributes ``1 - (1 - A)^N`` where A
            is the item's one-roll appearance in that sub-list,
          * a UseAll ``max_count`` entry-cap scales later entries by their
            "reached" probability (chance the cap isn't already full).
        Nested non-ForEach lists bubble their sources up, scaled by the parent
        entry's selection probability.
        """
        if isinstance(target_formid, (list, set, tuple, frozenset)):
            targets = {str(t).upper() for t in target_formid if t}
        else:
            targets = {(target_formid or "").upper()}
        if seen is None:
            seen = set()
        if list_id in seen or depth > 50:
            return []
        seen2 = seen | {list_id}

        flags = self.lvli.flags_for(list_id)
        is_use_all = flags["use_all"]
        is_first_match = flags["first_match"]

        raw: List[Dict[str, Any]] = []
        for entry in self.lvli.entries_by_list.get(list_id, []):
            idx = entry.get("EntryIndex")
            if idx is None:
                continue
            math = self.lvli.math_by_entry.get((list_id, idx))
            if not math:
                continue
            pw, cn = self._entry_pick_and_cn(math, entry, list_id)
            qty = self._entry_qty(entry)
            conditions = self._entry_conditions(entry)
            sub_lvli = (math.get("SubLVLI_FormID") or "").strip()
            ref = (entry.get("LVLO_Reference") or "").strip()
            if is_use_all and conditions:
                grp = self.extract_grp_chance(conditions)
                if grp is not None:
                    pw = grp
                    cn = 1.0
            raw.append({"pw": pw, "cn": cn, "qty": qty,
                        "sub": sub_lvli, "ref": ref, "conditions": conditions})

        if not raw:
            return []

        # ── per-entry selection probability ──
        if is_first_match:
            fm_rates = self.first_match_rates([r["conditions"] for r in raw])
            if fm_rates is not None:
                for i, r in enumerate(raw):
                    r["sel"] = fm_rates[i]
            else:
                cum = 1.0
                for r in raw:
                    s = r["pw"] * r["cn"]
                    r["sel"] = s * cum
                    cum *= (1.0 - s)
        elif is_use_all:
            max_count = self.lvli.max_count_for(list_id, self.globs, self.curvs)
            fire = [r["pw"] * r["cn"] for r in raw]
            if max_count and 0 < max_count < len(raw):
                # "reached" = P(fewer than max_count of the PRECEDING entries fired)
                dist = [1.0]  # dist[k] = P(k preceding entries fired)
                for i, r in enumerate(raw):
                    r["sel"] = fire[i] * sum(dist[:max_count])
                    nd = [0.0] * (len(dist) + 1)
                    for k, pk in enumerate(dist):
                        nd[k] += pk * (1.0 - fire[i])
                        nd[k + 1] += pk * fire[i]
                    dist = nd
            else:
                for i, r in enumerate(raw):
                    r["sel"] = fire[i]
        else:
            total_pw = sum(r["pw"] for r in raw)
            for r in raw:
                r["sel"] = (r["pw"] / total_pw) * r["cn"] if total_pw > 0 else 0.0

        out: List[float] = []
        for r in raw:
            if r["sub"]:
                a = self.appearance_prob(r["sub"], targets, depth + 1, seen2)
                if a <= 0:
                    continue
                if self.lvli.flags_for(r["sub"])["for_each"] and r["qty"] > 1:
                    out.append(r["sel"] * (1.0 - (1.0 - a) ** r["qty"]))
                else:
                    for s in self.entry_appearances(r["sub"], targets, depth + 1, seen2):
                        out.append(r["sel"] * s)
            else:
                fid = r["ref"].split(":")[0].upper() if ":" in r["ref"] else ""
                if fid and any(fid.endswith(t) for t in targets):
                    out.append(r["sel"])
        return out

    # ---- region-aware resolve (events only) ------------------------

    def resolve_with_region(
        self,
        list_id: str,
        region_map: Dict[str, str],
        depth: int = 0,
        seen: Optional[Set[str]] = None,
        inherited_region: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Like ``resolve_simple`` but tags each leaf with its source region.

        *region_map*: ``{edid_substring: "Region Name"}``
        Returns: ``[{formid, chance, region}]``

        Uses the same pick-weight/cn-factor approach as resolve_simple.
        """
        if seen is None:
            seen = set()
        if list_id in seen or depth > 8:
            return []
        seen = seen | {list_id}

        flags = self.lvli.flags_for(list_id)
        is_use_all = flags["use_all"]
        is_first_match = flags["first_match"]

        # ── gather entries ──
        raw: List[Dict[str, Any]] = []
        for e in self.lvli.entries_by_list.get(list_id, []):
            idx = e.get("EntryIndex")
            if idx is None:
                continue
            math = self.lvli.math_by_entry.get((list_id, idx))
            if not math:
                continue

            pw, cn = self._entry_pick_and_cn(math, e, list_id)
            sub = (math.get("SubLVLI_FormID") or "").strip()
            ref = (e.get("LVLO_Reference") or "").strip()
            conditions = self._entry_conditions(e)

            if is_use_all and conditions:
                grp = self.extract_grp_chance(conditions)
                if grp is not None:
                    pw = grp
                    cn = 1.0

            raw.append({"pw": pw, "cn": cn, "sub": sub, "ref": ref,
                        "conditions": conditions})

        # ── compute rates (same logic as resolve_simple) ──
        if is_first_match:
            fm_rates = self.first_match_rates([r["conditions"] for r in raw])
            if fm_rates is not None:
                for i, r in enumerate(raw):
                    r["rate"] = fm_rates[i]
            else:
                cum_fail = 1.0
                for r in raw:
                    s = r["pw"] * r["cn"]
                    r["rate"] = s * cum_fail
                    cum_fail *= (1.0 - s)
        elif is_use_all:
            for r in raw:
                r["rate"] = r["pw"] * r["cn"]
        else:
            total_pw = sum(r["pw"] for r in raw)
            if total_pw > 0:
                for r in raw:
                    r["rate"] = (r["pw"] / total_pw) * r["cn"]
            else:
                for r in raw:
                    r["rate"] = 0.0

        # ── recurse / accumulate ──
        results: List[Dict[str, Any]] = []
        for r in raw:
            chance = r["rate"]
            sub = r["sub"]
            if sub:
                sub_edid = self.lvli.edid_by_formid.get(sub, "").lower()
                region = inherited_region
                for substr, rname in region_map.items():
                    if substr in sub_edid:
                        region = rname
                        break
                for item in self.resolve_with_region(
                    sub, region_map, depth + 1, seen, region
                ):
                    results.append({
                        "formid": item["formid"],
                        "chance": item["chance"] * chance,
                        "region": item["region"] or inherited_region,
                    })
            else:
                ref = r["ref"]
                if ":" in ref:
                    fid = ref.split(":")[0]
                    results.append({
                        "formid": fid,
                        "chance": chance,
                        "region": inherited_region,
                    })
        return results

    # ---- internal helpers ------------------------------------------

    @staticmethod
    def _entry_chance(math: Dict[str, str]) -> float:
        """Apply the rng-76 formula to a single LVLI entry.

        Detects MinLvl GLOBs that xEdit misinterprets as ChanceNone,
        contaminating EntryChanceNoneResolved, EntryPresenceChance, and
        EntryAprioriChance_NoSublist.  When a MinLvl GLOB is present,
        the contaminated fields are overridden to their correct values
        (entry_none=0, entry_pres=1, apriori=1).
        """
        # Detect MinLvl GLOBs in EntryChanceNoneGlobal — these set the
        # minimum player level, NOT a chance of nothing.  xEdit bakes
        # the GLOB FLTV into Resolved/Presence/Apriori as if it were a
        # ChanceNone percentage, producing wildly wrong (even negative)
        # drop rates.
        entry_cn_glob = (math.get("EntryChanceNoneGlobal") or "")
        is_minlvl = "MinLvl" in entry_cn_glob

        list_none  = float(math.get("ListChanceNoneResolved") or 0)
        cond_rand  = float(math.get("EntryCondChance_RandomPercent") or 1)

        if is_minlvl:
            entry_pres = 1.0
            entry_none = 0.0
            apriori    = 1.0
        else:
            entry_pres = float(math.get("EntryPresenceChance") or 1)
            entry_none = float(math.get("EntryChanceNoneResolved") or 0)
            apriori    = float(math.get("EntryAprioriChance_NoSublist") or 1)

        return (1 - list_none) * entry_pres * (1 - entry_none) * cond_rand * apriori

    def _resolve_entry_chancenone(
        self,
        math: Dict[str, str],
        entry: Optional[Dict[str, str]] = None,
        lvli_formid: str = "",
    ) -> float:
        """
        Resolve the entry-level ChanceNone, falling back to GLOB/CURV lookup.

        Returns the ChanceNone as a 0-1 fraction (e.g. 0.95 for 95%).

        When the Math TSV's ``EntryChanceNoneResolved`` is zero but the
        Entries TSV row carries a GLOB reference in ``LVOC_ChanceNoneCurve``,
        resolve the GLOB value.  If the owning LVLI also has a curve table
        mapping (CURV), the GLOB value is used as the X-axis input into the
        curve and the interpolated Y value is the actual ChanceNone.

        Examples:
          - GLOB=75, no CURV → ChanceNone = 75%
          - GLOB=10, CURV maps X=10→Y=95 → ChanceNone = 95%
        """
        # If a MinLvl GLOB contaminated EntryChanceNoneResolved, ignore it
        entry_cn_glob = (math.get("EntryChanceNoneGlobal") or "")
        is_minlvl = "MinLvl" in entry_cn_glob

        entry_none = float(math.get("EntryChanceNoneResolved") or 0)
        if not is_minlvl and (entry_none > 0 or entry is None):
            return entry_none
        if is_minlvl:
            entry_none = 0.0  # Not a real ChanceNone
        for field_name in ("LVOC_ChanceNoneCurve", "LVOG_ChanceNoneGlobal"):
            raw = (entry.get(field_name) or "").strip()
            if not raw:
                continue
            gfid = glob_formid_from_lvli_field(raw)
            if gfid:
                gval = self.globs.value(gfid)
                if gval is not None and gval > 0:
                    # Check for a curve table on this LVLI
                    if self.curvs and lvli_formid:
                        curv_fid = self.curvs.curv_for_lvli(lvli_formid)
                        if curv_fid:
                            y = self.curvs.interpolate(curv_fid, gval)
                            if y is not None:
                                return y / 100.0
                    # No curve — GLOB value IS the ChanceNone
                    return gval / 100.0
        return 0.0

    # ---- public ChanceNone resolver (used by builders) ----------------

    def resolve_chance_none(
        self,
        math_row: Dict[str, str],
        field_prefix: str = "Entry",
    ) -> float:
        """
        Resolve ChanceNone for an LVLI entry or list from a Math TSV row.

        Handles GLOB/CURV resolution including:
          - Direct GLOB values (FLTV is the ChanceNone)
          - GLOB as tier index into CURV table → Curve(X=FLTV) = Y ChanceNone
          - LVLI→CURV mapping for indirect curve references
          - ChanceNoneCurve field holding a GLOB ref (not actual curve)

        Priority (matches game engine behaviour):
          1. CURV-column GLOB with LVLI→CURV mapping → tier into curve → Y
          2. Real CURV + GLOB → GLOB FLTV as X into curve → Y
          3. <prefix>ChanceNoneResolved (if non-zero and no curve path)
          4. <prefix>ChanceNoneGlobal → GLOB FLTV directly
          5. <prefix>ChanceNoneCurve GLOB FLTV (fallback)
          6. 0.0 (= 100% drop chance)

        Returns ChanceNone as 0-100 (e.g. 90.0 = 90% nothing, 10% drop).
        """
        glob_ref = (math_row.get(f"{field_prefix}ChanceNoneGlobal") or "").strip()
        curv_ref = (math_row.get(f"{field_prefix}ChanceNoneCurve") or "").strip()

        # Detect MinLvl GLOBs misplaced in the ChanceNone slot by xEdit.
        # These set the minimum player level for the entry, NOT a chance of
        # nothing.  Their FLTV (e.g. 1.0 for MinLvl_Guaranteed_ECON) would
        # otherwise be treated as a tiny ChanceNone, producing wrong rates.
        # When a MinLvl GLOB is present, both the GLOB FLTV and xEdit's
        # pre-computed Resolved value are contaminated — return 0.0 (100% drop).
        _is_minlvl = "MinLvl" in glob_ref

        # Resolve GLOB FLTV
        glob_fltv = None
        if glob_ref and not _is_minlvl:
            gfid = glob_formid_from_lvli_field(glob_ref)
            if gfid:
                glob_fltv = self.globs.value(gfid)

        # Try curve-based resolution FIRST (before trusting Resolved)
        if curv_ref:
            curv_fid = glob_formid_from_lvli_field(curv_ref)
            # Check if curv_ref points to actual curve data
            has_pts = (
                self.curvs is not None
                and curv_fid is not None
                and curv_fid in self.curvs.points
            )

            if has_pts and glob_fltv is not None:
                # Direct curve: GLOB FLTV is X, curve Y is ChanceNone
                y = self.curvs.interpolate(curv_fid, glob_fltv)
                if y is not None:
                    return y

            # ChanceNoneCurve field holds a GLOB ref (no actual curve
            # points in the CURV index for that FormID).  The GLOB's FLTV
            # is the X index into a CURV table that the LVLI references
            # via its LVOT field (not exported by the TSV script).
            # Use curv_for_lvli() to find the mapped CURV, then
            # interpolate GLOB FLTV (X) → Y = actual ChanceNone.
            if not has_pts and curv_fid:
                tier_fltv = self.globs.value(curv_fid)
                if tier_fltv is not None:
                    lvli_fid = (math_row.get("LVLI_FormID") or "").strip()
                    if self.curvs and lvli_fid:
                        mapped_curv = self.curvs.curv_for_lvli(lvli_fid)
                        if mapped_curv:
                            y = self.curvs.interpolate(mapped_curv, tier_fltv)
                            if y is not None:
                                return y
                    # No mapped curve found — fall back to GLOB value
                    if glob_fltv is None:
                        glob_fltv = tier_fltv

        # Fall back to xEdit's pre-computed Resolved value (skip when a
        # MinLvl GLOB contaminated it — the Resolved value would be the
        # minimum level, not a real ChanceNone percentage).
        if not _is_minlvl:
            resolved = safe_float(
                math_row.get(f"{field_prefix}ChanceNoneResolved"), 0.0
            )
            if resolved and resolved > 0:
                return resolved

        if glob_fltv is not None and glob_fltv > 0:
            return glob_fltv

        return 0.0

    def _grp_value(self, token: str) -> Optional[float]:
        """Resolve a GetRandomPercent right-hand side to a number.

        Accepts a literal (``20.000000``) or an xEdit GLOB reference
        (``EDID [GLOB:XXXXXXXX]``), whose FLTV is the threshold.
        """
        token = (token or "").strip()
        if not token:
            return None
        gm = re.search(r"\[GLOB:([0-9A-Fa-f]+)\]", token)
        if gm:
            return self.globs.value(gm.group(1))
        head = token.split()[0]
        try:
            return float(head)
        except ValueError:
            return None

    def extract_grp_span(
        self,
        raw_conds: List[str],
    ) -> Optional[Tuple[float, float]]:
        """
        The slice of the 0–100 roll satisfying the entry's GetRandomPercent
        condition(s), as ``(lo, hi)``.  None when the entry has no resolvable
        GetRandomPercent gate (which means "always passes").

        Multiple GetRandomPercent conditions on one entry are intersected.
        """
        span: Optional[Tuple[float, float]] = None
        for cond in raw_conds or []:
            parsed = parse_grp_condition(cond)
            if not parsed:
                continue
            op, token = parsed
            value = self._grp_value(token)
            if value is None:
                continue
            lo, hi = grp_span_for_value(op, value)
            span = (lo, hi) if span is None else (max(span[0], lo), min(span[1], hi))
        if span is None:
            return None
        return (span[0], max(span[0], span[1]))

    def extract_grp_chance(self, raw_conds: List[str]) -> Optional[float]:
        """Probability (0–1) that the entry's GetRandomPercent gate passes."""
        span = self.extract_grp_span(raw_conds)
        if span is None:
            return None
        return max(0.0, span[1] - span[0]) / 100.0

    def extract_grp_threshold(
        self,
        raw_conds: List[str],
    ) -> Optional[float]:
        """
        Raw ``GetRandomPercent`` threshold from condition strings — the number
        on the right of the comparison, ignoring which comparison it is.

        Kept for callers that only want to know "is there a GRP gate, and what
        number is on it".  For anything that needs a PROBABILITY, use
        ``extract_grp_chance()``/``extract_grp_span()`` instead: this value is
        direction-blind and treating it as a rate is what produced negative
        drop rates on the collectron lists (they compare with ``>=``).
        """
        for cond in raw_conds or []:
            parsed = parse_grp_condition(cond)
            if not parsed:
                continue
            value = self._grp_value(parsed[1])
            if value is not None:
                return value
        return None

    def first_match_rates(
        self,
        cond_lists: List[List[str]],
    ) -> Optional[List[float]]:
        """
        Per-entry selection rates (0–1) for a First Match (bit 6) list.

        One random roll is taken and entries are checked top-to-bottom; the
        first entry whose condition matches wins.  Each entry therefore gets the
        part of the roll range its condition covers that no earlier entry
        already claimed, and an entry with no condition sweeps up everything
        left.  Rates total 100%.

        Works for ascending ``<=`` lists and descending ``>=`` lists alike, and
        for lists that mix the two.  Returns None when no entry has a resolvable
        GetRandomPercent gate, so the caller can fall back to the ChanceNone
        cascade.
        """
        spans = [self.extract_grp_span(c) for c in cond_lists]
        if not any(s is not None for s in spans):
            return None
        rates: List[float] = []
        covered: List[Tuple[float, float]] = []
        for span in spans:
            eff = span if span is not None else (0.0, 100.0)
            rates.append(span_uncovered_width(eff, covered) / 100.0)
            covered = span_union(covered, eff)
        return rates

    # ---- internal helpers ------------------------------------------

    def _entry_qty(self, entry: Dict[str, str]) -> int:
        """Resolve quantity from entry row, including GLOB override."""
        qty_raw = (
            entry.get("LVIV_Quantity")
            or entry.get("LVLO_Count")
            or entry.get("Count")
            or "1"
        )
        try:
            qty = int(float(qty_raw))
        except (ValueError, TypeError):
            qty = 1

        qty_glob_ref = (entry.get("LVIG_QuantityGlobal") or "").strip()
        if qty_glob_ref:
            glob_fid = qty_glob_ref.split(":")[0] if ":" in qty_glob_ref else qty_glob_ref
            gv = self.globs.value(glob_fid)
            if gv is not None:
                qty = int(gv)

        return qty

    @staticmethod
    def _entry_conditions(entry: Dict[str, str]) -> List[str]:
        """Collect Cond1 .. Cond10 from an LVLI entry row."""
        conds: List[str] = []
        for i in range(1, 11):
            v = (entry.get(f"Cond{i}") or "").strip()
            if v:
                conds.append(v)
        return conds


# ============================================================
# 9. TIER DETECTION (used by titles)
# ============================================================

def tier_info_from_edid(edid: str) -> Optional[Tuple[str, str, int]]:
    """
    Detect tier patterns in LVLI EDIDs.

    Returns ``(family_key, tier_label, sort_order)`` or None.

    Supported patterns:
      - Bad / Good / Best         → orders 1 / 2 / 3
      - Tier_N / Reward_N         → "Tier N",   order N
      - Reward_Alt_N / Tier_Alt_N → "Mutated Tier N", order N + 100
      - Loot* / Crafted*          → preserved in family key
    """
    e = (edid or "").strip()
    if not e:
        return None
    el = e.lower()

    # Bad / Good / Best
    if "bad" in el or "good" in el or "best" in el:
        for lab, order in (("bad", 1), ("good", 2), ("best", 3)):
            if re.search(rf"(?:^|[_\-]){lab}(?:$|[_\-])", el):
                fam = re.sub(rf"([_\-]){lab}([_\-]|$)", r"\1", el)
                fam = re.sub(r"[_\-]+$", "", fam)
                fam = re.sub(r"[_\-]+", "_", fam).strip("_")
                return (fam, lab.capitalize(), order)

    # Mutated / Alt tier style
    m_alt_all = list(re.finditer(r"(tier|reward)[_\-]*alt[_\-]*(0?\d{1,2})\b", el))
    if m_alt_all:
        m = m_alt_all[-1]
        n = int(m.group(2).lstrip("0") or "0")
        if n > 0:
            fam = el[:m.start()] + el[m.end():]
            fam = re.sub(r"[_\-]+", "_", fam).strip("_")
            return (fam, f"Mutated Tier {n}", n + 100)

    # Numeric tier / reward
    m_all = list(re.finditer(r"(tier|reward)[_\-]*(0?\d{1,2})\b", el))
    if m_all:
        m = m_all[-1]
        n = int(m.group(2).lstrip("0") or "0")
        if n <= 0:
            return None
        fam = el[:m.start()] + el[m.end():]
        fam = re.sub(r"[_\-]+", "_", fam).strip("_")
        return (fam, f"Tier {n}", n)

    return None


# ============================================================
# 10. ChanceNone-BASED RATE RESOLVER (used by titles)
# ============================================================

def glob_formid_from_lvli_field(s: str) -> Optional[str]:
    """
    Extract a GLOB FormID from an LVLI global/curve field.

    Handles:
      - ``"0089EA90:Something:GLOB"``
      - ``"Name [GLOB:0085AD24]"``
      - Bare 8-hex FormID
      - Any embedded 8-hex word
    """
    s = (s or "").strip()
    if not s:
        return None
    m = re.match(r"^([0-9A-Fa-f]{8}):", s)
    if m:
        return m.group(1).upper()
    m2 = re.search(r"\[GLOB:([0-9A-Fa-f]{8})\]", s)
    if m2:
        return m2.group(1).upper()
    m3 = re.fullmatch(r"[0-9A-Fa-f]{8}", s)
    if m3:
        return s.upper()
    m4 = re.search(r"\b([0-9A-Fa-f]{8})\b", s)
    if m4:
        return m4.group(1).upper()
    return None


def compute_chancenone_rate(
    entry_row: Dict[str, str],
    lvli_fid: str,
    lvli_index: LvliIndex,
    glob_index: GlobIndex,
    curv_index: Optional["CurvIndex"] = None,
) -> Optional[str]:
    """
    Compute drop rate from ChanceNone fields (GLOB-first, curve-aware).

    Used by the titles/drop-rates builds for COBJ → BOOK → LVLI rate resolution.

    When a GLOB and CURV are both present, the GLOB FLTV is the X index
    into the curve table and the Y output is the actual ChanceNone.
    When only a GLOB is present, the FLTV IS the direct ChanceNone.

    Priority:
      1. Entry-level GLOB + CURV → Curve(X=FLTV) → 100 - Y
         Entry-level GLOB only   → 100 - FLTV
      2. List-level  GLOB + CURV → same curve logic
         List-level  GLOB only   → 100 - FLTV
      3. LVOV_ChanceNoneValue → 100 - value
      4. Blank/missing → 100%
    """
    list_row = lvli_index.list_by_formid.get(lvli_fid)

    # --- Helper: resolve a GLOB+CURV pair to ChanceNone (0-100) ---
    def _resolve_pair(glob_field: str, curv_field: str) -> Optional[float]:
        glob_fid = glob_formid_from_lvli_field(glob_field) if glob_field else None
        curv_fid = glob_formid_from_lvli_field(curv_field) if curv_field else None

        glob_fltv = glob_index.value(glob_fid) if glob_fid else None
        if glob_fltv is None and curv_fid:
            # Curve slot sometimes holds a GLOB ref — try as GLOB fallback
            glob_fltv = glob_index.value(curv_fid)
            if glob_fltv is not None:
                curv_fid = None  # not a real curve

        if glob_fltv is None:
            return None

        # If we have a real curve, evaluate it with the GLOB FLTV as X
        if curv_index and curv_fid:
            y = curv_index.interpolate(curv_fid, glob_fltv)
            if y is not None:
                return y  # Y is the actual ChanceNone

        # No curve — GLOB FLTV is the direct ChanceNone
        return glob_fltv

    # --- Entry-level ---
    lvog = (entry_row.get("LVOG_ChanceNoneGlobal") or "").strip()
    lvoc = (entry_row.get("LVOC_ChanceNoneCurve") or "").strip()
    cn = _resolve_pair(lvog, lvoc)
    if cn is not None and cn > 0:
        p = 100.0 - cn
        return fmt_pct(p) if p >= 0 else None

    # --- List-level ---
    if list_row:
        lvlg = (list_row.get("LVLG_ChanceNoneGlobal") or "").strip()
        lvct = (list_row.get("LVCT_ChanceNoneCurve") or "").strip()
        cn = _resolve_pair(lvlg, lvct)
        if cn is not None and cn > 0:
            p = 100.0 - cn
            return fmt_pct(p) if p >= 0 else None

    # Fallback: LVOV_ChanceNoneValue
    raw_cn = (entry_row.get("LVOV_ChanceNoneValue") or "").strip()
    if not raw_cn:
        return "100%"

    chance_none = safe_float(raw_cn)
    if chance_none is None:
        return None
    if abs(chance_none) < 1e-9:
        return "100%"

    p = 100.0 - chance_none
    if p < 0:
        return None
    return fmt_pct(p)


# ============================================================
# 11. POOL DEDUPLICATION
# ============================================================

def merge_duplicate_pools(pools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge pools that share the same ``lvliFormID`` but have mutually
    exclusive conditions (e.g. two GLOB toggle states).
    """
    seen: Dict[str, int] = {}
    merged: List[Dict[str, Any]] = []

    for pool in pools:
        lvli_fid = pool.get("lvliFormID", "")
        if not lvli_fid or lvli_fid not in seen:
            seen[lvli_fid] = len(merged)
            merged.append(pool)
        else:
            existing = merged[seen[lvli_fid]]
            existing_conds = existing.get("conditions", [])
            new_conds = pool.get("conditions", [])
            if new_conds and new_conds != existing_conds:
                existing["conditions"] = existing_conds + new_conds
                existing["conditionSummary"] = "Chance-based drop (toggle)"

    return merged


# ============================================================
# 12. LABEL PRETTIFICATION
# ============================================================

LVLI_LABEL_OVERRIDES: Dict[str, str] = {
    "enclave_plasmagun": "Enclave Plasma Gun Mod Boxes",
    "enclaveplasmagun":  "Enclave Plasma Gun Mod Boxes",
    "plasmagun_all":     "Enclave Plasma Gun Mod Boxes",
    "rewards_activit":   "Activity Rewards",
    "rewards_enclave":   "Enclave Activity Rewards",
}


def prettify_lvli_label(edid: str) -> str:
    """Turn an LVLI EDID into a human-friendly pool title."""
    t = (edid or "").strip()
    if not t:
        return ""
    tl = t.lower()
    for substr, label in LVLI_LABEL_OVERRIDES.items():
        if substr in tl:
            return label
    t = re.sub(
        r"^(LLS?|RA_LL|RA_LLS|RA|LL|QuestReward|Quest_Reward|Rewards)_+",
        "", t, flags=re.IGNORECASE,
    )
    t = re.sub(r"^LL_", "", t, flags=re.IGNORECASE)
    t = t.replace("__", "_").replace("_", " ").strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\bPublic Events\b", "Public Event Rewards", t, flags=re.IGNORECASE)
    t = re.sub(r"\bPublic Event Rewards Rewards\b", "Public Event Rewards", t, flags=re.IGNORECASE)
    t = re.sub(r"\bQuest Reward\b", "Event Rewards", t, flags=re.IGNORECASE)

    def _title(s):
        return " ".join(w.capitalize() if w else w for w in s.split())

    t = _title(t).replace(" Ll ", " LL ")
    return t.strip()


# ============================================================
# 13. REGION MAP (for events rewards)
# ============================================================

REGION_BY_SUBLVLI_EDID: Dict[str, str] = {
    "regionforest":         "Forest",  # sync

    "regionashheap":        "Ash Heap",
    "regioncranberrybog":   "Cranberry Bog",
    "regionmire":           "Mire",
    "regionsavagedivide":   "Savage Divide",
    "regiontoxicvalley":    "Toxic Valley",
    "regionskylinevalley":  "Skyline Valley",
    "regionburningsprings": "Burning Springs",
}


# ============================================================
# 14. CONVENIENCE: load everything from tsv/ root
# ============================================================

class Rng76Data:
    """
    One-shot loader: call ``Rng76Data.from_tsv_root("tsv/")``
    and get back a populated resolver + all indexes.
    """

    def __init__(self) -> None:
        self.lvli = LvliIndex()
        self.globs = GlobIndex()
        self.names = ItemNameIndex()
        self.curvs = CurvIndex()
        self.resolver: Optional[Rng76Resolver] = None

    @classmethod
    def from_tsv_root(cls, tsv_root: str) -> "Rng76Data":
        d = cls()
        d.lvli.load_from_tsv_root(tsv_root)
        d.globs.load_from_tsv_root(tsv_root)
        d.names.load_all_from_tsv_root(tsv_root)
        d.curvs.load_from_tsv_root(tsv_root)
        d.resolver = Rng76Resolver(d.lvli, d.globs, d.names, d.curvs)
        return d
