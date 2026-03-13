#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rng76.py — Shared Fallout 76 Drop Rate Engine

Centralises all LVLI tree walking, GLOB lookups, ChanceNone math,
tier detection, item-name resolution, and adaptive precision formatting
so that every page-specific build script can import from ONE place.

Used by:
  build_events_rewards_json.py   (event/activity reward pages)
  build_reho_json.py             (Raid · Expo · Hunts · Ops pages)
  build_titles_json.py           (Player & Camp title checklists)

Core formula (rng-76):
  dropRate = (1 - ListChanceNone)
           × EntryPresenceChance
           × (1 - EntryChanceNone)
           × CondChance            (GetRandomPercent)
           × AprioriChance         (pick-one weight)
"""

from __future__ import annotations

import csv
import glob as _glob
import os
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple


# ============================================================
# 1. TSV LOADING UTILITIES
# ============================================================

def newest(pattern: str) -> str:
    """Return the most-recently-modified file matching *pattern*."""
    files = _glob.glob(pattern)
    if not files:
        raise FileNotFoundError(pattern)
    files.sort(key=lambda x: os.path.getmtime(x))
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

    def load_all_from_tsv_root(self, tsv_root: str) -> None:
        """Auto-discover and load all item-name TSVs from *tsv_root*."""
        def _safe_load(pattern, loader, exclude_suffixes=None):
            files = _glob.glob(os.path.join(tsv_root, pattern))
            if exclude_suffixes:
                files = [f for f in files
                         if not any(suf in os.path.basename(f)
                                    for suf in exclude_suffixes)]
            if not files:
                return
            files.sort(key=lambda x: os.path.getmtime(x))
            loader(read_tsv(files[-1]))

        _safe_load("BOOK_Export_*.tsv", self.load_book,
                   exclude_suffixes=["_Locations"])
        _safe_load("MISC_Export_*.tsv", self.load_misc)
        _safe_load("KEYM_Export_*.tsv", self.load_keym,
                   exclude_suffixes=["_Locations", "_Refs", "_KYWD"])
        _safe_load("ARMO_Export_*.tsv", self.load_armo,
                   exclude_suffixes=["_SLOTS"])
        _safe_load("WEAP_Export_*.tsv", self.load_weap)
        _safe_load("ALCH_Export_*.tsv", self.load_alch)
        _safe_load("AMMO_Export_*.tsv", self.load_ammo)
        _safe_load("CREA_Export_*.tsv", self.load_crea)

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

    def load_from_tsv_root(self, tsv_root: str) -> None:
        try:
            self.load(read_tsv(newest(os.path.join(tsv_root, "GLOB_Export_*.tsv"))))
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


# ============================================================
# 5. FLAG PARSING
# ============================================================

def parse_lvlf_flags(flags_str: str) -> Dict[str, bool]:
    """
    Parse LVLF_Flags binary string.

    Returns: ``{use_all, for_each, level_filter, first_match}``
    """
    flags_str = (flags_str or "").strip()
    if not flags_str:
        return {"use_all": False, "for_each": False,
                "level_filter": False, "first_match": False}
    try:
        if all(c in "01" for c in flags_str):
            v = int(flags_str, 2)
        else:
            v = int(flags_str)
    except ValueError:
        v = 0
    return {
        "use_all":      bool(v & 4),
        "for_each":     bool(v & 2),
        "level_filter": bool(v & 1),
        "first_match":  bool(v & 64),
    }


# ============================================================
# 6. GetRandomPercent CONDITION PARSING
# ============================================================

def parse_randompercent_multiplier(conditions_text: str) -> float:
    """
    Extract ``GetRandomPercent <= N`` from condition strings and return
    the combined multiplier (0.0 – 1.0).

    Handles two xEdit export formats:
      - ``GetRandomPercent <= 50``   (standard)
      - ``GetRandomPercent 10100000 50.000000``  (raw GMRW flags format)
    """
    mult = 1.0
    for m in re.finditer(
        r"GetRandomPercent\s*<=\s*(\d+(?:\.\d+)?)",
        conditions_text or "", flags=re.IGNORECASE
    ):
        try:
            n = max(0, min(100, float(m.group(1))))
            mult *= n / 100.0
        except ValueError:
            pass
    for m in re.finditer(
        r"GetRandomPercent\s+\d+\s+(\d+(?:\.\d+)?)",
        conditions_text or "", flags=re.IGNORECASE
    ):
        try:
            n = max(0, min(100, float(m.group(1))))
            mult *= n / 100.0
        except ValueError:
            pass
    return mult


# ============================================================
# 7. ADAPTIVE PRECISION FORMATTING
# ============================================================

def pct(x: float) -> float:
    """Convert a 0-1 probability to a 0-100 percentage, 6 dp."""
    return round(max(0.0, float(x)) * 100, 6)


def fmt_pct(value: float) -> str:
    """
    Format a percentage value as a clean string.

    - Integer values:  ``"10%"``
    - Small values:    ``"0.05%"``
    - Tiny values:     ``"0.0012%"``

    Adaptive: uses the fewest decimal places needed.
    """
    if abs(value - round(value)) < 1e-6:
        return f"{int(round(value))}%"
    # 2 dp for common rates
    if value >= 0.01:
        s = f"{value:.2f}".rstrip("0").rstrip(".")
        return f"{s}%"
    # 4 dp for small
    if value >= 0.0001:
        s = f"{value:.4f}".rstrip("0").rstrip(".")
        return f"{s}%"
    # 6 dp for very small
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
    ) -> None:
        self.lvli = lvli
        self.globs = globs
        self.names = names
        self._cache: Dict[str, Dict[str, float]] = {}

    # ---- quick (cached) resolve ------------------------------------

    def resolve_simple(self, list_id: str) -> Dict[str, float]:
        """
        Resolve LVLI → ``{leaf_formid: total_chance}``.

        Cached.  Used by ``compute_lvli()`` in the events script.
        """
        if not list_id:
            return {}
        if list_id in self._cache:
            return self._cache[list_id]
        results: Dict[str, float] = {}
        for e in self.lvli.entries_by_list.get(list_id, []):
            idx = e.get("EntryIndex")
            if idx is None:
                continue
            math = self.lvli.math_by_entry.get((list_id, idx))
            if not math:
                continue
            sub = (math.get("SubLVLI_FormID") or "").strip()
            chance = self._entry_chance(math)
            if sub:
                for k, v in self.resolve_simple(sub).items():
                    results[k] = results.get(k, 0) + v * chance
            else:
                ref = (e.get("LVLO_Reference") or "").strip()
                if ":" in ref:
                    fid = ref.split(":")[0]
                    results[fid] = results.get(fid, 0) + chance
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

        Applies pick-one normalisation when Use All is off and total > 1.
        """
        if seen is None:
            seen = set()
        if list_id in seen or depth > 50:
            return []
        seen = seen | {list_id}

        flags = self.lvli.flags_for(list_id)
        is_use_all = flags["use_all"]

        items: List[Dict[str, Any]] = []
        for entry in self.lvli.entries_by_list.get(list_id, []):
            idx = entry.get("EntryIndex")
            if idx is None:
                continue
            math = self.lvli.math_by_entry.get((list_id, idx))
            if not math:
                continue

            drop_rate = self._entry_chance(math)
            qty = self._entry_qty(entry)
            conditions = self._entry_conditions(entry)
            sub_lvli = (math.get("SubLVLI_FormID") or "").strip()
            ref = (entry.get("LVLO_Reference") or "").strip()
            ref_sig = ref.split(":")[-1].upper() if ref.count(":") >= 2 else ""

            if sub_lvli:
                for sub_item in self.resolve_deep(sub_lvli, depth + 1, seen):
                    items.append({
                        "formid":     sub_item["formid"],
                        "name":       sub_item["name"],
                        "qty":        sub_item["qty"],
                        "dropRate":   sub_item["dropRate"] * drop_rate,
                        "edid":       sub_item["edid"],
                        "sig":        sub_item.get("sig", ""),
                        "conditions": conditions + (sub_item.get("conditions") or []),
                    })
            else:
                if ":" in ref:
                    fid = ref.split(":")[0]
                    edid = ref.split(":")[1] if len(ref.split(":")) > 1 else ""
                    name = self.names.resolve(fid, edid)
                    items.append({
                        "formid":     fid,
                        "name":       name,
                        "qty":        qty,
                        "dropRate":   drop_rate,
                        "edid":       edid,
                        "sig":        ref_sig,
                        "conditions": conditions,
                    })

        # Pick-one normalisation
        if not is_use_all and items:
            total = sum(it["dropRate"] for it in items)
            if total > 1.001:
                for it in items:
                    it["dropRate"] = it["dropRate"] / total

        return items

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
        """
        if seen is None:
            seen = set()
        if list_id in seen or depth > 8:
            return []
        seen = seen | {list_id}
        results: List[Dict[str, Any]] = []

        for e in self.lvli.entries_by_list.get(list_id, []):
            idx = e.get("EntryIndex")
            if idx is None:
                continue
            math = self.lvli.math_by_entry.get((list_id, idx))
            if not math:
                continue
            sub = (math.get("SubLVLI_FormID") or "").strip()
            chance = self._entry_chance(math)
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
                ref = (e.get("LVLO_Reference") or "").strip()
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
        """Apply the rng-76 formula to a single LVLI entry."""
        list_none  = float(math.get("ListChanceNoneResolved") or 0)
        entry_pres = float(math.get("EntryPresenceChance") or 1)
        entry_none = float(math.get("EntryChanceNoneResolved") or 0)
        cond_rand  = float(math.get("EntryCondChance_RandomPercent") or 1)
        apriori    = float(math.get("EntryAprioriChance_NoSublist") or 1)
        return (1 - list_none) * entry_pres * (1 - entry_none) * cond_rand * apriori

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
) -> Optional[str]:
    """
    Compute drop rate from ChanceNone fields (GLOB-first rule).

    Used by the titles build for COBJ → BOOK → LVLI rate resolution.

    Priority (first non-None wins):
      1. Entry-level LVOG_ChanceNoneGlobal → GLOB FLTV → 100 - FLTV
      2. Entry-level LVOC_ChanceNoneCurve → same
      3. List-level LVLG_ChanceNoneGlobal → same
      4. List-level LVCT_ChanceNoneCurve → same
      5. LVOV_ChanceNoneValue → 100 - value
      6. Blank/missing → 100%
    """
    list_row = lvli_index.list_by_formid.get(lvli_fid)

    # Collect GLOB candidates (order matters)
    candidates: List[str] = []

    lvog = (entry_row.get("LVOG_ChanceNoneGlobal") or "").strip()
    if lvog:
        candidates.append(lvog)

    lvoc = (entry_row.get("LVOC_ChanceNoneCurve") or "").strip()
    if lvoc:
        candidates.append(lvoc)

    if list_row:
        lvlg = (list_row.get("LVLG_ChanceNoneGlobal") or "").strip()
        if lvlg:
            candidates.append(lvlg)
        lvct = (list_row.get("LVCT_ChanceNoneCurve") or "").strip()
        if lvct:
            candidates.append(lvct)

    for field in candidates:
        gfid = glob_formid_from_lvli_field(field)
        if not gfid:
            continue
        dr = glob_index.drop_rate_str(gfid)
        if dr:
            return dr

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
    "regionforest":         "Forest",
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
        self.resolver: Optional[Rng76Resolver] = None

    @classmethod
    def from_tsv_root(cls, tsv_root: str) -> "Rng76Data":
        d = cls()
        d.lvli.load_from_tsv_root(tsv_root)
        d.globs.load_from_tsv_root(tsv_root)
        d.names.load_all_from_tsv_root(tsv_root)
        d.resolver = Rng76Resolver(d.lvli, d.globs, d.names)
        return d
