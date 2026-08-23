#!/usr/bin/env python3
"""Build legendary_crafting_planner.json from COBJ + OMOD + CURV TSV exports.

Generates the JSON consumed by df-bnb-legendary-crafting-planner.js.
Reads shard-crafting and random-roll COBJ recipes, resolves ingredient
quantities via CURV_POINTS, and looks up display names from OMOD/ALCH/MISC.

Usage (local):   python src/build_legendary_crafting_planner_json.py
Usage (CI):      python src/build_legendary_crafting_planner_json.py --tsv-dir tsv --out-dir dist
"""

import argparse
import csv
import glob
import json
import os
import re
from datetime import date
from pathlib import Path
import tsv_source          # one resolver for every export selection

# ── CLI ────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("--tsv-dir", default="tsv", help="Folder containing TSV exports")
parser.add_argument("--out-dir", default="dist", help="Output folder for JSON files")
args = parser.parse_args()

TSV_DIR = Path(args.tsv_dir)
OUT_DIR = Path(args.out_dir)
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

def newest(pattern):
    """Return the newest file matching *pattern* under TSV_DIR."""
    hits = glob.glob(str(TSV_DIR / pattern))
    return max(hits, key=tsv_source.export_key) if hits else None


def read_tsv(path):
    """Read a tab-separated file into a list of dicts."""
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", errors="replace") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def safe_int(val, default=None):
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return default


def pick(row, *keys, default=""):
    """Return the first non-empty value for the given keys."""
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return default


# ── Curve Resolution ──────────────────────────────────────────────────────────

def build_curve_lookup(curv_rows):
    """Build {curve_edid: {int(x): int(y)}} from the CURV_POINTS TSV."""
    curves = {}
    for r in curv_rows:
        edid = r.get("EDID", "").strip()
        x = safe_int(r.get("X"))
        y = safe_int(r.get("Y"))
        if edid and x is not None and y is not None:
            curves.setdefault(edid, {})[x] = y
    return curves


def resolve_qty(base_qty, curve_name, curves):
    """Look up the resolved quantity via the curve table; fall back to base."""
    if not curve_name or curve_name not in curves:
        return base_qty
    return curves[curve_name].get(base_qty, base_qty)


# ── Ingredient Name Resolution ────────────────────────────────────────────────

# Hardcoded names for c_* component EDIDs and other items whose TSV EDID
# differs from the FVPA ingredient EDID.  Updated when new ingredients appear.
_KNOWN_NAMES = {
    # Components (c_* prefix)
    "c_Acid":                "Acid",
    "c_AntiBallisticFiber":  "Ballistic Fiber",
    "c_Asbestos":            "Asbestos",
    "c_BlackTitanium":       "Black Titanium",
    "c_BlackTitanium_scrap": "Black Titanium Scrap",
    "c_Circuitry":           "Circuitry",
    "c_Cloth":               "Cloth",
    "c_Concrete":            "Concrete",
    "c_Cork":                "Cork",
    "c_Crystal_scrap":       "Crystal",
    "c_FiberOptics":         "Fiber Optics",
    "c_Fiberglass":          "Fiberglass",
    "c_Glass":               "Glass",
    "c_Gunpowder":           "Gunpowder",
    "c_Lead":                "Lead",
    "c_Leather":             "Leather",
    "c_LegendaryModule":     "Legendary Module",
    "c_NuclearMaterial":      "Nuclear Material",
    "c_NukeFlora_Blue":      "Cobalt Flux",
    "c_NukeFlora_Orange":    "Fluorescent Flux",
    "c_NukeFlora_Purple":    "Violet Flux",
    "c_NukeFlora_Red":       "Crimson Flux",
    "c_Oil":                 "Oil",
    "c_Plastic":             "Plastic",
    "c_Rubber":              "Rubber",
    "c_Rubber_scrap":        "Rubber",
    "c_Springs":             "Springs",
    "c_Steel":               "Steel",
    "c_Steel_Vault94_scrap": "Vault 94 Steel",
    "c_Ultracite":           "Ultracite",
    # Ammo
    "AmmoCannonBall":        "Cannonball",
    "AmmoFusionCore":        "Fusion Core",
    # Legendary tokens (used in attach recipes, but included for completeness)
    "LegendaryTokens":       "Legendary Scrip",
}


def _humanize_edid(edid):
    """Best-effort EDID → display name: strip c_, split on camelCase."""
    name = edid
    if name.startswith("c_"):
        name = name[2:]
    name = name.replace("_", " ")
    name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)
    return name.strip()


def build_name_lookup(alch_rows, misc_rows, ammo_rows):
    """Merge ALCH, MISC, and AMMO TSV EDID→FULL maps with hardcoded names."""
    lookup = dict(_KNOWN_NAMES)
    for r in misc_rows:
        edid = pick(r, "EDID")
        full = pick(r, "FULL")
        if edid and full:
            lookup.setdefault(edid, full)
    for r in alch_rows:
        edid = pick(r, "ALCH_EDID")
        full = pick(r, "FULL")
        if edid and full:
            lookup.setdefault(edid, full)
    for r in ammo_rows:
        edid = pick(r, "EDID", "AMMO_EDID")
        full = pick(r, "FULL")
        if edid and full:
            lookup.setdefault(edid, full)
    return lookup


def resolve_name(edid, lookup):
    """Return the display name for an ingredient EDID."""
    if edid in lookup:
        return lookup[edid]
    return _humanize_edid(edid)


# ── Shard & Random-Roll Parsing ───────────────────────────────────────────────

# Matches CNAM_EDID: [Prefix_]LegendaryShard_{Type}{Star}_{Effect}
_SHARD_CNAM_RE = re.compile(
    r"^(?:HTO_|RA_|P62_|SDOW_|BOUNTY_)?"
    r"LegendaryShard_(Armors?|Weapons?|Shared|PowerArmor)(\d)_(.+)$"
)

# Matches random-roll COBJ_EDID: co_mod_Legendary_Crafting_{Type}{Star}
_RANDOM_RE = re.compile(
    r"^co_mod_Legendary_Crafting_(Armor|Weapon|PowerArmor)(\d)$"
)

# Filter: shard-crafting COBJ_EDID
_SHARD_COBJ_RE = re.compile(
    r"^(?:HTO_|RA_|P62_|SDOW_|BOUNTY_)?co_LegendaryShard_"
)

# Strip star symbols ★☆ and replacement chars � from CNAM_FULL
_STARS_PREFIX_RE = re.compile(r"^[★☆�\s]+")


def _is_disabled(cobj_edid):
    """True if the record is disabled / WIP / deleted."""
    return cobj_edid.startswith("zzz_") or cobj_edid.startswith("DEL_")


def _detect_source(cobj_edid):
    """Derive the source tag from the COBJ_EDID prefix."""
    if "HTO_" in cobj_edid:
        return "HTO"
    if "BOUNTY_" in cobj_edid:
        return "Bounty"
    if cobj_edid.startswith("RA_"):
        return "Raid"
    # P62_, SDOW_, and unprefixed are all "Base"
    return "Base"


def _normalise_slot(raw):
    """Normalise shard type string → canonical slotType."""
    low = raw.lower()
    if low.startswith("armor"):
        return "Armor"
    if low.startswith("weapon"):
        return "Weapon"
    return raw  # Shared, PowerArmor


def _build_category(slot_type, stars, source, is_random=False):
    """Build the category string shown in the planner UI."""
    if is_random:
        if slot_type == "PowerArmor":
            return "Random Roll -- Power Armor"
        return f"Random Roll -- {slot_type}"
    if source == "HTO":
        return f"HTO Exclusive -- {stars}-Star"
    if source == "Raid":
        return f"Raid Exclusive -- {stars}-Star"
    if slot_type == "Shared":
        return f"Shared (Armor & Weapon) -- {stars}-Star"
    return f"{slot_type} -- {stars}-Star"


def _parse_fvpa(fvpa_str, curves, name_lookup):
    """Parse the FVPA pipe-delimited ingredient list.

    Format per segment: ComponentEDID:qty[:CurveTable]
    Segments separated by |.
    """
    if not fvpa_str or fvpa_str.strip() in ("", "0"):
        return []
    ingredients = []
    for part in fvpa_str.split("|"):
        part = part.strip()
        if not part:
            continue
        fields = part.split(":")
        if len(fields) < 2:
            continue
        ing_edid = fields[0].strip()
        if not ing_edid:
            continue
        raw_qty = safe_int(fields[1], 1)
        curve = fields[2].strip() if len(fields) > 2 else ""
        resolved_qty = resolve_qty(raw_qty, curve, curves) if curve else raw_qty
        ing_name = resolve_name(ing_edid, name_lookup)
        ingredients.append({
            "edid": ing_edid,
            "qty": resolved_qty,
            "curve": curve,
            "name": ing_name,
        })
    return ingredients


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    # --- Locate TSVs ---
    cobj_path = newest("COBJ_Export_*.tsv")
    if not cobj_path:
        print("ERROR: No COBJ_Export TSV found in", TSV_DIR)
        return

    curv_path = newest("CURV_Export_*_POINTS.tsv") or newest("CURV_POINTS.tsv")
    omod_path = newest("OMOD_Export_*.tsv")
    alch_path = newest("ALCH_Export_*.tsv")
    misc_path = newest("MISC_Export_*.tsv")
    ammo_path = newest("AMMO_Export_*.tsv")

    for label, p in [
        ("COBJ", cobj_path), ("CURV_PTS", curv_path), ("OMOD", omod_path),
        ("ALCH", alch_path), ("MISC", misc_path), ("AMMO", ammo_path),
    ]:
        print(f"  {label:12s} {p or '(not found)'}")

    # --- Load TSVs ---
    cobj_rows  = read_tsv(cobj_path)
    curves     = build_curve_lookup(read_tsv(curv_path))
    name_lookup = build_name_lookup(
        read_tsv(alch_path), read_tsv(misc_path), read_tsv(ammo_path)
    )

    # OMOD EDID → FULL lookup for effect-name fallback
    omod_name = {}
    for r in read_tsv(omod_path):
        edid = pick(r, "OMOD_EDID")
        full = pick(r, "FULL")
        if edid and full:
            omod_name[edid] = full

    # --- Process COBJ rows ---
    items = []
    seen = set()

    for row in cobj_rows:
        cobj_edid = pick(row, "COBJ_EDID")
        if not cobj_edid or _is_disabled(cobj_edid):
            continue

        # ---- Shard-crafting recipe ----
        if _SHARD_COBJ_RE.match(cobj_edid):
            cnam_edid = pick(row, "CNAM_EDID")
            cnam_full = pick(row, "CNAM_FULL")

            m = _SHARD_CNAM_RE.match(cnam_edid)
            if not m:
                continue

            slot_raw   = m.group(1)
            stars      = safe_int(m.group(2), 1)
            effect     = m.group(3)
            slot_type  = _normalise_slot(slot_raw)
            source     = _detect_source(cobj_edid)

            # Display name: strip star-prefix from CNAM_FULL, fall back to OMOD
            name = _STARS_PREFIX_RE.sub("", cnam_full).strip() if cnam_full else ""
            if not name:
                omod_edid = f"mod_Legendary_{slot_raw}{m.group(2)}_{effect}"
                name = omod_name.get(omod_edid, _humanize_edid(effect))

            category = _build_category(slot_type, stars, source)
            ingredients = _parse_fvpa(pick(row, "FVPA"), curves, name_lookup)

            if cobj_edid not in seen:
                seen.add(cobj_edid)
                items.append({
                    "edid": cobj_edid,
                    "name": name,
                    "stars": stars,
                    "category": category,
                    "slotType": slot_type,
                    "source": source,
                    "ingredients": ingredients,
                })

        # ---- Random-roll recipe ----
        elif _RANDOM_RE.match(cobj_edid):
            m = _RANDOM_RE.match(cobj_edid)
            slot_type = m.group(1)
            stars     = safe_int(m.group(2), 1)
            name      = f"Random ({stars}-star)"
            category  = _build_category(slot_type, stars, "Base", is_random=True)
            ingredients = _parse_fvpa(pick(row, "FVPA"), curves, name_lookup)

            if cobj_edid not in seen:
                seen.add(cobj_edid)
                items.append({
                    "edid": cobj_edid,
                    "name": name,
                    "stars": stars,
                    "category": category,
                    "slotType": slot_type,
                    "source": "Base",
                    "ingredients": ingredients,
                })

    # Stable sort: category then name
    items.sort(key=lambda x: (x["category"], x["name"]))

    # Unique category list
    categories = sorted({item["category"] for item in items})

    # --- Write JSON ---
    source_file = os.path.basename(cobj_path)
    output = {
        "_meta": {
            "generated": date.today().isoformat(),
            "source": source_file,
            "description": (
                "Legendary mod crafting recipes for the "
                "Legendary Crafting Cost Planner"
            ),
            "totalItems": len(items),
        },
        "categories": categories,
        "items": items,
    }

    out_path = OUT_DIR / "legendary_crafting_planner.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(output, fh, separators=(",", ":"), ensure_ascii=False)

    print(f"\nWrote {len(items)} items ({len(categories)} categories) "
          f"to {out_path}")


if __name__ == "__main__":
    main()
