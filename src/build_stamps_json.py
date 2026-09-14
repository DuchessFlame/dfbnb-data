#!/usr/bin/env python3
"""
build_stamps_json.py
====================
Builds dist/stamps.json for the Stamps module (Stamp Grind Calculator).

Sibling of build_currency_json.py. Reads directly from game export TSVs — no
dependency on any other builder's output, so the live and PTS channels can run
this independently off the same BOOK/ALCH/LVLI exports.

WHERE THE STOCK COMES FROM
--------------------------
Stamps are spent at Giuseppe Delcavo, the expedition vendor at Whitespring
Refuge. His stock is a leveled list:

    LVLI  XPD_LLV_ExpeditionVendor_Giuseppe
            -> BOOK  Recipe_*            (a plan; DATA_Value = stamp price)
            -> ALCH  SCORE_BobbleheadBox (a consumable; Value = stamp price)

The list is walked recursively, so if Bethesda ever nests a sub-list under it
the new stock is picked up without a code change.

Note the price is NOT the Econ_GoldVendor_Tier GLOB the gold vendors use — a
stamp item stores its price directly in its own value field. Several BOOKs
carry BOTH a Giuseppe reference and a gold-vendor tier GLOB (the Cremator plans
moved to Mortimer): those are gold-vendor plans now and are not in Giuseppe's
list, so walking the list rather than scanning BOOK EDIDs keeps them out.

OTHER STAMP VENDORS
-------------------
The three Atlantic City faction vendors are the known extension point:

    XPD_AC_LLV_ExpeditionVendor_Mobster
    XPD_AC_LLV_ExpeditionVendor_Muni
    XPD_AC_LLV_ExpeditionVendor_Showmen

Add them to STAMP_VENDORS below and everything downstream — the JSON, the
vendor filter and the calculator — picks them up; each item already carries its
vendor.

EXPEDITION PAYOUTS
------------------
meta.expeditions is built the same way — from the exports, not by hand. See the
comment above EXPEDITION_QUEST_TYPE for how a quest's stamp reward is resolved.
The calculator uses it to fill in "stamps per expedition" and the first-run
daily bonus when the reader picks a run; with no expedition data it falls back
to plain editable boxes, so these three inputs are optional.

Inputs:
  - tsv/LVLI_Export_*_LVLI_Entries.tsv   (vendor stock — REQUIRED)
  - tsv/BOOK_Export_*.tsv                (plan names + stamp prices)
  - tsv/ALCH_Export_*.tsv                (consumable names + stamp prices)
  - tsv/QUEST_Export_*.tsv               (expedition quests — optional)
  - tsv/GMRW_Export_*.tsv                (their stamp reward records — optional)
  - tsv/GLOB_Export_*.tsv                (the payout numbers — optional)

Output:
  - dist/stamps.json
  - dist/patchlog_latest_df_stamps.json

Usage:
  python build_stamps_json.py
  python build_stamps_json.py --book-tsv tsv/BOOK_Export_July_2026.tsv \
                              --lvli-entries tsv/LVLI_Export_July_2026_LVLI_Entries.tsv \
                              --alch-tsv tsv/ALCH_Export_July_2026.tsv \
                              --outdir dist
"""

import argparse
import csv
import glob
import json
import os
import re
import sys

from patchlog_utils import write_patchlog_feed

# ---------------------------------------------------------------------------
# Vendors whose leveled lists are priced in stamps.
#   key = LVLI EDID, value = display metadata
# ---------------------------------------------------------------------------
STAMP_VENDORS = {
    "XPD_LLV_ExpeditionVendor_Giuseppe": {
        "name":     "Giuseppe Delcavo",
        "location": "Whitespring Refuge",
    },
}

# ---------------------------------------------------------------------------
# Grind-model defaults for the calculator.
#
# These are NOT in the exports — no record carries a stamps-per-day cap the way
# Econ_GoldVendor does for bullion — so they ship as editable defaults. Every
# one of them is a number the reader can overwrite on the page; changing them
# here only changes what the boxes start at.
# ---------------------------------------------------------------------------
INCOME_DEFAULTS = {
    "stamps_per_expedition": 10,   # stamps for one expedition completion
    "expeditions_per_day":    3,   # runs the reader expects to do per day
    "daily_bonus_stamps":    25,   # first-expedition-of-the-day bonus
    "weekly_bonus_stamps":   50,   # weeklies / raid clears, applied per 7 days
}

# ---------------------------------------------------------------------------
# Expedition payouts — the one part of the grind model that IS in the exports.
#
# Every expedition is a QUST with "Quest Type = Expedition" pointing at a GMRW
# quest-reward record. Inside that GMRW, the stamp payouts are the rows whose
# QRCO_CurrencyObject is XPD_Stamps_Currency: their NAM8_CapsGlobal is a GLOB
# holding the number of stamps, and the row's tier condition says which payout
# applies —
#
#   GetExpeditionsInstanceNumOptbjectivesCompleted N  -> the payout for
#       completing N optional objectives (N = 0..3; Bethesda's typo, not ours)
#   GetValue 0                                        -> the once-a-day
#       first-run bonus, shared by every expedition in that region
#
# So nothing here is typed in by hand: change a payout in the game and the next
# export rebuilds the dropdown. An expedition whose rewards don't resolve (the
# templates, and Poke the Beehive, which carries no stamp reward record) is
# dropped and listed in meta.expeditions_unresolved rather than shipped at zero.
# ---------------------------------------------------------------------------
EXPEDITION_QUEST_TYPE   = "Expedition"
STAMP_CURRENCY_EDID     = "XPD_Stamps_Currency"
TIER_CONDITION_FUNC     = "GetExpeditionsInstanceNumOptbjectivesCompleted"
DAILY_BONUS_MARKER      = "DailyBonus"

# Quests that exist only as scaffolding for the real ones.
EXPEDITION_EDID_SKIP = ("TEMPLATE", "_Template_", "ModuleTest")

# ---------------------------------------------------------------------------
# Category heuristics — drive the filter chips on the calculator.
#
# Matched against the EDID first and the display name second, first rule wins,
# so ORDER IS THE WHOLE DESIGN. Every rule below is narrower than the one under
# it: "Union 42 Banners" is a workshop plan that happens to say Union, and the
# Union hunting rifle skin is a weapon skin that happens to say PowerArmor's
# faction — both land right only because C.A.M.P. and Power Armor are tested on
# the structural part of the EDID (Workshop_, mod_PowerArmor_) rather than on
# the faction word. Move a rule and you will silently mis-file a page of plans.
# ---------------------------------------------------------------------------
CATEGORY_RULES = [
    ("C.A.M.P.",    (r"_Workshop_", r"workshop_", r"_Signs?_")),
    ("Power Armor", (r"PowerArmor",)),
    ("Armour",      (r"mod_armor", r"_Armor_")),
    ("Weapon Mod",  (r"Recipe_mod_", r"_mod_", r"\bMods?\b", r"\bPaint\b", r"\bSkin\b")),
    ("Apparel",     (r"_Clothes_", r"_Headwear_", r"_Outfit", r"\bOutfit\b", r"\bHeadwear\b")),
    ("Weapon",      (r"co_Weapon_", r"_Weapon_", r"_StampVendor\b")),
]


# ---------------------------------------------------------------------------
# TSV helpers
# ---------------------------------------------------------------------------
def find_latest(pattern, exclude=None):
    """Newest-by-name match, with an optional substring blacklist.

    Companion exports (BOOK_Export_*_Locations.tsv) sort after the file we
    actually want, which is exactly how the Minerva build once picked up the
    wrong input — hence the explicit exclude.
    """
    files = sorted(glob.glob(pattern), reverse=True)
    if exclude:
        files = [f for f in files if exclude.lower() not in os.path.basename(f).lower()]
    return files[0] if files else None


MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def find_newest_export(pattern, exclude=None):
    """Newest export by the month and year in its filename.

    find_latest() sorts by name, which puts May_2026 ahead of July_2026 — fine
    for the inputs CI passes explicitly, wrong for the ones it leaves to
    auto-detection. This is the same month-aware ordering the workflow's own
    newest() helper does, so the two agree.
    """
    files = sorted(glob.glob(pattern))
    if exclude:
        files = [f for f in files if exclude.lower() not in os.path.basename(f).lower()]
    if not files:
        return None

    def key(path):
        parts  = re.split(r"[_./\- ]+", os.path.basename(path).lower())
        month  = max((MONTHS[p] for p in parts if p in MONTHS), default=0)
        years  = [int(p) for p in parts if re.fullmatch(r"\d{4}", p)]
        return (max(years) if years else 0, month, os.path.basename(path))

    return max(files, key=key)


def read_rows(path, encoding="latin-1"):
    with open(path, encoding=encoding, errors="replace") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            yield row


def split_ref(ref):
    """'00647BDE:Recipe_XPD_Clothes_SkippysOutfit:BOOK' -> (formid, edid, type)."""
    parts = (ref or "").split(":")
    if len(parts) < 3:
        return None, None, None
    return parts[0].strip().upper(), parts[1].strip(), parts[-1].strip().upper()


def as_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def categorise(edid, name, record_type="BOOK"):
    if record_type != "BOOK":
        # Only BOOKs are plans. Anything else on a stamp vendor's shelf is a
        # thing you consume or carry, not a recipe you learn.
        return "Consumable"
    blob = (edid or "") + " " + (name or "")
    for label, patterns in CATEGORY_RULES:
        for pattern in patterns:
            if re.search(pattern, blob, re.IGNORECASE):
                return label
    return "Other"


# ---------------------------------------------------------------------------
# Vendor stock
# ---------------------------------------------------------------------------
def collect_vendor_stock(entries_path):
    """Walk each stamp vendor's leveled list and return its stock entries.

    Returns {vendor_edid: [ {formid, edid, type, quantity}, ... ]}.
    Sub-lists are expanded recursively; a cycle can only cost us a repeat visit,
    which ``seen`` prevents.
    """
    by_list = {}
    for row in read_rows(entries_path, encoding="utf-8"):
        by_list.setdefault(row.get("LVLI_EDID") or "", []).append(row)

    def walk(list_edid, seen):
        if list_edid in seen:
            return []
        seen.add(list_edid)

        out = []
        for row in by_list.get(list_edid, []):
            formid, edid, rtype = split_ref(row.get("LVLO_Reference"))
            if not formid:
                continue
            if rtype == "LVLI":
                out.extend(walk(edid, seen))
                continue
            out.append({
                "formid":   formid,
                "edid":     edid,
                "type":     rtype,
                "quantity": as_int(row.get("LVIV_Quantity")) or 1,
            })
        return out

    stock = {}
    for vendor_edid in STAMP_VENDORS:
        if vendor_edid not in by_list:
            print(f"[build_stamps_json] WARNING: vendor list {vendor_edid} not in LVLI export.",
                  file=sys.stderr)
        stock[vendor_edid] = walk(vendor_edid, set())
    return stock


# ---------------------------------------------------------------------------
# Record resolution — one lookup table per record type the vendor can stock.
# ---------------------------------------------------------------------------
def index_book(path):
    out = {}
    for row in read_rows(path):
        formid = (row.get("FormID") or "").strip().upper()
        if not formid:
            continue
        out[formid] = {
            "edid":   (row.get("EDID") or "").strip(),
            "name":   (row.get("FULL") or "").strip(),
            "stamps": as_int(row.get("DATA_Value")),
        }
    return out


def index_alch(path):
    out = {}
    for row in read_rows(path):
        formid = (row.get("ALCH_FormID") or "").strip().upper()
        if not formid:
            continue
        out[formid] = {
            "edid":   (row.get("ALCH_EDID") or "").strip(),
            "name":   (row.get("FULL") or "").strip(),
            "stamps": as_int(row.get("Value") or row.get("ENIT_Value")),
        }
    return out


def build_items(stock, indexes):
    items    = []
    unpriced = []
    missing  = []
    seen     = set()

    for vendor_edid, entries in stock.items():
        vendor = STAMP_VENDORS[vendor_edid]
        for entry in entries:
            lookup = indexes.get(entry["type"])
            if lookup is None:
                missing.append(f"{entry['edid']} ({entry['type']} — no export loaded)")
                continue

            record = lookup.get(entry["formid"])
            if not record:
                missing.append(f"{entry['edid']} ({entry['type']} — not in export)")
                continue

            key = (entry["formid"], vendor_edid)
            if key in seen:
                continue
            seen.add(key)

            name = record["name"] or entry["edid"]
            item = {
                "formid":      entry["formid"],
                "edid":        record["edid"] or entry["edid"],
                "name":        name,
                "stamps":      record["stamps"],
                "record_type": entry["type"],
                "is_plan":     name.startswith("Plan:"),
                "category":    categorise(record["edid"] or entry["edid"], name, entry["type"]),
                "vendor":      vendor["name"],
                "vendor_edid": vendor_edid,
                "location":    vendor["location"],
                "stock":       entry["quantity"],
            }
            if item["stamps"] <= 0:
                # A zero price is a data problem, not a free item. Keep it out of
                # the picker so nobody totals a cart that is quietly short.
                unpriced.append(f"{item['edid']} ({name})")
                continue
            items.append(item)

    items.sort(key=lambda i: i["name"])
    return items, unpriced, missing


# ---------------------------------------------------------------------------
# Expeditions
# ---------------------------------------------------------------------------
def collect_expeditions(quest_path, gmrw_path, glob_path):
    """Resolve each expedition's stamp payouts straight out of the exports.

    Returns (expeditions, unresolved). Each expedition looks like:

        {"formid", "edid", "name", "region",
         "tiers": [{"optionals": 0, "stamps": 1}, ...],
         "daily_bonus_stamps": 8}

    Any missing export is not fatal — the calculator falls back to plain
    editable boxes when meta.expeditions is empty.
    """
    unresolved = []

    # 1. The expedition quests, with the reward records they point at.
    quests = []
    for row in read_rows(quest_path):
        if (row.get("Quest Type") or "").strip() != EXPEDITION_QUEST_TYPE:
            continue
        edid = (row.get("EDID") or "").strip()
        if any(skip.lower() in edid.lower() for skip in EXPEDITION_EDID_SKIP):
            continue
        # GMRWRef cells are "formid:EDID" — two parts, not the three that
        # split_ref expects, so they are pulled apart here instead.
        refs = []
        for i in range(10):
            parts = (row.get(f"GMRWRef{i}") or "").split(":")
            if len(parts) >= 2 and parts[1].strip():
                refs.append(parts[1].strip())
        quests.append({
            "formid": (row.get("FormID") or "").strip().upper(),
            "edid":   edid,
            "name":   (row.get("FULL - Name") or "").strip() or edid,
            "refs":   refs,
        })

    if not quests:
        return [], ["no quests with Quest Type = Expedition in the QUEST export"]

    # 2. The stamp-paying rows of every reward record, keyed by reward EDID.
    wanted_refs = {ref for q in quests for ref in q["refs"]}
    rewards     = {}   # reward_edid -> {"tiers": {n: glob_formid}, "daily": glob_formid}
    glob_wanted = set()

    for row in read_rows(gmrw_path):
        edid = (row.get("EDID") or "").strip()
        if edid not in wanted_refs:
            continue
        if STAMP_CURRENCY_EDID not in (row.get("QRCO_CurrencyObject") or ""):
            continue

        glob_formid, glob_edid, _ = split_ref(row.get("NAM8_CapsGlobal"))
        if not glob_formid:
            continue
        glob_wanted.add(glob_formid)

        bucket = rewards.setdefault(edid, {"tiers": {}, "daily": None})
        func   = (row.get("TierConditionFunc") or "").strip()
        if func == TIER_CONDITION_FUNC:
            bucket["tiers"][as_int(row.get("TierConditionValue"))] = glob_formid
        elif DAILY_BONUS_MARKER.lower() in (glob_edid or "").lower():
            bucket["daily"] = glob_formid

    # 3. Resolve the globals. Streamed and filtered — the GLOB export is ~35MB
    #    and we want a couple of dozen rows out of it.
    glob_values = {}
    if glob_path and os.path.exists(glob_path):
        for row in read_rows(glob_path):
            formid = (row.get("FormID") or "").strip().upper()
            if formid in glob_wanted:
                glob_values[formid] = as_int(row.get("FLTV"))
    else:
        unresolved.append("GLOB export missing — no stamp payouts could be resolved")
        return [], unresolved

    # 4. Stitch them together.
    expeditions = []
    for quest in quests:
        tiers = {}
        daily = 0
        for ref in quest["refs"]:
            bucket = rewards.get(ref)
            if not bucket:
                continue
            for optionals, glob_formid in bucket["tiers"].items():
                if glob_formid in glob_values:
                    tiers[optionals] = glob_values[glob_formid]
            if bucket["daily"] and bucket["daily"] in glob_values:
                daily = glob_values[bucket["daily"]]

        if not tiers:
            unresolved.append(f"{quest['edid']} ({quest['name']}) — no stamp reward record")
            continue

        # "Atlantic City: Tax Evasion" -> region "Atlantic City", short name
        # "Tax Evasion". The full name still ships for anything that wants it.
        region, _, short = quest["name"].partition(":")
        expeditions.append({
            "formid":             quest["formid"],
            "edid":               quest["edid"],
            "name":               quest["name"],
            "short_name":         (short or region).strip(),
            "region":             region.strip() if short else "",
            "tiers":              [{"optionals": n, "stamps": tiers[n]} for n in sorted(tiers)],
            "daily_bonus_stamps": daily,
        })

    expeditions.sort(key=lambda e: (e["region"], e["name"]))
    return expeditions, unresolved


# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Build stamps.json for the Stamp Grind Calculator.")
    parser.add_argument("--lvli-entries", default=None,
                        help="Path to LVLI_Export_*_LVLI_Entries.tsv (auto-detected from tsv/ if omitted)")
    parser.add_argument("--book-tsv", default=None,
                        help="Path to BOOK_Export_*.tsv (auto-detected from tsv/ if omitted)")
    parser.add_argument("--alch-tsv", default=None,
                        help="Path to ALCH_Export_*.tsv (auto-detected from tsv/ if omitted)")
    parser.add_argument("--quest-tsv", default=None,
                        help="Path to QUEST_Export_*.tsv (expedition payouts; auto-detected)")
    parser.add_argument("--gmrw-tsv", default=None,
                        help="Path to GMRW_Export_*.tsv (expedition payouts; auto-detected)")
    parser.add_argument("--glob-tsv", default=None,
                        help="Path to GLOB_Export_*.tsv (expedition payouts; auto-detected)")
    parser.add_argument("--outdir", default="dist",
                        help="Output directory (default: dist)")
    args = parser.parse_args()

    lvli_entries = args.lvli_entries or find_latest("tsv/LVLI_Export_*_LVLI_Entries.tsv")
    if not lvli_entries or not os.path.exists(lvli_entries):
        print("[build_stamps_json] ERROR: No LVLI_Export_*_LVLI_Entries.tsv found. "
              "Pass --lvli-entries or run from the repo root.", file=sys.stderr)
        sys.exit(1)

    book_tsv = args.book_tsv or find_latest("tsv/BOOK_Export_*.tsv", exclude="_locations")
    if not book_tsv or not os.path.exists(book_tsv):
        print("[build_stamps_json] ERROR: No BOOK_Export_*.tsv found. "
              "Pass --book-tsv or run from the repo root.", file=sys.stderr)
        sys.exit(1)

    alch_tsv = args.alch_tsv or find_latest("tsv/ALCH_Export_*.tsv", exclude="_effects")

    # Expedition payouts. All three are optional — without them the calculator
    # still works, it just has no expedition presets to offer.
    quest_tsv = args.quest_tsv or find_newest_export("tsv/QUEST_Export_*.tsv")
    gmrw_tsv  = args.gmrw_tsv  or find_newest_export("tsv/GMRW_Export_*.tsv")
    glob_tsv  = args.glob_tsv  or find_newest_export("tsv/GLOB_Export_*.tsv")

    print(f"[build_stamps_json] LVLI Entries: {lvli_entries}", file=sys.stderr)
    print(f"[build_stamps_json] BOOK TSV:     {book_tsv}", file=sys.stderr)
    print(f"[build_stamps_json] ALCH TSV:     {alch_tsv or 'none'}", file=sys.stderr)
    print(f"[build_stamps_json] QUEST TSV:    {quest_tsv or 'none'}", file=sys.stderr)
    print(f"[build_stamps_json] GMRW TSV:     {gmrw_tsv or 'none'}", file=sys.stderr)
    print(f"[build_stamps_json] GLOB TSV:     {glob_tsv or 'none'}", file=sys.stderr)

    indexes = {"BOOK": index_book(book_tsv)}
    if alch_tsv and os.path.exists(alch_tsv):
        indexes["ALCH"] = index_alch(alch_tsv)

    stock = collect_vendor_stock(lvli_entries)
    for vendor_edid, entries in stock.items():
        print(f"[build_stamps_json] {vendor_edid}: {len(entries)} stock entries.", file=sys.stderr)

    items, unpriced, missing = build_items(stock, indexes)

    for line in unpriced:
        print(f"[build_stamps_json] SKIPPED (no price): {line}", file=sys.stderr)
    for line in missing:
        print(f"[build_stamps_json] SKIPPED (unresolved): {line}", file=sys.stderr)

    expeditions, exp_unresolved = [], []
    if quest_tsv and gmrw_tsv and os.path.exists(quest_tsv) and os.path.exists(gmrw_tsv):
        expeditions, exp_unresolved = collect_expeditions(quest_tsv, gmrw_tsv, glob_tsv)
        print(f"[build_stamps_json] Expeditions: {len(expeditions)} with stamp payouts.",
              file=sys.stderr)
    else:
        exp_unresolved = ["QUEST and/or GMRW export missing — no expedition presets built"]

    for line in exp_unresolved:
        print(f"[build_stamps_json] EXPEDITION SKIPPED: {line}", file=sys.stderr)

    plans      = [i for i in items if i["is_plan"]]
    categories = sorted({i["category"] for i in items})
    prices     = [i["stamps"] for i in items]

    output = {
        "meta": {
            "built_by":        "build_stamps_json.py",
            "book_source":     os.path.basename(book_tsv),
            "alch_source":     os.path.basename(alch_tsv) if alch_tsv else None,
            "lvli_source":     os.path.basename(lvli_entries),
            "total_items":     len(items),
            "total_plans":     len(plans),
            "total_other":     len(items) - len(plans),
            "cheapest":        min(prices) if prices else 0,
            "dearest":         max(prices) if prices else 0,
            "full_set_cost":   sum(prices),
            "categories":      categories,
            "income_defaults": INCOME_DEFAULTS,
            "expeditions":     expeditions,
            "expeditions_unresolved": exp_unresolved,
            "quest_source":    os.path.basename(quest_tsv) if quest_tsv else None,
            "gmrw_source":     os.path.basename(gmrw_tsv) if gmrw_tsv else None,
            "glob_source":     os.path.basename(glob_tsv) if glob_tsv else None,
            "unpriced":        unpriced,
            "unresolved":      missing,
        },
        "vendors": [
            {"edid": edid, "name": meta["name"], "location": meta["location"],
             "item_count": sum(1 for i in items if i["vendor_edid"] == edid)}
            for edid, meta in STAMP_VENDORS.items()
        ],
        "items": items,
    }

    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, "stamps.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[build_stamps_json] Wrote {out_path} — {len(items)} items "
          f"({len(plans)} plans, {len(items) - len(plans)} other), "
          f"{sum(prices)} stamps for the lot.", file=sys.stderr)

    write_patchlog_feed(
        dist_dir=args.outdir,
        feed_name="patchlog_latest_df_stamps.json",
        current_items=items,
        key_field="formid",
        name_field="name",
        compare_fields=["name", "stamps", "vendor"],
        prev_json_path="dist/stamps.json",
        items_extractor=lambda d: d.get("items", []),
    )


if __name__ == "__main__":
    main()
