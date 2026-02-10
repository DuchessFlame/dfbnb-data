#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
from typing import Dict, Any, List, Tuple


def norm_path(p: str) -> str:
    """Normalize to /path/ form. Accepts full URLs or relative paths."""
    p = (p or "").strip()
    if not p:
        return ""

    if "://" in p:
        rest = p.split("://", 1)[1]
        if "/" not in rest:
            return ""
        p = "/" + rest.split("/", 1)[1]

    if not p.startswith("/"):
        p = "/" + p
    if not p.endswith("/"):
        p += "/"
    return p


def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def pick(row: Dict[str, str], *keys: str) -> str:
    """Fetch a TSV field by name, case-insensitive."""
    for k in keys:
        if k in row and row[k] is not None:
            return str(row[k]).strip()

    lower_map = {str(k).lower(): k for k in row.keys()}
    for k in keys:
        kk = str(k).lower()
        if kk in lower_map:
            return str(row[lower_map[kk]]).strip()

    return ""


def bump_csv_field_limit() -> None:
    """
    Fix for: csv.Error: field larger than field limit (131072)
    Your TSV can contain huge cells (tags/notes/etc). Raise as high as possible.
    """
    target = getattr(sys, "maxsize", 2**31 - 1)
    while True:
        try:
            csv.field_size_limit(target)
            return
        except (OverflowError, ValueError):
            target //= 2
            if target < 1024 * 1024:
                csv.field_size_limit(1024 * 1024)
                return

def decode_bytes(raw: bytes) -> str:
    """
    guide_index.tsv is not always clean UTF-8 (Excel exports often contain cp1252 bytes like 0x96).
    IMPORTANT: Never "guess" utf-16 unless there is a BOM, because random bytes can decode as utf-16 garbage.
    """
    raw = raw or b""

    # Only treat as UTF-16 if a BOM is present
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")

    # Best effort order for your exports:
    # 1) UTF-8 (with optional BOM)
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        pass

    # 2) Windows-1252 (common for “smart punctuation” from Excel/Word)
    try:
        return raw.decode("cp1252")
    except UnicodeDecodeError:
        pass

    # 3) Last resort: keep structure (tabs/newlines) even if some chars are mangled
    return raw.decode("utf-8", errors="replace")

def feed_url(dist_base_url: str, filename: str) -> str:
    dist = (dist_base_url or "").rstrip("/")
    fn = (filename or "").lstrip("/")
    return f"{dist}/{fn}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--guide-index", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--dist-base-url", required=True)

    # Optional: filter only public+published (recommended)
    ap.add_argument("--public-only", action="store_true", default=False)

    # Titles feed (generic)
    ap.add_argument("--titles-feed", default="patchlog_latest_titles.json")

    args = ap.parse_args()

    bump_csv_field_limit()

    # ---------------------------------------------
    # PREFIX rules = "everything under this category"
    # Your provided URL list (prefix form)
    # Each maps to a feed file name in dist/
    # NOTE: You must create these feed JSONs (or change names here to match what you already output).
    # ---------------------------------------------
    PREFIX_RULES: List[Tuple[str, str, str]] = [
        ("/df/calculators/",               "patchlog_latest_df_calculators.json",             "df-calculators"),
        ("/df/camp/",                      "patchlog_latest_df_camp.json",                    "df-camp"),
        ("/df/collectables/",              "patchlog_latest_df_collectables.json",            "df-collectables"),
        ("/df/events/",                    "patchlog_latest_df_events.json",                  "df-events"),
        ("/df/challenges/",                "patchlog_latest_df_challenges.json",              "df-challenges"),
        ("/df/minerva/",                   "patchlog_latest_df_minerva.json",                 "df-minerva"),
        ("/df/seasons/",                   "patchlog_latest_df_seasons.json",                 "df-seasons"),
        ("/df/scouts/",                    "patchlog_latest_df_scouts.json",                  "df-scouts"),
        ("/df/plan-checklists/",           "patchlog_latest_df_plan_checklists.json",         "df-plan-checklists"),
        ("/df/raids/",                     "patchlog_latest_df_raids.json",                   "df-raids"),
        ("/df/seasonal-events/",           "patchlog_latest_df_seasonal_events.json",         "df-seasonal-events"),
        ("/df/titles/",                    "patchlog_latest_titles.json",                     "df-titles"),
        ("/df/vendors/",                   "patchlog_latest_df_vendors.json",                 "df-vendors"),

        ("/bnb/armour/",                   "patchlog_latest_bnb_armour.json",                 "bnb-armour"),
        ("/bnb/buffs/",                    "patchlog_latest_bnb_buffs.json",                  "bnb-buffs"),
        ("/bnb/calculators-curve-tables/", "patchlog_latest_bnb_calculators_curve_tables.json","bnb-calculators-curve-tables"),
        ("/bnb/camp-items/",               "patchlog_latest_bnb_camp_items.json",             "bnb-camp-items"),
        ("/bnb/legendary-mods/",           "patchlog_latest_bnb_legendary_mods.json",         "bnb-legendary-mods"),
        ("/bnb/perk-cards/",               "patchlog_latest_bnb_perk_cards.json",             "bnb-perk-cards"),
        ("/bnb/plan-checklists/",          "patchlog_latest_bnb_plan_checklists.json",        "bnb-plan-checklists"),

        ("/bnb/staff/",                    "patchlog_latest_bnb_staff.json",                  "bnb-staff"),
        ("/df/staff/",                     "patchlog_latest_df_staff.json",                   "df-staff"),
    ]

    # ---------------------------------------------
    # EXACT rules = one-off pages you listed
    # ---------------------------------------------
    EXACT_RULES: List[Tuple[str, str, str]] = [
        ("/df/expos/atlantic-city/atlantic-city-expos-reward-checklist/",
         "patchlog_latest_df_expos_atlantic_city_rewards.json", "df-expos-atlantic-city-rewards"),
        ("/df/expos/the-pitt/pitt-expos-reward-checklist/",
         "patchlog_latest_df_expos_pitt_rewards.json", "df-expos-pitt-rewards"),
        ("/df/daily-ops/daily-ops-reward-checklist/",
         "patchlog_latest_df_daily_ops_rewards.json", "df-daily-ops-rewards"),
    ]

    prefix_rules = [(norm_path(p), feed, label) for (p, feed, label) in PREFIX_RULES]
    exact_rules = [(norm_path(p), feed, label) for (p, feed, label) in EXACT_RULES]

    titles_feed = feed_url(args.dist_base_url, args.titles_feed)

    raw = open(args.guide_index, "rb").read()
    text = decode_bytes(raw)

    import io
    f = io.StringIO(text)
    reader = csv.DictReader(f, delimiter="\t")

    by_page: Dict[str, Dict[str, str]] = {}

    scanned = 0
    matched = 0

    for row in reader:
        scanned += 1

        url = pick(row, "url", "URL")
        path = norm_path(url)
        if not path:
            continue

        if args.public_only:
            vis = pick(row, "visibility").lower()
            st = pick(row, "status").lower()
            ok_vis = (vis == "public" or not vis)
            ok_st = (st == "published" or not st)
            if not (ok_vis and ok_st):
                continue

        node_type = pick(row, "nodeType", "node_type").lower()
        slug = pick(row, "slug").lower()

        # We only map actual pages
        if node_type != "page":
            continue

        # 1) Exact URL rules (highest priority)
        exact_hit = False
        for exact_path, feed_file, label in exact_rules:
            if path == exact_path:
                by_page[path] = {"url": feed_url(args.dist_base_url, feed_file), "label": label}
                matched += 1
                exact_hit = True
                break
        if exact_hit:
            continue

        # 2) Titles checklist pages (player/camp titles checklists)
        is_titles_checklist = (
            slug == "checklist" and ("/player-titles/" in path or "/camp-titles/" in path)
        )
        if is_titles_checklist:
            by_page[path] = {"url": titles_feed, "label": "titles"}
            matched += 1
            continue

        # 3) Category prefix rules
        for prefix, feed_file, label in prefix_rules:
            if path.startswith(prefix):
                by_page[path] = {"url": feed_url(args.dist_base_url, feed_file), "label": label}
                matched += 1
                break

    print(f"[patchlog_manifest] scanned_rows={scanned} mapped_pages={len(by_page)} matched_hits={matched}")

    if not by_page:
        # Print a small hint: show a couple of example page URLs so you can see what the TSV contains
        # without dumping the whole TSV in Actions logs.
        print("[patchlog_manifest] ERROR: byPage is empty. This usually means your rules don't match the URL format in guide_index.tsv.")
        raise RuntimeError("patchlog_manifest is empty. No matching pages found. Check guide_index.tsv URLs and rules.")

    write_json(args.out, {"byPage": by_page})


if __name__ == "__main__":
    main()
