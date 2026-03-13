#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_challenges_json.py  (v3 — unified challenges + mini-seasons)

Reads game-data TSVs and outputs an enriched JSON consumed by
df-bnb-challenges.js for BOTH /df/challenges/* and /df/seasons/* pages.

Outputs:
    dist/challenges/challenges.json

Cross-references:
    CHAL  — challenge records (conditions, META/SUB hierarchy)
    GMRW  — game reward records (XP, caps, items)
    ENTM  — entitlements (Atom Shop items, images)
    BOOK  — notes / holotapes
    COBJ  — craftable objects
    KYWD  — keywords (weapon types, item categories)
    MISC  — misc items (drinking glasses, etc.)
    FLST  — form lists (grouped item references)
    guide_index.tsv — site guide pages for hyperlinking
"""

import csv
import glob
import json
import os
import re
from collections import defaultdict
from pathlib import Path

DIST_DIR = Path("dist/challenges")
DIST_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================================
# Helpers
# ==================================================================

def newest(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[-1]


def read_tsv(path):
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))
    except UnicodeDecodeError:
        with open(path, encoding="cp1252", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))


def pick(row, *keys, default=""):
    for k in keys:
        if k in row:
            v = row.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
    return default


def safe_int(val, default=None):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", str(s).lower()).strip("-")


# ==================================================================
# Load all TSVs
# ==================================================================

print("[challenges] Loading TSVs...")

CHAL = read_tsv(newest("tsv/CHAL_Export_*.tsv"))
GMRW = read_tsv(newest("tsv/GMRW_Export_*.tsv"))
ENTM = read_tsv(newest("tsv/ENTM_Export_*.tsv"))
BOOK = read_tsv(newest("tsv/BOOK_Export_*.tsv"))
KYWD = read_tsv(newest("tsv/KYWD_Export_*.tsv"))
FLST_ENTRIES = read_tsv(newest("tsv/FLST_Export_*_Entries.tsv"))

_cobj_path = newest("tsv/COBJ_Export_*.tsv")
COBJ = read_tsv(_cobj_path) if _cobj_path else []

_misc_path = newest("tsv/MISC_Export_*.tsv")
MISC = read_tsv(_misc_path) if _misc_path else []

_guide_path = "tsv/guide_index.tsv"
GUIDE_INDEX = read_tsv(_guide_path) if os.path.exists(_guide_path) else []

print(f"  CHAL: {len(CHAL)} rows")
print(f"  GMRW: {len(GMRW)} rows")
print(f"  ENTM: {len(ENTM)} rows")
print(f"  BOOK: {len(BOOK)} rows")
print(f"  KYWD: {len(KYWD)} rows")
print(f"  COBJ: {len(COBJ)} rows")
print(f"  MISC: {len(MISC)} rows")
print(f"  FLST: {len(FLST_ENTRIES)} entries")
print(f"  Guide Index: {len(GUIDE_INDEX)} rows")

# ==================================================================
# Build lookup dictionaries
# ==================================================================

entm_by_fid = {}
entm_by_edid = {}
for row in ENTM:
    fid = pick(row, "FormID")
    edid = pick(row, "EDID")
    if fid: entm_by_fid[fid] = row
    if edid: entm_by_edid[edid.upper()] = row

book_by_fid = {}
for row in BOOK:
    fid = pick(row, "FormID")
    if fid: book_by_fid[fid] = row

kywd_by_fid = {}
for row in KYWD:
    fid = pick(row, "FormID")
    if fid: kywd_by_fid[fid] = row

misc_by_fid = {}
for row in MISC:
    fid = pick(row, "FormID")
    if fid: misc_by_fid[fid] = row

cobj_by_fid = {}
for row in COBJ:
    fid = pick(row, "COBJ_FormID", "FormID")
    if fid: cobj_by_fid[fid] = row

flst_by_parent = defaultdict(list)
for row in FLST_ENTRIES:
    parent_fid = pick(row, "FLST_FormID")
    if parent_fid: flst_by_parent[parent_fid].append(row)

gmrw_by_fid = defaultdict(list)
for row in GMRW:
    fid = pick(row, "FormID")
    if fid: gmrw_by_fid[fid].append(row)

# Guide index
guide_links = []
guide_by_slug = {}
guide_by_tag = defaultdict(list)
for row in GUIDE_INDEX:
    brand = pick(row, "brand")
    url = pick(row, "url")
    title = pick(row, "title")
    slug = pick(row, "slug")
    tags = pick(row, "tags,nodeType", "tags")
    status = pick(row, "status")
    visibility = pick(row, "visibility")
    node_type = pick(row, "nodeType")
    if status != "published" or visibility != "public":
        continue
    if node_type not in ("page",):
        continue
    entry = {"title": title, "url": url, "slug": slug, "brand": brand,
             "tags": [t.strip().lower() for t in tags.split(",") if t.strip()]}
    guide_links.append(entry)
    if slug: guide_by_slug[slug.lower()] = entry
    for tag in entry["tags"]:
        guide_by_tag[tag].append(entry)


# ==================================================================
# Name resolution
# ==================================================================

KNOWN_FID_NAMES = {
    "0000000F": "Caps",
    "005652F9": "Legendary Module",
    "005A5443": "Treasury Note",
    "007FDC33": "Improved Bait",
    "003F7410": "Legendary Scrip",
    "005AD146": "Perk Coins",
    "00421760": "Possum Badge",
    "0042175F": "Tadpole Badge",
}

def resolve_name_from_fid(fid):
    if not fid: return ""
    fid = fid.strip()
    if fid in KNOWN_FID_NAMES: return KNOWN_FID_NAMES[fid]
    if fid in entm_by_fid: return pick(entm_by_fid[fid], "FULL", "NNAM") or fid
    if fid in book_by_fid: return pick(book_by_fid[fid], "FULL") or fid
    if fid in misc_by_fid: return pick(misc_by_fid[fid], "FULL", "EDID") or fid
    if fid in kywd_by_fid: return pick(kywd_by_fid[fid], "FULL_Name", "EDID") or fid
    if fid in cobj_by_fid: return pick(cobj_by_fid[fid], "CNAM_FULL", "COBJ_EDID") or fid
    return fid


def parse_ref_name(ref_str):
    if not ref_str: return ""
    parts = ref_str.strip().split(":")
    fid = parts[0]
    edid = parts[1] if len(parts) > 1 else ""
    name = resolve_name_from_fid(fid)
    if name and name != fid: return name
    label = re.sub(r"^(LLS?|RA_LL|LL|QuestReward)_+", "", edid, flags=re.IGNORECASE)
    label = label.replace("__", "_").replace("_", " ").strip()
    return re.sub(r"\s+", " ", label).strip() or fid


# ==================================================================
# GMRW reward resolution
# ==================================================================

def reward_label_from_gmrw_row(row):
    parts = []
    if pick(row, "XPCT_XPCurveTable"): parts.append("XP")
    nam8 = pick(row, "NAM8_CapsGlobal")
    if nam8:
        fid = nam8.split(":")[0]
        parts.append(KNOWN_FID_NAMES.get(fid, "Caps"))
    qrco = pick(row, "QRCO_CurrencyObject")
    if qrco:
        name = parse_ref_name(qrco)
        if name and name not in parts: parts.append(name)
    rewarded = pick(row, "RewardedItem")
    if rewarded:
        name = parse_ref_name(rewarded)
        if name and name not in parts: parts.append(name)
    qrli = pick(row, "QRLI_LegendaryItemRewardList")
    if qrli:
        name = parse_ref_name(qrli)
        if name and name not in parts: parts.append(f"Legendary: {name}")
    return parts


def get_rewards_for_gmrw(gmrw_fid):
    rows = gmrw_by_fid.get(gmrw_fid, [])
    seen, seen_set = [], set()
    for row in rows:
        for label in reward_label_from_gmrw_row(row):
            if label and label not in seen_set:
                seen_set.add(label); seen.append(label)
    return seen


def parse_dnam_rewards(dnam_str):
    if not dnam_str or dnam_str.strip() == "": return []
    all_rewards, seen_set = [], set()
    for entry in re.split(r"[|\n]", dnam_str):
        entry = entry.strip()
        if not entry: continue
        m = re.match(r"Reward\d+:([0-9A-Fa-f]+):.*:GMRW", entry)
        if m:
            for label in get_rewards_for_gmrw(m.group(1)):
                if label not in seen_set:
                    seen_set.add(label); all_rewards.append(label)
    return all_rewards


# ==================================================================
# ENTM image resolution
# ==================================================================

def resolve_entm_image(fid):
    row = entm_by_fid.get(fid) or entm_by_edid.get(str(fid).upper())
    if not row: return ""
    etdi = pick(row, "ETDI")
    edid = pick(row, "EDID")
    if etdi:
        fn = etdi.rsplit("/", 1)[-1] if "/" in etdi else etdi
        fn = fn.rsplit("\\", 1)[-1] if "\\" in fn else fn
        fn = re.sub(r"\.dds$", ".webp", fn, flags=re.IGNORECASE)
        return f"/wp-content/uploads/challenge-images/{fn}"
    if edid:
        return f"/wp-content/uploads/challenge-images/{edid.lower()}.webp"
    return ""


# ==================================================================
# Condition parsing
# ==================================================================

def parse_condition_items(conditions):
    items, keywords, seen_fids = [], [], set()
    for cond in (conditions or []):
        if not cond: continue
        # HasKeyword with quoted name
        for m in re.finditer(r'HasKeyword\(([^"]*?)\s*"([^"]*?)"\s*\[KYWD:([0-9A-Fa-f]+)\]', cond):
            kw_edid, kw_name, kw_fid = m.group(1), m.group(2), m.group(3)
            if kw_fid not in seen_fids:
                seen_fids.add(kw_fid)
                keywords.append({"name": kw_name.strip() or kw_edid.strip(), "formId": kw_fid, "edid": kw_edid.strip()})
        # HasKeyword without quoted name
        for m in re.finditer(r'HasKeyword\((\w+)\s*\[KYWD:([0-9A-Fa-f]+)\]\)', cond):
            kw_edid, kw_fid = m.group(1), m.group(2)
            if kw_fid not in seen_fids:
                seen_fids.add(kw_fid)
                kw_row = kywd_by_fid.get(kw_fid)
                display = pick(kw_row, "FULL_Name", "EDID") if kw_row else kw_edid
                keywords.append({"name": display or kw_edid, "formId": kw_fid, "edid": kw_edid})
        # GetIsID with any sig
        for m in re.finditer(r'GetIsID\(([^[]*?)\[(\w+):([0-9A-Fa-f]+)\]\)', cond):
            item_hint, sig, item_fid = m.group(1).strip(), m.group(2), m.group(3)
            if item_fid not in seen_fids:
                seen_fids.add(item_fid)
                name = resolve_name_from_fid(item_fid)
                if not name or name == item_fid:
                    name = item_hint.strip().strip('"').strip()
                items.append({"name": name or item_fid, "formId": item_fid, "sig": sig})
        # HasEntitlement
        for m in re.finditer(r'HasEntitlement\(([^"]*?)\s*"([^"]*?)"\s*\[ENTM:([0-9A-Fa-f]+)\]', cond):
            entm_edid, entm_name, entm_fid = m.group(1), m.group(2), m.group(3)
            if entm_fid not in seen_fids:
                seen_fids.add(entm_fid)
                items.append({"name": entm_name.strip() or entm_edid.strip(), "formId": entm_fid, "sig": "ENTM"})
    return {"items": items, "keywords": keywords}


def humanize_condition(cond_str):
    s = str(cond_str or "").strip()
    if not s: return ""
    s = re.sub(r"^(Top|More|And|Or):", "", s).strip()
    if "IsFalloutWorlds" in s and "<>" in s: return "Not in Fallout Worlds"
    if "IsFalloutWorlds" in s and "= 1" in s: return "In Fallout Worlds"
    m = re.search(r'WornInOrOutOfPowerArmorHasKeyword\([^"]*"([^"]+)"', s)
    if m: return f"Wearing: {m.group(1)}"
    m = re.search(r'HasKeyword\([^"]*"([^"]+)"', s)
    if m: return f"Target has keyword: {m.group(1)}"
    m = re.search(r'GetIsID\(\s*([^[]*?)\s*\[', s)
    if m:
        name = m.group(1).strip().strip('"').strip()
        if name: return f"Item: {name}"
    m = re.search(r'HasEntitlement\([^"]*"([^"]+)"', s)
    if m: return f"Requires: {m.group(1)}"
    m = re.search(r'GetPlayerSeasonRank\(\)\s*[>=<]+\s*(\w+)', s)
    if m: return f"Season Rank: {m.group(1)}"
    s = re.sub(r"Subject\.", "", s)
    s = re.sub(r"\s*=\s*1\.0+", "", s)
    return s


# ==================================================================
# Guide matching
# ==================================================================

# Words to exclude from guide search
STOP_WORDS = frozenset({
    "challenge", "daily", "weekly", "lifetime", "score", "event",
    "collect", "kill", "craft", "complete", "with", "from", "that",
    "this", "have", "been", "the", "and", "for", "any", "all",
})

def find_related_guides(item):
    matches = []
    seen_urls = set()
    full = str(item.get("full") or "").lower()
    edid = str(item.get("edid") or "").lower()
    conditions = item.get("conditions") or []
    search_terms = set()
    for word in re.findall(r"[a-z]{4,}", full):
        if word not in STOP_WORDS: search_terms.add(word)
    parsed = parse_condition_items(conditions)
    for kw in parsed.get("keywords", []):
        for word in re.findall(r"[a-z]{4,}", str(kw.get("name", "")).lower()):
            search_terms.add(word)
    for it in parsed.get("items", []):
        for word in re.findall(r"[a-z]{4,}", str(it.get("name", "")).lower()):
            search_terms.add(word)

    for term in search_terms:
        if term in guide_by_slug:
            g = guide_by_slug[term]
            if g["url"] not in seen_urls:
                seen_urls.add(g["url"])
                matches.append({"title": g["title"], "url": g["url"], "relevance": 5})
    for term in search_terms:
        for guide in guide_by_tag.get(term, []):
            if guide["url"] not in seen_urls:
                overlap = sum(1 for t in search_terms if t in guide["tags"])
                if overlap >= 2:
                    seen_urls.add(guide["url"])
                    matches.append({"title": guide["title"], "url": guide["url"], "relevance": overlap})

    matches.sort(key=lambda x: -x["relevance"])
    return [{"title": m["title"], "url": m["url"]} for m in matches[:5]]


# ==================================================================
# Classification helpers
# ==================================================================

CUT_PREFIXES = ("CUT", "DEL", "ZZZ", "ZZZZ", "POST")

def is_cut(edid):
    u = str(edid or "").upper()
    return any(u.startswith(p) for p in CUT_PREFIXES)

def is_meta(edid): return str(edid or "").upper().endswith("_META")

def is_sub(edid, enam):
    return "_SUB_" in str(edid or "").upper() or str(enam or "") == "Sub Challenge (Unsorted)"

def edid_base(edid):
    base = re.sub(r"_META$", "", str(edid or ""), flags=re.IGNORECASE)
    base = re.sub(r"_SUB_.+$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"_(Low|Mid|High|Lowest|Middle|Highest)$", "", base, flags=re.IGNORECASE)
    return base

def scope_bucket(cnam):
    c = str(cnam or "").lower().strip()
    if c == "daily": return "daily"
    if c == "weekly": return "weekly"
    if c == "lifetime": return "lifetime"
    if c in ("event", "events"): return "event"
    if c == "monthly": return "weekly"
    return "lifetime"

def is_epic(full_name):
    return str(full_name or "").strip().lower().startswith("epic")


# ==================================================================
# Challenge image from conditions
# ==================================================================

def find_challenge_image(conditions, edid):
    for cond in (conditions or []):
        for m in re.finditer(r'\[ENTM:([0-9A-Fa-f]+)\]', str(cond)):
            img = resolve_entm_image(m.group(1))
            if img: return img
    return ""


# ==================================================================
# Extract conditions from CHAL row
# ==================================================================

def extract_conditions(row):
    conds = []
    count = safe_int(pick(row, "CondCount"), 52)
    for i in range(1, min(count + 1, 53)):
        c = pick(row, f"Cond{i}")
        if c: conds.append(c)
    return conds


# ==================================================================
# Build item from CHAL row
# ==================================================================

def row_to_item(row, rewards_override=None):
    form_id = pick(row, "FormID")
    edid = pick(row, "EDID")
    full = pick(row, "FULL")
    snam = pick(row, "SNAM").lstrip()
    tnam = pick(row, "TNAM")
    cnam = pick(row, "CNAM")
    enam = pick(row, "ENAM")
    dnam = pick(row, "DNAM")
    jasf = pick(row, "JASF")

    required = safe_int(tnam) if tnam and tnam != "0" else None
    conditions = extract_conditions(row)
    rewards = rewards_override if rewards_override is not None else parse_dnam_rewards(dnam)
    reward_unknown = len(rewards) == 0
    parsed_conds = parse_condition_items(conditions)
    human_conditions = [hc for c in conditions if (hc := humanize_condition(c))]
    item_draft = {"form_id": form_id, "edid": edid, "full": full, "conditions": conditions}
    guides = find_related_guides(item_draft)
    image_url = find_challenge_image(conditions, edid)

    return {
        "form_id": form_id, "edid": edid, "full": full,
        "snam": snam if snam and snam != "NONE" else "",
        "required": required, "scope": cnam, "classification": enam,
        "conditions": conditions, "conditions_human": human_conditions,
        "condition_items": parsed_conds.get("items", []),
        "condition_keywords": parsed_conds.get("keywords", []),
        "rewards": rewards, "reward_unknown": reward_unknown,
        "is_cut": is_cut(edid), "is_meta": is_meta(edid),
        "is_sub": is_sub(edid, enam), "is_epic": is_epic(full),
        "children": [], "guides": guides, "image_url": image_url, "jasf": jasf,
    }


# ==================================================================
# Build all items + META/SUB hierarchy
# ==================================================================

print("[challenges] Building items...")
all_items = []
items_by_edid = {}
for row in CHAL:
    item = row_to_item(row)
    all_items.append(item)
    if item["edid"]: items_by_edid[item["edid"]] = item

meta_map = {}
for item in all_items:
    if item["is_meta"]:
        meta_map[edid_base(item["edid"])] = item

for item in all_items:
    if item["is_sub"]:
        base = edid_base(item["edid"])
        parent = meta_map.get(base)
        if parent: parent["children"].append(item)

print(f"  Total items: {len(all_items)}")
print(f"  META parents: {len(meta_map)}")

# ==================================================================
# Bucket items
# ==================================================================

buckets = defaultdict(list)
for item in all_items:
    if item["is_sub"] and edid_base(item["edid"]) in meta_map:
        continue
    if item["is_cut"]:
        buckets["cut"].append(item)
    else:
        buckets[scope_bucket(item["scope"])].append(item)

# ==================================================================
# Season detection
# ==================================================================

SEASON_PATTERNS = [
    (r"MiniSeason_?(\d{4})_(\w+)", "mini-season"),
    (r"ATX_DE(\d{4})_(\w+?)_Challenge", "event-season"),
]

def detect_season_slug(edid):
    e = str(edid or "")
    for pattern, _ in SEASON_PATTERNS:
        m = re.search(pattern, e, re.IGNORECASE)
        if m:
            raw_name = m.group(2)
            return slugify(re.sub(r"([a-z])([A-Z])", r"\1-\2", raw_name))
    return None

season_buckets = defaultdict(list)
for item in all_items:
    if item["is_cut"]: continue
    slug = detect_season_slug(item["edid"])
    if slug: season_buckets[slug].append(item)

print(f"  Scope buckets: {list(buckets.keys())}")
print(f"  Season slugs: {list(season_buckets.keys())}")

# ==================================================================
# Lifetime sub-categories
# ==================================================================

CLASSIFICATION_TO_SLUG = {
    "Combat": "combat", "Survival": "survival",
    "Social": "social", "World": "world", "Character": "character",
}

def classify_lifetime_page(item):
    edid = str(item.get("edid") or "").lower()
    enam = str(item.get("classification") or "")
    full = str(item.get("full") or "").lower()
    if "bobblehead" in edid or "bobblehead" in full: return "bobbleheads"
    if "magazine" in edid or "magazine" in full: return "magazines"
    if "fish" in edid or "fish" in full: return "fishing"
    if "burningsprings" in edid: return "springs"
    return CLASSIFICATION_TO_SLUG.get(enam, "world")

# ==================================================================
# Build pages dict
# ==================================================================

pages = {}
for item in buckets.get("daily", []):
    pages.setdefault("daily", []).append(item)
for item in buckets.get("weekly", []):
    pages.setdefault("weekly", []).append(item)
for item in buckets.get("event", []):
    pages.setdefault("events", []).append(item)
for item in buckets.get("lifetime", []):
    pages.setdefault(classify_lifetime_page(item), []).append(item)
pages["cut"] = buckets.get("cut", [])
for slug, items in season_buckets.items():
    pages[f"season:{slug}"] = items

def sort_key(item):
    return (0 if item.get("is_epic") else 1, str(item.get("full") or "").lower())

for key in pages:
    pages[key].sort(key=sort_key)

# ==================================================================
# Output
# ==================================================================

print("[challenges] Writing JSON...")
page_meta = {}
for key, items in pages.items():
    page_meta[key] = {"count": len(items), "has_meta": any(it["is_meta"] for it in items)}

output = {
    "generated": "build_challenges_json.py v3",
    "challenges": dict(buckets),
    "pages": {k: {"items": v} for k, v in pages.items()},
    "page_meta": page_meta,
    "season_slugs": sorted(season_buckets.keys()),
    "patch_log": [],
}

out_path = DIST_DIR / "challenges.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"[challenges] Written: {out_path} ({out_path.stat().st_size:,} bytes)")
for k, v in sorted(page_meta.items()):
    print(f"    {k}: {v['count']} items")
print("[challenges] Done.")
