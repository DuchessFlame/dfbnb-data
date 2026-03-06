#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_challenges_json.py

Reads CHAL_Export_*.tsv and GMRW_Export_*.tsv, outputs:
  dist/challenges/challenges.json

JSON shape (bucket format, consumed by df-bnb-challenges.js):
  {
    "challenges": {
      "daily":    [ <item>, ... ],
      "weekly":   [ <item>, ... ],
      "lifetime": [ <item>, ... ],
      "event":    [ <item>, ... ],
      "cut":      [ <item>, ... ],
    },
    "patch_log": []
  }

Item shape:
  {
    "form_id":       "003B8015",
    "edid":          "Challenge_Lifetime_Kill_Arthropods_META",
    "full":          "Kill Different Kinds of Arthropods",
    "snam":          "Creatures Killed",       # stat tracker label
    "required":      0,                         # TNAM target count
    "scope":         "Lifetime",                # CNAM
    "classification":"Combat",                  # ENAM
    "conditions":    ["Top:Target.HasKeyword(...)"],
    "rewards":       ["Caps", "XP"],            # human-readable from GMRW
    "is_cut":        false,
    "is_meta":       true,                      # parent challenge
    "children":      [ <item>, ... ]            # sub-challenges (nested)
  }
"""

import csv
import glob
import json
import os
import re
from collections import defaultdict
from pathlib import Path

DIST_DIR = Path("dist/challenges")

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def newest(pattern):
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files matched: {pattern}")
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[-1]


def read_tsv(path):
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


# --------------------------------------------------
# Load TSVs
# --------------------------------------------------

CHAL = read_tsv(newest("tsv/CHAL_Export_*.tsv"))
GMRW = read_tsv(newest("tsv/GMRW_Export_*.tsv"))

# --------------------------------------------------
# Build GMRW reward lookup
# Keyed by GMRW FormID -> human-readable reward strings
# --------------------------------------------------

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

def parse_ref_name(ref_str):
    """'FormID:EDID:SIG' -> human label."""
    if not ref_str:
        return ""
    parts = ref_str.strip().split(":")
    fid = parts[0]
    edid = parts[1] if len(parts) > 1 else ""
    if fid in KNOWN_FID_NAMES:
        return KNOWN_FID_NAMES[fid]
    # Prettify EDID
    label = edid
    label = re.sub(r"^(LLS?|RA_LL|LL|QuestReward)_+", "", label, flags=re.IGNORECASE)
    label = label.replace("__", "_").replace("_", " ").strip()
    label = re.sub(r"\s+", " ", label)
    return label.strip() or fid


def reward_label_from_gmrw_row(row):
    """Turn one GMRW TSV row into a human-readable reward label."""
    parts = []

    # XP curve
    xpct = pick(row, "XPCT_XPCurveTable")
    if xpct:
        parts.append("XP")

    # Caps global
    nam8 = pick(row, "NAM8_CapsGlobal")
    if nam8:
        fid = nam8.split(":")[0]
        parts.append(KNOWN_FID_NAMES.get(fid, "Caps"))

    # Currency object
    qrco = pick(row, "QRCO_CurrencyObject")
    if qrco:
        name = parse_ref_name(qrco)
        if name and name not in parts:
            parts.append(name)

    # Rewarded item
    rewarded = pick(row, "RewardedItem")
    if rewarded:
        name = parse_ref_name(rewarded)
        if name and name not in parts:
            parts.append(name)

    return parts


# Group GMRW rows by FormID
gmrw_by_formid = defaultdict(list)
for row in GMRW:
    fid = pick(row, "FormID")
    if fid:
        gmrw_by_formid[fid].append(row)


def get_rewards_for_gmrw(gmrw_fid):
    """Return deduplicated list of reward label strings for a GMRW FormID."""
    rows = gmrw_by_formid.get(gmrw_fid, [])
    seen = []
    seen_set = set()
    for row in rows:
        for label in reward_label_from_gmrw_row(row):
            if label and label not in seen_set:
                seen_set.add(label)
                seen.append(label)
    return seen


def parse_dnam_rewards(dnam_str):
    """
    DNAM: 'Reward0:00668A71:ChallengeReward_SCORE_Challenge_Daily_Collect:GMRW|Reward1:...'
    OR just 'Reward0:FormID:EDID:GMRW'
    Returns list of reward label strings.
    """
    if not dnam_str or dnam_str.strip() == "":
        return []
    all_rewards = []
    seen_set = set()
    # Split on pipe or newline if multiple rewards on one row
    entries = re.split(r"[|\n]", dnam_str)
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        # Format: RewardN:FormID:EDID:SIG
        m = re.match(r"Reward\d+:([0-9A-Fa-f]+):.*:GMRW", entry)
        if m:
            gmrw_fid = m.group(1)
            for label in get_rewards_for_gmrw(gmrw_fid):
                if label not in seen_set:
                    seen_set.add(label)
                    all_rewards.append(label)
    return all_rewards


# --------------------------------------------------
# Parse conditions from Cond1..Cond52
# --------------------------------------------------

def extract_conditions(row):
    conds = []
    count_str = pick(row, "CondCount")
    try:
        count = int(count_str)
    except (ValueError, TypeError):
        count = 52  # read all
    for i in range(1, min(count + 1, 53)):
        c = pick(row, f"Cond{i}")
        if c:
            conds.append(c)
    return conds


# --------------------------------------------------
# Classify challenges
# --------------------------------------------------

def is_cut(edid):
    u = edid.upper()
    return u.startswith("CUT") or u.startswith("DEL") or u.startswith("ZZZ") or u.startswith("POST")


def is_meta(edid):
    return edid.upper().endswith("_META")


def is_sub(edid, enam):
    return "_SUB_" in edid.upper() or enam == "Sub Challenge (Unsorted)"


def edid_base(edid):
    """Strip _META or _SUB_* suffix to get the shared parent prefix."""
    # Remove _META at end
    base = re.sub(r"_META$", "", edid, flags=re.IGNORECASE)
    # Remove _SUB_anything at end
    base = re.sub(r"_SUB_.+$", "", base, flags=re.IGNORECASE)
    # Also strip _Low/_Mid/_High/_Lowest etc
    base = re.sub(r"_(Low|Mid|High|Lowest|Middle|Highest)$", "", base, flags=re.IGNORECASE)
    return base


def scope_bucket(cnam):
    c = cnam.lower().strip()
    if c == "daily":
        return "daily"
    if c == "weekly":
        return "weekly"
    if c == "lifetime":
        return "lifetime"
    if c in ("event", "events"):
        return "event"
    if c == "monthly":
        return "weekly"  # treat monthly as weekly for routing
    return "lifetime"  # fallback


# --------------------------------------------------
# Build item dict from a CHAL row
# --------------------------------------------------

def row_to_item(row, rewards_override=None):
    form_id = pick(row, "FormID")
    edid    = pick(row, "EDID")
    full    = pick(row, "FULL")
    snam    = pick(row, "SNAM").lstrip()
    tnam    = pick(row, "TNAM")
    cnam    = pick(row, "CNAM")
    enam    = pick(row, "ENAM")
    dnam    = pick(row, "DNAM")

    try:
        required = int(tnam) if tnam and tnam != "0" else None
    except ValueError:
        required = tnam or None

    conditions = extract_conditions(row)

    if rewards_override is not None:
        rewards = rewards_override
    else:
        rewards = parse_dnam_rewards(dnam)

    return {
        "form_id":        form_id,
        "edid":           edid,
        "full":           full,
        "snam":           snam if snam and snam != "NONE" else "",
        "required":       required,
        "scope":          cnam,
        "classification": enam,
        "conditions":     conditions,
        "rewards":        rewards,
        "is_cut":         is_cut(edid),
        "is_meta":        is_meta(edid),
        "is_sub":         is_sub(edid, enam),
        "children":       [],  # filled in below
    }


# --------------------------------------------------
# Index all rows, build parent->children map
# --------------------------------------------------

all_rows = {pick(r, "FormID"): r for r in CHAL if pick(r, "FormID")}

# Build base -> list of child rows
children_by_base = defaultdict(list)
meta_by_base = {}

for fid, row in all_rows.items():
    edid = pick(row, "EDID")
    enam = pick(row, "ENAM")
    if is_sub(edid, enam):
        base = edid_base(edid)
        children_by_base[base].append(row)
    elif is_meta(edid):
        base = edid_base(edid)
        meta_by_base[base] = row


def build_item_with_children(row):
    edid = pick(row, "EDID")
    item = row_to_item(row)

    if item["is_meta"]:
        base = edid_base(edid)
        child_rows = children_by_base.get(base, [])
        # Only include children that match the scope (not zzz/cut unless the parent is cut)
        children = []
        for cr in child_rows:
            child_enam = pick(cr, "ENAM")
            child_edid = pick(cr, "EDID")
            # Skip purely-hidden sub challenges (zzz prefix) unless parent is also zzz/cut
            if child_enam == "Sub Challenge (Unsorted)" and not item["is_cut"]:
                # Include them only if they share the exact EDID base (not a different meta)
                child_base = edid_base(child_edid)
                if child_base != base:
                    continue
            child_item = row_to_item(cr)
            # Children inherit parent rewards if they have none
            if not child_item["rewards"] and item["rewards"]:
                child_item["rewards"] = item["rewards"]
            children.append(child_item)

        # Sort children by their full name
        children.sort(key=lambda x: (x.get("full") or "").lower())
        item["children"] = children

    return item


# --------------------------------------------------
# Bucket items into scope groups
# --------------------------------------------------

buckets = defaultdict(list)

# Track which EDIDs are sub-challenges so we don't double-add them
sub_edids = set()
for base, rows in children_by_base.items():
    for r in rows:
        sub_edids.add(pick(r, "EDID"))

for fid, row in all_rows.items():
    edid = pick(row, "EDID")
    enam = pick(row, "ENAM")
    cnam = pick(row, "CNAM")

    # Skip pure sub-challenges at top level — they live inside their parent
    # BUT: if a SUB has no matching meta parent in scope, include it standalone
    if is_sub(edid, enam):
        base = edid_base(edid)
        if base in meta_by_base:
            continue  # will be nested under parent

    item = build_item_with_children(row)
    bucket = scope_bucket(cnam)
    buckets[bucket].append(item)


# --------------------------------------------------
# Sort within each bucket
# --------------------------------------------------

def sort_key(item):
    full = (item.get("full") or "").lower()
    edid = (item.get("edid") or "").lower()
    return (full, edid)


for bucket in buckets:
    buckets[bucket].sort(key=sort_key)


# --------------------------------------------------
# Output
# --------------------------------------------------

output = {
    "challenges": {
        "daily":    buckets.get("daily", []),
        "weekly":   buckets.get("weekly", []),
        "lifetime": buckets.get("lifetime", []),
        "event":    buckets.get("event", []),
        "cut":      buckets.get("cut", []),
    },
    "patch_log": []
}

DIST_DIR.mkdir(parents=True, exist_ok=True)
out_path = DIST_DIR / "challenges.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

total = sum(len(v) for v in output["challenges"].values())
print(f"Done. {total} top-level challenges written to {out_path}")
for k, v in output["challenges"].items():
    metas  = sum(1 for x in v if x.get("is_meta"))
    subs   = sum(len(x.get("children", [])) for x in v)
    cut    = sum(1 for x in v if x.get("is_cut"))
    print(f"  {k:10s}: {len(v):4d} items  ({metas} meta w/ {subs} children, {cut} cut)")
