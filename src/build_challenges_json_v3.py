#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_challenges_json.py  (v4 — challenges + seasons + quests + encounters)

Reads game-data TSVs and outputs an enriched JSON consumed by
df-bnb-challenges.js for:
    /df/challenges/*          — lifetime / daily / weekly challenges
    /df/seasons/*             — mini-season challenge checklists
    /df/quests/*              — quest checklists (main, side, daily pipboy)
    /df/random-encounters/*   — random encounter cards

Outputs:
    dist/challenges/challenges.json

Cross-references:
    CHAL  — challenge records (conditions, META/SUB hierarchy)
    QUEST — quest records (quests, events, random encounters)
    GMRW  — game reward records (XP, caps, items)
    ENTM  — entitlements (Atom Shop items, images)
    BOOK  — notes / holotapes
    COBJ  — craftable objects
    KYWD  — keywords (weapon types, item categories)
    MISC  — misc items (drinking glasses, etc.)
    FLST  — form lists (grouped item references)
    guide_index.tsv — site guide pages for hyperlinking
"""

import copy
import csv
import glob
import json
import os
import re
from collections import defaultdict
from pathlib import Path

from patchlog_utils import diff_item_lists, _write_json, _git_show_json, write_empty_patchlog_feed

# Resolve paths relative to the repo root (one level up from src/) so the
# script produces correct output regardless of which directory it's run from.
_REPO_ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = _REPO_ROOT / "dist" / "challenges"
DIST_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================================
# Helpers
# ==================================================================

_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

def _filename_date_key(path):
    """Extract (year, month_number) from filenames like LVLI_Export_April_2026_*.tsv."""
    base = os.path.basename(path).lower()
    m = re.search(r'_([a-z]+)_(\d{4})', base)
    if m:
        month_num = _MONTH_ORDER.get(m.group(1), 0)
        if month_num:
            return (int(m.group(2)), month_num)
    return (0, 0)  # unknown → sort low so parseable dates always win

def newest(pattern):
    """Pick the most recent file matching *pattern*.
    Primary sort: parsed year+month from filename (reliable on GitHub Actions
    where git checkout mtimes vary by checkout order, not commit date).
    Tiebreaker: file mtime (useful on local machines)."""
    full_pattern = str(_REPO_ROOT / pattern)
    files = glob.glob(full_pattern)
    if not files:
        return None
    files.sort(key=lambda x: (_filename_date_key(x), os.path.getmtime(x)))
    return files[-1]


def previous(pattern):
    """Pick the SECOND most recent file matching *pattern*, or None.

    Drives the ★ NEW pill: a challenge is "new" when its FormID exists in the
    newest CHAL export but not in the one before it. Returns None when there
    is only one export (e.g. a fresh clone, or the PTS channel where
    normalize_pts_tsv.py leaves a single normalized file) — in that case
    nothing is flagged new, which is the correct conservative answer."""
    full_pattern = str(_REPO_ROOT / pattern)
    files = glob.glob(full_pattern)
    if len(files) < 2:
        return None
    files.sort(key=lambda x: (_filename_date_key(x), os.path.getmtime(x)))
    return files[-2]


def read_tsv(path):
    if not path or not os.path.exists(path):
        return []
    def _read(enc):
        with open(path, encoding=enc, errors="replace", newline="") as f:
            raw = f.read().replace("\x00", "")
            import io
            return list(csv.DictReader(io.StringIO(raw), delimiter="\t"))
    try:
        return _read("utf-8-sig")
    except UnicodeDecodeError:
        return _read("cp1252")


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

# ---- ★ NEW pill source: FormIDs present in the newest CHAL export but not
# ---- the one before it. See previous() for the single-export case.
_chal_prev_path = previous("tsv/CHAL_Export_*.tsv")
PREV_CHAL_FIDS = set()
if _chal_prev_path:
    PREV_CHAL_FIDS = {
        str(r.get("FormID") or "").strip().upper()
        for r in read_tsv(_chal_prev_path)
        if str(r.get("FormID") or "").strip()
    }
    print(f"  NEW-pill baseline: {os.path.basename(_chal_prev_path)} ({len(PREV_CHAL_FIDS)} FormIDs)")
else:
    print("  NEW-pill baseline: none (only one CHAL export) — nothing will be flagged new")

# The Apr–Jun 2026 exports shipped with CNAM/ENAM blank because the xEdit
# script asked for the wrong element labels. Fail loudly rather than silently
# rebuilding a site with no challenge frequencies or categories.
_cnam_populated = sum(1 for r in CHAL if str(r.get("CNAM") or "").strip())
if CHAL and _cnam_populated == 0:
    raise SystemExit(
        "[challenges] ABORT: CNAM (Challenge Frequency) is empty for all "
        f"{len(CHAL)} rows of {os.path.basename(newest('tsv/CHAL_Export_*.tsv') or '')}. "
        "Re-run '!!!Wordpress - ExportCHALToTSV.pas' — it reads "
        "'CNAM - Challenge Frequency' / 'ENAM - Challenge Category'."
    )

# ---- GLOB values: tiered lifetime challenges leave TNAM empty and point at a
# ---- global instead (HNAM = "Challenge_Global_0076 [GLOB:00432F7B]" → 76).
# ---- Without this the Required Count box renders "—" on most Lifetime rows.
# The GLOB export is ~36 MB because every row carries a Ref1..Ref29 tail. We
# only need FormID + FLTV, so stream it and split the first three columns
# rather than building a 30-key dict per row (minutes → under a second).
def load_glob_values():
    path = newest("tsv/GLOB_Export_*.tsv")
    out = {}
    if not path:
        return out
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, encoding=enc, errors="replace", newline="") as f:
                header = f.readline().rstrip("\r\n").split("\t")
                try:
                    i_fid, i_val = header.index("FormID"), header.index("FLTV")
                except ValueError:
                    return out
                hi = max(i_fid, i_val) + 1
                for line in f:
                    cols = line.split("\t", hi)
                    if len(cols) <= max(i_fid, i_val):
                        continue
                    fid = cols[i_fid].strip().upper()
                    raw = cols[i_val].strip()
                    if not fid or not raw:
                        continue
                    try:
                        out[fid] = int(round(float(raw)))
                    except ValueError:
                        continue
            return out
        except UnicodeDecodeError:
            continue
    return out

GLOB_VALUES = load_glob_values()
print(f"  GLOB: {len(GLOB_VALUES)} numeric globals")

GMRW = read_tsv(newest("tsv/GMRW_Export_*.tsv"))
ENTM = read_tsv(newest("tsv/ENTM_Export_*.tsv"))
BOOK = read_tsv(newest("tsv/BOOK_Export_*.tsv"))
KYWD = read_tsv(newest("tsv/KYWD_Export_*.tsv"))
FLST_ENTRIES = read_tsv(newest("tsv/FLST_Export_*_Entries.tsv"))

_cndf_path = newest("tsv/CNDF_Export_*.tsv")
CNDF = read_tsv(_cndf_path) if _cndf_path else []

_cobj_path = newest("tsv/COBJ_Export_*.tsv")
COBJ = read_tsv(_cobj_path) if _cobj_path else []

_misc_path = newest("tsv/MISC_Export_*.tsv")
MISC = read_tsv(_misc_path) if _misc_path else []

QUEST = read_tsv(newest("tsv/QUEST_Export_*.tsv"))

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
print(f"  QUEST: {len(QUEST)} rows")
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

cndf_by_fid = {}
for row in CNDF:
    fid = pick(row, "FormID")
    if fid: cndf_by_fid[fid.upper()] = row
print(f"  CNDF: {len(cndf_by_fid)} rows")

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


# ==================================================================
# xEdit-format condition decode (leaf-expands CNDF condition-forms)
# Output: ".{Func}({ResolvedName [REC:FormID]}) = {value}"  (leading "."
# from a Subject run-on; other run-ons are prefixed by name). Any
# IsTrueForConditionForm(... [CNDF:x]) is resolved down to the leaf check
# (e.g. HasKeyword CampPets_Cat) per the challenge-style-guide decision.
# ==================================================================

# Raw numeric condition-function indices that the export leaves undecoded.
# The World Pet species CNDFs use these with a CampPets_* keyword param, so
# they read as a keyword check. Extend as more indices are encountered.
FUNC_INDEX = {
    "940": "HasKeyword",
    "941": "HasKeyword",
}

def _fid_from_bytes(b):
    """'1C AF 79 00' -> '0079AF1C' (little-endian form ref); else None."""
    parts = str(b or "").strip().split()
    if len(parts) == 4 and all(re.fullmatch(r"[0-9A-Fa-f]{2}", p) for p in parts):
        return "".join(reversed([p.upper() for p in parts]))
    return None

def _fmt_cond_value(v):
    try:
        f = float(v)
        return str(int(f)) if f == int(f) else f"{f:.6f}"
    except Exception:
        return str(v or "").strip()

def _cond_param_label(param):
    """Resolve a condition param into (display, '[REC:FormID]')."""
    p = str(param or "").strip()
    ref_m = re.search(r'\[(\w+):([0-9A-Fa-f]+)\]', p)
    if ref_m:
        rec, fid = ref_m.group(1), ref_m.group(2).upper()
        qm = re.search(r'"([^"]+)"', p)
        edid_m = re.match(r'([A-Za-z0-9_]+)\s*[\["]', p)
        hint = qm.group(1) if qm else (edid_m.group(1) if edid_m else "")
        resolved = resolve_name_from_fid(fid)
        disp = resolved if (resolved and resolved != fid) else hint
        return (disp or hint or fid), f"[{rec}:{fid}]"
    fid = _fid_from_bytes(p)
    if fid:
        resolved = resolve_name_from_fid(fid)
        sig = "KYWD" if fid in kywd_by_fid else "FORM"
        return (resolved if resolved and resolved != fid else fid), f"[{sig}:{fid}]"
    return p, ""

def _cndf_conditions(row):
    """Pull Cond1..N off a CNDF row."""
    out = []
    count = safe_int(pick(row, "CondCount"), 76) or 76
    for i in range(1, min(count + 1, 77)):
        c = pick(row, f"Cond{i}")
        if c: out.append(c)
    return out

def _is_null_leaf(line):
    """True for an uninformative leaf whose form ref resolved to all-zeros
    (e.g. an engine check that takes no form param, like the IsPet active check)."""
    return bool(re.search(r'\[(?:FORM|KYWD):0+\]', line) or re.search(r'\(0+\s', line))

def decode_condition(cond_str, _seen=None):
    """One raw pipe condition -> list of xEdit display lines, CNDF-leaf-expanded."""
    if _seen is None: _seen = set()
    s = str(cond_str or "").strip()
    if not s: return []
    if "|" not in s:  # already human/decoded — pass through, strip flag prefix
        return [re.sub(r'^(Top|More|And|Or):', '', s).strip()]
    parts = s.split("|")
    value = _fmt_cond_value(parts[1] if len(parts) > 1 else "")
    func = parts[2] if len(parts) > 2 else ""
    func = FUNC_INDEX.get(func.strip(), func.strip())
    param = parts[5] if len(parts) > 5 else ""
    runon = (parts[9].strip() if len(parts) > 9 else "") or "Subject"
    prefix = "." if runon == "Subject" else f"{runon}."
    # Leaf-expand IsTrueForConditionForm(CNDF) down to the underlying check, but
    # only when the leaf is informative. If every inner leaf resolves to a null
    # form ref (an engine check with no form param), keep the named condition-form
    # line instead — its EDID is self-descriptive (e.g. ...CheckPetActive...IsPet).
    if func == "IsTrueForConditionForm":
        ref_m = re.search(r'\[CNDF:([0-9A-Fa-f]+)\]', param)
        if ref_m:
            cfid = ref_m.group(1).upper()
            if cfid not in _seen and cfid in cndf_by_fid:
                _seen.add(cfid)
                leaves = []
                for ic in _cndf_conditions(cndf_by_fid[cfid]):
                    leaves.extend(decode_condition(ic, _seen))
                meaningful = [l for l in leaves if not _is_null_leaf(l)]
                if meaningful:
                    return meaningful
    disp, ref = _cond_param_label(param)
    if ref:
        return [f"{prefix}{func}({disp} {ref}) = {value}"]
    if disp:
        return [f"{prefix}{func}({disp}) = {value}"]
    return [f"{prefix}{func}() = {value}"]

def conditions_display(conditions):
    """All raw conditions -> deduped xEdit display lines. Skips engine-plumbing
    checks (IsFalloutWorlds -> shown as a notice; GetIsForm -> sub-challenge wiring)."""
    out, seen = [], set()
    for c in (conditions or []):
        cs = str(c or "")
        if "IsFalloutWorlds" in cs or "GetIsForm" in cs:
            continue
        for line in decode_condition(cs):
            if line and line not in seen:
                seen.add(line); out.append(line)
    return out


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

# Pioneer Scouts badges: "..._Tadpole_Badge_Possum_Chemist" is the umbrella
# challenge, "..._Tadpole_Badge_Possum_Chemist_Sub_AcquireBeaker" its steps.
# The umbrella carries no _META suffix, so nothing nested them and all 357
# steps rendered as loose root rows.
SCOUT_BADGE_RE = re.compile(r"Tadpole_Badge_(Possum|Tadpole)_([A-Za-z]+)$", re.IGNORECASE)
SCOUT_ANY_RE = re.compile(r"Tadpole_Badge_(Possum|Tadpole)_", re.IGNORECASE)

def is_meta(edid):
    """_META as a SEGMENT, not just a suffix. Challenge_Lifetime_Picture_
    Creatures_META_Camera carries a trailing _Camera, so an endswith() test
    never registered it as a META and its 53 sub-challenges stayed loose.
    Scout badge umbrellas count too — see SCOUT_BADGE_RE."""
    u = str(edid or "").upper()
    if u.endswith("_META") or "_META_" in u:
        return True
    return bool(SCOUT_BADGE_RE.search(str(edid or "")))

def scout_track(edid):
    """'possum' / 'tadpole' for a Pioneer Scouts badge row, else None."""
    m = SCOUT_ANY_RE.search(str(edid or ""))
    return m.group(1).lower() if m else None

def is_sub_edid(edid):
    """STRONG signal: the EDID says so. "_SUB_<name>" (most content) or a bare
    trailing "_SUB" (Burning Springs head hunts, ..._Group05_KillSniper_SUB)."""
    u = str(edid or "").upper()
    return "_SUB_" in u or u.endswith("_SUB")

def is_sub_candidate(edid, enam):
    """WEAK signal, for parent-hunting only.

    ENAM "Sub Challenge (Unsorted)" is NOT a sub marker — it is the default
    category, present on 4,519 of 5,436 records (83%), including every plain
    SCORE daily. Treating it as authoritative flagged 3,860 ordinary root rows
    as subs and put a SUB pill on all of them. Use it to widen the search for a
    parent, then let the final is_sub be decided by whether a parent was
    actually found (see the is_sub resolution pass after pairing)."""
    # An "_Intro" row is the tutorial-tier version of a challenge and a root
    # row in its own right, never a sub — exclude it explicitly, or the region
    # normalisation above makes Challenge_Lifetime_Discover_MireRegion_Intro
    # look like a one-segment child of Challenge_Lifetime_Discover_Mire_META.
    if str(edid or "").upper().endswith("_INTRO"):
        return False
    return is_sub_edid(edid) or str(enam or "") == "Sub Challenge (Unsorted)"

def edid_base(edid):
    # Strip _META whether it ends the EDID or sits mid-string with a trailing
    # qualifier (..._Picture_Creatures_META_Camera -> ..._Picture_Creatures).
    base = re.sub(r"_META(_.+)?$", "", str(edid or ""), flags=re.IGNORECASE)
    # Subs mark themselves either as "_SUB_<name>" or with a bare trailing
    # "_SUB" after the action segment (Burning Springs head-hunt groups use
    # the latter: ..._Group05_KillSniper_SUB).
    base = re.sub(r"_SUB(_.+)?$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"_(Low|Mid|High|Lowest|Middle|Highest)$", "", base, flags=re.IGNORECASE)
    # Region METAs drop the word their subs keep:
    #   META  Challenge_Lifetime_Discover_Mire_META
    #   SUBs  Challenge_Lifetime_Discover_MireRegion_SUB_Berkeley
    #   SUBs  Challenge_Lifetime_Discover_ForestRegion_PointPleasant  (unmarked)
    # Normalising "Region" out of BOTH sides makes them compare equal.
    base = re.sub(r"Region(?=_|$)", "", base)
    return base

def scope_bucket(cnam):
    c = str(cnam or "").lower().strip()
    if c == "daily": return "daily"
    if c == "weekly": return "weekly"
    if c == "lifetime": return "lifetime"
    if c in ("event", "events"): return "event"
    if c == "monthly": return "weekly"
    return "lifetime"


# ------------------------------------------------------------------
# Root group label — LOCKED ORDER: Daily → Weekly → Event → Lifetime - X
# ------------------------------------------------------------------
# Every challenge page groups its root panels the same way. Daily, Weekly and
# Event are single panels; Lifetime splits by ENAM category into
# "Lifetime - World", "Lifetime - Social", etc. so a long lifetime list reads
# the way the in-game Challenges menu does.
#
# ENAM values seen in the July 2026 export:
#   Sub Challenge (Unsorted), Tracker, Burning Springs, Combat, Fishing,
#   World, Character, Social, Survival
# The PTS build also emits a raw ordinal 11 for the new World Pets levelling
# challenges, which xEdit has no label for yet.

ENAM_ALIASES = {
    "11": "World Pets",
}

# Categories that are bookkeeping, not a player-facing grouping. A lifetime
# row carrying one of these falls back to the generic "Lifetime" panel.
ENAM_NOT_A_CATEGORY = {"sub challenge (unsorted)", "tracker", ""}

def category_label(enam):
    raw = str(enam or "").strip()
    return ENAM_ALIASES.get(raw, raw)

def root_group(item):
    """Panel label for a challenge row. Renderers order panels
    Daily → Weekly → Event → Lifetime*, then anything else alphabetically."""
    bucket = scope_bucket(item.get("scope"))
    if bucket == "daily":  return "Daily"
    if bucket == "weekly": return "Weekly"
    if bucket == "event":  return "Event"
    cat = category_label(item.get("classification"))
    if cat.lower() in ENAM_NOT_A_CATEGORY:
        return "Lifetime"
    return f"Lifetime - {cat}"


# ------------------------------------------------------------------
# Reward fallback — Daily and Weekly challenges always pay SCORE
# ------------------------------------------------------------------
# Daily/Weekly (S.C.O.R.E.) challenges have no MNAM reward text because the
# payout is scoreboard progress, not an item. Rather than render an empty
# gold Reward box on every one of them, label it.

SCORE_REWARD = "S.C.O.R.E."
ATOMS_REWARD = "Atoms"

def default_rewards(item):
    if item.get("rewards"):
        return item["rewards"]
    if scope_bucket(item.get("scope")) not in ("daily", "weekly"):
        return []
    # The 11 ATOMS_-prefixed dailies/weeklies pay Atoms, not scoreboard
    # progress. They sit next to a same-titled SCORE version, so labelling
    # both "S.C.O.R.E." made the pair look like a duplicate AND was wrong.
    if str(item.get("edid") or "").upper().startswith("ATOMS_"):
        return [ATOMS_REWARD]
    return [SCORE_REWARD]

# Limited-time event content. Any EDID carrying this marker ran during one
# event and is not part of the repeatable daily/weekly pool.
ATX_EVENT_RE = re.compile(r"ATX_DE\d{4}", re.IGNORECASE)

# Dev/debug leftovers that shipped in the record set. Treated as cut content so
# they stay inspectable on the cut page but never pollute a live pool — e.g.
# SCORE_TEST_Challenge_Monthly_Collect_Soap, which collided with the real
# "Collect Soap" weekly at a nonsense required count of 500.
TEST_EDID_RE = re.compile(r"(^|_)TEST(_|$)", re.IGNORECASE)

def same_frequency(child, parent):
    """A sub must share its META's CNAM frequency.

    Without this a Lifetime META adopts unrelated SCORE dailies that happen to
    share a topic token: Challenge_Lifetime_ClaimWorkshops_META ("Claim
    Different Workshops", 21 required) swallowed
    SCORE_Challenge_Daily_ClaimWorkshop and the Weekly one, removing them from
    the Daily and Weekly pages entirely."""
    return scope_bucket(child.get("scope")) == scope_bucket(parent.get("scope"))

def is_epic(full_name, edid=""):
    """Epic rows are normally titled "Epic - …", but at least one
    (SCORE_Challenge_Weekly_Shelters_Build_Decor_Epic) is only marked in the
    EDID. Check both, or it lands in the Standard panel."""
    if str(full_name or "").strip().lower().startswith("epic"):
        return True
    return str(edid or "").strip().lower().endswith("_epic")


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
    # Added to the CHAL export July 2026.
    mnam = pick(row, "MNAM")   # Reward Display, e.g. "Stimpaks (3)"
    rnam = pick(row, "RNAM")   # Reward Icon, e.g. "IconCR_Stimpaks"
    hnam = pick(row, "HNAM")   # Required Count Global (GLOB-driven counts)
    anam = pick(row, "ANAM")   # Pre-requisite challenge

    required = safe_int(tnam) if tnam and tnam != "0" else None
    # TNAM empty → the count lives in the HNAM global. Resolve it so the
    # Required Count box shows a real number instead of "—".
    required_from_glob = False
    if required is None and hnam:
        _gm = re.search(r'\[GLOB:([0-9A-Fa-f]{8})\]', hnam)
        if _gm:
            _gv = GLOB_VALUES.get(_gm.group(1).upper())
            if _gv:
                required = _gv
                required_from_glob = True
    conditions = extract_conditions(row)
    rewards = rewards_override if rewards_override is not None else parse_dnam_rewards(dnam)
    # MNAM is the record's own reward text and beats a DNAM-derived guess.
    # Guard against corrupt strings: at least one MNAM in the July 2026 export
    # comes through as "¬¬¬¬" (Challenge_Weekly_RD01_EncAll). Anything with no
    # letter or digit in it is garbage, not a reward name — drop it and let the
    # normal fallbacks apply rather than printing mojibake in the gold box.
    if mnam and not re.search(r"[A-Za-z0-9]", mnam):
        mnam = ""
    if mnam and not rewards:
        rewards = [mnam]
    reward_unknown = len(rewards) == 0
    parsed_conds = parse_condition_items(conditions)
    human_conditions = [hc for c in conditions if (hc := humanize_condition(c))]
    display_conditions = conditions_display(conditions)
    item_draft = {"form_id": form_id, "edid": edid, "full": full, "conditions": conditions}
    guides = find_related_guides(item_draft)
    image_url = find_challenge_image(conditions, edid)

    item = {
        "form_id": form_id, "edid": edid, "full": full,
        "snam": snam if snam and snam != "NONE" else "",
        "required": required, "scope": cnam, "classification": enam,
        "category": category_label(enam),
        "reward_icon": rnam, "required_count_global": hnam,
        "required_from_glob": required_from_glob, "prerequisite": anam,
        "conditions": conditions, "conditions_human": human_conditions,
        "conditions_display": display_conditions,
        "condition_items": parsed_conds.get("items", []),
        "condition_keywords": parsed_conds.get("keywords", []),
        "rewards": rewards, "reward_unknown": reward_unknown,
        "is_cut": is_cut(edid) or bool(TEST_EDID_RE.search(edid)),
        "is_meta": is_meta(edid),
        # is_sub starts as the strong EDID signal and is finalised after
        # pairing; sub_candidate is the wide net used to hunt for a parent.
        "is_sub": is_sub_edid(edid), "sub_candidate": is_sub_candidate(edid, enam),
        "is_epic": is_epic(full, edid),
        "is_new": bool(PREV_CHAL_FIDS) and form_id.strip().upper() not in PREV_CHAL_FIDS,
        "children": [], "guides": guides, "image_url": image_url, "jasf": jasf,
    }
    # Daily/Weekly pay SCORE; do this after the dict exists so it can read scope.
    item["rewards"] = default_rewards(item)
    item["reward_unknown"] = not item["rewards"]
    item["group"] = root_group(item)
    return item


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
    if item["sub_candidate"] and not item["is_cut"]:
        base = edid_base(item["edid"])
        parent = meta_map.get(base)
        if parent and parent is not item and same_frequency(item, parent):
            parent["children"].append(item)
            item["parent_edid"] = parent["edid"]

# ---- Second pass: sub bases that EXTEND their META's base --------------
# Burning Springs head hunts name the action inside the sub:
#     META  Challenge_Lifetime_BurnBounty_CompleteHeadGroups_Group05_META
#     SUB   Challenge_Lifetime_BurnBounty_CompleteHeadGroups_Group05_KillSniper_SUB
# so the sub's base is the META's base plus a segment. Attach to the LONGEST
# META base that is a proper prefix, which keeps Group05 from stealing rows
# that belong to the broader _All_META.
_meta_bases_by_len = sorted(meta_map.keys(), key=len, reverse=True)
_prefix_paired = 0
for item in all_items:
    if not item["sub_candidate"] or item["is_meta"] or item["is_cut"]:
        continue
    if item.get("parent_edid"):
        continue
    base = edid_base(item["edid"])
    for mbase in _meta_bases_by_len:
        # The sub's base must be the META's base plus EXACTLY ONE segment.
        # Without that limit a generic META swallows every deeper row:
        # Challenge_Lifetime_Collect_Magazines is a prefix of every series'
        # issues, so "Collect an Issue from Different Magazines" claimed all
        # 10 Grognak issues (21 subs against a required count of 11) and left
        # the Grognak META empty.
        if (len(mbase) < len(base) and base.startswith(mbase + "_")
                and "_" not in base[len(mbase) + 1:]):
            parent = meta_map[mbase]
            if parent is not item and same_frequency(item, parent):
                parent["children"].append(item)
                item["parent_edid"] = parent["edid"]
                _prefix_paired += 1
            break

# ---- Third pass: orphan SUB rows --------------------------------------
# Exact edid_base matching misses pairs where the META and its subs use
# different verbs. Bobbleheads is the canonical case:
#     META  Challenge_Lifetime_ItemsConsumed_Bobbleheads_META
#     SUBs  Challenge_Lifetime_Collect_Bobbleheads_SUB_Charisma  (x21)
# Both end in the same topic token ("Bobbleheads"), so pair on that. Only
# attach when EXACTLY ONE META claims the token — an ambiguous token is left
# alone rather than risking a wrong parent.

def _topic_token(edid):
    base = edid_base(edid)
    # A numbered issue/part suffix is not the topic — the segment before it is.
    # ..._Collect_Magazines_Grognak_Issue07 -> "grognak", not "issue07".
    base = re.sub(r"_(Issue|Part|Vol|No)\s*\d+$", "", base, flags=re.IGNORECASE)
    parts = [p for p in re.split(r"[_\W]+", base) if p]
    return parts[-1].lower().rstrip("s") if parts else ""

def _topic_path(edid):
    """Everything before the topic token — the shared 'folder' two related
    records live in. Used to stop a loose token prefix pairing rows from
    completely different content."""
    base = re.sub(r"_(Issue|Part|Vol|No)\s*\d+$", "", edid_base(edid), flags=re.IGNORECASE)
    parts = [p for p in re.split(r"[_\W]+", base) if p]
    return "_".join(parts[:-1]).lower()

_meta_by_topic = defaultdict(list)
for _base, _meta in meta_map.items():
    _meta_by_topic[_topic_token(_meta["edid"])].append(_meta)

_orphans_paired = 0
for item in all_items:
    # Cut rows belong on the cut page only — never nested into a live META.
    if not item["sub_candidate"] or item["is_meta"] or item["is_cut"]:
        continue
    # Skip anything ALREADY parented by pass 1 or pass 2. Testing
    # edid_base in meta_map only catches pass 1, so a row paired by the
    # prefix pass got appended to a second META as well — that is what
    # doubled the magazine issue counts (13 subs rendering as 26).
    if item.get("parent_edid"):
        continue
    tok = _topic_token(item["edid"])
    candidates = _meta_by_topic.get(tok, [])
    if not candidates and len(tok) >= 5:
        # A META may spell the series out in full where its subs abbreviate:
        #   META  ..._Collect_Magazines_GrognakTheBarbarian_META
        #   SUBs  ..._Collect_Magazines_Grognak_Issue01..10
        # Accept a META whose topic token STARTS WITH the sub's — but ONLY if
        # the two also sit under the same leading EDID path. Without that, a
        # generic verb matches anything: 36 ATX_DE*_Challenge_*_Complete rows
        # (token "complete") were pairing into the Burning Springs
        # Challenge_Lifetime_BurnBounty_CompletedHuntLocation_META, giving it
        # 44 subs against a required count of 8.
        # Bidirectional: the META may be the longer name (GrognakTheBarbarian
        # vs Grognak_IssueNN) OR the shorter one — Challenge_Lifetime_Discover_
        # AshHeap_META parents Challenge_Lifetime_Discover_AshHeapRegion_SUB_*,
        # where the sub carries the extra "Region". Both directions still
        # require the same leading EDID path, which is what stops a generic
        # verb from matching unrelated content.
        pref = [m for t, ms in _meta_by_topic.items()
                if (t.startswith(tok) or tok.startswith(t)) and len(t) >= 5
                for m in ms if _topic_path(m["edid"]) == _topic_path(item["edid"])]
        if len(pref) == 1:
            candidates = pref
    if (len(candidates) == 1 and candidates[0] is not item
            and same_frequency(item, candidates[0])):
        candidates[0]["children"].append(item)
        item["parent_edid"] = candidates[0]["edid"]
        _orphans_paired += 1

# ---- Resolve is_sub now that pairing is done -------------------------
# A row is a sub if its EDID says so, or if a parent was actually found.
# The wide ENAM net is dropped here: a row that merely carried the default
# "Sub Challenge (Unsorted)" category and never found a parent is an ordinary
# root challenge and must NOT wear a SUB pill.
_demoted = 0
for item in all_items:
    resolved = is_sub_edid(item["edid"]) or bool(item.get("parent_edid"))
    if item["sub_candidate"] and not resolved:
        _demoted += 1
    item["is_sub"] = resolved
    item.pop("sub_candidate", None)

# ---- Childless METAs are not METAs to the reader --------------------
# Some records are EDID-marked _META but ship no sub-challenges at all —
# the five regional Photomode ones ("Use Photomode around the Ash Heap
# Region", 20 required) are plain counting challenges gated by a location
# condition. Keep is_meta as the record-level truth (meta_map and the
# Technical block rely on it) but flag whether it actually parents anything,
# so the renderer can withhold the META pill and the "META" type label from a
# row with nothing beneath it.
_childless_metas = 0
for item in all_items:
    item["has_subs"] = bool(item.get("children"))
    if item["is_meta"] and not item["has_subs"]:
        _childless_metas += 1
print(f"  METAs with no sub-challenges (no META pill): {_childless_metas}")

def is_nested_sub(item):
    """True when this row is rendered INSIDE a META's Sub-challenges expand,
    so it must not also appear as a root row on the page."""
    if item["is_meta"]:
        return False
    return bool(item.get("parent_edid"))

print(f"  Total items: {len(all_items)}")
print(f"  META parents: {len(meta_map)}")
print(f"  SUBs paired by base prefix: {_prefix_paired}")
print(f"  Orphan SUBs paired by topic token: {_orphans_paired}")
print(f"  ENAM-only 'subs' demoted to root rows (no SUB pill): {_demoted}")
_still_loose = sum(1 for i in all_items
                   if i["is_sub"] and not i["is_meta"] and not i["is_cut"]
                   and not i.get("parent_edid"))
print(f"  EDID-marked SUB rows with no META (render as root rows): {_still_loose}")

# ==================================================================
# Bucket items
# ==================================================================

buckets = defaultdict(list)
for item in all_items:
    if is_nested_sub(item):
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
    # Burning Springs is a content pack with its own page. Without this entry
    # its 133 lifetime challenges fell through to the generic "world" page.
    "Burning Springs": "springs",
    "Fishing": "fishing",
}

def topic_page(item):
    # Pioneer Scouts badges have their own checklists:
    #   /df/scouts/possum/checklist/   /df/scouts/tadpole/checklist/
    # They were 357 of the 592 rows on /df/challenges/world/ (61%).
    track = scout_track(item.get("edid"))
    if track:
        return f"scouts-{track}"
    return _topic_page_inner(item)

def _topic_page_inner(item):
    """A topic page owns EVERY challenge about its subject regardless of
    frequency, so /df/challenges/bobbleheads/ carries the Daily and Weekly
    bobblehead challenges alongside the Lifetime ones and can render the full
    Daily → Weekly → Event → Lifetime panel set. Returns None when the row has
    no explicit topic, in which case it routes by frequency instead."""
    edid = str(item.get("edid") or "").lower()
    full = str(item.get("full") or "").lower()
    if "bobblehead" in edid or "bobblehead" in full: return "bobbleheads"
    if "magazine" in edid or "magazine" in full: return "magazines"
    if "fish" in edid or "fish" in full: return "fishing"
    # Burning Springs content uses the "Burn_" DLC prefix as well as the
    # spelled-out region name; match both or the Daily/Weekly/Event rows and
    # everything named after a Burning Springs enemy or location go missing.
    if "burningsprings" in edid: return "springs"
    if edid.startswith("burn_") or "_burn_" in edid: return "springs"
    if str(item.get("classification") or "") == "Burning Springs": return "springs"
    return None

# Page slug → the ENAM category slug it represents, where the two differ.
PAGE_CATEGORY_ALIASES = {
    "springs": "burning-springs",
    # The scout badge pages are all ENAM "World"; a lone "Lifetime - World"
    # panel on a badges checklist is noise, so collapse it to "Lifetime".
    "scouts-possum": "world",
    "scouts-tadpole": "world",
}

def classify_lifetime_page(item):
    slug = topic_page(item)
    if slug:
        return slug
    enam = str(item.get("classification") or "")
    return CLASSIFICATION_TO_SLUG.get(enam, "world")

# ---- Fishing-page sub-grouping (Daily / Weekly / Lifetime / Event) ----
# Fishing challenges carry no CNAM scope, so every one lands in the
# "lifetime" bucket and routes onto the fishing page. The actual cadence
# lives in the EDID, so derive a display group from it. Mini-season fishing
# challenges are excluded from the fishing page — they belong on their own
# mini-season checklist page.

def is_mini_season_edid(edid):
    return bool(re.search(r"MiniSeason_?\d{4}", str(edid or ""), re.IGNORECASE))

def fishing_group(edid):
    e = str(edid or "")
    if re.search(r"ATX_DE\d{4}", e, re.IGNORECASE):  return "Event"
    if re.search(r"_Daily_", e, re.IGNORECASE):      return "Daily"
    if re.search(r"_Weekly_", e, re.IGNORECASE):     return "Weekly"
    return "Lifetime"

# ---- Pint-Sized Phantoms page (Slasher / SDOW content) ----
# The Slasher update ships its challenges under three EDID shapes:
#   SDOW_Challenge_Lifetime_Collect_SlasherClue_0*   — Slasher Mask collection
#   SCORE_Challenge_(Daily|Weekly)_..._SDOW_SQ01     — "Disturbed Graves" quest
#   SCORE_Challenge_(Daily|Weekly)_Kill_SlasherFan*  — Pint-Sized Phantom kills
# The kill set and the two PartyCrasher bounty rows name only "SlasherFan" /
# "PartyCrasher" in the EDID and link to the content through an SDOW_ form in
# their conditions, so match on EITHER the EDID or a condition reference —
# matching on EDID alone silently drops 17 of the 26 rows.
#
# Deliberately NOT matched: SCORE_Challenge_*_Seasonal_Kill_Lost_MN2_Quest_Mischief.
# Those target LostRace during Mischief Night and carry no SDOW reference; they
# belong to the Mischief Night mini-season, not this page.
#
# These challenges carry no CNAM scope, so without this they all fall through
# scope_bucket() into "lifetime" and land on /df/challenges/world/.

SDOW_EDID_RE = re.compile(r"SDOW", re.IGNORECASE)
SDOW_COND_RE = re.compile(r"\bSDOW_\w+", re.IGNORECASE)

def is_pint_sized_phantom(item):
    if SDOW_EDID_RE.search(str(item.get("edid") or "")):
        return True
    for cond in (item.get("conditions") or []):
        if SDOW_COND_RE.search(str(cond)):
            return True
    return False

def phantom_group(edid):
    e = str(edid or "")
    if re.search(r"Collect_SlasherClue", e, re.IGNORECASE): return "Slasher Masks"
    if re.search(r"_Daily_", e, re.IGNORECASE):             return "Daily"
    if re.search(r"_Weekly_", e, re.IGNORECASE):            return "Weekly"
    return "Other"

# ==================================================================
# Build pages dict
# ==================================================================

pages = {}

# Pint-Sized Phantoms claim their rows first so they appear on their own page
# only, the same way mini-season fishing challenges are held back from the
# fishing page. Cut rows still go to the cut page.
phantom_ids = set()
phantom_items = []
for item in all_items:
    if item["is_cut"] or not is_pint_sized_phantom(item):
        continue
    if is_nested_sub(item):
        continue
    item["group"] = phantom_group(item.get("edid"))
    phantom_items.append(item)
    phantom_ids.add(id(item))
if phantom_items:
    pages["pint-sized-phantoms"] = phantom_items

# Daily / Weekly / Event rows about an explicit topic (bobbleheads, magazines,
# fishing, Burning Springs) are routed to that topic page so it can show the
# full Daily → Weekly → Event → Lifetime panel set. Everything else routes by
# frequency to /df/challenges/daily|weekly|events/.
for bucket_key, freq_page in (("daily", "daily"), ("weekly", "weekly"), ("event", "events")):
    for item in buckets.get(bucket_key, []):
        if id(item) in phantom_ids: continue

        # Limited-time event challenges (ATX_DE2021-2025 Anniversary, Halloween,
        # Valentines, The Pitt, Rip Daring, BoS …) are NOT part of the
        # repeatable daily/weekly pool — they ran once, during one event, and
        # they already have a home on their seasonal-event page and on
        # /df/challenges/events/. Leaving them here duplicated 107 rows across
        # pages and dumped 14 identically-titled "ANNIVERSARY: Claim the
        # mystery item of the day" rows into the Daily list with nothing to
        # tell them apart. Same rule as mini-season fishing on the Fishing page.
        # The events page keeps them: that page IS the limited-time aggregate.
        # Match on the ATX_DE marker itself, not on detect_season_slug(), which
        # needs an event name between the year and "_Challenge" and so misses
        # the 7 generically-named ATX_DE2022_Challenge_Daily_* rows.
        if bucket_key in ("daily", "weekly") and ATX_EVENT_RE.search(str(item.get("edid") or "")):
            continue

        slug = topic_page(item)
        if slug == "fishing" and is_mini_season_edid(item.get("edid")):
            continue  # mini-season fishing lives on its own page
        pages.setdefault(slug or freq_page, []).append(item)

for item in buckets.get("lifetime", []):
    if id(item) in phantom_ids: continue
    slug = classify_lifetime_page(item)
    if slug == "fishing":
        # Mini-season fishing challenges live on their own page, not here.
        if is_mini_season_edid(item.get("edid")):
            continue
    # A page dedicated to one category shouldn't repeat it in the panel name —
    # /df/challenges/fishing/ gets "Lifetime", not "Lifetime - Fishing", and
    # /df/challenges/springs/ collapses "Lifetime - Burning Springs" the same way.
    if item["group"].startswith("Lifetime - "):
        cat_slug = slugify(item["group"][len("Lifetime - "):])
        if cat_slug == slug or PAGE_CATEGORY_ALIASES.get(slug) == cat_slug:
            item["group"] = "Lifetime"
    pages.setdefault(slug, []).append(item)
pages["cut"] = buckets.get("cut", [])
for slug, items in season_buckets.items():
    pages[f"season:{slug}"] = items

def _natural_key(text):
    """Split digits out so "Reach Level 4!" sorts before "Reach Level 10!".
    Plain alphabetical puts 10, 100 and 15 ahead of 4, which is how the
    Character page's 22-step level ladder ended up shuffled."""
    return [(1, int(t)) if t.isdigit() else (0, t)
            for t in re.split(r'(\d+)', str(text or "").lower()) if t != ""]

def sort_key(item):
    # Epic rows first, then natural name order, then by required count so
    # same-named tiers ("Collect Caps" 10/25/76/760) read smallest first.
    req = item.get("required")
    return (
        0 if item.get("is_epic") else 1,
        _natural_key(item.get("full")),
        req if isinstance(req, int) else 0,
    )

for key in pages:
    pages[key].sort(key=sort_key)
    # Sub-challenges inside a META are rendered in array order, so they need
    # the same sort — otherwise they arrive in raw TSV order.
    for _it in pages[key]:
        if _it.get("children"):
            _it["children"].sort(key=sort_key)

# ==================================================================
# QUEST / RANDOM ENCOUNTER PROCESSING
# ==================================================================

print("[quests] Processing QUEST TSV...")

# ---- EDID prefix → DLC/content label (for public-facing names) ----
DLC_LABELS = {
    "BS01": "Steel Dawn",
    "BS02": "Steel Reign",
    "BS_RE": "Steel Dawn / Steel Reign",
    "W05": "Wastelanders",
    "W05_RE": "Wastelanders",
    "Storm": "Skyline Valley",
    "Storm_RE": "Skyline Valley",
    "Burn": "Burning Springs",
    "Burn_RE": "Burning Springs",
    "MOON": "Moonshine Jamboree / Skyline Drive",
    "AC": "Atlantic City",
    "XPD": "Expeditions",
    "EN": "Enclave",
    "FS": "Free States",
    "MTN": "Top of the World",
    "MTR": "Fire Breathers",
    "BoS": "Brotherhood of Steel",
    "RS": "Responders",
    "RSVP": "Responders",
    "GHL": "Ghoulification",
    "P62": "Gleaming Depths",
    "COMP": "Companions",
    "NWOT": "Night of the Wendigo",
    "SHEL": "Shelters",
    "QDL": "Duchess Lessons",
    "TW": "Events",
    "RD01": "Gleaming Depths",
}

def get_dlc_label(edid):
    """Return a DLC/content pack label from the EDID prefix."""
    for prefix, label in sorted(DLC_LABELS.items(), key=lambda x: -len(x[0])):
        if edid.startswith(prefix):
            return label
    return ""

# ---- Random Encounter classification ----

# Map EDID region codes to region display names
REGION_CODES = {
    "KMK": "Skyline Valley", "CMB": "Cranberry Bog", "MP": "The Mire",
    "TS": "Toxic Valley", "DWD": "Savage Divide", "JM": "The Forest",
    "MJP": "Ash Heap", "MD": "Savage Divide", "MT": "Burning Springs",
    "BB": "The Forest", "SM": "Savage Divide", "PS": "The Forest",
    "CT": "Cranberry Bog", "GR": "Burning Springs", "GO": "Burning Springs",
    "RK": "Skyline Valley", "LD": "Burning Springs", "OB": "The Forest",
    "MOON": "Skyline Drive", "AF": "The Forest", "ZW": "Ash Heap",
    "JP": "The Forest", "BG": "Cranberry Bog",
}

RE_CATEGORY_MAP = {
    "Assault": "re-assault",
    "Camp": "re-camp",
    "Travel": "re-travel",
    "Object": "re-object",
    "Scene": "re-scene",
    "WhitespringAssault": "re-whitespring-external",
    "Mining": "re-object",  # mining encounters go with objects
}

def classify_random_encounter(edid):
    """Return (category_slug, region_name) or (None, None) if not an RE."""
    e = str(edid or "")
    # Strip DLC prefixes to get to RE_ part
    clean = re.sub(r"^(Burn|BS|W05|Storm|COMP)_", "", e)
    if not clean.startswith("RE_"):
        return None, None
    # Parse: RE_CategorySuffix
    after_re = clean[3:]  # everything after RE_
    # WhitespringAssault special case
    if after_re.startswith("WhitespringAssault"):
        return "re-whitespring-external", "Whitespring"
    # Standard categories
    for cat_name, slug in RE_CATEGORY_MAP.items():
        if after_re.startswith(cat_name):
            rest = after_re[len(cat_name):]
            # Extract region code (first 2-4 uppercase letters)
            m = re.match(r"([A-Z]{2,4})", rest)
            region = REGION_CODES.get(m.group(1), "") if m else ""
            # Special: Zetan encounters
            if "_Zetan" in e or after_re.startswith(cat_name + "_Zetan"):
                return "re-limited-invaders", "Limited Time"
            return slug, region
    return None, None

def classify_hub_re(edid):
    """Check if this is a Whitespring Refuge (Hub) random encounter."""
    return "HubRE" in str(edid or "")

def is_random_encounter(edid):
    """Quick check if EDID is any kind of random encounter."""
    e = str(edid or "")
    return ("_RE_" in e or e.startswith("RE_") or "HubRE" in e)

def is_template(edid, full):
    """Check if this is a template/test entry."""
    e = str(edid or "").upper()
    f = str(full or "")
    return ("TEMPLATE" in e or f.startswith("[Template") or
            f.startswith("TEMPLATE") or "_Test" in e and f.startswith("["))

# ---- Quest classification ----

# Quest types we include on the quest checklist
QUEST_TYPES_MAIN = {"Primary"}
QUEST_TYPES_SIDE = {"Side Quest"}
QUEST_TYPES_SECONDARY = {"Secondary"}
QUEST_TYPES_MISC = {"Miscellaneous"}
QUEST_TYPES_DAILY = {"Daily"}
QUEST_TYPES_EXPEDITION = {"Expedition"}

# Quest types that are NOT quests (events, activities)
QUEST_TYPES_SKIP = {"Event", "Public Event", "Module", "Server", "Caravan", "Daily Ops", "Raid"}

def is_internal_quest(edid, full):
    """Filter out internal/debug quests."""
    e = str(edid or "")
    f = str(full or "")
    if e.startswith(("zzz", "ZZZ", "CUT", "DEL", "test_", "Test_", "Debug")):
        return True
    if f.startswith("[") or not f.strip():
        return True
    if "Template" in e and ("Template" in f or f.startswith("[")):
        return True
    # Dialogue-only quests
    if "Dialogue" in e and ("Dialogue" in f or "Bot Dialogue" in f):
        return True
    return False

def is_expo_repeatable(edid, full):
    """Check if an Expedition is a repeatable Expo (not a questline)."""
    e = str(edid or "")
    f = str(full or "")
    # Actual questlines have "Mission" in EDID and specific story names
    # Templates and test missions
    if "Template" in e or "Test" in e:
        return True
    # The repeatable expedition missions have "Mission" but we include those
    # since user said include if unsure
    return False

def quest_line_prefix(edid):
    """Extract the quest line prefix from an EDID for grouping.
    E.g. BS01_MQ01_Trust -> BS01, W05_MQ_001P_Wayward -> W05_MQ,
    Storm_MQ01 -> Storm_MQ, COMP_Quest_Intro_Full_Beckett -> COMP_Beckett"""
    e = str(edid or "")
    # Companion quests group by NPC name
    if e.startswith("COMP_"):
        # Extract companion name: COMP_Quest_..._Beckett, COMP_RQ_..._Beckett
        m = re.search(r"_(Beckett|Astronaut|Raider|Scavenger|Wanderer|Hunter|Beggar)", e)
        if m:
            return f"COMP_{m.group(1)}"
        return "COMP_Radiant"
    # Standard pattern: PREFIX_MQ/SQ/etc
    m = re.match(r"^([A-Za-z]+\d{2})_", e)
    if m:
        return m.group(1)
    # Prefix without number: Storm_MQ, W05_MQ
    m = re.match(r"^([A-Za-z0-9]+_(?:MQ|SQ|MQS|MQR|MQA))", e)
    if m:
        return m.group(1)
    # Broader prefix
    m = re.match(r"^([A-Za-z]+\d*)_", e)
    if m:
        return m.group(1)
    return e

def quest_sort_order(edid):
    """Extract a numeric sort key from EDID for ordering quest parts."""
    e = str(edid or "")
    # Find MQ01, SQ02, MQ_001P, Lesson02 etc
    nums = re.findall(r"(\d+)", e)
    if nums:
        return int(nums[-1])
    return 0

# ---- Process QUEST rows ----

QUEST_LINE_NAMES = {
    # Main quest lines
    "76CharGen": "Vault 76 / Reclamation Day",
    "RS": "Responders",
    "RSVP": "Responders",
    "EN01": "Enclave: Bunker Buster",
    "EN02": "Enclave: One of Us",
    "EN05": "Enclave: Officer on Deck",
    "EN06": "Enclave: Race for the Presidency",
    "EN07": "Enclave: I Am Become Death",
    "FS01": "Free States: Early Warnings",
    "FS02": "Free States: Reassembly Required",
    "FS03": "Free States: Coming to Fruition",
    "MTNL01": "Top of the World: Key To the Past",
    "MTNM01": "Top of the World: Flavors of Mayhem",
    "MTNS01": "Top of the World: Signal Strength",
    "MTN": "Top of the World",
    "MTR01": "Fire Breathers",
    "MTR06": "Fire Breathers: Into the Fire",
    "BoS": "Brotherhood of Steel (Legacy)",
    "BS01": "Steel Dawn",
    "BS02": "Steel Reign",
    "W05_MQ": "Wastelanders: Main",
    "W05_MQS": "Wastelanders: Foundation",
    "W05_MQR": "Wastelanders: Crater",
    "W05_MQA": "Wastelanders: Secrets Revealed",
    "Storm_MQ": "Skyline Valley",
    "AC_MQ": "Atlantic City: Main",
    "AC_SQ": "Atlantic City: Side Quests",
    "BURN_SQ": "Burning Springs",
    "Burn_MQ": "Burning Springs: Main",
    "Burn_SQ": "Burning Springs: Side",
    "GHL00": "Ghoulification",
    "P62": "Gleaming Depths",
    "QDL": "Duchess Lessons",
    "XPD_Pitt": "The Pitt",
    "XPD_AC": "Atlantic City Expeditions",
    "XPD_RTTP": "The Pitt Expeditions",
    "COMP_Beckett": "Ally: Beckett",
    "COMP_Astronaut": "Ally: Commander Daguerre",
    "COMP_Radiant": "Companion: Radiant Quests",
    "COMP_Raider": "Ally: Raider Punk",
    "COMP_Scavenger": "Ally: Scavenger",
    "COMP_Wanderer": "Ally: Settler Wanderer",
    "COMP_Hunter": "Ally: Hunter",
    "COMP_Beggar": "Ally: Beggar",
    "NWOT": "Night of the Wendigo",
    "SHEL": "Shelters",
    "MQ": "Main Quest: Overseer",
    "GQ": "Global Quests",
    "MILE": "Milestones",
    "MTR04": "Daily: Camden Park",
    "Moon_SQ": "Moonshine Jamboree: Costa Business",
    "MOON_SQ": "Moonshine Jamboree: Costa Business",
    "NPE": "New Player Quests",
    "Fishing": "Fishing",
    "RD01": "Gleaming Depths Raid",
}

def get_quest_line_name(prefix):
    """Get a human-friendly quest line name from the prefix."""
    if prefix in QUEST_LINE_NAMES:
        return QUEST_LINE_NAMES[prefix]
    # Try shorter prefixes
    for length in range(len(prefix), 1, -1):
        short = prefix[:length]
        if short in QUEST_LINE_NAMES:
            return QUEST_LINE_NAMES[short]
    # Fallback: humanize the prefix
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", prefix)
    name = name.replace("_", " ").strip()
    return name or prefix


quest_items = []         # for quest checklist pages
daily_quest_items = []   # for daily pipboy quests page
encounter_items = []     # for random encounter pages

re_pages = defaultdict(list)   # slug -> list of encounter items
quest_groups = defaultdict(list)  # quest_line_prefix -> list of quest items

for row in QUEST:
    edid = pick(row, "EDID")
    full = pick(row, "FULL - Name", "FULL")
    desc = pick(row, "DESC - Description", "DESC")
    quest_type = pick(row, "Quest Type")
    location = pick(row, "LNAM - Location", "LNAM")

    # Skip empty/internal/template
    if is_internal_quest(edid, full):
        continue
    if is_template(edid, full):
        continue
    if is_cut(edid):
        continue

    # Resolve rewards from GMRWRef columns
    rewards = []
    for i in range(10):
        ref = pick(row, f"GMRWRef{i}")
        if ref:
            fid = ref.split(":")[0]
            for label in get_rewards_for_gmrw(fid):
                if label not in rewards:
                    rewards.append(label)

    dlc_label = get_dlc_label(edid)

    item = {
        "form_id": pick(row, "FormID"),
        "edid": edid,
        "full": full,
        "desc": desc,
        "quest_type": quest_type,
        "location": location,
        "rewards": rewards,
        "reward_unknown": len(rewards) == 0,
        "dlc_label": dlc_label,
        "is_cut": False,
        "record_type": "quest",  # quest or encounter
    }

    # ---- Random Encounters ----
    if is_random_encounter(edid):
        # Hub RE → Whitespring Refuge
        if classify_hub_re(edid):
            item["record_type"] = "encounter"
            item["encounter_category"] = "re-whitespring-refuge"
            item["encounter_region"] = "Whitespring Refuge"
            item["has_vendor_inventory"] = False
            re_pages["re-whitespring-refuge"].append(item)
            continue

        cat, region = classify_random_encounter(edid)
        if cat:
            item["record_type"] = "encounter"
            item["encounter_category"] = cat
            item["encounter_region"] = region
            # Travel and Camp encounters may have vendor NPCs
            item["has_vendor_inventory"] = cat in ("re-travel", "re-camp")
            item["vendor_inventory"] = []  # placeholder for LVLI data
            re_pages[cat].append(item)
            continue

    # ---- Skip event/activity types ----
    if quest_type in QUEST_TYPES_SKIP:
        continue

    # ---- Daily quests → daily pipboy page ----
    if quest_type in QUEST_TYPES_DAILY:
        item["record_type"] = "quest"
        daily_quest_items.append(item)
        continue

    # ---- Expedition quests ----
    if quest_type in QUEST_TYPES_EXPEDITION:
        if is_expo_repeatable(edid, full):
            continue
        item["record_type"] = "quest"
        prefix = quest_line_prefix(edid)
        quest_groups[prefix].append(item)
        quest_items.append(item)
        continue

    # ---- Main / Side / Secondary / Misc quests ----
    if quest_type in (QUEST_TYPES_MAIN | QUEST_TYPES_SIDE | QUEST_TYPES_SECONDARY | QUEST_TYPES_MISC):
        item["record_type"] = "quest"
        prefix = quest_line_prefix(edid)
        quest_groups[prefix].append(item)
        quest_items.append(item)
        continue

    # ---- "None" type that isn't RE — skip (dialogue, creature, etc.) ----

# Sort quest groups internally by EDID sort order
for prefix in quest_groups:
    quest_groups[prefix].sort(key=lambda q: quest_sort_order(q["edid"]))

# Sort encounter pages by name
for slug in re_pages:
    re_pages[slug].sort(key=lambda e: str(e.get("full") or "").lower())

# Sort daily quests by name
daily_quest_items.sort(key=lambda q: str(q.get("full") or "").lower())

# ---- Build quest checklist page structure ----
# Group quests by type then by quest line

def build_quest_page_groups(quest_type_set, items):
    """Build groups for a quest type."""
    groups = []
    seen_prefixes = set()
    for item in items:
        if item["quest_type"] not in quest_type_set:
            continue
        prefix = quest_line_prefix(item["edid"])
        if prefix in seen_prefixes:
            continue
        seen_prefixes.add(prefix)
        group_items = quest_groups.get(prefix, [])
        # Only include items of the right type in this group
        typed_items = [q for q in group_items if q["quest_type"] in quest_type_set]
        if typed_items:
            groups.append({
                "group_name": get_quest_line_name(prefix),
                "group_prefix": prefix,
                "dlc_label": typed_items[0].get("dlc_label", ""),
                "items": typed_items,
            })
    return groups

quest_page_structure = {
    "main_questlines": build_quest_page_groups(QUEST_TYPES_MAIN, quest_items),
    "side_quests": build_quest_page_groups(QUEST_TYPES_SIDE, quest_items),
    "secondary_quests": build_quest_page_groups(QUEST_TYPES_SECONDARY, quest_items),
    "miscellaneous_quests": build_quest_page_groups(QUEST_TYPES_MISC, quest_items),
    "expedition_quests": build_quest_page_groups(QUEST_TYPES_EXPEDITION, quest_items),
}

# ---- Build encounter page structure ----
# Whitespring page gets two sections: External + The Refuge

whitespring_page = {
    "external": re_pages.get("re-whitespring-external", []),
    "the_refuge": re_pages.get("re-whitespring-refuge", []),
}

encounter_page_structure = {
    "re-assault": {"title": "Assault Encounters", "items": re_pages.get("re-assault", [])},
    "re-camp": {"title": "CAMP Encounters", "items": re_pages.get("re-camp", [])},
    "re-travel": {"title": "Travel Encounters", "items": re_pages.get("re-travel", [])},
    "re-object": {"title": "Object Encounters", "items": re_pages.get("re-object", [])},
    "re-scene": {"title": "Scene Encounters", "items": re_pages.get("re-scene", [])},
    "re-whitespring": {"title": "Whitespring Random Encounters", "sections": whitespring_page},
    "re-limited-invaders": {"title": "Invaders from Beyond", "items": re_pages.get("re-limited-invaders", [])},
}

print(f"  Quest items: {len(quest_items)}")
print(f"  Daily pipboy quests: {len(daily_quest_items)}")
print(f"  Quest line groups: {len(quest_groups)}")
for qt_key, groups in quest_page_structure.items():
    total = sum(len(g["items"]) for g in groups)
    print(f"    {qt_key}: {len(groups)} groups, {total} quests")
print(f"  Encounter pages:")
for slug, data in encounter_page_structure.items():
    if "sections" in data:
        ext = len(data["sections"].get("external", []))
        ref = len(data["sections"].get("the_refuge", []))
        print(f"    {slug}: External={ext}, The Refuge={ref}")
    else:
        print(f"    {slug}: {len(data.get('items', []))} encounters")


# ==================================================================
# Output
# ==================================================================

print("[challenges] Writing JSON...")
page_meta = {}
for key, items in pages.items():
    page_meta[key] = {"count": len(items), "has_meta": any(it["is_meta"] for it in items)}

# Add quest and encounter page meta
page_meta["quest:fallout-76-quests-checklist"] = {
    "count": len(quest_items),
    "has_quest_groups": True,
}
page_meta["quest:daily-pipboy-quests"] = {
    "count": len(daily_quest_items),
}
for slug, data in encounter_page_structure.items():
    if "sections" in data:
        count = sum(len(v) for v in data["sections"].values())
    else:
        count = len(data.get("items", []))
    page_meta[f"encounter:{slug}"] = {"count": count}

output = {
    "generated": "build_challenges_json.py v4",
    "pages": {k: {"items": v} for k, v in pages.items()},
    "page_meta": page_meta,
    "season_slugs": sorted(season_buckets.keys()),
    "patch_log": [],
    # Quest pages
    "quest_pages": {
        "fallout-76-quests-checklist": quest_page_structure,
        "daily-pipboy-quests": {"items": daily_quest_items},
    },
    # Random encounter pages
    "encounter_pages": encounter_page_structure,
}

out_path = DIST_DIR / "challenges.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(copy.deepcopy(output), f, ensure_ascii=False, indent=2)

print(f"[challenges] Written: {out_path} ({out_path.stat().st_size:,} bytes)")
for k, v in sorted(page_meta.items()):
    print(f"    {k}: {v['count']} items" if 'count' in v else f"    {k}: {v}")

# Generate patchlog feed
# Challenges structure has pages with items, plus quest_pages and encounter_pages
# Extract all challenge items from all pages for diffing
def extract_all_challenges(data):
    if not data:
        return []
    items = []
    # Main challenge pages
    pages = data.get('pages', {})
    for page_name, page_data in pages.items():
        if isinstance(page_data, dict):
            items.extend(page_data.get('items', []))
    # Quest pages
    quest_pages = data.get('quest_pages', {})
    for page_name, page_data in quest_pages.items():
        if isinstance(page_data, dict):
            items.extend(page_data.get('items', []))
    # Encounter pages
    encounter_pages = data.get('encounter_pages', {})
    for page_name, page_data in encounter_pages.items():
        if isinstance(page_data, dict):
            items.extend(page_data.get('items', []))
    return items

dist_base = str(DIST_DIR.parent)  # Go up one level to dist/
prev_json = _git_show_json('HEAD^', str(out_path))

entry = diff_item_lists(
    prev_items=extract_all_challenges(prev_json),
    curr_items=extract_all_challenges(output),
    key_field='form_id',
    name_field='full,edid',
    compare_fields=['scope', 'classification', 'required', 'rewards'],
)
feed = {"entries": [entry]}
feed_path = os.path.join(dist_base, 'patchlog_latest_df_challenges.json')
_write_json(feed_path, feed)
a, r, c = len(entry["added"]), len(entry["removed"]), len(entry["changed"])
print(f"[patchlog] patchlog_latest_df_challenges.json: current={entry['current']}  added={a}  removed={r}  changed={c}")
print("[challenges] Done.")
