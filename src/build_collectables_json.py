#!/usr/bin/env python3
"""
Build collectables JSON dist files for Fallout 76.

Takes xEdit TSV exports and builds JSON distributions for:
- Bobbleheads (from ALCH + GLOB)
- Plushies (from KYWD_Refs TSV + MISC)
- Notes (from BOOK)
- Manifest (summary counts)

Plushie data comes from the normalized KYWD_Export_*_Refs.tsv produced by the
split xEdit KYWD export script (ExportKYWDToTSV.pas).

Usage:
    python build_collectables_json.py --tsv-root <path> --seasons <path> --outdir <path>
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from html.parser import HTMLParser
from html import unescape

from patchlog_utils import write_patchlog_feed, _git_show_json, diff_item_lists, _write_json


class HTMLStripper(HTMLParser):
    """Strip HTML tags and preserve text content."""
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, data):
        self.fed.append(data)

    def handle_starttag(self, tag, attrs):
        if tag in ('br', 'p'):
            self.fed.append('\n')

    def handle_endtag(self, tag):
        if tag == 'p':
            self.fed.append('\n')

    def get_data(self):
        return ''.join(self.fed)


def strip_html_to_text(html):
    """
    Strip HTML tags from text, decode entities, preserve and restore line breaks.

    The xEdit TSV export (CleanCell) replaces actual newlines with spaces,
    so multiple consecutive spaces in the DESC are used as paragraph/line breaks
    by the game engine. We restore these to real newlines for display.
    """
    if not html:
        return ""

    # Replace common HTML block tags with newlines before stripping
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</p>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'<p[^>]*>', '\n', html, flags=re.IGNORECASE)

    # Strip all remaining HTML tags (font, size, etc.)
    text = re.sub(r'<[^>]+>', '', html)

    # Decode HTML entities
    text = unescape(text)

    # The game engine uses multiple spaces as paragraph separators
    # (because actual newlines were flattened by CleanCell in xEdit export).
    # Convert 2+ consecutive spaces to a single newline.
    text = re.sub(r'  +', '\n', text)

    # Clean up each line
    lines = text.split('\n')
    lines = [line.strip() for line in lines]

    # Remove leading/trailing blank lines but keep intentional internal blank lines
    # (a blank line between paragraphs is meaningful)
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()

    text = '\n'.join(lines)

    return text


def read_tsv_rows(path):
    """
    Read all rows from a TSV file.
    Returns list of dicts with FormID/EDID/FULL aliasing.
    Uses UTF-8 with error handling to be fast.
    """
    rows = []
    try:
        # Try UTF-8 first (fastest, usually works)
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                rows.append(row)
        return rows
    except Exception:
        pass

    # Fallback to latin1 (accepts all bytes)
    try:
        with open(path, 'r', encoding='latin1') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                rows.append(row)
        return rows
    except Exception as e:
        raise RuntimeError(f"Could not read {path}: {e}")


def starts_cut(edid):
    """
    Check if EDID indicates a cut/deleted item.
    """
    CUT_PREFIXES = ("DEL", "POST", "CUT", "ZZZ", "ZZZZ")
    CUT_SUFFIXES = ("_COPY01",)

    if not edid:
        return False

    e = edid.strip().upper()

    if any(e.startswith(p) for p in CUT_PREFIXES):
        return True

    if any(e.endswith(s) for s in CUT_SUFFIXES):
        return True

    return False


def starts_cut_notes(edid):
    """
    Check if EDID indicates a cut note, with additional CUT infixes,
    dev/test/template prefixes, and an explicit list of known cut EDIDs
    whose naming convention doesn't follow the standard patterns.
    """
    if starts_cut(edid):
        return True

    e = (edid or "").strip()
    eu = e.upper()

    # Additional infixes for notes
    if "_CUT_" in eu or "BURN_CUT_" in eu or "ZZZBURN_" in eu:
        return True

    # Dev/test/debug/template prefixes — these are Bethesda internal items
    if (eu.startswith("TEMPLATE_") or eu.startswith("TEST") or
            eu.startswith("DEBUG")):
        return True

    # _Unused suffix
    if eu.endswith("_UNUSED"):
        return True

    # Note: _OLD suffix is NOT checked as a blanket rule because some _OLD
    # EDIDs are the only version (e.g. MTR06_StudyMaterials_FirstAid_OLD
    # is the Fire Breathers study note — 1 ref, no replacement exists).
    # Specific confirmed-cut _OLD items go in _MANUAL_CUT_EDIDS instead.

    # COPY0000 suffix (xEdit copy artifacts) — but not DUPLICATE000 items
    # that are the only version of legitimate content
    if "COPY0000" in eu or "COPY00" in eu:
        # Exclude known legitimate items that happen to have COPY in their EDID
        if e not in _MANUAL_CUT_EXCLUSIONS:
            return True

    # Explicit list of known cut notes whose EDIDs don't follow standard
    # CUT naming. Verified as cut from the game (no valid REFR placement,
    # not obtainable in-game).
    if e in _MANUAL_CUT_EDIDS:
        return True

    return False


# EDIDs with COPY/DUPLICATE that are actually the ONLY version of
# legitimate in-game content — do NOT mark as cut
_MANUAL_CUT_EXCLUSIONS = {
    "E08B_Note_Rads_2DUPLICATE000",          # Crater Observations 2 — only version
    "LC044_JeremiahWardNote01DUPLICATE000",   # Last Will and Testament — only version, 3 refs
}

_MANUAL_CUT_EDIDS = {
    # Original manual cut list
    "Morgantown_Lore_MartialLawNotice",       # Public Notice - Martial Law
    "Morgantown_Lore_RallyNotice",            # Rally for Justice!
    "Morgantown_Lore_RallyFlyer",             # Rally Flyer
    # Skyline Valley dev notes
    "76StormCDB_TestNote",                    # "My Test Note" — CDB test
    "Storm_PresidentalNote",                  # "President Test Note" — rendering test
    "Storm_Test_EasyReadStressTest",          # UI stress test, 0 refs
    # Temp/placeholder notes
    "W05_RE_TempClueNote01",                  # "Clue 01" — dev placeholder, 0 refs
    "W05_RE_TempClueNote02",                  # "Clue 02" — dev placeholder, 0 refs
    # Superseded versions with _OLD suffix that didn't match the suffix rule
    "LC167_CEONoteOLD",                       # "Notebook 02 Test Note" — 0 refs
    "LC149_Jesse_White_Set_List_old",         # Superseded by LC149_Jesse_White_Set_List
    # Copy artifacts
    "BMO_BunkerNote03_MarketingMemoCOPY0000", # Duplicate of BMO_BunkerNote03_MarketingMemo, 0 refs
    # Blank / unused
    "Burn_AshCave_Unused",                    # "BlankNote" — explicitly unused, 0 refs
    # Watoga Underground placeholders (real lore content but 0 refs, never placed)
    "WU_HolotapePlaceholder01",               # "Alex's Observations 8" — Placeholder EDID, 0 refs
    "WU_HolotapePlaceholder02",               # "Alex's Observations 2" — Placeholder EDID, 0 refs
}


def is_alias_template_name(full):
    """
    Check if the FULL name is an unresolved alias template.
    Names like '<Alias=MissileSilo> Launch Code Solution' are runtime aliases
    that are meaningless in a static checklist — exclude them.
    """
    f = (full or "").strip()
    return f.startswith('<Alias=') or f.startswith('<alias=')


def can_note_be_collected(btof):
    """
    Parse the BTOF (Book Take Flags) field to determine collectability.

    BTOF = 0 (or empty, or all-zero string) → note can be picked up.
    BTOF = 1 or 01 → note is a fixed environment prop, cannot be taken.

    The xEdit export may represent the flag as:
      - '' (empty) — no flag set
      - '1' or '01' — Can't be Taken flag set
      - A 64-character binary/hex zero string — flags = 0 (can be taken)
    """
    b = (btof or '').strip()
    if not b:
        return True

    # Try decimal integer
    try:
        return int(b) == 0
    except ValueError:
        pass

    # Try hexadecimal (handles '01', long hex strings)
    try:
        return int(b, 16) == 0
    except ValueError:
        pass

    # Unknown format — assume collectable
    return True


def derive_location_from_edid(edid):
    """
    Fallback: derive a readable location hint from the EDID when no
    REFR cell name is available in the refs data.

    Strips common quest/lore prefixes and splits CamelCase.
    e.g. 'W05_Lore_FS_AutomatedSwitch' -> 'FS Automated Switch'
    """
    if not edid:
        return "Unknown Location"

    e = edid

    # Strip common leading quest/prefix tokens (longest first to avoid partial matches)
    STRIP_PREFIXES = [
        'W05_Lore_', 'EN07_Lore_', 'MQ_Lore_',
        'W05_', 'EN07_', 'MQ_', 'SB_', 'BS_',
    ]
    for prefix in STRIP_PREFIXES:
        if e.startswith(prefix):
            e = e[len(prefix):]
            break

    # Split on underscores
    parts = e.split('_')

    # Insert space before uppercase letters that follow lowercase letters (CamelCase split)
    result_parts = []
    for part in parts:
        spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', part)
        # Also split sequences like 'Silo02' -> 'Silo 02'
        spaced = re.sub(r'([A-Za-z])(\d)', r'\1 \2', spaced)
        result_parts.append(spaced)

    return ' '.join(result_parts)


# Test cell name patterns (match PAS IsTestCellName)
_TEST_CELL_PATTERNS = re.compile(
    r'quick\s+test|test\s+cell|test\s+combat|test\s+outsource|test\s+muck'
    r'|debugmatt|76\s+qa|^qa$|dev\s+room',
    re.IGNORECASE
)

def is_test_cell_name(name):
    """Return True if the location name looks like a developer test/debug cell."""
    if not name:
        return False
    n = name.strip()
    if re.search(r'(?i)(test|debug)', n):
        return True
    return bool(_TEST_CELL_PATTERNS.search(n))


# Exterior cell EDID suffixes to strip before CamelCase splitting
_EXT_SUFFIX = re.compile(r'(?i)Ext\d*([NSEW]{1,2}\d*)?$|\d+[NSEW]{0,2}$')

def parse_ext_cell_location(edid):
    """
    Convert a raw CELL EDID (exterior cell) into a human-readable location name.

    e.g. 'ClarksburgExt05'       -> 'Clarksburg'
         'MonongahExt01NE'       -> 'Monongah'
         'SavageDivideExt03'     -> 'Savage Divide'
         'CampMcClintockExt04'   -> 'Camp McClintock'
         'AppalachiaExt01'       -> ''  (generic worldspace, returns empty)
         '[CELL:00050B2C]'       -> ''  (no EDID available)
    """
    if not edid:
        return ''

    e = edid.strip()

    # Raw FormID bracket string — no EDID available for this cell
    if e.startswith('['):
        return ''

    # Strip trailing direction/number/Ext suffixes (repeat up to 4 times for compound suffixes)
    for _ in range(4):
        prev = e
        e = _EXT_SUFFIX.sub('', e).rstrip('_').strip()
        if e == prev:
            break

    if not e:
        return ''

    # CamelCase split — preserve Mc/Mac prefixes (McClintock, MacDonald)
    e = re.sub(r'([a-z])([A-Z])', r'\1 \2', e)
    e = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', e)
    # Rejoin "Mc X" / "Mac X" that got split
    e = re.sub(r'\b(Mc|Mac) ([A-Z])', r'\1\2', e)
    e = re.sub(r'  +', ' ', e).strip()

    # Drop generic worldspace names that don't help the user
    if e.lower() in ('appalachia', 'wasteland', 'commonwealth', 'commonwealth exterior'):
        return ''

    return e


def load_book_locations(path):
    """
    Load BOOK_*_Locations.tsv and return a dict:
      { FormID -> {loc_name, loc_source, quest_name} }
    """
    result = {}
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                fid = row.get('BOOK_FormID', '').strip()
                if not fid:
                    continue
                result[fid] = {
                    'loc_name':   row.get('LocationName',   '').strip(),
                    'loc_source': row.get('LocationSource', '').strip(),
                    'quest_name': row.get('QuestName',      '').strip(),
                }
    except Exception as e:
        print(f"  WARNING: Could not load locations file: {e}", file=sys.stderr)
    return result


def resolve_note_location(formid, edid, locations):
    """
    Resolve the best display location string for a note.

    Priority:
      1. Locations TSV (physical REFR placement)
         - ExtCell source  -> parse CELL EDID -> readable name
                              -> "Not near a named location" if no useful parse
         - CellFULL/NameParse -> use directly unless it's "Appalachia"
         - "Appalachia"    -> "Not near a named location"
      2. Quest-given (QUST ref, no physical location)
         -> "Quest: <quest name>"
      3. Fallback: derive from EDID (CamelCase split, prefix stripping)
    """
    loc_data = locations.get(formid) if locations else None

    if loc_data:
        loc_name   = loc_data['loc_name']
        loc_source = loc_data['loc_source']
        quest_name = loc_data['quest_name']

        if loc_source == 'ExtCell':
            parsed = parse_ext_cell_location(loc_name)
            return parsed if parsed else "Not near a named location"

        if loc_source in ('CellFULL', 'NameParse', 'CellEDID'):
            if not loc_name or loc_name.lower() in ('appalachia',):
                pass  # fall through to quest / EDID fallback
            elif not is_test_cell_name(loc_name):
                return loc_name
            # test cell name with no other data -> fall through

        # No usable physical location -> check quest
        if quest_name:
            return "Quest: " + re.sub(r'  +', ' ', quest_name).strip()

        # Physical location was empty or test cell
        if loc_name and not is_test_cell_name(loc_name) and loc_name.lower() != 'appalachia':
            return loc_name

    # Last resort: EDID-based derivation
    return derive_location_from_edid(edid)


def location_from_refs(row):
    """
    Parse the Ref columns to find the best physical location for a note.

    The updated xEdit script stores refs as: FormID:EDID:SIG:CellName
    For REFR records, CellName is the FULL name of the containing CELL
    (interior) or worldspace (exterior).

    Returns the first non-empty CellName found, or None.
    """
    try:
        ref_count = int(row.get('ReferencedByCount', 0) or 0)
    except (ValueError, TypeError):
        return None

    for k in range(1, ref_count + 1):
        ref_val = (row.get(f'Ref{k}', '') or '').strip()
        if not ref_val:
            continue

        # Format: FormID:EDID:SIG:CellName
        # Split on ':' — FormID is 8 hex chars, EDID has no colons,
        # SIG is 4 chars, CellName is the remainder.
        parts = ref_val.split(':', 3)  # max 4 parts

        if len(parts) < 3:
            continue

        sig = parts[2].strip().upper()
        if sig != 'REFR':
            continue

        if len(parts) >= 4:
            cell_name = parts[3].strip()
            if cell_name:
                return cell_name

    return None


def convert_duration_to_text(seconds):
    """
    Convert duration in seconds to human-readable text.
    Examples: 3600 -> "1 hour", 1800 -> "30 minutes"
    """
    seconds = float(seconds)

    if seconds == 0:
        return "0 seconds"

    if seconds >= 3600:
        hours = int(seconds // 3600)
        remainder = seconds % 3600
        if remainder == 0:
            if hours == 1:
                return "1 hour"
            else:
                return f"{hours} hours"
        else:
            minutes = int(remainder // 60)
            if minutes == 0:
                if hours == 1:
                    return f"1 hour"
                else:
                    return f"{hours} hours"
            else:
                if hours == 1:
                    return f"1 hour {minutes} minutes"
                else:
                    return f"{hours} hours {minutes} minutes"
    elif seconds >= 60:
        minutes = int(seconds // 60)
        remainder = seconds % 60
        if remainder == 0:
            if minutes == 1:
                return "1 minute"
            else:
                return f"{minutes} minutes"
        else:
            if minutes == 1:
                return f"1 minute {int(remainder)} seconds"
            else:
                return f"{minutes} minutes {int(remainder)} seconds"
    else:
        if seconds == 1:
            return "1 second"
        else:
            return f"{int(seconds)} seconds"


def seasons_map(seasons_path):
    """
    Read fallout76_seasons.tsv and return {season_number: season_name}.
    Uses UTF-8 with error replacement for speed.
    """
    seasons = {}
    try:
        with open(seasons_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                try:
                    season_num = int(row['SeasonNumber'])
                    season_name = row['SeasonName']
                    seasons[season_num] = season_name
                except (ValueError, KeyError):
                    pass
        return seasons
    except Exception:
        pass

    # Fallback to latin1
    try:
        with open(seasons_path, 'r', encoding='latin1') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                try:
                    season_num = int(row['SeasonNumber'])
                    season_name = row['SeasonName']
                    seasons[season_num] = season_name
                except (ValueError, KeyError):
                    pass
    except Exception:
        pass

    return seasons


def gmrw_parentquest_map(gmrw_rows):
    """
    Build a map from GMRW EDID token to "Event: EventName" label.
    Example: "MN2_Workshop_..." -> "Event: Moonshine Jamboree"
    """
    event_map = {}

    for row in gmrw_rows:
        edid = row.get('EDID', '').strip()
        full = row.get('ParentQuestDisplay', '').strip()

        if edid and full:
            # Extract prefix before underscore (e.g., MN2)
            match = re.match(r'^([A-Z0-9]+)', edid)
            if match:
                prefix = match.group(1)
                if prefix not in event_map:
                    event_map[prefix] = f"Event: {full}"

    return event_map


# ---------------------------------------------------------------------------
# Mystery Bobblehead Box — known obtain sources.
# Keys are EDID substrings found in the box's referenced-by LVLI records.
# Values are the human-readable labels shown in the "How to Obtain" box.
# The Drifter entry (P62_LLS_Drifter) is cut content and is intentionally
# excluded from this map.
# ---------------------------------------------------------------------------
_MBB_FORMID = '0072D4FC'

_MBB_REF_LABELS = {
    'MTRz05_LL_02_ProspectorMine':       'U Mine It Maps – Prospector',
    'MTRz05_LL_03_ExcavatorMine':        'U Mine It Maps – Excavator',
    'XPD_LLV_ExpeditionVendor_Giuseppe': 'Purchased from Giuseppe the Stamp Vendor',
    'SCORE_COEN_Utility_BobbleheadBox':  'Purchased from the Scoreboard with Tickets',
    'RD01_LLS_Raids_Rewards':            'Gleaming Depths Raid Rewards',
    'RA_LL_Rewards_PublicEvents':        'Public Event Rewards',
}

# Ordered display list used when no ref data is available in the TSV.
_MBB_FALLBACK_SOURCES = [
    'U Mine It Maps – Prospector',
    'U Mine It Maps – Excavator',
    'Purchased from Giuseppe the Stamp Vendor',
    'Gleaming Depths Raid Rewards',
    'Public Event Rewards',
    'Purchased from the Scoreboard with Tickets',
]


def _parse_mbb_sources_from_refs(row):
    """
    Try to read Mystery Bobblehead Box obtain sources from the ALCH TSV
    Refs_Flat / Ref_N columns.  Returns a list of label strings, or []
    if the columns are empty (common when the ALCH script does not export
    referenced-by data for this item).
    """
    refs_flat = row.get('Refs_Flat', '').strip().strip('"')
    sources = []
    seen = set()

    if refs_flat:
        edids = [r.strip() for r in refs_flat.split('|')]
    else:
        edids = []
        for i in range(1, 31):
            val = row.get(f'Ref_{i}', '').strip().strip('"')
            if val:
                # Format may be FormID:EDID:SIG or just EDID
                parts = val.split(':')
                edids.append(parts[1] if len(parts) >= 2 else parts[0])

    for edid in edids:
        for pattern, label in _MBB_REF_LABELS.items():
            if pattern.lower() in edid.lower() and label not in seen:
                seen.add(label)
                sources.append(label)
                break

    return sources


def build_bobbleheads(alch_path, glob_rows):
    """
    Build bobbleheads list from ALCH TSV + GLOB rows.
    Streams ALCH file to avoid OOM on large files.

    Each item carries:
      formId, edid, name, group, tradeable, mysteryBoxSources, isCut

    tradeable       -- True unless the NonPlayerTradable keyword is present.
    mysteryBoxSources -- ordered list of how to obtain the Mystery Bobblehead
                        Box (parsed from the box's ref columns, or fallback).

    Returns (groups, cut_items).
    groups: [{name, items: [...]}, ...]
    cut_items: [item, item, ...]
    """
    # Build GLOB lookup: FormID -> GLOB value
    print("  Building GLOB lookup...", file=sys.stderr)
    glob_lookup = {}
    for row in glob_rows:
        formid = row.get('FormID', '').strip()
        fltv = row.get('FLTV', '').strip()
        if formid and fltv:
            try:
                glob_lookup[formid] = float(fltv)
            except ValueError:
                pass

    # Stream ALCH file for bobbleheads
    print("  Streaming ALCH file...", file=sys.stderr)
    bobbleheads_live = []
    bobbleheads_cut = []
    mbb_sources = []  # populated if we find the Mystery Box row

    try:
        with open(alch_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            count = 0
            for row in reader:
                count += 1
                if count % 1000 == 0:
                    print(f"    Read {count} ALCH rows...", file=sys.stderr)

                edid   = row.get('ALCH_EDID', '').strip().strip('"') or row.get('EDID', '').strip().strip('"')
                formid = row.get('ALCH_FormID', '').strip().strip('"') or row.get('FormID', '').strip().strip('"')

                # Capture Mystery Bobblehead Box row for obtain-source parsing
                if formid.upper() == _MBB_FORMID.upper() or 'BobbleheadBox' in edid:
                    parsed = _parse_mbb_sources_from_refs(row)
                    mbb_sources = parsed if parsed else _MBB_FALLBACK_SOURCES
                    continue  # Box itself is not a collectible row

                if 'bobble' not in edid.lower():
                    continue

                full = row.get('FULL', '').strip().strip('"')

                is_cut = starts_cut(edid)

                # Tradeable: absence of NonPlayerTradable keyword
                kw_flat = row.get('Keywords_Flat', '').strip()
                tradeable = 'NonPlayerTradable' not in kw_flat

                # Determine group
                group = "Glowing Bobbleheads" if "Glowing" in edid else "Bobbleheads"

                item = {
                    "formId":   formid,
                    "edid":     edid,
                    "name":     full,
                    "group":    group,
                    "tradeable": tradeable,
                    "isCut":    is_cut,
                }

                if is_cut:
                    bobbleheads_cut.append(item)
                else:
                    bobbleheads_live.append(item)

    except Exception as e:
        print(f"  ERROR reading ALCH: {e}", file=sys.stderr)
        return [], []

    # If we never found the MBB row (e.g. filtered export), use fallback
    if not mbb_sources:
        mbb_sources = _MBB_FALLBACK_SOURCES

    # Attach MBB sources to every live item
    for item in bobbleheads_live:
        item["mysteryBoxSources"] = mbb_sources

    # Group live bobbleheads
    groups_dict = {}
    for item in bobbleheads_live:
        group_name = item['group']
        if group_name not in groups_dict:
            groups_dict[group_name] = []
        groups_dict[group_name].append(item)

    # Sort within each group
    for group_name in groups_dict:
        groups_dict[group_name].sort(key=lambda x: x['name'])

    # Build groups list in order
    groups = []
    for group_name in ["Bobbleheads", "Glowing Bobbleheads"]:
        if group_name in groups_dict:
            groups.append({
                "name": group_name,
                "items": groups_dict[group_name]
            })

    print(f"  MBB sources: {mbb_sources}", file=sys.stderr)
    return groups, bobbleheads_cut


def build_plushies(kywd_refs_rows, misc_rows, seasons, gmrw_pq_map):
    """
    Build plushies list from KYWD_Refs TSV + MISC rows.

    kywd_refs_rows: all rows from KYWD_Export_*_Refs.tsv
    misc_rows: all MISC rows
    seasons: {season_num: season_name}
    gmrw_pq_map: {prefix: "Event: Name"}

    Returns (live_items, cut_items).
    """
    PLUSHIES_KEYWORD = "006A57A2"

    # Filter refs TSV for PlushiesKeyword rows
    plushie_refs = [r for r in kywd_refs_rows
                    if r.get('KeywordFormID', '').strip().upper() == PLUSHIES_KEYWORD]

    if not plushie_refs:
        print(f"  WARNING: PlushiesKeyword ({PLUSHIES_KEYWORD}) not found in KYWD_Refs TSV",
              file=sys.stderr)
        return [], []

    print(f"  Found {len(plushie_refs)} PlushiesKeyword refs", file=sys.stderr)

    # Build MISC lookup: FormID -> {edid, full}
    misc_lookup = {}
    for row in misc_rows:
        formid = row.get('FormID', '').strip()
        edid = row.get('EDID', '').strip()
        full = row.get('FULL', '').strip()
        if formid:
            misc_lookup[formid] = {'edid': edid, 'full': full}

    # Extract MISC FormIDs from refs
    plushie_formids = []
    for ref in plushie_refs:
        fid = ref.get('RefFormID', '').strip()
        if fid and re.fullmatch(r'[0-9A-Fa-f]{8}', fid):
            plushie_formids.append(fid)

    # Build plushies
    plushies_live = []
    plushies_cut = []

    for plushie_fid in plushie_formids:
        if plushie_fid not in misc_lookup:
            continue

        misc_data = misc_lookup[plushie_fid]
        edid = misc_data['edid']
        full = misc_data['full']

        is_cut = starts_cut(edid)

        # Determine how to obtain
        how = None
        source = None

        if edid.startswith('ATX_'):
            how = "Can be purchased with certain bundles from the Atom Shop."
            source = "atomshop"
        elif re.match(r'^SCORE_S(\d+)_', edid):
            match = re.match(r'^SCORE_S(\d+)_', edid)
            if match:
                season_num = int(match.group(1))
                if season_num in seasons:
                    season_name = seasons[season_num]
                    how = f"Purchase with tickets from the {season_name} Scoreboard (Season {season_num})"
                else:
                    how = f"Purchase with tickets from the Season {season_num} Scoreboard"
                source = "scoreboard"
        elif edid.startswith('SCORE_MiniSeason_'):
            # Extract mini season name from EDID
            mini_name = edid.replace('SCORE_MiniSeason_', '').replace('_', ' ')
            how = f"Claim from the Mini Season - {mini_name}"
            source = "miniseason"
        elif edid.startswith('Community_'):
            how = "Awarded through a Bethesda community event or promotion."
            source = "community"
        elif edid.startswith('Fishing_') or 'Fishing' in edid:
            how = "Obtained through Fishing."
            source = "fishing"
        else:
            # Check if it's an event token (GMRW reference)
            match = re.match(r'^([A-Z0-9]+)_', edid)
            if match:
                prefix = match.group(1)
                if prefix in gmrw_pq_map:
                    how = gmrw_pq_map[prefix]
                    source = "event"

        # Check for craftable pattern
        if how is None and ('Craft' in edid or 'craft' in edid or 'COBJ' in edid):
            how = "Craft at a Workbench (requires plan)"
            source = "craftable"

        # Fallback
        if how is None:
            how = "How to obtain this plushie is unknown."
            source = "unknown"

        item = {
            "formId": plushie_fid,
            "edid": edid,
            "name": full,
            "how": how,
            "dropRate": "N/A",
            "source": source,
            "isCut": is_cut
        }

        if is_cut:
            plushies_cut.append(item)
        else:
            plushies_live.append(item)

    # Sort live plushies alphabetically
    plushies_live.sort(key=lambda x: x['name'])

    return plushies_live, plushies_cut


# =============================================================================
# NOTE_OVERRIDES — manual corrections applied after xEdit data is processed.
#
# Keys are note display names (FULL field). Supported patch fields:
#   location     — replaces the xEdit-resolved location string
#   canCollect   — overrides the BTOF-derived collectability boolean
#   technicalNote — extra note shown in the Technical sub-expand on the frontend
#
# Keyed by name rather than EDID because these corrections come from gameplay
# observation rather than xEdit inspection. If EDIDs are confirmed in future
# they should be used instead for robustness.
# =============================================================================

_GRAFTON_PAWN_NOTE = (
    "Can be initially collected from its delivery package, "
    "but once placed on the wall at Grafton Pawn Shop it can only be read."
)

NOTE_OVERRIDES = {
    # ----- Incorrect locations (xEdit resolved wrong cell) -----
    "Filtcher Farm Report": {
        "location": "Silva Homestead\n(In the package for Madeleine de Silva)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Holland Chase Invoice #9021": {
        "location": "Charleston Capitol Building\n(In the package for Sam Blackwell)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Holland Chase Invoice #9033": {
        "location": "Charleston Capitol Building\n(In the package for Sam Blackwell)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Shanghai Sally: Berkeley Springs": {
        "location": "Monongah Police Station\n(In the package for Sheriff Darcy)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Shanghai Sally: Casino Shootout": {
        "location": "Monongah Police Station\n(In the package for Sheriff Darcy)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Shanghai Sally: Chapter Closed": {
        "location": "Monongah Police Station\n(In the package for Sheriff Darcy)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Shanghai Sally: Conclusions": {
        "location": "Monongah Police Station — front desk\n(In the package for Sheriff Darcy)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Suspicious death at Harpers Ferry": {
        "location": "Van Lowe Taxidermy\n(In the package for Calvin van Lowe)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Suspicious death of Alicia Shay": {
        "location": "Van Lowe Taxidermy\n(In the package for Calvin van Lowe)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Suspicious death of Emmanuel Tillings": {
        "location": "Van Lowe Taxidermy\n(In the package for Calvin van Lowe)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Suspicious deaths overview": {
        "location": "Van Lowe Taxidermy\n(In the package for Calvin van Lowe)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Vigilant Citizen's note to Blackwell": {
        "location": "Charleston Capitol Building\n(In the package for Sam Blackwell)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Vigilant Citizen's note to Carter": {
        "location": "Charleston Herald building\n(In the package for Quinn Carter)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Vigilant Citizen's note to Leah de Silva": {
        "location": "Silva Homestead\n(In the package for Madeleine de Silva)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Vigilant Citizen's note to Sheriff Darcy": {
        "location": "Monongah Police Station\n(In the package for Sheriff Darcy)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Vigilant Citizen's note to Van Lowe": {
        "location": "Van Lowe Taxidermy\n(In the package for Calvin van Lowe)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    "Weigh station logs": {
        "location": "Charleston Herald building\n(In the package for Quinn Carter)",
        "technicalNote": _GRAFTON_PAWN_NOTE,
    },
    # ----- Location name changes (game updates renamed locations) -----
    "A Blessed Gift": {
        "location": "Sacramental Glade",
    },
    "A Worthy Sacrifice": {
        "location": "Sacramental Glade",
    },
    "A Life Without Keith": {
        "location": "The Coop",
    },
    "Doomed": {
        "location": "The Coop",
    },
    # Middle Mountain Cabins was renamed to Middle Mountain Pitstop in-game
    "Pitstop Note": {
        "location": "Middle Mountain Pitstop",
    },
    "Investigation Report": {
        "location": "Middle Mountain Pitstop",
    },
    "Letter To Vera": {
        "location": "Middle Mountain Pitstop",
    },
    "Rest In Peace": {
        "location": "Middle Mountain Pitstop",
    },
    "Weird Note": {
        "location": "Middle Mountain Pitstop",
    },
    "Brag's Note": {
        "location": "Middle Mountain Pitstop",
    },
    # Postcard from Elizabeth — xEdit resolves to VaultTecU cell but the note
    # is actually found in a destroyed house in Morgantown
    "Postcard from Elizabeth": {
        "location": "Morgantown",
    },
    # ----- Vague/incorrect locations (xEdit resolved nearby cell, not actual spawn) -----
    "A Father's Lament": {
        "location": "South River Bridge — on the edge of the bridge itself",
    },
    "Birthday Letter From Dad": {
        "location": "South River Bridge (inside the crashed bus)",
    },
    "Birthday Letter From Mom": {
        "location": "South River Bridge (inside the crashed bus)",
    },
    "A Job Opportunity": {
        "location": "Gilman Lumber Mill",
    },
    "Quartermaster's Report": {
        "location": "Gilman Lumber Mill",
    },
    "To Addie": {
        "location": "Gilman Lumber Mill",
    },
    "A failed dinner": {
        "location": "Old Danielson Cabin",
    },
    "Change of Plans": {
        "location": "Old Danielson Cabin",
    },
    "Hetty is loosing her mind": {
        "location": "Old Danielson Cabin",
    },
    # ----- Collectability fixes (BTOF incorrectly flags as non-collectible) -----
    "Adelaide Reminder": {
        "canCollect": True,
    },
}


def apply_note_overrides(items_live, items_cut):
    """
    Apply NOTE_OVERRIDES to notes after xEdit-derived data is built.
    Patches location, canCollect, and/or technicalNote in-place.
    Logs each override applied.
    """
    all_items = items_live + items_cut
    applied = 0
    for item in all_items:
        override = NOTE_OVERRIDES.get(item.get('name', ''))
        if not override:
            continue
        for field, value in override.items():
            item[field] = value
        applied += 1
        print(f"    Override applied: {item['name']!r}", file=sys.stderr)
    print(f"  Applied {applied} note overrides ({len(NOTE_OVERRIDES)} defined)", file=sys.stderr)


def _is_note_model(model):
    """
    Check if the Model field indicates a note/paper item.

    Notes in Fallout 76 use specific 3D models — this is a far more reliable
    identifier than checking for HTML tags in DESC, since many legitimate
    in-game notes use plain-text descriptions.

    Matches any model path containing 'note' (case-insensitive), which covers:
      - Props\\Note_LowPoly.nif, props/note_lowpoly.nif
      - props/note_classified.nif, props/note_classfied.nif (typo in data)
      - props/noteripped_lowpoly.nif, Props\\NoteRipped_LowPoly.nif
      - props/note_topsecret.nif, props/Note_Propaganda01.nif
      - Interface\\Note\\DotMatrixPage01.nif and other interface/note/ variants
      - interface/note/Postcard_LowPoly01.nif
      - interface/note/lowpoly_notepad01.nif
      - SetDressing\\DotMatrixPrinter\\DotMatrixPrinterNote01.nif
      - note01/note01.nif
    """
    return bool(model) and 'note' in model.lower()


def build_notes(book_path, locations=None):
    """
    Build notes list from BOOK TSV.
    Streams BOOK file to avoid OOM on large files.

    Identifies notes by their 3D Model field (any model path containing
    'note'), which is more reliable than the previous HTML-in-DESC check.
    Recipes/plans that happen to use a note model are excluded by EDID/FULL.

    Returns (live_items, cut_items).

    Location resolution priority:
      1. REFR ref CellName from updated xEdit export (FormID:EDID:REFR:CellName)
      2. Fallback: derive from EDID (CamelCase split, prefix stripping)

    canCollect:
      Derived from BTOF field. BTOF != 0 means the note is a fixed environment
      prop that cannot be picked up.
    """
    print("  Streaming BOOK file...", file=sys.stderr)
    notes_live = []
    notes_cut = []

    try:
        with open(book_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            count = 0
            for row in reader:
                count += 1
                if count % 5000 == 0:
                    print(f"    Read {count} BOOK rows...", file=sys.stderr)

                model = row.get('Model', '').strip()

                # Primary filter: Model field must be a note model.
                # This catches all notes regardless of whether DESC has HTML.
                if not _is_note_model(model):
                    continue

                desc = row.get('DESC', '').strip()
                edid = row.get('EDID', '').strip()
                full = row.get('FULL', '').strip()

                # Exclude recipes/plans that happen to use a note model
                # (e.g. 16 recipes use props\note_classified.nif)
                if (edid.lower().startswith('recipe_') or
                    full.startswith('Plan:') or full.startswith('Recipe:')):
                    continue

                # Must have a display name
                if not full:
                    continue

                formid = row.get('FormID', '').strip()
                edid = row.get('EDID', '').strip()
                full = row.get('FULL', '').strip()
                btof = row.get('BTOF', '').strip()

                is_cut = starts_cut_notes(edid)

                # Filter out unresolved alias template names
                # e.g. '<Alias=MissileSilo> Launch Code Solution'
                if is_alias_template_name(full):
                    is_cut = True

                # Location: use locations TSV (physical cell + quest), fall back to EDID
                location = resolve_note_location(formid, edid, locations)

                # Collectability from BTOF flag
                collect = can_note_be_collected(btof)

                # Contents: strip HTML tags, decode entities, restore paragraph breaks
                contents = strip_html_to_text(desc)

                # Quest-given detection (from locations TSV)
                loc_data   = (locations or {}).get(formid, {})
                quest_name = loc_data.get('quest_name', '')
                quest_given = bool(quest_name)

                item = {
                    "formId": formid,
                    "edid": edid,
                    "name": full,
                    "location": location,
                    "questGiven": quest_given,
                    "questName": quest_name,
                    "contents": contents,
                    "canCollect": collect,
                    "btof": btof,
                    "isCut": is_cut
                }

                if is_cut:
                    notes_cut.append(item)
                else:
                    notes_live.append(item)

    except Exception as e:
        print(f"  ERROR reading BOOK: {e}", file=sys.stderr)
        return [], []

    # Sort live notes: collectible notes first, then environment spawns,
    # each group sorted alphabetically by name.
    # canCollect=True  -> 0 (first)
    # canCollect=False -> 1 (second)
    notes_live.sort(key=lambda x: (0 if x.get('canCollect', True) else 1, x['name']))

    return notes_live, notes_cut


def build_holotape_games(book_path, locations=None):
    """
    Build holotape games list from BOOK TSV.
    Filters for rows where EDID contains 'Magazine_Holotape_'.

    Holotape games are magazine-type BOOK records. They spawn at random
    magazine locations throughout Appalachia.

    Returns (live_items, cut_items).
    """
    print("  Streaming BOOK file for holotape games...", file=sys.stderr)
    live = []
    cut = []

    try:
        with open(book_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                edid = row.get('EDID', '').strip()

                if 'Magazine_Holotape_' not in edid:
                    continue

                formid = row.get('FormID', '').strip()
                full = row.get('FULL', '').strip()

                # Skip nameless items (e.g. CharGen duplicates)
                if not full:
                    continue

                is_cut = starts_cut(edid)

                item = {
                    "formId": formid,
                    "edid": edid,
                    "name": full,
                    "isCut": is_cut
                }

                if is_cut:
                    cut.append(item)
                else:
                    live.append(item)

    except Exception as e:
        print(f"  ERROR reading BOOK for holotape games: {e}", file=sys.stderr)
        return [], []

    live.sort(key=lambda x: x['name'])
    return live, cut


def build_magazines(book_path, locations=None):
    """
    Build magazines list from BOOK TSV.
    Filters for rows where EDID contains 'Magazine_' but NOT 'Magazine_Holotape_'.
    Includes BACKUP_ variants.

    Returns (live_items, cut_items).
    """
    print("  Streaming BOOK file for magazines...", file=sys.stderr)
    live = []
    cut = []

    try:
        with open(book_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                edid = row.get('EDID', '').strip()
                edid_upper = edid.upper()

                # Must contain 'Magazine_' somewhere in EDID
                if 'MAGAZINE_' not in edid_upper:
                    continue

                # Exclude holotape games
                if 'MAGAZINE_HOLOTAPE_' in edid_upper:
                    continue

                formid = row.get('FormID', '').strip()
                full = row.get('FULL', '').strip()
                desc = row.get('DESC', '').strip()
                btof = row.get('BTOF', '').strip()

                if not full:
                    continue

                is_cut = starts_cut(edid)

                # Location
                location = resolve_note_location(formid, edid, locations)

                # Collectability
                collect = can_note_be_collected(btof)

                # Contents — strip HTML if present
                contents = strip_html_to_text(desc) if desc else ""

                item = {
                    "formId": formid,
                    "edid": edid,
                    "name": full,
                    "location": location,
                    "contents": contents,
                    "canCollect": collect,
                    "isCut": is_cut
                }

                if is_cut:
                    cut.append(item)
                else:
                    live.append(item)

    except Exception as e:
        print(f"  ERROR reading BOOK for magazines: {e}", file=sys.stderr)
        return [], []

    live.sort(key=lambda x: x['name'])
    return live, cut


def build_holotapes(book_path, misc_rows, note_formids=None, locations=None):
    """
    Build holotapes list from BOOK + MISC TSVs.

    From BOOK: rows where EDID or FULL contains 'Holotape' but NOT 'Magazine_Holotape_'
    (these are audio holotapes, not holotape games).

    Also from BOOK: rows that are NOT captured as notes (no <font tag in DESC),
    NOT plans/recipes, NOT magazines, NOT player titles, NOT holotape games,
    and have a non-empty name. These are typically holotape transcripts stored as
    plain-text BOOK records.

    From MISC: rows where EDID or FULL contains 'Holotape'
    (some quest holotapes are MISC objects).

    note_formids: set of FormIDs already captured by build_notes, to avoid
    double-counting items that appear in both lists.

    Returns (live_items, cut_items).
    """
    print("  Building holotapes from BOOK + MISC...", file=sys.stderr)
    live = []
    cut = []
    seen_formids = set()
    note_formids = note_formids or set()

    # Patterns that identify plans/recipes (EDID or FULL)
    _PLAN_PREFIXES = ('RECIPE', 'RECIPE_')
    _PLAN_NAME_PREFIXES = ('PLAN:', 'RECIPE:')

    # 1. BOOK holotapes — two-pass approach:
    #    Pass A: explicit 'Holotape' in EDID/FULL (high confidence)
    #    Pass B: plain-text BOOK records that aren't notes/plans/magazines (holotape transcripts)
    book_a_count = 0
    book_b_count = 0

    try:
        with open(book_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                edid = row.get('EDID', '').strip()
                full = row.get('FULL', '').strip()
                formid = row.get('FormID', '').strip()
                desc = row.get('DESC', '').strip()
                edid_upper = edid.upper()
                full_upper = (full or '').upper()

                if not full or formid in seen_formids:
                    continue

                # Skip if already captured as a note
                if formid in note_formids:
                    continue

                # --- Always exclude these categories ---
                # Holotape games (Magazine_Holotape_)
                if 'MAGAZINE_HOLOTAPE_' in edid_upper:
                    continue
                # Magazines (Magazine_ or PerkMag)
                if 'MAGAZINE_' in edid_upper or edid_upper.startswith('PERKMAG'):
                    continue
                # Plans and recipes
                if any(edid_upper.startswith(p) for p in _PLAN_PREFIXES):
                    continue
                if any(full_upper.startswith(p) for p in _PLAN_NAME_PREFIXES):
                    continue
                # Player titles
                if 'PLAYERTITLE' in edid_upper:
                    continue
                # Camp titles
                if 'CAMPTITLE' in edid_upper:
                    continue
                # Test/debug records
                if edid_upper.startswith('TEST') or edid_upper.startswith('DEBUG'):
                    continue

                # --- Pass A: explicit holotape match ---
                has_holotape = ('HOLOTAPE' in edid_upper or
                                'HOLOTAPE' in full_upper)

                # --- Pass B: plain-text BOOK records (not HTML-formatted notes) ---
                # These are holotape transcripts: BOOK records with plain text DESC,
                # no HTML formatting (<font, <p tags), not plans/mags/titles.
                has_html = ('<font' in desc.lower() or
                            '<p ' in desc.lower() or
                            '<p>' in desc.lower())
                is_plain_text_book = bool(desc) and not has_html

                if not has_holotape and not is_plain_text_book:
                    continue

                seen_formids.add(formid)

                btof = row.get('BTOF', '').strip()
                is_cut = starts_cut(edid)

                location = resolve_note_location(formid, edid, locations)
                collect = can_note_be_collected(btof)
                contents = strip_html_to_text(desc) if desc else ""

                item = {
                    "formId": formid,
                    "edid": edid,
                    "name": full,
                    "location": location,
                    "contents": contents,
                    "canCollect": collect,
                    "isCut": is_cut
                }

                if is_cut:
                    cut.append(item)
                else:
                    live.append(item)

                if has_holotape:
                    book_a_count += 1
                else:
                    book_b_count += 1

    except Exception as e:
        print(f"  ERROR reading BOOK for holotapes: {e}", file=sys.stderr)

    print(f"    BOOK explicit holotape match: {book_a_count}", file=sys.stderr)
    print(f"    BOOK plain-text transcripts: {book_b_count}", file=sys.stderr)

    # 2. MISC holotape objects
    misc_count = 0
    for row in misc_rows:
        edid = row.get('EDID', '').strip()
        full = row.get('FULL', '').strip()
        edid_upper = edid.upper()

        has_holotape = ('HOLOTAPE' in edid_upper or
                        'HOLOTAPE' in (full or '').upper())
        if not has_holotape:
            continue

        formid = row.get('FormID', '').strip()
        if not full or formid in seen_formids:
            continue
        seen_formids.add(formid)

        is_cut = starts_cut(edid)

        item = {
            "formId": formid,
            "edid": edid,
            "name": full,
            "location": derive_location_from_edid(edid),
            "contents": "",
            "canCollect": True,
            "isCut": is_cut
        }

        if is_cut:
            cut.append(item)
        else:
            live.append(item)
        misc_count += 1

    print(f"    MISC holotape objects: {misc_count}", file=sys.stderr)

    live.sort(key=lambda x: x['name'])
    print(f"  Found {len(live)} live holotapes, {len(cut)} cut (total)", file=sys.stderr)
    return live, cut


def load_keym_locations(path):
    """
    Load KEYM_*_Locations.tsv and return a dict:
      { FormID -> {loc_name, loc_source, quest_name} }
    Same format as load_book_locations but keyed on KEYM_FormID.
    """
    result = {}
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                fid = row.get('KEYM_FormID', '').strip()
                if not fid:
                    continue
                result[fid] = {
                    'loc_name':   row.get('LocationName',   '').strip(),
                    'loc_source': row.get('LocationSource', '').strip(),
                    'quest_name': row.get('QuestName',      '').strip(),
                }
    except Exception as e:
        print(f"  WARNING: Could not load KEYM locations file: {e}", file=sys.stderr)
    return result


def _clean_alias_tags(s):
    """Strip <Alias=...> tags from quest location strings."""
    return re.sub(r'<[Aa]lias=[^>]*>', '', s).strip()


def resolve_keym_location(formid, edid, locations):
    """
    Resolve the best display location string for a key.
    Uses the same priority as resolve_note_location.
    """
    loc_data = locations.get(formid) if locations else None

    if loc_data:
        loc_name   = loc_data['loc_name']
        loc_source = loc_data['loc_source']
        quest_name = loc_data['quest_name']

        if loc_source == 'ExtCell':
            parsed = parse_ext_cell_location(loc_name)
            return parsed if parsed else "Not near a named location"

        if loc_source in ('CellFULL', 'NameParse', 'CellEDID'):
            if not loc_name or loc_name.lower() in ('appalachia',):
                pass  # fall through
            elif not is_test_cell_name(loc_name):
                return loc_name

        if quest_name:
            cleaned = _clean_alias_tags(quest_name)
            return "Quest: " + re.sub(r'  +', ' ', cleaned).strip()

        if loc_name and not is_test_cell_name(loc_name) and loc_name.lower() != 'appalachia':
            return loc_name

    return derive_location_from_edid(edid)


def build_keys(keym_rows, misc_rows, keym_locations=None):
    """
    Build keys list from KEYM TSV + MISC TSV.

    Primary source: KEYM records (the dedicated key record type in FO76).
    Secondary source: MISC records where EDID or FULL contains 'Key' or 'Keycard'.

    Excludes false positives from MISC (miscmod, whiskey, turkey, monkey, keyboard, etc.).

    Returns (live_items, cut_items).
    """
    print("  Building keys from KEYM + MISC...", file=sys.stderr)
    live = []
    cut = []
    seen_formids = set()

    # 1. KEYM records (primary source — all KEYM records are keys)
    for row in keym_rows:
        edid = row.get('EDID', '').strip()
        full = row.get('FULL', '').strip()
        formid = row.get('FormID', '').strip()

        if not full or formid in seen_formids:
            continue
        seen_formids.add(formid)

        edid_upper = edid.upper()

        is_cut = starts_cut(edid)

        # Mark test/debug items as cut
        if edid_upper.startswith('TEST') or edid_upper.startswith('DEBUG'):
            is_cut = True

        location = resolve_keym_location(formid, edid, keym_locations)

        # If location resolves to a test cell or debug quest, mark as cut
        if (is_test_cell_name(location) or
            re.search(r'(?i)\btest', location) or
            re.search(r'(?i)\bdebug', location)):
            is_cut = True

        item = {
            "formId": formid,
            "edid": edid,
            "name": full,
            "location": location,
            "contents": "",
            "canCollect": True,
            "isCut": is_cut
        }

        if is_cut:
            cut.append(item)
        else:
            live.append(item)

    print(f"    KEYM: {len(live)} live, {len(cut)} cut", file=sys.stderr)

    # 2. MISC key-like objects (secondary — catches quest keys stored as MISC)
    # Words that contain 'key' but aren't actual keys
    FALSE_POSITIVES = (
        'MISCMOD', 'WHISKEY', 'TURKEY', 'DONKEY', 'MONKEY',
        'KEYBOARD', 'JANGLES', 'HOCKEY',
    )

    misc_live = 0
    misc_cut = 0
    for row in misc_rows:
        edid = row.get('EDID', '').strip()
        full = row.get('FULL', '').strip()
        formid = row.get('FormID', '').strip()

        if not full or formid in seen_formids:
            continue

        edid_upper = edid.upper()
        full_upper = (full or '').upper()

        # Must contain 'Key' or 'Keycard' in EDID or name
        has_key = ('KEY' in edid_upper or 'KEY' in full_upper or
                   'KEYCARD' in edid_upper or 'KEYCARD' in full_upper)
        if not has_key:
            continue

        # Exclude false positives
        combined = edid_upper + ' ' + full_upper
        if any(fp in combined for fp in FALSE_POSITIVES):
            continue

        seen_formids.add(formid)
        is_cut = starts_cut(edid)

        item = {
            "formId": formid,
            "edid": edid,
            "name": full,
            "location": derive_location_from_edid(edid),
            "contents": "",
            "canCollect": True,
            "isCut": is_cut
        }

        if is_cut:
            cut.append(item)
            misc_cut += 1
        else:
            live.append(item)
            misc_live += 1

    print(f"    MISC: {misc_live} live, {misc_cut} cut", file=sys.stderr)

    live.sort(key=lambda x: x['name'])
    print(f"  Found {len(live)} live keys, {len(cut)} cut (total)", file=sys.stderr)
    return live, cut


def main():
    parser = argparse.ArgumentParser(
        description='Build collectables JSON dist files for Fallout 76'
    )
    parser.add_argument('--tsv-root', required=True, help='Root directory for TSV exports')
    parser.add_argument('--seasons', required=True, help='Path to fallout76_seasons.tsv')
    parser.add_argument('--outdir', required=True, help='Output directory for JSON files')

    args = parser.parse_args()

    tsv_root = Path(args.tsv_root)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Auto-discover TSV files
    alch_files = list(tsv_root.glob('ALCH_Export*.tsv'))
    glob_files = list(tsv_root.glob('GLOB_Export*.tsv'))
    book_files = list(tsv_root.glob('BOOK_Export*.tsv'))
    misc_files = list(tsv_root.glob('MISC_Export*.tsv'))
    gmrw_files = list(tsv_root.glob('GMRW_Export*.tsv'))

    # KYWD_Export_*_Refs.tsv (normalized refs from split xEdit KYWD export)
    kywd_refs_files = list(tsv_root.glob('KYWD_Export*_Refs.tsv'))

    # KEYM_Export*.tsv (key records — dedicated key record type)
    keym_files = [p for p in tsv_root.glob('KEYM_Export*.tsv')
                  if not p.name.endswith('_Locations.tsv')]

    if not alch_files:
        print("ERROR: No ALCH_Export*.tsv files found", file=sys.stderr)
        sys.exit(1)
    if not glob_files:
        print("ERROR: No GLOB_Export*.tsv files found", file=sys.stderr)
        sys.exit(1)
    if not book_files:
        print("ERROR: No BOOK_Export*.tsv files found", file=sys.stderr)
        sys.exit(1)
    if not kywd_refs_files:
        print("ERROR: No KYWD_Export*_Refs.tsv files found", file=sys.stderr)
        print("  Run the ExportKYWDToTSV.pas xEdit script to generate the split KYWD files.", file=sys.stderr)
        sys.exit(1)
    if not misc_files:
        print("ERROR: No MISC_Export*.tsv files found", file=sys.stderr)
        sys.exit(1)
    if not gmrw_files:
        print("ERROR: No GMRW_Export*.tsv files found", file=sys.stderr)
        sys.exit(1)
    if not keym_files:
        print("WARNING: No KEYM_Export*.tsv files found — keys will only come from MISC", file=sys.stderr)

    # Locations TSV (companion file from updated BOOK export script)
    # Pattern: BOOK_Export_*_Locations.tsv
    loc_files = list(tsv_root.glob('BOOK_Export*_Locations.tsv'))

    # KEYM Locations TSV
    keym_loc_files = list(tsv_root.glob('KEYM_Export*_Locations.tsv'))

    # When multiple exports exist, pick the latest by filename (sorted descending)
    alch_path = sorted(alch_files, key=lambda p: p.name)[-1]
    glob_path = sorted(glob_files, key=lambda p: p.name)[-1]
    # Exclude *_Locations.tsv from main BOOK file selection
    book_files_main = [p for p in book_files if not p.name.endswith('_Locations.tsv')]
    book_path = sorted(book_files_main, key=lambda p: p.name)[-1]
    misc_path = sorted(misc_files, key=lambda p: p.name)[-1]
    gmrw_path = sorted(gmrw_files, key=lambda p: p.name)[-1]
    kywd_refs_path = sorted(kywd_refs_files, key=lambda p: p.name)[-1]
    keym_path = sorted(keym_files, key=lambda p: p.name)[-1] if keym_files else None
    print(f"  Using ALCH: {alch_path.name}", file=sys.stderr)
    print(f"  Using KYWD Refs: {kywd_refs_path.name}", file=sys.stderr)
    print(f"  Using MISC: {misc_path.name}", file=sys.stderr)
    if keym_path:
        print(f"  Using KEYM: {keym_path.name}", file=sys.stderr)

    print("Loading seasons...")
    seasons = seasons_map(args.seasons)
    print(f"  Loaded {len(seasons)} seasons")

    print("Loading GLOB...")
    glob_rows = read_tsv_rows(glob_path)
    print(f"  Loaded {len(glob_rows)} GLOB rows")

    print("Loading MISC...")
    misc_rows = read_tsv_rows(misc_path)
    print(f"  Loaded {len(misc_rows)} MISC rows")

    keym_rows = []
    keym_locations = {}
    if keym_path:
        print("Loading KEYM...")
        keym_rows = read_tsv_rows(keym_path)
        print(f"  Loaded {len(keym_rows)} KEYM rows")

        if keym_loc_files:
            keym_loc_path = sorted(keym_loc_files, key=lambda p: p.name)[-1]
            print(f"Loading KEYM locations from {keym_loc_path.name}...")
            keym_locations = load_keym_locations(keym_loc_path)
            print(f"  Loaded {len(keym_locations)} KEYM location entries")
        else:
            print("  NOTE: No KEYM_Export*_Locations.tsv found — key locations will use EDID fallback", file=sys.stderr)

    print("Loading GMRW...")
    gmrw_rows = read_tsv_rows(gmrw_path)
    print(f"  Loaded {len(gmrw_rows)} GMRW rows")
    gmrw_pq_map = gmrw_parentquest_map(gmrw_rows)
    print(f"  Built event map with {len(gmrw_pq_map)} events")

    print("Loading KYWD Refs...")
    kywd_refs_rows = read_tsv_rows(kywd_refs_path)
    print(f"  Loaded {len(kywd_refs_rows)} KYWD ref rows")

    # Build bobbleheads (streams ALCH file)
    print("Building bobbleheads...")
    bobblehead_groups, bobblehead_cut = build_bobbleheads(alch_path, glob_rows)

    # Build plushies (from KYWD_Refs TSV + MISC)
    print("Building plushies...")
    plushies, plushies_cut = build_plushies(kywd_refs_rows, misc_rows, seasons, gmrw_pq_map)

    # Build notes (streams BOOK file)
    # Load locations TSV if available
    book_locations = {}
    if loc_files:
        loc_path = sorted(loc_files, key=lambda p: p.name)[-1]
        print(f"Loading locations from {loc_path.name}...")
        book_locations = load_book_locations(loc_path)
        print(f"  Loaded {len(book_locations)} location entries")
    else:
        print("  NOTE: No BOOK_Export*_Locations.tsv found -- location data will use EDID fallback only", file=sys.stderr)

    print("Building notes...")
    notes, notes_cut = build_notes(book_path, locations=book_locations)
    print("Applying note overrides...")
    apply_note_overrides(notes, notes_cut)
    # Re-sort after overrides (overrides can change canCollect, shifting group membership)
    notes.sort(key=lambda x: (0 if x.get('canCollect', True) else 1, x['name']))

    # Build holotape games (from BOOK)
    print("Building holotape games...")
    holotape_games, holotape_games_cut = build_holotape_games(book_path, locations=book_locations)

    # Build magazines (from BOOK)
    print("Building magazines...")
    magazines, magazines_cut = build_magazines(book_path, locations=book_locations)

    # Collect FormIDs from notes + magazines so holotapes can avoid double-counting
    note_formids = set()
    for n in notes:
        note_formids.add(n.get('formId', ''))
    for n in notes_cut:
        note_formids.add(n.get('formId', ''))
    for m in magazines:
        note_formids.add(m.get('formId', ''))
    for m in magazines_cut:
        note_formids.add(m.get('formId', ''))
    for g in holotape_games:
        note_formids.add(g.get('formId', ''))
    for g in holotape_games_cut:
        note_formids.add(g.get('formId', ''))

    # Build holotapes (from BOOK + MISC, excluding items already in notes)
    print("Building holotapes...")
    holotapes, holotapes_cut = build_holotapes(book_path, misc_rows,
                                                note_formids=note_formids,
                                                locations=book_locations)

    # Build keys (from KEYM + MISC)
    print("Building keys...")
    keys, keys_cut = build_keys(keym_rows, misc_rows, keym_locations=keym_locations)

    # Generate timestamp
    generated_at = datetime.now(timezone.utc).isoformat()

    # Write bobbleheads JSON
    bobbleheads_data = {
        "generatedAt": generated_at,
        "type": "bobbleheads",
        "groups": bobblehead_groups,
        "cutContent": bobblehead_cut
    }
    bobbleheads_file = outdir / "collectables_bobbleheads.json"
    with open(bobbleheads_file, 'w', encoding='utf-8') as f:
        json.dump(bobbleheads_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {bobbleheads_file}")

    # Write plushies JSON
    plushies_data = {
        "generatedAt": generated_at,
        "type": "plushies",
        "items": plushies
    }
    plushies_file = outdir / "collectables_plushies.json"
    with open(plushies_file, 'w', encoding='utf-8') as f:
        json.dump(plushies_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {plushies_file}")

    # Write notes JSON
    notes_data = {
        "generatedAt": generated_at,
        "type": "notes",
        "items": notes,
        "cutContent": notes_cut
    }
    notes_file = outdir / "collectables_notes.json"
    with open(notes_file, 'w', encoding='utf-8') as f:
        json.dump(notes_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {notes_file}")

    # Write holotape games JSON
    holotape_games_data = {
        "generatedAt": generated_at,
        "type": "holotape-games",
        "items": holotape_games,
        "cutContent": holotape_games_cut
    }
    holotape_games_file = outdir / "collectables_holotape_games.json"
    with open(holotape_games_file, 'w', encoding='utf-8') as f:
        json.dump(holotape_games_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {holotape_games_file}")

    # Write magazines JSON
    magazines_data = {
        "generatedAt": generated_at,
        "type": "magazines",
        "items": magazines,
        "cutContent": magazines_cut
    }
    magazines_file = outdir / "collectables_magazines.json"
    with open(magazines_file, 'w', encoding='utf-8') as f:
        json.dump(magazines_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {magazines_file}")

    # Write holotapes JSON
    holotapes_data = {
        "generatedAt": generated_at,
        "type": "holotapes",
        "items": holotapes,
        "cutContent": holotapes_cut
    }
    holotapes_file = outdir / "collectables_holotapes.json"
    with open(holotapes_file, 'w', encoding='utf-8') as f:
        json.dump(holotapes_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {holotapes_file}")

    # Write keys JSON
    keys_data = {
        "generatedAt": generated_at,
        "type": "keys",
        "items": keys,
        "cutContent": keys_cut
    }
    keys_file = outdir / "collectables_keys.json"
    with open(keys_file, 'w', encoding='utf-8') as f:
        json.dump(keys_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {keys_file}")

    # Write manifest
    bobblehead_count = sum(len(g['items']) for g in bobblehead_groups)
    bobblehead_groups_map = {g['name']: len(g['items']) for g in bobblehead_groups}

    manifest_data = {
        "generatedAt": generated_at,
        "collectables": {
            "bobbleheads": {
                "total": bobblehead_count,
                "groups": bobblehead_groups_map,
                "cut": len(bobblehead_cut)
            },
            "plushies": {
                "total": len(plushies),
                "cut": len(plushies_cut)
            },
            "notes": {
                "total": len(notes),
                "cut": len(notes_cut)
            },
            "holotape_games": {
                "total": len(holotape_games),
                "cut": len(holotape_games_cut)
            },
            "magazines": {
                "total": len(magazines),
                "cut": len(magazines_cut)
            },
            "holotapes": {
                "total": len(holotapes),
                "cut": len(holotapes_cut)
            },
            "keys": {
                "total": len(keys),
                "cut": len(keys_cut)
            }
        }
    }
    manifest_file = outdir / "collectables_manifest.json"
    with open(manifest_file, 'w', encoding='utf-8') as f:
        json.dump(manifest_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {manifest_file}")

    # Generate patchlog feed combining all collectables items
    all_items = []

    # Extract items from bobbleheads (nested in groups)
    for group in bobblehead_groups:
        all_items.extend(group.get('items', []))

    # Extract items from other collectables
    all_items.extend(plushies)
    all_items.extend(notes)
    all_items.extend(holotape_games)
    all_items.extend(magazines)
    all_items.extend(holotapes)
    all_items.extend(keys)

    # Load previous items from git for comparison
    def extract_all_prev_items(prev_json_data):
        """Extract all items from previously generated collectables JSON files."""
        if not prev_json_data:
            return []
        prev_items = []

        # Handle different structures: some have "items", bobbleheads have "groups"
        if "groups" in prev_json_data:
            # Bobbleheads structure
            for group in prev_json_data.get("groups", []):
                prev_items.extend(group.get("items", []))
        elif "items" in prev_json_data:
            # Standard structure for plushies, notes, etc.
            prev_items.extend(prev_json_data.get("items", []))

        return prev_items

    # Load previous data from all collectables files and combine
    prev_all_items = []
    collectables_files = [
        "dist/collectables_bobbleheads.json",
        "dist/collectables_plushies.json",
        "dist/collectables_notes.json",
        "dist/collectables_holotape_games.json",
        "dist/collectables_magazines.json",
        "dist/collectables_holotapes.json",
        "dist/collectables_keys.json",
    ]

    for cf in collectables_files:
        prev_data = _git_show_json("HEAD^", cf)
        if prev_data:
            prev_all_items.extend(extract_all_prev_items(prev_data))

    # Generate patchlog entry
    entry = diff_item_lists(
        prev_items=prev_all_items,
        curr_items=all_items,
        key_field="formId",
        name_field="name,edid",
        compare_fields=["name", "description", "locations"],
    )

    # Write patchlog feed
    feed = {"entries": [entry]}
    feed_path = outdir / "patchlog_latest_df_collectables.json"
    _write_json(str(feed_path), feed)

    # Log summary
    a, r, c = len(entry["added"]), len(entry["removed"]), len(entry["changed"])
    print(
        f"[patchlog] patchlog_latest_df_collectables.json: current={entry['current']}  "
        f"added={a}  removed={r}  changed={c}"
    )

    # Print summary
    print("\n=== Summary ===")
    print(f"Bobbleheads: {bobblehead_count} live ({bobblehead_groups_map}), {len(bobblehead_cut)} cut")
    print(f"Plushies: {len(plushies)} live, {len(plushies_cut)} cut")
    print(f"Notes: {len(notes)} live, {len(notes_cut)} cut (includes {sum(1 for n in notes_cut if is_alias_template_name(n.get('name',''))) } alias-template exclusions)")
    print(f"Holotape Games: {len(holotape_games)} live, {len(holotape_games_cut)} cut")
    print(f"Magazines: {len(magazines)} live, {len(magazines_cut)} cut")
    print(f"Holotapes: {len(holotapes)} live, {len(holotapes_cut)} cut")
    print(f"Keys: {len(keys)} live, {len(keys_cut)} cut")


if __name__ == '__main__':
    main()
