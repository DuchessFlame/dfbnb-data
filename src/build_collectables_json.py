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
    Strip HTML tags from text, decode entities, preserve line breaks.
    """
    if not html:
        return ""

    # Replace common HTML tags with newlines
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</p>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'<p[^>]*>', '\n', html, flags=re.IGNORECASE)

    # Strip all HTML tags
    text = re.sub(r'<[^>]+>', '', html)

    # Decode HTML entities
    text = unescape(text)

    # Collapse excessive whitespace but preserve intentional line breaks
    lines = text.split('\n')
    lines = [line.strip() for line in lines]
    lines = [line for line in lines if line]  # Remove empty lines
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
    Check if EDID indicates a cut note, with additional CUT infixes.
    """
    if starts_cut(edid):
        return True

    # Additional infixes for notes
    e = (edid or "").upper()
    if "_CUT_" in e or "BURN_CUT_" in e or "ZZZBURN_" in e:
        return True

    return False


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


def build_bobbleheads(alch_path, glob_rows):
    """
    Build bobbleheads list from ALCH TSV + GLOB rows.
    Streams ALCH file to avoid OOM on large files.

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

    try:
        with open(alch_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            count = 0
            for row in reader:
                count += 1
                if count % 1000 == 0:
                    print(f"    Read {count} ALCH rows...", file=sys.stderr)

                edid = row.get('ALCH_EDID', '').strip() or row.get('EDID', '').strip()

                if 'bobble' not in edid.lower():
                    continue

                formid = row.get('ALCH_FormID', '').strip() or row.get('FormID', '').strip()
                full = row.get('FULL', '').strip()
                mgef_edid = row.get('MGEF_EDID', '').strip()
                mgef_full = row.get('MGEF_FULL', '').strip()
                magnitude_str = row.get('EFIT_Magnitude', '').strip() or "0"
                duration_str = row.get('EFIT_Duration', '').strip() or "0"
                magg_formid = row.get('MAGG_GLOB_FormID', '').strip()

                is_cut = starts_cut(edid)

                # Determine magnitude
                magnitude = 0
                try:
                    magnitude = float(magnitude_str)
                except ValueError:
                    pass

                # Check if magnitude references a GLOB
                if magg_formid and magg_formid in glob_lookup:
                    magnitude = glob_lookup[magg_formid]

                # Convert duration
                duration_text = convert_duration_to_text(duration_str)

                # Build buff string
                effect_name = mgef_full if mgef_full else mgef_edid
                magnitude_str_final = f"{int(magnitude)}" if magnitude == int(magnitude) else f"{magnitude}"
                buff = f"{effect_name}: +{magnitude_str_final}% for {duration_text}"

                # Determine group
                group = "Glowing Bobbleheads" if "Glowing" in edid else "Bobbleheads"

                item = {
                    "formId": formid,
                    "edid": edid,
                    "name": full,
                    "group": group,
                    "buff": buff,
                    "effectName": effect_name,
                    "magnitude": magnitude_str_final,
                    "duration": duration_text,
                    "isCut": is_cut
                }

                if is_cut:
                    bobbleheads_cut.append(item)
                else:
                    bobbleheads_live.append(item)

    except Exception as e:
        print(f"  ERROR reading ALCH: {e}", file=sys.stderr)
        return [], []

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


def build_notes(book_path):
    """
    Build notes list from BOOK TSV.
    Streams BOOK file to avoid OOM on large files.

    Filters for rows where DESC contains <font face= (identifies notes).

    Returns (live_items, cut_items).
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

                desc = row.get('DESC', '').strip()

                # Filter for notes: DESC must contain <font face=
                if '<font face=' not in desc:
                    continue

                formid = row.get('FormID', '').strip()
                edid = row.get('EDID', '').strip()
                full = row.get('FULL', '').strip()
                btof = row.get('BTOF', '').strip()

                is_cut = starts_cut_notes(edid)

                # Location: derive from EDID by replacing underscores with spaces
                location = edid.replace('_', ' ')

                # Contents: strip HTML tags, decode entities
                contents = strip_html_to_text(desc)

                item = {
                    "formId": formid,
                    "edid": edid,
                    "name": full,
                    "location": location,
                    "contents": contents,
                    "contentsRaw": desc,
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

    # Sort live notes alphabetically
    notes_live.sort(key=lambda x: x['name'])

    return notes_live, notes_cut


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

    # When multiple exports exist, pick the latest by filename (sorted descending)
    alch_path = sorted(alch_files, key=lambda p: p.name)[-1]
    glob_path = sorted(glob_files, key=lambda p: p.name)[-1]
    book_path = sorted(book_files, key=lambda p: p.name)[-1]
    misc_path = sorted(misc_files, key=lambda p: p.name)[-1]
    gmrw_path = sorted(gmrw_files, key=lambda p: p.name)[-1]
    kywd_refs_path = sorted(kywd_refs_files, key=lambda p: p.name)[-1]
    print(f"  Using ALCH: {alch_path.name}", file=sys.stderr)
    print(f"  Using KYWD Refs: {kywd_refs_path.name}", file=sys.stderr)
    print(f"  Using MISC: {misc_path.name}", file=sys.stderr)

    print("Loading seasons...")
    seasons = seasons_map(args.seasons)
    print(f"  Loaded {len(seasons)} seasons")

    print("Loading GLOB...")
    glob_rows = read_tsv_rows(glob_path)
    print(f"  Loaded {len(glob_rows)} GLOB rows")

    print("Loading MISC...")
    misc_rows = read_tsv_rows(misc_path)
    print(f"  Loaded {len(misc_rows)} MISC rows")

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
    print("Building notes...")
    notes, notes_cut = build_notes(book_path)

    # Generate timestamp
    generated_at = datetime.now(timezone.utc).isoformat()

    # Write bobbleheads JSON
    bobbleheads_data = {
        "generatedAt": generated_at,
        "type": "bobbleheads",
        "groups": bobblehead_groups
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
        "items": notes
    }
    notes_file = outdir / "collectables_notes.json"
    with open(notes_file, 'w', encoding='utf-8') as f:
        json.dump(notes_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {notes_file}")

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
            }
        }
    }
    manifest_file = outdir / "collectables_manifest.json"
    with open(manifest_file, 'w', encoding='utf-8') as f:
        json.dump(manifest_data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {manifest_file}")

    # Print summary
    print("\n=== Summary ===")
    print(f"Bobbleheads: {bobblehead_count} live ({bobblehead_groups_map}), {len(bobblehead_cut)} cut")
    print(f"Plushies: {len(plushies)} live, {len(plushies_cut)} cut")
    print(f"Notes: {len(notes)} live, {len(notes_cut)} cut")


if __name__ == '__main__':
    main()
