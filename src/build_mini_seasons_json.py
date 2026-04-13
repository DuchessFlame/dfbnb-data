#!/usr/bin/env python3
"""
build_mini_seasons_json.py — Build mini_seasons.json from CHAL TSV exports.

Reads CHAL_Export_*.tsv files from tsv/ directory, extracts all mini-season
challenge data (ATX_DE* and selected SCORE_Challenge* EDIDs), groups them
by event with week assignments, and outputs dist/mini_seasons/mini_seasons.json.

Week assignment uses three strategies:
  1. Explicit _Week1_ / _Week2_ in EDID
  2. Completion challenge condition references (GetIsForm -> sub-challenge)
  3. CNAM scope fallback (Daily -> week1, Weekly -> week2)

Usage:
  python src/build_mini_seasons_json.py
  # or with explicit paths:
  python src/build_mini_seasons_json.py --tsv-root tsv --outdir dist/mini_seasons
"""
import csv, re, os, json, sys, glob, argparse
from collections import OrderedDict

# ─────────────────────────────────────────────────────────────
# Condition parsing
# ─────────────────────────────────────────────────────────────

def parse_condition_display(raw):
    """Parse condition into human-readable display.
    Handles both formats:
      1. Pre-parsed: "Top:Subject.FuncName(Param) = value"
      2. Pipe-separated: "FLAGS|VALUE|FUNC|...|PARAM|..."
    """
    c = str(raw or '').strip()
    if not c:
        return None

    if '|' in c and c.count('|') >= 5:
        # ── Pipe-separated raw format ──
        parts = c.split('|')
        value = parts[1] if len(parts) > 1 else ''
        func  = parts[2] if len(parts) > 2 else ''
        param = parts[5] if len(parts) > 5 else ''
        subj  = parts[-1].rstrip('|').strip() if parts else ''

        if func in ('IsFalloutWorlds', 'GetIsForm'):
            return None

        name_m = re.search(r'"([^"]+)"', param)
        name   = name_m.group(1) if name_m else ''
        ref_m  = re.search(r'\[(\w+):([0-9A-Fa-f]+)\]', param)
        ref    = f"[{ref_m.group(1)}:{ref_m.group(2)}]" if ref_m else ''

        try:
            v = float(value)
            value = str(int(v)) if v == int(v) else f"{v:.6f}"
        except Exception:
            pass

        if name and ref:
            return f"{subj}.{func}({name} {ref}) = {value}"
        elif name:
            return f"{subj}.{func}({name}) = {value}"
        else:
            return f"{subj}.{func}() = {value}"
    else:
        # ── Pre-parsed human-readable format ──
        if 'IsFalloutWorlds' in c:
            return None
        if '.GetIsForm(' in c:
            return None
        return re.sub(r'^Top:', '', c)


def extract_chal_refs(raw_conds):
    """Extract challenge EDIDs referenced in GetIsForm conditions."""
    refs = []
    for c in raw_conds:
        if 'GetIsForm' in c:
            refs.extend(re.findall(r'(\w+)\s+"[^"]*"\s+\[CHAL:', c))
    return refs


# ─────────────────────────────────────────────────────────────
# TSV parsing
# ─────────────────────────────────────────────────────────────

def parse_chal_tsv(path):
    """Parse a CHAL TSV → dict of edid → row (only rows with SNAM)."""
    out = {}
    with open(path, 'r', encoding='utf-8-sig', errors='replace') as fh:
        rdr = csv.DictReader(fh, delimiter='\t')
        for row in rdr:
            edid = (row.get('EDID', '') or '').strip().rstrip('\r')
            snam = (row.get('SNAM', '') or '').strip().rstrip('\r')
            if not snam:
                continue

            raw = []
            for i in range(1, 53):
                v = (row.get(f'Cond{i}', '') or '').strip().rstrip('\r')
                if v:
                    raw.append(v)

            out[edid] = {
                'form_id':  (row.get('FormID', '') or '').strip().rstrip('\r'),
                'edid':     edid,
                'full':     (row.get('FULL', '') or '').strip().rstrip('\r'),
                'snam':     snam,
                'tnam':     (row.get('TNAM', '') or '').strip().rstrip('\r'),
                'cnam':     (row.get('CNAM', '') or '').strip().rstrip('\r'),
                'enam':     (row.get('ENAM', '') or '').strip().rstrip('\r'),
                'conditions':     [pc for rc in raw if (pc := parse_condition_display(rc))],
                'raw_conditions': raw,
            }
    return out


def parse_tsv_nosnam(path):
    """Parse April-style TSV (no SNAM) for new challenges."""
    out = {}
    with open(path, 'r', encoding='utf-8-sig', errors='replace') as fh:
        rdr = csv.DictReader(fh, delimiter='\t')
        for row in rdr:
            edid = (row.get('EDID', '') or '').strip().rstrip('\r')
            full = (row.get('FULL', '') or '').strip().rstrip('\r')
            if not full or edid in out:
                continue
            raw = []
            for i in range(1, 53):
                v = (row.get(f'Cond{i}', '') or '').strip().rstrip('\r')
                if v:
                    raw.append(v)
            out[edid] = {
                'form_id':  (row.get('FormID', '') or '').strip().rstrip('\r'),
                'edid': edid, 'full': full,
                'snam': '', 'tnam': '', 'cnam': '', 'enam': '',
                'conditions':     [pc for rc in raw if (pc := parse_condition_display(rc))],
                'raw_conditions': raw,
            }
    return out


# ─────────────────────────────────────────────────────────────
# Event definitions
# ─────────────────────────────────────────────────────────────

EVENT_DEFS = OrderedDict([
    ('love-hurts',          {'title': 'Love Hurts',                                      'url': '/df/mini-seasons/love-hurts/challenge-checklist/',                                   'patterns': [r'ATX_DE2025_LoveHurts']}),
    ('sunset-stranger',     {'title': 'Sunset Stranger',                                  'url': '/df/mini-seasons/sunset-stranger/challenge-checklist/',                               'patterns': [r'ATX_DE2025_SunsetStranger']}),
    ('night-at-the-morgue', {'title': 'Night at the Morgue',                              'url': '/df/mini-seasons/night-at-the-morgue/challenge-checklist/',                           'patterns': [r'ATX_DE2025_Halloween']}),
    ('marshal-mallows',     {'title': "Marshal Mallow's Marvelous Fishing Excursion",     'url': '/df/mini-seasons/marshal-mallows-marvelous-fishing-excursion/challenge-checklist/',    'patterns': [r'ATX_DE2025_MMMFE']}),
    ('appalachian-outlaws',  {'title': 'Appalachian Outlaws',                              'url': '/df/mini-seasons/appalachian-outlaws/challenge-checklist/',                           'patterns': [r'ATX_DE2025_AppalachianOutlaws']}),
    ('science-of-love',      {'title': 'Science of Love',                                  'url': '/df/mini-seasons/science-of-love/challenge-checklist/',                               'patterns': [r'ATX_DE2024_ScienceOfLove']}),
    ('uncharted-scouts',     {'title': 'Uncharted Scouts',                                 'url': '/df/mini-seasons/uncharted-scouts/challenge-checklist/',                              'patterns': [r'ATX_DE2024_UnchartedScouts']}),
    ('spring-cleaning',      {'title': 'Spring Cleaning',                                  'url': '/df/mini-seasons/spring-cleaning/challenge-checklist/',                               'patterns': [r'ATX_DE2024_SpringCleaning']}),
    ('burning-love',         {'title': 'Burning Love',                                     'url': '/df/mini-seasons/burning-love/challenge-checklist/',                                  'patterns': [r'ATX_DE2024_Valentines']}),
    ('birthday',             {'title': 'Birthday Challenge',                               'url': '/df/mini-seasons/birthday-challenge/challenge-checklist/',                            'patterns': [r'ATX_DE2023_Birthday']}),
    ('summer-camp',          {'title': 'Summer Camp',                                      'url': '/df/mini-seasons/summer-camp/challenge-checklist/',                                   'patterns': [r'ATX_DE2023_SummerCamp']}),
    ('rip-daring',           {'title': 'Rip Daring and the Cryptid Hunt',                  'url': '/df/mini-seasons/rip-daring-and-the-cryptid-hunt/challenge-checklist/',               'patterns': [r'ATX_DE2023_RipDaring']}),
    ('call-to-axe-ion',      {'title': 'Call to Axe-ion',                                  'url': '/df/mini-seasons/call-to-axe-ion/challenge-checklist/',                               'patterns': [r'ATX_DE2022_Pitt']}),
    ('anniversary',          {'title': 'Anniversary',                                      'url': '/df/mini-seasons/anniversary/challenge-checklist/',                                   'patterns': [r'ATX_DE2022_Anniversary']}),
    ('nuka-connoisseur',     {'title': 'Nuka Connoisseur',                                 'url': '/df/mini-seasons/nuka-connoisseur/challenge-checklist/',                              'patterns': [r'ATX_DE2022_NukaWorld']}),
    ('the-coming-storm',     {'title': 'The Coming Storm',                                 'url': '/df/mini-seasons/the-coming-storm/challenge-checklist/',                              'patterns': [r'ATX_DE2021_BoS']}),
    ('spread-the-love-2021', {'title': 'Spread the Love 2021',                             'url': '/df/mini-seasons/spread-the-love/2021/challenge-checklist/',                          'patterns': [r'ATX_DE2021_Love']}),
    ('spread-the-love-2023', {'title': 'Spread the Love 2023',                             'url': '/df/mini-seasons/spread-the-love/2023/challenge-checklist/',                          'patterns': [r'ATX_DE2023_Valentines']}),
    ('free-cam',             {'title': 'Free Cam Challenge',                               'url': '/df/mini-seasons/free-cam-challenge/challenge-checklist/',                            'patterns': [r'ATX_DE2022.*FreeCam', r'SCORE.*FreeCam']}),
    ('st-patricks-day',      {'title': "St Patrick's Day Challenge",                       'url': '/df/mini-seasons/st-patricks-day-challenge/challenge-checklist/',                     'patterns': [r'SCORE.*St_Patrick']}),
    ('big-bloom',            {'title': 'The Big Bloom Challenge',                          'url': '/df/mini-seasons/big-bloom-challenge/challenge-checklist/',                           'patterns': [r'ATX_DE2024.*BigBloom', r'SCORE.*BigBloom']}),
    ('weapons-expert',       {'title': "Rip Daring's Weapons Expert Extraordinaire",       'url': '/df/mini-seasons/rip-daring-weapons-expert-extraordinaire/challenge-checklist/',      'patterns': [r'ATX_DE2026_WeaponsExpert']}),
    ('halloween-2021',       {'title': 'Halloween 2021',                                   'url': '/df/mini-seasons/halloween/2021/challenge-checklist/',                                'patterns': [r'ATX_DE2021_Halloween']}),
    ('halloween-2022',       {'title': 'Halloween 2022',                                   'url': '/df/mini-seasons/halloween/2022/challenge-checklist/',                                'patterns': [r'ATX_DE2022_Halloween']}),
    ('halloween-2023',       {'title': 'Halloween 2023',                                   'url': '/df/mini-seasons/halloween/2023/challenge-checklist/',                                'patterns': [r'ATX_DE2023_Halloween']}),
    ('halloween-2024',       {'title': 'Halloween 2024',                                   'url': '/df/mini-seasons/halloween/2024/challenge-checklist/',                                'patterns': [r'ATX_DE2024_Halloween']}),
])


def classify(edid):
    e = re.sub(r'^(ZZZ_|CUT_|DEL_)', '', edid)
    is_cut = edid != e
    for key, ev in EVENT_DEFS.items():
        for pat in ev['patterns']:
            if re.match(pat, e):
                return key, is_cut
    return None, False


def is_completion(edid):
    clean = re.sub(r'^(ZZZ_|CUT_|DEL_)', '', edid)
    return bool(
        re.search(r'_(Week\d_Complete|MiniSeason_Complete|Challenge_Complete|Event_Complete|EventDUPLICATE|Challenge_Week\d_Complete)$', clean) or
        re.search(r'_META$', clean)
    )


def infer_week(edid, row, week_map):
    clean = re.sub(r'^(ZZZ_|CUT_|DEL_)', '', edid)

    if is_completion(edid):
        return 'bonus'
    if '_Week1_' in clean:
        return 'week1'
    if '_Week2_' in clean:
        return 'week2'
    if edid in week_map:
        return week_map[edid]

    cnam = row.get('cnam', '')
    if cnam == 'Daily':
        return 'week1'
    if cnam == 'Weekly':
        return 'week2'
    if '_Daily_' in clean:
        return 'week1'
    if '_Weekly_' in clean:
        return 'week2'

    return 'week1'


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tsv-root', default='tsv', help='Dir containing CHAL_Export_*.tsv')
    parser.add_argument('--outdir', default='dist/mini_seasons', help='Output directory')
    args = parser.parse_args()

    # Find all CHAL TSVs, sort by name (oldest first so newest wins)
    tsv_files = sorted(glob.glob(os.path.join(args.tsv_root, 'CHAL_Export_*.tsv')))
    if not tsv_files:
        print(f"ERROR: No CHAL_Export_*.tsv found in {args.tsv_root}", file=sys.stderr)
        sys.exit(1)

    merged = {}
    for f in tsv_files:
        data = parse_chal_tsv(f)
        print(f"  Loaded {len(data):>5} challenges from {os.path.basename(f)}")
        # For challenges with no SNAM (new additions), try nosnam parse
        nosnam = parse_tsv_nosnam(f)
        for edid, row in nosnam.items():
            if edid not in merged and 'WeaponsExpert' in edid:
                merged[edid] = row
        merged.update(data)

    print(f"  Total unique EDIDs: {len(merged)}")

    # Build completion-based week map
    week_map = {}
    for edid, row in merged.items():
        if edid.startswith('ZZZ_'):
            continue
        is_w1 = '_Week1_Complete' in edid or '_Challenge_Week1_Complete' in edid
        is_w2 = '_Week2_Complete' in edid or '_Challenge_Week2_Complete' in edid
        if not (is_w1 or is_w2):
            continue
        week = 'week1' if is_w1 else 'week2'
        for ref in extract_chal_refs(row['raw_conditions']):
            week_map[ref] = week

    print(f"  Week map: {len(week_map)} challenges assigned via completions")

    # Build output
    output = {}
    for key, evdef in EVENT_DEFS.items():
        live, cut = [], []
        for edid, row in sorted(merged.items()):
            ev_key, is_cut = classify(edid)
            if ev_key != key:
                continue
            if edid.startswith('SCORE_') and not any(k in edid for k in ['FreeCam', 'St_Patrick', 'BigBloom']):
                continue

            entry = {
                'id':            row['form_id'],
                'edid':          row['edid'],
                'name':          row['full'],
                'snam':          row['snam'],
                'required':      row['tnam'],
                'scope':         row['cnam'],
                'category':      row['enam'],
                'conditions':    row['conditions'],
                'guide_links':   [],
                'week':          infer_week(edid, row, week_map),
                'is_cut':        is_cut,
                'is_completion': is_completion(edid),
            }
            (cut if is_cut else live).append(entry)

        output[key] = {
            'key':        key,
            'title':      evdef['title'],
            'url':        evdef['url'],
            'week1':      [c for c in live if c['week'] == 'week1'],
            'week2':      [c for c in live if c['week'] == 'week2'],
            'bonus':      [c for c in live if c['week'] == 'bonus'],
            'cut':        cut,
            'total_live': len(live),
            'total_cut':  len(cut),
        }

    # Write
    os.makedirs(args.outdir, exist_ok=True)
    outpath = os.path.join(args.outdir, 'mini_seasons.json')
    with open(outpath, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    print(f"\n  Written {outpath} ({os.path.getsize(outpath):,} bytes)")
    print(f"\n  {'Event':<48} {'W1':>3} {'W2':>3} {'B':>3} {'C':>3} {'T':>3}")
    print('  ' + '-' * 66)
    g = {'w1': 0, 'w2': 0, 'b': 0, 'c': 0}
    for k, ev in output.items():
        w1 = len(ev['week1']); w2 = len(ev['week2'])
        b  = len(ev['bonus']); c  = len(ev['cut'])
        g['w1'] += w1; g['w2'] += w2; g['b'] += b; g['c'] += c
        print(f"  {ev['title']:<48} {w1:>3} {w2:>3} {b:>3} {c:>3} {w1+w2+b+c:>3}")
    print('  ' + '-' * 66)
    t = g['w1'] + g['w2'] + g['b'] + g['c']
    print(f"  {'TOTAL':<48} {g['w1']:>3} {g['w2']:>3} {g['b']:>3} {g['c']:>3} {t:>3}")
    print(f"\n  {len(output)} events")


if __name__ == '__main__':
    main()
