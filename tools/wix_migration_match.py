"""Match a Wix draft to its WordPress guide page and track nightly migrations.

Usage:
  python tools/wix_migration_match.py match <wix_slug> "<wix title>"
      -> prints the best WP page candidate(s) as JSON (redirect map first, then title match)
  python tools/wix_migration_match.py done-ids
      -> prints every Wix draft id already in the tracker (done / skipped / no-page)
  python tools/wix_migration_match.py log <wix_id> <wix_slug> "<wix title>" <status> <wp_url> <skill> "<notes>"
      -> appends one row to tsv/wix_migration_tracker.tsv
Status values: done | skipped-questions | skipped-no-skill | no-page | already-built
"""
import csv, json, sys, os, re, difflib, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GITHUB = os.path.dirname(ROOT)
TRACKER = os.path.join(ROOT, 'tsv', 'wix_migration_tracker.tsv')
INDEX = os.path.join(ROOT, 'tsv', 'guide_index.tsv')
DATAMAP = os.path.join(ROOT, 'data', 'guide_index_data_map.json')
REDIRECTS = os.path.join(GITHUB, 'wix-redirects-ready.csv')
HEADER = ['wix_id', 'wix_slug', 'wix_title', 'status', 'wp_url', 'skill', 'date', 'notes']
SKIP_TEMPLATES = {'category-hub', 'external-link'}


def norm(s):
    s = (s or '').lower()
    s = re.sub(r'fallout 76|in fallout 76|where to find( and how to farm for)?|how to farm for|guide', ' ', s)
    return re.sub(r'[^a-z0-9]+', ' ', s).strip()


def load_index():
    with open(INDEX, encoding='utf-8') as f:
        return [r for r in csv.DictReader(f, delimiter='\t')]


def load_datamap():
    try:
        return json.load(open(DATAMAP, encoding='utf-8')).get('pages', {})
    except Exception:
        return {}


def load_redirects():
    out = {}
    if os.path.exists(REDIRECTS):
        for r in csv.DictReader(open(REDIRECTS, encoding='utf-8')):
            if r['source'].startswith('/post/'):
                out[r['source'][6:].strip('/')] = r['destination']
    return out


def describe(row, dm):
    data = dm.get(row['url'])
    return {
        'url': row['url'], 'title': row['title'], 'template': row['template'],
        'visibility': row['visibility'], 'id': row['id'],
        'loads_data': data or [],
        # a hand-written 'guide' page that loads no dist data is the empty placeholder we fill
        'looks_empty': row['template'] == 'guide' and not data,
    }


def match(slug, title):
    idx = load_index(); dm = load_datamap(); red = load_redirects()
    by_url = {r['url']: r for r in idx}
    res = {'wix_slug': slug, 'wix_title': title, 'redirect': None, 'title_matches': []}
    dest = red.get(slug)
    if dest:
        res['redirect'] = describe(by_url[dest], dm) if dest in by_url else {'url': dest, 'missing_from_index': True}
    t = norm(title)
    scored = []
    for r in idx:
        if r['template'] in SKIP_TEMPLATES or r['nodeType'] == 'top':
            continue
        sc = max(difflib.SequenceMatcher(None, t, norm(r['title'])).ratio(),
                 difflib.SequenceMatcher(None, t, norm(r['menuTitle'])).ratio())
        if sc >= 0.55:
            scored.append((sc, r))
    scored.sort(key=lambda x: -x[0])
    res['title_matches'] = [dict(describe(r, dm), score=round(sc, 2)) for sc, r in scored[:5]]
    return res


def ensure_tracker():
    if not os.path.exists(TRACKER):
        with open(TRACKER, 'w', encoding='utf-8', newline='') as f:
            csv.writer(f, delimiter='\t').writerow(HEADER)


def done_ids():
    ensure_tracker()
    with open(TRACKER, encoding='utf-8') as f:
        return [r['wix_id'] for r in csv.DictReader(f, delimiter='\t')]


def log(wix_id, slug, title, status, url, skill, notes):
    ensure_tracker()
    with open(TRACKER, 'a', encoding='utf-8', newline='') as f:
        csv.writer(f, delimiter='\t').writerow(
            [wix_id, slug, title, status, url, skill, datetime.date.today().isoformat(), notes])


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'match':
        print(json.dumps(match(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ''), indent=1))
    elif cmd == 'done-ids':
        print('\n'.join(done_ids()))
    elif cmd == 'log':
        log(*sys.argv[2:9]); print('logged')
    else:
        print(__doc__)
