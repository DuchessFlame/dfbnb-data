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

Guide linking extracts item/creature/material names from challenge conditions
and matches them against the site's guide_index.tsv to auto-populate guide_links.

Usage:
  python src/build_mini_seasons_json.py
  # or with explicit paths:
  python src/build_mini_seasons_json.py --tsv-root tsv --outdir dist/mini_seasons
"""
import csv, re, os, json, sys, glob, argparse
from collections import OrderedDict

# Static ticket-value overlay (see mini_seasons_tickets.py).
# Ticket rewards per challenge and ticket costs per reward item are not in
# the game files, so we maintain them manually and merge them in after the
# TSV-derived data is built.
try:
    from mini_seasons_tickets import apply_ticket_overlay
except ImportError:
    # Allow running as `python src/build_mini_seasons_json.py` from repo root.
    _this_dir = os.path.dirname(os.path.abspath(__file__))
    if _this_dir not in sys.path:
        sys.path.insert(0, _this_dir)
    from mini_seasons_tickets import apply_ticket_overlay

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

        if func == 'GetIsForm':
            return None

        name_m = re.search(r'"([^"]+)"', param)
        name   = name_m.group(1) if name_m else ''
        ref_m  = re.search(r'\[(\w+):([0-9A-Fa-f]+)\]', param)
        ref    = f"[{ref_m.group(1)}:{ref_m.group(2)}]" if ref_m else ''

        # If no quoted name but there IS a ref bracket, grab the EDID before it
        # e.g. "WeaponsExpert_StatueTypeItem_GatheringBuff [KYWD:008B1D60]"
        if not name and ref:
            edid_m = re.match(r'([A-Za-z0-9_]+)\s*\[', param.strip())
            if edid_m:
                name = edid_m.group(1)

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
        if '.GetIsForm(' in c:
            return None
        return re.sub(r'^Top:', '', c)


# ─────────────────────────────────────────────────────────────
# CNDF leaf-expansion (challenge-style-guide decision: Technical → Conditions
# shows the underlying leaf check, e.g. an IsTrueForConditionForm(... [CNDF:x])
# is resolved to HasKeyword(ObjectTypeFish [KYWD:...])). Mirrors the same logic
# in build_challenges_json_v3.py. _load_cndf() must run before parse_chal_tsv().
# ─────────────────────────────────────────────────────────────
_CNDF_BY_FID = {}

def _load_cndf(tsv_root):
    files = sorted(glob.glob(os.path.join(tsv_root, 'CNDF_Export_*.tsv')), key=os.path.getmtime)
    if not files:
        return
    with open(files[-1], 'r', encoding='utf-8-sig', errors='replace') as fh:
        for row in csv.DictReader(fh, delimiter='\t'):
            fid = (row.get('FormID', '') or '').strip().upper()
            if not fid:
                continue
            try:
                n = int((row.get('CondCount', '') or '0').strip())
            except Exception:
                n = 76
            conds = []
            for i in range(1, min(n + 1, 77)):
                v = (row.get(f'Cond{i}', '') or '').strip()
                if v:
                    conds.append(v)
            _CNDF_BY_FID[fid] = conds

def _is_null_leaf(line):
    return bool(re.search(r'\[(?:FORM|KYWD):0+\]', line or '') or re.search(r'\(0+\s', line or ''))

def expand_condition_display(raw, _seen=None):
    """One raw condition -> display string(s), leaf-expanding CNDF condition-forms."""
    if _seen is None:
        _seen = set()
    s = str(raw or '').strip()
    if not s:
        return []
    if '|' in s and s.count('|') >= 5:
        parts = s.split('|')
        func = parts[2] if len(parts) > 2 else ''
        param = parts[5] if len(parts) > 5 else ''
        if func == 'IsTrueForConditionForm':
            m = re.search(r'\[CNDF:([0-9A-Fa-f]+)\]', param)
            if m:
                cfid = m.group(1).upper()
                if cfid not in _seen and cfid in _CNDF_BY_FID:
                    _seen.add(cfid)
                    leaves = []
                    for ic in _CNDF_BY_FID[cfid]:
                        leaves.extend(expand_condition_display(ic, _seen))
                    meaningful = [l for l in leaves if not _is_null_leaf(l)]
                    if meaningful:
                        return meaningful
    pc = parse_condition_display(raw)
    return [pc] if pc else []


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

# ─────────────────────────────────────────────────────────────
# Guide linking
# ─────────────────────────────────────────────────────────────

# Condition item names that should NEVER generate guide links.
# These are cosmetic items, CAMP categories, event refs, regions,
# generic gameplay terms, or internal-only references.
GUIDE_SKIP = {
    # Cosmetic / event-specific gear (no farming guide exists)
    'tough love helmet', 'flower crown', 'wasteland wanderer outfit',
    'wasteland wanderer backpack', 'wasteland wanderer headwear',
    'wasteland wanderer headwear with cap', 'coin flip emote',
    'sunset sarsaparilla deputy', 'marshal mallow',
    # CAMP build categories (no individual guide)
    'floors', 'walls', 'roofs', 'lights', 'shelves', 'displays',
    'floor decor', 'wall decor', 'stash boxes', 'generators',
    'power connectors',
    # Regions (not items)
    'ash heap', 'burning springs', 'the forest', 'savage divide',
    'toxic valley', 'skyline valley',
    # Locations
    "johnson's acre", 'the slumber mill motel', 'tunnel of love', 'vault 76',
    # Generic / internal terms
    'costume', 'human', 'energy', 'food', 'lost', 'cap', 'pistol',
    '1-hand melee', '2-hand melee', 'all events',
    'event: the big bloom',
    # Mystery items (placeholders)
    'mystery item day 2', 'mystery item day 4', 'mystery item day 6',
    'mystery item day 7', 'mystery item day 9', 'mystery item day 11',
    'mystery item day 14',
    # Misc items with no dedicated guide
    'broom', 'clean broom', 'soap', 'plunger', 'toilet paper',
    'balloon animal bouquet', 'moneybag',
    'beaker', 'large beaker', 'thin beaker', 'microscope',
    'scalpel', 'surgical scalpel',
    'cake pan', 'clean cake pan', 'ceremonial cannon',
    'autopsy board game', 'blast radius board game',
    'catch the commie board game',
    'bait - common', 'bait - improved', 'bait - superb',
    'water',
}

# Manual alias map: condition item name (lower) → guide (title, url).
# Handles Nuka-Cola hyphenation, scrap variants, race→creature mappings,
# and items whose condition name differs from the guide subCategory.
GUIDE_ALIASES = {
    # ── Nuka-Cola products (condition uses hyphens, guides use spaces) ──
    'nuka-cola':            ('Nuka Cola Guide',            '/bnb/farming/nuka-cola/nuka-cola/guide/'),
    'nuka-cherry':          ('Nuka Cola Cherry Guide',     '/bnb/farming/nuka-cola/nuka-cola-cherry/guide/'),
    'nuka-grape':           ('Nuka Cola Grape Guide',      '/bnb/farming/nuka-cola/nuka-cola-grape/guide/'),
    'nuka-orange':          ('Nuka Cola Orange Guide',     '/bnb/farming/nuka-cola/nuka-cola-orange/guide/'),
    'nuka-cola quantum':    ('Nuka Cola Quantum Guide',    '/bnb/farming/nuka-cola/nuka-cola-quantum/guide/'),
    'nuka-cola dark':       ('Nuka Cola Dark Guide',       '/bnb/farming/nuka-cola/nuka-cola-dark/guide/'),
    'nuka-cola cranberry':  ('Nuka Cola Cranberry Guide',  '/bnb/farming/nuka-cola/nuka-cola-cranberry/guide/'),
    'nuka-cola twist':      ('Nuka Cola Twist Guide',      '/bnb/farming/nuka-cola/nuka-cola-twist/guide/'),
    'nuka-cola wild':       ('Nuka Cola Wild Guide',       '/bnb/farming/nuka-cola/nuka-cola-wild/guide/'),
    'fermentable nuka-cola dark': ('Nuka Cola Dark Guide', '/bnb/farming/nuka-cola/nuka-cola-dark/guide/'),

    # ── Scrap / raw variants → base junk guide ──
    'steel scrap':          ('Steel Farming Guide',        '/df/farming/junk/steel/farming-guide/'),
    'ceramic scrap':        ('Ceramic Farming Guide',      '/df/farming/junk/ceramic/farming-guide/'),
    'glass shards':         ('Glass Farming Guide',        '/df/farming/junk/glass/farming-guide/'),
    'gold scrap':           ('Gold Farming Guide',         '/df/farming/junk/gold/farming-guide/'),
    'silver scrap':         ('Silver Farming Guide',       '/df/farming/junk/silver/farming-guide/'),
    'fiber optics bundle':  ('Fiber Optics Farming Guide', '/df/farming/junk/fiber-optics/farming-guide/'),
    'waste acid':           ('Acid Farming Guide',         '/df/farming/junk/acid/farming-guide/'),
    'raw fertilizer':       ('Fertilizer Farming Guide',   '/df/farming/junk/fertilizer/farming-guide/'),
    'molded plastic':       ('Plastic Farming Guide',      '/df/farming/junk/plastic/farming-guide/'),
    'nuclear waste':        ('Nuclear Material Farming Guide', '/df/farming/junk/nuclear-material/farming-guide/'),

    # ── Eggs (condition uses singular, guide subCategory uses plural) ──
    'deathclaw egg':        ('Deathclaw Egg Guide',        '/bnb/farming/eggs/eggs-deathclaw/deathclaw-guide/'),
    'mothman egg':          ('Mothman Egg Guide',          '/bnb/farming/eggs/eggs-mothman/mothman-guide/'),
    'perfect mothman egg':  ('Mothman Egg Guide',          '/bnb/farming/eggs/eggs-mothman/mothman-guide/'),

    # ── Meat with "Meat" suffix in condition name ──
    'opossum meat':         ('Opossum Meat Guide',         '/bnb/farming/meat/opossum/opossum/'),
    'queen mirelurk meat':  ('Mirelurk Queen Meat Guide',  '/bnb/farming/meat/mirelurk-queen/mirelurk-queen-guide/'),
    'brahmin milk':         ('Brahmin Meat Guide',         '/bnb/farming/meat/brahmin/brahmin-guide/'),

    # ── Creature race names → meat guides ──
    'honey beast':          ('Honey Guide',                '/bnb/farming/non-perishable/non-perishable-honey/honey-guide/'),
    'feral ghoul':          ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'wild mongrel':         ('Mongrel Dog Meat Guide',     '/bnb/farming/meat/mongrel-dog/mongrel-dog-guide/'),
    'super mutant':         ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'wendigo colossus':     ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'mirelurk hunter':      ('Mirelurk Meat Guide',        '/bnb/farming/meat/mirelurk/mirelurk-guide/'),
    'mirelurk king':        ('Mirelurk Meat Guide',        '/bnb/farming/meat/mirelurk/mirelurk-guide/'),
    'mirelurk spawn':       ('Mirelurk Meat Guide',        '/bnb/farming/meat/mirelurk/mirelurk-guide/'),
    'fev hound':            ('Mongrel Dog Meat Guide',     '/bnb/farming/meat/mongrel-dog/mongrel-dog-guide/'),
    'floater':              ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'scorched':             ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'protectron':           ('Circuitry Farming Guide',    '/df/farming/junk/circuitry/farming-guide/'),
    'robobrain':            ('Circuitry Farming Guide',    '/df/farming/junk/circuitry/farming-guide/'),
    'trog':                 ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'ghoul':                ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'alien':                ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'radturkey':            ('Radstag Meat Guide',         '/bnb/farming/meat/radstag/radstag-guide/'),
    'radant':               ('Rad Ant Meat Guide',         '/bnb/farming/meat/rad-ant/rad-ant-guide/'),

    # ── Big Bloom creatures ──
    'overgrown pollinator': ('Honey Guide',                '/bnb/farming/non-perishable/non-perishable-honey/honey-guide/'),
    'overgrown tank':       ('Mirelurk Meat Guide',        '/bnb/farming/meat/mirelurk/mirelurk-guide/'),
    'overgrown thorn':      ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'lesser devil':         ('Honey Guide',                '/bnb/farming/non-perishable/non-perishable-honey/honey-guide/'),
    'blue devil':           ('Honey Guide',                '/bnb/farming/non-perishable/non-perishable-honey/honey-guide/'),
    'the beast of beckley': ('Honey Guide',                '/bnb/farming/non-perishable/non-perishable-honey/honey-guide/'),

    # ── Chems → chem buffs guide ──
    'med-x':                ('Chem Buffs Guide',           '/bnb/buffs/chems/chem-buffs/'),
    'psycho':               ('Chem Buffs Guide',           '/bnb/buffs/chems/chem-buffs/'),
    'disease cure':         ('Chem Buffs Guide',           '/bnb/buffs/chems/chem-buffs/'),
    'antibiotics':          ('Chem Buffs Guide',           '/bnb/buffs/chems/chem-buffs/'),
    'healing salve':        ('Chem Buffs Guide',           '/bnb/buffs/chems/chem-buffs/'),

    # ── Food items → food buffs guide ──
    'cranberry cobbler':    ('Food Buffs Guide',           '/bnb/buffs/food/food-buffs/'),
    'starlight berry cobbler': ('Food Buffs Guide',        '/bnb/buffs/food/food-buffs/'),
    'fermentable wine':     ('Alcohol Buffs Guide',        '/bnb/buffs/alcohol/alcohol-buffs/'),

    # ── Junk items referenced by non-standard names ──
    'pre-war money':        ('Cloth Farming Guide',        '/df/farming/junk/cloth/farming-guide/'),
    'bag of chlorine':      ('Acid Farming Guide',         '/df/farming/junk/acid/farming-guide/'),

    # ── Tobacco / cigarette items → no dedicated guide, link to junk overview ──
    'pack of cigarettes':       ('Plastic Farming Guide',  '/df/farming/junk/plastic/farming-guide/'),
    'preserved cigarette pack': ('Plastic Farming Guide',  '/df/farming/junk/plastic/farming-guide/'),
    'undamaged cigarettes':     ('Plastic Farming Guide',  '/df/farming/junk/plastic/farming-guide/'),
    'cigar box':                ('Wood Farming Guide',     '/df/farming/junk/wood/farming-guide/'),
    'cigarette carton':         ('Cloth Farming Guide',    '/df/farming/junk/cloth/farming-guide/'),
    'box of san francisco sunlights': ('Plastic Farming Guide', '/df/farming/junk/plastic/farming-guide/'),

    # ── Cleaning items → relevant junk guide ──
    'abraxo cleaner':              ('Acid Farming Guide',       '/df/farming/junk/acid/farming-guide/'),
    'abraxo cleaner industrial grade': ('Acid Farming Guide',   '/df/farming/junk/acid/farming-guide/'),
    'undamaged abraxo cleaner':    ('Acid Farming Guide',       '/df/farming/junk/acid/farming-guide/'),

    # ── Skull parts → bone junk guide ──
    'skull':                ('Bone Farming Guide',         '/df/farming/junk/bone/farming-guide/'),
    'skull cap bone':       ('Bone Farming Guide',         '/df/farming/junk/bone/farming-guide/'),
    'skull eye socket':     ('Bone Farming Guide',         '/df/farming/junk/bone/farming-guide/'),
    'skull faceplate':      ('Bone Farming Guide',         '/df/farming/junk/bone/farming-guide/'),
    'skull fragment':       ('Bone Farming Guide',         '/df/farming/junk/bone/farming-guide/'),
    'upper skull':          ('Bone Farming Guide',         '/df/farming/junk/bone/farming-guide/'),
    'capless skull':        ('Bone Farming Guide',         '/df/farming/junk/bone/farming-guide/'),
    'human jaw':            ('Bone Farming Guide',         '/df/farming/junk/bone/farming-guide/'),

    # ── Spice items ──
    'spices':               ('Spices Guide',               '/bnb/farming/non-perishable/salt-pepper-spices-sugar/perishable-spices-guide/'),
    'sugar':                ('Sugar Guide',                '/bnb/farming/non-perishable/salt-pepper-spices-sugar/perishable-sugar-guide/'),

    # ── Radtoad → egg guide (toad challenges are usually egg-related) ──
    'radtoad':              ('Radtoad Egg Guide',          '/bnb/farming/eggs/eggs-radtoad/radtoad-guide/'),

    # ── Fanatics ──
    'fanatics faction':     ('Glowing Meat Guide',         '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
}

# Keyword EDID → guide mapping.
# Conditions using HasKeyword() reference items by EDID rather than quoted name.
# This maps the keyword EDID to the corresponding guide.
KEYWORD_GUIDES = {
    # Plants
    'PlantTypeAshRose':        ('Ash Rose Guide',           '/bnb/farming/plants/ash-rose/ash-rose-guide/'),
    'PlantTypeBrainFungus':    ('Brain Fungus Guide',       '/bnb/farming/plants/brain-fungus/brain-fungus-guide/'),
    'PlantTypeCarrotFlower':   ('Carrot Guide',             '/bnb/farming/plants/carrot/carrot-guide/'),
    'PlantTypeFeverBlossom':   ('Fever Blossom Guide',      '/bnb/farming/plants/fever-blossom/fever-blossom-guide/'),
    'PlantTypeFirecapFungus':  ('Firecap Guide',            '/bnb/farming/plants/firecap/firecap-guide/'),
    'PlantTypeSootFlower':     ('Toxic Soot Flower Guide',  '/bnb/farming/plants/toxic-soot-flower/toxic-soot-flower-guide/'),

    # Chems
    'ChemTypeMentats':         ('Chem Buffs Guide',         '/bnb/buffs/chems/chem-buffs/'),
    'ChemTypeRadaway':         ('Chem Buffs Guide',         '/bnb/buffs/chems/chem-buffs/'),
    'ChemTypeStimpack':        ('Chem Buffs Guide',         '/bnb/buffs/chems/chem-buffs/'),
    'ObjectTypeChem':          ('Chem Buffs Guide',         '/bnb/buffs/chems/chem-buffs/'),
    'ObjectTypeStimpak':       ('Chem Buffs Guide',         '/bnb/buffs/chems/chem-buffs/'),

    # Creatures
    'ActorTypeWendigo':        ('Glowing Meat Guide',       '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'ActorTypeWendigoColossus':('Glowing Meat Guide',       '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'ActorTypeWendigoSpawn':   ('Glowing Meat Guide',       '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'ActorTypeGlowing':        ('Glowing Meat Guide',       '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'ActorTypeFloaterGnasher': ('Glowing Meat Guide',       '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    'ActorTypeOvergrown':      ('Honey Guide',              '/bnb/farming/non-perishable/non-perishable-honey/honey-guide/'),
    'ActorTypeCryptid':        ('Mothman Egg Guide',        '/bnb/farming/eggs/eggs-mothman/mothman-guide/'),

    # Food
    'DrinkTypeAlcohol':        ('Alcohol Buffs Guide',      '/bnb/buffs/alcohol/alcohol-buffs/'),
    'MealTypeCooked':          ('Food Buffs Guide',         '/bnb/buffs/food/food-buffs/'),
    'MealTypeSteak':           ('Food Buffs Guide',         '/bnb/buffs/food/food-buffs/'),
    'MealTypeDogfood':         ('Food Buffs Guide',         '/bnb/buffs/food/food-buffs/'),
    'MealTypeBirthdayCake':    ('Food Buffs Guide',         '/bnb/buffs/food/food-buffs/'),
    'ObjectTypeCakesPies':     ('Food Buffs Guide',         '/bnb/buffs/food/food-buffs/'),
    'ObjectTypeCandy':         ('Food Buffs Guide',         '/bnb/buffs/food/food-buffs/'),
}


def load_guide_index(guide_tsv_path):
    """Load guide_index.tsv and build a subCategory → (title, url) lookup.

    Only includes guide-type pages (template or title contains 'guide').
    Returns dict keyed by lowercase subCategory.
    """
    lookup = {}
    if not os.path.isfile(guide_tsv_path):
        print(f"  WARNING: guide_index.tsv not found at {guide_tsv_path}, skipping guide linking")
        return lookup

    with open(guide_tsv_path, 'r', encoding='utf-8-sig', errors='replace') as fh:
        rdr = csv.DictReader(fh, delimiter='\t')
        for row in rdr:
            nt = (row.get('nodeType', '') or '').strip().rstrip('\r')
            if nt != 'page':
                continue
            title = (row.get('title', '') or '').strip().rstrip('\r')
            url   = (row.get('url', '') or '').strip().rstrip('\r')
            subcat = (row.get('subCategory', '') or '').strip().rstrip('\r')
            template = (row.get('template', '') or '').strip().rstrip('\r')

            if 'guide' not in template and 'guide' not in title.lower():
                continue

            key = subcat.lower().strip()
            if key and key not in lookup:
                lookup[key] = (title, url)

    print(f"  Guide index: {len(lookup)} subCategory→guide mappings loaded")
    return lookup


def extract_item_names(conditions):
    """Extract quoted item/creature names and keyword EDIDs from parsed conditions.

    Returns list of (item_name, record_type) tuples.
    record_type is the 4-letter record prefix (ALCH, MISC, CMPO, RACE, KYWD etc.)
    or empty string if not found.

    Also extracts HasKeyword EDID names prefixed with 'KYWD:' for keyword-based
    guide matching.
    """
    items = []
    for cond in conditions:
        # Match GetIsID/GetIsRace patterns: "Item Name" [TYPE:FormID]
        for m in re.finditer(r'"([^"]+)"\s*\[(\w+):', cond):
            name = m.group(1)
            rtype = m.group(2)
            items.append((name, rtype))

        # Match HasKeyword patterns: HasKeyword(KeywordEdid [KYWD:...])
        # or HasKeyword(KeywordEdid "Name" [KYWD:...])
        for m in re.finditer(r'HasKeyword\((\w+)\s+(?:"[^"]*"\s+)?\[KYWD:', cond):
            edid = m.group(1)
            items.append((f'KYWD:{edid}', 'KYWD'))

        # Also match quoted names without brackets (rare edge case)
        if '[' not in cond:
            for m in re.finditer(r'"([^"]+)"', cond):
                items.append((m.group(1), ''))
    return items


def resolve_guide_links(conditions, guide_lookup, challenge_name=''):
    """Resolve challenge conditions → guide_links array.

    Strategy:
      1. Extract all quoted item names + keyword EDIDs from conditions
      2. Skip items in GUIDE_SKIP (cosmetics, regions, internal refs)
      3. For KYWD: prefixed items, check KEYWORD_GUIDES table
      4. For regular items, check GUIDE_ALIASES then subCategory lookup
      5. If no links found from conditions, try name-based extraction
      6. Deduplicate by URL (same guide may match multiple condition items)

    Returns list of {title, url} dicts.
    """
    items = extract_item_names(conditions)

    seen_urls = set()
    links = []

    for item_name, rtype in items:
        title, url = None, None

        # Handle keyword EDID lookups
        if item_name.startswith('KYWD:'):
            edid = item_name[5:]
            if edid in KEYWORD_GUIDES:
                title, url = KEYWORD_GUIDES[edid]
        else:
            key = item_name.lower().strip()

            # Skip known non-linkable items
            if key in GUIDE_SKIP:
                continue

            # 1. Check manual alias table first
            if key in GUIDE_ALIASES:
                title, url = GUIDE_ALIASES[key]

            # 2. Direct subCategory match
            if not url and key in guide_lookup:
                title, url = guide_lookup[key]

        if url and url not in seen_urls:
            seen_urls.add(url)
            links.append({'title': title, 'url': url})

    # ── Fallback: name-based creature/item extraction ──
    # If condition-based matching found nothing, try extracting known
    # creature or item names from the challenge name itself.
    if not links and challenge_name:
        name_links = _extract_from_challenge_name(challenge_name, guide_lookup)
        for gl in name_links:
            if gl['url'] not in seen_urls:
                seen_urls.add(gl['url'])
                links.append(gl)

    return links


# Creature / item names to search for in challenge names, ordered longest first
# to prevent partial matches (e.g., "Mirelurk Queen" before "Mirelurk").
_NAME_KEYWORDS = [
    # Multi-word creatures (must come before single-word)
    ('mirelurk queen',    'Mirelurk Queen Meat Guide',  '/bnb/farming/meat/mirelurk-queen/mirelurk-queen-guide/'),
    ('mirelurk king',     'Mirelurk Meat Guide',        '/bnb/farming/meat/mirelurk/mirelurk-guide/'),
    ('mirelurk hunter',   'Mirelurk Meat Guide',        '/bnb/farming/meat/mirelurk/mirelurk-guide/'),
    ('cave cricket',      'Cave Cricket Meat Guide',     '/bnb/farming/meat/cave-cricket/cave-cricket-guide/'),
    ('fog crawler',       'Fog Crawler Meat Guide',      '/bnb/farming/meat/fog-crawler/fog-crawler-guide/'),
    ('feral ghoul',       'Glowing Meat Guide',          '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    ('super mutant',      'Glowing Meat Guide',          '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    ('mega sloth',        'Megasloth Meat Guide',        '/bnb/farming/meat/megasloth/megasloth-guide/'),
    ('mongrel dog',       'Mongrel Dog Meat Guide',      '/bnb/farming/meat/mongrel-dog/mongrel-dog-guide/'),
    ('honey beast',       'Honey Guide',                 '/bnb/farming/non-perishable/non-perishable-honey/honey-guide/'),
    ('hermit crab',       'Hermit Crab Meat Guide',      '/bnb/farming/meat/hermit-crab/hermit-crab-guide/'),
    ('mole rat',          'Mole Rat Meat Guide',         '/bnb/farming/meat/mole-rat/mole-rat-guide/'),
    ('yao guai',          'Yao Guai Meat Guide',         '/bnb/farming/meat/yao-guai/yao-guai/'),
    ('wendigo colossus',  'Glowing Meat Guide',          '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    ('grafton monster',   'Glowing Meat Guide',          '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    ('mutant hound',      'Mongrel Dog Meat Guide',      '/bnb/farming/meat/mongrel-dog/mongrel-dog-guide/'),
    # Single-word creatures
    ('deathclaw',         'Deathclaw Meat Guide',        '/bnb/farming/meat/deathclaw/deathclaw-guide/'),
    ('mirelurk',          'Mirelurk Meat Guide',         '/bnb/farming/meat/mirelurk/mirelurk-guide/'),
    ('radscorpion',       'Radscorpion Meat Guide',      '/bnb/farming/meat/radscorpion/radscorpion-guide/'),
    ('scorchbeast',       'Scorchbeast Meat Guide',      '/bnb/farming/meat/scorchbeast/scorchbeast-guide/'),
    ('snallygaster',      'Glowing Meat Guide',          '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    ('brahmin',           'Brahmin Meat Guide',           '/bnb/farming/meat/brahmin/brahmin-guide/'),
    ('radstag',           'Radstag Meat Guide',           '/bnb/farming/meat/radstag/radstag-guide/'),
    ('radhog',            'Radhog Meat Guide',            '/bnb/farming/meat/radhog/radhog-guide/'),
    ('radroach',          'Radroach Meat Guide',          '/bnb/farming/meat/radroach/radroach-guide/'),
    ('radrat',            'Radrat Meat Guide',            '/bnb/farming/meat/radrat/radrat/'),
    ('bloatfly',          'Bloatfly Meat Guide',          '/bnb/farming/meat/bloatfly/bloatfly-guide/'),
    ('bloodbug',          'Bloodbug Meat Guide',          '/bnb/farming/meat/bloodbug/bloodbug-guide/'),
    ('gulper',            'Gulper Meat Guide',            '/bnb/farming/meat/gulper/gulper-guide/'),
    ('angler',            'Angler Meat Guide',            '/bnb/farming/meat/angler/angler-guide/'),
    ('megasloth',         'Megasloth Meat Guide',         '/bnb/farming/meat/megasloth/megasloth-guide/'),
    ('wendigo',           'Glowing Meat Guide',           '/bnb/farming/meat/glowing-meat/glowing-meat-guide/'),
    ('wolf',              'Wolf Meat Guide',              '/bnb/farming/meat/wolf/wolf-guide/'),
    ('ogua',              'Gulper Meat Guide',            '/bnb/farming/meat/gulper/gulper-guide/'),
    ('stingwing',         'Bloatfly Meat Guide',          '/bnb/farming/meat/bloatfly/bloatfly-guide/'),
]


def _extract_from_challenge_name(name, guide_lookup):
    """Try to extract creature/item names from challenge name text.

    Uses word-boundary matching to avoid false positives
    (e.g., "Brahmin" shouldn't match inside "Abrahmin").
    """
    lower = name.lower()
    seen = set()
    links = []

    for keyword, title, url in _NAME_KEYWORDS:
        if url in seen:
            continue
        # Word boundary match: keyword must not be inside a larger word.
        # Allow trailing 's' for plurals (e.g., "Mirelurks" matches "mirelurk").
        pattern = r'(?<![a-z])' + re.escape(keyword) + r's?(?![a-z])'
        if re.search(pattern, lower):
            seen.add(url)
            links.append({'title': title, 'url': url})

    return links


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
                'conditions':     [pc for rc in raw for pc in expand_condition_display(rc)],
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
                'conditions':     [pc for rc in raw for pc in expand_condition_display(rc)],
                'raw_conditions': raw,
            }
    return out


# ─────────────────────────────────────────────────────────────
# Event definitions
# ─────────────────────────────────────────────────────────────

EVENT_DEFS = OrderedDict([
    # ── Mini Seasons (ticket system: complete tasks → earn tickets → buy rewards) ──
    ('summer-sock-hop',     {'title': 'Summer Sock Hop',                                  'url': '/df/mini-seasons/summer-sock-hop/challenge-checklist/',                              'patterns': [r'(?:ATX_)?DE2026_SockHop'],              'type': 'mini_season',       'start_date': '2026-07-21', 'end_date': '2026-08-04'}),
    ('love-hurts',          {'title': 'Love Hurts',                                      'url': '/df/mini-seasons/love-hurts/challenge-checklist/',                                   'patterns': [r'ATX_DE2025_LoveHurts'],                'type': 'mini_season',       'start_date': '2026-02-03', 'end_date': '2026-02-17'}),
    ('sunset-stranger',     {'title': 'Sunset Stranger',                                  'url': '/df/mini-seasons/sunset-stranger/challenge-checklist/',                               'patterns': [r'ATX_DE2025_SunsetStranger'],            'type': 'mini_season',       'start_date': '2025-12-23', 'end_date': '2026-01-06'}),
    ('night-at-the-morgue', {'title': 'Night at the Morgue',                              'url': '/df/mini-seasons/night-at-the-morgue/challenge-checklist/',                           'patterns': [r'ATX_DE2025_Halloween'],                 'type': 'mini_season',       'start_date': '2025-10-21', 'end_date': '2025-11-04'}),
    ('marshal-mallows',     {'title': "Marshal Mallow's Marvelous Fishing Excursion",     'url': '/df/mini-seasons/marshal-mallows-marvelous-fishing-excursion/challenge-checklist/',    'patterns': [r'ATX_DE2025_MMMFE'],                     'type': 'mini_season',       'start_date': '2025-08-12', 'end_date': '2025-08-26'}),
    ('appalachian-outlaws',  {'title': 'Appalachian Outlaws',                              'url': '/df/mini-seasons/appalachian-outlaws/challenge-checklist/',                           'patterns': [r'ATX_DE2025_AppalachianOutlaws'],        'type': 'mini_season',       'start_date': '2025-04-22', 'end_date': '2025-05-06'}),
    ('science-of-love',      {'title': 'Science of Love',                                  'url': '/df/mini-seasons/science-of-love/challenge-checklist/',                               'patterns': [r'ATX_DE2024_ScienceOfLove'],             'type': 'mini_season',       'start_date': '2025-02-04', 'end_date': '2025-02-18'}),
    ('uncharted-scouts',     {'title': 'Uncharted Scouts',                                 'url': '/df/mini-seasons/uncharted-scouts/challenge-checklist/',                              'patterns': [r'ATX_DE2024_UnchartedScouts'],           'type': 'mini_season',       'start_date': '2024-08-06', 'end_date': '2024-08-20'}),
    ('spring-cleaning',      {'title': 'Spring Cleaning',                                  'url': '/df/mini-seasons/spring-cleaning/challenge-checklist/',                               'patterns': [r'ATX_DE2024_SpringCleaning'],            'type': 'mini_season',       'start_date': '2024-05-07', 'end_date': '2024-05-21'}),
    ('burning-love',         {'title': 'Burning Love',                                     'url': '/df/mini-seasons/burning-love/challenge-checklist/',                                  'patterns': [r'ATX_DE2024_Valentines'],                'type': 'mini_season',       'start_date': '2024-01-30', 'end_date': '2024-02-13'}),
    ('weapons-expert',       {'title': "Rip Daring's Weapons Expert Extraordinaire",       'url': '/df/mini-seasons/rip-daring-weapons-expert-extraordinaire/challenge-checklist/',      'patterns': [r'ATX_DE2026_WeaponsExpert'],             'type': 'mini_season',       'start_date': '2026-04-21', 'end_date': '2026-05-05'}),

    # ── Limited Time Events (complete task → get reward directly) ──
    ('birthday',             {'title': 'Birthday Challenge',                               'url': '/df/mini-seasons/birthday-challenge/challenge-checklist/',                            'patterns': [r'ATX_DE2023_Birthday'],                  'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('summer-camp',          {'title': 'Summer Camp',                                      'url': '/df/mini-seasons/summer-camp/challenge-checklist/',                                   'patterns': [r'ATX_DE2023_SummerCamp'],                'type': 'limited_time_event', 'start_date': '2023-07-25', 'end_date': '2023-08-08'}),
    ('rip-daring',           {'title': 'Rip Daring and the Cryptid Hunt',                  'url': '/df/mini-seasons/rip-daring-and-the-cryptid-hunt/challenge-checklist/',               'patterns': [r'ATX_DE2023_RipDaring'],                 'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('call-to-axe-ion',      {'title': 'Call to Axe-ion',                                  'url': '/df/mini-seasons/call-to-axe-ion/challenge-checklist/',                               'patterns': [r'ATX_DE2022_Pitt'],                      'type': 'limited_time_event', 'start_date': '2022-11-15', 'end_date': '2022-11-29'}),
    ('anniversary',          {'title': 'Anniversary',                                      'url': '/df/mini-seasons/anniversary/challenge-checklist/',                                   'patterns': [r'ATX_DE2022_Anniversary'],               'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('nuka-connoisseur',     {'title': 'Nuka Connoisseur',                                 'url': '/df/mini-seasons/nuka-connoisseur/challenge-checklist/',                              'patterns': [r'ATX_DE2022_NukaWorld'],                 'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('the-coming-storm',     {'title': 'The Coming Storm',                                 'url': '/df/mini-seasons/the-coming-storm/challenge-checklist/',                              'patterns': [r'ATX_DE2021_BoS'],                       'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('spread-the-love-2021', {'title': 'Spread the Love 2021',                             'url': '/df/mini-seasons/spread-the-love/2021/challenge-checklist/',                          'patterns': [r'ATX_DE2021_Love'],                      'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('spread-the-love-2023', {'title': 'Spread the Love 2023',                             'url': '/df/mini-seasons/spread-the-love/2023/challenge-checklist/',                          'patterns': [r'ATX_DE2023_Valentines'],                'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('free-cam',             {'title': 'Free Cam Challenge',                               'url': '/df/mini-seasons/free-cam-challenge/challenge-checklist/',                            'patterns': [r'ATX_DE2022.*FreeCam', r'SCORE.*FreeCam'], 'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('st-patricks-day',      {'title': "St Patrick's Day Challenge",                       'url': '/df/mini-seasons/st-patricks-day-challenge/challenge-checklist/',                     'patterns': [r'SCORE.*St_Patrick'],                    'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('big-bloom',            {'title': 'The Big Bloom Challenge',                          'url': '/df/mini-seasons/big-bloom-challenge/challenge-checklist/',                           'patterns': [r'ATX_DE2024.*BigBloom', r'SCORE.*BigBloom'], 'type': 'limited_time_event', 'start_date': '2025-05-06', 'end_date': '2025-05-20'}),
    ('halloween-2021',       {'title': 'Halloween 2021',                                   'url': '/df/mini-seasons/halloween/2021/challenge-checklist/',                                'patterns': [r'ATX_DE2021_Halloween'],                 'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('halloween-2022',       {'title': 'Halloween 2022',                                   'url': '/df/mini-seasons/halloween/2022/challenge-checklist/',                                'patterns': [r'ATX_DE2022_Halloween'],                 'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('halloween-2023',       {'title': 'Halloween 2023',                                   'url': '/df/mini-seasons/halloween/2023/challenge-checklist/',                                'patterns': [r'ATX_DE2023_Halloween'],                 'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
    ('halloween-2024',       {'title': 'Halloween 2024',                                   'url': '/df/mini-seasons/halloween/2024/challenge-checklist/',                                'patterns': [r'ATX_DE2024_Halloween'],                 'type': 'limited_time_event', 'start_date': 'TBA', 'end_date': 'TBA'}),
])


# ENTM EDID patterns for matching rewards to events.
# These differ from the CHAL patterns because ENTM naming is inconsistent.
# Each event key maps to a list of regex patterns that match its ENTM EDIDs.
ENTM_PATTERNS = {
    # ── Mini Seasons ──
    'summer-sock-hop':     [r'(?i)SCORE_MiniSeason_2026_SockHop'],
    'love-hurts':          [r'(?i)SCORE_MiniSeason.*LoveHurts'],
    'sunset-stranger':     [r'(?i)SCORE_MiniSeason_2025_SunsetStranger'],
    'night-at-the-morgue': [r'(?i)SCORE_MiniSeason_2025_NightAtTheMorgue'],
    'marshal-mallows':     [r'(?i)SCORE_MiniSeason_2025_MMMFE'],
    'appalachian-outlaws':  [r'(?i)SCORE_MiniSeason.*AppalachianOutlaws'],
    'science-of-love':      [r'(?i)DE2024_ScienceOfLove', r'(?i)DE2024_Scienceoflove'],
    'uncharted-scouts':     [r'(?i)DE2024_ScoutsUncharted_ENTM'],
    'spring-cleaning':      [r'(?i)DE2024_SpringCleaning_ENTM'],
    'burning-love':         [],  # No ENTM entries in game files
    'weapons-expert':       [r'(?i)SCORE_MiniSeason_2026_WeaponsExpert'],

    # ── Limited Time Events ──
    'birthday':             [r'(?i)ATX_ENTM.*Birthday', r'(?i)SCORE_S14_ENTM.*Birthday'],
    'summer-camp':          [r'(?i)ATX_Shelters_ENTM_ShelterEntrance_SummerCamp'],
    'rip-daring':           [r'(?i)ATX_ENTM_CAMP_Bed_RipDaringEvent'],
    'call-to-axe-ion':      [r'(?i)ATX_DE2022_Pitt_ENTM', r'(?i)ATX_Upgrade2022_Pitt_ENTM'],
    'anniversary':          [r'(?i)ATX_ENTM_AnniversaryEvent'],
    'nuka-connoisseur':     [],  # No dedicated ENTM entries in game files
    'the-coming-storm':     [],  # No ENTM entries in game files
    'spread-the-love-2021': [r'(?i)ATX_DE2021_Love_ENTM'],
    'spread-the-love-2023': [],  # No ENTM entries in game files
    'free-cam':             [],  # Building challenge — no cosmetic rewards
    'st-patricks-day':      [r'(?i)ATX_ENTM.*Leprechaun', r'(?i)ATX_ENTM.*StPatrick', r'(?i)ATX_ENTM_Skin_PipBoySkin_StPatricks'],
    'big-bloom':            [],  # Seasonal public event — rewards come from the event itself
    'halloween-2021':       [],  # Generic Halloween ENTM items exist but aren't tied to a specific year
    'halloween-2022':       [],  # Generic Halloween ENTM items exist but aren't tied to a specific year
    'halloween-2023':       [],  # Generic Halloween ENTM items exist but aren't tied to a specific year
    'halloween-2024':       [r'(?i)DE2024_Halloween_ENTM'],
}

# Gallery images: manually maintained per-event.
# Each entry is a dict with 'url' (full path on site) and 'alt' (description).
GALLERY_IMAGES = {
    'weapons-expert': [
        # Promo / event art
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/76_ATX_PROMOTE_P68_S24_MS1_NOTAG.avif',    'alt': 'Weapons Expert Promotion'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/s24_miniseason1_background.avif',           'alt': 'Weapons Expert Background'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/s24_miniseason1_marquee.avif',              'alt': 'Weapons Expert Marquee'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/s24_miniseason1_seasonselection.avif',      'alt': 'Weapons Expert Season Selection'},
        # Reward closeups
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaoponsexpert_camp_floordecor_miniripboystatue_c1.avif', 'alt': 'Miniature Rip Daring Vault Boy Statue'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaoponsexpert_camp_floordecor_miniripboystatue_c2.avif', 'alt': 'Miniature Rip Daring Vault Boy Statue (alt)'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_backpackflair_spacecowflair_c1.avif',       'alt': 'Space Cow Backpack Flair'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_backpackflair_spacecowflair_c2.avif',       'alt': 'Space Cow Backpack Flair (alt)'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_camp_displaycase_jetpackdisplay_c1.avif',   'alt': 'Jetpack Display'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_camp_displaycase_jetpackdisplay_c2.avif',   'alt': 'Jetpack Display (alt)'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_camp_floordecor_taxidermy_megaslothtaxidermy_c1.avif', 'alt': 'Taxidermy Hanging Mega Sloth'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_camp_floordecor_taxidermy_megaslothtaxidermy_c2.avif', 'alt': 'Taxidermy Hanging Mega Sloth (alt)'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_camp_furniture_ripboystatue_c2.avif',       'alt': 'Rip Daring Vault Boy Statue'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_camp_furniture_ripboystatue_c3.avif',       'alt': 'Rip Daring Vault Boy Statue (alt)'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_camp_furniture_ripdaringpinballmachine_c1.avif', 'alt': 'Rip Daring Pinball Machine'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_camp_furniture_ripdaringpinballmachine_c2.avif', 'alt': 'Rip Daring Pinball Machine (alt)'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/gallery/score_miniseason_2026_weaponsexpert_photomode_logo_marqueelogo_ms67_c1.avif',   'alt': 'Weapons Expert Marquee Photomode Logo'},
    ],
}

# ── Per-event reward image subfolder ─────────────────────────────────
# Maps event_key → subfolder under mini-seasons/ where its reward images
# live. When set, auto-generated image URLs include this subfolder so the
# path matches the FileZilla upload location.
EVENT_IMAGE_SUBDIR = {
    'summer-sock-hop': 'summer-sock-hop',
}


# ── Per-event reward image overrides ──────────────────────────────────
# Maps event_key → {img_edid (lowercase, _entm_ stripped) → actual URL}.
# Used by load_entm_rewards() to override the auto-generated image_url
# when the uploaded filename differs from the EDID-derived path.
REWARD_IMAGE_OVERRIDES = {
    'weapons-expert': {
        'score_miniseason_2026_weaponsexpert_photomode_logo_marqueelogo_ms67':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaponsexpert_photomode_logo_marqueelogo_ms67_l.avif',
        'score_miniseason_2026_weaponsexpert_backpackflair_spacecowflair':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaponsexpert_backpackflair_spacecowflair_l.avif',
        'score_miniseason_2026_weaponsexpert_playertitles_prefix_merciless':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaponsexpert_playertitles_prefix_merciless_l.avif',
        'score_miniseason_2026_weaponsexpert_playertitles_prefix_ripped':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaponsexpert_playertitles_prefix_ripped_l.avif',
        'score_miniseason_2026_weaponsexpert_camptitles_prefix_sharpshooters':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaponsexpert_camptitles_prefix_sharpshooters_l.avif',
        'score_miniseason_2026_weaponsexpert_playertitles_suffix_weaponmaster':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaponsexpert_playertitles_suffix_weaponmaster_l.avif',
        'score_miniseason_2026_weaponsexpert_camptitles_suffix_arsenal':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaponsexpert_camptitles_suffix_arsenal_l.avif',
        'score_miniseason_2026_weaponsexpert_camp_floordecor_taxidermy_megaslothtaxidermy':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaoponsexpert_camp_floordecor_taxidermy_megaslothtaxidermy_l.avif',
        'score_miniseason_2026_weaponsexpert_camp_floordecor_miniripboystatue':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaoponsexpert_camp_floordecor_miniripboystatue_c2.avif',
        'score_miniseason_2026_weaponsexpert_camp_furniture_ripdaringpinballmachine':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_camp_furniture_ripdaringpinballmachine_l.avif',
        'score_miniseason_2026_weaponsexpert_camp_furniture_ripboystatue':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaoponsexpert_camp_floordecor_miniripboystatue_l.avif',
        'score_miniseason_2026_weaponsexpert_camp_walldisplay_jetpackdisplay':
            '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/rewards/score_miniseason_2026_weaponsexpert_camp_displaycase_jetpackdisplay_l.avif',
    },
}


def _static_challenge(name, *, snam='Community Goal', required='1', scope='Event',
                       category='Community Challenge', conditions=None,
                       week='week1', note=''):
    """Helper to build a challenge dict for static (non-game-file) events."""
    return {
        'id':            '',
        'edid':          '',
        'name':          name,
        'snam':          snam,
        'required':      required,
        'scope':         scope,
        'category':      category,
        'conditions':    conditions or [],
        'guide_links':   [],
        'keyword_items': {},
        'week':          week,
        'is_cut':        False,
        'is_completion': False,
        'note':          note,
    }


def _build_static_events():
    """Build event data for community events not present in the game's CHAL records.

    These were real-time community challenges tracked server-side, not personal
    CHAL records, so they don't appear in xEdit exports.
    """
    static = {}

    # ──────────────────────────────────────────────────────────────
    # PROJECT CLEAN APPALACHIA (September 10 – October 23, 2019)
    # 6-week limited-time community event series.
    # ──────────────────────────────────────────────────────────────
    pca_weeks = {}

    # Community Challenge 1 — Clear the Skies (Sep 10–23)
    pca_weeks['week1'] = [
        _static_challenge(
            'Community Challenge: Clear the Skies — Take down 100,000 Scorchbeasts',
            snam='Scorchbeasts Killed', required='100,000', week='week1',
            note='Community-wide goal. Stretch goal rewards: Scorchbeast Player Icon, Curly Bun Hairstyle, and a full week of Meat Week.'),
        _static_challenge(
            'Double XP Weekend (September 12–16)',
            snam='Event', required='—', week='week1', category='Weekly Event',
            note='Double XP active September 12–16.'),
        _static_challenge(
            'Atomic Shop Freebie: Tripod Floor Lamp',
            snam='Free Item', required='—', week='week1', category='Weekly Freebie'),
    ]

    # Week 2 — freebies only (Sep 23–30)
    pca_weeks['week2'] = [
        _static_challenge(
            'Atomic Shop Freebie: Mid-Century Disc Lamps',
            snam='Free Item', required='—', week='week2', category='Weekly Freebie'),
    ]

    # Week 3 — Meat Week returns + freebie (Sep 26 – Oct 3)
    pca_weeks['week3'] = [
        _static_challenge(
            'Meat Week Returns (September 26 – October 3)',
            snam='Event', required='—', week='week3', category='Weekly Event',
            note='Meat Week unlocked as a stretch goal reward from Clear the Skies.'),
        _static_challenge(
            'Atomic Shop Freebie: Grafton Photomode Frame & Grafton High Bed',
            snam='Free Item', required='—', week='week3', category='Weekly Freebie'),
    ]

    # Community Challenge 2 — Take Out the Trash (Oct 1–14)
    pca_weeks['week4'] = [
        _static_challenge(
            'Community Challenge: Take Out the Trash — Take down 8,000,000 Scorched',
            snam='Scorched Killed', required='8,000,000', week='week4',
            note='Community-wide goal. Stretch goal rewards: Honeybeast Player Icon, Vault-Tec Flamer Skin, and a 50% discount during the Legendary Vendor Sale.'),
        _static_challenge(
            "Purveyor Murgh's Mystery Pick (October 3–7)",
            snam='Event', required='—', week='week4', category='Weekly Event'),
        _static_challenge(
            'Atomic Shop Freebie: Mr. Handy Player Icon',
            snam='Free Item', required='—', week='week4', category='Weekly Freebie'),
    ]

    # Week 5 — freebie (Oct 7–14)
    pca_weeks['week5'] = [
        _static_challenge(
            'Atomic Shop Freebie: Face Paint Bundle',
            snam='Free Item', required='—', week='week5', category='Weekly Freebie'),
    ]

    # Week 6 — Legendary Vendor Sale + freebie (Oct 14–21)
    pca_weeks['week6'] = [
        _static_challenge(
            'Legendary Vendor Community Sale (October 17–21)',
            snam='Event', required='—', week='week6', category='Weekly Event',
            note='25% off standard, or 50% off if Take Out the Trash stretch goal was unlocked.'),
        _static_challenge(
            'Atomic Shop Freebie: Modern Art Statue',
            snam='Free Item', required='—', week='week6', category='Weekly Freebie'),
    ]

    pca_total = sum(len(v) for v in pca_weeks.values())
    static['project-clean-appalachia'] = {
        'key':          'project-clean-appalachia',
        'title':        'Project Clean Appalachia',
        'type':         'limited_time_event',
        'url':          '/df/mini-seasons/project-clean-appalachia/challenge-checklist/',
        'start_date':   '2019-09-10',
        'end_date':     '2019-10-23',
        **pca_weeks,
        'cut':          [],
        'total_live':   pca_total,
        'total_cut':    0,
        'rewards':      [],
        'gallery':      [],
        'group_labels': {
            'week1': 'Week 1 — Clear the Skies (Sep 10–23)',
            'week2': 'Week 2 (Sep 23–30)',
            'week3': 'Week 3 — Meat Week Returns (Sep 26 – Oct 3)',
            'week4': 'Week 4 — Take Out the Trash (Oct 1–14)',
            'week5': 'Week 5 (Oct 7–14)',
            'week6': 'Week 6 — Legendary Vendor Sale (Oct 14–21)',
        },
    }

    # ──────────────────────────────────────────────────────────────
    # FORTIFYING ATLAS (August 4 – September 10, 2020)
    # Two-phase community resource donation event.
    # ──────────────────────────────────────────────────────────────
    fa_weeks = {}

    # Project Alpha (Aug 4–18)
    fa_weeks['week1'] = [
        _static_challenge(
            'Deliver 125,000,000 Steel',
            snam='Steel Delivered', required='125,000,000', week='week1',
            note='Reward: Brotherhood of Steel Beret.'),
        _static_challenge(
            'Deliver 150,000,000 Concrete',
            snam='Concrete Delivered', required='150,000,000', week='week1',
            note='Reward: Brotherhood of Steel C.A.M.P. Banner.'),
        _static_challenge(
            'Deliver 200,000,000 Cork',
            snam='Cork Delivered', required='200,000,000', week='week1',
            note='Reward: High S.C.O.R.E. Double Daily Challenges (Aug 20–24).'),
        _static_challenge(
            'Deliver 150,000,000 Plastic',
            snam='Plastic Delivered', required='150,000,000', week='week1',
            note='Reward: Bonus Challenges Week (Aug 26–31).'),
    ]

    # Project Bravo (Aug 27 – Sep 10)
    fa_weeks['week2'] = [
        _static_challenge(
            'Deliver 150,000,000 Wood',
            snam='Wood Delivered', required='150,000,000', week='week2',
            note='Reward: Steel Dawn Army Fatigues.'),
        _static_challenge(
            'Deliver 200,000,000 Cloth',
            snam='Cloth Delivered', required='200,000,000', week='week2',
            note='Reward: Brotherhood of Steel Collectron Station.'),
        _static_challenge(
            'Deliver 175,000,000 Leather',
            snam='Leather Delivered', required='175,000,000', week='week2',
            note='Reward: Purveyor 50% off Super Sale (Sep 10–14).'),
        _static_challenge(
            'Deliver 250,000,000 Glass',
            snam='Glass Delivered', required='250,000,000', week='week2',
            note='Reward: Meat Week, A Second Helping (Sep 22–28).'),
    ]

    fa_total = sum(len(v) for v in fa_weeks.values())
    static['fortifying-atlas'] = {
        'key':          'fortifying-atlas',
        'title':        'Fortifying ATLAS',
        'type':         'limited_time_event',
        'url':          '/df/mini-seasons/fortifying-atlas/challenge-checklist/',
        'start_date':   '2020-08-04',
        'end_date':     '2020-09-10',
        **fa_weeks,
        'cut':          [],
        'total_live':   fa_total,
        'total_cut':    0,
        'rewards':      [],
        'gallery':      [],
        'group_labels': {
            'week1': 'Project Alpha (August 4–18)',
            'week2': 'Project Bravo (August 27 – September 10)',
        },
    }

    return static


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
        re.search(r'_ChallengeComplete_', clean) or  # Summer Sock Hop weekly rollups (no _Week#_ marker)
        re.search(r'_META$', clean)
    )


def infer_week(edid, row, week_map):
    clean = re.sub(r'^(ZZZ_|CUT_|DEL_)', '', edid)

    # Manual overrides take priority (for events without week EDID patterns)
    if clean in MANUAL_WEEK_OVERRIDES:
        return MANUAL_WEEK_OVERRIDES[clean]

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
# Manual week overrides (for events where devs didn't use
# _Week1_/_Week2_ EDID patterns)
# ─────────────────────────────────────────────────────────────

MANUAL_WEEK_OVERRIDES = {
    # Weapons Expert — Week 1
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Shotgun':         'week1',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Archaic_Bow':    'week1',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Thrown':          'week1',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Energy':          'week1',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Explosive':       'week1',
    # Weapons Expert — Week 2
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Heavy':           'week2',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Melee1h':         'week2',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Melee2h':         'week2',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_MeleeAutomatic':  'week2',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Pistol':          'week2',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Unarmed':         'week2',
    # Weapons Expert — Bonus (two-week)
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Ranged':          'bonus',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Any':             'bonus',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_Ballistic':       'bonus',
    'ATX_DE2026_WeaponsExpert_Challenge_Deal_Damage_MeleeAny':        'bonus',

    # Summer Sock Hop — Week 1 (from ChallengeComplete_Emote_All_Locations GetIsForm refs)
    'DE2026_SockHop_Challenge_Emote_SandySockHop_Location':           'week1',
    'DE2026_SockHop_Challenge_Emote_CamdenPark_Location':             'week1',
    'DE2026_SockHop_Challenge_Emote_BerkeleySprings_Location':        'week1',
    'DE2026_SockHop_Challenge_Emote_BlackBearLodge_Location':         'week1',
    'DE2026_SockHop_Challenge_Emote_CAMP_Other':                      'week1',
    'DE2026_SockHop_Challenge_Emote_Event_Activity':                  'week1',
    'DE2026_SockHop_Challenge_Kill_Blood_Eagle':                      'week1',
    # Summer Sock Hop — Week 2 (from ChallengeComplete_Other GetIsForm refs)
    'DE2026_SockHop_Challenge_Emote_CAMP_Own':                        'week2',
    'DE2026_SockHop_Challenge_Emote_MakeoutPoint_Location':           'week2',
    'DE2026_SockHop_Challenge_Emote_MorgantownHighSchoolDungeon_Location': 'week2',
    'DE2026_SockHop_Challenge_Emote_Shelter_Other':                   'week2',
    'DE2026_SockHop_Challenge_Emote_SunnytopSkiLanes_Location':       'week2',
    'DE2026_SockHop_Challenge_Emote_WatogaHighSchoolDungeon_Location': 'week2',
    'DE2026_SockHop_Challenge_Kill_RustRaider':                       'week2',
}


# ─────────────────────────────────────────────────────────────
# Keyword item lookup (KYWD Refs TSV)
# ─────────────────────────────────────────────────────────────

# Prefixes that indicate creature/NPC/cut/internal/debug weapons
_KYWD_SKIP_PREFIXES = (
    'cr',  'zzz', 'ZZZ', 'DEL', 'del', 'CUT', 'cut',
    'CharGen', 'POST', 'debug', 'Debug', 'DEPRECATED',
    'Test_', 'test_',
    'Creature_',        # creature-only weapons (Candy Cane, Slay Bells, etc.)
    'Burn_cr',          # creature attacks (Rad Scorpion sting, Sting Wing etc.)
    'DailyOps_cr', 'Daily_Ops_cr',  # Daily Ops creature variants
)
# Substrings anywhere in the EDID that indicate non-player items
_KYWD_SKIP_CONTAINS = (
    'NONPLAYABLE', 'Vertibird', 'Workshop_Trap', 'WorkshopTrap',
    'Workshop_Artillery', 'WorkshopArtillery',
    'OrbitalStrike', 'Orbital_Strike',
    'Invaders_Missile', 'InvadersMissile',
    'Muni_Turret', 'MuniTurret', 'Turret_Mounted',
    'EN02_', 'SFZ14_', 'V96_', 'LC096_',
    'AC_MQ02', 'W05_MQ', 'W05_COMP', 'W05_Minigun',
    'DLC05Workshop', 'Firework_Weapon', 'FireworkWeapon',
    'Firework_Mine', 'DLC05_Firework',
    '_cr_', 'P62_cr', 'RD01_cr', 'HTO_cr',
    'Unarmed_Human', 'UnarmedHuman',
    'Unarmed_Power', 'UnarmedPower',
    'Pain_Train', 'PainTrain',
    'Creature_Holiday',   # Holiday creature weapons
    'SubGraphData',       # RACE sub-graph data, not weapons
    '_Platform_Gun', 'PlatformGun',  # Enclave event platform guns
    'alienprobe',         # internal alien probe weapon
    'meltdown',           # internal meltdown weapon
    'NoName',             # CharGen items with no display name
    'Liberator',          # Liberator robot weapons
    'EyeBot', 'Eyebot',  # Eyebot robot weapons
    'Turret_',            # Turret-mounted weapons
    'JerseyDevil', 'Jersey_Devil',  # Creature attack
    '_PA_', 'PowerArmor_Gun',  # Power Armor dual-wield gun variants
    'PA_T',               # PA turret/gun
    'Bomb_Weapon',        # Internal bomb weapon
)


def load_keyword_refs(tsv_root):
    """Load KYWD_Export_*_Refs.tsv → dict[kywd_formid] → list of {edid, sig, name}.

    The 'name' field is the FULL - Name (in-game display name) from the referenced
    record, added in the Apr 2026 xEdit script update.  Older TSVs without the
    RefName column will have name=''.
    """
    refs_files = glob.glob(os.path.join(tsv_root, 'KYWD_Export_*_Refs.tsv'))
    if not refs_files:
        return {}

    # Pick the newest file by modification time (alphabetical sort fails on
    # month names like "Apr" vs "March").
    refs_file = max(refs_files, key=os.path.getmtime)
    lookup = {}
    # xEdit saves in ANSI (Windows-1252 / cp1252).  Use cp1252 which is a
    # superset of latin-1 and handles accented characters (e.g. "Michellé").
    with open(refs_file, newline='', encoding='cp1252') as fh:
        reader = csv.DictReader(fh, delimiter='\t')
        for row in reader:
            kid = row.get('KeywordFormID', '').strip()
            ref_edid = row.get('RefEDID', '').strip()
            ref_sig = row.get('RefSignature', '').strip()
            ref_name = row.get('RefName', '').strip()  # display name (may be empty on old TSVs)
            if not kid or not ref_edid:
                continue
            lookup.setdefault(kid, []).append({
                'edid': ref_edid,
                'sig':  ref_sig,
                'name': ref_name,
            })

    return lookup


def prettify_edid(edid):
    """Turn an EDID like 'DLC03_PoleHook' into 'Pole Hook'."""
    # Strip common prefixes (DLC##_, ATX_, E##X_, DN###_, XPD_xxx_, SCORE_S##_,
    # MTNL##_, MTNS##_, MTNM##_, MTR##_, Mo_M_, Joey_Bello_, Survival_)
    s = re.sub(
        r'^(DLC\d+_|ATX_|atx_|E\d+[A-Z]?_|DN\d+_|XPD_\w+_|SCORE_S\d+_'
        r'|MTNL?\d+_|MTNS\d+_|MTNM\d+_|MTR\d+_|Mo_M\d*_'
        r'|Joey_Bello_|Survival_|Burn_cr_)',
        '', edid
    )
    # Split on underscores and CamelCase
    s = s.replace('_', ' ')
    s = re.sub(r'([a-z])([A-Z])', r'\1 \2', s)
    # Fix double-leading-consonant artifacts (e.g. CCrossbow → Crossbow)
    s = re.sub(r'\b([A-Z])\1([a-z])', r'\1\2', s)
    # Remove leading "gauss" duplication like "gaussshotgun" → "Gauss Shotgun"
    s = re.sub(r'\bgauss([a-z])', lambda m: 'Gauss ' + m.group(1).upper(), s, flags=re.IGNORECASE)
    # Collapse multiple spaces
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# Signatures worth listing (player-facing items/creatures/objects).
# RACE removed — RACE refs on weapon keywords are unarmed attack definitions,
# not player-facing items (e.g. "Human Race", "Power Armor Race").
_KYWD_ALLOWED_SIGS = {'WEAP', 'ALCH', 'NPC_', 'FLOR', 'MISC', 'ARMO', 'BOOK'}

# Item-type signatures that MUST have a display name (FULL - Name) to be listed.
# If the RefName column is empty for these sigs, the record is internal/non-player.
# NPC_ is excluded because NPCs/creatures often lack FULL names.
_KYWD_REQUIRE_NAME_SIGS = {'WEAP', 'ALCH', 'FLOR', 'MISC', 'ARMO', 'BOOK'}

# Display-name substrings that indicate non-player items (checked after name resolution).
# Catches NPC/creature/turret weapons that have valid FULL names but aren't player-usable.
_KYWD_SKIP_DISPLAY_CONTAINS = (
    '[Left]', '[Right]',            # Power Armor dual-wield gun variants
    'Turret',                       # Turret-mounted weapons
    'Liberator',                    # Liberator robot weapons
    'EyeBot', 'Eyebot',            # Eyebot robot weapons
    'Jersey Devil',                 # Creature attack
    'Charge Attack',                # Creature charge attacks
    'Thirst Zapper',                # Joke weapon (NW only)
)

# Keywords where listing refs is not useful (cosmetics, CAMP categories, buffs, etc.)
_KYWD_SKIP_NAMES = {
    'costume', 'flower crown', 'tough love helmet',
    'marshal mallow', 'sunset sarsaparilla deputy', 'moneybag',
    'floors', 'walls', 'roofs', 'lights', 'shelves', 'displays',
    'floor decor', 'wall decor', 'stash boxes', 'generators',
    'power connectors', 'water',
}


# Condition-function → keyword(s) fallback map.
#
# Some Fallout 76 challenge conditions use hard-coded game functions instead
# of keyword filters, so they have no inline [KYWD:xxxxxxxx] tag for the
# resolver to latch onto. Example:
#
#     Subject.CHAL_IsTargetWeaponThrown = 1
#
# For these, map the function name → (pill label, keyword FormID) and the
# resolver will synthesise a matching condition so keyword_items gets
# populated as if the challenge had referenced the keyword directly.
#
# Each function can map to multiple keywords if needed (e.g. a challenge
# that covers "Thrown OR Bow" would get entries for both).
_COND_FN_KEYWORDS = {
    # Deal Thrown Weapon Damage — Weapons Expert mini-season.
    # WeaponTypeThrowingKnife covers Tomahawk, Throwing Knife, Meat Cleaver,
    # Sheepsquatch Shard (and cr- creature variants, deduped by display name).
    'CHAL_IsTargetWeaponThrown': [
        ('Thrown', '003879A4'),  # WeaponTypeThrowingKnife
    ],
}


def _is_player_item(edid):
    """Return True if this EDID looks like a player-relevant item."""
    if any(edid.startswith(p) for p in _KYWD_SKIP_PREFIXES):
        return False
    if any(sub in edid for sub in _KYWD_SKIP_CONTAINS):
        return False
    return True


def resolve_keyword_items(raw_conditions, kywd_lookup):
    """For each KYWD reference in conditions, return {keyword_name: [item_names]}.
    Deduplicates by prettified name (case-insensitive).
    Works for weapons, food, creatures, armour — any keyword type.

    Raw condition formats:
      Pipe-separated: ...WeaponTypeArchaic "Archaic" [KYWD:0033AB23]...
      Pre-parsed:     ...DoesTargetWeaponHaveKeyword(Archaic [KYWD:0033AB23]) = 1

    Hard-coded condition functions (e.g. CHAL_IsTargetWeaponThrown) don't
    contain a [KYWD:xxxx] tag. For those, _COND_FN_KEYWORDS supplies the
    keyword(s) explicitly — we synthesise a matching condition string so the
    regex below can treat them uniformly.
    """
    # Synthesise [KYWD:xxxx]-style conditions for known hard-coded functions.
    # Only fires when the function name is actually present in a raw condition,
    # so unrelated challenges are unaffected.
    conds = list(raw_conditions)
    for c in raw_conditions:
        for fn_name, fn_kws in _COND_FN_KEYWORDS.items():
            if fn_name in c:
                for label, kw_fid in fn_kws:
                    conds.append(f'CHAL_DoesTargetWeaponHaveKeyword("{label}" [KYWD:{kw_fid}]) = 1')

    result = {}
    for c in conds:
        # Skip non-keyword conditions
        if 'KYWD:' not in c:
            continue
        # Skip IsFalloutWorlds, HasMagicEffectKeyword (buff checks, not item types)
        if 'IsFalloutWorlds' in c or 'HasMagicEffectKeyword' in c:
            continue

        # Match keywords in conditions.  Two formats:
        #   Quoted:   "Archaic" [KYWD:0033AB23]  or  WeaponTypeArchaic "Archaic" [KYWD:0033AB23]
        #   Unquoted: Archaic [KYWD:0033AB23]  or  1-Hand Melee [KYWD:0004A0A4]
        # Try quoted first (more specific), then unquoted as fallback.
        kw_matches = list(re.finditer(r'"([^"]+)"\s*\[KYWD:([0-9A-Fa-f]+)\]', c))
        if not kw_matches:
            # Unquoted: capture everything before [KYWD:...] that looks like a name
            # e.g.  "DoesTargetWeaponHaveKeyword(Archaic [KYWD:0033AB23])"
            #        "WornHasKeyword(1-Hand Melee [KYWD:0004A0A4])"
            kw_matches = list(re.finditer(r'(?:[(,]\s*|[|]\s*)([A-Za-z0-9][A-Za-z0-9 _-]*?)\s*\[KYWD:([0-9A-Fa-f]+)\]', c))
        for m in kw_matches:
            kw_name = m.group(1).strip()
            kw_fid = m.group(2).strip()

            # Skip cosmetic/CAMP/non-useful keywords
            if kw_name.lower() in _KYWD_SKIP_NAMES:
                continue

            if kw_fid not in kywd_lookup:
                continue

            # Filter to player-facing signatures, skip internal items, deduplicate.
            # Use the in-game display name (RefName) when available; fall back to
            # prettify_edid() for old TSVs without the RefName column.
            # For item signatures (WEAP, ALCH, FLOR, MISC, ARMO, BOOK), skip refs
            # that have no display name — they're internal/non-player records.
            items = []
            seen = set()
            for ref in kywd_lookup[kw_fid]:
                if ref['sig'] not in _KYWD_ALLOWED_SIGS:
                    continue
                if not _is_player_item(ref['edid']):
                    continue

                display_name = ref.get('name', '').strip()

                # If the TSV has the RefName column: use it, and require it for
                # item-type signatures (weapons, food, flora, armour, etc.).
                if display_name:
                    pretty = display_name
                elif ref['sig'] in _KYWD_REQUIRE_NAME_SIGS:
                    # No display name → internal/non-player record → skip
                    continue
                else:
                    # NPC_ / RACE may not have FULL names; fall back to EDID
                    pretty = prettify_edid(ref['edid'])

                # Skip display names that indicate NPC/creature/turret weapons
                if any(sub in pretty for sub in _KYWD_SKIP_DISPLAY_CONTAINS):
                    continue

                key = pretty.lower()
                if key not in seen:
                    seen.add(key)
                    items.append(pretty)

            if items:
                items.sort(key=str.lower)
                result[kw_name] = items

    return result


# ─────────────────────────────────────────────────────────────
# ENTM reward loading
# ─────────────────────────────────────────────────────────────

def load_entm_rewards(tsv_root):
    """Load ENTM TSV and match rewards to events using ENTM_PATTERNS.

    Returns dict[event_key] → list of {name, description, edid, image_url}.
    Image URLs use .avif format under /wp-content/uploads/guide-images/mini-seasons/.
    """
    entm_files = sorted(glob.glob(os.path.join(tsv_root, 'ENTM_Export_*.tsv')))
    if not entm_files:
        print("  WARNING: No ENTM_Export_*.tsv found, skipping reward loading")
        return {}

    # Load ALL ENTM files and merge by EDID (newest wins, same as CHAL loading).
    # Using only the most-recently-modified file broke on GitHub Actions where
    # checkout sets every file to the same mtime, so the picked file was arbitrary
    # and often missed newer event rewards (e.g. WeaponsExpert in Apr but CI
    # picked the March file).
    all_rows = []
    seen_edids = set()
    for entm_file in entm_files:
        print(f"  Loading ENTM rewards from {os.path.basename(entm_file)}")
        with open(entm_file, 'r', encoding='utf-8-sig', errors='replace') as fh:
            rdr = csv.DictReader(fh, delimiter='\t')
            for row in rdr:
                edid = (row.get('EDID', '') or '').strip()
                if not edid:
                    continue
                if edid not in seen_edids:
                    all_rows.append(row)
                    seen_edids.add(edid)

    # Match rows to events
    rewards_by_event = {}
    for key, patterns in ENTM_PATTERNS.items():
        if not patterns:
            rewards_by_event[key] = []
            continue

        matched = []
        for row in all_rows:
            edid = (row.get('EDID', '') or '').strip()
            # Skip ZZZ_ (cut/deprecated) entries
            if edid.startswith('ZZZ_') or edid.startswith('zzz_'):
                continue
            for pat in patterns:
                if re.search(pat, edid):
                    full = (row.get('FULL', '') or '').strip()
                    desc = (row.get('DESC', '') or '').strip()
                    nnam = (row.get('NNAM', '') or '').strip()

                    # Build image URL from EDID (check overrides first)
                    img_edid = edid.lower()
                    img_edid = img_edid.replace('_entm_', '_')
                    override_map = REWARD_IMAGE_OVERRIDES.get(key, {})
                    if img_edid in override_map:
                        image_url = override_map[img_edid]
                    else:
                        subdir = EVENT_IMAGE_SUBDIR.get(key, '')
                        sub = f"{subdir}/" if subdir else ''
                        image_url = f"/wp-content/uploads/guide-images/mini-seasons/{sub}{img_edid}.avif"

                    # Derive display name — title rewards get suffix like
                    # "Arsenal Suffix Camp Title" per user convention.
                    display_name = full or nnam or edid
                    title_m = re.search(
                        r'(PlayerTitles|CAMPTitles)_(Prefix|Suffix)_',
                        edid, re.IGNORECASE)
                    if title_m:
                        kind = 'Player' if 'player' in title_m.group(1).lower() else 'Camp'
                        fix  = title_m.group(2).capitalize()   # Prefix or Suffix
                        display_name = f"{display_name} {fix} {kind} Title"

                    matched.append({
                        'name':        display_name,
                        'description': desc,
                        'edid':        edid,
                        'form_id':     (row.get('FormID', '') or '').strip().rstrip('\r'),
                        'image_url':   image_url,
                    })
                    break  # Don't double-match same row

        rewards_by_event[key] = matched

    # Sort mini season rewards alphabetically by name
    for key in rewards_by_event:
        if key in EVENT_DEFS and EVENT_DEFS[key].get('type') == 'mini_season':
            rewards_by_event[key].sort(key=lambda r: r['name'].lower())

    total = sum(len(v) for v in rewards_by_event.values())
    non_empty = sum(1 for v in rewards_by_event.values() if v)
    print(f"  ENTM rewards: {total} items across {non_empty} events")
    return rewards_by_event


# ─────────────────────────────────────────────────────────────
# LTE per-challenge reward extraction
# ─────────────────────────────────────────────────────────────

# Consumable rewards embedded in CHAL EDID suffixes.
# Maps suffix → display name for the reward item.
CONSUMABLE_REWARD_MAP = {
    'PerkPack':        'Perk Card Pack',
    'Lunchbox':        'Lunchbox',
    'LegendaryModule': 'Legendary Module',
    'RepairKit':       'Improved Repair Kit',
    'CarryBooster':    'Carry Weight Booster',
    'ScrapKit':        'Scrap Kit',
}


def extract_challenge_reward(edid, entm_rewards_for_event):
    """Extract the per-challenge reward for an LTE challenge.

    For daily challenges: extracts consumable reward from EDID suffix.
    For milestone/completion challenges: attempts to match ENTM cosmetics.

    Returns dict with {type, name, description, image_url, edid} or None.
    """
    # 1) Check for consumable reward suffix on dailies
    parts = edid.rsplit('_', 1)
    if len(parts) == 2:
        suffix = parts[1]
        if suffix in CONSUMABLE_REWARD_MAP:
            return {
                'type':        'consumable',
                'name':        CONSUMABLE_REWARD_MAP[suffix],
                'description': '',
                'image_url':   '',
                'edid':        '',
            }

    # 2) For completion challenges, no automatic ENTM mapping
    #    (the event-level rewards section already shows all ENTM items)
    return None


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tsv-root', default='tsv', help='Dir containing CHAL_Export_*.tsv')
    parser.add_argument('--outdir', default='dist/mini_seasons', help='Output directory')
    parser.add_argument('--guide-index', default='tsv/guide_index.tsv',
                        help='Path to guide_index.tsv for guide linking')
    args = parser.parse_args()

    # Load CNDF condition-forms first so conditions can be leaf-expanded.
    _load_cndf(args.tsv_root)
    print(f"  CNDF: {len(_CNDF_BY_FID)} condition-forms loaded")

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
            if edid not in merged and ('WeaponsExpert' in edid or 'SockHop' in edid):
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

    # Load guide index for guide linking
    guide_lookup = load_guide_index(args.guide_index)

    # Load keyword refs for keyword item resolution
    kywd_lookup = load_keyword_refs(args.tsv_root)
    print(f"  Keyword refs: {len(kywd_lookup)} keywords loaded")

    # Load ENTM rewards for each event
    entm_rewards = load_entm_rewards(args.tsv_root)

    # Diagnostic: count keyword-bearing conditions in merged data
    _kw_diag_total = 0
    _kw_diag_with_kywd = 0
    for _de, _dr in merged.items():
        _kw_diag_total += 1
        if any('KYWD:' in c for c in _dr['raw_conditions']):
            _kw_diag_with_kywd += 1
    print(f"  Keyword diagnostics: {_kw_diag_with_kywd}/{_kw_diag_total} merged challenges have KYWD in raw_conditions")

    # Build output
    guide_link_stats = {'linked': 0, 'total': 0}
    kw_items_stats = {'with': 0, 'without': 0}
    output = {}
    for key, evdef in EVENT_DEFS.items():
        live, cut = [], []
        for edid, row in sorted(merged.items()):
            ev_key, is_cut = classify(edid)
            if ev_key != key:
                continue
            if edid.startswith('SCORE_') and not any(k in edid for k in ['FreeCam', 'St_Patrick', 'BigBloom']):
                continue

            # Resolve guide links from raw conditions (has quoted names + FormIDs)
            guide_links = resolve_guide_links(row['raw_conditions'], guide_lookup, row['full'])
            guide_link_stats['total'] += 1
            if guide_links:
                guide_link_stats['linked'] += 1

            # Resolve keyword items (e.g. Archaic → list of player weapons)
            kw_items = resolve_keyword_items(row['raw_conditions'], kywd_lookup)
            if kw_items:
                kw_items_stats['with'] += 1
            else:
                kw_items_stats['without'] += 1

            entry = {
                'id':            row['form_id'],
                'edid':          row['edid'],
                'name':          row['full'],
                'snam':          row['snam'],
                'required':      row['tnam'],
                'scope':         row['cnam'],
                'category':      row['enam'],
                'conditions':    row['conditions'],
                'guide_links':   guide_links,
                'keyword_items': kw_items,
                'week':          infer_week(edid, row, week_map),
                'is_cut':        is_cut,
                'is_completion': is_completion(edid),
            }

            # For LTE events, attach per-challenge reward
            if evdef.get('type') == 'limited_time_event':
                chal_reward = extract_challenge_reward(
                    edid, entm_rewards.get(key, []))
                if chal_reward:
                    entry['reward'] = chal_reward
            (cut if is_cut else live).append(entry)

        ev_out = {
            'key':        key,
            'title':      evdef['title'],
            'type':       evdef.get('type', 'limited_time_event'),
            'url':        evdef['url'],
            'start_date': evdef.get('start_date', 'TBA'),
            'end_date':   evdef.get('end_date', 'TBA'),
        }
        # Dynamically add week1–week8 if they have challenges
        for wi in range(1, 9):
            wk = f'week{wi}'
            wk_list = [c for c in live if c['week'] == wk]
            if wk_list:
                ev_out[wk] = wk_list
        # Always include week1/week2 even if empty (for consistency with existing pages)
        ev_out.setdefault('week1', [])
        ev_out.setdefault('week2', [])
        # Bonus and cut
        bonus_list = [c for c in live if c['week'] == 'bonus']
        if bonus_list:
            ev_out['bonus'] = bonus_list
        ev_out['cut']        = cut
        ev_out['total_live'] = len(live)
        ev_out['total_cut']  = len(cut)
        ev_out['rewards']    = entm_rewards.get(key, [])
        ev_out['gallery']    = GALLERY_IMAGES.get(key, [])
        # Custom group labels (if defined in event def)
        if evdef.get('group_labels'):
            ev_out['group_labels'] = evdef['group_labels']
        output[key] = ev_out

    # ── Inject static events (not in game files) ──────────────────
    for skey, sdata in _build_static_events().items():
        output[skey] = sdata

    # ── Apply static ticket + LTE reward overlay ─────────────────
    # Ticket values (mini seasons) and per-challenge item rewards (LTEs) are
    # not in the game files — they're maintained by hand in
    # src/mini_seasons_tickets.py. Merge them in now so they land in dist JSON.
    overlay_stats = apply_ticket_overlay(output)
    print(f"  Static overlay applied: {overlay_stats}")

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
        w1 = len(ev.get('week1', [])); w2 = len(ev.get('week2', []))
        # Count all extra weeks (week3–week8) into w2 for summary display
        for wi in range(3, 9):
            w2 += len(ev.get(f'week{wi}', []))
        b  = len(ev.get('bonus', [])); c  = len(ev['cut'])
        g['w1'] += w1; g['w2'] += w2; g['b'] += b; g['c'] += c
        print(f"  {ev['title']:<48} {w1:>3} {w2:>3} {b:>3} {c:>3} {w1+w2+b+c:>3}")
    print('  ' + '-' * 66)
    t = g['w1'] + g['w2'] + g['b'] + g['c']
    print(f"  {'TOTAL':<48} {g['w1']:>3} {g['w2']:>3} {g['b']:>3} {g['c']:>3} {t:>3}")
    print(f"\n  {len(output)} events")
    print(f"  Guide links: {guide_link_stats['linked']}/{guide_link_stats['total']} challenges linked")
    print(f"  Keyword pills: {kw_items_stats['with']} challenges with keyword_items, {kw_items_stats['without']} without")

    # Reward stats
    reward_total = sum(len(ev['rewards']) for ev in output.values())
    reward_events = sum(1 for ev in output.values() if ev['rewards'])
    gallery_events = sum(1 for ev in output.values() if ev['gallery'])
    print(f"  Rewards: {reward_total} items across {reward_events} events")
    print(f"  Gallery: {gallery_events} events with gallery images")


if __name__ == '__main__':
    main()
