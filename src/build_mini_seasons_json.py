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
    # ── Mini Seasons (ticket system: complete tasks → earn tickets → buy rewards) ──
    ('love-hurts',          {'title': 'Love Hurts',                                      'url': '/df/mini-seasons/love-hurts/challenge-checklist/',                                   'patterns': [r'ATX_DE2025_LoveHurts'],                'type': 'mini_season'}),
    ('sunset-stranger',     {'title': 'Sunset Stranger',                                  'url': '/df/mini-seasons/sunset-stranger/challenge-checklist/',                               'patterns': [r'ATX_DE2025_SunsetStranger'],            'type': 'mini_season'}),
    ('night-at-the-morgue', {'title': 'Night at the Morgue',                              'url': '/df/mini-seasons/night-at-the-morgue/challenge-checklist/',                           'patterns': [r'ATX_DE2025_Halloween'],                 'type': 'mini_season'}),
    ('marshal-mallows',     {'title': "Marshal Mallow's Marvelous Fishing Excursion",     'url': '/df/mini-seasons/marshal-mallows-marvelous-fishing-excursion/challenge-checklist/',    'patterns': [r'ATX_DE2025_MMMFE'],                     'type': 'mini_season'}),
    ('appalachian-outlaws',  {'title': 'Appalachian Outlaws',                              'url': '/df/mini-seasons/appalachian-outlaws/challenge-checklist/',                           'patterns': [r'ATX_DE2025_AppalachianOutlaws'],        'type': 'mini_season'}),
    ('science-of-love',      {'title': 'Science of Love',                                  'url': '/df/mini-seasons/science-of-love/challenge-checklist/',                               'patterns': [r'ATX_DE2024_ScienceOfLove'],             'type': 'mini_season'}),
    ('uncharted-scouts',     {'title': 'Uncharted Scouts',                                 'url': '/df/mini-seasons/uncharted-scouts/challenge-checklist/',                              'patterns': [r'ATX_DE2024_UnchartedScouts'],           'type': 'mini_season'}),
    ('spring-cleaning',      {'title': 'Spring Cleaning',                                  'url': '/df/mini-seasons/spring-cleaning/challenge-checklist/',                               'patterns': [r'ATX_DE2024_SpringCleaning'],            'type': 'mini_season'}),
    ('burning-love',         {'title': 'Burning Love',                                     'url': '/df/mini-seasons/burning-love/challenge-checklist/',                                  'patterns': [r'ATX_DE2024_Valentines'],                'type': 'mini_season'}),
    ('weapons-expert',       {'title': "Rip Daring's Weapons Expert Extraordinaire",       'url': '/df/mini-seasons/rip-daring-weapons-expert-extraordinaire/challenge-checklist/',      'patterns': [r'ATX_DE2026_WeaponsExpert'],             'type': 'mini_season'}),

    # ── Limited Time Events (complete task → get reward directly) ──
    ('birthday',             {'title': 'Birthday Challenge',                               'url': '/df/mini-seasons/birthday-challenge/challenge-checklist/',                            'patterns': [r'ATX_DE2023_Birthday'],                  'type': 'limited_time_event'}),
    ('summer-camp',          {'title': 'Summer Camp',                                      'url': '/df/mini-seasons/summer-camp/challenge-checklist/',                                   'patterns': [r'ATX_DE2023_SummerCamp'],                'type': 'limited_time_event'}),
    ('rip-daring',           {'title': 'Rip Daring and the Cryptid Hunt',                  'url': '/df/mini-seasons/rip-daring-and-the-cryptid-hunt/challenge-checklist/',               'patterns': [r'ATX_DE2023_RipDaring'],                 'type': 'limited_time_event'}),
    ('call-to-axe-ion',      {'title': 'Call to Axe-ion',                                  'url': '/df/mini-seasons/call-to-axe-ion/challenge-checklist/',                               'patterns': [r'ATX_DE2022_Pitt'],                      'type': 'limited_time_event'}),
    ('anniversary',          {'title': 'Anniversary',                                      'url': '/df/mini-seasons/anniversary/challenge-checklist/',                                   'patterns': [r'ATX_DE2022_Anniversary'],               'type': 'limited_time_event'}),
    ('nuka-connoisseur',     {'title': 'Nuka Connoisseur',                                 'url': '/df/mini-seasons/nuka-connoisseur/challenge-checklist/',                              'patterns': [r'ATX_DE2022_NukaWorld'],                 'type': 'limited_time_event'}),
    ('the-coming-storm',     {'title': 'The Coming Storm',                                 'url': '/df/mini-seasons/the-coming-storm/challenge-checklist/',                              'patterns': [r'ATX_DE2021_BoS'],                       'type': 'limited_time_event'}),
    ('spread-the-love-2021', {'title': 'Spread the Love 2021',                             'url': '/df/mini-seasons/spread-the-love/2021/challenge-checklist/',                          'patterns': [r'ATX_DE2021_Love'],                      'type': 'limited_time_event'}),
    ('spread-the-love-2023', {'title': 'Spread the Love 2023',                             'url': '/df/mini-seasons/spread-the-love/2023/challenge-checklist/',                          'patterns': [r'ATX_DE2023_Valentines'],                'type': 'limited_time_event'}),
    ('free-cam',             {'title': 'Free Cam Challenge',                               'url': '/df/mini-seasons/free-cam-challenge/challenge-checklist/',                            'patterns': [r'ATX_DE2022.*FreeCam', r'SCORE.*FreeCam'], 'type': 'limited_time_event'}),
    ('st-patricks-day',      {'title': "St Patrick's Day Challenge",                       'url': '/df/mini-seasons/st-patricks-day-challenge/challenge-checklist/',                     'patterns': [r'SCORE.*St_Patrick'],                    'type': 'limited_time_event'}),
    ('big-bloom',            {'title': 'The Big Bloom Challenge',                          'url': '/df/mini-seasons/big-bloom-challenge/challenge-checklist/',                           'patterns': [r'ATX_DE2024.*BigBloom', r'SCORE.*BigBloom'], 'type': 'limited_time_event'}),
    ('halloween-2021',       {'title': 'Halloween 2021',                                   'url': '/df/mini-seasons/halloween/2021/challenge-checklist/',                                'patterns': [r'ATX_DE2021_Halloween'],                 'type': 'limited_time_event'}),
    ('halloween-2022',       {'title': 'Halloween 2022',                                   'url': '/df/mini-seasons/halloween/2022/challenge-checklist/',                                'patterns': [r'ATX_DE2022_Halloween'],                 'type': 'limited_time_event'}),
    ('halloween-2023',       {'title': 'Halloween 2023',                                   'url': '/df/mini-seasons/halloween/2023/challenge-checklist/',                                'patterns': [r'ATX_DE2023_Halloween'],                 'type': 'limited_time_event'}),
    ('halloween-2024',       {'title': 'Halloween 2024',                                   'url': '/df/mini-seasons/halloween/2024/challenge-checklist/',                                'patterns': [r'ATX_DE2024_Halloween'],                 'type': 'limited_time_event'}),
])


# ENTM EDID patterns for matching rewards to events.
# These differ from the CHAL patterns because ENTM naming is inconsistent.
# Each event key maps to a list of regex patterns that match its ENTM EDIDs.
ENTM_PATTERNS = {
    # ── Mini Seasons ──
    'love-hurts':          [r'(?i)SCORE_MiniSeason.*LoveHurts'],
    'sunset-stranger':     [r'(?i)SCORE_MiniSeason_2025_SunsetStranger'],
    'night-at-the-morgue': [r'(?i)SCORE_MiniSeason_2025_NightAtTheMorgue'],
    'marshal-mallows':     [r'(?i)SCORE_MiniSeason_2025_MMMFE'],
    'appalachian-outlaws':  [r'(?i)SCORE_MiniSeason.*AppalachianOutlaws'],
    'science-of-love':      [r'(?i)DE2024_ScienceOfLove', r'(?i)DE2024_Scienceoflove'],
    'uncharted-scouts':     [],  # No ENTM entries found
    'spring-cleaning':      [r'(?i)DE2024_SpringCleaning'],
    'burning-love':         [],  # No ENTM entries found
    'weapons-expert':       [r'(?i)SCORE_MiniSeason_2026_WeaponsExpert'],

    # ── Limited Time Events ──
    'birthday':             [r'(?i)ATX_ENTM.*Birthday', r'(?i)SCORE_S14_ENTM.*Birthday'],
    'summer-camp':          [r'(?i)DE2023_SummerCamp'],
    'rip-daring':           [r'(?i)ATX_ENTM_CAMP_Bed_RipDaringEvent'],
    'call-to-axe-ion':      [r'(?i)ATX_DE2022_Pitt_ENTM', r'(?i)ATX_Upgrade2022_Pitt_ENTM'],
    'anniversary':          [r'(?i)ATX_ENTM_AnniversaryEvent'],
    'nuka-connoisseur':     [],  # No dedicated ENTM entries
    'the-coming-storm':     [],  # No ENTM entries found
    'spread-the-love-2021': [r'(?i)ATX_DE2021_Love_ENTM'],
    'spread-the-love-2023': [],  # No ENTM entries found
    'free-cam':             [],  # No ENTM entries found
    'st-patricks-day':      [],  # No ENTM entries found
    'big-bloom':            [],  # No ENTM entries found
    'halloween-2021':       [],  # No ENTM entries found
    'halloween-2022':       [],  # No ENTM entries found
    'halloween-2023':       [],  # No ENTM entries found
    'halloween-2024':       [r'(?i)ATX_DE2024_Halloween_ENTM'],
}

# Gallery images: manually maintained per-event.
# Each entry is a dict with 'url' (full path on site) and 'alt' (description).
GALLERY_IMAGES = {
    'weapons-expert': [
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/76_ATX_PROMOTE_P68_S24_MS1_NOTAG - Copy.avif', 'alt': 'Weapons Expert Promotion'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/s24_miniseason1_background - Copy.avif',        'alt': 'Weapons Expert Background'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/s24_miniseason1_marquee - Copy.avif',           'alt': 'Weapons Expert Marquee'},
        {'url': '/wp-content/uploads/guide-images/mini-seasons/rip-darling-weapons-expert/s24_miniseason1_seasonselection - Copy.avif',   'alt': 'Weapons Expert Season Selection'},
    ],
}


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
# Keyword item lookup (KYWD Refs TSV)
# ─────────────────────────────────────────────────────────────

# Prefixes that indicate creature/NPC/cut/internal/debug weapons
_KYWD_SKIP_PREFIXES = (
    'cr',  'zzz', 'ZZZ', 'DEL', 'del', 'CUT', 'cut',
    'CharGen', 'POST', 'debug', 'Debug', 'DEPRECATED',
    'Test_', 'test_',
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
    '_cr_', 'P62_cr', 'RD01_cr',
    'Unarmed_Human', 'UnarmedHuman',
    'Unarmed_Power', 'UnarmedPower',
    'Pain_Train', 'PainTrain',
)


def load_keyword_refs(tsv_root):
    """Load KYWD_Export_*_Refs.tsv → dict[kywd_formid] → list of {edid, sig}."""
    refs_files = sorted(glob.glob(os.path.join(tsv_root, 'KYWD_Export_*_Refs.tsv')))
    if not refs_files:
        return {}

    refs_file = refs_files[-1]
    lookup = {}
    with open(refs_file, newline='', encoding='utf-8-sig') as fh:
        reader = csv.DictReader(fh, delimiter='\t')
        for row in reader:
            kid = row.get('KeywordFormID', '').strip()
            ref_edid = row.get('RefEDID', '').strip()
            ref_sig = row.get('RefSignature', '').strip()
            if not kid or not ref_edid:
                continue
            lookup.setdefault(kid, []).append({
                'edid': ref_edid,
                'sig':  ref_sig,
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


# Signatures worth listing (player-facing items/creatures/objects)
_KYWD_ALLOWED_SIGS = {'WEAP', 'ALCH', 'NPC_', 'FLOR', 'MISC', 'RACE', 'ARMO', 'BOOK'}

# Keywords where listing refs is not useful (cosmetics, CAMP categories, buffs, etc.)
_KYWD_SKIP_NAMES = {
    'costume', 'flower crown', 'tough love helmet',
    'marshal mallow', 'sunset sarsaparilla deputy', 'moneybag',
    'floors', 'walls', 'roofs', 'lights', 'shelves', 'displays',
    'floor decor', 'wall decor', 'stash boxes', 'generators',
    'power connectors', 'water',
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
    """
    result = {}
    for c in raw_conditions:
        # Skip non-keyword conditions
        if 'KYWD:' not in c:
            continue
        # Skip IsFalloutWorlds, HasMagicEffectKeyword (buff checks, not item types)
        if 'IsFalloutWorlds' in c or 'HasMagicEffectKeyword' in c:
            continue

        # Match: "DisplayName" [KYWD:HexID]  or  EdidName "DisplayName" [KYWD:HexID]
        for m in re.finditer(r'"([^"]+)"\s*\[KYWD:([0-9A-Fa-f]+)\]', c):
            kw_name = m.group(1).strip()
            kw_fid = m.group(2).strip()

            # Skip cosmetic/CAMP/non-useful keywords
            if kw_name.lower() in _KYWD_SKIP_NAMES:
                continue

            if kw_fid not in kywd_lookup:
                continue

            # Filter to player-facing signatures, skip internal items, deduplicate
            items = []
            seen = set()
            for ref in kywd_lookup[kw_fid]:
                if ref['sig'] not in _KYWD_ALLOWED_SIGS:
                    continue
                if not _is_player_item(ref['edid']):
                    continue
                pretty = prettify_edid(ref['edid'])
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
    Image URLs use the storefront convention: edid.lower() + '.webp'
    under /wp-content/uploads/fo76/storefront/.
    """
    entm_files = glob.glob(os.path.join(tsv_root, 'ENTM_Export_*.tsv'))
    if not entm_files:
        print("  WARNING: No ENTM_Export_*.tsv found, skipping reward loading")
        return {}

    # Use most recently modified file (sorted() alphabetically is unreliable
    # because month names don't sort chronologically)
    entm_file = max(entm_files, key=os.path.getmtime)
    print(f"  Loading ENTM rewards from {os.path.basename(entm_file)}")

    # Read all ENTM rows
    all_rows = []
    with open(entm_file, 'r', encoding='utf-8-sig', errors='replace') as fh:
        rdr = csv.DictReader(fh, delimiter='\t')
        for row in rdr:
            edid = (row.get('EDID', '') or '').strip()
            if not edid:
                continue
            all_rows.append(row)

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

                    # Build storefront image URL from EDID
                    img_edid = edid.lower()
                    img_edid = img_edid.replace('_entm_', '_')
                    image_url = f"/wp-content/uploads/fo76/storefront/{img_edid}.webp"

                    matched.append({
                        'name':        full or nnam or edid,
                        'description': desc,
                        'edid':        edid,
                        'image_url':   image_url,
                    })
                    break  # Don't double-match same row

        rewards_by_event[key] = matched

    total = sum(len(v) for v in rewards_by_event.values())
    non_empty = sum(1 for v in rewards_by_event.values() if v)
    print(f"  ENTM rewards: {total} items across {non_empty} events")
    return rewards_by_event


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

    # Load guide index for guide linking
    guide_lookup = load_guide_index(args.guide_index)

    # Load keyword refs for keyword item resolution
    kywd_lookup = load_keyword_refs(args.tsv_root)
    print(f"  Keyword refs: {len(kywd_lookup)} keywords loaded")

    # Load ENTM rewards for each event
    entm_rewards = load_entm_rewards(args.tsv_root)

    # Build output
    guide_link_stats = {'linked': 0, 'total': 0}
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
            (cut if is_cut else live).append(entry)

        output[key] = {
            'key':        key,
            'title':      evdef['title'],
            'type':       evdef.get('type', 'limited_time_event'),
            'url':        evdef['url'],
            'week1':      [c for c in live if c['week'] == 'week1'],
            'week2':      [c for c in live if c['week'] == 'week2'],
            'bonus':      [c for c in live if c['week'] == 'bonus'],
            'cut':        cut,
            'total_live': len(live),
            'total_cut':  len(cut),
            'rewards':    entm_rewards.get(key, []),
            'gallery':    GALLERY_IMAGES.get(key, []),
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
    print(f"  Guide links: {guide_link_stats['linked']}/{guide_link_stats['total']} challenges linked")

    # Reward stats
    reward_total = sum(len(ev['rewards']) for ev in output.values())
    reward_events = sum(1 for ev in output.values() if ev['rewards'])
    gallery_events = sum(1 for ev in output.values() if ev['gallery'])
    print(f"  Rewards: {reward_total} items across {reward_events} events")
    print(f"  Gallery: {gallery_events} events with gallery images")


if __name__ == '__main__':
    main()
