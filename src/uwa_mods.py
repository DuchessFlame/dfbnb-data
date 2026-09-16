"""Resolve the fixed / unique mods that a unique weapon or armour ships with.

Three things go missing without this:
  * fixed star effects — items whose own WEAP record carries the legendary
    mods show "Random" if you read the BASE weapon's template instead;
  * the custom mod that gives the item its identity and its special effect;
  * the non-standard parts it ships pre-installed (Electrified, Beam Focuser…).

Matching order, most specific first:
  1. a WEAP/ARMO record whose FULL is the item name  ("Commander's Charge")
  2. an OMOD whose FULL is the item name             ("Holy Fire")
  3. an OMOD whose EDID contains the item name       (mod_Custom_Pyrolyzer)
"""
import re
from collections import defaultdict

MODREF = re.compile(r'^(\S+)\s+"*(.*?)"*\s*\[OMOD:([0-9A-Fa-f]{8})\]$')
STAR_ANY = re.compile(r'(?:RA_)?mod_Legendary_(?:Weapon|Armor)(\d)_', re.I)
# Parts every weapon of that class ships with — not what makes this one special.
FILLER_NAME = re.compile(r'^(standard\b|no upgrade\b|no \w+$|default appearance|range offset)', re.I)
FILLER_EDID = re.compile(r'_null\b|null_|_none\b', re.I)
CUSTOM_EDID = re.compile(r'mod_custom|mod_description', re.I)
# Purely visual custom mods. NOTE: *_CustomName is deliberately NOT here — the
# custom-name mod is the one that makes a unique weapon unique, so it stays.
COSMETIC_EDID = re.compile(r'appearance|paint|modelswap|skin', re.I)
# The custom mod's FULL is usually the item name plus a bookkeeping suffix
# ("Piercing Love Custom Mod"), so the raw name never matched it.
ZZZ_EDID = re.compile(r'(^|_)zzz', re.I)
NAME_SUFFIX = re.compile(r'\s*(Custom Mod|Custom Name|Custom Paint|Custom|Bounty)\s*$', re.I)


def _norm(s):
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())


def _clean(s):
    return (s or '').strip().strip('"').strip()


def build_mod_indexes(omod_rows, weap_ot_rows, armo_ot_rows):
    """omod_rows: OMOD_Export rows. *_ot_rows: ObjectTemplate rows."""
    omod_by_fid, omod_by_full, omod_by_edid = {}, defaultdict(list), {}
    for r in omod_rows:
        fid = _clean(r.get('OMOD_FormID')).upper()
        ed = _clean(r.get('OMOD_EDID'))
        if fid:
            omod_by_fid[fid] = r
        if ed:
            omod_by_edid[ed] = r
        full = _clean(r.get('FULL'))
        if full:
            omod_by_full[_norm(full)].append(r)
            stem = _norm(NAME_SUFFIX.sub('', full))
            if stem and stem != _norm(full):
                omod_by_full[stem].append(r)

    def load(rows, fidcol, fullcol):
        combos = defaultdict(lambda: defaultdict(lambda: {'mods': [], 'default': ''}))
        by_full = defaultdict(list)
        for r in rows:
            f = _clean(r.get(fidcol)).upper()
            if not f:
                continue
            name = _clean(r.get(fullcol))
            if name and f not in by_full[_norm(name)]:
                by_full[_norm(name)].append(f)
            c = combos[f][_clean(r.get('CombinationIndex'))]
            c['default'] = _clean(r.get('OBTS_Default'))
            m = _clean(r.get('Include_Mod'))
            if m:
                c['mods'].append(m)
        return combos, by_full

    wc, wf = load(weap_ot_rows, 'WEAP_FormID', 'WEAP_FULL')
    ac, af = load(armo_ot_rows, 'ARMO_FormID', 'ARMO_FULL')
    return {'omod_by_fid': omod_by_fid, 'omod_by_full': omod_by_full,
            'omod_by_edid': omod_by_edid, 'weap_combos': wc, 'weap_by_full': wf,
            'armo_combos': ac, 'armo_by_full': af}


def _default_combo(combos, fid):
    cs = combos.get(fid) or {}
    for c in cs.values():
        if c['default'] == 'True':
            return c
    return next(iter(cs.values()), None)


_PREFIX = re.compile(r'^(?:[A-Za-z0-9]{2,6}_)?(?:zzz_)?mod_(?:custom|description)_', re.I)


def _pretty(edid):
    """Readable label for a mod whose FULL is blank: strip the mod_Custom_ /
    mod_Description_ prefix and split the CamelCase tail."""
    tail = _PREFIX.sub('', edid or '')
    tail = re.sub(r'^(?:[A-Za-z0-9]{2,6}_)?(?:zzz_)?mod_', '', tail, flags=re.I)
    tail = tail.replace('_', ' ').strip()
    return re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', tail).strip()


# "Piercing Love Custom Mod" is a record name, not something to print on the
# page. The bookkeeping tail comes off the display name only — never off the
# keys, which still have to match what the exports say.
DISPLAY_SUFFIX = re.compile(r'\s*(Custom Mod|Custom Name|Custom Paint)\s*$', re.I)


def _entry(edid, name, fid, omod_by_fid, kind):
    row = omod_by_fid.get(fid, {})
    nm = _clean(name) or _clean(row.get('FULL'))
    nm = DISPLAY_SUFFIX.sub('', nm).strip() or nm
    if not nm:
        # A mod_Description_* record is pure effect text with no display name;
        # a made-up label from its EDID reads worse than an honest generic one.
        nm = 'Unique Effect' if re.search(r'mod_description', edid, re.I) else _pretty(edid)
    return {'name': nm, 'edid': edid, 'formId': fid, 'kind': kind,
            'desc': _clean(row.get('DESC'))}


def _walk_combo(combo, omod_by_fid):
    """-> (fixed stars by slot, unique mods, pre-installed parts).

    Cosmetic customs (paint / appearance / model swap / custom name) are
    dropped: they say nothing about what the item does."""
    stars, custom, fixed = {}, [], []
    for m in combo['mods']:
        mm = MODREF.match(m)
        if not mm:
            continue
        edid, name, fid = mm.group(1), mm.group(2), mm.group(3).upper()
        st = STAR_ANY.match(edid)
        if st:
            stars[int(st.group(1))] = _entry(edid, name, fid, omod_by_fid, 'star')
            continue
        if FILLER_NAME.match(_clean(name)) or FILLER_EDID.search(edid):
            continue
        # ap_customName is the game's own marker for "this is the mod that makes
        # the item that item". Going by the EDID alone missed every unique mod
        # Bethesda named after the weapon instead of mod_Custom_* — the V63
        # Zweihaender's Storm Cutter was filed as an ordinary fitted part.
        # A paint hung on ap_customName is still a paint, and a zzz_ record is a
        # cut placeholder — neither is what the item does.
        if COSMETIC_EDID.search(edid) or ZZZ_EDID.search(edid):
            continue
        ap = _clean(omod_by_fid.get(fid, {}).get('AttachPoint_EDID')).lower()
        if ap == 'ap_customname' or CUSTOM_EDID.search(edid):
            custom.append(_entry(edid, name, fid, omod_by_fid, 'unique'))
        else:
            fixed.append(_entry(edid, name, fid, omod_by_fid, 'preinstalled'))
    return stars, custom, fixed


def resolve_item_mods(item, idx):
    """Return (starEffects|None, uniqueMods list). starEffects is None when the
    item has no record of its own and the existing ones should be kept."""
    key = _norm(item.get('name'))
    omod_by_fid = idx['omod_by_fid']
    stars, custom, fixed = {}, [], []

    # 1. the item's own WEAP/ARMO record
    by_full = idx['weap_by_full'] if item.get('kind') == 'Weapon' else idx['armo_by_full']
    combos = idx['weap_combos'] if item.get('kind') == 'Weapon' else idx['armo_combos']
    for rec_fid in by_full.get(key, []):
        combo = _default_combo(combos, rec_fid)
        if not combo:
            continue
        s, c, f = _walk_combo(combo, omod_by_fid)
        stars = stars or s
        custom += [x for x in c if x['formId'] not in {y['formId'] for y in custom}]
        fixed += [x for x in f if x['formId'] not in {y['formId'] for y in fixed}]
        if s or c:
            break

    # 2./3. the custom OMOD that carries the item's name
    if not custom:
        rows = list(idx['omod_by_full'].get(key, []))
        # A by-name hit that turns out to be a paint or a model swap is not a
        # reason to stop looking: the real custom mod is usually the EDID match
        # sitting behind it. Piercing Love ships both, and the model swap won.
        seen_fids = {_clean(r.get('OMOD_FormID')).upper() for r in rows}
        for ed, r in idx['omod_by_edid'].items():
            # A whole segment of the EDID has to BE the name. Plain substring
            # matching let "Rage" claim Vengeful Rage's mutation name mod.
            if key and any(_norm(seg) == key for seg in str(ed).split('_')):
                f = _clean(r.get('OMOD_FormID')).upper()
                if f not in seen_fids:
                    seen_fids.add(f)
                    rows.append(r)
        for r in rows:
            fid = _clean(r.get('OMOD_FormID')).upper()
            ed = _clean(r.get('OMOD_EDID'))
            full = _clean(r.get('FULL'))
            if FILLER_EDID.search(ed) or ZZZ_EDID.search(ed):
                continue
            # Cosmetics name themselves in the EDID sometimes and only in the
            # FULL other times ("Red Terror Paint" on a clean EDID), so both get
            # tested here.
            if COSMETIC_EDID.search(ed) or COSMETIC_EDID.search(full):
                continue
            # This is a name match, not a structural one, so it has to look like
            # a custom mod before it is believed: anything else sharing the
            # item's name is a different record that happens to be named after
            # it (the Cosmic Knife Super-Heated variant, a spare name mod).
            ap = _clean(r.get('AttachPoint_EDID')).lower()
            if (ap != 'ap_customname'
                    and not CUSTOM_EDID.search(ed)
                    and not _clean(r.get('DESC'))):
                continue
            # The name in the EDID is not enough on its own: a sibling item
            # built on the same base carries it too (mod_custom_CosmicKnife_
            # Superheated belongs to Cosmic Knife Super-Heated, its own row on
            # the page). Believe it only when the mod is named after this item,
            # carries effect text, or the EDID ends on the name.
            if not (_norm(NAME_SUFFIX.sub('', full)) == key
                    or _clean(r.get('DESC'))
                    or _norm(str(ed).split('_')[-1]) == key):
                continue
            e = _entry(ed, full, fid, omod_by_fid, 'unique')
            if e['formId'] not in {y['formId'] for y in custom}:
                custom.append(e)

    # Headline order: a unique mod that actually describes an effect first.
    custom.sort(key=lambda e: not e['desc'])

    # Collapse duplicates that differ only by record (a base mod and its cr*
    # copy both resolve to the same display name). Keep the one with text.
    unique_mods, seen = [], {}
    for e in custom + fixed:
        k = e['name'].strip().lower()
        if k in seen:
            if e['desc'] and not unique_mods[seen[k]]['desc']:
                unique_mods[seen[k]] = e
            continue
        seen[k] = len(unique_mods)
        unique_mods.append(e)

    star_list = None
    if stars:
        star_list = []
        for n in sorted(stars):
            e = stars[n]
            star_list.append({'star': n, 'name': e['name'], 'edid': e['edid'],
                              'formId': e['formId'], 'fixed': True})
    return star_list, unique_mods


def enrich(items, idx):
    """Fill uniqueMods / fix starEffects / backfill inherentEffect, in place."""
    st = {'stars_fixed': 0, 'mods_added': 0, 'effect_added': 0, 'no_match': 0}
    for it in items:
        star_list, mods = resolve_item_mods(it, idx)
        if star_list:
            # Merge by slot — never drop a star we already knew about, and never
            # turn a known effect back into "Random".
            cur = {s['star']: s for s in (it.get('starEffects') or [])}
            for s in star_list:
                cur[s['star']] = s
            top = max(cur) if cur else 0
            merged = []
            for n in range(1, top + 1):
                merged.append(cur.get(n, {'star': n, 'name': 'Random', 'edid': '',
                                          'formId': '', 'fixed': False}))
            if merged != it.get('starEffects'):
                it['starEffects'] = merged
                st['stars_fixed'] += 1
        it['uniqueMods'] = mods
        if mods:
            st['mods_added'] += 1
        else:
            st['no_match'] += 1
        if not _clean(it.get('inherentEffect')):
            for m in mods:
                if m['desc']:
                    it['inherentEffect'] = m['desc']
                    st['effect_added'] += 1
                    break
    return st


def build_indexes_for_channel(pick, read_tsv, channel):
    """Convenience wrapper for the build script: resolve the three exports this
    module needs through the caller's own `pick` / `read_tsv` helpers."""
    def rows(pattern, exclude=None):
        p = pick(pattern, channel, exclude=exclude) if exclude else pick(pattern, channel)
        return list(read_tsv(p)) if p else []
    return build_mod_indexes(
        rows("OMOD_Export_*.tsv", exclude="_Properties"),
        rows("WEAP_Export_*_ObjectTemplate.tsv"),
        rows("ARMO_Export_*_ObjectTemplate.tsv"),
    )
