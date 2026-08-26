# Farming URL normalisation — 26 Aug 2026

Every farming page now lives at `/{brand}/farming/{category}/…`. Fixed at source in
guide_index.tsv (the importer's source of truth), then propagated to the three layers
that had drifted from it. **No redirects were used** — the WP pages get moved by the sync.

## The rule

    /{brand}/farming/{category}/{item}/{page}/

Child slugs no longer repeat the category name (`chems-addictol` → `addictol`).

## What moved

| Category | Was | Now | Rows |
|---|---|---|---|
| bnb Chems | `/bnb/chems/` | `/bnb/farming/chems/` | 149 |
| bnb Non Perishable | `/bnb/non-perishable/` | `/bnb/farming/non-perishable/` | 53 |
| bnb Nuka Cola | `/bnb/nuka-cola/` | `/bnb/farming/nuka-cola/` | 13 |
| bnb Eggs | `/bnb/eggs/` (hub only — children were already correct) | `/bnb/farming/eggs/` | 13 |
| bnb Critters | slugs `critters-ash-heap` etc. | `ash-heap` etc. | 9 |
| Farming hub, both brands | `/{brand}/farming-guides/` | `/{brand}/farming/` | 14 |
| df Junk | top slug `farming-junk` (url was already right) | `junk` | 1 |

252 rows rewritten. Row **ids were deliberately left untouched** so the WP sync matches on
`_dfbnb_tsv_id` and *moves* each page instead of creating a duplicate.

## Deliberate exceptions

- `junk-management-guide` kept — "Junk Management Guide" is the page's name, not a category prefix.
- `critters-guide` → `farming-guide`, matching `/df/farming/junk/{item}/farming-guide/`.
  A bare `guide` was too thin.
- `nuka-cola-locations` kept — the spawns engine derives its JSON key by stripping
  `-locations`, so a bare `locations` would resolve to an empty key.

## Layers updated beyond the index

1. **nav.json** — 252 urls, node count unchanged, JSON revalidated.
2. **Renderer routing** (`functions.php`, `df-bnb-guide.js`,
   `df-bnb-farming-non-perishable-guide.js`) — the enqueue rules and path regexes match on
   path substrings, so every one of them would have stopped firing.
   - The chems branch used to read the JSON slug straight off the URL folder
     (`chems-addictol` → `chems-addictol_spawns.json`). The folder no longer carries the
     prefix, so the renderer now re-adds it. Data filenames are unchanged.
   - The eggs branch matched `/eggs/eggs-([a-z0-9-]+)/`; now `/eggs/([a-z0-9-]+)/`.
3. **Spawns engine** — added `cfg.urlPath` (defaults to `urlBase`) so Nuka-Cola's pages can
   sit under `/bnb/farming/` while `urlBase` stays `nuka-cola` for filenames and script hints.
4. **`nuka_cola_spawns_config.py`** — page slugs updated; the 9 flavour dist files were
   renamed to match (`nuka_cola_spawns_nuka-cola-cherry.json` → `…_cherry.json`) and the
   manifest rewritten.
5. **`build_mini_seasons_json.py`** — a hand-maintained lookup table of 94 guide URLs, 37 of
   which pointed at pages that have never existed (`/bnb/farming/meat/bloatfly/bloatfly-guide/`
   — bloatfly is an Insect; `/bnb/farming/nuka-cola/nuka-cola-cherry/guide/` — invented shape).
   All 37 rewired against the index.
6. **dist/** — 617 baked links corrected so the live site is right before CI next runs.

## Also fixed

- Added the missing row `bnb-farming-non-perishable-salt-pepper-spices-sugar-location-guide`.
  Its ten region children existed; the index page they hang off did not.
- `check_guide_links_baseline.txt`: **61 → 31 entries, zero farming**. The old baseline had
  frozen the wrong direction as canonical (`/bnb/non-perishable/` over
  `/bnb/farming/non-perishable/`).

## Still needs a human

**1. One row skipped — needs a content decision.**

    bnb-sc-farming-insects-tick
      topCategory: Farming - Insects
      url:         /bnb/farming/non-perishable/non-perishable-tick-blood/non-perishable-tick-blood-guide/

It is an Insects sub-node pointing at the Non-Perishable tick-blood guide — the *same* URL as
`bnb-farming-non-perishable-tick-blood-guide`. Two rows on one URL means the importer will
fight over a single page. Either delete this row, or give it its own page at
`/bnb/farming/insects/tick/`. It is the last stale prefix left in the index.

**2. Duplicate WordPress pages (pre-existing, needs admin access).**

Two pages share each of these URLs — the 26 Aug import created new pages instead of matching
the old ones:

| URL | Keep | Trash |
|---|---|---|
| `/df/farming/consumables/bobbleheads/` | 27491 | 19439 |
| `/df/farming/consumables/magazines/` | 27492 | 19450 |

Also orphaned — the legacy region shape, not in the index, superseded by
`…/bobbleheads/location-guide/{region}/`:

    19440–19449   /df/farming/consumables/bobbleheads/{region}-bobbleheads/

The sync's prune step sets managed pages missing from the TSV back to draft, so these should
drop out on the next run *if* they carry a `_dfbnb_tsv_id`. Check after running.

**3. Two pre-existing faults, unrelated to this work but worth knowing.**

- `assets/df-bnb-drop-chances.js` has **200 trailing NUL bytes** and fails `node --check`.
  The browser rejects it the same way. This is the OneDrive truncation failure mode — the
  file needs restoring from a good copy.
- `src/extract_titles_storefront_images_local.py` has an IndentationError at line 303.

**4. Seven nav.json entries point at pages with no index row** (all outside farming):

    /df/mini-seasons/halloween/challenge-checklist/
    /df/mini-seasons/rip-daring-weapons-expert-extraordinaire/guide/
    /df/mini-seasons/spread-the-love/challenge-checklist/
    /df/vendors/treasury-notes/notes-savage-divide/
    /df/vendors/treasury-notes/notes-skyline-valley/
    /df/vendors/treasury-notes/notes-toxic-valley/
    /df/vendors/treasury-notes/the-pitt/

## Order of operations

1. Run the guide sync **with `dry_run` first** — expect ~252 "Would update", zero "Would create".
   Any "Would create" means a row lost its id binding: stop and check before committing.
2. Run it for real.
3. Commit `tsv/`, `src/`, `dist/` and the child-theme assets, and push (CI rebuilds from the TSV).
4. Purge the Cloudflare cache — child-theme JS/CSS is cached against `?ver=<mtime>` and stale
   HTML keeps pointing at the old `?ver`.

Backups of every file touched: `outputs/farmfix/` (`guide_index.tsv.bak`, `nav.json.bak`,
`codebak/`), plus `rewrites.txt` — the full old → new URL list.

---

# Admin portal → Visibility: missing farming pages

Follow-up, same day. The six farming categories were already on the
`bnb_farming_category_has_pages()` whitelist, so that was never the problem.

## Cause

`bnb_get_guide_tree()` in `inc/category-visibility.php` had:

    if ( $nodeType !== 'page' ) continue;

`sub` rows are real, published WP pages — the item landing page between a category and its
guides (`/bnb/farming/chems/addictol/`, `/df/farming/consumables/bobbleheads/`). They were
dropped on the floor, so they never reached the Visibility tab and could not be toggled.

Second, smaller fault: the four **Consumables** `sub` rows carried `subCategory = "Consumables"`
while their child pages carried the item name, so their hubs would have grouped under a bogus
"Consumables" heading instead of with their own pages. Chems, Insects, Meat and Plants were
already correct.

## Fix

1. `bnb_get_guide_tree()` now accepts `page` **and** `sub`. `top` rows stay out — the category
   landing page is governed by the category tier itself.
2. Sub rows are tagged `isHub` and sorted to the head of their own group.
3. `df-bnb-admin-portal.js` renders a small **Hub** pill on those rows; `.vis-page-hub` added
   to `df-bnb-admin-portal.css`. This matters in Chems, where the hub and its guide share a
   title — without the pill you get two rows both labelled "Addictol".
4. Consumables sub rows: `subCategory` set to the item name.

## Result

| Category | Pages now shown | Groups | of which hubs |
|---|---|---|---|
| Farming - Consumables | 30 | 4 | 4 |
| Farming - Chems | 148 | 73 | 74 |
| Farming - Critters | 9 | 1 | 0 |
| Farming - Insects | 19 | 10 | 10 |
| Farming - Meat | 62 | 31 | 31 |
| Farming - Plants | 86 | 43 | 43 |

Critters has no `sub` rows at all — its nine pages hang straight off the category, so it
correctly renders as one flat group.

## Site-wide side effect (intended)

The tree feeds `bnb_get_url_to_category_index()`, which `bnb_can_view_page()` uses for
front-end enforcement. Hub pages were invisible to it before, so a per-page override on a hub
could not be enforced. Reachable rows go from 1315 to 1733 (+418 hubs across all categories,
not just farming). Non-whitelisted farming categories — Junk — are still collapsed, so the
admin payload does not balloon.

## Note

The tab reads `wp-content/uploads/guide_index.tsv`, **not** the repo copy. The updated TSV has
to reach uploads before any of this shows.

## Cosmetic follow-up, outside the six

Three other categories have `sub` rows whose `subCategory` doesn't match their children's, so
their newly-visible hubs group oddly:

- **Activities** (27 hubs) and **Fishing** (3) — blank `subCategory`, so they list loose at the
  category level. Reads fine.
- **Buffs** (9 hubs) — each lands in its own single-row group (Alcohol, Bobbleheads, Chems,
  Hybrid Food). Not broken, just uneven.
