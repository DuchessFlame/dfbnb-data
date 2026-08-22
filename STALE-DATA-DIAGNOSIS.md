# Stale datamined data — diagnosis and architecture answer

_Answer to `STALE-DATA-PROBLEM.md`. Investigated 2026-08-22 against the repo at commit of that date._

---

## Short version

The graves page is not an outlier and it is not a missing-check problem.

1. **The repo has never recorded an observation.** It records modifications. Re-exporting a
   record type that didn't change produces a byte-identical file and git records nothing, so
   "exported today, unchanged" and "never re-exported since July" are literally the same state
   on disk. That is why the 665 old TSVs can't be triaged — the distinguishing fact was never
   written down, and no script can recover it after the fact.
2. **Every published file stamps the build time and calls it freshness.** The graves JSON says
   `"generated": "2026-08-22"` — today. It is derived from a placement export taken on
   18 July. The metadata is at its most confident exactly when the data is most wrong.
3. **The graves page is wrong rather than merely stale because it is rendered from PTS data.**
   That is a specific, findable bug, below.
4. **The patch log is a diff of our own output.** A diff of outputs cannot detect a missing
   input, by construction. It was never going to catch this and never will.
5. **The 30-second load is two files**, not the file count.

---

## 1. Blast radius

### Measured

| Fact | Value |
|---|---|
| TSV inputs present | 799 (782 tracked, 24 untracked in `tsv/pts/` from today) |
| Build modules transitively depending on local-only inputs | **42**, not 10 |
| Published files in the geo / placement family | **97** (excl. the `dist/pts/` mirror) |
| `REFR_Placements` exports in the entire repo history | **3 — all PTS, all 2026-07-18** |
| Top-level `dist/*.json` with no provenance field at all | 100 of 167 |
| Distinct provenance key names among the other 67 | 6 (`_meta`, `_generated`, `generated`, `source`, `_source`, mixtures) |
| CI "verification" statements that assert anything beyond file existence | 4 (all in the minerva step) |

The 42 modules, tainted transitively through imports as well as directly:

```
build_blood_sac_spawns_json          build_hto_locations_json           locate_map_screenshot
build_canned_coffee_spawns_json      build_nuka_cola_spawns_json        nuka_cola_spawns_geo
build_canned_meat_stew_spawns_json   build_perfect_bubblegum_spawns…    nuka_cola_spawns_sources
build_collectable_spawns_json        build_phantom_grave_sites_tsv      render_slasher_maps
build_cream_spawns_json              build_purified_water_spawns_json   render_spawn_maps
build_deathclaw_egg_spawns_json      build_royal_jelly_spawns_json      render_spawn_maps_manifest
build_farming_meat_json              build_salt_pepper_spices_sugar…    spawns_configs/bobbleheads
build_farming_spawns_json            build_spawns                       spawns_configs/cryptids
build_farming_used_for               build_sugar_bombs_spawns_json      spawns_configs/farming
build_glowing_blood_spawns_json      build_treasure_map_dig_sites_tsv   spawns_configs/meat
build_honey_spawns_json              build_vendors_json                 spawns_configs/nuka_cola
build_honeycomb_spawns_json          crossref_mappalachia_markers       spawns_engine/geo
build_hotdog_spawns_json             diff_refr_placements               tools/build_npc_spawns
                                     farming_spawns_config              tools/extract_curvetables
                                     honeycomb_honey_beast
```

The 97 published files: 20 `dist/farming_spawns/*`, 38 `dist/meat/*`, 15 `dist/cryptids/*`,
13 `nuka_cola_spawns_*`, 2 `collectable_spawns_*`, 2 `dist/infestations/*`, plus
`npc_spawns`, `vendors`, `treasure_maps`, `cryptids`, `bobbleheads_spawns` and 3 manifests.

### The export calendar — the thing nobody is looking at

Reconstructing sweeps from filenames:

**LIVE channel**, record types captured per month:

```
2026-03  32     2026-05  34     2026-07  41
2026-04  32     2026-06  39     2026-08   3   <- ALCH, FLOR, LGDI only
```

**PTS channel**, record types per sweep date:

```
2026-06-22  37    2026-07-11  21    2026-08-04  26
2026-06-27  40    2026-07-12  21    2026-08-05  14
2026-07-04   7    2026-07-18  33  <- the only REFR export ever
2026-07-05  27    2026-07-19   8    2026-08-22  30  <- today
```

Two things fall out.

**The live channel stopped being exported six weeks ago while the PTS channel kept going.**
38 of the 41 record types feeding live pages are on July exports or older. The August live
sweep is three record types. Meanwhile a 30-type PTS sweep landed today. The graves page is
not a special failure; it is the first one you happened to open in xEdit.

**No sweep is complete, and "complete" is undefined.** Every sweep covers a different subset —
37, 40, 7, 27, 6, 21, 21, 33, 8, 1, 26, 14, 1, 1, 1, 30. There is no declared set of record
types, so there is nothing for "missing from this sweep" to mean. REFR has been in exactly one
sweep, ever.

### How to tell stale from legitimately frozen — structurally

You cannot, from what the repo currently holds, and no script can fix that. Modification date
is the wrong axis. The axis you need is **observation**: not "when did this file last change"
but "when was this record type last *looked at*, and against which game build".

That fact only exists at the moment of export, and it is currently discarded. Section 4 is
about writing it down. It is one line per export, appended by the tool you already run.

Note the precedent sitting on your own disk: `mappalachia.db` carries
`Meta.GameVersion = 1.7.25.39`. Mappalachia already stamps what it was extracted from. Your
exports do not.

---

## 2. Why graves went *wrong*, not just stale

Three separate defects stack.

**(a) Channel leak.** `build_phantom_grave_sites_tsv.py::_newest_refr_placements()` globs
`tsv/REFR_Placements_*.tsv` **and** `tsv/pts/REFR_Placements_*.tsv` into one list and takes the
lexically-last match. It has no `--pts` handling, unlike the 33 other scripts that follow the
`--pts` / `DFBNB_CHANNEL` convention. Since no live REFR export has ever existed, the live page
has *always* been rendered from PTS placements. PTS content diverges from live by design — so
"graves that aren't there" is the expected result of this code, not an anomaly.

Sub-defect: the three PTS files sort `..._0747 < ..._0757 < ..._07571`. The winner is decided
by a typo'd filename.

**(b) The merge key is the volatile thing.** `HANDFILL_COLS`
(`directions`, `photo_region`, `photo_approach`, `photo_spawn`) are carried across a rebuild
keyed on `ref_formid`. Placement refs are exactly what Bethesda churns. When graves 05/07/09
were re-placed under `00930AA0/AA7/AAE`, the key changed, the join missed, and the hand-authored
directions and three photo paths were silently dropped. The durable identity — "the grave the
player is told to find is grave 7" — is present in the data and is not used as the key.

This is also why the two converted photos can't be wired: the hand-authored payload is stored
inside the generated artifact, so any rebuild is entitled to delete it.

**(c) The fallbacks launder staleness.** `geo_cache.json` re-resolves refs by FormID when the
Mappalachia DB is absent; `crossref` falls back to it in CI; the handfill merge preserves the
previous answer; several builders are DB-optional. Each was added to make CI deterministic.
Each works by **preserving the last known answer**. Together they are a machine for turning
stale input into confident-looking output. The green build is not a coincidence — the system
was carefully engineered to stay green.

And the FormID-keyed cache has a second failure mode: a re-placed ref has no cache entry, so it
resolves as unresolved rather than as changed. A relocation looks like a hole.

---

## 3. Why the patch log never showed placements

Not a missing handler. `patchlog_utils.write_patchlog_feed()` diffs `dist/X.json` against
`git show HEAD~:dist/X.json` — **it diffs our own published artifact against its previous
revision**.

That measures what we republished, never what the game did. If an input stops arriving, the
output stops changing, and the feed correctly reports "no changes". A diff of outputs is
mathematically incapable of detecting a missing input. Adding a `write_patchlog_feed` call to
the graves builder would have produced a permanently empty, permanently green feed.

Secondary: only 12 of 152 build scripts call it at all, and none of the 97 geo-family files do.
There are 32 `patchlog_latest_*.json` feeds and none covers spawns, placements or graves — so
the graves page has no `byPage` entry, and its expand has nothing to render even in principle.

Good news for the fix: the plumbing is already there and already shared. `df-bnb-guide.js`
(6,645 lines, enqueued on every guide page by `functions.php`) fetches `patchlog_manifest.json`
once, caches it across navigation, and looks up `byPage[pathname]`. Nothing needs inventing —
the feed just has to contain something true.

`src/diff_refr_placements.py` already exists — a band-aid nobody runs. Its existence is
evidence: the right comparison was identified and then parked somewhere nothing would call it.

**The fix is to move the diff upstream to the export boundary**, not to add feeds downstream.
See §4.4.

---

## 4. The design

Four changes. Net effect on moving parts: **−2 scripts, ±0 fetched files, one new ledger.**

### 4.1 Record the observation — one line, at the xEdit boundary

Add to the `.pas` export script you already run, so it costs no new habit: every export appends
or updates one row in a single ledger, `tsv/_exports.tsv`:

```
channel   record_type   exported_at   game_version   rows    sha256
live      ACTI          2026-08-22    1.7.25.39      41822   a3f1…
live      REFR          2026-08-22    1.7.25.39      918442  9c02…
pts       ACTI          2026-08-22    1.7.26.1       41830   77bd…
```

This is the one fact the repo has never held. Everything else follows from it for free:

- **Stale vs frozen becomes decidable without a script.** Frozen = `exported_at` current,
  content unchanged. Stale = `exported_at` old. Two states that are currently identical on
  disk become distinguishable.
- **"Complete sweep" becomes definable.** The ledger's record-type list is the manifest.
  Anything absent from the newest sweep is provably unverified — including REFR, which would
  have shown as unverified since 18 July.
- **Builds stamp observation, not build time.** Every builder replaces its lying `generated`
  field with the *oldest* `exported_at` among its inputs. One helper in `patchlog_utils`,
  which every builder already imports; no new module.
- **CI gets something real to assert.** Replace `test -f` with a comparison of each output's
  observation date against the newest `release_date` in `tsv/fallout76_patches.tsv` (already
  maintained) cross-checked against `Meta.GameVersion` from `mappalachia.db` (independently
  updated, currently 1.7.25.39). Behind the current patch → **annotate, do not fail.** A red
  build you can't fix without xEdit is a build you'll learn to ignore.

Caveat worth naming: `fallout76_patches.tsv` is itself hand-maintained (last touched 2026-08-02),
so it is a weak watermark on its own. The Mappalachia `GameVersion` is the second opinion, and
disagreement between the two is itself a useful signal.

### 4.2 Stop presenting stale data as current — one edit, in the shell you already have

You asked: stop presenting it as current, *or* stop depending on a human. Do the first
unconditionally — it's cheap, it's honest, and it holds even for pages that will never be
re-exported.

Every page renders a provenance line built from §4.1 data:

> _Placements last verified 18 Jul 2026 against PTS 1.7.25.39. Game is now on 1.7.26.x._

Two or more patches behind, the same line becomes a visible "may be out of date" banner.

**Where:** `df-bnb-guide.js` — the shared shell, enqueued on every guide page, which *already*
fetches `patchlog_manifest.json`, already caches it across navigation, and already resolves
`byPage[pathname]`. One block next to the existing patch-log expand. **One file edited, not 87.**

**Which file:** add a `provenance` key alongside `url`/`label` in each `byPage` entry of
`dist/patchlog_manifest.json`. **No new published file, no new fetch** — it rides the request
the shell already makes.

This converts a silent correctness failure into a visible, low-stakes, honest one. Constraint 3
is satisfied: a page nobody will ever re-export is fine forever — it just says when it was last
checked.

### 4.3 Make the refresh machine-triggered, not memory-triggered

The game files and the 480 MB Mappalachia DB are on your PC. The refresh doesn't need to leave
that machine — it needs to stop needing you. Register your PC as a **self-hosted GitHub Actions
runner** (same repo, same workflows) with a job that watches the game install's build number and,
on change, runs xEdit headless with the export script and commits.

This is not "run a command every two weeks". The trigger is the game patching, and the machine
notices. Be clear-eyed though: it is the highest-effort item here and it needs the PC powered on.
That is exactly why §4.1 and §4.2 come first — they make failure *visible* this week, rather
than *impossible* next quarter.

### 4.4 Diff inputs, not outputs

When the ledger shows a new export of record type R, diff export N against N−1 for R. That
yields real game-level changes, including the ones placements need:

- added / removed REFR
- **moved** — same base, same ref, coords delta beyond a threshold
- **re-placed** — same base, coords within threshold, different ref FormID → pair them and
  report as "moved", not "deleted + added"

That last rule is the exact case that read as a deletion. Pairing has to happen at the export
boundary because that is the only place both sides of the pair are visible.

This **replaces** `diff_refr_placements.py` and folds into `patchlog_utils` rather than adding
alongside them. The per-page expand then shows real patch content for the first time.

### 4.5 Decouple hand-authored content from the generated artifact

Move `directions` and the three photo slots out of `tsv/phantom_grave_sites.tsv` into a small
hand-file keyed on **grave number** — the identity the player is given, which survives
re-placement — and have the builder join it in. A rebuild then physically cannot take your
photo paths with it, which is the coupling blocking the two converted images.

Same pattern applies to every `HANDFILL_COLS` in the spawns engine.

---

## 4A. The builders are not reading the newest TSVs — they are reading May

_Added after the first pass, while implementing §4.5. This is worse than the export gap and it
is entirely inside the code._

Exports are named `ACTI_Export_July_2026_ACTI.tsv`. Sorting those **alphabetically** does not
sort them chronologically. `August` sorts before `July`, `June` before `March`, and `May` beats
all of them. Every builder that picks its input with a plain `sorted(glob(...))[-1]` therefore
reads **May 2026**, no matter what is committed.

Measured against the TSVs actually in the repo right now:

| Pattern | `sorted()[-1]` picks | Actually newest |
|---|---|---|
| `ACTI_Export_*_ACTI` | **May 2026** | July 2026 |
| `ALCH_Export_*` | **May 2026** | **August 2026** |
| `ARMO_Export_*` | **May 2026** | July 2026 |
| `CHAL_Export_*` | **May 2026** | July 2026 |
| `COBJ_Export_*` | **May 2026** | July 2026 |
| `LVLI_Export_*` | **March 2026** | July 2026 |
| `WEAP_Export_*` | **May 2026** | July 2026 |
| `OMOD_Export_*` | **May 2026** | July 2026 |
| `BOOK_Export_*` | **May 2026** | July 2026 |
| `PERK_Export_*` | **May 2026** | July 2026 |
| `NPC_Export_*` | **June 2026** | July 2026 |
| `QUEST_Export_*` | **May 2026** | July 2026 |

Twelve for twelve. The August ALCH export — the one record type that *was* re-exported this
month — is being ignored in favour of a file three months older.

There is a second, independent version of the same bug: **~15 selection sites sort by
`os.path.getmtime`**. In CI that is meaningless — `actions/checkout` writes every file with the
checkout timestamp, so "newest by mtime" resolves to whatever the filesystem felt like. It
happens to work on your machine and is arbitrary in the build that actually publishes.

Verified selection sites:

- **Lexical** (~21 sites, resolve to May): `build_bounty_hunting_json:228`,
  `build_bounty_hunting_rewards_json:35`, `build_camp_json:27`, `build_cobj_recipes_json:365`,
  `build_farming_used_for:109,145`, `build_fishing_calculator_json:62`,
  `build_hto_locations_json:126`, `build_hto_rewards_json:30`,
  `build_mini_seasons_json:1409,1555`, `build_pennants_json:507`,
  `build_pts_season_scoreboard:120`, `build_recipe_guide_json:288`,
  `build_titles_generator_json:53`, `build_weak_spot_multipliers_json:46`,
  `crossref_mappalachia_markers:407`, `normalize_pts_tsv:70`, `build_all:22`,
  plus the two channel-leaking REFR readers.
- **mtime-only** (~15 sites): `build_activities_rewards_json:560`, `build_mini_seasons_json:103`,
  `build_perk_cards_json:70`, `build_treasure_maps_json:105`,
  `build_unique_weapons_json:65,423`, `build_vendors_json:115`,
  `crossref_mappalachia_markers:120`, `build_seasonal_events_json:1987,2010`,
  `spawns_configs/bobbleheads:111`, `spawns_configs/cryptids:126`,
  `spawns_engine/events:76`, `spawns_engine/sources:45`,
  `tools/build_activities_rewards_json:510`.
- **Date-keyed and correct for live naming**: 15 files — but they carry **22 separate private
  copies** of a date-key helper, no two guaranteed identical, and none of them understands the
  PTS naming scheme (`ACTI_Export_PTS_2026-08-22_0925.tsv`). Both `_filename_date_key` and
  `_tsv_date_key` return `(0, 0)` for every PTS file, i.e. "undated, sort last".

### The fix — one resolver, 36 sites deleted

This is a net **reduction**: one shared module replaces 22 divergent private helpers and ~36
ad-hoc selection expressions. It is a module builders import, not a script anyone runs.

`src/tsv_source.py` — `newest(record_type, section=None, channel="live")` — with five
properties:

1. **Understands both naming schemes.** `Month_Year` and `PTS_YYYY-MM-DD_HHMM`, parsed to a
   real date. Never lexical.
2. **Never uses mtime.** Filenames and the export ledger only, so local and CI agree.
3. **Channel-explicit.** `live` reads `tsv/`, `pts` reads `tsv/pts/`, never both. Cross-channel
   fallback is what put PTS graves on a live page.
4. **Returns what it chose**, so the caller can stamp `observed` into its output (§4.1) instead
   of a build timestamp.
5. **Raises on no match** rather than silently returning `None` and building an empty page.

### What makes it stay fixed

Add one assertion to the build: **every input a builder resolved must be the newest file
present for its record type and channel.** If a builder resolves `ALCH_Export_May_2026` while
`ALCH_Export_August_2026` sits beside it, the build fails and names both files.

That is the property you actually asked for, and it becomes *checked* rather than intended:
drop a new export into `tsv/`, and within one build cycle every page that consumes that record
type picks it up, or CI stops and says which builder didn't.

One thing this cannot do, stated plainly: it makes the site always current **with respect to
the exports in the repo**. It cannot make an export appear — that is still §4.1 and §4.3. But
it closes the half of the gap that is pure code, and today that half is three months wide.

**Payload, not file count.** The 30-second load is not 800 JSONs. It is:

| File | Size | Fetched by |
|---|---|---|
| `dist/activities/activities_rewards_by_page.json` | **67 MB** | `functions.php` |
| `dist/events/events_rewards_by_page.json` | **60 MB** | `df-bnb-seasonal-events.js`, `functions.php` |
| `dist/patchlog_manifest.json` | 398 KB | the shell, once per session, to read one key |

`_by_page` means the browser downloads every page's data to render one page. Across 87
renderers there are 194 `fetch()` calls hitting 89 distinct URLs — roughly 2–3 per page, which
is fine. The 398 KB manifest is cached across navigation by the shell, so it costs once, not
per page — worth trimming eventually, but it is not the problem. Splitting the two monsters into per-page slices *adds files on disk* and cuts
bytes-per-pageview by ~99%. **Files on disk are free; bytes over the wire are not** — this looks
like it violates "no proliferation", and it doesn't. The constraint that matters is bytes.

**Get hot data off `raw.githubusercontent.com`.** It is not a site CDN — short cache lifetime,
rate limits, no control at the edge. Either proxy same-origin with a long cache from
`functions.php`, or point at jsDelivr, which fronts the same repo through a real CDN. Same file
count, materially better TTFB.

**Stop committing a 67 MB artifact every 6 hours.** `dist/` is 722 MB across 800 JSONs and the
big ones are rewritten on every scheduled run. That is the CI cost and the clone cost. Those
belong as release assets or build artifacts, not tracked files.

---

## 6. Immediate item — the grave JSON

Ready to apply now, using your figures (19 − 4 + 3 = 18):

**Remove:** `008F675C` (grave 5, Hopewell Cave), `008F1738` (grave 7), `008F6750` (grave 9),
`0090FBDD` (the blank-EDID Forest / WV Lumber row).

**Add**, resolved against `mappalachia.db` just now:

| Ref | Grave | Region | Closest fast travel |
|---|---|---|---|
| `00930AA0` | 5 | (needs coords) | Colonel Kelly Monument |
| `00930AA7` | 7 | **Forest** | **Lady Janet's Soft Serve** |
| `00930AAE` | 9 | **Savage Divide** | **Yellow Sandy's Still** |

The photo filenames confirm the pairing — `…grave-7…` → `00930AA7`, `…grave-9…` → `00930AAE` —
which leaves `00930AA0` as the re-placed grave 5.

Caveats, stated rather than papered over: the DB knows the map markers but **not** the SDOW
grave activator refs (it doesn't index that signature), so `x/y/z` for the three new rows can't
be derived — only a real REFR export gives those. The marker label is `Colonel Kelly Monument`,
one _l_, not "Col Kelley". And the DB itself is a 2026-07-21 snapshot at `1.7.25.39`, so it is
five weeks old too.

Doing §4.5 first means the photo paths land on grave 7 and grave 9 and survive the next
re-placement. Doing it after means wiring them twice.

---

## What I'd do in what order

1. ~~**§4.5** — decouple the handfills.~~ **Done 2026-08-22.**
2. ~~**§6** — apply the 18-grave correction.~~ **Done 2026-08-22.**
3. **§4A** — the shared TSV resolver. Promoted to the top: it is pure code, it needs no export
   and no discipline, and it is currently costing three months of freshness on data that is
   already sitting in the repo. Biggest correctness win per hour of work.
4. **§4.1** — the export ledger. One block in the `.pas`, one helper in `patchlog_utils`.
   This is the load-bearing change for the half §4A can't reach.
5. **§4.2** — the provenance line in `df-bnb-guide.js`. Cheap, and it ends the class of silent
   failure even where the data stays stale.
6. **§5** — split the two monster payloads. Biggest user-visible win, independent of the rest.
7. **§4.4**, then **§4.3**.

Steps 1–5 remove `build_phantom_grave_sites_tsv.py`'s channel bug, `diff_refr_placements.py`,
22 duplicated date helpers and ~36 ad-hoc selection expressions. They add no scheduled human
action and no file the browser fetches.

### Done in this pass

| File | Change | Lines |
|---|---|---|
| `tsv/phantom_grave_notes.tsv` | **new** — editorial content keyed on grave identity | 19 |
| `tsv/phantom_grave_sites.tsv` | placements only, corrected to 18, `source_export` column | 19 |
| `src/build_phantom_grave_sites_tsv.py` | channel-explicit, date-aware, regression guard, no longer writes editorial columns | 261 |
| `src/build_collectable_spawns_json.py` | joins the notes file by grave key; `_meta.observed` provenance | 502 |
| `dist/collectable_spawns_pint-sized-phantom-graves.json` | rebuilt — 18 sites, photos on graves 7 and 9 | — |
