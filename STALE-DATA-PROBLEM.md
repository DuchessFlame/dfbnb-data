# Prompt: stale datamined data is reaching the live site undetected

Paste this into a new thread. Repo: `dfbnb-data`. Site: buffsnbrew.com.

---

## What happened

The Pint-Sized Phantoms grave-site page has been serving **wrong** data, not just
stale data, and nothing in the pipeline noticed for five weeks.

`dist/collectable_spawns_pint-sized-phantom-graves.json` currently claims **19**
grave sites. The game has **18**. Specifically:

- Four references in the live JSON no longer exist in the game: `008F675C`
  (Hopewell Cave), `008F1738` and `008F6750` (Kanawha County Cemetery), `0090FBDD`
  (WV Lumber Co.). The page sends players to graves that aren't there.
- Three real references are missing entirely: `00930AA0` (Col Kelley Monument),
  `00930AA7` (Lady Janet's Soft Serve), `00930AAE` (Yellow Sandy's Still).
- Bethesda **re-placed** graves 05/07/09 under new ref FormIDs with new EDIDs
  (`SDOW__Grave05`, double underscore) at different map markers. A removal plus an
  addition — which reads as "deleted" unless something explicitly pairs them up.

This was only discovered by opening xEdit by hand and eyeballing the "Referenced By"
list for `ACTI 008F1672`.

## Why the pipeline could not catch it

The chain is:

```
xEdit export (MANUAL, needs game files)  ->  tsv/pts/REFR_Placements_*.tsv
build_phantom_grave_sites_tsv.py (LOCAL ONLY, needs Mappalachia DB)  ->  tsv/phantom_grave_sites.tsv
build_collectable_spawns_json.py (CI, every 6h)  ->  dist/*.json
```

- `dfbnb-patch-build.yml` runs every 6 hours and **does** rebuild the JSON. It was
  green the whole time. It was faithfully rebuilding from an input last committed
  **18 July**.
- `build_phantom_grave_sites_tsv.py` is **not referenced in any workflow**, and could
  not run in one anyway: it hard-exits without the 459 MB local-only Mappalachia DB.
- Neither workflow verifies the graves JSON at all. Both assert
  `collectable_spawns_manifest.json` and the *masks* file exist; nothing checks the
  graves file, its ref count, or its freshness.
- The patch-log scripts (`build_data_miner_patch_notes_json.py`,
  `append_patchlog_history.py`, `patchlog_utils.py`, `build_patches_json.py`)
  contain **no REFR / placement / spawn handling whatsoever**. Placement changes are
  structurally invisible to the patch log, so they never appeared in the
  patch-notes expand at the bottom of the page.

So: a green build, on a 6-hour cadence, publishing confidently wrong data, with no
signal anywhere.

## The questions I actually need answered

**1. Blast radius.** Which other pages are in this same position? The repo has
**799 TSV inputs, 665 of them predating August**. Some are legitimately old (records
that genuinely never change); some are stale and wrong. Nothing distinguishes the
two. I need a way to tell them apart — not a script I run, something structural.

Ten build scripts depend on local-only inputs that CI can never refresh:
`build_collectable_spawns_json.py`, `build_hto_locations_json.py`,
`build_phantom_grave_sites_tsv.py`, `build_vendors_json.py`,
`crossref_mappalachia_markers.py`, `farming_spawns_config.py`,
`nuka_cola_spawns_geo.py`, `render_spawn_maps.py`, plus two I added this session.
Every one of those is a candidate for the same failure.

**2. Root cause, not symptom.** This is an architecture problem, not a missing
check. Data that can only be refreshed by a human running xEdit will always drift.
Either the site must stop presenting it as current, or the refresh has to stop
depending on a human remembering.

**3. Patch log.** Placement changes (REFR added / removed / moved / re-placed under a
new FormID) should appear in the patch-notes expand at the bottom of each page, like
every other datamined change. Why were placements never included, and what is the
right way to fold them in?

## Constraints — please respect these

- **No more scripts.** The answer is not another `.py` I have to remember to run.
  Several band-aids already exist and they are part of the problem.
- **No "run this command every two weeks."** If the fix depends on my discipline it
  has already failed once and will fail again.
- **I won't re-export from xEdit for a single page** that will never change again.
  A fix that makes every page depend on manual exports is not viable.
- **Performance is already bad.** The site takes ~30 seconds to load because of the
  sheer number of JSONs being fetched and Python builds feeding them. Any answer that
  adds more JSON files or more build steps makes the primary problem worse.
  Consolidation is welcome; proliferation is not.

## What I want out of it

A diagnosis of how many pages are affected and how to identify them, then a design
that fixes the class of problem — ideally one that *reduces* the number of moving
parts rather than adding to it. Tell me if the current architecture should change
shape (fewer, larger, versioned data files? a single manifest with provenance and
freshness? server-side rather than per-page fetches?) rather than patching the
symptom.

## Immediate outstanding item

The grave JSON is still wrong right now. Photos for the two relocated graves are
converted, named and sitting unwired at:

```
Forest/AVIF for Upload/forest-lady-janets-soft-serve-grave-7-{region,map,item}.avif      -> 00930AA7
Savage Divide/AVIF for Upload/savage-divide-yellow-sandys-still-grave-9-{region,map,item}.avif -> 00930AAE
```

Upload prefix: `/wp-content/uploads/guide-images/treasure-maps/slasher-grave-locations/`

They can't be wired without a current REFR export, because the TSV rebuild drops any
row absent from the newest export and would take the photo paths with it. Fixing that
coupling is part of the problem above.
