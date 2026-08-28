# Stale output audit — Aug 2026

Triggered by finding `dist/calculators/upcoming_rewards_s1..s23.json` still being
served months after those seasons launched. This is the sweep for the same shape
elsewhere: **output directories that are only ever written to.**

## The bug shape

A builder writes one file per key and never deletes anything. The directory can
only grow. When a key disappears from the source — a page retired, a slug
renamed, a family split in two — its file stays and keeps being served. Nothing
errors, nothing warns; stale data never announces itself.

## What was actually stale

77 orphaned files across five families, all removed:

| Family | Was | Now | Orphans removed |
|---|---:|---:|---|
| `dist/plants/` | 86 | 43 | 43 duplicate `-Sally` files no builder produces |
| `dist/farming_spawns/` | 114 | 94 | 20 (`-Sally` dupes + renamed `chems-super-chem-mk-*`) |
| `dist/pts/farming_spawns/` | 98 | 94 | 4 renamed chem slugs |
| `dist/meat/` | 39 | 31 | 8 insect pages left behind when insects split into their own family |
| `dist/cryptids/` | 15 | 13 | 2 (`beast-of-beckley`, `honey-beast`) — no page exists for either |
| `dist/pts/meat/` | 31 | 31 | mirror re-synced (it had drifted from 39 → 31 by overlay) |

`dist/insects/` and `dist/activity_guides/` were already clean.

**Verified before deleting:** every published page in `guide_index.tsv` under
meat / insects / plants / cryptids still resolves to a data file — 0 broken. The
eight `dist/meat/` insect files were duplicates: `df-bnb-farming-meat.js:469`
builds its URL as `RAW_ROOT + category + "/" + slug + ".json"` where `category`
comes from the page URL, so `/bnb/farming/insects/bloatfly/` reads
`dist/insects/bloatfly.json` and never touched the meat copy.

## The near miss

Keying a prune off one config list would have **deleted eight live pages**.
`dist/meat/` is written by `spawns_configs/meat.py`, whose `MEAT` roster does not
list insects — so a naive "delete anything not in MEAT" looked correct and would
have taken out `bloatfly`, `bloodbug`, `cave-cricket`, `fog-crawler`, `rad-ant`,
`radroach`, `radscorpion` and `stingwing`, all of which are published.

They were only safe to remove because the same slugs also exist in
`dist/insects/` and that is where the renderer reads them. **A prune must be
checked against what the site fetches, not against one builder's config.**

## What now prunes

`src/prune_outputs.py` is the shared helper. Wired into:

| Builder | Output | Keyed on |
|---|---|---|
| `spawns_configs/plants.py` | `dist/plants/`, `dist/pts/plants/` | `guide_index.tsv` rows (highest churn) |
| `spawns_configs/meat.py` | `dist/meat/` | `MEAT` config list |
| `spawns_configs/insects.py` | `dist/insects/` | `INSECTS` config list |
| `spawns_configs/cryptids.py` | `dist/cryptids/` | `PAGES` config list |
| `spawns_configs/farming.py` | `dist/farming_spawns/`, PTS | `ALL_SETS` (on `--all` only) |
| `build_activity_guides_json.py` | `dist/activity_guides/` | the manifest actually written |
| `build_upcoming_rewards_json.py` | `dist/calculators/upcoming_rewards_s*.json` | seasons written this run |
| `by_page_slices.py` | `dist/activities/by_page/`, `dist/events/by_page/` | already pruned |

Three rules the helper enforces, each of which had a real failure behind it:

1. **Prune after the build loop, never before.** `meat.py`, `insects.py` and
   `cryptids.py` all read the previous document back in to carry editorial
   fields forward.
2. **Skip when the run was filtered to one key.** Otherwise a one-slug run
   deletes everything else.
3. **Prune the mirror too.** `shutil.copytree(dirs_exist_ok=True)` only
   overlays — a file deleted from the source survives in `dist/pts/`
   indefinitely. `mirror_dir()` copies *and* removes;
   `build_farming_meat_json.py` now uses it.

## A prune that ran and was thrown away

`by_page_slices.py` has pruned correctly since it was written — but six
workflows staged only the monolithic JSON and never the slice directory:

    build-activities.yml, build-events-rewards-json.yml, build-public-events.yml,
    build-daily-ops.yml, build-expos.yml, build-raids.yml

So on any run triggered by those, the deletion happened on the runner and was
discarded. All six now `git add -A dist/{activities,events}/by_page`.

**Correct behaviour is not enough if the result is never committed.** Worth
checking whenever a workflow stages named files rather than a directory.

## Still open — found, not fixed

Ranked by how quietly they fail.

### Actively writing wrong data
- **`dfbnb-patch-build.yml:355` / `dfbnb-pts-build.yml:383`** — "Stage
  scoreboards patchlog feed (placeholder)" overwrites
  `patchlog_latest_df_scoreboards.json` with an empty, freshly-timestamped stub
  **every 6 hours**. The scoreboard pages therefore report "no recent changes"
  with a current timestamp, forever. This is the same pathology the comment at
  `dfbnb-patch-build.yml:1051` says was removed elsewhere.

### Will fail the build on correct data
- `build-head-hunt-bosses.yml:59-60` — `assert groupCount == 7` and
  `totalBosses == 30`. The PTS channel already sees an extra seasonal group.
- `build_fishing_daily_rewards.yml:81` — `campRecipePlans != 13`.
- `build-currency.yml:95` — `len(slots) == 24`.
- `build-fishing-challenges-checklist.yml:93` — asserts one named EDID still
  pays a Plushie.
- `dfbnb-patch-build.yml:1025`, `dfbnb-pts-build.yml:1132` — require one named
  seasonal collectable set to exist.

These should be floors or invariants, not exact equality on live game content.

### Silently stops checking / silently uses stale input
- `dfbnb-pts-build.yml:800` and `:843` — hardcoded seed lists for collectable
  and Nuka-Cola spawns, each with a comment saying "add a line when a new one
  ships". Anything shipped later loses its hand-filled content, and `|| true`
  makes it silent. `Seed Farming Spawns` at `:869` enumerates from
  `git ls-tree` and does not go stale — copy that.
- `dfbnb-patch-build.yml:738,748,751` — dig-site and grave-site TSV builders run
  with `|| true`; a crash leaves the committed TSV in place with nothing
  asserting freshness. This is the exact chain behind `STALE-DATA-PROBLEM.md`.
- `tools/build_curves_json.mjs:22-24` — builder default is a literal
  `PCRD_Export_March_2026.tsv`. Four workflows warn and continue when their glob
  misses, so a rename means silently building from March 2026 data.
- `verify_camp_items_json.py --allow-missing` (5 workflows) — a missing file
  becomes `SKIP`, so a builder that stops emitting passes the contract check.
- `build-df-calculators-json.yml:94` — `git add dist/calculators/*.json` misses
  anything written to a subdirectory.
- Four challenge workflows regenerate `patchlog_latest_df_challenges.json` and
  never stage it.

## The convention

Any builder that writes one file per key must prune. Use
`prune_outputs.prune_outputs()`; do not hand-roll it. If two builders share an
output directory, keep the glob tight enough that each only prunes its own
family — and check what the *site* fetches before deleting anything.
