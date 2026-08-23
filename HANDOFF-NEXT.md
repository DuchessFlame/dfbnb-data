# Prompt: what's left on the dfbnb-data staleness work

Paste this into a new thread. Repo: `dfbnb-data`. Site: buffsnbrew.com.
Theme files live in OneDrive at `Guides and Stuff\Json Files for Website\1 site-data\json\dfbnb-child\`.

_Last updated 2026-08-23._

---

## Context

A previous session traced why the Pint-Sized Phantoms grave page served wrong data for five
weeks with no signal. The full diagnosis is in `STALE-DATA-DIAGNOSIS.md` — **read that first**,
it explains why each remaining item matters.

### Already done — don't redo

- **`src/tsv_source.py`** — one resolver for every export selection. Chronological, never
  lexical, never mtime, never crosses live/PTS. 22 duplicate date helpers delegate to it;
  ~36 ad-hoc selection sites replaced. `python src/tsv_source.py --lint` fails the build if
  either pattern reappears, and runs first in both workflows.
- **Grave data corrected** (18 sites), editorial content moved to `tsv/phantom_grave_notes.tsv`
  keyed on grave number so a placement rebuild can't delete it.
- **Export ledger** — all 49 xEdit scripts append one line to `tsv/_exports.tsv` per export.
- **Patch log rewritten** — diffs game exports against each other, not our own output.
- **Placement generators are CI-able** — `data/mappalachia_geo.json` replaces the 480 MB DB.

### Done 2026-08-23

- **Perk↔curve linkage — root cause found and fixed.** It was never only a column-name bug.
  The July PERK export was taken with a script that omits `EffectLink_1..30`,
  `Spell_FormID` and `CurveTable_FormID` — the only route from a perk to its curve. Verified
  dead ends: `SPEL_Export_*_EFFECTS.CVTO_CURV_FormID` and `ENCH_Export_*.Effect_N_CURV_EID`
  are declared but empty in every row, and `MGEF.PerkToApply` is empty in all 3,069 records.
  March resolves 40 perk cards with curves (exactly what is published); July resolves 6.
  - `tools/build_curves_json.mjs` now reads both export schemas (`FormID`/`CURV_FormID`,
    `Ref_1`/`Ref1`, `Rank_N_Male|FemalePerk_FormID`/`RankPERK_N_FormID`, `MNAM_Name`/
    `MNAM_MaleName`), strips the quotes xEdit wraps cells in, throws if the CURV ref index
    comes out empty, and stamps a `linkage` block into `perk_cards.json` naming its sources.
  - It picks the PERK export by **measurement, not by header**: `perkLinkScore()` counts how
    many of each candidate export's links actually land in the CURV ref index built by that
    same run, and takes the newest export scoring as well as the best available. A header
    check is not enough — the August 2026 export declares `CurveTable_FormID` and
    `Spell_FormID` and leaves every row of both empty, and its `EPFD_Float` still yields 274
    FormIDs, so it passes any weaker test and still resolves 6 perk cards instead of 40.
    Scored fresh each build against the current CURV export, so it is not the
    preserve-the-last-known-answer pattern and a fixed export wins outright the moment it
    lands. Current scores: August 24, March 98. **This is what stops the live page
    regressing 40 → 6.**
  - `.github/workflows/build-perk-cards.yml` and `build_curves_json.yml` set PCRD/PERK/
    CURV_HDR the same way `dfbnb-patch-build.yml` does. They were still falling through to
    the literal `tsv/*_March_2026.tsv` defaults.
  - **The export fix landed in the real script**: `GitHub\xedit scripts\!!!Wordpress -
    ExportPERKToTSV.pas`, the one with the channel prompt and the `_exports.tsv` ledger
    line, and the one that actually produced the July export. It now emits
    `CurveTable_FormID/EDID`, `Spell_FormID/EDID`, `EffectLinks_Flat` and
    `EffectLink_1..N` — variable width, same `maxRefs` pattern the ref columns already use,
    de-duped so a repeated condition parameter can't fill the row with one link. It warns at
    the end of the run if it finds no outgoing links at all. Re-run it and the linkage comes
    from a current export instead of March.
- **The two monster payloads are split.** `src/by_page_slices.py` writes one file per page
  plus a small `_index.json`; `build_activities_rewards_json.py` and
  `build_events_rewards_json.py` call it beside the monolith. Measured on the real files:
  67 MB → 86 KB and 60 MB → 86 KB per pageview, and disk halves too (67 MB → 33.5 MB)
  because slug and URL keys were storing two copies of every page. All 164 keys verified to
  round-trip byte-identical.
  - `df-bnb-guide.js` gained `window.DFBNB_BY_PAGE.loadByPageSlice()`; `df-bnb-activities.js`,
    `df-bnb-events-rewards.js` and `df-bnb-public-events.js` use it and fall back to the
    monolith. Their `prefetch()` warmed the whole monolith — they now warm one slice.
- **31 PTS exports that were filed as live are gone.** Found by hashing every live export
  against every PTS export: 31 files in `tsv/` were byte-identical to exactly one PTS sweep,
  2026-06-27 — the whole June "live" sweep was that PTS sweep copied into the live folder.
  The graves bug again, committed at the filing step rather than in code.
  - **It answers the BOOK question.** The 88 records "added in June, removed in July" were
    SDOW (Pint-Sized Slasher), SCORE_S25/S26 and STORM plans — unreleased content that was
    never on live. Not real churn; a channel leak.
  - Removal was lossless (every one is still in `tsv/pts/` under its real name) and verified
    byte-identical immediately before each unlink. All 31 export families resolve to exactly
    the same July/August file afterwards as before — no regressions.
  - `python src/tsv_source.py --channels` is the new check, and also runs after `--lint`. It
    reports rather than fails: which copy to keep is the author's call, not CI's. The
    discriminator is how many PTS sweeps a live file's hash matches — exactly one means a
    copy, more than one means a record type that simply isn't changing.
- **`src/build_farming_guides_json.py` no longer hardcodes filenames.** It carried twelve
  hand-typed newest-first lists — a fourth independent "which export is newest", needing an
  edit every sweep, and unable to notice that several of the files it named were the PTS
  copies above. Now derived from `tsv_source` via `_EXPORT_PATTERNS` / `export_globs()`,
  keeping the ordered-chain behaviour that several callers depend on. Side effect: ALCH now
  resolves to the **August** export, which the old list never mentioned.
- **`tools/*.pas` is empty — all six removed.** The real xEdit library lives in
  `GitHub\xedit scripts\` (49 scripts, all with the channel prompt and the `_exports.tsv`
  ledger line). The repo held stale copies that shadowed them:
  - `ExportPERKToTSV` was a March-format reproduction that had never been run.
  - `ExportACTI2ToCSV`, `ExportMISC2ToCSV`, `ExportNPCToTSV`, `ExportOMODToTSV` were all
    older than their real counterparts — none had the ledger, two lacked the channel prompt.
    Checked for unique content first: the only difference was NPC's `AJNG_LvlMinGlob` /
    `AJXG_LvlMaxGlob`, which the real script already covers as `AJNG_EDID` / `AJXG_EDID`.
  - `ExportNukaColaLocationsToTSV` was the pre-Mappalachia attempt — committed 2026-08-01,
    never run (its `NukaCola_CollectableLocations_*.tsv` exists nowhere), nothing reads it,
    and Nuka Cola spawns now go through `spawns_configs/nuka_cola.py` via the spawns engine.
    Recoverable with `git show 702bcd39d:tools/ExportNukaColaLocationsToTSV.pas` if the
    Mappalachia route ever needs replacing.

  **Edit xEdit scripts in `GitHub\xedit scripts\`, never in the repo.** A second copy in
  `tools/` is how the PERK fix nearly landed in a file that is never run.

---

## What still needs doing

### 1. Nothing is deployed yet — do this in this order

**Order matters.** The new patch log feeds carry objects where the old ones carried strings.
The old renderer does `String(item)` on them, so data-before-JS renders every Added/Removed
line as `[object Object]`. The same now applies to the by-page slices: the new JS reads both
shapes, the old JS cannot read slices.

1. `cd` to the theme folder and run `.\sync_theme_to_site.ps1` (preview), then `-Go`.
   First run: use `-Only df-bnb-guide.*,df-bnb-activities.*,df-bnb-events-rewards.*,
   df-bnb-public-events.*,df-bnb-admin-portal.*`, or `-Baseline` first if local and the
   server are already in step. Credentials go in `wpengine.credentials.ps1` beside it.
2. Commit and push `dfbnb-data`. The push triggers `dfbnb-patch-build` by itself. The PTS
   build needs a manual dispatch or a `tsv/pts` push.

### 3. Re-export PERK — the August attempt is not there yet

`tsv/PERK_Export_August_2026.tsv` (23 Aug) was the first run with the new columns. Partial
result:

| Column | Outcome |
|---|---|
| `CurveTable_FormID` / `_EDID` | present, **0 rows populated** |
| `Spell_FormID` / `_EDID` | present, **0 rows populated** |
| `EffectLinks_Flat` | present, **0 rows populated** |
| `EffectLink_1..N` | **absent** — `maxLinks` was 0, so no columns were emitted |

Cause: the first version of the helpers re-looked-up `EPFD` and the conditions with
`ElementBySignature` / `ElementByPath` on the effect element. In this script EPFT/EPFD/EPFB
are nested inside a "Function Parameters" container and conditions are found by scanning
child names, which is why `Process` walks children by index instead of using fixed paths.
The lookups found nothing, so every link came back nil.

Fixed 2026-08-23: `CollectEffectLinks` and `ResolveEffectTarget` now take the `condEl`,
`epfdEl` and `dataEl` that `Process` already resolved, and a new `DeepLink()` helper tries
`LinksTo` on the element and then on its children (EPFD is often a struct — `Spell=... [SPEL:0079C8A9]`
— where `LinksTo` on the struct itself returns nothing).

**Re-run it.** The xEdit log must say `Max EffectLinks:` with a number above zero; it warns
explicitly if it is zero. For reference, March had 1,214 perks carrying links and August had
226 (EPFD spells only, no condition links).

Until a good export lands, the builder reads March for linkage and says so in
`perk_cards.json` — the page keeps its 40 charts either way.

**Then retire two things:**
- Once a complete PERK export is in, `src/build_curves_json.py` can go. It is dead code in
  no workflow, shadows the `.mjs` CI actually runs, has the same wrong column names, and is
  destructive — it wipes `dist/curves/`, so running it by hand clobbers the good file.
- Once the slice-aware JS is live, delete the monolith writes in
  `build_activities_rewards_json.py` and `build_events_rewards_json.py` (both are commented
  where they are), and drop the two `*_rewards_by_page.json` files.

### 4. Still on `raw.githubusercontent.com`

Not a site CDN — short cache, rate limits, no edge control. jsDelivr fronts the same repo
properly, or proxy same-origin from `functions.php`. Now more worthwhile than before: the
slices make many small requests where there used to be one huge one, so TTFB matters more.
`dist/` is 722 MB tracked; the big artifacts belong as release assets.

### 5. Map image renderers aren't in CI

`src/render_slasher_maps.py` and `src/render_spawn_maps.py` produce PNGs and are in no
workflow. Worth deciding whether they should be automated at all.

### 6. Exports are still manual — the actual remaining root cause

Everything above makes staleness **visible**. Nothing makes it **impossible**. §4.3 of
`STALE-DATA-DIAGNOSIS.md` proposes registering the local PC as a self-hosted GitHub Actions
runner, watching the game install's build number, and running xEdit headless on change.
Highest effort, and the only item that closes the loop.

### 7. Outstanding one-offs

- **A LIVE REFR export has never been run.** Base FormIDs: `008F1672` (ACTI, graves),
  `008E069E` (MISC, masks), `000355A1` (ACTI, U-Mine-It), and
  `TreasureMapMoundActivator01–35` (ACTI, 35 records, skip `…TEMP` `00142FB6`). Run
  `Other → Build Reference Info` first. The grave page shows a red "never checked against
  the live game" banner until this happens — which is correct.
- **`dist/pts/_pts_changelog.md`** shows as modified but is byte-identical apart from line
  endings. `git checkout -- dist/pts/_pts_changelog.md` on Windows.
- ~~The June→July BOOK 88-record add-then-remove.~~ **Answered — see item 1.**

---

## Constraints (standing)

- **Generative or nothing.** Every page must rebuild itself from the game files. Hand-typed
  data and "run this occasionally" are both failures.
- **No new scripts to remember.** Modules that builders import are fine; another `.py` to
  run by hand is not.
- **Consolidation, not proliferation** — of build steps and of *bytes fetched per pageview*.
  More small files on disk is fine if it reduces what the browser downloads.
- **Don't guess at record semantics.** If a fix needs a decision about what a game record
  means, ask rather than inventing something plausible. Confidently wrong data is the exact
  failure this whole effort exists to stop.
- Answers concise and direct; check claims against the repo before asserting them.
