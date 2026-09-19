# Handoff — Notes page: season NEW pill (DONE) + region grouping (BLOCKED on export)

Date: 2026-09-19

## 1. Collectables NEW pill — season window (DONE)

`src/build_collectables_json.py` no longer uses a rolling 30-day window for the
`isNew` flag. It now pins to the **current season** from `tsv/fallout76_seasons.tsv`:

- New helpers `_parse_dmy()` + `current_season_start()` read the seasons TSV the
  same way `build_new_plans_json.py` does and return the latest season whose
  StartDate <= today.
- `compute_new_cutoff(seasons_path)` returns that season-start date; falls back
  to the legacy 30-day window if the TSV is missing/unparseable.
- Applies to **all seven** collectable types (shared `apply_is_new` loop):
  bobbleheads, plushies, notes, holotape games, magazines, holotapes, keys.
- The renderer (`df-bnb-collectables.js`) reads `item.isNew` directly — no JS
  change needed.

Verified on the 2026-09-19 rebuild: cutoff = 2026-09-15 (S26 start), 20 items
flagged new (all first-seen 2026-09-17), 0 mismatches vs first-seen dates.

Backup of the pre-edit script: `src/build_collectables_json.py.bak-20260919-105922`.

**Deploy:** push `src/build_collectables_json.py` (+ rebuilt `dist/collectables_*.json`)
to `main`. The "Build Collectables JSON" workflow rebuilds + commits all seven
JSONs and re-derives the season each run, so it rolls over on its own.

## 2. Notes region grouping — BLOCKED on a coordinate export

Goal: regroup the Notes checklist into FO76 map regions (root expands) with the
notes nested by region; env-spawns fold into their region (icon kept); Cut
Content stays a trailing group.

Blocker: notes only have a location *name*, no coordinates. The real region
engine (`src/crossref_mappalachia_markers.py`, same one the spawn pages use)
resolves region by point-in-polygon on placement X/Y. Name-only matching via the
LCTN table covers ~33% (509/1553); the rest need coordinates.

**Action needed (you):** run the new xEdit script to export note placements.

- Script: `GitHub\xedit scripts\!!!Wordpress - ExportBOOK2ToCSV.pas`
  (notes are BOOK records in FO76, so this is a BOOK second pass, modelled on
  `ExportMISC2ToCSV.pas` / `ExportACTI2ToCSV.pas`).
- Run it the same way you run the MISC2/ACTI2 passes: load your FO76 plugins,
  select the same BOOK selection you use for `ExportBOOKToTSV`, right-click ->
  Apply Script -> ExportBOOK2ToCSV. It filters BOOK -> note-type itself
  (model path 'note' or MiscNote01/02/03_Torn keyword).
- Channel prompt writes `BOOK2_Export_<mmmm_yyyy>.tsv` into `tsv\` (LIVE) or
  `tsv\pts\` (PTS) — 14-column contract `resolve_dataset()` already reads.

Once the TSV is in, the remaining work is: wire `build_notes()` to call
`xref.resolve_dataset(input_glob="tsv/BOOK2_Export_*.tsv")`, emit a `region`
field per note (canonical 10 regions; quest-only notes -> "Other/Unknown"), and
group the renderer by region. Not started — waiting on the export.

Note: the earlier draft `tools/ExportNoteLocationsToTSV.pas` was superseded and
neutered to a placeholder (delete it when convenient — auto-delete was blocked).

— Claude (Cowork), for Duchess
