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

### DONE (2026-09-19) — region grouping built

The BOOK2 export landed (`tsv/BOOK2_Export_September_2026.tsv`, 1,991 placement
rows / 1,289 notes) and the region grouping is built:

- `build_collectables_json.py` now has `resolve_note_regions()` (called after
  `build_notes`). It point-in-polygons each placement via
  `crossref_mappalachia_markers` (Mappalachia DB locally, else the committed
  `data/mappalachia_geo.json` snapshot — so CI resolves too), with an LCTN
  location-name fallback + manual AC/The Pitt map for interiors, then
  "Other/Unknown". Emits a `region` field per note.
- Coverage: **1,003 / 1,553 notes (65%)** resolved to a region (coords 843,
  LCTN-name 146, manual 14). **550 -> Other/Unknown**, of which **201 are
  quest-given** (no placement). The rest are interior placements with cell-local
  coords the map can't point-in-polygon and whose location name didn't match
  LCTN — this is the practical ceiling from current data (the note's `location`
  string already equals the cell name, so a richer export wouldn't add much;
  only per-interior exterior-door coords would, a much deeper job).
- Renderer `df-bnb-collectables.js`: the Notes page groups by region (canonical
  A-Z root expands), env-spawn notes folded into their region (keep the 📌
  badge), "Other/Unknown" then "Cut Content" trailing. Holotapes/magazines/keys
  are untouched. Per-note checkbox + Location/Contents/Technical sub-expands
  kept; progress excludes env-spawns (0 of 1,416). jsdom-verified.

Note: the earlier draft `tools/ExportNoteLocationsToTSV.pas` was superseded and
neutered to a placeholder (delete it when convenient — auto-delete was blocked).

— Claude (Cowork), for Duchess
