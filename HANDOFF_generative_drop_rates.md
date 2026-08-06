# Handoff — finish making farming drop rates fully generative

## Context
Repo: `dfbnb-data` (buffsnbrew.com / DuchessFlame). The "{Item} Spawn Locations"
farming pages (cream + the eggs) show vendor/world/creature drop rates. These used
to be **hardcoded** in `src/farming_spawns_config.py`. We rebuilt them to be
**computed from the datamined game data** by the shared `rng76` engine — nothing
typed in. Before touching drop-rate code, read the **drop-rate-engine** skill and
the **spawn-guide** skill (its §9e now documents this system).

## What's already done (do not redo)
- **`src/rng76.py`** — added three methods on `Rng76Resolver`:
  - `appearance_prob(list_id, formid)` — P(item appears ≥1 in one roll). Single scalar.
  - `pick_rate(list_id, formid)` — highest single-roll per-entry pick rate.
  - `entry_appearances(list_id, formid)` — **the one we display**: a list with one
    probability per contributing source entry (ChanceNone applied once; ForEach
    sub-list rolled qty N → `1-(1-A)^N`; UseAll `max_count` entry-cap "reached"
    scaling). For cream at a general vendor it returns `[0.7824, 0.2778]`, which
    **sum to the rng76 harness's 106.01852%** (the harness sums per-entry
    probabilities, so it can exceed 100%). All three verified against the harness
    and Monte Carlo. Existing `resolve_deep` is untouched (regression cases pass:
    Fertilizer 10/10/100, Enclave mods 2.5%).
- **`src/build_farming_used_for.py`** — `VendorRates` helper + `_patch_drop_rates`:
  - Every vendor rate is computed per-entry into `rate_lines` (e.g.
    `["78.24%","27.78%"]`), `rate_display` (joined), `rate_value` (max, sort only).
  - Named vendors resolve from each vendor's REAL inventory tree (`vendor.sells`
    EDIDs → LVLI FormIDs, keep ROOT lists only so nothing double-counts). Raider
    faction pools fold in automatically (extra `7.5%` line).
  - `_patch_drop_rates` computes ALL drop_rates sections: `vendors.*` and
    `creature_drops` → per-entry `rate_lines`; `world_spawns` (`list_id`/`list_ids`)
    and `containers` → single computed `rate`. **Containers** read the ItemTwo
    ChanceNone GLOB by EDID → `(1 - GLOB/100) × nest appearance` (deathclaw = 90%,
    data-driven). Each node gets `rate_source: "computed"`.
  - Sources with no probability data (scripted ACTI/FLOR harvestables) get
    `rate_source: "not_leveled_list"` — never fabricate a number.
- **`assets/df-bnb-farming-non-perishable-guide.js`** — `rate(v)` stacks
  `rate_lines` with `<br>` (two lines per vendor).
- **`src/farming_spawns_config.py`** — CREAM block already stripped to IDs only
  (`list_id`s, GLOB EDIDs, notes, Vera location/qty). Egg blocks still contain
  dead hardcoded `rate`/`rate_display` (overwritten at build — see task 1).
- **Skill doc** written to repo root: `spawn-guide-SKILL-updated.md` (fold into the
  plugin the way the other `*-SKILL-updated.md` files are handled).

Rebuild command:
```
python src/build_farming_spawns_json.py --all
python src/build_farming_used_for.py --all
```

## Current state of each page
All real (leveled-list) drop rates are generative. Only **mirelurk** and **mothman**
world eggs are scripted harvestables with NO probability in the export → flagged
`not_leveled_list`, still showing the old 100%.

## TODO — two tasks

### Task 1 — strip the now-dead hardcoded rates from `farming_spawns_config.py`
Remove every `rate` and `rate_display` (and the now-unused `chance_none_value`)
from the DEATHCLAW_EGG / FROG_EGG / MIRELURK_EGG / MOTHMAN_EGG / RADSCORPION_EGG /
RADTOAD_EGG blocks — for the sections that compute (vendors, creature_drops,
world_spawns with a resolvable list, containers). Keep `list_id`/`list_ids`,
`chance_none_glob`, `nest_list_id`/`container_id`, `qty`, `location`/`region`, and
notes. **Keep** the mirelurk/mothman `world_spawns` rate_display for now (they're
`not_leveled_list` — no data source yet; see task 2), but add a comment. Rebuild
with `--all` and confirm every `rate_source` is `computed` except mirelurk/mothman
world. A typed rate in config is a bug (it already hid the mirelurk/radscorpion
Whitespring "100%" that was really a pick-one pool → correctly now ~3–20%).

### Task 2 — make harvestables generative (xEdit export change)
mirelurk (`MirelurkEgg_Harvestable` 001715CD) and mothman world eggs are
FLOR/ACTI harvest nodes; the yield is defined in the FLOR "Produce"/ACTI record,
which the current TSV exports don't include. Spec + add an xEdit export that pulls
the harvest-produce (FLOR PFPC/PFIG produce component, or the ACTI's linked
ingredient/count) into a TSV, then have `build_farming_used_for.py` read it so the
harvest yield (currently a constant 100%) becomes data-driven and updates if
Bethesda changes it. The xEdit `!!!Wordpress - Export*` scripts live in the user's
xEdit Edit Scripts folder (not in the repo) — ask the user to paste the relevant
one to adapt it.

## Reminders
- Per drop-rate-engine §12: I only ADDED rng76 methods (didn't change existing
  formulas) and this pipeline imports rng76 directly, so the standalone copy in
  `build_activities_rewards_json.py` needs no mirroring.
- Verify with a rebuild + spot-check `rate_source` on every page after any change.
