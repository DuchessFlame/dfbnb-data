# GetRandomPercent operator fix — 30 Aug 2026

## What was wrong

`rng76.py` treated **every** `GetRandomPercent` condition as `<= N`. It isn't.
The TSV writes the CTDA "Type" byte as an 8-character bit string with **bit 0
first**, and the comparison operator is the low three bits — the first three
characters:

| bits  | value | operator | count in July 2026 export |
|-------|-------|----------|---------------------------|
| `101` | 5     | `<=`     | 1035 |
| `110` | 3     | `>=`     | 198  |
| `010` | 2     | `>`      | 2    |

Bits 3+ are flags; bit 5 = "use global", which is why those rows carry a
`[GLOB:xxxxxxxx]` reference instead of a literal number.

Reading a `>=` as `<=` made the First Match cascade subtract downwards, so the
rates went **negative**. The Auto-Miner Collectron shipped as Ultracite 98% /
Black Titanium −8% / Copper −10% / Aluminium −15% / Lead −15% / Iron 50%. It
should be — and now is — Iron 50 / Aluminium 15 / Lead 15 / Copper 10 / Black
Titanium 8 / Ultracite 2.

The `>=` conditions are concentrated in the collectron and resource-producer
produce lists, which is why the CAMP pages showed it worst: 347 negative rows
across 16 stations.

**Decoding confirmed against known data:** `RA_LL_Rewards_LegendaryItems`
(0086A8BD) exports `10100000 20.000000` and the skill documents it as `<= 20`;
`GetActorValueForCurrentLocation ... 01000000 0.000000` is plainly `> 0`; and
`ATX_Resources_Collectron_Raider_AlcoholAndChems` exports CNone GLOBs of
90/70/50/0 in that order, which under `>=` partitions cleanly into rare chems
10% / basic chems 20% / liquor 20% / beer 50% = 100%.

## What changed

`rng76.py` — new operator-aware GetRandomPercent layer:

- `parse_grp_condition(cond)` → `(operator_code, value_token)`, handling both
  the canonical `GetRandomPercent <= 50` form and the raw
  `GetRandomPercent(...) 10100000 50.000000` form.
- `grp_span_for_value(op, value)` → the slice of the 0–100 roll the condition
  covers. `<=`/`<` → `(0, N)`; `>=`/`>` → `(N, 100)`.
- `Rng76Resolver.extract_grp_span()` / `.extract_grp_chance()` — resolve the
  right-hand side (literal or GLOB FLTV) and return the interval / probability.
- `Rng76Resolver.first_match_rates(cond_lists)` — the First Match cascade as
  **interval coverage**: each entry claims the part of the roll range no earlier
  entry took. Ascending `<=` lists, descending `>=` lists and mixes of the two
  all work, and the rates total 100%.
- `extract_grp_threshold()` kept for callers that only want the raw number, with
  a docstring warning never to use it as a rate.
- `parse_randompercent_multiplier()` is now operator-aware: `<= 40` is a 40%
  gate, `>= 40` is a 60% gate.

All five First Match sites and all five UseAll-GetRandomPercent overrides inside
rng76 now route through the shared helpers.

### Standalone copies (drop-rate-engine skill §12)

Mirrored into every copy that carries its own cascade:

- `src/build_activities_rewards_json.py` — wrappers `_extract_grp_chance()` /
  `_first_match_rates_for()` delegating to the resolver.
- `src/build_events_rewards_json.py` — same.
- `src/build_seasonal_events_json.py`
- `src/spawns_configs/cryptids.py`
- `tools/build_activities_rewards_json.py` — the true standalone copy (no rng76
  import); the whole operator layer is duplicated there with a comment saying so.

### Memory fix (incidental, needed to run the builds)

`GLOB_Export_July_2026.tsv` is 34 MB across **5,582 columns** — one `RefN`
back-reference column per referencing record — for 5,873 rows. `read_tsv()`
materialised all 32.8 million cells, costing ~1.6 GB. Several item-name exports
are wide for the same reason.

Added `read_tsv_columns(path, wanted)` and pointed `GlobIndex` and
`ItemNameIndex` at it. `Rng76Data.from_tsv_root()` went from **1857 MB / 3.9 s**
to **257 MB / 2.0 s**. Verified byte-identical: `by_formid`, `by_edid`,
`glob.vals` and `glob.edids` all compare equal to the full-read versions.
`build_camp_items_json.py` got the same treatment for its own GLOB load.

## Verification

Known-good cases from the skill, all still correct:

| FormID | List | Expected | Got |
|--------|------|----------|-----|
| 00432CA5 | LLS_Scrap_Fertilizer | 10 / 10 / 100 | ✓ |
| 008494B6 | Enclave Plasma Gun mods | 2.5% × 40 | ✓ |
| 001642A9 | DEBUG PipeGun | 100% each | ✓ |
| 0086A8BD | Legendary Items | 20 / 18 / 62 split | ✓ (6.667×3, 4.5×4, 15.5×4) |

Across the whole export, **276** First Match lists have GetRandomPercent gates;
**272** now total exactly 100%. The four that don't are lists with no catch-all
entry, so the remainder is a genuine "nothing" chance:
`ATX_Resources_Collectron_BoS_MissionSupplies` (95%), `XPD_LLS_Rewards_Currency`
(40%), `RD01_LLS_Raids_Rewards_RepairKitImproved` (50%),
`P62_LLS_Drifter_Rewards_LegendaryItems_Tranche01` (50%).

No negative rates remain in `collectrons.json` or `resource_producers.json`.
`verify_camp_items_json.py` passes on all 11 CAMP files.

## Rebuilt

`dist/collectrons.json` (31), `dist/resource_producers.json` (106),
`dist/allies.json` (19), `dist/pets.json` (25), `dist/weather_stations.json` (19),
`dist/repair-bots.json` (4), `dist/cryos.json` (3), `dist/fridges.json` (20),
`dist/pet-furniture.json` (14), `dist/pet-apparel.json` (13).

Not rebuilt (user scoped this pass to CAMP): activities, public events, seasonal
events, daily ops, treasure maps, cryptids. Ten non-CAMP lists are affected —
bobbleheads, Daily Ops currency, Treasure Hunt chests, Colossus rewards, Moon
treasury notes — and will correct on their next normal build.

---

## Still open — data-shaped, needs a decision

### 1. Tied thresholds share a slice, or does the first win?

`SCORE_S17_Resources_Collectron_Scoutmaster` (00771DC4) authors its entries in
**tiers**: one at `>= 98`, three at `>= 90`, four at `>= 80`, four at `>= 70`,
three at `>= 60`, three at `>= 50`, then a catch-all. Strict First Match gives
the whole tier slice to the first entry in each tier and 0% to its siblings, so
the page shows Cranberries 2 / S'mores 8 / Mr. Fuzzy 10 / Combat Knife 10 /
Vegetarian Ham 10 / Tomahawk 10 / Wood 50 and eleven items at 0%.

The obvious authoring intent is that each tier's slice is **split** among the
items sharing that threshold (so `>= 90` → 8% ÷ 3 ≈ 2.67% each). This behaviour
is unchanged from before the fix — the siblings were already 0% — so nothing
regressed, but it is probably part of "the drop rates look wrong".

### 2. Contextual (non-GetRandomPercent) gates swallow the cascade

A First Match entry whose condition is `GetInCurrentLocation`, a weather check
or a keyword check is currently treated as "no gate → always passes", so it
takes the entire remaining roll range and every entry below it goes to 0%.

- **`LL_Mods_Armor_AllRegions` (0031364A)** — 8 region branches, one per region.
  Only Forest survives. This is what hides **380 items from Del Lawson's
  inventory** (1,118 shown, 380 dropped by the `chance <= 0` filter in
  `resolve_ally_inventory`): every Trapper / Combat / Leather / Robot armour mod
  from the other seven regions. It is the "inventory lists are missing items"
  report, and it is a *different* bug from the operator one — the operator fix
  alone changed no ally inventory at all.
- **`Fishing_LL_Forest` (007AED13)** and the other eight regional fishing lists —
  entries [2..8] are weather branches (Nuke / Radstorm / Rain / None / CAMP
  variants). Nuke takes 70% and the other six show 0%.
- 14 First Match lists are gated on `GetInCurrentLocation` with no
  GetRandomPercent at all; 171 lists overall have at least one entry the cascade
  leaves unreachable.

These branches are mutually exclusive *by world state*, not by the roll, so they
should each be reachable and labelled with their context (region / weather)
rather than cascaded to zero. `rng76.resolve_with_region()` already exists for
the region half of this. Deciding how the Inventory and Output & Effects
sub-expands present it is a page-design call, not something to guess at.

### 3. Gold / Silver Collectron use a 0–1 scale

`ATX_Resources_Collectron_Gold_Scrap` (005F0D28) and
`ATX_Resources_Collectron_Silver_Scrap` (0064A14A) compare `>= 0.95 / 0.82 /
0.65` — fractions, where every other list uses 0–100. They are also flagged
pick-one rather than First Match. Left resolving exactly as the TSV says, per
the same policy the skill uses for the Fertilizer/Screws flag bug (§13) and the
power-armour tier lists (§14): if Bethesda authored them on a 0–1 scale by
mistake, the fix ships with the next TSV drop and needs no code change.

`SCORE_S22_Resources_Collector_SoulSoupServer_Food` (008308D7) is the same shape
the other way round — textbook descending First Match thresholds
(92/80/63/45/25 + catch-all) on a list flagged pick-one, so it resolves as a
uniform 16.67% × 6 instead of a cascade. Also left as the data says.

---

Written by Claude, 30 Aug 2026.
