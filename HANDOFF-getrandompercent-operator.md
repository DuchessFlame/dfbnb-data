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

# Second pass — tied thresholds and world-state branches

Both of the items flagged as open above are now fixed.

## 4. Tied thresholds split their slice

`SCORE_S17_Resources_Collectron_Scoutmaster` (00771DC4) authors its entries in
**tiers**: one at `>= 98`, three at `>= 90`, four at `>= 80`, four at `>= 70`,
three at `>= 60`, three at `>= 50`, then a catch-all. A strict reading handed
the whole tier slice to whichever entry was listed first and left its siblings
at 0% — eleven dead items on that one station.

`Rng76Resolver._first_match_pass()` now groups entries by identical span,
positioned at the group's first appearance, and splits the group's uncovered
width evenly. Scoutmaster resolves to 2 / 2.667×3 / 2.5×4 / 2.5×4 / 3.333×3 /
3.333×3 / 50, totalling 100%.

**Only a real, shared gate groups.** Entries with no GetRandomPercent at all get
a key unique to themselves, so two ungated entries never "tie" — the first one
genuinely takes the rest of the range and the ones after it genuinely are
unreachable. Getting this wrong made `Fishing_LL_Forest`'s weather branches
split 50/50 with the fallback.

A list without ties is untouched: a group of one gets the whole slice, exactly
as before.

## 5. World-state branches get their own cascade

A First Match or waterfall entry gated on `GetInCurrentLocation`, a weather
condition form, a global flag or a worn-keyword check is not competing for the
roll — nothing is rolled, and exactly one branch is live at a time depending on
where the player is and what is happening. Treating those as "no gate → always
passes" made the first branch swallow the range and everything below it report
0%.

New in `rng76.py`:

- `first_match_contexts(cond_lists)` — the tuple of non-GetRandomPercent
  conditions on each entry, or None. Entries sharing a key are branches of the
  same choice.
- `context_passes(cond_lists)` — the ungated entries first ("nothing special is
  happening"), then one group per distinct gate holding the ungated entries plus
  that gate's branches, in list order. Entries sharing a gate land in the same
  group and still compete, which is what keeps the two Fasnacht entries behind
  `Update01_Quest_Fasnacht` cascading against each other (10% / 2%) instead of
  both claiming the top of the range.
- `waterfall_rates(drops, cond_lists)` — the UseAll + `max_count == 1` cascade,
  also run per context, and now the single implementation behind all three
  waterfall sites in the module.

An entry reports its best rate across the passes it takes part in — "when this
branch is the live one, this is the chance". **Each individual pass totals
100%, so a multi-context list sums above 100% by design.** Those branches are
alternatives, never simultaneous. This matches how the codebase already treats
multi-path items (`consolidate_drops` keeps the max; `pick_rate` documents
returning "the highest (least-gated) path").

What this recovered:

| List | Before | After |
|------|--------|-------|
| `LL_Mods_Armor_AllRegions` (0031364A) | Forest only | all 8 regions |
| `Fishing_LL_Forest` (007AED13) + 8 more | Nuke only | all 7 weather branches |
| `LPI_Clothes_LocTheme` (00064B92) | 1 of 20 | 20 of 20 |
| `LLS_UseItem_Contextual_Ammo` (006A2511) | 1 of 41 | 41 of 41 |
| `CreatureOutfit_Scorched` (0038F0BB) | 1 of 17 | 17 of 17 |

**Del Lawson's inventory went from 1,118 items to 1,498** — the 380 Trapper /
Combat / Leather / Robot armour mods from the other seven regions are back. That
was the "inventory lists are missing items" report, and it was a different bug
from the operator one: the operator fix alone changed no ally inventory at all.

The three collectron stations that gained entries: Scoutmaster (12 items off 0%),
Santatron (16), Fasnacht (10), plus Ashforge (22) and Brewing Vat (2) among the
resource producers.

## Second-pass verification

Known-good cases from the skill are all still exact — Fertilizer 10/10/100,
Enclave Plasma mods 2.5% × 40, DEBUG PipeGun 100% each, Legendary Items
6.667×3 / 4.5×4 / 15.5×4, Auto-Miner 50/15/15/10/8/2. None of those lists has a
tie or a world-state gate, so they route through the same code path as before.

410 First Match lists now resolve; every context pass totals exactly 100% except
seven lists with no catch-all entry, where the remainder is a genuine "nothing"
chance (`ATX_Resources_Collectron_BoS_MissionSupplies` 95%,
`LLS_Ammo_Loot_Contextual_NoFallback` 10% — the EDID says so —
`XPD_LLS_Rewards_Currency` 40%, `RD01_LLS_Raids_Rewards_RepairKitImproved` 50%,
`P62_LLS_Drifter_Rewards_LegendaryItems_Tranche01` 50%, `LPI_FloraTatoPlant01`
and `02` 65%).

The standalone copy in `tools/` was checked against `rng76` by extracting its
helper functions with `ast` and running both implementations over the same five
shapes — ascending `<=`, descending `>=`, a tied tier, region branches, and a
mixed context+roll entry. Identical to 1e-12 on all five.

`verify_camp_items_json.py` passes on all 11 CAMP files; no negative rates
anywhere.

---

# Still open — genuine data bugs, reported to Bethesda

Ten collectron leveled lists are wrong in the game data, in four distinct ways.
Bug reports written and saved to the Buffs n Brew workspace folder:

- `bethesda-bug-report-bivbev-collectron-single-item.txt` — the three Biv Bev
  pools are waterfalls with ChanceNone 0 on every entry, so only the first item
  can ever drop (22 items unreachable).
- `bethesda-bug-report-raider-sirloin-collectron-first-pool-only.txt` — Raider,
  Free States and Sir Loin waterfalls whose *first* entry is ChanceNone 0, so
  the rarity ladder below it is dead.
- `bethesda-bug-report-collectron-firstmatch-threshold-order.txt` — BoS,
  Santatron and Raider Ranged First Match thresholds out of order.
- `bethesda-bug-report-collectron-missing-firstmatch-flag.txt` — Soul Soup
  Server, Gold and Silver authored as tiers but with no First Match flag; Gold
  and Silver additionally use a 0–1 scale.

The site resolves all of these exactly as the data says, so they self-correct on
the next TSV drop once Bethesda fixes them — no code change needed. That is the
same policy the drop-rate-engine skill applies to the Fertilizer/Screws flag bug
(§13) and the power-armour tier lists (§14).

### Gold / Silver Collectron use a 0–1 scale

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

Both are covered by `bethesda-bug-report-collectron-missing-firstmatch-flag.txt`.

---

# Third pass — full rebuild of the downstream pages

## `build_all.py` is not the pipeline runner

Worth writing down because it reads like one: `src/build_all.py` is a 35-line
script that dumps **every** TSV in `tsv/` straight to `dist/<name>.json`. Running
it would flood `dist/` with 352 raw exports, GLOB's 5,582-column table included.
The real rebuild is the individual builders the workflows call.

## An over-count the context change introduced, and the fix

Rebuilding `drop_rates.json` surfaced a bug in my own work. `resolve_simple()`
and `appearance_prob()` **sum** each entry's contribution, which is right for
entries that fire together but wrong for world-state branches — those are
alternatives. An 8-region armour-mod list was reporting ~800% for a recipe that
appears in every region.

The first fix (ungated sum + best branch) was still wrong: on a First Match list
the ungated fallback only fires when *no* branch matched, so it is an
alternative too. `LLS_XPD_Contextual_AmmoType_Quest_Repeatable` reported 105.56%
for Broadhead Arrows — 100% from its own carried-weapon branch plus 5.56% from
the fallback pool.

The fix is `Rng76Resolver.rate_vectors()`: resolve the list once per context and
return a rate vector per pass. `resolve_simple` then sums **within** a pass and
takes each item's **best** pass; `appearance_prob` does the same. A list with no
world-state gate returns a single vector, so its arithmetic is untouched.

Result: pool entries over 100% went 139 (pre-session) → 825 (my first attempt) →
**127 now**, i.e. slightly below the baseline. The remaining 127 are legitimate —
`LL_DailyOps_Rewards_AdditionalCurrency_Tier03` draws the same currency pool
twice (once ungated, once in mutation mode) for a genuine 120%.

## What was rebuilt

| Builder | Time | Note |
|---------|------|------|
| `build_drop_rates.py` | 49 s | 260 pools changed, **4,890 entries went from unreachable to a real rate** |
| `build_activities_rewards_json.py` | 110 s | 82 pages, 164 by-page slices |
| `build_events_rewards_json.py` | 96 s | 82 pages |
| `build_seasonal_events_json.py` | 10 s | +757 KB |
| `build_daily_ops_json.py` | 6 s | |
| `build_treasure_maps_json.py` | 8 s | +321 KB |
| `build_cryptids_json.py` | 12 s | 12 live + 13 page docs |
| `build_fishing_calculator_json.py` | 2 s | |
| `build_fishing_daily_rewards_json.py` | 5 s | |
| `build_farming_meat_json.py` | 17 s | 31 pages |
| `build_farming_guides_json.py` | 14 s | |
| `build_farming_used_for.py --all` | ~5 min | 94 slugs + the `chem_loot_collapse` pass |

82 files changed, all parse, **zero negative rates anywhere**, CAMP contract
still passes.

## Fishing did NOT move — I was wrong about that

`dist/fishing.json` is byte-identical (41,299 bytes both sides). The
`Fishing_LLS_*_WeatherCheck_*` First Match lists I fixed are real and the fix is
correct, but no built page consumes them: `build_fishing_calculator_json.py`
models weather from GLOBs directly, and the fishing pools aren't among the 1,988
in `drop_rates.json`. The prediction that "fishing will move a lot" was wrong.

## Two things to watch when running these locally

**`src/build_fishing_json.py` is the legacy script — do not run it.** The comment
at `dfbnb-patch-build.yml:851` says so explicitly. It writes a competing
`dist/fishing.json` with 67 fish against the calculator's 74, and its patchlog
entry claimed 7 fish had been removed. That entry was stripped back out.

**Several builders wipe their patchlog history when run outside CI.** The
workflows check out with `fetch-depth: 2` so the writer has a diff baseline;
without it the feed is replaced by a single empty entry.
`patchlog_latest_df_cryptids`, `_df_treasure_maps`, `_df_farming` and
`_bnb_farming_meat` were each flattened and restored from `git show HEAD:<path>`
(`git checkout` fails on this mount — "unable to unlink", so redirect the blob
over the file instead). Only three patchlogs now differ from the pre-session
state, all legitimately: `_df_activities` (3 changed), `_df_events` (no change
recorded), `_df_fishing` (the bogus legacy entry removed).

## Memory: the wide-column problem is broader than GLOB

`read_tsv` in `build_activities_rewards_json.py`, `build_events_rewards_json.py`,
`build_treasure_maps_json.py` and the `tools/` copy now drops xEdit's `RefN`
back-reference columns on the way in. Note the underscore: the COBJ/BOOK exports
use `Ref_1`..`Ref_37` and those **are** read, so only the unsuffixed form is
dropped. Without this the activities build is OOM-killed at 11 s on a 4 GB box.

## Not rebuilt

`build_spawns.py` — it is not in the patch pipeline, and it needs the local
Mappalachia Position DB (`D:\Mappalachia\data\mappalachia.db`, ~480 MB) to
resolve coordinates. Running `deathclaw-egg` from the committed geo cache alone
dropped the file from 50 KB to 34 KB and lost the whole collectrons block, so it
was reverted. The collectron cards come from `build_farming_used_for.py`, which
did run. Run the spawn families on the machine that has the DB.

---

## Final state

| File | Lines |
|------|-------|
| `src/rng76.py` | 2483 (was 2029) |
| `src/build_camp_items_json.py` | 1462 (was 1452) |
| `src/build_activities_rewards_json.py` | 5070 (was 5016) |
| `src/build_events_rewards_json.py` | 3502 (was 3453) |
| `src/build_seasonal_events_json.py` | 3919 (was 3922) |
| `src/build_treasure_maps_json.py` | 2391 (was 2370) |
| `src/spawns_configs/cryptids.py` | 1192 (was 1196) |
| `tools/build_activities_rewards_json.py` | 4117 (was 3934) |

Rebuilt across all three passes: the 11 CAMP files, `drop_rates.json`,
activities (+ 83 by-page slices), events, seasonal events, daily ops, treasure
maps, cryptids (+ 13 page docs), fishing, meat (31 pages), farming guides and 94
farming spawn docs. 82 files changed in total, none added or removed.

Left for the machine with the Mappalachia DB: the `build_spawns.py` families
(nuka-cola, farming, chems, plants, bobbleheads). Everything else is current.

---

Written by Claude, 30 Aug 2026.
