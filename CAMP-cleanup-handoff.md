# CAMP category cleanup — what changed + build-script handoff

_June 2026. Files edited live in `…/dfbnb-child/assets/`. Data enrichment (this
doc's second half) must be run on your machine — the build pipeline can't run in
the Cowork sandbox because the OneDrive mount serves bash truncated copies of
files._

## Part 1 — renderer + CSS (done, live)

Edited `df-bnb-camp-items.js`, `df-bnb-camp.js`, `df-bnb-camp-items.css`:

- **Head pills removed on every page.** Per the camp-item-expands spec, source
  and tradeability are per-obtain-route, so a single head pill is wrong for
  multi-route items. That info now lives in How to Obtain (per route) + Technical.
  This kills the "some pages have pill boxes, others don't" inconsistency.
- **One skin for the whole category.** The clean `apw-sg` list skin is now applied
  to every page type (was only the 5 migrated ones). The legacy `pet-furniture`
  and `pet-apparel` pages now use the same 5-sub-expand body as everything else
  instead of the old 3-box grid.
- **Route resolution fixed.** `resource-generators` (the real nav slug) now resolves
  to the resource-producers renderer, and the combined **`fridges-cryos`** page now
  loads BOTH source files and shows them under Fridges / Cryos headings — previously
  it silently dropped every cryo item.
- **Double-render collision fixed.** `camp-pets` / `camp-pet-furniture` /
  `camp-pet-apparel` were owned by BOTH `df-bnb-camp.js` and `df-bnb-camp-items.js`
  (both tried to render them). They're now fully migrated into
  `df-bnb-camp-items.js` (unified layout + checklist + live search), with their
  original progress ids preserved (`camp:camp-pets` etc.) so saved ticks survive.
  `df-bnb-camp.js` is now an inert stub.
- **Light/dark (DF cream / BNB black) fixed.** The Crafting Requirements block had
  hardcoded near-black text (`#1a120e`) and teal (`#205d58`) that were invisible /
  off-brand on the BNB dark theme. Both now flow from `var(--text)` / `var(--accent)`.
  Everything else in the stylesheet was already variable-driven.
- **Coming-soon cards** added to `best-builds`, `camp-building-guides`,
  `camp-icon-meanings` — the site-canonical card (title, "Coming soon.", divider,
  dead search bar, "This page is coming soon.").

All 19 real nav URLs were verified to resolve to the correct renderer.

### Note: legacy pages render now, but thin
The legacy types (pets, cryos, fridges, allies, pet-furniture, pet-apparel) render
in the unified layout immediately, but their **How to Obtain** falls back to the flat
text line and their **Output / Build Information** are sparse, because their JSON
doesn't carry the structured fields yet. That's Part 2.

## Part 2 — build-script enrichment (DONE in code — you just run the workflow)

`dfbnb-data/src/build_allies_pets_weather_json.py` has been edited. A shared
`simple_obtain_routes()` helper was added (next to the existing
`make_obtain_routes`), and `obtainRoutes` is now emitted by all six legacy
builders: `build_pets`, `build_pet_furniture`, `build_pet_apparel`,
`build_cryos`, `build_fridges`, `build_allies`. Logic:

- Seasonal item → **Scoreboard** route (with the ≤15 "Claim" / ≥16 "Purchase with
  tickets" wording the existing `scoreboard_how()` already applies).
- Gold-vendor item (cryos with a plan book) → **Gold Bullion** route (plan / vendor
  / price block).
- Atom-shop item → **Atom Shop** route.
- Craft-only / quest / event / vendor item → routes left **empty**, so the renderer
  keeps the flat `howToObtain` line instead of nine dimmed N/A rows.

All sources are vendor/unlock/claim, so every route's drop rate is `N/A`.

**No PowerShell, no workflow edits needed.** The build script edit is the whole job.
To ship it:

1. Commit + push the changed `src/build_allies_pets_weather_json.py`.
   That push **auto-triggers** the `Build Allies / Pets / Weather JSON` workflow
   (`.github/workflows/build-allies-pets-weather.yml` watches that file), or run it
   manually via **Actions → Build Allies / Pets / Weather JSON → Run workflow**.
2. The workflow rebuilds and commits the enriched `dist/*.json` (pets, pet-furniture,
   pet-apparel, cryos, fridges, allies …) to `main`.
3. The site reads `dist` straight from GitHub raw, so the richer **How to Obtain**
   appears once the workflow finishes — no sync step.

### Two separate deploys (worth knowing)
- **Renderer (Part 1)** — JS/CSS edits live in the WordPress child theme
  (`dfbnb-child/assets/…`); they reach the site however your theme/site-data is
  deployed, independently of the dfbnb-data repo.
- **Data (Part 2)** — JSON is built + committed by the GitHub workflow above and
  served from GitHub raw.

### Intentionally NOT emitted: `buildInfo` for the legacy types
Build limits / power / flamingo units come from workshop-count GLOBs keyed per
object type. The migrated pages (weather/buff) have known tokens; the legacy
cosmetic/utility items don't have confirmed tokens, so emitting them would mean
guessing values. They were left off rather than shipped wrong — the Technical
section still shows Tradeable + record IDs correctly. Add later if/when the GLOB
tokens are confirmed (reuse `workshop_count_for()` + `_limit_str()`).

## Part 3 — build-limit display + curve-table failsafe (DONE in code)

Two more fixes, both shipped via the same workflow (push the script → it rebuilds
dist) plus the renderer for the display side:

**Build-limit "0" vs "no global" (`_limit_str` in `build_allies_pets_weather_json.py`).**
A bare `0` read as "unlimited" to players when it means the opposite. Now:
- no GLOB (`None`) → **"As many as your CAMP budget allows"**
- GLOB present and `0` → **"0 — Cannot be built"**
- otherwise → the literal number.
Only the weather/repair-bot builder emits build limits today, so that's the only
script affected.

**Curve-table crafting failsafe (FVPA parsers + renderer).**
Investigation: scanning the full COBJ export, **no camp item uses a curve-driven
component count** — every camp recipe is plain integers. 224 components do have a
curve-driven count (exported as `0`), but they're all armour/weapon mods, none on
camp pages. Still, a failsafe was added so a future curve-driven camp material
won't render as a misleading `×0`/`×1`/`×NaN`:
- `_parse_fvpa_qty()` added to all three FVPA parsers (`build_allies_pets_weather_json.py`,
  `build_buff_stations_json.py`, `build_camp_items_json.py`): a `0` / blank /
  non-numeric count now emits `qty: null, scaled: true` instead of a fake number.
- `renderCraftingRequirements()` in `df-bnb-camp-items.js` renders a scaled (or any
  ≤0 / non-numeric) count as **`×(varies)`**, and still shows normal `×N` otherwise.
Belt-and-suspenders: the build flags it AND the renderer guards against a raw 0
leaking through from older JSON. Both buff-stations and camp-items (collectrons /
resource producers) have their own build workflows — re-run whichever you touch.
