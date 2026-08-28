# Scoreboards page rules

Four page types share the Scoreboards category — Scoreboard, Upcoming Rewards,
Season Ticket Calculator, S.C.O.R.E. Progression Calculator. These rules apply
to all of them, and the shared helpers live in `df-bnb-guide.js` so the four
renderers cannot drift apart.

## 1. The page title is the guide title

The H1 in a module's own header card must read exactly what the Guide dropdown
says — "Legacy Season 4: Armor Ace and the Power Patrol in Cold Steel -
Scoreboard", not "Scoreboards - Season 4: …". A page that announces itself as
one thing while the picker calls it another is just confusing, and legacy
seasons disagreed on every single page.

`window.__DFBNB_GUIDE_TITLE(fallback)` returns it. It reads the guide record
published by `setGuideHeader`, falls back to the selected option text, and
finally to whatever fallback the caller passes. The title itself comes from the
`title` column of `tsv/guide_index.tsv`.

## 2. Every Scoreboards page shows the run history

Under the season name:

    Original run: 27 Apr 2021 – 7 Jul 2021
    Re-run 1: 15 Sep 2026 – 15 Dec 2026 (estimated end)

A season is not a one-off any more. Bethesda re-releases old boards, and a
player needs to see whether they have already had a shot at this one and how
many times it has come round.

`window.__DFBNB_SEASON_RUNS(allSeasons, seasonNum)` builds the list from
`all_seasons.json` — the season's own dates for the original run, then every
entry in `legacy[]` for that season, oldest first and numbered. "(estimated
end)" appears when the end date was borrowed from the season running alongside
it rather than announced.

Each renderer paints it: `.sbRuns` on the scoreboard and upcoming pages,
`.dfcalcRuns` on the calculators.

## 3. Upcoming Rewards intro card: the warning line, nothing else

    ⚠ Pulled from the Public Test Server — subject to change

That is the whole intro card. No "PTS · Datamined" pill, no explanatory
paragraph inside the box, no "Every scoreboard reward datamined for Season N …
listed A–Z" blurb. All three restated the same warning before the reader
reached a single reward.

`renderHeader()` in `df-bnb-upcoming-rewards.js`, `.urWarn--titleOnly`.

## 4. Upcoming Rewards images come from the image manifest, not the .dds name

The renderer used to build the image path from the `.dds` basename. That breaks
whenever Bethesda reuses a texture: the Tree Branch Chandelier ships as
`score_s1_camp_lights_treebranchchandelier.dds`, but Season 4's upload is named
`score_s4_camp_lights_treebranchchandelier.avif` after the curated reward row.
The guess 404s and the reward reads "Image not yet uploaded" while the
scoreboard page displays it perfectly well.

`build_upcoming_rewards_json.py` now resolves `imageUrl` per item from
`dist/season_images/season_{N}_images.json`, keyed on entitlement — the only
record of what was actually uploaded. The renderer prefers it and falls back to
the `.dds` guess only for a brand-new PTS season that has not been through the
image pipeline yet.

## 5. The upcoming builder reads the PTS export by default

`build_upcoming_rewards_json.py` defaults to `tsv/pts/ENTM_Export_PTS_*.tsv`,
falling back to the live export. It used to be the other way round, which meant
a plain run rebuilt every upcoming page from shipped data — Season 26 went from
66 rewards to 0, and Season 4's re-release lost the five titles Bethesda added
to it, which are the entire point of the page.

It also builds any season listed in `tsv/legacy_seasons.tsv` even though that
season is curated and finished, because a re-run genuinely is upcoming.

## 6. Legacy additions get a permanent NEW pill

A legacy re-run is not a straight repeat — Bethesda quietly adds rewards, and
nothing announces them.

    python src/apply_legacy_additions.py [--season N] [--dry-run]

compares the PTS export against the curated board, matching on entitlement EDID
(names get reworded between runs; EDIDs do not). Anything in the export with no
curated row is written into `season_rewards.tsv` with `addedInRerun` set, which
emits `isNew` and renders the permanent gold ★ NEW pill. The script is
idempotent — a second run finds nothing.

These rewards have no rank, because they were not on the original board. They
render in their own "Added in the {label} re-release" group, open by default,
above the "Not placed on the board" section.

Season 4's Sep 2026 re-run added five: Framed Cold Steel Wall Art, and the
titles Power Patrol, Icebreaker, Icebreaker's and Yukon Five's.

## Rebuild order

    python src/build_upcoming_rewards_json.py    # PTS export -> upcoming pages
    python src/apply_legacy_additions.py         # diff -> season_rewards.tsv
    python src/build_season_rewards.py           # -> season_tickets_s*.json
