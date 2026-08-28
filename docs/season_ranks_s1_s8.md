# Seasons 1–8 — the board-game scoreboards

## What was wrong

Seasons 1–8 ran on the old 100-rank board game. There were never any pages.
The page numbers those seasons carried in `season_rewards.tsv` were invented by
`build_pts_season_scoreboard.py`, which buckets rewards by category about eight
to a page — which is why Page 1 of Season 1 was nothing but player icons.

Half of each board was missing as well. Atoms, Caps, Lunchboxes, Perk Card
Packs, Repair Kits, Scrap Kits and the rest are scoreboard rewards that never
appear in ENTM, so a datamined backfill cannot see them. Season 1 had 57 rows
for a board that handed out rewards at 99 ranks.

## What replaced it

`tsv/season_ranks_s1_s8.tsv` is the curated rank-by-rank board list — one row
per reward per rank, including the currency and consumable ranks.

`src/apply_season_ranks.py` folds it into `tsv/season_rewards.tsv`:

- matches each board entry to the existing curated row by name, so the artwork,
  in-game description and storefront entitlement survive;
- creates a row for every reward the datamine never had, tagging the
  currency/consumable ones with a `tallyCategory` and the shared utility art;
- writes the `rank` column and blanks `page`;
- keeps any curated row the board list cannot place, with a blank rank, and
  lists it in `dist/season_ranks_report.txt`.

`build_season_rewards.py` then emits `rank` per item and `layout: "rank"` for
the season, and `df-bnb-seasons.js` renders one flat ordered RANK 1–100 list
with no page headers.

Re-run after editing either TSV:

    python src/apply_season_ranks.py     # --dry-run to preview
    python src/build_season_rewards.py

`apply_season_ranks.py` rebuilds the S1–S8 rows from scratch every run, so run
it against a clean `season_rewards.tsv` rather than over its own output.

## Sources

Primary: the independent Fallout Wiki, `https://fallout.wiki` — one page per
season, raw wikitext via `index.php?title=…&action=raw` or
`api.php?action=parse&…&prop=wikitext`.

| Season | Page |
|---|---|
| 1 | The Legendary Run (season) |
| 2 | Armor Ace and the Power Patrol (season) |
| 3 | The Scribe of Avalon |
| 4 | Cold Steel (season) |
| 5 | Escape from the 42nd Century |
| 6 | The Unstoppables vs The Diabolicals |
| 7 | Zorbo's Revenge (season) |
| 8 | A Better Life Underground (season) |

Cross-checked rank-for-rank against `fallout.fandom.com` through its
`api.php` (direct page fetches are Cloudflare-blocked). Where the two wikis
name the same form ID differently, the fallout.wiki name was used. Bethesda's
own "Inside the Vault" season articles are JS-rendered and returned nothing
usable through the Wayback Machine, so they could not serve as a third check.

## Things worth knowing

- **Rank 1 carries no reward** on any season. It is the starting space, and
  every board table on both wikis begins at rank 2.
- **Fallout 1st bonus rewards started in Season 3.** Seasons 1 and 2 had none.
  The backfill had set `isFirst` on roughly half of Season 1 — Clean Sink,
  Chicken Coop, Planetarium Lamp — all wrong. `apply_season_ranks.py` now takes
  `isFirst` only from the board list and never carries the old value over.
- **Fallout 1st rows are unnumbered on the wikis.** They sit under the rank they
  follow, every fifth rank, and are additions to that rank rather than
  replacements for its base reward.
- **The backfill filed some rows under the wrong season.** Season 8 held Season
  7's "Dr. Zorbo's Magic Pose" and Season 5's Chronotron backpack; Season 3
  held Season 4's checkered vault floors. The second matching pass moves them.
- **Seasons 9–17 were board-game seasons too.** Their pages are still synthetic
  and have the same problem. The per-page screenshots in
  `Season Photos\Season {N}\` are the in-game reward viewer's rank groups
  ("REWARD RANK 1-10, 11-20 …"), not pages, so they can be transcribed the same
  way when someone gets to them.

## Still missing

Around 160 rows have no artwork. Eight shared utility icons would cover about
135 of them — Perk Card Pack, Scrap Kit, Perfect Bubblegum, Vault-Tec Supply
Package, Legendary Core, Liquid Courage, Fireworks and Tadpole Badge — dropped
into `season_images/utility/` and wired into `UTILITY_RULES`. The remainder are
Fallout 1st bonus cosmetics that have no ENTM record to resolve a texture from.

`score_utility_nuclearkeycard.avif` is referenced by existing rows but is not
in the uploaded utility folder — it was already missing before this change.
