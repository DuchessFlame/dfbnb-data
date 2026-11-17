# Score Challenges - Challenge Keyword Finder

17 Nov 2026. Page: `/df/score-challenges/challenge-keyword-finder/`

Pick **Daily** or **Weekly**, then a challenge (A–Z, Epic versions included, required count in brackets, e.g. `Kill a Canine (5)`). The page lists what the game counts toward it in plain in-game names, A–Z, de-duplicated, then an **Also needs** part for situational conditions (in a Shelter, on a team, Well Fed…), each linked to a guide when a public one covers it. Under the list: one horizontal line, then **Guides**. If a challenge has no public guides, the line and the heading are both hidden.

> Note: the brief pointed at `claude/level-terminal-locations.md`. That file wasn't in the repo, so this doc stands on its own. Conventions came from the Costume Keyword Index code, `challenge_required.py`, `build_challenges_json_v3.py` and `guide_index.tsv`.

## Files

| File | What it is |
|---|---|
| `src/build_challenge_keywords_json.py` | **New.** Build script. |
| `dist/challenges/challenge_keywords.json` | **New.** Live data (933 challenges: 505 Daily, 428 Weekly). |
| `dist/pts/challenges/challenge_keywords.json` | **New.** PTS copy (`--pts`). |
| `tsv/challenge_counter_targets.tsv` | **New, hand-kept, exe-checked.** Maps a stat counter to actor keywords (see below). |
| `.github/workflows/build-challenge-keywords.yml` | **New.** Standalone build: live, then `--pts`, then commits both JSONs. Same pattern as `build-costume-keywords.yml`. |
| `.github/workflows/dfbnb-patch-build.yml` / `dfbnb-pts-build.yml` | **Edited.** New step "Build Challenge Keyword Finder JSON" (`--pts` on the PTS build), straight after the Pre-War Food index, so it runs after Build Challenges JSON. In the PTS run, `dist/` is wiped first, so `--pts` reads the fresh PTS `challenges.json` and the relocate step merges the output into `dist/pts/challenges/`. |
| `tsv/challenge_name_overrides.tsv` | **New, hand-made.** Renames or drops a game name. |
| `tsv/challenge_guide_map.tsv` | **New, hand-made.** Adds or removes guide links per challenge. |
| `tsv/challenge_situations.tsv` | **New, hand-made.** Situational condition → plain "Also needs" line + optional guide (see below). |
| `audits/challenge_keywords_unresolved.tsv` (+ `_pts`) | **New, rewritten every build.** Conditions left off the page, plus challenges with no guide and challenges with no names. |
| `tsv/guide_index.tsv` | **Edited.** New row `df-score-challenge-keyword-finder` under `df-tc-score-challenges`, menu/browse order 80, straight after the terminal rows (70–73). |
| `dfbnb-child/assets/df-bnb-challenge-keywords.js` | **New.** Renderer. |
| `dfbnb-child/assets/df-bnb-challenge-keywords.css` | **New.** Page extras (`.ckf-`). The top half reuses `.cki-` from `df-bnb-costume-keywords.css`. |
| `dfbnb-child/functions.php` | **Edited.** Enqueues `df-bnb-costume-keywords.css`, `df-bnb-challenge-keywords.css` and `.js`, only when the path matches `/score-challenges/challenge-keyword-finder/`. |
| `dfbnb-child/df-bnb-guide.js` | **Edited.** Same wiring as the Costume / Pre-War Food indexes: `isChallengeKeywordsPath()`, CSS loader, `maybeMountChallengeKeywords()` at the 3 mount points, and the HTML-injection guard. Without this, SPA navigation into the page (clicking it from the hub) would show the empty WP body. |

Backups (before edits): `_claude_backups/2026-11-17/`, including `dfbnb-patch-build.yml` and `dfbnb-pts-build.yml`. `guide_index.tsv` is backed up under its own name. Earlier sessions today had already backed up `functions.php` and `df-bnb-guide.js`, so mine are saved as `functions.before-challenge-keyword-finder.php` and `df-bnb-guide.before-challenge-keyword-finder.js`, and the earlier backups were left untouched. Later rounds saved `build_challenge_keywords_json.py` (before the cut cross-check) plus `*.before-situations.*` copies of the build script, counter TSV, renderer JS/CSS and this doc.

## How the names are found

Source list: `dist/challenges/challenges.json` (pages `daily` / `weekly`). This gives the name, the required count, the Epic and cut flags, the raw CHAL conditions and the site's existing guide matches. Two records that share a label (name + count) are merged into one row.

### Cut content (cross-checked 17 Nov 2026)

The first build only used `challenges.json`'s `is_cut` flag (which comes from `plan_sources.is_cut_content`) for the challenge list, and the spin-wheel prefix rule (`spin.is_cut()`, zzz / CUT / DEL / test) for the counted names. It did **not** run the site-wide cut test on the names, so cut things were showing. It now runs every record (challenges, items, weapons, apparel, races, NPCs, quests, form-list members) through `cut_reason()`:

1. `plan_sources.is_cut_content()`: **the** site-wide test (Milepost Zero, The Drifter, Vault 94 / 96, Nuclear Winter / Babylon, The Messenger… plus zzz / CUT / DEL / test).
2. `cut_content.is_cut()`: the item-level list (Nukashine 2, SFS01 brews, Firecracker Whiskey, spoiled food…). This applies to **items only**. Two of its broad patterns hit live records here, so they're kept back with `CUT_CONTENT_LIVE` in the build:
   - `Firecracker` (meant for Firecracker Whiskey) also hit the Firecracker Berry plant and the four live "Harvest a Firecracker Berry" challenges.
   - `^E07A_Mothman` (meant for the Cultist High Priest Pack) also hit the Mothman Equinox event and the Perfect Mothman Egg.
3. `spin.is_cut()` (the prefix rule).
4. `dist/cut_content.json` (the Cut Content page), matched by FormID. This only uses the Challenges, Quests and Collectables groups, because their `id` is the record itself. The Unique Weapons / Armour rows carry the *base* weapon's FormID (Crimson Sky = `0010F0EC` = the Tesla Rifle), so matching those would wrongly cut the Tesla Rifle, Assaultron Blade and so on.

**Results against the previous build:**
- **Cut challenges dropped from the dropdowns: 0.** The Daily / Weekly pages in `challenges.json` already exclude every cut challenge. The 2,778 cut challenges live on the Cut Content page, and none of the Daily / Weekly FormIDs appear in it.
- **Cut names removed from the lists: 89 live, 85 PTS**, across 609 live challenge rows. No challenge was left empty. Examples:
  - **The Drifter:** Splinter, Tempest, Chaos Engine, Crackler Grenade.
  - **Vault 94:** the Vault 94 holotapes, Antonio's Holotape, the Glowing One - Billy / Cole / Lucas / Red NPCs, and the V94 Solar / Thorn / Strangler Heart armour records.
  - **Vault 96:** Vault 96 Suppression grenades, mines and syringe; the Corrupted Sentry Bot.
  - **Milepost Zero:** Caravan: Skyline Drive, the Deathclaw / Gulper Skin outfits, Lederhosen and Redcoat outfits, the Gulper Plushie.
  - **Nuclear Winter:** Ghillie Suit, Vault 51 Jumpsuit, Centurion Helmet, Survivor's Denim, Extra Crispy Scorchbeast.
  - **Event: The Messenger.**
  - **cut_content list:** Firecracker Whiskey (×5 variants), Nukashine 2 Electric Boogaloo, Muttberry / Sunday / Tater Shine, Calmex Silk, Formula B, Spoiled Fruit / Meat / Vegetables, Glowing Fish Bits and its dishes, Raw Yellowcake Flux.
- Every removed name, with the source that removed it, is listed under `CUT_NAME` in the audit TSV.
- **Worth a look:** Strangler Heart, Thorn and Solar armour pieces were removed because these particular records are the `_V94` Vault 94 copies, which is the site rule. If a live copy of the same name carried the keyword, it would still show.

Each condition is expanded through condition forms (CNDF) down to its leaves, then turned into names:

| Condition | Becomes | Group shown |
|---|---|---|
| `GetIsID` / `GetIsForm` | that record's in-game name | What counts (a weapon on the player → Weapons that count) |
| `HasKeyword` on the target, actor keyword | races and NPCs carrying the keyword (from KYWD refs) | What counts |
| `HasKeyword` on the target, item keyword | every item carrying it (plants first for "Plants Harvested") | What counts |
| `GetIsRace` | race name, unless an actor keyword narrows it (keyword + race = that keyword's actors of that race) | What counts |
| `GetInFaction` / `GetIsClass` | faction or class name ("Fanatics") | What counts |
| `WornHasKeyword`, `WornInOrOutOfPowerArmorHasKeyword`, `CHAL_DoesTargetWeaponHaveKeyword` | weapons carrying the keyword, otherwise apparel | Weapons that count / What to wear |
| `CHAL_IsTargetWorkshopRecipeInCategory` / `CHAL_IsTargetWorkshopRecipe` | build-menu category / what the recipe builds | What counts |
| `IsInList` (form list) | list members, nested lists followed | What counts (on the player → When it counts) |
| `PlayerHasQuest` | quest / event name | When it counts |
| `GetInCurrentLocation`, `GetInWorldspace` | place name | Where it counts |

"Not X" conditions (`== 0`) narrow a challenge rather than adding to it, so they're left off without a note. Name hygiene drops the following: cut, ZZZ, test, DEL, unused and NPC-copy records; CAMP pets, Collectrons, corpses, templates and audio templates; names that are really EDIDs; and names with `<Alias=…>` or `[…]` in them. A trailing `: <Alias=…>` is stripped instead ("Uplink: <Alias=Location>" → "Uplink"), so the Daily Ops modes, Uplink and Decryption, now list properly.

**Counter-only challenges.** Some challenges have no conditions at all. The game counts them by stat (CHAL SNAM, e.g. "Deathclaws Killed"). For those, `challenge_counter_targets.tsv` maps the counter to an actor keyword. The mappings are checked against **Fallout76.exe** (live 23 Oct and PTS 14 Nov). The exe holds the kill-stat list (`sStatDeathclawsKilled`…) right beside a matching keyword list (`ActorTypeDeathclaw_DO`…). 18 of the 19 rows are exact stat ↔ keyword pairs from that list, and each row's `note` says which pair proves it.

**Canines Killed: fixed, now `ActorTypeFeralDog`** (it was `ActorTypeDog`, a guess). Two proofs from the game files, live and PTS alike:
1. The game's own live lifetime challenge **"Kill a Canine"** (`Challenge_Lifetime_Kill_Animals_SUB_Canine`, CHAL `0043AD21`) tests `HasKeyword ActorTypeFeralDog [KYWD:0002CB97]` on the target. It's the only non-cut challenge in either CHAL export that defines a canine by condition. The Daily / Weekly "Kill a Canine" have no conditions; they use the counter.
2. The engine's dog default object, `KeywordActorTypeDog_DO` (DFOB `0007CB51`; exe text "This identifies an actor that is a dog."), points at **ActorTypeFeralDog**, not ActorTypeDog (KYWD refs export).

ActorTypeFeralDog is on these races: Fox, Wild Mongrel, FEV Hound (shown as Mutant Hound), Wolf, Attack Dog and **Blue Devil**. So the list is now Attack Dog, Blue Devil, Fox, Mutant Hound, Wild Mongrel, Wolf. CAMP pet dogs and Dogmeat aren't included (ActorTypeDog had pulled in the Blood Eagle / Scorched dog NPCs). The stat itself is counted on Bethesda's server, so the exact counter can't be read. These two sources are the game's own definition of a canine.

**Game files.** The live and PTS game folders (`D:\steamlibrary\steamapps\common\Fallout76` and `…\Fallout 76 Playtest`) were connected and searched for the counter proof above. The ESM data comes in through the patch exports (`tsv/` live October 2026; `tsv/pts/` PTS 2026-11-14, which matches the 14 Nov PTS ESM). That way CI can rebuild without the game installed.

If a challenge has nothing narrower than its name (Gain XP, Collect Caps, Kill a Creature…), the page says: *"The game doesn't narrow this one down — anything that fits the challenge name counts."* (71 challenges live; most of the rest now show Also needs lines instead.)

## Guides and the visibility rule

The site already marks guides as public or hidden in `guide_index.tsv`: `visibility` must be `public` **and** `status` must be `published`. This is the same rule `build_challenges_json_v3.py` uses (line ~339) and `functions.php` uses for meta descriptions (~6558). The build only writes guides that pass it. On the page, the renderer also checks the WordPress visibility maps (`dfbnbData.page_visibility`, `subcategory_visibility`, `category_visibility`, the same ones `df-bnb-category.js` reads). Any tier other than `everyone` hides the link. Right now every row in `guide_index.tsv` is public and published, so nothing is hidden yet.

A challenge's guides are worked out in this order:
1. `challenge_guide_map.tsv` (`add` rows). Seeded with: NPC spawn pages (Cultist, Blood Eagle, Scorched, Trog…); terminal pages for "Hack a Terminal" (all four) and "Skill Level N Terminal" (that level).
2. Keyword rule: a costume keyword → Costume Keyword Index; `MealTypePackaged`/`MealTypeDogfood` → Pre-War Food Keyword Index; any `WeaponType*` → Weapon Keyword Index.
3. The existing word-match guides from `challenges.json` (`find_related_guides`).
4. `remove` rows in the map then take links away. Seeded with one: the ghoul *build* page no longer shows on "Cripple a Ghoul's Leg".

270 of 933 challenges have a guide. The 663 without one are listed under `NO_GUIDE` in `audits/challenge_keywords_unresolved.tsv`.

## Also needs (situational conditions)

These conditions don't name a thing; they say when or where the challenge counts. `tsv/challenge_situations.tsv` turns each one into a plain line under **Also needs**:

- **Places:** Inside a Shelter / Not inside a Shelter (your C.A.M.P. or a Workshop); Inside your own / another player's Shelter.
- **Building:** Built using the Free Cam build mode.
- **Teams and C.A.M.P.s:** While on a team / on a public team; At a teammate's / your own / a Best Build C.A.M.P.
- **Your state:** While Well Fed; While intoxicated by Hard Lemonade / Lead Champagne / Mire Magic Moonshine / Nuka-Cola Dark; While under the effects of Psycho; While you have at least one Mutation.
- **The target or event:** The enemy must be Legendary / a 3-star Legendary; The public event must be a Mutated Public Event; Any Daily challenge counts.

Rows match on the condition function, plus an optional parameter regex, compare operator and value. The first match wins.

**Guide links on these lines** come from the same TSV's `guide` column and only render if that guide is public + published. The build only resolves public rows. The page also applies the WordPress visibility maps; when a guide is hidden, the text stays and the link goes. Seeded links:
- Well Fed → Hunger and Thirst Bar Explained
- the four intoxicated lines → Alcohol Buffs Guide (Nuka-Cola Dark → Nuka Cola Product Buffs Guide)
- Psycho → Chem Buffs Guide
- Mutation → Mutation Buffs Guide
- Mutated Public Event → Mutated Events Guide
- Best Build C.A.M.P. → Best Builds
- Daily challenge → Daily Challenge Checklist

There's no Shelters, Free Cam or teams guide in `guide_index.tsv` yet, so those lines are plain text. Add the guide id to the TSV row when one goes live.

## Conditions still left off the page (no plain name)

These are listed in full, with the challenges they affect, in `audits/challenge_keywords_unresolved.tsv`:
- **Keywords nothing in the export carries**: FestiveScorchedKeyword, Burn_ActorTypeDeathclawArmored, ExplodableCar, InfusionKeyword, ObjectTypeFlux / PlantTypeFlux, ObjectTypeTrap, ScrapTypeWood, ArmorTypeHelmet, AmmoTypeExplosive, WeaponTypePumpkinGrenade.
- **Records with no name**: the Communist ghoul NPCs (`LvlPRCGhoul*`) on "Epic - Kill a Communist", and the Heavy machinegun turret recipe (its built object has no name).

## Testing (17 Nov 2026)

- `python src/build_challenge_keywords_json.py` and `--pts`: both build clean (live 505 Daily / 428 Weekly). All three workflow YAMLs parse. A scan of the live JSON found no names with `_`, `[`, FormIDs or lower-case EDIDs.
- jsdom harness (scratch, not saved in the repo), against the live JSON: **28/28 pass** (re-run after the cut / Also needs / Canines changes).
  - Both dropdowns fill (505 Daily / 428 Weekly, A–Z, Epic included).
  - Switching to Weekly resets dropdown 2 and clears the result.
  - Kill a Canine (5) → Attack Dog, Blue Devil, Fox, Mutant Hound, Wild Mongrel, Wolf (ActorTypeFeralDog).
  - Also needs:
    - "While Well Fed" links to the public hunger guide.
    - Shelter + Free Cam show as plain text.
    - A hidden guide keeps the text and drops the link.
    - Still at most one line on the page.
  - Cut content: Tempest isn't on "Craft or Scrap a Melee Weapon", and the Vault 94 holotapes aren't on "Collect an audio Holotape".
  - Collect a Teddy Bear (3) → 14 bears, A–Z.
  - Epic - Kill a Deathclaw with a Pistol (1) → Deathclaw, plus 27 pistols, plus 2 guides.
  - Exactly one `<hr>` under the list (2 on the whole page: the header divider and this one), with Guides straight under it.
  - No EDIDs, keywords or FormIDs in the result.
  - A challenge with no guides shows no line and no heading.
  - A guide hidden by `page_visibility` or `category_visibility` doesn't render; when all of a challenge's guides are hidden, the line and heading go too.
  - The page re-mounts if the shell wipes the body.
  - Links use the brand accent.
- Build-level checks: with a temp `guide_index.tsv` where one guide is `visibility=hidden` and another `status=draft`, neither is written; a `draft` hunger guide is never linked from an Also needs line.
- `node --check` passes on `df-bnb-guide.js` and `df-bnb-challenge-keywords.js`. `functions.php` couldn't be linted here (no PHP in the sandbox); the new block copies the existing terminal / costume enqueue pattern.

## Open questions

1. **Counter map**: all 19 rows are now backed by the game files: 18 by the exe's stat/keyword pairs, and Canines by the "Kill a Canine" lifetime challenge plus the dog default object. Canines now includes **Blue Devil**, because its race carries ActorTypeFeralDog. Say if it should be renamed or dropped with `challenge_name_overrides.tsv`.
2. Race-level names: "Wild Mongrel" and "Attack Dog" are race names. "FEV Hound" is renamed to Mutant Hound in `challenge_name_overrides.tsv`. Add more renames there if any race name reads oddly.
3. Also needs wording is in `challenge_situations.tsv`. Reword freely, and add a Shelters / teams guide there once one exists.
4. 663 challenges have no guide. Add rows to `challenge_guide_map.tsv` as guides go live.

## Make it live

1. **WordPress**: create the page with slug `challenge-keyword-finder` under the Score Challenges parent, so the URL is `/df/score-challenges/challenge-keyword-finder/`. Use the guide shell template and leave the body empty.
2. **Commit** in GitHub Desktop (dfbnb-data):
   - `src/build_challenge_keywords_json.py`
   - `dist/challenges/challenge_keywords.json`
   - `dist/pts/challenges/challenge_keywords.json`
   - `tsv/challenge_counter_targets.tsv`, `tsv/challenge_name_overrides.tsv`, `tsv/challenge_guide_map.tsv`, `tsv/challenge_situations.tsv`
   - `.github/workflows/build-challenge-keywords.yml`, `.github/workflows/dfbnb-patch-build.yml`, `.github/workflows/dfbnb-pts-build.yml`
   - `tsv/guide_index.tsv`
   - `audits/challenge_keywords_unresolved*.tsv`
   - `claude/challenge-keyword-finder.md`
3. **FileZilla** upload to the child theme:
   - `assets/df-bnb-challenge-keywords.js`
   - `assets/df-bnb-challenge-keywords.css`
   - `df-bnb-guide.js`
   - `functions.php`
   - also upload `guide_index.tsv` to `/wp-content/uploads/` and regenerate `nav.json` as usual, so the page shows in the Score Challenges menu.
4. **Purge Cloudflare** (the page URL plus the three theme files, or purge everything).
5. **Rebuilds** happen automatically: the patch and PTS builds run it after Challenges, and the standalone workflow runs on `tsv/**` or script pushes. By hand, run `build_challenges_json_v3.py` first, then `build_challenge_keywords_json.py` and `--pts`.

— Claude
