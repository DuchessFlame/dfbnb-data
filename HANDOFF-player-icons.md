# Player Icons page — handoff

New page: **`/df/atom-shop/player-icons/`** — 431 icons, A–Z, generative.
Scoreboard player-icon images were also consolidated onto one folder.

---

## 1. Upload the images (do this first)

The 649 converted AVIFs are sitting flat in:

```
C:\Users\Duche\OneDrive\Guides and Stuff\.Atom Shop\Player Icons\
```

They go to:

```
/wp-content/uploads/guide-images/atom-shop/player-icons/
```

Either run `.\sync_player_icons_to_site.ps1` (WinSCP, same pattern as the bundle
sync) or drag the folder contents up in FileZilla.

**Before or instead of that, copy the contents of the OLD folder
`/wp-content/uploads/storefront/player-icons/` into the new one.** Two icons the
scoreboards reference live only there, because their `.dds` was never in the
texture extract:

- `atx_playericon_playericon1.avif` (S16)
- `score_s9_playericon_dreadislandinverse.avif` (S9)

Skip that copy and those two season rewards render as broken images. The sync
script does it automatically (`-NoSeed` opts out); the old folder can be retired
once the copy is done.

**Do not wipe the new folder on later re-uploads.** It holds those two files and
nothing regenerates them.

---

## 2. What was built

| File | What it does |
|---|---|
| `src/build_player_icons_json.py` | Enumerates icons from the newest ENTM export → `dist/player_icons.json` (+ `dist/pts/`). Resolves obtain routes, 31-day NEW pill. |
| `src/apply_player_icon_images.py` | Repoints every `kind=playerIcon` row in `season_rewards.tsv` at the new folder, back-fills blank entitlements. |
| `sync_player_icons_to_site.ps1` | SFTP upload. |
| `.github/workflows/build-player-icons.yml` | Manual one-off rebuild. |
| `dfbnb-patch-build.yml` / `dfbnb-pts-build.yml` | Both builders wired into the patch + PTS channels, with sanity checks. |
| `df-bnb-atom-shop.js` / `.css` | The page renderer (`pi-` classes) — sixth page in the atom-shop module. |
| `df-bnb-guide.js` | `/player-icons` registered in `isAtomShopPath` + `isAtomShopJSPath`. |
| `guide_index.tsv` ×2, `nav.json` | Page registered, menuOrder 26, after Survival Tent Skins. |

**Image naming rule:** the icon's DDS filename, lowercased.
`ETDI "ATX_PlayerIcon_Cappy.dds"` → `atx_playericon_cappy.avif`. Same rule on the
page and on every scoreboard row, so there is one filename convention and no
guessing. A handful of textures ship only as an `_l` variant; the renderer
retries `<name>_l.avif` before falling back to a placeholder.

**Obtain routes** (live counts): Scoreboard 207, Atom Shop 184, Legacy 19,
Twitch Prime 7, Fallout 1st 6, Mini-Season 3, Community 3, Seasonal Event 2.
PTS adds 8 World Pets icons and 6 more scoreboard icons — 445 total.

---

## 3. Known gaps — need you

**Four icons have no texture in the extract.** They render as "Image not
available" until the `.dds` is pulled from the BA2s and converted:

- `atx_playericon_golden76.avif` — Golden 76
- `atx_playericon_holiday_happynewyear.avif` — Happy New Year
- `vaults_playericon_vault96_gold.avif` — Vault 96 Gold
- `atx_playericon_wendigo1.avif` — Wendigo (2019 community promo)

**"Player Icon: Cyprus Cat" (S25, page 9)** is the one scoreboard row left on the
old `season_images` webp. There is no Cyprus Cat *icon* anywhere in the game
files — only the CAMP pet (`SCORE_S25_CAMP_CAMPPets_Cat_CyprusCat`). Either the
curated row is a mislabelled pet reward or the icon was cut. `season_rewards.tsv`
was left untouched rather than guessing.

**Two rows both display as "Vault 94".** That is faithful to the game data —
`Vaults_ENTM_PlayerIcon_Vault96_Standard` genuinely carries `FULL = "Vault 94
Player Icon"` and points at the Vault 94 texture. A Bethesda data bug, not a
build bug. The EDID in the Technical block tells them apart. Say the word if you
want an override map instead.

**226 AVIFs are unused.** They have no ENTM record in any export (`cappy`,
`blueprint`, `bottle`, `chaos`…) — leftover art, never shipped as an
entitlement. They're uploaded anyway, harmless, and mean any icon Bethesda
switches on later already has its image in place.

---

## 4. Worth knowing

**The shifted ENTM rows turned out to be a bug in the export scripts** — see
`HANDOFF-export-quote-splits.md`. Short version: 219 ENTM rows (and a handful in
BOOK/ACTI/COBJ/MISC/NOTE) had been split in half by the xEdit exporter. The
committed exports are now repaired, the 31 `.pas` scripts are fixed, and both
workflows run the repair before any builder. The S10–S12 scoreboard icons were
the visible casualties on this page.

**Limited Time Bundles is missing from `nav.json`.** Pre-existing, unrelated to
this work: `df-atom-shop-bundles` is in `guide_index.tsv` with `showInMenu=1` but
has no node under `df-tc-atom-shop` in `nav.json`. Left alone — flagging in case
it is an oversight rather than deliberate.

Backups written next to each edited file (`*.bak-20260828-*`).
