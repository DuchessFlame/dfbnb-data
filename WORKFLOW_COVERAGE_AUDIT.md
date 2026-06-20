# DFBNB Data — Workflow Coverage Audit
_Audited 2026-06-20. Resolved 2026-06-20. Goal: every live page/JSON rebuilds automatically so nothing goes stale._

## Status: RESOLVED

All gaps below have been fixed. Everything the live site reads is now generated into the repo `dist/` and served from `raw.githubusercontent.com`. Nothing the site needs is served from `wp-content` anymore.

What changed:
- **5 orphan builders added to `dfbnb-patch-build.yml`** (rebuild every 6h): pennants, world_pet_challenges, weak_spot_multipliers, drop_chances_data, legendary_mod_drop_chances — each with a verify line.
- **`challenges.json` + `npc_spawns.json` moved off `wp-content` onto GitHub-served.** `functions.php` now points `challenges_url` and `spawns_url` at raw.github `dist/`; the wp-content fallback URLs were removed from the four renderers.
- **npc-spawns is now generative into the repo** with a cache pattern (see below) + its own step in the patch build + a standalone `build-npc-spawns.yml`.

---

## How deployment works (single model now)

JSON is **served straight from GitHub** — `raw.githubusercontent.com/DuchessFlame/dfbnb-data/main/dist/<file>.json`. Committing to `dist/` on `main` = instantly live. No server upload for data.

The only server-side deploy left is the **child theme** itself (`functions.php`, the `assets/*.js` renderers) — that still syncs from your OneDrive `1 site-data/json/dfbnb-child/` tree to `wp-content` the usual way. Data does not.

The master build (`dfbnb-patch-build.yml`) runs every 6 hours + on any push to `tsv/ src/ tools/ data/`, executes the build scripts, and commits to `dist/`.

> ⚠️ The old `wp-content/uploads/json/challenges/challenges.json` used a different schema (`challenges` key) than the renderers expect (`pages` key). That stale file is why the challenge + spawn pages were blank. It can be deleted from the server — nothing reads it now.

---

## The npc-spawns cache pattern (why the DB step is local-only)

`tools/build_npc_spawns.py` needs three inputs:

| Input | Source | Lives where | Changes when |
|---|---|---|---|
| Spawn counts | Mappalachia dig `.txt` | `data/npc_spawns/digs/` (repo) | you re-dig locally |
| "Used For" challenges | CHAL export | `tsv/CHAL_Export_*.tsv` (repo) | each patch |
| Region + companions | **Mappalachia SQLite DB** | `D:\Mappalachia\data\mappalachia.db` (**local only, ~480 MB**) | you update Mappalachia |

The DB can't go in GitHub Actions (too big, local-only). So the script is **DB-optional**:

- **Local run (DB present):** computes regions + companions fresh **and** writes `data/npc_spawns/geo_cache.json`.
- **CI run (no DB):** reads `geo_cache.json` for regions + companions, and rebuilds counts + "Used For" from the committed inputs. Output is byte-identical to a DB run given the same cache.

**Practical rule:** when you update Mappalachia or re-run a dig, run `python tools/build_npc_spawns.py` locally once and commit the refreshed `geo_cache.json` + `dist/npc_spawns.json`. After that, the 6-hourly patch build keeps the dig counts and challenge associations current on its own. Adding a new creature = drop its dig `.txt` in `data/npc_spawns/digs/`, add an entry to `NPCS` in the script, run locally, commit.

> Note: the master's commit step only auto-stages `dist/`. `geo_cache.json` lives in `data/`, so it must be included in **your** commit when it changes (CI never writes it — it only reads it).

---

## Dead / superseded scripts (safe — no action needed)

Page-named workflows that actually run the **consolidated** builders, so the old per-page scripts are obsolete:
- `build-lifetime-challenges.yml`, `build-quests.yml`, `build-random-encounters.yml`, `build-pioneer-scouts.yml` → `build_challenges_json_v3.py`
- `build-raids.yml`, `build-public-events.yml` → `build_events_rewards_json.py` / `build_reho_json.py`
- `build-activities.yml` → `build_activities_rewards_json.py`

Legacy scripts no longer used (safe to archive/delete): `build_lifetime_challenges_json.py`, `build_quests_json.py`, `build_raids_json.py`, `build_public_events_json.py`, `build_random_encounters_json.py`, `build_pioneer_scouts_json.py`, `build_activities_json.py`, `build_expos_json.py`, `build_mule_master_items_json.py` (its `mule-defaults.json` isn't referenced by any renderer).

Pure helper libraries (correctly have no workflow): `patchlog_utils.py`, `rng76.py`, `diagnostics.py`, `cut_content.py`, `append_patchlog_history.py`, `build_all.py`.

---

## To deploy these changes

1. **Commit + push** the repo — include `dist/npc_spawns.json`, `data/npc_spawns/geo_cache.json`, `tools/build_npc_spawns.py`, the two workflow files. raw.github then serves the spawn data.
2. **Sync the child theme** (`functions.php` + the 4 `assets/*.js` renderers) to the server the usual way.
3. Optional: delete the stale `wp-content/uploads/json/challenges/challenges.json` and `.../npc-spawns/` from the server — nothing reads them now.
