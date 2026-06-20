# DFBNB Data — Workflow Coverage Audit
_Generated 2026-06-20. Goal: make sure every live page/JSON is rebuilt automatically so nothing goes stale._

## How deployment actually works (two models)

Your JSON reaches the site **two different ways**, and this is the root of the confusion:

1. **Served straight from GitHub** — `raw.githubusercontent.com/DuchessFlame/dfbnb-data/main/dist/<file>.json`.
   For these, **committing to `dist/` on `main` = instantly live.** No server upload.
   Examples: pennants, world_pet_challenges, world_pet_types, plan_master, mini_seasons, titles, collectables.

2. **Served from the WordPress server** — `buffsnbrew.com/wp-content/uploads/json/<file>.json`.
   These are **NOT** served from GitHub. They must be uploaded to the server's `wp-content/uploads/json/` tree (your OneDrive `1 site-data/json/` mirror). Building in CI does nothing for these.
   Examples: `challenges.json`, and **`npc_spawns.json`**.

The master build (`dfbnb-patch-build.yml`) runs every 6 hours + on any push to `tsv/ src/ tools/ data/`. It executes ~69 build scripts and commits to `dist/`. That covers **model 1** files only — and only the scripts it explicitly lists.

---

## 1. The blank Spawn Locations page (immediate cause)

`npc_spawns.json` has **no build script, no workflow, and no deploy** anywhere.
- The renderer (`df-bnb-npc-spawns.js`) fetches it from `wp-content/uploads/json/npc-spawns/` (model 2) → returns **404** on the live server.
- The file only exists in OneDrive `1 site-data/json/npc-spawns/npc_spawns.json` (valid: blood-eagle + rust-raider, 13 locations).
- It is **not** in the repo `dist/` at all.

**Fix options:**
- **Quick:** upload `npc_spawns.json` to `wp-content/uploads/json/npc-spawns/` on the server (same way `challenges.json` got there). Page renders immediately.
- **Consistent (recommended):** move npc-spawns onto model 1 like its DF siblings — commit `npc_spawns.json` to repo `dist/`, point the renderer at `raw.githubusercontent.com/.../dist/npc_spawns.json`, and add a small `build-npc-spawns.yml` (or fold it into the master). Then patch-day updates are automatic.

---

## 2. Live pages whose builder is in NO workflow (real stale risk)

These JSONs are fetched by live pages but are rebuilt by **nothing** in CI — they only change when you run the script locally and commit by hand. On a patch, they silently go stale.

| Script | Output JSON | Consumed by | In any workflow? |
|---|---|---|---|
| `build_pennants_json.py` | `dist/pennants.json` | `df-bnb-plan-checklists.js` (Pennants) | ❌ none |
| `build_world_pet_challenges_json.py` | `dist/world_pet_challenges.json` | `df-bnb-world-pets.js` | ❌ none |
| `build_weak_spot_multipliers_json.py` | `dist/calculators/weak_spot_multipliers.json` | `df-bnb-bnb-calculators.js` | ❌ none |
| `build_drop_chances_data.py` | `dist/drop_chances_data.json` | `df-bnb-drop-chances.js` | ❌ none |
| `build_legendary_mod_drop_chances_json.py` | legendary mod drop data | `df-bnb-legendary-mod-calc.js` | ❌ none |
| `build_staff_seed_json.py` | `dist/seed-staff.json` | `df-bnb-staff-portal.js` | ❌ none (likely one-off seed, lower risk) |

**Recommendation:** add each of these to the master `dfbnb-patch-build.yml` (one `run:` line each), or give each its own small workflow. Pennants and world_pet_challenges are the highest priority — they're active game-data pages you've edited within the last week.

---

## 3. Dead / superseded scripts (safe — no action needed)

These page-named workflows exist but run the **consolidated** builders, so the old per-page scripts are obsolete (not a stale risk):

- `build-lifetime-challenges.yml`, `build-quests.yml`, `build-random-encounters.yml`, `build-pioneer-scouts.yml` → all run `build_challenges_json_v3.py`.
- `build-raids.yml`, `build-public-events.yml` → run `build_events_rewards_json.py` / `build_reho_json.py`.
- `build-activities.yml` → runs `build_activities_rewards_json.py`.

Correspondingly, these legacy scripts are no longer used and can be archived/deleted to reduce noise:
`build_lifetime_challenges_json.py`, `build_quests_json.py`, `build_raids_json.py`, `build_public_events_json.py`, `build_random_encounters_json.py`, `build_pioneer_scouts_json.py`, `build_activities_json.py`, `build_expos_json.py`, `build_mule_master_items_json.py` (output `mule-defaults.json` not referenced by any renderer).

Pure helper libraries (correctly have no workflow): `patchlog_utils.py`, `rng76.py`, `diagnostics.py`, `cut_content.py`, `append_patchlog_history.py`, `build_all.py`.

---

## Quick action list

1. **npc_spawns** — upload to server now (unblocks the blank page), then decide whether to migrate it to the raw.github model.
2. **Add to master build:** `build_pennants_json.py`, `build_world_pet_challenges_json.py`, `build_weak_spot_multipliers_json.py`, `build_drop_chances_data.py`, `build_legendary_mod_drop_chances_json.py`.
3. **Optional cleanup:** delete the superseded legacy scripts in §3.
