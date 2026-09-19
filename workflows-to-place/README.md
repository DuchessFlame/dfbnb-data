# workflows-to-place

GitHub will not let the remote file tools write into `.github/workflows/`, so
any new or edited workflow lands here and has to be moved in by hand.

| File | Move to | Why |
|---|---|---|
| `build-plan-checklist-datasets.yml` | `.github/workflows/build-plan-checklist-datasets.yml` | Builds `camera_mods.json` and `scoreboard_art.json` for both channels. `build_camera_mods_json.py` has never had a workflow step; `build_scoreboard_art_json.py` is new. |
| `build-plan-obtain.yml` | `.github/workflows/build-plan-obtain.yml` | Plan-master consolidation, 20 Sept 2026 — see below. |
| `dfbnb-patch-build.yml` | `.github/workflows/dfbnb-patch-build.yml` | Same. |
| `dfbnb-pts-build.yml` | `.github/workflows/dfbnb-pts-build.yml` | Same. |

Nothing else needs a new step. `plan_subpages`, `plan_consumables`,
`add_weapon_groups` and `add_armour_groups` all run **inside**
`build_plan_obtain_json.py`, so `build-plan-obtain.yml` and the patch/PTS
builds pick them up with no change.

To bring an already-built `plan_master.json` up to date without the ~80 minute
rebuild — which is what to run after editing any enricher:

    python3 src/reenrich_plan_master.py --all

It runs every post-pass in the one load-bearing order against both published
copies (`dist/` and `dist/pts/`), picks each channel's own export root, and
verifies at the end that the copies agree and that every live plan lands on
exactly one page.

## build-new-plans.yml (updated 16 Sept 2026)

Replaces the copy in `.github/workflows/`. Two changes:

- A new **Attach change notes** step runs `src/plan_changes.py` over
  `dist/plan_master.json` before the page is built, so a plan that was already
  here and has since become tradeable or picked up a source joins the New Plans
  type groups with a ↻ Changed pill.
- The commit step now also stages `dist/plan_master.json`, because that is
  where the `changes` the other checklist pages read are written.

It deliberately does NOT pass `--write`. `data/plan_snapshot.json` is the
baseline the next diff compares against; re-taking it every CI run would walk
it forward one build at a time until it always matched and nothing ever looked
changed. Re-take it by hand once a patch has settled:

    python3 src/reenrich_plan_master.py --channel live --snapshot
    python3 src/reenrich_plan_master.py --channel pts  --snapshot

## Also new, no workflow change needed

`src/add_cobj_link.py` re-resolves `cobj` / `cnam` from the newest COBJ export
as a pure join, and `reenrich_plan_master.py` runs it as **pass 0** — before the
art pass, which names its candidate files after those records. Any workflow that
already calls `reenrich` picks it up with no edit.

Before it existed, a newer COBJ export could only reach `plan_master` through
the ~80 minute rebuild, which is why 69 plans sat for weeks with no recipe, no
created record and no art.


## Plan-master consolidation (20 Sept 2026) — 3 files to move, 2 to DELETE

There were three `plan_master.json` copies and two builders. There are now two
copies — one per published channel — and one builder.

`src/plan-system/build-plan-master.mjs` was 24 lines of node that copied
`src/plan-system/plan_master.json` over `dist/plan_master.json`. Its stated job
was to guarantee `dist/plan_master.json` existed before the Python builder ran.
`dist/` is committed and the site fetches it from `main`, so the checkout
already guaranteed that — and `build_plan_obtain_json.py` only ever **writes**
plan_master, it never reads one, so there was nothing for a seed to provide.

What the copy did instead was create a second roster that could win by running
last. That is why every Python build ended with
`cp dist/plan_master.json src/plan-system/plan_master.json`, and why the PTS
channel once published the **live** roster to `dist/pts/plan_master.json` for
weeks. Both halves are gone.

### Move these three in

| File | Change |
|---|---|
| `build-plan-obtain.yml` | drops the `cp` back to the staging copy, and the staging path from `git add` |
| `dfbnb-patch-build.yml` | drops the node seed step, the `cp` back, and `git add src/plan-system/plan_master.json` |
| `dfbnb-pts-build.yml` | drops the node seed step |

### Delete these two

    .github/workflows/build-plan-master.yml
    .github/workflows/build-plan-checklists.yml

Both exist only to run the node copy. `build-plan-checklists.yml` is the one
that matters: it fires on **push** to `src/plan-system/**`, so while it exists
it reacts to the very commit that deletes that folder, and then fails because
its source JSON is gone.

### Do it in ONE commit

Delete the two workflows, move the three edited ones in, and delete
`src/plan-system/` in the same commit. A push-triggered workflow runs the
version of itself that is in the pushed commit, so if the deletion travels with
everything else, it never fires.

Also deleted, as dead weight (~36 MB of stale rosters nothing read):
`src/plan-system/plan_master-Sally.json`, `-Sally-2`, `-WorkHorse`, `-pts`,
and `dist/plan_master-Sally.json`, `dist/plan_master-WorkHorse.json`,
`dist/new_plans-Sally.json`, `dist/new_plans-WorkHorse.json`.


## Everything now hangs off one script (20 Sept 2026)

`reenrich_plan_master.py` said it re-ran "every plan_master post-pass, in
order". It did not — it skipped the **recipe-only rows** (the challenge- and
workshop-taught recipes and the 863 scrap-to-learn mods, none of which have a
plan book), so a reenrich had to be chased by hand with `add_recipe_unlocks.py`
or the roster came back several hundred rows short. Both passes are pure joins
that replace their own previous output, so they are now steps 0a of the script
and running it twice changes nothing.

It also now rebuilds **`make_plan_checklist.json`** per channel, and so does
`build_plan_obtain_json.py` at the end of a full build. That file feeds
`/df/plan-checklists/make-your-own/`, every value copied verbatim out of
plan_master, and **nothing rebuilt it** — it had no workflow at all. On 20 Sept
it was still serving 901 plans as "Unknown" tradeable that the roster had long
since resolved. It cannot drift now without plan_master drifting with it.

So the one command after editing any enricher is still:

    python3 src/reenrich_plan_master.py --all

Deleted as dead: `src/build_plan_checklists_json.py` — a wrapper that shelled
out to `tools/build-plan-master.mjs`, a path that does not exist. Its only
referrer was `build-plan-checklists.yml`, which is being deleted too.
