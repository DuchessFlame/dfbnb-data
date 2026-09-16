# workflows-to-place

GitHub will not let the remote file tools write into `.github/workflows/`, so
any new or edited workflow lands here and has to be moved in by hand.

| File | Move to | Why |
|---|---|---|
| `build-plan-checklist-datasets.yml` | `.github/workflows/build-plan-checklist-datasets.yml` | Builds `camera_mods.json` and `scoreboard_art.json` for both channels. `build_camera_mods_json.py` has never had a workflow step; `build_scoreboard_art_json.py` is new. |

Nothing else needs a new step. `plan_subpages`, `plan_consumables`,
`add_weapon_groups` and `add_armour_groups` all run **inside**
`build_plan_obtain_json.py`, so `build-plan-obtain.yml` and the patch/PTS
builds pick them up with no change.

To bring an already-built `plan_master.json` up to date without the ~80 minute
rebuild — which is what to run after editing any enricher:

    python3 src/reenrich_plan_master.py --all

It runs all five post-passes in the one load-bearing order against all three
copies (`src/plan-system/`, `dist/`, `dist/pts/`), picks each channel's own
export root, and verifies at the end that the copies agree and that every live
plan lands on exactly one page.

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
