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
