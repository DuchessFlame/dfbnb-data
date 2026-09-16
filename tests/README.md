# tests — jsdom harnesses for the plan checklists

The renderer is one shared module, so the plan-checklist pages break each
other. These two harnesses boot `df-bnb-plan-checklists.js` in jsdom over the
REAL `dist/*.json` and assert what every page has to show. They replace the
throwaway harnesses that were rebuilt from scratch three sessions running.

    npm install jsdom
    node tests/plan-checklists.test.js live
    node tests/plan-checklists.test.js pts
    node tests/plan-checklists-behaviour.test.js

Both read from a `UP` / `DIST` path at the top of the file. Point those at this
repo's `dist/` and at the folder holding `df-bnb-plan-checklists.js` before
running — they were written against a staging copy and the paths are the one
thing that is not portable.

## plan-checklists.test.js — shape

Per page: row count equals what `plan_page` says it should be; no
`.reward--cut` and no `.pillCut` anywhere (New Plans is the only page that
keeps cut content, and it reads its own dataset); the count line agrees with
the rows; one checkbox per row; Technical is the last sub-expand on every row;
all four controls present. Across pages: **no plan id renders twice**, and
every group head pill matches the rows inside it.

Then the page-specific shapes — the Recipe roots in page order with Alcohol
non-empty (the trap that emptied it), Mods and Skins inside every armour set,
the Scoreboard Art rerun sub-expand — and a regression pass over New Plans,
Pennants, Displays and Underarmour, which this work did not touch and which
must still mount without error.

## plan-checklists-behaviour.test.js — behaviour

Expand All / Close All act on the root level; Tick All ticks every row and the
group heads follow; Reset clears; a group tick-box ticks its own rows and no
others; search narrows, opens the sub-expand holding the match, and restores
every row when cleared.
