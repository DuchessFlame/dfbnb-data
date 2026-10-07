# Plan routes audit: 6 Oct route fix, vendors and cut content (7 Oct 2026)

These files compare the plan sources before and after the 6 Oct route fix and this rebuild. Nothing in them is committed or pushed.

| File | What it lists |
|---|---|
| `route_audit_live.csv` | Every Live plan whose routes changed. Compares 3 Oct (before the fix) with this rebuild. One row per route added or removed, with a verdict and the reason. |
| `route_audit_pts.csv` | The same for PTS. The "before" side is the committed 6 Oct PTS build, which still used the old route code. |
| `rename_log_live.csv` / `rename_log_pts.csv` | Each label change: old label, new label, number of plans, and where the new name comes from. |
| `vendor_check_live.csv` / `vendor_check_pts.csv` | Each plan checked against each vendor chest whose stock list can reach it in the game files, with whether the vendor is shown. |
| `no_source_plans.csv` | Each plan with no source (Live and PTS), and why. |
| `labels_still_editor_style.csv` | Labels that still read like editor names because the game data has no better name. These need a decision. |

## Verdicts in route_audit_*.csv

- **NESTED_OK**: the route was dropped correctly. Its list only sits inside another list, so its rate was the chance *after* the outer roll had already picked it. The real source is now shown at the real rate; the reason column names the row.
- **RENAMED**: the same leveled list or lists now appear under a new name, or merged into a vendor-network row.
- **RATE_CHANGED**: same source, different rate. Either it now shows the rate of the list a player actually reaches, or the rate comes from the intentional rng76 First Match change.
- **RETIRED**: moved to `retired_routes` because it is dead or cut, for example the Mole Miner Mystery Crate.
- **CONTAINER**: the 14 Camping Cooler, Gone Fission and Icebox rows. These are Atom Shop CAMP storage that nothing places in the world.
- **CAPPED** and **CHECK**: both are now **0**. No real source was left out.

For Live, all 2,095 changed plans were checked. None of the removed routes was a real source dropped by mistake.

## Fixes made in this pass

1. **"Locker" vendors.** 40 vendor chests carry the FULL name "Locker", including Grahm, the Wastelanders C.A.M.P. traders, workshop vendors, Milepost Zero and Fishing. In the 6 Oct build that produced 481 rows called "Locker", and different vendors were merged into one row. A FULL name shared by more than one CONT record is no longer used.
2. **Vendor NPC names.** Vendors are now named after their NPC, using the NPC2_Vendors export (chest → NPC FULL). Examples:
   - Carver Timmerman, not "E05 Carver Caravan vendor"
   - Grahm, not "GQ10 Travelling Workshops"
   - Vendor Bot Phoenix (Watoga), not "Location based PA System in Watago"
   - Regs, Del Lawson, Pendleton, Antoine
3. **Faction vendor networks.** Station vendors that sell the same faction stock at the same rate are merged into one row, e.g. "Responders vendors (Camden Park, Charleston, …)". Before this, about 12 identical station rows filled the 12-row cap and hid every other source.
4. **Restored real sources.** A nested list now keeps its route when no list above it can publish one. Example: Mischief Night (White Springs) on Resort Sign, Resort Lamps and Gilded Wall Clock.
5. **Rows that were really a quest reward.** A list paid out by a quest reward no longer shows as a second vendor row. Example: Event: Distinguished Guests, which was a duplicate "Whitespring Gourmet vendor".
6. **Mole Miner Mystery Crate is now cut content.** It is listed in `plan_sources.CONFIRMED_CUT_SOURCES`, and its routes move to `retired_routes`:
   - Live: 344 routes on 317 plans
   - PTS: 328 routes on 303 plans
   - Neither channel publishes a live route from it any more
   - Two plans whose only source was the crate are now cut: Pioneer Scout Banner: Squirrel and Pioneer Scout Poster.
   - The farming notes for Tick Blood and Blood Sac no longer mention the crate.
7. **Naming clean-ups.**
   - LPI_Recipes_* lists are now "Plans lying in the world".
   - Quest titles that end in a trailing `<Alias=…>` are now usable.
   - Generic pool words ("Power Armor", "SPOTLIGHT Workshop") are named after the quest that pays them out. System names such as Raids and Daily Ops are kept.
   - Mutated Party Pack rows still file under Mutated Party Packs on New Plans.

## Still needs a decision

- `labels_still_editor_style.csv`, for example "Camp - Weapon Vendor - Merchant", "Daily Ops - Chase", "Tool Box Expert", and "HIDE Drifter" on PTS. There is no in-game name for these in the exports.
- `no_source_plans.csv`:
  - Live has 141 plans with no source. 33 are Nuclear Winter rewards, 31 have only dead sources, and the other 77 are given by scripts, quests or the Atom Shop, which the exports do not link.
  - PTS has 279, of which 151 are new on PTS.
  - The 6 Oct route fix did not leave any plan without a source.
  - "Slab of Ground/Steak Meat Plushie" are cut in both the 3 Oct and 6 Oct data (no recipe uses them and nothing gives them out). The empty Discord post for them comes from the bot, not from the data.
- 16 vendor/plan pairs per channel resolve to 0% in rng76. They were not shown in any earlier build either.
- About 47 vendor rows are hidden because a route that is later retired as dead still takes the 12th slot. The 12-row cap is applied before dead routes are pruned. Pruning before the cap would fix this, but that is a separate change.
