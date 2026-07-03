# Scrap breakdown — how to find junk-item yields in the TSVs

Paste this into a new thread when you want to work out what a junk item scraps
into (e.g. "Toothbrush = 1x Plastic", "House Teapot = 2x Ceramic").

---

**Context — the FO76 scrap data model.**

What a junk item yields is a two-table join across two xEdit TSV exports. You do
**not** need to touch a component's "Referenced By" list (Steel alone has 8000+
refs) — everything you need is on the front-facing records.

Table 1 — `MISC_Export_*.tsv` (the junk items). Relevant columns:
`FormID`, `EDID`, `FULL` (display name), `MCQP`.
The `MCQP` column lists the item's scrap components, pipe-separated, each entry
formatted `ComponentEDID:TierKeyword`:

```
c_Plastic:ComponentQuantityLow
c_Steel:ComponentQuantityMedium|c_Plastic:ComponentQuantityHigh
```

The tier keyword (Low / Medium / High / Rare / Bulk / etc.) is **not** a number —
it's a pointer into the component's own amount table.

Table 2 — `CMPO_Export_*.tsv` (the components). Relevant columns:
`CMPO_EDID` (the component, e.g. `c_Plastic`), `FULL` (component name, "Plastic"),
`ScrapItem_FULL` (the scrap item you receive, e.g. "Molded Plastic"), `CVPA`.
The `CVPA` column is that component's tier→amount table, pipe-separated, each
entry formatted `TierKeyword:Count:CurveTable` (CurveTable is normally blank):

```
ComponentQuantity_Scrap_Singular:1:|ComponentQuantityRare:5:|ComponentQuantityMedium:2:|ComponentQuantityLow:1:|ComponentQuantityHigh:3:|ComponentQuantityBulk:30:
```

**The join.** For each `ComponentEDID:TierKeyword` in a MISC item's `MCQP`:
look up that `ComponentEDID` in the CMPO table (`CMPO_EDID`), then read the
`Count` for that `TierKeyword` out of its `CVPA` table. That count is the amount.

```
Toothbrush.MCQP = c_Plastic:ComponentQuantityLow
c_Plastic.CVPA  -> ComponentQuantityLow = 1
=> Toothbrush yields 1x Plastic
```

**Critical gotcha — amounts are per-component, not universal.** The same tier
keyword resolves to different numbers on different components. Example:
`ComponentQuantityBulk` = **30** on Steel but **10** on Plastic. So you must
read the count from *that component's* CVPA row — never assume a fixed value for
a tier, and never hardcode a tier→number map.

**Cut content.** Skip any `EDID` that `src/cut_content.py` flags (prefixes like
`zzz_`, `CUT_`, `TEST`, `PTS_`, etc.).

---

**The automated builder.** This join is already scripted:

- Script: `src/build_scrap_breakdown_json.py`
- Run: `python src/build_scrap_breakdown_json.py` (reads the newest
  `CMPO_Export_*.tsv` + `MISC_Export_*.tsv` from `tsv/`, writes
  `dist/scrap-breakdown.json`).
- Output has `items` (item → components, with `amount` per component) and
  `by_component` (reverse index: component → items that yield it).
- If it reports `needs_misc_reexport: true`, the newest MISC export is missing
  the `MCQP` column — re-run `!!!Wordpress - ExportMISCToCSV.pas` on the MISC
  branch in xEdit and drop the new TSV in `tsv/`.

The xEdit export scripts that produce these TSVs:
`GitHub/xedit scripts/!!!Wordpress - ExportCMPOToTSV.pas` (CMPO/CVPA) and
`!!!Wordpress - ExportMISCToCSV.pas` (MISC/MCQP).
