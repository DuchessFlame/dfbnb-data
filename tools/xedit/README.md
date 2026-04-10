# xEdit export scripts

Pascal scripts that run inside [xEdit](https://github.com/TES5Edit/TES5Edit) (Fallout 76 branch, aka FO76Edit) to dump game records to TSV for the builders in `src/`.

## Files

- **`ExportCOBJToTSV.pas`** — Dumps selected COBJ (Constructible Object / crafting recipe) records to `COBJ_Export_<Month>_<Year>.tsv`. Consumed by `build_cobj_recipes_json.py`.
- **`ExportALCHToTSV.pas`** — Dumps selected ALCH (Ingestible / consumable) records to `ALCH_Export_<Month>_<Year>.tsv`. Consumed by `build_alch_items_json.py` / downstream menu classifiers.

## How to run

1. Open FO76Edit and load `SeventySix.esm` (plus any DLC/updates).
2. Wait for the background loader to finish.
3. Right-click the **COBJ — Constructible Object** branch (or **ALCH — Ingestible**) → **Apply Script…**
4. Pick `ExportCOBJToTSV.pas` (or `ExportALCHToTSV.pas`) from this folder.
5. Save the resulting `.tsv` into `dfbnb-data/tsv/`.
6. The GitHub Action on push will rebuild `dist/cobj-recipes.json`, `dist/menu-items.json`, and friends.

## Why signature-based lookups

The original versions of these scripts used `ElementByPath(e, 'GNAM - Workbench')` etc. — the string after the signature (`GNAM - Workbench`, `FNAM - Keywords`, `ENIT - Effect item settings`) is a **display label** that the xEdit team occasionally renames. When it changes, `ElementByPath` silently returns `nil` and the affected columns come out empty, which is exactly what happened to `COBJ_Export_Apr_2026.tsv`.

Both scripts were rewritten in April 2026 to use `ElementBySignature(e, 'XXXX')` instead, where `XXXX` is the four-character record code. Signatures are part of the file format and never change between xEdit versions.

## COBJ column additions

The rewrite added a **`BNAM_*`** column group (FormID / EDID / FULL) for the actual workbench keyword. In Fallout 76 COBJ, the workbench is stored in BNAM — GNAM is a legacy/unused slot. `build_cobj_recipes_json.py` now prefers `BNAM_EDID` for food/chem category classification when present, falling back to FNAM keywords when absent (older TSVs).

Full COBJ header emitted by the new script:

```
COBJ_FormID   COBJ_EDID
CNAM_FormID   CNAM_EDID   CNAM_FULL     (created object)
BNAM_FormID   BNAM_EDID   BNAM_FULL     (workbench keyword — NEW)
GNAM_FormID   GNAM_EDID   GNAM_FULL     (legacy/optional)
FNAM_Keywords FVPA        ReferencedBy_Flat   ReferencedByCount
Ref_1 … Ref_N
```

The `build_cobj_recipes_json.py` builder accepts either `COBJ_EDID` or `EDID` for the second column, so older exports still parse.

## FVPA parsing

FVPA (component requirements) is an array of structs. Each struct has a `Component` (form ref) and a `Count` (integer). The rewritten script reads these sub-fields explicitly via `ElementByName` + positional fallback, rather than relying on `GetEditValue` returning a pre-formatted string — different xEdit builds format that string differently (or not at all), which was another source of empty FVPA cells.

Output format per row: `ComponentEDID:Count|ComponentEDID:Count|...`
