# data/chainsaw_spawns/

Committed geo cache for the **Weapons - Chainsaws** pages
(`src/spawns_configs/chainsaws.py` → `dist/chainsaws/<slug>.json`).

## geo_cache.json

`{ instanceFormID: { base, space, x, y, region, marker } }` — one entry per resolved
world placement, exactly the shape every other spawn family uses.

Coordinates live only in the local Mappalachia `Position` table (~459 MB, not in CI), so
the cache is how CI reproduces the Fixed Spawn Locations expand without the database:

Seed it in **two steps**, on purpose. rng76 holds the whole LVLI export in memory
(~260 MB) and the Mappalachia DB is a ~459 MB SQLite scan; running both in one process
peaks around 700 MB and is enough to put a laptop into swap. `--geo-only` loads neither
rng76 nor the camp-item / vendor / curve exports — it peaks around 80 MB plus the DB.

```powershell
cd C:\Users\Duche\OneDrive\GitHub\dfbnb-data
$env:MAPPALACHIA_DB = "D:\Mappalachia\data\mappalachia.db"
Test-Path $env:MAPPALACHIA_DB                                  # must print True

python src/build_chainsaw_spawns_json.py --geo-only            # DB pass — writes the cache only
python src/build_chainsaw_spawns_json.py --pts                 # normal build — reads the cache

git add data/chainsaw_spawns/geo_cache.json dist/chainsaws.json dist/chainsaws/ dist/pts/chainsaws*
```

Every later CI run rebuilds the same page from this cache plus the committed TSVs.

Running the normal build *with* `MAPPALACHIA_DB` still set also works and does both at
once — it just costs the full ~700 MB, and it prints a note saying so.

**Until that first local pass, the cache is `{}` and Fixed Spawn Locations renders its
empty state** — the rest of the page (Weapon Stats, Mods & Plans, Containers, Events &
Activities, Vendors, the CAMP producer expands) is resolved entirely from the TSV
exports and is correct without the DB.

The chainsaw seeds 28 direct `REFR` placements through `LPI_Weapon_Melee_Chainsaw_76`
(`003B81B8`), so a successful local pass should populate roughly that many entries.

## Spawn maps

`src/render_spawn_maps.py` reads this cache and the built doc. The chainsaw family is
registered in its `SOURCES` table as `chainsaw`, so it needs no per-run configuration:

```powershell
$env:MAPPALACHIA_DIR = "D:\Mappalachia"          # the map art
$env:MAPPALACHIA_DB  = "D:\Mappalachia\data\mappalachia.db"
python src/render_spawn_maps.py --set chainsaw --source chainsaw `
  --out "C:\Users\Duche\OneDrive\Guides and Stuff\.Weapons\Chainsaws"
```

Backgrounds are the illustrated `img/wrld/Appalachia_menu.jpg` and per-cell
`img/cell/<spaceEditorID>.jpg` — never the satellite map. Output is the standard layout:
`01 Full Maps (4096)`, `02 Numbered Maps (4096)`, `03 Region Tiles`, `04 Interior Maps`,
plus the coord CSVs.

At 29 placements that is 19 exterior dots across 5 region tiles (Burning Springs,
Cranberry Bog, Forest, Savage Divide, Toxic Valley) and 10 interior dots across 6 cells
(WVLumberCo01, PoseidonPlant02, XPDPitt01Foundry, XPDPitt01FoundryDungeon,
XPDAC03CommunityCenter, Vault63Organics). Every spawn is `direct`, so the legend is the
single amber "Loose spawn" row.

Hand-authored per-spawn photos and directions are **not** stored here — they live in
`dist/chainsaws/<slug>.json` under each location's `spawns[]` and are merge-preserved by
`spawns_engine.build.load_existing()` on every rebuild, keyed by placement ref.
