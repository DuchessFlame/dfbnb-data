# Shared image library

One set of item pictures on the server, used by every reward page, so nothing is
uploaded twice.

## How a page picks a picture

Every builder asks `src/image_index.py`, in this order. The first hit wins.

1. **Shared library**, matched by FormID first, then EditorID:
   - `guide-images/plan-checklist/<type>/`: plans, apparel, weapons, masks, CAMP items, recipes …
   - `guide-images/titles/`, `guide-images/atom-shop/`, `guide-images/camp-items/`, `season_images/`

   Babylon / Nuclear Winter items always resolve to `plan-checklist/legacy-nuclear-winter/`,
   never to an event-folder copy.
2. **The page's own event folder**: the file the builder proposed, used only if it really is on the server.
3. **Another page's event folder**: the same item, reused rather than uploaded again.
4. **Placeholder**:
   - Titles get the blank name tag for their own affix (Prefix / Suffix / Prefix-Suffix).
   - Weapon mod plans get the yellow mod box. Skins and paints are not mods and never get it.
   - Anything else gets nothing, and the page draws its own dashed placeholder.

Every URL that ends up on a page is in the server listing, so pages never point at a 404.

## After every upload batch

```powershell
cd "C:\Users\Duche\OneDrive\GitHub\dfbnb-data"
python tools\list_server_images.py --filezilla "<your FileZilla site name>"
REFRESH-IMAGES.bat
```

- `list_server_images.py` walks `wp-content/uploads` over the same connection FileZilla uses.
  It asks for the password and never saves it. Run `--filezilla ""` to see your saved site names.
  The first time, run `pip install paramiko`.
- `REFRESH-IMAGES.bat` rebuilds `dist/image_index.json`, re-points the plan checklists
  (`add_plan_images.py`), then re-points the seasonal, mutated, Daily Ops, activities,
  public events and treasure maps JSON already in `dist/`. Nothing is rebuilt from the game files.
- Then commit:
  - `data/server_listing.tsv`
  - `dist/image_index.json`
  - `dist/missing_images.json`
  - `audits/missing_images.md`
  - `audits/image_cleanup.md`
  - every changed `dist/` file

  The builders read the committed index, so CI builds pick the same pictures.

## Where new uploads go (FileZilla)

| What it is | Folder on the server |
|---|---|
| A plan, or the item a plan builds | `wp-content/uploads/guide-images/plan-checklist/<type>/` (the folder the missing report names: `apparel`, `weapons`, `workshop`, `recipes`, `power-armour`, `body-armour`, `underarmour`, `backpack`, `mines-and-grenades`, `snowglobe`, `fishing-rod`, `camera`, `photomode`, `display`, `pts-pennants`, `scoreboard-art`) |
| Apparel with no plan | `wp-content/uploads/guide-images/plan-checklist/apparel-without-plans/` |
| Babylon / Nuclear Winter | `wp-content/uploads/guide-images/plan-checklist/legacy-nuclear-winter/` only |
| A title | `wp-content/uploads/guide-images/titles/titles-player/` or `titles-camp/` (named as in `dist/titles/title_art_missing.json`) |
| An odd reward that exists nowhere else | that event's existing reward folder, e.g. `guide-images/seasonal-events/treasure-hunters/` |
| Cover, gallery, maps, guide images and photos, reward checklists | the event folder's own subfolders, unchanged |

## How to name a new file

- **Use the FormID of the thing in the picture.** For a plan, that's the item it builds. The missing report gives the exact name.
  - `0063336C.avif`: the main picture
  - `0063336C_go.avif`: an outfit's folded, item-only render (this becomes the main picture)
  - `0063336C_c1.avif`, `_c2` …: extra views for the Item Image carousel, such as an outfit's mannequin view

  A plan's FormID and its item's FormID share one file, so upload it once.
- One picture serving several rewards (for example colour variants) stays one file. Name it after one of them, then point the others at it with an `overrides` entry in `data/plan_images.json`.
- AVIF only. Delete the original and any duplicates once it's converted.
- Never upload a mannequin-only picture as the main image. If there's no folded render, leave the item on the missing list.
- Existing files keep their names. The index already finds them by editor ID, texture or name slug.

## Reports

- `audits/missing_images.md`: the to-do list. It starts with files that are already in your local `.Plan Checklist` folder but not on the server (upload them as they are), then everything with no picture anywhere, grouped by page, with the folder and file name to use. Activities, public events and treasure maps come last, because those pages don't draw reward thumbnails yet.
- `audits/image_cleanup.md`: tidying on the server:
  - originals left beside their AVIF
  - Nuclear Winter art copied into event folders
  - event-folder copies of library art
  - duplicate uploads
  - names shared by two different pictures
  - library files nothing uses

  Guide, map, gallery, checklist and cover files are never listed. Nothing is deleted automatically.

## Files

| File | Job |
|---|---|
| `tools/list_server_images.py` | server listing → `data/server_listing.tsv` |
| `src/build_image_index.py` | listing + game files + `dist/` → `dist/image_index.json`, `audits/image_cleanup.md` |
| `src/image_index.py` | the lookup every builder calls, plus the per-page handlers |
| `src/apply_image_index.py` | applies the handlers to `dist/` without a rebuild; writes the missing report |
| `src/plan_images.py` | plan checklists: asks the library first; staged stems come from the listing |
