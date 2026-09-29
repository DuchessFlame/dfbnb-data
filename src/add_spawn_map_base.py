#!/usr/bin/env python3
r"""
add_spawn_map_base.py — stamp `map_base` onto every spawn doc, for the region maps.

The Fixed Spawn Locations expand now carries a "View {Region} spawn map →" link per
region, the same way Chance to Spawn Locations already did. Both links resolve to the
item's OWN guide-images folder:

    <map_base><region-slug>-spawn-map.avif     (fixed  — new)
    <map_base><region-slug>-chance-map.avif    (chance — already rendered by
                                                render_spawn_maps.py, folder 05)

`map_base` is DERIVED, never typed (spawn-guide §9k):
  Rule 1 — reuse the guide-images/<cat>/<item>/ folder of any hand-authored spawn
           photo already in the doc. The live site is the truth; it is the only way
           to know Deathclaw Egg's folder is `deathclaw-eggs`, plural.
  Rule 2 — fall back to <category from the doc's family>/<slug from the display name>.

This runs as a POST-STEP over committed dist/, like add_container_types.py and
add_treasure_maps.py, so no builder had to change and it is safe to re-run. It writes
a top-level `map_base` on every doc and backfills `chance_spawns.map_base` when the
family has one and it is missing.

Sept 2026 — the pages now CHECK for each map before showing its link (a HEAD
request in the renderer), so this step is safe to run over every doc before any map
is uploaded: `map_base` and `full_map` are stamped everywhere, and the links appear
on their own once Duchess uploads the files. It runs in CI after every spawn build
(dfbnb-patch-build.yml / dfbnb-pts-build.yml), so a rebuild can never strip them.
`full_map` is derived (<map_base><render-slug>.jpg) — no local Guides folder needed;
when that folder IS on this machine the upload sheet lists the local file too.
NPC score-challenge pages (npc-*) are skipped — finish_npc_spawn_maps.py owns them.

Usage:
    python src/add_spawn_map_base.py            # all families
    python src/add_spawn_map_base.py --pts      # PTS dist tree
    python src/add_spawn_map_base.py --dry-run
    python src/add_spawn_map_base.py --full-map-only   # just the 4K download links
"""

import argparse, json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spawns_engine import build as ebuild   # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (glob root, category) — the category is the guide-images subfolder on the site,
# matching the FTP layout under /wp-content/uploads/guide-images/.
FAMILIES = [
    ("farming_spawns", None),            # chems / eggs / non-perishable — derived per slug
    ("meat",           "farming-meat"),
    ("plants",         "farming-plants"),
    ("insects",        "farming-insects"),
    ("nuka",           "farming-nuka-cola"),
]


def dist_root(pts):
    return os.path.join(REPO, "dist", "pts") if pts else os.path.join(REPO, "dist")


def docs_for(family, root):
    """-> [(path, slug)] for one family."""
    if family == "nuka":
        out = []
        for fn in sorted(os.listdir(root)):
            if (fn.startswith("nuka_cola_spawns_") and fn.endswith(".json")
                    and "-WorkHorse" not in fn and "manifest" not in fn):
                out.append((os.path.join(root, fn),
                            fn[len("nuka_cola_spawns_"):-len(".json")]))
        return out

    d = os.path.join(root, family)
    if not os.path.isdir(d):
        return []
    out = []
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".json") or "-WorkHorse" in fn:   # OneDrive conflict copies
            continue
        slug = fn[:-len(".json")]
        if family == "farming_spawns":
            slug = slug[:-len("_spawns")] if slug.endswith("_spawns") else slug
        out.append((os.path.join(d, fn), slug))
    return out


OVERRIDES_PATH = os.path.join(REPO, "data", "spawn_map_base_overrides.tsv")

GUIDES_ROOT = os.environ.get(
    "GUIDES_ROOT", r"C:\Users\Duche\OneDrive\Guides and Stuff")


def render_job(family, slug):
    """(render source, render slug) for a doc, or None when it has no farming map.

    The meat LOCATION GUIDES live in farming_spawns as `meat-<slug>` but their maps
    are rendered from dist/meat as source `meat`. NPC score-challenge pages have
    their own finisher (finish_npc_spawn_maps.py) and the meat hub docs are not
    guide pages, so neither gets a full map from here."""
    if family == "meat":
        return None
    if family == "farming_spawns":
        if slug.startswith("npc-"):
            return None
        if slug.startswith("meat-"):
            return ("meat", slug[len("meat-"):])
        return ("farming", slug)
    return ({"nuka": "nuka", "plants": "plants", "insects": "insects"}[family], slug)


def local_full_map(family, slug, doc):
    """-> (render slug, path) of the 4096 full map on disk, or None.

    render_spawn_maps.py writes `01 Full Maps (4096)/<render-slug>.jpg` in the
    item's own folder; render_all_maps.item_folder() finds that folder the same
    way the renderer did."""
    job = render_job(family, slug)
    if not job or not os.path.isdir(GUIDES_ROOT):
        return None
    try:
        import render_all_maps as RA
    except Exception:
        return None
    source, rslug = job
    cat_dir = os.path.join(GUIDES_ROOT, RA.category_folder(source, rslug))
    name = doc.get("name") or rslug
    if source == "meat" and name.endswith(" Meat"):
        name = name[:-len(" Meat")]
    folder = RA.item_folder(cat_dir, name, rslug)
    path = os.path.join(folder, "01 Full Maps (4096)", f"{rslug}.jpg")
    return (rslug, path) if os.path.exists(path) else None


def load_overrides():
    """slug -> map_base. For folders the derivation cannot guess.

    The live site is the truth and it is not always regular: farming-eggs holds
    `deathclaw-eggs` AND `mirelurk-eggs`, both plural, while the slugs are singular.
    Deathclaw resolves itself because the doc already carries photos from that folder
    (Rule 1); Mirelurk has none yet, so it needs a line here. Two columns, tab
    separated: slug <tab> map_base. Blank lines and # comments ignored.
    """
    out = {}
    if not os.path.exists(OVERRIDES_PATH):
        return out
    with open(OVERRIDES_PATH, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 2 or parts[0].lower() == "slug":
                continue
            base = parts[1].strip()
            if base and not base.endswith("/"):
                base += "/"
            out[parts[0].strip()] = base
    return out


def resolve_base(doc, slug, category, overrides=None):
    """The item's guide-images folder. Override > Rule 1 (existing photo) > Rule 2."""
    if overrides and slug in overrides:
        return overrides[slug]

    base = ebuild.image_base(doc, doc.get("set") or slug, doc.get("name") or "",
                             category=category)

    # Rule 1 searches the whole doc for a guide-images path, so it can latch onto a
    # value stamped by an EARLIER, wrong derivation and keep re-applying it — that is
    # how nuka-cola-cherry ended up under farming-non-perishable. When the family
    # tells us the category outright, the category wins and only the item folder is
    # taken from the match.
    if category and base:
        parts = [p for p in base.strip("/").split("/") if p]
        if len(parts) >= 2 and parts[-2] != category:
            base = f"{ebuild.UPLOADS}{category}/{parts[-1]}/"
    return base


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--full-map-only", action="store_true",
                    help="stamp only full_map; leave map_base alone")
    args = ap.parse_args()

    root = dist_root(args.pts)
    if not os.path.isdir(root):
        print(f"no dist tree at {root}", file=sys.stderr)
        return 1

    overrides = load_overrides()
    changed = missing = total = 0
    full_maps = []
    for family, category in FAMILIES:
        for path, slug in docs_for(family, root):
            total += 1
            try:
                doc = json.load(open(path, encoding="utf-8"))
            except Exception as e:
                print(f"  !! {path}: {e}", file=sys.stderr)
                continue

            if family == "farming_spawns" and slug.startswith("npc-"):
                continue
            if family == "farming_spawns" and slug.startswith("meat-") and slug not in overrides:
                # Meat location guides live in the creature's farming-meat folder,
                # same as build_meat_guide_docs.MAP_ROOT.
                base = f"{ebuild.UPLOADS}farming-meat/{slug[len('meat-'):]}/"
            else:
                base = resolve_base(doc, slug, category, overrides)
            if not base:
                missing += 1
                print(f"  ?? {slug}: could not derive a map_base")
                continue

            dirty = False
            if not args.full_map_only and doc.get("map_base") != base:
                doc["map_base"] = base
                dirty = True
            # "Download the full 4K spawn map here" (Fixed Spawn Locations). Maps go
            # on the site as JPEG: `<render-slug>.jpg` in the page's own folder. The
            # page hides the link until the file is uploaded, so it is stamped on
            # every page; an existing value is never replaced.
            job = render_job(family, slug)
            if job and not doc.get("full_map"):
                fbase = doc.get("map_base") or base
                doc["full_map"] = fbase + job[1] + ".jpg"
                fm = local_full_map(family, slug, doc)
                full_maps.append((slug, fm[1] if fm else "(not rendered yet)", fbase))
                dirty = True
            cs = doc.get("chance_spawns")
            if (not args.full_map_only and isinstance(cs, dict) and cs.get("regions")
                    and not cs.get("map_base")):
                cs["map_base"] = base
                dirty = True

            if dirty:
                changed += 1
                if not args.dry_run:
                    # indent=1 matches every other dist writer — keep the diffs small.
                    with open(path, "w", encoding="utf-8") as fh:
                        json.dump(doc, fh, ensure_ascii=False, indent=1)

    for slug, path, base in full_maps:
        print(f"  full map  {slug:34} {path}  ->  {base}")
    verb = "would update" if args.dry_run else "updated"
    print(f"map_base: {verb} {changed} of {total} docs"
          + (f" ({missing} unresolved)" if missing else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
