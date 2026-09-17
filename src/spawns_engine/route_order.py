"""Per-item WALKING-ROUTE order for the Fixed Spawn Locations expand.

The engine's default ordering is mechanical: regions in `ALL_REGIONS` order, markers
A-Z inside a region, placements by (source_type, ref). That is stable and repeatable,
but it is not the order a player walks the item, so a guide built from it can send the
reader into the middle of the map, skip a spawn and make them double back. The written
guide is the authority on the route; this file lets that route drive the document.

`data/spawn_route_order.tsv` — columns: slug, region, marker, order

  * `marker` EMPTY  -> `order` is a "|"-separated list of MARKER NAMES, in the order
                       the guide visits them inside that region.
  * `marker` SET    -> `order` is a space-separated list of REFS (the hex form used in
                       `spawns[].ref`), in the order the guide visits the spawns at
                       that marker.

Anything the file does not mention keeps the mechanical order and falls in BEHIND the
listed entries, so a partial route is safe and a new placement never disappears.

Ref order matters twice over: `spawns_configs/farming._attach_breakdowns()` numbers the
per-spawn labels by walking `spawns[]` in order, so "Deathclaw Nest #1" becomes the
FIRST nest on the route rather than the lowest FormID.
"""

import csv
import os

__all__ = ["load", "TSV_NAME"]

TSV_NAME = os.path.join("data", "spawn_route_order.tsv")


def _repo_root():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load(slug, path=None):
    """Return {"markers": {region: [marker, ...]}, "spawns": {(region, marker): [ref, ...]}}
    for `slug`. Missing file or no rows for the slug -> empty dicts, i.e. the
    mechanical order, so this is safe to call unconditionally."""
    path = path or os.path.join(_repo_root(), TSV_NAME)
    out = {"markers": {}, "spawns": {}}
    if not os.path.exists(path):
        return out
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            if (row.get("slug") or "").strip() != slug:
                continue
            region = (row.get("region") or "").strip()
            marker = (row.get("marker") or "").strip()
            order = (row.get("order") or "").strip()
            if not region or not order:
                continue
            if marker:
                out["spawns"][(region, marker)] = order.split()
            else:
                out["markers"][region] = [m.strip() for m in order.split("|") if m.strip()]
    return out
