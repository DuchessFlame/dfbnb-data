"""
by_page_slices.py — split a monolithic {"byPage": {...}} artifact into one file
per page, plus a small index.

WHY
---
`dist/activities/activities_rewards_by_page.json` is 67 MB and
`dist/events/events_rewards_by_page.json` is 60 MB. Both are fetched in full to
render a single page, because "_by_page" means "every page's data in one file".
The median page inside them is 79 KB. A visitor therefore downloads roughly
850x more than the page needs, and CI rewrites and commits both files every six
hours.

Splitting them adds files on disk and removes bytes from the wire. That is the
right trade: the standing constraint is consolidation of *bytes fetched per
pageview*, not of files on disk. Disk is free; the 30-second load is not.

The same page object is stored under several keys — the bare slug, the full
URL, and the URL without its trailing slash. Those are aliases for one page, so
each distinct page is written once and every key that points at it is recorded
in the index. That alone halves what the monolith holds.

SHAPE
-----
  <dist_dir>/by_page/_index.json
      {"_meta": {...}, "pages": {"<key>": "<file>.json", ...}}

  <dist_dir>/by_page/<file>.json
      {"_meta": {...}, "keys": ["<slug>", "<url>", ...], "page": {...}}

A renderer reads _index.json once (a few KB, cacheable across navigation),
looks up its slug or pathname, and fetches exactly one slice.

This is a module builders import, not a script anyone runs.
"""

from __future__ import annotations

import hashlib as _hashlib
import json
import os
import re
import unicodedata
from pathlib import Path

__all__ = ["write_by_page_slices", "slice_filename"]

_SAFE = re.compile(r"[^a-z0-9._-]+")


def slice_filename(key: str) -> str:
    """A filesystem- and URL-safe filename for a page key.

    Deliberately lossy in a stable way: two keys that differ only by case,
    slashes or punctuation collapse to the same name, which is fine because
    they are aliases for one page anyway. Collisions between genuinely
    different pages are resolved by the caller.
    """
    s = unicodedata.normalize("NFKD", str(key or "")).encode("ascii", "ignore").decode()
    s = s.strip().strip("/").lower()
    s = s.replace("/", "-")
    s = _SAFE.sub("-", s).strip("-.")
    return (s or "page")[:120] + ".json"


def _pick_name_key(keys):
    """The key a slice should be named after.

    Prefer the bare slug (no slashes) — it is the shortest stable identity and
    the one the page itself advertises. Fall back to the longest path, whose
    last segment is usually that same slug.
    """
    bare = [k for k in keys if "/" not in k]
    if bare:
        return min(bare, key=len)
    return max(keys, key=len)


def write_by_page_slices(dist_dir, by_page, *, name, meta=None, prune=True):
    """Write one slice per distinct page under <dist_dir>/by_page/.

    dist_dir : directory the monolith is written to (slices go in a by_page/
               subfolder of it)
    by_page  : the {key: page_object} mapping, keys aliasing the same object
    name     : short label for the family, e.g. "activities" — recorded in
               _meta so a slice can be traced back to its builder
    meta     : extra provenance merged into every slice's _meta, e.g. the
               observed export date. Stamp what was OBSERVED, not build time.
    prune    : delete slice files left over from a previous build

    Returns (pages_written, total_bytes).
    """
    out_dir = Path(dist_dir) / "by_page"
    out_dir.mkdir(parents=True, exist_ok=True)

    base_meta = {"family": name}
    if meta:
        base_meta.update(meta)

    # Group aliases so that three keys pointing at one page produce one file,
    # not three copies of it. Identity catches the common case (a builder
    # assigning the same object under slug, url and url-minus-slash); the
    # content hash catches the rest, including a by_page that was round-tripped
    # through JSON and so lost identity. Each distinct page is serialised once.
    by_id = {}
    for key, page in by_page.items():
        by_id.setdefault(id(page), (page, []))[1].append(key)

    groups = {}
    for page, keys in by_id.values():
        body = json.dumps(page, separators=(",", ":"), sort_keys=True)
        digest = _hashlib.sha1(body.encode("utf-8")).hexdigest()
        if digest in groups:
            groups[digest][1].extend(keys)
        else:
            groups[digest] = (page, list(keys))

    # Assign filenames, resolving the rare genuine collision deterministically.
    assigned = {}
    taken = set()
    for gid, (page, keys) in sorted(groups.items(), key=lambda kv: _pick_name_key(kv[1][1])):
        fname = slice_filename(_pick_name_key(keys))
        if fname in taken:
            stem, ext = os.path.splitext(fname)
            n = 2
            while f"{stem}-{n}{ext}" in taken:
                n += 1
            fname = f"{stem}-{n}{ext}"
        taken.add(fname)
        assigned[gid] = fname

    index = {}
    total_bytes = 0

    for gid, (page, keys) in groups.items():
        fname = assigned[gid]
        keys_sorted = sorted(set(keys))
        payload = {"_meta": dict(base_meta), "keys": keys_sorted, "page": page}
        path = out_dir / fname
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"))
        total_bytes += path.stat().st_size
        for k in keys_sorted:
            index[k] = fname

    index_payload = {
        "_meta": dict(base_meta, pages=len(groups), keys=len(index)),
        "pages": dict(sorted(index.items())),
    }
    index_path = out_dir / "_index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index_payload, f, separators=(",", ":"))

    if prune:
        # Only ever removes files in the slice directory this helper owns.
        keep = taken | {"_index.json"}
        for stale in out_dir.glob("*.json"):
            if stale.name not in keep:
                stale.unlink()

    biggest = max((f.stat().st_size for f in out_dir.glob("*.json")), default=0)
    print(
        f"[by_page_slices] {name}: {len(groups)} pages, {len(index)} keys -> "
        f"{out_dir} ({total_bytes / 1048576:.1f} MB total, largest slice "
        f"{biggest / 1024:.0f} KB)"
    )
    return len(groups), total_bytes
