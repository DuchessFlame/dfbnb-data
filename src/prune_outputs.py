#!/usr/bin/env python3
"""
prune_outputs.py
----------------
One helper for the rule that generated output directories must be pruned, not
appended to.

WHY THIS EXISTS
    A builder that writes one file per key and never deletes anything is
    append-only: dist/ keeps serving whatever it was last given, forever, and
    nothing reports it. dist/ was still serving Seasons 1-23 "upcoming rewards"
    months after those seasons launched; dist/meat/ still held eight insect
    pages from before insects moved to their own family; dist/plants/ carried 43
    duplicate "-Sally" files no builder has produced in months.

    None of that surfaced as an error. Stale data never does - that is exactly
    what makes it dangerous.

THE RULE
    After the build loop, delete every file in the output directory that this
    run did not write. Deleting is safe precisely because the directory is
    generated: if a key should have a file, the same run just wrote it.

THREE THINGS TO GET RIGHT
    1. Run it AFTER the build loop, never before. Several builders read the
       previous document back in to carry editorial fields forward
       (spawns_configs/meat.py, insects.py, cryptids.py all do this).
    2. Skip it when the run was filtered to one key. "Not written" then only
       means "not asked for", and pruning would delete everything else.
    3. Prune the mirror too. A builder that also writes dist/pts/ must prune
       both, and shutil.copytree(dirs_exist_ok=True) does NOT remove files that
       have gone from the source.

USAGE
    from prune_outputs import prune_outputs

    written = set()
    for page in roster:
        ...
        written.add(page["slug"] + ".json")

    prune_outputs(out_dir, written, tag="[plants]", skip=bool(slug_filter))

STATUS: active
"""

from __future__ import annotations

import os
from pathlib import Path

__all__ = ["prune_outputs"]


def prune_outputs(
    out_dir,
    keep,
    pattern: str = "*.json",
    tag: str = "[prune]",
    skip: bool = False,
    also_keep=("_index.json",),
    verbose: bool = True,
) -> list:
    """Delete files in out_dir matching pattern that are not in `keep`.

    out_dir   directory to prune (str or Path). Missing directory is a no-op.
    keep      filenames written by this run, e.g. {"tato.json", "corn.json"}.
              Bare keys are accepted too and get the pattern's suffix applied.
    pattern   glob for the files this builder owns. Keep it tight: a directory
              shared by two builders must only ever prune its own family.
    skip      True when the run was narrowed to a subset - prunes nothing.
    also_keep filenames that are never per-key output (manifests, indexes).

    Returns the list of names removed.
    """
    out_dir = Path(out_dir)
    if skip:
        if verbose:
            print(f"{tag} prune skipped (run limited to a subset)")
        return []
    if not out_dir.is_dir():
        return []

    suffix = pattern[pattern.rfind("."):] if "." in pattern else ""
    keep_names = set()
    for k in keep:
        k = str(k)
        keep_names.add(k if k.endswith(suffix) else k + suffix)
    keep_names.update(also_keep)

    removed, blocked = [], []
    for path in sorted(out_dir.glob(pattern)):
        if path.name in keep_names:
            continue
        try:
            path.unlink()
            removed.append(path.name)
        except OSError:
            # A checkout inside OneDrive refuses deletes without permission.
            # Report and carry on: CI runs on a plain filesystem and prunes for
            # real, so the committed output still ends up correct.
            blocked.append(path.name)

    if verbose and removed:
        print(f"{tag} pruned {len(removed)} stale file(s): "
              + ", ".join(removed[:10])
              + (f" … and {len(removed)-10} more" if len(removed) > 10 else ""))
    if verbose and blocked:
        print(f"{tag} [WARN] could not delete {len(blocked)} stale file(s) "
              f"(no delete permission here): " + ", ".join(blocked[:10]))
    return removed


def mirror_dir(src_dir, dst_dir, pattern: str = "*.json", tag: str = "[mirror]") -> None:
    """Copy src_dir over dst_dir, removing anything that has left the source.

    shutil.copytree(dirs_exist_ok=True) overlays: a file deleted from the source
    survives in the mirror indefinitely. dist/meat/ and dist/pts/meat/ had
    already drifted apart that way (39 files against 31).
    """
    import shutil

    src_dir, dst_dir = Path(src_dir), Path(dst_dir)
    if not src_dir.is_dir():
        return
    dst_dir.mkdir(parents=True, exist_ok=True)

    names = {p.name for p in src_dir.glob(pattern)}
    for p in src_dir.glob(pattern):
        shutil.copy2(p, dst_dir / p.name)
    prune_outputs(dst_dir, names, pattern=pattern, tag=tag, also_keep=())
