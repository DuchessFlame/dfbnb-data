#!/usr/bin/env python3
"""
backfill_page_patchlog.py — rebuild per-page patch log history from the exports
already sitting in tsv/ and tsv/pts/.

Every monthly live export and every timestamped PTS pull is still in the repo, so
history does not have to be dug out of old commits (whose builders were different
code and would report code changes as game changes). Instead, for each cut-off
date this:

  1. makes a throw-away repo tree whose tsv/ only holds exports dated <= cut-off
     (PTS: live exports + the newest PTS pull <= cut-off, normalised exactly as
     the PTS workflow does),
  2. runs TODAY's builders over it,
  3. reduces every page to facts with page_patchlog,

then diffs consecutive cut-offs. Because the builder code is identical for every
snapshot, every difference is a difference in the game data.

Writes <dist>/patchlogs/pages/... and <dist>/patchlogs/_state/... — the same
files the CI step maintains — so CI carries on from where this leaves off.

  python tools/backfill_page_patchlog.py --channel live
  python tools/backfill_page_patchlog.py --channel pts
  python tools/backfill_page_patchlog.py --channel live --family events --keep-trees
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
import page_patchlog as P   # noqa: E402
import tsv_source          # noqa: E402

BUILDERS = {                      # family -> builder script (run in the temp tree)
    "events": "src/build_events_rewards_json.py",
    "activities": "src/build_activities_rewards_json.py",
}

# Baseline is April 2026: the first FULL LVLI export. Diffing March (a partial
# LVLI export) against April reports the exporter growing, not the game changing.
LIVE_CUTOFFS = ["2026-04-30", "2026-05-31", "2026-06-30",
                "2026-07-31", "2026-08-31", "2026-09-30"]


def pts_cutoffs(gap_days=2):
    """One cut-off per PTS sweep. Pulls within `gap_days` of each other are one sweep."""
    dates = sorted({tsv_source.export_date(p) for p in (REPO / "tsv" / "pts").glob("*.tsv")}
                   - {dt.date.min})
    groups, cur = [], []
    for d in dates:
        if cur and (d - cur[-1]).days > gap_days:
            groups.append(cur)
            cur = []
        cur.append(d)
    if cur:
        groups.append(cur)
    return [g[-1] for g in groups]


def make_tree(root: Path, cutoff: dt.date, channel: str) -> Path:
    if root.exists():
        shutil.rmtree(root)
    (root / "tsv").mkdir(parents=True)
    (root / "dist").mkdir()
    shutil.copytree(REPO / "src", root / "src")
    os.symlink(REPO / "data", root / "data")
    os.symlink(REPO / "tools", root / "tools")

    live_cut = cutoff
    for f in (REPO / "tsv").iterdir():
        if f.is_dir():
            continue
        d = tsv_source.export_date(f)
        if "_Export_" in f.name and d != dt.date.min and d > live_cut:
            continue
        os.symlink(f, root / "tsv" / f.name)

    if channel == "pts":
        src = root / "_pts_src"
        norm = root / "_pts_norm"
        src.mkdir()
        for f in (REPO / "tsv" / "pts").glob("*.tsv"):
            if tsv_source.export_date(f) <= cutoff:
                os.symlink(f, src / f.name)
        subprocess.run([sys.executable, "src/normalize_pts_tsv.py", "--src", str(src),
                        "--dst", str(norm)], cwd=root, check=True,
                       stdout=subprocess.DEVNULL)
        subprocess.run([sys.executable, "tools/repair_export_quote_splits.py",
                        "--path", "_pts_norm"], cwd=root, check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for f in norm.iterdir():
            dst = root / "tsv" / f.name
            if dst.is_symlink() or dst.exists():
                dst.unlink()                 # never write through a symlink into tsv/
            shutil.move(str(f), dst)
    return root


def cached_snapshot(cache: Path, key: str, cutoff, channel, families, work, keep) -> dict:
    """snapshot() with an on-disk cache of each cut-off's built page JSON. A re-run
    (say after a facts() tweak) recomputes facts from the cache instead of
    re-running the builders."""
    cdir = cache / key
    if cdir.is_dir() and (cdir / "_done").exists():
        return _facts_from_dist(cdir / "dist", cdir / "sig.json", families)
    t = make_tree(work / key, cutoff, channel)
    for fam in families:
        _build(t, fam)
    snap = _facts_from_dist(t / "dist", None, families, tsv_dir=t / "tsv")
    cdir.mkdir(parents=True, exist_ok=True)
    for fam in families:
        rel = Path(P.FAMILIES[fam]["slices"])
        dst = cdir / "dist" / rel
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(t / "dist" / rel, dst)
    json.dump({f: snap[f]["sig"] for f in families}, open(cdir / "sig.json", "w"))
    (cdir / "_done").write_text("ok")
    if not keep:
        shutil.rmtree(t)
    return snap


def _build(tree: Path, fam: str) -> None:
    r = subprocess.run([sys.executable, BUILDERS[fam]], cwd=tree,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{BUILDERS[fam]} failed in {tree}:\n{r.stdout[-2000:]}")


def _facts_from_dist(dist: Path, sig_file, families, tsv_dir=None) -> dict:
    sigs = json.load(open(sig_file)) if sig_file else {}
    out = {}
    for fam in families:
        F = P.FAMILIES[fam]
        if tsv_dir is not None:
            P._HASH_CACHE.clear()
            sig = P.signature(F, tsv_root=str(tsv_dir))
        else:
            sig = sigs[fam]
        pages = {u: (s, F["facts"](pg)) for u, s, pg in P.iter_pages(dist, F)}
        out[fam] = {"sig": sig, "pages": pages}
    return out


def snapshot(tree: Path, families) -> dict:
    """{family: {"sig": {...}, "pages": {url: (slug, facts)}}} for one tree."""
    for fam in families:
        r = subprocess.run([sys.executable, BUILDERS[fam]], cwd=tree,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"{BUILDERS[fam]} failed in {tree}:\n{r.stdout[-2000:]}")
    out = {}
    for fam in families:
        F = P.FAMILIES[fam]
        P._HASH_CACHE.clear()
        sig = P.signature(F, tsv_root=str(tree / "tsv"))
        pages = {u: (s, F["facts"](pg)) for u, s, pg in P.iter_pages(tree / "dist", F)}
        out[fam] = {"sig": sig, "pages": pages}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", choices=("live", "pts"), default="live")
    ap.add_argument("--family", nargs="*", default=list(BUILDERS))
    ap.add_argument("--dist", default=None, help="default: dist (live) / dist/pts (pts)")
    ap.add_argument("--work", default=None, help="scratch dir for temp trees")
    ap.add_argument("--keep-trees", action="store_true")
    ap.add_argument("--cache", default=None,
                    help="keep each cut-off's built page JSON here; re-runs reuse it")
    a = ap.parse_args()

    dist = Path(a.dist) if a.dist else REPO / ("dist/pts" if a.channel == "pts" else "dist")
    work = Path(a.work or tempfile.mkdtemp(prefix="pplog_"))
    fams = a.family

    snaps = []                    # [(date, snapshot)]
    if a.channel == "live":
        cuts = [dt.date.fromisoformat(c) for c in LIVE_CUTOFFS]
        cuts = [c for c in cuts if c <= dt.date.today() + dt.timedelta(days=31)]
    else:
        # Baseline = live as it stood just before the first PTS sweep, so the first
        # PTS entry reads as "what this PTS changes versus live".
        cuts = pts_cutoffs()

    cache = Path(a.cache) if a.cache else work / "_cache"
    tag = "-".join(sorted(fams))

    if a.channel == "pts":
        base_cut = cuts[0] - dt.timedelta(days=1)
        print(f"[backfill] pts baseline = live @ {base_cut}", file=sys.stderr)
        snaps.append((None, cached_snapshot(cache, f"live_{base_cut}_{tag}", base_cut,
                                            "live", fams, work, a.keep_trees)))

    for c in cuts:
        print(f"[backfill] {a.channel} @ {c}", file=sys.stderr)
        try:
            snaps.append((c, cached_snapshot(cache, f"{a.channel}_{c}_{tag}", c,
                                             a.channel, fams, work, a.keep_trees)))
        except RuntimeError as e:
            print(f"[backfill]   skipped: {e}", file=sys.stderr)

    for fam in fams:
        feeds = {}                       # url -> (slug, [entries])
        prev = None
        for when, snap in snaps:
            cur = snap[fam]
            if prev is not None:
                changed = [p for p, v in cur["sig"].items()
                           if (prev["sig"].get(p) or {}).get("sha") != v["sha"]]
                if changed or a.channel == "pts":
                    # A PTS sweep's date is its own; a live month is the newest
                    # changed export's month.
                    if a.channel == "pts":
                        stamp = when
                    else:
                        ds = [dt.date.fromisoformat(cur["sig"][p]["date"])
                              for p in changed if cur["sig"][p]["date"]]
                        stamp = max(ds) if ds else when
                    at = tsv_source.patch_at(stamp) if a.channel != "pts" else None
                    for url, (slug, facts) in cur["pages"].items():
                        pf = prev["pages"].get(url)
                        if pf is None:
                            continue
                        d = P.diff_facts(pf[1], facts)
                        if not (d["added"] or d["removed"] or d["changed"] or d["summary"]):
                            continue
                        feeds.setdefault(url, (slug, []))[1].insert(0, {
                            "ts": stamp.isoformat(),
                            "label": P.entry_label(stamp, a.channel),
                            "channel": a.channel,
                            "game_version": at["version"] if at else None,
                            "patch_name": at["name"] if at else None,
                            "sources": sorted(cur["sig"][p]["file"] for p in changed),
                            **d,
                        })
            prev = cur

        # Write feeds + state. State = the LAST snapshot, so the next CI run
        # compares against exactly what this backfill ended on.
        F = P.FAMILIES[fam]
        prov = tsv_source.provenance(F["primary"], a.channel)
        out_dir = dist / "patchlogs" / "pages" / fam
        st_dir = dist / "patchlogs" / "_state" / fam
        index = {}
        n_entries = 0
        for url, (slug, facts) in prev["pages"].items():
            entries = feeds.get(url, (slug, []))[1][:P.MAX_ENTRIES]
            n_entries += len(entries)
            P._write(out_dir / f"{slug}.json", {"page": url, "family": fam,
                                                "channel": a.channel, "provenance": prov,
                                                "entries": entries})
            P._write(st_dir / f"{slug}.json", {"page": url, "facts": facts}, compact=True)
            index[url] = f"{fam}/{slug}.json"
        P._write(st_dir / "_signature.json", {"inputs": prev["sig"],
                                              "updated": dt.date.today().isoformat(),
                                              "backfilled": True})
        idx_path = dist / "patchlogs" / "pages" / "_index.json"
        idx = P._load(idx_path, git_fallback=False) or {}
        pages = {k: v for k, v in (idx.get("pages") or {}).items() if not v.startswith(f"{fam}/")}
        pages.update(index)
        P._write(idx_path, {"pages": dict(sorted(pages.items()))})
        print(f"[backfill] {fam} ({a.channel}): {len(index)} pages, {n_entries} entries "
              f"across {len(feeds)} pages", file=sys.stderr)

    if not a.keep_trees and not a.cache:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
