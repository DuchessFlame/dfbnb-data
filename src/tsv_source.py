#!/usr/bin/env python3
r"""
tsv_source.py — the one place that decides which xEdit export a build reads.

WHY THIS EXISTS
===============
Exports are named by month: ``ACTI_Export_July_2026_ACTI.tsv``. Sorting those
alphabetically does NOT sort them chronologically:

    August  <  December  <  July  <  June  <  March  <  May

"May" wins against every other month. So every ``sorted(glob(...))[-1]`` in this
repo silently resolved to **May 2026**, no matter what was committed. Measured
across the twelve main record types in August 2026, twelve of twelve resolved to
the wrong file — including ALCH, where a fresh August export was passed over for
one three months older.

The second flavour of the same bug is ``key=os.path.getmtime``. That works on a
dev box and is meaningless in CI: ``actions/checkout`` stamps every file with the
checkout time, so "newest by mtime" resolves to whatever order the filesystem
felt like. The build that actually publishes is the one where it doesn't work.

Before this module there were 22 separate private copies of a date-key helper,
no two guaranteed identical, none of which understood PTS filenames — both
``_filename_date_key`` and ``_tsv_date_key`` scored every ``*_PTS_2026-08-22_*``
file as ``(0, 0)``, i.e. "undated, sort last".

WHAT IT GUARANTEES
==================
1. Chronological, never lexical, never mtime — so local and CI always agree.
2. Understands both naming schemes:
       ACTI_Export_July_2026_ACTI.tsv              -> 2026-07-01
       ACTI_Export_PTS_2026-08-22_0925_ACTI.tsv    -> 2026-08-22 09:25
3. Channel-explicit. ``live`` reads ``tsv/``; ``pts`` reads ``tsv/pts/``. Never
   both. Silently crossing channels is what put PTS grave placements on a live
   page for five weeks.
4. Records every resolution, so a builder can stamp *what it observed* into its
   output instead of a meaningless build timestamp.
5. Raises when a required pattern matches nothing, instead of returning None and
   letting a builder publish an empty page over a good one.

TIE-BREAK
---------
Among files sharing the newest date, the **base** record file wins over its
companions. ``ALCH_Export_*.tsv`` resolves to ``ALCH_Export_August_2026.tsv``,
not ``..._Effects.tsv``. Ask for a companion by naming it:
``ALCH_Export_*_Effects.tsv``.

This reverses the original migration rule, which kept the lexically-last file so
that fixing the *date* would not also change the *shape*. That was the right call
for a behaviour-neutral migration and the wrong one to leave standing: a bare
pattern silently returned a file with a different schema. ``_Effects`` carries no
FULL and no Keywords_Flat and repeats a record once per magic effect, so every
caller asking for "the ALCH export" got nameless, untradeable-blind, duplicated
rows — which is exactly how the bobblehead pages shipped 42 unnamed entries.
See STALE-DATA-DIAGNOSIS.md §4A.

USAGE
-----
    import tsv_source

    path  = tsv_source.newest("ALCH_Export_*.tsv")             # raises if absent
    maybe = tsv_source.newest("OMOD_Export_*.tsv", required=False)
    every = tsv_source.all_matching("CHAL_Export_*.tsv")        # oldest -> newest
    book  = tsv_source.newest("BOOK_Export_*.tsv", exclude="Locations")

    tsv_source.newest("ACTI_Export_*.tsv", channel="pts")       # tsv/pts/ only

Patterns without a directory component resolve under the channel's TSV root.
Patterns that contain a path separator are used as given.

LINT
----
    python src/tsv_source.py --lint

Fails if any lexical or mtime-based export selection has reappeared anywhere in
src/ or tools/. The resolver makes the build correct; the lint makes it stay
correct. Wired into dfbnb-patch-build.yml.
"""

from __future__ import annotations

import datetime
import glob as _glob
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

LIVE_ROOT = os.path.join(REPO, "tsv")
PTS_ROOT = os.path.join(REPO, "tsv", "pts")

MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

# ACTI_Export_PTS_2026-08-22_0925_ACTI.tsv
_PTS_RE = re.compile(r"_PTS_(\d{4})-(\d{2})-(\d{2})(?:[_-](\d{3,6}))?", re.I)
# ACTI_Export_July_2026_ACTI.tsv   (also Dec_2025, Sept_2026, ...)
_MONTH_RE = re.compile(r"_([A-Za-z]{3,9})_(\d{4})(?:\D|$)")

_UNDATED = (datetime.date.min, 0)

# Everything this process resolved: {abs_path: pattern}. Read it to stamp
# provenance into a build's output, or to report what a run actually consumed.
_RESOLVED: dict[str, str] = {}


class NoExportFound(FileNotFoundError):
    """A required export pattern matched nothing for the requested channel."""


def export_key(path) -> tuple:
    """(date, time) parsed from an export filename. Undated files sort oldest.

    Never touches the filesystem — mtime is not part of the answer, because a
    fresh git checkout gives every file the same one.
    """
    name = os.path.basename(path)
    m = _PTS_RE.search(name)
    if m:
        hhmm = int((m.group(4) or "0").zfill(4)[:4] or 0)
        return (datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3))), hhmm)
    m = _MONTH_RE.search(name)
    if m and m.group(1).lower() in MONTHS:
        return (datetime.date(int(m.group(2)), MONTHS[m.group(1).lower()], 1), 0)
    return _UNDATED


def export_date(path) -> datetime.date:
    """Just the date part of export_key(); date.min when the name carries none."""
    return export_key(path)[0]


def _root(channel: str) -> str:
    ch = (channel or "live").strip().lower()
    if ch == "pts":
        return PTS_ROOT
    if ch == "live":
        return LIVE_ROOT
    raise ValueError(f"unknown channel {channel!r} — expected 'live' or 'pts'")


def _expand(pattern: str, channel: str):
    """(full_glob, is_bare). A bare pattern resolves under the channel root; a
    pattern that already carries a directory is honoured exactly as given, since
    the caller has chosen its own root (several builders point TSV_DIR at
    tsv/pts themselves)."""
    if os.path.isabs(pattern) or os.sep in pattern or "/" in pattern:
        return pattern, False
    return os.path.join(_root(channel), pattern), True


def _is_companion(path, hits) -> bool:
    """True when `path` is a companion of another file in the same result set.

    A companion is a same-date sibling whose name is a strict extension of a
    shorter match: BOOK_Export_July_2026_Locations.tsv beside
    BOOK_Export_July_2026.tsv, ALCH..._Effects beside ALCH..., NPC..._Refs
    beside NPC..., OMOD..._Properties beside OMOD....

    Derived from the files actually present rather than a hard-coded suffix list,
    so a new companion suffix is handled the day it first appears — the failure
    mode here was always a NEW file quietly outranking the one callers meant.
    """
    base = os.path.basename(path)
    stem = base[:-4] if base.lower().endswith(".tsv") else base
    key = export_key(path)
    for other in hits:
        ob = os.path.basename(other)
        if ob == base or export_key(other) != key:
            continue
        ostem = ob[:-4] if ob.lower().endswith(".tsv") else ob
        if len(ostem) < len(stem) and stem.startswith(ostem):
            return True
    return False


def all_matching(pattern: str, *, channel: str = "live", exclude=None) -> list[str]:
    """Every file matching `pattern`, sorted OLDEST -> NEWEST.

    `exclude` is a substring (or iterable of substrings) filtered out of the
    basename — for patterns like BOOK_Export_*.tsv that also catch _Locations.
    """
    full, is_bare = _expand(pattern, channel)
    hits = _glob.glob(full)

    # A bare live pattern must not reach into tsv/pts/. Cross-channel fallback is
    # exactly the bug that published PTS placements on a live page. An explicit
    # path is left alone — the caller already picked its root.
    if is_bare and _root(channel) == LIVE_ROOT:
        hits = [h for h in hits
                if os.path.basename(os.path.dirname(os.path.abspath(h))) != "pts"]

    if exclude:
        terms = [exclude] if isinstance(exclude, str) else list(exclude)
        hits = [h for h in hits
                if not any(t.lower() in os.path.basename(h).lower() for t in terms)]

    # newest() takes the LAST element, so the preferred file must sort last:
    # base variants rank above their same-date companions.
    return sorted(hits, key=lambda p: (export_key(p),
                                       not _is_companion(p, hits),
                                       os.path.basename(p)))


def newest(pattern: str, *, channel: str = "live", exclude=None, required: bool = True):
    """The chronologically newest export matching `pattern`, or None/raise.

    Set required=False for genuinely optional inputs. Leave it True for anything
    a page depends on — a builder that quietly writes an empty page over a good
    one is the failure mode this whole module exists to stop.
    """
    hits = all_matching(pattern, channel=channel, exclude=exclude)
    if not hits:
        if not required:
            return None
        raise NoExportFound(
            f"No export matches {pattern!r} on the {channel} channel "
            f"(looked in {_root(channel)}). Export this record type, or pass "
            f"required=False if the build can genuinely proceed without it."
        )
    chosen = hits[-1]
    _RESOLVED[os.path.abspath(chosen)] = pattern
    return chosen


def newest_pair(pattern: str, *, channel: str = "live", exclude=None):
    """(newest, previous) — for the latest-vs-previous diffs. previous may be None."""
    hits = all_matching(pattern, channel=channel, exclude=exclude)
    if not hits:
        return None, None
    for h in hits[-2:]:
        _RESOLVED[os.path.abspath(h)] = pattern
    return hits[-1], (hits[-2] if len(hits) > 1 else None)


def resolved() -> dict:
    """{abs_path: pattern} for everything resolved so far in this process."""
    return dict(_RESOLVED)


def observed() -> str:
    """Oldest export date among everything resolved, as YYYY-MM-DD.

    A dataset is only as fresh as its stalest input, so the OLDEST wins. This is
    what belongs in a build's `_meta.observed` — `generated` is the build clock
    and says nothing about whether the data is current.
    """
    dates = [export_date(p) for p in _RESOLVED]
    dates = [d for d in dates if d != datetime.date.min]
    return min(dates).isoformat() if dates else "unknown"


def channel_of(argv=None) -> str:
    """'pts' when --pts is on the command line or DFBNB_CHANNEL=pts, else 'live'."""
    argv = sys.argv if argv is None else argv
    if "--pts" in argv:
        return "pts"
    return "pts" if os.environ.get("DFBNB_CHANNEL", "").strip().lower() == "pts" else "live"


def derived(name: str, channel: str = "live") -> str:
    """Path for a TSV we generate ourselves (not a raw xEdit export), per channel.

    LIVE -> tsv/<name>, PTS -> tsv/pts/<name>.

    These files are second-generation data: build_phantom_grave_sites_tsv.py turns a
    REFR export into resolved grave rows, and so on. Writing them to a single
    channel-blind path means a PTS run silently overwrites the live file — the exact
    shape of the bug that put PTS grave placements on a live page. The generator and
    the consumer both go through here so they cannot disagree about where the file is.
    """
    return os.path.join(_root(channel), name)


def derived_read(name: str, channel: str = "live") -> str:
    """Where to READ a derived TSV from, falling back to live if PTS has none.

    A fallback is safe in this direction: PTS pages showing live-derived data are
    merely behind, whereas live pages showing PTS data are wrong.
    """
    p = derived(name, channel)
    if os.path.exists(p):
        return p
    return derived(name, "live")


# ---------------------------------------------------------------------------
# Export ledger — the record that an export actually happened
# ---------------------------------------------------------------------------
#
# tsv/_exports.tsv is appended by every xEdit export script:
#
#     channel  record_type  exported_at       file
#     LIVE     CHAL         2026-08-22 19:40  CHAL_Export_August_2026.tsv
#
# It exists because re-exporting a record type that did not change produces a
# byte-identical file, so git records nothing. Without this, "exported last week,
# unchanged" and "not exported since February" are the same thing on disk — which
# is why a wrong page could not be told apart from a correct old one.
#
# Rows whose file is missing are ignored, so a cancelled or crashed export that
# wrote a ledger line but no TSV corrects itself.

LEDGER = os.path.join(LIVE_ROOT, "_exports.tsv")
PATCHES_TSV = os.path.join(LIVE_ROOT, "fallout76_patches.tsv")

_ledger_cache = None
_patches_cache = None


def ledger(refresh: bool = False) -> list:
    """[{channel, record_type, exported_at (date), file, path}], oldest first."""
    global _ledger_cache
    if _ledger_cache is not None and not refresh:
        return _ledger_cache
    rows = []
    if os.path.exists(LEDGER):
        with open(LEDGER, encoding="utf-8", errors="replace") as f:
            head = f.readline().rstrip("\n").split("\t")
            idx = {h.strip(): i for i, h in enumerate(head)}
            for line in f:
                if not line.strip():
                    continue
                c = line.rstrip("\n").split("\t")

                def cell(name):
                    i = idx.get(name)
                    return c[i].strip() if i is not None and i < len(c) else ""

                fn = cell("file")
                if not fn:
                    continue
                chan = (cell("channel") or "live").lower()
                path = os.path.join(_root("pts" if chan == "pts" else "live"), fn)
                if not os.path.exists(path):
                    continue          # export was cancelled or never landed
                raw = cell("exported_at")[:10]
                try:
                    d = datetime.date(*(int(x) for x in raw.split("-")))
                except Exception:
                    d = export_date(fn)
                rows.append({"channel": chan, "record_type": cell("record_type"),
                             "exported_at": d, "file": fn, "path": path})
    rows.sort(key=lambda r: (r["exported_at"], r["file"]))
    _ledger_cache = rows
    return rows


def last_verified(record_type: str, channel: str = "live", pattern: str = None):
    """When this record type was last EXPORTED (looked at), not last changed.

    Falls back to the newest export's filename date when the ledger has no row —
    which is the case for everything exported before the ledger existed. Pass
    `pattern` for record types whose files don't follow <RT>_Export_* (MESG ships
    as MESG_Help_Export_*, vendors as NPC2_Vendors_*).
    """
    rows = [r for r in ledger()
            if r["record_type"].upper() == record_type.upper()
            and r["channel"] == (channel or "live").lower()]
    if rows:
        return rows[-1]["exported_at"]
    hits = []
    for p in ([pattern] if pattern else []) + [f"{record_type}_Export_*.tsv",
                                               f"{record_type}_Placements_*.tsv"]:
        hits = all_matching(p, channel=channel)
        if hits:
            break
    return export_date(hits[-1]) if hits else None


def patches(refresh: bool = False) -> list:
    """[{version, name, release_date}] from tsv/fallout76_patches.tsv, oldest first."""
    global _patches_cache
    if _patches_cache is not None and not refresh:
        return _patches_cache
    out = []
    if os.path.exists(PATCHES_TSV):
        with open(PATCHES_TSV, encoding="utf-8", errors="replace") as f:
            head = f.readline().rstrip("\n").split("\t")
            idx = {h.strip(): i for i, h in enumerate(head)}
            for line in f:
                c = line.rstrip("\n").split("\t")

                def cell(name):
                    i = idx.get(name)
                    return c[i].strip() if i is not None and i < len(c) else ""

                raw = cell("release_date")
                try:
                    d = datetime.date(*(int(x) for x in raw.split("-")))
                except Exception:
                    continue
                out.append({"version": cell("version"), "name": cell("name"),
                            "type": cell("type"), "release_date": d})
    out.sort(key=lambda p: p["release_date"])
    _patches_cache = out
    return out


def patch_at(d):
    """The patch the game was on for a given date — newest released on or before it.

    Context only. Entries are keyed on the EXPORT, not the patch, because
    hotfixes land between patches and an export can straddle them.
    """
    if not d:
        return None
    prior = [p for p in patches() if p["release_date"] <= d]
    return prior[-1] if prior else None


def current_patch():
    """The newest patch we know about — what the game should be on right now."""
    ps = patches()
    return ps[-1] if ps else None


def patches_behind(d):
    """How many patches shipped since this data was verified. None = never verified."""
    if not d:
        return None
    return sum(1 for p in patches() if p["release_date"] > d)


def provenance(record_type: str, channel: str = "live", pattern: str = None) -> dict:
    """The freshness block a page shows: when it was last checked, against what.

    `generated` (build time) says nothing about whether data is current — this does.
    """
    d = last_verified(record_type, channel, pattern)
    at = patch_at(d)
    cur = current_patch()
    return {
        "record_type": record_type.upper(),
        "channel": (channel or "live").lower(),
        "verified_date": d.isoformat() if d else None,
        "verified_game_version": at["version"] if at else None,
        "verified_patch_name": at["name"] if at else None,
        "current_game_version": cur["version"] if cur else None,
        "current_patch_date": cur["release_date"].isoformat() if cur else None,
        "patches_behind": patches_behind(d),
    }


# ---------------------------------------------------------------------------
# Lint — stop the bug coming back
# ---------------------------------------------------------------------------

_BAD = [
    (re.compile(r"sorted\((?:[^()]|\([^()]*\))*?\.glob\((?:[^()]|\([^()]*\))*?\)\s*\)"
                r"(?!\s*,\s*key)"),
     "lexical sort of an export glob — 'August' sorts before 'July'"),
    (re.compile(r"key\s*=\s*(?:lambda\s+\w+\s*:\s*)?os\.path\.getmtime"),
     "mtime sort — every file shares the checkout timestamp in CI"),
    (re.compile(r"key\s*=\s*os\.path\.getmtime"),
     "mtime sort — every file shares the checkout timestamp in CI"),
]

# A key= that is still lexical. Adding key=lambda p: p.name to a bad sort used to
# SILENCE this lint while changing nothing — that is how build_collectables_json
# read May 2026 for three months with the lint green. Sorting by name, str, or
# basename is the exact bug this module exists to stop.
_LEXICAL_KEY = re.compile(
    r"key\s*=\s*(?:lambda\s+\w+\s*:\s*)?"
    r"(?:\w+\.name\b|str\b|os\.path\.basename|\w+\.stem\b)"
)

# sorted()/max()/min() over a list of export paths.
_PICK = re.compile(r"\b(?:sorted|max|min)\s*\(\s*([A-Za-z_]\w*)\b")

# name = ...glob('..._Export...')  /  name = [p for p in other if ...]
_GLOB_ASSIGN = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*=\s*.*\.glob\(\s*[\"'][^\"']*"
    r"(?:_Export|_Placements|_EXPORT)[^\"']*[\"']"
)
_DERIVED_ASSIGN = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*=\s*[\[\(].*\bfor\s+\w+\s+in\s+([A-Za-z_]\w*)\b"
)

_LINT_SKIP = {"tsv_source.py", "normalize_pts_tsv.py"}


def _export_list_vars(lines):
    """Variables in this file that hold a list of export paths.

    Selection is often split across statements — the glob lands in a variable on
    one line and the sort that picks from it is forty lines further down. The old
    two-line window could not see that far, so the whole shape was invisible.
    Derived lists (filtering _Locations out of a BOOK glob, say) stay tainted.
    """
    tainted = set()
    for _ in range(3):          # let derivations chain a few levels
        before = len(tainted)
        for line in lines:
            m = _GLOB_ASSIGN.match(line)
            if m:
                tainted.add(m.group(1))
                continue
            m = _DERIVED_ASSIGN.match(line)
            if m and m.group(2) in tainted:
                tainted.add(m.group(1))
        if len(tainted) == before:
            break
    return tainted


def lint(roots=("src", "tools")) -> int:
    """Report every export-selection site that bypasses this module."""
    problems = []
    for root in roots:
        base = os.path.join(REPO, root)
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]
            for fn in filenames:
                if not fn.endswith(".py") or fn in _LINT_SKIP:
                    continue
                path = os.path.join(dirpath, fn)
                try:
                    lines = open(path, encoding="utf-8", errors="replace").read().split("\n")
                except OSError:
                    continue
                rel = os.path.relpath(path, REPO).replace("\\", "/")
                export_vars = _export_list_vars(lines)

                for i, line in enumerate(lines, 1):
                    window = line + (" " + lines[i].strip() if i < len(lines) else "")
                    if "tsv_source" in window:
                        continue

                    # (a) selection split across statements: the glob is in a
                    # variable defined elsewhere in the file, and this line picks
                    # from it. Any key= that is not a real date key is a bug.
                    m = _PICK.search(line)
                    if m and m.group(1) in export_vars:
                        if "getmtime" in window:
                            why = "mtime sort — every file shares the checkout timestamp in CI"
                        elif _LEXICAL_KEY.search(window):
                            why = ("lexical key= on an export list — sorting by name is "
                                   "still 'August' before 'July'")
                        elif not re.search(r"key\s*=", window):
                            why = "lexical sort of an export list — 'August' sorts before 'July'"
                        else:
                            continue        # a real key= — assume date-aware
                        problems.append(f"{rel}:{i}  {why}\n      {line.strip()[:100]}")
                        continue

                    # (b) the original inline shape, plus lexical key= on it
                    if "glob" not in line.lower() and "getmtime" not in line:
                        continue
                    # Only export selection is in scope. Iterating every *.tsv to
                    # convert it (build_all.py) is not picking a newest anything.
                    if not re.search(r"_Export_|_Placements_|_EXPORT_", window):
                        continue
                    hit = next((why for rx, why in _BAD if rx.search(window)), None)
                    if hit is None and _PICK.search(window) and _LEXICAL_KEY.search(window):
                        hit = ("lexical key= on an export glob — sorting by name is "
                               "still 'August' before 'July'")
                    if hit:
                        problems.append(f"{rel}:{i}  {hit}\n      {line.strip()[:100]}")
    if problems:
        print("tsv_source lint FAILED — export selection must go through "
              "tsv_source.newest():\n")
        for p in problems:
            print("  " + p)
        print(f"\n{len(problems)} site(s). Replace each with tsv_source.newest(pattern).")
        return 1
    print("tsv_source lint OK — every export selection goes through tsv_source.")
    return 0


def lint_channels(verbose: bool = True) -> int:
    """Report LIVE exports that are byte-identical to a PTS export.

    Why this exists
    ---------------
    In June 2026 an entire 31-file PTS sweep (2026-06-27) was copied into tsv/
    and filed as the live June sweep. Every live page built from a June export
    was therefore rendering PTS content, and the patch log showed 88 records
    added in June and removed again in July — content that never shipped to
    live at all. It is the same failure as the graves page, committed at the
    filing step instead of in code, and no amount of correct selection logic
    can see it: the wrong file was in the right place with the right name.

    The signal is content, not naming. An export copied across channels is
    byte-identical to its source. The discriminator is HOW MANY PTS sweeps a
    live file matches:

      exactly one   -> a copy. That live file is that PTS export.
      more than one -> a record type that simply is not changing between
                       builds, identical across several sweeps by nature.
                       Not evidence of anything.

    Reports rather than fails: the files may be the only copy of a record type
    the live channel has, and deleting them is the author's call, not CI's.
    Returns the number of suspects.
    """
    import hashlib

    def digest(path):
        m = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                m.update(chunk)
        return m.hexdigest()

    pts_by_hash = {}
    for p in _glob.glob(os.path.join(PTS_ROOT, "*.tsv")):
        pts_by_hash.setdefault(digest(p), []).append(p)

    suspects = []
    for p in _glob.glob(os.path.join(LIVE_ROOT, "*.tsv")):
        matches = pts_by_hash.get(digest(p))
        if not matches:
            continue
        # _PTS_RE splits the date into three groups; the sweep identity is all
        # three joined, not group(1) (the year) on its own.
        sweeps = {"-".join(m.group(1, 2, 3)) for m in
                  (_PTS_RE.search(os.path.basename(x)) for x in matches) if m}
        if len(sweeps) == 1:
            suspects.append((p, sorted(matches)[0], sweeps.pop()))

    if not verbose:
        return len(suspects)

    if not suspects:
        print("tsv_source channel lint OK — no live export is a copy of a PTS export.")
        return 0

    by_sweep = {}
    for live_path, pts_path, sweep in suspects:
        by_sweep.setdefault(sweep, []).append((live_path, pts_path))

    print(f"tsv_source channel lint: {len(suspects)} live export(s) are "
          f"byte-identical to exactly one PTS export.\n")
    for sweep in sorted(by_sweep):
        rows = by_sweep[sweep]
        print(f"  PTS sweep {sweep} appears in tsv/ as {len(rows)} live file(s):")
        for live_path, pts_path in sorted(rows):
            newer = [x for x in all_matching(_family_pattern(os.path.basename(live_path)))
                     if export_key(x) > export_key(live_path)]
            state = "superseded" if newer else "STILL THE NEWEST LIVE FILE"
            print(f"    {os.path.basename(live_path):<52} {state}")
        print()
    print("  Each is already present under tsv/pts/ with its real name, so removing\n"
          "  the live copy loses nothing. Anything marked STILL THE NEWEST LIVE FILE\n"
          "  is being served on a live page right now.")
    return len(suspects)


def _family_pattern(basename: str) -> str:
    """ACTI_Export_June_2026_ACTI.tsv -> ACTI_Export_*_ACTI.tsv"""
    return re.sub(
        r"_(January|February|March|Apr|April|May|June|July|August|Sept|September|"
        r"Oct|October|Nov|November|Dec|December)_\d{4}",
        "_*", basename, count=1)


def _report():
    """Show what each pattern resolves to now vs under the old lexical sort."""
    pats = ["ACTI_Export_*_ACTI.tsv", "ALCH_Export_*.tsv", "ARMO_Export_*.tsv",
            "CHAL_Export_*.tsv", "COBJ_Export_*.tsv", "LVLI_Export_*.tsv",
            "WEAP_Export_*.tsv", "OMOD_Export_*.tsv", "BOOK_Export_*.tsv",
            "PERK_Export_*.tsv", "NPC_Export_*.tsv", "QUEST_Export_*.tsv"]
    print(f"{'PATTERN':<28}{'LEXICAL (old)':<42}{'CHRONOLOGICAL (now)':<42}")
    for p in pats:
        hits = all_matching(p)
        if not hits:
            continue
        old = os.path.basename(sorted(hits)[-1])
        new = os.path.basename(hits[-1])
        flag = "" if old == new else "   FIXED"
        print(f"{p:<28}{old:<42}{new:<42}{flag}")


if __name__ == "__main__":
    if "--lint" in sys.argv:
        rc = lint()
        # Reported after the selection lint and never fatal on its own: a PTS
        # file mis-filed as live is the author's to remove, not CI's, and a red
        # build nobody can fix from CI is a build everyone learns to ignore.
        print()
        lint_channels()
        raise SystemExit(rc)
    if "--channels" in sys.argv:
        raise SystemExit(0 if lint_channels() == 0 else 0)
    _report()
