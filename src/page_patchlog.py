#!/usr/bin/env python3
"""
page_patchlog.py — one PATCH LOG per page, built from what the page actually shows.

WHY (2026-09-10)
================
The old engine (patchlog_utils.FEED_SPECS) wrote 44 feeds for 1,268 pages. Every
page in a category shared one log, and each log diffed a WHOLE record type across
the game: the Activities log counted all 2,206 QUEST records and its newest
"change" was "Hellos to NPCs in combat — Type: — → None". It only watched
Name / Level / Value, so a real change such as a boss's health or a drop rate
never appeared at all.

The model here:

  * ONE LOG PER PAGE. Each page's log lives at
        <dist>/patchlogs/pages/<family>/<slug>.json
  * IT DIFFS WHAT THE PAGE SHOWS. After the builders run, each page's built JSON
    is reduced to a flat set of "facts" (XP, caps, every reward and its drop rate,
    pool chances, location...) and compared with the facts stored the last time
    a NEW TSV came in.
  * ONE ENTRY PER TSV, NEVER PER BUILD. The inputs a family reads are fingerprinted
    (content hash of the exact export each pattern resolves to). Same fingerprint
    = no new export = no entry. If the facts moved anyway, that was a code change,
    not a game change, so the snapshot is refreshed silently.
  * CHANNELS ARE SEPARATE. Live writes dist/patchlogs/, PTS writes
    dist/pts/patchlogs/. The front end's PTS toggle already redirects dist/ ->
    dist/pts/, so PTS changes only ever show in PTS mode.

Adding a category = add a FAMILIES row + a facts() function. Nothing else.

USAGE
-----
  python src/page_patchlog.py --dist dist --channel live
  python src/page_patchlog.py --dist dist/pts --channel pts          # after relocate
  python src/page_patchlog.py --dist dist --family events --dry-run  # print, write nothing

Backfill (re-runs today's builders over older TSV sets): tools/backfill_page_patchlog.py
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tsv_source  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent

MAX_ENTRIES = 60          # entries kept per page (newest first)
MAX_LIST = 400            # names kept per added/removed/changed list
BULK_MIN = 15             # this many same-ratio changes in one group -> one summary line
RIPPLE_TOL = 0.05         # within 5% of the group's common ratio counts as "ripple"
NEGLIGIBLE = 0.01         # a % that moved < 1% of itself is not reported
MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July",
               "August", "September", "October", "November", "December"]


# ===========================================================================
# Formatting — compare on the DISPLAYED value so float noise can't make a note
# ===========================================================================

def pct(v) -> str:
    """Drop-rate display. 3.99994 -> '4%', 0.022321 -> '0.0223%'."""
    if v is None or v == "":
        return ""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if f >= 1:
        s = f"{f:.2f}"
    elif f >= 0.1:
        s = f"{f:.3f}"
    else:
        s = f"{f:.2g}" if f else "0"
        if "e" in s:
            s = f"{f:.6f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s + "%"


def num(v) -> str:
    if v is None or v == "":
        return ""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v).strip()
    if f.is_integer():
        return f"{int(f):,}"
    return f"{f:,.2f}".rstrip("0").rstrip(".")


def txt(v) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", str(v or ""))).strip()


def conds(lst) -> str:
    if not lst:
        return ""
    if isinstance(lst, (list, tuple)):
        return "; ".join(sorted(txt(c) for c in lst if txt(c)))
    return txt(lst)


def _fid(v) -> str:
    return str(v or "").strip().upper()


def _put(out: dict, key: str, name: str, group: str, fields: dict) -> None:
    """Add one record. Blank fields are dropped so 'missing' and 'blank' agree."""
    f = {k: v for k, v in fields.items() if v not in ("", None)}
    if key in out:                         # identical key twice -> keep the first,
        return                             # never silently merge two rows
    out[key] = {"name": name, "group": group, "fields": f}


# ===========================================================================
# Facts — one function per page family
# ===========================================================================

def facts_reward_page(page: dict) -> dict:
    """Events / Activities 'All Rewards' pages (dist/{events,activities}/by_page)."""
    out: dict = {}
    if not isinstance(page, dict):
        return out

    # --- the event itself -------------------------------------------------
    locs = []
    for rl in page.get("regionLocations") or []:
        loc = re.sub(r"\s*\[[A-Z]{4}:[0-9A-F]{8}\]", "", txt(rl.get("location")))
        locs.append(" – ".join(x for x in (txt(rl.get("region")), loc) if x))
    _put(out, "event", txt(page.get("name")) or "Event", "Event", {
        "Description": txt(page.get("description")),
        "Location": "; ".join(sorted(set(locs))),
    })

    # --- base rewards per tier ---------------------------------------------
    for t in (page.get("baseRewards") or {}).get("tiers") or []:
        tier = txt(t.get("tier")) or "base"
        label = "Base" if tier == "base" else tier.replace("_", " ").title()
        _put(out, f"tier:{tier}", f"{label} tier rewards", "Base rewards", {
            "XP": num(t.get("xp")),
            "Caps": num(t.get("caps")),
            "Legendary rank": num(t.get("legendaryRank")),
            "Titles": ", ".join(sorted(txt(x.get("title")) for x in t.get("titles") or [])),
            "Requirements": conds(t.get("conditions")),
        })

    for fr in page.get("freeRewards") or []:
        lab = txt(fr.get("label"))
        if lab:
            _put(out, f"free:{lab.lower()}", lab, "Base rewards", {"Amount": num(fr.get("value"))})

    for i, cr in enumerate(page.get("conditionalRewards") or []):
        if isinstance(cr, dict):
            lab = txt(cr.get("label") or cr.get("name")) or f"Conditional reward {i + 1}"
            _put(out, f"cond:{lab.lower()}", lab, "Conditional rewards", {
                "Amount": num(cr.get("value")),
                "Requirement": conds(cr.get("conditions") or cr.get("condition")),
            })

    ad = page.get("activityData") or {}
    br = ad.get("baseRewards") or {}
    if br:
        leg = br.get("legendaryItems") or {}
        scrip = br.get("legendaryScrip") or {}
        maps = br.get("uMineItMaps") or {}
        _put(out, "activity:base", "Completion rewards", "Base rewards", {
            "XP": num(br.get("xp")),
            "Caps": num(br.get("caps")),
            "Legendary rank": num(leg.get("rank")),
            "Legendary drop rate": pct(leg.get("dropRate")),
            "Legendary scrip (avg)": num(scrip.get("expectedValue")) if scrip.get("expectedValue") else "",
            "U-Mine-It map chance": pct(maps.get("dropRate")),
        })
        for x in br.get("xpBreakdown") or []:
            lab = txt(x.get("label"))
            if lab:
                _put(out, f"xp:{lab.lower()}", f"{lab} XP", "Base rewards", {"XP": num(x.get("xp"))})

    # --- reward pools --------------------------------------------------------
    for p in page.get("pools") or []:
        lvli = _fid(p.get("lvliFormID"))
        tier = txt(p.get("tier"))
        title = txt(p.get("title")) or txt(p.get("lvliEdid")) or lvli
        pk = f"pool:{lvli}:{tier}"
        _put(out, pk, title, "Reward pools", {
            "Pool chance": pct(p.get("poolChance")),
            "Picks": num(p.get("count")),
            "Requirements": conds(p.get("conditions")),
        })
        for it in p.get("items") or []:
            fid = _fid(it.get("formid"))
            q = it.get("qty") or 1
            nm = txt(it.get("name")) or fid
            if q not in (1, "1"):
                nm = f"{nm} ×{num(q)}"
            _put(out, f"{pk}:{fid}:{q}", nm, title, {"Drop rate": pct(it.get("dropRate"))})

    # --- activity reward lists ------------------------------------------------
    for key, group in (("uniqueEventRewards", "Unique rewards"),
                       ("planRewards", "Plan rewards"),
                       ("chemRewards", "Chem rewards")):
        for it in ad.get(key) or []:
            fid = _fid(it.get("formid"))
            q = it.get("qty") or 1
            kind = txt(it.get("kind"))
            nm = txt(it.get("name")) or fid
            if q not in (1, "1"):
                nm = f"{nm} ×{num(q)}"
            _put(out, f"{key}:{fid}:{q}:{kind}", nm, group, {
                "Drop rate": pct(it.get("dropRate")),
                "Requirements": conds(it.get("conditions")),
            })

    for i, b in enumerate(page.get("banners") or []):
        if isinstance(b, dict):
            nm = txt(b.get("name") or b.get("title") or b.get("label")) or f"Banner {i + 1}"
            _put(out, f"banner:{nm.lower()}", nm, "Banners", {})
    for i, s in enumerate(page.get("scenarios") or []):
        if isinstance(s, dict):
            nm = txt(s.get("name") or s.get("title") or s.get("label")) or f"Scenario {i + 1}"
            _put(out, f"scenario:{nm.lower()}", nm, "Scenarios", {})
    return out


# ===========================================================================
# Families — which pages, from which built JSON, fed by which exports
# ===========================================================================
#
# inputs: the export patterns the family's builder resolves with newest(). Only a
# change in one of THESE starts a new entry. guide_index.tsv is deliberately NOT
# an input: a menu edit is not a game change.

_REWARD_INPUTS = [
    ("QUEST_Export_*.tsv", None), ("GMRW_Export_*.tsv", None),
    ("LVLI_Export_*_LVLI_List.tsv", None), ("LVLI_Export_*_LVLI_Entries.tsv", None),
    ("LVLI_Export_*_LVLI_Math.tsv", None), ("BOOK_Export_*.tsv", "Locations"),
    ("ARMO_Export_*.tsv", ("SLOTS", "ObjectTemplate")), ("GLOB_Export_*.tsv", None),
    ("MISC_Export_*.tsv", None), ("WEAP_Export_*_Base.tsv", None),
    ("ALCH_Export_*.tsv", "_Effects"), ("CURV_Export_*.tsv", ("_POINTS", "CurvePoints")),
    ("PLYT_Export_*.tsv", None), ("CMPT_Export_*.tsv", None), ("COBJ_Export_*.tsv", None),
]

FAMILIES: Dict[str, dict] = {
    "events": {
        "slices": "events/by_page",                 # relative to --dist
        "prefixes": ("/df/public-events/",),
        "primary": "QUEST",
        "inputs": _REWARD_INPUTS,
        "facts": facts_reward_page,
    },
    "activities": {
        "slices": "activities/by_page",
        "prefixes": ("/df/activities/",),
        "primary": "QUEST",
        "inputs": _REWARD_INPUTS,
        "facts": facts_reward_page,
    },
}


# ===========================================================================
# IO helpers
# ===========================================================================

def _load(path: Path, git_fallback: bool = True):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        pass
    except Exception:
        return None
    if not git_fallback:
        return None
    try:
        rel = path.resolve().relative_to(REPO_ROOT).as_posix()
        out = subprocess.check_output(["git", "-C", str(REPO_ROOT), "show", f"HEAD:{rel}"],
                                      stderr=subprocess.DEVNULL, timeout=60)
        return json.loads(out.decode("utf-8"))
    except Exception:
        return None


def _write(path: Path, data, compact=False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        if compact:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        else:
            json.dump(data, f, ensure_ascii=False, indent=1)
        f.write("\n")


def _sha1(path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


_HASH_CACHE: Dict[str, str] = {}


def signature(fam: dict, tsv_root: Optional[str] = None) -> dict:
    """{pattern: {file, sha, date}} for the export each input pattern resolves to.

    Resolves against the working tsv/ tree — in the PTS job that tree has already
    been normalised to the newest PTS pull, which is exactly what the builders saw.
    """
    sig = {}
    for pat, excl in fam["inputs"]:
        full = os.path.join(tsv_root, pat) if tsv_root else pat
        try:
            p = tsv_source.newest(full, exclude=excl, required=False)
        except Exception:
            p = None
        if not p:
            continue
        ap = os.path.abspath(p)
        if ap not in _HASH_CACHE:
            _HASH_CACHE[ap] = _sha1(ap)
        d = tsv_source.export_date(p)
        sig[pat] = {"file": os.path.basename(p), "sha": _HASH_CACHE[ap],
                    "date": d.isoformat() if d and d != _dt.date.min else ""}
    return sig


def _pts_pull_date(changed_patterns) -> Optional[_dt.date]:
    """Normalised PTS names only carry a month; the real pull date is in tsv/pts/."""
    best = None
    for pat in changed_patterns:
        try:
            hits = tsv_source.all_matching(pat.replace("_Export_*", "_Export_PTS_*"), channel="pts")
        except Exception:
            hits = []
        for h in hits[-1:]:
            d = tsv_source.export_date(h)
            if d and d != _dt.date.min and (best is None or d > best):
                best = d
    return best


def entry_label(date: Optional[_dt.date], channel: str) -> str:
    if not date:
        return "PTS update" if channel == "pts" else "Game data update"
    if channel == "pts":
        return f"PTS · {date.day} {MONTH_NAMES[date.month - 1][:3]} {date.year}"
    if date.day == 1:                                   # monthly live export
        return f"{MONTH_NAMES[date.month - 1]} {date.year}"
    return f"{date.day} {MONTH_NAMES[date.month - 1]} {date.year}"


# ===========================================================================
# Diff
# ===========================================================================

def _brief(rec: dict) -> str:
    f = rec.get("fields") or {}
    for k in ("Drop rate", "Amount", "XP", "Pool chance"):
        if f.get(k):
            return f"{k.lower() if k != 'XP' else 'XP'} {f[k]}"
    return ""


def _as_num(v):
    try:
        return float(str(v).replace("%", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _negligible(a, b) -> bool:
    """A percentage that moved by under 1% of itself (99.21% -> 99.19%) is the
    ripple of something listed elsewhere, not news for a player."""
    if not (str(a).endswith("%") and str(b).endswith("%")):
        return False
    x, y = _as_num(a), _as_num(b)
    if not x or y is None:
        return False
    return abs(y / x - 1) < NEGLIGIBLE


def diff_facts(prev: dict, curr: dict) -> dict:
    added, removed, changed = [], [], []
    for k, r in curr.items():
        if k not in prev:
            added.append({"name": r["name"], "group": r["group"], "detail": _brief(r)})
    for k, r in prev.items():
        if k not in curr:
            removed.append({"name": r["name"], "group": r["group"], "detail": _brief(r)})
    for k, r in curr.items():
        p = prev.get(k)
        if not p:
            continue
        pf, cf = p.get("fields") or {}, r.get("fields") or {}
        ch = [{"field": fld, "from": pf.get(fld, ""), "to": cf.get(fld, "")}
              for fld in sorted(set(pf) | set(cf))
              if pf.get(fld, "") != cf.get(fld, "")
              and not _negligible(pf.get(fld, ""), cf.get(fld, ""))]
        # Name-only differences are NOT reported. Backfilling Apr-Aug 2026 showed
        # every one of them was the name lookup flip-flopping between exports
        # ("Cursed Pickaxe" -> "custom CultistPiercer Effect Pickaxe" -> back),
        # never the game renaming a reward. The record is matched on FormID, so a
        # rename can't hide an add/remove; the page simply shows the newest name.
        if ch:
            changed.append({"name": r["name"], "group": r["group"], "changes": ch})

    # A pool that gains one item re-divides every other item's share. Listing 300
    # "0.31% -> 0.30%" rows buries the one line that matters. So within a group
    # where BULK_MIN+ records moved on the same single field, the records that
    # moved by the COMMON ratio (the ripple) become one summary line, and anything
    # that moved differently (the map that was halved) is still listed by name.
    summary, keep = [], []
    by_gf: Dict[tuple, list] = {}
    for c in changed:
        if len(c["changes"]) == 1:
            by_gf.setdefault((c["group"], c["changes"][0]["field"]), []).append(c)

    rippled = set()
    for (grp, fld), lst in sorted(by_gf.items()):
        if len(lst) < BULK_MIN:
            continue
        ratios = {}
        for c in lst:
            a, b = _as_num(c["changes"][0]["from"]), _as_num(c["changes"][0]["to"])
            if a and b is not None:
                ratios[id(c)] = b / a
        if len(ratios) < BULK_MIN:
            continue
        med = sorted(ratios.values())[len(ratios) // 2]
        def _is_ripple(c):
            if id(c) not in ratios or not med:
                return False
            if abs(ratios[id(c)] / med - 1) <= RIPPLE_TOL:
                return True
            # Tiny rates are shown to 2 significant figures, so 0.0015% -> 0.0014%
            # looks like -7% when the real move was the same -11% as everyone else.
            # Within one unit of the last displayed digit of the expected value = ripple.
            ch = c["changes"][0]
            a_, b_ = _as_num(ch["from"]), _as_num(ch["to"])
            dec = len(str(ch["to"]).replace("%", "").partition(".")[2])
            return abs(b_ - a_ * med) <= 10 ** (-dec) + 1e-12
        ripple = [c for c in lst if _is_ripple(c)]
        if len(ripple) < BULK_MIN:
            continue
        rippled.update(id(c) for c in ripple)
        n_add = sum(1 for a in added if a["group"] == grp)
        n_rem = sum(1 for a in removed if a["group"] == grp)
        n_other = len(lst) - len(ripple)
        if n_add and n_rem:
            why = f"because {n_add} added and {n_rem} removed"
        elif n_add:
            why = f"because {n_add} {'item was' if n_add == 1 else 'items were'} added"
        elif n_rem:
            why = f"because {n_rem} {'item was' if n_rem == 1 else 'items were'} removed"
        elif n_other:
            why = "because of the change" + ("" if n_other == 1 else "s") + " below"
        else:
            why = ""
        move = abs(med - 1) * 100
        how = "shifted slightly" if move < 10 else f"{'rose' if med > 1 else 'fell'} by about {move:.0f}%"
        ex = ripple[0]["changes"][0]
        noun = f"{fld.lower()}s" if not fld.lower().endswith("s") else fld.lower()
        line = (f"{grp}: {'the other ' if n_other else ''}{len(ripple):,} items' {noun} "
                f"{how} (e.g. {ex['from']} → {ex['to']})")
        summary.append(line + (f" {why}." if why else "."))

    keep = [c for c in changed if id(c) not in rippled]

    key = lambda x: (x["group"], x["name"])
    return {"added": sorted(added, key=key)[:MAX_LIST],
            "removed": sorted(removed, key=key)[:MAX_LIST],
            "changed": sorted(keep, key=key)[:MAX_LIST],
            "summary": summary,
            "counts": {"added": len(added), "removed": len(removed), "changed": len(changed)}}


# ===========================================================================
# Driver
# ===========================================================================

def _page_url(keys) -> Optional[str]:
    for k in keys or []:
        k = str(k)
        if k.startswith("/"):
            return k if k.endswith("/") else k + "/"
    return None


def iter_pages(dist: Path, fam: dict):
    """Yield (url, slug, page_obj) for every page slice the family owns."""
    base = dist / fam["slices"]
    if not base.is_dir():
        return
    for fp in sorted(base.glob("*.json")):
        if fp.name.startswith("_"):
            continue
        try:
            d = json.load(open(fp, encoding="utf-8"))
        except Exception:
            continue
        url = _page_url(d.get("keys"))
        if not url or not url.startswith(fam["prefixes"]):
            continue
        yield url, fp.stem, d.get("page") or {}


def run_family(name: str, dist: Path, channel: str, *, tsv_root=None, sig=None,
               when: Optional[_dt.date] = None, dry_run=False, verbose=True,
               git_fallback=True) -> dict:
    fam = FAMILIES[name]
    out_dir = dist / "patchlogs" / "pages" / name
    state_dir = dist / "patchlogs" / "_state" / name
    sig = sig if sig is not None else signature(fam, tsv_root)
    sig_file = state_dir / "_signature.json"
    prev_sig = (_load(sig_file, git_fallback) or {}).get("inputs") or {}

    changed_pats = [p for p, v in sig.items()
                    if (prev_sig.get(p) or {}).get("sha") != v["sha"]]
    new_export = bool(prev_sig) and bool(changed_pats)
    first_run = not prev_sig

    if when is None and new_export:
        if channel == "pts":
            when = _pts_pull_date(changed_pats)
        if when is None:
            ds = [_dt.date.fromisoformat(sig[p]["date"]) for p in changed_pats if sig[p]["date"]]
            when = max(ds) if ds else None

    # A PTS pull is a preview of the NEXT patch; labelling it with the live patch
    # it was pulled during would read as "this is in Infestations".
    at = tsv_source.patch_at(when) if (when and channel != "pts") else None
    prov = tsv_source.provenance(fam["primary"], channel)
    stats = {"pages": 0, "entries": 0, "refreshed": 0}
    index = {}

    for url, slug, page in iter_pages(dist, fam):
        stats["pages"] += 1
        facts = fam["facts"](page)
        st_path = state_dir / f"{slug}.json"
        feed_path = out_dir / f"{slug}.json"
        prev = _load(st_path, git_fallback)
        prev_facts = (prev or {}).get("facts")
        feed = _load(feed_path, git_fallback) or {}
        entries = [e for e in (feed.get("entries") or []) if isinstance(e, dict)]

        wrote_entry = False
        if new_export and prev_facts is not None:
            d = diff_facts(prev_facts, facts)
            if d["added"] or d["removed"] or d["changed"] or d["summary"]:
                entry = {
                    "ts": when.isoformat() if when else "",
                    "label": entry_label(when, channel),
                    "channel": channel,
                    "game_version": at["version"] if at else None,
                    "patch_name": at["name"] if at else None,
                    "sources": sorted(sig[p]["file"] for p in changed_pats),
                    **d,
                }
                # Same export re-diffed (re-run, retry) replaces, never duplicates.
                entries = [e for e in entries if e.get("ts") != entry["ts"]
                           or e.get("channel") != channel]
                entries.insert(0, entry)
                entries.sort(key=lambda e: e.get("ts") or "", reverse=True)
                entries = entries[:MAX_ENTRIES]
                wrote_entry = True
                stats["entries"] += 1
                if verbose:
                    c = d["counts"]
                    print(f"[page-patchlog] {name}/{slug}: +{c['added']} "
                          f"-{c['removed']} ~{c['changed']}", file=sys.stderr)
        elif prev_facts is not None and prev_facts != facts:
            stats["refreshed"] += 1          # code change, same exports: no entry

        index[url] = f"{name}/{slug}.json"
        if dry_run:
            continue
        _write(st_path, {"page": url, "facts": facts}, compact=True)
        _write(feed_path, {"page": url, "family": name, "channel": channel,
                           "provenance": prov, "entries": entries})

    if not dry_run:
        _write(sig_file, {"inputs": sig, "updated": _dt.date.today().isoformat()})
        idx_path = dist / "patchlogs" / "pages" / "_index.json"
        idx = _load(idx_path, git_fallback) or {}
        pages = {k: v for k, v in (idx.get("pages") or {}).items()
                 if not v.startswith(f"{name}/")}
        pages.update(index)
        _write(idx_path, {"pages": dict(sorted(pages.items()))})

    if verbose:
        why = ("first run — baseline stored, no entries" if first_run else
               f"new export(s): {', '.join(sig[p]['file'] for p in changed_pats)}"
               if new_export else "no new export — no entries")
        print(f"[page-patchlog] {name} ({channel}): {stats['pages']} pages, "
              f"{stats['entries']} new entries, {stats['refreshed']} silent refreshes — {why}",
              file=sys.stderr)
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser(description="Per-page patch logs (diff what each page shows).")
    ap.add_argument("--dist", default="dist", help="dist root (dist, or dist/pts after relocate)")
    ap.add_argument("--channel", default=os.environ.get("DFBNB_CHANNEL", "live"))
    ap.add_argument("--family", nargs="*", default=None, help="limit to these families")
    ap.add_argument("--tsv-root", default=None, help="resolve inputs here instead of tsv/")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    dist = Path(a.dist)
    if not dist.is_absolute():
        dist = (Path.cwd() / dist).resolve()
    for fam in a.family or list(FAMILIES):
        run_family(fam, dist, a.channel, tsv_root=a.tsv_root, dry_run=a.dry_run)


if __name__ == "__main__":
    main()
