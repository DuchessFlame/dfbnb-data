#!/usr/bin/env python3
"""
Shared patchlog utilities for all DFBNB build scripts.

WHAT CHANGED (2026-08-22) AND WHY
=================================
The old patch log compared **our own published JSON against its own previous git
revision**. The build reruns every 6 hours; if the inputs did not change, the
output did not change, so every entry read:

    2026-08-22 18:43Z
    Current: 76  Added: 0  Removed: 0  Changed: 0
    Added: —   Removed: —   Changed: —

That is a report about *our file*, not about the game. It says "nothing changed"
just as loudly when nothing changed as when nobody has exported since July — and
it stamps today's date on both. A diff of outputs cannot detect a missing input.
The Pint-Sized Phantoms grave page was wrong for five weeks behind exactly that
green, cheerful, empty box.

The new model diffs the GAME EXPORTS against each other — CHAL_Export_July vs
CHAL_Export_June — not our JSON against our JSON. That means:

  * One entry per EXPORT, never per build. A rebuild with no new export produces
    no entry at all, instead of a fresh "nothing changed" line every 6 hours.
  * Hotfixes land between patches and an export can straddle them, so the entry
    boundary is the export, not the patch. The patch is shown as context.
  * Removals are real: a record present in June and absent in July is a removal,
    which the output-diff could never see while the input sat still.
  * Changes carry detail — "drop rate 12% -> 18%" — not just a name.

Frontend contract — each feed file is:
{
  "provenance": {                                // the freshness line
    "verified_date": "2026-07-18",               // when we last LOOKED, not built
    "verified_game_version": "1.7.25.15",
    "current_game_version": "1.7.25.39",
    "patches_behind": 1                          // null = never verified
  },
  "entries": [
    {
      "ts":       "2026-07-18",                  // the EXPORT date
      "source":   "CHAL_Export_July_2026.tsv",
      "game_version": "1.7.25.15",               // context, not the grouping key
      "current":  322,
      "added":    [{"name": "...", "id": "..."}],
      "removed":  [{"name": "...", "id": "..."}],
      "changed":  [{"name": "...", "id": "...",
                    "changes": [{"field": "Drop rate", "from": "12%", "to": "18%"}]}]
    }
  ]
}

Plain string entries in added/removed/changed are still accepted by the renderer,
so feeds written by the old path keep working while callers are migrated.


Usage in a build script:
    from patchlog_utils import write_patchlog_feed

    write_patchlog_feed(
        dist_dir="dist",
        feed_name="patchlog_latest_bnb_armour.json",
        current_items=current_items,   # list of dicts
        key_field="formId",            # unique identifier field
        name_field="name",             # human-readable name field
        compare_fields=["name", "description", "stats"],  # fields to diff
        prev_json_path="dist/armour.json",  # path to the output JSON (for git prev)
        items_extractor=lambda d: d.get("items", []),  # how to pull items from the JSON
    )
"""

import datetime as dt
import json
import os
import subprocess
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """UTC ISO-8601 timestamp, no microseconds."""
    utc = getattr(dt, "UTC", dt.timezone.utc)
    return dt.datetime.now(utc).replace(microsecond=0).isoformat()


def _git_show_json(rev: str, path: str) -> Optional[Any]:
    """Load a JSON file from a previous git revision. Returns None on failure."""
    try:
        out = subprocess.check_output(
            ["git", "show", f"{rev}:{path}"],
            stderr=subprocess.DEVNULL,
            timeout=30,
        )
        return json.loads(out.decode("utf-8"))
    except Exception:
        return None


def _load_json_file(path: str) -> Optional[Any]:
    """Load a JSON file from disk. Returns None if missing or invalid."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_json(path: str, data: Any) -> None:
    """Write JSON with consistent formatting."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


# ---------------------------------------------------------------------------
# Core diff logic
# ---------------------------------------------------------------------------

def diff_item_lists(
    prev_items: List[dict],
    curr_items: List[dict],
    key_field: str = "formId",
    name_field: str = "name",
    compare_fields: Optional[Sequence[str]] = None,
) -> dict:
    """
    Compare two lists of item dicts and return a patchlog entry.

    Parameters
    ----------
    prev_items : list of dict
        Items from the previous build (may be empty).
    curr_items : list of dict
        Items from the current build.
    key_field : str
        Field used as unique identifier (e.g. "formId").
    name_field : str
        Field used for human-readable display name.
        Can be a comma-separated fallback chain: "name,displayName,edid"
    compare_fields : list of str or None
        Fields to check for changes. If None, compares all fields.

    Returns
    -------
    dict matching the frontend entry contract:
        {ts, current, added, removed, changed}
    """
    # Support fallback name fields like "name,displayName,edid"
    name_fields = [f.strip() for f in name_field.split(",")]

    def get_name(item: dict) -> str:
        for nf in name_fields:
            val = item.get(nf)
            if val and str(val).strip():
                return str(val).strip()
        # Last resort: use the key field value
        return str(item.get(key_field, "Unknown")).strip()

    def get_key(item: dict) -> str:
        return str(item.get(key_field, "")).strip()

    # Index by key
    prev_by_key: Dict[str, dict] = {}
    for item in prev_items:
        k = get_key(item)
        if k:
            prev_by_key[k] = item

    curr_by_key: Dict[str, dict] = {}
    for item in curr_items:
        k = get_key(item)
        if k:
            curr_by_key[k] = item

    # Diff
    added_names: List[str] = []
    removed_names: List[str] = []
    changed_names: List[str] = []

    for k, item in curr_by_key.items():
        if k not in prev_by_key:
            added_names.append(get_name(item))

    for k, item in prev_by_key.items():
        if k not in curr_by_key:
            removed_names.append(get_name(item))

    for k, curr_item in curr_by_key.items():
        if k in prev_by_key:
            prev_item = prev_by_key[k]
            if compare_fields:
                differs = any(
                    prev_item.get(f) != curr_item.get(f)
                    for f in compare_fields
                )
            else:
                # Compare all fields (excluding debug/meta)
                skip = {"debug", "_meta", "_source"}
                all_keys = set(prev_item.keys()) | set(curr_item.keys())
                differs = any(
                    prev_item.get(f) != curr_item.get(f)
                    for f in all_keys
                    if f not in skip
                )
            if differs:
                changed_names.append(get_name(curr_item))

    # Cap at 500 to keep feed files manageable
    return {
        "ts": _now_iso(),
        "current": len(curr_by_key),
        "added": sorted(added_names)[:500],
        "removed": sorted(removed_names)[:500],
        "changed": sorted(changed_names)[:500],
    }


# ---------------------------------------------------------------------------
# Convenience: diff for nested/paged structures
# ---------------------------------------------------------------------------

def diff_paged_items(
    prev_data: Optional[dict],
    curr_data: dict,
    pages_key: str = "pages",
    items_key: str = "items",
    key_field: str = "formId",
    name_field: str = "name",
    compare_fields: Optional[Sequence[str]] = None,
) -> dict:
    """
    Diff for structures like {pages: {page_name: {items: [...]}}}
    Combines all items across all pages into a single diff.
    """
    def extract_all_items(data: Optional[dict]) -> List[dict]:
        if not data:
            return []
        pages = data.get(pages_key, {})
        if isinstance(pages, dict):
            items = []
            for page_data in pages.values():
                if isinstance(page_data, dict):
                    items.extend(page_data.get(items_key, []))
                elif isinstance(page_data, list):
                    items.extend(page_data)
            return items
        return []

    return diff_item_lists(
        prev_items=extract_all_items(prev_data),
        curr_items=extract_all_items(curr_data),
        key_field=key_field,
        name_field=name_field,
        compare_fields=compare_fields,
    )


def diff_keyed_list(
    prev_data: Optional[dict],
    curr_data: dict,
    list_key: str = "events",
    key_field: str = "questFormID",
    name_field: str = "name",
    compare_fields: Optional[Sequence[str]] = None,
) -> dict:
    """
    Diff for structures like {events: [{questFormID, name, ...}]}
    where the top-level value is a flat list of items.
    """
    prev_items = (prev_data or {}).get(list_key, []) if prev_data else []
    curr_items = curr_data.get(list_key, [])

    if not isinstance(prev_items, list):
        prev_items = []
    if not isinstance(curr_items, list):
        curr_items = []

    return diff_item_lists(
        prev_items=prev_items,
        curr_items=curr_items,
        key_field=key_field,
        name_field=name_field,
        compare_fields=compare_fields,
    )


# ---------------------------------------------------------------------------
# Main entry point for build scripts
# ---------------------------------------------------------------------------

def write_patchlog_feed(
    dist_dir: str,
    feed_name: str,
    current_items: List[dict],
    key_field: str = "formId",
    name_field: str = "name",
    compare_fields: Optional[Sequence[str]] = None,
    prev_json_path: Optional[str] = None,
    items_extractor: Optional[Callable[[Any], List[dict]]] = None,
    prev_git_rev: str = "HEAD^",
    record_type: Optional[str] = None,
    channel: str = "live",
) -> dict:
    """
    High-level convenience: generate and write a patchlog feed file.

    Parameters
    ----------
    dist_dir : str
        Output directory (e.g. "dist").
    feed_name : str
        Feed filename (e.g. "patchlog_latest_bnb_armour.json").
        Written directly into dist_dir (NOT a patchlogs/ subfolder).
    current_items : list of dict
        The current build's items to diff against previous.
    key_field, name_field, compare_fields :
        Passed through to diff_item_lists().
    prev_json_path : str or None
        Relative path (from repo root) to the main JSON output file.
        Used to load the previous version from git for comparison.
        If None, no diff is performed (just records current state).
    items_extractor : callable or None
        Function to extract an items list from the loaded prev JSON.
        E.g. lambda d: d.get("items", [])
        If None, assumes the prev JSON IS the items list.
    prev_git_rev : str
        Git revision to load previous data from (default "HEAD^").

    Returns
    -------
    The patchlog entry dict that was written.
    """
    # Load previous items for comparison
    prev_items: List[dict] = []
    if prev_json_path:
        prev_data = _git_show_json(prev_git_rev, prev_json_path)
        if prev_data is not None:
            if items_extractor:
                prev_items = items_extractor(prev_data)
            elif isinstance(prev_data, list):
                prev_items = prev_data
            else:
                prev_items = prev_data.get("items", [])

    entry = diff_item_lists(
        prev_items=prev_items,
        curr_items=current_items,
        key_field=key_field,
        name_field=name_field,
        compare_fields=compare_fields,
    )

    feed_path = os.path.join(dist_dir, feed_name)
    a, r, c = len(entry["added"]), len(entry["removed"]), len(entry["changed"])

    # A build with nothing to report must NOT write an entry. The old code wrote
    # one every run, so every feed showed "today · Added 0 Removed 0 Changed 0"
    # forever — a green box that said nothing, on data that was five weeks wrong.
    # Keep whatever real entries the feed already has instead of overwriting them.
    existing = _load_json_file(feed_path) or {}
    entries = existing.get("entries") if isinstance(existing, dict) else None
    entries = [e for e in (entries or [])
               if (e.get("added") or e.get("removed") or e.get("changed"))]

    if a or r or c:
        entries.insert(0, entry)
    entries = entries[:MAX_ENTRIES]

    feed = {"entries": entries}
    if record_type:
        prov = tsv_source.provenance(record_type, channel)
        prov["current"] = entry["current"]
        feed["provenance"] = prov

    _write_json(feed_path, feed)

    if a or r or c:
        print(f"[patchlog] {feed_name}: current={entry['current']}  "
              f"added={a}  removed={r}  changed={c}", file=sys.stderr)
    else:
        print(f"[patchlog] {feed_name}: no change — no entry written "
              f"({len(entries)} kept)", file=sys.stderr)

    return entry


# ===========================================================================
# NEW ENGINE — diff the game exports against each other
# ===========================================================================

import csv as _csv
import datetime
import re
import tsv_source

MAX_ENTRIES = 40          # how many exports back a page's log goes
MAX_NAMES = 400           # per list, per entry


def _read_export(path, key_col, name_cols, fields, scope=None):
    """{key: row} for one export TSV, filtered to the page's scope."""
    out = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
        for row in _csv.DictReader(f, delimiter="\t"):
            k = str(row.get(key_col, "") or "").strip().upper()
            if not k:
                continue
            if scope and not scope(row):
                continue
            keep = {"__name": _first(row, name_cols) or k}
            for col in (fields or {}):
                keep[col] = _clean(row.get(col, ""))
            out[k] = keep
    return out


_TAGS = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def _clean(v):
    """Strip game markup and collapse whitespace.

    Descriptions ship as rich text — "<font face='$TerminalFont'>..." — so a diff on
    the raw value reports markup churn as a game change and then renders the tags at
    the reader. Cleaning before BOTH the comparison and the display means only the
    words count, and only the words show.
    """
    s = _WS.sub(" ", _TAGS.sub("", str(v or ""))).strip()
    return s


def _display(v, limit=90):
    s = _clean(v)
    return s if len(s) <= limit else s[:limit - 1].rstrip() + "…"


def _first(row, cols):
    for c in cols:
        v = str(row.get(c, "") or "").strip()
        if v:
            return v
    return ""


ARTIFACT_MIN = 25         # this many one-sided blank changes in one field is not the game
ARTIFACT_BLANK_SHARE = 0.90


def _collapse_export_artifacts(changed, total):
    """Separate real game changes from the exporter being fixed.

    When a column starts (or stops) being populated — the CHAL exporter shipped
    blank CNAM / ENAM / MNAM from April to July 2026 — every record carrying that
    field "changes" at once. That is a change to how we look at the game, not a
    change to the game, and listing 400 identical rows buries anything real that
    shipped in the same export.

    Rule: if a field changed 25+ times and 90%+ of those were blank on one side,
    collapse it to a single note and drop it from the per-record list. A record
    with other genuine changes keeps those. Nothing is hidden — the note carries
    the count, so a real patch that happened to fill 25 blanks still gets said out
    loud, just in one line instead of 25.
    """
    if not changed or total <= 0:
        return changed, []

    per_field, blank_from, blank_to = {}, {}, {}
    for c in changed:
        for d in c["changes"]:
            f = d["field"]
            per_field[f] = per_field.get(f, 0) + 1
            if not str(d["from"]).strip():
                blank_from[f] = blank_from.get(f, 0) + 1
            if not str(d["to"]).strip():
                blank_to[f] = blank_to.get(f, 0) + 1

    artifacts, notes = set(), []
    for f, n in per_field.items():
        if n < ARTIFACT_MIN:
            continue
        if blank_from.get(f, 0) >= n * ARTIFACT_BLANK_SHARE:
            notes.append(f"export now includes {f} — {n} records gained a value "
                         f"(exporter fix, not a game change)")
            artifacts.add(f)
        elif blank_to.get(f, 0) >= n * ARTIFACT_BLANK_SHARE:
            notes.append(f"export stopped providing {f} — {n} records lost their value "
                         f"(export problem, not a game change)")
            artifacts.add(f)

    # These notes go to the BUILD LOG only, never into the feed. Players want
    # added / removed / changed and nothing else; our exporter's history is our
    # problem. The changes themselves are still dropped so they can't masquerade
    # as game changes.
    for n in notes:
        print(f"[patchlog] suppressed: {n}", file=sys.stderr)

    if not artifacts:
        return changed, []

    kept = []
    for c in changed:
        rest = [d for d in c["changes"] if d["field"] not in artifacts]
        if rest:
            kept.append({**c, "changes": rest})
    return kept, []


def diff_exports(record_type, key_col, name_cols=("FULL", "EDID"), fields=None,
                 scope=None, channel="live", pattern=None, exclude=None,
                 max_entries=MAX_ENTRIES):
    """One entry per export of `record_type`, newest first. No export, no entry.

    fields: {tsv_column: "Display label"} — the columns worth reporting a change in.
            A change in a column NOT listed is ignored, so cosmetic export churn
            (column reordering, whitespace) never manufactures a fake patch note.
    scope:  callable(row) -> bool, applied to BOTH sides of every pair, so a record
            that left the page shows up as a removal rather than silently vanishing.
    """
    pat = pattern or f"{record_type}_Export_*.tsv"
    paths = tsv_source.all_matching(pat, channel=channel, exclude=exclude)
    if len(paths) < 2:
        return []          # one export (or none) is nothing to compare against

    name_cols = list(name_cols)
    fields = fields or {}
    entries = []

    prev = _read_export(paths[0], key_col, name_cols, fields, scope)
    for path in paths[1:]:
        curr = _read_export(path, key_col, name_cols, fields, scope)
        if not curr:
            continue                     # unreadable / wrong-shape export: skip, don't
                                         # report every record as removed
        added, removed, changed = [], [], []
        for k, r in curr.items():
            if k not in prev:
                added.append({"name": r["__name"], "id": k})
        for k, r in prev.items():
            if k not in curr:
                removed.append({"name": r["__name"], "id": k})
        for k, r in curr.items():
            p = prev.get(k)
            if not p:
                continue
            deltas = [{"field": label,
                       "from": _display(p.get(col, "")), "to": _display(r.get(col, ""))}
                      for col, label in fields.items()
                      if p.get(col, "") != r.get(col, "")]
            if deltas:
                changed.append({"name": r["__name"], "id": k, "changes": deltas})

        changed, notes = _collapse_export_artifacts(changed, len(curr))

        if added or removed or changed or notes:
            d = tsv_source.export_date(path)
            at = tsv_source.patch_at(d)
            entries.append({
                "ts": d.isoformat() if d and d != datetime.date.min else "",
                "source": os.path.basename(path),
                "game_version": at["version"] if at else None,
                "patch_name": at["name"] if at else None,
                "current": len(curr),
                "added": sorted(added, key=lambda x: x["name"])[:MAX_NAMES],
                "removed": sorted(removed, key=lambda x: x["name"])[:MAX_NAMES],
                "changed": sorted(changed, key=lambda x: x["name"])[:MAX_NAMES],
                "notes": notes,
            })
        prev = curr

    entries.reverse()                     # newest first
    return entries[:max_entries]


def write_export_patchlog(dist_dir, feed_name, record_type, key_col,
                          name_cols=("FULL", "EDID"), fields=None, scope=None,
                          channel="live", pattern=None, current_count=None):
    """Write a feed built from export-to-export diffs, plus the freshness block.

    Always writes the file — a page with no recorded changes still needs to say
    when it was last checked, which is the whole point.
    """
    entries = diff_exports(record_type, key_col, name_cols, fields, scope,
                           channel=channel, pattern=pattern)
    prov = tsv_source.provenance(record_type, channel)
    if current_count is not None:
        prov["current"] = current_count

    feed = {"provenance": prov, "entries": entries}
    _write_json(os.path.join(dist_dir, feed_name), feed)

    behind = prov.get("patches_behind")
    behind_txt = "never verified" if behind is None else f"{behind} patch(es) behind"
    print(f"[patchlog] {feed_name}: {len(entries)} export entr(ies), "
          f"verified {prov.get('verified_date') or 'never'} ({behind_txt})",
          file=sys.stderr)
    return feed


# ===========================================================================
# FEED REGISTRY — every page's patch log, in one table
# ===========================================================================
#
# One row per feed instead of an edit in 30 build scripts. Each row says which
# game record type the page is really about, which column identifies a record,
# and which columns are worth reporting a change in. A column that is not listed
# is ignored, so export churn can never manufacture a patch note.
#
# A feed may draw on more than one record type (an event page is QUEST + GMRW);
# entries from each are merged newest-first and labelled with the file they came
# from.
#
# Feeds NOT in this table are pages with no single datamined record type behind
# them — Minerva's rotation, the inspiration generators, hand-curated lists. They
# stay on the item-diff path. Guessing a record type for those would produce
# confident nonsense, which is the whole failure being fixed here.

_ARMO = {"rt": "ARMO", "pattern": "ARMO_Export_*_ARMOUR.tsv", "key": "ARMO_FormID",
         "names": ("ARMO_FULL", "ARMO_EDID"),
         "fields": {"ARMO_FULL": "Name", "DATA_Value": "Value", "DATA_Weight": "Weight"}}
_WEAP = {"rt": "WEAP", "pattern": "WEAP_Export_*_Base.tsv", "key": "WEAP_FormID",
         "names": ("WEAP_FULL", "WEAP_EDID"),
         "fields": {"WEAP_FULL": "Name", "EILV_Level": "Level"}}
_ALCH = {"rt": "ALCH", "pattern": "ALCH_Export_*.tsv", "exclude": "_Effects",
         "key": "ALCH_FormID", "names": ("FULL", "ALCH_EDID"),
         "fields": {"FULL": "Name", "DESC": "Description", "Weight": "Weight", "Value": "Value"}}
_MGEF = {"rt": "MGEF", "pattern": "MGEF_Export_*.tsv", "key": "MGEF_FormID",
         "names": ("FULL", "EDID"),
         "fields": {"FULL": "Name", "DNAM_MagicItemDescription": "Effect", "BaseCost": "Base cost"}}
_FURN = {"rt": "FURN", "pattern": "FURN_Export_*_FURN.tsv", "key": "FURN_FormID",
         "names": ("FURN_FULL", "FURN_EDID"), "fields": {"FURN_FULL": "Name"}}
_FISH = {"rt": "FISH", "pattern": "FISH_Export_*.tsv", "key": "FormID",
         "names": ("FULL", "EDID"), "fields": {"FULL": "Name", "FISP": "Fish points"}}
_BOOK = {"rt": "BOOK", "pattern": "BOOK_Export_*.tsv", "exclude": "Locations",
         "key": "FormID", "names": ("FULL", "EDID"),
         "fields": {"FULL": "Name", "DESC": "Description"}}
_MISC = {"rt": "MISC", "pattern": "MISC_Export_*.tsv", "key": "FormID",
         "names": ("FULL", "EDID"),
         "fields": {"FULL": "Name", "Value": "Value", "Weight": "Weight"}}
_NOTE = {"rt": "NOTE", "pattern": "NOTE_Export_*.tsv", "key": "NOTE_FormID",
         "names": ("FULL", "NOTE_EDID"),
         "fields": {"FULL": "Name", "DATA_Value": "Value"}}
_NPC = {"rt": "NPC", "pattern": "NPC_Export_*.tsv", "exclude": "_Refs",
        "key": "FormID", "names": ("FULL", "EDID"),
        "fields": {"FULL": "Name", "RNAM_EDID": "Race"}}
_QUEST = {"rt": "QUEST", "pattern": "QUEST_Export_*.tsv", "key": "FormID",
          "names": ("FULL - Name", "EDID"),
          "fields": {"FULL - Name": "Name", "Quest Type": "Type", "LNAM - Location": "Location"}}
_GMRW = {"rt": "GMRW", "pattern": "GMRW_Export_*.tsv", "key": "FormID",
         "names": ("ParentQuestDisplay", "EDID"),
         "fields": {"ParentQuestDisplay": "Quest", "RewardsCount": "Reward count"}}
_ENTM = {"rt": "ENTM", "pattern": "ENTM_Export_*.tsv", "key": "FormID",
         "names": ("FULL", "EDID"), "fields": {"FULL": "Name", "DESC": "Description"}}
_LGDI = {"rt": "LGDI", "pattern": "LGDI_Export_*.tsv", "exclude": "_Mods",
         "key": "LGDI_FormID", "names": ("FULL", "LGDI_EDID"),
         "fields": {"FULL": "Name", "ScripCost": "Scrip cost"}}
_OMOD = {"rt": "OMOD", "pattern": "OMOD_Export_*.tsv", "exclude": "_Properties",
         "key": "OMOD_FormID", "names": ("FULL", "OMOD_EDID"),
         "fields": {"FULL": "Name", "DESC": "Description", "MaxRank": "Max rank"}}
_PCRD = {"rt": "PCRD", "pattern": "PCRD_Export_*.tsv", "key": "PCRD_FormID",
         "names": ("MNAM_Name", "PCRD_EDID"),
         "fields": {"MNAM_Name": "Name", "DESC": "Description",
                    "DATA_Value": "Cost", "DATA_MinLevel": "Min level"}}
_AVIF = {"rt": "AVIF", "pattern": "AVIF_Export_*.tsv", "key": "FormID",
         "names": ("FULL", "EDID"), "fields": {"FULL": "Name", "DESC": "Description"}}
# Help messages ship as MESG_Help_Export_*, not MESG_Export_*.
_MESG = {"rt": "MESG", "pattern": "MESG_Help_Export_*.tsv", "key": "FormID",
         "names": ("FULL", "EDID"), "fields": {"FULL": "Name", "DESC": "Description"}}
_LSCR = {"rt": "LSCR", "pattern": "LSCR_Export_*.tsv", "key": "LSCR_FormID",
         "names": ("LSCR_EDID",), "fields": {"DESC_Description": "Text"}}
_PLYT = {"rt": "PLYT", "pattern": "PLYT_Export_*.tsv", "key": "FormID",
         "names": ("ANAM - Male Title", "EDID - Editor ID"),
         "fields": {"ANAM - Male Title": "Title", "BNAM - Female Title": "Title (female)"}}
_CMPT = {"rt": "CMPT", "pattern": "CMPT_Export_*.tsv", "key": "FormID",
         "names": ("ANAM", "EDID"), "fields": {"ANAM": "Title"}}
_FLOR = {"rt": "FLOR", "pattern": "FLOR_Export_*.tsv", "key": "FLOR_FormID",
         "names": ("FLOR_FULL", "FLOR_EDID"),
         "fields": {"FLOR_FULL": "Name", "Produce": "Produce"}}
# Vendors ship as NPC2_Vendors_*, with a sibling _Placements file to skip.
# Minerva's stock is BOOK records whose name starts "Plan:" — build_minerva.py reads
# the same export and filters the same way, so the log tracks exactly what the page
# shows. Nothing here is hand-curated.
_BOOK_PLANS = {"rt": "BOOK", "pattern": "BOOK_Export_*.tsv", "exclude": "Locations",
               "key": "FormID", "names": ("FULL", "EDID"),
               "fields": {"FULL": "Name", "DESC": "Description"},
               "scope": lambda r: (r.get("FULL", "") or "").strip().startswith("Plan:")}

# Build inspiration is generated from the FLST entry lists + keyword tables; outfit
# inspiration from the ARMO export (BOD2 + keywords). Both are datamined, so both
# get a real log — see tools/build_df_calculators_json.mjs.
_FLST = {"rt": "FLST", "pattern": "FLST_Export_*_List.tsv", "key": "FLST_FormID",
         "names": ("FLST_FULL", "FLST_EDID"),
         "fields": {"FLST_FULL": "Name", "EntryCount": "Entries"}}
_KYWD = {"rt": "KYWD", "pattern": "KYWD_Export_*.tsv", "exclude": "_Refs",
         "key": "FormID", "names": ("NNAM_DisplayName", "FULL_Name", "EDID"),
         "fields": {"NNAM_DisplayName": "Display name", "TNAM_Type": "Type"}}

_NPC2 = {"rt": "NPC2", "pattern": "NPC2_Vendors_*.tsv", "exclude": "_Placements",
         "key": "FormID", "names": ("FULL", "EDID"), "fields": {"FULL": "Name"}}

FEED_SPECS = {
    "patchlog_latest_bnb_armour.json":         [_ARMO],
    "patchlog_latest_bnb_weapons.json":        [_WEAP],
    "patchlog_latest_bnb_unique_weapons.json": [_WEAP],
    "patchlog_latest_bnb_buffs.json":          [_ALCH, _MGEF],
    "patchlog_latest_bnb_camp_items.json":     [_FURN],
    "patchlog_latest_bnb_farming_meat.json":   [_ALCH],
    "patchlog_latest_bnb_legendary_mods.json": [_LGDI, _OMOD],
    "patchlog_latest_bnb_perk_cards.json":     [_PCRD],
    "patchlog_latest_bnb_specials_stats.json": [_AVIF],

    "patchlog_latest_df_activities.json":      [_QUEST, _GMRW],
    "patchlog_latest_df_camp.json":            [_FURN],
    "patchlog_latest_df_collectables.json":    [_MISC, _BOOK, _NOTE],
    "patchlog_latest_df_cryptids.json":        [_NPC],
    "patchlog_latest_df_events.json":          [_QUEST, _GMRW],
    "patchlog_latest_df_farming.json":         [_ALCH, _FLOR],
    "patchlog_latest_df_fishing.json":         [_FISH],
    "patchlog_latest_df_scoreboards.json":     [_ENTM],
    "patchlog_latest_df_seasonal_events.json": [_ENTM],
    "patchlog_latest_df_treasure_maps.json":   [_BOOK],
    "patchlog_latest_df_vendors.json":         [_NPC2],
    "patchlog_latest_df_world_pet_types.json": [_NPC],
    "patchlog_latest_world_pet_types.json":    [_NPC],
    "patchlog_latest_help_menu.json":          [_MESG],
    "patchlog_latest_load_screens.json":       [_LSCR],
    "patchlog_latest_titles.json":             [_PLYT, _CMPT],

    "patchlog_latest_df_minerva.json":            [_BOOK_PLANS],
    "patchlog_latest_df_minerva_calculator.json": [_BOOK_PLANS],
    "patchlog_latest_df_build_inspiration.json":  [_FLST, _KYWD],
    "patchlog_latest_df_outfit_inspiration.json": [_ARMO],
}


def write_registry_feeds(dist_dir, channel="live", only=None):
    """Write every registry feed for one channel. Returns {feed: entry_count}."""
    written = {}
    for feed, specs in FEED_SPECS.items():
        if only and feed not in only:
            continue
        entries, prov, total = [], None, 0
        for s in specs:
            try:
                got = diff_exports(s["rt"], key_col=s["key"],
                                   name_cols=s.get("names", ("FULL", "EDID")),
                                   fields=s.get("fields"), channel=channel,
                                   pattern=s.get("pattern"), exclude=s.get("exclude"),
                                   scope=s.get("scope"))
            except Exception as e:
                print(f"[patchlog] {feed}: {s['rt']} skipped ({e})", file=sys.stderr)
                continue
            entries.extend(got)
            p = tsv_source.provenance(s["rt"], channel, s.get("pattern"))
            # A page is only as fresh as its stalest input.
            if prov is None or _older(p, prov):
                prov = p
            if got:
                total = max(total, got[0].get("current", 0))

        entries.sort(key=lambda e: (e.get("ts") or "", e.get("source") or ""), reverse=True)
        entries = entries[:MAX_ENTRIES]
        if prov is None:
            prov = tsv_source.provenance(specs[0]["rt"], channel,
                                         specs[0].get("pattern"))
        prov["current"] = total

        _write_json(os.path.join(dist_dir, feed), {"provenance": prov, "entries": entries})
        written[feed] = len(entries)
        print(f"[patchlog] {feed}: {len(entries)} entr(ies), verified "
              f"{prov.get('verified_date') or 'never'}", file=sys.stderr)
    return written


def _older(a, b):
    """True when provenance `a` was verified longer ago than `b` (None = never)."""
    da, db = a.get("verified_date"), b.get("verified_date")
    if da is None:
        return True
    if db is None:
        return False
    return da < db


def write_empty_patchlog_feed(dist_dir: str, feed_name: str, current_count: int = 0) -> None:
    """
    Write a valid but empty patchlog feed (for scaffolded/stub scripts).
    The frontend will render this as "Current: N  Added: 0  Removed: 0  Changed: 0".
    """
    entry = {
        "ts": _now_iso(),
        "current": current_count,
        "added": [],
        "removed": [],
        "changed": [],
    }
    feed = {"entries": [entry]}
    feed_path = os.path.join(dist_dir, feed_name)
    _write_json(feed_path, feed)
    print(f"[patchlog] {feed_name}: empty feed (current={current_count})", file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI — one pass writes every registry feed for a channel
# ---------------------------------------------------------------------------
#
#   python src/patchlog_utils.py --write-feeds --dist dist            (live)
#   python src/patchlog_utils.py --write-feeds --dist dist --channel pts
#
# Run this AFTER the builders, so it takes precedence over any feed still being
# written by the old item-diff path.

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Write export-diff patch log feeds.")
    ap.add_argument("--write-feeds", action="store_true")
    ap.add_argument("--dist", default="dist")
    ap.add_argument("--channel", default=os.environ.get("DFBNB_CHANNEL", "live"))
    ap.add_argument("--only", nargs="*", default=None)
    args = ap.parse_args()

    if args.write_feeds:
        os.makedirs(args.dist, exist_ok=True)
        got = write_registry_feeds(args.dist, channel=args.channel, only=args.only)
        n = sum(1 for v in got.values() if v)
        print(f"[patchlog] {len(got)} feed(s) written for the {args.channel} channel; "
              f"{n} have at least one export entry.")
    else:
        ap.print_help()
