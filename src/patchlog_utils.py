#!/usr/bin/env python3
"""
Shared patchlog utilities for all DFBNB build scripts.

Every build script that produces JSON data should use this module to generate
a patchlog feed file that the frontend (df-bnb-guide.js) can consume.

Frontend contract — each feed file must be:
{
  "entries": [
    {
      "ts":      "2026-04-10T12:00:00+00:00",   // ISO-8601 timestamp
      "current": 322,                            // current item count
      "added":   ["Item Name 1", "Item Name 2"], // human-readable names
      "removed": ["Old Item"],
      "changed": ["Modified Item"]
    }
  ]
}

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

    feed = {"entries": [entry]}

    feed_path = os.path.join(dist_dir, feed_name)
    _write_json(feed_path, feed)

    # Summary
    a, r, c = len(entry["added"]), len(entry["removed"]), len(entry["changed"])
    print(
        f"[patchlog] {feed_name}: current={entry['current']}  "
        f"added={a}  removed={r}  changed={c}"
    )

    return entry


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
    print(f"[patchlog] {feed_name}: empty feed (current={current_count})")
