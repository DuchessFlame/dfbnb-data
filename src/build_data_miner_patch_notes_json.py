#!/usr/bin/env python3
"""
build_data_miner_patch_notes_json.py
====================================
Builds dist/data-miner-patch-notes.json for the
/df/useful-links/data-miner-patch-notes/ (and BNB mirror) page on
buffsnbrew.com / theduchessflame.com (rendered by
df-bnb-data-miner-patch-notes.js in the dfbnb-child theme).

Source of truth
---------------
Scratchy1024 publishes every set of annotated PTS patch notes as a
*subdirectory* inside the single GitHub Pages repo
`Scratchy1024/scratchy1024.github.io`, NOT as separate repos. So we:

  1. List the repo root via /repos/.../contents/
  2. Keep directories whose name contains "PTS-Patch-Notes"
     (case-insensitive). This filter rejects:
       - the LIVE patch repo (`Gleaming-Depths-LIVE-Patch-Notes`)
       - the legacy space-named duplicate
         (`Gleaming Depths PTS Patch Notes`) which redirects to the
         hyphenated version
       - the `RemoveSpacesFromPNG.bat` file at the root
  3. For each surviving directory, find the OLDEST commit that
     touched that path. That commit's year is the year we bucket the
     entry into. (Repo creation date isn't useful — the whole repo was
     created in 2024 even though Fishing-in-Appalachia patch notes
     pre-date that.)
  4. Build the GitHub Pages URL as
     `https://scratchy1024.github.io/<dir-name>/`.

Output
------
  dist/data-miner-patch-notes.json
    {
      "title": "Data Miner Patch Notes",
      "subtitle": "...",
      "current_year": 2026,
      "source": { "user": ..., "repo": ..., "filter": ..., "count": N },
      "years": [
        { "year": 2026, "entries": [
            { "name": "...", "slug": "...", "url": "...",
              "first_commit_at": "...", "last_commit_at": "..." },
            ...
        ]},
        ...
      ]
    }

  Years are sorted newest-first. Within a year, entries are sorted by
  first_commit_at, newest-first.

Env
---
  GITHUB_TOKEN — optional. If set, used to authenticate the GitHub
                 API requests (5000/hr instead of 60/hr). The workflow
                 forwards the default GITHUB_TOKEN; running this
                 script locally without one is also fine — the script
                 makes only ~1 + N requests where N is the number of
                 directories.

Usage
-----
  python src/build_data_miner_patch_notes_json.py
  python src/build_data_miner_patch_notes_json.py --outdir dist
  python src/build_data_miner_patch_notes_json.py --user Scratchy1024 \
      --repo scratchy1024.github.io --filter PTS-Patch-Notes
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


GITHUB_USER         = "Scratchy1024"
GITHUB_REPO         = "scratchy1024.github.io"
PAGES_HOST          = "scratchy1024.github.io"   # lowercased for the URL
DIR_FILTER          = "PTS-Patch-Notes"          # case-insensitive substring
PAGE_TITLE          = "Data Miner Patch Notes"
PAGE_SUBTITLE       = (
    "Scratchy's annotated PTS patch notes for Fallout 76 — every set of "
    "data-mined notes the Data Miners Discord community has published."
)


def http_get_json(url: str, token: str | None = None) -> tuple[object, dict]:
    """GET a URL and parse the response as JSON. Returns (body, headers).
    GitHub requires a User-Agent header on every request — without one
    we get a 403."""
    req = urllib.request.Request(url, headers={
        "User-Agent":  "dfbnb-data-build/1.0 (+https://www.buffsnbrew.com)",
        "Accept":      "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        headers = {k.lower(): v for k, v in resp.headers.items()}
        return data, headers


def list_repo_root(user: str, repo: str, token: str | None) -> list[dict]:
    """Return the list of items at the root of a repo."""
    url = f"https://api.github.com/repos/{user}/{repo}/contents/"
    body, _ = http_get_json(url, token=token)
    if not isinstance(body, list):
        raise RuntimeError(f"Unexpected /contents response: {body!r}")
    return body


def commits_touching_path(
    user: str, repo: str, path: str, token: str | None
) -> list[dict]:
    """Return every commit that touched `path`, newest first.

    Paginate via the Link header (per_page=100). Stop after 10 pages as a
    safety net — Scratchy's per-directory commit count is small."""
    out: list[dict] = []
    base = f"https://api.github.com/repos/{user}/{repo}/commits"
    page = 1
    while page <= 10:
        url = (
            f"{base}?per_page=100&page={page}"
            f"&path={urllib.parse.quote(path, safe='')}"
        )
        body, headers = http_get_json(url, token=token)
        if not isinstance(body, list):
            raise RuntimeError(f"Unexpected /commits response: {body!r}")
        out.extend(body)
        link = headers.get("link") or ""
        if 'rel="next"' not in link:
            break
        page += 1
    return out


def humanise_name(dir_name: str) -> str:
    """'Gleaming-Depths-PTS-Patch-Notes' -> 'Gleaming Depths PTS Patch Notes'.
    Preserve casing as supplied by the directory name."""
    s = re.sub(r"[-_]+", " ", dir_name).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def commit_date_iso(commit: dict) -> str:
    """Pull the ISO-8601 date string out of a /commits response item.
    Prefer the committer date (when it landed on the branch); fall back
    to the author date if for some reason the former is absent."""
    c = commit.get("commit") or {}
    com = c.get("committer") or {}
    aut = c.get("author") or {}
    return str(com.get("date") or aut.get("date") or "")


def year_from_iso(iso: str) -> int:
    m = re.match(r"^(\d{4})-", iso)
    return int(m.group(1)) if m else 0


def build(
    user: str, repo: str, name_filter: str, token: str | None
) -> dict:
    items = list_repo_root(user, repo, token)

    needle = name_filter.lower()
    dirs: list[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if it.get("type") != "dir":
            continue
        name = str(it.get("name") or "")
        if not name:
            continue
        if needle not in name.lower():
            continue
        dirs.append(name)

    entries: list[dict] = []
    for d in dirs:
        commits = commits_touching_path(user, repo, d, token)
        if not commits:
            # No commits found is suspicious but not fatal — skip rather
            # than crash the whole build.
            print(f"  WARN: no commits found for path {d!r}", file=sys.stderr)
            continue
        # commits is newest-first; first_commit is the LAST entry, last
        # commit is the FIRST entry.
        first_commit_at = commit_date_iso(commits[-1])
        last_commit_at  = commit_date_iso(commits[0])

        # PAGES_HOST is lowercased even when the username is mixed-case,
        # because GitHub Pages always serves at the lowercased user host
        # (`scratchy1024.github.io`, not `Scratchy1024.github.io`).
        entries.append({
            "name":            humanise_name(d),
            "slug":            d,
            "url":             f"https://{PAGES_HOST}/{urllib.parse.quote(d)}/",
            "first_commit_at": first_commit_at,
            "last_commit_at":  last_commit_at,
        })

    # Group by year of FIRST commit (matches the user's "created date"
    # intent — the year Scratchy first published the patch notes).
    by_year: dict[int, list[dict]] = {}
    for e in entries:
        y = year_from_iso(e["first_commit_at"])
        by_year.setdefault(y, []).append(e)

    # Newest-first ordering at every level.
    for y in by_year:
        by_year[y].sort(key=lambda e: e["first_commit_at"], reverse=True)
    years_sorted = sorted(by_year.keys(), reverse=True)

    current_year = years_sorted[0] if years_sorted else 0

    return {
        "title":        PAGE_TITLE,
        "subtitle":     PAGE_SUBTITLE,
        "current_year": current_year,
        "source": {
            "user":   user,
            "repo":   repo,
            "filter": name_filter,
            "count":  len(entries),
        },
        "years": [
            {"year": y, "entries": by_year[y]}
            for y in years_sorted
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="dist")
    ap.add_argument("--user",   default=GITHUB_USER)
    ap.add_argument("--repo",   default=GITHUB_REPO)
    ap.add_argument("--filter", default=DIR_FILTER)
    args = ap.parse_args()

    token = os.environ.get("GITHUB_TOKEN") or None

    try:
        data = build(args.user, args.repo, args.filter, token)
    except urllib.error.HTTPError as e:
        print(f"GitHub API HTTPError: {e.code} {e.reason}", file=sys.stderr)
        return 2
    except urllib.error.URLError as e:
        print(f"GitHub API URLError: {e.reason}", file=sys.stderr)
        return 2

    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data-miner-patch-notes.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Wrote {out_path}")
    print(f"  source: {data['source']}")
    print(f"  years:  {[y['year'] for y in data['years']]}")
    for y in data["years"]:
        for e in y["entries"]:
            print(f"    {y['year']}  {e['name']:<48}  {e['url']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
