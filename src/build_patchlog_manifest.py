#!/usr/bin/env python3
import argparse
import json
import os
from typing import Dict, Any

def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

def norm_path(p: str) -> str:
    p = (p or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    if not p.endswith("/"):
        p = p + "/"
    return p

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output manifest JSON path")
    ap.add_argument("--base-url", required=True, help="Base URL to dist/ (no trailing slash preferred)")
    ap.add_argument("--titles-feed", default="patchlog_latest_titles.json", help="Feed filename inside dist/")
    args = ap.parse_args()

    base = (args.base_url or "").strip().rstrip("/")
    feed_url = f"{base}/{args.titles_feed}"

    # Titles pages that should show Patch Log.
    # Include new routes + back-compat routes (adjust any you don’t want).
    pages = [
        "/df/titles/camp-titles/checklist/",
        "/df/titles/player-titles/checklist/",
        "/df/titles/camp-titles/generator/",
        "/df/titles/player-titles/generator/",
        "/df/camp/camp-titles/checklist/",
        "/df/collectables/player-titles/checklist/",

        "/bnb/titles/camp-titles/checklist/",
        "/bnb/titles/player-titles/checklist/",
        "/bnb/titles/camp-titles/generator/",
        "/bnb/titles/player-titles/generator/",
    ]

    by_page: Dict[str, Dict[str, str]] = {}
    for p in pages:
        by_page[norm_path(p)] = {"url": feed_url, "label": "titles"}

    manifest = {"byPage": by_page}
    write_json(args.out, manifest)

if __name__ == "__main__":
    main()
