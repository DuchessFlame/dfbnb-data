#!/usr/bin/env python3
import argparse
import csv
import json
import os
from typing import Dict, Any, List

def norm_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""
    # If it's a full URL, strip domain part
    if "://" in p:
        try:
            p = p.split("://", 1)[1]
            p = p.split("/", 1)[1] if "/" in p else ""
            p = "/" + p
        except Exception:
            pass
    if not p.startswith("/"):
        p = "/" + p
    if not p.endswith("/"):
        p = p + "/"
    return p

def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--guide-index", required=True, help="Path to guide_index.tsv in repo")
    ap.add_argument("--out", required=True, help="Output manifest JSON path, e.g. dist/patchlog_manifest.json")
    ap.add_argument("--dist-base-url", required=True, help="Base URL to dist/ on raw.githubusercontent.com")
    ap.add_argument("--titles-feed", default="patchlog_latest_titles.json", help="Feed filename in dist/")
    ap.add_argument("--df-prefix", default="/df/", help="Only include pages under this prefix")
    ap.add_argument("--bnb-prefix", default="/bnb/", help="Only include pages under this prefix")
    args = ap.parse_args()

    dist_base = args.dist_base_url.rstrip("/")
    titles_feed_url = f"{dist_base}/{args.titles_feed}"

    by_page: Dict[str, Dict[str, str]] = {}

    # Read TSV
    with open(args.guide_index, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            url = (row.get("url") or row.get("URL") or "").strip()
            template = (row.get("template") or row.get("Template") or "").strip().lower()
            tags = (row.get("tags") or row.get("Tags") or "").strip().lower()

            path = norm_path(url)
            if not path:
                continue

            # Only map pages that should show TITLES patchlog.
            # Rule set (edit later, but start strict):
            # 1) Must be under df/ or bnb/
            # 2) Must look like a titles page by path or template or tags
            if not (path.startswith(args.df_prefix) or path.startswith(args.bnb_prefix)):
                continue

            is_titles = (
                "/titles/" in path
                or "titles" in template
                or "titles" in tags
            )

            if not is_titles:
                continue

            by_page[path] = {"url": titles_feed_url, "label": "titles"}

    write_json(args.out, {"byPage": by_page})

if __name__ == "__main__":
    main()
