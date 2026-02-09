#!/usr/bin/env python3
import argparse
import csv
import json
import os
from typing import Dict, Any, TextIO


def norm_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""

    # If it's a full URL, strip scheme+domain (keep only path)
    if "://" in p:
        try:
            p = p.split("://", 1)[1]
            p = "/" + p.split("/", 1)[1]
        except Exception:
            return ""

    if not p.startswith("/"):
        p = "/" + p
    if not p.endswith("/"):
        p += "/"
    return p


def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def pick(row: Dict[str, str], *keys: str) -> str:
    # direct match
    for k in keys:
        if k in row and row[k] is not None:
            return str(row[k]).strip()

    # case-insensitive match
    lower_map = {str(k).lower(): k for k in row.keys()}
    for k in keys:
        kk = str(k).lower()
        if kk in lower_map:
            return str(row[lower_map[kk]]).strip()

    return ""

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--guide-index", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--dist-base-url", required=True)  # raw.githubusercontent.com/.../dist
    ap.add_argument("--titles-feed", default="patchlog_latest_titles.json")
    args = ap.parse_args()

    dist_base = (args.dist_base_url or "").rstrip("/")
    titles_feed_url = f"{dist_base}/{args.titles_feed}"

    by_page: Dict[str, Dict[str, str]] = {}

    # Read TSV as raw bytes, then decode safely (Excel-proof)
    raw = open(args.guide_index, "rb").read()

    text = None
    for enc in ("utf-8-sig", "utf-16", "cp1252"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    if text is None:
        raise RuntimeError("Unable to decode guide_index.tsv with known encodings")

    import io
    f = io.StringIO(text)
    reader = csv.DictReader(f, delimiter="\t")

    for row in reader:
        url = pick(row, "url", "URL")
        template = pick(row, "template", "Template").lower()
        tags = pick(row, "tags", "Tags").lower()

        path = norm_path(url)
        if not path:
            continue

        # Rule: include ONLY pages that should show Titles patch logs.
        is_titles_page = (
            "/titles/" in path
            or "/collectables/player-titles/" in path
            or "/camp/camp-titles/" in path
            or "titles" in template
            or "titles" in tags
        )
        if not is_titles_page:
            continue

        by_page[path] = {"url": titles_feed_url, "label": "titles"}

    write_json(args.out, {"byPage": by_page})

if __name__ == "__main__":
    main()
