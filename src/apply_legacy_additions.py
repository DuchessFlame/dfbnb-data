#!/usr/bin/env python3
"""
apply_legacy_additions.py
-------------------------
Finds the rewards Bethesda ADDED when a season was re-released as a legacy
season, and writes them into tsv/season_rewards.tsv with `addedInRerun` set so
they render with the permanent gold NEW pill.

WHY THIS EXISTS
    A legacy re-run is not a straight repeat. Bethesda quietly adds rewards -
    usually titles and the end-of-season wall art - that were never on the
    original board. Nothing announces them, and `season_rewards.tsv` is the
    curated record of the ORIGINAL run, so the additions were simply invisible:
    the upcoming-rewards page (built straight from the PTS entitlement export)
    listed them, and the scoreboard page did not.

    This is the comparison that catches them: PTS export vs curated board.

HOW IT DECIDES
    Match is on `storefrontEntitlement` / `edid` - the game's own form EDID, not
    the display name. Names get reworded between runs; EDIDs do not. Any
    entitlement in the PTS export for season N that has no curated row in
    season N is an addition.

    Rewards with no rank on the original board are correct here: they were not
    on it. They render in their own "Added in the legacy re-release" section.

STATUS: active
INPUT:  tsv/season_rewards.tsv
        tsv/legacy_seasons.tsv
        dist/calculators/upcoming_rewards_s{N}.json   (the PTS export)
        dist/season_images/season_{N}_images.json     (artwork, optional)
OUTPUT: tsv/season_rewards.tsv (backed up first)
        dist/legacy_additions_report.txt
USAGE:  python src/apply_legacy_additions.py [--season N] [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST = REPO_ROOT / "dist"
CALC = DIST / "calculators"
IMAGES = DIST / "season_images"

REWARDS_TSV = TSV_DIR / "season_rewards.tsv"
LEGACY_TSV = TSV_DIR / "legacy_seasons.tsv"
REPORT = DIST / "legacy_additions_report.txt"

TAG = "[apply_legacy_additions]"

# EDID fragment -> (kind, tallyCategory). Titles and icons are what legacy
# re-runs actually add, and both need a kind for the renderer to label them.
KIND_RULES: list[tuple[str, str, str]] = [
    (r"playertitles_prefixsuffix_", "playerTitlePrefixSuffix", "camp_player_title"),
    (r"playertitles_prefix_",       "playerTitlePrefix",       "camp_player_title"),
    (r"playertitles_suffix_",       "playerTitleSuffix",       "camp_player_title"),
    (r"camptitles_prefix_",         "campTitlePrefix",         "camp_player_title"),
    (r"camptitles_suffix_",         "campTitleSuffix",         "camp_player_title"),
    (r"playericon_",                "playerIcon",              "player_icon"),
]


def read_tsv(path: Path) -> tuple[list[str], list[dict]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader.fieldnames or []), list(reader)


def write_tsv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def snake(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", name.lower())
    return re.sub(r"_+", "_", s).strip("_")


def rerun_labels(path: Path) -> dict[int, list[str]]:
    """Season number -> the re-run labels ('Sep 2026'), oldest first."""
    out: dict[int, list[str]] = {}
    if not path.exists():
        return out
    _, rows = read_tsv(path)
    for row in rows:
        try:
            num = int((row.get("SeasonNumber") or "").strip())
            start = datetime.strptime((row.get("StartDate") or "").strip(), "%d/%m/%Y")
        except (ValueError, TypeError):
            continue
        out.setdefault(num, []).append(start.strftime("%b %Y"))
    return out


def image_by_entitlement(season: int) -> dict[str, str]:
    """Entitlement -> the uploaded image URL, from the season image manifest.

    The manifest is the only thing that knows the real uploaded filename. The
    PTS export carries a .dds handle, and the upload is renamed after the
    curated row, so the .dds name is not a safe guess.
    """
    path = IMAGES / f"season_{season}_images.json"
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    out = {}
    for img in data.get("images", []):
        ent = (img.get("entitlement") or "").strip().lower()
        name = (img.get("outAvif") or "").strip()
        upload = (img.get("uploadTo") or "").strip()
        if ent and name and upload:
            out[ent] = upload + name
    return out


def classify(edid: str) -> tuple[str, str]:
    low = edid.lower()
    for pattern, kind, tally in KIND_RULES:
        if re.search(pattern, low):
            return kind, tally
    return "", ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, help="only process this season")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    fields, rows = read_tsv(REWARDS_TSV)
    for col in ("rank", "addedInRerun"):
        if col not in fields:
            fields.append(col)

    labels = rerun_labels(LEGACY_TSV)
    if not labels:
        print(f"{TAG} no legacy runs in {LEGACY_TSV.name} - nothing to compare")
        return 0

    seasons = [args.season] if args.season else sorted(labels)
    lines: list[str] = [f"{TAG} run {datetime.now().isoformat(timespec='seconds')}", ""]
    additions: list[dict] = []

    for season in seasons:
        pts_path = CALC / f"upcoming_rewards_s{season}.json"
        if not pts_path.exists():
            lines.append(f"Season {season}: no upcoming_rewards_s{season}.json - skipped")
            continue

        curated = [r for r in rows if (r.get("seasonNumber") or "").strip() == str(season)]
        if not curated:
            lines.append(f"Season {season}: no curated rows - skipped")
            continue

        have = {(r.get("storefrontEntitlement") or "").strip().lower()
                for r in curated if (r.get("storefrontEntitlement") or "").strip()}
        # Guard against re-running: an addition already written is already 'have'.
        pts = json.loads(pts_path.read_text(encoding="utf-8")).get("items", [])
        art = image_by_entitlement(season)
        label = labels[season][-1]

        new_rows = []
        for item in pts:
            edid = (item.get("edid") or "").strip()
            if not edid or edid.lower() in have:
                continue
            kind, tally = classify(edid)
            name = (item.get("name") or "").strip()
            new_rows.append({
                "seasonNumber": str(season),
                "id": f"S{season}_NEW_{snake(name)}",
                "page": "",
                "rank": "",
                "name": name,
                "cost": "0",
                "isFirst": "TRUE" if item.get("falloutFirst") else "",
                "kind": kind,
                "value": name if kind else "",
                "tallyCategory": tally,
                "imageUrl": art.get(edid.lower(), ""),
                "description": (item.get("description") or "").strip(),
                "storefrontEntitlement": edid,
                "reappearances": "",
                "addedInRerun": label,
            })

        lines.append(f"Season {season} (re-run {label}): "
                     f"{len(pts)} in PTS export, {len(curated)} curated, "
                     f"{len(new_rows)} added")
        for r in new_rows:
            art_note = "" if r["imageUrl"] else "   [no artwork]"
            kind_note = f"  ({r['kind']})" if r["kind"] else ""
            lines.append(f"    + {r['name']}{kind_note}{art_note}")
        additions.extend(new_rows)

    if not additions:
        lines.append("")
        lines.append("No additions found - every PTS entitlement already has a curated row.")

    # Insert each addition directly after the last row of its season so the TSV
    # stays grouped by season.
    for add in additions:
        season = add["seasonNumber"]
        last = max((i for i, r in enumerate(rows)
                    if (r.get("seasonNumber") or "").strip() == season), default=-1)
        rows.insert(last + 1, add)

    report = "\n".join(lines)
    DIST.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(report + "\n", encoding="utf-8")
    print(report)

    if args.dry_run:
        print(f"\n{TAG} dry run - {REWARDS_TSV.name} not written")
        return 0

    if additions:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(REWARDS_TSV, REWARDS_TSV.with_suffix(f".tsv.bak-{stamp}"))
        write_tsv(REWARDS_TSV, fields, rows)
        print(f"\n{TAG} wrote {REWARDS_TSV} (+{len(additions)} rows)")
    print(f"{TAG} report at {REPORT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
