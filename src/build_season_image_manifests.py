#!/usr/bin/env python3
"""
build_season_image_manifests.py
-------------------------------
Generative per-season image manifests for the /df/scoreboards/season-{N}/ pages.

build_season_ticket_images.py only ever handled ONE season (whichever was in
dist/calculators/season_tickets.json). This does every curated season at once and
writes a manifest per season, so seasons 1-23 can be back-filled with art without
hand-listing a single texture path.

For each season it joins:
    dist/calculators/season_tickets_s{N}.json   (the curated reward list)
  x tsv/ENTM_Export_*.tsv                        (EDID -> ETIP + ETDI texture path)

and emits, per reward:
    entitlement   SCORE_S12_ENTM_CAMP_AidBox_Alien
    ddsPath       textures/atx/storefront/camp/aidbox/score_s12_camp_aidbox_alien.dds
    outAvif       score_s12_camp_aidbox_alien.avif
    uploadTo      /wp-content/uploads/season_images/season-12/

The outAvif name is the same rule the site already uses - see
df-bnb-seasons.js resolveImageUrl(), which rewrites the reward imageUrl
    /season_images/score_s{N}_*.webp  ->  /season_images/season-{N}/*.avif
so no JSON needs editing once the art lands.

NOTE ON SEASON NUMBERING: seasons 1-9 are NOT zero padded in the ENTM export.
The EDIDs are SCORE_S1_ENTM_*, not SCORE_S01_ENTM_*. Matching on "SCORE_S0\\d"
silently finds nothing for the first nine seasons.

STATUS: active
INPUT:  dist/calculators/season_tickets_s*.json, tsv/ENTM_Export_*.tsv
OUTPUT: dist/season_images/season_{N}_images.json
        dist/season_images/coverage.json
USAGE:  python src/build_season_image_manifests.py
        python src/build_season_image_manifests.py --season 12
        python src/build_season_image_manifests.py --entm-tsv tsv/ENTM_Export_July_2026.tsv
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
CALC_DIR = REPO_ROOT / "dist" / "calculators"
OUT_DIR = REPO_ROOT / "dist" / "season_images"

TAG = "[build_season_image_manifests]"

# Seasons 1-9 are unpadded (SCORE_S1_ENTM_), 10+ are plain (SCORE_S23_ENTM_).
# The optional zzz prefix marks a cut/deprecated record; keep those so the
# manifest still explains where a blank-name reward came from.
EDID_RE = re.compile(r"^(?:zzz_?)?SCORE_S(\d+)_ENTM_(.+)$", re.IGNORECASE)

UPLOAD_ROOT = "/wp-content/uploads/season_images"


def newest_entm_tsv() -> Path:
    """Pick the ENTM export with the latest Month_Year in its filename."""
    months = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
    }
    best = None
    for p in TSV_DIR.glob("ENTM_Export_*.tsv"):
        parts = re.split(r"[_.]", p.name)
        m = y = 0
        for seg in parts:
            low = seg.lower()
            if low in months:
                m = months[low]
            elif re.fullmatch(r"\d{4}", seg):
                y = int(seg)
        key = (y, m, p.name)
        if best is None or key > best[0]:
            best = (key, p)
    if best is None:
        sys.exit(f"{TAG} [ERROR] No tsv/ENTM_Export_*.tsv found")
    return best[1]


def entitlement_to_avif(edid: str) -> str:
    """SCORE_S12_ENTM_CAMP_AidBox -> score_s12_camp_aidbox.avif

    Mirrors entitlement_to_webp_name() in build_season_ticket_images.py, with the
    "zzz" deprecation prefix stripped so cut records resolve to a real texture name.
    """
    k = (edid or "").strip().lower()
    k = re.sub(r"^zzz+_?", "", k)
    k = k.replace("_entm_", "_")
    return f"{k}.avif" if k else ""


def dds_path(etip: str, etdi: str) -> str:
    """Join the ENTM texture-in-path (ETIP, a folder) and texture filename (ETDI).

    BOTH halves are required. Plenty of ENTM rows carry a folder with no filename;
    accepting those yields a directory masquerading as a texture path, which then
    silently fails at extraction time. Return "" so the caller reports it as
    missing instead.
    """
    etip = (etip or "").strip().replace("\\", "/")
    etdi = (etdi or "").strip().replace("\\", "/")
    if not etip or not etdi:
        return ""
    p = (etip.rstrip("/") + "/" + etdi.lstrip("/")).lstrip("/")
    return p.lower()


def load_entm(path: Path) -> dict[int, dict[str, dict]]:
    """Return {seasonNum: {edid_lower: {'edid','full','dds'}}}."""
    by_season: dict[int, dict[str, dict]] = {}
    with path.open("r", encoding="latin1", errors="replace", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            edid = (row.get("EDID") or "").strip()
            m = EDID_RE.match(edid)
            if not m:
                continue
            season = int(m.group(1))
            by_season.setdefault(season, {})[edid.lower()] = {
                "edid": edid,
                "full": (row.get("FULL") or "").strip(),
                "dds": dds_path(row.get("ETIP", ""), row.get("ETDI", "")),
            }
    return by_season


def curated_seasons() -> list[int]:
    nums = []
    for p in CALC_DIR.glob("season_tickets_s*.json"):
        m = re.fullmatch(r"season_tickets_s(\d+)\.json", p.name)
        if m:
            nums.append(int(m.group(1)))
    return sorted(nums)


def build_manifest(season: int, entm: dict[str, dict]) -> dict:
    src = CALC_DIR / f"season_tickets_s{season}.json"
    data = json.loads(src.read_text(encoding="utf-8"))
    items = data.get("items") or []

    entries: list[dict] = []
    no_entitlement: list[str] = []
    no_texture: list[str] = []

    for it in items:
        img = it.get("imageUrl") or ""
        # Shared utility/currency art is season-agnostic and already uploaded once
        # to /season_images/utility/ - never re-extract it per season.
        if "/utility/" in img:
            continue

        ent = (it.get("storefrontEntitlement") or "").strip()
        if not ent:
            no_entitlement.append(it.get("name") or it.get("id") or "?")
            continue

        rec = entm.get(ent.lower())
        out_avif = entitlement_to_avif(ent)

        if not rec or not rec.get("dds"):
            no_texture.append(ent)
            continue

        entries.append({
            "id": it.get("id", ""),
            "name": it.get("name", ""),
            "entitlement": ent,
            "ddsPath": rec["dds"],
            "outAvif": out_avif,
            "uploadTo": f"{UPLOAD_ROOT}/season-{season}/",
        })

    return {
        "_generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_note": (
            "Extract each ddsPath from the game archives (BAE), convert to AVIF "
            "(texconv -ft png, then avifenc --min 0 --max 20), name the output "
            "outAvif, then upload with: sync_season_images_to_site.ps1 -Season "
            f"{season}"
        ),
        "seasonNumber": season,
        "seasonName": data.get("seasonName", f"Season {season}"),
        "uploadTo": f"{UPLOAD_ROOT}/season-{season}/",
        "imageCount": len(entries),
        "itemsWithoutEntitlement": no_entitlement,
        "itemsWithoutTexturePath": no_texture,
        "images": entries,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, help="Only build this season.")
    ap.add_argument("--entm-tsv", help="Override the ENTM export to read.")
    args = ap.parse_args()

    entm_path = Path(args.entm_tsv) if args.entm_tsv else newest_entm_tsv()
    if not entm_path.exists():
        sys.exit(f"{TAG} [ERROR] Missing ENTM export: {entm_path}")
    print(f"{TAG} ENTM export: {entm_path.name}")

    entm = load_entm(entm_path)
    print(f"{TAG} ENTM seasons found: {len(entm)}")

    seasons = [args.season] if args.season else curated_seasons()
    if not seasons:
        sys.exit(f"{TAG} [ERROR] No season_tickets_s*.json in {CALC_DIR}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    coverage = []
    for n in seasons:
        if not (CALC_DIR / f"season_tickets_s{n}.json").exists():
            print(f"{TAG} [WARN] No curated data for S{n} - skipping")
            continue

        man = build_manifest(n, entm.get(n, {}))
        out = OUT_DIR / f"season_{n}_images.json"
        out.write_text(json.dumps(man, indent=2, ensure_ascii=False) + "\n",
                       encoding="utf-8")

        missing = len(man["itemsWithoutTexturePath"])
        flag = f"  [{missing} without texture path]" if missing else ""
        print(f"{TAG} S{n:<3} {man['imageCount']:>3} textures -> {out.name}{flag}")

        coverage.append({
            "seasonNumber": n,
            "seasonName": man["seasonName"],
            "imageCount": man["imageCount"],
            "withoutTexturePath": missing,
            "withoutEntitlement": len(man["itemsWithoutEntitlement"]),
            "manifest": out.name,
        })

    cov_path = OUT_DIR / "coverage.json"
    cov_path.write_text(json.dumps({
        "_generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source": entm_path.name,
        "totalImages": sum(c["imageCount"] for c in coverage),
        "seasons": coverage,
    }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"{TAG} Written: {cov_path.name}  "
          f"({sum(c['imageCount'] for c in coverage)} textures across "
          f"{len(coverage)} seasons)")
    print(f"{TAG} Done.")


if __name__ == "__main__":
    main()
