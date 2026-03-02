#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r'["“”\'`]', "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def strip_prefixes(name: str) -> str:
    s = (name or "").strip()
    # Common prefixes you use in season json
    prefixes = [
        "Player Icon:", "CAMP Title Prefix:", "CAMP Title Suffix:",
        "Player Title Prefix:", "Player Title Suffix:", "Player Title Prefix/Suffix:"
    ]
    for p in prefixes:
        if s.startswith(p):
            s = s[len(p):].strip()
            break
    # Remove surrounding quotes
    s = s.strip().strip('"').strip()
    return s

def to_dds_path(etip: str, etdi: str) -> str:
    # ENTM gives: ETIP="Textures/ATX/Storefront/Player/PlayerIcons/"
    #             ETDI="ATX_PlayerIcon_S24_SpaceCow.dds"
    p = (etip or "").strip().replace("\\", "/") + (etdi or "").strip().replace("\\", "/")
    p = p.lstrip("/")
    # extractor expects "textures/..." style keys
    if p.lower().startswith("textures/"):
        p = "textures/" + p[9:]
    return p.lower()

def entitlement_to_webp_name(edid: str) -> str:
    k = (edid or "").strip().lower()
    k = k.replace("_entm_", "_")
    return f"{k}.webp"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season-json", required=True)
    ap.add_argument("--entm-tsv", required=True)
    ap.add_argument("--out-manifest", required=True)
    ap.add_argument("--out-season-json", required=True)
    ap.add_argument("--img-url-root", default="/wp-content/uploads/season_images/")
    args = ap.parse_args()

    season_path = Path(args.season_json)
    entm_path = Path(args.entm_tsv)

    season = json.loads(season_path.read_text(encoding="utf-8"))
    season_num = int(season.get("seasonNumber") or 0)
    if not season_num:
        raise SystemExit("seasonNumber missing/invalid in season JSON")

    prefix = f"score_s{season_num}_"

    # Read ENTM TSV (xEdit exports are often latin1/cp1252-ish)
    rows = []
    with entm_path.open("r", encoding="latin1", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            edid = (r.get("EDID") or "").strip()
            if not edid:
                continue
            if not edid.lower().startswith(prefix):
                continue
            # Must have storefront image info
            if not (r.get("ETIP") or "").strip():
                continue
            if not (r.get("ETDI") or "").strip():
                continue
            rows.append(r)

    # Build a lookup by FULL and NNAM (Display Name)
    by_name: Dict[str, Dict[str, str]] = {}
    for r in rows:
        full = norm(r.get("FULL") or "")
        nnam = norm(r.get("NNAM") or "")
        if full and full not in by_name:
            by_name[full] = r
        if nnam and nnam not in by_name:
            by_name[nnam] = r

    items: List[Dict] = list(season.get("items") or [])
    ent_list: List[str] = []
    dds_list: List[str] = []

    matched = 0
    for it in items:
        raw_name = str(it.get("name") or "").strip()
        if not raw_name:
            continue

        n0 = norm(raw_name)
        n1 = norm(strip_prefixes(raw_name))

        hit = by_name.get(n0) or by_name.get(n1)

        # If still not found, try a “contains” pass (safe, but only if unique)
        if not hit:
            candidates = []
            for k, r in by_name.items():
                if n1 and (k == n1 or n1 in k or k in n1):
                    candidates.append(r)
            # Use only if exactly one match
            if len(candidates) == 1:
                hit = candidates[0]

        if not hit:
            continue

        edid = (hit.get("EDID") or "").strip()
        dds = to_dds_path(hit.get("ETIP") or "", hit.get("ETDI") or "")
        if not edid or not dds:
            continue

        it["storefrontEntitlement"] = edid
        it["imageUrl"] = (args.img_url_root.rstrip("/") + "/" + entitlement_to_webp_name(edid))

        ent_list.append(edid)
        dds_list.append(dds)
        matched += 1

    # Write manifest for the existing extractor script:
    # One task with equal-length arrays = deterministic index pairing.
    manifest = {
        "type": "season-ticket-images",
        "seasonNumber": season_num,
        "tasks": [
            {
                "entitlementEdids": ent_list,
                "ddsPaths": dds_list
            }
        ]
    }

    Path(args.out_manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_season_json).parent.mkdir(parents=True, exist_ok=True)

    Path(args.out_manifest).write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    Path(args.out_season_json).write_text(json.dumps(season, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"[OK] Season {season_num}: matched {matched} items to ENTM storefront images")
    print(f"[OK] Wrote manifest: {args.out_manifest}")
    print(f"[OK] Wrote season json: {args.out_season_json}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())