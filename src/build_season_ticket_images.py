#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List


def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r'["“”\'`]', "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def strip_prefixes(name: str) -> str:
    s = (name or "").strip()
    prefixes = [
        "Player Icon:", "CAMP Title Prefix:", "CAMP Title Suffix:",
        "Player Title Prefix:", "Player Title Suffix:", "Player Title Prefix/Suffix:"
    ]
    for p in prefixes:
        if s.startswith(p):
            s = s[len(p):].strip()
            break
    s = s.strip().strip('"').strip()
    return s


_QTY_TAIL_RE = re.compile(r"\s*x\s*\d+\s*$", re.IGNORECASE)   # "Re-Roller x5" -> "Re-Roller"
_LEADING_NUM_RE = re.compile(r"^\d+\s+")                       # "200 Atoms" -> "Atoms"


def strip_quantity(name: str) -> str:
    s = (name or "").strip()
    s = _QTY_TAIL_RE.sub("", s).strip()
    s = _LEADING_NUM_RE.sub("", s).strip()
    return s


# The one routing rule, shared with the renderers. See src/asset_paths.py.
from asset_paths import asset_url  # noqa: E402


def utility_image_url(raw_name: str) -> str:
    # Season-agnostic icons hosted in /season_images/utility/
    root = "/wp-content/uploads/season_images/utility/"
    base = norm(strip_quantity(strip_prefixes(raw_name)))

    # Currencies
    if base == "atoms":
        return root + "score_currency_atoms.avif"
    if base == "bullion":
        return root + "score_currency_bullion.avif"
    if base == "caps":
        return root + "score_currency_caps.avif"
    if base in ("perk coin", "perk coins"):
        return root + "score_currency_perkcoin.avif"
    if base in ("legendary scrip", "scrip"):
        return root + "score_currency_scrip.avif"
    if base == "stamps":
        return root + "score_currency_stamps.avif"

    # Utilities
    if base in ("legendary module", "legendary modules"):
        return root + "score_game_legendarymodule.avif"
    if base in ("carry weight booster", "carryweight booster"):
        return root + "score_utility_carryweight.avif"
    if base == "improved bait":
        return root + "score_utility_improvedbait.avif"
    if base in ("superb bait", "superb-bait"):
        return root + "score_utility_superbait.avif"
    if base in ("re-roller", "reroller", "re roller"):
        return root + "score_utility_reroller.avif"
    if base in ("score booster", "scorebooster"):
        return root + "score_utility_scorebooster.avif"
    if base in ("lunchbox", "lunch box", "lunchboxes", "lunch boxes"):
        return root + "atx_store_lunchbox001.avif"
    if base == "banner":
        return root + "score_coen_utility_banner.avif"
    if base in ("magazine and book box", "magazine book box", "magazinebookbox"):
        return root + "score_utility_magazinebookbox.avif"
    if base in ("mystery bobblehead", "mysterybobblehead"):
        return root + "score_utility_mysterybobblehead.avif"
    if base in ("basic repair kit", "repair kit", "repairkit"):
        return root + "atx_utility_repairkit_basic.avif"
    if base in ("sugar-free nukashine", "sugar free nukashine", "nukashine sugarfree"):
        return root + "score_item_nukashine_sugarfree.avif"

    return ""


def to_dds_path(etip: str, etdi: str) -> str:
    p = (etip or "").strip().replace("\\", "/") + (etdi or "").strip().replace("\\", "/")
    p = p.lstrip("/")
    if p.lower().startswith("textures/"):
        p = "textures/" + p[9:]
    return p.lower()


def entitlement_to_webp_name(edid: str) -> str:
    k = (edid or "").strip().lower()
    k = k.replace("_entm_", "_")
    return f"{k}.avif"


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

    rows: List[dict] = []
    with entm_path.open("r", encoding="latin1", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            edid = (r.get("EDID") or "").strip()
            if not edid:
                continue
            if not edid.lower().startswith(prefix):
                continue
            if not (r.get("ETIP") or "").strip():
                continue
            if not (r.get("ETDI") or "").strip():
                continue
            rows.append(r)

    # Hard override for specific Player Icons that fuzzy match sometimes skips
    ICON_EDID_OVERRIDES = {
        "space cow": "SCORE_S24_ENTM_PlayerIcon_SpaceCow",
        "alien patch": "SCORE_S24_ENTM_PlayerIcon_AlienPatch",
        "abduction": "SCORE_S24_ENTM_PlayerIcon_UFOAbduction",
        "guinevere in space": "SCORE_S24_ENTM_PlayerIcon_SpaceGuinevere",
        # TSV has typo "Beckly" instead of "Beckley" so fuzzy match fails
        "taxidermy beast of beckley": "SCORE_S24_ENTM_CAMP_FloorDecor_TaxidermyBeast",
    }

    by_name: Dict[str, dict] = {}
    for r in rows:
        full = norm(r.get("FULL") or "")
        nnam = norm(r.get("NNAM") or "")
        if full and full not in by_name:
            by_name[full] = r
        if nnam and nnam not in by_name:
            by_name[nnam] = r

    items: List[dict] = list(season.get("items") or [])
    ent_list: List[str] = []
    dds_list: List[str] = []
    matched = 0

    for it in items:
        raw_name = str(it.get("name") or "").strip()
        if not raw_name:
            continue

        # Utility override: imageUrl only (conversion handled by run_season_ticket_images.ps1 $UtilityIcons list)
        u = utility_image_url(raw_name)
        if u:
            it["imageUrl"] = asset_url(u)
            continue

        base0 = raw_name
        base1 = strip_prefixes(raw_name)
        base2 = strip_quantity(base1)

        n0 = norm(base0)
        n1 = norm(base1)
        n2 = norm(base2)

        # First check hard EDID overrides
        override_key = n1
        override_edid = ICON_EDID_OVERRIDES.get(override_key)

        hit = None
        if override_edid:
            for r in rows:
                if (r.get("EDID") or "").strip().lower() == override_edid.lower():
                    hit = r
                    break
        else:
            hit = by_name.get(n0) or by_name.get(n1) or by_name.get(n2)

        if not hit and n1:
            candidates = []
            for k, r in by_name.items():
                if k == n1 or n1 in k or k in n1:
                    candidates.append(r)
            if len(candidates) == 1:
                hit = candidates[0]

        if not hit:
            continue

        edid = (hit.get("EDID") or "").strip()
        dds = to_dds_path(hit.get("ETIP") or "", hit.get("ETDI") or "")
        if not edid or not dds:
            continue

        it["storefrontEntitlement"] = edid
        # Route it, do not just concatenate. A title, icon or utility reward
        # belongs in its shared folder, not under this season - which is what
        # a bare join produced, and why those rewards 404'd on every page.
        it["imageUrl"] = asset_url(
            args.img_url_root.rstrip("/") + "/" + entitlement_to_webp_name(edid)
        )

        # Optional: attach ENTM description (for showing above image on the Season Ticket page)
        desc = (hit.get("DESC") or "").strip()
        if desc:
            it["description"] = desc
        ent_list.append(edid)
        dds_list.append(dds)
        matched += 1

    manifest = {
        "type": "season-ticket-images",
        "seasonNumber": season_num,
        "tasks": [{"entitlementEdids": ent_list, "ddsPaths": dds_list}],
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
