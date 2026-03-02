#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ----------------------------
# Normalization helpers
# ----------------------------

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
        "Player Icon:",
        "CAMP Title Prefix:",
        "CAMP Title Suffix:",
        "Player Title Prefix:",
        "Player Title Suffix:",
        "Player Title Prefix/Suffix:",
    ]
    for p in prefixes:
        if s.startswith(p):
            s = s[len(p):].strip()
            break
    s = s.strip().strip('"').strip()
    return s


_QTY_TAIL_RE = re.compile(r"\s*x\s*\d+\s*$", re.IGNORECASE)   # "Re-Roller x5" -> "Re-Roller"
_LEADING_NUM_RE = re.compile(r"^\d+\s+")                     # "200 Atoms" -> "Atoms"


def strip_quantity(name: str) -> str:
    s = (name or "").strip()
    s = _QTY_TAIL_RE.sub("", s).strip()
    s = _LEADING_NUM_RE.sub("", s).strip()
    return s


# ----------------------------
# Path helpers
# ----------------------------

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
    """
    Turns an entitlement-ish identifier into the output webp filename.
    IMPORTANT: we intentionally allow slashes here (ex: "utility/foo")
    so the converter can write into subfolders.
    """
    k = (edid or "").strip().lower()
    k = k.replace("_entm_", "_")
    return f"{k}.webp"


# ----------------------------
# Utility mapping (auto-convert + auto-upload)
# ----------------------------

def utility_rule(raw_name: str) -> Optional[Tuple[str, str, Optional[str]]]:
    """
    Returns (imageUrl, manifestEntitlementId, ddsPathOrNone)

    manifestEntitlementId is intentionally "utility/<basename>" so that
    entitlement_to_webp_name() yields "utility/<basename>.webp".
    """
    root_url = "/wp-content/uploads/season_images/utility/"

    base = norm(strip_quantity(strip_prefixes(raw_name)))

    # ---- currencies (usually already present; DDS sources not provided) ----
    if base == "atoms":
        return (root_url + "score_currency_atoms.webp", "utility/score_currency_atoms", None)
    if base == "bullion":
        return (root_url + "score_currency_bullion.webp", "utility/score_currency_bullion", None)
    if base == "caps":
        return (root_url + "score_currency_caps.webp", "utility/score_currency_caps", None)
    if base in ("perk coin", "perk coins"):
        return (root_url + "score_currency_perkcoin.webp", "utility/score_currency_perkcoin", None)
    if base in ("legendary scrip", "scrip"):
        return (root_url + "score_currency_scrip.webp", "utility/score_currency_scrip", None)
    if base == "stamps":
        return (root_url + "score_currency_stamps.webp", "utility/score_currency_stamps", None)

    # ---- common utilities (DDS sources provided by you) ----
    # NOTE: We reference the *_l.dds you gave. If your converter expects non-_l,
    # fix it in the PowerShell, not here.
    if base in ("re-roller", "reroller", "re roller"):
        return (
            root_url + "score_utility_reroller.webp",
            "utility/score_utility_reroller",
            "textures/atx/storefront/utility/score_utility_reroller_l.dds",
        )

    if base in ("score booster", "scorebooster"):
        return (
            root_url + "score_utility_scorebooster.webp",
            "utility/score_utility_scorebooster",
            "textures/atx/storefront/utility/score_utility_scorebooster_l.dds",
        )

    if base in ("lunchbox", "lunch box", "lunchboxes", "lunch boxes"):
        return (
            root_url + "atx_store_lunchbox001.webp",
            "utility/atx_store_lunchbox001",
            "textures/atx/storefront/utility/atx_store_lunchbox001_l.dds",
        )

    if base in ("banner",):
        return (
            root_url + "score_coen_utility_banner.webp",
            "utility/score_coen_utility_banner",
            "textures/atx/storefront/utility/score_coen_utility_banner_l.dds",
        )

    if base in ("magazine and book box", "magazine book box", "magazinebookbox"):
        return (
            root_url + "score_utility_magazinebookbox.webp",
            "utility/score_utility_magazinebookbox",
            "textures/atx/storefront/utility/score_utility_magazinebookbox_l.dds",
        )

    if base in ("mystery bobblehead", "mysterybobblehead"):
        return (
            root_url + "score_utility_mysterybobblehead.webp",
            "utility/score_utility_mysterybobblehead",
            "textures/atx/storefront/utility/score_utility_mysterybobblehead_l.dds",
        )

    if base in ("basic repair kit", "repair kit", "repairkit"):
        return (
            root_url + "atx_utility_repairkit_basic.webp",
            "utility/atx_utility_repairkit_basic",
            "textures/atx/storefront/utility/atx_utility_repairkit_basic_l.dds",
        )

    if base in ("sugar-free nukashine", "sugar free nukashine", "nukashine sugarfree"):
        return (
            root_url + "score_item_nukashine_sugarfree.webp",
            "utility/score_item_nukashine_sugarfree",
            "textures/atx/storefront/utility/score_item_nukashine_sugarfree_l.dds",
        )

    # Existing ones you already had on-site
    if base in ("legendary module", "legendary modules"):
        return (root_url + "score_game_legendarymodule.webp", "utility/score_game_legendarymodule", None)
    if base in ("carry weight booster", "carryweight booster"):
        return (root_url + "score_utility_carryweight.webp", "utility/score_utility_carryweight", None)
    if base in ("improved bait",):
        return (root_url + "score_utility_improvedbait.webp", "utility/score_utility_improvedbait", None)
    if base in ("superb bait", "superb-bait"):
        return (
            root_url + "score_utility_superbait.webp",
            "utility/score_utility_superbait",
            "textures/atx/storefront/utility/score_utility_superbbait_l.dds",
        )

    return None


# ----------------------------
# Main
# ----------------------------

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
    rows: List[Dict[str, str]] = []
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

    # Lookup by FULL and NNAM
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

    matched_entm = 0
    matched_utility = 0

    def add_manifest_pair(ent_id: str, dds_path: str) -> None:
        ent_list.append(ent_id)
        dds_list.append(dds_path)

    for it in items:
        raw_name = str(it.get("name") or "").strip()
        if not raw_name:
            continue

        # 1) Utility override (sets imageUrl AND includes DDS in manifest if we know it)
        u = utility_rule(raw_name)
        if u:
            image_url, manifest_id, dds_path = u
            it["imageUrl"] = image_url

            if dds_path:
                add_manifest_pair(manifest_id, dds_path)
                matched_utility += 1

            continue

        # 2) ENTM match (season-specific storefront images)
        base0 = raw_name
        base1 = strip_prefixes(raw_name)
        base2 = strip_quantity(base1)

        n0 = norm(base0)
        n1 = norm(base1)
        n2 = norm(base2)

        hit = by_name.get(n0) or by_name.get(n1) or by_name.get(n2)

        # safe contains-pass, only if unique
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
        it["imageUrl"] = (args.img_url_root.rstrip("/") + "/" + entitlement_to_webp_name(edid))

        add_manifest_pair(edid, dds)
        matched_entm += 1

    manifest = {
        "type": "season-ticket-images",
        "seasonNumber": season_num,
        "tasks": [
            {
                "entitlementEdids": ent_list,
                "ddsPaths": dds_list,
            }
        ],
    }

    Path(args.out_manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_season_json).parent.mkdir(parents=True, exist_ok=True)

    Path(args.out_manifest).write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    Path(args.out_season_json).write_text(json.dumps(season, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"[OK] Season {season_num}: matched ENTM={matched_entm}, utilityDDS={matched_utility}")
    print(f"[OK] Wrote manifest: {args.out_manifest}")
    print(f"[OK] Wrote season json: {args.out_season_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())