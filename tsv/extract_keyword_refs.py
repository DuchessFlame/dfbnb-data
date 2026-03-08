#!/usr/bin/env python3
"""
Extract specific keyword references from the massive KYWD TSV into a small JSON.

The full KYWD export is ~457 MB (44,553 columns) because system keywords like
MultirefLOD have tens of thousands of refs. This script streams it once and
extracts only the keywords needed for collectable/display pages into a single
JSON file small enough to commit to GitHub.

Run locally after each xEdit KYWD export:

    python extract_keyword_refs.py --kywd tsv/KYWD_Export_March_2026.tsv --out tsv/keyword_refs.json

The output is used by build_collectables_json.py (and future build scripts)
instead of the full KYWD TSV.

Output format:
{
  "extractedAt": "2026-03-08T12:00:00+00:00",
  "source": "KYWD_Export_March_2026.tsv",
  "keywords": {
    "006A57A2": {
      "edid": "PlushiesKeyword",
      "full": "",
      "refCount": 137,
      "refs": [
        {"formId": "00059B14", "edid": "Plushie_TeddyBear", "type": "MISC"},
        ...
      ]
    },
    ...
  }
}
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


# ============================================================
# Keywords to extract.
# Add new FormIDs here as you build more collectable pages.
# ============================================================
TARGET_KEYWORDS = {
    # Plushies
    "006A57A2": "PlushiesKeyword",
    "007331D8": "LargePlushiesKeyword",
    "0074FC35": "MediumPlushiesKeyword",

    # Bobbleheads
    "00135E6C": "BobbleheadKeyword",
    "00436F6C": "BobbleheadDispelEffects",

    # Magazines
    "001D4A70": "MagazineKeyword",
    "0043AAE3": "SpawnManagedMagazineKeyword",
    "00436F74": "MagazineDispelEffects",
    "003F5165": "BabylonMagazine",
    "004907B8": "MagazineTypeBedtime",
    "004907B9": "MagazineTypeAwesome",
    "004907BA": "MagazineTypeGrognak",
    "004907BB": "MagazineTypeLiveAndLove",
    "004907BC": "MagazineTypeScout",
    "004907BD": "MagazineTypeTumblers",
    "004907BE": "MagazineTypeUnstoppables",
    "00485281": "MagazineTypeBackwoodsman",
    "004852A2": "MagazineTypeTeslaScience",
    "00485292": "MagazineTypeGunsAndBullets",
    "004852AE": "MagazineTypeUSCovertOperations",
    "003F0B79": "MagazineTypeGrognak08",
    "00391F15": "BACKUP_EffectTypeMagazine",

    # Bobblehead types (individual)
    "00494A1E": "BobbleheadTypeAgility",
    "00494A1D": "BobbleheadTypeBigGuns",
    "00494A1C": "BobbleheadTypeCaps",
    "00494A1B": "BobbleheadTypeCharisma",
    "00494A1A": "BobbleheadTypeEndurance",
    "00494A19": "BobbleheadTypeEnergy",
    "00494A18": "BobbleheadTypeExplosive",
    "00494A17": "BobbleheadTypeIntelligence",
    "00494A16": "BobbleheadTypeLeader",
    "00494A15": "BobbleheadTypeLockpicking",
    "00494A14": "BobbleheadTypeLuck",
    "00494A13": "BobbleheadTypeMedicine",
    "00494A12": "BobbleheadTypeMelee",
    "00494A11": "BobbleheadTypePerception",
    "00494A10": "BobbleheadTypeRepair",
    "00494A0F": "BobbleheadTypeScience",
    "00494A0E": "BobbleheadTypeSmallGuns",
    "00494A0D": "BobbleheadTypeSneak",
    "00494A0C": "BobbleheadTypeStrength",
    "00494A0B": "BobbleheadTypeUnarmed",

    # Holotapes
    "002FB570": "qgHolotapes",
    "00854EAA": "Burn_SQ04_HolotapeKeyword",

    # Display keywords
    "00415577": "DisplayTypePlushCat",

    # Misc backup/dispel
    "00391F16": "BACKUP_EffectTypeBobblehead",
}


def extract_keywords(kywd_path, target_formids):
    """
    Stream the KYWD TSV once and extract all target keywords.
    Returns dict of formid -> {edid, full, refCount, refs[]}.
    """
    target_set = {fid.upper() for fid in target_formids}
    results = {}

    with open(kywd_path, 'r', encoding='utf-8', errors='replace') as f:
        header_line = f.readline().rstrip('\n').rstrip('\r')
        columns = header_line.split('\t')

        # Find key column indices
        col_idx = {}
        for i, col in enumerate(columns):
            c = col.strip()
            if c in ('FormID', 'EDID', 'FULL_Name', 'RefCount'):
                col_idx[c] = i

        formid_idx = col_idx.get('FormID')
        edid_idx = col_idx.get('EDID')
        full_idx = col_idx.get('FULL_Name')
        refcount_idx = col_idx.get('RefCount')

        if formid_idx is None:
            print("ERROR: No FormID column found in KYWD TSV", file=sys.stderr)
            return {}

        # Build list of Ref column indices (Ref1, Ref2, ...)
        ref_col_indices = []
        for i, col in enumerate(columns):
            if re.match(r'^Ref\d+$', col.strip()):
                ref_col_indices.append(i)

        found_count = 0
        row_num = 0

        for line in f:
            row_num += 1
            if row_num % 2000 == 0:
                print(f"  Scanned {row_num} rows, found {found_count}/{len(target_set)}...",
                      file=sys.stderr)

            # Quick check: only split if FormID prefix could match
            # (optimization: check the first field before full split)
            tab_pos = line.find('\t')
            if tab_pos > 0:
                raw_fid = line[:tab_pos].strip().upper()
            else:
                raw_fid = line.strip().upper()

            if raw_fid not in target_set:
                continue

            # Full parse for matching rows
            fields = line.rstrip('\n').rstrip('\r').split('\t')
            fid = fields[formid_idx].strip().upper() if formid_idx < len(fields) else ""

            if fid not in target_set:
                continue

            edid = fields[edid_idx].strip() if edid_idx is not None and edid_idx < len(fields) else ""
            full = fields[full_idx].strip() if full_idx is not None and full_idx < len(fields) else ""

            try:
                rc = int(fields[refcount_idx].strip()) if refcount_idx is not None and refcount_idx < len(fields) else 0
            except ValueError:
                rc = 0

            # Extract refs
            refs = []
            for ri in ref_col_indices:
                if ri >= len(fields):
                    break
                val = fields[ri].strip()
                if not val:
                    continue
                parts = val.split(':')
                if parts and re.fullmatch(r'[0-9A-Fa-f]{8}', parts[0]):
                    refs.append({
                        "formId": parts[0],
                        "edid": parts[1] if len(parts) >= 2 else "",
                        "type": parts[2] if len(parts) >= 3 else ""
                    })

            results[fid] = {
                "edid": edid,
                "full": full,
                "refCount": rc,
                "refs": refs
            }

            found_count += 1
            print(f"  Found {edid} ({fid}): {len(refs)} refs", file=sys.stderr)

            # Early exit if we've found all targets
            if found_count >= len(target_set):
                break

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Extract collectable keyword refs from KYWD TSV into a small JSON'
    )
    parser.add_argument('--kywd', required=True, help='Path to KYWD_Export*.tsv')
    parser.add_argument('--out', required=True,
                        help='Output JSON path (e.g. tsv/keyword_refs.json)')
    parser.add_argument('--extra-formids', nargs='*', default=[],
                        help='Additional keyword FormIDs to extract (8 hex chars each)')

    args = parser.parse_args()

    kywd_path = Path(args.kywd)
    if not kywd_path.exists():
        print(f"ERROR: KYWD file not found: {kywd_path}", file=sys.stderr)
        sys.exit(1)

    # Merge built-in targets with any extras from CLI
    all_targets = dict(TARGET_KEYWORDS)
    for fid in args.extra_formids:
        fid_clean = fid.strip().upper()
        if re.fullmatch(r'[0-9A-F]{8}', fid_clean):
            if fid_clean not in all_targets:
                all_targets[fid_clean] = f"Custom_{fid_clean}"
        else:
            print(f"WARNING: Skipping invalid FormID: {fid}", file=sys.stderr)

    print(f"Streaming {kywd_path.name} for {len(all_targets)} keywords...", file=sys.stderr)
    keywords = extract_keywords(kywd_path, all_targets)

    # Report missing
    found_set = set(keywords.keys())
    missing = set(k.upper() for k in all_targets.keys()) - found_set
    if missing:
        print(f"\nWARNING: {len(missing)} keywords not found in TSV:", file=sys.stderr)
        for fid in sorted(missing):
            print(f"  {fid} ({all_targets.get(fid, '?')})", file=sys.stderr)

    output = {
        "extractedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source": kywd_path.name,
        "keywords": keywords
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total_refs = sum(len(kw['refs']) for kw in keywords.values())
    file_size = out_path.stat().st_size

    print(f"\nDone: {len(keywords)} keywords, {total_refs} total refs", file=sys.stderr)
    print(f"Saved to {out_path} ({file_size / 1024:.1f} KB)", file=sys.stderr)


if __name__ == '__main__':
    main()
