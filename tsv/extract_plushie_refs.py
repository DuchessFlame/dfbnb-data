#!/usr/bin/env python3
"""
Extract PlushiesKeyword references from the massive KYWD TSV into a tiny JSON.

Run this locally after each xEdit KYWD export. The output file is small enough
to commit to GitHub, so the CI build script never needs the full KYWD TSV.

Usage:
    python extract_plushie_refs.py --kywd tsv/KYWD_Export_March_2026.tsv --out tsv/plushies_keyword_refs.json

The output JSON looks like:
{
  "keyword": "006A57A2",
  "edid": "PlushiesKeyword",
  "refs": [
    {"formId": "00059B14", "edid": "Plushie_TeddyBear", "type": "MISC"},
    {"formId": "0008906C", "edid": "Plushie_TeddyBear_BabyTeddy", "type": "MISC"},
    ...
  ]
}
"""

import argparse
import json
import re
import sys
from pathlib import Path

PLUSHIES_KEYWORD_FORMID = "006A57A2"


def extract_refs(kywd_path, target_formid):
    """
    Stream the KYWD TSV line-by-line, find the target row, and extract all Ref fields.
    Returns (edid, refs_list) or (None, []) if not found.
    """
    with open(kywd_path, 'r', encoding='utf-8', errors='replace') as f:
        header_line = f.readline().rstrip('\n').rstrip('\r')
        columns = header_line.split('\t')

        # Find FormID column index
        formid_idx = None
        edid_idx = None
        for i, col in enumerate(columns):
            if col.strip() == 'FormID':
                formid_idx = i
            elif col.strip() == 'EDID':
                edid_idx = i

        if formid_idx is None:
            print("ERROR: No FormID column found in KYWD TSV", file=sys.stderr)
            return None, []

        target_upper = target_formid.strip().upper()

        for line in f:
            fields = line.rstrip('\n').rstrip('\r').split('\t')
            if len(fields) <= formid_idx:
                continue
            if fields[formid_idx].strip().upper() != target_upper:
                continue

            # Found the target row
            row_edid = fields[edid_idx].strip() if edid_idx is not None and edid_idx < len(fields) else ""

            refs = []
            for i, col_name in enumerate(columns):
                # Only process Ref1, Ref2, ... (skip RefCount)
                if not re.match(r'^Ref\d+$', col_name):
                    continue
                if i >= len(fields):
                    continue
                val = fields[i].strip()
                if not val:
                    continue

                # Format: "006A57C0:ATX_Plushie_Alien_Misc:MISC"
                parts = val.split(':')
                if len(parts) >= 1 and re.fullmatch(r'[0-9A-Fa-f]{8}', parts[0]):
                    ref = {
                        "formId": parts[0],
                        "edid": parts[1] if len(parts) >= 2 else "",
                        "type": parts[2] if len(parts) >= 3 else ""
                    }
                    refs.append(ref)

            return row_edid, refs

    return None, []


def main():
    parser = argparse.ArgumentParser(
        description='Extract PlushiesKeyword refs from KYWD TSV into a small JSON'
    )
    parser.add_argument('--kywd', required=True, help='Path to KYWD_Export*.tsv')
    parser.add_argument('--out', required=True, help='Output JSON path (e.g. tsv/plushies_keyword_refs.json)')
    parser.add_argument('--formid', default=PLUSHIES_KEYWORD_FORMID,
                        help=f'Keyword FormID to extract (default: {PLUSHIES_KEYWORD_FORMID})')

    args = parser.parse_args()

    kywd_path = Path(args.kywd)
    if not kywd_path.exists():
        print(f"ERROR: KYWD file not found: {kywd_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Streaming {kywd_path.name} for FormID {args.formid}...")
    edid, refs = extract_refs(kywd_path, args.formid)

    if edid is None:
        print(f"ERROR: FormID {args.formid} not found in {kywd_path.name}", file=sys.stderr)
        sys.exit(1)

    output = {
        "keyword": args.formid.upper(),
        "edid": edid,
        "refs": refs
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    misc_count = sum(1 for r in refs if r.get('type') == 'MISC')
    print(f"Done: {len(refs)} refs extracted ({misc_count} MISC items)")
    print(f"Saved to {out_path}")
    print(f"File size: {out_path.stat().st_size / 1024:.1f} KB")


if __name__ == '__main__':
    main()
