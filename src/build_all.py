import csv
import json
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist"

def tsv_to_json(tsv_path: Path, json_path: Path) -> None:
    with tsv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)

    json_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as out:
        json.dump(rows, out, ensure_ascii=False, indent=2)

def main() -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    tsv_files = sorted(TSV_DIR.glob("*.tsv"))
    if not tsv_files:
        raise SystemExit(f"No TSV files found in {TSV_DIR}")

    for tsv_path in tsv_files:
        json_name = tsv_path.stem + ".json"
        json_path = DIST_DIR / json_name
        print(f"Converting {tsv_path.name} -> dist/{json_name}")
        tsv_to_json(tsv_path, json_path)

    print("Done.")

if __name__ == "__main__":
    main()
