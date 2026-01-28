import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist"

# Match: CHAL_Export_Dec_2025.tsv (month as 3 letters)
CHAL_RE = re.compile(r"^CHAL_Export_([A-Za-z]+)_(\d{4})\.tsv$")

MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

@dataclass(frozen=True)
class ChalFile:
    path: Path
    year: int
    month: int
    label: str  # e.g. "Dec 2025"

def parse_chal_filename(p: Path) -> Optional[ChalFile]:
    m = CHAL_RE.match(p.name)
    if not m:
        return None
    mon_str = m.group(1).lower()
    year = int(m.group(2))
    if mon_str not in MONTHS:
        return None
    month = MONTHS[mon_str]
    label = f"{m.group(1).title()} {year}"
    return ChalFile(path=p, year=year, month=month, label=label)

def pick_latest_previous(files: List[ChalFile]) -> Tuple[ChalFile, ChalFile]:
    files_sorted = sorted(files, key=lambda x: (x.year, x.month))
    if len(files_sorted) < 2:
        raise SystemExit("Need at least TWO CHAL TSV files to compare (previous + latest).")
    return files_sorted[-1], files_sorted[-2]

def load_tsv_rows(tsv_path: Path) -> Tuple[List[str], List[dict]]:
    with tsv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)
        cols = reader.fieldnames or []
    return cols, rows

def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as out:
        json.dump(payload, out, ensure_ascii=False, indent=2)

def main() -> None:
    if not TSV_DIR.exists():
        raise SystemExit(f"Missing tsv dir: {TSV_DIR}")

    chal_files = []
    for p in TSV_DIR.glob("*.tsv"):
        cf = parse_chal_filename(p)
        if cf:
            chal_files.append(cf)

    if not chal_files:
        raise SystemExit("No CHAL TSV files found matching pattern CHAL_Export_Mmm_YYYY.tsv")

    latest, previous = pick_latest_previous(chal_files)

    latest_cols, latest_rows = load_tsv_rows(latest.path)
    prev_cols, prev_rows = load_tsv_rows(previous.path)

    manifest = {
        "chal": {
            "latest": {"file": latest.path.name, "label": latest.label, "year": latest.year, "month": latest.month},
            "previous": {"file": previous.path.name, "label": previous.label, "year": previous.year, "month": previous.month},
        }
    }

    write_json(DIST_DIR / "manifest.json", manifest)

    write_json(DIST_DIR / "chal_latest.json", {
        "_meta": {"source_file": latest.path.name, "label": latest.label, "row_count": len(latest_rows), "columns": latest_cols},
        "rows": latest_rows
    })

    write_json(DIST_DIR / "chal_previous.json", {
        "_meta": {"source_file": previous.path.name, "label": previous.label, "row_count": len(prev_rows), "columns": prev_cols},
        "rows": prev_rows
    })

    print("Latest:", latest.path.name, latest.label)
    print("Previous:", previous.path.name, previous.label)
    print("Wrote dist/manifest.json, dist/chal_latest.json, dist/chal_previous.json")

if __name__ == "__main__":
    main()
