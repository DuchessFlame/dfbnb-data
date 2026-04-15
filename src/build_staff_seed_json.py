#!/usr/bin/env python3
"""
Group tsv/staff.tsv (one row per account per platform) into one record
per person for the bnb_staff WordPress CPT seeder.

Input columns:
  Staff Member | Account Type | GT | Role | Time Zone |
  Discord Username | Bethesda.Net | Email | Platform | Birthday

Output (JSON array, one record per person):
  {
    "display_name":       "Amy Smith",
    "role":               "Chef",
    "time_zone":          "EST UTC-4",
    "birthday":           "",
    "email":              "",
    "bethesda_id":        "Hlma979901",
    "discord_username":   "Insecuredrop420",
    "facebook_username":  "",
    "handles_discord":    true,
    "handles_facebook":   false,
    "gt_xbox":            "Insecuredrop626",
    "psn":                "",
    "ign_pc":             "insecuredrop626",
    "mules": [
      { "platform": "XBOX", "gt": "HvyMtlGrl" },
      { "platform": "XBOX", "gt": "AngryCake3391" }
    ]
  }

Rules:
- Platform values are normalised: XBOX, PC, PS -> XBOX, PC, PlayStation
  (matches the customer enum).
- Main accounts go into the per-platform gt fields.
- Mule accounts go into the mules array in the order they appear.
- Scalars like role/time_zone/birthday/email/bethesda_id/discord_username
  take the FIRST non-empty value encountered. This is intentional — the
  TSV has identical scalars duplicated across rows, so first-wins is safe.
- handles_discord defaults to True when a Discord Username is present,
  False otherwise. handles_facebook defaults to False everywhere (no FB
  data exists yet — Duchess will flip the toggle per-person through the
  admin UI once the Staff tab is live).
- Output is written to dist/seed-staff.json AND also copied to the WP
  child theme (inc/seed-staff.json) so the PHP seeder can read it on
  first init without a live network dependency.

Run from the dfbnb-data repo root:
    python3 src/build_staff_seed_json.py
"""
import csv
import json
import sys
from collections import OrderedDict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TSV = ROOT / "tsv" / "staff.tsv"
DIST = ROOT / "dist" / "seed-staff.json"

# The WP theme is synced manually — write a second copy there so Duchess
# can push it up alongside inc/staff-portal.php without an extra step.
THEME_COPY = Path(
    "/sessions/sleepy-ecstatic-goldberg/mnt/1 site-data/json/dfbnb-child/inc/seed-staff.json"
)

PLATFORM_MAP = {
    "XBOX": "XBOX",
    "PC":   "PC",
    "PS":   "PlayStation",
}

FIELD_XBOX = "gt_xbox"
FIELD_PSN  = "psn"
FIELD_PC   = "ign_pc"

PLATFORM_FIELD = {
    "XBOX":        FIELD_XBOX,
    "PlayStation": FIELD_PSN,
    "PC":          FIELD_PC,
}


def new_record(name: str) -> dict:
    """Skeleton for a fresh person record."""
    return {
        "display_name":      name,
        "role":              "",
        "time_zone":         "",
        "birthday":          "",
        "email":             "",
        "bethesda_id":       "",
        "discord_username":  "",
        "facebook_username": "",
        "handles_discord":   False,
        "handles_facebook":  False,
        "gt_xbox":           "",
        "psn":               "",
        "ign_pc":            "",
        "mules":             [],
    }


def take_first(record: dict, key: str, value: str) -> None:
    """First-wins: only fill a blank field, never overwrite."""
    if value and not record[key]:
        record[key] = value


def main() -> int:
    if not TSV.exists():
        print(f"FAIL staff.tsv not found at {TSV}", file=sys.stderr)
        return 1

    # OrderedDict keyed by display_name preserves TSV appearance order so
    # the seeded posts have natural IDs (first person listed = lowest ID).
    people: "OrderedDict[str, dict]" = OrderedDict()

    with TSV.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            name = (row.get("Staff Member") or "").strip()
            if not name:
                continue

            rec = people.get(name)
            if rec is None:
                rec = new_record(name)
                people[name] = rec

            account_type = (row.get("Account Type") or "").strip()
            gt           = (row.get("GT") or "").strip()
            role         = (row.get("Role") or "").strip()
            tz           = (row.get("Time Zone") or "").strip()
            discord      = (row.get("Discord Username") or "").strip()
            bethesda     = (row.get("Bethesda.Net") or "").strip()
            email        = (row.get("Email") or "").strip()
            platform_raw = (row.get("Platform") or "").strip().upper()
            birthday     = (row.get("Birthday") or "").strip()

            take_first(rec, "role", role)
            take_first(rec, "time_zone", tz)
            take_first(rec, "discord_username", discord)
            take_first(rec, "bethesda_id", bethesda)
            take_first(rec, "email", email)
            take_first(rec, "birthday", birthday)

            platform = PLATFORM_MAP.get(platform_raw, "")
            if not platform:
                continue  # unknown platform — skip the account row

            if account_type.lower() == "main":
                field = PLATFORM_FIELD[platform]
                # First-wins at the per-platform field too so duplicate
                # mains don't overwrite each other silently.
                if gt and not rec[field]:
                    rec[field] = gt
            elif account_type.lower() == "mule":
                if gt:
                    rec["mules"].append({"platform": platform, "gt": gt})
            # Empty account type rows are skipped silently.

    # Default the Discord toggle on for anyone with a Discord handle.
    # Facebook stays off everywhere — no FB data exists yet.
    for rec in people.values():
        rec["handles_discord"] = bool(rec["discord_username"])

    out = list(people.values())

    DIST.parent.mkdir(parents=True, exist_ok=True)
    with DIST.open("w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)

    # Best-effort copy into the theme dir so the seeder can read it.
    try:
        THEME_COPY.parent.mkdir(parents=True, exist_ok=True)
        with THEME_COPY.open("w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=2, ensure_ascii=False)
        theme_note = f"  + copied to {THEME_COPY}"
    except Exception as err:
        theme_note = f"  ! theme copy skipped: {err}"

    print(f"OK {len(out)} staff records -> {DIST}")
    print(theme_note)

    mule_total = sum(len(r["mules"]) for r in out)
    discord_on = sum(1 for r in out if r["handles_discord"])
    print(f"  mules attached: {mule_total}")
    print(f"  discord-enabled by default: {discord_on}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
