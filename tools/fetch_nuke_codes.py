#!/usr/bin/env python3
"""
fetch_nuke_codes.py — refresh dist/nuke_codes.json

Fallout 76's three silo launch codes (Alpha / Bravo / Charlie) are issued by
Bethesda's servers every week. They are NOT present in the game files and
cannot be derived from them — the game ships the keyword-cipher machinery, but
the week's keyword and the resulting codes arrive from the server. So the only
way to publish them is to read a community source that has already solved the
week's cipher.

Source: NukaCrypt's GraphQL API (https://api.nukacrypt.com/graphql).

The site normally reads this data live through the WordPress proxy
(/wp-json/dfbnb/v1/nuke-codes, see the child theme's functions.php). This file
is the FALLBACK the proxy drops to if that API is unreachable, so it only has
to be roughly current — a weekly run is plenty.

Usage:
    python tools/fetch_nuke_codes.py            # write dist/nuke_codes.json
    python tools/fetch_nuke_codes.py --check    # print only, write nothing
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

API_URL = "https://api.nukacrypt.com/graphql"
QUERY = "{ nukeCodes { alpha bravo charlie sinceEpoch } }"

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = REPO_ROOT / "dist" / "nuke_codes.json"

WEEK_SECONDS = 7 * 24 * 60 * 60


def fetch_codes() -> dict:
    """Ask the API for this week's codes. Raises on any failure."""
    body = json.dumps({"variables": {}, "query": QUERY}).encode("utf-8")
    req = urllib.request.Request(
        API_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "dfbnb-data/fetch_nuke_codes",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=20) as res:
        payload = json.loads(res.read().decode("utf-8"))

    node = (payload.get("data") or {}).get("nukeCodes")
    if not isinstance(node, dict):
        raise ValueError(f"unexpected API shape: {payload!r}")

    alpha = str(node.get("alpha", "")).strip()
    bravo = str(node.get("bravo", "")).strip()
    charlie = str(node.get("charlie", "")).strip()
    since = int(node.get("sinceEpoch") or 0)

    # All three codes are 8 digits. Anything else means the source changed
    # shape or is serving a placeholder — better to fail loudly than to
    # publish junk the homepage will happily render.
    for name, code in (("alpha", alpha), ("bravo", bravo), ("charlie", charlie)):
        if not (len(code) == 8 and code.isdigit()):
            raise ValueError(f"{name} is not an 8-digit code: {code!r}")
    if since <= 0:
        raise ValueError(f"bad sinceEpoch: {since!r}")

    return {
        "alpha": alpha,
        "bravo": bravo,
        "charlie": charlie,
        "since": since,
        "resets": since + WEEK_SECONDS,
        "source": "nukacrypt",
        "fetched": int(datetime.now(timezone.utc).timestamp()),
    }


def describe(data: dict) -> str:
    since = datetime.fromtimestamp(data["since"], timezone.utc)
    resets = datetime.fromtimestamp(data["resets"], timezone.utc)
    left = resets - datetime.now(timezone.utc)
    left = max(left, timedelta(0))
    return (
        f"  Alpha   {data['alpha']}\n"
        f"  Bravo   {data['bravo']}\n"
        f"  Charlie {data['charlie']}\n"
        f"  week of {since:%Y-%m-%d %H:%M UTC}\n"
        f"  resets  {resets:%Y-%m-%d %H:%M UTC} (in {left})"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Refresh dist/nuke_codes.json")
    ap.add_argument(
        "--check",
        action="store_true",
        help="fetch and print, but do not write the file",
    )
    args = ap.parse_args()

    try:
        data = fetch_codes()
    except (urllib.error.URLError, ValueError, json.JSONDecodeError, OSError) as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        # Leave the existing file alone. A week-old fallback still beats none.
        return 1

    print(describe(data))

    if args.check:
        print("\n--check: nothing written.")
        return 0

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"\nwrote {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
