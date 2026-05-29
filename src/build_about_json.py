#!/usr/bin/env python3
"""
build_about_json.py
===================
Builds dist/about.json for the /about/ pages on buffsnbrew.com and
theduchessflame.com (rendered by df-bnb-about.js in the dfbnb-child theme).

Inputs:
  src/about/vault_legends.tsv  — one donor per row (column: name)

  Future TSVs can be added here for the other /about/ pages
  (community_groups, contact, follow, support_my_work). Each gets
  picked up by an `if path.exists()` branch below — keep that style
  so the script never crashes when a TSV is missing.

Output:
  dist/about.json  — keyed by page slug (hyphens, matching the URL):
    {
      "vault-legends":     { "title": ..., "subtitle": ..., "donors": [...] },
      "community-groups":  { "title": ..., "subtitle": ..., ... },
      "contact":           { ... },
      "follow":            { ... },
      "support-my-work":   { ... }
    }

Usage:
  python src/build_about_json.py
  python src/build_about_json.py --outdir dist
"""

import argparse
import csv
import json
import sys
from pathlib import Path


VAULT_LEGENDS_SUBTITLE = (
    "Thank you to the following players. Without your support and donations, "
    "this website would not have been possible."
)

PLACEHOLDER_SUBTITLE = "Coming soon."

# Support My Work — narrative + action cards. Edit this constant to update
# the page content; the workflow regenerates dist/about.json on push.
SUPPORT_MY_WORK = {
    "title": "Support My Work",
    "subtitle": "Hey there, I’m Kat — also known as Duchess!",
    "intro": [
        "I’m an Australian data miner and content creator who makes "
        "Fallout 76 guides — farming routes, event reward breakdowns, "
        "food and chem buff math, season pass tracking, plan checklists, "
        "and everything in between.",
        "I firmly believe knowledge should be accessible to everyone, so the "
        “guides will always be free and ad-free. No paywalls, no popups.”,
        "That said, running a site like this isn’t free. Web hosting, "
        "the build pipeline that needs to be ran after every patch and update, image "
        "and video storage, photo and video editing software, and the hours "
        "of comparing game files at 2am all add up. If the guides have saved "
        "you grind time, helped you finish a season pass, or just made the "
        "wasteland a little less confusing, here are a few ways you can help "
        "keep them going:",
    ],
    "actions": [
        {
            "label": "Share My Guides",
            "description": "The single most valuable thing you can do. Drop "
                           "a guide link in your friend group, mention me "
                           "when someone asks “where do I find X” "
                           "on Reddit, share a checklist with your team "
                           "before a public event. Word of mouth is what "
                           "keeps fan sites alive.",
        },
        {
            "label": "Follow Me on Social Media",
            "description": "Following on Facebook, Twitter, Bluesky and "
                           "Discord helps each post reach more vault "
                           "dwellers (the algorithms reward engagement) and "
                           "means you’ll see new guides and patch "
                           "breakdowns the moment they go live.",
            "url": "/about/follow/",  # internal — JS prepends current brand (/df or /bnb)
        },
        {
            "label": "One-Time Donation",
            "description": "Buy me a Ko-fi (coffee). Caffeine is what gets "
                           "me through patch days when Bethesda drops a "
                           "4 GB update at 1am my time and the whole "
                           "pipeline needs to be re-run from scratch.",
            "url": "https://ko-fi.com/duchessflame",
        },
        {
            "label": "Monthly Contribution",
            "description": "Become a regular supporter on Ko-fi. Recurring "
                           "support lets me plan ahead — hosting "
                           "upgrades, better tools, and keeping things "
                           "steady through the quiet months between big "
                           "Fallout updates.",
            "url": "https://ko-fi.com/duchessflame",
        },
    ],
    "closing": [
        "Every bit of support — a kind word in my DMs, a share, your "
        "time, or a donation — helps keep this thing going. Honestly, "
        "knowing the guides are useful to someone is the biggest part of "
        "what keeps me at it.",
        "Thanks for being here :)",
        "— Kat",
    ],
}


def read_tsv_column(path: Path, column: str) -> list[str]:
    """Read a single column out of a TSV, stripping blanks. Returns [] if the
    file doesn't exist — pages without a TSV yet get an empty list."""
    if not path.exists():
        return []
    out: list[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            v = (row.get(column) or "").strip()
            if v:
                out.append(v)
    return out


def build(src_dir: Path) -> dict:
    about = {}

    # Vault Legends — donor names
    donors = read_tsv_column(src_dir / "vault_legends.tsv", "name")
    about["vault-legends"] = {
        "title": "Vault Legends",
        "subtitle": VAULT_LEGENDS_SUBTITLE,
        "donors": donors,
    }

    # Support My Work — narrative + action cards.
    # Content is static prose so it lives here in the build script rather
    # than a TSV (TSVs are for tabular/growing data like donor lists).
    about["support-my-work"] = SUPPORT_MY_WORK

    # Other /about/ pages — placeholder until content is added.
    # Each is keyed by URL slug; the JS module dispatches by slug.
    for slug, title in [
        ("community-groups", "Community Groups"),
        ("contact",          "Contact"),
        ("follow",           "Follow"),
    ]:
        about[slug] = {
            "title": title,
            "subtitle": PLACEHOLDER_SUBTITLE,
            "items": [],
        }

    return about


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="src/about", help="Directory containing about TSVs")
    ap.add_argument("--outdir", default="dist", help="Output directory")
    args = ap.parse_args()

    src_dir = Path(args.src)
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "about.json"

    data = build(src_dir)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Wrote {out_path}")
    print(f"  vault-legends donors: {len(data['vault-legends']['donors'])}")
    print(f"  support-my-work    actions: {len(data['support-my-work']['actions'])}")
    for k in ("community-groups", "contact", "follow"):
        print(f"  {k:<18} title={data[k]['title']!r}, subtitle={data[k]['subtitle']!r}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
