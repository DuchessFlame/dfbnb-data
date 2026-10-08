#!/usr/bin/env python3
"""
build_plan_check_mule_json.py
=============================
Page data for the three Community pages that list the plans each Plan Check
Mule is still missing:

    /df/community/xbox-plan-check-mule-missing-plans-list/
    /df/community/playstation-plan-check-mule-missing-plans-list/
    /df/community/pc-plan-check-mule-missing-plans-list/

Rendered by renderPlanCheckMule() in df-bnb-plan-checklists.js, through the
same plan row the plan checklists draw (Item Image / How to Obtain /
Output & Effects / Technical) -- with no tick boxes and no progress bar.

WHERE THE LIST COMES FROM
-------------------------
Brewmaster (the Discord bot) owns the lists. It commits one file per mule:

    dist/missing_plans_<platform>.json              e.g. missing_plans_xbox.json
    dist/missing_plans_<platform>_<mule>.json       e.g. missing_plans_xbox_facebook.json

Each file's "source" field names the mule ("discord", "facebook"). A file with
no "source" takes it from the filename suffix, and a bare
missing_plans_<platform>.json with neither is the Discord mule. Rows can be
listed flat under "items" or grouped under "groups[].items" -- both are read.

This builder does NOT trust the bot's copy of each plan row. The bot snapshots
plan_master at the moment someone updates the list, so its images, routes and
rates go stale the next time plan_master is rebuilt. Each row is re-read from
the current plan_master by its id (PLAN_xxxxxxxx); the bot's copy is only
used for a plan plan_master no longer has. Images therefore always follow
plan_images.py's hosted-first order: scoreboard / Atom Shop / titles / player
icons art already on the site first, then the plan-checklist folder.

OUTPUT
------
    dist/community/plan_check_mule_<platform>.json          (live)
    dist/pts/community/plan_check_mule_<platform>.json      (--pts)

    {
      "platform": "xbox", "title": ..., "intro": [..], "about": {heading, paragraphs},
      "donate": [ {via, url, link, text} ], "thanks": "...",
      "count": N,                         # unique plans across every mule
      "layout": "mules" | "flat",         # Xbox: one root expand per mule
      "mules": [ { "key": "facebook", "label": "XBOX - Facebook Plan Check Mule", "donate": [..],
                   "updated_at": "...", "count": N, "items": [ <plan row>, ... ] } ],
      "_meta": {...}
    }

Rows inside a mule are A-Z by plan name, "Plan: " ignored.

Usage:
    python src/build_plan_check_mule_json.py            # live
    python src/build_plan_check_mule_json.py --pts      # PTS rows -> dist/pts/community/
  In the PTS build workflow dist/ has been wiped and holds the PTS plan_master,
  so it passes the paths itself:
    python src/build_plan_check_mule_json.py --pts --master dist/plan_master.json
"""

import argparse
import glob
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ─────────────────────────────────────────────────────────────────────────────
#  PAGE COPY -- the intro card and the donation blocks. Edit the words here.
#
#  intro     paragraphs at the top of the intro card
#  about     the "What's a plan mule?" panel in the intro card
#  donate    "How to donate" panel, in the intro card on every page. A mule
#            can also carry its own (shown at the top of its root expand), but
#            none do now so all three pages look the same.
#  thanks    closing line of the intro card
#
#  A donate entry is { "via", "url", "link", "text" }: `link` is the words the
#  link shows, `text` the sentence after it.
# ─────────────────────────────────────────────────────────────────────────────
ABOUT = {
    "heading": "What's a plan mule?",
    "paragraphs": [
        "A plan mule is a character made just to hold every plan in the game, "
        "so you can check which ones you're missing.",
        "The mule isn't for trading. Meet up with it, open the trade window and "
        "scroll to the bottom: the game lists every plan your character doesn't "
        "know. Take a quick screenshot so you've got your missing list for later.",
    ],
}

THANKS = ("Thank you to everyone who's able to help. The Plan Check Mules have "
          "helped thousands of players over the years, and your donations keep "
          "this service running for the community.")

DISCORD_URL = "https://discord.gg/9aP4jrQhM"


def discord_donate():
    return {
        "via": "Discord", "url": DISCORD_URL, "link": "Join the Discord",
        "text": "then pick the Plan Collector role for your platform, head to the "
                "donations channel and let us know which plans you can donate.",
    }


def facebook_donate(url, chefs=True):
    text = "then make a post letting us know which plans you can donate."
    if chefs:
        text += (" One of our chefs will collect them and deliver them to the "
                 "Plan Check Mule for you.")
    return {"via": "Facebook", "url": url, "link": "Request to join the group", "text": text}


PAGES = {
    "xbox": {
        "title": "XBOX Plan Check Mule Missing Plans List",
        "intro": [
            "The Plan Check Mules are still missing the plans below. If you have "
            "a spare copy of any of them, a donation would be very welcome.",
        ],
        "about": ABOUT,
        # Same How to donate panel as PlayStation and PC, in the intro card.
        "donate": [discord_donate(),
                   facebook_donate("https://www.facebook.com/groups/theduchessflame", chefs=False)],
        "thanks": THANKS,
        # One root expand per mule, in this order. Their plans are sub-expands.
        "mules": [
            ("facebook", "XBOX - Facebook Plan Check Mule", []),
            ("discord",  "XBOX - Discord Plan Check Mule",  []),
        ],
    },
    "playstation": {
        "title": "PlayStation Plan Check Mule Missing Plans List",
        "intro": [
            "The Plan Check Mule is still missing the plans below. If you have a "
            "spare copy of any of them, a donation would be very welcome.",
        ],
        "about": ABOUT,
        "donate": [discord_donate(),
                   facebook_donate("https://www.facebook.com/groups/buffsnbrewps/")],
        "thanks": THANKS,
        "mules": [("discord", "PlayStation Plan Check Mule", [])],
    },
    "pc": {
        "title": "PC Plan Check Mule Missing Plans List",
        "intro": [
            "The Plan Check Mule is still missing the plans below. If you have a "
            "spare copy of any of them, a donation would be very welcome.",
        ],
        "about": ABOUT,
        "donate": [discord_donate(),
                   facebook_donate("https://www.facebook.com/groups/buffsnbrewpc/")],
        "thanks": THANKS,
        "mules": [("discord", "PC Plan Check Mule", [])],
    },
}
# A platform with ONE mule renders its plans as root expands ("flat"); more
# than one and each mule becomes a root expand with its plans inside ("mules").

_LIST_RE = re.compile(r"missing_plans_([a-z]+)(?:_([a-z0-9-]+))?\.json$")


def _git_show(path):
    """A committed file's text, for the PTS build where dist/ has been wiped."""
    try:
        return subprocess.run(["git", "show", f"HEAD:{path}"], cwd=ROOT, check=True,
                              capture_output=True, text=True, encoding="utf-8").stdout
    except Exception:  # noqa: BLE001
        return None


def read_lists(platform):
    """{mule_key: bot_list_dict} for one platform."""
    rel_glob = f"dist/missing_plans_{platform}*.json"
    found = {}
    paths = sorted(glob.glob(os.path.join(ROOT, rel_glob)))
    texts = [(os.path.relpath(p, ROOT).replace(os.sep, "/"),
              open(p, encoding="utf-8").read()) for p in paths]
    if not texts:
        # PTS workflow: dist/ was cleared, so read the bot's committed copies.
        listing = subprocess.run(["git", "ls-files", rel_glob], cwd=ROOT,
                                 capture_output=True, text=True).stdout.split()
        texts = [(p, _git_show(p)) for p in listing]
    for rel, text in texts:
        m = _LIST_RE.search(rel)
        if not m or m.group(1) != platform or not text:
            continue
        try:
            data = json.loads(text)
        except ValueError:
            print(f"  [WARN] {rel} is not valid JSON -- skipped", file=sys.stderr)
            continue
        mule = (data.get("source") or m.group(2) or "discord").strip().lower()
        if mule in found:
            print(f"  [WARN] two lists for {platform}/{mule}; using {rel}", file=sys.stderr)
        found[mule] = data
        print(f"  {rel}: mule={mule} rows={sum(1 for _ in rows_of(data))}")
    return found


def rows_of(data):
    for it in data.get("items") or []:
        yield it
    for g in data.get("groups") or []:
        for it in g.get("items") or []:
            yield it


def plan_sort_key(it):
    name = it.get("display_name") or it.get("name") or it.get("id") or ""
    return re.sub(r"^\s*plan:\s*", "", name, flags=re.I).lower()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--master", help="plan_master.json to read rows from")
    ap.add_argument("--out-dir", help="where plan_check_mule_<platform>.json is written")
    a = ap.parse_args()

    channel = "pts" if a.pts else "live"
    master_path = a.master or os.path.join(ROOT, "dist", "pts" if a.pts else "", "plan_master.json")
    out_dir = a.out_dir or os.path.join(ROOT, "dist", "pts" if a.pts else "", "community")
    master_path = os.path.normpath(master_path)

    print("=" * 60)
    print(f"  Building Plan Check Mule pages ({channel.upper()})")
    print(f"  plan_master: {os.path.relpath(master_path, ROOT)}")
    print("=" * 60)

    master = {}
    if os.path.exists(master_path):
        with open(master_path, encoding="utf-8") as f:
            raw = json.load(f)
        master = {str(it.get("id")): it for it in (raw.get("items") or []) if it.get("id")}
    else:
        print(f"  [WARN] {master_path} not found -- using the bot's own row copies", file=sys.stderr)

    os.makedirs(out_dir, exist_ok=True)
    for platform, cfg in PAGES.items():
        lists = read_lists(platform)
        mules, seen_all = [], set()
        for key, label, mule_donate in cfg["mules"]:
            data = lists.pop(key, None) or {}
            rows, seen, stale = [], set(), 0
            for it in rows_of(data):
                pid = str(it.get("id") or "")
                if not pid or pid in seen:
                    continue
                seen.add(pid)
                fresh = master.get(pid)
                if fresh is None:
                    stale += 1
                rows.append(dict(fresh) if fresh is not None else dict(it))
            rows.sort(key=plan_sort_key)
            seen_all |= seen
            if stale:
                print(f"  [WARN] {platform}/{key}: {stale} plan(s) not in plan_master -- bot copy used",
                      file=sys.stderr)
            mules.append({
                "key": key,
                "label": label,
                "donate": mule_donate,
                "updated_at": data.get("updated_at") or data.get("synced_at"),
                "count": len(rows),
                "items": rows,
            })
        for extra in lists:
            print(f"  [WARN] {platform}: list for mule '{extra}' has no entry in PAGES -- not shown",
                  file=sys.stderr)

        out = {
            "platform": platform,
            "channel": channel,
            "title": cfg["title"],
            "intro": cfg["intro"],
            "about": cfg.get("about"),
            "donate": cfg["donate"],
            "thanks": cfg.get("thanks", ""),
            "layout": "mules" if len(cfg["mules"]) > 1 else "flat",
            "count": len(seen_all),
            "mules": mules,
            "_meta": {
                "built": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "master": os.path.basename(master_path),
            },
        }
        path = os.path.join(out_dir, f"plan_check_mule_{platform}.json")
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(out, f, ensure_ascii=False, indent=1)
            f.write("\n")
        print(f"  -> {os.path.relpath(path, ROOT)}: {out['count']} plan(s) "
              + ", ".join(f"{m['key']}={m['count']}" for m in mules))
    print("  Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
