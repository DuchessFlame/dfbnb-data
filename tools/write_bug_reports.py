#!/usr/bin/env python3
"""write_bug_reports.py — Current Bugged Plans -> Bethesda bug report files.

Reads dist/bugged_plans.json (the copy on this PC after a pull, or --url for
the published one on GitHub) and writes a .txt bug report for every row that
carries a "report" (built by src/build_bugged_plans_json.py: strong or
confirmed rows only) into

    C:\\Users\\Duche\\OneDrive\\Guides and Stuff\\.Bethesda Bug Reports

where Brewmaster picks it up. A plan already reported (its FormID appears in any
.txt in the folder, including hand-written reports) is skipped, and nothing
is ever overwritten, so it is safe to run any time.

Player details (username, Buid, platform, patch, mule) come from
data/local/reporter.json, which is git-ignored because the repo is public.

Usage:
  python tools/write_bug_reports.py                 # local dist/bugged_plans.json
  python tools/write_bug_reports.py --url           # published copy on GitHub
  python tools/write_bug_reports.py --dry-run
"""
import argparse, json, os, sys, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "src"))
import bug_report_text  # noqa: E402

RAW = "https://raw.githubusercontent.com/DuchessFlame/dfbnb-data/main/dist/bugged_plans.json"
DEFAULT_OUT = os.path.join(os.path.dirname(os.path.dirname(REPO)), "Guides and Stuff", ".Bethesda Bug Reports")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default=os.path.join(REPO, "dist", "bugged_plans.json"))
    ap.add_argument("--url", nargs="?", const=RAW, default=None)
    ap.add_argument("--out", default=os.environ.get("DFBNB_BUG_REPORTS") or DEFAULT_OUT)
    ap.add_argument("--player", default=os.path.join(REPO, "data", "local", "reporter.json"))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    if a.url:
        with urllib.request.urlopen(a.url, timeout=60) as r:
            doc = json.loads(r.read().decode("utf-8"))
    else:
        with open(a.json, encoding="utf-8") as f:
            doc = json.load(f)
    player = {}
    if os.path.exists(a.player):
        with open(a.player, encoding="utf-8") as f:
            player = json.load(f)
    else:
        print(f"[bug_reports] no {a.player} - player details left as placeholders")
    if not os.path.isdir(a.out):
        sys.exit(f"[bug_reports] folder not found: {a.out}")

    have = set(os.listdir(a.out))
    # every report already in the folder, hand-written ones too: a plan whose
    # FormID is in one of them has been reported and is never sent again
    seen_text = ""
    for h in have:
        if h.lower().endswith(".txt"):
            try:
                with open(os.path.join(a.out, h), encoding="utf-8", errors="replace") as f:
                    seen_text += f.read().upper() + "\n"
            except OSError:
                pass
    wrote = skipped = 0
    for it in doc.get("items", []):
        rep = it.get("report")
        if not rep:
            continue
        name = rep["filename"]
        stem = name[:-4]
        if any(h == name or h.startswith(stem + " -") for h in have) or \
                (it.get("id") and it["id"].upper() in seen_text):
            skipped += 1
            continue
        text = bug_report_text.render(rep, player)
        print(f"[bug_reports] {'would write' if a.dry_run else 'wrote'} {name}  ({it['name']})")
        if not a.dry_run:
            with open(os.path.join(a.out, name), "w", encoding="utf-8", newline="\n") as f:
                f.write(text)
        wrote += 1
    print(f"[bug_reports] {wrote} new, {skipped} already in the folder -> {a.out}")


if __name__ == "__main__":
    main()
