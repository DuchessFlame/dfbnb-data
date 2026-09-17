#!/usr/bin/env python3
r"""
add_recipe_unlocks.py — re-decide "is this cut" and "what unlocks it", in place.

The same shape as add_cobj_link.py and add_collectable_challenges.py, and for
the same reason: a full plan_master build is ~80 minutes per channel because of
the rng76 waterfall, and none of what this script changes needs a single rate.
Cut detection, the GNAM unlock sentence and the obtain ledger are pure joins
over COBJ / CHAL / GMRW / QUST, so they run in seconds against the committed
document.

    python3 src/add_recipe_unlocks.py dist/plan_master.json
    python3 src/add_recipe_unlocks.py dist/pts/plan_master.json --tsv-dir tsv/pts

Run it AFTER add_cobj_link.py — this reads `cobj` and can only consult a recipe
the row is actually linked to.

It also appends the recipe-only rows (`plan_recipe_rows`) — craftables a
challenge or a workshop claim teaches that have no plan book at all — because
they are the other half of the same reading of COBJ.GNAM. `--no-recipe-rows`
leaves them out.

WHAT IT CHANGES
---------------
`cut` / `cut_reason`   a plan whose recipe is unlocked by a challenge or a
                       workshop claim is not cut, whatever its EditorID says
`obtain_unlocks`       recomputed, with the GNAM sentence leading
`obtain`               the one-line summary follows the verdict
`obtain_ledger`        pure re-sort of the two lists, so it is refreshed too

WHAT IT WILL NOT TOUCH
----------------------
`obtain_routes` and every rate in them. Nothing here resolves a drop, and a
rescued plan has none by definition — nothing referenced its BOOK, which is how
it ended up flagged cut in the first place.

`type`. The bucket is the progress-store key (`plans:{brand}:{type}:{mode}`), so
moving a row strands whatever ticks a reader has against it. Same call
add_cobj_link.py and plan_subpages.py already made.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_plan_obtain_json as bpo
import plan_recipe_rows
import plan_sources
import plan_unlocks


def _book_refs(tsv_dir):
    """BOOK FormID -> its ReferencedBy list, the input cut_reason() needs."""
    out = {}
    path = bpo.newest("BOOK_Export_*.tsv", tsv_dir)
    if not path:
        return out
    for row in bpo.read_rows(path):
        fid = (row.get("FormID") or "").strip().upper()
        if not fid:
            continue
        out[fid] = [v for v in ((row.get(f"Ref{j}") or "").strip()
                                for j in range(1, 46)) if v]
    return out


def attach(items, tsv_dir="tsv", stats=None):
    stats = stats if stats is not None else {}

    def bump(k):
        stats[k] = stats.get(k, 0) + 1

    tsv_dir = os.path.abspath(tsv_dir)
    bpo.TSV = tsv_dir
    unlocks = plan_unlocks.RecipeUnlocks(tsv_dir, bpo.newest)
    idx = plan_sources.UnlockIndex(tsv_dir, lambda pat, root: bpo.newest(pat, root))
    refs_by_book = _book_refs(tsv_dir)

    stats["exports"] = dict(unlocks.exports)
    stats["rescued"] = []
    stats["newly_cut"] = []

    for item in items:
        plan = item.get("plan_item") or {}
        fid = (plan.get("formid") or "").upper()
        edid = plan.get("edid") or ""
        if not fid:
            bump("no_plan_item")
            continue

        co_fid = (item.get("cobj") or {}).get("formid") or ""
        unlock = unlocks.proof_of_life(co_fid)
        refs = refs_by_book.get(fid)
        was_cut = bool(item.get("cut"))
        cut_why = plan_sources.cut_reason(edid, refs, recipe_unlock=unlock)

        if was_cut and not cut_why:
            stats["rescued"].append(f"{item.get('name')} <- {unlock['full']}"
                                    if unlock else item.get("name"))
        elif cut_why and not was_cut:
            stats["newly_cut"].append(f"{item.get('name')} [{edid}]")

        item["cut"] = bool(cut_why)
        item["cut_reason"] = cut_why

        if cut_why:
            new_unlocks = []
        else:
            new_unlocks = idx.unlocks_for(fid, edid, refs or [],
                                          has_routes=bool(item.get("obtain_routes")))
            sentence = unlocks.sentence(unlock)
            if sentence and sentence not in new_unlocks:
                new_unlocks.insert(0, sentence)
                bump("gnam_sentence")
        if new_unlocks != (item.get("obtain_unlocks") or []):
            bump("unlocks_changed")
        item["obtain_unlocks"] = new_unlocks

        routes = item.get("obtain_routes") or []
        if cut_why:
            obtain = ("Cut content. This plan is still in the game files but "
                      "nothing gives it out — it cannot be obtained in game.")
        elif routes:
            obtain = ("Learned from a plan. Drops from the sources below, each "
                      "with its resolved chance.")
        elif new_unlocks:
            obtain = "Learned from a plan. It is not random loot — see below."
        else:
            obtain = ("Learned from a plan. No source was resolved from the game "
                      "files — see Technical for the recipe details.")
        if obtain != item.get("obtain"):
            bump("obtain_changed")
        item["obtain"] = obtain

        item["obtain_ledger"] = plan_sources.obtain_ledger(item)

    return stats


def report(stats, stream=sys.stdout):
    ex = stats.get("exports") or {}
    print(f"  [unlocks] COBJ {ex.get('COBJ','?')} | CHAL {ex.get('CHAL','?')}", file=stream)
    for key in sorted(k for k in stats
                      if k not in ("exports", "rescued", "newly_cut")):
        print(f"    {key:22s} {stats[key]}", file=stream)
    for label, rows in (("rescued from cut", stats.get("rescued") or []),
                        ("newly flagged cut", stats.get("newly_cut") or [])):
        if not rows:
            continue
        print(f"    {len(rows)} {label}:", file=stream)
        for line in rows[:30]:
            print(f"      {line}", file=stream)
        if len(rows) > 30:
            print(f"      ... and {len(rows) - 30} more", file=stream)


def main(argv=None):
    ap = argparse.ArgumentParser(description="re-decide cut / unlocks in place")
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--tsv-dir", default="tsv")
    ap.add_argument("--no-recipe-rows", action="store_true",
                    help="skip the recipes that have no plan book")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[unlocks] skip (missing): {path}")
            continue
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        items = doc.get("items") or []
        print(f"[unlocks] {path}: {len(items)} rows")
        report(attach(items, args.tsv_dir))
        if not args.no_recipe_rows:
            print("  [recipe-rows] recipes with no plan book")
            plan_recipe_rows.report(plan_recipe_rows.attach(items, args.tsv_dir))
            doc["count"] = len(items)
        if args.dry_run:
            continue
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
