#!/usr/bin/env python3
r"""
apply_legacy_nw.py — put the Legacy Nuclear Winter page (and the hosted-first
image rule) onto an already-built plan_master.json, touching nothing else.

reenrich_plan_master.py re-runs EVERY post-pass, including the change notes and
the COBJ links, so on a checkout whose dist/ is older than its enrichers it
rewrites far more than the page it was run for. This runs only the three passes
that decide a row's picture and its page:

    1. plan_images.attach    tags Legacy NW rows (legacy_nw.py), sets image_dir
                             + images — hosted art first, own folder last
    2. plan_apparel_class    statless body-armour -> apparel (reads image_dir)
    3. plan_subpages         plan_page / plan_page_group (reads image_dir)

and then takes the Legacy NW plans out of underarmour.json, whose page selects
its rows by name rather than plan_page.

    python3 src/apply_legacy_nw.py                 # live + pts
    python3 src/apply_legacy_nw.py --channel live

The full build (build_plan_obtain_json.py) and reenrich_plan_master.py already
run the same code, so CI needs no extra step — this is the local fast path.
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import add_weapon_groups
import plan_images
import plan_apparel_class
import plan_subpages

CHANNELS = {
    "live": (os.path.join(REPO, "dist"), os.path.join(REPO, "tsv")),
    "pts":  (os.path.join(REPO, "dist", "pts"), os.path.join(REPO, "tsv", "pts")),
}


def apply(dist_dir, tsv_dir):
    path = os.path.join(dist_dir, "plan_master.json")
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    items = doc.get("items") or []
    # plan_apparel_class reads ARMO through add_weapon_groups' TSV root, which
    # defaults to live — point it at this channel's exports, as reenrich does.
    add_weapon_groups.set_tsv_dir(tsv_dir)
    idx, staged = plan_images.load(dist_dir, tsv_dir, verbose=False)
    plan_images.report(plan_images.attach(items, idx, staged), stream=sys.stdout)
    plan_apparel_class.report(plan_apparel_class.attach(items))
    plan_subpages.report(plan_subpages.attach(items))
    doc["plan_subpages_schema"] = plan_subpages.SCHEMA
    doc["plan_subpages"] = plan_subpages.config()
    lost = plan_subpages.homeless([i for i in items if not i.get("cut")])
    if lost:
        print(f"  *** {len(lost)} live rows on no page — not writing {path}")
        return False
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
    print(f"  wrote {path}")

    nw_ids = {i["id"] for i in items if i.get("legacy_nw")}
    ua_path = os.path.join(dist_dir, "underarmour.json")
    if os.path.exists(ua_path):
        with open(ua_path, encoding="utf-8") as fh:
            ua = json.load(fh)
        dropped = []
        for g in ua.get("groups") or []:
            keep = [r for r in g.get("items") or [] if r.get("id") not in nw_ids]
            dropped += [r.get("name") for r in g.get("items") or [] if r.get("id") in nw_ids]
            g["items"] = keep
            if "count" in g:
                g["count"] = len(keep)
        if dropped:
            if "count" in ua:
                ua["count"] = sum(len(g.get("items") or []) for g in ua.get("groups") or [])
            with open(ua_path, "w", encoding="utf-8") as fh:
                json.dump(ua, fh, ensure_ascii=False, indent=2)
            print(f"  underarmour.json: moved to the Legacy NW page: {', '.join(dropped)}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", choices=sorted(CHANNELS), default="")
    args = ap.parse_args()
    ok = True
    for ch in ([args.channel] if args.channel else ["live", "pts"]):
        dist_dir, tsv_dir = CHANNELS[ch]
        if not os.path.exists(os.path.join(dist_dir, "plan_master.json")):
            print(f"[apply_legacy_nw] {ch}: no plan_master.json, skipped")
            continue
        print(f"[apply_legacy_nw] {ch}")
        ok = apply(dist_dir, tsv_dir) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
