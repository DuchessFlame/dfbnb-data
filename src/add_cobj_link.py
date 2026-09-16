#!/usr/bin/env python3
r"""
add_cobj_link.py — re-resolve a plan's recipe and created record, in place.

`cobj` and `cnam` are the plan's link to the COBJ recipe it teaches and the
record that recipe makes. Everything downstream leans on them: Technical prints
the recipe and the created record, Output & Effects only exists when the created
record is an OMOD, plan_images names its candidate files after them, and the
backpack / weapon / armour classifiers read them.

They are resolved inside build_plan_obtain_json.py's main loop, so until now the
only way to pick up a newer COBJ export was the ~80 minute rebuild — even though
the resolution itself is a pure join costing seconds. That gap had teeth: for
most of September 2026 the live BOOK export was current and COBJ was three
months stale, so all 69 plans the Slasher patch added carried `cobj: null` and
`cnam: null`, showed no recipe in Technical, and could not be matched to art
named after the record it pictures.

This runs that join, and only that join, against the newest exports:

    python3 src/add_cobj_link.py dist/plan_master.json

It reuses build_plan_obtain_json's own functions rather than reimplementing the
resolution. The CondProxy hop and the EditorID recovery fallback are fiddly and
were got wrong more than once; a second copy of them here would drift from the
builder the first time either was touched, which is the failure the three
plan_master copies already taught this repo once.

WHAT IT WILL NOT TOUCH
----------------------
`type` — the record bucket. It is the progress-store key, so moving a plan
between buckets strands every tick a reader has made on it. A bucket that would
now classify differently is REPORTED and left alone, the same call
plan_subpages.py made when it stopped routing on `type` altogether.

`obtain_routes` / `obtain_ledger` — those come from LVLI and the rng76 waterfall,
not from COBJ. Nothing here can improve them and nothing here may damage them.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_plan_obtain_json as bpo


def _roster_co_refs(tsv_dir):
    """BOOK FormID -> the first :COBJ in its ReferencedBy columns.

    The fallback link, used when a plan has no HasLearnedRecipe drop entry —
    which is every vendor-only and quest-only plan, and every plan whose LVLI
    export is older than its BOOK export. Same scan the builder does.
    """
    out = {}
    path = bpo.newest("BOOK_Export_*.tsv", tsv_dir)
    if not path:
        return out
    for row in bpo.read_rows(path):
        full = (row.get("FULL") or "").strip()
        if not (full.startswith("Plan: ") or full.startswith("Recipe: ")):
            continue
        fid = (row.get("FormID") or "").strip().upper()
        for j in range(1, 46):
            v = (row.get(f"Ref{j}") or "").strip()
            if v.upper().endswith(":COBJ"):
                out[fid] = v.split(":")[0].strip().upper()
                break
    return out


def attach(items, tsv_dir="tsv", effects=True, stats=None):
    """Refresh cobj / cnam (and, where it newly resolves, effects) on every row."""
    stats = stats if stats is not None else {}

    def bump(k):
        stats[k] = stats.get(k, 0) + 1

    tsv_dir = os.path.abspath(tsv_dir)
    bpo.TSV = tsv_dir                       # every newest() below reads THIS channel
    bpo.SIG_INDEX = bpo.build_sig_index()
    cobj_idx = bpo.build_cobj_index()
    book_ent = bpo.build_book_entry_index()
    omod_by_edid = bpo.build_omod_edid_index()
    co_refs = _roster_co_refs(tsv_dir)

    fx_tables = None
    if effects:
        fx_tables = (bpo.build_omod_prop_index(), bpo.build_ench_index(),
                     bpo.build_mgef_index(), bpo.build_perk_desc_by_mgef(),
                     bpo.build_curve_index())

    stats["exports"] = {
        "COBJ": os.path.basename(bpo.newest("COBJ_Export_*.tsv", tsv_dir) or ""),
        "BOOK": os.path.basename(bpo.newest("BOOK_Export_*.tsv", tsv_dir) or ""),
        "OMOD": os.path.basename(bpo.newest("OMOD_Export_*.tsv", tsv_dir) or ""),
    }
    stats["bucket_drift"] = []

    for item in items:
        plan = item.get("plan_item") or {}
        fid = (plan.get("formid") or "").upper()
        edid = plan.get("edid") or ""
        if not fid:
            bump("no_plan_item")
            continue

        # Same order of preference the builder uses: the HasLearnedRecipe COBJ
        # named on a drop entry is authoritative, because it is the game telling
        # us which recipe the entry gates on. The BOOK's own ReferencedBy link is
        # the fallback for a plan that never drops.
        co_fid = ""
        for en in book_ent.get(fid, []):
            if en["recipe_cobj"]:
                co_fid = en["recipe_cobj"]
                break
        if not co_fid:
            co_fid = co_refs.get(fid, "")
        cobj = cobj_idx.get(co_fid) if co_fid else None

        if cobj and not cobj.get("cnam_fid") and \
           "condproxy" in (cobj.get("edid") or "").lower():
            import re
            stem = re.sub(r".*condproxy_?", "", cobj["edid"], flags=re.I).lower()
            if stem:
                for cfid, c in cobj_idx.items():
                    ce = (c.get("edid") or "").lower()
                    if c.get("cnam_fid") and stem in ce and "condproxy" not in ce:
                        co_fid, cobj = cfid, c
                        break
        if not (cobj or {}).get("cnam_fid"):
            om = bpo.omod_from_book_edid(edid, omod_by_edid)
            if om:
                cobj = dict(cobj or {"formid": co_fid, "edid": ""})
                cobj["cnam_fid"], cobj["cnam_edid"] = om["fid"], om["edid"]

        cat, has_img, cnam_sig, cnam_fid, cnam_edid = bpo.classify_plan(edid, cobj)

        was_cobj = item.get("cobj")
        was_cnam = item.get("cnam")
        new_cobj = {"formid": co_fid, "edid": cobj["edid"]} if cobj else None
        new_cnam = ({"formid": cnam_fid, "edid": cnam_edid, "sig": cnam_sig}
                    if cnam_fid else None)

        # Never trade a resolved link for an unresolved one. A newer export that
        # drops a record is far more likely to be an export that did not finish
        # than the studio deleting a recipe, and blanking Technical on a row that
        # was fine is a worse outcome than carrying a stale FormID for a patch.
        if new_cobj is None and was_cobj is not None:
            bump("kept_old_cobj")
            new_cobj = was_cobj
        if new_cnam is None and was_cnam is not None:
            bump("kept_old_cnam")
            new_cnam = was_cnam

        if was_cobj is None and new_cobj is not None:
            bump("gained_cobj")
        elif was_cobj != new_cobj and new_cobj is not None:
            bump("changed_cobj")
        if was_cnam is None and new_cnam is not None:
            bump("gained_cnam")
        elif was_cnam != new_cnam and new_cnam is not None:
            bump("changed_cnam")

        item["cobj"] = new_cobj
        item["cnam"] = new_cnam

        # Display-only, safe to refresh. `type` is not — see the module docstring.
        if cat and cat != item.get("type"):
            stats["bucket_drift"].append(
                f"{item.get('name')}  {item.get('type')} -> {cat}")
        if item.get("has_image_box") != has_img:
            item["has_image_box"] = has_img
            bump("image_box_changed")
        label = bpo.category_label(item.get("type"), has_img, cnam_sig)
        if label and label != item.get("category_label"):
            item["category_label"] = label
            bump("category_label_changed")

        # Output & Effects exists only for a created OMOD. A plan that just
        # gained its link can gain its effects in the same pass.
        if fx_tables and cnam_sig == "OMOD" and cnam_fid:
            fx = bpo.resolve_effects(cnam_fid, *fx_tables)
            if fx and not item.get("effects"):
                bump("gained_effects")
            if fx:
                item["effects"] = fx
    return stats


def report(stats, stream=sys.stderr):
    ex = stats.get("exports") or {}
    print(f"  [cobj-link] from {ex.get('COBJ', '?')}", file=stream)
    for key in sorted(k for k in stats if k not in ("exports", "bucket_drift")):
        print(f"    {key:24s} {stats[key]}", file=stream)
    drift = stats.get("bucket_drift") or []
    if drift:
        print(f"    {len(drift)} plans would now classify into a different "
              f"bucket — NOT moved (the bucket is the progress-store key):",
              file=stream)
        for line in drift[:15]:
            print(f"      {line}", file=stream)
        if len(drift) > 15:
            print(f"      ... and {len(drift) - 15} more", file=stream)


def main(argv=None):
    ap = argparse.ArgumentParser(description="re-resolve cobj/cnam in place")
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--tsv-dir", default="tsv")
    ap.add_argument("--no-effects", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    for path in args.paths:
        if not os.path.exists(path):
            print(f"[cobj-link] skip (missing): {path}")
            continue
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        items = doc.get("items") or []
        print(f"[cobj-link] {path}: {len(items)} rows")
        report(attach(items, args.tsv_dir, not args.no_effects), stream=sys.stdout)
        if args.dry_run:
            continue
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
