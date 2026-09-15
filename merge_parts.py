#!/usr/bin/env python3
"""Stitch chunked plan_master parts back into one file.

Companion to build_chunked.sh. Each part is a complete plan_master document for
its slice of the roster, so merging is: take the first part's envelope, and
concatenate every part's `items` in offset order.

Verifies as it goes — duplicate ids, or a total that does not match the roster
size passed as --expect, fail loudly rather than publishing a short file.

  python3 merge_parts.py /tmp/parts_live dist/plan_master.json --expect 2866
"""
import argparse, glob, json, os, sys


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("parts_dir")
    ap.add_argument("out")
    ap.add_argument("--expect", type=int, default=0,
                    help="roster size; the merge fails if the item count differs")
    args = ap.parse_args(argv)

    files = sorted(glob.glob(os.path.join(args.parts_dir, "part_*.json")))
    if not files:
        raise SystemExit(f"[merge] no parts in {args.parts_dir}")

    envelope, items, seen = None, [], set()
    for f in files:
        with open(f, encoding="utf-8") as fh:
            d = json.load(fh)
        if envelope is None:
            envelope = {k: v for k, v in d.items() if k != "items"}
        for it in d.get("items", []):
            i = it.get("id")
            if i in seen:
                raise SystemExit(f"[merge] duplicate id {i} — overlapping chunks?")
            seen.add(i)
            items.append(it)
        print(f"[merge] {os.path.basename(f):24} +{len(d.get('items', [])):5}  total {len(items)}")

    envelope["count"] = len(items)
    envelope["items"] = items

    # The roster is every plan; the published file is every plan MINUS the
    # backpack cosmetics the builder drops upstream, so a small shortfall is
    # expected and a large one is a missing chunk.
    if args.expect:
        missing = args.expect - len(items)
        print(f"[merge] roster {args.expect}, published {len(items)}, difference {missing}")
        if missing < 0 or missing > 60:
            raise SystemExit(f"[merge] refusing to write — difference of {missing} "
                             f"is outside the backpack-cosmetic range; a chunk is probably missing")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(envelope, fh, ensure_ascii=False, indent=2)
    print(f"[merge] wrote {args.out}  ({len(items)} plans)")

    cut = sum(1 for it in items if it.get("cut"))
    live = [it for it in items if not it.get("cut")]
    print(f"[merge]   cut {cut} | obtainable {len(live)}")
    print(f"[merge]   with drop routes {sum(1 for it in live if it.get('obtain_routes'))}"
          f" | with unlock routes {sum(1 for it in live if it.get('obtain_unlocks'))}"
          f" | no source {sum(1 for it in live if not it.get('obtain_routes') and not it.get('obtain_unlocks'))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
