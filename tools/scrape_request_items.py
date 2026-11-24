#!/usr/bin/env python3
r"""
tools/scrape_request_items.py - watch Bethesda's "items you can request" list.

Bethesda's support form for requesting an Atomic Shop item (help.bethesda.net,
Fallout 76 > Atomic Shop > request an item) has an "Item List" drop-down. That
list is the source of truth for the Items Available to Request page
(/df/atom-shop/items-available-to-request/). When Bethesda adds an item to it,
this script picks it up.

How: it opens the support page in a real (headless) Chromium, exactly as a
visitor would, and reads the form the page itself downloads (the
Node/getByParams answer). The item list is the ConciergeListProd1 field:
{ "<option id>": "<item name>" }. Option ids only ever go up, so a new item is
simply an id we haven't seen before.

Writes:
  data/request_items_bethesda.json   the current list + firstSeen date per option id.
                                     First run seeds every item as already seen
                                     (no NEW pill for the existing 500+).
Then build_atom_shop_json.py:
  * gives the NEW pill (31 days) to every page item whose name was first seen
    in the last 31 days, and
  * adds a page row for any newly listed item the page doesn't have yet
    (name matched to its ENTM record for formId / edid / picture / description).

  python tools/scrape_request_items.py            # update data/request_items_bethesda.json
  python tools/scrape_request_items.py --dry-run  # print the list changes only

Needs: pip install playwright && python -m playwright install chromium
Run daily by .github/workflows/scrape-request-items.yml.
"""
import argparse
import datetime
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(REPO, "data", "request_items_bethesda.json")
PAGE = ("https://help.bethesda.net/#en/home/product/1129/category/14/platform1/6/"
        "subcategory/954/question/yes/subcat3/904")
FIELD = "ConciergeListProd1"
TAG = "[request-items]"


def fetch_options():
    """{option id: item name} straight from the form the support page loads."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            page = browser.new_page()
            with page.expect_response(lambda r: "/cc/v2/Node/getByParams" in r.url and r.ok,
                                      timeout=60000) as resp:
                page.goto(PAGE, wait_until="domcontentloaded", timeout=60000)
            data = resp.value.json()
        finally:
            browser.close()
    fields = (((data or {}).get("node") or {}).get("metadata") or {}).get("fields") or {}
    opts = (fields.get(FIELD) or {}).get("options") or {}
    if not opts:
        raise SystemExit(f"{TAG} ERROR: the form came back without the {FIELD} item list "
                         f"(fields: {', '.join(fields) or 'none'}). Bethesda may have changed the form.")
    return {str(k): str(v).strip() for k, v in opts.items() if str(v).strip()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    today = datetime.date.today().isoformat()
    opts = fetch_options()
    print(f"{TAG} Bethesda lists {len(opts)} items you can request")

    old = {"items": {}}
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as fh:
            old = json.load(fh)
    bootstrap = not old.get("items")
    seen = old.get("items", {})

    added, renamed, removed = [], [], []
    items = {}
    for oid, name in sorted(opts.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 0):
        prev = seen.get(oid)
        if prev is None:
            first = "2020-01-01" if bootstrap else today
            if not bootstrap:
                added.append((oid, name))
            items[oid] = {"name": name, "firstSeen": first}
        else:
            items[oid] = dict(prev)
            if prev.get("name") != name:
                renamed.append((oid, prev.get("name"), name))
                items[oid]["name"] = name
                items[oid].setdefault("oldNames", []).append(prev.get("name"))
    for oid, prev in seen.items():
        if oid not in opts:
            removed.append((oid, prev.get("name")))
            gone = dict(prev)
            gone.setdefault("removed", today)
            items[oid] = gone          # kept, so a re-listed item isn't NEW again

    for oid, name in added:
        print(f"{TAG} NEW      #{oid} {name}")
    for oid, a, b in renamed:
        print(f"{TAG} RENAMED  #{oid} {a} -> {b}")
    for oid, name in removed:
        if not seen[oid].get("removed"):
            print(f"{TAG} REMOVED  #{oid} {name}")
    if bootstrap:
        print(f"{TAG} first run: seeded {len(items)} items as already seen (no NEW pills)")
    if args.dry_run:
        return
    out = {
        "_note": "Bethesda support form 'Item List' (help.bethesda.net, Fallout 76 > request an Atomic Shop item). "
                 "Written by tools/scrape_request_items.py. firstSeen drives the 31-day NEW pill.",
        "source": PAGE,
        "checked": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "items": items,
    }
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=1)
        fh.write("\n")
    print(f"{TAG} wrote {os.path.relpath(OUT, REPO)}")


if __name__ == "__main__":
    sys.exit(main())
