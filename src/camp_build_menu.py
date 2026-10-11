#!/usr/bin/env python3
"""
camp_build_menu.py - stamps every CAMP item with its in-game Build Menu path and
the CAMP page(s) it belongs on. Called by each CAMP builder just before it writes
its dist JSON, so the three builders share one rule set.

    item["buildMenu"] = [{"main": "Utility", "sub": "Player Buffs"}, ...]
    item["pages"]     = ["player-buffs"]          # renderer page keys

The path comes from build_menu.py (the game's workshop.json, nothing typed).
An item with no path gets buildMenu [] and stays on its builder's natural page
with no path line (Duchess, 11 Oct 2026).

Page rules (Duchess, 11 Oct 2026 - see claude/camp-build-menu-handoff.md):
  buff-stations.json
    Utility > Instruments      -> instruments   (pathless Well Tuned items too)
    Utility > Services         -> services      (Healing Arch, Sympto-matic)
    everything else            -> player-buffs  (beds, Blood Transfusion Pump...)
  resource_producers.json
    Resources > Producers      -> producers
    Resources > Food           -> food
    Resources > Water          -> water
    + any producer whose output is Dirty / Boiled / Purified Water or Toxic Goo
      -> water as well (it stays on producers if it is in Producers too)
    Resources > Collectors only (extractors, deposits) -> no page
    no path at all             -> producers (natural page)
  build_menu_items.json ("build-menu" mode, build_build_menu_items_json.py)
    each Build Menu section the item is in -> that page (Services, Water,
    Producers, Food, Player Buffs, Instruments), plus the producers water rule
  every other CAMP JSON        -> its own page key (the caller passes it)

PTS: tsv/pts/ carries no WORKSHOP export, so a PTS build falls back to the live
menu (it changes far less often than the item data) and says so.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# Every FormID field a CAMP item can carry; build_menu resolves any of them.
FID_KEYS = ("formId", "entmFormId", "furnFormId", "actiFormId", "contFormId",
            "resoFormId", "cobjFormId", "npcFormId", "goldVendorFormId")

WATER_BUCKETS = {"dirty water", "boiled water", "purified water", "toxic goo"}

_CACHE = {}


def channel_for(tsv_dir):
    parts = os.path.normpath(str(tsv_dir or "")).replace("\\", "/").lower().split("/")
    return "pts" if "pts" in parts else "live"


def menu(channel="live"):
    if channel not in _CACHE:
        import build_menu
        try:
            _CACHE[channel] = build_menu.load(channel)
        except Exception as e:  # no WORKSHOP export on this channel
            if channel == "live":
                raise
            print(f"  [build menu] no {channel} WORKSHOP export ({e.__class__.__name__}) "
                  f"- using the live Build Menu", file=sys.stderr)
            _CACHE[channel] = menu("live")
    return _CACHE[channel]


def _fids(item):
    return [item[k] for k in FID_KEYS if item.get(k)]


def _subs(item):
    return {(p.get("sub") or "").lower() for p in item.get("buildMenu") or []}


def _buff_pages(item):
    subs = _subs(item)
    if "instruments" in subs:
        return ["instruments"]
    if "services" in subs:
        return ["services"]
    if not subs and "welltuned" in (item.get("buffTypes") or []):
        return ["instruments"]          # non-buildable instruments (no path)
    return ["player-buffs"]


def _producer_pages(item):
    subs = _subs(item)
    pages = []
    if "producers" in subs or not subs:
        pages.append("producers")
    if "food" in subs:
        pages.append("food")
    buckets = set()
    prod = item.get("production") or {}
    for d in prod.get("drops") or []:
        b = (d.get("bucket") or "").lower()
        if b:
            buckets.add(b)
    buckets.add((item.get("bucket") or "").lower())
    if "water" in subs or (buckets & WATER_BUCKETS and pages):
        pages.append("water")
    return pages


# Build Menu sub-section name -> renderer page key ("build-menu" mode).
SUB_PAGE = {"services": "services", "water": "water", "producers": "producers",
            "food": "food", "player buffs": "player-buffs", "instruments": "instruments"}


def _menu_pages(item):
    pages = []
    for p in item.get("buildMenu") or []:
        key = SUB_PAGE.get((p.get("sub") or "").lower())
        if key and key not in pages:
            pages.append(key)
    if "producers" in pages and "water" not in pages and \
            (item.get("bucket") or "").lower() in WATER_BUCKETS:
        pages.append("water")
    return pages


def annotate(items, page, tsv_dir="tsv"):
    """Stamp buildMenu + pages on every item, in place. `page` is the builder's
    page key: "buff-stations", "resource-producers", or the page itself."""
    bm = menu(channel_for(tsv_dir))
    for it in items or []:
        if not isinstance(it, dict):
            continue
        fids = _fids(it)
        it["buildMenu"] = bm.public(*fids) if fids else []
        if page == "buff-stations":
            it["pages"] = _buff_pages(it)
        elif page == "resource-producers":
            it["pages"] = _producer_pages(it)
        elif page == "build-menu":
            it["pages"] = _menu_pages(it)
        else:
            it["pages"] = [page]
    return items
