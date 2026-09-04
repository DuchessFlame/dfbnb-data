#!/usr/bin/env python3
r"""
gold_vendor.py — the one generative source for the Gold Bullion obtain route.

WHY THIS EXISTS
===============
Before this module, every CAMP builder decided the Gold Bullion route from a
hand-written table:

    build_allies_pets_weather_json.py   WEATHER_PLAN_BOOKS = {one entry}
    build_buff_stations_json.py         gold_merged / gold_how / gold_tech
    build_camp_items_json.py            (nothing at all)

A table only grows when somebody remembers to grow it, so the route was right
for the handful of items somebody had looked at and silently N/A for the rest.
An audit in September 2026 found 24 CAMP items on live pages that a gold vendor
demonstrably sells and whose pages said the Gold Bullion route was N/A —
including every purchasable ally. "N/A" is not a neutral default here: the page
contract (camp-item-expands, "How to Obtain") is that a dimmed N/A route means
*we checked and it genuinely isn't sold there*, so a missing route reads as a
positive claim that the item cannot be bought. Twelve allies said so wrongly.

THE CHAIN
=========
There is an exact path from a CAMP item to its gold-vendor plan. No token
guessing, no display-name matching — both of which false-positive badly here
("Silver Collectron" vs "Scavenging Station with Silver Scavenge Bot",
"nukacola" against three different fridges).

    1. BOOK   SCORE_Recipe_workshop_CAMP_Utility_<X>_GoldVendor
              carries FULL ("Plan: <X>") and BVGO -> Econ_GoldVendor_Tier_NN
    2. LVLI   W05_LLV_GoldVendor_<Faction>_<Vendor>_<n>_<Rank> stocks that BOOK
              (recursively — a vendor list may nest sub-lists)
    3. COBJ   SCORE_workshop_CondProxy_co_Category<Type>_<X>_GoldVendor
              has GNAM = the BOOK. It has NO CNAM, which is why the item cannot
              be reached from the COBJ alone.
    4. CNDF   <Season>_COBJ_<X>_Condition names BOTH that CondProxy COBJ and the
              item's ENTM in its conditions:
                  HasLearnedRecipe(<CondProxy COBJ>)
                  HasEntitlement(<ENTM>)

Step 4 is the join. Every CAMP page item already carries ``entmFormId``, so
ENTM -> plan -> vendor/rank/price resolves with zero heuristics. As of the
July 2026 export all 56 gold CondProxy conditions yield both halves.

STOCKED vs MERELY PRESENT
=========================
Bethesda authors the plan record before the vendor sells it. Eleven
``*_GoldVendor`` BOOKs exist that no leveled list references at all — the
Hollywood Vanity's among them. Those are NOT a Gold Bullion route; emitting one
would tell readers to go buy something no vendor has. ``route_for_entm``
returns nothing for them and they are listed in ``unstocked`` so a build log can
report them and they can be picked up the patch they go live.

USAGE
-----
    import gold_vendor

    gv = gold_vendor.index()                 # channel="pts" for the PTS build
    lines = gv.route_for_entm(entm_form_id)  # [] when not sold
    if lines:
        populated["Gold Bullion"] = (lines, tradeable, "N/A")

``route_for_entm`` returns the labelled rows the renderer aligns
(camp-item-expands, "Route detail formats"), in the fixed order
Plan / Vendor / Cost / Reputation.
"""

import csv
import re

import tsv_source

# Where each gold vendor stands. Static: these are fixed NPCs at fixed
# settlements, not something the exports carry.
VENDOR_LOCATIONS = {
    "Samuel":   "Foundation",
    "Mortimer": "Crater",
    "Molly":    "Foundation",
    "Reginald": "Whitespring Refuge",
    "Minerva":  "travelling vendor — rotating stock",
}

# W05_LLV_GoldVendor_Settler_Samuel_6_Ally -> faction / vendor / rank
_VENDOR_LIST_RE = re.compile(
    r"LLV_GoldVendor_(?P<faction>[A-Za-z]+)_(?P<vendor>[A-Za-z]+)"
    r"(?:_(?P<order>\d+)_(?P<rank>[A-Za-z]+))?$",
    re.IGNORECASE,
)
_MINERVA_RE = re.compile(r"Minerva", re.IGNORECASE)
_BVGO_RE = re.compile(r"^\s*(?P<edid>\S+)\s*\[GLOB:(?P<fid>[0-9A-Fa-f]+)\]")
_COBJ_REF_RE = re.compile(r"\[COBJ:([0-9A-Fa-f]{8})\]")
_ENTM_REF_RE = re.compile(r"\[ENTM:([0-9A-Fa-f]{8})\]")
# Deprecated records: Bethesda's own "this is dead" marker.
_DEAD_RE = re.compile(r"^z{2,}", re.IGNORECASE)


def _rows(path):
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _fid(v):
    return (v or "").strip().strip('"').upper()


def _faction_display(name):
    """LVLI faction token -> the plural players see: "Settler" -> "Settlers"."""
    n = (name or "").strip()
    if not n:
        return ""
    n = n[0].upper() + n[1:]
    return n if n.endswith("s") else n + "s"


def _norm_name(name):
    """Fold an item name for the ENTM-name fallback index.

    Case, punctuation and the ENTM "Lite Ally: " prefix carry no information
    here — "Lite Ally: Sam Nguyen", "Sam Nguyen" and "sam nguyen" are one item.
    Nothing else is stripped: dropping words like "Station" would collapse
    "Silver Collectron" onto "Scavenging Station with Silver Scavenge Bot",
    which is exactly the false positive this module exists to avoid.
    """
    n = re.sub(r"^\s*lite\s+ally\s*:\s*", "", name or "", flags=re.I)
    return re.sub(r"[^a-z0-9]+", "", n.lower())


def _thousands(value):
    try:
        return "{:,}".format(int(str(value).replace(",", "").strip()))
    except (ValueError, TypeError):
        return str(value)


class GoldVendorIndex:
    """ENTM FormID -> the Gold Bullion route for that item."""

    def __init__(self, by_entm, plans, unstocked, by_name=None):
        self._by_entm = by_entm      # ENTM fid -> plan fid
        self._plans = plans          # plan fid -> resolved plan dict
        self.unstocked = unstocked   # plan fid -> EDID, authored but unsold
        self._by_name = by_name or {}  # normalised ENTM name -> plan fid

    # -- queries ---------------------------------------------------------
    def plan_for_entm(self, entm_form_id):
        """The stocked gold plan FormID for an ENTM, or "" if none."""
        plan = self._by_entm.get(_fid(entm_form_id), "")
        return plan if plan in self._plans else ""

    def plan_for_name(self, name):
        """The stocked gold plan for an ENTM *display name*, or "".

        The name fallback for items whose page record carries no ``entmFormId``
        at all. Several builders key a CAMP item off its ATX store record
        (``ATX_COMP_Furniture_Weapons_Mechanic_Sam``) and never pick up the
        SCORE ENTM, so the exact ENTM -> plan chain has nothing to join on and
        the item goes silently N/A — Sam Nguyen, the Brewing Vat and the
        Radstag Field Dressing Station all did.

        This is deliberately NOT general name matching, which false-positives
        badly on CAMP items. The candidate pool is only the ENTMs that already
        resolved to a gold plan through the exact chain — 63 names as of the
        July 2026 export, with no collisions — so a hit means "this name is one
        of the known gold-vendor items", not "these two strings look alike".
        Callers must use it only when ``entmFormId`` is empty.
        """
        return self._by_name.get(_norm_name(name), "")

    def details_for_entm(self, entm_form_id):
        """The resolved plan dict (name/vendor/faction/rank/cost), or None."""
        plan = self.plan_for_entm(entm_form_id)
        return self._plans.get(plan)

    def route_for_entm(self, entm_form_id):
        """The Gold Bullion route lines for an item, or [] when not sold.

        Fixed label order per camp-item-expands: Plan, Vendor, Cost,
        Reputation. A label whose data is missing is dropped, never faked.
        """
        return self._route_for_plan(self.plan_for_entm(entm_form_id))

    def route_for_name(self, name):
        """The Gold Bullion route lines for an item name, or [].

        The name-keyed twin of ``route_for_entm``. Use ONLY for records with no
        ``entmFormId`` — see ``plan_for_name``.
        """
        return self._route_for_plan(self.plan_for_name(name))

    def _route_for_plan(self, plan_fid):
        d = self._plans.get(plan_fid)
        if not d:
            return []
        lines = []
        if d["plan_name"]:
            lines.append(f"Plan: {d['plan_name']}")
        if d["vendor"]:
            loc = VENDOR_LOCATIONS.get(d["vendor"], "")
            lines.append(f"Vendor: {d['vendor']} — {loc}" if loc
                         else f"Vendor: {d['vendor']}")
        if d["cost"]:
            lines.append(f"Cost: {_thousands(d['cost'])} Gold Bullion")
        if d["rank"]:
            lines.append(f"Reputation: {d['faction']} — {d['rank']}"
                         if d["faction"] else f"Reputation: {d['rank']}")
        return lines

    def tech_lines_for_entm(self, entm_form_id):
        """Form ID / EDID rows for the Technical block, or []."""
        plan = self.plan_for_entm(entm_form_id)
        if not plan:
            return []
        d = self._plans[plan]
        out = [f"Gold Vendor Plan: {d['plan_edid']} ({plan})"]
        if d["list_edid"]:
            out.append(f"Vendor List: {d['list_fid']} {d['list_edid']}")
        if d["price_edid"]:
            out.append(f"Price Global: {d['price_fid']} {d['price_edid']}"
                       f" = {d['cost']}")
        return out

    def apply_to_routes(self, routes, entm_form_id, tradeable=False,
                        lines=None):
        """Fill in the Gold Bullion entry of a built 9-route array, in place.

        Returns True when a route was written. Replaces an existing Gold
        Bullion entry as well as filling an empty one: a builder's own
        hand-rolled block predates this module and is missing the Reputation
        and vendor-location rows, so the generative one is strictly better and
        keeps every page's wording identical. Does nothing when the item has no
        stocked gold plan — never overwrite a real route with an empty one.
        """
        if lines is None:
            lines = self.route_for_entm(entm_form_id)
        if not lines:
            return False
        for entry in routes:
            if entry.get("route") == "Gold Bullion":
                entry["populated"] = True
                entry["lines"] = lines
                entry["tradeable"] = tradeable
                entry["dropRate"] = "N/A"
                return True
        return False

    def apply_to_items(self, items, label=""):
        """Fill the Gold Bullion route on every item in a built page list.

        One call per page, just before the JSON is written, keyed on each
        item's ``entmFormId``. Doing it here rather than at each construction
        site is deliberate: the builders emit items from several places (RESO
        grouping, entitlement-only leftovers, hand-built bed rows) and a route
        that is only right on some of those paths is how this went wrong the
        first time. Prints what it changed so the build log carries the count.
        """
        filled, by_name = [], []
        for it in items:
            entm = it.get("entmFormId") or ""
            lines = self.route_for_entm(entm) if entm else []
            named = False
            if not lines and not entm:
                # No ENTM on the record at all — fall back on the item's own
                # name against the known gold-plan ENTM names. See
                # plan_for_name for why this is safe and why it is limited to
                # records with no ENTM: an item that HAS an entmFormId and
                # still resolves to nothing genuinely has no gold plan, and
                # must stay N/A.
                for cand in (it.get("displayName"), it.get("planName")):
                    plan = self.plan_for_name(cand)
                    if plan:
                        lines, named = self._route_for_plan(plan), True
                        break
            if not lines:
                continue
            tradeable = bool(it.get("tradeable"))
            if self.apply_to_routes(it.get("obtainRoutes") or [], entm,
                                    tradeable=tradeable, lines=lines):
                name = it.get("displayName") or entm
                filled.append(name)
                if named:
                    by_name.append(name)
        if filled:
            print(f"  Gold Bullion route filled for {len(filled)} "
                  f"{label or 'item'}(s): {', '.join(sorted(filled))}")
        if by_name:
            print(f"  [WARN] {len(by_name)} of those matched by NAME, not "
                  f"ENTM — the page record has no entmFormId: "
                  f"{', '.join(sorted(by_name))}")
        return filled

    def _plan_of_item(self, it):
        entm = _fid(it.get("entmFormId") or "")
        if entm:
            return self._by_entm.get(entm, "")
        for cand in (it.get("displayName"), it.get("planName")):
            plan = self._by_name.get(_norm_name(cand))
            if plan:
                return plan
        return ""

    def report_unstocked(self, items, label=""):
        """Warn about items whose gold plan exists but no vendor stocks yet.

        These are correctly N/A today. They are worth a build-log line because
        the patch that puts one on a vendor's list changes nothing else in the
        export — without the warning the page would stay silently wrong.

        Read the plan EDID in the line, not just the item name. Bethesda
        mis-authored COBJ 008A706E (the S24 Survival Cache CondProxy): its GNAM
        points at the S23 Crashed Cargo Bot plan, so Rip Daring's Survival
        Cache reports under CrashedCargoBot_GoldVendor. Harmless while both are
        unstocked; if the Cargo Bot plan ever goes on a vendor list the Cache
        will inherit its route and must be excluded by hand.
        """
        for it in items:
            plan = self.unstocked.get(self._plan_of_item(it), "")
            if plan:
                print(f"  [INFO] {it.get('displayName')}: gold-vendor plan "
                      f"{plan} exists but no leveled list stocks it — "
                      f"Gold Bullion stays N/A")

    def __len__(self):
        return len(self._by_entm)


def index(channel="live"):
    """Build the ENTM -> gold-vendor-plan index from the newest exports."""
    lvli_entries = _rows(tsv_source.newest(
        "LVLI_Export_*_LVLI_Entries.tsv", channel=channel))
    cobj_rows = _rows(tsv_source.newest("COBJ_Export_*.tsv", channel=channel))
    cndf_rows = _rows(tsv_source.newest("CNDF_Export_*.tsv", channel=channel))
    book_rows = _rows(tsv_source.newest(
        "BOOK_Export_*.tsv", channel=channel, exclude="Locations"))
    glob_rows = _rows(tsv_source.newest("GLOB_Export_*.tsv", channel=channel))

    # ---- 1. what the gold vendors actually stock -----------------------
    by_list, edid_of = {}, {}
    for r in lvli_entries:
        by_list.setdefault(_fid(r["LVLI_FormID"]), []).append(r)
        edid_of[_fid(r["LVLI_FormID"])] = r["LVLI_EDID"] or ""

    stocked = {}   # entry FormID -> list of (list_fid, list_edid)

    def walk(root_fid, fid, seen):
        if fid in seen:
            return
        seen = seen | {fid}
        for r in by_list.get(fid, ()):
            parts = (r["LVLO_Reference"] or "").split(":")
            if len(parts) < 3:
                continue
            ref_fid, _, ref_type = _fid(parts[0]), parts[1], parts[2]
            if ref_type == "LVLI":
                walk(root_fid, ref_fid, seen)
            else:
                stocked.setdefault(ref_fid, []).append(
                    (root_fid, edid_of.get(root_fid, "")))

    for fid, edid in edid_of.items():
        if "GoldVendor" in edid:
            walk(fid, fid, set())

    # ---- 2. CondProxy COBJ -> plan BOOK --------------------------------
    proxy_plan = {}
    for c in cobj_rows:
        gnam = _fid(c.get("GNAM_FormID"))
        if gnam and "CondProxy" in (c.get("COBJ_EDID") or ""):
            proxy_plan[_fid(c.get("COBJ_FormID"))] = gnam

    # ---- 3. CNDF: CondProxy + ENTM in the same condition set -----------
    by_entm = {}
    for r in cndf_rows:
        conds = " ".join((r.get(f"Cond{i}") or "") for i in range(1, 18))
        if "GoldVendor" not in conds:
            continue
        plans = {proxy_plan[p] for p in map(_fid, _COBJ_REF_RE.findall(conds))
                 if p in proxy_plan}
        if len(plans) != 1:
            # Two plans in one condition set would make the mapping ambiguous;
            # skip rather than pick one. Has not occurred as of July 2026.
            continue
        plan = plans.pop()
        for entm in _ENTM_REF_RE.findall(conds):
            by_entm[_fid(entm)] = plan

    # ---- 4. resolve each plan: name, vendor, rank, price ---------------
    glob_val, glob_edid = {}, {}
    for g in glob_rows:
        glob_val[_fid(g["FormID"])] = g.get("FLTV") or g.get("Value") or ""
        glob_edid[_fid(g["FormID"])] = g.get("EDID") or ""

    book_by_id = {_fid(b["FormID"]): b for b in book_rows}
    plans, unstocked = {}, {}

    for plan_fid in set(by_entm.values()):
        b = book_by_id.get(plan_fid)
        if not b:
            continue
        edid = b.get("EDID") or ""
        if _DEAD_RE.match(edid):
            continue
        if plan_fid not in stocked:
            unstocked[plan_fid] = edid
            continue

        # Prefer the ranked list with the LOWEST order number — that is the
        # reputation the player actually needs, not the highest that also
        # happens to carry it.
        best = None
        for list_fid, list_edid in stocked[plan_fid]:
            m = _VENDOR_LIST_RE.search(list_edid)
            if m and m.group("rank"):
                order = int(m.group("order"))
                if best is None or order < best[0]:
                    best = (order, list_fid, list_edid, m)
        if best is None:
            for list_fid, list_edid in stocked[plan_fid]:
                m = _VENDOR_LIST_RE.search(list_edid)
                if m:
                    best = (99, list_fid, list_edid, m)
                    break
        vendor = rank = faction = ""
        list_fid = list_edid = ""
        if best:
            _, list_fid, list_edid, m = best
            vendor = (m.group("vendor") or "").capitalize()
            rank = (m.group("rank") or "").capitalize()
            faction = _faction_display(m.group("faction"))
        elif any(_MINERVA_RE.search(e) for _, e in stocked[plan_fid]):
            vendor = "Minerva"
            list_fid, list_edid = stocked[plan_fid][0]

        cost = price_fid = price_edid = ""
        m = _BVGO_RE.match(b.get("BVGO") or "")
        if m:
            price_fid = _fid(m.group("fid")).zfill(8)
            price_edid = glob_edid.get(price_fid) or m.group("edid")
            raw = glob_val.get(price_fid, "")
            try:
                cost = str(int(float(raw)))
            except (ValueError, TypeError):
                cost = raw

        plans[plan_fid] = {
            "plan_fid":   plan_fid,
            "plan_edid":  edid,
            "plan_name":  re.sub(r"^\s*plan\s*:\s*", "",
                                 (b.get("FULL") or "").strip(), flags=re.I),
            "vendor":     vendor,
            "faction":    faction,
            "rank":       rank,
            "cost":       cost,
            "list_fid":   list_fid,
            "list_edid":  list_edid,
            "price_fid":  price_fid,
            "price_edid": price_edid,
        }

    by_entm = {e: p for e, p in by_entm.items()
               if p in plans or p in unstocked}

    # ---- 5. name fallback for records that carry no ENTM ---------------
    # Names of the ENTMs already resolved above — nothing wider. A collision
    # would make a name ambiguous, so both sides are dropped rather than
    # guessed; the build log says which.
    entm_rows = _rows(tsv_source.newest("ENTM_Export_*.tsv", channel=channel))
    by_name, clashes = {}, set()
    for r in entm_rows:
        plan = by_entm.get(_fid(r.get("FormID")))
        if not plan:
            continue
        for cand in (r.get("NNAM"), r.get("FULL")):
            n = _norm_name(cand)
            if not n:
                continue
            if by_name.get(n, plan) != plan:
                clashes.add(n)
            by_name[n] = plan
    for n in clashes:
        by_name.pop(n, None)
    if clashes:
        print(f"  [INFO] gold_vendor: {len(clashes)} ENTM name(s) map to more "
              f"than one plan and are excluded from the name fallback")

    return GoldVendorIndex(by_entm, plans, unstocked, by_name)


if __name__ == "__main__":
    gv = index()
    print(f"ENTM -> gold plan: {len(gv)}")
    print(f"stocked plans:     {len(gv._plans)}")
    print(f"authored but unsold ({len(gv.unstocked)}):")
    for fid, edid in sorted(gv.unstocked.items(), key=lambda x: x[1]):
        print(f"  {fid}  {edid}")
