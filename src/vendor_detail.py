"""vendor_detail.py - Vendor / Cost / Reputation / Stock for vendor routes,
for EVERY page that prints a plan's obtain routes (Duchess, 24 Oct 2026:
"fix it in the base code so we don't have to fix it again").

One source for the numbers, shared with the Unique Weapons & Armour page:
  Gold Bullion  gold_vendor.routes_for_plan (BVGO price, else the plan's
                DATA_Value; Minerva at build_minerva.MINERVA_DISCOUNT with her
                Emporium / Big Sale list numbers; faction vendor reputation)
  Stamps        build_stamps_json.STAMP_VENDORS (vendor + place, keyed on the
                vendor's leveled list) + the plan BOOK's DATA_Value
  Caps          NOT priced on purpose: caps prices move with Charisma, perks
                and reputation, so any number would be wrong for most readers.

attach(items, tsv_dir) adds route["vendor_detail"] to vendor routes of
plan_master items:
    {"vendor": "Minerva", "location": "travelling vendor",
     "cost": 1000, "currency": "Gold Bullion",
     "reputation": "", "stock": "List 21 (Emporium), List 24 (Big Sale)",
     "lines": ["Vendor: Minerva — travelling vendor", "Stock: ...",
               "Cost: 1,000 Gold Bullion"]}
Renderers print `lines` (label: value) instead of the 100% a vendor route
would otherwise show.
"""
import csv
import glob
import os
import re

csv.field_size_limit(10 ** 9)


def _fid(v):
    v = (v or "").strip().strip('"').upper().split(":")[0]
    return v.zfill(8) if re.fullmatch(r"[0-9A-F]{1,8}", v) else v


def _parse(lines):
    d = {"vendor": "", "location": "", "cost": None, "currency": "",
         "reputation": "", "stock": "", "lines": []}
    for l in lines:
        if l.startswith("Plan:"):
            continue
        d["lines"].append(l)
        k, _, v = l.partition(":")
        v = v.strip()
        if k == "Vendor":
            d["vendor"], _, d["location"] = [x.strip() for x in v.partition("—")]
        elif k == "Cost":
            m = re.match(r"([\d,]+)\s+(.+)$", v)
            if m:
                d["cost"] = int(m.group(1).replace(",", ""))
                d["currency"] = m.group(2)
        elif k == "Reputation":
            d["reputation"] = v
        elif k == "Stock":
            d["stock"] = v
    return d


class VendorDetail:
    def __init__(self, tsv_dir, channel="live"):
        self.gv = None
        try:
            import gold_vendor
            self.gv = gold_vendor.index(channel=channel)
        except Exception:                                 # noqa: BLE001
            self.gv = None
        try:
            import build_stamps_json as bs
            self.stamp_vendors = bs.STAMP_VENDORS
        except Exception:                                 # noqa: BLE001
            self.stamp_vendors = {}
        self.book_value = {}
        self.list_edid = {}
        books = [p for p in glob.glob(os.path.join(tsv_dir, "BOOK_Export_*.tsv"))
                 if "Locations" not in p and "BOOK2" not in os.path.basename(p)]
        if books:
            with open(max(books, key=os.path.getmtime), encoding="utf-8",
                      errors="replace", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
                    try:
                        self.book_value[_fid(r.get("FormID"))] = int(float(r.get("DATA_Value") or 0))
                    except ValueError:
                        pass
        lists = glob.glob(os.path.join(tsv_dir, "LVLI_Export_*_LVLI_List.tsv"))
        if lists:
            with open(max(lists, key=os.path.getmtime), encoding="utf-8",
                      errors="replace", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
                    self.list_edid[_fid(r.get("LVLI_FormID"))] = (r.get("LVLI_EDID") or "").strip()
        self.stamp_by_fid = {}
        for fid, ed in self.list_edid.items():
            if ed in self.stamp_vendors:
                self.stamp_by_fid[fid] = self.stamp_vendors[ed]

    def gold_blocks(self, plan_fid):
        if not self.gv:
            return []
        try:
            return [_parse(b) for b in self.gv.routes_for_plan(plan_fid)]
        except Exception:                                 # noqa: BLE001
            return []

    def for_route(self, plan_fid, route):
        """vendor_detail for one plan route, or None."""
        name = route.get("route") or ""
        low = name.lower()
        lvli = [_fid(x) for x in route.get("lvli") or []]
        if "gold bullion" in low:
            blocks = self.gold_blocks(plan_fid)
            who = name.split("(")[0].strip().lower()
            for b in blocks:
                v = b["vendor"].lower()
                # "Regs" on the route, "Reginald" in the list EditorID
                if v == who or v[:3] == who[:3]:
                    return b
            return blocks[0] if len(blocks) == 1 else None
        stamp = next((self.stamp_by_fid[l] for l in lvli if l in self.stamp_by_fid), None)
        if stamp or "stamp" in low:
            stamp = stamp or (next(iter(self.stamp_vendors.values())) if len(self.stamp_vendors) == 1 else None)
            if not stamp:
                return None
            cost = self.book_value.get(_fid(plan_fid)) or None
            lines = [f"Vendor: {stamp['name']} — {stamp['location']}"]
            if cost:
                lines.append(f"Cost: {cost:,} Stamps")
            return {"vendor": stamp["name"], "location": stamp["location"], "cost": cost,
                    "currency": "Stamps" if cost else "", "reputation": "", "stock": "",
                    "lines": lines}
        return None


def attach(items, tsv_dir, channel="live"):
    vd = VendorDetail(tsv_dir, channel)
    n = 0
    for it in items:
        fid = ((it.get("plan_item") or {}).get("formid")
               or re.sub(r"^PLAN_", "", it.get("id") or "") or "")
        for key in ("obtain_routes", "retired_routes"):
            for r in it.get(key) or []:
                r.pop("vendor_detail", None)
                if r.get("source_type") != "vendor":
                    continue
                d = vd.for_route(fid, r)
                if d:
                    r["vendor_detail"] = d
                    n += 1
    return n
