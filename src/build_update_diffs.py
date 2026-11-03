#!/usr/bin/env python3
"""
build_update_diffs.py — the "Datamined Changes" data for Game Update Guide pages.

For every update or hotfix listed in tsv/update_diffs.tsv (the update register),
compare the game-file exports from before it with the exports from after it and
write one frozen file per drop:

    dist/update_diffs/<drop_id>.json     the changes, grouped for the page
    dist/update_diffs/index.json         which drops each guide page shows

A drop's file is written ONCE. Re-running skips drops that already have a file,
so a page keeps showing the same fixed before/after even after newer exports
replace the old ones. Use --rebuild <drop_id> (or --rebuild all) to redo one.

Register columns (tsv/update_diffs.tsv):
    drop_id         update-70, hotfix-2026-10-01 ...
    update_number   70 (blank for hotfixes)
    kind            Update | Hotfix
    name            Shadows of the Dead of Winter
    version         1.7.26.13
    live_date       2026-09-15
    before_exports  newest export of each type at or before this month is "before"
    after_exports   the export set made straight after the drop went live
    page_slug       the /df/game-update-guides/<slug>/ page that shows it
    notes           free text

Duchess's rules (3 Nov 2026):
    * Developer/test records (debug, test, sandbox, test chest, dev, QA) are left out.
    * Enemy-only gear (NONPLAYABLE) is left out.
    * Cut records (zzz_ / CUT_) are shown with a "Cut" flag. A record that was in
      game and is renamed to zzz_ in this drop is shown as "Cut this patch".
    * Creature "weapons" (cr...) are kept.
    * Nameless NPC templates are grouped into one row per family so players can
      see new enemy types are coming, without hundreds of nameless rows.

Usage:
    python src/build_update_diffs.py                 build any missing drops
    python src/build_update_diffs.py --rebuild all   rebuild every drop
    python src/build_update_diffs.py --rebuild update-70
"""
import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone

csv.field_size_limit(10 ** 9)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TSV_DIR = os.path.join(REPO, "tsv")
REGISTER = os.path.join(TSV_DIR, "update_diffs.tsv")
OUT_DIR = os.path.join(REPO, "dist", "update_diffs")

# ── Months in export filenames ──────────────────────────────────────────────
_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def month_key(tag):
    """'September_2026' -> 202609 ; None if it doesn't look like a month tag."""
    m = re.fullmatch(r"([A-Za-z]+)_(\d{4})", tag or "")
    if not m or m.group(1).lower() not in _MONTHS:
        return None
    return int(m.group(2)) * 100 + _MONTHS[m.group(1).lower()]


# ── What gets compared ──────────────────────────────────────────────────────
# prefix, suffix: file is <prefix>_Export_<Month>_<Year><suffix>.tsv
# fields: (column, label) compared for "Modified" rows. Additions and removals
# are reported for every record whatever its columns.
TYPES = [
    dict(key="WEAP", sig="WEAP", group="Weapons", prefix="WEAP", suffix="_Base",
         fid="WEAP_FormID", edid="WEAP_EDID", name="WEAP_FULL",
         fields=[("WEAP_FULL", "Name"), ("EILV_Level", "Item level")]),
    dict(key="ARMO", sig="ARMO", group="Armour & Apparel", prefix="ARMO", suffix="_ARMOUR",
         fid="ARMO_FormID", edid="ARMO_EDID", name="ARMO_FULL",
         fields=[("ARMO_FULL", "Name"), ("DAMA_Physical_Amount", "Damage resist"),
                 ("DAMA_Energy_Amount", "Energy resist"), ("DAMA_Rad_Amount", "Rad resist"),
                 ("DATA_Weight", "Weight"), ("DATA_Value", "Value")]),
    dict(key="OMOD", sig="OMOD", group="Weapon & Armour Mods", prefix="OMOD", suffix="",
         fid="OMOD_FormID", edid="OMOD_EDID", name="FULL",
         fields=[("FULL", "Name"), ("DESC", "Description")]),
    dict(key="NPC", sig="NPC_", group="NPCs & Enemies", prefix="NPC", suffix="",
         fid="FormID", edid="EDID", name="FULL",
         fields=[("FULL", "Name"), ("ACBS_Level", "Level"), ("DNAM_CalcHealth", "Health"),
                 ("ACBS_XPOffset", "XP offset")]),
    dict(key="PERK", sig="PERK", group="Perks & Effects", prefix="PERK", suffix="",
         fid="PERK_FormID", edid="PERK_EDID", name="FULL",
         fields=[("FULL", "Name"), ("DESC", "Description")]),
    dict(key="CHAL", sig="CHAL", group="Challenges", prefix="CHAL", suffix="",
         fid="FormID", edid="EDID", name="FULL",
         fields=[("FULL", "Name"), ("SNAM", "Required count"), ("MNAM", "Reward")]),
    dict(key="ALCH", sig="ALCH", group="Food, Drink & Chems", prefix="ALCH", suffix="",
         fid="ALCH_FormID", edid="ALCH_EDID", name="FULL",
         fields=[("FULL", "Name"), ("DESC", "Description"), ("Weight", "Weight"), ("Value", "Value")]),
    dict(key="BOOK", sig="BOOK", group="Plans, Recipes & Books", prefix="BOOK", suffix="",
         fid="FormID", edid="EDID", name="FULL",
         fields=[("FULL", "Name"), ("DATA_Value", "Value")]),
    dict(key="MISC", sig="MISC", group="Misc Items & Junk", prefix="MISC", suffix="",
         fid="FormID", edid="EDID", name="FULL",
         fields=[("FULL", "Name"), ("Value", "Value"), ("Weight", "Weight")]),
    dict(key="NOTE", sig="NOTE", group="Notes & Holotapes", prefix="NOTE", suffix="",
         fid="NOTE_FormID", edid="NOTE_EDID", name="FULL",
         fields=[("FULL", "Name")]),
    dict(key="ENTM", sig="ENTM", group="Atom Shop Items", prefix="ENTM", suffix="",
         fid="FormID", edid="EDID", name="FULL",
         fields=[("FULL", "Name"), ("DESC", "Description")]),
    dict(key="FISH", sig="FISH", group="Fish", prefix="FISH", suffix="",
         fid="FormID", edid="EDID", name="FULL",
         fields=[("FULL", "Name")]),
]

GROUP_ORDER = ["Leveled Lists", "Weapons", "Weapon & Armour Mods", "Armour & Apparel",
               "NPCs & Enemies", "Perks & Effects", "Challenges", "Food, Drink & Chems",
               "Plans, Recipes & Books", "Misc Items & Junk", "Notes & Holotapes",
               "Atom Shop Items", "Fish"]

# ── Duchess's rules ─────────────────────────────────────────────────────────
DEV_RE = re.compile(r"debug|sandbox|testchest|test_?cell|(^|_)(test|dev|qa)(_|\d|$)", re.I)
NONPLAYABLE_RE = re.compile(r"NONPLAYABLE", re.I)
CUT_RE = re.compile(r"^(zzz|cut_)", re.I)


def is_dev(edid):
    return bool(DEV_RE.search(edid or ""))


def is_hidden(edid):
    return is_dev(edid) or bool(NONPLAYABLE_RE.search(edid or ""))


def is_cut(edid):
    return bool(CUT_RE.search(edid or ""))


def humanize(edid):
    """'SDOW_LvlSlasherFanMelee' -> 'Slasher Fan Melee' (best effort, for nameless records)."""
    s = re.sub(r"^(zzz_?|zzzz_?)", "", edid or "", flags=re.I)
    parts = [p for p in s.split("_") if p]
    words = []
    for p in parts:
        p = re.sub(r"([a-z])([A-Z])", r"\1 \2", p)
        p = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", p)
        words.extend(p.split())
    drop = {"LL", "LLD", "LLS", "LLV", "Lvl", "Enc", "HTO", "ATX", "SDOW", "cr"}
    kept = [w for w in words if w not in drop] or words
    out = " ".join(kept)
    for a, b in (("Bo S", "BoS"), ("Mod ", "Mod "), ("Power Armor", "Power Armour")):
        out = out.replace(a, b)
    return out


# ── Reading exports ─────────────────────────────────────────────────────────
def read_tsv(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        r = csv.reader(f, delimiter="\t")
        try:
            head = next(r)
        except StopIteration:
            return [], []
        return head, [row for row in r]


def pick_export(prefix, suffix, month_tag, exact):
    """Path of <prefix>_Export_<Month>_<Year><suffix>.tsv for month_tag
    (exact=True) or the newest one at or before it (exact=False)."""
    want = month_key(month_tag)
    pat = re.compile(rf"^{re.escape(prefix)}_Export_([A-Za-z]+_\d{{4}}){re.escape(suffix)}\.tsv$")
    best = None
    for fn in os.listdir(TSV_DIR):
        m = pat.match(fn)
        if not m:
            continue
        k = month_key(m.group(1))
        if k is None:
            continue
        if (exact and k == want) or (not exact and k <= want):
            if best is None or k > best[0]:
                best = (k, fn)
    return os.path.join(TSV_DIR, best[1]) if best else None


def index_rows(head, rows, fid_col):
    i = head.index(fid_col)
    out = {}
    for r in rows:
        if len(r) > i and r[i].strip():
            out[r[i].strip().strip('"').upper()] = r
    return out


def cell(head, row, col):
    if col not in head:
        return ""
    i = head.index(col)
    return row[i].strip().strip('"') if i < len(row) else ""


def norm(v):
    """Numbers compare by value, so 0.100000 and 0.1 are the same."""
    v = (v or "").strip()
    try:
        f = float(v)
        return repr(round(f, 4))
    except ValueError:
        return v


def show_num(v):
    try:
        f = float(v)
        return str(int(f)) if f == int(f) else ("%g" % f)
    except ValueError:
        return v


# ── Record types ────────────────────────────────────────────────────────────
def diff_type(t, before, after, names):
    """Rows for one record type. names: FormID -> display name (filled in)."""
    hb, rb = read_tsv(before)
    ha, ra = read_tsv(after)
    ib, ia = index_rows(hb, rb, t["fid"]), index_rows(ha, ra, t["fid"])
    for fid, r in ia.items():
        nm = cell(ha, r, t["name"])
        if nm:
            names[fid] = nm
    for fid, r in ib.items():
        nm = cell(hb, r, t["name"])
        if nm and fid not in names:
            names[fid] = nm

    rows, nameless_npcs = [], defaultdict(list)
    for fid, r in ia.items():
        edid, nm = cell(ha, r, t["edid"]), cell(ha, r, t["name"])
        if is_hidden(edid):
            continue
        if fid not in ib:
            if t["key"] == "NPC" and not nm:
                nameless_npcs[(edid.split("_")[0] or "Other")].append(edid)
                continue
            rows.append(dict(k="add", sig=t["sig"], fid=fid, edid=edid,
                             name=nm or humanize(edid), named=bool(nm), cut=is_cut(edid)))
            continue
        pr = ib[fid]
        pedid = cell(hb, pr, t["edid"])
        changes = []
        for col, label in t["fields"]:
            a, b = cell(hb, pr, col), cell(ha, r, col)
            if norm(a) != norm(b):
                changes.append(dict(field=label, before=show_num(a) or "none", after=show_num(b) or "none"))
        newly_cut = is_cut(edid) and not is_cut(pedid)
        if changes or newly_cut:
            row = dict(k="mod", sig=t["sig"], fid=fid, edid=edid,
                       name=nm or cell(hb, pr, t["name"]) or humanize(edid),
                       named=bool(nm), cut=is_cut(edid), changes=changes)
            if newly_cut:
                row["cutThisPatch"] = True
                row["oldEdid"] = pedid
            rows.append(row)
    for fid, r in ib.items():
        if fid in ia:
            continue
        edid = cell(hb, r, t["edid"])
        if is_hidden(edid):
            continue
        nm = cell(hb, r, t["name"])
        rows.append(dict(k="rem", sig=t["sig"], fid=fid, edid=edid,
                         name=nm or humanize(edid), named=bool(nm), cut=is_cut(edid)))

    # Nameless NPC templates: one row per family, the variants listed under it.
    for fam, edids in sorted(nameless_npcs.items()):
        edids.sort()
        rows.append(dict(k="add", sig="NPC_", fid="", edid=f"{fam}_*", group=True,
                         name=f"{len(edids)} new enemy and NPC templates ({fam})",
                         what="Behind-the-scenes spawn templates: the game uses these to spawn the named enemies you fight.",
                         sub=[dict(k="add", text=humanize(e), edid=e) for e in edids]))
    return rows


def ref_parts(ref):
    """'0008E3A8:LLS_Creature_Bloatfly:LVLI' -> ('0008E3A8', 'LLS_Creature_Bloatfly', 'LVLI')."""
    p = (ref or "").split(":")
    while len(p) < 3:
        p.append("")
    return p[0].upper(), p[1], p[2]


def entries_by_list(path):
    h, rows = read_tsv(path)
    col = {c: (h.index(c) if c in h else None) for c in
           ("LVLI_FormID", "LVLO_Reference", "LVOV_ChanceNoneValue", "LVOG_ChanceNoneGlobal",
            "LVIV_Quantity", "LVLV_MinimumLevel")}
    out = defaultdict(OrderedDict)
    for r in rows:
        g = lambda c: (r[col[c]].strip() if col[c] is not None and col[c] < len(r) else "")
        lid, ref = g("LVLI_FormID").upper(), g("LVLO_Reference")
        if not lid or not ref:
            continue
        key, n = ref, 2
        while key in out[lid]:          # the same item listed twice in one list
            key = f"{ref}#{n}"
            n += 1
        out[lid][key] = dict(ref=ref, chance=g("LVOV_ChanceNoneValue"),
                             chanceGlob=g("LVOG_ChanceNoneGlobal"),
                             qty=g("LVIV_Quantity"), minlvl=g("LVLV_MinimumLevel"))
    return out


def ref_label(ref, names):
    fid, edid, sig = ref_parts(ref)
    return names.get(fid) or humanize(edid) or fid, edid, sig


def diff_lvli(before_m, after_m, names):
    lb = pick_export("LVLI", "_LVLI_List", before_m, exact=False)
    la = pick_export("LVLI", "_LVLI_List", after_m, exact=True)
    eb = pick_export("LVLI", "_LVLI_Entries", before_m, exact=False)
    ea = pick_export("LVLI", "_LVLI_Entries", after_m, exact=True)
    if not all((lb, la, eb, ea)):
        return [], None
    hb, rb = read_tsv(lb)
    ha, ra = read_tsv(la)
    ib, ia = index_rows(hb, rb, "LVLI_FormID"), index_rows(ha, ra, "LVLI_FormID")
    for fid, r in ia.items():
        names.setdefault(fid, cell(ha, r, "LVLI_FULL") or "")
    pe, ce = entries_by_list(eb), entries_by_list(ea)

    list_fields = [("LVCV_ChanceNoneValue", "Chance none"), ("LVLG_ChanceNoneGlobal", "Chance none (global)"),
                   ("LVMV_MaxValue", "Max")]
    rows = []

    def sub_line(k, e, extra=""):
        nm, edid, sig = ref_label(e["ref"], names)
        return dict(k=k, text=nm + extra, edid=edid, sig=sig)

    for fid, r in ia.items():
        edid = cell(ha, r, "LVLI_EDID")
        if is_hidden(edid):
            continue
        nm = cell(ha, r, "LVLI_FULL") or humanize(edid)
        cur = ce.get(fid, {})
        visible = lambda d: OrderedDict((k, v) for k, v in d.items() if not is_hidden(ref_parts(v["ref"])[1]))
        cur = visible(cur)
        if fid not in ib:
            rows.append(dict(k="add", sig="LVLI", fid=fid, edid=edid, name=nm, named=False, cut=is_cut(edid),
                             what=f"{len(cur)} entries",
                             sub=[sub_line("add", e) for e in list(cur.values())[:25]],
                             subMore=max(0, len(cur) - 25)))
            continue
        prev = visible(pe.get(fid, {}))
        subs = []
        for k, e in cur.items():
            if k not in prev:
                subs.append(sub_line("add", e))
        for k, e in prev.items():
            if k not in cur:
                subs.append(sub_line("rem", e))
        for k, e in cur.items():
            if k in prev:
                o = prev[k]
                bits = []
                for f, label in (("chance", "chance none"), ("chanceGlob", "chance none global"),
                                 ("qty", "quantity"), ("minlvl", "min level")):
                    if norm(o[f]) != norm(e[f]):
                        bits.append(f"{label} {show_num(o[f]) or 'none'} → {show_num(e[f]) or 'none'}")
                if bits:
                    subs.append(sub_line("mod", e, ": " + "; ".join(bits)))
        changes = []
        pr = ib[fid]
        for col, label in list_fields:
            a, b = cell(hb, pr, col), cell(ha, r, col)
            if norm(a) != norm(b):
                changes.append(dict(field=label, before=show_num(a) or "none", after=show_num(b) or "none"))
        newly_cut = is_cut(edid) and not is_cut(cell(hb, pr, "LVLI_EDID"))
        if subs or changes or newly_cut:
            if len(prev) != len(cur):
                changes.insert(0, dict(field="Entries", before=str(len(prev)), after=str(len(cur))))
            row = dict(k="mod", sig="LVLI", fid=fid, edid=edid, name=nm, named=False, cut=is_cut(edid),
                       changes=changes, sub=subs[:25], subMore=max(0, len(subs) - 25))
            if newly_cut:
                row["cutThisPatch"] = True
            rows.append(row)
    for fid, r in ib.items():
        if fid in ia:
            continue
        edid = cell(hb, r, "LVLI_EDID")
        if is_hidden(edid):
            continue
        rows.append(dict(k="rem", sig="LVLI", fid=fid, edid=edid,
                         name=cell(hb, r, "LVLI_FULL") or humanize(edid), named=False, cut=is_cut(edid)))
    return rows, dict(before=[os.path.basename(lb), os.path.basename(eb)],
                      after=[os.path.basename(la), os.path.basename(ea)])


# ── One drop ────────────────────────────────────────────────────────────────
KIND_ORDER = {"add": 0, "mod": 1, "rem": 2}


def build_drop(d):
    names, groups, sources, skipped = {}, {}, {}, []
    for t in TYPES:
        b = pick_export(t["prefix"], t["suffix"], d["before_exports"], exact=False)
        a = pick_export(t["prefix"], t["suffix"], d["after_exports"], exact=True)
        if not b or not a:
            skipped.append(t["key"])
            continue
        rows = diff_type(t, b, a, names)
        sources[t["key"]] = dict(before=os.path.basename(b), after=os.path.basename(a))
        groups.setdefault(t["group"], []).extend(rows)
    lv_rows, lv_src = diff_lvli(d["before_exports"], d["after_exports"], names)
    if lv_src:
        groups["Leveled Lists"] = lv_rows
        sources["LVLI"] = lv_src
    else:
        skipped.append("LVLI")

    out_groups, totals = [], dict(add=0, mod=0, rem=0)
    for g in GROUP_ORDER:
        rows = groups.get(g) or []
        if not rows:
            continue
        # Named things first within each kind: they are what players recognise.
        rows.sort(key=lambda r: (KIND_ORDER[r["k"]], not r.get("named", True) and not r.get("group"),
                                 r["name"].lower()))
        tot = dict(add=0, mod=0, rem=0)
        for r in rows:
            tot[r["k"]] += 1
            totals[r["k"]] += 1
        out_groups.append(dict(name=g, total=tot, rows=rows))

    return OrderedDict(
        schema=1,
        drop=d["drop_id"], kind=d["kind"], update=d["update_number"] or None,
        name=d["name"], version=d["version"], liveDate=d["live_date"],
        beforeExports=d["before_exports"], afterExports=d["after_exports"],
        generatedAt=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        totals=totals, groups=out_groups, sources=sources, notCompared=skipped,
    )


def read_register():
    with open(REGISTER, encoding="utf-8", newline="") as f:
        return [r for r in csv.DictReader(f, delimiter="\t") if (r.get("drop_id") or "").strip()]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--rebuild", default="", help="drop_id to rebuild, or 'all'")
    args = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)

    drops = read_register()
    index = OrderedDict(schema=1, pages=OrderedDict(), drops=OrderedDict())
    for d in drops:
        d = {k: (v or "").strip() for k, v in d.items()}
        path = os.path.join(OUT_DIR, d["drop_id"] + ".json")
        frozen = os.path.exists(path) and args.rebuild not in ("all", d["drop_id"])
        if frozen:
            data = json.load(open(path, encoding="utf-8"))
            print(f"[update_diffs] {d['drop_id']}: frozen, kept as is")
        else:
            data = build_drop(d)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            t = data["totals"]
            print(f"[update_diffs] {d['drop_id']}: wrote {os.path.relpath(path, REPO)} "
                  f"(+{t['add']} ~{t['mod']} -{t['rem']}; not compared: {', '.join(data['notCompared']) or 'none'})")
        has_changes = sum(data["totals"].values()) > 0
        index["drops"][d["drop_id"]] = OrderedDict(
            kind=d["kind"], update=d["update_number"] or None, name=d["name"], version=d["version"],
            liveDate=d["live_date"], totals=data["totals"], file=d["drop_id"] + ".json", hasChanges=has_changes)
        if has_changes:   # a hotfix that changed no game files gets no tab
            index["pages"].setdefault(d["page_slug"], []).append(d["drop_id"])
    for slug in index["pages"]:
        index["pages"][slug].sort(key=lambda i: index["drops"][i]["liveDate"])
    with open(os.path.join(OUT_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=1)
    print(f"[update_diffs] index: {sum(len(v) for v in index['pages'].values())} drops on {len(index['pages'])} pages")
    return 0


if __name__ == "__main__":
    sys.exit(main())
