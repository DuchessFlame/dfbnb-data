#!/usr/bin/env python3
"""
build_build_menu_items_json.py - every CAMP Build Menu object that no other CAMP
builder covers, for the Services / Water / Producers pages (Oct 2026).

Walks the game's own Build Menu (workshop.json via WORKSHOP_Export) for the
sections in SECTIONS, expands each recipe to the objects it places (a recipe's
CNAM is the object itself, or a workshop_LL_* list of variants - one item per
variant), and keeps every object that is NOT already an item in another CAMP
dist JSON, in workshop_producers.json, or in a data/camp/*.json "excluded"
table. So a Decontamination Shower or a Water Purifier gets a full expand the
moment it is in the menu, and an item that later gets its own builder drops
out of here on its own. Nothing is typed.

Per item, all from the exports (camp-item-expands skill):
  name / description / image   ENTM (the variant's Atom Shop entitlement, from
                               the list entry's HasEntitlement condition or the
                               recipe's COBJ2 condition), else the object FULL,
                               else the recipe's CNAM_FULL. Image: art the site
                               already hosts (reusable_images), else the ETDI
                               stem under camp-items/<page>/.
  How to Obtain                build_camp_items_json.resolve_obtain + the
                               9-route builder, plan from COBJ GNAM -> BOOK.
  Output & Effects             - produces items: RESO whose actor value is one
                                 of the CONT's *ResourceAV properties -> produce
                                 LVLI via rng76 + the NAM4 interval GLOB;
                               - dispenses items: a script LVLI property
                                 (ATX_NukaMysteryMachine::pColaDispensed) via
                                 rng76, with RequiredCaps and the cooldown
                                 spell's duration;
                               - a spell on use (DrinkingSpell / DeconArchSpell
                                 script properties) -> SPEL effects;
                               - PlayerRechargerKeyword -> recharges the core
                                 type in its own name.
  Technical                    crafting from COBJ FVPA; Build Limit per Camp /
                               Workshop from the WorkshopCount GLOBs that
                               reference the recipe; Power Required (PowerRequired
                               property or a WorkshopCanBePowered keyword);
                               Flamingo Units (WorkshopBudgetObjectMultiplier).
  Page                         camp_build_menu.annotate(items, "build-menu").

Output: dist/build_menu_items.json  (--pts: dist/pts/, reads tsv/pts/)

  python src/build_build_menu_items_json.py
  python src/build_build_menu_items_json.py --pts
"""
import argparse
import collections
import csv
import datetime
import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import tsv_source                         # noqa: E402
import camp_build_menu                    # noqa: E402
import build_camp_items_json as C         # noqa: E402  shared CAMP helpers

csv.field_size_limit(10 ** 9)

# Build Menu sections this builder fills (sub-category keyword suffix -> page).
SECTIONS = {
    "Utility_Services":    "services",
    "Resources_Water":     "water",
    "Resources_Producers": "producers",
}
# Every CAMP dist JSON whose items are already covered by another builder.
COVERED_FILES = ("buff-stations.json", "resource_producers.json", "collectrons.json",
                 "workshop_producers.json", "weather_stations.json", "repair-bots.json",
                 "allies.json", "pets.json", "cryos.json", "fridges.json", "dispensers.json")
OUT_NAME = "build_menu_items.json"
IMG_BASE = "/wp-content/uploads/guide-images/camp-items/{}"
DEAD = re.compile(r"^(zzz|del_|cut_|test|qa_|debug)|duplicate\d*$|_takenfromatx$", re.I)
FID_RE = re.compile(r"\b([0-9A-Fa-f]{8})\b")
COND_ENTM = re.compile(r"\[ENTM:([0-9A-Fa-f]{8})\]")
COND_CNDF = re.compile(r"\[CNDF:([0-9A-Fa-f]{8})\]")


def fid(v):
    m = FID_RE.search(v or "")
    return m.group(1).upper() if m else ""


def rows(path):
    if not path:
        return
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t")


def newest(pattern, ch, **kw):
    return tsv_source.newest(pattern, channel=ch, required=False, **kw)


# ---------------------------------------------------------------- load
class Data:
    def __init__(self, ch):
        self.ch = ch
        self.tsv = os.path.join(REPO, "tsv", "pts") if ch == "pts" else os.path.join(REPO, "tsv")
        menu_path = newest("WORKSHOP_Export_*.json", ch) or tsv_source.newest("WORKSHOP_Export_*.json")
        self.menu = json.load(open(menu_path, encoding="utf-8"))
        self.cobj = {fid(r["COBJ_FormID"]): r for r in rows(newest("COBJ_Export_*.tsv", ch))}
        cndf = collections.defaultdict(set)
        for r in rows(newest("CNDF_Export_*.tsv", ch)):
            for k, v in r.items():
                if k and k.startswith("Cond") and v:
                    cndf[fid(r.get("FormID"))] |= {e.upper() for e in COND_ENTM.findall(v)}

        def entms(text):
            out = [e.upper() for e in COND_ENTM.findall(text or "")]
            for c in COND_CNDF.findall(text or ""):
                out += sorted(cndf.get(c.upper(), ()))
            return out
        self.cobj_entm = collections.defaultdict(list)
        for r in rows(newest("COBJ2_Export_*.tsv", ch)):
            self.cobj_entm[fid(r.get("COBJ_FormID"))] += entms(r.get("Conditions"))
        self.kids = collections.defaultdict(list)
        self.entry_entm = collections.defaultdict(list)
        for r in rows(newest("LVLI_Export_*_LVLI_Entries.tsv", ch)):
            c = fid(r.get("LVLO_Reference"))
            if not c:
                continue
            self.kids[fid(r.get("LVLI_FormID"))].append(c)
            for k, v in r.items():
                if k and k.startswith("Cond") and v and ("ENTM:" in v or "CNDF:" in v):
                    self.entry_entm[c] += entms(v)
        self.entm = {fid(r["FormID"]): r for r in rows(newest("ENTM_Export_*.tsv", ch))}
        # placed objects: FormID -> (sig, row) with a uniform view
        self.obj = {}
        for pat, sig, fk, ek, nk, kwfmt in (
                ("ACTI_Export_*_ACTI.tsv", "ACTI", "ACTI_FormID", "ACTI_EDID", "ACTI_FULL", "KW_{}"),
                ("FURN_Export_*_FURN.tsv", "FURN", "FURN_FormID", "FURN_EDID", "FURN_FULL", "KW_{}"),
                ("CONT_Export_*.tsv", "CONT", "FormID", "EDID", "FULL", "KW{}"),
                ("MISC_Export_*.tsv", "MISC", "FormID", "EDID", "FULL", None)):
            for r in rows(newest(pat, ch, exclude=["Sally", "_Refs"])):
                f = fid(r.get(fk))
                if not f:
                    continue
                kws = [r.get(kwfmt.format(i)) or "" for i in range(1, 12)] if kwfmt else []
                props = {}
                for i in range(1, 7):
                    av = (r.get(f"Prop_{i}_AV") or "").strip()
                    if av:
                        props[av] = C.safe_float(r.get(f"Prop_{i}_Val") or "")
                self.obj[f] = {"sig": sig, "edid": (r.get(ek) or "").strip(),
                               "full": (r.get(nk) or "").strip(), "kw": " ".join(kws),
                               "props": props, "vmad": r.get("VMAD_Scripts") or "",
                               "premium": "premium" in (r.get("XALG_Flags") or "").lower()}
        self.book = {fid(r["FormID"]): r for r in rows(newest("BOOK_Export_*.tsv", ch, exclude=["_Locations"]))}
        glob_rows = list(rows(newest("GLOB_Export_*.tsv", ch)))
        self.glob_index = C.build_index(glob_rows, "FormID")
        # WorkshopCount GLOBs -> the recipes they limit (same rule as the buff stations)
        self.limits = collections.defaultdict(list)
        for r in glob_rows:
            e = (r.get("EDID") or "")
            if "workshopcount" not in e.lower() or DEAD.search(e):
                continue
            val = C.safe_float(r.get("FLTV") or "")
            if val is None:
                continue
            for k, v in r.items():
                parts = (v or "").split(":")
                if k and k.lower().startswith("ref") and len(parts) >= 3 and parts[2] == "COBJ":
                    self.limits[parts[0].upper()].append((e, int(val)))
        # RESO by actor value, for collectors
        self.reso_by_av = collections.defaultdict(list)
        for r in rows(newest("RESO_Export_*.tsv", ch)):
            av = C.extract_avif_edid(C.clean_str(r.get("NAM1_ActorValue") or ""))
            if av and not C.starts_cut(r.get("EDID") or ""):
                self.reso_by_av[av.upper()].append(r)
        # SPEL effects
        self.spel = collections.defaultdict(list)
        for r in rows(newest("SPEL_Export_*_EFFECTS.tsv", ch)):
            self.spel[fid(r.get("SPEL_FormID"))].append(r)
        # covered / excluded FormIDs
        dist = os.path.join(REPO, "dist", "pts") if ch == "pts" else os.path.join(REPO, "dist")
        self.dist = dist
        self.covered = set()
        for name in COVERED_FILES:
            p = os.path.join(dist, name)
            if not os.path.exists(p):
                p = os.path.join(REPO, "dist", name)
            try:
                items = json.load(open(p, encoding="utf-8")).get("items") or []
            except Exception:
                continue
            for it in items:
                for k, v in it.items():
                    if (k == "formId" or k.endswith("FormId")) and v:
                        self.covered.add(str(v).upper()[-8:])
        for p in glob.glob(os.path.join(REPO, "data", "camp", "*.json")):
            try:
                self.covered |= {str(k).upper() for k in (json.load(open(p, encoding="utf-8")).get("excluded") or {})}
            except Exception:
                pass
        self.rng = None

    def resolver(self):
        if self.rng is None:
            from rng76 import Rng76Data
            self.rng = Rng76Data.from_tsv_root(self.tsv).resolver
        return self.rng


# ---------------------------------------------------------------- output
def _fmt_secs(s):
    s = int(round(s))
    if s >= 3600 and s % 3600 == 0:
        return f"{s // 3600} hour{'s' if s // 3600 != 1 else ''}"
    if s >= 60:
        m = s / 60
        return f"{m:g} min"
    return f"{s}s"


_ROBOT = re.compile(r"RobotFaction.*?=\s*1\.0", re.I)


def spell_rows(D, spel_fid):
    """One labelled row per effect of a spell cast on use (the game's own MGEF
    names and magnitudes). Hunger / thirst show the meter seconds and minutes."""
    out = []
    for r in D.spel.get(spel_fid, []):
        mgef = (r.get("EFID_MGEF_EDID") or "")
        name = (r.get("EFID_MGEF_FULL") or "").strip()
        mag = C.safe_float(r.get("EFIT_Magnitude") or "") or 0.0
        dur = C.safe_float(r.get("EFIT_Duration") or "") or 0.0
        robots = bool(_ROBOT.search(r.get("Conditions") or ""))
        if mgef.startswith("SURV_Drink_Effect"):
            out.append(f"Quenches Thirst: {mag:g} ({_fmt_secs(mag)})")
        elif mgef.startswith("SURV_Food_Effect"):
            out.append(f"Satisfies Hunger: {mag:g} ({_fmt_secs(mag)})")
        elif "DiseaseVector" in mgef:
            out.append(f"Disease Chance: {mag:g}%")
        elif mgef.startswith("RestoreHealth"):
            total = mag * dur if dur > 1 else mag
            out.append(f"Restores Health: {total:g}" + (f" over {_fmt_secs(dur)}" if dur > 1 else ""))
        elif name and not name.startswith(("SURV_", "zzz")):
            label = re.sub(r"DUPLICATE\d*$", "", name).strip()
            val = f"{mag:g}" if mag else "Yes"
            if dur > 1:
                val += f" for {_fmt_secs(dur)}"
            out.append(f"{label}{' (robots)' if robots else ''}: {val}")
    return out


def vmad_props(vmad):
    """'Script::Prop=FID:EDID:SIG##...' -> {Prop: (fid, edid, sig) or raw value}"""
    out = {}
    for part in (vmad or "").split("##"):
        if "::" not in part or "=" not in part:
            continue
        key, val = part.split("::", 1)[1].split("=", 1)
        bits = val.split(":")
        out[key.strip()] = (bits[0].upper(), bits[1], bits[2]) if len(bits) >= 3 else val.strip()
    return out


def output_for(D, o, name):
    """-> (outputInfo, outputRows, production, outputNotes)"""
    info, out_rows, production, notes = "", [], None, []
    props = vmad_props(o["vmad"])
    # dispensers (mystery machines, vending machines)
    lv = props.get("pColaDispensed")
    if isinstance(lv, tuple):
        drops = C.consolidate_drops(C.resolve_drops_via_rng76(D.resolver(), lv[0]))
        production = {"intervalHours": None, "intervalDisplay": "", "resourceName": "",
                      "resourceEdid": "", "drops": drops, "modes": [
                          {"name": "Default", "resoFormId": "", "lvliFormId": lv[0],
                           "resourceName": "", "drops": drops}]}
        cost = props.get("RequiredCaps")
        if cost and not isinstance(cost, tuple):
            out_rows.append(f"Cost per Use: {C._thousands(cost)} Caps")
        cd = props.get("CooldownSpell")
        if isinstance(cd, tuple):
            durs = [C.safe_float(r.get("EFIT_Duration") or "") or 0 for r in D.spel.get(cd[0], [])]
            if durs and max(durs) > 0:
                out_rows.append(f"Cooldown: {_fmt_secs(max(durs))}")
        info = "Pay to dispense one random item from the list below."
    # collectors / purifiers: RESO whose actor value is one of the CONT's
    # properties (Water, DirtyWater, ResourceAV_*). The property value is how
    # many the object makes per collection.
    if production is None and o["sig"] == "CONT":
        resos = [(av, r) for av in o["props"] for r in D.reso_by_av.get(av.upper(), [])]
        if resos:
            av, r = resos[0]
            amount = o["props"].get(av)
            if amount:
                out_rows.append(f"Amount per Collection: {amount:g}")
            lvli = C.extract_lvli_formid(C.clean_str(r.get("NAM2_Produce") or ""))
            drops = C.consolidate_drops(C.resolve_drops_via_rng76(D.resolver(), lvli)) if lvli else []
            hours, disp = None, ""
            g = C.extract_glob_formid(C.clean_str(r.get("NAM4_Interval") or ""))
            if g:
                hours = C.glob_fltv(D.glob_index, g)
                disp = C.interval_display(hours) if hours is not None else ""
            production = {"intervalHours": hours, "intervalDisplay": disp,
                          "resourceName": C.resource_name_for_reso(r) if hasattr(C, "resource_name_for_reso") else "",
                          "resourceEdid": "", "drops": drops,
                          "modes": [{"name": "Default", "resoFormId": fid(r.get("FormID")),
                                     "lvliFormId": lvli or "", "resourceName": "", "drops": drops}]}
    # a spell on use: drinking water, decontamination
    bucket = ""
    for key in ("DrinkingSpell", "DeconArchSpell"):
        sp = props.get(key)
        if isinstance(sp, tuple):
            out_rows += spell_rows(D, sp[0])
            if key == "DrinkingSpell":
                bucket = "Drinking Water"   # you drink straight from it
    if "playerrechargerkeyword" in o["kw"].lower():
        core = "Plasma Cores" if "plasma" in (name + o["edid"]).lower() else "Fusion Cores"
        info = f"Recharges {core} placed in it while it is powered."
    return info, out_rows, production, notes, bucket


# ---------------------------------------------------------------- items
def build_info(D, recipe, o):
    camp = work = None
    for e, val in D.limits.get(recipe, []):
        if re.search(r"_camp$", e, re.I):
            camp = val
        else:
            work = val
    lims = D.limits.get(recipe, [])
    if camp is None and work is not None and len(lims) == 1:
        camp = work
    p = o["props"]
    power = bool((p.get("PowerRequired") or 0) > 0 or "workshopcanbepowered" in o["kw"].lower())
    flam = p.get("WorkshopBudgetObjectMultiplier")
    return ("Build Limit per Camp: {}\nBuild Limit per Workshop: {}\nPower Required: {}\n"
            "Flamingo Units: {}").format(
        camp if camp is not None else "—", work if work is not None else "—",
        "Yes" if power else "No", f"{flam:g}" if flam is not None else "—")


def leaves(D, f, seen):
    if f in seen:
        return []
    seen.add(f)
    if f in D.kids:
        out = []
        for c in D.kids[f]:
            out += leaves(D, c, seen)
        return out
    return [f]


def make_item(D, page, recipe, obj_fid, entm_fid):
    o = D.obj.get(obj_fid) or {"sig": "", "edid": "", "full": "", "kw": "", "props": {},
                              "vmad": "", "premium": False}
    cobj = D.cobj.get(recipe, {})
    e = D.entm.get(entm_fid) if entm_fid else None
    name = (C.clean_str((e or {}).get("FULL") or "") or o["full"]
            or C.clean_str(cobj.get("CNAM_FULL") or ""))
    if not name:
        return None
    entm_edid = C.clean_str((e or {}).get("EDID") or "") or o["edid"]
    gnam = fid(cobj.get("GNAM_FormID"))
    book = C.parse_book_for_plan(D.book.get(gnam), D.glob_index)
    obtain = C.resolve_obtain(entm_edid if e else (o["edid"] if o["premium"] else ""), book)
    if not e and not o["premium"] and book.get("planName"):
        obtain["display"] = "Craftable at a C.A.M.P. after learning {}.".format(book["planName"])
    folder = "resource-producers" if page == "producers" else page
    C.IMAGE_SETS.setdefault("camp-items-" + folder, {
        "base": IMG_BASE.format(folder), "ext": ".avif", "main_from": "etdi"})
    info, out_rows, production, notes, bucket = output_for(D, o, name)
    item = {
        "formId": entm_fid or obj_fid,
        "entmFormId": entm_fid or "",
        "cobjFormId": recipe,
        "edid": entm_edid,
        "displayName": name,
        "description": C.clean_desc((e or {}).get("DESC") or ""),
        "imageUrl": C.image_webp_url(e, "camp-items-" + folder) if e else None,
        "imageCarousel": C.image_carousel_urls(e, "camp-items-" + folder) if e else [],
        "howToObtain": obtain,
        "obtainRoutes": C.build_obtain_routes(obtain, book),
        "tradeable": obtain.get("tradeable"),
        "planName": book.get("planName") or "",
        "craftingRequirements": C.fvpa_to_array(C.effective_fvpa(cobj)) if cobj else [],
        "buildInfo": build_info(D, recipe, o),
        "outputInfo": info,
        "outputRows": out_rows,
        "outputNotes": notes,
        "seasonNumber": C.parse_season_from_edid(entm_edid),
        "status": "live",
        "cutContent": False,
    }
    if o["sig"]:
        item[o["sig"].lower() + "FormId"] = obj_fid
    if production:
        item["production"] = production
        item["intervalDisplay"] = production.get("intervalDisplay") or ""
    drops = (production or {}).get("drops") or []
    item["bucket"] = bucket or C.classify_producer_bucket(drops, o["edid"], entm_edid, name.replace(" ", ""))
    tech = []
    if e:
        tech += [f"ENTM EDID: {entm_edid}", f"ENTM FormID: {entm_fid}"]
    if o["edid"]:
        tech += [f"{o['sig']} EDID: {o['edid']}", f"{o['sig']} FormID: {obj_fid}"]
    tech += [f"COBJ EDID: {cobj.get('COBJ_EDID', '')}", f"COBJ FormID: {recipe}"]
    item["technicalNotes"] = "\n".join(tech)
    return item


def camp_guides():
    """{title: url} of the written CAMP guide pages (template "guide" under the
    CAMP category in tsv/guide_index.tsv). An item whose name contains a guide's
    title links to it - "My Personal Terminal" -> the Personal Terminal guide."""
    out = {}
    path = os.path.join(REPO, "tsv", "guide_index.tsv")
    for r in rows(path if os.path.exists(path) else None):
        if r.get("parentId") == "df-tc-camp" and r.get("template") == "guide" and r.get("title"):
            out[r["title"]] = r.get("url") or ""
    return out


def build(ch):
    D = Data(ch)
    guides = camp_guides()
    C.HOSTED = C.load_reusable_images(D.dist)
    # fallout76_seasons.tsv is a hand-kept reference table that lives in tsv/
    # only; PTS reads the same season names.
    seasons = D.tsv if os.path.exists(os.path.join(D.tsv, "fallout76_seasons.tsv")) \
        else os.path.join(REPO, "tsv")
    C._load_season_themes(seasons)
    items, seen = [], set()
    for cat in D.menu:
        for sub in cat.get("SubCategories") or []:
            sedid = sub["CategoryKeyword"]["FormEditorID"]
            page = next((p for k, p in SECTIONS.items() if sedid.endswith(k)), None)
            if not page:
                continue
            for rec in sub.get("Recipes") or []:
                recipe = "%08X" % int(rec["FormID"])
                cobj = D.cobj.get(recipe, {})
                if DEAD.search(cobj.get("COBJ_EDID") or "") or recipe in D.covered:
                    continue
                cnam = fid(cobj.get("CNAM_FormID"))
                objs = leaves(D, cnam, set()) if cnam else []
                for of in objs:
                    o = D.obj.get(of, {})
                    if of in seen or of in D.covered or DEAD.search(o.get("edid") or ""):
                        continue
                    ents = D.entry_entm.get(of) or (D.cobj_entm.get(recipe) if len(objs) == 1 else [])
                    ents = [x for x in ents if x in D.entm and not DEAD.search(D.entm[x].get("EDID") or "")]
                    ent = ents[0] if ents else ""
                    if ent and ent in D.covered:
                        continue
                    it = make_item(D, page, recipe, of, ent)
                    if it:
                        for title, url in guides.items():
                            if url and title.lower() in it["displayName"].lower():
                                it["guideUrl"], it["guideTitle"] = url, title
                        seen.add(of)
                        items.append(it)
    # One row per item as a player sees it: variants of one object that share
    # an entitlement (the open / closed Camden Park bin) or a name (a Gold
    # Bullion vendor copy of the Chicken Coop) collapse to the first, an
    # entitled one preferred.
    items.sort(key=lambda i: (not i["entmFormId"], i["displayName"].lower()))
    kept, keys = [], set()
    for i in items:
        k = (i["entmFormId"] or "", ) if i["entmFormId"] else ("name", i["displayName"].lower())
        nk = ("name", i["displayName"].lower())
        if k in keys or nk in keys:
            continue
        keys |= {k, nk}
        kept.append(i)
    items = kept
    camp_build_menu.annotate(items, "build-menu", D.tsv)
    items = [i for i in items if i.get("pages")]
    items.sort(key=lambda i: i["displayName"].lower())
    out = {"generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
           "type": "build_menu_items", "count": len(items), "items": items}
    os.makedirs(D.dist, exist_ok=True)
    path = os.path.join(D.dist, OUT_NAME)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    by = collections.Counter(p for i in items for p in i["pages"])
    print(f"[OK] {os.path.relpath(path, REPO)}: {len(items)} items {dict(by)}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args()
    sys.exit(build("pts" if a.pts else "live"))
