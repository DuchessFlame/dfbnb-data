#!/usr/bin/env python3
r"""
build_quest_lore_pages_json.py - the Quest Lore pages (Duchess, 14-15 Nov 2026).

Every note, holotape and terminal in the game, split by the quest it belongs to:

  /df/quests/quest-lore/main-quest-lore/           Primary quests
  /df/quests/quest-lore/side-quest-lore/           Secondary / side quests
  /df/quests/quest-lore/daily-and-unmarked-lore/   Daily, unmarked (misc), events, expeditions
  /df/quests/quest-lore/world-lore-<region>/       lore no quest owns, one page per region,
                                                   grouped by place (+ world-lore-other)

On the quest pages each quest is one ROOT expand (A-Z) holding its lore A-Z,
wherever on the map it is - each item says where in How to Obtain. Same JSON
shape and renderer as the lore checklists (df-bnb-collectables.js renderSlasherLore).

Sources (newest of each, live tsv/ or tsv/pts/):
  holotapes  build_lore_holotapes_json.build()  (NOTE + NOTE2 + ALLYDLG _Lore)
  notes      dist[/pts]/collectables_notes.json (quest + text) + BOOK2 placements
             + ALLYDLG _LoreNotes (the note as the game lays it out)
  terminals  TERMLORE_Export (every terminal: name, menu entries, conditions)
             + TERM2_Export (where each terminal stands). An entry is lore when it
             shows text (ItemType 8) of 100+ characters; sub-terminals are walked up
             to the placed terminal that opens them. One row per placed terminal.
  quests     QUEST_Export (name, Quest Type). A note / holotape names its quest;
             a terminal's quest = a quest its entries' conditions name, else the
             quest whose EditorID the terminal's EditorID starts with.
Cut / test content (plan_sources.is_cut_content, test-room-only placements, cut
quests) is left out.

    python src/build_quest_lore_pages_json.py          # live -> dist/quest_lore/
    python src/build_quest_lore_pages_json.py --pts    # PTS  -> dist/pts/quest_lore/
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

import tsv_source                      # noqa: E402
import note_markup                     # noqa: E402
import plan_sources                    # noqa: E402
import build_lore_holotapes_json as H  # noqa: E402
from build_slasher_lore_json import _rows, _slug, on_server   # noqa: E402

csv.field_size_limit(1 << 30)

IMG_DIR = "/wp-content/uploads/guide-images/quests/quest-lore/"
MIN_ENTRY = 100            # characters of shown text that make a terminal entry lore
# test / template records, and Babylon = Nuclear Winter (the whole mode was removed,
# so its terminals aren't "still in the world" even though the files place them)
TESTY = re.compile(r"(?i)(test|template|debug|smoke|placeholder|babylon)")
JUNK_TERM = re.compile(r"(?i)^(test|debug|qa|zz|temp|x01x|cut_|del_)")
PAGES = {
    "main":  ("Main Quest Lore", "Every note, holotape and terminal entry from the main quests, one quest at a time, with where to find it and the full text."),
    "side":  ("Side Quest Lore", "Every note, holotape and terminal entry from the side quests, one quest at a time, with where to find it and the full text."),
    "daily": ("Daily & Unmarked Lore", "Every note, holotape and terminal entry from daily quests, unmarked quests, events and expeditions, one quest at a time."),
}
# Holotape / note quest labels that are internal holders, and the player-facing names
QUEST_ALIAS = {"[RSVP Master]": "Survivor Stories", "[Badge Handler Quest]": "Pioneer Scouts",
               "[Misc - A Perfect Getaway]": "A Perfect Getaway", "[W05 Holotapes]": "Wastelanders"}


def newest(root, pat):
    return tsv_source.newest(str(Path(root) / pat), required=False)


def norm(s):
    s = re.sub(r"<Alias=[^>]*>", "", s or "")
    s = re.sub(r"\s{2,}", ": ", s.strip())
    return s.lower()


ALIAS_TAG = re.compile(r"\s*<Alias=[^>]*>")
ERROR_ENTRY = re.compile(r"(?i)^\W*(error|access denied|file not found|insufficient)")


def clean_name(s):
    """Drop <Alias=...> tags (filled in by the game at run time)."""
    return re.sub(r"\s{2,}", " ", ALIAS_TAG.sub("", s or "")).strip()


def unesc(s):
    return (s or "").replace("\\n", "\n").replace("\\t", "\t").replace("\\\\", "\\")


class Quests:
    RANK = {"Primary": 0, "Secondary": 1, "Side Quest": 1}

    def __init__(self, root):
        self.by_name, self.by_edid = {}, {}
        p = newest(root, "QUEST_Export_*.tsv")
        for r in (_rows(p) if p else []):
            ed = (r.get("EDID") or "").strip()
            nm = re.sub(r"<Alias=[^>]*>", "", (r.get("FULL - Name") or "")).strip()
            q = {"edid": ed, "name": nm, "type": (r.get("Quest Type") or "").strip(),
                 "formId": (r.get("FormID") or "").strip().upper()}
            self.by_edid[ed] = q
            if nm and not nm.startswith("["):
                k = norm(nm)
                old = self.by_name.get(k)
                if not old or self.RANK.get(q["type"], 9) < self.RANK.get(old["type"], 9):
                    self.by_name[k] = q
        self.named = {e: q for e, q in self.by_edid.items() if q["name"] and not q["name"].startswith("[")}

    def from_name(self, name):
        if not name:
            return None
        if name in QUEST_ALIAS:
            return {"edid": name, "name": QUEST_ALIAS[name], "type": "Miscellaneous", "formId": ""}
        if name.startswith("[") or "_" in name or name in ("HolotapeQuest",) or name.startswith("SHELVED"):
            return None
        q = self.by_name.get(norm(name))
        if q:
            return q
        return {"edid": "", "name": re.sub(r"\s{2,}", ": ", name.strip()), "type": "", "formId": ""}

    def for_terminal(self, edid, cond_quests):
        for e in cond_quests:
            if e in self.named:
                return self.named[e]
        parts = edid.split("_")
        for i in range(len(parts), 1, -1):
            pre = "_".join(parts[:i])
            if pre in self.named:
                return self.named[pre]
            c = sorted((k for k in self.named if k.startswith(pre + "_")), key=len)
            if c:
                return self.named[c[0]]
        return None


def page_of(q):
    t = q.get("type", "")
    if t == "Primary":
        return "main"
    if t in ("Secondary", "Side Quest") or q["name"].startswith("Ally:"):
        return "side"
    return "daily"            # Daily, Miscellaneous (unmarked), events, expeditions, other


def quest_is_cut(q):
    return bool(q and q.get("edid") and plan_sources.is_cut_content(q["edid"]))


def load_placements(root, pat):
    p = newest(root, pat)
    per, test_only = defaultdict(list), set()
    for r in (_rows(p) if p else []):
        fid = (r.get("item_formid") or "").strip().upper()
        test_only.add(fid)
        if H.TEST_CELL.search(r.get("cell") or "") or H.TEST_CELL.search(r.get("worldspace") or "") \
                or (r.get("loc_name") or "").strip().lower() == "quick test cell":
            continue
        per[fid].append(r)
    return per, test_only - set(per)


def loc_block(places, slug, fixed=True, given=None):
    if places:
        shown = ", ".join(places[:4])
        more = len(places) - 4
        text = "Found " + ("" if shown.startswith("near ") else "at ") + shown + \
               (" and %d more places" % more if more > 0 else "") + "."
    else:                  # no fixed place: text only, no picture slots (as on the Slasher Lore page)
        text = given or "Location not added yet."
        fixed = False
    return {"text": text, "fromFiles": True, "noFixedPlace": not fixed,
            "mapImage": on_server(IMG_DIR + slug + "-map.avif"),
            "locationImage": on_server(IMG_DIR + slug + "-location.avif")}


def build(root, dist):
    Q = Quests(root)
    rt = H.region_tools(root)
    slugs = Counter()

    def slug_for(name, fid):
        s = _slug(name) or fid.lower()
        slugs[s] += 1
        return s if slugs[s] == 1 else s + "-" + fid.lower()

    items = []          # (quest dict or None, region, place, item)

    # ---- holotapes
    holo = H.build(root)
    for g in holo["groups"]:
        for it in g["items"]:
            qn = next((r["name"] for r in it["routes"] if r["row"] == "Quests"), None)
            q = Q.from_name(qn)
            if quest_is_cut(q):
                # cut quest (e.g. the Vault 96 missions): keep the tape on World Lore
                # if it still lies in the world, drop it if only the quest gave it
                if it["location"].get("noFixedPlace"):
                    continue
                q = None
                it = dict(it, routes=[r for r in it["routes"] if r["row"] != "Quests"])
            it = dict(it)
            s = slug_for(it["name"], it["formId"])
            it["location"] = dict(it["location"], mapImage=on_server(IMG_DIR + s + "-map.avif"),
                                  locationImage=on_server(IMG_DIR + s + "-location.avif"))
            it.pop("root", None)
            place = it["location"]["text"][9:-1].split(",")[0] if it["location"]["text"].startswith("Found at ") else ""
            items.append((q, it.pop("region", H.OTHER), place, it))

    # ---- notes
    notes_p = Path(dist) / "collectables_notes.json"
    texts = {}
    lp = newest(root, "ALLYDLG_Export_*_LoreNotes.tsv")
    for r in (_rows(lp) if lp else []):
        texts[(r.get("FormID") or "").strip().upper()] = r
    bplaced, btest = load_placements(root, "BOOK2_Export_*.tsv")
    nd = json.loads(notes_p.read_text(encoding="utf-8")) if notes_p.exists() else {}
    for n in nd.get("items", []) + nd.get("cutContent", []):
        fid = n["formId"].upper()
        cut_marked = bool(n.get("isCut") or plan_sources.is_cut_content(n.get("edid", "")))
        if cut_marked and (not bplaced.get(fid) or JUNK_TERM.match(n.get("edid", "")) or TESTY.search(n.get("edid", ""))):
            continue      # cut and nowhere in the world (or a test / template record)
        if fid in btest:
            continue                       # only ever in the developers' test rooms
        q = Q.from_name(n.get("questName")) if n.get("questName") else None
        places, regs = H.places_regions(bplaced.get(fid, []), rt)
        if quest_is_cut(q):
            if not places:
                continue          # only the cut quest gave it
            q = None              # still lying in the world: World Lore
        region = Counter(regs).most_common(1)[0][0] if regs else (n.get("region") or H.OTHER)
        if region not in H.REGIONS_AZ:
            region = H.OTHER
        loc_txt = (n.get("location") or "").strip()
        if not places and loc_txt and loc_txt.lower() not in ("unknown", "appalachia"):
            places = [loc_txt]
        # the notes checklist adds " — <EditorID bits>" to tell same-named notes apart;
        # here each sits under its own quest / place, so the plain name reads better
        n = dict(n, name=re.sub(r"\s+—\s+.*$", "", n["name"]).strip() or n["name"])
        s = slug_for(n["name"], fid)
        given = ("Given during " + q["name"] + ".") if q and n.get("questGiven") and not places else None
        it = {"formId": fid, "edid": n["edid"], "name": n["name"], "kind": "Note",
              "location": loc_block(places, s, given=given),
              "routes": ([{"row": "Quests", "name": q["name"], "conditions": []}] if q else []) +
                        ([{"row": "World Spawns", "name": ", ".join(places[:3]), "conditions": []}] if places else []),
              "lead": "A note lying in the world." if places else "A note you are given during a quest.",
              "contents": n.get("contents", ""), "image": None}
        ln = texts.get(fid)
        if ln and (ln.get("Text") or "").strip():
            blocks = note_markup.parse(ln["Text"])
            if blocks:
                it["contents"] = note_markup.plain(blocks)
                it["note"] = {"blocks": blocks, "paper": (ln.get("PaperEDID") or "").strip() or None}
        if cut_marked:
            it["cutMarked"] = True
        items.append((q, region, places[0] if places else "", it))

    # ---- terminals
    tp = newest(root, "TERMLORE_Export_*[0-9].tsv")
    trows = defaultdict(list)
    # the TERM exports quote every value CSV-style (quotes doubled), unlike the other TSVs
    with open(tp, encoding="utf-8", errors="replace", newline="") if tp else open(__file__) as fh:
        for r in (csv.DictReader(fh, delimiter="\t") if tp else []):
            trows[r["TERM_FormID"].upper()].append(r)
    tplaced, ttest = load_placements(root, "TERM2_Export_*.tsv")
    name_of = {f: next((unesc(r["Text"]) for r in rs if r["Row"] == "NAME"), "") for f, rs in trows.items()}
    edid_of = {f: rs[0]["TERM_EDID"] for f, rs in trows.items()}
    kids = {f: [r["Target_FormID"].upper() for r in rs if r["Row"] == "ITEM" and r["ItemType"] == "4" and r["Target_FormID"]]
            for f, rs in trows.items()}

    def entries_of(f):
        return [r for r in trows.get(f, []) if r["Row"] == "ITEM" and r["ItemType"] == "8"
                and len(unesc(r["Display"]).strip()) >= MIN_ENTRY
                and not ERROR_ENTRY.match(unesc(r["Display"]).strip())]

    def ok_term(f):
        e = edid_of.get(f, "")
        if not e or JUNK_TERM.match(e):
            return False
        if plan_sources.is_cut_content(e):
            # a cut quest's terminal that still stands in the world stays (Duchess,
            # 15 Nov 2026: the Vault 96 missions are cut, the vault's lore may not be)
            if f not in tplaced or re.search(r"(?i)(^|_)(zzz|cut|del|debug|test)", e):
                return False
            # Nuclear Winter (Babylon) terminals inside Vault 51 stay: you can still get
            # in there (Duchess, 15 Nov 2026). Its outdoor loot terminals don't.
            if re.search(r"(?i)babylon", e):
                return any("vault 51" in (r.get("loc_name") or "").lower() for r in tplaced[f])
            return not TESTY.search(e)
        return True

    claimed = set()

    def walk(root_fid):
        out, seen, queue = [], set(), [(root_fid, "")]
        while queue:
            f, path = queue.pop(0)
            if f in seen or f not in trows or not ok_term(f):
                continue
            seen.add(f)
            for r in entries_of(f):
                out.append((f, path, r))
            for r in trows[f]:
                if r["Row"] == "ITEM" and r["ItemType"] == "4" and r["Target_FormID"]:
                    queue.append((r["Target_FormID"].upper(), (path + " › " if path else "") + unesc(r["Text"]).strip()))
        claimed.update(seen)
        return out

    roots = [f for f in trows if f in tplaced and ok_term(f)]
    roots += [f for f in trows if f not in tplaced and f not in ttest and ok_term(f)
              and not any(f in kids.get(p, []) for p in trows)]
    for f in sorted(roots, key=lambda f: (f not in tplaced, f)):
        if f in claimed and f not in tplaced:
            continue
        ents = walk(f)
        if not ents:
            continue
        conds = []
        for _f, _p, r in ents:
            for c in (r.get("Conditions") or "").split(" && "):
                p = c.split("|")
                if len(p) > 5 and p[5] == "QUST":
                    conds.append(p[4])
        q = Q.for_terminal(edid_of[f], conds)
        if quest_is_cut(q):
            if f not in tplaced:
                continue
            q = None
        places, regs = H.places_regions(tplaced.get(f, []), rt)
        region = Counter(regs).most_common(1)[0][0] if regs else H.OTHER
        nm = clean_name(name_of.get(f)) or "Terminal"
        if places and nm.lower() in ("terminal", "robco terminal", ""):
            nm = nm + " — " + places[0].replace("near ", "")
        s = slug_for(nm, f)
        it = {"formId": f, "edid": edid_of[f], "name": nm, "kind": "Terminal",
              "location": loc_block(places, s, given=("Appears during " + q["name"] + ".") if q else None),
              "routes": ([{"row": "Quests", "name": q["name"], "conditions": []}] if q else []) +
                        ([{"row": "World Spawns", "name": ", ".join(places[:3]), "conditions": []}] if places else []),
              "lead": "A terminal you can read in the world." if places else
                      "A terminal that only appears during a quest or event.",
              "entries": [{"menu": clean_name(p), "title": clean_name(unesc(r["Text"])), "text": unesc(r["Display"]).strip()}
                          for _f, p, r in ents],
              "image": None}
        if plan_sources.is_cut_content(edid_of[f]):
            it["cutMarked"] = True
        items.append((q, region, places[0].replace("near ", "") if places else "", it))

    # ---- pages
    KIND_ORDER = {"Holotape": 0, "Note": 1, "Terminal": 2}
    sortkey = lambda it: (it["name"].lower(), KIND_ORDER.get(it["kind"], 9), it["formId"])
    pages = {}
    for key, (title, sub) in PAGES.items():
        byq = defaultdict(list)
        for q, region, place, it in items:
            if q and page_of(q) == key:
                byq[q["name"]].append(it)
        groups = [{"key": _slug(qn), "label": qn, "items": sorted(v, key=sortkey)}
                  for qn, v in sorted(byq.items(), key=lambda kv: kv[0].lower())]
        pages[key] = {"title": title, "sub": sub, "groups": groups}
    for reg in H.REGIONS_AZ + [H.OTHER]:
        byp = defaultdict(list)
        for q, region, place, it in items:
            if not q and region == reg:
                byp[place or "Place not known yet"].append(it)
        groups = [{"key": _slug(p), "label": p, "items": sorted(v, key=sortkey)}
                  for p, v in sorted(byp.items(), key=lambda kv: (kv[0] == "Place not known yet", kv[0].lower()))]
        nm = "Other / Unknown" if reg == H.OTHER else reg
        pages["world-" + ("other" if reg == H.OTHER else _slug(reg))] = {
            "title": "World Lore - " + nm,
            "sub": "Notes, holotapes and terminals no quest owns, found in %s, grouped by place." % (
                "places the game files can't put in a region" if reg == H.OTHER else reg),
            "groups": groups}
    now = datetime.now(timezone.utc).isoformat()
    out = {}
    for key, pg in pages.items():
        out[key] = {"generatedAt": now, "type": "quest-lore", "page": key, "title": pg["title"], "sub": pg["sub"],
                    "count": sum(len(g["items"]) for g in pg["groups"]),
                    "kinds": dict(Counter(i["kind"] for g in pg["groups"] for i in g["items"])),
                    "groups": pg["groups"], "maps": {}}
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args(argv)
    root = REPO / "tsv" / ("pts" if a.pts else "")
    dist = REPO / "dist" / ("pts" if a.pts else "")
    out = dist / "quest_lore"
    out.mkdir(parents=True, exist_ok=True)
    pages = build(root, dist)
    index = []
    for key, data in pages.items():
        (out / ("%s.json" % key)).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        index.append({"page": key, "title": data["title"], "count": data["count"], "kinds": data["kinds"],
                      "groups": len(data["groups"])})
        print("  %-22s %5d items  %4d groups  %s" % (key, data["count"], len(data["groups"]), data["kinds"]))
    (out / "index.json").write_text(json.dumps({"generatedAt": datetime.now(timezone.utc).isoformat(),
                                                "pages": index}, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Wrote %d pages to %s" % (len(pages), out))


if __name__ == "__main__":
    main()
