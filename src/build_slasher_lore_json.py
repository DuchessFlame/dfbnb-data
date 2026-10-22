#!/usr/bin/env python3
r"""
build_slasher_lore_json.py — /df/collectables/slasher-lore/checklist/

The Slasher's lore as a tickable checklist, grouped by event phase: every note
(BOOK with text), every holotape (NOTE that plays a scene) and every radio
broadcast, with the full text / transcript, where to find it and how to get it.

Everything comes from the game files and is rebuilt every patch:
  notes       BOOK export (EDID prefix SDOW_, has DESC text, not a recipe)
  holotapes   NOTE export (EDID prefix SDOW_) + ALLYDLG _Lore (lines of the
              scene the holotape plays, matched on HolotapeFormID)
  broadcasts  ALLYDLG _Lore rows of SDOW_ radio quests (scenes with no holotape)
  phase       the quest that hands the item out (BOOK/NOTE references, or the
              leveled list's quest), mapped through slasher_guide's phases;
              a broadcast's phase is the story quest whose objective quotes its
              name ('Listen to the "Breaking News" radio broadcast'); Debunker
              reports by a word in the scene EditorID (DEBUNKER_PHASE below)
  how to get  the quest (and its step), or the enemies whose death list holds
              it, with the rng76 rate and drop conditions
  location    a quest objective that names the item and a place
              ('Find the "Phantoms' Orders" in RobCo Research Center'), else
              data/slasher_lore_locations.json (Duchess fills it in)

Pictures: the page looks for guide-images/collectables/Slasher-Lore/
<slug>-map.avif (map marker) and <slug>-location.avif (location photo) — upload
with those names and they appear; data/slasher_lore_locations.json can point
elsewhere ("image", "mapImage", "locationImage" per EditorID).
  notes      <slug>-note.avif — the note on its in-game paper, made by
             tools/make_lore_note_images.py from the game's paper texture and
             the note's own text (the game has no picture per note: it lays the
             text over a plain paper model, BOOK INAM)
  holotapes  the site-wide holotape picture (src/shared_images.py), row thumb only
  radio      by speaker (SPEAKER_IMAGE): debunker-news.avif for Remington
             Goodwin's Debunker News, row thumb only
Note text comes from ALLYDLG _LoreNotes (keeps the game's line breaks, fonts,
colours and alignment as "note.blocks"); the BOOK export is the fallback.

    python src/build_slasher_lore_json.py                 # live  -> dist/
    python src/build_slasher_lore_json.py --pts           # PTS   -> dist/pts/
"""
from __future__ import annotations

import argparse
import collections
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

import tsv_source            # noqa: E402
import note_markup           # noqa: E402
import shared_images         # noqa: E402
import plan_quest_guides     # noqa: E402
import slasher_guide         # noqa: E402

csv.field_size_limit(1 << 30)

PREFIX = "SDOW_"
OUT_NAME = "collectables_slasher_lore.json"
LOCATIONS_JSON = REPO / "data" / "slasher_lore_locations.json"
IMAGE_DIR = "/wp-content/uploads/guide-images/collectables/Slasher-Lore/"
HOLOTAPE_IMAGE = shared_images.HOLOTAPE_IMAGE      # the site-wide holotape picture
# Radio row picture by who speaks in it (voice type EditorID -> file). The
# Debunker's is his own talking-head portrait from interface/vocharacteranim.swf
# (frame label SDOW_NPCM_Debunker), the one the HUD shows when he talks.
# The Reborn Slasher wears the Atom Shop Pint-Sized Slasher Costume + Mask
# (his outfit list SDOW_LL_Outfit_SlasherBoss_Normal), so his picture is that
# costume's Atom Shop art, already on the site (full path = used as is).
SPEAKER_IMAGE = {
    "SDOW_NPCM_Debunker": "debunker-news.avif",
    "SDOW_NPCM_SlasherImpostor": "/wp-content/uploads/guide-images/atom-shop/bundle-images/ATX_Apparel_Outfit_PintsizedSlasher_MP4.avif",
}
DEV_RX = re.compile(r"^(zzz|cut_|del_|post_)", re.I)
RECIPE_KW = "ObjectTypeRecipe"

# Debunker radio report scene EditorID word -> phase key (structure only).
DEBUNKER_PHASE = [
    ("ScavengerHunt", "phase-1"), ("GraveDigging", "phase-2"), ("DailyOps", "phase-2"),
    ("Infestation", "phase-3"), ("Mischief", "phase-4"), ("SpookyScorched", "phase-5"),
    ("HeadHunt", "finale"), ("ImpostorKilled", "finale"),
]
ANY_KEY = "any"
ANY_LABEL = "Any Phase"


def _rows(p):
    with open(p, encoding="utf-8", errors="replace", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)


def _ref(cell):
    bits = (cell or "").strip().split(":")
    if len(bits) < 3:
        return None
    return bits[0].upper(), ":".join(bits[1:-1]), bits[-1].upper()


def _slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")


def _html_text(s):
    """Note DESC (game HTML) -> plain paragraphs."""
    s = s or ""
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</p\s*>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    s = re.sub(r"[ \t]{2,}", "\n\n", s)            # the export flattens line breaks to runs of spaces
    lines = [l.strip() for l in s.split("\n")]
    out, blank = [], False
    for l in lines:
        if not l:
            blank = True
            continue
        if blank and out:
            out.append("")
        out.append(l)
        blank = False
    return "\n".join(out).strip()


def _rate4(r):
    return "{:.4f}%".format((r or 0) * 100)


class Build:
    def __init__(self, tsv_root, dist_root):
        self.tsv = str(tsv_root)
        self.dist = Path(dist_root)
        nw = lambda pat: tsv_source.newest(os.path.join(self.tsv, pat), required=False)
        self.newest = nw
        self.guide = slasher_guide.build(self.tsv, self.dist)
        self.guides = plan_quest_guides.Guides(self.tsv)
        # phase key per quest EditorID
        self.phase_of_quest = {}
        for k, e in slasher_guide.QUEST_FOR_PHASE.items():
            self.phase_of_quest[e] = k
        for k, lst in slasher_guide.EXTRA_QUESTS.items():
            for e in lst:
                self.phase_of_quest[e] = k
        self.phases = [p for p in self.guide.get("phases", [])]
        self.quest_name = {}
        p = nw("QUEST_Export_*.tsv")
        if p:
            for r in _rows(p):
                self.quest_name[(r.get("EDID") or "").strip()] = plan_quest_guides.clean_name(r.get("FULL - Name"))
        self.lvli_parents = collections.defaultdict(list)
        p = nw("LVLI_Export_*_LVLI_Refs.tsv")
        if p:
            with open(p, encoding="utf-8", errors="replace", newline="") as f:
                rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
                next(rd, None)
                for row in rd:
                    if row:
                        self.lvli_parents[row[0].strip().upper()] = [x for x in (_ref(c) for c in row[3:]) if x]
        self.npc_by_inam = collections.defaultdict(list)
        p = nw("NPC_Export_*.tsv")
        if p and not re.search(r"_(PRPS|Refs)\.tsv$", p):
            for r in _rows(p):
                inam = (r.get("INAM_FormID") or "").strip().upper()
                if inam:
                    self.npc_by_inam[inam].append((r.get("EDID") or "", (r.get("FULL") or "").strip()))
        self.overrides = {}
        if LOCATIONS_JSON.exists():
            try:
                self.overrides = json.load(open(LOCATIONS_JSON, encoding="utf-8")).get("items", {})
            except Exception:                                  # noqa: BLE001
                self.overrides = {}
        # note text exactly as the game lays it out (line breaks, fonts)
        self.note_text = {}
        p = nw("ALLYDLG_Export_*_LoreNotes.tsv")
        if p:
            for r in _rows(p):
                fid = (r.get("FormID") or "").strip().upper()
                if fid:
                    self.note_text[fid] = r
        self._rng = None
        self._conds = None

    # ---------------------------------------------------------------- engines
    def rng(self):
        if self._rng is None:
            from rng76 import Rng76Data
            self._rng = Rng76Data.from_tsv_root(self.tsv).resolver
        return self._rng

    def conds(self):
        if self._conds is None:
            import plan_conditions
            self._conds = plan_conditions.ConditionIndex(
                self.tsv, lambda pat, root=None: tsv_source.newest(
                    os.path.join(root or self.tsv, pat), required=False))
        return self._conds

    # ---------------------------------------------------------------- phases
    def phase_label(self, key):
        for p in self.phases:
            if p["key"] == key:
                q = p.get("quest")
                cfg_title = {"phase-4": "Mischief Night", "phase-5": "Spooky Scorched"}.get(key)
                return "{}: {}".format(p["label"], q["name"] if q else (cfg_title or p.get("gameText", "")))
        return ANY_LABEL

    def objectives_by_phase(self):
        out = {}
        for p in self.phases:
            q = p.get("quest")
            if not q:
                continue
            fid = q["formId"]
            out[p["key"]] = [plan_quest_guides.clean_step(t) for _, t in sorted(self.guides.quests[fid]["obj"])]
            for sq in p.get("sideQuests") or []:
                out[p["key"]] += [plan_quest_guides.clean_step(t) for _, t in sorted(self.guides.quests[sq["formId"]]["obj"])]
        return out

    # ---------------------------------------------------------------- sources
    def routes_for(self, fid, refs):
        """[(phaseKey|None, route dict)] from an item's ReferencedBy cells."""
        out = []
        seen = set()

        def from_list(lid, path, depth=0):
            if depth > 6 or lid in seen:
                return
            seen.add(lid)
            if lid in self.npc_by_inam:
                names = [n for _, n in self.npc_by_inam[lid] if n]
                fam = collections.Counter(re.sub(r"^(Pint-Sized Phantom)\b.*", r"\1s", n) for n in names).most_common(1)
                who = fam[0][0] if fam else "enemies"
                rate = None
                try:
                    rate = self.rng().appearance_prob(lid, fid)
                except Exception:                              # noqa: BLE001
                    rate = None
                cl = []
                try:
                    cl, _ = self.conds().for_lists([lid], fid)
                except Exception:                              # noqa: BLE001
                    cl = []
                out.append((None, {"row": "Enemies", "name": who, "rate": rate,
                                   "rateDisplay": _rate4(rate) if rate is not None else None,
                                   "conditions": cl}))
            for pf, ped, psig in self.lvli_parents.get(lid, []):
                if DEV_RX.match(ped):
                    continue
                if psig == "LVLI":
                    from_list(pf, path + [pf], depth + 1)
                elif psig == "QUST":
                    add_quest(ped)

        def add_quest(qedid):
            if DEV_RX.match(qedid) or qedid in seen:
                return
            seen.add(qedid)
            key = self.phase_of_quest.get(qedid)
            name = self.quest_name.get(qedid) or qedid
            out.append((key, {"row": "Quests", "name": name, "conditions": []}))

        for fidr, ed, sig in refs:
            if DEV_RX.match(ed):
                continue
            if sig == "QUST":
                add_quest(ed)
            elif sig == "LVLI":
                from_list(fidr, [fidr])
        return out

    # ---------------------------------------------------------------- location
    def location_from_objectives(self, name, objectives):
        # Only an objective that quotes the item's FULL name counts: a shared
        # prefix ("Phantoms' Orders") would hand every page the same place.
        hits = []
        for key, objs in objectives.items():
            for o in objs:
                if name and '"{}"'.format(name).lower() in o.lower():
                    m = re.search(r"\b(?:in|at)\s+(.+)$", o)
                    if m and '"' not in m.group(1):
                        hits.append(m.group(1).strip())
        hits = list(dict.fromkeys(hits))
        if not hits:
            return None
        return hits[0] if len(hits) == 1 else "One of: " + " or ".join(hits)

    def location_block(self, edid, name, objectives):
        ov = self.overrides.get(edid) or {}
        text = ov.get("location") or self.location_from_objectives(name, objectives)
        slug = _slug(name)
        return {
            "text": text,
            "fromFiles": bool(text and not ov.get("location")),
            "mapImage": ov.get("mapImage") or IMAGE_DIR + slug + "-map.avif",
            "locationImage": ov.get("locationImage") or IMAGE_DIR + slug + "-location.avif",
        }

    # ---------------------------------------------------------------- build
    def build(self):
        objectives = self.objectives_by_phase()
        items = []
        # notes ---------------------------------------------------------
        p = self.newest("BOOK_Export_*.tsv")
        for r in (_rows(p) if p else []):
            edid = (r.get("EDID") or "").strip()
            if not edid.startswith(PREFIX) or DEV_RX.match(edid):
                continue
            kws = " ".join(v for k, v in r.items() if k and k.startswith("KW") and v)
            text = _html_text(r.get("DESC"))
            if RECIPE_KW in kws or not text:
                continue
            fid = (r.get("FormID") or "").strip().upper()
            refs = [x for x in (_ref(v) for k, v in r.items() if k and re.match(r"Ref\d+$", k)) if x]
            routes = self.routes_for(fid, refs)
            if not routes:
                continue                                  # nothing gives it out
            it = self._item(fid, edid, r.get("FULL"), "Note", routes, objectives, contents=text)
            ln = self.note_text.get(fid)
            if ln and (ln.get("Text") or "").strip():
                blocks = note_markup.parse(ln["Text"])
                if blocks:
                    it["contents"] = note_markup.plain(blocks)
                    it["note"] = {"blocks": blocks, "paper": (ln.get("PaperEDID") or "").strip() or None}
            it["image"] = (self.overrides.get(edid) or {}).get("image") or IMAGE_DIR + _slug(it["name"]) + "-note.avif"
            items.append(it)
        # holotapes -----------------------------------------------------
        lore = collections.defaultdict(list)
        radio = collections.OrderedDict()
        radio_img = {}
        lp = self.newest("ALLYDLG_Export_*_Lore.tsv")
        for r in (_rows(lp) if lp else []):
            if not (r.get("QuestEDID") or "").startswith(PREFIX):
                continue
            line = {"speaker": (r.get("SpeakerName") or "").strip(), "text": (r.get("Text") or "").strip()}
            if r.get("HolotapeFormID"):
                lore[r["HolotapeFormID"].upper()].append((int(r.get("LineOrder") or 0), line))
            else:
                k = (r.get("QuestEDID"), r.get("QuestName"), r.get("SceneEDID"), r.get("SceneFormID"))
                radio.setdefault(k, []).append((int(r.get("LineOrder") or 0), line))
                sp = _ref(r.get("Speaker"))
                if sp and sp[1] in SPEAKER_IMAGE:
                    f = SPEAKER_IMAGE[sp[1]]
                    radio_img.setdefault(k, f if f.startswith("/") else IMAGE_DIR + f)
        p = self.newest("NOTE_Export_*.tsv")
        for r in (_rows(p) if p else []):
            edid = (r.get("NOTE_EDID") or "").strip()
            fid = (r.get("NOTE_FormID") or "").strip().upper()
            if not edid.startswith(PREFIX) or DEV_RX.match(edid) or fid not in lore:
                continue
            refs = [x for x in (_ref(v) for k, v in r.items() if k and re.match(r"Ref_\d+$", k)) if x]
            routes = self.routes_for(fid, refs)
            if not routes:
                continue
            tr = [l for _, l in sorted(lore[fid], key=lambda x: x[0])]
            it = self._item(fid, edid, r.get("FULL"), "Holotape", routes, objectives, transcript=tr)
            it["image"] = (self.overrides.get(edid) or {}).get("image") or HOLOTAPE_IMAGE
            items.append(it)
        # radio broadcasts ----------------------------------------------
        for (qed, qname, sed, sfid), lines in radio.items():
            qname0 = qname
            tr = [l for _, l in sorted(lines, key=lambda x: x[0])]
            if not tr:
                continue
            qname = plan_quest_guides.clean_name(qname)
            key = None
            for k, objs in objectives.items():
                if qname and any('"{}"'.format(qname).lower() in o.lower() for o in objs):
                    key = k
            if key is None:
                key = next((k for w, k in DEBUNKER_PHASE if w.lower() in (sed or "").lower()), ANY_KEY)
            station = qname or "Radio"
            name = qname if (qname and len([1 for kk in radio if kk[0] == qed]) == 1) else \
                "{}: {}".format(station, self._report_name(sed))
            it = self._item((sfid or "").upper(), sed, name, "Radio Broadcast",
                            [(key, {"row": "Radio", "name": station, "conditions": []})], objectives, transcript=tr)
            img = (self.overrides.get(sed) or {}).get("image") or radio_img.get((qed, qname0, sed, sfid))
            if img:
                it["image"] = img
            items.append(it)

        groups = []
        order = [p["key"] for p in self.phases] + [ANY_KEY]
        for k in order:
            its = [i for i in items if i["group"] == k]
            if its:
                kind_order = {"Radio Broadcast": 0, "Holotape": 1, "Note": 2}
                its.sort(key=lambda i: (kind_order.get(i["kind"], 9), i["name"]))
                groups.append({"key": k, "label": self.phase_label(k) if k != ANY_KEY else ANY_LABEL,
                               "items": its})
        return {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "type": "slasher-lore",
            "count": sum(len(g["items"]) for g in groups),
            "groups": groups,
        }

    @staticmethod
    def _report_name(sed):
        s = re.sub(r"^.*?Radio_\d+_", "", sed or "")
        s = s.replace("SpecialReport_", "Special Report: ")
        s = s.replace("GenericReports_ImpostorKilled", "General Reports (after the impostor's death)")
        s = s.replace("GenericReports", "General Reports")
        s = s.replace("Intro_QuestCompleted", "Return of The Debunker")
        s = re.sub(r"_+", " ", s)
        s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
        return re.sub(r"\s+", " ", s).strip()

    def _item(self, fid, edid, name, kind, routes, objectives, contents=None, transcript=None):
        name = (name or edid).strip()
        keys = [k for k, _ in routes if k]
        group = keys[0] if keys else ANY_KEY
        it = {
            "formId": fid, "edid": edid, "name": name, "kind": kind, "group": group,
            "location": self.location_block(edid, name, objectives),
            "routes": [r for _, r in routes],
        }
        if contents is not None:
            it["contents"] = contents
        if transcript is not None:
            it["transcript"] = transcript
        return it


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--data-dir")
    ap.add_argument("--out")
    a = ap.parse_args(argv)
    tsv = a.data_dir or str(REPO / "tsv" / ("pts" if a.pts else ""))
    dist = REPO / "dist" / ("pts" if a.pts else "")
    data = Build(tsv, dist).build()
    out = Path(a.out) if a.out else dist / OUT_NAME
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("[slasher-lore] wrote {}  ({} items)".format(out, data["count"]))
    for g in data["groups"]:
        print("   {}: {}".format(g["label"], ", ".join("{} ({})".format(i["name"], i["kind"]) for i in g["items"])))


if __name__ == "__main__":
    main()
