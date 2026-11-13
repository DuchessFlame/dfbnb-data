#!/usr/bin/env python3
r"""
build_quest_lore_json.py - lore checklists for the collectable quests, built
like The Slasher Lore checklist (same JSON shape, same renderer:
df-bnb-collectables.js renderSlasherLore).

  /df/collectables/brain-in-a-jar/lore-checklist/
      dist/collectables_brain_in_a_jar_lore.json   (PTS-only content today)
      the 15 Scan holotapes Regina Marcs hands over (A Matter of Matter) + notes
  /df/collectables/abraxodyne-intel-briefcases/lore-checklist/
      dist/collectables_abraxodyne_lore.json
      the 15 Abraxodyne Tapes Bodhi hands over (Dirty Laundry) + notes

Everything comes from the game files and is rebuilt every patch:
  holotapes   NOTE export (EditorID prefix) + ALLYDLG _Lore lines of the scene
              the holotape plays (HolotapeFormID); sorted by the number in the name
  notes       BOOK export (listed EditorIDs) + ALLYDLG _LoreNotes text/paper
  hand-ins    the quest's collectable script (QUST2 _ScriptProps):
              CollectablesNeededPerBatch, in order -> tape N = batch N
  places      BOOK _Locations (placed notes)
Words the files can't give (who hands the tapes over, where) sit in CONFIGS.
Pictures are only linked when the file is on data/server_listing.tsv.

    python src/build_quest_lore_json.py            # live -> dist/
    python src/build_quest_lore_json.py --pts      # PTS  -> dist/pts/
"""
from __future__ import annotations

import argparse
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
from build_slasher_lore_json import _rows, _html_text, _slug, on_server   # noqa: E402

csv.field_size_limit(1 << 30)

IMG = "/wp-content/uploads/guide-images/collectables/"

CONFIGS = {
    "brain-in-a-jar": {
        "out": "collectables_brain_in_a_jar_lore.json",
        "type": "brain-lore",
        "quest": "RTSV_SQ01_Collectible",
        "tapePrefix": "RTSV_SQ01_Collectible_Holotape_",
        "tapeGroup": ("scans", "Scan Holotapes",
                      "Regina Marcs decodes one from the brains every time you hand in a batch."),
        "notes": ["RTSV_Note_IggyFinalWords"],
        "noteGroup": ("notes", "Notes", "Paper notes from the A Matter of Matter quest."),
        "giver": "Regina Marcs",
        "givePlace": "the Brown House at Rapidan Camp",
        "collectable": ("brain", "brains"),
        "imageDir": IMG + "Brain-in-a-Jar-Locations/jar-lore/",
        # Notes the files place but can't explain on their own
        "noteText": {
            "RTSV_Note_IggyFinalWords": {
                "location": "In the bunker under the Brown House, during the finale of A Matter of Matter.",
                "lead": "A note you find during the finale of A Matter of Matter.",
            },
        },
        "lastTapeNote": "This is the last one: Marcs gives it to you when she asks for your help with the bunker under the Brown House.",
    },
    "abraxodyne": {
        "out": "collectables_abraxodyne_lore.json",
        "type": "abraxodyne-lore",
        "quest": "Burn_SQ04_DirtyLaundry",
        "tapePrefix": "Burn_SQ04_DirtyLaundry_Holotape",
        "tapeGroup": ("tapes", "Abraxodyne Tapes",
                      "Bodhi gives you one for every batch of Abraxodyne Intel Briefcases you bring him."),
        "notes": ["Burn_SQ04_ScrambledIntel", "Burn_Note_AbraxodyneIntercept"],
        "noteGroup": ("notes", "Notes", "Paper notes from the Dirty Laundry quest."),
        "giver": "Bodhi",
        "givePlace": "Abraxodyne Chemical's Legal Office in Athens",
        "collectable": ("Abraxodyne Intel Briefcase", "Abraxodyne Intel Briefcases"),
        "imageDir": IMG + "Abraxodyne-Intel-Briefcase-Locations/briefcase-lore/",
        "noteText": {
            "Burn_SQ04_ScrambledIntel": {
                "location": "Inside each Abraxodyne Cache. Every cache you open gives one.",
                "lead": "A note from the Abraxodyne Caches.",
                "route": {"row": "Containers", "name": "Abraxodyne Cache (one per cache)"},
                "obtainLink": {"label": "Where the caches are:", "text": "Abraxodyne Intel Briefcase Guide",
                               "url": "/df/collectables/abraxodyne-intel-briefcases/guide/"},
            },
        },
    },
}


class Build:
    def __init__(self, tsv_root):
        self.tsv = str(tsv_root)

    def newest(self, pat):
        return tsv_source.newest(os.path.join(self.tsv, pat), required=False)

    def batches(self, quest):
        p = self.newest("QUST2_Export_*_ScriptProps.tsv")
        out = []
        for r in (_rows(p) if p else []):
            if (r.get("QuestEDID") or "") == quest and (r.get("Property") or "") == "CollectablesNeededPerBatch":
                try:
                    out.append(int(r.get("Value") or 0))
                except ValueError:
                    pass
        return out

    def quest_name(self, quest):
        # QUST2 _Quests has no FULL column filled; every other QUST2 file
        # carries QuestName next to QuestEDID.
        for pat in ("QUST2_Export_*_ScriptProps.tsv", "QUST2_Export_*_RewardQuests.tsv"):
            p = self.newest(pat)
            for r in (_rows(p) if p else []):
                if (r.get("QuestEDID") or "") == quest and (r.get("QuestName") or "").strip():
                    return r["QuestName"].strip()
        return quest

    def build(self, cfg):
        qname = self.quest_name(cfg["quest"])
        per = self.batches(cfg["quest"])
        one, many = cfg["collectable"]
        # transcripts
        lore = {}
        lp = self.newest("ALLYDLG_Export_*_Lore.tsv")
        for r in (_rows(lp) if lp else []):
            h = (r.get("HolotapeFormID") or "").upper()
            if h:
                lore.setdefault(h, []).append((int(r.get("LineOrder") or 0),
                                               {"speaker": (r.get("SpeakerName") or "").strip(),
                                                "text": (r.get("Text") or "").strip()}))
        tapes = []
        p = self.newest("NOTE_Export_*.tsv")
        for r in (_rows(p) if p else []):
            edid = (r.get("NOTE_EDID") or "").strip()
            fid = (r.get("NOTE_FormID") or "").strip().upper()
            if not edid.startswith(cfg["tapePrefix"]) or fid not in lore:
                continue
            m = re.search(r"(\d+)\s*$", edid)
            n = int(m.group(1)) if m else 0
            name = (r.get("FULL") or edid).strip()
            tr = [l for _, l in sorted(lore[fid], key=lambda x: x[0])]
            if n and n <= len(per):
                k = per[n - 1]
                total = sum(per[:n])
                loc = ("{giver} gives you this holotape when you hand in batch {n} of {nb}: "
                       "{k} {w} ({t} in total). Hand them in at {place}.").format(
                    giver=cfg["giver"], n=n, nb=len(per), k=k, w=one if k == 1 else many, t=total,
                    place=cfg["givePlace"])
                if n == len(per) and cfg.get("lastTapeNote"):
                    loc += " " + cfg["lastTapeNote"]
                route = "{} - hand-in {} ({} {} in total)".format(qname, n, total, one if total == 1 else many)
            else:
                loc = "{} gives you this holotape during {}.".format(cfg["giver"], qname)
                route = qname
            tapes.append((n, {
                "formId": fid, "edid": edid, "name": name, "kind": "Holotape", "group": cfg["tapeGroup"][0],
                "location": {"text": loc, "fromFiles": True, "noFixedPlace": True,
                             "mapImage": None, "locationImage": None},
                "routes": [{"row": "Quests", "name": route, "conditions": []}],
                "lead": "A holotape {} hands you during {}.".format(cfg["giver"], qname),
                "transcript": tr,
                "image": shared_images.HOLOTAPE_IMAGE,
                "batch": n,
            }))
        tapes = [t for _, t in sorted(tapes, key=lambda x: x[0])]

        # notes
        texts = {}
        p = self.newest("ALLYDLG_Export_*_LoreNotes.tsv")
        for r in (_rows(p) if p else []):
            texts[(r.get("FormID") or "").strip().upper()] = r
        places = {}
        p = self.newest("BOOK_Export_*_Locations.tsv")
        for r in (_rows(p) if p else []):
            if (r.get("LocationName") or "").strip():
                places[(r.get("BOOK_EDID") or "").strip()] = r["LocationName"].strip()
        notes = []
        p = None
        for cand in sorted(Path(self.tsv).glob("BOOK_Export_*.tsv"), key=lambda x: x.stat().st_mtime, reverse=True):
            if not cand.name.endswith("_Locations.tsv"):
                p = str(cand)
                break
        for r in (_rows(p) if p else []):
            edid = (r.get("EDID") or "").strip()
            if edid not in cfg["notes"]:
                continue
            fid = (r.get("FormID") or "").strip().upper()
            name = (r.get("FULL") or edid).strip()
            hand = cfg["noteText"].get(edid, {})
            where = hand.get("location") or (
                "Lying in {}.".format(places[edid]) if edid in places else "Not confirmed yet.")
            it = {
                "formId": fid, "edid": edid, "name": name, "kind": "Note", "group": cfg["noteGroup"][0],
                "location": {"text": where, "fromFiles": not hand.get("location"),
                             "noFixedPlace": True, "mapImage": None, "locationImage": None},
                "routes": [dict(hand["route"], conditions=[]) if hand.get("route") else
                       {"row": "Quests" if hand.get("lead") else "World Spawns",
                        "name": qname if hand.get("lead") else (places.get(edid) or "In the world"),
                        "conditions": []}],
                "lead": hand.get("lead") or "A note lying in the world.",
                "contents": _html_text(r.get("DESC")),
            }
            if hand.get("obtainLink"):
                it["obtainLink"] = hand["obtainLink"]
            ln = texts.get(fid)
            if ln and (ln.get("Text") or "").strip():
                blocks = note_markup.parse(ln["Text"])
                if blocks:
                    it["contents"] = note_markup.plain(blocks)
                    it["note"] = {"blocks": blocks, "paper": (ln.get("PaperEDID") or "").strip() or None}
            it["image"] = on_server(cfg["imageDir"] + _slug(name) + "-note.avif")
            notes.append(it)
        notes.sort(key=lambda i: i["name"])

        groups = []
        for (key, label, sub), its in ((cfg["tapeGroup"], tapes), (cfg["noteGroup"], notes)):
            if its:
                groups.append({"key": key, "label": label, "sub": sub, "items": its})
        return {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "type": cfg["type"],
            "quest": qname,
            "batches": per,
            "count": sum(len(g["items"]) for g in groups),
            "groups": groups,
            "maps": {},
        }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--data-dir")
    ap.add_argument("--only", choices=sorted(CONFIGS))
    a = ap.parse_args(argv)
    tsv = a.data_dir or str(REPO / "tsv" / ("pts" if a.pts else ""))
    dist = REPO / "dist" / ("pts" if a.pts else "")
    b = Build(tsv)
    for key, cfg in CONFIGS.items():
        if a.only and key != a.only:
            continue
        data = b.build(cfg)
        out = dist / cfg["out"]
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print("[quest-lore] {} -> {} ({} items, batches {})".format(key, out, data["count"], data["batches"]))
        for g in data["groups"]:
            print("   {}: {}".format(g["label"], ", ".join(i["name"] for i in g["items"])))


if __name__ == "__main__":
    main()
