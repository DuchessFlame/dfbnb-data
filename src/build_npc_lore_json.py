#!/usr/bin/env python3
r"""
build_npc_lore_json.py - NPC Lore & Dialogue pages (Duchess, 22 Nov 2026).

  /df/quests/character-lore/marshal-canton-lore-and-dialogue/   (PTS only for now)
  /df/quests/character-lore/the-ghoul-lore-and-dialogue/         (Billie = his codename; her lines count as his)
  /df/minerva/minerva-lore-and-dialogue/                  (+ Tommy Ten-Toes)
  /df/quests/character-lore/aries-lore-and-dialogue/

Prose (who they are, where to find them) lives in data/npc_lore_pages.json.
Everything else is read from the game-file exports on every build:
  NPCDLG_Export_*_Lines.tsv / _Scenes.tsv   every line, menu, condition, speaker
                                            (tools/npc_dialogue_export.py)
  NPC_Export_*.tsv                          voice type -> NPC name
  GLOB_Export_*.tsv                         {posterPrice} (BountyHunty_CapsPriceHeadHunt)
  LCTN_Export_*_LCSR.tsv / _LCTN.tsv        {spawns}: every placed copy of the NPC, by region
  REFR_Placements_*.tsv                     nearest map marker for spots with no named location
  dist[/pts]/quest_lore/*.json, collectables_notes.json, collectables_lore_holotapes.json,
  collectables_brain_in_a_jar_lore.json     lore documents (notes, holotapes, terminals)

    python src/build_npc_lore_json.py          # live -> dist/npc_lore/<slug>.json
    python src/build_npc_lore_json.py --pts    # PTS  -> dist/pts/npc_lore/<slug>.json
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import tsv_source  # noqa: E402

csv.field_size_limit(10 ** 9)
CONFIG = os.path.join(REPO, "data", "npc_lore_pages.json")


def rows(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


# ---------------------------------------------------------------- conditions --
OPS = {"100": "==", "010": ">", "110": ">=", "001": "<", "101": "<=", "000": "!="}


def cond_parts(raw):
    """Exporter condition cells -> [(run, func, param, op, value, or)]."""
    out = []
    for part in [p for p in (raw or "").split(" ; ") if p.strip()]:
        f = part.split("|")
        if len(f) < 6:
            continue
        t = f[0]
        out.append((f[9] if len(f) > 9 else "", f[2], f[5], OPS.get(t[:3], "?"), f[1], len(t) > 3 and t[3] == "1"))
    return out


def av_words(edid, strip):
    w = edid
    for p in strip:
        w = w.replace(p, "")
    w = re.sub(r"_AV$", "", w)
    w = re.sub(r"([a-z])([A-Z])", r"\1 \2", w).replace("_", " ").strip().lower()
    return {"reached bh": "you've heard how bounty hunting works", "reached bounties": "you've asked for a bounty",
            "has met": "you've met", "intro complete": "the introduction is done", "breakdown": "the breakdown scene",
            "probed background": "you've questioned who he really is", "reached hunting": "you've heard the bounty pitch",
            "reached questions": "you've asked your first questions",
            "has heard about head hunts": "you've heard about head hunts"}.get(w, w)


def quoted(s):
    m = re.search(r'"([^"]+)"', s or "")
    return m.group(1) if m else ""


def cond_text(c, strip):
    run, fn, a, op, val, _or = c
    try:
        v = float(val)
    except ValueError:
        v = 0.0
    vi = int(v) if v == int(v) else v
    if "Bounty_Pointer" in a or "BountyPointer" in a:
        yes = (op in ("==", ">=") and v >= 1) or (op == "!=" and v == 0)
        return "Once he has already pointed you to Burning Springs" if yes else "Before he has pointed you to Burning Springs"
    if fn in ("GetIsAliasRef", "GetIsVoiceType", "GetIsID"):
        return None
    if fn == "GetGlobalValue" and "TrueGlobal" in a:
        return "This reply is switched off"
    if fn == "GetCurrency" and "Cap" in a:
        return f"Needs {vi:,} caps"
    if fn == "GetItemCount":
        nm = quoted(a) or a
        if op == "<" and vi == 1:
            return f"Only if you don't have a {nm}"
        if op == ">=" and vi == 1:
            return f"Only if you already have a {nm}"
    if fn == "GetNumTimesCompletedQuest":
        q = quoted(a) or a.split(" ")[0]
        if op in (">=", "==") and vi >= 1:
            return f"Only after you have completed {q}"
        if op == "<" and vi == 1:
            return f"Only if you have never completed {q}"
    if fn == "GetValue" and "Intelligence" in a:
        return f"Intelligence {vi}+"
    if fn == "GetValue" and "_AV" in a:
        w = av_words(a.split(" ")[0], strip)
        if (op == "==" and vi == 1):
            return f"Only after: {w}"
        if (op == "!=" and vi == 1) or (op == "==" and vi == 0):
            return f"Only until: {w}"
        return None          # scene-routing values (2, 3 ...) - plumbing, not a player condition
    if fn == "GetIsPlayerGhoul":
        return "If you are a ghoul" if (op == "==" and vi == 1) else "If you are not a ghoul"
    if fn == "GetInCurrentLocation":
        return f"Only at {quoted(a) or a}"
    if fn == "IsPowerArmorOwner":
        return "If you own power armour"
    if fn == "HasKeyword" and "GruntHunt_Active" in a:
        return "While you have a Grunt Hunt running"
    if fn == "GetStageDoneCurrentInstance":
        return "Before the lead has been decoded"
    if fn == "GetGlobalValue":
        g = a.split(" ")[0]
        return f"{g} {op} {vi}"
    clean = re.sub(r" \[[A-Z_]+:[0-9A-F]+\]", "", a)
    return f"{fn}({clean}) {op} {vi}"


def conds_for(raw, strip):
    out = []
    for c in cond_parts(raw):
        t = cond_text(c, strip)
        if t:
            out.append(t)
    return out


# ------------------------------------------------------------------ dialogue --
def scene_label(edid, strip):
    w = edid
    for p in strip:
        w = w.replace(p, "")
    w = re.sub(r"([a-z])([A-Z])", r"\1 \2", w).replace("_", " ").strip()
    return w or edid


class Dialogue:
    def __init__(self, lines_path, scenes_path, npc_path):
        self.info = {}
        self.topic = defaultdict(list)
        for r in rows(lines_path):
            i = self.info.get(r["InfoFormID"])
            if not i:
                i = self.info[r["InfoFormID"]] = dict(fid=r["InfoFormID"], topic=r["TopicFormID"], quest=r["QuestEDID"],
                                                      sub=r["TopicSubtype"], prompt=r["PlayerPrompt"] or r["TopicPrompt"],
                                                      own_prompt=r["PlayerPrompt"], voices=[v for v in r["VoiceTypes"].split(",") if v],
                                                      cond=r["Conditions"], starts=r["StartsScenes"], scripts=r["Scripts"], resp=[])
                self.topic[r["TopicFormID"]].append(i)
            if r["ResponseText"]:
                i["resp"].append(r["ResponseText"])
        self.scenes = defaultdict(list)   # quest -> [scene dict in file order]
        sc = {}
        for r in rows(scenes_path):
            s = sc.get(r["SceneFormID"])
            if not s:
                s = sc[r["SceneFormID"]] = dict(fid=r["SceneFormID"], edid=r["SceneEDID"], quest=r["QuestEDID"], actions={})
                self.scenes[r["QuestEDID"]].append(s)
            a = s["actions"].setdefault(int(r["ActionIndex"]), dict(type=int(r["ActionType"]), topics=[]))
            if r["TopicFormID"]:
                a["topics"].append(r["TopicFormID"])
        self.vt_names = defaultdict(set)
        for r in rows(npc_path):
            v = (r.get("VTCK_EDID") or "").lower()
            if v and r.get("FULL"):
                self.vt_names[v].add(r["FULL"])

    def speaker(self, i, names):
        for v in i["voices"]:
            if v in names:
                return names[v]
        for v in i["voices"]:
            n = self.vt_names.get(v)
            if n and len(n) == 1:
                return next(iter(n))
        return ""

    def build(self, quests, voice, strip, names, only_speaker=(), all_scenes=False):
        voices = set(voice) if isinstance(voice, (list, tuple)) else {voice}
        npc_name = next((names[v] for v in voices if v in names), "")
        idat = {}
        for q, _t in quests:
            for i in self.info.values():
                if i["quest"] == q and i["sub"] == "IDAT" and i["own_prompt"] and i["resp"]:
                    idat.setdefault(i["own_prompt"], i)
        used, groups = set(), []
        for q, qtitle in quests:
            label = {s["fid"]: scene_label(s["edid"], strip) for s in self.scenes.get(q, [])}
            out_scenes = []
            for s in self.scenes.get(q, []):
                touches, acts = False, []
                for ai in sorted(s["actions"]):
                    a = s["actions"][ai]
                    if a["type"] not in (0, 3, 5):
                        continue
                    items = []
                    for t in a["topics"]:
                        for i in self.topic.get(t, []):
                            used.add(i["fid"])
                            if voices & set(i["voices"]):
                                touches = True
                            resp, who = i["resp"], self.speaker(i, names)
                            if a["type"] == 3 and not resp and i["prompt"] in idat:
                                resp, who = idat[i["prompt"]]["resp"], self.speaker(idat[i["prompt"]], names)
                                used.add(idat[i["prompt"]]["fid"])
                            if a["type"] != 3 and not resp:
                                continue
                            if q in only_speaker and a["type"] != 3 and who != npc_name:
                                continue
                            nxt = []
                            for st in [x for x in i["starts"].split(" ; ") if x]:
                                f = st.split(":")[0]
                                if f == s["fid"]:
                                    nxt.append("back")
                                elif f in label:
                                    nxt.append(label[f])
                            items.append(dict(prompt=i["prompt"] if a["type"] == 3 else "",
                                              who=who, lines=resp, conds=conds_for(i["cond"], strip), next=nxt,
                                              givesPoster="GiveHeadHuntPoster" in (i["scripts"] or ""), fid=i["fid"]))
                    if items:
                        acts.append(dict(kind="menu" if a["type"] == 3 else "line", items=items))
                if acts and (touches or all_scenes):
                    out_scenes.append(dict(label=label[s["fid"]], edid=s["edid"], fid=s["fid"], actions=acts))
            if out_scenes:
                groups.append(dict(quest=q, title=qtitle, scenes=out_scenes))
        subname = {"HELO": "Hello (when you walk past)", "GREE": "Greeting (when you start talking)", "IDLE": "Idle chatter",
                   "IDAT": "Other lines", "GBYE": "Goodbye", "HIT_": "When hit", "CUST": "Other lines"}
        loose = defaultdict(list)
        for i in self.info.values():
            if i["fid"] in used or not (voices & set(i["voices"])) or not i["resp"] or i["own_prompt"]:
                continue
            if i["quest"] not in [q for q, _t in quests]:
                continue
            loose[(subname.get(i["sub"], "Other lines"), i["quest"])].append(
                dict(who=self.speaker(i, names), lines=i["resp"], conds=conds_for(i["cond"], strip)))
        qt = dict(quests)
        loose_out = [dict(label=k[0], quest=qt.get(k[1], k[1]), items=v) for k, v in loose.items()]
        order = ["Greeting (when you start talking)", "Hello (when you walk past)", "Idle chatter", "When hit", "Goodbye", "Other lines"]
        loose_out.sort(key=lambda x: (order.index(x["label"]) if x["label"] in order else 9, x["quest"]))
        return groups, loose_out


# ---------------------------------------------------------------------- lore --
def lore_items(dist):
    items = []

    def add(x, text, src):
        loc = x.get("location")
        if isinstance(loc, dict):
            loc = loc.get("text", "")
        q = x.get("questName") or ""
        if not q and isinstance(x.get("routes"), list):
            q = ", ".join(r.get("name", "") for r in x["routes"] if isinstance(r, dict))
        items.append(dict(name=x.get("name") or "", edid=x.get("edid") or "", kind=x.get("kind") or ("Note" if src == "notes" else ""),
                          where=loc or "", quest=q, text=text))

    for f in sorted(glob.glob(os.path.join(dist, "quest_lore", "*.json"))) + [os.path.join(dist, "collectables_lore_holotapes.json"),
                                                                             os.path.join(dist, "collectables_brain_in_a_jar_lore.json")]:
        if not os.path.isfile(f):
            continue
        j = json.load(open(f, encoding="utf-8"))
        for g in j.get("groups", []):
            for x in g.get("items", []):
                t = ""
                if isinstance(x.get("transcript"), list):
                    t = "\n".join(f"{l.get('speaker')}: {l.get('text')}" for l in x["transcript"])
                if isinstance(x.get("entries"), list):
                    t += "\n\n".join((f"[{e.get('title')}]\n" if e.get("title") else "") + (e.get("text") or "") for e in x["entries"])
                t += x.get("contents") or ""
                add(x, t, f)
    nf = os.path.join(dist, "collectables_notes.json")
    if os.path.isfile(nf):
        for x in json.load(open(nf, encoding="utf-8")).get("items", []):
            add(x, x.get("contents", ""), "notes")
    return items


def pick_lore(items, cfg):
    kw = cfg.get("keywords", [])
    rx = re.compile("|".join(r"\b" + re.escape(k) + r"\b" for k in kw)) if kw else None
    seen, out = set(), []
    for it in items:
        if it["name"] in cfg.get("exclude", []) or not rx:
            continue
        if rx.search((it["text"] or "") + " " + it["name"]):
            key = (it["edid"], it["name"])
            tkey = (it["text"] or "").strip()[:400]
            if key in seen or (tkey and tkey in seen):
                continue
            seen.add(key)
            seen.add(tkey)
            out.append(it)
    return out


# ---------------------------------------------------------------------- main --
REGION_KW = [("LocRegionForest", "The Forest"), ("LocRegionToxicValley", "Toxic Valley"), ("LocRegionMTR", "Ash Heap"),
             ("LocRegionMountain", "Savage Divide"), ("LocRegionSwamp", "The Mire"), ("LocRegionCranberryBog", "Cranberry Bog")]
REGION_ORDER = ["Burning Springs", "The Forest", "Toxic Valley", "Ash Heap", "Savage Divide", "The Mire", "Cranberry Bog", "Skyline Valley", "Other"]


def slugify(t):
    return re.sub(r"[^a-z0-9]+", "-", t.lower()).strip("-")


def spawns_html(cfg, ch):
    """Every placed copy of the NPC, read from the location ref lists (LCSR), grouped by region."""
    npc = cfg["npc"].upper()
    lcsr = tsv_source.newest("LCTN_Export_*_LCSR.tsv", channel=ch)
    lctn = {r["LCTN_FormID"]: r for r in rows(tsv_source.newest("LCTN_Export_*_LCTN.tsv", channel=ch))}
    markers = []
    for r in rows(tsv_source.newest("REFR_Placements_*.tsv", channel=ch)):
        if r.get("MarkerName") and r.get("WorldspaceEDID") == "APPALACHIA":
            try:
                markers.append((float(r["X"]), float(r["Y"]), r["MarkerName"]))
            except ValueError:
                pass
    seen, spots = set(), defaultdict(list)
    for r in rows(lcsr):
        if f"[NPC_:{npc}]" not in r.get("Ref_Display", "").upper():
            continue
        ref = r["Ref_FormID"]
        if ref in seen:
            continue
        seen.add(ref)
        loc = lctn.get(r["LCTN_FormID"], {})
        kws = " ".join(v for k, v in loc.items() if k.startswith("KW_") and v)
        region = next((name for kw, name in REGION_KW if kw in kws), None)
        edid = r["LCTN_EDID"]
        if not region:
            region = "Burning Springs" if "Burn" in edid else ("Skyline Valley" if "RTSV" in edid or "Skyline" in edid else "Other")
        name = loc.get("LCTN_FULL") or edid
        home = cfg.get("home", {}).get(name)
        if home:
            label, img = home["label"], home["img"]
        else:
            label = name
            if "LocTypeSubRegion" in kws and markers:
                try:
                    gx, gy = int(r["GridX"]) * 4096 + 2048, int(r["GridY"]) * 4096 + 2048
                    label = "Near " + min(markers, key=lambda m: (m[0] - gx) ** 2 + (m[1] - gy) ** 2)[2]
                except ValueError:
                    pass
            img = cfg.get("imagePrefix", "") + slugify(label.replace("Near ", "")) + ".avif"
        spots[region].append((label, img))
    out = []
    for region in REGION_ORDER:
        if region not in spots:
            continue
        out.append(f"<h3>{region}</h3>")
        for label, img in sorted(spots[region]):
            out.append(f"<p>{label}</p>{{img:{img}}}")
    n = sum(len(v) for v in spots.values())
    return "".join(out), n, len(spots)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args()
    ch = "pts" if a.pts else "live"
    dist = os.path.join(REPO, "dist", "pts") if a.pts else os.path.join(REPO, "dist")
    out_dir = os.path.join(dist, "npc_lore")
    os.makedirs(out_dir, exist_ok=True)
    lines_p = tsv_source.newest("NPCDLG_Export_*_Lines.tsv", channel=ch)
    scenes_p = tsv_source.newest("NPCDLG_Export_*_Scenes.tsv", channel=ch)
    npc_p = tsv_source.newest("NPC_Export_*.tsv", channel=ch, exclude=("_PRPS", "_Refs", "ScriptProps"))
    glob_p = tsv_source.newest("GLOB_Export_*.tsv", channel=ch)
    price = ""
    for r in rows(glob_p):
        if r.get("EDID") == "BountyHunty_CapsPriceHeadHunt":
            price = f"{int(float(r['FLTV'])):,}"
    dlg = Dialogue(lines_p, scenes_p, npc_p)
    have_quests = {i["quest"] for i in dlg.info.values()}
    lore = lore_items(dist)
    cfg = json.load(open(CONFIG, encoding="utf-8"))["pages"]
    index = {}
    for slug, p in cfg.items():
        quests = [tuple(x) for x in p["dialogueQuests"]]
        if not any(q in have_quests for q, _t in quests):
            print(f"  {slug}: none of its quests are in the {ch} files - not written")
            continue
        names = {v: p["npc"] for v in (p["voice"] if isinstance(p["voice"], list) else [p["voice"]])}
        for ex in p.get("extraDialogue", []):
            names[ex["voice"]] = ex["npc"]
        groups, loose = dlg.build(quests, p["voice"], p["strip"], names, p.get("onlySpeakerQuests", []), p.get("allScenes", False))
        dialogue = [dict(title="Full Dialogue", npc=p["npc"], groups=groups, loose=loose)]
        for ex in p.get("extraDialogue", []):
            g2, l2 = dlg.build([tuple(x) for x in ex["quests"]], ex["voice"], p["strip"], names)
            dialogue.append(dict(title=ex["title"], npc=ex["npc"], groups=g2, loose=l2))
        sp_html = ""
        if p.get("spawns"):
            sp_html, n_sp, n_reg = spawns_html(p["spawns"], ch)
            print(f"  {slug}: {n_sp} placed copies across {n_reg} regions")
        sections = [dict(title=s["title"], html=s["html"].replace("{posterPrice}", price or "5,000").replace("{spawns}", sp_html))
                    for s in p["sections"]]
        docs = pick_lore(lore, p.get("lore", {}))
        data = dict(slug=slug, title=p["title"], blurb=p["blurb"], channel=ch, ptsOnly=p.get("ptsOnly", False),
                    generated=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    sources=[os.path.basename(x) for x in (lines_p, scenes_p, npc_p, glob_p)],
                    imageDir=p.get("imageDir", ""), sections=sections, footNote=p.get("footNote", ""),
                    lore=dict(intro=p.get("loreIntro", ""), docs=docs), dialogue=dialogue)
        with open(os.path.join(out_dir, slug + ".json"), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        n = sum(len(it["lines"]) for d in dialogue for g in d["groups"] for s in g["scenes"] for a in s["actions"] for it in a["items"])
        index[slug] = dict(title=p["title"], lines=n, docs=len(docs))
        print(f"  {slug}: {n} dialogue lines, {len(docs)} lore documents")
    with open(os.path.join(out_dir, "_index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
