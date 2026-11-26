"""npc_dialogue_export.py - the NPCDLG export (22 Nov 2026).

Full dialogue for the quests listed in tools/npc_dialogue_quests.txt, for the
NPC Lore & Dialogue pages (src/build_npc_lore_json.py), plus every collectable
quest (script property CollectablesNeededPerBatch) for the collectable guides:

  NPCDLG_Export_<..>_Lines.tsv    one row per response line of every INFO in
                                  those quests: topic, the player's prompt
                                  (INFO RNAM, else the topic's FULL), the reply,
                                  who speaks it (the voice-type folders its audio
                                  sits in, from the Voices .ba2 name tables),
                                  conditions, scenes it starts, scripts.
  NPCDLG_Export_<..>_Scenes.tsv   one row per topic a scene action uses, in
                                  action order: action type (3 = player menu,
                                  0/5 = NPC line), phase, topic role (ESCS =
                                  menu choice, DATA = spoken line).
Called from export_from_esm.py (EXPORTS["NPCDLG"]).
"""
import os
import re
import struct

HERE = os.path.dirname(os.path.abspath(__file__))
QUEST_LIST = os.path.join(HERE, "npc_dialogue_quests.txt")
VOICE_BA2 = ("SeventySix - Voices.ba2", "SeventySix - 00UpdateMain.ba2", "SeventySix - 01UpdateMain.ba2",
             "SeventySix - 01UpdateVoices.ba2", "SeventySix - 02UpdateMain.ba2", "SeventySix - 02UpdateVoices.ba2")


def wanted_quests():
    out = []
    for line in open(QUEST_LIST, encoding="utf-8"):
        line = line.split("#", 1)[0].strip()
        if line:
            out.append(line)
    return out


def voice_map(data_dir):
    """{INFO FormID: set(voice type folder)} from the .ba2 name tables."""
    rx = re.compile(r"sound/voice/seventysix\.esm/([^/]+)/([0-9a-f]{8})_\d+\.", re.I)
    m = {}
    for name in VOICE_BA2:
        p = os.path.join(data_dir, name)
        if not os.path.isfile(p):
            continue
        with open(p, "rb") as f:
            _magic, _ver, _typ, n, nto = struct.unpack("<4sI4sIQ", f.read(24))
            f.seek(nto)
            for _ in range(n):
                ln = struct.unpack("<H", f.read(2))[0]
                hit = rx.match(f.read(ln).decode("utf-8", "replace"))
                if hit:
                    m.setdefault(int(hit.group(2), 16), set()).add(hit.group(1).lower())
    return m


def collectable_quests(esm, X):
    """Collectable quests (Dirty Laundry, A Matter of Matter, The Slasher:
    Masked Truth): a quest script property CollectablesNeededPerBatch - the same
    rule QUST2 uses for Kind "Collectable". Their dialogue checks go on the
    collectable guides' Finale sections (Duchess, 27 Nov 2026)."""
    out = set()
    for r in X.R.xedit_order(esm, "QUST"):
        if b"CollectablesNeededPerBatch" in (dict(X._qust_tree(r)["head"]).get("VMAD") or b""):
            out.add(r.edid)
    return out


def exp_npc_dialogue(esm, X):
    want = set(wanted_quests()) | collectable_quests(esm, X)
    kids, infos = X._quest_children(esm)
    vm = voice_map(esm.data_dir)
    cc = X.cc
    lines = ["QuestFormID\tQuestEDID\tQuestName\tTopicFormID\tTopicEDID\tTopicSubtype\tTopicPrompt\tInfoOrder\tInfoFormID\t"
             "ResponseIndex\tResponseText\tPlayerPrompt\tVoiceTypes\tConditions\tStartsScenes\tScripts\tScriptProps"]
    scenes = ["QuestFormID\tQuestEDID\tSceneFormID\tSceneEDID\tPhaseNames\tActionIndex\tActionType\tStartPhase\tTopicRole\tTopicFormID"]
    for r in X.R.xedit_order(esm, "QUST"):
        if r.edid not in want:
            continue
        qfid = "%08X" % r.fid
        qname = cc(esm.lstring(r.first("FULL")))
        tree = X._qust_tree(r)
        arows = X._q2_alias_rows(esm, r, tree)
        named = {int(aid): name for _i, _a, aid, name, k in arows if aid and name and k != "RefCol"}
        ctx = X.C.Ctx(esm, r, named)
        for sig, fid, subs in kids.get(r.fid, []):
            sd = dict(subs)
            if sig == "DIAL":
                tprompt = cc(esm.lstring(sd.get("FULL")))
                sn = sd.get("SNAM")
                sub = sn[:4].decode("latin1") if sn and len(sn) >= 4 else ""
                for io, (ifid, isubs) in enumerate(infos.get(fid, [])):
                    isd = dict(isubs)
                    conds = X._ad_conds(ctx, isubs)
                    rn = isd.get("RNAM")
                    iprompt = cc(esm.lstring(rn)) if rn else ""
                    starts = [X._q2_linkcell(esm, struct.unpack_from("<I", v, 0)[0]) for k, v in isubs if k == "TSCE" and len(v) == 4]
                    names, _f, _g = X._q2_vmad(isd.get("VMAD"))
                    props = []
                    vmr = isd.get("VMAD")
                    if vmr:
                        try:
                            for sname, plist in X._VmadReader(vmr).scripts():
                                for pn, v in plist:
                                    if v[0] == "obj" and v[1]:
                                        props.append("%s=%s" % (pn, X._q2_linkcell(esm, v[1])))
                                    elif v[0] in ("int", "float", "bool"):
                                        props.append("%s=%g" % (pn, v[1]))
                        except Exception:  # noqa: BLE001
                            pass
                    resps, cur = [], None
                    for k, v in isubs:
                        if k == "TRDA":
                            cur = {}
                            resps.append(cur)
                        elif cur is not None and k == "NAM1":
                            cur.setdefault(k, v)
                    vt = ",".join(sorted(vm.get(ifid, [])))
                    base = [qfid, cc(r.edid), qname, "%08X" % fid, cc(X._str(sd.get("EDID"))), sub, tprompt, str(io), "%08X" % ifid]
                    tail = [iprompt, vt, conds, " ; ".join(s for s in starts if s), " ; ".join(cc(n) for n in names if n), cc(" ; ".join(props))]
                    if not resps:
                        lines.append("\t".join(base + ["", ""] + tail))
                    for ri, rs in enumerate(resps):
                        lines.append("\t".join(base + [str(ri), cc(esm.lstring(rs.get("NAM1")))] + tail))
            elif sig == "SCEN":
                ed = cc(X._str(sd.get("EDID")))
                acts, cur, phases = [], None, []
                for k, v in subs:
                    if k == "ANAM":
                        cur = {"type": struct.unpack("<H", v)[0], "f": []} if len(v) == 2 else None
                        if cur is not None:
                            acts.append(cur)
                        continue
                    if cur is not None:
                        cur["f"].append((k, v))
                    elif k == "NAM0" and v and v != b"\x00":
                        phases.append(v.rstrip(b"\0").decode("latin1"))
                for ai, a in enumerate(acts):
                    f = {}
                    for k, v in a["f"]:
                        f.setdefault(k, v)
                    ph = str(struct.unpack("<I", f["SNAM"][:4])[0]) if "SNAM" in f else ""
                    roles = []
                    if a["type"] == 3:
                        roles = [("ESCS", v) for k, v in a["f"] if k == "ESCS" and len(v) == 4]
                    elif a["type"] in (0, 5):
                        roles = [("DATA", v) for k, v in a["f"] if k == "DATA" and len(v) == 4]
                    elif a["type"] == 4:
                        roles = [("SCEN", v) for k, v in a["f"] if k in ("LCEP", "DATA") and len(v) == 4]
                    if not roles:
                        scenes.append("\t".join([qfid, cc(r.edid), "%08X" % fid, ed, " | ".join(phases), str(ai), str(a["type"]), ph, "", ""]))
                    for role, v in roles:
                        scenes.append("\t".join([qfid, cc(r.edid), "%08X" % fid, ed, " | ".join(phases), str(ai), str(a["type"]), ph, role,
                                                 "%08X" % struct.unpack("<I", v[:4])[0]]))
    return {"_Lines": lines, "_Scenes": scenes}
