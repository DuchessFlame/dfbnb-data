#!/usr/bin/env python3
"""
esm_buff_rules_extract.py — read buff RULES straight out of SeventySix.esm.

Two exports the Recommended Food Buff Calculator needs that the existing xEdit
exports do not carry:

  tsv/MGEFKW_Export_<Month>_<Year>.tsv
      one row per MGEF: keywords + DATA flags (detrimental bit).
      The keywords decide everything about stacking and mutations:
        SURV_EffectTypeFoodBuff / FoodHunger / FoodHealing
            -> the ONLY effects Carnivore / Herbivore multiply
        FoodDispelEffect_*  /  Stack*
            -> effects sharing one of these overwrite each other (one buff
               slot), e.g. two Strength foods, or Whiskey + Mire Magic STR

  tsv/PERKEP_Export_<Month>_<Year>.tsv
      one row per PERK effect (entry points, abilities): entry point id,
      function, value and every condition with its keyword / form EDID.
      This is where the mutation rules live:
        Mutation_EatAllTheMeat_Perk    Meat items x2 (x2.5 "super")
        Mutation_EatNoVeggies_Perk     Vegetable items x0
        Mutation_EatAllTheVeggies_Perk Vegetable / Herb / Fruit items x2 (x2.5)
        Mutation_EatNoMeat_Perk        Meat items x0
      and Party Boy/Girl (alcohol x2 / x3), Chem Fiend, the ghoul perk, etc.

The xEdit scripts "!!!Wordpress - ExportMGEFKeywordsToTSV.pas" and
"!!!Wordpress - ExportPERKEntryPointsToTSV.pas" write the same two files from
inside xEdit. Either source works — build_food_buff_calculator_json.py reads
whichever is newest and only relies on the EDIDs inside the condition text.

Stdlib only. Reads only the top-level groups it needs (seeks past the rest), so
a full run takes a second or two.

Usage:
  python tools/esm_buff_rules_extract.py "D:\\steamlibrary\\steamapps\\common\\Fallout76\\Data\\SeventySix.esm"
  python tools/esm_buff_rules_extract.py <esm> --outdir tsv --stamp October_2026
  python tools/esm_buff_rules_extract.py <esm> --outdir tsv/pts --pts
"""
import argparse, datetime, os, struct, sys, zlib

WANT = ("MGEF", "PERK", "KYWD", "CNDF", "SPEL", "ALCH", "GLOB", "AVIF")
COND_OPS = ["=", "!=", ">", ">=", "<", "<="]


def _parse_subs(d):
    out, i, big = [], 0, None
    while i + 6 <= len(d):
        t = d[i:i + 4].decode("latin1")
        s = struct.unpack("<H", d[i + 4:i + 6])[0]
        i += 6
        if t == "XXXX":
            big = struct.unpack("<I", d[i:i + 4])[0]
            i += 4
            continue
        if big is not None:
            s, big = big, None
        out.append((t, d[i:i + s]))
        i += s
    return out


def _parse_group(buf):
    i, n = 0, len(buf)
    while i < n:
        t = buf[i:i + 4]
        sz = struct.unpack("<I", buf[i + 4:i + 8])[0]
        if t == b"GRUP":
            yield from _parse_group(buf[i + 24:i + sz])
            i += sz
            continue
        flags, fid = struct.unpack("<II", buf[i + 8:i + 16])
        data = buf[i + 24:i + 24 + sz]
        i += 24 + sz
        if flags & 0x00040000:
            data = zlib.decompress(data[4:])
        yield t.decode("latin1"), fid, _parse_subs(data)


def read_esm(path, want=WANT):
    recs = {}
    with open(path, "rb") as f:
        size = f.seek(0, 2)
        f.seek(0)
        hdr = f.read(24)
        f.seek(24 + struct.unpack("<I", hdr[4:8])[0])
        while f.tell() < size:
            gh = f.read(24)
            if len(gh) < 24:
                break
            if gh[:4] != b"GRUP":
                raise SystemExit(f"unexpected record at {f.tell() - 24}: {gh[:4]!r}")
            gsize = struct.unpack("<I", gh[4:8])[0]
            label = gh[8:12].decode("latin1")
            start = f.tell() - 24
            if label in want:
                for t, fid, subs in _parse_group(f.read(gsize - 24)):
                    recs.setdefault(t, {})[fid] = subs
            f.seek(start + gsize)
    return recs


class Names:
    def __init__(self, recs):
        self.edid, self.sig = {}, {}
        for sig, d in recs.items():
            for fid, subs in d.items():
                self.sig[fid] = sig
                for st, b in subs:
                    if st == "EDID":
                        self.edid[fid] = b.rstrip(b"\0").decode("latin1", "replace")
                        break

    def ref(self, fid):
        if not fid:
            return ""
        e = self.edid.get(fid)
        sig = self.sig.get(fid, "")
        return f"{e} [{sig}:{fid:08X}]" if e else f"[{fid:08X}]"


def ctda_text(b, names):
    op = COND_OPS[(b[0] >> 5)] if (b[0] >> 5) < 6 else "?"
    is_or = bool(b[0] & 1)
    use_glob = bool(b[0] & 4)
    comp = struct.unpack("<I", b[4:8])[0]
    comp_txt = names.ref(comp) if use_glob else ("%g" % struct.unpack("<f", b[4:8])[0])
    func = struct.unpack("<H", b[8:10])[0]
    p1, p2 = struct.unpack("<II", b[12:20])
    runon = struct.unpack("<I", b[20:24])[0]
    run_txt = {0: "Subject", 1: "Target", 2: "Reference", 3: "CombatTarget", 4: "LinkedRef"}.get(runon, f"RunOn{runon}")
    params = ", ".join(x for x in (names.ref(p1) if p1 in names.edid else (f"{p1}" if p1 else ""),
                                   names.ref(p2) if p2 in names.edid else "") if x)
    return f"{run_txt}.Func{func}({params}) {op} {comp_txt} {'OR' if is_or else 'AND'}"


def mgef_rows(recs, names):
    rows = []
    for fid, subs in sorted(recs.get("MGEF", {}).items()):
        kws, flags = [], ""
        detr = 0
        for st, b in subs:
            if st == "KWDA":
                kws = struct.unpack("<%dI" % (len(b) // 4), b)
            elif st == "DATA" and len(b) >= 4:
                fl = struct.unpack("<I", b[:4])[0]
                flags = f"{fl:08X}"
                detr = 1 if fl & 0x4 else 0
        flat = " | ".join(f"{names.edid.get(k, '')}[{k:08X}]" for k in kws)
        if any(names.edid.get(k) == "EffectTypeDetrimental" for k in kws):
            detr = 1
        rows.append([f"{fid:08X}", names.edid.get(fid, ""), flags, str(detr), str(len(kws)), flat])
    return ["MGEF_FormID", "EDID", "DATA_Flags_Hex", "Detrimental", "KeywordCount", "Keywords_Flat"], rows


def perk_rows(recs, names):
    rows = []
    for fid, subs in sorted(recs.get("PERK", {}).items()):
        effects, cur, tab = [], None, None
        for st, b in subs:
            if st == "PRKE":
                cur = {"type": b[0], "rank": b[1] if len(b) > 1 else 0,
                       "prio": b[2] if len(b) > 2 else 0, "ep": "", "fn": "",
                       "val": "", "ab": 0, "conds": []}
                effects.append(cur)
                tab = None
            elif cur is None:
                continue
            elif st == "DATA" and cur["ep"] == "" and cur["ab"] == 0:
                if cur["type"] == 2 and len(b) >= 2:
                    cur["ep"], cur["fn"] = str(b[0]), str(b[1])
                elif len(b) >= 4:
                    cur["ab"] = struct.unpack("<I", b[:4])[0]
            elif st == "PRKC":
                tab = b[0]
            elif st == "CTDA":
                cur["conds"].append(f"[tab {tab}] " + ctda_text(b, names))
            elif st == "EPFD":
                if len(b) == 4:
                    v = struct.unpack("<f", b)[0]
                    cur["val"] = "%g" % v
                else:
                    cur["val"] = b.hex()
        for i, e in enumerate(effects):
            kind = {0: "Quest", 1: "Ability", 2: "EntryPoint"}.get(e["type"], str(e["type"]))
            rows.append([f"{fid:08X}", names.edid.get(fid, ""), str(i), kind, str(e["rank"]),
                         str(e["prio"]), e["ep"], e["fn"], e["val"],
                         f"{e['ab']:08X}" if e["ab"] else "", names.edid.get(e["ab"], "") if e["ab"] else "",
                         str(len(e["conds"])), " | ".join(e["conds"])])
    hdr = ["PERK_FormID", "PERK_EDID", "EffectIndex", "EffectType", "Rank", "Priority",
           "EntryPoint", "Function", "Value", "Ability_FormID", "Ability_EDID",
           "ConditionCount", "Conditions"]
    return hdr, rows


def write_tsv(path, hdr, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("\t".join(hdr) + "\n")
        for r in rows:
            f.write("\t".join(str(c).replace("\t", " ").replace("\n", " ") for c in r) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("esm")
    ap.add_argument("--outdir", default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tsv"))
    ap.add_argument("--stamp", default=None, help="e.g. October_2026 (default: the ESM's modified month)")
    ap.add_argument("--pts", action="store_true", help="name files *_PTS_<date>.tsv")
    a = ap.parse_args()
    recs = read_esm(a.esm)
    names = Names(recs)
    mt = datetime.datetime.fromtimestamp(os.path.getmtime(a.esm))
    stamp = a.stamp or mt.strftime("%B_%Y")
    if a.pts:
        stamp = "PTS_" + mt.strftime("%Y-%m-%d_%H%M")
    h, r = mgef_rows(recs, names)
    p1 = os.path.join(a.outdir, f"MGEFKW_Export_{stamp}.tsv")
    write_tsv(p1, h, r)
    h, r2 = perk_rows(recs, names)
    p2 = os.path.join(a.outdir, f"PERKEP_Export_{stamp}.tsv")
    write_tsv(p2, h, r2)
    print(f"wrote {p1} ({len(r)} MGEF)\nwrote {p2} ({len(r2)} PERK effects)")


if __name__ == "__main__":
    main()
