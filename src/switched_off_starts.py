#!/usr/bin/env python3
"""
switched_off_starts.py — content whose start item can't currently be obtained
==============================================================================

A REPORT, not a rule. Nothing here marks anything cut.

WHY
---
Bethesda rarely deletes retired content. It leaves the quest, the rewards and
the loot lists wired up and switches the way in off. The Drifter (P62_*) is
the case that started this (Duchess, 21 Oct 2026): "Defeat The Drifter" pays
out from real reward records and published 92 plan routes as live, but the
fight only starts with the Drifter Activation Card, and every reward that hands
the card out rolls

    GetRandomPercent <= P62_LCP_TheDrifter_KeycardChance_<Events|DailyOps|...>

with every one of those settings at 0. Nothing in the files proves that is
permanent: a seasonal event's switch looks exactly the same between runs, and
only 44 GLOBs changed value across eight exports Dec 2025 - Oct 2026 (the
server overrides them, the files don't). So this can't be an automatic cut
rule. What it CAN do is find every case, every build, so a person confirms
each one once (plan_sources.CONFIRMED_CUT_SOURCE_PATTERNS) instead of finding
it page by page.

WHAT IT CHECKS
--------------
A start item is a KEYM that a quest references and an ID-card reader ACTI
reads (IDCardReaderScript::idCard). Its ways out are:
  * a GMRW reward row that rolls a list holding it - off when the row's
    GetRandomPercent threshold resolves to 0 (a literal 0 or a GLOB at 0),
    or the row's reward record is cut / unreferenced
  * a CONT, NPC or placed REFR holding it - always counted as ON (a person
    can pick it up)
  * an LVLI parent - ON unless every one of its own ways out is off
It is reported when it has at least one way out and every one is off.

The report names the quest, the start item, the switches, how many plan routes
on plan_master run through that quest's rewards, and whether the family is
already a confirmed cut source. Read by build_bugged_plans_json.py
(`switched_off_starts`) and printed in the build log.

Standalone:
    python src/switched_off_starts.py --data-dir tsv --master dist/plan_master.json
"""
import argparse, collections, csv, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
csv.field_size_limit(1 << 30)

import tsv_source                      # noqa: E402
import plan_sources                    # noqa: E402

RX_REF_COL = re.compile(r"^Ref_?\d+$")
RX_IDCARD = re.compile(r"idCard=([0-9A-Fa-f]{8}):")
RX_GRP = re.compile(r"GetRandomPercent\([^)]*\)\s*([01]{8})\s*(.*)$", re.I)
RX_GLOB = re.compile(r"\[GLOB:([0-9A-Fa-f]{8})\]")


def _newest(root, pat):
    return tsv_source.newest(os.path.join(root, pat), required=False)


def _rows(path):
    if not path:
        return
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
        r = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        head = next(r)
        for row in r:
            yield head, row


def _ref(s):
    bits = (s or "").strip().split(":")
    if len(bits) < 3:
        return ((bits[0] if bits else "").upper(), "", "")
    return bits[0].strip().upper(), ":".join(bits[1:-1]).strip(), bits[-1].strip().upper()


def _dev(edid):
    e = (edid or "").strip()
    return bool(e) and (plan_sources.is_dev_record(e)
                        or bool(plan_sources._RX_DEV_RECORD.search(e)))


def _grp_ceiling(cond_full, globs):
    """Upper bound (0-100) a GetRandomPercent condition lets through, or None
    when the row has no GetRandomPercent at all (= always rolls)."""
    best = None
    for part in (cond_full or "").split("|"):
        m = RX_GRP.search(part)
        if not m:
            continue
        bits, rhs = m.group(1), m.group(2).strip()
        op = int(bits[0]) + 2 * int(bits[1]) + 4 * int(bits[2])
        g = RX_GLOB.search(rhs)
        try:
            val = globs.get(g.group(1).upper()) if g else float(rhs.split()[0])
        except (ValueError, IndexError):
            val = None
        if val is None:
            return 100.0          # unreadable -> don't judge
        if op in (4, 5):          # < / <=
            ceil = val
        elif op in (2, 3):        # > / >=
            ceil = 100.0 - val
        else:
            ceil = 100.0
        best = ceil if best is None else min(best, ceil)
    return best


def scan(root, master_path=None):
    globs, glob_edid = {}, {}
    for h, row in _rows(_newest(root, "GLOB_Export_*.tsv")):
        if len(row) > 2:
            try:
                globs[row[0].upper()] = float(row[2])
            except ValueError:
                pass
            glob_edid[row[0].upper()] = row[1]

    quests = {}
    for h, row in _rows(_newest(root, "QUEST_Export_*.tsv")):
        if len(row) > 2:
            quests[row[0].upper()] = (row[1], row[2])

    # ID-card readers: KEYM fid -> reader EditorIDs
    readers = collections.defaultdict(list)
    for h, row in _rows(_newest(root, "ACTI_Export_*_ACTI.tsv")):
        for m in RX_IDCARD.finditer("\t".join(row)):
            readers[m.group(1).upper()].append(row[1] if len(row) > 1 else "")

    keym = {}
    for h, row in _rows(_newest(root, "KEYM_Export_*[0-9].tsv")):
        d = dict(zip(h, row))
        refs = [_ref(row[i]) for i, c in enumerate(h)
                if RX_REF_COL.match(c) and i < len(row) and row[i].strip()]
        keym[(d.get("FormID") or "").upper()] = {
            "edid": d.get("EDID") or "", "full": d.get("FULL") or "", "refs": refs}

    lvli_refs = {}
    for h, row in _rows(_newest(root, "LVLI_Export_*_LVLI_Refs.tsv")):
        lvli_refs[row[0].upper()] = [_ref(x) for x in row[3:] if x.strip()]
    lvli_edid = {}
    for h, row in _rows(_newest(root, "LVLI_Export_*_LVLI_List.tsv")):
        if len(row) > 1:
            lvli_edid[row[0].upper()] = row[1]

    # GMRW reward rows by rolled list, and quest -> reward records
    gmrw_rows = collections.defaultdict(list)     # LVLI fid -> [(gmrw fid, edid, ceiling)]
    gmrw_quest = {}
    gmrw_refs = {}
    gp = _newest(root, "GMRW_Export_*.tsv")
    if gp:
        with open(gp, encoding="utf-8", errors="replace") as f:
            for d in csv.DictReader(f, delimiter="\t"):
                gf = (d.get("FormID") or "").upper()
                it = _ref(d.get("RewardedItem") or "")
                q = _ref(d.get("ParentQuestLink") or "")
                if q[0]:
                    gmrw_quest[gf] = q[0]
                gmrw_refs[gf] = [_ref(v) for k, v in d.items() if RX_REF_COL.match(k or "") and v]
                if it[2] == "LVLI":
                    gmrw_rows[it[0]].append((gf, d.get("EDID") or "",
                                             _grp_ceiling(d.get("ConditionsFull"), globs)))

    def gmrw_live(gf, edid):
        if _dev(edid) or plan_sources.cut_source(edid):
            return False
        refs = gmrw_refs.get(gf) or []
        return any(e and not _dev(e) for _, e, _ in refs)

    def list_ways(lf, seen):
        """(on, off) ways out for a list, each as a short string."""
        on, off = [], []
        if lf in seen:
            return on, off
        seen.add(lf)
        for gf, ge, ceil in gmrw_rows.get(lf, []):
            if not gmrw_live(gf, ge):
                continue
            (off if ceil is not None and ceil <= 0 else on).append(ge)
        for rf, re_, rs in lvli_refs.get(lf, []):
            if rs == "GMRW":
                continue          # judged row by row above
            if _dev(re_):
                continue
            if rs == "LVLI":
                o2, f2 = list_ways(rf, seen)
                on += o2
                off += f2
            else:
                on.append(f"{re_ or rf} [{rs}]")
        return on, off

    out = []
    for kf, rd in sorted(readers.items()):
        k = keym.get(kf)
        if not k:
            continue
        qs = [(f, e) for f, e, s in k["refs"] if s == "QUST" and not _dev(e)]
        if not qs:
            continue
        on, off, lists = [], [], []
        for rf, re_, rs in k["refs"]:
            if rs in ("QUST", "ACTI", "FLST") or _dev(re_):
                continue
            if rs == "LVLI":
                lists.append(re_)
                o, f_ = list_ways(rf, set())
                on += o
                off += f_
            else:
                on.append(f"{re_ or rf} [{rs}]")
        if on or not off:
            continue
        # the GLOBs that gate it, by name, for the reader
        gp_globs = sorted({glob_edid.get(g, g) for lf in [r[0] for r in k["refs"] if r[2] == "LVLI"]
                           for gf, ge, ceil in gmrw_rows.get(lf, [])
                           for g in _globs_of_row(gp, gf)} if gp else set())
        for qf, qe in qs:
            rewards = [g for g, q in gmrw_quest.items() if q == qf]
            out.append({
                "quest": (quests.get(qf) or (qe, ""))[1] or qe,
                "quest_edid": qe,
                "start_item": k["full"] or k["edid"],
                "start_item_edid": k["edid"],
                "ways_out_switched_off": len(off),
                "switches": [{"glob": g, "value": globs.get(_glob_fid(glob_edid, g))}
                             for g in gp_globs],
                "reward_records": len(rewards),
                "confirmed_cut": bool(plan_sources.cut_source(qe)),
                "plan_routes": _plan_routes(master_path, rewards, lvli_refs, gmrw_rows),
            })
    return out


_ROW_GLOBS = {}


def _globs_of_row(gpath, gmrw_fid):
    """GLOB FormIDs a GMRW record's GetRandomPercent rows point at."""
    if gpath not in _ROW_GLOBS:
        idx = collections.defaultdict(set)
        with open(gpath, encoding="utf-8", errors="replace") as f:
            for d in csv.DictReader(f, delimiter="\t"):
                for part in (d.get("ConditionsFull") or "").split("|"):
                    if "GetRandomPercent" in part:
                        for g in RX_GLOB.findall(part):
                            idx[(d.get("FormID") or "").upper()].add(g.upper())
        _ROW_GLOBS[gpath] = idx
    return _ROW_GLOBS[gpath].get(gmrw_fid, set())


def _glob_fid(glob_edid, name):
    for f, e in glob_edid.items():
        if e == name:
            return f
    return name


def _plan_routes(master_path, rewards, lvli_refs, gmrw_rows):
    """How many published plan routes (live or retired) run through lists
    these reward records roll, directly or through sub-lists."""
    if not master_path or not os.path.exists(master_path):
        return None
    rewarded = {lf for lf, rows in gmrw_rows.items() if any(g in rewards for g, _, _ in rows)}
    # every list whose ancestors include a rewarded list
    children = collections.defaultdict(set)
    for lf, refs in lvli_refs.items():
        for rf, _e, rs in refs:
            if rs == "LVLI":
                children[rf].add(lf)
    reach, stack = set(rewarded), list(rewarded)
    while stack:
        x = stack.pop()
        for c in children.get(x, ()):
            if c not in reach:
                reach.add(c)
                stack.append(c)
    with open(master_path, encoding="utf-8") as f:
        m = json.load(f)
    n = 0
    for it in m.get("items", []):
        for r in (it.get("obtain_routes") or []) + (it.get("retired_routes") or []):
            if {str(x).upper() for x in r.get("lvli") or []} & reach:
                n += 1
    return n


def report(rows, stream=None):
    stream = stream or sys.stdout
    print(f"[switched_off_starts] {len(rows)} quest(s) whose start item every source hands "
          f"out at 0% (needs a person to confirm cut or seasonal):", file=stream)
    for r in rows:
        tag = "confirmed cut" if r["confirmed_cut"] else "NOT YET CONFIRMED"
        sw = ", ".join(f"{s['glob']}={s['value']:g}" if isinstance(s["value"], float)
                       else s["glob"] for s in r["switches"][:4])
        print(f"    {r['quest']} - starts with {r['start_item']}; "
              f"{r['ways_out_switched_off']} way(s) out, all off ({sw}); "
              f"plan routes: {r['plan_routes']}; {tag}", file=stream)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.path.join(REPO, "tsv"))
    ap.add_argument("--master", default=os.path.join(REPO, "dist", "plan_master.json"))
    a = ap.parse_args(argv)
    report(scan(a.data_dir, a.master))


if __name__ == "__main__":
    main()
