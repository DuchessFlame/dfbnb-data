#!/usr/bin/env python3
"""
plan_learn_routes.py — the real route for plan rows the loot walk finds no
source for, and which of them a plan mule can never hold (31 Oct 2026).

WHY
---
The plan check (Brewmaster) and the mule lists use every TRADEABLE plan in
plan_master. 64 tradeable plans had no drop route and no unlock, so they all
read "bugged and not currently dropping". Checked one by one against
SeventySix.esm (every record, byte search for the BOOK and its recipe FormIDs)
most of them are not bugged at all — the plan item is simply not how the game
hands the thing out. Every rule below is a structural fact from the exports,
no names, no FormIDs:

  quest item     The BOOK sits in a live quest's item alias flagged
                 "Quest Object": the quest puts it in your pack and makes you
                 read it as a step (Hunter for Hire: "Read the 'Plan: Crane
                 Treasure Hunting Sign' schematics from your inventory"). A quest
                 object can't be dropped, traded or sold -> tradeable False.

  dead giver     The only things that give the BOOK are quests / quest rewards
                 whose quest is a dev record (zzz/cut/debug ...). Their unlock
                 sentences move to `retired_unlocks`, same as prune_dead_routes
                 does for dead lists. (Enjoy Your Stay = ZZZNPE_MQ01_Enjoy.)

  quest learned  Nothing gives the BOOK, and its recipe (COBJ) is handed out
                 by a quest reward (GMRW RewardedItem = the COBJ: Wasted on
                 Nukashine gives the Brewing + Fermenting Station recipes) or is
                 held by a live quest's own script (Miner Miracles holds the six
                 Excavator recipes). The quest teaches the recipe; no plan item
                 ever exists -> tradeable False, `quest_learned`, `learn_quest`.

  duplicate      Another BOOK with the same name drops; this copy is given by
                 nothing. Folded into the live copy (`merged_plans`), marked cut
                 so it leaves the pages, tradeable False — same as the Nuclear
                 Winter duplicate merge in legacy_nw.py. In game they are one plan.

  unused         Nothing gives the BOOK AND reading it would teach nothing: no
                 recipe is unlocked by it, or every recipe it unlocks is a dev
                 record, or a "condition proxy" recipe (no created object) that
                 no other record checks. -> not_obtainable, tradeable False.
                 Where an entitlement of the same name exists, the sentence says
                 the item itself is a Scoreboard / Atom Shop reward.

Everything else with no source stays as it was: a real plan whose recipe works
but which nothing in the current files gives out — "not currently dropping".
Old copies can still be in players' hands (Sara's mule has Laser Chainsaw Paint),
so those stay tradeable.

Pure join over the exports. No rate is touched (plan-obtain-pipeline).
Runs inside build_plan_obtain_json.py after prune_dead_routes and before source
tags. Standalone over a finished file (seconds):
    python src/plan_learn_routes.py dist/plan_master.json --data-dir tsv
Then re-run apply_source_pill.py? No — this module rebuilds the ledger and the
source tag of every row it changes itself.
"""
import argparse
import collections
import csv
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import tsv_source                                     # noqa: E402
import plan_sources                                   # noqa: E402
import plan_source_pill                               # noqa: E402

csv.field_size_limit(10 ** 9)

# a giver is dead when its own EditorID is a dev record (shared test)
def _dev(edid):
    e = edid or ""
    return (bool(plan_sources.is_dev_record(e)) or bool(plan_sources._RX_DEV_RECORD.search(e))
            or bool(re.match(r"^zzz", e, re.I)))

# references that can hand a plan item out (anything else is wiring)
_GIVER_SIGS = {"LVLI", "CONT", "NPC_", "QUST", "GMRW", "TERM", "REFR", "COEN", "ENTM", "ACHR"}
# references that prove a no-object recipe is actually checked by something
_PROXY_USER_SIGS = {"LVLI", "COBJ", "IDLE", "CNDF", "PERK", "SPEL", "MGEF", "ACTI",
                    "FURN", "STAT", "TERM", "QUST", "CHAL", "GMRW", "MESG", "SCOL"}


# Hand calls (keep short; a file rule wins). Each one names who decided it.
# Recipe unlocked by finishing a quest where the plan BOOK itself is linked to
# no recipe, so no rule can join the two: (sentence, quest edid, quest name).
QUEST_UNLOCK_BY_HAND = {
    # co_Armor_Backpack_Standard: "Known by default or when conditions are met",
    # condition GetQuestCompleted(P01C_Tadpole). Kat, 31 Oct 2026: quest reward
    # after the Tadpole and Possum badges.
    "0046A36C": ("Unlocked when you complete the quest: The Order of the Tadpole "
                 "(the backpack recipe is known once the quest is done — no plan is handed out).",
                 "P01C_Tadpole", "The Order of the Tadpole"),
}
# Never released (Kat, 31 Oct 2026).
CUT_BY_HAND = {
    "008591ED": "never released (Kat, 31 Oct 2026)",          # Loveland Frog Plushie
}


def _newest(tsv_dir, pat):
    return tsv_source.newest(os.path.join(tsv_dir, pat), required=False)


def _rows(path):
    if not path or not os.path.exists(path):
        return
    with open(path, encoding="utf-8", errors="replace") as f:
        yield from csv.DictReader(f, delimiter="\t")


def _refs(cells):
    """['FID:EDID:SIG', ...] -> [(fid, edid, sig)]"""
    out = []
    for c in cells:
        c = (c or "").strip()
        if c.count(":") < 2:
            continue
        fid, rest = c.split(":", 1)
        edid, sig = rest.rsplit(":", 1)
        out.append((fid.upper(), edid, sig))
    return out


class Index:
    def __init__(self, tsv_dir):
        self.book_refs = {}
        p = _newest(tsv_dir, "BOOK_Export_*.tsv")
        for r in _rows(p):
            fid = (r.get("FormID") or "").upper()
            cells = [v for k, v in r.items() if k and re.match(r"^Ref\d+$", k)]
            self.book_refs[fid] = _refs(cells)
        # recipes each plan unlocks (COBJ.GNAM = the plan)
        self.cobj_by_plan = collections.defaultdict(list)
        p = _newest(tsv_dir, "COBJ_Export_*.tsv")
        for r in _rows(p):
            g = (r.get("GNAM_FormID") or "").upper()
            if not g:
                continue
            flat = (r.get("ReferencedBy_Flat") or "").split("|")
            self.cobj_by_plan[g].append({
                "formid": (r.get("COBJ_FormID") or "").upper(),
                "edid": r.get("COBJ_EDID") or "",
                "cnam": (r.get("CNAM_FormID") or "").upper(),
                "refs": _refs(flat),
            })
        # GMRW -> its quest (EDID, name)
        self.gmrw_quest = {}
        p = _newest(tsv_dir, "GMRW_Export_*.tsv")
        for r in _rows(p):
            link = (r.get("ParentQuestLink") or "").split(":")
            self.gmrw_quest[(r.get("FormID") or "").upper()] = (
                link[1] if len(link) > 1 else "", (r.get("ParentQuestDisplay") or "").strip())
        # quest names
        self.quest_name = {}
        p = _newest(tsv_dir, "QUEST_Export_*.tsv")
        for r in _rows(p):
            vals = list(r.values())
            if len(vals) > 2:
                self.quest_name[(vals[0] or "").upper()] = ((vals[1] or ""), (vals[2] or "").strip())
        # item aliases flagged Quest Object: BOOK fid -> [(quest fid, quest edid, name)]
        self.quest_object = collections.defaultdict(list)
        p = _newest(tsv_dir, "QUST2_Export_*_ItemAliases.tsv")
        for r in _rows(p):
            if "quest object" not in (r.get("AliasFlags") or "").lower():
                continue
            m = re.search(r"([0-9A-F]{8}):[^:]*:BOOK", r.get("Links") or "", re.I)
            if m:
                self.quest_object[m.group(1).upper()].append(
                    ((r.get("QuestFormID") or "").upper(), r.get("QuestEDID") or "",
                     (r.get("QuestName") or "").strip()))
        # entitlements by normalised name (the item as a Scoreboard / Atom Shop reward)
        self.entm = collections.defaultdict(list)
        p = _newest(tsv_dir, "ENTM_Export_*.tsv")
        for r in _rows(p):
            e = r.get("EDID") or ""
            if _dev(e):
                continue
            self.entm[_norm(r.get("FULL") or "")].append(e)

    def quest_of(self, ref):
        fid, edid, sig = ref
        if sig == "GMRW":
            q_edid, q_name = self.gmrw_quest.get(fid, ("", ""))
            return q_edid, q_name
        if sig == "QUST":
            return self.quest_name.get(fid, (edid, ""))
        return edid, ""


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", re.sub(r"^(plan|recipe)\s*:\s*", "", s.lower()))


def _season_word(entms):
    for e in entms:
        m = re.match(r"^SCORE_S(\d+)_", e, re.I)
        if m:
            return f"a Season {int(m.group(1))} Scoreboard reward"
    for e in entms:
        if re.match(r"^SCORE_MiniSeason_", e, re.I):
            return "a mini-season Scoreboard reward"
    for e in entms:
        if re.match(r"^ATX_", e, re.I):
            return "an Atom Shop item"
    return ""


def _live_givers(idx, fid):
    out = []
    for ref in idx.book_refs.get(fid, []):
        rfid, edid, sig = ref
        # Leveled lists were already judged by the route walk (and dead ones
        # retired by prune_dead_routes): a row with no obtain_routes has none live.
        if sig not in _GIVER_SIGS or sig == "LVLI" or not edid or _dev(edid):
            continue
        if sig in ("GMRW", "QUST"):
            q_edid, _n = idx.quest_of(ref)
            if q_edid and _dev(q_edid):
                continue
        out.append(ref)
    return out


def _teaches(c):
    """Does reading the plan for this recipe do anything in game?"""
    if _dev(c["edid"]):
        return False
    if c["cnam"]:
        return True
    return any(sig in _PROXY_USER_SIGS and not _dev(e) for _f, e, sig in c["refs"])


def _learn_quests(idx, cobjs):
    """[(kind, quest edid, quest name)] for live quests that hand out / hold a recipe."""
    out = []
    for c in cobjs:
        if _dev(c["edid"]):
            continue
        for ref in c["refs"]:
            rfid, edid, sig = ref
            if sig not in ("GMRW", "QUST") or _dev(edid):
                continue
            q_edid, q_name = idx.quest_of(ref)
            if not q_edid or _dev(q_edid) or not plan_sources.usable_quest_name(q_name or ""):
                continue
            out.append(("reward" if sig == "GMRW" else "script", q_edid, q_name or q_edid))
    # rewards first, then one entry per quest
    seen, res = set(), []
    for k in sorted(out, key=lambda t: t[0] != "reward"):
        if k[1] in seen:
            continue
        seen.add(k[1])
        res.append(k)
    return res


def _finish(it):
    it["obtain_ledger"] = plan_sources.obtain_ledger(it)
    it["source_tag"] = plan_source_pill.source_tag(it)


def attach(items, tsv_dir):
    idx = Index(tsv_dir)
    # PTS carries upcoming plans whose recipes are often not hooked up yet, so
    # "reading it teaches nothing" is not evidence there. Live only.
    pts = "pts" in os.path.normpath(os.path.abspath(tsv_dir)).lower().split(os.sep)
    stats = collections.Counter()
    if pts:
        # Kat, 31 Oct 2026: the plan check / mule lists are live only — PTS rows
        # are left exactly as the route walk made them.
        stats["skipped: PTS"] = 1
        stats["_changed"] = []
        return stats
    changed = []
    by_name = collections.defaultdict(list)
    for it in items:
        if it.get("plan_item") and not it.get("cut"):
            by_name[_norm(it.get("name") or "")].append(it)

    for it in items:
        pi = (it.get("plan_item") or {}).get("formid", "").upper()
        if not pi or it.get("cut") or it.get("recipe_only"):
            continue

        # 1. quest item: the quest puts the plan in your pack as a Quest Object
        qo = [q for q in idx.quest_object.get(pi, []) if not _dev(q[1])]
        if qo:
            names = [q[2] or q[1] for q in qo]
            sent = [f"Given to you during the quest {n} as a quest item — you read it as "
                    f"one of the quest's steps, so it can't be traded, dropped or sold."
                    for n in names]
            keep = [u for u in it.get("obtain_unlocks") or []
                    if not any(n in u for n in names)]
            it["obtain_unlocks"] = sent + keep
            it["quest_item"] = True
            it["tradeable"] = False
            _finish(it)
            stats["quest item"] += 1
            changed.append((it, "quest item"))
            continue

        # Only rows the plan check / mule lists would count: tradeable, nothing
        # resolved. A row with a real route is never touched.
        if it.get("obtain_routes") or it.get("tradeable") is not True:
            continue
        givers = _live_givers(idx, pi)
        quest_refs = [r for r in idx.book_refs.get(pi, []) if r[2] in ("GMRW", "QUST")]

        # 2. dead giver: unlocks that only come from a dev quest
        if it.get("obtain_unlocks") and not givers and quest_refs \
                and all(_dev(idx.quest_of(r)[0] or r[1]) for r in quest_refs):
            it["retired_unlocks"] = (it.get("retired_unlocks") or []) + list(it["obtain_unlocks"])
            it["obtain_unlocks"] = []
            stats["unlock from a dev quest retired"] += 1
        if it.get("obtain_unlocks"):
            continue

        cobjs = idx.cobj_by_plan.get(pi, [])

        # 3. the quest teaches the recipe itself
        lq = _learn_quests(idx, cobjs) if not givers else []
        if lq:
            sents = []
            for kind, q_edid, q_name in lq:
                if kind == "reward":
                    sents.append(f"Learned automatically as a reward for completing the quest: "
                                 f"{q_name}. The quest teaches the recipe itself — no plan is handed out.")
                else:
                    sents.append(f"Learned during the quest: {q_name}. The quest teaches the recipe "
                                 f"itself — no plan is handed out.")
            it["obtain_unlocks"] = sents
            it["obtain"] = ("There is no plan to find — a quest teaches this recipe "
                            "directly, so there is no plan item to trade.")
            it["quest_learned"] = True
            it["learn_quest"] = {"edid": lq[0][1], "name": lq[0][2]}
            it["tradeable"] = False
            _finish(it)
            stats["quest learned"] += 1
            changed.append((it, "quest learned"))
            continue

        if pi in QUEST_UNLOCK_BY_HAND:
            sent, q_edid, q_name = QUEST_UNLOCK_BY_HAND[pi]
            it["obtain_unlocks"] = [sent]
            it["obtain"] = ("There is no plan to find — finishing a quest unlocks this recipe, "
                            "so there is no plan item to trade.")
            it["quest_learned"] = True
            it["learn_quest"] = {"edid": q_edid, "name": q_name}
            it["tradeable"] = False
            _finish(it)
            stats["quest learned (hand)"] += 1
            changed.append((it, "quest learned"))
            continue

        if givers:
            continue

        cut_why = CUT_BY_HAND.get(pi)
        # 3b. every place that holds the plan is a cut record — a test chest
        # (Kat, 31 Oct 2026: anything in a test case is cut content) or a
        # zzz_/POST_/DEL_ list.
        holders = [(e, sig) for _f, e, sig in idx.book_refs.get(pi, [])
                   if sig in ("CONT", "LVLI", "NPC_", "REFR", "GMRW", "QUST", "TERM")]
        if not cut_why and holders and all(_dev(e) for e, _s in holders):
            if all(sig == "CONT" for _e, sig in holders):
                cut_why = "only in a developer test chest — nothing in the game gives it out"
            else:
                cut_why = ("its only sources are cut records ("
                           + ", ".join(sorted({e for e, _s in holders})) + ")")
        # 3c. photomode poses are never plans (Kat, 31 Oct 2026): every recipe
        # this plan unlocks makes nothing and is only checked by a pose (IDLE).
        if not cut_why and cobjs and all(
                not c["cnam"] and c["refs"] and all(sig == "IDLE" for _f, _e, sig in c["refs"])
                for c in cobjs):
            cut_why = ("photomode poses come from the Atom Shop, the Scoreboard or unlock "
                       "automatically — never from a plan, and nothing gives this one out")
        if cut_why:
            it["cut"] = True
            it["cut_reason"] = cut_why
            it["tradeable"] = False
            stats["cut"] += 1
            changed.append((it, "cut"))
            continue

        # 4. duplicate: another copy with the same name is the one that drops
        twins = [o for o in by_name.get(_norm(it.get("name") or ""), [])
                 if o is not it and (o.get("obtain_routes") or o.get("obtain_unlocks"))
                 and not o.get("cut")]
        if twins:
            keep = max(twins, key=lambda r: len(r.get("obtain_routes") or []) + len(r.get("obtain_unlocks") or []))
            keep.setdefault("merged_plans", [])
            if not any((m.get("plan_item") or {}).get("formid") == pi for m in keep["merged_plans"]):
                keep["merged_plans"].append({"name": it.get("name"), "plan_item": it.get("plan_item"),
                                             "cobj": it.get("cobj")})
            it["cut"] = True
            it["cut_reason"] = (f"duplicate of {keep['plan_item']['formid']} — the same plan in game; "
                                f"that copy is the one that drops, nothing gives this one out")
            it["plan_duplicate"] = True
            it["tradeable"] = False
            stats["duplicate"] += 1
            changed.append((it, "duplicate"))
            continue

        # 5. unused: nothing gives it, and reading it would teach nothing -> cut
        if not pts and not any(_teaches(c) for c in cobjs):
            what = _season_word(idx.entm.get(_norm(it.get("name") or ""), []))
            why = ("no recipe uses it" if not cobjs else
                   "the only recipe it unlocks is never used by anything")
            text = ("Not obtainable. Nothing in the current game files gives this plan out, and "
                    f"reading it would teach nothing ({why}).")
            dead_q = sorted({idx.quest_of(r) for r in quest_refs if _dev(idx.quest_of(r)[0] or r[1])})
            if dead_q:
                text += (" Its only giver is a quest marked cut in the game files: "
                         + ", ".join(f"{n or e} ({e})" for e, n in dead_q) + ".")
            if what:
                text += f" The item itself is {what}, not a plan."
            # Kat, 31 Oct 2026: not a real plan, so off the pages and the lists.
            it["obtain"] = text
            it["cut"] = True
            it["cut_reason"] = "not a real plan — " + text[len("Not obtainable. "):]
            it["unused_plan"] = True
            it["tradeable"] = False
            _finish(it)
            stats["unused"] += 1
            changed.append((it, "unused"))
            continue
        stats["still no source (not currently dropping)"] += 1

    stats["_changed"] = changed
    return stats


def report(stats, stream=None):
    stream = stream or sys.stdout
    changed = stats.pop("_changed", [])
    print("  " + ", ".join(f"{k}: {v}" for k, v in sorted(stats.items())), file=stream)
    for it, why in changed:
        print(f"    {why:14s} {it.get('name')} [{(it.get('plan_item') or {}).get('formid')}]", file=stream)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--data-dir", default=os.path.join(os.path.dirname(HERE), "tsv"))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    for path in a.files:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        print(f"[plan-learn-routes] {path}")
        report(attach(doc.get("items") or [], a.data_dir))
        if not a.dry_run:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
