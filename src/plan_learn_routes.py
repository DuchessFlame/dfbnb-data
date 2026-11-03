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
Old copies can still be in players' hands (e.g. Laser Chainsaw Paint),
so those stay tradeable.

KAT'S CALLS (data/plan_status.json, 1 Nov 2026)
  Every plan left with no source is the Duchess's to decide, one at a time in Mia's To
  finish list; Brewmaster commits her answer to data/plan_status.json and
  apply_status() below reads it: unreleased -> cut; legacy -> kept, never cut;
  bugged / leave -> unchanged, status_checked true. A tradeable plan with no
  source and no entry gets status_checked false ("not checked yet").

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
    # condition GetQuestCompleted(P01C_Tadpole). Duchess, 31 Oct 2026: quest reward
    # after the Tadpole and Possum badges.
    "0046A36C": ("Unlocked when you complete the quest: The Order of the Tadpole "
                 "(the backpack recipe is known once the quest is done — no plan is handed out).",
                 "P01C_Tadpole", "The Order of the Tadpole"),
}
# the Duchess's calls on plans with no source live in ONE file, data/plan_status.json
# (1 Nov 2026 — it replaced the CUT_BY_HAND table that used to sit here). Mia
# asks her about each plan one at a time; Brewmaster writes her answer there.
#   unreleased -> cut, not tradeable, cut_reason "never released (Duchess, <date>)"
#   legacy     -> stays (tradeable as the keywords say), legacy: true, obtain
#                 "No longer drops — old copies can still be traded." Never cut.
#   bugged     -> stays as it is (tradeable, no source), status_checked: true
#   leave      -> no change (status_checked: true)
#   learnt     -> (2 Nov 2026) the item IS in the game, but you learn it from a quest
#                 or know it from the start: no plan to find. Not tradeable (off the
#                 plan check and mule lists), but NOT cut, so the item stays on the
#                 site with the note as its "how to obtain" line. The zzz_/CUT_/POST_
#                 rule still wins: a plan the files mark cut stays cut.
#   cut        -> (2 Nov 2026) cut content: cut, not tradeable. A file rule's own
#                 cut reason is kept; otherwise "cut (Duchess, <date>) — <note>".
#   no entry   -> a tradeable plan with no source gets status_checked: false
STATUS_PATH = os.path.join(os.path.dirname(HERE), "data", "plan_status.json")
STATUSES = ("bugged", "unreleased", "legacy", "leave", "learnt", "cut")
LEGACY_OBTAIN = "No longer drops — old copies can still be traded."
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def load_status(path=None):
    """{BOOK FormID: entry} from data/plan_status.json. Missing / broken file -> {}
    (a bad file must never stop a build; the build log says so)."""
    path = path or STATUS_PATH
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as exc:                      # noqa: BLE001
        print(f"  WARNING: {path} unreadable ({exc}) — plan statuses ignored", file=sys.stderr)
        return {}
    out = {}
    for k, v in (doc or {}).items():
        if k.startswith("_") or not isinstance(v, dict):
            continue
        if v.get("status") in STATUSES:
            out[k.upper()] = v
    return out


def _kat_date(iso):
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", iso or "")
    if not m:
        return iso or "date not given"
    return f"{int(m.group(3))} {_MONTHS[int(m.group(2)) - 1]} {m.group(1)}"


def unreleased_reason(entry):
    return f"never released ({entry.get('by') or 'Duchess'}, {_kat_date(entry.get('decided'))})"


def _no_source(it):
    return not it.get("obtain_routes") and not it.get("obtain_unlocks")


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


def apply_status(items, status, stats, changed):
    """the Duchess's plan_status.json calls, applied to every row they name (and the
    status_checked flag on every other tradeable plan with no source). Runs
    after the rules above and is safe to re-run over a finished file: it only
    ever sets the same fields to the same values."""
    for it in items:
        pi = (it.get("plan_item") or {}).get("formid", "").upper()
        if not pi or it.get("recipe_only"):
            continue
        entry = status.get(pi)
        # re-worked out every run (Duchess, 1 Nov: "the code will always check it in case it moves to CUT or a
        # source gets added"), so a flag clears itself when it stops being true
        for k in ("plan_status_outdated", "cut_in_files"):
            it.pop(k, None)
        if not entry:
            # nobody has looked at this one yet: a tradeable plan, not cut,
            # with no route and no unlock — "not checked yet" on the bugged page
            if not it.get("cut") and it.get("tradeable") is True and _no_source(it):
                if it.get("status_checked") is not False:
                    it["status_checked"] = False
                    changed.append((it, "not checked yet"))
                stats["not checked yet"] += 1
            continue
        st = entry["status"]
        if st in ("learnt", "cut"):
            # these two are about the plan item, not its sources: a quest unlock or
            # a challenge route doesn't make them outdated
            note = entry.get("note") or ""
            it["plan_status"] = st
            it["status_checked"] = True
            it["tradeable"] = False
            if st == "cut":
                if not it.get("cut"):
                    it["cut_reason"] = (f"cut ({entry.get('by') or 'Duchess'}, {_kat_date(entry.get('decided'))})"
                                        + (f" — {note}" if note else ""))
                it["cut"] = True
            else:
                # "default" = known from the start (Brewmaster words it that way); else learnt from a quest
                it["learnt"] = "default" if note.lower().startswith("known by default") else "quest"
                if not _dev((it.get("plan_item") or {}).get("edid")) and not it.get("plan_duplicate"):
                    # an earlier rule may have cut it as "teaches nothing" — the item is real
                    if it.get("cut"):
                        it["cut"] = False
                        it.pop("cut_reason", None)
                    it.pop("unused_plan", None)
                if not it.get("obtain_unlocks") and note:
                    it["obtain_unlocks"] = [note[0].upper() + note[1:] + "."]
                    it["obtain"] = ("There is no plan to find — you already know this recipe, "
                                    "so there is no plan item to trade.")
                _finish(it)
            stats[f"{st} (Duchess)"] += 1
            changed.append((it, st))
            continue
        if not _no_source(it):
            # a source turned up since Duchess decided — the decision no longer
            # applies. The row is left exactly as the route walk made it (a real
            # plan with its routes), flagged so Mia asks Duchess about it again.
            it.pop("plan_status", None)
            it["plan_status_outdated"] = {"was": st, "decided": entry.get("decided") or "",
                                          "now": "has a source"}
            stats[f"{st} but has a source now"] += 1
            changed.append((it, f"{st}: has a source now"))
            continue
        if st == "unreleased" and _dev((it.get("plan_item") or {}).get("edid")):
            # the game files now mark it cut too (zzz_/CUT_/DEL_/POST_...)
            it["cut_in_files"] = True
            stats["unreleased and now cut in the files"] += 1
        it["plan_status"] = st
        it["status_checked"] = True
        if st == "unreleased":
            why = unreleased_reason(entry)
            # a file rule that already cut it (test chest, POST_ list...) keeps
            # its own reason; the Duchess's earlier hand reasons are replaced
            if not it.get("cut") or re.search(r"\(\w+, \d{1,2} \w{3} \d{4}\)", it.get("cut_reason") or ""):
                it["cut_reason"] = why
            it["cut"] = True
            it["tradeable"] = False
            stats["unreleased (Duchess)"] += 1
            changed.append((it, "unreleased"))
        elif st == "legacy":
            it["legacy"] = True
            it["obtain"] = LEGACY_OBTAIN
            stats["legacy (Duchess)"] += 1
            changed.append((it, "legacy"))
        else:
            stats[f"{st} (Duchess)"] += 1
            changed.append((it, st))


def attach(items, tsv_dir, status=None):
    idx = Index(tsv_dir)
    status = load_status() if status is None else status
    # PTS carries upcoming plans whose recipes are often not hooked up yet, so
    # "reading it teaches nothing" is not evidence there. Live only.
    pts = "pts" in os.path.normpath(os.path.abspath(tsv_dir)).lower().split(os.sep)
    stats = collections.Counter()
    if pts:
        # Duchess, 31 Oct 2026: the plan check / mule lists are live only — PTS rows
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

        st = (status.get(pi) or {}).get("status")
        if st == "legacy":
            # Duchess: legacy plans are never cut, whatever the rules below say
            stats["kept: legacy (Duchess)"] += 1
            continue
        cut_why = unreleased_reason(status[pi]) if st == "unreleased" else None
        # 3b. every place that holds the plan is a cut record — a test chest
        # (Duchess, 31 Oct 2026: anything in a test case is cut content) or a
        # zzz_/POST_/DEL_ list.
        holders = [(e, sig) for _f, e, sig in idx.book_refs.get(pi, [])
                   if sig in ("CONT", "LVLI", "NPC_", "REFR", "GMRW", "QUST", "TERM")]
        if not cut_why and holders and all(_dev(e) for e, _s in holders):
            if all(sig == "CONT" for _e, sig in holders):
                cut_why = "only in a developer test chest — nothing in the game gives it out"
            else:
                cut_why = ("its only sources are cut records ("
                           + ", ".join(sorted({e for e, _s in holders})) + ")")
        # 3c. photomode poses are never plans (Duchess, 31 Oct 2026): every recipe
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
            # Duchess, 31 Oct 2026: not a real plan, so off the pages and the lists.
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

    apply_status(items, status, stats, changed)
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
