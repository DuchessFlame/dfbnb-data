"""uwa_entries.py - one How to Obtain ENTRY per thing you actually get
(Duchess, 17 Oct 2026).

A route used to hold everything from one route kind in one block:

    Drops   Ready-made weapon and its plan
    Source  Raid: Gleaming Depths - Ultracite Terror - 10.0000%

which hid that the weapon and the plan are two separate drops with two
separate chances, conditions and tradeability. The rules, all read from the
game files each build:

1. Weapon and plan are two entries. A route whose sources hand out both is
   split into a "Ready-made weapon" entry and a "Plan only" entry, each with
   only its own (list, record) paths - so each gets its own rate (uwa_rates)
   and its own Drop Conditions (attach_conditions). The split is decided by
   the path's TARGET: the plan BOOK's FormID is the plan, anything else is the
   item. Raid stages hold the plan under the stage reward list
   (RD01_LLS_Raids_Rewards_Enc02_Weapons_DrillFist); those (stage list, plan)
   paths are added here so the plan entry is rated too.

2. One source per line. "Opening any Mutated Package or Mutated Party Pack"
   becomes one line per package (Mutated Package, Double Mutated Package,
   Mutated Party Pack, Double Mutated Party Pack), each rated from the list
   that package's magic effect rolls (MGEF -> LVLI), not from the shared pool.

3. A plan entry's tradeability is the plan BOOK's own keywords
   (build_plan_obtain_json.resolve_tradeable - one rule, not a copy). The
   Gleaming Depths and stamp-vendor plans carry NonPlayerTradable.

4. Quest crafting unlocks. A recipe whose unlock is
   recipe_Dummy_LearnedViaScriptRecipes and that a live quest references is
   granted by that quest (co_MoM_BladeofBastet <- MoM02B "Forging a Legend").
   That is its own entry: "Unlocks crafting (Weapon Workbench)".

5. Text the reader needs in game (final pass, after rates):
   - the Plan line is the plan's exact in-game name, "Plan: Circuit Breaker",
     so it can be matched against a vendor's stock as written;
   - a Scoreboard source names the season: "The Big Score (Season 15
     Scoreboard)", from tsv/fallout76_seasons.tsv (the table every other
     builder uses). A season not in the table keeps "Season N Scoreboard".

An entry is a normal route object; entries of one route sit next to each
other in obtainRoutes with the same "route" name. The renderer draws them in
one How to Obtain row, one block each.
"""
import csv
import os
import re

import uwa_obtain
from build_plan_obtain_json import resolve_tradeable

_SRC = "Source: "
_PKG = re.compile(r"^Source: Opening an?y? ", re.I)
_BENCHES = {"workbench_crafting_weapon": "Weapon Workbench",
            "workbench_crafting_armor": "Armor Workbench",
            "workbench_crafting_tinker": "Tinker's Workbench"}
SCRIPT_RECIPE = "recipe_Dummy_LearnedViaScriptRecipes"


def _f(v):
    v = str(v or "").strip().strip('"').split(":")[0]
    return v.upper().zfill(8)[-8:] if v else ""


def _q(v):
    return str(v or "").strip().strip('"').strip()


# ---------------------------------------------------------------- packages
def _a(word):
    return "an" if re.match(r"^[AEIOUaeiou]", word or "") else "a"


def _package_tops(idx, list_fid, depth=5):
    """{package name: {list the package's magic effect rolls}} walking up from
    list_fid - the same walk as ObtainIndex._packages, keeping the list."""
    out, seen, todo = {}, set(), [(list_fid, 0)]
    while todo:
        lf, d = todo.pop(0)
        if lf in seen or d > depth:
            continue
        seen.add(lf)
        for ref in sorted(idx.parents.get(lf, ())):
            ed, _, sig = ref.rpartition(":")
            if uwa_obtain.DEAD_REWARD.search(ed) or uwa_obtain.ROSTER_LIST.search(ed):
                continue
            if sig == "MGEF":
                for nm in idx.mgef_alch.get(ed, ()):
                    out.setdefault(nm, set()).add(lf)
            elif sig == "LVLI" and ed in idx.list_fid:
                todo.append((idx.list_fid[ed], d + 1))
    return out


def split_packages(r, idx):
    lp = r.get("_linePaths") or {}
    out, changed = [], False
    for ln in r.get("lines") or []:
        if not _PKG.match(ln) or not lp.get(ln):
            out.append(ln)
            continue
        per = {}
        for lst, tgt in lp[ln]:
            if not lst:
                continue
            for nm, tops in _package_tops(idx, _f(lst)).items():
                for top in tops:
                    p = [top, tgt]
                    if p not in per.setdefault(nm, []):
                        per[nm].append(p)
        if not per:
            out.append(ln)
            continue
        rest = [p for p in lp.pop(ln)
                if not any([_f(p[0]), p[1]] in v or p in v for v in per.values())
                and not _package_tops(idx, _f(p[0]))]
        for nm in sorted(per, key=uwa_obtain.package_order):
            new = f"Source: Opening {_a(nm)} {nm}"
            out.append(new)
            lp.setdefault(new, []).extend(per[nm])
        if rest:
            # a way into the pool no package rolls - keep it on the old line
            # rather than lose it (and its rate) without a word
            out.append(ln)
            lp[ln] = rest
        changed = True
    r["lines"] = out
    if changed:
        r["_linePaths"] = lp
    return changed


# ---------------------------------------------------------------- weapon / plan
class Books:
    def __init__(self, pick, read_tsv):
        self.trade, self.full = {}, {}
        p = pick("BOOK_Export_*.tsv", exclude="Locations")
        for r in read_tsv(p) if p else []:
            fid = _f(r.get("FormID"))
            kws = " ".join(_q(v) for k, v in r.items() if k and re.match(r"^KW\d+$", k))
            self.trade[fid] = resolve_tradeable(kws)
            self.full[fid] = _q(r.get("FULL"))


def _plan_fids(it, labels):
    out = {_f(it.get("planFormId"))} - {""}
    names = [it.get("name")] + list(it.get("aka") or []) + \
        [re.sub(r"^\s*Plan:\s*", "", str(it.get("planName") or ""), flags=re.I)]
    from uwa_route_labels import _norm
    for n in names:
        if n:
            out |= labels.book.get(_norm(n), set())
    return out


def _new_entry(r, lines, paths, lp, tradeable):
    e = {k: v for k, v in r.items() if k not in ("lines", "paths", "_linePaths")}
    e.update(lines=lines, populated=bool(lines), tradeable=tradeable)
    if paths:
        e["paths"] = paths
    if lp:
        e["_linePaths"] = lp
    return e


def split_weapon_plan(it, r, labels, books, plan_fids, log_hit):
    """Return a list of entries for route r (just [r] when nothing to split)."""
    lines = list(r.get("lines") or [])
    lp = r.get("_linePaths") or {}
    drops = next((l for l in lines if l.startswith("Drops:")), "")
    plan_line = next((l for l in lines if l.startswith("Plan:")), "")
    made = "armour" if str(it.get("kind") or "").lower().startswith("armour") else "weapon"

    # raid stages: the plan sits under the stage's reward list
    added = {}
    for ln in lines:
        if not ln.startswith(_SRC) or ln[8:] not in labels.raid:
            continue
        _new, tops = labels.raid[ln[8:]]
        for pf in plan_fids:
            for top in sorted(tops):
                if labels._under([top], pf):
                    added.setdefault(ln, []).append([top, pf])

    def kind_of(tgt):
        return "plan" if _f(tgt) in plan_fids else "item"

    per = {}                           # source line -> {"item": [...], "plan": [...]}
    for ln in lines:
        if not ln.startswith(_SRC):
            continue
        k = {"item": [], "plan": []}
        for p in lp.get(ln, []):
            k[kind_of(p[1])].append(p)
        for p in added.get(ln, []):
            if p not in k["plan"]:
                k["plan"].append(p)
        per[ln] = k

    both_says = "and its plan" in drops
    has_item = any(v["item"] for v in per.values())
    has_plan = any(v["plan"] for v in per.values())
    if not (both_says or (has_item and has_plan) or (added and has_item)):
        if drops.startswith("Drops: Plan only"):
            t = _book_trade(books, plan_fids)
            if t is not None:
                r["tradeable"] = t
        return [r]

    plan_name = plan_line[6:].strip() if plan_line else \
        re.sub(r"^\s*Plan:\s*", "", str(it.get("planName") or ""), flags=re.I).strip()
    item_lines, plan_lines, extra = [f"Drops: Ready-made {made}"], ["Drops: Plan only"], []
    item_lp, plan_lp = {}, {}
    for ln in lines:
        if ln.startswith("Drops:") or ln.startswith("Plan:"):
            continue
        if not ln.startswith(_SRC):
            extra.append(ln)
            continue
        k = per[ln]
        unknown = not k["item"] and not k["plan"]
        if k["item"] or (unknown and not drops.startswith("Drops: Plan only")):
            item_lines.append(ln)
            if k["item"]:
                item_lp[ln] = k["item"]
        if k["plan"] or (unknown and both_says):
            plan_lines.append(ln)
            if k["plan"]:
                plan_lp[ln] = k["plan"]
    if plan_name:
        plan_lines.append(f"Plan: {plan_name}")
    item_lines += extra

    paths = r.get("paths") or []
    item_paths = [p for p in paths if kind_of(p[1]) == "item"]
    plan_paths = [p for p in paths if kind_of(p[1]) == "plan"]
    for ps in added.values():
        plan_paths += [p for p in ps if p not in plan_paths]

    out = []
    if len(item_lines) > 1 and any(l.startswith(_SRC) for l in item_lines):
        out.append(_new_entry(r, item_lines, item_paths, item_lp, r.get("tradeable")))
    else:
        plan_lines += extra
    if any(l.startswith(_SRC) for l in plan_lines):
        t = _book_trade(books, plan_fids)
        out.append(_new_entry(r, plan_lines, plan_paths, plan_lp,
                              r.get("tradeable") if t is None else t))
    if not out:
        return [r]
    log_hit(f"{it.get('name')} ({r.get('route')}): {len(out)} entries")
    return out


def _book_trade(books, plan_fids):
    vals = {books.trade.get(f) for f in plan_fids} - {None}
    return vals.pop() if len(vals) == 1 else None


def split(items, idx, labels, books, log=print):
    """Before attach_conditions: packages one per line, weapon/plan apart."""
    pk, hits = [], []
    for it in items:
        plan_fids = _plan_fids(it, labels)
        new = []
        for r in it.get("obtainRoutes") or []:
            if not r.get("populated"):
                new.append(r)
                continue
            if split_packages(r, idx):
                pk.append(it.get("name"))
            new += split_weapon_plan(it, r, labels, books, plan_fids, hits.append)
        it["obtainRoutes"] = new
    log(f"  entries - packages one per line: {len(set(pk))} item(s); "
        f"weapon/plan split: {len(hits)} route(s)")
    for h in hits[:80]:
        log(f"    {h}")


# ---------------------------------------------------------------- timed ranks
# Daily Ops hands out its chase lists by clear time: each GMRW reward row is
# gated on GetRemainingQuestTimeSeconds against a DailyOps_Timer_Tier_* GLOB
# (High 480 s / Medium 720 s / Low 960 s), and a faster clear passes every
# slower row as well (build_daily_ops_rewards_json: "rank roots are
# cumulative"). Each chase list also carries its own list-level
# GetRandomPercent gate (Tier03 100%, Tier02 10%, Tier01 5%), which rng76's
# list walk does not apply - uwa_rates multiplies it in with rng76's own
# extract_grp_chance. One "Daily Ops" line therefore becomes one line per
# rank, each with the chance for a clear at that rank.
# The rank NAMES are not in the files (the GLOBs say High/Medium/Low); they
# are the in-game names, the same table the Daily Ops page uses.
RANK_NAME = {"high": "Elder", "medium": "Paladin", "low": "Knight"}


class Timers:
    def __init__(self, pick, read_tsv):
        secs, gname = {}, {}
        p = pick("GLOB_Export_*.tsv")
        for r in read_tsv(p) if p else []:
            try:
                secs[_f(r.get("FormID"))] = float(_q(r.get("FLTV")))
                gname[_f(r.get("FormID"))] = _q(r.get("EDID"))
            except ValueError:
                pass
        self.timed = {}                    # list fid -> (rank name, seconds)
        p = pick("GMRW_Export_*.tsv")
        for r in read_tsv(p) if p else []:
            item = _q(r.get("RewardedItem"))
            if not item.endswith(":LVLI") or "GetRemainingQuestTimeSeconds" not in (r.get("Conditions") or ""):
                continue
            g = _f(_q(r.get("ConditionGlobs")))
            m = re.search(r"_Timer_Tier_(High|Medium|Low)$", gname.get(g, ""), re.I)
            if m and g in secs:
                self.timed[_f(item)] = (RANK_NAME[m.group(1).lower()], secs[g])


def _ancestors(idx, lf, depth=8):
    seen, todo = set(), [(lf, 0)]
    while todo:
        f, d = todo.pop(0)
        if f in seen or d > depth:
            continue
        seen.add(f)
        for ref in idx.parents.get(f, ()):
            ed, _, sig = ref.rpartition(":")
            if sig == "LVLI" and ed in idx.list_fid:
                todo.append((idx.list_fid[ed], d + 1))
    return seen


def split_ranks(items, idx, timers, log=print):
    hits = []
    for it in items:
        for r in it.get("obtainRoutes") or []:
            lp = r.get("_linePaths") or {}
            out = []
            for ln in r.get("lines") or []:
                ps = lp.get(ln) or []
                comps = {}
                for lst, tgt in ps:
                    if not lst:
                        continue
                    for a in _ancestors(idx, _f(lst)):
                        if a in timers.timed:
                            # one roll per timed list, whichever path named it
                            comps.setdefault((a, tgt) if not any(k[0] == a for k in comps) else
                                             next(k for k in comps if k[0] == a), timers.timed[a])
                if not ln.startswith(_SRC) or not comps:
                    out.append(ln)
                    continue
                ranks = sorted({v for v in comps.values()}, key=lambda v: v[1])
                lp.pop(ln)
                for name, sec in ranks:
                    m, s_ = divmod(int(sec), 60)
                    new = f"{ln}, {name} rank (cleared in {m}:{s_:02d} or less)"
                    out.append(new)
                    lp[new] = ps
                    r.setdefault("_combos", {})[new] = [
                        [a, tgt] for (a, tgt), (_n, t) in comps.items() if t >= sec]
                hits.append(f"{it.get('name')}: {ln[8:]}")
            r["lines"] = out
    log(f"  timed ranks (Daily Ops) - lines split by rank: {len(hits)}")
    for h in hits[:40]:
        log(f"    {h}")


# ---------------------------------------------------------------- quest crafting
def quest_craft_entries(items, idx, log=print):
    """After attach_conditions: a recipe a quest's script grants is an entry."""
    rows = uwa_obtain._rows(idx._pick("COBJ_Export_*.tsv"))
    by_cnam = {}
    for r in rows:
        if _q(r.get("GNAM_EDID")) != SCRIPT_RECIPE:
            continue
        quests = [_q(v) for k, v in r.items() if k and k.startswith("Ref_") and _q(v).endswith(":QUST")]
        if not quests:
            continue
        bench = _BENCHES.get(_q(r.get("BNAM_EDID")).lower())
        if not bench:
            continue
        by_cnam.setdefault(_f(r.get("CNAM_FormID")), []).append((bench, quests))
    done = []
    for it in items:
        recs = {_f(x) for x in [it.get("formId")] + list(it.get("recordIds") or []) if x}
        for f in sorted(recs):
            for bench, quests in by_cnam.get(f, []):
                for q in quests:
                    qed = q.split(":")[1] if q.count(":") >= 2 else q
                    if uwa_obtain.DEAD_REWARD.search(qed):
                        continue
                    name = idx.quest_name.get(qed) or idx._quest_name(qed)
                    if not name:
                        continue
                    lines = [f"Drops: Unlocks crafting ({bench})", f"Source: {name}"]
                    routes = it["obtainRoutes"]
                    if any(e.get("lines") == lines for e in routes):
                        continue
                    e = {"route": "Quests", "populated": True, "lines": lines,
                         "tradeable": it.get("tradeable"), "dropRate": "",
                         "conditions": [], "conditionsChecked": True}
                    _insert(routes, e)
                    done.append(f"{it.get('name')}: {name}")
    log(f"  quest crafting unlocks: {len(done)} {done}")


def _insert(routes, e):
    """Put an entry after the last entry of its route; replace an empty one."""
    at = [i for i, r in enumerate(routes) if r.get("route") == e["route"]]
    if not at:
        routes.append(e)
        return
    if len(at) == 1 and not routes[at[0]].get("populated"):
        routes[at[0]] = e
        return
    routes.insert(at[-1] + 1, e)


# ---------------------------------------------------------------- final text
def load_seasons(repo_root):
    out = {}
    p = os.path.join(repo_root, "tsv", "fallout76_seasons.tsv")
    try:
        with open(p, encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh, delimiter="\t"):
                try:
                    out[int(_q(r.get("SeasonNumber")))] = _q(r.get("SeasonName"))
                except ValueError:
                    pass
    except OSError:
        pass
    return out


def final_text(items, seasons, log=print):
    n_plan = n_season = 0
    sea = re.compile(r"^(Source: )Season (\d+) Scoreboard$")
    note = re.compile(r"(Season (\d+) Scoreboard reward)")
    for it in items:
        for r in it.get("obtainRoutes") or []:
            out = []
            for ln in r.get("lines") or []:
                m = re.match(r"^Plan:\s*(.+)$", ln)
                if m and not re.match(r"^\s*Plan:", m.group(1), re.I):
                    ln = f"Plan: Plan: {m.group(1).strip()}"
                    n_plan += 1
                m = sea.match(ln)
                if m and seasons.get(int(m.group(2))):
                    ln = f"{m.group(1)}{seasons[int(m.group(2))]} (Season {m.group(2)} Scoreboard)"
                    n_season += 1
                elif ln.startswith("Note:"):
                    m2 = note.search(ln)
                    if m2 and seasons.get(int(m2.group(2))) and seasons[int(m2.group(2))] not in ln:
                        ln = ln.replace(m2.group(1), f"{m2.group(1)} ({seasons[int(m2.group(2))]})")
                        n_season += 1
                out.append(ln)
            r["lines"] = out
    log(f"  final text - plan names: {n_plan}, season names: {n_season}")
