#!/usr/bin/env python3
"""
build_bugged_plans_json.py — Current Bugged Plans
=================================================
Page: /df/data-mining/current-bugged-plans/   Output: dist/bugged_plans.json

Walks EVERY plan BOOK, EVERY scrap-to-learn recipe and EVERY leveled list that
touches a plan, and lists the plans the game data says are broken. Nothing is
hardcoded — no plan names, no FormIDs, no list names. Re-run it on each new
export and the page follows the game.

WHAT COUNTS AS BUGGED
---------------------
Every check is a structural fact read from the exports, never a guess from a
name. Each one is the generalised form of a bug already confirmed by hand:

  orphan      A leveled list holding the plan is connected to nothing — no
              quest reward, NPC, container, vendor or world placement ever rolls
              it, directly or through a parent list.
              (Tunnel of Love: E09C_LL_LoveTunnel_QuestReward_Recipes has 0 refs.)
  shadowed    In a First Match list the plan's entry can never win the roll: the
              entries above it already claim every number its GetRandomPercent
              condition needs. Uses rng76.first_match_rates — the same cascade
              the drop-rate pages use — so the two can never disagree.
              (Resolve Breaker Paint: <= 5 under an earlier <= 10.)
  never_rolls The entry's roll condition can never pass (GetRandomPercent <= 0),
              it is set to a literal 100% chance of nothing, or its quantity is 0.
  no_recipe   A live plan that no recipe (COBJ) points at — reading it teaches
              nothing.
  wrong_tier  The plan names one tier (Standard/Treated/.../Shielded, Light/
              Sturdy/Heavy, Mk N) and the recipe it unlocks makes another.
  scrap_cut   A scrap-to-learn mod (COBJ creates an OMOD, GNAM is a WEAP/ARMO)
              whose weapon/armour is cut, so there is nothing to scrap.

SEVERITY
  unobtainable  every route is broken and nothing else gives the plan out
  route_broken  one route is broken but the plan is still obtainable elsewhere

WHAT IS NOT A BUG
  * Cut content (plan_sources.cut_reason — dev prefix AND no live reference).
  * Lists whose own EditorID is editor-only (DEL_/zzz/Test/Babylon...).
  * A roll gated by a GLOBAL that is currently 0/100. Those are Bethesda's
    on/off switches for seasonal events; flagging them would call every event
    "bugged" between runs. Counted in the stats, never listed.
  * Anything granted purely by a Papyrus script — the exports can't see it.
    That caveat is printed on the page.

Does not touch rng76 (read-only import) — see plan-obtain-pipeline skill.

YOUR REVIEW FILE  (data/bugged_plans_review.json)
-------------------------------------------------
Accuracy lever. Mark a result once and every future run remembers it:

    { "0068B8F0": {"status": "confirmed", "note": "tested 23 Sep, never drops"},
      "00825AA7": {"status": "not_a_bug", "note": "challenge auto-unlocks it"} }

  confirmed  -> shows a "Confirmed in game" pill; safe to send to Bethesda
  not_a_bug  -> hidden from the page for good (still counted in stats)
Keys are the row "id" (the plan BOOK FormID, or the COBJ FormID for scrap rows).

HISTORY
-------
The previous dist/bugged_plans.json is read before it is overwritten, so each
row carries `first_seen`, rows that weren't there last run get `is_new`, and
rows that disappeared are listed under `fixed` (kept 3 runs).

CHECKLIST CROSS-CHECK
---------------------
Reads dist/plan_master.json and lists checklist routes whose leveled list is
never rolled (e.g. Ghillie Suit -> Minerva Backlog_02 at 100%). Written to
`checklist_dead_routes` for fixing the plan pages; not rendered.

Usage:
  python src/build_bugged_plans_json.py                         # live
  python src/build_bugged_plans_json.py --channel pts --previous /tmp/prev_pts.json   # PTS build
  python src/build_bugged_plans_json.py --data-dir tsv/pts --out dist/bugged_plans_pts.json
"""
import argparse, collections, csv, datetime, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
csv.field_size_limit(1 << 30)

import tsv_source                      # noqa: E402
import rng76                           # noqa: E402
import plan_sources                    # noqa: E402
import plan_unlocks                    # noqa: E402

RX_REF_COL = re.compile(r"^Ref_?\d+$")
RX_COND_COL = re.compile(r"^Cond\d+$")
RX_DEV = plan_sources._RX_DEV_RECORD   # babylon/zw/test/qa/debug/cut/del/post/template
RECIPE_KW = "ObjectTypeRecipe"
# An orphaned list whose EditorID says it is a holding pen is parked stock that
# Bethesda never marked cut (e.g. Minerva's ..._GoldVendor_Backlog_02, 0 refs since
# at least July 2026). Those are reported in their own "cut but not zzz'd" section,
# never as a plan bug — the plan was retired, not broken.
RX_PARKED = re.compile(r"backlog|(^|_)temp(_|$)|unused|placeholder", re.I)


# ── io ───────────────────────────────────────────────────────────────────────
def newest(pat, root):
    return tsv_source.newest(os.path.join(root, pat), required=False)


def rows(path):
    """(dict_of_scalar_cols, refs_list, conds_list) per row. Ref*/Cond* columns
    are collected positionally so the 5,000-column exports stay cheap."""
    if not path:
        return
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
        r = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        head = next(r)
        ref_ix = [i for i, h in enumerate(head) if RX_REF_COL.match(h)]
        cond_ix = [i for i, h in enumerate(head) if RX_COND_COL.match(h)]
        scal_ix = [i for i in range(len(head)) if i not in set(ref_ix) | set(cond_ix)]
        for row in r:
            d = {head[i]: (row[i] if i < len(row) else "") for i in scal_ix}
            refs = [row[i] for i in ref_ix if i < len(row) and row[i].strip()]
            conds = [row[i] for i in cond_ix if i < len(row) and row[i].strip()]
            yield d, refs, conds


def split_ref(ref):
    """'00664D60:QuestReward_E09C_LoveTunnel_Stage9000_01:GMRW' -> (fid, edid, sig)."""
    bits = (ref or "").strip().split(":")
    if len(bits) < 3:
        return (bits[0].upper() if bits else "", "", "")
    return bits[0].strip().upper(), ":".join(bits[1:-1]).strip(), bits[-1].strip().upper()


def is_dev(edid):
    e = (edid or "").strip()
    return bool(e) and (plan_sources.is_dev_record(e) or bool(RX_DEV.search(e)))


def fnum(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


# ── tier words (wrong_tier) ──────────────────────────────────────────────────
TIER_SETS = [
    ["standard", "treated", "resistant", "protective", "shielded"],
    ["light", "sturdy", "heavy"],
]
RX_MK = re.compile(r"\bmk\s*([ivx]+|\d+)\b", re.I)
ROMAN = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6}


def tier_marks(text):
    t = (text or "").lower()
    words = set(re.findall(r"[a-z]+", t))
    out = {}
    for n, tiers in enumerate(TIER_SETS):
        hit = [w for w in tiers if w in words]
        if len(hit) == 1:
            out[f"set{n}"] = hit[0]
    m = RX_MK.search(t)
    if m:
        v = m.group(1).lower()
        out["mk"] = str(ROMAN.get(v, v))
    return out


# ── main scan ────────────────────────────────────────────────────────────────
class Scan:
    def __init__(self, root):
        self.root = root
        self.files = {}
        self.stats = collections.Counter()
        self.data = rng76.Rng76Data.from_tsv_root(root)
        self.res = self.data.resolver
        self.quests = plan_sources.QuestNames(newest("QUEST_Export_*.tsv", root))
        self.unlocks = plan_unlocks.RecipeUnlocks(root, lambda pat, r: newest(pat, r))
        self.parked = collections.defaultdict(set)
        self._load()

    def _f(self, key, pat):
        p = newest(pat, self.root)
        self.files[key] = os.path.basename(p) if p else None
        return p

    # ---------------------------------------------------------------- load
    def _load(self):
        # sigs for scrap-to-learn
        self.sig, self.full, self.edid = {}, {}, {}
        for key, pat, sig, fcol, ecol, ncol in (
            ("WEAP", "WEAP_Export_*_Base.tsv", "WEAP", "WEAP_FormID", "WEAP_EDID", "WEAP_FULL"),
            ("ARMO", "ARMO_Export_*ARMOUR.tsv", "ARMO", "ARMO_FormID", "ARMO_EDID", "ARMO_FULL"),
            ("OMOD", "OMOD_Export_*.tsv", "OMOD", "OMOD_FormID", "OMOD_EDID", "FULL"),
        ):
            for d, _, _ in rows(self._f(key, pat)):
                fid = (d.get(fcol) if fcol else (d.get("FormID") or d.get("FormId") or "")) or ""
                fid = fid.strip().upper()
                if not fid:
                    continue
                self.sig[fid] = sig
                self.edid[fid] = (d.get(ecol) if ecol else (d.get("EDID") or d.get("EditorID") or "")).strip()
                self.full[fid] = (d.get(ncol) if ncol else (d.get("FULL") or d.get("Name") or "")).strip()

        # BOOK
        self.books = {}
        for d, refs, _ in rows(self._f("BOOK", "BOOK_Export_*.tsv")):
            fid = (d.get("FormID") or "").strip().upper()
            if not fid:
                continue
            kws = " ".join(v for k, v in d.items() if k.startswith("KW"))
            self.books[fid] = {"fid": fid, "edid": (d.get("EDID") or "").strip(),
                               "full": (d.get("FULL") or "").strip(),
                               "recipe_kw": RECIPE_KW in kws,
                               "refs": [split_ref(x) for x in refs]}
            self.sig[fid] = "BOOK"

        # GMRW — a reward record nothing points at is never paid out (the Burning
        # Springs challenges teach their recipe directly via COBJ.GNAM and left the
        # old plan-book reward records behind with 0 refs, e.g. Balloon Turbine).
        self.gmrw_dead = set()
        self.gmrw_items = collections.defaultdict(set)   # GMRW -> rewarded FormIDs
        self.gmrw_quest = {}                             # GMRW -> quest EditorID
        gpath = self._f("GMRW", "GMRW_Export_*.tsv")
        if gpath:
            with open(gpath, encoding="utf-8", errors="replace") as f:
                for d in csv.DictReader(f, delimiter="\t"):
                    fid = (d.get("FormID") or "").strip().upper()
                    it = split_ref(d.get("RewardedItem") or "")
                    if fid and it[0]:
                        self.gmrw_items[fid].add(it)
                    q = split_ref(d.get("ParentQuestLink") or "")
                    if fid and q[1]:
                        self.gmrw_quest[fid] = q[1]
                    refs = [split_ref(v)[1] for k, v in d.items() if RX_REF_COL.match(k or "") and v]
                    # 0 refs, or only referenced by cut quests (ZZZNPE_MQ01_Enjoy...)
                    if fid and (not refs or all(e and is_dev(e) for e in refs)):
                        self.gmrw_dead.add(fid)

        # COBJ
        self.cobj = {}
        self.cobj_by_gnam = collections.defaultdict(list)
        self.cobj_by_cnam = collections.defaultdict(list)
        for d, _, _ in rows(self._f("COBJ", "COBJ_Export_*.tsv")):
            fid = (d.get("COBJ_FormID") or "").strip().upper()
            if not fid:
                continue
            c = {"fid": fid, "edid": (d.get("COBJ_EDID") or "").strip(),
                 "cnam": (d.get("CNAM_FormID") or "").strip().upper(),
                 "cnam_edid": (d.get("CNAM_EDID") or "").strip(),
                 "cnam_full": (d.get("CNAM_FULL") or "").strip(),
                 "gnam": (d.get("GNAM_FormID") or "").strip().upper(),
                 "gnam_edid": (d.get("GNAM_EDID") or "").strip(),
                 "gnam_full": (d.get("GNAM_FULL") or "").strip()}
            self.cobj[fid] = c
            if c["gnam"]:
                self.cobj_by_gnam[c["gnam"]].append(c)
            if c["cnam"]:
                self.cobj_by_cnam[c["cnam"]].append(c)

        # LVLI
        self.lvli = {}
        for d, _, conds in rows(self._f("LVLI_List", "LVLI_Export_*_LVLI_List.tsv")):
            fid = (d.get("LVLI_FormID") or "").strip().upper()
            self.lvli[fid] = {"fid": fid, "edid": (d.get("LVLI_EDID") or "").strip(),
                              "flags": rng76.parse_lvlf_flags(d.get("LVLF_Flags") or ""),
                              "cn": fnum(d.get("LVCV_ChanceNoneValue")),
                              "cn_dyn": bool((d.get("LVLG_ChanceNoneGlobal") or "").strip()
                                             or (d.get("LVCT_ChanceNoneCurve") or "").strip()),
                              "entries": [], "refs": []}
        for d, _, conds in rows(self._f("LVLI_Entries", "LVLI_Export_*_LVLI_Entries.tsv")):
            fid = (d.get("LVLI_FormID") or "").strip().upper()
            L = self.lvli.get(fid)
            if L is None:
                continue
            rfid, redid, rsig = split_ref(d.get("LVLO_Reference"))
            L["entries"].append({
                "idx": int(fnum(d.get("EntryIndex")) or 0),
                "fid": rfid, "edid": redid, "sig": rsig, "conds": conds,
                "cn": fnum(d.get("LVOV_ChanceNoneValue")),
                "cn_dyn": bool((d.get("LVOG_ChanceNoneGlobal") or "").strip()
                               or (d.get("LVOC_ChanceNoneCurve") or "").strip()),
                "qty": fnum(d.get("LVIV_Quantity")),
                "qty_dyn": bool((d.get("LVIG_QuantityGlobal") or "").strip()
                                or (d.get("LVIT_QuantityCurve") or "").strip()),
            })
        for d, refs, _ in rows(self._f("LVLI_Refs", "LVLI_Export_*_LVLI_Refs.tsv")):
            fid = (d.get("LVLI_FormID") or "").strip().upper()
            if fid in self.lvli:
                self.lvli[fid]["refs"] = [split_ref(x) for x in refs]
        for L in self.lvli.values():
            L["entries"].sort(key=lambda e: e["idx"])
        self.stats["lists_total"] = len(self.lvli)

    # ------------------------------------------------------- entry liveness
    def _grp_uses_glob(self, conds):
        return any("GetRandomPercent" in c and "[GLOB:" in c for c in conds)

    def analyse_entries(self):
        """entry['dead'] = None | {kind, ...}.  Dynamic (GLOB/CURV) causes are
        recorded as kind '*_switch' and never reported as bugs."""
        for L in self.lvli.values():
            ents = L["entries"]
            list_dead = (L["cn"] is not None and L["cn"] >= 100 and not L["cn_dyn"])
            fm = None
            if L["flags"].get("first_match") and ents:
                fm = self.res.first_match_rates([e["conds"] for e in ents])
            spans = [self.res.extract_grp_span(e["conds"]) for e in ents]
            for i, e in enumerate(ents):
                dead = None
                if list_dead:
                    dead = {"kind": "never_rolls", "why": "list_cn100"}
                elif e["cn"] is not None and e["cn"] >= 100:
                    dead = {"kind": "never_rolls_switch" if e["cn_dyn"] else "never_rolls",
                            "why": "entry_cn100"}
                elif spans[i] is not None and spans[i][1] - spans[i][0] <= 0:
                    dead = {"kind": "never_rolls_switch" if self._grp_uses_glob(e["conds"])
                            else "never_rolls", "why": "grp_zero"}
                elif fm is not None and fm[i] <= 0:
                    above = [j for j in range(i) if fm[j] > 0]
                    glob = any(self._grp_uses_glob(ents[j]["conds"]) for j in above + [i])
                    dead = {"kind": "shadowed_switch" if glob else "shadowed",
                            "why": "first_match", "above": above}
                e["dead"] = dead
                if dead:
                    self.stats["dead_entries_" + dead["kind"]] += 1

    # ------------------------------------------------------- reachability
    def _root_ref(self, sig, edid):
        """A non-LVLI reference that proves the list is actually rolled."""
        if sig == "LVLI":
            return False
        if edid and is_dev(edid):
            return False
        if sig == "COBJ" and not edid:
            return False
        return True

    def analyse_reach(self):
        live = set()
        stack = []
        for fid, L in self.lvli.items():
            if any(self._root_ref(s, e) for _, e, s in L["refs"]):
                live.add(fid)
                stack.append(fid)
        while stack:
            fid = stack.pop()
            for e in self.lvli[fid]["entries"]:
                if e["sig"] == "LVLI" and e["fid"] in self.lvli and e["fid"] not in live:
                    if not e["dead"] or e["dead"]["kind"].endswith("_switch"):
                        live.add(e["fid"])
                        stack.append(e["fid"])
        self.live = live
        self.stats["lists_live"] = len(live)

    def edge_entries(self, parent, child):
        return [e for e in self.lvli[parent]["entries"] if e["fid"] == child]

    def edge_state(self, parent, child):
        """'live' | 'switch' | ('dead', [entries])"""
        ents = self.edge_entries(parent, child)
        if not ents:
            return "live"          # referenced some other way (list condition...) — don't judge
        if any(not e["dead"] for e in ents):
            return "live"
        if any(e["dead"]["kind"].endswith("_switch") for e in ents):
            return "switch"
        return ("dead", ents)

    def why_unreached(self, fid, seen=None, depth=0):
        """Reasons a non-live list is never rolled: orphan tops + dead edges."""
        seen = seen if seen is not None else set()
        if fid in seen or depth > 25:
            return []
        seen.add(fid)
        L = self.lvli[fid]
        parents = [(f, e) for f, e, s in L["refs"] if s == "LVLI" and f in self.lvli]
        out = []
        real_parents = [(f, e) for f, e in parents if not is_dev(e)]
        for pf, _ in real_parents:
            if pf in self.live:
                st = self.edge_state(pf, fid)
                if isinstance(st, tuple):
                    out.append({"kind": "dead_edge", "parent": pf, "child": fid, "entries": st[1]})
                elif st == "switch":
                    out.append({"kind": "switch"})
            else:
                out.extend(self.why_unreached(pf, seen, depth + 1))
        if not real_parents:
            out.append({"kind": "orphan", "list": fid,
                        "dev_refs": [e for _, e, s in L["refs"] if e]})
        return out

    # ------------------------------------------------------------- naming
    def label(self, edid):
        try:
            lab = plan_sources.source_label(edid, self.quests)
        except Exception:
            lab = None
        return lab or edid

    def rec(self, fid, edid, sig):
        return {"form_id": fid, "edid": edid, "type": sig}

    # --------------------------------------------------------------- plans
    def plan_cobjs(self, book):
        seen, out = set(), []
        for c in self.cobj_by_gnam.get(book["fid"], []):
            if c["fid"] not in seen:
                seen.add(c["fid"]); out.append(c)
        for f, _, s in book["refs"]:
            if s == "COBJ" and f in self.cobj and f not in seen:
                seen.add(f); out.append(self.cobj[f])
        return out

    def other_way_to_learn(self, b, cobjs):
        """A sentence if the recipe this plan teaches is learnable without it:
        scrapping a live weapon/armour, or a non-plan GNAM on a recipe making
        the same record (challenge, script, workshop claim)."""
        for c in cobjs:
            q = self.cobj_given_directly(c["fid"])
            if q:
                return f"Recipe given directly by {self.label(q)}"
        for c in cobjs:
            for o in self.cobj_by_cnam.get(c["cnam"], []) if c["cnam"] else []:
                g = o["gnam"]
                if not g or g == b["fid"] or is_dev(o["gnam_edid"]):
                    continue
                gs = self.sig.get(g)
                if gs in ("WEAP", "ARMO") and self.full.get(g) and not is_dev(self.edid.get(g, "")):
                    return f"Learnable by scrapping a {self.full[g]}"
                if gs != "BOOK" and o["gnam_edid"] and "Uncraftable" not in o["gnam_edid"]:
                    return f"Recipe also unlocked by {o['gnam_full'] or o['gnam_edid']}"
        return None

    # ------------------------------------------------------ confidence
    @staticmethod
    def quest_code(edid):
        """E09C_LL_LoveTunnel_... -> E09C ; AC_SQ04_LL_Rewards -> AC_SQ04."""
        parts = [p for p in (edid or "").split("_") if p]
        if not parts:
            return None
        if re.search(r"\d", parts[0]):
            return parts[0]
        if len(parts) > 1 and re.search(r"\d", parts[1]) and len(parts[0]) <= 4:
            return parts[0] + "_" + parts[1]
        return None

    def orphan_is_strong(self, list_edid):
        """An unlinked list only proves something when its quest's reward record
        is visible in the files AND hands out items without it. The export can't
        see Papyrus: A Grand Reopening, Reassembly Required and every Wastelanders
        gold-bullion list have 0 references yet really pay out, because a script
        does it. Tunnel of Love's reward record lists its items and skips the pool."""
        code = self.quest_code(list_edid)
        if not code:
            return False
        ltoks = {t.lower() for t in list_edid.split("_") if t}
        for g, q in self.gmrw_quest.items():
            if g in self.gmrw_dead or not (q == code or q.startswith(code + "_")):
                continue
            # the list must name THIS quest, not just share an expansion code:
            # W05_LLV_GoldVendor_* shares "W05" with every Wastelanders quest
            # but belongs to none of them (a vendor script stocks it)
            if not {t.lower() for t in q.split("_") if t} <= ltoks:
                continue
            if self.gmrw_items.get(g):
                return True
        return False

    def cobj_given_directly(self, cobj_fid):
        for g, items in self.gmrw_items.items():
            if g in self.gmrw_dead:
                continue
            for f, e, sg in items:
                if f == cobj_fid:
                    return self.gmrw_quest.get(g) or e
        return None

    def scan_plans(self):
        out = []
        for b in self.books.values():
            if not (b["recipe_kw"] or b["fid"] in self.cobj_by_gnam
                    or re.match(r"^(plan|recipe)\s*:", b["full"], re.I)):
                continue
            self.stats["plans_scanned"] += 1
            cobjs = self.plan_cobjs(b)
            proof = next((self.unlocks.proof_of_life(c["fid"]) for c in cobjs
                          if self.unlocks.proof_of_life(c["fid"])), None)
            raw_refs = [f"{f}:{e}:{s}" for f, e, s in b["refs"]]
            alt = self.other_way_to_learn(b, cobjs)
            if plan_sources.cut_reason(b["edid"], raw_refs, recipe_unlock=proof):
                self.stats["plans_cut"] += 1
                continue
            if is_dev(b["edid"]) and not plan_sources.meaningful_refs(raw_refs) and not proof:
                self.stats["plans_cut"] += 1
                continue

            live_routes, dead_routes, switch = [], [], False
            for f, e, s in b["refs"]:
                if s == "LVLI" and f in self.lvli:
                    if is_dev(e):
                        continue
                    if f in self.live:
                        st = self.edge_state(f, b["fid"])
                        if st == "live":
                            live_routes.append(self.rec(f, e, s))
                        elif st == "switch":
                            switch = True
                        else:
                            dead_routes.append({"holder": f, "reasons": [
                                {"kind": "dead_edge", "parent": f, "child": b["fid"], "entries": st[1]}]})
                    else:
                        rs = self.why_unreached(f)
                        if any(r["kind"] == "switch" for r in rs):
                            switch = True
                        rs = [r for r in rs if r["kind"] != "switch"]
                        if rs:
                            dead_routes.append({"holder": f, "reasons": rs})
                        else:
                            switch = True
                elif s in ("COBJ",):
                    continue
                elif s == "FLST" and (not e or is_dev(e) or e.startswith("Challenge_All_Recipes")):
                    continue
                elif e and is_dev(e):
                    continue
                elif s == "GMRW" and f in self.gmrw_dead:
                    self.stats["plans_via_unreferenced_gmrw"] += 1
                    continue
                elif s == "CONT" and e and is_dev(e):
                    continue
                else:
                    live_routes.append(self.rec(f, e, s))

            bugs, severity = [], None
            if dead_routes:
                obtainable = bool(live_routes or proof or switch or alt)
                severity = "route_broken" if obtainable else "unobtainable"
                found = self.describe_dead(b, dead_routes)
                if obtainable:
                    # an orphaned list next to a working route is legacy wiring, not a
                    # player-facing bug; only real roll defects are worth listing
                    found = [x for x in found if x["kind"] != "orphan"]
                bugs.extend(found)
                if not found:
                    severity = None

            # no_recipe — only meaningful for a plan something actually gives out
            named_routes = [r for r in live_routes if r["edid"]]
            if not cobjs and (named_routes or proof) and \
                    self.unlocks.link(b["fid"], b["edid"], b["full"])[1] in ("none", "ambiguous"):
                bugs.append({"kind": "no_recipe",
                             "title": "Teaches nothing",
                             "text": "No recipe in the game files is linked to this plan, so "
                                     "reading it doesn't unlock anything."})
                severity = severity or "broken_item"

            # wrong_tier
            pm = tier_marks(b["full"])
            for c in cobjs:
                cm = tier_marks(c["cnam_full"] or c["cnam_edid"])
                clash = [k for k in pm if k in cm and pm[k] != cm[k]]
                if clash and c["gnam"] == b["fid"]:
                    bugs.append({"kind": "wrong_tier", "title": "Unlocks the wrong tier",
                                 "text": f"The plan is for {b['full'].split(':',1)[-1].strip()}, but the "
                                         f"recipe it unlocks makes {c['cnam_full'] or c['cnam_edid']}.",
                                 "records": [self.rec(c["fid"], c["edid"], "COBJ"),
                                             self.rec(c["cnam"], c["cnam_edid"], self.sig.get(c["cnam"], ""))]})
                    severity = severity or "broken_item"

            if not bugs:
                continue
            out.append({
                "id": b["fid"], "name": b["full"] or b["edid"], "kind": "plan",
                "severity": severity, "bugs": bugs,
                "still_from": (sorted({self.label(r["edid"]) for r in live_routes if r["edid"]})[:8]
                               + ([self.unlocks.sentence(proof)] if proof else [])
                               + ([alt] if alt else [])),
                "technical": {
                    "plan": self.rec(b["fid"], b["edid"], "BOOK"),
                    "recipes": [self.rec(c["fid"], c["edid"], "COBJ") for c in cobjs],
                    "creates": [self.rec(c["cnam"], c["cnam_edid"], self.sig.get(c["cnam"], ""))
                                for c in cobjs if c["cnam"]][:4],
                },
            })
        return out

    def _cond_text(self, conds):
        out = []
        for c in conds:
            p = rng76.parse_grp_condition(c)
            if p:
                sym = {5: "<=", 4: "<", 3: ">=", 2: ">", 0: "==", 1: "!="}.get(p[0], "?")
                v = self.res._grp_value(p[1])
                out.append(f"GetRandomPercent {sym} {v:g}" if v is not None else f"GetRandomPercent {sym} {p[1]}")
            else:
                out.append(re.sub(r"\(.*?\)", "", c).split()[0] if c.strip() else c)
        return out

    def describe_dead(self, b, dead_routes):
        bugs, seen = [], set()
        for dr in dead_routes:
            holder = self.lvli[dr["holder"]]
            for r in dr["reasons"]:
                if r["kind"] == "orphan" and RX_PARKED.search(self.lvli[r["list"]]["edid"]):
                    self.parked[r["list"]].add(b["fid"])
                    continue
                if r["kind"] == "orphan":
                    top = self.lvli[r["list"]]
                    key = ("orphan", top["fid"])
                    if key in seen:
                        continue
                    seen.add(key)
                    chain = [] if top["fid"] == holder["fid"] else [holder]
                    src = self.label(top["edid"])
                    bugs.append({
                        "kind": "orphan", "strong": self.orphan_is_strong(top["edid"]),
                        "title": "Reward list isn't connected to anything",
                        "text": (f"The leveled list that holds this plan ({top['edid']}) isn't referenced by any "
                                 f"quest reward, enemy, container, vendor or parent list, so nothing in the game "
                                 f"ever rolls it." + (f" It looks like it belongs to {src}." if src != top['edid'] else "")),
                        "records": [self.rec(top["fid"], top["edid"], "LVLI")]
                                   + [self.rec(x["fid"], x["edid"], "LVLI") for x in chain]})
                elif r["kind"] == "dead_edge":
                    P = self.lvli[r["parent"]]
                    for e in r["entries"]:
                        key = ("edge", P["fid"], e["idx"])
                        if key in seen:
                            continue
                        seen.add(key)
                        d = e["dead"]
                        what = "this plan" if r["child"] == b["fid"] else f"the list holding this plan ({e['edid']})"
                        rec_rows = [self.rec(P["fid"], P["edid"], "LVLI")]
                        if r["child"] != b["fid"]:
                            rec_rows.append(self.rec(e["fid"], e["edid"], "LVLI"))
                        if d["kind"] == "shadowed":
                            mine = " and ".join(x for x in self._cond_text(e["conds"]) if x.startswith("GetRandomPercent")) or "no roll condition"
                            above = []
                            for j in d["above"]:
                                aj = P["entries"][j]
                                grp = [x for x in self._cond_text(aj["conds"]) if x.startswith("GetRandomPercent")]
                                above.append(f"entry {aj['idx']} ({aj['edid'] or aj['fid']}, {grp[0] if grp else 'no roll condition'})")
                            bugs.append({"kind": "shadowed", "title": "Can never win its roll",
                                         "text": (f"In {P['edid']}, entry {e['idx']} ({what}) needs {mine}, but the list "
                                                  f"is First Match and the entries above it already claim every roll it "
                                                  f"needs — {', '.join(above)}. It can never be picked."),
                                         "records": rec_rows})
                        else:
                            why = {"list_cn100": f"the whole list {P['edid']} is set to a 100% chance of dropping nothing",
                                   "entry_cn100": f"entry {e['idx']} in {P['edid']} is set to a 100% chance of dropping nothing",
                                   "qty0": f"entry {e['idx']} in {P['edid']} gives a quantity of 0",
                                   "grp_zero": f"entry {e['idx']} in {P['edid']} has a roll condition that can never pass "
                                               f"({', '.join(self._cond_text(e['conds']))})"}[d["why"]]
                            bugs.append({"kind": "never_rolls", "title": "Entry can never drop",
                                         "text": f"{what[0].upper() + what[1:]} never drops: {why}.",
                                         "records": rec_rows})
        return bugs

    def _nice(self, edid):
        lab = self.label(edid)
        return None if not lab or lab == edid or "_" in lab else lab

    def summary(self, it, bug):
        """One plain-English line for the row; the EditorID detail goes in Technical."""
        recs = bug.get("records") or []
        src = next((self._nice(r["edid"]) for r in recs if self._nice(r["edid"])), None)
        k = bug["kind"]
        if k == "orphan":
            return (f"The {src} reward pool that holds this plan isn't hooked up to anything, so it never drops."
                    if src else "The reward pool that holds this plan isn't hooked up to anything, so it never drops.")
        if k == "shadowed":
            return (f"{src}: another reward always wins the roll before this plan gets its turn, so it never drops."
                    if src else "Another reward always wins the roll before this plan gets its turn, so it never drops.")
        if k == "never_rolls":
            return "The entry that should give out this plan can never drop."
        if k == "no_recipe":
            return "Reading this plan doesn't unlock anything \u2014 no craftable recipe is linked to it."
        if k == "wrong_tier":
            return bug["text"]
        if k == "scrap_cut":
            return bug["text"]
        return bug.get("text", "")

    # --------------------------------------------------------------- scrap
    def scan_scrap(self):
        out = []
        for c in self.cobj.values():
            if self.sig.get(c["cnam"]) != "OMOD" or self.sig.get(c["gnam"]) not in ("WEAP", "ARMO"):
                continue
            self.stats["scrap_scanned"] += 1
            if is_dev(c["edid"]) or is_dev(c["cnam_edid"]):
                continue
            tgt_edid = self.edid.get(c["gnam"], c["gnam_edid"])
            tgt_full = self.full.get(c["gnam"], c["gnam_full"])
            problem = None
            if is_dev(tgt_edid) or rng76_is_cut(tgt_edid):
                problem = f"cut content ({tgt_edid})"
            elif not tgt_full:
                problem = f"a record with no in-game name ({tgt_edid}), so it can never be held"
            if not problem:
                continue
            others = [o for o in self.cobj_by_cnam.get(c["cnam"], [])
                      if o["fid"] != c["fid"] and o["gnam"] and not is_dev(o["gnam_edid"])
                      and not is_dev(self.edid.get(o["gnam"], ""))]
            out.append({
                "id": c["fid"], "name": f"Plan: {c['cnam_full'] or c['cnam_edid']}",
                "kind": "scrap", "severity": "route_broken" if others else "unobtainable",
                "bugs": [{"kind": "scrap_cut", "title": "Nothing to scrap",
                          "text": f"This mod is learned by scrapping {tgt_full or tgt_edid}, but that "
                                  f"{'weapon' if self.sig[c['gnam']] == 'WEAP' else 'armour'} is {problem}."}],
                "still_from": [o["gnam_full"] or o["gnam_edid"] for o in others][:8],
                "technical": {"plan": None,
                              "recipes": [self.rec(c["fid"], c["edid"], "COBJ")],
                              "creates": [self.rec(c["cnam"], c["cnam_edid"], "OMOD")],
                              "scrap_item": self.rec(c["gnam"], tgt_edid, self.sig[c["gnam"]])},
            })
        return out


def rng76_is_cut(edid):
    try:
        from cut_content import is_cut
        return is_cut(edid)
    except Exception:
        return False


def _load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def checklist_dead_routes(scan, master_path):
    """Plan-checklist routes that point only at lists the game never rolls."""
    m = _load_json(master_path, None)
    if not m:
        return None
    items = m.get("items", m) if isinstance(m, dict) else m
    out = []
    for it in items:
        for r in it.get("obtain_routes") or []:
            fids = [str(x).upper() for x in (r.get("lvli") or []) if x]
            known = [f for f in fids if f in scan.lvli]
            if not known or any(f in scan.live for f in known):
                continue
            # 0 links alone is NOT proof (scripts hand out plenty of 0-link
            # lists). Only report a route the data shows is dead: a cut-named
            # list, a parked holding pen, a proven orphan, or a dead entry.
            why = set()
            for f in known:
                if is_dev(scan.lvli[f]["edid"]):
                    why.add("cut list")
                for rr in scan.why_unreached(f):
                    if rr["kind"] == "dead_edge":
                        why.add("entry can never roll")
                    elif rr["kind"] == "orphan":
                        e = scan.lvli[rr["list"]]["edid"]
                        if is_dev(e):
                            why.add("cut list")
                        elif RX_PARKED.search(e):
                            why.add("parked list (cut, not marked)")
                        elif scan.orphan_is_strong(e):
                            why.add("reward pool not hooked up")
            pid = ((it.get("plan_item") or {}).get("formid") or "").upper()
            if why == {"reward pool not hooked up"} and any(
                    f == pid for g, items in scan.gmrw_items.items() if g not in scan.gmrw_dead
                    for f, _, _ in items):
                continue   # the quest hands the plan over directly; only the label is off
            if why:
                out.append({"why": sorted(why), "plan": it.get("name"), "id": (it.get("plan_item") or {}).get("formid") or it.get("id"),
                            "route": r.get("route"), "rate": r.get("rate_display"),
                            "lists": [scan.rec(f, scan.lvli[f]["edid"], "LVLI") for f in known]})
    return out


def gate(scan, items, master_path):
    """Second opinion before anything reaches the page. Two independent checks;
    a row either one disagrees with is HELD BACK (kept in the JSON with the
    reason, never rendered):

      1. rng76 — for every "never wins its roll" / "can never drop" bug, the
         site's own drop-rate engine (rng76.pick_rate, same maths as every
         drop-rate page, per the drop-rate-engine skill) must also say 0%.
      2. plan_master — for every "can't get it at all" plan, the plan checklist
         must not list a working route with a real rate through a list the
         game actually rolls.
    """
    master = _load_json(master_path, None)
    routes = {}
    if master:
        for it in (master.get("items", master) if isinstance(master, dict) else master):
            pid = ((it.get("plan_item") or {}).get("formid") or it.get("id") or "").upper()
            routes.setdefault(pid, []).extend(it.get("obtain_routes") or [])
    keep, held = [], []
    for it in items:
        why = []
        for b in it["bugs"]:
            if b["kind"] not in ("shadowed", "never_rolls"):
                continue
            recs = b.get("records") or []
            parent = recs[0]["form_id"] if recs else None
            # always ask about the PLAN itself: pick_rate walks sub-lists down to
            # leaf items, so asking about a sub-list FormID would read 0% for
            # anything and let every row through unchecked
            child = it["id"]
            if parent and it["kind"] == "plan":
                try:
                    rate = scan.res.pick_rate(parent, child)
                except Exception as e:          # engine can't resolve -> can't confirm
                    why.append(f"rng76 could not resolve {recs[0]['edid']} ({e.__class__.__name__})")
                    continue
                if rate > 0:
                    why.append(f"rng76 gives {rate:.2%} from {recs[0]['edid']}")
        if it["severity"] == "unobtainable" and it["kind"] == "plan":
            for r in routes.get(it["id"], []):
                live = [f for f in (str(x).upper() for x in r.get("lvli") or []) if f in scan.live]
                if (r.get("rate") or 0) > 0 and live:
                    why.append(f"checklist shows {r.get('route')} at {r.get('rate_display')} through a live list")
        if why:
            held.append({"id": it["id"], "name": it["name"], "why": why})
        else:
            keep.append(it)
    return keep, held


SEV_ORDER = {"unobtainable": 0, "route_broken": 1, "broken_item": 2}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.path.join(REPO, "tsv"))
    ap.add_argument("--out", default=os.path.join(REPO, "dist", "bugged_plans.json"))
    ap.add_argument("--review", default=os.path.join(REPO, "data", "bugged_plans_review.json"))
    ap.add_argument("--master", default=os.path.join(REPO, "dist", "plan_master.json"))
    ap.add_argument("--channel", choices=("live", "pts"), default=None,
                    help="label the output; the PTS build passes pts (its tsv/ is already PTS-normalised)")
    ap.add_argument("--previous", default=None,
                    help="previous run for NEW/fixed history (default: --out). The PTS build clears "
                         "dist/ first, so it passes the last committed dist/pts/bugged_plans.json")
    a = ap.parse_args(argv)

    review = _load_json(a.review, {}) or {}
    review = {k.upper(): v for k, v in review.items() if not k.startswith("_")}
    prev = _load_json(a.previous or a.out, {}) or {}
    prev_items = {x["id"]: x for x in prev.get("items", [])}
    today = datetime.date.today().isoformat()

    s = Scan(a.data_dir)
    s.analyse_entries()
    s.analyse_reach()
    items = s.scan_plans() + s.scan_scrap()
    items, held_back = gate(s, items, a.master)
    STRONG = {"shadowed", "never_rolls", "wrong_tier"}
    for it in items:
        for bug in it["bugs"]:
            bug["summary"] = s.summary(it, bug)
            bug["detail"] = bug.pop("text")
        strong = [b["kind"] in STRONG or bool(b.get("strong")) for b in it["bugs"]]
        # "can't get it at all" needs EVERY broken route to be proven; one broken
        # route (or a broken plan) only needs its own evidence to be solid
        ok = all(strong) if it["severity"] == "unobtainable" else any(strong)
        it["confidence"] = "strong" if ok else "check"
    parked = [{"list": s.rec(fid, s.lvli[fid]["edid"], "LVLI"),
               "source": s.label(s.lvli[fid]["edid"]),
               "plans": sorted((s.books[b]["full"] or s.books[b]["edid"]) for b in bs)}
              for fid, bs in sorted(s.parked.items())]
    hidden = [x for x in items if (review.get(x["id"]) or {}).get("status") == "not_a_bug"]
    items = [x for x in items if x not in hidden]
    for x in items:
        rv = review.get(x["id"]) or {}
        x["confirmed"] = rv.get("status") == "confirmed"
        if rv.get("note"):
            x["review_note"] = rv["note"]
        old = prev_items.get(x["id"])
        x["first_seen"] = (old or {}).get("first_seen") or today
        x["is_new"] = bool(prev_items) and old is None
    cur_ids = {x["id"] for x in items}
    fixed = [{"id": i, "name": o["name"], "fixed_on": today} for i, o in prev_items.items()
             if i not in cur_ids and i not in {h["id"] for h in hidden}]
    # keep earlier fixes for a few runs so readers see what changed
    for f in prev.get("fixed", []):
        if f["id"] not in cur_ids and f["id"] not in {x["id"] for x in fixed} and \
                len({g["fixed_on"] for g in fixed} | {f["fixed_on"]}) <= 3:
            fixed.append(f)
    items.sort(key=lambda x: (SEV_ORDER.get(x["severity"], 9),
                              re.sub(r"^(plan|recipe)\s*:\s*", "", x["name"], flags=re.I).lower()))
    counts = collections.Counter(x["severity"] for x in items)
    counts.update("conf_" + x["confidence"] for x in items)
    counts["confirmed"] = sum(1 for x in items if x.get("confirmed"))
    doc = {
        "_generated_by": "src/build_bugged_plans_json.py",
        "generated": datetime.date.today().isoformat(),
        "channel": a.channel or ("pts" if "pts" in os.path.normpath(a.data_dir).split(os.sep) else "live"),
        "exports": s.files,
        "stats": dict(s.stats),
        "counts": {"total": len(items), **counts},
        "items": items,
        "parked_lists": parked,
        "fixed": fixed,
        "held_back": held_back,
        "hidden_not_a_bug": len(hidden),
        "checklist_dead_routes": checklist_dead_routes(s, a.master),
    }
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1, ensure_ascii=False)
    print(f"[bugged_plans] {len(items)} bugged ({dict(counts)}) -> {a.out}")
    print(f"[bugged_plans] stats {dict(s.stats)}")
    cd = doc["checklist_dead_routes"]
    print(f"[bugged_plans] new {sum(x['is_new'] for x in items)}, fixed {len(fixed)}, "
          f"held back by gate {len(held_back)}, hidden (not a bug) {len(hidden)}, checklist dead routes {len(cd) if cd is not None else 'n/a'}")


if __name__ == "__main__":
    main()
