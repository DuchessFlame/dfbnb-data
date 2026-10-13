"""Where a unique weapon / armour piece really comes from — traced from the game files.

The chain Bethesda uses for every unique (checked Oct 2026):

    custom mod (OMOD, adds CustomItemName_<X>)
      -> sits in a WEAP ObjectTemplate combination tagged if_tmp_<X>
         (OBTS_Keywords column of the WEAP export)
      -> the leveled lists that reference if_tmp_<X> are the unique's OWN lists
      -> walk those lists up through every parent leveled list
      -> stop at whatever hands them out: a GMRW quest / challenge reward, a
         QUST, a vendor or container, an enemy (NPC_), a scoreboard
         entitlement, a treasure-map mound …

A unique can also exist as its own WEAP copy record (E09C_CompoundBow_
BurningLove) carrying the custom mod; the lists holding those copies are
walked the same way. Armour templates have no keyword column yet, so armour
keywords are matched by name (if_tmp_StandFast <-> Stand Fast) — still read
from the files, never a table.

Why walk ALL the way up: the old reader stopped one level above the own list,
so most uniques kept a hand-written line ("Treasure Hunter event reward" on
Burning Love, which the files give out from Tunnel of Love). A unique in a
shared pool is given out by EVERY holder of that pool, so every holder is
listed — that is what the files say, not a guess at one of them.

Skipped on the way up: the aggregate rosters (LL_Weapon_Unique_All,
LL_Armor_Unique_All, WeaponsUniqueNamedList — catalogues, they hand out
nothing), cut lists (zzz / CUT / DEL / DONOTUSE / DEPRECATED / test), and
the item's own records (WEAP/ARMO/OMOD/KYWD/COBJ).
"""
import re
from collections import defaultdict, deque

import plan_sources

_REF = re.compile(r"^([0-9A-Fa-f]{8}):([^:]*):([A-Z_]{4})$")
_FIDTAG = re.compile(r"\[([A-Z_]{4}):([0-9A-Fa-f]{8})\]")
_OMOD = re.compile(r"\[OMOD:([0-9A-Fa-f]{8})\]")
_KYWD = re.compile(r"\[KYWD:([0-9A-Fa-f]{8})\]")
_DEAD = re.compile(r"(^|_)(zzz|CUT|DONOTUSE|DEL|DEPRECATED|TEST|Debug|QA)(_|$|\d)|^zzz|^DEL_|^XPD_|BabylonExclude", re.I)
_ROSTER = re.compile(r"^(LL_Weapon_Unique_All|LL_Armor_Unique_All|WeaponsUniqueNamedList)$", re.I)
_NPC_WEAP = re.compile(r"^(cr|DEL_|zzz|XPD_)|_cr[A-Z]|NONPLAYABLE|^MTR\d*_cr|^SDOW_cr|^P62_cr", re.I)
_GENERIC_TMP = re.compile(r"if_tmp_(DEBUG|Random|Simple|Default|Base|Any)", re.I)

ROUTES = ["Caps", "Stamps", "Scoreboard", "Gold Bullion", "Atom Shop",
          "Limited Time Bundle", "Events & Activities", "Quests", "Challenges"]


def _f(v):
    v = (v or "").strip().upper()
    return v[-8:].zfill(8) if v else ""


def _norm(s):
    s = re.sub(r"^\s*the\s+", "", (s or "").lower()).replace("vault 63", "v63")
    return re.sub(r"[^a-z0-9]", "", s)


def _tmp_tail(edid):
    t = re.sub(r"^.*?if_tmp_", "", edid or "", flags=re.I)
    return re.sub(r"^(melee|ranged|bounty)_", "", t, flags=re.I)


class Tracer:
    def __init__(self, pick, read_tsv):
        """pick(pattern, exclude=None) -> path; read_tsv(path) -> list of dict rows."""
        self.pick, self.read = pick, read_tsv
        self.unlabelled = defaultdict(set)     # holder edid -> item names (build log)
        self._load()

    def _rows(self, pattern, exclude=None):
        p = self.pick(pattern, exclude=exclude) if exclude else self.pick(pattern)
        return self.read(p) if p else []

    @staticmethod
    def _refs(row):
        for k, v in row.items():
            if k and k.startswith("Ref") and k[3:].isdigit() and v:
                m = _REF.match(v.strip())
                if m:
                    yield _f(m.group(1)), m.group(2), m.group(3)

    def _load(self):
        self.lvli_edid, self.lvli_refs = {}, defaultdict(list)
        for r in self._rows("LVLI_Export_*_LVLI_Refs.tsv"):
            fid = _f(r.get("LVLI_FormID"))
            self.lvli_edid[fid] = r.get("LVLI_EDID", "")
            self.lvli_refs[fid] = list(self._refs(r))
        self.in_lists = defaultdict(set)
        self.list_by_edid = {}
        for r in self._rows("LVLI_Export_*_LVLI_Entries.tsv"):
            m = _REF.match((r.get("LVLO_Reference") or "").strip())
            if m:
                lf = _f(r.get("LVLI_FormID"))
                self.in_lists[_f(m.group(1))].add(lf)
                self.lvli_edid.setdefault(lf, r.get("LVLI_EDID", ""))
        self.list_by_edid = {e: f for f, e in self.lvli_edid.items() if e}
        self.kw_edid, self.kw_refs = {}, defaultdict(list)
        for r in self._rows("KYWD_Export_*_Refs.tsv"):
            k = _f(r.get("KeywordFormID"))
            self.kw_edid[k] = r.get("KeywordEDID", "")
            self.kw_refs[k].append((_f(r.get("RefFormID")), r.get("RefEDID", ""), r.get("RefSignature", "")))
        self.tmp_by_tail = defaultdict(set)
        for k, e in self.kw_edid.items():
            if re.search(r"if_tmp_", e, re.I) and not _GENERIC_TMP.search(e) and not _DEAD.search(e):
                self.tmp_by_tail[_norm(_tmp_tail(e))].add(k)
        # WEAP ObjectTemplate combinations: mods + if_tmp keywords per combination
        combos = defaultdict(lambda: [set(), set()])
        for r in self._rows("WEAP_Export_*_ObjectTemplate.tsv"):
            if _NPC_WEAP.search(r.get("WEAP_EDID") or ""):
                continue
            key = (_f(r.get("WEAP_FormID")), r.get("CombinationIndex"))
            m = _OMOD.search(r.get("Include_Mod") or "")
            if m:
                combos[key][0].add(_f(m.group(1)))
            for k in _KYWD.findall(r.get("OBTS_Keywords") or ""):
                combos[key][1].add(_f(k))
        self.combos = list(combos.values())
        self.tmp_combos = defaultdict(list)
        for mods, kws in self.combos:
            for k in kws:
                if re.search(r"if_tmp_", self.kw_edid.get(k, ""), re.I):
                    self.tmp_combos[k].append(mods)
        self.omod_edid = {}
        for r in self._rows("OMOD_Export_*.tsv", exclude="Properties"):
            self.omod_edid[_f(r.get("OMOD_FormID"))] = (r.get("OMOD_EDID") or "").strip('"')
        # quests
        self.quest = {}
        for pattern, col in (("QUEST_Export_*.tsv", "FULL - Name"), ("QUST2_Export_*_Quests.tsv", "FULL")):
            for r in self._rows(pattern):
                t = plan_sources.tidy_quest_title(r.get(col) or "")
                if t and plan_sources.usable_quest_name(t) and t.lower() != "none":
                    self.quest.setdefault(_f(r.get("FormID")), t)
        self.gmrw = {}
        for r in self._rows("GMRW_Export_*.tsv"):
            fid = _f(r.get("FormID"))
            g = self.gmrw.setdefault(fid, {"edid": r.get("EDID", ""), "quests": [], "chal": [],
                                           "display": r.get("ParentQuestDisplay", "")})
            for rf, _re, sig in self._refs(r):
                if sig == "QUST" and rf not in g["quests"]:
                    g["quests"].append(rf)
                if sig == "CHAL" and rf not in g["chal"]:
                    g["chal"].append(rf)
            m = _FIDTAG.search(r.get("ParentQuestLink") or "") or _REF.match((r.get("ParentQuestLink") or "").strip())
            if m:
                q = _f(m.group(2) if m.re is _FIDTAG else m.group(1))
                if q not in g["quests"]:
                    g["quests"].append(q)

        def names(pattern):
            out = {}
            for r in self._rows(pattern):
                fid = _f(r.get("FormID"))
                if fid:
                    out[fid] = (r.get("FULL") or "").strip().strip('"')
            return out
        self.cont, self.npc, self.chal = names("CONT_Export_*.tsv"), names("NPC_Export_*.tsv"), names("CHAL_Export_*.tsv")

    # ------------------------------------------------------------ keywords
    def tmp_keywords(self, mod_fids, tokens):
        mods = {_f(m) for m in mod_fids if m}
        out = set()
        for k, combo_mods in self.tmp_combos.items():
            e = self.kw_edid.get(k, "")
            if _GENERIC_TMP.search(e) or _DEAD.search(e):
                continue
            # owned: every combination carrying the keyword builds this unique
            if combo_mods and all(cm & mods for cm in combo_mods):
                out.add(k)
        for t in tokens:
            out |= self.tmp_by_tail.get(_norm(t), set())
        return out

    def related_mods(self, tokens):
        """Every custom OMOD whose EditorID carries one of the item's name tokens
        (RD01_Mod_Custom_BoilingPoint_Appearance for Boiling Point)."""
        toks = {_norm(t) for t in tokens if len(_norm(t)) >= 5}
        out = set()
        for fid, e in self.omod_edid.items():
            if not re.search(r"mod_custom", e, re.I) or _DEAD.search(e):
                continue
            ne = _norm(e)
            if any(t in ne for t in toks):
                out.add(fid)
        return out

    # ---------------------------------------------------------------- walk
    def trace(self, tmpl_keywords=(), copy_records=(), list_edids=()):
        own, starts = [], []
        for e in list_edids:
            f = self.list_by_edid.get(e)
            if f and not _DEAD.search(e) and not _ROSTER.match(e) and f not in starts:
                own.append((f, e))
                starts.append(f)
        for kw in tmpl_keywords:
            for f, e, s in self.kw_refs.get(kw, []):
                if s == "LVLI" and not _DEAD.search(e) and f not in starts:
                    own.append((f, e))
                    starts.append(f)
        for rec in copy_records:
            for lf in self.in_lists.get(_f(rec), ()):
                e = self.lvli_edid.get(lf, "")
                if not _DEAD.search(e) and not _ROSTER.match(e) and lf not in starts:
                    own.append((lf, e))
                    starts.append(lf)
        holders, seen, keys = [], set(), set()
        q = deque((f, 0, f) for f in starts)
        while q:
            fid, depth, via = q.popleft()
            if fid in seen or depth > 14:
                continue
            seen.add(fid)
            for rf, red, sig in self.lvli_refs.get(fid, []):
                if _ROSTER.match(red or "") or _DEAD.search(red or ""):
                    continue
                if sig == "LVLI":
                    q.append((rf, depth + 1, via))
                    continue
                if sig in ("WEAP", "ARMO", "OMOD", "KYWD", "COBJ", "LVLN", "PERK"):
                    continue
                if (sig, rf) in keys:
                    continue
                keys.add((sig, rf))
                holders.append({"sig": sig, "fid": rf, "edid": red, "depth": depth,
                                "list": fid, "own": via})
        return own, holders

    # --------------------------------------------------------------- label
    def quest_route(self, title):
        t = (title or "").strip()
        m = re.match(r"^\s*(?:Public\s+)?Event\s*:\s*(.+)$", t, re.I)
        if m:
            return "Events & Activities", m.group(1).strip()
        if re.match(r"^\s*([A-Za-z ]*Activity|Raid|Repeatable|Expedition|Daily Ops|Seasonal)\b", t, re.I) \
                or re.search(r"\bModule\b", t):
            return "Events & Activities", t
        return "Quests", t

    def label(self, h, obtain_idx=None, item_name=""):
        """(route, label) for one holder, or (None, None) when it names nothing
        a reader can go and do."""
        sig, fid, edid = h["sig"], h["fid"], h["edid"] or ""
        if sig == "GMRW":
            g = self.gmrw.get(fid, {})
            # Daily Ops mode quests are titled after the mode ("Uplink"); the
            # reward is the Daily Ops one whatever mode paid it.
            if re.search(r"(^|_)DailyOps(_|$)", edid, re.I):
                return "Events & Activities", "Daily Ops"
            for c in g.get("chal", []):
                if self.chal.get(c) and plan_sources.usable_quest_name(self.chal[c]):
                    return "Challenges", self.chal[c]
            if obtain_idx is not None and getattr(obtain_idx, "reward_chal", {}).get(edid):
                return "Challenges", obtain_idx.reward_chal[edid]
            for qf in g.get("quests", []):
                if self.quest.get(qf):
                    return self.quest_route(self.quest[qf])
            d = plan_sources.tidy_quest_title(g.get("display", ""))
            if d and plan_sources.usable_quest_name(d):
                return self.quest_route(d)
            if re.search(r"(^|_)DailyOps(_|$)", edid, re.I):
                return "Events & Activities", "Daily Ops"
        elif sig == "QUST":
            if self.quest.get(fid):
                return self.quest_route(self.quest[fid])
        elif sig == "FLST":
            if re.search(r"DailyOps", edid, re.I):
                return "Events & Activities", "Daily Ops"
        elif sig == "MGEF":
            if re.search(r"^MutatedEvents", edid, re.I):
                return "Events & Activities", "Mutated Public Events"
        elif sig == "COEN":
            m = re.match(r"^SCORE_S(\d+)_", edid, re.I)
            if m:
                return "Scoreboard", f"Season {int(m.group(1))} Scoreboard"
            m = re.match(r"^SCORE_MiniSeason_([A-Za-z0-9]+)_", edid, re.I)
            if m:
                return "Scoreboard", plan_sources.source_label(m.group(1)) or m.group(1)
        elif sig == "ACTI":
            if re.search(r"TreasureMap", edid, re.I):
                return "Events & Activities", "Treasure Maps"
        elif sig == "CONT":
            lab = (getattr(obtain_idx, "vendor_label", {}) or {}).get(h["list"]) if obtain_idx else None
            if not lab and re.search(r"vendor", edid, re.I):
                lab = plan_sources.source_label(edid)
            if lab:
                low = lab.lower()
                return ("Gold Bullion" if "gold bullion" in low else
                        "Stamps" if "stamp" in low else "Caps"), lab
            nm = self.cont.get(fid)
            if nm and plan_sources.usable_quest_name(nm):
                return "Events & Activities", nm
        elif sig == "NPC_":
            nm = self.npc.get(fid, "")
            if re.match(r"^\d+DR\b", nm) or not re.search(r"[A-Za-z]{3}", nm):
                return None, None
            if re.search(r"vendor", edid, re.I):
                return "Caps", f"{nm} (vendor)"
            if nm:
                return "Events & Activities", f"Enemy drop: {nm}"
        elif sig in ("FURN", "OTFT", "LGDI"):
            if re.search(r"Bounty", edid, re.I):
                return "Events & Activities", "Bounty hunt target"
            if sig == "LGDI":
                return "Events & Activities", "Legendary enemy drop"
        # Last resort for records that name a place or a machine: the quest the
        # record's content code belongs to, then the shared plan-page naming.
        if sig in ("QUST", "CONT", "ACTI", "TERM", "MGEF"):
            lab = obtain_idx._quest_name(edid) if obtain_idx is not None and sig in ("QUST", "MGEF") else ""
            if lab and "_" not in lab and plan_sources.usable_quest_name(lab):
                return self.quest_route(lab)
            lab = plan_sources.source_label(edid)
            if lab and not plan_sources.looks_like_wiring(lab):
                # "Treasure Hunter - Craftable Effect (Mole Miner Pail)" ->
                # "Treasure Hunter (Mole Miner Pail)"; "... Chum Trough Activator"
                lab = re.sub(r"\s*-?\s*\b(Craftable|Effect|Activator)\b", "", lab)
                lab = re.sub(r"\s{2,}", " ", lab).strip(" -")
                return "Events & Activities", lab
        self.unlabelled[f"{sig}:{edid}"].add(item_name)
        return None, None


def _put(routes, route, line, path, tradeable):
    r = next((x for x in routes if x.get("route") == route), None)
    if r is None:
        return False
    if not r.get("populated"):
        r.update({"populated": True, "lines": [], "tradeable": tradeable, "dropRate": "N/A"})
    if line in r["lines"]:
        return False
    at = max((i for i, l in enumerate(r["lines"]) if l.startswith("Source:")), default=-1)
    if at < 0 and r["lines"] and r["lines"][0].startswith("Drops:"):
        at = 0
    r["lines"].insert(at + 1, line)
    r.setdefault("paths", []).append(path)
    return True


_SUMMARY = {"Challenges": "Challenge", "Quests": "Quest", "Caps": "Vendor", "Stamps": "Vendor",
            "Gold Bullion": "Vendor", "Events & Activities": "Event", "Scoreboard": "Score"}


def apply(items, tracer, names=None, obtain_idx=None, hand_only=(), log=print):
    """Add every traced source to each item's routes, drop the hand-written
    ones the files now cover, and rebuild the one-line summary."""
    import uwa_obtain
    hand = {n: h for n, h in hand_only}
    report = {"traced": 0, "untraced": [], "replaced": [], "revived": []}
    for it in items:
        tokens = [it.get("name")] + list(it.get("_altNames") or []) + list(it.get("aka") or [])
        kws = list(it.get("_nameKeywords") or [])
        tokens += [re.sub(r"^.*?CustomItemName_", "", k) for k in kws]
        mods = set(it.get("_cmod_fids") or []) | {m.get("formId") for m in it.get("uniqueMods") or []
                                                   if m.get("kind") == "unique"}
        if names is not None:
            for k in kws:
                mods |= names.kw_mods.get(k, set())
        mods |= tracer.related_mods(tokens)
        tmp = tracer.tmp_keywords(mods, tokens)
        rosters = list(it.get("_rosterLists") or []) + ([it["_rosterList"]] if it.get("_rosterList") else [])
        own, holders = tracer.trace(tmp, it.get("recordIds") or [], rosters)
        it["_ownLists"] = [e for _f2, e in own]
        found = []
        groups = defaultdict(list)          # NPC drops via one list -> one line
        for h in sorted(holders, key=lambda x: (x["depth"], x["edid"] or "")):
            route, lab = tracer.label(h, obtain_idx, it.get("name", ""))
            if not route:
                continue
            if lab.startswith("Enemy drop: "):
                groups[h["list"]].append((lab[12:], [h["list"], h["own"]]))
                continue
            found.append((route, lab, [h["list"], h["own"]]))
        for lf, enemies in groups.items():
            ns = sorted({n for n, _p in enemies})
            text = ", ".join(ns[:4]) + (f" and {len(ns) - 4} more" if len(ns) > 4 else "")
            found.append(("Events & Activities", f"Enemy drop: {text}", enemies[0][1]))
        if not found:
            report["untraced"].append((it.get("name"), it.get("howToObtain")))
            continue
        report["traced"] += 1
        routes = it.get("obtainRoutes") or uwa_obtain.blank_routes()
        it["obtainRoutes"] = routes
        # Hand-written lines the files now cover: the fallback summary
        # (uwa_obtain._fallback) and the "retired" box of a Survival-era item
        # the files hand out again (Face Breaker: Daily Ops, A Knight's Penance).
        hand_how = hand.get(it.get("name")) or next(
            (hand[a] for a in it.get("_altNames") or [] if a in hand), None)
        if hand_how:
            # Only the fallback's own line goes ("Source: Treasure Hunter event
            # reward"); lines other steps read from the files stay.
            detail = (hand_how.split(":", 1)[1] if ":" in hand_how else hand_how).strip()
            for r in routes:
                if not r.get("populated"):
                    continue
                r["lines"] = [l for l in r["lines"] if l.strip() != f"Source: {detail}"]
                if not any(l.startswith("Source:") for l in r["lines"]) and "paths" not in r:
                    r.update({"populated": False, "lines": [], "tradeable": None, "dropRate": None})
            report["replaced"].append((it.get("name"), hand_how))
        for r in routes:
            if r.get("populated") and any("no longer obtainable" in l.lower() for l in r["lines"]):
                kept = [l for l in r["lines"] if l.lower().startswith("legacy:")]
                r.update({"populated": False, "lines": [], "tradeable": None, "dropRate": None})
                r.pop("paths", None)
                report["revived"].append(it.get("name"))
                it["_legacyNote"] = kept
        shown = " ".join(l for r in routes if r.get("populated") for l in r["lines"]).lower()
        for route, lab, path in found:
            core = re.sub(r"^(activity|event)\s*:\s*", "", lab.lower())
            if lab.lower() in shown or core in shown:
                continue
            r = next(x for x in routes if x["route"] == route)
            if not r.get("populated"):
                r.update({"populated": True, "dropRate": "N/A", "tradeable": it.get("tradeable"),
                          "lines": [uwa_obtain._drops_line({"weapon"}, it.get("kind"))]})
            _put(routes, route, f"Source: {lab}", path, it.get("tradeable"))
            shown += " " + lab.lower()
        for l in it.pop("_legacyNote", None) or []:
            home = next((r for r in routes if r.get("populated")), None)
            if home and l not in home["lines"]:
                home["lines"].append(l)
        # summary line, game-file sources first
        parts, first, seen_k = [], None, set()

        def _add(route, lab):
            k = re.sub(r"^(activity|event|enemy drop)\s*:\s*", "", lab.lower()).strip()
            if k in seen_k:
                return
            seen_k.add(k)
            pre = "" if re.match(r"^(Enemy drop|Activity|Enclave Activity|Repeatable|Daily)\b", lab) \
                else f"{_SUMMARY.get(route, 'Source')}: "
            parts.append(pre + lab)
        for route, lab, _p in found:
            _add(route, lab)
            first = first or (route, lab)
        for r in routes:
            if not r.get("populated"):
                continue
            for l in r["lines"]:
                if l.startswith("Source:"):
                    _add(r["route"], l.split(":", 1)[1].strip())
        old = it.get("howToObtain")
        shown_parts = parts[:4] + ([f"and {len(parts) - 4} more"] if len(parts) > 4 else [])
        it["howToObtain"] = " / ".join(shown_parts)
        it["sourceType"] = _SUMMARY.get(first[0], "Event")
        it["sourceRef"] = first[1]
        it["questName"] = first[1] if first[0] == "Quests" else ""
        if old and old != it["howToObtain"]:
            it["_howWas"] = old
    log(f"  traced sources (own lists walked to their holders): {report['traced']} item(s)")
    log(f"  hand-written lines replaced by traced sources: {len(report['replaced'])}")
    for n, h in report["replaced"]:
        log(f"    {n}: {h!r}")
    if report["revived"]:
        log(f"  'no longer obtainable' boxes removed — the files hand these out: {sorted(set(report['revived']))}")
    log(f"  NOT traced (hand line or nothing): {len(report['untraced'])}")
    for n, h in report["untraced"]:
        log(f"    {n}: {h!r}")
    if tracer.unlabelled:
        log(f"  holders with no readable name (skipped): {len(tracer.unlabelled)}")
        for k, v in sorted(tracer.unlabelled.items())[:40]:
            log(f"    {k} <- {', '.join(sorted(v))[:120]}")
    return report
