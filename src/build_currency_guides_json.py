#!/usr/bin/env python3
r"""
build_currency_guides_json.py — one JSON per currency for the DF Currency pages
(/df/currency/guide-<slug>/), rendered by df-bnb-currency-guide.js.

Generative from the game-file exports, same shape of answer as the farming /
spawn guides: where you GET the currency (every source expand) and where you
SPEND it (Vendors). Nothing is typed per currency except the server-side
limits, which are not in the game files at all (see LIMITS below).

WHAT COMES FROM WHERE
---------------------
Currency list      NPC2_Vendors_*_Currencies.tsv  (CNCY: name, plural, MXCT max)
                   + MISC currencies (Legendary Module, Treasury Note) found by
                   matching a Currency page title to a MISC FULL name.
                   Fallback when the split is missing: FLST Collections_Currency.
Pages              tsv/guide_index.tsv rows under df-tc-currency; title matched
                   to the currency's singular or plural name. A currency with
                   no page still gets a JSON (has_page false) so a new one
                   (Mr. Fuzzy Tokens, Supplies) shows up in the build log.
Limits             data/currency/limits.tsv — daily limit, refresh time and
                   period. SERVER-SIDE: checked every GLOB / GMST in the ESM on
                   18 Oct 2026, they are not there. Max held is NOT in this
                   table — it is MXCT, read from the game.
Events/Activities  every leveled list that can hand the currency out, walked up
                   to whatever holds it: GMRW quest reward (named by its quest),
                   MGEF (a loot bag like a Holiday Gift), QUST script. Plus GMRW
                   records that give the currency directly (RewardedItem, or
                   QRCO currency + NAM8 amount GLOB). Amounts + chances from
                   rng76.resolve_deep — no rate maths here.
Random Encounters  the same, where the quest is an RE_ quest.
Containers         CONT holders of those lists (vendor chests excluded).
Creatures          NPC_ holders (death lists).
Challenges         dist/challenges/challenges.json rewards naming the currency.
Collectrons /      dist/collectrons.json / dist/resource_producers.json drops.
Resource Gens
Treasure Maps      treasure_map_sources.build_maps (same as the farming pages).
Vendors            NPC2_Vendors_*_Factions.tsv: every vendor FACTION whose VBCY
                   currency is this one. Where it stands: _FactionsPlacements
                   (vending machine ACTI / NPC_ REFRs), else the stock chest's own
                   cell. What it sells: the chest's leveled lists, resolved with
                   rng76 (chance per restock) and priced from the item record
                   (LGDI ScripCost, BOOK/ARMO/MISC/ALCH value). What it buys: the
                   faction's VEND keyword list (e.g. VendorItemsLegendary).
                   Restock: the faction's VENR days GLOB.
                   A vendor with no placement anywhere in the world is a
                   travelling vendor -> "Chance to Spawn Vendors".

Usage:
  python src/build_currency_guides_json.py            # live -> dist/currency_guides/
  python src/build_currency_guides_json.py --pts      # tsv/pts -> dist/pts/currency_guides/
"""
import argparse
import collections
import csv
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import rng76                                   # noqa: E402
import rate_format                             # noqa: E402
import tsv_source                              # noqa: E402
from spawns_engine import sources as S        # noqa: E402

csv.field_size_limit(10 ** 9)

CURRENCY_HUB = "df-tc-currency"
# Currency pages that are not a single currency.
NOT_A_CURRENCY_PAGE = re.compile(r"daily limits|planner|calculator", re.I)
# Caps is parked (Duchess, 18 Oct 2026): too many sources and vendors to decide
# the layout yet. Not built at all until then - its doc is ~13 MB.
PARKED = {"Caps001"}
# Engine / cut currencies — never a page.
DEAD = re.compile(r"^(zzz|CUT_|DEL_)|Babylon|Tutorial", re.I)
# Placements that are not somewhere a player can walk to.
DEV_CELL = re.compile(r"Debug|zzz|Test|HoldingCell|Warehouse|(^|[^a-z])QA|Smoke", re.I)
# Lists that only exist for the PTS loadout / default-object plumbing.
SKIP_HOLDER_SIGS = {"LOUT", "DFOB", "FLST", "KYWD"}


# ─────────────────────────────── small helpers ──────────────────────────────
def U(s):
    return (s or "").strip().upper()


def rows(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def fid_of(token):
    return U((token or "").split(":")[0])


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")


def fmt(p):
    return rate_format.fmt_rate(p)


def num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f == int(f) else round(f, 2)


# ─────────────────────────────── loader ─────────────────────────────────────
class Data:
    def __init__(self, tsv_root, dist_dir, channel):
        self.tsv, self.dist, self.channel = tsv_root, dist_dir, channel

        def pick(pat, exclude=None):
            return tsv_source.newest(os.path.join(tsv_root, pat), channel=channel,
                                     exclude=exclude, required=False)
        self.pick = pick

        print("[currency] loading rng76 …")
        self.rng = rng76.Rng76Data.from_tsv_root(tsv_root)
        self.res = self.rng.resolver
        ent = pick("LVLI_Export_*_LVLI_Entries.tsv")
        self.c2p, self.p2c, self.lvli_edid = S._load_child_to_parents(ent)
        self.lvli_refs = S._load_lvli_refs(pick("LVLI_Export_*_LVLI_Refs.tsv"))

        self._load_currencies()
        self._load_quests()
        self._load_names()
        self._load_vendors()

    # currencies ------------------------------------------------------------
    def _load_currencies(self):
        self.currencies = {}
        cpath = self.pick("NPC2_Vendors_*_Currencies.tsv")
        if cpath:
            for r in rows(cpath):
                fid = U(r.get("CNCY_FormID"))
                self.currencies[fid] = {
                    "form_id": fid, "edid": r.get("CNCY_EDID") or "", "sig": "CNCY",
                    "name": (r.get("FULL") or "").strip(),
                    "plural": (r.get("PluralName") or "").strip(),
                    "max": num(r.get("MaxCount")),
                    "desc": (r.get("DESC") or "").strip()}
        else:
            print("[currency] WARNING: no NPC2_Vendors_*_Currencies.tsv — re-run "
                  "ExportNPC2VendorsToTSV.pas. Falling back to the FLST names (no max).")
            for r in rows(self.pick("FLST_Export_*_Entries.tsv")):
                pass
            fl = self.pick("FLST_Export_*_Entries.tsv")
            with open(fl, encoding="utf-8", errors="replace") as fh:
                for rr in csv.reader(fh, delimiter="\t"):
                    if len(rr) > 7 and rr[4] == "CNCY":
                        self.currencies[U(rr[5])] = {"form_id": U(rr[5]), "edid": rr[6], "sig": "CNCY",
                                                     "name": rr[7], "plural": "", "max": None, "desc": ""}
        self.misc = {}
        for r in rows(self.pick("MISC_Export_*.tsv", exclude=["MISC2"])):
            self.misc[U(r.get("FormID"))] = r

    # quests / gmrw ---------------------------------------------------------
    def _load_quests(self):
        self.quest = {}
        for r in rows(self.pick("QUEST_Export_*.tsv")):
            self.quest[U(r.get("FormID"))] = {"edid": r.get("EDID") or "",
                                              "full": (r.get("FULL - Name") or "").strip(),
                                              "type": (r.get("Quest Type") or "").strip()}
        self.gmrw = collections.defaultdict(list)     # GMRW FormID -> rows
        for r in rows(self.pick("GMRW_Export_*.tsv")):
            self.gmrw[U(r.get("FormID"))].append(r)
        self.globs = {}
        for r in rows(self.pick("GLOB_Export_*.tsv")):
            self.globs[U(r.get("FormID"))] = (r.get("EDID") or "", num(r.get("FLTV")))
        try:
            import plan_sources
            self.qnames = plan_sources.QuestNames(self.pick("QUEST_Export_*.tsv"))
            self.usable = plan_sources.usable_quest_name
            chal = [(r.get("EDID") or "", r.get("FULL") or "") for r in rows(self.pick("CHAL_Export_*.tsv"))]
            self.raid_enc = plan_sources.raid_encounter_names(
                [(q["edid"], q["full"]) for q in self.quest.values()], chal)
        except Exception as ex:                        # pragma: no cover
            print(f"[currency] plan_sources unavailable ({ex}); plain quest names")
            self.qnames, self.raid_enc = None, {}
            self.usable = lambda n: n if n and "<" not in n and "[" not in n else ""

    # names / values --------------------------------------------------------
    def _load_names(self):
        self.names = self.rng.names
        self.value = {}
        for pat, fcol, vcol, ex in (("BOOK_Export_*.tsv", "FormID", "DATA_Value", ["Locations", "BOOK2"]),
                                    ("MISC_Export_*.tsv", "FormID", "Value", ["MISC2"]),
                                    ("ALCH_Export_*.tsv", "ALCH_FormID", "Value", ["Effects"]),
                                    ("ARMO_Export_*_ARMOUR.tsv", "ARMO_FormID", "DATA_Value", None),
                                    ("KEYM_Export_*.tsv", "FormID", "DATA_Value", ["Locations"])):
            for r in rows(self.pick(pat, exclude=ex)):
                v = num(r.get(vcol))
                if v is not None:
                    self.value[U(r.get(fcol))] = v
        self.lgdi = {}
        for r in rows(self.pick("LGDI_Export_*.tsv", exclude=["Mods"])):
            full = r.get("FULL") or ""
            stars = len(re.match(r"^\s*(¬*)", full).group(1))
            nm = re.sub(r"^[¬\s]+", "", full).strip()
            nm = re.sub(r"\bArmor\b", "Armour", nm)
            if stars and nm.startswith("Legendary"):
                nm = f"{stars}\u2605 {nm}"          # drop-rate-engine §16: "1★ Legendary …"
            self.lgdi[U(r.get("LGDI_FormID"))] = {
                "name": nm,
                "cost": num(r.get("ScripCost"))}
        self.cont = {}
        with open(self.pick("CONT_Export_*.tsv"), encoding="utf-8", errors="replace") as fh:
            rd = csv.reader(fh, delimiter="\t")
            next(rd, None)
            for r in rd:
                if len(r) > 2:
                    self.cont[U(r[0])] = (r[1], r[2])
        # MGEF -> the consumable that carries it (loot bags: Holiday Gift …)
        self.mgef_item = {}
        alch_name = {U(r.get("ALCH_FormID")): (r.get("FULL") or "").strip()
                     for r in rows(self.pick("ALCH_Export_*.tsv", exclude=["Effects"]))}
        for r in rows(self.pick("ALCH_Export_*_Effects.tsv")):
            m, a = U(r.get("MGEF_FormID")), U(r.get("ALCH_FormID"))
            if m and alch_name.get(a):
                self.mgef_item.setdefault(m, alch_name[a])
        self.npc_full = {}
        self.npc_factions = {}
        for r in rows(self.pick("NPC_Export_*.tsv", exclude=["PRPS", "Refs"])):
            f = U(r.get("FormID"))
            self.npc_full[f] = (r.get("FULL") or "").strip()
            self.npc_factions[f] = re.findall(r"([A-Za-z0-9_]+)\[Rank", r.get("Factions_Flat") or "")
        try:
            import plan_vendor_regions as PVR
            self.loc = PVR.Locations(self.tsv)
        except Exception as ex:
            print(f"[currency] no LCTN export ({ex}) — vendor places unnamed")
            self.loc = None
        try:
            import plan_vendor_regions as PVR
            self.mapp = PVR.Mappalachia()
        except Exception:
            self.mapp = None

    # vendors ---------------------------------------------------------------
    def _load_vendors(self):
        self.factions = rows(self.pick("NPC2_Vendors_*_Factions.tsv"))
        self.fplace = collections.defaultdict(list)
        for r in rows(self.pick("NPC2_Vendors_*_FactionsPlacements.tsv")):
            self.fplace[U(r.get("Faction_FormID"))].append(r)
        if not self.factions:
            print("[currency] WARNING: no NPC2_Vendors_*_Factions.tsv — Vendors expands "
                  "will be empty until ExportNPC2VendorsToTSV.pas is re-run.")
        # token-exchange terminals (TERM export, Oct 2026)
        self.term_rows = collections.defaultdict(list)
        tp = self.pick("TERM_Export_*.tsv", exclude=["Quests", "Placements"])
        for r in rows(tp):
            self.term_rows[U((r.get("TERM_FormID") or "").strip('"'))].append(r)
        self.term_place = collections.defaultdict(list)
        for r in rows(self.pick("TERM_Export_*_Placements.tsv")):
            self.term_place[U(r.get("TERM_FormID"))].append(r)
        # chest base CONT -> the leveled lists it holds
        self.cont_lists = collections.defaultdict(list)
        for lv, refs in self.lvli_refs.items():
            for rf, _e, sig in refs:
                if sig == "CONT":
                    self.cont_lists[rf].append(lv)

    # helpers ---------------------------------------------------------------
    def item_name(self, fid, edid="", sig=""):
        if sig == "LGDI" and fid in self.lgdi:
            return self.lgdi[fid]["name"]
        if fid in self.currencies:
            return self.currencies[fid]["name"]
        n = self.names.resolve(fid, edid)
        return n or rng76.humanize_edid(edid)

    def place(self, loc_fid, x=None, y=None, ref=None, exterior=False):
        name, region = "", ""
        if self.loc and loc_fid:
            name = self.loc.full.get(U(loc_fid)) or ""
            reg = self.loc.region_of(U(loc_fid))
            region = self.loc.full.get(reg) if reg else ""
        if not region and self.mapp:
            r = self.mapp.by_ref(ref) if ref else None
            if not r and exterior and x is not None and y is not None:
                r = self.mapp.outline(x, y)
            region = r or ""
        return name, region


# ─────────────────────────────── naming ─────────────────────────────────────
TYPE_WORD = {"public event": "Public Event", "event": "Event", "daily": "Daily Quest",
             "daily ops": "Daily Ops", "expedition": "Expedition", "raid": "Raid",
             "caravan": "Caravan", "side quest": "Quest", "primary": "Quest",
             "secondary": "Quest", "miscellaneous": "Quest", "module": "Activity"}


_ALIAS = re.compile(r"\s*-?\s*<Alias=[^>]*>")


def orphan_quest(D, gmrw_edid):
    """A GMRW with no parent quest (the Atlantic City repeatables): find the
    quest whose title ends with the words in the reward's EditorID."""
    tail = re.sub(r"^.*?QuestReward_", "", gmrw_edid or "")
    words = norm(rng76.humanize_edid(tail.split("_")[-1]))
    if len(words) < 6:
        return ""
    for qf, q in D.quest.items():
        bare = norm(re.sub(r"^.*?:", "", q["full"] or ""))
        if bare and bare == words:
            return qf
    return ""


def quest_label(D, qfid, gmrw_row=None):
    """(name, type) for a quest, player-facing."""
    if not qfid and gmrw_row is not None:
        qfid = orphan_quest(D, gmrw_row.get("EDID"))
    q = D.quest.get(qfid, {"edid": "", "full": "", "type": ""})
    edid, qtype = q["edid"], q["type"].lower()
    full = _ALIAS.sub("", q["full"] or "").strip()
    if re.match(r"^RE_", edid):
        kind = "Random Encounter"
    else:
        kind = TYPE_WORD.get(qtype, "")
    m = re.match(r"^(Event|Activity|Enclave Activity|Daily|Raid|Caravan)\s*:\s*(.+)$", full or "")
    name = D.usable(full or "")
    if m and name:
        pre, bare = m.group(1), m.group(2).strip()
        kind = {"Event": "Public Event" if qtype == "public event" else "Event",
                "Daily": "Daily Quest"}.get(pre, pre)
        name = bare
    if not name and D.qnames:
        hit, _n = D.qnames.lookup(edid.split("_"))
        if hit:
            name = re.sub(r"^(Event|Activity|Daily|Raid)\s*:\s*", "", hit)
    if kind == "Daily Ops":
        name = "Daily Ops"
    # raids: "Raid: Gleaming Depths - <encounter>"
    if gmrw_row is not None:
        mm = re.match(r"^(RD\d+)_QuestReward_Enc(\d+)$", gmrw_row.get("EDID") or "", re.I)
        if mm:
            key = (mm.group(1).upper(), int(mm.group(2)))
            if key in D.raid_enc:
                raid, who = D.raid_enc[key]
                return f"{raid} - {who}", "Raid"
    if not name:
        name = rng76.humanize_edid(re.sub(r"^(QuestReward_|.*?_QuestReward_)", "", (gmrw_row or {}).get("EDID", "") or edid))
        name = re.sub(r"^[A-Z]{2,}\d*\s+", "", name)      # "MILE Mole Miner …" -> "Mole Miner …"
    return name, (kind or "Quest")


# Daily Ops ranks: the GLOBs only say High / Medium / Low; the names are the
# in-game ones, the same table uwa_entries and the Daily Ops page use.
RANK_NAME = {"high": "Elder", "medium": "Paladin", "low": "Knight"}
_TIER_D = None


def tier_text(g):
    lab = (g.get("TierLabel") or "").strip()
    if lab:
        return lab
    ge = (_TIER_D.globs.get(fid_of(g.get("ConditionGlobs"))) or ("", None))[0] if _TIER_D else ""
    m = re.search(r"_Timer_Tier_(High|Medium|Low)$", ge or "", re.I)
    if m and "GetRemainingQuestTimeSeconds" in (g.get("Conditions") or ""):
        return RANK_NAME[m.group(1).lower()] + " rank"
    return ""


def gmrw_chance(g):
    """GetRandomPercent on the GMRW reward row itself. Kept SEPARATE from the
    list rate (drop-rate-engine §7) and rendered as its own note."""
    if (g.get("TierConditionFunc") or "").strip() == "GetRandomPercent":
        return num(g.get("TierConditionValue"))
    m = re.search(r"GetRandomPercent \d{8} ([\d.]+)", g.get("Conditions") or "")
    return num(m.group(1)) if m else None


# ─────────────────────────────── sources ────────────────────────────────────
# Double-mutation reward lists are cut content site-wide (plan_route_kinds
# CUT_LIST_RX) — the same rule here, read off the entry's conditions.
_CUT_COND = re.compile(r"DoubleMutation", re.I)


def cond_note(conds):
    """A few plain words for the conditions on one payout, or "". Only what
    can be read without decoding comparison flags."""
    t = " ".join(conds or [])
    bits = []
    if re.search(r"GetGlobalValue[^\]]*(LimitedTime|LTT_)", t):
        bits.append("Limited-time")
    return ", ".join(bits)


def branch_note(edid):
    """Words a reward list's own EditorID carries that a reader needs."""
    bits = []
    if re.search(r"Fallout_?1st|FO1", edid or "", re.I):
        bits.append("Fallout 1st")
    if re.search(r"Mutat", edid or "", re.I):
        bits.insert(0, "Double-mutated event" if re.search(r"Double", edid or "", re.I) else "Mutated event")
    return ", ".join(bits)


def _branches(D, root):
    """For a Use All list with no cap (every entry fires on its own), the
    entries are separate payouts — e.g. a mutated event's normal list and its
    Fallout 1st list. Returns [(sub_list, entry_factor, note)] or None."""
    fl = D.rng.lvli.flags_for(root)
    if not fl.get("use_all") or D.rng.lvli.max_count_for(root):
        return None
    out = []
    for e in D.rng.lvli.entries_by_list.get(root, []):
        m = D.rng.lvli.math_by_entry.get((root, e.get("EntryIndex")))
        sub = (m or {}).get("SubLVLI_FormID") or ""
        if not sub:
            continue
        ed = D.lvli_edid.get(U(sub), "")
        if _CUT_LIST.search(ed):
            continue
        pw, cn = D.res._entry_pick_and_cn(m, e, root)
        out.append((U(sub), pw * cn, branch_note(ed)))
    # Only worth splitting when the branches say something different (normal vs
    # Fallout 1st). Two plain entries pointing at the same pool are two rolls of
    # it, which resolve_deep already adds up.
    if len(out) < 2 or not any(n for _s, _f, n in out):
        return None
    return out


_CUT_LIST = re.compile(r"DoubleMutat", re.I)


def amounts_in(D, root, targets, _depth=0):
    """[{qty, rate, note}] for the currency inside one reward list (rng76).
    Payouts behind different conditions are alternatives, so each keeps its
    own row; the same amount under the same condition is one row."""
    br = _branches(D, root) if _depth == 0 else None
    if br:
        merged = {}
        for sub, factor, note in br:
            for a in amounts_in(D, sub, targets, _depth + 1):
                key = (a["qty"], ", ".join(x for x in (note, a["note"]) if x))
                merged[key] = max(merged.get(key, 0.0), a["rate"] * factor)
        return [{"qty": q, "note": n, "rate": round(r, 6), "rate_display": fmt(r)}
                for (q, n), r in sorted(merged.items(), key=lambda kv: (kv[0][1], -kv[0][0])) if r > 0]
    rows_ = {}
    for it in D.res.resolve_deep(root):
        f = U(it.get("formid"))
        if not any(f.endswith(t) for t in targets):
            continue
        conds = it.get("conditions") or []
        if _CUT_COND.search(" ".join(conds)):
            continue
        key = (int(it.get("qty") or 1), cond_note(conds))
        r = float(it.get("dropRate") or 0)
        rows_[key] = max(rows_.get(key, 0.0), r) if key[1] else min(1.0, rows_.get(key, 0.0) + r)
    out = [{"qty": q, "note": n, "rate": round(r, 6), "rate_display": fmt(r)}
           for (q, n), r in sorted(rows_.items(), key=lambda kv: (-kv[0][0], kv[0][1])) if r > 0]
    return out


def expected(amts):
    return round(sum(a["qty"] * a["rate"] for a in amts), 2)


def build_sources(D, cur):
    targets = {cur["form_id"]}
    closure = S._closure(set(targets), D.c2p)
    events, randoms, containers, creatures = {}, {}, {}, {}

    def add(bucket, key, row):
        if key in bucket:
            # same source reached by two lists: keep the bigger payout
            if expected(row["amounts"]) <= expected(bucket[key]["amounts"]):
                return
        bucket[key] = row

    for lv in closure:
        if lv in targets:
            continue
        holders = [h for h in D.lvli_refs.get(lv, []) if h[2] != "LVLI"]
        if not holders:
            continue
        amts = None
        for hf, he, hs in holders:
            if hs in SKIP_HOLDER_SIGS:
                continue
            if amts is None:
                amts = amounts_in(D, lv, targets)
            if not amts:
                break
            if hs == "GMRW" and re.match(r"ChallengeReward", he or "", re.I):
                continue          # a challenge's reward - listed under Challenges
            if hs == "GMRW":
                grows = [g for g in D.gmrw.get(hf, []) if fid_of(g.get("RewardedItem")) == lv] \
                    or D.gmrw.get(hf, [])[:1]
                for g in grows[:1]:
                    q = fid_of(g.get("ParentQuestLink"))
                    name, kind = quest_label(D, q, g)
                    tier = tier_text(g)
                    if not tier and kind == "Daily Ops" and re.search(r"Repeatable", D.lvli_edid.get(lv, ""), re.I):
                        tier = "Repeat runs"
                    row = {"name": name, "type": kind, "tier": tier, "amounts": amts,
                           "expected": expected(amts), "condition_chance": gmrw_chance(g),
                           "via": D.lvli_edid.get(lv, "")}
                    bucket = randoms if kind == "Random Encounter" else events
                    add(bucket, (name, kind, tier), row)
            elif hs == "MGEF":
                item = D.mgef_item.get(hf) or rng76.humanize_edid(he)
                add(events, (item, "Loot Bag", ""), {"name": item, "type": "Loot Bag", "tier": "",
                                                     "amounts": amts, "expected": expected(amts),
                                                     "condition_chance": None, "via": D.lvli_edid.get(lv, "")})
            elif hs == "QUST":
                name, kind = quest_label(D, hf)
                bucket = randoms if kind == "Random Encounter" else events
                add(bucket, (name, kind, ""), {"name": name, "type": kind, "tier": "", "amounts": amts,
                                               "expected": expected(amts), "condition_chance": None,
                                               "via": D.lvli_edid.get(lv, "")})
            elif hs == "CONT":
                ed, full = D.cont.get(hf, (he, ""))
                if re.search(r"VendorChest|VendingMachine|Collectron", ed, re.I) or not full:
                    continue
                add(containers, (full,), {"name": full, "amounts": amts, "expected": expected(amts)})
            elif hs == "NPC_":
                nm = D.npc_full.get(hf) or ""
                if not nm:
                    continue
                add(creatures, (nm,), {"name": nm, "amounts": amts, "expected": expected(amts)})

    # GMRW records that hand the currency over directly (no list)
    for gfid, grows in D.gmrw.items():
        for g in grows:
            qty = None
            if fid_of(g.get("RewardedItem")) in targets:
                qty = num(g.get("RewardedItemCount")) or 1
            elif fid_of(g.get("QRCO_CurrencyObject")) in targets and g.get("NAM8_CapsGlobal"):
                qty = (D.globs.get(fid_of(g.get("NAM8_CapsGlobal"))) or ("", None))[1]
            if not qty or re.match(r"ChallengeReward", g.get("EDID") or "", re.I):
                continue
            q = fid_of(g.get("ParentQuestLink"))
            if re.search(r"(^|_)(zzz|CUT|DEL|Test|Debug)(_|$)", D.quest.get(q, {}).get("edid", ""), re.I):
                continue
            name, kind = quest_label(D, q, g)
            amts = [{"qty": qty, "rate": 1.0, "rate_display": "100%"}]
            row = {"name": name, "type": kind, "tier": tier_text(g), "amounts": amts,
                   "expected": qty, "condition_chance": gmrw_chance(g), "via": g.get("EDID") or ""}
            bucket = randoms if kind == "Random Encounter" else events
            add(bucket, (name, kind, row["tier"]), row)

    def ordered(b, keys=("type", "name", "tier")):
        return sorted(b.values(), key=lambda r: tuple((r.get(k) or "").lower() for k in keys))

    return {"events_activities": ordered(events), "random_encounters": ordered(randoms),
            "containers": sorted(containers.values(), key=lambda r: -r["expected"]),
            "creatures": sorted(creatures.values(), key=lambda r: r["name"])}


def build_challenges(D, cur):
    path = os.path.join(D.dist, "challenges", "challenges.json")
    if not os.path.exists(path):
        path = os.path.join(REPO, "dist", "challenges", "challenges.json")
    try:
        data = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return [], []
    names = {n.lower() for n in (cur["name"], cur.get("plural") or "") if n}
    stem = re.escape(re.sub(r"s$", "", cur["name"]).lower())
    rx_reward = re.compile(r"^(?:" + stem + r")s?\s*(?:\((\d+)\))?$", re.I)
    rx_use = re.compile(r"\b" + stem + r"s?\b", re.I)
    rewards, uses, seen_r, seen_u = [], [], set(), set()

    def ctype(page, node):
        if (page or "").startswith("season:"):
            return "Mini Season"
        return (node.get("scope") or node.get("group") or (page or "").replace("-", " ").title()).strip()

    def walk(page, node):
        if isinstance(node, list):
            for x in node:
                walk(page, x)
            return
        if not isinstance(node, dict):
            return
        if node.get("edid") and not node.get("is_cut"):
            for r in node.get("rewards") or []:
                m = rx_reward.match(str(r).strip())
                if m and node["edid"] not in seen_r:
                    seen_r.add(node["edid"])
                    rewards.append({"name": node.get("full") or "", "type": ctype(page, node),
                                    "required": node.get("required"),
                                    "qty": int(m.group(1)) if m.group(1) else 1})
            text = " ".join([node.get("full") or ""] + list(node.get("conditions_human") or []))
            if rx_use.search(text) and node["edid"] not in seen_u and not node.get("is_sub"):
                seen_u.add(node["edid"])
                uses.append({"name": node.get("full") or "", "type": ctype(page, node),
                             "required": node.get("required")})
        for k in ("children", "items", "groups", "challenges"):
            if k in node:
                walk(page, node[k])
    for page, node in (data.get("pages") or {}).items():
        walk(page, node)
    key = lambda r: (r["type"].lower(), r["name"].lower())
    return sorted(rewards, key=key), sorted(uses, key=key)


def build_producers(D, cur):
    out = {"collectrons": [], "resource_generators": []}
    for key, fn in (("collectrons", "collectrons.json"), ("resource_generators", "resource_producers.json")):
        try:
            data = json.load(open(os.path.join(D.dist, fn), encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for st in data.get("items") or []:
            for d in ((st.get("production") or {}).get("drops") or []):
                if U(d.get("formId")) == cur["form_id"]:
                    ch = (d.get("chance") or 0) / 100.0
                    out[key].append({"name": st.get("displayName") or "", "interval": (st.get("production") or {}).get("intervalDisplay"),
                                     "rate": ch, "rate_display": fmt(ch)})
    return out


def build_treasure(D, cur):
    try:
        import treasure_map_sources as TMS
        return TMS.build_maps(D.dist, {cur["form_id"]},
                              lambda L, t: D.res.appearance_prob(L, t), D.rng.lvli, fmt)
    except Exception as ex:
        print(f"[currency] treasure maps skipped: {ex}")
        return []


# ─────────────────────────────── vendors ────────────────────────────────────
def vendor_stock(D, base_fid):
    """Every item the chest can stock, chance per restock, price."""
    items = {}
    for lv in D.cont_lists.get(base_fid, []):
        for it in D.res.resolve_deep(lv):
            f = U(it.get("formid"))
            sig = it.get("sig") or ""
            edid = it.get("edid") or ""
            price = None
            if sig == "LGDI" or f in D.lgdi:
                price = (D.lgdi.get(f) or {}).get("cost")
                sig = "LGDI"
            if price is None:
                price = D.value.get(f)
            name = D.item_name(f, edid, sig)
            cond = " ".join(it.get("conditions") or [])
            gated = bool(re.search(r"GetGlobalValue|GetQuestCompleted|GetStageDone|HasLearned", cond))
            key = (f, int(it.get("qty") or 1))
            row = items.get(key)
            r = float(it.get("dropRate") or 0)
            if row:
                row["rate"] = min(1.0, row["rate"] + r)
            else:
                items[key] = {"name": name, "qty": int(it.get("qty") or 1), "price": price,
                              "rate": r, "form_id": f, "limited": gated}
    out = sorted(items.values(), key=lambda x: x["name"].lower())
    for x in out:
        x["rate"] = round(x["rate"], 6)
        x["rate_display"] = fmt(x["rate"])
    return out


_STOCK_CACHE = {}
PAGES = []


def build_sellers(D, cur):
    """Vendors whose stock holds the currency itself (the Purveyor sells
    Legendary Modules for scrip). A source, priced in the vendor's currency."""
    out = []
    for f in D.factions:
        base = U(f.get("ContainerBase_FormID"))
        if not base or DEAD.search(f.get("Faction_EDID") or ""):
            continue
        if base not in _STOCK_CACHE:
            _STOCK_CACHE[base] = vendor_stock(D, base) if D.cont_lists.get(base) else []
        hit = [x for x in _STOCK_CACHE[base] if x["form_id"] == cur["form_id"]]
        if not hit:
            continue
        cfid = U(f.get("Currency_FormID")) or "0000000F"
        c = D.currencies.get(cfid) or {}
        pg = match_page(PAGES, [c.get("name"), c.get("plural")]) if c else None
        cname = (pg or {}).get("title") or c.get("plural") or "Caps"
        who = [p.get("Holder_FULL") for p in D.fplace.get(U(f.get("Faction_FormID")), [])
               if p.get("Holder_FULL") and holder_trades_here(D, p, f.get("Faction_EDID") or "")]
        who = [w for w in dict.fromkeys(who)]
        for x in hit:
            out.append({"name": ", ".join(who) or rng76.humanize_edid(f.get("Faction_EDID") or ""),
                        "price": x["price"], "price_currency": cname, "qty": x["qty"],
                        "rate": x["rate"], "rate_display": x["rate_display"]})
    return sorted(out, key=lambda r: r["name"])


def holder_trades_here(D, p, fedid):
    """An NPC in two vendor factions (Minerva is in the Purveyor's as well as
    her own) trades in the one that names her."""
    if p.get("Holder_Sig") != "NPC_":
        return True
    hf = U(p.get("Holder_FormID"))
    vend = {U(z.get("Faction_EDID")) for z in D.factions}
    facs = [x for x in D.npc_factions.get(hf, []) if U(x) in vend]
    nm = re.sub(r"[^a-z0-9]", "", (p.get("Holder_FULL") or "").lower())
    own = [x for x in facs if nm and nm in x.lower()]
    return not own or fedid in own


def build_vendors(D, cur):
    fixed, travel = [], []
    for f in D.factions:
        if U(f.get("Currency_FormID")) != cur["form_id"]:
            continue
        ffid = U(f.get("Faction_FormID"))
        fedid = f.get("Faction_EDID") or ""
        if DEAD.search(fedid) or re.search(r"(^|_)(QA|Test|Debug)", fedid):
            continue
        # who stands at this faction's stall
        holders = collections.OrderedDict()
        for p in D.fplace.get(ffid, []):
            hf = U(p.get("Holder_FormID"))
            if not holder_trades_here(D, p, fedid):
                continue
            h = holders.setdefault(hf, {"name": p.get("Holder_FULL") or "", "sig": p.get("Holder_Sig"),
                                        "places": []})
            if p.get("Ref_FormID") and not DEV_CELL.search(p.get("Cell_EDID") or "") \
                    and not DEV_CELL.search(p.get("Location_EDID") or "") \
                    and (p.get("Worldspace_EDID") or p.get("Cell_EDID")):
                x, y = num(p.get("X")), num(p.get("Y"))
                loc_name, region = D.place(p.get("Location_FormID"), x, y, p.get("Ref_FormID"),
                                           exterior=(p.get("Worldspace_EDID") or "").upper() == "APPALACHIA")
                h["places"].append({"location": loc_name or "", "region": region or ""})
        # the stall itself, when the chest stands in the world
        stall = None
        if not DEV_CELL.search(f.get("Cell_EDID") or "") and (f.get("Cell_EDID") or f.get("Worldspace_EDID")):
            ln, rg = D.place(f.get("Location_FormID"))
            stall = {"location": ln, "region": rg}
        restock = num(f.get("RestockDays_Value"))
        buys = f.get("BuySell_EDID") or ""
        stock = vendor_stock(D, U(f.get("ContainerBase_FormID")))
        # a vending machine's faction name is all the place it gives (Legendary
        # Exchange at <station>): one row per placement keeps them apart
        for hf, h in (holders.items() or [("", {"name": "", "sig": "", "places": []})]):
            places = h["places"] or ([stall] if stall else [])
            places = [p for i, p in enumerate(places)
                      if p and (p.get("location") or p.get("region")) and p not in places[:i]]
            entry = {"name": h["name"] or "Vendor", "kind": "Vending machine" if h["sig"] in ("ACTI", "FURN") else "Vendor",
                     "faction": fedid, "locations": places, "restock_days": restock,
                     "buys": rng76.humanize_edid(buys.replace("VendorItems", "")) if buys else "",
                     "stock": stock}
            (fixed if places else travel).append(entry)
    # The same machine in many places (Legendary Exchange at every station) is
    # ONE card listing every place. A machine with no place in the world is a
    # dev / holding-cell copy, not a travelling vendor.
    merged = collections.OrderedDict()
    for v in fixed:
        sig = (v["name"], v["kind"], v["buys"], json.dumps(v["stock"], sort_keys=True))
        if sig in merged:
            for p in v["locations"]:
                if p not in merged[sig]["locations"]:
                    merged[sig]["locations"].append(p)
        else:
            merged[sig] = v
    fixed = list(merged.values())
    for v in fixed:
        v["locations"].sort(key=lambda p: (p.get("region") or "zz", p.get("location") or ""))
    travel = [v for v in travel if v["kind"] == "Vendor"]
    # sorted by region then place so the list reads like a map
    key = lambda v: ((v["locations"][0]["region"] if v["locations"] else "zz"),
                     (v["locations"][0]["location"] if v["locations"] else ""), v["name"])
    fixed.sort(key=key)
    travel.sort(key=lambda v: v["name"])
    return fixed, travel


_PRICE = re.compile(r"^\s*(.+?)\s+-\s+([\d,]+)\s+([A-Za-z.]+)\s*$")


def _item_refs(D, cur):
    """TERM records that point at the currency item (the MISC's own
    ReferencedBy; a CNCY is matched to the MISC of the same name, which is how
    Mr. Fuzzy Tokens moved from an item to a wallet currency on PTS)."""
    names = {norm(cur["name"]), norm(cur.get("plural") or "")}
    out = set()
    for fid, r in D.misc.items():
        if fid != cur["form_id"] and norm(r.get("FULL")) not in names:
            continue
        for k, v in r.items():
            if k and re.match(r"Ref\d+$", k) and (v or "").endswith(":TERM"):
                out.add(fid_of(v))
    return out


def build_terminals(D, cur):
    out = []
    for tf in sorted(_item_refs(D, cur)):
        rows_ = D.term_rows.get(tf) or []
        if not rows_:
            continue
        q = lambda r, k: (r.get(k) or "").strip('"')
        head = next((q(r, "Text") for r in rows_ if q(r, "Row") == "HEADER"), "")
        items = []
        for r in rows_:
            if q(r, "Row") != "ITEM":
                continue
            m = _PRICE.match(q(r, "Text"))
            if m:
                items.append({"name": m.group(1).strip(), "price": int(m.group(2).replace(",", "")),
                              "rate": None, "rate_display": "", "qty": 1})
        places = []
        for p in D.term_place.get(tf, []):
            if DEV_CELL.search(p.get("Cell_EDID") or "") or DEV_CELL.search(p.get("Location_EDID") or ""):
                continue
            x, y = num(p.get("X")), num(p.get("Y"))
            ln, rg = D.place(p.get("Location_FormID"), x, y, p.get("Ref_FormID"),
                             exterior=(p.get("Worldspace_EDID") or "").upper() == "APPALACHIA" or not p.get("Cell_EDID"))
            pl = {"location": ln, "region": rg}
            if pl not in places:
                places.append(pl)
        out.append({"name": head or "Terminal", "kind": "Terminal", "faction": "", "locations": places,
                    "restock_days": None, "buys": "", "stock": items, "terminal": True})
    return out


# ─────────────────────────────── limits ─────────────────────────────────────
def load_limits():
    path = os.path.join(REPO, "data", "currency", "limits.tsv")
    out = {}
    for r in rows(path):
        out[(r.get("currency_edid") or "").strip()] = r
    return out


# ─────────────────────────────── pages ──────────────────────────────────────
def load_pages():
    out = []
    for r in rows(os.path.join(REPO, "tsv", "guide_index.tsv")):
        if r.get("parentId") == CURRENCY_HUB and not NOT_A_CURRENCY_PAGE.search(r.get("title") or ""):
            out.append({"title": (r.get("title") or "").strip(), "slug": r.get("slug") or "",
                        "url": r.get("url") or "", "blurb": (r.get("blurb") or "").strip()})
    return out


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def match_page(pages, names):
    keys = {norm(n) for n in names if n}
    keys |= {k + "s" for k in list(keys)} | {k[:-1] for k in list(keys) if k.endswith("s")}
    for p in pages:
        if norm(p["title"]) in keys:
            return p
    return None


def currency_list(D, pages):
    out = []
    for c in D.currencies.values():
        if DEAD.search(c["edid"]) or not c["name"] or c["max"] in (1,):
            continue          # 1 = a flag (Ultracell Charge), not a wallet
        out.append(dict(c))
    # MISC currencies: only where a Currency page names them
    for p in pages:
        if match_page([p], [c["name"] for c in out] + [c["plural"] for c in out]):
            continue
        for fid, r in D.misc.items():
            full = (r.get("FULL") or "").strip()
            if full and not DEAD.search(r.get("EDID") or "") and match_page([p], [full]):
                kw = r.get("Keywords") or ""
                out.append({"form_id": fid, "edid": r.get("EDID") or "", "sig": "MISC", "name": full,
                            "plural": full + "s", "max": None, "desc": "",
                            "tradeable": "NonPlayerTradable" not in kw, "weight": num(r.get("Weight"))})
                break
    return out


def build_one(D, cur, pages, limits):
    page = match_page(pages, [cur["name"], cur.get("plural")])
    slug = (page["slug"] if page else "guide-" + slugify(cur.get("plural") or cur["name"]))
    lim = limits.get(cur["edid"], {})
    src = build_sources(D, cur)
    chal_rewards, chal_uses = build_challenges(D, cur)
    prod = build_producers(D, cur)
    fixed, travel = build_vendors(D, cur)
    terms = build_terminals(D, cur)
    fixed += terms            # a terminal is bolted down even when its place is unknown
    doc = {
        "slug": slug, "url": page["url"] if page else "", "has_page": bool(page),
        "parked": cur["edid"] in PARKED,
        "name": page["title"] if page else (cur.get("plural") or cur["name"]),
        "currency": {"form_id": cur["form_id"], "edid": cur["edid"], "record": cur["sig"],
                     "singular": cur["name"], "plural": cur.get("plural") or ""},
        "intro": {"blurb": (page or {}).get("blurb") or ""},
        "limits": {
            "daily_limit": (lim.get("daily_limit") or "").strip(),
            "refresh_time": (lim.get("refresh_time") or "").strip(),
            "refresh_period": (lim.get("refresh_period") or "").strip(),
            "max_limit": cur.get("max"),
            "max_source": "game files (CNCY MXCT)" if cur.get("max") is not None else "",
            "notes": (lim.get("notes") or "").strip(),
            "confirmed": (lim.get("confirmed") or "").strip().lower() in ("1", "yes", "y", "true")},
        "details": {
            "tradeable": cur.get("tradeable", (lim.get("tradeable") or "").strip() or None),
            "weight": cur.get("weight"),
            "vendor_restock_days": sorted({v["restock_days"] for v in fixed + travel if v["restock_days"]}),
        },
        "used_for": {"challenges": chal_uses},
        "sources": {
            "challenges": chal_rewards,
            "collectrons": prod["collectrons"],
            "containers": src["containers"],
            "creatures": src["creatures"],
            "events_activities": src["events_activities"],
            "random_encounters": src["random_encounters"],
            "resource_generators": prod["resource_generators"],
            "treasure_maps": build_treasure(D, cur),
            "vendors": build_sellers(D, cur),
        },
        "vendors": {"fixed": fixed, "traveling": travel},
    }
    return doc


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    ap.add_argument("--tsv", default=None)
    ap.add_argument("--dist", default=None)
    ap.add_argument("--only", default=None, help="currency EDID or slug")
    a = ap.parse_args(argv)
    channel = "pts" if a.pts else "live"
    tsv_root = a.tsv or os.path.join(REPO, "tsv", "pts" if a.pts else "")
    dist = a.dist or os.path.join(REPO, "dist", "pts" if a.pts else "")
    global _TIER_D
    # dist is also where the other builders' outputs are read from
    # (challenges, treasure maps, collectrons) - the same channel's copy.
    D = Data(tsv_root.rstrip("/\\"), dist, channel)
    _TIER_D = D
    global PAGES
    pages = PAGES = load_pages()
    limits = load_limits()
    outdir = os.path.join(dist, "currency_guides")
    os.makedirs(outdir, exist_ok=True)
    index = []
    for cur in currency_list(D, pages):
        if a.only and a.only not in (cur["edid"], slugify(cur["name"])):
            continue
        if cur["edid"] in PARKED:
            print(f"  {cur['name']:<22} parked - not built")
            continue
        doc = build_one(D, cur, pages, limits)
        doc["channel"] = channel
        path = os.path.join(outdir, doc["slug"] + ".json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=1)
        s = doc["sources"]
        print(f"  {doc['name']:<22} page={'yes' if doc['has_page'] else 'NO '} max={doc['limits']['max_limit']} "
              f"events={len(s['events_activities'])} REs={len(s['random_encounters'])} "
              f"chal={len(s['challenges'])} cont={len(s['containers'])} crea={len(s['creatures'])} "
              f"maps={len(s['treasure_maps'])} vendors={len(doc['vendors']['fixed'])}+{len(doc['vendors']['traveling'])}")
        index.append({"slug": doc["slug"], "name": doc["name"], "edid": cur["edid"],
                      "has_page": doc["has_page"], "parked": doc["parked"]})
    for p in pages:
        if not any(i["has_page"] and i["slug"] == p["slug"] for i in index) and \
                not match_page([p], [c["name"] for c in D.currencies.values() if c["edid"] in PARKED]):
            print(f"  NOTE: page '{p['title']}' matches no currency record in the game files")
    with open(os.path.join(outdir, "index.json"), "w", encoding="utf-8") as fh:
        json.dump({"channel": channel, "currencies": index}, fh, ensure_ascii=False, indent=1)
    print(f"[currency] wrote {len(index)} docs -> {outdir}")


if __name__ == "__main__":
    main()
