#!/usr/bin/env python3
r"""
build_hideouts_json.py - dist/hideouts/hideouts.json for the Hideouts category
(World Pets / Hideouts PTS, Oct 2026; Hideouts planned for 2027).

Feeds df-bnb-hideouts.js on four pages:
  /df/hideouts/hideouts-guide/       how runs work, difficulties, bonus objectives
  /df/hideouts/hideout-leads/        where each lead drops + Tinker's Workbench recipes
  /df/hideouts/hideout-locations/    every Hideout with a map marker
  /df/hideouts/hideout-all-rewards/  completion rewards, Fallen Deputy's Packs,
                                     Hideout Caches, The Drifter
(Boss stats are their own page: build_boss_stats_json.py -> boss_stats/hideouts.json.)

EVERY RATE COMES FROM rng76 (drop-rate-engine skill §12): pools are resolved with
Rng76Resolver.resolve_deep on the reward list, and lead sources with appearance_prob.
GMRW chance conditions are not baked in (§7) - the page states them.

Inputs - all from exports, nothing typed per item:
  GMRW_Export      HIDE_QuestReward_GMRW (completion + bonus objective rows, keyed by
                   quest stage: 1000/1010/1020/1030 = difficulty), Drifter GMRWs,
                   HIDE_QuestReward_Intro_CantonBreadcrumb_GMRW
  BOOK_Export      HIDELead* (the four leads, FULL = "Hideout Lead: <Difficulty>")
  COBJ_Export      HIDE_co_Lead_* (Tinker's Workbench recipes, FVPA components)
  LVLI exports     reward lists + HIDE_LL_*_Leads source lists (via rng76)
  GLOB / CURV      caps values and XP curves
  QUEST_Export     intro quest name ("Chasing A Lead")
  FLST_Entries     HIDELocationFormlist (every Hideout, incl. ones with no marker yet)
  data/hideouts/containers_<channel>.tsv  CONT -> inventory LVLI (Hideout Cache,
                   Drifter Loot Bag) - the CONT export has no inventory column, so
                   this is read once from the ESM (esm notes in that file's header).
  data/hideouts/locations_<channel>.tsv   map markers (esm_scan_placements.py --names)

Channel: DFBNB_CHANNEL=pts reads tsv/pts and the *_pts data files. Writes
OUT_DIR/hideouts/hideouts.json (default dist/); the PTS workflow relocates to dist/pts/.
If the channel has no Hideout data (live, today) it writes nothing and exits 0.
"""
import csv, json, os, re, sys, datetime
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import tsv_source
from rng76 import Rng76Data, prettify_lvli_label, parse_randompercent_multiplier

csv.field_size_limit(10 ** 9)
CHANNEL = tsv_source.channel_of()
OUT_DIR = os.environ.get("OUT_DIR", os.path.join(REPO, "dist"))
DATA = os.path.join(REPO, "data", "hideouts")
IMAGE_BASE = "/wp-content/uploads/guide-images/hideouts/"
TSV_ROOT = os.path.join(REPO, "tsv", "pts") if CHANNEL == "pts" else os.path.join(REPO, "tsv")
# Quest stage -> difficulty key, as HIDE_QuestReward_GMRW conditions use them.
STAGE_DIFF = {"1000": "Easy", "1010": "Medium", "1020": "Hard", "1030": "Marked"}
DIFF_ORDER = ["Easy", "Medium", "Hard", "Marked"]
# The stage the completion row carries is the difficulty; any second stage on a
# pack row is the bonus objective that paid it out.
_LEG = re.compile(r"LegendaryItems_(?:Drifter_)?(\w+?)_Rank(\d)$")
_KIND = {"Armor": "Armour", "PowerArmor": "Power Armour", "Weapons_Melee": "Melee Weapon",
         "Weapons_Ranged": "Ranged Weapon", "Weapons": "Weapon", "AllItems": "Item"}


def rows(pattern, exclude=None):
    p = tsv_source.newest(pattern, channel=CHANNEL, exclude=exclude, required=False)
    if not p:
        return []
    with open(p, encoding="utf-8", errors="replace") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def ref_parts(s):
    """'0094F836:HIDE_LL_QuestRewards_Easy:LVLI' -> (fid, edid, sig)."""
    p = (s or "").split(":")
    return (p[0], p[1] if len(p) > 1 else "", p[2] if len(p) > 2 else "") if p and p[0] else ("", "", "")


def item_label(name, edid):
    m = _LEG.search(edid or "")
    if m:   # drop-rate-engine §16: star before "Legendary"
        kind = _KIND.get(m.group(1), re.sub(r"(?<!^)(?=[A-Z])", " ", m.group(1).replace("_", " ")))
        return f"{m.group(2)}★ Legendary {kind}"
    return name or edid


def clean_label(edid):
    """Readable pool name from a list EDID (display only; the EDID stays in the JSON)."""
    s = re.sub(r"^(?:HIDE_|SDOW_|RA_|BURN_|Burn_)+", "", edid or "")
    s = re.sub(r"^(?:cr)?LL[SDCIE]?_", "", s)
    s = re.sub(r"^(?:Drifter_Rewards_|Rewards_(?:PublicEvents_|Activities_|General_)?|Loot_|Systemic_Rewards_|DailyOps_)", "", s)
    s = re.sub(r"_(?:All|List|Normal|Common)$", "", s)
    words = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s.replace("_", " ")).split()
    fix = {"Lli": "", "Lls": "", "Ll": "", "LL": "", "Bobbleheadbox": "Bobblehead Box",
           "Treasurynotes": "Treasury Notes", "Legendaryitems": "Legendary Items", "Improvedbait": "Improved Bait"}
    words = [fix.get(w, w) for w in words]
    return " ".join(w for w in words if w).strip() or edid


def merge_items(items):
    """Same item listed by several entries of one list -> one row, rates added
    (they are competing outcomes of the same roll), quantities kept as a range."""
    out = {}
    for i in items:
        k = i["name"]
        if k in out:
            o = out[k]
            o["dropRate"] = round(o["dropRate"] + i["dropRate"], 2)
            o["qtyMin"] = min(o["qtyMin"], i["qty"] or 1); o["qtyMax"] = max(o["qtyMax"], i["qty"] or 1)
        else:
            out[k] = dict(i, qtyMin=i["qty"] or 1, qtyMax=i["qty"] or 1)
    return sorted(out.values(), key=lambda i: (-i["dropRate"], i["name"]))


def fmt(x):
    return round(float(x) * 100, 2)


class Engine:
    def __init__(self):
        self.d = Rng76Data.from_tsv_root(TSV_ROOT)
        self.R = self.d.resolver

    def name(self, fid, edid=""):
        return self.d.names.resolve(fid, edid) or edid

    def pools(self, list_fid):
        """One pool per top-level entry of list_fid (grouping only). Each item's
        number is rng76's appearance_prob(list_fid, item): the chance the item is
        in the haul at least once when the WHOLE reward list is rolled once - the
        question a reader asks, and safe for independent (Use All) and exclusive
        (pick-one / waterfall) lists alike. Repeated entries for one item become
        one row with a quantity range."""
        refs, order = Counter(), []
        for e in self.d.lvli.entries_by_list.get(list_fid, []):
            fid, edid, sig = ref_parts(e.get("LVLO_Reference"))
            if not fid:
                continue
            if fid not in refs:
                order.append((fid, edid, sig))
            refs[fid] += 1
        out = []
        for fid, edid, sig in order:
            leaves = self.R.resolve_deep(fid) if sig == "LVLI" else [
                {"formid": fid, "name": self.name(fid, edid), "edid": edid, "qty": 1}]
            byf = {}
            for it in leaves:
                f = it["formid"].upper()
                q = it.get("qty") or 1
                if f not in byf:
                    byf[f] = {"formid": it["formid"], "name": item_label(it.get("name"), it.get("edid")),
                              "qtyMin": q, "qtyMax": q}
                else:
                    byf[f]["qtyMin"] = min(byf[f]["qtyMin"], q); byf[f]["qtyMax"] = max(byf[f]["qtyMax"], q)
            items = []
            for f, row in byf.items():
                raw = self.R.appearance_prob(list_fid, row["formid"])
                if raw > 1.0000001:
                    # A probability cannot pass 100%. This is an rng76 edge case (seen on
                    # the contextual-ammo lists, Oct 2026) - reported, NOT patched here:
                    # rng76 changes need Duchess's OK (drop-rate-engine skill).
                    print(f"  [rng76 >100%] {self.d.lvli.edid_for(list_fid)} -> {row['name']}: {raw:.4f} (shown as 100%)")
                    row["capped"] = True
                    raw = 1.0
                row["dropRate"] = fmt(raw)
                if row["dropRate"] > 0:
                    items.append(row)
            items.sort(key=lambda i: (-i["dropRate"], i["name"]))
            names = {i["name"] for i in items}
            label = next(iter(names)) if len(names) == 1 else clean_label(edid)
            out.append({"label": label + (f" (\u00d7{refs[fid]})" if refs[fid] > 1 else ""),
                        "edid": edid, "rolls": refs[fid], "items": items})
        return out


def glob_val(globs, ref):
    fid, edid, _ = ref_parts(ref)
    v = globs.get(edid)
    return int(round(v)) if v is not None else None


# ─────────────────────────────────────────────────────────────────────────────
# The Drifter — his own pages (Drifter Guide / Drifter All Rewards, Oct 2026)
# ─────────────────────────────────────────────────────────────────────────────
# Words only. Every ability below is listed ONLY when its record is in this
# build's exports (presence is data-driven); the sentence is ours, written from
# the record names / descriptions. "game" quotes the record's own text.
DRIFTER_ABILITIES = [
    ("Absorbing", ("HIDE_AbDrifter_AbsorbingBallisticSpell", "HIDE_AbDrifter_AbsorbingEnergySpell",
                   "HIDE_AbDrifter_AbsorbingFireSpell", "HIDE_AbDrifter_AbsorbingMeleeSpell"),
     "Switches on an absorbing effect against one damage type at a time: Ballistic, Energy, Fire or Melee. "
     "Change to a different damage type while it is up."),
    ("Air tank weak point", ("HIDE_AbDrifter_WeakPointDamageEffect",),
     "The air tank on his back is a weak spot and can be blown up."),
    ("Air tank stagger", ("HIDE_AbDrifter_WeakPointStaggerEffect",),
     "Blowing the air tank staggers him, the one time he can be staggered."),
    ("Air tank frenzy", ("HIDE_AbDrifter_WeakPointFrenzyEffect",),
     "The air tank blast can frenzy its target for a short time."),
    ("Enraged", ("HIDE_AbDrifter_EnragedEffect",), "Goes into an enraged state."),
    ("Chameleon", ("HIDE_AbDrifter_ChameleonSpell", "HIDE_AbDrifter_ChamleonEffect"), "Can turn invisible."),
    ("Fortify Speed", ("HIDE_AbDrifter_FortifySpeedSpell",), "Moves faster than normal."),
    ("No stagger", ("HIDE_AbDrifter_NoStaggerPerk",), "Cannot be staggered by normal hits."),
    ("No crippled limbs", ("HIDE_AbDrifter_NoLimbCripplePerk",), "His limbs cannot be crippled."),
]
# Teleporting lives in combat-style (CSTY) and explosion records, which are not
# exported; confirmed by reading the PTS ESM directly (17 Oct 2026).
DRIFTER_ESM_ONLY = [("Teleport", "Teleports around the arena. How often depends on which of his six combat styles is active.")]
_POOL_LABEL_FIX = {"XPDThe Pitt": "The Pitt Rewards", "Unique Named Armor": "Unique Named Armour"}


def _decode_cond(c):
    """Raw CHAL condition 'flags|value|Func|..|..|Param|..|..|..|RunOn|' -> display line."""
    b = (c or "").split("|")
    if len(b) < 10 or not b[2]:
        return ""
    func, run = b[2], (b[9] or "").strip()
    if func in ("IsFalloutWorlds", "GetIsForm"):
        return ""
    param = re.sub(r'\s+"[^"]*"\s*', " ", b[5] or "").strip()
    try:
        v = float(b[1]); val = str(int(v)) if v.is_integer() else str(v)
    except ValueError:
        val = b[1]
    return f"{'' if run in ('', 'Subject') else run}.{func}({param}) = {val}"


def drifter_extras(drifter, globs, recipes, misc, locs, in_files):
    gv = globs.get
    lo, hi = gv("HIDE_Renorm_MinLVL_Drifter_Boss"), gv("HIDE_Renorm_MaxLVL_Drifter_Boss")
    drifter["levels"] = {"min": int(lo), "max": int(hi)} if lo and hi else None
    drifter["spawnChance"] = {k: round(gv(g), 2) for k, g in (("Hard", "HIDEDrifterChanceHardGlobal"),
                                                              ("Marked", "HIDEDrifterChanceMarkedGlobal")) if gv(g) is not None}
    drifter["randomEncounter"] = (gv("HIDE_LCP_Drifter_RE_Toggle") or 0) >= 1

    def words(t):
        return {w for w in re.findall(r"[a-z]+", re.sub(r"(?<=[a-z])(?=[A-Z])", " ", t).lower()) if len(w) >= 5}
    names = [l["name"] for l in locs] + list(in_files)
    spawn = []
    for r in rows("FLST_Export_*_Entries.tsv"):
        m = re.match(r"^HIDE\d+([A-Za-z]+?)_DrifterKeyword_FormList$", r.get("FLST_EDID") or "")
        if not m or m.group(1) == "Tunnel":        # HIDE02Tunnel is a test cell, not a Hideout
            continue
        tok = m.group(1)
        hit = next((n for n in names if words(n) & words(tok)), None)
        nm = hit or re.sub(r"(?<=[a-z])(?=[A-Z])", " ", tok)
        if not any(x["name"] == nm for x in spawn):
            spawn.append({"name": nm, "points": 0})
        next(x for x in spawn if x["name"] == nm)["points"] += 1
    drifter["spawnHideouts"] = spawn

    present = set()
    for pat, col in (("MGEF_Export_*.tsv", "EDID"), ("PERK_Export_*.tsv", "PERK_EDID"),
                     ("SPEL_Export_*_HEADER.tsv", "SPEL_EDID")):
        present.update((r.get(col) or "") for r in rows(pat))
    drifter["abilities"] = [{"name": n, "text": t, "records": [e for e in eds if e in present]}
                            for n, eds, t in DRIFTER_ABILITIES if any(e in present for e in eds)]
    drifter["abilities"] += [{"name": n, "text": t, "records": [], "esmOnly": True} for n, t in DRIFTER_ESM_ONLY]
    if gv("HIDE_Drifter_EnragedMagnitude") is not None:
        for a in drifter["abilities"]:
            if a["name"] == "Enraged":
                a["game"] = f"Damage <mag> HP (magnitude {int(gv('HIDE_Drifter_EnragedMagnitude'))})"
    for a in drifter["abilities"]:
        if a["name"] == "Air tank frenzy":
            a["game"] = "Frenzies Target For <DUR> Seconds."

    drifter["weapons"] = [{"name": r.get("WEAP_FULL"), "edid": r.get("WEAP_EDID"), "formid": r.get("WEAP_FormID")}
                          for r in rows("WEAP_Export_*_Base.tsv") if (r.get("WEAP_EDID") or "").startswith("HIDE_crDrifter")]
    drifter["weapons"].sort(key=lambda w: w["name"] or "")

    chals = []
    for r in rows("CHAL_Export_*.tsv"):
        if not (r.get("EDID") or "").startswith("HIDE_Challenge_Lifetime_Complete_Drifter_"):
            continue
        conds = [_decode_cond(r.get(f"Cond{i}")) for i in range(1, 30)]
        chals.append({"formId": r["FormID"], "edid": r["EDID"], "name": r.get("FULL") or "",
                      "required": int(float(r.get("TNAM") or 0)), "counter": r.get("SNAM") or "",
                      "scope": r.get("CNAM") or "Lifetime", "category": r.get("ENAM") or "",
                      "reward": (r.get("MNAM") or "").strip(),
                      "conditions_display": [c for c in conds if c and "TrueGlobal" not in c]})
    chals.sort(key=lambda c: c["required"])
    drifter["challenges"] = chals

    intel = misc.get("HIDE_DrifterIntel") or "Drifter Intel"
    uses = [r["makes"] for r in recipes if any(c["name"] == intel for c in r.get("components", []))]
    drifter["intel"] = {"name": intel, "craftChance": gv("HIDE_CriticalCrafting_DrifterIntel_Chance"),
                        "craftFrom": "Hideout Lead: Lethal", "usedIn": uses,
                        "recipes": [r for r in recipes if r["makes"] in uses]}

    for coll in (drifter.get("deathLoot") or [], (drifter.get("lootBag") or {}).get("pools") or []):
        for pl in coll:
            pl["label"] = _POOL_LABEL_FIX.get(pl["label"], pl["label"])
            tok = re.sub(r"^HIDE_crLLS_Drifter_Rewards_", "", pl.get("edid") or "")
            tg = next((g for g in globs if g.startswith(f"HIDE_LCP_Drifter_{tok}Reward") and g.endswith("_Toggle")), None)
            if tg:
                pl["toggle"] = {"global": tg, "on": globs[tg] >= 1}
    bag = drifter.get("lootBag") or {}
    bag["sameAsDeathLoot"] = bool(bag.get("pools")) and bag.get("pools") == drifter.get("deathLoot")
    drifter["setBonus"] = ({"name": "Secret Settler Armor Set Bonus",
                            "text": "Wearing the full Secret Settler set reflects ballistic damage back at whoever shot you."}
                           if "HIDE_SetBonus_SecretService_Drifter_ReflectivePerk" in present else None)


def main():
    gm_all = rows("GMRW_Export_*.tsv")
    gm = [g for g in gm_all if g.get("EDID", "").startswith("HIDE")]
    if not gm:
        print(f"[hideouts] no HIDE_ GMRW rows on {CHANNEL} - nothing to build.")
        return
    E = Engine()
    globs = {r["EDID"]: float(r["FLTV"]) for r in rows("GLOB_Export_*.tsv") if r.get("FLTV")}
    leads = {r["EDID"]: r for r in rows("BOOK_Export_*.tsv", exclude=("Locations",)) if r.get("EDID", "").startswith("HIDELead")}
    lead_by_diff = {}
    for edid, r in leads.items():
        k = edid.replace("HIDELead", "")
        lead_by_diff[k] = {"formid": r["FormID"], "edid": edid, "name": r.get("FULL", edid),
                           "difficulty": (r.get("FULL") or "").split(":")[-1].strip() or k}

    def xp_curve(ref):
        fid, edid, _ = ref_parts(ref)
        if not edid:
            return None
        pts = {L: E.d.curvs.interpolate(fid, L) for L in (50, 100, 200)}
        return {"curve": edid, "atLevel": {str(k): int(round(v)) for k, v in pts.items() if v is not None}}

    # Completion + bonus objective rows from HIDE_QuestReward_GMRW.
    diffs = {k: {"key": k, "name": (lead_by_diff.get(k) or {}).get("difficulty", k),
                 "lead": lead_by_diff.get(k), "completion": None, "pack": None} for k in DIFF_ORDER}
    pack_stages = {}
    for g in gm:
        if g["EDID"] != "HIDE_QuestReward_GMRW":
            continue
        stages = re.findall(r"GetStageDoneCurrentInstance\([^,]*,[^,]*, (\d+),", g.get("ConditionsFull") or g.get("Conditions") or "")
        diff = next((STAGE_DIFF[s] for s in stages if s in STAGE_DIFF), None)
        if not diff:
            continue
        fid, edid, sig = ref_parts(g.get("RewardedItem"))
        if sig == "LVLI":
            diffs[diff]["completion"] = {"lvli": edid, "caps": glob_val(globs, g.get("NAM8_CapsGlobal")),
                                         "xp": xp_curve(g.get("XPCT_XPCurveTable")), "pools": E.pools(fid)}
        elif sig == "ALCH":
            other = [s for s in stages if s not in STAGE_DIFF]
            pack_stages.setdefault(diff, set()).update(other)
            if not diffs[diff]["pack"]:
                loot = next((f for f, e in E.d.lvli.edid_by_formid.items()
                             if e == edid.replace("HIDE_BloodyRewardPack_", "HIDE_BloodyRewardPackLoot_")), None)
                diffs[diff]["pack"] = {"formid": fid, "edid": edid, "name": E.name(fid, edid),
                                       "lootList": E.d.lvli.edid_for(loot) if loot else "",
                                       "pools": E.pools(loot) if loot else []}
            xp = xp_curve(g.get("XPCT_XPCurveTable"))
            if xp:
                diffs[diff]["pack"].setdefault("xpOnStage", {})[",".join(other)] = xp
    for k, st in pack_stages.items():
        if diffs[k]["pack"]:
            diffs[k]["pack"]["bonusObjectiveStages"] = sorted(st)

    # Hideout Caches + Drifter Loot Bag (CONT inventory from data/hideouts).
    cont = {}
    cp = os.path.join(DATA, f"containers_{CHANNEL}.tsv")
    if os.path.exists(cp):
        with open(cp, encoding="utf-8") as fh:
            for r in csv.DictReader((l for l in fh if not l.startswith("#")), delimiter="\t"):
                cont[r["CONT_EDID"]] = r
    for k in DIFF_ORDER:
        c = cont.get(f"HIDE_BonusContainer_{k}")
        diffs[k]["cache"] = ({"name": c["CONT_FULL"], "edid": c["CONT_EDID"], "lootList": E.d.lvli.edid_for(c["LVLI_FormID"]),
                              "pools": E.pools(c["LVLI_FormID"])} if c else None)

    # The Drifter.
    drifter = {"deathLoot": [], "lootBag": None, "questReward": None, "challengeReward": None, "plans": []}
    dl = next((f for f, e in E.d.lvli.edid_by_formid.items() if e == "HIDE_crLLD_Drifter_Rewards"), None)
    if dl:
        drifter["deathLoot"] = E.pools(dl)
    bag = cont.get("HIDE_Lootbag_Drifter")
    if bag:
        drifter["lootBag"] = {"name": bag["CONT_FULL"], "lootList": E.d.lvli.edid_for(bag["LVLI_FormID"]),
                              "pools": E.pools(bag["LVLI_FormID"])}
    for g in gm:
        if g["EDID"] == "HIDE_QuestReward_CapsAndXP_Drifter_GMRW":
            drifter["questReward"] = {"caps": glob_val(globs, g.get("NAM8_CapsGlobal")),
                                      "xp": glob_val(globs, g.get("NAM7_XPGlobal"))}
        elif g["EDID"] == "HIDE_ChallengeReward_Challenge_Lifetime_Drifter_GMRW" and not drifter["challengeReward"]:
            lg = ref_parts(g.get("QRLI_LegendaryItemRewardList"))
            rank = g.get("QRLR_LegendaryItemRewardRank") or ""
            drifter["challengeReward"] = {"legendaryList": lg[1], "rank": int(rank) if rank.isdigit() else None,
                                          "label": f"{rank}★ Legendary Item" if rank.isdigit() else lg[1]}
    drifter["plans"] = sorted({r.get("FULL") for r in rows("BOOK_Export_*.tsv", exclude=("Locations",))
                               if (r.get("EDID") or "").startswith("HIDE_Recipe_") and r.get("FULL")})

    # Leads: where each one drops (rng76 appearance_prob on every list that holds a
    # HIDE_LL_*_Leads list) + the Tinker's Workbench recipes.
    lead_sources = []
    for fid, edid in sorted(E.d.lvli.edid_by_formid.items(), key=lambda kv: kv[1]):
        if not (edid.startswith("HIDE_LL_") and edid.endswith("_Leads")) and edid != "HIDE_LL_CriticalCrafting_HardLead":
            continue
        inner = [ref_parts(e.get("LVLO_Reference")) for e in E.d.lvli.entries_by_list.get(fid, [])]
        targets = sorted({f for f, _, s in inner if s in ("BOOK", "MISC")})
        parents = []
        for p in sorted(E.d.lvli.parent_map.get(fid.upper(), set())):
            pe = E.d.lvli.edid_for(p)
            if pe.startswith("HIDE_"):
                continue
            rate = max((E.R.appearance_prob(p, t) for t in targets), default=0)
            parents.append({"edid": pe, "label": clean_label(pe), "rate": fmt(rate), "via": "loot list"})
        # quest rewards (GMRW) that pay this lead list out directly
        for g in gm_all:
            gf, ge, gs = ref_parts(g.get("RewardedItem"))
            if gf.upper() != fid.upper():
                continue
            conds = g.get("ConditionsFull") or g.get("Conditions") or ""
            parents.append({"edid": g.get("EDID", ""), "label": (g.get("ParentQuestDisplay") or g.get("EDID", "")).strip(),
                            "rate": fmt(max((E.R.appearance_prob(fid, t) for t in targets), default=0)),
                            "gmrwChance": round(parse_randompercent_multiplier(conds) * 100, 2), "via": "quest reward"})
        lead_sources.append({"list": edid, "label": clean_label(edid),
                             "gives": [E.name(t) for t in targets], "parents": parents})
    misc = {r.get("EDID"): r.get("FULL") for r in rows("MISC_Export_*.tsv", exclude=("Refs",)) if r.get("FULL")}
    recipes = []
    for r in rows("COBJ_Export_*.tsv"):
        if (r.get("COBJ_EDID") or "").startswith("HIDE_co_Lead_"):
            comps = []
            for part in (r.get("FVPA") or "").split("|"):
                bits = part.split(":")
                if len(bits) >= 2 and bits[0]:
                    nm = ({"Caps001": "Caps"}.get(bits[0]) or (leads.get(bits[0]) or {}).get("FULL")
                          or misc.get(bits[0]) or E.name("", bits[0]) or bits[0])
                    comps.append({"name": nm, "qty": int(float(bits[1] or 0))})
            recipes.append({"makes": r.get("CNAM_FULL"), "workbench": "Tinker's Workbench" if "Tinkers" in (r.get("BNAM_EDID") or "") else r.get("BNAM_EDID"),
                            "components": comps})
    recipes.sort(key=lambda r: DIFF_ORDER.index(next((k for k, v in lead_by_diff.items() if v["name"] == r["makes"]), "Marked")))

    # Locations.
    locs, have = [], set()
    lp = os.path.join(DATA, f"locations_{CHANNEL}.tsv")
    if os.path.exists(lp):
        with open(lp, encoding="utf-8") as fh:
            for r in csv.DictReader(fh, delimiter="\t"):
                if r.get("name") in ("Vault 63",):      # the vault itself, not a Hideout
                    continue
                locs.append({"name": r["name"], "region": r["region"], "nearMarker": r["marker"],
                             "coords": [round(float(r["X"]), 1), round(float(r["Y"]), 1)],
                             # drawn by render_hideout_maps.py, uploaded under these names
                             "map": IMAGE_BASE + re.sub(r"[^a-z0-9]+", "-", r["name"].lower()).strip("-") + "-map.jpg"})
                have.add(r["name"].lower())
    in_files = []
    flp = tsv_source.newest("FLST_Export_*_Entries.tsv", channel=CHANNEL, required=False)
    if flp:
        with open(flp, encoding="utf-8", errors="replace") as fh:
            for r in csv.DictReader(fh, delimiter="\t"):
                if r.get("FLST_EDID") == "HIDELocationFormlist":
                    full = (r.get("Entry_FULL") or "").strip()
                    if full and not full.isupper():
                        in_files.append(full)
    def _words(t):
        return {w for w in re.findall(r"[a-z]+", t.lower()) if len(w) >= 5}
    # A Hideout counts as mapped when it shares a real word with a marker name
    # (LCTN "Immortality Center" = marker "Vault 63 Immortality Sector").
    unmapped = [n for n in in_files if not any(_words(n) & _words(h) for h in have)]

    drifter_extras(drifter, globs, recipes, misc, locs, in_files)

    intro = next((g for g in gm if g["EDID"] == "HIDE_QuestReward_Intro_CantonBreadcrumb_GMRW"), None)
    intro_q = next((q.get("FULL - Name") for q in rows("QUEST_Export_*.tsv") if q.get("EDID") == "HIDE_Intro_CantonBreadcrumb"), "")
    doc = {
        "_meta": {"generated": datetime.date.today().isoformat(), "channel": CHANNEL,
                  "isPts": CHANNEL == "pts", "engine": "rng76"},
        "difficulties": [diffs[k] for k in DIFF_ORDER],
        "drifter": drifter,
        "leads": {"items": [lead_by_diff[k] for k in DIFF_ORDER if k in lead_by_diff],
                  "sources": lead_sources, "recipes": recipes,
                  "drifterIntel": misc.get("HIDE_DrifterIntel") or E.name("00948FB6", "HIDE_DrifterIntel")},
        "introQuest": {"name": intro_q, "caps": glob_val(globs, intro.get("NAM8_CapsGlobal")) if intro else None,
                       "xp": xp_curve(intro.get("XPCT_XPCurveTable")) if intro else None},
        "locations": locs, "locationsWithoutMarker": unmapped,
        "fullMap": IMAGE_BASE + "hideout_locations.jpg",
    }
    out = os.path.join(OUT_DIR, "hideouts")
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "hideouts.json"), "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=False)
    print(f"hideouts.json: {sum(1 for d in doc['difficulties'] if d['completion'])} completion tiers, "
          f"{sum(1 for d in doc['difficulties'] if d['pack'])} packs, {sum(1 for d in doc['difficulties'] if d['cache'])} caches, "
          f"{len(lead_sources)} lead lists, {len(recipes)} recipes, {len(locs)} locations (+{len(unmapped)} unmapped)")


if __name__ == "__main__":
    main()
