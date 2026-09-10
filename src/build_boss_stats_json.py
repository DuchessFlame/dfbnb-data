#!/usr/bin/env python3
"""
build_boss_stats_json.py — Boss Stats pages (Infestations / Daily Ops / Head Hunts)

Builds the data behind three flat written-guide pages on buffsnbrew.com:

  /df/infestations/infestation-boss-stats/          -> dist/boss_stats/infestations.json
  /df/daily-ops/daily-ops-boss-stats/                -> dist/boss_stats/daily_ops.json
  /df/bounty-hunting/head-hunts/head-hunt-groups/    -> dist/boss_stats/head_hunts.json
                                                        (page title: Head Hunt Boss Stats)

Every page has the same layout, rendered by df-bnb-boss-stats.js:
  Boss Profiles · Boss Mutations · Boss Mechanics · Health Comparison ·
  Damage Comparison · Support Creatures · NPCs

EVERYTHING NUMERIC IS READ FROM THE TSV EXPORTS so the pages follow each patch
and PTS build without a code change:

  NPC_Export_*.tsv          name, race, level-range globals (AJNG / AJXG), template
  NPC_Export_*_PRPS.tsv     Health / resistance / speed properties and their curves
  NPC_Export_*_Refs.tsv     template usage, LVLN role lists (Boss / Helper / mob),
                            WAVE membership (Head Hunt gang members)
  GLOB_Export_*.tsv         Renorm_MinLVL_* / Renorm_MaxLVL_* level-range values
  CURV_Export_*_POINTS.tsv  health / armour / damage curve tables (value at a level)
  WEAP_Export_*_DNAM.tsv    boss weapon name, ammo, range, damage curve
  LVLI_Export_*_Entries.tsv boss grenade lists
  FLST_Export_*_Entries.tsv mutation pools, faction lists, Daily Ops enemy families
  SPEL_Export_*_HEADER.tsv  mutation names + one-line descriptions
  KYWD_Export_*.tsv         Daily Ops enemy-family names
  dist/bounty-hunting/head_hunt_bosses.json   Head Hunt groups, weapons, abilities
                                              (built by build_head_hunt_bosses_json.py)

What is NOT in any export is kept small and explicit below (MUTATION_DETAIL,
MECHANICS_NOTES, page intros). Those are words, never numbers.

Usage:
  python src/build_boss_stats_json.py          # live  -> dist/boss_stats/
  python src/build_boss_stats_json.py --pts    # PTS twin: tsv/pts/ (falls back to
                                               # live per record type) -> dist/pts/boss_stats/

In dfbnb-pts-build.yml the tsv/pts exports are normalised into tsv/ and the
whole dist/ tree is relocated to dist/pts/ afterwards, so that workflow runs the
plain (non --pts) form with DFBNB_PTS_LABEL=1 (labels the output as PTS without
changing where it is written).
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from bisect import bisect_left
from datetime import datetime, timezone
from pathlib import Path

import tsv_source

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
CHANNEL = tsv_source.channel_of()
PTS = CHANNEL == "pts"
OUT_DIR = ROOT / "dist" / ("pts/boss_stats" if PTS else "boss_stats")
# dfbnb-pts-build.yml normalises tsv/pts into tsv/ and relocates dist/ -> dist/pts/
# afterwards, so it runs the plain form; DFBNB_PTS_LABEL=1 still stamps the
# output as PTS so the page shows its PTS banner.
PTS_LABEL = PTS or os.environ.get("DFBNB_PTS_LABEL", "").strip() == "1"

USED: dict[str, str] = {}          # record type -> file actually read (stamped into output)


# ─────────────────────────────────────────────────────────────────────────────
# TSV access
# ─────────────────────────────────────────────────────────────────────────────

def pick(label: str, pattern: str, exclude=None, required=True):
    """Newest export on this channel; PTS falls back to the live export per type
    (a PTS page showing live data is merely behind — never the other way round)."""
    hit = None
    if PTS:
        hit = tsv_source.newest(pattern, channel="pts", exclude=exclude, required=False)
    if not hit:
        hit = tsv_source.newest(pattern, channel="live", exclude=exclude, required=required)
    if hit:
        USED[label] = os.path.relpath(hit, ROOT).replace("\\", "/")
    return hit


def read_rows(path, keep=None):
    """List of dicts. `keep` limits the columns held in memory (GLOB is ~35MB)."""
    if not path:
        return []
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, newline="", encoding=enc) as fh:
                rd = csv.reader(fh, delimiter="\t")
                head = next(rd)
                idx = [(i, h) for i, h in enumerate(head) if keep is None or h in keep]
                out = []
                for row in rd:
                    out.append({h: (row[i] if i < len(row) else "") for i, h in idx})
                return out
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Could not decode {path}")


def fnum(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def clean_edid(ref: str) -> str:
    """'CT_Foo [CURV:0076E9BB]' / 'Foo "Bar" [AMMO:..]' -> 'CT_Foo'."""
    return (ref or "").split(" ", 1)[0].strip()


def quoted_name(ref: str) -> str:
    m = re.search(r'"([^"]+)"', ref or "")
    return m.group(1) if m else ""


# ─────────────────────────────────────────────────────────────────────────────
# Game data model
# ─────────────────────────────────────────────────────────────────────────────

class Game:
    def __init__(self):
        print(f"[Boss Stats] channel={CHANNEL}")
        self.npc = {r["EDID"]: r for r in read_rows(
            pick("NPC", "NPC_Export_*.tsv", exclude=("PRPS", "Refs")))}
        self.prps: dict[str, dict[str, dict]] = {}
        for r in read_rows(pick("NPC_PRPS", "NPC_Export_*_PRPS.tsv")):
            self.prps.setdefault(r["NPC_EDID"], {})[r["ActorValue_EDID"]] = r
        # refs_to[X] = [(RefBy_EDID, sig)] ; refs_by[Y] = [(X, sig)] i.e. what Y references
        self.refs_to: dict[str, list] = {}
        self.refs_by: dict[str, list] = {}
        for r in read_rows(pick("NPC_Refs", "NPC_Export_*_Refs.tsv")):
            self.refs_to.setdefault(r["NPC_EDID"], []).append((r["RefBy_EDID"], r["RefBy_Signature"]))
            self.refs_by.setdefault(r["RefBy_EDID"], []).append((r["NPC_EDID"], r["RefBy_Signature"]))
        self.glob = {r["EDID"]: fnum(r["FLTV"]) for r in read_rows(
            pick("GLOB", "GLOB_Export_*.tsv"), keep={"EDID", "FLTV"})}
        self.curves: dict[str, list] = {}
        for r in read_rows(pick("CURV_POINTS", "CURV_Export_*_POINTS.tsv"),
                           keep={"EDID", "X", "Y"}):
            self.curves.setdefault(r["EDID"], []).append((fnum(r["X"]), fnum(r["Y"])))
        for k in self.curves:
            self.curves[k].sort()
        self.weap = {r["WEAP_EDID"]: r for r in read_rows(pick("WEAP_DNAM", "WEAP_Export_*_DNAM.tsv"))}
        self.lvli: dict[str, list] = {}
        for r in read_rows(pick("LVLI_Entries", "LVLI_Export_*_LVLI_Entries.tsv"),
                           keep={"LVLI_EDID", "LVLO_Reference"}):
            self.lvli.setdefault(r["LVLI_EDID"], []).append(r["LVLO_Reference"])
        self.flst: dict[str, list] = {}
        for r in read_rows(pick("FLST_Entries", "FLST_Export_*_Entries.tsv")):
            self.flst.setdefault(r["FLST_EDID"], []).append(r)
        for k in self.flst:
            self.flst[k].sort(key=lambda e: int(fnum(e.get("EntryIndex"), 0)))
        self.spel = {r["SPEL_EDID"]: r for r in read_rows(pick("SPEL_HEADER", "SPEL_Export_*_HEADER.tsv"))}
        self.kywd = {r.get("EDID", ""): r for r in read_rows(
            pick("KYWD", "KYWD_Export_*.tsv", exclude="Refs", required=False))}
        print(f"  NPC {len(self.npc)} · PRPS {len(self.prps)} · curves {len(self.curves)} · "
              f"WEAP {len(self.weap)} · FLST {len(self.flst)} · SPEL {len(self.spel)}")

    # ── curves ────────────────────────────────────────────────────────────
    def curve_at(self, name: str, x: float):
        pts = self.curves.get(name)
        if not pts:
            return None
        xs = [p[0] for p in pts]
        if x <= xs[0]:
            return pts[0][1]
        if x >= xs[-1]:
            return pts[-1][1]
        i = bisect_left(xs, x)
        (x0, y0), (x1, y1) = pts[i - 1], pts[i]
        if x1 == x0:
            return y1
        return y0 + (y1 - y0) * (x - x0) / (x1 - x0)

    # ── NPC helpers ───────────────────────────────────────────────────────
    def templates_of(self, edid: str) -> list[str]:
        """NPC templates this actor pulls from (Stats / Faction / SpellList ...)."""
        return [x for x, sig in self.refs_by.get(edid, []) if sig == "NPC_" and x in self.npc]

    def lists_containing(self, edid: str, sig: str) -> list[str]:
        return [x for x, s in self.refs_to.get(edid, []) if s == sig]

    def members_of(self, list_edid: str, sig: str) -> list[str]:
        return [x for x, s in self.refs_by.get(list_edid, []) if s == sig]

    def stats_source(self, edid: str) -> str:
        """The record whose PRPS actually drives the actor's stats: an attached
        '*Template_Stats*' template when there is one, else the actor itself,
        else the first NPC in its TPLT chain that carries PRPS."""
        for t in self.templates_of(edid):
            if "Template_Stats" in t:
                return t
        seen, cur = set(), edid
        while cur and cur not in seen:
            seen.add(cur)
            if cur in self.prps and "Health" in self.prps[cur]:
                return cur
            cur = (self.npc.get(cur) or {}).get("TPLT_EDID", "")
        return edid

    def level_range(self, edid: str):
        r = self.npc.get(edid) or {}
        lo = self.glob.get(r.get("AJNG_EDID", ""))
        hi = self.glob.get(r.get("AJXG_EDID", ""))
        if lo is None or hi is None:
            base = fnum(r.get("ACBS_Level"), 0)
            if base >= 1:
                lo = lo if lo is not None else base
                hi = hi if hi is not None else base
        if lo is None or hi is None:
            return None, None
        lo, hi = int(round(lo)), int(round(hi))
        return (min(lo, hi), max(lo, hi))

    def av(self, src: str, av: str, level):
        p = (self.prps.get(src) or {}).get(av)
        if not p:
            return None, ""
        curve = p.get("CurveTable_EDID", "")
        if curve:
            v = self.curve_at(curve, level) if level is not None else None
            return (round(v) if v is not None else None), curve
        return fnum(p.get("Value")), ""

    def display_name(self, edid: str) -> str:
        r = self.npc.get(edid) or {}
        if r.get("FULL"):
            return r["FULL"]
        # Unnamed leveled actor: borrow the name of the named Enc record(s) its
        # TPLT leveled list holds, else walk TPLT, else humanise the EDID.
        tplt = r.get("TPLT_EDID", "")
        if tplt and tplt not in self.npc:
            names = sorted({(self.npc.get(m) or {}).get("FULL", "") for m in self.members_of(tplt, "LVLN")} - {""})
            if names:
                return " / ".join(names)
        seen, cur = set(), tplt
        while cur and cur in self.npc and cur not in seen:
            seen.add(cur)
            if self.npc[cur].get("FULL") and "Template" not in self.npc[cur]["FULL"]:
                return self.npc[cur]["FULL"]
            cur = self.npc[cur].get("TPLT_EDID", "")
        return humanise(edid)

    def npc_stats(self, edid: str, extra_levels=()):
        """Profile block for one actor — every number from PRPS + CURV + GLOB."""
        r = self.npc.get(edid) or {}
        lo, hi = self.level_range(edid)
        src = self.stats_source(edid)
        hp_lo, curve = self.av(src, "Health", lo)
        hp_hi, _ = self.av(src, "Health", hi)
        out = {
            "edid": edid,
            "formId": r.get("FormID", ""),
            "name": self.display_name(edid),
            "race": (r.get("RNAM_EDID", "") or "").replace("Race", "").strip(),
            "levelMin": lo, "levelMax": hi,
            "hpMin": hp_lo, "hpMax": hp_hi,
            "healthCurve": curve,
            "statsFrom": src if src != edid else "",
        }
        res = {}
        for key, label in (("DamageResist", "DR"), ("EnergyResist", "ER"), ("FireResist", "Fire"),
                           ("FrostResist", "Cryo"), ("PoisonResist", "Poison"),
                           ("RadResistExposure", "Rad")):
            v, _c = self.av(src, key, hi)
            if v is not None:
                res[label] = v
        if res:
            out["resist"] = res
        spd, _ = self.av(src, "SpeedMult", hi)
        if spd:
            out["speedMult"] = spd
        er, _ = self.av(src, "EpicRankAV", hi)
        if er:
            out["epicRank"] = er
        return out

    # ── weapons ───────────────────────────────────────────────────────────
    def weapon(self, wedid: str, lo, hi):
        w = self.weap.get(wedid)
        if not w:
            return None
        curve = clean_edid(w.get("CVT0_DamageCurve", ""))
        base = fnum(w.get("DNAM_BaseDamage"))
        if curve and curve in self.curves:
            dmin = self.curve_at(curve, lo) if lo is not None else None
            dmax = self.curve_at(curve, hi) if hi is not None else None
        else:
            dmin = dmax = base or None
        ammo = quoted_name(w.get("DNAM_Ammo", "")) or ""
        name = (w.get("WEAP_FULL") or "").strip() or humanise(wedid)
        return {
            "edid": wedid,
            "name": name,
            "ammo": ammo,
            "range": round(fnum(w.get("DNAM_MaxRange"))),
            "type": w.get("DNAM_WeaponType", ""),
            "damageMin": round(dmin) if dmin else None,
            "damageMax": round(dmax) if dmax else None,
            "damageCurve": curve,
        }


def humanise(edid: str) -> str:
    s = re.sub(r"^(HTO_|POST_|SDOW_|Burn_|DailyOps_)", "", edid)
    s = re.sub(r"_(DailyOps|XPD)(?=_|$)", "", s)
    s = re.sub(r"^(Lvl|Enc|cr)", "", s, flags=re.I)
    s = s.replace("_", " ")
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)
    s = re.sub(r"\bSupermutant\b", "Super Mutant", s)
    s = re.sub(r"\bMolerat\b", "Mole Rat", s)
    return re.sub(r"\s+", " ", s).strip()


def merge_lr(weapons: list[dict]) -> list[dict]:
    """'Laser Gun [Left]' + 'Laser Gun [Right]' -> one 'Laser Gun (L/R)' row."""
    out, seen = [], {}
    for w in weapons:
        nm = re.sub(r"\bBoss\b\s*", "", w["name"]).strip()
        if not re.search(r"\[(Left|Right)\]|\b(Left|Right)\b", nm):
            out.append(dict(w, name=nm))
            continue
        base = re.sub(r"\s*\[(Left|Right)\]|\b(Left|Right)\b", "", nm)
        base = re.sub(r"\s+", " ", base).strip()
        if base in seen:
            continue
        w = dict(w, name=f"{base} (L/R)")
        seen[base] = w
        out.append(w)
    return out


def strongest(weapons):
    best = None
    for w in weapons or []:
        if w.get("type", "").lower().startswith("grenade"):
            continue
        if w.get("damageMax") and (best is None or w["damageMax"] > best["damageMax"]):
            best = w
    return best


def health_table(bosses, levels):
    """Rows grouped by health curve — bosses sharing a curve share a row."""
    groups: dict[str, dict] = {}
    for b in bosses:
        key = b.get("healthCurve") or f"flat:{b.get('hpMax')}"
        g = groups.setdefault(key, {"curve": b.get("healthCurve", ""), "bosses": [], "src": b})
        g["bosses"].append(b["name"])
    return list(groups.values())


def fmt_level_steps(lo, hi, step):
    if lo is None or hi is None:
        return []
    vals = list(range(int(lo), int(hi) + 1, step))
    if vals[-1] != hi:
        vals.append(int(hi))
    return vals


def build_health_section(game: Game, bosses, step, intro):
    # Columns follow the level range most bosses share; a boss outside it shows
    # "—" for the levels it never spawns at.
    ranges = [(b["levelMin"], b["levelMax"]) for b in bosses if b.get("levelMin") is not None]
    lo = hi = None
    if ranges:
        lo, hi = max(set(ranges), key=ranges.count)
    levels = fmt_level_steps(lo, hi, step)
    rows = []
    for g in health_table(bosses, levels):
        src = g["src"]
        vals = []
        for L in levels:
            if src.get("healthCurve"):
                v = game.curve_at(src["healthCurve"], L)
                in_range = src["levelMin"] is not None and src["levelMin"] <= L <= src["levelMax"]
                vals.append(round(v) if (v is not None and in_range) else None)
            else:
                vals.append(src.get("hpMax"))
        label = (f"All {len(g['bosses'])} bosses" if len(g["bosses"]) == len(bosses) and len(bosses) > 3
                 else ", ".join(g["bosses"]))
        rows.append({"label": label, "bosses": g["bosses"], "curve": g["curve"], "values": vals})
    return {"intro": intro, "levels": levels, "rows": rows}


def build_damage_section(bosses, intro):
    rows = []
    for b in bosses:
        w = strongest(b.get("weapons"))
        rows.append({
            "boss": b["name"],
            "weapon": w["name"] if w else "—",
            "levelMin": b.get("levelMin"), "levelMax": b.get("levelMax"),
            "damageMin": w.get("damageMin") if w else None,
            "damageMax": w.get("damageMax") if w else None,
        })
    rows.sort(key=lambda r: -(r["damageMax"] or 0))
    return {"intro": intro, "rows": rows}


def mutation_list(game: Game, flst_edid: str):
    out = []
    for e in game.flst.get(flst_edid, []):
        sp = e.get("Entry_EDID", "")
        s = game.spel.get(sp) or {}
        name = (s.get("SPEL_FULL") or e.get("Entry_FULL") or humanise(sp)).replace(" (Double)", "")
        out.append({
            "edid": sp,
            "name": name,
            "summary": (s.get("SPEL_DESC") or "").strip(),
            "detail": MUTATION_DETAIL.get(sp, ""),
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Words the exports do not carry (kept deliberately small — no numbers here
# except where the number is itself the mechanic's description)
# ─────────────────────────────────────────────────────────────────────────────

MUTATION_DETAIL = {
    "DailyOps_Mutation_ActiveCamouflage": "Invisible by default; becomes visible while firing or attacking.",
    "DailyOps_Mutation_DangerCloud": "Periodically releases a poison cloud that damages health and drains Action Points of players caught inside it.",
    "DailyOps_Mutation_FreezingTouch": "Melee hits trigger a cryo burst — Chilled, then Frozen on repeated hits, slowing you until you are immobilised.",
    "DailyOps_Mutation_ReflectiveSkin": "Reflects part of the damage it takes back to the attacker on a timed cycle. It glows briefly before the reflect window opens.",
    "DailyOps_Mutation_Resilient": "Cannot be killed by ranged damage — the final blow has to be a melee hit.",
    "DailyOps_Mutation_SavageStrike": "Bonus damage, ignores part of your damage resistance and has much tighter accuracy.",
    "DailyOps_Mutation_SwiftFooted": "Moves faster and reloads much quicker.",
    "DailyOps_Mutation_Volatile": "Explodes when killed — back off before the last hit lands.",
    "DailyOps_Mutation_ToxicBlood": "Leaves a poison hazard on the ground when killed.",
    "DailyOps_Mutation_GroupRegeneration": "Heals while grouped up with allies in close range — pull enemies apart.",
    "Burn_Bounty_Mutation_Chameleon": "Turns invisible while it stands still.",
    "Burn_Bounty_Mutation_UnstableIsotope": "Forced Unstable Isotope — can release a radiation burst when struck.",
    "Burn_Bounty_Mutation_PlagueWalker": "Emits a poison aura while it is sick.",
    "Burn_Bounty_Mutation_HerdMentality": "Weaker alone, stronger while grouped with its gang.",
    "Burn_Bounty_Mutation_FreezingTouch": "Melee hits slow the target down.",
    "Burn_Bounty_Mutation_ElectricallyCharged": "Can shock melee attackers.",
}

# Carried over from the hand-researched Infestations guide (Sept 2026). Words
# only — every stat on the card still comes from the exports.
HTO_BOSS_NOTES = {
    "Robot": {"weakSpot": "None — head and torso are armoured. Aim for the limbs.",
              "specials": [("Arm-mounted weapons", "Uses hard-wired creature weapons rather than a leveled weapon list, and fires the left and right arm weapons at the same time."),
                           ("Head Laser", "Constant-effect enchantment on the head laser."),
                           ("Lightning Strike", "Camera shake and stagger enchantments on hit.")]},
    "BloodEagle": {"weakSpot": "Head"},
    "PRCGhoul": {"weakSpot": "Head",
                 "specials": [("Communist T-60 Power Armor", "The only Infestation boss that wears Power Armor — a full five-piece Communist T-60 set.")]},
    "Cultist": {"weakSpot": "Head",
                "specials": [("Tesla Cannon", "Carries the BigStagger keyword and has a reload delay.")]},
    "MoleMiner": {"weakSpot": "Head"},
    "Scorched": {"weakSpot": "Head",
                 "specials": [("V63 Laser Carbine (Meltdown)", "Unique weapon — non-tradable, non-droppable."),
                              ("Holiday outfits", "Festive (Santa hat and beard) and Spooky (pumpkin head) variants, picked from a leveled outfit list.")]},
    "SuperMutant": {"weakSpot": "Head",
                    "specials": [("Cremator", "Unique fire weapon with a fire hit-effect enchantment and a faster reload."),
                                 ("Broadsider", "Single-shot cannonball launcher.")]},
}

HTO_MECHANICS = [
    ["Radiation immunity", "Immune to radiation damage"],
    ["Poison immunity", "Immune to poison damage"],
    ["Fall damage immunity", "Cannot take fall damage"],
    ["Stagger immunity", "Cannot be staggered"],
    ["Weapon selection", "Each boss equips one weapon from its faction pool per spawn (listed under Boss Profiles)"],
    ["Grenades", "Grenade use is enabled for every boss"],
]

INTRO = {
    "infestations": {
        "title": "Infestations - Infestation Boss Stats",
        "blurb": ("Every Infestation is led by one faction boss, backed by waves of mobs and support "
                  "creatures. This page lists each boss's weapons, health, damage and resistances, the "
                  "mutation pool they draw from, and the enemies that fight alongside them. All numbers "
                  "are read straight from the game files and update with every patch."),
        "profiles": "Each Infestation spawns one of the faction bosses below. The weapon rolled for a spawn comes from that boss's pool.",
        "mutations": "Each Infestation boss spawns with one randomly selected mutation from this pool.",
        "mechanics": "All Infestation bosses share the following traits.",
        "health": "Health is read from the curve table assigned to each boss, at the levels bosses spawn between. Bosses on the same curve share a row.",
        "damage": "Damage per hit from each boss's hardest-hitting weapon, at the bottom and top of its spawn level range.",
        "support": "Each faction also spawns support-tier creatures alongside its mobs — smaller enemies that complement the main force.",
        "npcs": "Each Infestation spawns waves of regular enemies (mobs) alongside the boss. The mob mix depends on which faction controls the Infestation.",
    },
    "daily_ops": {
        "title": "Daily Ops - Daily Ops Boss Stats",
        "blurb": ("Each Daily Op pits you against one rotating enemy family, and every family ends in a boss "
                  "fight. This page lists each family's boss and variant bosses, their weapons, health and "
                  "damage, the Daily Ops mutation pool, and the enemies that support them. All numbers are "
                  "read from the game files and update with every patch."),
        "profiles": "One enemy family is active per Op. Its boss and any variant bosses are listed below.",
        "mutations": "Every Daily Op applies one mutation to its enemies, drawn from the pool below. On Double Mutated days a combined mutation from the second list is used instead.",
        "mechanics": "Boss stats that apply across the Daily Ops enemy families.",
        "health": "Health is read from the curve table assigned to each boss across the Daily Ops boss level range.",
        "damage": "Damage per hit from each boss's hardest-hitting weapon, at the bottom and top of its level range.",
        "support": "Support creatures spawn alongside each family's humanoid enemies.",
        "npcs": "Regular enemies for each family.",
    },
    "head_hunts": {
        "title": "Bounty Hunting - Head Hunt Boss Stats",
        "blurb": ("Each Head Hunt boss comes with a distinct weapon setup and a signature mechanic, and the "
                  "gang they bring follows the same theme. This page lists every boss by group with their "
                  "weapons, health, damage and resistances, plus the gang members and support wave that "
                  "fight alongside them. Tick bosses off as you complete them."),
        "profiles": "Bosses are grouped the same way as the Head Hunt Groups lifetime challenges. Tick a boss when you have killed it — ticking every boss in a group completes the group.",
        "mutations": "Mutation effects used by bounty targets. One-star outlaws are named after theirs, and some Head Hunt bosses carry a forced mutation as part of their signature attack.",
        "mechanics": "Head Hunt bosses share the following stats.",
        "health": "Health is read from the curve table assigned to each boss across the bounty level range. Bosses on the same curve share a row.",
        "damage": "Damage per hit from each boss's main weapon at the bottom and top of the bounty level range.",
        "support": "Every Head Hunt also sends a support wave that fights alongside the boss and its gang.",
        "npcs": "Each boss brings its own gang members, listed with the boss they belong to.",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Infestations (HTO)
# ─────────────────────────────────────────────────────────────────────────────

# The PRC faction template is titled "(PRC)"; the site calls that faction
# Communists everywhere else (and the Daily Ops wave keyword agrees).
FACTION_DISPLAY = {"PRC": "Communists"}


def hto_faction_template(game: Game, boss_edid: str) -> str:
    for t in game.templates_of(boss_edid):
        if t.startswith("HTO_Template_Faction_"):
            return t
    return ""


def hto_faction_name(game: Game, template: str, token: str) -> str:
    """'HTO_Template_Faction_BloodEagle' FULL '(Blood Eagles]' -> 'Blood Eagles'."""
    r = game.npc.get(template) or {}
    m = re.search(r"[\(\[]([^\)\]]+)[\)\]]", r.get("FULL", ""))
    name = m.group(1).strip() if m else humanise(token)
    return FACTION_DISPLAY.get(name, name)


def hto_role(game: Game, edid: str) -> str:
    for lst in game.lists_containing(edid, "LVLN"):
        if lst.startswith("HTO_LChar_Faction_"):
            if lst.endswith("_Boss"):
                return "boss"
            if lst.endswith("_Helper"):
                return "support"
            return "mob"
    tpls = game.templates_of(edid)
    for pat in (r"HTO_Template_Stats_\w+?_(Boss|Mob|Support)$", r"HTO_Template_Keywords_(Boss|Mob|Support)$"):
        for t in tpls:
            m = re.match(pat, t)
            if m:
                return m.group(1).lower()
    return ""


def build_infestations(game: Game):
    # Faction tokens as the boss NPCs spell them: HTO_Lvl<Token>_Boss_T1
    tokens = sorted({m.group(1) for e in game.npc
                     for m in [re.match(r"^HTO_Lvl(\w+?)_Boss_T\d+$", e)] if m})
    bosses, support, npcs = [], [], []
    for tok in tokens:
        tier_edids = sorted(e for e in game.npc if re.match(rf"^HTO_Lvl{tok}_Boss_T\d+$", e))
        rep = tier_edids[0]
        ftpl = hto_faction_template(game, rep)
        faction = hto_faction_name(game, ftpl, tok)
        b = game.npc_stats(rep)
        b["faction"] = faction
        b["factionKey"] = tok
        b["tierRecords"] = len(tier_edids)
        notes = HTO_BOSS_NOTES.get(tok) or {}
        if notes.get("weakSpot"):
            b["weakSpot"] = notes["weakSpot"]
        if notes.get("specials"):
            b["specials"] = [{"name": n, "text": t} for n, t in notes["specials"]]
        variants = sorted({game.npc[e]["FULL"] for e in game.npc
                           if re.match(rf"^HTO_Lvl{tok}_Boss_T\d+_(?!Fallback)\w+$", e) and game.npc[e].get("FULL")})
        if variants:
            b["variants"] = variants
        weps = [game.weapon(w, b["levelMin"], b["levelMax"])
                for w in sorted(game.weap) if w.startswith(f"HTO_cr{tok}_Boss_")]
        b["weapons"] = merge_lr([w for w in weps if w])
        gren = []
        for ref in game.lvli.get(f"HTO_crLLI_{tok}_Boss_Grenade", []):
            wid = ref.split(":")[1] if ":" in ref else ""
            w = game.weap.get(wid)
            gren.append((w or {}).get("WEAP_FULL") or humanise(wid))
        if gren:
            b["grenades"] = gren
        bosses.append(b)

        # Mobs and support creatures: every leveled actor that uses this boss's
        # faction template (catches e.g. HTO_LvlPRC_Liberator beside HTO_LvlPRCGhoul_*).
        members = sorted({x for x, sig in game.refs_to.get(ftpl, []) if sig == "NPC_"} |
                         {e for e in game.npc if e.startswith(f"HTO_Lvl{tok}_")})
        seen_names = set()
        for e in members:
            if not e.startswith("HTO_Lvl") or "_Boss" in e or e not in game.npc:
                continue
            if re.search(r"_(Festive|Spooky)$", e):
                continue
            role = hto_role(game, e)
            if role not in ("mob", "support"):
                continue
            n = game.npc_stats(e)
            n["faction"] = faction
            n["role"] = role
            n["loadout"] = humanise(re.sub(r"^HTO_Lvl[A-Za-z]+?_", "", e))
            key = (n["name"], role)
            if key in seen_names:
                continue
            seen_names.add(key)
            (support if role == "support" else npcs).append(n)

    lvl_tiers = {}
    for group in (npcs, support):
        for n in group:
            lvl_tiers.setdefault(n["role"], set()).add((n["levelMin"], n["levelMax"]))

    mutations = mutation_list(game, "HTO_BossMutation_Valid_FormList")

    b0 = bosses[0] if bosses else {}
    mech_rows = [
        ["Spawn level range", f"{b0.get('levelMin')} – {b0.get('levelMax')}"],
    ] + [list(r) for r in HTO_MECHANICS]
    for role, label in (("mob", "Mob level range"), ("support", "Support level range")):
        rng = sorted(lvl_tiers.get(role, []), key=lambda t: (t[0] or 0))
        if rng:
            mech_rows.append([label, " / ".join(f"{a} – {c}" for a, c in rng)])
    spell = game.spel.get("HTO_crFortifyHealthBossSpell")
    if spell:
        mech_rows.append(["Scripted health", "Infestation actors also carry a scripted Fortify Health effect, so in-game health can differ from the curve value shown here"])

    return {
        "page": "infestations",
        "intro": INTRO["infestations"],
        "bosses": bosses,
        "mutations": {"intro": INTRO["infestations"]["mutations"], "list": mutations},
        "mechanics": {"intro": INTRO["infestations"]["mechanics"], "rows": mech_rows,
                      "resist": [{"name": b["name"], **(b.get("resist") or {})} for b in bosses]},
        "health": build_health_section(game, bosses, 25, INTRO["infestations"]["health"]),
        "damage": build_damage_section(bosses, INTRO["infestations"]["damage"]),
        "support": {"intro": INTRO["infestations"]["support"], "rows": support},
        "npcs": {"intro": INTRO["infestations"]["npcs"], "rows": npcs},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Daily Ops
# ─────────────────────────────────────────────────────────────────────────────

# Wave-keyword token -> tokens the NPC / WEAP EDIDs use for that family.
DO_TOKEN_ALIASES = {
    "SuperMutant": ["SuperMutant", "Supermutant"],
    "Robot": ["Robot", "Assaultron", "BotWeap"],
    "Fanatics": ["Fanatic"],
    "Zetan": ["Zetan", "Alien"],
}
DO_SKIP = re.compile(r"^(DEL_|DELETED_|zzz|ZZZ|DO_NOT)|Template|_Atlas$", re.I)
# Races that make an actor a support creature rather than a regular enemy.
CREATURE_RACES = re.compile(r"Dog|Hound|Molerat|Mothman|Floater|EyeBot|Wolf|Bloodbug|Cricket|"
                            r"Liberator|Bloatfly|RadAnt|Pollinator|Thorn|Drone", re.I)


def build_daily_ops(game: Game):
    # Enemy families = the wave keywords in any per-location encounter list.
    fam_keys: list[tuple[str, str]] = []
    for lst, entries in game.flst.items():
        if not lst.startswith("DailyOps_Encounter_FormList_"):
            continue
        for e in entries:
            k = e.get("Entry_EDID", "")
            if k.startswith("DailyOpsWaveKeyword_") and k not in [f[0] for f in fam_keys]:
                fam_keys.append((k, e.get("Entry_FULL") or (game.kywd.get(k) or {}).get("FULL", "")))

    do_npcs = [e for e in game.npc if "DailyOps" in e and not DO_SKIP.search(e)]
    bosses, support, npcs = [], [], []
    # Each actor belongs to the family whose token appears EARLIEST in its EDID
    # (POST_EncBloodEagleCultistBladeBoss is a Blood Eagle carrying a Cultist Blade).
    fam_alias = {kw: [a.lower() for a in DO_TOKEN_ALIASES.get(kw[len("DailyOpsWaveKeyword_"):],
                                                             [kw[len("DailyOpsWaveKeyword_"):]])]
                 for kw, _ in fam_keys}

    def owner(edid):
        low, best = edid.lower(), None
        for kw, al in fam_alias.items():
            pos = min((low.find(a) for a in al if a in low), default=-1)
            if pos >= 0 and (best is None or pos < best[0]):
                best = (pos, kw)
        return best[1] if best else None

    for kw, fam_name in fam_keys:
        tok = kw[len("DailyOpsWaveKeyword_"):]
        aliases = fam_alias[kw]
        members = [e for e in do_npcs if owner(e) == kw]
        boss_edids = sorted(e for e in members if "boss" in e.lower())
        other = [e for e in members if e not in boss_edids]
        fam_weps = sorted(w for w in game.weap
                          if "dailyops" in w.lower() and "boss" in w.lower() and not DO_SKIP.search(w)
                          and any(a in w.lower() for a in aliases))
        extra_weps = sorted(w for w in game.weap if "dailyops" in w.lower() and not DO_SKIP.search(w)
                            and "boss" not in w.lower() and any(a in w.lower() for a in aliases)
                            and tok in ("Robot",) and "Assaultron" in w)

        # One profile per distinct boss name. Prefer the Lvl… record (the one
        # the wave spawns); the Enc Tier01–03 records are its difficulty rungs.
        by_name: dict[str, list[str]] = {}
        for e in boss_edids:
            nm = game.npc[e].get("FULL") or ""
            if not nm:
                continue
            by_name.setdefault(nm, []).append(e)
        profiles = []
        for nm, eds in by_name.items():
            eds.sort(key=lambda x: (not x.startswith("Lvl"), x.startswith("POST_"), x))
            rep = eds[0]
            b = game.npc_stats(rep)
            b["faction"] = fam_name
            b["factionKey"] = tok
            b["variant"] = rep.startswith("POST_")
            b["records"] = len(eds)
            profiles.append(b)
        # Main boss first, variants after.
        profiles.sort(key=lambda b: (b["variant"], b["name"]))
        mains = [p for p in profiles if not p["variant"]] or profiles[:1]
        for w in fam_weps + extra_weps:
            wn = w.lower()
            target = None
            if w.startswith("POST_"):
                word = re.sub(r"^post_cr", "", wn)
                for a in aliases:
                    word = word.replace(a, "")
                word = word.replace("boss", "").replace("_dailyops", "")
                for p in profiles:
                    if not p["variant"]:
                        continue
                    m = re.match(r"post_enc(.+?)boss_dailyops", p["edid"].lower())
                    pw = m.group(1) if m else ""
                    for a in aliases:
                        pw = pw.replace(a, "")
                    if pw and (pw in word or pw.replace("nn", "n") in word):
                        target = p
                        break
            if target is None and not w.startswith("POST_"):
                target = mains[0] if mains else None
            if target is None:
                continue
            wo = game.weapon(w, target.get("levelMin"), target.get("levelMax"))
            if wo:
                target.setdefault("weapons", []).append(wo)
        for p in profiles:
            p["weapons"] = merge_lr(p.get("weapons", []))
        bosses.extend(profiles)

        seen = set()
        for e in sorted(other):
            n = game.npc_stats(e)
            if not n.get("hpMax") and not n.get("levelMax"):
                continue
            creature = bool(CREATURE_RACES.search(game.npc[e].get("RNAM_EDID", ""))) or "Creature" in e
            n["faction"] = fam_name
            n["role"] = "support" if creature else "mob"
            lo = humanise(e)
            words = [humanise(a) for a in DO_TOKEN_ALIASES.get(tok, [tok])] + [fam_name, "Creature", "PRC Ghoul", "PRC"]
            for a in words:
                lo = re.sub(r"\b" + re.escape(a) + r"\b", "", lo, flags=re.I)
            n["loadout"] = re.sub(r"\s+", " ", lo).strip() or "—"
            if not game.npc[e].get("FULL"):
                n["name"] = re.sub(r"^(Robot|PRC Creature|Mole Miner Creature|Scorched Creature)\s+", "", n["name"])
            key = (n["name"], n["role"], n["loadout"])
            if key in seen:
                continue
            seen.add(key)
            (support if creature else npcs).append(n)

    mut = mutation_list(game, "DailyOps_Mutation_Valid_FormList")
    dbl = mutation_list(game, "DailyOps_DoubleMutation_Mutation_Valid_FormList")

    main_bosses = [b for b in bosses if not b.get("variant")]
    rng = sorted({(b["levelMin"], b["levelMax"]) for b in main_bosses if b.get("levelMin") is not None})
    mech_rows = []
    if rng:
        mech_rows.append(["Boss level range", " / ".join(f"{a} – {c}" for a, c in rng)])
    mech_rows.append(["Enemy families", str(len(fam_keys))])
    mech_rows.append(["Mutation pool", f"{len(mut)} single mutations · {len(dbl)} double mutations"])

    return {
        "page": "daily_ops",
        "intro": INTRO["daily_ops"],
        "families": [{"key": k[len("DailyOpsWaveKeyword_"):], "name": n} for k, n in fam_keys],
        "bosses": bosses,
        "mutations": {"intro": INTRO["daily_ops"]["mutations"], "list": mut,
                      "doubleIntro": "Double Mutated days combine two effects into one of these.",
                      "double": dbl},
        "mechanics": {"intro": INTRO["daily_ops"]["mechanics"], "rows": mech_rows,
                      "resist": [{"name": b["name"], **(b.get("resist") or {})} for b in bosses]},
        "health": build_health_section(game, main_bosses or bosses, 20, INTRO["daily_ops"]["health"]),
        "damage": build_damage_section(bosses, INTRO["daily_ops"]["damage"]),
        "support": {"intro": INTRO["daily_ops"]["support"], "rows": support},
        "npcs": {"intro": INTRO["daily_ops"]["npcs"], "rows": npcs},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Head Hunts
# ─────────────────────────────────────────────────────────────────────────────

def load_head_hunt_json():
    cands = []
    if PTS:
        cands.append(ROOT / "dist" / "pts" / "bounty-hunting" / "head_hunt_bosses.json")
    cands.append(ROOT / "dist" / "bounty-hunting" / "head_hunt_bosses.json")
    for p in cands:
        if p.exists():
            USED["head_hunt_bosses"] = os.path.relpath(p, ROOT).replace("\\", "/")
            return json.loads(p.read_text(encoding="utf-8"))
    return None


def build_head_hunts(game: Game):
    hh = load_head_hunt_json() or {"groups": []}
    targets = {}
    for e, r in game.npc.items():
        if re.match(r"^Burn_BountyTarget_BIG_", e) and r.get("FULL") and not DO_SKIP.search(e):
            targets.setdefault(r["FULL"].strip().lower(), e)

    groups, all_bosses, gang_rows = [], [], []
    for g in hh.get("groups", []):
        gb = []
        for hb in g.get("bosses", []):
            e = targets.get(hb["name"].strip().lower())
            b = game.npc_stats(e) if e else {"name": hb["name"], "levelMin": None, "levelMax": None}
            b["name"] = hb["name"]
            b["gang"] = hb.get("gangName", "")
            b["imageSlug"] = hb.get("imageSlug", "")
            if hb.get("challenge"):
                b["challenge"] = hb["challenge"]
            weps = []
            for key, label in (("weapon", "Main"), ("meleeWeapon", "Secondary"), ("grenade", "Thrown")):
                w = hb.get(key)
                if not w:
                    continue
                wo = game.weapon(w.get("baseEdid", ""), b.get("levelMin"), b.get("levelMax")) or {}
                wo.update({"name": w.get("name"), "slot": label,
                           "type": "Grenade" if key == "grenade" else (wo.get("type") or w.get("weaponType", "")),
                           "ammo": wo.get("ammo") or quoted_name(w.get("ammo", "")),
                           "range": wo.get("range") or round(fnum(w.get("maxRange")))})
                if w.get("legendaryMods"):
                    wo["legendary"] = [f"{m.get('star')}★ {m.get('name')}" for m in w["legendaryMods"]]
                if w.get("customMods"):
                    wo["custom"] = [{"name": m.get("name"), "effect": m.get("effect", "")} for m in w["customMods"]]
                weps.append(wo)
            b["weapons"] = weps
            specials = []
            if hb.get("enchantment"):
                en = hb["enchantment"]
                specials.append({"name": en.get("name"), "text": " · ".join(
                    x.get("text") or x.get("effect", "") for x in en.get("effects", []))})
            if hb.get("weaponMod"):
                wm = hb["weaponMod"]
                specials.append({"name": wm.get("name"), "text": wm.get("desc", "")})
            if hb.get("specialAbility"):
                sa = hb["specialAbility"]
                txt = sa.get("description") or " · ".join(
                    (x.get("name", "") + (f" — {x['description']}" if x.get("description") else ""))
                    for x in sa.get("effects", []))
                specials.append({"name": sa.get("name"), "text": txt})
            if hb.get("note"):
                specials.append({"name": "Note", "text": hb["note"]})
            b["specials"] = specials

            # Gang members: every non-boss NPC on the same WAVE record(s).
            gang = []
            if e:
                for wave in game.lists_containing(e, "WAVE"):
                    for m in game.members_of(wave, "WAVE"):
                        if m == e or m in [x["edid"] for x in gang] or "BIG_" in m:
                            continue
                        n = game.npc_stats(m)
                        n["boss"] = hb["name"]
                        n["gang"] = b["gang"]
                        gang.append(n)
            b["gangMembers"] = [x["name"] for x in gang]
            gang_rows.extend(gang)
            gb.append(b)
            all_bosses.append(b)
        grp = {"groupNumber": g.get("groupNumber"), "label": g.get("label") or f"Head Hunter Group {g.get('groupNumber')}",
               "seasonal": bool(g.get("seasonal")), "bosses": gb}
        if g.get("metaChallenge"):
            grp["metaChallenge"] = g["metaChallenge"]
        if g.get("blurb"):
            grp["blurb"] = g["blurb"]
        groups.append(grp)

    # Support wave: the Rust Raider actors on the Head Hunt support keyword.
    support = []
    for e in sorted(game.npc):
        if e.startswith("Burn_LvlRustRaider") and not DO_SKIP.search(e):
            n = game.npc_stats(e)
            if not game.npc[e].get("FULL"):
                parts = e.split("_")
                cls = re.sub(r"\bRaider\b", "", humanise(parts[2] if len(parts) > 2 else "")).strip()
                n["name"] = f"Rust Raider ({cls})" if cls else "Rust Raider"
            support.append(n)

    # Mutations: forced NPC mutation spells + outlaw mutation effects.
    muts = []
    for sp in sorted(game.spel):
        if re.match(r"^Burn_Bounty_Mutation_", sp):
            s = game.spel[sp]
            muts.append({"edid": sp, "name": re.sub(r"^(NPC|Forced|Bounty)\s+", "", s.get("SPEL_FULL", "")) or humanise(sp),
                         "summary": (s.get("SPEL_DESC") or "").strip(), "detail": MUTATION_DETAIL.get(sp, "")})
    outlaws = sorted({(game.npc[e].get("FULL") or "") for e in game.npc
                      if e.startswith("Burn_BountyTarget_SML_") and re.search(r"_[MF]$", e)} - {""})

    real = [b for b in all_bosses if b.get("levelMin") is not None]
    b0 = real[0] if real else {}
    mech_rows = []
    if b0:
        mech_rows.append(["Bounty level range", f"{b0.get('levelMin')} – {b0.get('levelMax')}"])
        if b0.get("speedMult"):
            mech_rows.append(["Move speed", f"{int(b0['speedMult'])}% of normal"])
    mech_rows.append(["Bosses", f"{sum(1 for g in groups if not g['seasonal'] for _ in g['bosses'])} across {sum(1 for g in groups if not g['seasonal'])} groups"])
    if support:
        mech_rows.append(["Support wave", hh.get("sidekickType") or "Rust Raiders"])

    return {
        "page": "head_hunts",
        "intro": INTRO["head_hunts"],
        "storeKey": "dfbnb_hh_bosses_v1",
        "allChallenge": hh.get("allChallenge"),
        "groups": groups,
        "bosses": all_bosses,
        "mutations": {"intro": INTRO["head_hunts"]["mutations"], "list": muts,
                      "outlaws": outlaws},
        "mechanics": {"intro": INTRO["head_hunts"]["mechanics"], "rows": mech_rows,
                      "resist": [{"name": b["name"], **(b.get("resist") or {})} for b in real]},
        "health": build_health_section(game, real, 25, INTRO["head_hunts"]["health"]),
        "damage": build_damage_section(real, INTRO["head_hunts"]["damage"]),
        "support": {"intro": INTRO["head_hunts"]["support"], "rows": support},
        "npcs": {"intro": INTRO["head_hunts"]["npcs"], "rows": gang_rows},
    }


# ─────────────────────────────────────────────────────────────────────────────

def write(name: str, data: dict):
    data = {
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "channel": "pts" if PTS_LABEL else CHANNEL,
        "isPts": PTS_LABEL,
        "sources": dict(sorted(USED.items())),
        **data,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{name}.json"
    out.write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"  -> {out.relative_to(ROOT)}  ({out.stat().st_size:,} bytes)  "
          f"bosses={len(data.get('bosses', []))} support={len(data['support']['rows'])} "
          f"npcs={len(data['npcs']['rows'])} mutations={len(data['mutations']['list'])}")


def main():
    game = Game()
    hto = build_infestations(game)
    do = build_daily_ops(game)
    hh = build_head_hunts(game)
    # Guard: never overwrite a good page with an empty one.
    for name, d in (("infestations", hto), ("daily_ops", do), ("head_hunts", hh)):
        if not d["bosses"]:
            print(f"[Boss Stats] FATAL: {name} resolved 0 bosses — refusing to write.")
            sys.exit(1)
    write("infestations", hto)
    write("daily_ops", do)
    write("head_hunts", hh)


if __name__ == "__main__":
    main()
