#!/usr/bin/env python3
"""
bounty_trees.py - the Grunt Hunt reward tree, read from the game files (Nov 2026).

Both regions' Grunt Hunts hand out the same reward record,
QuestReward_BountyHuntingGruntHunt_Stage9000 [GMRW:007D6A81], whose leveled
list is BountyHunt_GruntHunt_LL_QuestRewards [LVLI:007D6A6D]. On the Skyline
Valley PTS that root became a region switch:

    007D6A6D  UseAll
      Legendary Modules 1-3
      ..._QuestRewards_BurningSprings  (LocationHierarchyHasKeyword LocRegionBurningSprings)
      ..._QuestRewards_Skyline         (LocationHierarchyHasKeyword LocRegionStorm)
      Bounty Sniffer rewards           (World Pets buff)
      Hideout Leads                    (GetRandomPercent <= HIDE_Lead_GruntHunt_Droprate)

On live it is still the flat Burning Springs list. Nothing here is typed in:
`grunt_event_pools()` walks whichever shape the TSVs hold, keeps the entries
that apply to the region asked for, and returns the pool shape
df-bnb-bounty-hunting.js draws ({label, formid, blurb, dropRate, items |
children, conditions, warningNote}).

Rates come from rng76 (entry pick weight x ChanceNone, GetRandomPercent gates,
GLOB ChanceNone) - the rng76 migration noted in project memory for this
builder. World-state switches (GetGlobalValue on a GLOB) are evaluated against
the GLOB export, so a switched-off LTT bonus reads 0% with the toggle warning;
player-side conditions (star level, a poster already in the bag, a pet buff)
are written out as plain sentences.

Item rates in the returned pools are the chance per Grunt Hunt completion
(already multiplied through the parent entry), so the unique checklist can
read them straight.
"""
import re

import plan_conditions as pc

TOGGLE_WARNING = (
    "This reward is enabled or disabled at Bethesda's discretion "
    "and may not always be active."
)

# Region keyword on the location of the bounty -> region id used by the pages.
REGION_KEYWORDS = {
    "burning-springs": "LocRegionBurningSprings",
    "skyline-valley": "LocRegionStorm",
}
REGION_NAMES = {"burning-springs": "Burning Springs", "skyline-valley": "Skyline Valley"}

_LGDI_RE = re.compile(
    r"^(?P<pfx>BOUNTY_|LTT_)?LegendaryItems_(?P<kind>Weapons_Melee|Weapons_Ranged|Weapons|"
    r"PowerArmor|Armor)?_?Rank(?P<rank>\d)$", re.I)
_LGDI_KIND = {
    None: "Item", "weapons_melee": "Melee Weapon", "weapons_ranged": "Ranged Weapon",
    "weapons": "Weapon", "armor": "Armour", "powerarmor": "Power Armour",
}


def lgdi_name(edid, fallback=""):
    """BOUNTY_LegendaryItems_Weapons_Rank2 -> '2★ Legendary BOUNTY Weapon'."""
    m = _LGDI_RE.match(edid or "")
    if not m:
        return fallback or edid
    kind = _LGDI_KIND.get((m.group("kind") or "").lower() or None, "Item")
    bounty = " BOUNTY" if (m.group("pfx") or "").upper() == "BOUNTY_" else ""
    return f"{m.group('rank')}★ Legendary{bounty} {kind}"


def _ref_fid(ref):
    return (ref or "").split(":")[0].strip().upper()


def _ref_edid(ref):
    parts = (ref or "").split(":")
    return parts[1] if len(parts) > 1 else ""


def _ref_sig(ref):
    parts = (ref or "").split(":")
    return parts[-1].upper() if len(parts) >= 3 else ""


class GruntTreeWalker:
    """One walker per channel: wraps an rng76 resolver plus the GLOB values."""

    def __init__(self, data):
        self.d = data
        self.R = data.resolver
        self.L = data.lvli

    # ── conditions ─────────────────────────────────────────────────────────
    def glob_value(self, fid):
        v = self.d.globs.value(fid)
        return 0.0 if v is None else float(v)

    def _compare(self, a, op, b):
        return {pc.OP_EQ: a == b, pc.OP_NE: a != b, pc.OP_GT: a > b, pc.OP_GE: a >= b,
                pc.OP_LT: a < b, pc.OP_LE: a <= b}.get(op, True)

    def read_conditions(self, raw_conds, region=None):
        """-> (passes_world_state, region_ok, star, toggle_glob, sentences)

        passes_world_state: False when a GetGlobalValue switch is off.
        region_ok: False when a LocationHierarchyHasKeyword names another region.
        star: the star level an entry is gated on (GetValue ...GruntStarLevel...).
        """
        world_ok, region_ok, star, toggle, out = True, True, None, None, []
        for raw in raw_conds or []:
            c = pc.parse_condition(raw)
            if not c:
                continue
            fn = c["fn"]
            ref = (c.get("refs") or [{}])[0]
            edid = ref.get("edid") or ""
            if fn == "GetRandomPercent":
                continue                                   # inside the rate
            if fn == "LocationHierarchyHasKeyword":
                want = REGION_KEYWORDS.get(region or "")
                if want and edid != want and edid in REGION_KEYWORDS.values():
                    region_ok = False
                continue
            if fn == "GetGlobalValue":
                fid = (ref.get("fid") or "").upper()
                val = self.glob_value(fid)
                num = c.get("number")
                if num is None and c.get("value_ref"):
                    num = self.glob_value((c["value_ref"].get("fid") or "").upper())
                if num is not None and not self._compare(val, c["op"], num):
                    world_ok = False
                if re.search(r"Toggle|LTT_|LCP_", edid):
                    toggle = fid
                continue
            if fn == "GetValue" and re.search(r"GruntStarLevel", edid) and c.get("number") is not None:
                star = int(c["number"])
                continue
            if fn == "GetValue" and re.search(r"RewardWantedPoster", edid):
                lo = int(self.glob_value("00848D6A") or 3)
                hi = int(self.glob_value("00848D6B") or 6)
                out.append(f"Only on the Grunt Hunt the game picks to hand out a poster "
                           f"(one every {lo} to {hi} Grunt Hunts)")
                continue
            if fn == "GetItemCount":
                name = ref.get("full") or pc._ref_name(ref) or "it"
                yes = pc._truthy(c)
                out.append(f"Only while you are carrying a {name}" if yes
                           else f"Only while you do not already have a {name}")
                continue
            if fn == "HasActiveMagicEffect":
                m = re.search(r"(\d+)$", edid)
                name = ref.get("full") or pc._ref_name(ref) or "a buff"
                rank = f" (rank {int(m.group(1))})" if m else ""
                out.append(f"Only while your World Pet's {name} buff{rank} is active")
                continue
            if fn == "HasLearnedRecipe" and "CondProxy" in edid and pc._truthy(c) is False:
                out.append("Stops dropping once you have learned it")
                continue
            s = pc.sentence(c)
            if s:
                out.append(s)
        return world_ok, region_ok, star, toggle, out

    # ── one entry of a Use-All list ────────────────────────────────────────
    def entry_rate(self, list_id, entry):
        """Chance (0-1) this entry fires in a Use-All, max-count-0 list."""
        math = self.L.math_by_entry.get((list_id, entry.get("EntryIndex")))
        if not math:
            return 1.0
        pw, cn = self.R._entry_pick_and_cn(math, entry, list_id)
        grp = self.R.extract_grp_chance(self.R._entry_conditions(entry))
        if grp is not None:
            pw, cn = grp, 1.0
        return max(0.0, min(1.0, pw * cn))

    def list_world_ok(self, list_id):
        ok, _, _, toggle, _ = self.read_conditions(self.L.list_conditions_for(list_id))
        return ok, toggle

    # ── leaves of a sub-list, rates relative to the sub-list firing ───────
    def leaves(self, list_id, region):
        out, merged = [], {}
        # Merge repeats only where they are the same pick (a pick-one list that
        # holds the same junk roll twice). In a Use-All list each entry is its
        # own drop - Legendary Modules 3 + 60% + 40% stay three rows.
        can_merge = not self.L.flags_for(list_id).get("use_all")
        # A GetRandomPercent gate on an entry of a PICK-ONE list (Head Hunt
        # Hideout Lead: HIDE_Lead_HeadHunt_Droprate) - rng76.resolve_deep only
        # reads those gates on Use-All lists, so apply it here.
        grp_gate = {}
        if can_merge:
            for e in self.L.entries_by_list.get(list_id, []):
                g = self.R.extract_grp_chance(self.R._entry_conditions(e))
                if g is not None and _ref_sig(e.get("LVLO_Reference")) != "LVLI":
                    grp_gate[_ref_fid(e.get("LVLO_Reference"))] = g
        for it in self.R.resolve_deep(list_id):
            ok, reg_ok, _, _, sentences = self.read_conditions(it.get("conditions"), region)
            if not reg_ok:
                continue
            sig = (it.get("sig") or "").upper()
            name = lgdi_name(it.get("edid"), it.get("name")) if sig == "LGDI" else it.get("name")
            rate = (it.get("dropRate") or 0.0) * 100.0 if ok else 0.0
            if (it.get("formid") or "").upper() in grp_gate:
                rate *= grp_gate[(it.get("formid") or "").upper()]
            key = (it.get("formid"), it.get("qty") or 1, tuple(sentences))
            prev = merged.get(key) if can_merge else None
            if prev is not None:                 # same item reached twice (pick-one of
                prev["dropRate"] += rate         # two identical junk rolls) - one row
                continue
            row = {
                "name": name, "formid": it.get("formid"), "edid": it.get("edid") or "",
                "sig": sig, "qty": it.get("qty") or 1, "dropRate": rate,
                "conditions": sentences,
            }
            merged[key] = row
            out.append(row)
        for r in out:
            r["dropRate"] = round(r["dropRate"], 6)
        return out

    # ── the root, flattened for one region ─────────────────────────────────
    def _flatten(self, list_id, region, depth=0):
        """Top-level reward entries for `region`: a region sub-list (gated by
        LocationHierarchyHasKeyword) is opened and its entries take its place."""
        rows = []
        for e in self.L.entries_by_list.get(list_id, []):
            conds = self.R._entry_conditions(e)
            ok, reg_ok, star, toggle, sentences = self.read_conditions(conds, region)
            if not reg_ok:
                continue
            ref = (e.get("LVLO_Reference") or "").strip()
            is_region_list = any("LocationHierarchyHasKeyword" in c for c in conds)
            if is_region_list and _ref_sig(ref) == "LVLI" and depth < 3:
                rows.extend(self._flatten(_ref_fid(ref), region, depth + 1))
                continue
            rows.append({"list": list_id, "entry": e, "ref": ref, "ok": ok, "star": star,
                         "toggle": toggle, "conds": sentences,
                         "rate": self.entry_rate(list_id, e)})
        return rows

    def _label_for(self, sub_edid, leaves):
        e = sub_edid or ""
        m = re.search(r"LegendaryTemplate_(\d)Star", e)
        if m:
            return (f"LTT {m.group(1)}★ Legendary" if e.startswith("LTT_")
                    else f"{m.group(1)}★ BOUNTY Legendary Items")
        if re.search(r"Treasury_Note", e):
            return "Treasury Notes"
        if re.search(r"LegendaryModule", e):
            return "Legendary Modules"
        if re.search(r"BountySniffer", e):
            return "Bounty Sniffer Bonus Legendary"
        if re.search(r"HIDE_.*Lead", e):
            return "Hideout Lead"
        if leaves and len({l["name"] for l in leaves}) == 1:
            return leaves[0]["name"]
        return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ",
                      re.sub(r"^(?:Burn_)?(?:LL_|LLS_)?", "", e).replace("_", " ")).strip()

    def grunt_event_pools(self, root_fid, region):
        rows = self._flatten(root_fid, region)
        pools = []
        starred = {"std": [], "ltt": []}
        for r in rows:
            ref, e = r["ref"], r["entry"]
            sub = _ref_fid(ref) if _ref_sig(ref) == "LVLI" else ""
            if sub:
                list_ok, list_toggle = self.list_world_ok(sub)
                leaves = self.leaves(sub, region)
            else:
                list_ok, list_toggle = True, None
                sig = _ref_sig(ref)
                leaves = [{"name": self.d.names.resolve(_ref_fid(ref), _ref_edid(ref)),
                           "formid": _ref_fid(ref), "edid": _ref_edid(ref), "sig": sig,
                           "qty": self.R._entry_qty(e), "dropRate": 100.0, "conditions": []}]
            live = r["ok"] and list_ok
            rate = r["rate"] * 100.0 if live else 0.0
            toggle = r["toggle"] or list_toggle
            for lf in leaves:                                   # per-completion rates
                lf["dropRate"] = round(lf["dropRate"] * rate / 100.0, 6)
            if len(leaves) == 1:              # one item: the pool is only as likely as it
                rate = min(rate, leaves[0]["dropRate"])
            pool = {
                "label": self._label_for(_ref_edid(ref), leaves),
                "formid": sub or _ref_fid(ref),
                "edid": _ref_edid(ref),
                "dropRate": round(rate, 6),
                "items": leaves,
            }
            if r["conds"]:
                pool["conditions"] = r["conds"]
            if toggle:
                pool["warningNote"] = TOGGLE_WARNING
                pool["toggleGlob"] = toggle
            if r["star"] is not None:
                pool["star"] = r["star"]
                starred["ltt" if toggle else "std"].append(pool)
                continue
            pool["blurb"] = self._blurb(pool, live)
            pools.append(pool)

        # Star-gated templates: one expand per family, a child per star level.
        for fam, kids in (("std", starred["std"]), ("ltt", starred["ltt"])):
            if not kids:
                continue
            kids.sort(key=lambda p: p["star"])
            for k in kids:
                k["label"] = f"{k['star']}★ Grunt Hunt"
                k.setdefault("conditions", []).insert(0, f"Only on a {k['star']}★ Grunt Hunt")
                k["blurb"] = self._blurb(k, k["dropRate"] > 0)
            rate = max(k["dropRate"] for k in kids)
            parent = {
                "label": "BOUNTY Legendary Item" if fam == "std" else "LTT Bonus Legendary",
                "formid": "-".join(k["formid"] for k in kids)[:26] + ("-ltt" if fam == "ltt" else ""),
                "dropRate": rate,
                "children": kids,
                "mode": "starlevel",
            }
            if fam == "std":
                parent["blurb"] = ("Guaranteed drop of 1 item · the star level of the Grunt Hunt "
                                   "decides which list rolls")
            else:
                parent["warningNote"] = TOGGLE_WARNING
                parent["blurb"] = ("Bonus legendary from the Live Tuning Table · " +
                                   (f"currently active · {round(rate, 4):g}% chance per completion"
                                    if rate > 0 else "currently disabled by Bethesda"))
            pools.insert(0 if fam == "std" else min(1, len(pools)), parent)
        return pools

    @staticmethod
    def _blurb(pool, live):
        if not live or pool["dropRate"] <= 0:
            return ("Currently disabled by Bethesda" if pool.get("warningNote")
                    else "Does not drop at the moment")
        items = pool.get("items") or []
        n = len(items)
        r = pool["dropRate"]
        if n > 1 and len({i.get("formid") for i in items}) == 1 and r >= 99.9999:
            # Same item rolled several times (Legendary Modules: 1 + 60% + 40%).
            sure = sum(i.get("qty") or 1 for i in items if i.get("dropRate", 0) >= 99.9999)
            most = sum(i.get("qty") or 1 for i in items)
            if sure:
                return f"Guaranteed {sure} · up to {most} with luck"
            qs = sorted(i.get("qty") or 1 for i in items)      # pick one quantity
            return f"Guaranteed · {qs[0]} to {qs[-1]} {items[0].get('name') or 'items'}"
        lead = "Guaranteed" if r >= 99.9999 else f"{round(r, 4):g}% chance"
        if n == 1:
            q = pool["items"][0].get("qty") or 1
            return f"{lead} · {q} item" + ("s" if q != 1 else "")
        return f"{lead} · {n} possible items"

    def _is_list_of_lists(self, list_id):
        ents = self.L.entries_by_list.get(list_id, [])
        return (len(ents) > 1 and self.L.flags_for(list_id).get("use_all")
                and all(_ref_sig(e.get("LVLO_Reference")) == "LVLI" for e in ents))

    @staticmethod
    def _roll_label(edid, n):
        e = edid or ""
        kind = "Rare" if re.search(r"Rare|High", e, re.I) else ("Common" if re.search(r"Common|Low", e, re.I) else "")
        return (kind + " roll").strip().capitalize() + f" {n}" if kind else f"Roll {n}"

    # ── enemy death list (the Outlaw's own inventory) ─────────────────────
    def death_pools(self, list_id, region=None):
        """One pool per entry of the death list (caps, Rad-X, ammo ...)."""
        pools = []
        flags = self.L.flags_for(list_id)
        for e in self.L.entries_by_list.get(list_id, []):
            ref = (e.get("LVLO_Reference") or "").strip()
            sub = _ref_fid(ref) if _ref_sig(ref) == "LVLI" else ""
            rate = self.entry_rate(list_id, e) * 100.0 if flags.get("use_all") else None
            if sub and re.search(r"Ammo_Loot_Contextual", _ref_edid(ref), re.I):
                # 100+ per-ammo-type branches, one fires: the one for your weapon.
                leaves = [{"name": "Ammo for your equipped weapon", "formid": sub,
                           "edid": _ref_edid(ref), "sig": "LVLI", "qty": 1,
                           "dropRate": 100.0, "conditions": []}]
            elif sub and self._is_list_of_lists(sub):
                # e.g. Bounty Junk: Use-All of 2 common rolls + 1 rare roll.
                # Keep the rolls apart instead of one 156-row table.
                kids = []
                for ce in self.L.entries_by_list.get(sub, []):
                    cref = (ce.get("LVLO_Reference") or "").strip()
                    cl = [l for l in self.leaves(_ref_fid(cref), region) if l["dropRate"] > 0]
                    if not cl:
                        continue
                    crate = self.entry_rate(sub, ce) * 100.0
                    kids.append({"label": self._roll_label(_ref_edid(cref), len(kids) + 1),
                                 "formid": _ref_fid(cref), "edid": _ref_edid(cref),
                                 "dropRate": round(crate, 6), "items": cl, "mode": "pickone",
                                 "blurb": (("Guaranteed" if crate >= 99.9999 else f"{round(crate, 4):g}% chance")
                                           + f" · pick one of {len(cl)} items")})
                leaves = None
            elif sub:
                leaves = self.leaves(sub, region)
            else:
                leaves = [{"name": self.d.names.resolve(_ref_fid(ref), _ref_edid(ref)),
                           "formid": _ref_fid(ref), "edid": _ref_edid(ref), "sig": _ref_sig(ref),
                           "qty": self.R._entry_qty(e), "dropRate": 100.0, "conditions": []}]
            if rate is None:
                rate = 100.0
            if leaves is None:
                if not kids:
                    continue
                ed = _ref_edid(ref)
                pools.append({"label": "Bounty Junk" if re.search(r"Junk", ed, re.I) else self._label_for(ed, []),
                              "formid": sub, "edid": ed, "dropRate": round(rate, 6),
                              "children": kids, "mode": "useall",
                              "blurb": f"{len(kids)} rolls · " + ", ".join(k["label"] for k in kids)})
                continue
            leaves = [l for l in leaves if l["dropRate"] > 0]
            if not leaves:
                continue
            qty = self.R._entry_qty(e)
            label = self._label_for(_ref_edid(ref), leaves)
            ed = _ref_edid(ref)
            for rx, nice in ((r"Caps", "Caps"), (r"RadX", "Rad-X"), (r"Stimpak", "Stimpaks"),
                             (r"Contextual", "Contextual Ammo"), (r"Junk", "Bounty Junk"),
                             (r"RadAway", "RadAway")):
                if re.search(rx, ed, re.I):
                    label = nice
                    break
            same = next((p for p in pools if sub and p["formid"] == sub and p.get("items")), None)
            if same is not None:                      # the same list rolled again (Caps x3)
                same["repeats"] = same.get("repeats", 1) + 1
                n = same["repeats"] * (same.get("rolls") or 1)
                same["blurb"] = f"{n} rolls · " + (f"{len(leaves)} possible items" if len(leaves) > 1 else "1 item")
                continue
            pool = {"label": label, "formid": sub or _ref_fid(ref), "edid": _ref_edid(ref),
                    "dropRate": round(rate, 6), "items": leaves}
            if qty and qty > 1 and sub:
                pool["rolls"] = qty
            pool["blurb"] = (f"{qty} rolls · " if qty and qty > 1 and sub else "") + \
                (f"{len(leaves)} possible items" if len(leaves) > 1 else "1 item")
            pools.append(pool)
        return pools
