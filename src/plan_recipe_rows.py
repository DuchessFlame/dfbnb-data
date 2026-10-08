#!/usr/bin/env python3
r"""
plan_recipe_rows.py — checklist rows for recipes that have no plan book.

WHY
---
Every row on a plan checklist has, until now, started life as a BOOK: the plan
item you find, trade and learn. That is one of the ways Bethesda teaches a
recipe, and the pipeline treated it as the only one.

`plan_unlocks` reads COBJ.GNAM, where the game states the route itself, and 147
live recipes name a CHALLENGE there instead of a plan. Most pair with a plan
book — those are handled by the cut rescue in `add_recipe_unlocks.py`. The rest
have no plan book at all, and there has never been anywhere for them to go:

    Beer Pump                  <- Kill Bounties from Group 2
    Aerial Turbine             <- Complete Grunt Hunts
    Dog Tamer Outfit           <- Dog - Reach Level 190
    Spicy Eelgrass Soup        <- Catch Fall Seasonal Local Legend: "Sludge Eye"
    Guard Post                 <- learned by claiming a workshop

A player knows these the same way they know any other recipe, and tracks them
the same way. So a checklist row is a RECIPE you can learn, and a plan book is
one route to it rather than the definition of a row.

WHAT IS EXCLUDED, AND WHY
-------------------------
Emitting one row per COBJ would publish the same craftable several times over,
because a single thing a player builds is often several recipes:

  * **A recipe a plan already covers.** Skipped on the COBJ FormID first, then
    on the created record's name — "Rocket Bobber" is both a rod mod with a plan
    and a `RodBobber_Display_Rocket` souvenir without one, and two rows called
    Rocket Bobber on the fishing page is a bug however defensible the data is.
  * **Stubs with no created record.** 53 of them: condition proxies whose whole
    job is to carry the GNAM. There is nothing to name a row after.
  * **Duplicates within this set**, collapsed on the created record's name, so
    the workbench and souvenir variants of one item yield one row.

ROW SHAPE
---------
Identical to a plan row, with `plan_item: null` and an id of `RECIPE_<COBJ>`.

`kind` stays **"plan"**. It is tempting to invent `kind: "recipe"`, and it is
wrong: `selectPlanRows()` in the renderer filters every checklist on
`(it.kind || "plan") === "plan"`, and the progress store, the export poster and
the group counts all follow it. A new kind would publish these rows into a
dataset nothing reads. What sets them apart is `plan_item: null`, which the
renderer is already defensive about everywhere it touches it, plus an explicit
`recipe_only` flag for anything that wants to say so.
`type` comes from the same `classify_plan()` the plan rows use, so a recipe
lands on the page its craftable belongs to and nowhere else. The id is stable
across builds because a FormID is, which matters: it is the progress-store key,
and a reader's ticks follow it.

`tradeable` is **null** for challenge/workshop recipes, not False. A recipe with
no plan item is not a thing that can change hands at all, so both answers would
be wrong; null is what the renderer already draws as "Unknown".

SCRAP-TO-LEARN IS THE EXCEPTION: **always `tradeable: False`.**
(So is a challenge reward — see `plan_sources.apply_tradeable_rules`.)
A scrap-learned mod is not "we don't know" — the game states the route, and that
route is the only one: there is no plan book for it, so there is nothing to find,
trade, sell or drop. Printing "Unknown" there invites a reader to go looking for a
plan in someone's vendor that cannot exist. The rule is the route, not a per-mod
lookup, so it is enforced centrally in `enforce_scrap_untradeable()` and holds for
any row flagged `scrap_learn` whatever built it.
"""

import collections
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_plan_obtain_json as bpo
import plan_images
import plan_sources
import plan_unlocks


def _norm(name):
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _plan_name(full):
    return _norm(re.sub(r"^(?:Plan|Recipe)\s*:\s*", "", full or ""))


def build(items, tsv_dir="tsv", stats=None):
    """Return the recipe-only rows to append to *items*."""
    stats = stats if stats is not None else collections.Counter()
    tsv_dir = os.path.abspath(tsv_dir)
    bpo.TSV = tsv_dir
    bpo.SIG_INDEX = bpo.build_sig_index()
    cobj_idx = bpo.build_cobj_index()
    unlocks = plan_unlocks.RecipeUnlocks(tsv_dir, bpo.newest)

    covered_fids = {(it.get("cobj") or {}).get("formid", "").upper()
                    for it in items if it.get("cobj")}
    covered_names = {_plan_name(it.get("name")) for it in items}
    covered_names |= {_norm((it.get("cnam") or {}).get("edid")) for it in items}
    covered_names.discard("")

    rows, seen = [], {}
    for co_fid in sorted(unlocks.by_cobj):
        unlock = unlocks.proof_of_life(co_fid)
        if not unlock:
            continue
        if co_fid in covered_fids:
            stats["covered_by_a_plan"] += 1
            continue
        cobj = cobj_idx.get(co_fid) or {}
        name = (cobj.get("cnam_full") or "").strip()
        if not name:
            stats["no_created_record"] += 1
            continue
        key = _norm(name)
        if key in covered_names:
            stats["same_name_as_a_plan"] += 1
            continue
        if key in seen:
            stats["duplicate_recipe"] += 1
            continue

        cat, has_img, cnam_sig, cnam_fid, cnam_edid = bpo.classify_plan(
            cobj.get("edid") or "", cobj)
        sentence = unlocks.sentence(unlock)
        row = {
            "kind": "plan", "recipe_only": True, "brand": "df", "type": cat,
            "id": f"RECIPE_{co_fid}", "name": name,
            "has_image_box": has_img, "image_dir": "",
            "obtain": (bpo.LEARNT_DIRECT + " It is not random loot; see below."),
            "category_label": bpo.category_label(cat, has_img, cnam_sig,
                                                 physical=False),
            "obtain_routes": [],
            "obtain_unlocks": [sentence] if sentence else [],
            # No BOOK exists. Every consumer tests this for None already, because
            # a plan row with an unresolved created object has always been able
            # to carry a null `cobj`/`cnam`.
            "plan_item": None,
            "cobj": {"formid": co_fid, "edid": cobj.get("edid") or ""},
            "cnam": ({"formid": cnam_fid, "edid": cnam_edid, "sig": cnam_sig}
                     if cnam_fid else None),
            "tradeable": None, "stops_dropping": None, "effects": None,
            "cut": False, "cut_reason": None,
            "changes": [],
        }
        # The art folder decides which checklist page the row lands on
        # (plan_subpages keys on it), so it is resolved with the same classifier
        # the plan rows use rather than left as the bucket name. plan_images
        # overwrites it with the same answer when the art step runs.
        row["image_dir"] = plan_images.page_folder(row) or cat or ""
        row["obtain_ledger"] = plan_sources.obtain_ledger(row)
        seen[key] = row
        rows.append(row)
        stats["emitted"] += 1
        stats["by_type_" + (cat or "none")] += 1

    rows += _build_scrap(items, cobj_idx, unlocks, covered_fids, stats)
    rows += _build_default_weapons(items, cobj_idx, unlocks, stats)
    rows += _build_consumables(items, cobj_idx, stats)
    return rows, stats


# ── weapons every character already knows ───────────────────────────────────
# Duchess, 7 Oct 2026: the Pipe guns, Syringer, Machete, Hatchet, Board, Combat
# Knife and Throwing Knife belong on the Weapon page even though there is no
# plan for them — a reader looking for "Pipe Revolver" should find it and be
# told it is known by default, not find nothing.
#
# The game's own test, nothing by name:
#   * a base-game weapon recipe (co_Weapon_…) at the weapons workbench that
#     creates a WEAP — content-drop recipes (Storm_, W05_, MoM_ …) and the
#     NOCRAFT / REPAIRONLY / zzz copies are not "known by default"
#   * no GNAM: nothing in the files gates the recipe, so it is known from the
#     start (a gated one is a challenge / scrap / script / item unlock instead)
#   * not ordnance (grenades and mines have their own page)
#   * no plan already makes it. A scoreboard plan that only BORROWED the weapon
#     from this recipe (bpo.vendor_twin — the Cosmic Knife plan makes "Knife")
#     does not count, or the plain Combat Knife would vanish behind it.
DEFAULT_NAMES = {"Pipe": "Pipe Gun"}          # the WEAP FULL is just "Pipe"
_RX_DEFAULT_COBJ = re.compile(r"^co_Weapon_(Ranged|Melee|Thrown)_", re.I)
_RX_DEFAULT_SKIP = re.compile(r"nocraft|repaironly|copy|test|questreward", re.I)
DEFAULT_OBTAIN = ("Known by default — there is no plan to find. Every character "
                  "can craft this at a weapons workbench from the start.")
PICKUP_OBTAIN = ("There is no plan to find — you learn to craft this the first time "
                 "you pick one up.")
QUEST_OBTAIN = ("There is no plan to find — you learn this automatically when you "
                "complete the quest below.")
# Recipes that are not "known by default" in spirit even when the COBJ2 file is
# missing (it only exists from the Oct 2026 COBJ export on).
_RX_ANY_WEAPON_COBJ_SKIP = re.compile(r"nocraft|repaironly|^zzz|^del_|^cut_|test|"
                                      r"copy|questreward|^survival_|^atx_|^score_",
                                      re.I)


def _cobj2(tsv_dir):
    """COBJ FormID -> learn method / conditions row, from COBJ2_Export_*.tsv.

    Written by the same xEdit run as the main COBJ file (ExportCOBJToTSV.pas,
    Oct 2026). Empty when no COBJ2 export exists yet — callers then fall back
    to the GNAM-only test.
    """
    path = bpo.newest("COBJ2_Export_*.tsv", tsv_dir)
    out = {}
    if not path:
        return out
    for r in bpo.read_rows(path):
        fid = (r.get("COBJ_FormID") or "").strip().upper()
        if fid:
            out[fid] = r
    return out


def _build_default_weapons(items, cobj_idx, unlocks, stats):
    """Known-by-default weapons, and weapons a quest teaches automatically.

    With a COBJ2 export (LRNM + Conditions) a recipe is only "known by
    default" when it carries NO conditions. A GetQuestCompleted condition
    makes it a quest-learned weapon instead (V63 Zweihaender: The Eye of the
    Storm), which gets its own row and a Quests ledger line.
    """
    ordnance = plan_images.load_ordnance(bpo.TSV)
    cond = _cobj2(bpo.TSV)
    covered_co, covered_weap = set(), set()
    for it in items:
        if it.get("cut") or it.get("recipe_only"):
            continue
        co = ((it.get("cobj") or {}).get("formid") or "").upper()
        covered_co.add(co)
        own = cobj_idx.get(co) or {}
        twin = bpo.vendor_twin(own, cobj_idx) if own else None
        if twin:
            covered_co.add(next((f for f, c in cobj_idx.items() if c is twin), ""))
        elif own.get("cnam_fid"):
            covered_weap.add(own["cnam_fid"].upper())
    rows, seen = [], set()
    for co_fid in sorted(cobj_idx):
        c = cobj_idx[co_fid]
        edid, cnam = c.get("edid") or "", (c.get("cnam_fid") or "").upper()
        if _RX_ANY_WEAPON_COBJ_SKIP.search(edid):
            continue
        if bpo.SIG_INDEX.get(cnam) != "WEAP" or cnam in ordnance:
            continue
        if (c.get("bnam_edid") or "") != "Workbench_Crafting_Weapon":
            continue
        cr = cond.get(co_fid) or {}
        quest = (cr.get("Quest_FULL") or cr.get("Quest_EDID") or "").strip()
        n_cond = int((cr.get("CondCount") or "0").strip() or 0) if cr else 0
        unlock = unlocks.unlock_for(co_fid)
        lrnm = (cr.get("LRNM_LearnMethod") or "").strip().lower()
        pickup = (not quest) and lrnm.startswith("learned when picked up")
        if pickup:
            # Blade of Bastet / Voice of Set: LRNM "Learned when picked up or by
            # script" — you know the recipe once you have held one. Their only
            # condition (GetItemHealthPercent) is about the item, not the player.
            if unlock and unlock.get("kind") not in (plan_unlocks.SCRIPT,):
                continue
        elif quest:
            # Quest-taught: a GNAM, if any, is only the "learned via script"
            # placeholder (Blade of Bastet / Voice of Set).
            if unlock and unlock.get("kind") not in (plan_unlocks.SCRIPT,):
                continue
        else:
            if not _RX_DEFAULT_COBJ.match(edid) or _RX_DEFAULT_SKIP.search(edid):
                continue
            if unlock or n_cond:
                stats["default_has_conditions"] += bool(n_cond)
                continue
        if co_fid in covered_co or cnam in covered_weap or cnam in seen:
            stats["default_covered_by_a_plan"] += 1
            continue
        full = (c.get("cnam_full") or "").strip()
        if not full:
            continue
        seen.add(cnam)
        if pickup:
            row = {
                "kind": "plan", "recipe_only": True, "pickup_learned": True,
                "brand": "df", "type": "weapon",
                "id": f"RECIPE_{co_fid}", "name": DEFAULT_NAMES.get(full, full),
                "has_image_box": True, "image_dir": "",
                "obtain": PICKUP_OBTAIN,
                "category_label": "Weapon (learned on pick-up)",
                "obtain_routes": [], "obtain_unlocks": [],
                "plan_item": None,
                "cobj": {"formid": co_fid, "edid": edid},
                "cnam": {"formid": cnam, "edid": c.get("cnam_edid") or "", "sig": "WEAP"},
                "tradeable": None, "stops_dropping": None, "effects": None,
                "cut": False, "cut_reason": None,
                "changes": [], "source_tag": "Pickup",
            }
            row["image_dir"] = plan_images.page_folder(row) or "weapons"
            row["obtain_ledger"] = []
            rows.append(row)
            stats["pickup_learned_emitted"] += 1
            continue
        if quest:
            sentence = f"Learned automatically when you complete the quest: {quest}."
            row = {
                "kind": "plan", "recipe_only": True, "quest_learned": True,
                "brand": "df", "type": "weapon",
                "id": f"RECIPE_{co_fid}", "name": DEFAULT_NAMES.get(full, full),
                "has_image_box": True, "image_dir": "",
                "obtain": QUEST_OBTAIN,
                "category_label": "Weapon (quest reward recipe)",
                "obtain_routes": [], "obtain_unlocks": [sentence],
                "plan_item": None,
                "cobj": {"formid": co_fid, "edid": edid},
                "cnam": {"formid": cnam, "edid": c.get("cnam_edid") or "", "sig": "WEAP"},
                # No plan item exists, so nothing can change hands (same call
                # as scrap-to-learn rows) — False, not an "Unknown" pill.
                "tradeable": False, "stops_dropping": None, "effects": None,
                "cut": False, "cut_reason": None,
                "changes": [], "source_tag": "Quest",
                "learn_quest": {"formid": cr.get("Quest_FormID") or "",
                                "edid": cr.get("Quest_EDID") or "", "name": quest},
            }
            row["image_dir"] = plan_images.page_folder(row) or "weapons"
            row["obtain_ledger"] = plan_sources.obtain_ledger(row)
            rows.append(row)
            stats["quest_learned_emitted"] += 1
            continue
        row = {
            "kind": "plan", "recipe_only": True, "known_by_default": True,
            "brand": "df", "type": "weapon",
            "id": f"RECIPE_{co_fid}", "name": DEFAULT_NAMES.get(full, full),
            "has_image_box": True, "image_dir": "",
            "obtain": DEFAULT_OBTAIN,
            "category_label": "Weapon (known by default)",
            "obtain_routes": [], "obtain_unlocks": [],
            "plan_item": None,
            "cobj": {"formid": co_fid, "edid": edid},
            "cnam": {"formid": cnam, "edid": c.get("cnam_edid") or "", "sig": "WEAP"},
            # There is no plan, so there is nothing to trade: no pill at all.
            "tradeable": None, "stops_dropping": None, "effects": None,
            "cut": False, "cut_reason": None,
            "changes": [], "source_tag": "Default",
        }
        row["image_dir"] = plan_images.page_folder(row) or "weapons"
        row["obtain_ledger"] = []
        rows.append(row)
        stats["default_emitted"] += 1
    return rows


# Structural mod EDID tokens the family resolver protects — see add_weapon_groups
# / plan_images. A scrap-learned mod that turns out to be a paint or material is
# a skin, not a functional mod, but both still nest under the same weapon, so the
# distinction is left to add_weapon_groups.classify() (which reads the OMOD attach
# point) rather than second-guessed here.
# ── food, drink and chem recipes with no plan ──────────────────────────────
# Duchess, 8 Oct 2026: the Recipe page must list every consumable recipe a
# player can know, not only the ones taught by a plan book. The game states the
# route itself in COBJ2 (LRNM learn method + conditions), so nothing here is
# decided by name:
#
#   * "Learned when picked up or by script" with a GNAM: picking up the GNAM
#     item teaches it (Radstag Meat -> Grilled Radstag). "Learned when
#     ingested": eating one teaches it.
#   * "Known by default" with no conditions (or only a world GLOB toggle that is
#     not a quest switch): every character knows it — the Brewing Station basics,
#     boiled/purified water, and the Cannery versions of meals.
#   * Cannery recipes gated on HasLearnedRecipe(<bundle plan>): learned from that
#     plan (Plan: Cannery Recipe Bundle #1/#2).
#   * Plan-taught, challenge-taught and quest-only recipes are left alone — the
#     plan rows and the GNAM-unlock rows above already own those.
#
# A "Canned …" row carries `canned: true` and `sort_name` = its base recipe's
# row title, so the page lists Canned Brain Bombs straight under Brain Bombs
# instead of under C (Duchess: break A–Z for these).
_CONS_BENCH = {
    "Workbench_Crafting_Cooking": "cooking station",
    "Workbench_Crafting_Chemlab": "chemistry station",
    "Workbench_Crafting_Brewing": "Brewing Station",
    "Workbench_Crafting_Cannery": "Cannery",
}
_RX_CONS_SKIP = re.compile(r"^zzz|^cut_|^del_|^post_|test|nocraft|copy|condproxy", re.I)
_RX_CANNERY_TAIL = re.compile(r"_Cannery(_G\d+)?$", re.I)
_RX_SCORE_HEAD = re.compile(r"^(zzz_?)?SCORE_S?\d+_", re.I)
_RX_COND_RECIPE = re.compile(r"HasLearnedRecipe\|[^|]*\|[^|]*\|([A-Za-z0-9_]+) \[COBJ:([0-9A-F]{8})\]")
_RX_COND_FUNC = re.compile(r"\|(?:[0-9.]+)\|([A-Za-z]+)\|")
_RX_COND_GLOB = re.compile(r"GetGlobalValue\|[^|]*\|[^|]*\|([A-Za-z0-9_]+) \[GLOB")


def _cons_display_name(full):
    return re.sub(r"^Fermentable\s+", "", full or "").strip()


def _build_consumables(items, cobj_idx, stats):
    cond = _cobj2(bpo.TSV)
    if not cond:
        stats["consumables_no_cobj2"] += 1
        return []
    path = bpo.newest("COBJ_Export_*.tsv", bpo.TSV)
    gnam = {}
    for r in bpo.read_rows(path):
        fid = (r.get("COBJ_FormID") or "").strip().upper()
        gnam[fid] = ((r.get("GNAM_FormID") or "").strip().upper(),
                     (r.get("GNAM_FULL") or "").strip(),
                     (r.get("GNAM_EDID") or "").strip())

    covered_fid = {((it.get("cobj") or {}).get("formid") or "").upper() for it in items}
    covered_cnam = {((it.get("cnam") or {}).get("formid") or "").upper()
                    for it in items if not it.get("cut")}
    covered_name = set()
    bundle_name = {}
    for it in items:
        nm = re.sub(r"^(?:Plan|Recipe)\s*:\s*", "", it.get("name") or "")
        covered_name.add(_norm(nm))
        covered_name.add(_norm(re.sub(r"\s*\([^)]*\)\s*$", "", nm)))
        co = (it.get("cobj") or {}).get("formid") or ""
        if co:
            bundle_name[co.upper()] = it.get("name") or ""
    covered_name.discard("")

    rows, seen = [], set()
    for co_fid in sorted(cobj_idx):
        c = cobj_idx[co_fid]
        edid = c.get("edid") or ""
        cnam = (c.get("cnam_fid") or "").upper()
        if bpo.SIG_INDEX.get(cnam) != "ALCH" or _RX_CONS_SKIP.search(edid):
            continue
        bench = _CONS_BENCH.get(c.get("bnam_edid") or "")
        if not bench:
            continue
        cr = cond.get(co_fid) or {}
        lrnm = (cr.get("LRNM_LearnMethod") or "").strip().lower()
        n_cond = int((cr.get("CondCount") or "0").strip() or 0)
        conds = cr.get("Conditions") or ""
        g_fid, g_full, _g_edid = gnam.get(co_fid, ("", "", ""))

        flags, obtain, unlocks_txt, learn_item = {}, "", [], ""
        if lrnm.startswith("learned when picked up"):
            if n_cond:
                stats["consumable_pickup_conditioned"] += 1
                continue
            flags["pickup_learned"] = True
            learn_item = g_full
            obtain = ("There is no plan to find — you learn to craft this the first "
                      f"time you pick up {g_full}." if g_full else PICKUP_OBTAIN)
            tag = "Pickup"
        elif lrnm.startswith("learned when ingested"):
            flags["pickup_learned"] = True
            obtain = ("There is no plan to find — you learn to craft this the first "
                      "time you eat one.")
            tag = "Pickup"
        elif lrnm.startswith("known by default"):
            if g_fid:
                continue                         # gated: the GNAM rows own it
            funcs = set(_RX_COND_FUNC.findall(conds))
            bundle = _RX_COND_RECIPE.search(conds)
            globs = _RX_COND_GLOB.findall(conds)
            if n_cond and bundle and bench == "Cannery":
                bname = bundle_name.get(bundle.group(2).upper()) or bundle.group(1)
                flags["recipe_bundle"] = True
                obtain = (f"Learned from {bname}. You also need a Cannery in your "
                          "C.A.M.P. to make it.")
                unlocks_txt = [obtain]
                tag = "Plan"
            elif n_cond and (funcs - {"GetGlobalValue"}
                             or any("quest" in g.lower() for g in globs)):
                stats["consumable_default_conditioned"] += 1
                continue                         # pets, quest items, entitlements
            else:
                flags["known_by_default"] = True
                obtain = ("Known by default — there is no plan to find. Anyone with a "
                          "Cannery in their C.A.M.P. can make this."
                          if bench == "Cannery" else
                          "Known by default — there is no plan to find. Every "
                          f"character can make this at a {bench} from the start.")
                tag = "Default"
        else:
            continue                             # plan / challenge / quest taught

        full = _cons_display_name(c.get("cnam_full") or "")
        key = _norm(full)
        if not full or key in seen:
            continue
        if co_fid in covered_fid or (cnam in covered_cnam and bench != "Cannery") \
                or key in covered_name:
            stats["consumable_covered_by_a_plan"] += 1
            continue
        seen.add(key)
        row = {
            "kind": "plan", "recipe_only": True, **flags,
            "brand": "df", "type": "recipe",
            "id": f"RECIPE_{co_fid}", "name": full,
            "has_image_box": True, "image_dir": "",
            "obtain": obtain,
            "category_label": f"Recipe ({bench})",
            "workbench": bench[0].upper() + bench[1:],
            "obtain_routes": [], "obtain_unlocks": unlocks_txt,
            "plan_item": None,
            "cobj": {"formid": co_fid, "edid": edid},
            "cnam": {"formid": cnam, "edid": c.get("cnam_edid") or "", "sig": "ALCH"},
            "tradeable": None, "stops_dropping": None, "effects": None,
            "cut": False, "cut_reason": None,
            "changes": [], "source_tag": tag,
        }
        if learn_item:
            row["learn_item"] = learn_item
        if bench == "Cannery":
            row["canned"] = True
        row["image_dir"] = plan_images.page_folder(row) or "recipes"
        row["obtain_ledger"] = []
        rows.append(row)
        stats["consumable_" + tag.lower()] += 1

    # Canned rows file under their base recipe. The base is the recipe whose
    # COBJ EditorID is the cannery one minus its _Cannery tail and season head
    # (SCORE_25_co_meal_BrainBombsGourmet_Cannery_G1 -> co_meal_BrainBombsGourmet).
    by_co = {}
    for it in list(items) + rows:
        e = ((it.get("cobj") or {}).get("edid") or "").lower()
        if e and not it.get("canned") and not it.get("cut"):
            by_co.setdefault(e, it)
    for row in rows:
        if not row.get("canned"):
            continue
        stem = _RX_SCORE_HEAD.sub("", _RX_CANNERY_TAIL.sub("", row["cobj"]["edid"])).lower()
        base = by_co.get(stem) or next((v for k, v in by_co.items() if k.endswith(stem)), None)
        if base:
            row["sort_name"] = re.sub(r"^(?:Plan|Recipe)\s*:\s*", "",
                                      base.get("display_name") or base.get("name") or "")
            row["canned_of"] = base.get("id")
        else:
            row["sort_name"] = re.sub(r"^Canned\s+", "", row["name"])
            stats["consumable_canned_no_base"] += 1
    return rows


def _build_scrap(items, cobj_idx, unlocks, covered_fids, stats):
    """Recipe-only rows for mods learned by SCRAPPING a weapon or armour.

    Distinct from build() above in two ways, both because a scrap mod is
    per-weapon, not one-of-a-kind:

      * It does NOT collapse on the created record's display NAME. "Hair Trigger
        Receiver" is a separate learnable for the 10mm, the Pipe and the Combat
        Rifle, and each has to be its own row so it can nest under its own
        weapon. Collapsing them the way build() collapses souvenir duplicates
        would publish one row and drop the rest. Dedup is on the created OMOD
        FormID instead — that folds only the genuine workbench duplicates (two
        COBJs making the same OMOD), keeping the shorter/earliest COBJ id.

      * The `type` is taken from the GNAM target's signature (WEAP -> weapon,
        ARMO -> armour), not from classify_plan(). A ranged weapon mod's EDID
        (`co_mod_10mm_Receiver_...`) carries no "weapon"/"gun" token, so
        classify_plan() would file it under `recipe` and it would never reach the
        weapon page; the parent item's signature is the data-driven truth.

    A scrap COBJ already carried by a real plan row (same COBJ, or a book mod
    that creates the same OMOD) is skipped, so a mod obtainable both ways is not
    doubled.
    """
    covered_cnam = {((it.get("cnam") or {}).get("formid") or "").upper()
                    for it in items if it.get("cnam")}
    covered_cnam.discard("")

    # One row per created OMOD; if several COBJs make it, keep the lowest FormID
    # so the row id is stable across builds.
    by_omod = {}
    for co_fid in unlocks.by_cobj:
        unlock = unlocks.unlock_for(co_fid)
        if not unlock or unlock.get("kind") != plan_unlocks.SCRAP:
            continue
        if co_fid in covered_fids:
            stats["scrap_covered_by_a_plan"] += 1
            continue
        cobj = cobj_idx.get(co_fid) or {}
        cnam_fid = (cobj.get("cnam_fid") or "").upper()
        if cnam_fid and cnam_fid in covered_cnam:
            stats["scrap_mod_already_a_plan"] += 1
            continue
        dedup_key = cnam_fid or co_fid
        prev = by_omod.get(dedup_key)
        if prev is None or co_fid < prev[0]:
            by_omod[dedup_key] = (co_fid, cobj, unlock)

    rows = []
    for co_fid, cobj, unlock in by_omod.values():
        name = (cobj.get("cnam_full") or "").strip()
        if not name:
            stats["scrap_no_created_record"] += 1
            continue
        cat = "armour" if unlock.get("item_sig") == "ARMO" else "weapon"
        cnam_fid = (cobj.get("cnam_fid") or "").upper()
        cnam_edid = cobj.get("cnam_edid") or ""
        sentence = unlocks.sentence(unlock)
        row = {
            "kind": "plan", "recipe_only": True, "scrap_learn": True,
            "brand": "df", "type": cat,
            "id": f"RECIPE_{co_fid}", "name": name,
            "has_image_box": False, "image_dir": "",
            "obtain": (bpo.LEARNT_DIRECT +
                       " It is learned by scrapping the base item — see below."),
            "category_label": bpo.category_label(cat, False, "OMOD", physical=False),
            "obtain_routes": [],
            "obtain_unlocks": [sentence] if sentence else [],
            "plan_item": None,
            "cobj": {"formid": co_fid, "edid": cobj.get("edid") or ""},
            # cnam.sig MUST be OMOD: add_weapon_groups / add_armour_groups key
            # their "this is a mod, find its weapon" path on it.
            "cnam": ({"formid": cnam_fid, "edid": cnam_edid, "sig": "OMOD"}
                     if cnam_fid else None),
            # Scrap-to-learn is never tradeable — see ROW SHAPE above.
            "tradeable": False, "stops_dropping": None, "effects": None,
            "cut": False, "cut_reason": None,
            "changes": [],
        }
        row["image_dir"] = plan_images.page_folder(row) or cat or ""
        row["obtain_ledger"] = plan_sources.obtain_ledger(row)
        rows.append(row)
        stats["scrap_emitted"] += 1
        stats["scrap_by_type_" + cat] += 1
    return rows


def attach(items, tsv_dir="tsv", stats=None):
    """Append recipe-only rows to *items*, replacing any from a previous run."""
    stats = stats if stats is not None else collections.Counter()
    before = len(items)
    items[:] = [it for it in items if not it.get("recipe_only")]
    stats["replaced"] = before - len(items)
    rows, stats = build(items, tsv_dir, stats)
    items.extend(rows)
    # Atom Shop / Scoreboard weapon skins (Weapon page, greyed, uncounted).
    import weapon_shop_skins
    weapon_shop_skins.attach(items, tsv_dir, stats)
    # Route-decided tradeability (scrap-to-learn, challenge rewards), applied to
    # every row here because attach() is the one place all three builders
    # (build_plan_obtain_json, merge_parts, add_recipe_unlocks) pass through.
    plan_sources.apply_tradeable_rules(items, stats)
    return stats


def report(stats, stream=sys.stdout):
    for key in sorted(stats):
        print(f"    {key:24s} {stats[key]}", file=stream)
