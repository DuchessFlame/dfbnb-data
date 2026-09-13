#!/usr/bin/env python3
r"""
build_underarmour_json.py — Underarmour Plan Checklist
(/df/plan-checklists/underarmour/), rendered by df-bnb-plan-checklists.js →
renderUnderarmour.

WHAT AN UNDERARMOUR IS
----------------------
Every underarmour garment has exactly TWO mod slots, and the ARMO records say so
outright — `ap_armor_Lining` and `ap_underarmor_style`. Nothing else. So the page
is three groups, not one per set:

    Skins    the garment you craft and wear      (BOOK -> ARMO)
    Lining   slot 1 — where the resistances live (BOOK -> OMOD @ ap_armor_Lining)
    Style    slot 2 — the look AND the S.P.E.C.I.A.L. (BOOK -> OMOD @ ap_underarmor_style)

Linings and styles are UNIVERSAL: any style fits any garment, and the in-game
"Available Mods" list shows the five lining tiers whatever you are modding. The
per-set OMODs (mod_armor_UnderArmor_Casual_Treated vs _BoS_Treated) are Bethesda's
internal plumbing, but the PLANS are still learned per set — hence 7 sets x 4
learnable tiers = 28 lining plans, carried as a `set` pill rather than a heading.

WHERE THE BUFFS ACTUALLY LIVE  (this is the whole point of the page)
--------------------------------------------------------------------
  * The GARMENT carries none. `Armor_Casual_Underarmor_FlannelJeans` holds a
    Value, a Weight and two slots — no S.P.E.C.I.A.L., no DAMA resistances at
    all. Skins therefore get NO Buffs & Effects box, and the group carries a
    note saying so, because a reader who sees an empty box assumes the data was
    forgotten.
  * The STYLE carries the S.P.E.C.I.A.L. `mod_armor_UnderArmor_style_Enclave`
    is Perception +4 / Intelligence +2 / Agility +4 — which is exactly what the
    game shows on a Flannel Shirt with Enclave Style fitted. These already
    resolve through the normal plan_master path.
  * The LINING carries the resistances, but NOT on the per-set OMOD. Those hold
    a single `dn_VaultSuitLiningN` keyword, which resolve_effects skips as
    plumbing — which is why every lining row used to come back empty. The real
    Damage Type Values sit on five shared parent records:
        _PARENT_mod_UnderArmorMaterial_{Standard,Treated,Resistant,Protective,Shielded}
    each with six Damage Type Value properties (Physical / Energy / Rad / Fire /
    Cryo / Poison) pointing at CT_Player_Armor_Universal_TierNN curves.

    The link is the per-set OMOD's own Includes array:
        mod_armor_UnderArmor_VaultSuit_Resistant
            DATA -> Includes -> Include #0 -> Mod
                 -> _PARENT_mod_UnderArmorMaterial_Resistant
    Every set's lining of a given tier includes the SAME parent, which is why the
    game offers one "Treated Lining" rather than eight and why the tier gives
    identical resistances on every set.

    That array was invisible until now: it sits under DATA, and the OMOD export
    read it at record level, so all 12,446 rows shipped IncludeCount 0 and an
    empty Includes_Flat (fixed 2026-09-13 in ExportOMODToTSV.pas v3, same nesting
    bug that hid Properties before its v2). Re-export OMOD and `_parent_by_includes`
    picks the real link up automatically.

    `PARENT_BY_TIER` remains as the fallback for any export taken before that fix,
    and the build log says which path each tier used. It is a sound fallback — the
    five parents are named for the five tiers and nothing else uses those names —
    but the Includes link is the authority when present.

ROSTER
------
Underarmour plans are BOOK records whose EDID/COBJ/CNAM mention underarmour.
Rows are copied VERBATIM out of the plan_master.json the same build produced, so
an underarmour row here and the same plan's row on the Apparel or Body Armour
checklist are the same bytes and cannot drift. This builder therefore runs AFTER
build_plan_obtain_json.py in every workflow. The only fields added on top are
`group`, `set`, `tier`, `is_new`, `obtain_ledger` and (for linings) `effects`.

THE ★ NEW PILL — TSV DIFF, NOT A DATE
-------------------------------------
`is_new` is true when the plan is in the newest BOOK export and absent from the
baseline one — the same comparison the New Plans page uses, shared from
build_new_plans_json.resolve_baseline so the two pages can never disagree. It is
NOT the 31-day `added`-date rule the pennant page uses: these plans carry no
`added` date, and a date rule would go stale on its own. Upload a newer export
and last patch's pills clear themselves.

Live diffs newest-vs-previous; PTS diffs newest-PTS-vs-newest-LIVE (pass
--baseline-dir tsv), so the PTS page marks what the live game has not seen.

USAGE
-----
    python src/build_underarmour_json.py --data-dir tsv --outdir dist
    python src/build_underarmour_json.py --data-dir tsv/pts --baseline-dir tsv --outdir dist
"""
import os, re, csv, json, argparse, sys, collections
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV  = os.path.join(REPO, "tsv")
DIST = os.path.join(REPO, "dist")
sys.path.insert(0, HERE)

import tsv_source
import build_new_plans_json as bnp            # the shared newest-vs-baseline diff

# NOT importing build_plan_obtain_json. Its resolve_effects() would do this job,
# but reaching it means importing rng76, spawns_engine and build_farming_used_for
# at module scope — the heaviest stack in the repo — to use one branch of one
# function. The parent material OMODs hold nothing but Damage Type Value, Weight
# and Value properties (no ENCH, no MGEF, no PERK), so the 40 lines below cover
# them completely and this builder stays standalone and quick.
# If that module is ever split so the effects resolver imports on its own, delete
# _resolve_dmgt_effects / _curve_* / _num and call it instead.

# ── what counts as underarmour ───────────────────────────────────────────────
RX_UA = re.compile(r"underarmor|under_armor|underarmour", re.I)

# ...and what does not, despite saying so. A few plans are named for an
# underarmour set but make a piece of headwear, which is a different item with
# none of the two mod slots — the Marine Tactical Helmet is the one in the
# current data. Matched on the CREATED record only: the plan's own EditorID says
# "Underarmor_Helmet", so testing that would throw the whole Marine set out.
RX_NOT_UA_CREATED = re.compile(r"headwear", re.I)

# Set key -> the name shown on the pill. Order here is cosmetic; groups sort A-Z.
SETS = [
    ("BOS",           "Brotherhood of Steel"),
    ("Casual",        "Casual"),
    ("Enclave",       "Operative"),
    ("Marine",        "Marine"),
    ("Raider",        "Raider"),
    ("SecretService", "Secret Service"),
    ("Muni",          "Civil Engineer"),
    ("VaultSuit",     "Vault Suit"),
]
# VaultSuit is matched last and by a looser pattern: the vault garments are named
# for their vault number (VaultSuit76, VaultSuit94, VaultSuitVT) and a couple of
# one-offs (Survivors Denim, the Nuclear Winter tracksuit) carry no set token.
RX_VAULTISH = re.compile(r"vaultsuit|survivorsdenim|tracksuit", re.I)

TIER_BY_WORD = {"standard": "Mk1", "treated": "Mk2", "resistant": "Mk3",
                "protective": "Mk4", "shielded": "Mk5"}
TIER_LABEL = {"Mk1": "Standard", "Mk2": "Treated", "Mk3": "Resistant",
              "Mk4": "Protective", "Mk5": "Shielded"}
# The name join described in the docstring. Mk1 Standard is fitted by default and
# has no plan, so it never reaches a row — it is listed for completeness.
PARENT_BY_TIER = {t: f"_PARENT_mod_UnderArmorMaterial_{w.title()}"
                  for w, t in TIER_BY_WORD.items()}

# Trailing (?=_|$) not \b — "_" is a word character, so \b never fires between
# "Standard" and a following "_", and half the matches silently miss.
RX_TIER  = re.compile(r"(?:^|[_\s])(standard|treated|resistant|protective|shielded)(?=[_\s]|$)", re.I)
RX_MK    = re.compile(r"_Mk(\d)(?=_|$)", re.I)
RX_STYLE = re.compile(r"_style_", re.I)

GROUPS = [("skin", "Skins"), ("lining", "Lining"), ("style", "Style")]

# ── the How to Obtain ledger ────────────────────────────────────────────────
# A fixed list of routes, every one printed even when it does not apply. The N/A
# rows are the point: a reader who sees only two lines cannot tell whether the
# rest were checked or forgotten. Order is the house order from the CAMP pages.
LEDGER_ROWS = ["Caps", "Stamps", "Scoreboard", "Gold Bullion", "Atom Shop",
               "Limited Time Bundle", "Containers", "Events & Activities",
               "Quests", "Challenges"]

# Vendor routes only fill the Caps row when the holder really is a caps vendor.
# Anything else stays in Containers rather than being guessed into Caps.
RX_CAPS_VENDOR = re.compile(r"vendor|trader|merchant|shop", re.I)
RX_GOLD   = re.compile(r"goldvendor|_w05_|goldbullion", re.I)
RX_STAMP  = re.compile(r"stampvendor|_stamps?_", re.I)
RX_SCORE  = re.compile(r"^score_|_score_|scoreboard", re.I)
RX_ATOM   = re.compile(r"^atx_|atomshop", re.I)


def read_rows(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        yield from csv.DictReader(f, delimiter="\t")


def newest_in(data_dir, pattern):
    return tsv_source.newest(os.path.join(data_dir, pattern), required=False)


# ── minimal effects resolver (Damage Type Value + curve ladder) ──────────────
# Mirrors the shapes build_plan_obtain_json emits so the renderer needs no
# special case: {"summary": str, "rows": [{label, value, points, curve}]}.
_DMGT_LABEL = {
    "dtphysical": "Damage Resistance", "dtenergy": "Energy Resistance",
    "dtradiationexposure": "Radiation Resistance", "dtfire": "Fire Resistance",
    "dtcryo": "Cryo Resistance", "dtpoison": "Poison Resistance",
}

def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    s = f"{f:g}"
    return f"+{s}" if f > 0 else s


def _curve_index(data_dir):
    """EDID(lower) -> [(level, value)], sorted, levels above 100 dropped."""
    f = newest_in(data_dir, "CURV_Export_*_POINTS.tsv")
    out = collections.defaultdict(list)
    if not f:
        return {}
    for row in read_rows(f):
        eid = (row.get("EDID") or "").strip()
        if not eid:
            continue
        try:
            out[eid.lower()].append((float(row.get("X") or 0), float(row.get("Y") or 0)))
        except ValueError:
            continue
    return {k: sorted(p for p in v if p[0] <= 100) for k, v in out.items()}


def _curve_pts(curve_idx, field):
    """The breakpoints the curve defines. A flat curve is not a ladder."""
    pts = curve_idx.get((field or "").split("[")[0].strip().lower())
    if not pts or len({y for _, y in pts}) <= 1:
        return None
    return [{"lv": int(x), "v": _num(y)} for x, y in pts]


def _curve_display(curve_idx, field):
    pts = curve_idx.get((field or "").split("[")[0].strip().lower())
    if not pts:
        return None
    lo, hi = pts[0], pts[-1]
    return _num(lo[1]) if lo[1] == hi[1] else \
        f"{_num(lo[1])} at Lv {int(lo[0])} → {_num(hi[1])} at Lv {int(hi[0])}"


def _resolve_dmgt_effects(props, curve_idx):
    rows, seen = [], set()
    for p in props:
        if (p.get("PropertyName") or "").strip() != "Damage Type Value":
            continue                       # Weight / Value / Keywords are not shown
        v1 = p.get("Value1") or ""
        key = (v1.split()[0] if v1.split() else "").lower()
        label = _DMGT_LABEL.get(key) or key.title() or "Resistance"
        curve = p.get("CurveTable") or ""
        value = _curve_display(curve_idx, curve)
        if value is None:
            value = _num(p.get("Value2"))
            if value in ("+0", "0"):
                continue
        k = (label, value)
        if k in seen:
            continue
        seen.add(k)
        rows.append({"label": label, "value": value,
                     "points": _curve_pts(curve_idx, curve),
                     "curve": curve.split("[")[0].strip() or None})
    return {"summary": "", "rows": rows} if rows else None


# Includes_Flat is emitted as  EDID "FULL" [OMOD:xxxxxxxx] | EDID "FULL" [...]
_RX_INCL_FID = re.compile(r"\[OMOD:([0-9A-Fa-f]{8})\]")


def _omod_index(data_dir):
    """(edid_lower -> formid, formid -> [property rows], formid -> [included formids])."""
    by_edid, props = {}, collections.defaultdict(list)
    includes = {}
    f = newest_in(data_dir, "OMOD_Export_*.tsv")
    if f:
        for row in read_rows(f):
            eid = (row.get("OMOD_EDID") or "").strip()
            fid = (row.get("OMOD_FormID") or "").strip().upper()
            if eid and fid:
                by_edid.setdefault(eid.lower(), fid)
            if fid:
                inc = _RX_INCL_FID.findall(row.get("Includes_Flat") or "")
                if inc:
                    includes[fid] = [x.upper() for x in inc]
    fp = newest_in(data_dir, "OMOD_Export_*_Properties.tsv")
    if fp:
        for row in read_rows(fp):
            fid = (row.get("OMOD_FormID") or "").strip().upper()
            if fid:
                props[fid].append(row)
    return by_edid, dict(props), includes


def blob_of(it):
    return " ".join([
        (it.get("plan_item") or {}).get("edid") or "",
        (it.get("cobj") or {}).get("edid") or "",
        (it.get("cnam") or {}).get("edid") or "",
    ])


def set_of(blob):
    for key, label in SETS:
        if key == "VaultSuit":
            continue
        if re.search(key, blob, re.I):
            return label
    if RX_VAULTISH.search(blob):
        return "Vault Suit"
    return None


def group_of(it, blob):
    """skin / lining / style — decided by what the plan creates, not its name."""
    sig = ((it.get("cnam") or {}).get("sig") or "").upper()
    if RX_STYLE.search(blob):
        return "style"
    if sig == "OMOD" and RX_TIER.search(blob):
        return "lining"
    if sig == "ARMO":
        return "skin"
    # No created record resolved. A tier word still means a lining; otherwise the
    # plan makes the garment itself.
    return "lining" if RX_TIER.search(blob) else "skin"


def _tier_from_word(text):
    m = RX_TIER.search(text or "")
    return TIER_BY_WORD.get(m.group(1).lower()) if m else None


def tier_of(it, blob):
    """Mk tier for a lining plan, and whether the sources agreed.

    Read in this order, and the order matters:
      1. the Mk number in the plan's own BOOK EditorID
      2. the tier word in the plan's in-game name
      3. the tier word on the created OMOD  (LAST — it is the unreliable one)

    Those first two agree on all 28 lining plans. The created OMOD does NOT: four
    rows point at the wrong tier's record — "Plan: Shielded Lining BoS Underarmor"
    (EditorID ..._Mk5) links to CNAM mod_armor_UnderArmor_BOS_Standard, and three
    Operative plans are each off by one. That is an upstream data error, not a
    parsing bug, so trusting the created record would put Mk5 resistances on an
    Mk1 row and mis-sort the group. Disagreements are counted and reported.
    """
    edid = (it.get("plan_item") or {}).get("edid") or ""
    m = RX_MK.search(edid)
    by_edid = f"Mk{m.group(1)}" if m else None
    by_name = _tier_from_word(it.get("name") or "")
    by_cnam = _tier_from_word((it.get("cnam") or {}).get("edid") or "")

    tier = by_edid or by_name or by_cnam or _tier_from_word(blob)
    agreed = (by_cnam is None) or (by_cnam == tier)
    return tier, agreed, by_cnam


def build_ledger(it, blob):
    """The fixed route table. Returns [{label, applies, detail{}|rows[]}]."""
    routes = it.get("obtain_routes") or []
    conts = [r for r in routes if r.get("source_type") == "container"]
    evq   = [r for r in routes if r.get("source_type") in ("event-quest", "creature", "fixed")]
    vend  = [r for r in routes if r.get("source_type") == "vendor"]

    caps_vendors = [r for r in vend if RX_CAPS_VENDOR.search(r.get("route") or "")]
    # A vendor route whose holder does not read as a shop is not evidence of a
    # caps purchase — it goes to Containers with the rest of the world loot.
    other_vendors = [r for r in vend if r not in caps_vendors]

    def rate_rows(rs):
        return [{"route": r.get("route"), "rate_display": r.get("rate_display"),
                 "rate": r.get("rate")} for r in rs[:6]]

    filled = {}
    if RX_GOLD.search(blob):
        filled["Gold Bullion"] = {"kind": "kv", "kv": [
            ["Plan", (it.get("name") or "").replace("Plan: ", "")],
            ["Vendor", "Gold Bullion vendor"],
        ]}
    if RX_STAMP.search(blob):
        filled["Stamps"] = {"kind": "kv", "kv": [["Vendor", "Stamp vendor"]]}
    if RX_SCORE.search(blob):
        filled["Scoreboard"] = {"kind": "kv", "kv": [["Source", "Season scoreboard reward"]]}
    if RX_ATOM.search(blob):
        filled["Atom Shop"] = {"kind": "kv", "kv": [["Source", "Atom Shop"]]}
    if caps_vendors:
        filled["Caps"] = {"kind": "rates", "rows": rate_rows(caps_vendors)}
    if conts or other_vendors:
        filled["Containers"] = {"kind": "rates", "rows": rate_rows(conts + other_vendors)}
    if evq:
        filled["Events & Activities"] = {"kind": "rates", "rows": rate_rows(evq)}

    out = []
    for label in LEDGER_ROWS:
        hit = filled.get(label)
        out.append({"label": label, "applies": bool(hit), **(hit or {})})
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=TSV, help="TSV export root; tsv/pts on the PTS channel")
    ap.add_argument("--baseline-dir", default="",
                    help="NEW-pill baseline root. PTS passes 'tsv' so its pills mark "
                         "what the live game has not seen.")
    ap.add_argument("--outdir", default=DIST)
    ap.add_argument("--master", default="", help="default <outdir>/plan_master.json")
    ap.add_argument("--out", default="", help="default <outdir>/underarmour.json")
    args = ap.parse_args(argv)

    master_path = args.master or os.path.join(args.outdir, "plan_master.json")
    out_path    = args.out    or os.path.join(args.outdir, "underarmour.json")

    print(f"[underarmour] reading {master_path}")
    with open(master_path, encoding="utf-8") as f:
        master = json.load(f)

    def created_blob(it):
        return " ".join([(it.get("cobj") or {}).get("edid") or "",
                         (it.get("cnam") or {}).get("edid") or ""])

    candidates = [it for it in master.get("items", []) if RX_UA.search(blob_of(it))]
    roster, not_ua = [], []
    for it in candidates:
        if RX_NOT_UA_CREATED.search(created_blob(it)):
            not_ua.append(it.get("name"))
        else:
            roster.append(it)
    print(f"[underarmour] roster: {len(roster)} underarmour plans")
    if not_ua:
        print(f"[underarmour] excluded {len(not_ua)} plan(s) that name an underarmour set but "
              f"make headwear: {', '.join(not_ua)}")
    if not roster:
        raise SystemExit("[underarmour] no underarmour plans found — is plan_master.json the right channel?")

    # ── the ★ NEW set, shared with the New Plans page ────────────────────────
    newest_f = tsv_source.newest(os.path.join(args.data_dir, bnp.BOOK_GLOB),
                                 exclude=bnp.BOOK_EXCLUDE, required=False)
    new_ids, baseline_name, mode = set(), None, "vs-previous"
    if newest_f:
        cur = bnp.book_plan_ids(newest_f)
        base_f, mode, skipped = bnp.resolve_baseline(args.data_dir, args.baseline_dir, cur, newest_f)
        if base_f:
            new_ids = set(cur) - set(bnp.book_plan_ids(base_f))
            baseline_name = os.path.basename(base_f)
        print(f"[underarmour] NEW pill: mode={mode} newest={os.path.basename(newest_f)} "
              f"baseline={baseline_name} added={len(new_ids)}"
              + (f" (skipped {len(skipped)} duplicate export(s))" if skipped else ""))

    # ── lining effects: resolve from the shared parent material OMOD ─────────
    print("[underarmour] loading OMOD + curve tables ...")
    omod_by_edid, omod_props, omod_includes = _omod_index(args.data_dir)
    curve_idx = _curve_index(args.data_dir)

    parent_fid = {t: omod_by_edid[e.lower()]
                  for t, e in PARENT_BY_TIER.items() if e.lower() in omod_by_edid}
    tier_by_parent_fid = {fid: t for t, fid in parent_fid.items()}

    def _parent_by_includes(cnam_fid):
        """Tier of the parent material record this lining actually includes.

        The authoritative link (see WHERE THE BUFFS LIVE). Returns None when the
        OMOD export predates the Includes fix, and the caller falls back to the
        tier-name join.
        """
        for inc_fid in omod_includes.get((cnam_fid or "").upper(), []):
            t = tier_by_parent_fid.get(inc_fid)
            if t:
                return t
        return None
    missing = [t for t in PARENT_BY_TIER if t not in parent_fid]
    if missing:
        print(f"[underarmour] WARNING: no parent material OMOD for tier(s) {missing} — "
              f"those linings will have no resistances. Check PARENT_BY_TIER against "
              f"the OMOD export; Bethesda may have renamed the records.")

    parent_effects = {}
    for tier, fid in parent_fid.items():
        fx = _resolve_dmgt_effects(omod_props.get(fid) or [], curve_idx)
        if fx:
            parent_effects[tier] = fx
    print(f"[underarmour] resolved resistances for {len(parent_effects)} lining tier(s): "
          f"{', '.join(sorted(parent_effects)) or '(none)'}")
    if not omod_includes:
        print("[underarmour] NOTE: this OMOD export carries no Includes data, so the "
              "lining->parent link falls back to the tier-name join. Re-export OMOD with "
              "ExportOMODToTSV.pas v3 or newer to use the real link.")

    # ── build the rows ──────────────────────────────────────────────────────
    buckets = {k: [] for k, _ in GROUPS}
    no_set, no_effects, tier_conflicts = [], [], []
    link_real = link_name = 0
    for it in roster:
        if it.get("cut"):
            continue
        blob = blob_of(it)
        grp  = group_of(it, blob)
        st   = set_of(blob)
        if not st:
            no_set.append(it.get("name"))
            st = "Other"
        tier = None
        if grp == "lining":
            tier, agreed, by_cnam = tier_of(it, blob)
            if not agreed:
                tier_conflicts.append(f"{it.get('name')} (EditorID says {tier}, "
                                      f"created OMOD says {by_cnam})")

        row = dict(it)                       # verbatim — see ROSTER in the docstring
        row["group"] = grp
        row["set"]   = st
        row["tier"]  = tier
        row["tier_label"] = TIER_LABEL.get(tier) if tier else None
        row["is_new"] = ((it.get("plan_item") or {}).get("formid") or "").upper() in new_ids
        row["obtain_ledger"] = build_ledger(it, blob)

        if grp == "lining":
            # Swap in the parent's resistances. The per-set OMOD only ever holds
            # the tier keyword, so whatever plan_master resolved here is empty.
            #
            # Which parent: follow the created OMOD's Includes when the export has
            # them, else fall back to the tier-name join. Both are recorded so the
            # log can say how many rows used the real link.
            cnam_fid = (it.get("cnam") or {}).get("formid")
            linked_tier = _parent_by_includes(cnam_fid)
            if linked_tier:
                link_real += 1
            else:
                link_name += 1

            # The plan's own name and EditorID decide which tier the row IS —
            # they agree on all 28, and they are what the player reads. The
            # created OMOD is NOT trusted for this: four plans point at the wrong
            # tier's record (see tier_conflicts). Where the two disagree the row
            # keeps its claimed tier and carries a warning, rather than silently
            # showing one tier's name above another tier's numbers.
            fx = parent_effects.get(tier)
            row["effects"] = fx
            row["effects_source"] = PARENT_BY_TIER.get(tier) if fx else None
            row["effects_link"] = "includes" if linked_tier else "tier-name"
            if linked_tier and linked_tier != tier:
                row["tier_conflict"] = {
                    "claimed": tier,
                    "claimed_label": TIER_LABEL.get(tier),
                    "actual": linked_tier,
                    "actual_label": TIER_LABEL.get(linked_tier),
                }
            if not fx:
                no_effects.append(it.get("name"))
        elif grp == "skin":
            # Garments carry no effects at all — see WHERE THE BUFFS LIVE. The
            # renderer omits the box entirely rather than showing an empty one.
            row["effects"] = None
            row["no_effects_by_design"] = True
        buckets[grp].append(row)

    buckets["skin"].sort(key=lambda r: (r["name"] or "").lower())
    buckets["style"].sort(key=lambda r: (r["name"] or "").lower())
    buckets["lining"].sort(key=lambda r: (int((r["tier"] or "Mk9")[2:]), (r["set"] or "").lower()))

    groups = [{"key": k, "label": lbl, "count": len(buckets[k]), "items": buckets[k]}
              for k, lbl in GROUPS if buckets[k]]

    out = {
        "version": 1,
        "generated": datetime.now(timezone.utc).isoformat(),
        "count": sum(len(buckets[k]) for k, _ in GROUPS),
        "slots": ["ap_armor_Lining", "ap_underarmor_style"],
        "new_pill": {"mode": mode,
                     "newest": os.path.basename(newest_f) if newest_f else None,
                     "baseline": baseline_name,
                     "count": sum(1 for g in groups for i in g["items"] if i["is_new"])},
        "groups": groups,
    }
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[underarmour] wrote {out_path}  ({out['count']} plans)")
    for g in groups:
        print(f"   {g['label']}: {g['count']}")
    print(f"   NEW pills: {out['new_pill']['count']}")
    print(f"   lining->parent link: {link_real} via Includes, {link_name} via tier name")
    if no_set:
        print(f"[underarmour] {len(no_set)} plan(s) matched no set, filed under 'Other': "
              f"{', '.join(no_set[:6])}" + (" ..." if len(no_set) > 6 else ""))
    if no_effects:
        print(f"[underarmour] {len(no_effects)} lining(s) resolved no resistances: "
              f"{', '.join(no_effects[:6])}" + (" ..." if len(no_effects) > 6 else ""))
    if tier_conflicts:
        # Upstream data error, not a build failure — the EditorID is trusted and
        # the page is correct. Printed so a NEW conflict is visible rather than
        # silently absorbed.
        print(f"[underarmour] {len(tier_conflicts)} plan(s) whose created OMOD names a "
              f"different tier than the plan does (EditorID wins):")
        for c in tier_conflicts:
            print(f"     {c}")
    return out


if __name__ == "__main__":
    main()
