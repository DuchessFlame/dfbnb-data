#!/usr/bin/env python3
r"""
build_brain_in_a_jar_json.py - dist JSON for the Brain in a Jar pages (Return to
Skyline Valley, quest "A Matter of Matter", RTSV_SQ01_Collectible).

  /df/collectables/brain-in-a-jar/guide/                      guide  (dist/brain_in_a_jar_guide.json)
  /df/collectables/brain-in-a-jar/checklist/                  reward checklist
  /df/collectables/brain-in-a-jar/brain-in-a-jar-locations-<a>-<b>/   30 per page

Same document shape as dist/abraxodyne_intel_briefcases.json (pages[], locations[]
with n / fast_travel / directions / image_top / image_bottom / exists) so the
renderer pattern carries over, plus a data-driven rewards block.

Inputs (nothing typed - every name comes from the exports):
  data/brain_in_a_jar/placements_<channel>.tsv   esm_scan_placements.py (local, DB)
  data/brain_in_a_jar/numbering.tsv              fixed numbers; new refs appended here
  CHAL_Export_*.tsv     the collect-brain challenge tiers -> milestone + reward text
  GMRW_Export_*.tsv     the per-brain hand-in reward (QuestReward_RTSV_SQ01_*)
  GLOB_Export_*.tsv     caps value of that reward
  NOTE_Export_*.tsv     the Scan holotapes (RTSV_SQ01_Collectible_Holotape_##)

Hand-authored fields (fast_travel, directions, image_top, image_bottom) are kept
across rebuilds, keyed by the brain's REF FormID, never by its number.

Channel: DFBNB_CHANNEL=pts (or --pts) reads tsv/pts + placements_pts.tsv. Output goes
to OUT_DIR (default dist/); the PTS workflow relocates dist/ -> dist/pts/.
"""
import csv, json, math, os, re, sys, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import tsv_source

csv.field_size_limit(10 ** 9)
DATA = os.path.join(REPO, "data", "brain_in_a_jar")
OUT_DIR = os.environ.get("OUT_DIR", os.path.join(REPO, "dist"))
OUT_NAME = "brain_in_a_jar.json"
PAGE_SIZE = 30
BRAIN_FORMID = "008ABB18"                 # RTSV_SQ01_BrainInJar  "Brain in a Jar"
QUEST_EDID = "RTSV_SQ01_Collectible"
CHAL_PREFIX = "Challenge_Lifetime_RTSV_Collect_Brain_Tier"
HOLO_PREFIX = "RTSV_SQ01_Collectible_Holotape_"
IMAGE_BASE = "/wp-content/uploads/guide-images/collectables/Brain-in-a-Jar-Locations/"
URL_BASE = "/df/collectables/brain-in-a-jar/"

# CHAL RNAM (the reward icon) -> the reward type the checklist shows.
ICON_TYPE = {"IconCR_Recipe": "CAMP item", "IconCR_WeaponMod": "Weapon mod",
             "IconCR_Weapon": "Plan", "IconCR_Armor": "Apparel", "IconCR_Caps": "Caps"}


def _clean(s):
    return re.sub(r"\s+", " ", (s or "").replace("¬", "")).strip()


def _rows(pattern, channel):
    p = tsv_source.newest(pattern, channel=channel, required=False)
    if not p:
        print(f"  (no {pattern} on {channel})")
        return []
    with open(p, encoding="utf-8", errors="replace") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def load_numbering():
    path = os.path.join(DATA, "numbering.tsv")
    head, num = [], {}
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    head = [l for l in lines if l.startswith("#")]
    for r in csv.DictReader([l for l in lines if not l.startswith("#")], delimiter="\t"):
        num[r["RefFormID"]] = int(r["n"])
    return path, head, num


def load_placements(channel):
    path = os.path.join(DATA, f"placements_{channel}.tsv")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return [r for r in csv.DictReader(fh, delimiter="\t") if r["BaseFormID"] == BRAIN_FORMID]


def assign_numbers(place, num_path, head, num):
    """Keep every existing number. New refs are appended in nearest-neighbour order
    from the last numbered brain, so the route keeps going rather than restarting."""
    new = [r for r in place if r["RefFormID"] not in num]
    if not new:
        return num
    xy = {r["RefFormID"]: (float(r["X"]), float(r["Y"])) for r in place}
    last = max(num, key=num.get) if num else None
    cur = xy.get(last, xy[new[0]["RefFormID"]])
    left = {r["RefFormID"] for r in new}
    n = max(num.values(), default=0)
    while left:
        k = min(left, key=lambda k: math.hypot(xy[k][0] - cur[0], xy[k][1] - cur[1]))
        n += 1; num[k] = n; cur = xy[k]; left.remove(k)
    with open(num_path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(head) + "\nn\tRefFormID\n")
        for k, v in sorted(num.items(), key=lambda kv: kv[1]):
            fh.write(f"{v}\t{k}\n")
    print(f"  numbered {len(new)} new brain(s)")
    return num


def build_rewards(channel):
    tiers = []
    for r in _rows("CHAL_Export_*.tsv", channel):
        eid = r.get("EDID", "")
        if not eid.startswith(CHAL_PREFIX):          # zzz = cut tier, skipped by prefix
            continue
        try:
            need = int(float(r.get("TNAM") or 0))
        except ValueError:
            continue
        if need <= 0 or need >= 9999999:
            continue
        name = _clean(r.get("MNAM"))
        tiers.append({"id": "rw-" + re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-"),
                      "name": name, "milestone": need,
                      "type": ICON_TYPE.get(r.get("RNAM", ""), "Reward"),
                      "challenge": _clean(r.get("FULL")), "form_id": r.get("FormID", ""),
                      "edid": eid, "image": "", "note": ""})
    tiers.sort(key=lambda t: t["milestone"])

    globs = {g.get("EDID"): g.get("FLTV") for g in _rows("GLOB_Export_*.tsv", channel)}
    hand_in = None
    for g in _rows("GMRW_Export_*.tsv", channel):
        if QUEST_EDID in (g.get("Ref1") or "") and g.get("EDID", "").startswith("QuestReward_"):
            caps_glob = (g.get("NAM8_CapsGlobal") or "").split(":")[1:2]
            caps = globs.get(caps_glob[0]) if caps_glob else None
            item = (g.get("RewardedItem") or "").split(":")
            hand_in = {"edid": g["EDID"],
                       "caps": int(float(caps)) if caps else None,
                       "xp_curve": (g.get("XPCT_XPCurveTable") or "").split(":")[1:2] or None,
                       "loot_list": item[1] if len(item) > 1 else None}
            break
    return tiers, hand_in


def build_holotapes(channel):
    out = []
    for r in _rows("NOTE_Export_*.tsv", channel):
        eid = r.get("EDID") or r.get("NOTE_EDID") or ""          # NOTE export prefixes its columns
        if eid.startswith(HOLO_PREFIX):
            out.append({"form_id": r.get("FormID") or r.get("NOTE_FormID", ""), "edid": eid,
                        "name": _clean(r.get("FULL"))})
    return sorted(out, key=lambda h: h["name"])


def _index(d):
    return {l["ref"]: l for l in (d or {}).get("locations", []) if l.get("ref")}


def load_existing(out_path, channel):
    """Hand-authored fields from the last build. The PTS workflow starts with
    `rm -rf dist`, so on CI the previous file is only in git - fall back to
    `git show HEAD:<path>` or every rebuild would wipe the photos/directions."""
    rel = f"dist/{'pts/' if channel == 'pts' else ''}{OUT_NAME}"
    for path in (out_path, os.path.join(REPO, rel)):
        try:
            with open(path, encoding="utf-8") as fh:
                got = _index(json.load(fh))
            if got:
                return got
        except (OSError, ValueError):
            pass
    try:
        import subprocess
        raw = subprocess.run(["git", "-C", REPO, "show", f"HEAD:{rel}"], capture_output=True,
                             text=True, encoding="utf-8", check=True).stdout
        return _index(json.loads(raw))
    except Exception:
        return {}


def main():
    channel = tsv_source.channel_of()
    place = load_placements(channel)
    if place is None:
        print(f"No data/brain_in_a_jar/placements_{channel}.tsv - nothing to build on this channel.")
        return
    num_path, head, num = load_numbering()
    num = assign_numbers(place, num_path, head, num)

    out_path = os.path.join(OUT_DIR, OUT_NAME)
    old = load_existing(out_path, channel)

    total = len(place)
    pages = []
    for a in range(1, total + 1, PAGE_SIZE):
        b = min(a + PAGE_SIZE - 1, total)
        slug = f"brain-in-a-jar-locations-{a}-{b}"
        pages.append({"slug": slug, "title": f"Brain in a Jar Locations {a} - {b}",
                      "from": a, "to": b, "url": f"{URL_BASE}{slug}/",
                      "map": f"{IMAGE_BASE}{slug}-map.jpg"})

    locs = []
    for r in place:
        ref = r["RefFormID"]; n = num[ref]
        prev = old.get(ref, {})
        page = pages[(n - 1) // PAGE_SIZE]
        locs.append({
            "n": n, "ref": ref, "region": r["region"], "marker": r["marker"],
            "fast_travel": prev.get("fast_travel") or r["marker"],
            "directions": prev.get("directions", ""),
            "image_top": prev.get("image_top", ""),
            "image_bottom": prev.get("image_bottom", ""),
            "exists": True,
            "coords": [round(float(r["X"]), 1), round(float(r["Y"]), 1), round(float(r["Z"]), 1)],
            "page": page["slug"],
        })
    locs.sort(key=lambda l: l["n"])

    tiers, hand_in = build_rewards(channel)
    quest = next((q.get("FULL") for q in _rows("QUST2_Export_*_Quests.tsv", channel)
                  if q.get("EDID") == QUEST_EDID), None) or "A Matter of Matter"

    doc = {
        "_meta": {"generated": datetime.date.today().isoformat(), "channel": channel,
                  "source": "SeventySix.esm placements + CHAL/GMRW/GLOB/NOTE exports",
                  "note": "***Subject to change pending porting to live servers***" if channel == "pts" else ""},
        "set": "brain-in-a-jar", "name": "Brain in a Jar", "category": "Collectables",
        "quest": quest,
        "blurb": f"Every Brain in a Jar for the {quest} collectable quest, in route order. "
                 "Most of them are in Skyline Valley; follow the numbers and the page map.",
        "image_base": IMAGE_BASE,
        "full_map": IMAGE_BASE + "brain_in_a_jar.jpg",
        "numbered_map": IMAGE_BASE + "brain_in_a_jar_numbered.jpg",
        "region_maps": {reg: f"{IMAGE_BASE}{re.sub(r'[^a-z0-9]+', '-', reg.lower()).strip('-')}-spawn-map.jpg"
                        for reg in sorted({l['region'] for l in locs if l['region']})},
        "total_slots": total, "total_real": total,
        "pages": pages, "locations": locs,
        "rewards": {"milestones": tiers, "hand_in": hand_in},
        "holotapes": build_holotapes(channel),
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=False)
    print(f"{OUT_NAME}: {total} brains, {len(pages)} pages, {len(tiers)} reward tiers, "
          f"{len(doc['holotapes'])} holotapes -> {out_path}")
    build_guide(doc, channel)
    print("brain_in_a_jar_guide.json written")


def build_guide(doc, channel):
    """dist/brain_in_a_jar_guide.json - the shape df-bnb-brain-in-a-jar.js mounts
    (title / subtitle / sections[{title, html}]). Prose from data/brain_in_a_jar/guide.json,
    everything countable generated from doc."""
    import html as _h
    with open(os.path.join(DATA, "guide.json"), encoding="utf-8") as fh:
        g = json.load(fh)
    by_reg = {}
    for l in doc["locations"]:
        by_reg[l["region"]] = by_reg.get(l["region"], 0) + 1
    regions = ", ".join(f"{n} in {r}" for r, n in sorted(by_reg.items(), key=lambda kv: -kv[1]))
    hi = (doc.get("rewards") or {}).get("hand_in") or {}
    caps = f"{hi['caps']:,}" if hi.get("caps") is not None else "an unknown number of"
    fill = lambda t: (t.replace("{total}", str(doc["total_real"])).replace("{quest}", doc["quest"])
                      .replace("{terminal}", g.get("terminal", "")).replace("{caps}", caps)
                      .replace("{regions}", regions))
    E = _h.escape
    locs = "".join(f'<li><a href="{E(p["url"])}">{E(p["title"])}</a></li>' for p in doc["pages"])
    maps = "".join(f'<li><a href="{E(u)}" target="_blank" rel="noopener">{E(r)} map</a></li>'
                   for r, u in doc.get("region_maps", {}).items())
    maps += f'<li><a href="{E(doc["full_map"])}" target="_blank" rel="noopener">Full 4K map</a></li>'
    miles = "".join(f'<li><strong>{m["milestone"]}</strong> {"brain" if m["milestone"] == 1 else "brains"}: {E(m["name"])}</li>'
                    for m in doc["rewards"]["milestones"])
    tapes = "".join(f"<li>{E(h['name'])}</li>" for h in doc["holotapes"])
    intro = {"title": fill(g["intro"]["title"]), "html": fill(g["intro"]["html"])}
    if g["intro"].get("images"):          # names under <IMAGE_BASE>guide-images/<name>.avif
        intro["images"] = list(g["intro"]["images"])
    secs = [intro,
            {"title": "Brain in a Jar Locations", "html": f"<p>The brains are split into pages of {PAGE_SIZE}, in route order.</p><ul>{locs}</ul><h4>Maps</h4><ul>{maps}</ul>"},
            {"title": "Milestone Rewards", "html": f"<ul>{miles}</ul><p>Tick them off on the <a href=\"{URL_BASE}checklist/\">Brain in a Jar Reward Checklist</a>.</p>"}]
    if tapes:
        secs.append({"title": "The Scan Holotapes", "html": f"<p>{len(doc['holotapes'])} holotapes go with the questline.</p><ul>{tapes}</ul>"})
    secs.append({"title": fill(g["faq"]["title"]), "html": fill(g["faq"]["html"])})
    out = {"_meta": doc["_meta"], "title": fill(g["title"]), "subtitle": fill(g["subtitle"]), "sections": secs}
    if g.get("image_dims"):               # [w, h] per guide image, so the page reserves the space
        out["image_dims"] = g["image_dims"]
    with open(os.path.join(OUT_DIR, "brain_in_a_jar_guide.json"), "w", encoding="utf-8", newline="\n") as fh:
        json.dump(out, fh, indent=1, ensure_ascii=False)


if __name__ == "__main__":
    main()
