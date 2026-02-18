import csv
import json
import re
from datetime import datetime
from pathlib import Path

# ----------------------------
# CONFIG (edit paths as needed)
# ----------------------------
QUEST_TSV = Path("tsv/QUEST_Export_March_2026.tsv")
GMRW_TSV  = Path("tsv/GMRW_Export_March_2026.tsv")
LVLI_LIST_TSV    = Path("tsv/LVLI_Export_March_2026_LVLI_List.tsv")
LVLI_ENTRIES_TSV = Path("tsv/LVLI_Export_March_2026_LVLI_Entries.tsv")
GUIDE_INDEX_TSV  = Path("tsv/guide_index.tsv")

OUT_JSON = Path("dist/events_rewards.json")

MAX_GMRW_REFS = 10  # must match your QUEST export
MAX_REWARDS_PER_GMRW_ROW = None  # not used; GMRW export already provides rows

# ----------------------------
# Helpers
# ----------------------------
def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_ref_cell(cell: str):
    """
    Input examples:
      '0069FB67:QuestReward_MOON_MSignals_Stage1000'
      '0069FB67' (rare)
    Returns: (formid, edid_or_none)
    """
    cell = (cell or "").strip()
    if not cell:
        return None, None
    parts = cell.split(":")
    if len(parts) >= 2 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return parts[0].upper(), parts[1]
    if re.fullmatch(r"[0-9A-Fa-f]{8}", cell):
        return cell.upper(), None
    return None, None

def parse_item_token(token: str):
    """
    RewardedItem looks like:
      '0000000F:Caps001:CNCY'
      '00ABCDEF:SomeList:LVLI'
      '00ABCDEF:Something:WEAP' etc
    Returns dict with {formid, edid, sig}
    """
    token = (token or "").strip()
    if not token:
        return None
    parts = token.split(":")
    if len(parts) >= 3 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return {"formid": parts[0].upper(), "edid": parts[1], "sig": parts[2]}
    if len(parts) == 2 and re.fullmatch(r"[0-9A-Fa-f]{8}", parts[0]):
        return {"formid": parts[0].upper(), "edid": parts[1], "sig": None}
    return {"raw": token}

def best_bucket_from_edid(edid: str):
    """
    Heuristic bucket split:
    - If the LVLI EDID or reward EDID contains Common/Uncommon/Rare, use that.
    Otherwise Default.
    """
    t = (edid or "").lower()
    if "rare" in t:
        return "rare"
    if "uncommon" in t:
        return "uncommon"
    if "common" in t:
        return "common"
    return "default"

# ----------------------------
# Load guide index
# ----------------------------
def load_guides(path: Path):
    guides = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            title = row.get("title") or ""
            menu = row.get("menuTitle") or ""
            url  = row.get("url") or ""
            slug = row.get("slug") or ""
            guides.append({
                "title": title,
                "menuTitle": menu,
                "slug": slug,
                "url": url,
                "k": norm(title) or norm(menu) or norm(slug)
            })
    return guides

def match_guide_url(guides, quest_full_name: str):
    # strip "Event:" / "Activity:" prefix for matching
    name = quest_full_name.strip()
    name = re.sub(r"^(event|activity)\s*:\s*", "", name, flags=re.I).strip()
    key = norm(name)
    if not key:
        return ""

    # exact-ish match first
    for g in guides:
        if g["k"] == key:
            return g["url"]

    # contains match fallback
    for g in guides:
        if g["k"] and (g["k"] in key or key in g["k"]):
            return g["url"]

    return ""

# ----------------------------
# Load LVLI list + entries (indexed)
# ----------------------------
def load_lvli_list(path: Path):
    d = {}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            fid = (row.get("LVLI_FormID") or "").upper()
            if not fid:
                continue
            d[fid] = row
    return d

def load_lvli_entries_for_needed(path: Path, needed_formids: set):
    entries = {fid: [] for fid in needed_formids}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            fid = (row.get("LVLI_FormID") or "").upper()
            if fid in entries:
                entries[fid].append(row)
    return entries

# ----------------------------
# Load GMRW grouped by FormID
# ----------------------------
def load_gmrw(path: Path):
    g = {}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            fid = (row.get("FormID") or "").upper()
            if not fid:
                continue
            g.setdefault(fid, []).append(row)
    return g

# ----------------------------
# Load quests (Event/Activity only)
# ----------------------------
def load_event_quests(path: Path):
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            full = (row.get("FULL - Name") or "").strip()
            if not full:
                continue
            if not (full.lower().startswith("event:") or full.lower().startswith("activity:")):
                continue
            rows.append(row)
    return rows

# ----------------------------
# Build JSON
# ----------------------------
def main():
    guides = load_guides(GUIDE_INDEX_TSV)
    gmrw_by_id = load_gmrw(GMRW_TSV)

    quests = load_event_quests(QUEST_TSV)

    # Pass 1: determine LVLI formids needed from GMRW RewardedItem sig=LVLI
    needed_lvli = set()
    for q in quests:
        for i in range(MAX_GMRW_REFS):
            cell = q.get(f"GMRWRef{i}") or ""
            fid, _ = parse_ref_cell(cell)
            if not fid:
                continue
            for row in gmrw_by_id.get(fid, []):
                item = parse_item_token(row.get("RewardedItem") or "")
                if item and item.get("sig") == "LVLI":
                    needed_lvli.add(item["formid"])

    lvli_list = load_lvli_list(LVLI_LIST_TSV)
    lvli_entries = load_lvli_entries_for_needed(LVLI_ENTRIES_TSV, needed_lvli)

    out = {
        "generatedAt": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": {
            "questTsv": QUEST_TSV.as_posix(),
            "gmrwTsv": GMRW_TSV.as_posix(),
            "lvliListTsv": LVLI_LIST_TSV.as_posix(),
            "lvliEntriesTsv": LVLI_ENTRIES_TSV.as_posix(),
            "guideIndexTsv": GUIDE_INDEX_TSV.as_posix(),
        },
        "events": []
    }

    for q in quests:
        q_formid = (q.get("FormID") or "").upper()
        q_edid   = q.get("EDID") or ""
        q_full   = q.get("FULL - Name") or ""
        q_desc   = q.get("DESC - Description") or ""
        q_type   = q.get("Quest Type") or ""
        q_lnam   = q.get("LNAM - Location") or ""

        guide_url = match_guide_url(guides, q_full)

        event_obj = {
            "quest": {
                "formid": q_formid,
                "edid": q_edid,
                "full": q_full,
                "desc": q_desc,
                "type": q_type,
                "location": q_lnam,
            },
            "guideUrl": guide_url,
            "groups": {
                "default": [],
                "common": [],
                "uncommon": [],
                "rare": [],
                "bonus": []
            }
        }

        # Collect rewards from GMRW refs
        for i in range(MAX_GMRW_REFS):
            cell = q.get(f"GMRWRef{i}") or ""
            gmrw_id, gmrw_edid = parse_ref_cell(cell)
            if not gmrw_id:
                continue

            rows = gmrw_by_id.get(gmrw_id, [])
            if not rows:
                continue

            # Each row in your GMRW export is already “a reward row”
            for rrow in rows:
                rewarded_item = parse_item_token(rrow.get("RewardedItem") or "")
                bucket_hint = best_bucket_from_edid((rewarded_item or {}).get("edid") or gmrw_edid)

                reward = {
                    "source": {
                        "gmrwFormid": gmrw_id,
                        "gmrwEdid": gmrw_edid or (rrow.get("EDID") or ""),
                    },
                    "rewardIndex": rrow.get("RewardIndex"),
                    "rewardedItemIndex": rrow.get("RewardedItemIndex"),
                    "item": rewarded_item,
                    "count": rrow.get("RewardedItemCount"),
                    "xpGlobal": rrow.get("NAM7_XPGlobal") or "",
                    "capsGlobal": rrow.get("NAM8_CapsGlobal") or "",
                    "xpCurve": rrow.get("XPCT_XPCurveTable") or "",
                    "currencyObject": rrow.get("QRCO_CurrencyObject") or "",
                    "legendaryList": rrow.get("QRLI_LegendaryItemRewardList") or "",
                    "legendaryRank": rrow.get("QRLR_LegendaryItemRewardRank") or "",
                    "legendaryRankRandom": rrow.get("QRRI_LegendaryRankRandom") or "",
                    "conditions": rrow.get("Conditions") or "",
                    "conditionGlobs": rrow.get("ConditionGlobs") or "",

                    # Website-facing fields (may be filled later when you plug item metadata)
                    "imageUrl": "",
                    "releaseYear": "",
                    "tradeable": "",
                    "dropRate": "",          # can be computed later
                    "howToObtain": "",        # can be computed later

                    # LVLI expansion if the item is LVLI
                    "lvli": None
                }

                if rewarded_item and rewarded_item.get("sig") == "LVLI":
                    fid = rewarded_item["formid"]
                    reward["lvli"] = {
                        "list": lvli_list.get(fid, {}),
                        "entries": lvli_entries.get(fid, [])
                    }

                # Bonus bucket is a “future smart” bucket.
                # For now: only auto-place as bonus if it has Conditions text AND it looks like additive XP/Caps.
                is_bonus = False
                if (reward["conditions"] or reward["conditionGlobs"]) and (reward["xpGlobal"] or reward["capsGlobal"]):
                    is_bonus = True

                if is_bonus:
                    event_obj["groups"]["bonus"].append(reward)
                else:
                    event_obj["groups"][bucket_hint].append(reward)

        # Remove empty groups so UI can hide expands cleanly
        event_obj["groups"] = {k: v for k, v in event_obj["groups"].items() if v}
        out["events"].append(event_obj)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_JSON}")

if __name__ == "__main__":
    main()
