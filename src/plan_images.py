#!/usr/bin/env python3
r"""
plan_images.py — art for the plan checklist rows.

Two sources, in this order:

  1. ART THE SITE ALREADY HOSTS. Most plans build something another page has
     already photographed — a weather station, a buff station, a scoreboard
     reward, an Atom Shop item, a title. Those pages resolved their own art
     from the game files and the images are already on the server, so the plan
     row reuses that URL verbatim. Nothing is copied, nothing is staged twice,
     and an item whose art gets replaced upstream changes on the plan page too.

  2. THE PLAN'S OWN PAGE FOLDER. Whatever is left is staged by hand under
     /wp-content/uploads/guide-images/plan-checklist/<folder>/, one folder per
     page. Only stems with an .avif actually staged are emitted, so the front
     end never points at a 404; a plan with no art keeps the dashed placeholder
     and needs no code change when the file lands later.

The staged stem list is the same contract build_displays_json.py uses: run
locally with --avif-dir to scan the staging folders and persist the stems into
data/plan_images.json, commit that, and every later run — CI included —
rebuilds from the committed list. Without it a CI run sees empty folders and
would quietly strip every image.

New Plans is the oddball page the category rules warn about: it copies rows
verbatim out of plan_master, so each row already carries the folder of the page
it belongs to and lands on the right art with no special case.

Usage as a library:

    import plan_images
    idx = plan_images.load(dist_dir="dist", tsv_dir="tsv")
    plan_images.attach(items, idx)                      # sets image_dir+images
"""

import csv
import glob
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import asset_paths
except ImportError:                                # pragma: no cover
    asset_paths = None


def _route(url):
    """Season art through the site's own routing rule; anything else as-is."""
    if not url or asset_paths is None:
        return url
    try:
        return asset_paths.asset_url(url)
    except Exception:                              # noqa: BLE001 - never fatal
        return url

# ── page folders ────────────────────────────────────────────────────────────
# plan_master `type` (and the pages that are not plan_master rows) -> the folder
# on the server. These are the folders that exist under
# /wp-content/uploads/guide-images/plan-checklist/ and the ones mirrored in
# "Guides and Stuff\.Plan Checklist". One page per folder; the names are the
# SERVER's spelling, which is not always the page slug (recipe -> recipes,
# weapon -> weapons, body-armour page -> body-armour but the data bucket is
# still called armour).
PAGE_FOLDER = {
    "apparel":        "apparel",
    "armour":         "body-armour",
    "backpack-mod":   "backpack",
    "recipe":         "recipes",
    "weapon":         "weapons",
    "underarmour":    "underarmour",
    # Pages that do not render from plan_master, listed so this map is the one
    # place the folder names live.
    "workshop":       "workshop",
    "camera-mod":     "camera",
    "photomode":      "photomode",
    "displays":       "display",
    "pennants":       "pts-pennants",
    "fishing-rod":    "fishing-rod",
    "power-armour":   "power-armour",
    "snow-globes":    "snowglobe",
    "scoreboard-art": "scoreboard-art",
}

FOLDERS = sorted(set(PAGE_FOLDER.values()))

CONFIG = os.path.join("data", "plan_images.json")

_RX_IS_APPAREL = re.compile(
    r"\b(hats?|masks?|outfits?|costumes?|uniforms?|dress|bandana|glasses|"
    r"goggles)\b", re.I)
# A helmet is armour, whatever its record calls itself. Matching the EDID's
# "Headwear" instead of the name swept every Marine / Secret Service / Recon
# helmet into apparel — they are set pieces with resistances, not hats.
_RX_NOT_APPAREL = re.compile(r"\bhelmets?\b|power[_\s]*armou?r", re.I)


# ── which page folder a row's art belongs in ────────────────────────────────
# NOT the plan_master bucket. That bucket is the record type, and it lies: the
# "recipe" bucket holds 1,446 rows of which only 170 are food — the rest are
# CAMP furniture, wall decor, weapon mods and fishing gear that were filed there
# because nothing else fitted. Routing art by bucket put gravestones and posters
# in the recipes folder.
#
# So the row is classified by what the thing IS to a player, from the record it
# creates and the words in its EditorID. Order matters and is load-bearing:
#
#   * weapons before workshop — weapon paints carry "Workshop" in their EditorID
#     (MOON_General_Workshop_Mod_...), which would otherwise read as a CAMP item.
#   * power armour only for a mod or an armour piece — "Plan: Power Armor
#     Stations" is a FURN, a crafting station you build in your CAMP.
#   * fishing needs a rod-part word, not just "fishing" — a CAMP fishing barrel
#     is a workshop item.
#
# The CAMP folder is called "workshop" because that is what the EditorIDs call
# these records (SDOW_Workshop_SlasherBalloon, *_Recipe_Workshop_WallDecor_*).
_CAMP_SIGS = {"FURN", "ACTI", "STAT", "MSTT", "CONT", "FLOR", "LIGH", "DOOR", "TERM"}

_RX_FISHING   = re.compile(r"bobber|\bfloat\b|rodbase|fishing[_\s]*mod|fishing[_\s]*rod|"
                           r"\blure\b", re.I)
_RX_CAMERA    = re.compile(r"\bcamera\b|photomode[_\s]*lens", re.I)
# Photo mode has its own folder: frames AND poses. The camera rule runs first,
# because a photomode LENS is a camera mod.
_RX_PHOTOMODE = re.compile(r"photo[_\s]*mode|photo[_\s]*frame|photo[_\s]*poses?|"
                           r"\bposes?\b", re.I)
_RX_SNOWGLOBE = re.compile(r"snow[_\s]*globe", re.I)
_RX_POWERARM  = re.compile(r"power[_\s]*armou?r|\bPA[_\s]?(helmet|torso|arm|leg|jetpack)|"
                           r"jetpack", re.I)
_RX_UNDERARM  = re.compile(r"underarmou?r", re.I)
_RX_BACKPACK  = re.compile(r"backpack", re.I)
_RX_WEAPON    = re.compile(r"\bweapon|\bgun\b|rifle|pistol|shotgun|revolver|launcher|"
                           r"\bbat\b|\baxe\b|sword|knife|blade|spear|sledge|bow\b|"
                           r"grenade|mine\b|melee|shovel|tambo|gauntlet|minigun|"
                           r"harpoon|flamer|cryolator|railway|gatling|musket", re.I)
_RX_CAMP      = re.compile(r"workshop|walldecor|floordecor|wall[_\s]*decor|floor[_\s]*decor|"
                           r"structure|furniture|displaycase|stashbox|collector|utility|"
                           r"machinery|shelter|light\b|radio|statue|poster|plushie|"
                           r"gravestone|balloon|sign\b|banner|rug\b|planter", re.I)
_RX_FOOD      = re.compile(r"\bfood\b|drink|chem\b|brew|cook|meal|soup|stew|recipe_rsvp|"
                           r"\bpie\b|cake|juice|tea\b|coffee", re.I)


def page_folder(item):
    """The server folder this row's art belongs in — by what it IS."""
    cnam = item.get("cnam") or {}
    sig  = (cnam.get("sig") or "").upper()
    kind = item.get("type") or ""
    # Underscores are word characters, so \bcamera\b does NOT match
    # mod_Camera_Snapmatic_Lens_105mm — which is how four camera lenses ended up
    # in the weapons folder. Every rule below reads this spaced-out copy.
    blob = " ".join([item.get("name") or "",
                     cnam.get("edid") or "",
                     (item.get("plan_item") or {}).get("edid") or ""])
    blob = blob.replace("_", " ")

    if _RX_UNDERARM.search(blob):
        return "underarmour"
    if _RX_FISHING.search(blob):
        return "fishing-rod"
    if _RX_CAMERA.search(blob):
        return "camera"
    if _RX_PHOTOMODE.search(blob):
        return "photomode"
    if _RX_SNOWGLOBE.search(blob):
        return "snowglobe"
    # A PA paint or piece, not the CAMP station you dock in.
    if _RX_POWERARM.search(blob) and (sig in ("OMOD", "ARMO") or kind == "armour"):
        return "power-armour"
    if _RX_BACKPACK.search(blob):
        return "backpack"
    if sig == "WEAP" or generic_kind(item) == "weapon-mod" or kind == "weapon":
        return "weapons"
    if _RX_IS_APPAREL.search(blob) and not _RX_NOT_APPAREL.search(blob):
        return "apparel"
    if kind in ("apparel", "armour") or sig == "ARMO":
        return "body-armour" if kind == "armour" else "apparel"
    if sig in _CAMP_SIGS or _RX_CAMP.search(blob):
        return "workshop"
    if sig == "ALCH" or _RX_FOOD.search(blob):
        return "recipes"
    return PAGE_FOLDER.get(kind, "")


# ── which published sets to search, best first ──────────────────────────────
# Order is the answer to "if two pages both have a picture of this, whose do we
# use". Scoreboard art wins because a scoreboard reward's own tile is the one
# players saw in game; the CAMP pages next because their art is the transparent
# single-item tile this row wants; Atom Shop last of the picture sets because
# its storefront shots are the most likely to be a composite.
SOURCES = [
    ("season",            None),                     # tsv/season_rewards.tsv
    ("weather-stations",  "weather_stations.json"),
    ("buff-stations",     "buff-stations.json"),
    ("titles-camp",       "titles_camp.json"),
    ("titles-player",     "titles_player.json"),
    ("collectrons",       "collectrons.json"),
    ("resource-producers","resource_producers.json"),
    ("fridges",           "fridges.json"),
    ("cryos",             "cryos.json"),
    ("repair-bots",       "repair-bots.json"),
    ("pets",              "pets.json"),
    ("pet-apparel",       "pet-apparel.json"),
    ("pet-furniture",     "pet-furniture.json"),
    ("allies",            "allies.json"),
    ("camp",              "camp.json"),
    ("fishing-equipment", "fishing_equipment.json"),
    ("bundles",           "bundles.json"),
    ("atom-shop",         "atom_shop.json"),
]

_RX_PLAN_PREFIX = re.compile(r"^\s*(plan|recipe|schematic)\s*:\s*", re.I)
_RX_NONWORD     = re.compile(r"[^a-z0-9]+")
# ENTM records name themselves with an _ENTM_ infix the created object does not
# carry (SCORE_S24_ENTM_CAMP_Utility_WeatherStation vs the FURN it builds), so
# it is dropped before two EditorIDs are compared.
_RX_ENTM        = re.compile(r"entm", re.I)


def norm_name(s):
    """A plan name and a storefront name, reduced to the same key."""
    s = _RX_PLAN_PREFIX.sub("", str(s or ""))
    return _RX_NONWORD.sub("", s.lower())


def norm_edid(s):
    return _RX_NONWORD.sub("", _RX_ENTM.sub("", str(s or "")).lower())


class ImageIndex:
    """Every image the site already hosts, keyed by FormID, EditorID and name.

    First writer wins per key, and SOURCES is walked in priority order, so a
    later page cannot displace a better page's art.
    """

    def __init__(self):
        self.by_fid = {}
        self.by_edid = {}
        self.by_name = {}
        self.counts = {}

    def _put(self, table, key, url, source):
        if key and url and key not in table:
            table[key] = (url, source)
            self.counts[source] = self.counts.get(source, 0) + 1

    def add(self, source, fids=(), edids=(), names=(), url=""):
        url = (url or "").strip()
        # Only absolute wp-content paths are reusable as-is. A bare stem means
        # the other page resolves it against its own base, which this page does
        # not know, so it is not usable here.
        if not url.startswith("/"):
            return
        for fid in fids:
            self._put(self.by_fid, str(fid or "").strip().upper(), url, source)
        for edid in edids:
            self._put(self.by_edid, norm_edid(edid or ""), url, source)
        for name in names:
            self._put(self.by_name, norm_name(name or ""), url, source)

    def lookup(self, item):
        """Best already-published image for one plan row, or (None, None).

        FormID first — it is the only key that cannot be two different things —
        then EditorID, then the display name. The created object is asked about
        before the plan itself: the picture wanted here is of the thing you
        build, not of the recipe paper.
        """
        cnam = item.get("cnam") or {}
        cobj = item.get("cobj") or {}
        plan = item.get("plan_item") or {}

        for fid in ((cnam.get("formid") or ""), (plan.get("formid") or "")):
            hit = self.by_fid.get(fid.strip().upper())
            if hit:
                return hit
        for edid in ((cnam.get("edid") or ""), (cobj.get("edid") or ""),
                     (plan.get("edid") or "")):
            hit = self.by_edid.get(norm_edid(edid))
            if hit:
                return hit
        key = norm_name(item.get("name") or "")
        # Two-character names are not identity, they are a collision waiting to
        # happen.
        if len(key) >= 4:
            hit = self.by_name.get(key)
            if hit:
                return hit
        return None, None


def _walk(node, out):
    """Every dict in a dist JSON that carries an image — shapes differ per page."""
    if isinstance(node, dict):
        if node.get("imageUrl") or node.get("images"):
            out.append(node)
        for v in node.values():
            _walk(v, out)
    elif isinstance(node, list):
        for v in node:
            _walk(v, out)


def load(dist_dir="dist", tsv_dir="tsv", config_path=CONFIG, verbose=True):
    """Index the published art, and read the staged-stem list.

    Returns (ImageIndex, staged) where staged maps folder -> set of stems.
    """
    idx = ImageIndex()

    # Scoreboard art first — season_rewards.tsv is the curated master, and its
    # imageUrl is already the URL the scoreboard pages serve.
    # season_rewards.tsv is a curated live file — the PTS channel points
    # --data-dir at tsv/pts, which does not carry it, so fall back to the live
    # root rather than silently dropping every scoreboard image on that build.
    season = os.path.join(tsv_dir, "season_rewards.tsv")
    if not os.path.exists(season):
        season = os.path.join("tsv", "season_rewards.tsv")
    if os.path.exists(season):
        with open(season, encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                # NEVER use the TSV value raw. The TSV holds the flat authoring
                # form (/season_images/score_s3_*.webp); the file is served from
                # /season_images/season-3/score_s3_*.avif. asset_paths.asset_url
                # is the one routing rule the scoreboard pages use, so the plan
                # row lands on exactly the URL the scoreboard row does.
                idx.add("season",
                        edids=[r.get("storefrontEntitlement") or ""],
                        names=[r.get("name") or ""],
                        url=_route(r.get("imageUrl") or ""))
    elif verbose:
        print(f"  WARNING: no {season} — scoreboard art will not be reused",
              file=sys.stderr)

    for source, fname in SOURCES:
        if not fname:
            continue
        path = os.path.join(dist_dir, fname)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                blob = json.load(f)
        except (ValueError, OSError) as exc:
            if verbose:
                print(f"  WARNING: {path}: {exc}", file=sys.stderr)
            continue
        rows = []
        _walk(blob, rows)
        for row in rows:
            url = row.get("imageUrl")
            if not url and isinstance(row.get("images"), list) and row["images"]:
                first = row["images"][0]
                url = first if isinstance(first, str) and first.startswith("/") else ""
            # Every id and every name the page carries. The CAMP pages are the
            # reason for the long list: a weather station knows its FURN, its
            # ENTM, its container and its resource record, and the one that
            # matches a plan's created object is not the same one every time.
            # `planName` is the strongest key of all — it IS the plan row's
            # name, written by the page that already found the picture.
            idx.add(source,
                    fids=[row.get("formId"), row.get("formid"),
                          row.get("entmFormId"), row.get("resoFormId"),
                          row.get("contFormId")],
                    edids=[row.get("edid")],
                    names=[row.get("name"), row.get("displayName"),
                           row.get("shortName"), row.get("planName")],
                    url=url or "")

    staged = read_staged(config_path, verbose=verbose)
    if verbose:
        print("  published art indexed: {} FormIDs, {} EditorIDs, {} names"
              .format(len(idx.by_fid), len(idx.by_edid), len(idx.by_name)),
              file=sys.stderr)
    return idx, staged


# ── staged stems ────────────────────────────────────────────────────────────

def read_staged(config_path=CONFIG, verbose=True):
    """folder -> {stem, …} from the committed config."""
    out = {f: set() for f in FOLDERS}
    if not os.path.exists(config_path):
        if verbose:
            print(f"  WARNING: {config_path} missing — no staged art will be "
                  f"emitted; run with --avif-dir to create it", file=sys.stderr)
        return out
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    for folder, stems in (cfg.get("staged_images") or {}).items():
        out.setdefault(folder, set()).update(s.lower() for s in stems)
    return out


def scan_staging(avif_root, config_path=CONFIG, verbose=True):
    """Scan the local staging root and persist the stem list into the config.

    `avif_root` is the folder that mirrors the server — one subfolder per page,
    same names. Run this whenever art is added or removed, then commit the
    config so CI builds the same rows this machine does.
    """
    staged = {}
    for folder in FOLDERS:
        path = os.path.join(avif_root, folder)
        stems = set()
        if os.path.isdir(path):
            for dirpath, _dirs, files in os.walk(path):
                for fn in files:
                    if fn.lower().endswith(".avif"):
                        stems.add(os.path.splitext(fn)[0].lower())
        elif verbose:
            print(f"  WARNING: {path} is not a directory", file=sys.stderr)
        staged[folder] = stems
        if verbose:
            print(f"  {folder:16s} {len(stems):4d} staged .avif", file=sys.stderr)

    cfg = {}
    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
    cfg["staged_images"] = {k: sorted(v) for k, v in staged.items()}
    cfg.setdefault("overrides", {})
    os.makedirs(os.path.dirname(config_path) or ".", exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    if verbose:
        print(f"  wrote {config_path}", file=sys.stderr)
    return staged


# ── generic art: one picture for a whole class of plan ──────────────────────
# A weapon mod has no item of its own to photograph — the game shows you a
# receiver, a barrel, a sight, and there are 394 of them. Rather than leave
# every one of those rows with a dashed placeholder forever, they all use one
# picture, staged once in the weapons folder. Duchess's call, and it holds
# everywhere the site shows a weapon mod, not just on the weapon page: a weapon
# mod misfiled into the recipe bucket gets it too, because the rule is about
# what the thing IS.
#
# This is a LAST resort. Real art for that specific mod — published elsewhere on
# the site, or staged under its own name — always wins.
PLAN_IMG_BASE = "/wp-content/uploads/guide-images/plan-checklist/"

GENERIC_ART = {
    "weapon-mod": PLAN_IMG_BASE + "weapons/weapon_mod.avif",
    # Armour and power-armour mods have the same problem and no picture yet;
    # drop an armour_mod.avif in the body-armour folder and add it here.
}

# Armour, power-armour, backpack and underarmour mods are all OMODs too, so they
# are named out rather than left to fall into the weapon bucket.
#
# The pattern is deliberately anchored on mod_<slot> and on the paint/jetpack
# words, NOT on the bare word "armor": several real weapon mods are called
# ArmorPiercing or ArmorPen, and a looser rule silently stripped the picture off
# every one of them.
_RX_NOT_WEAPON_MOD = re.compile(
    r"mod[_\s]*(armor|armour|powerarmor|power_armor|backpack|underarmor|underarmour)"
    r"|armou?rpaint|armou?rskin|jetpack|_armor_|_armour_", re.I)


# A weapon mod whose created record never resolved (the S26 Cryolator muzzles,
# for one) has no OMOD signature to go on, so the name is read instead. These
# are the slots a weapon mod is named after; "Mod"/"Mods" on the end of the name
# counts too.
_RX_WEAPON_MOD_NAME = re.compile(
    r"\b(muzzle|receiver|barrel|magazine|sights?|scope|stock|grip|bolt|"
    r"suppressor|silencer|capacitor|core|nozzle|lobber)\b|\bmods?$", re.I)


def generic_kind(item):
    """The class of plan this row belongs to, for GENERIC_ART, or ""."""
    cnam = item.get("cnam") or {}
    name = item.get("name") or ""
    edid = (cnam.get("edid") or "") + " " + name
    if _RX_NOT_WEAPON_MOD.search(edid):
        return ""
    if (cnam.get("sig") or "").upper() == "OMOD":
        return "weapon-mod"
    # No created record. Only a weapon or recipe row can fall through to the
    # name test — a CAMP or apparel plan called "… Core" is not a weapon mod.
    if (item.get("type") or "") in ("weapon", "recipe") and _RX_WEAPON_MOD_NAME.search(name):
        return "weapon-mod"
    return ""


# ── candidate stems for the hand-staged folders ─────────────────────────────

def candidate_stems(item):
    """Filenames this plan's art could reasonably be saved under.

    Deliberately several: the pages that already have art name their files
    after the ENTM texture, while a file dropped in by hand is usually named
    after the record it is a picture of. Whichever spelling is staged wins, so
    nobody has to remember one rule.
    """
    cnam = item.get("cnam") or {}
    plan = item.get("plan_item") or {}
    out = []
    for edid in ((cnam.get("edid") or ""), (plan.get("edid") or "")):
        e = edid.strip().lower()
        if not e:
            continue
        out += [e + "_l", e]          # _l is the transparent tile convention
    name = _RX_PLAN_PREFIX.sub("", item.get("name") or "").strip().lower()
    if name:
        out.append(re.sub(r"[^a-z0-9]+", "_", name).strip("_"))
    seen, uniq = set(), []
    for s in out:
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def attach(items, idx, staged, folder_override="", stats=None):
    """Set `image_dir` and `images` on every row. Returns the stats dict.

    `images` is what the renderer draws, in order. An entry starting with "/"
    is an absolute URL used as-is (art another page already hosts); anything
    else is a stem inside this page's own folder.
    """
    stats = stats if stats is not None else {}
    for item in items:
        folder = folder_override or page_folder(item)
        item["image_dir"] = folder or (item.get("type") or "")

        url, source = idx.lookup(item)
        if url:
            item["images"] = [url]
            item["image_source"] = source
            _bump(stats, folder, source)
            continue

        pool = staged.get(folder) or set()
        hit = next((s for s in candidate_stems(item) if s in pool), "")
        if hit:
            item["images"] = [hit]
            item["image_source"] = "staged"
            _bump(stats, folder, "staged")
            continue

        # Last resort: the one picture that stands for this whole class of plan.
        # Only on the page it was drawn for — a bobber or a backpack mod is an
        # OMOD too, and a picture of a weapon receiver on the fishing page is
        # worse than the honest empty slot.
        kind = generic_kind(item) if folder == "weapons" else ""
        generic = GENERIC_ART.get(kind, "")
        item["images"] = [generic] if generic else []
        item["image_source"] = kind if generic else ""
        _bump(stats, folder, kind if generic else "none")
    return stats


def _bump(stats, folder, source):
    row = stats.setdefault(folder or "(unmapped)", {})
    row[source] = row.get(source, 0) + 1


def report(stats, stream=sys.stderr):
    print("  image coverage by page folder:", file=stream)
    for folder in sorted(stats):
        row = stats[folder]
        total = sum(row.values())
        none = row.get("none", 0)
        got = total - none
        bits = ", ".join(f"{k} {v}" for k, v in sorted(row.items()) if k != "none")
        print(f"    {folder:16s} {got:5d}/{total:<5d} with art"
              f"{('   [' + bits + ']') if bits else ''}"
              f"{('   missing ' + str(none)) if none else ''}", file=stream)
