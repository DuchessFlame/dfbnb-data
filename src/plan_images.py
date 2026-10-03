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
try:
    import legacy_nw                               # Legacy Nuclear Winter page
except ImportError:                                # pragma: no cover
    legacy_nw = None
try:
    import reusable_images                         # season-upload manifests (titles' rule)
except ImportError:                                # pragma: no cover
    reusable_images = None


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
    # Legacy Nuclear Winter plans. Claimed by WHAT THE PLAN WAS (a Nuclear
    # Winter reward, see legacy_nw.py), not by what it builds, so it is tested
    # before every other rule in page_folder().
    "legacy-nuclear-winter-plans": "legacy-nuclear-winter",
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
# "jetpack" used to be an alternative of its own here, which swept in three
# BODY armour mods whose EditorID never says power armour at all — the Secret
# Service, Brotherhood Recon and Civil Engineer jet packs
# (mod_armor_SecretService_Torso_Jetpack). Those are torso mods for a body
# armour set; filing them under power armour put them on the wrong page AND
# left them with no family to group under, because no PA set owns them. A real
# power-armour jetpack still matches: its EditorID says PowerArmor.
_RX_POWERARM  = re.compile(r"power[_\s]*armou?r|\bPA[_\s]?(helmet|torso|arm|leg|jetpack)", re.I)
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

    # A Legacy Nuclear Winter plan belongs to its own page whatever it builds —
    # a paint, a hat or a stash box. legacy_nw.tag() sets the flag in attach().
    if item.get("legacy_nw"):
        return PAGE_FOLDER["legacy-nuclear-winter-plans"]
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
    ("player-icons",      "player_icons.json"),
    ("bundles",           "bundles.json"),
    ("atom-shop",         "atom_shop.json"),
]

# Sources whose display names are not identity (see load()).
NAME_UNSAFE = {"player-icons"}

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
                    # Player icons are named after things ("Minigun", "Vault
                    # Boy"), so a name match would put an icon on the plan for
                    # the real item. They match by FormID / EditorID only.
                    names=[] if source in NAME_UNSAFE else
                          [row.get("name"), row.get("displayName"),
                           row.get("shortName"), row.get("planName")],
                    url=url or "")

    # The season-upload manifests, filtered by the upload checker — the same
    # index the Titles checklists ask first (reusable_images.py). Searched after
    # the published-page sets above and before any page's own folder.
    idx.reuse = None
    if reusable_images is not None:
        try:
            idx.reuse = reusable_images.build_index(dist_dir)
        except Exception as exc:                   # noqa: BLE001 - never fatal
            if verbose:
                print(f"  WARNING: reusable_images: {exc}", file=sys.stderr)

    # Legacy Nuclear Winter tagging rides on the index so every caller of
    # load()/attach() — full build, add_plan_images, reenrich, underarmour, new
    # plans — tags the rows in the same pass that decides their folder.
    idx.legacy = None
    if legacy_nw is not None:
        try:
            idx.legacy = legacy_nw.load(tsv_dir, verbose=verbose)
        except Exception as exc:                   # noqa: BLE001 - never fatal
            if verbose:
                print(f"  WARNING: legacy_nw: {exc}", file=sys.stderr)

    staged = read_staged(config_path, verbose=verbose)
    if verbose:
        print("  published art indexed: {} FormIDs, {} EditorIDs, {} names"
              .format(len(idx.by_fid), len(idx.by_edid), len(idx.by_name)),
              file=sys.stderr)
    return idx, staged


# ── THE HOSTED-FIRST RULE ───────────────────────────────────────────────────
# Every plan checklist page — plan_master pages, and the pages with their own
# dataset (pennants, camera mods, displays, Legacy Nuclear Winter) — asks this
# BEFORE it looks in its own /guide-images/plan-checklist/<folder>/. Same rule
# the Titles checklists follow: if the site already serves a picture of this
# item anywhere under /wp-content/uploads/, use that URL; only fall back to the
# page's own folder when nothing is hosted. One upload serves every page, and
# nobody pays to store the same picture twice.
#
# Order, first hit wins:
#   1. the published page sets (ImageIndex: scoreboard, CAMP pages, titles,
#      Atom Shop, bundles …) by FormID, EditorID, then display name
#   2. the season-upload manifests (reusable_images: verified-hosted only)
#      by entitlement EditorID, then texture name
#   3. a stem staged in ANOTHER plan-checklist folder (already uploaded there)
# and only then the page's own folder.
def hosted_url(idx, fids=(), edids=(), names=(), textures=()):
    """(url, source) for art the site already hosts, or ("", "")."""
    for fid in fids:
        hit = idx.by_fid.get(str(fid or "").strip().upper())
        if hit:
            return hit
    for edid in edids:
        if edid:
            hit = idx.by_edid.get(norm_edid(edid))
            if hit:
                return hit
    for name in names:
        key = norm_name(name)
        if len(key) >= 4:
            hit = idx.by_name.get(key)
            if hit:
                return hit
    reuse = getattr(idx, "reuse", None)
    if reuse:
        for edid in edids:
            if edid:
                url = reuse.find(edid=edid)
                if url:
                    return url, "season-upload"
        for tex in textures:
            if tex:
                url = reuse.find(texture=tex)
                if url:
                    return url, "season-upload"
    return "", ""


def hosted_first(own, hosted):
    """A page's own image list with hosted art put in front of it.

    `own` is the page's own ordered list (bare stems or absolute URLs). The
    hosted URL replaces the own-folder copy of the SAME picture (same texture
    stem, any _l/_cN suffix aside) so the row does not show one image twice;
    the page's other images (colour variants) stay after it.
    """
    own = list(own or [])
    if not hosted:
        return own
    def key(v):
        st = os.path.splitext(os.path.basename(str(v)))[0].lower()
        return re.sub(r"_(l|c\d)$", "", st)
    main = key(hosted)
    rest = [v for v in own if not (key(v) == main and not re.search(r"_c\d$", str(v).lower()))]
    return [hosted] + rest


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


_OVERRIDE_CACHE = {}


def read_overrides(config_path=CONFIG, verbose=True):
    """The manual escape hatch: a plan -> picture map the resolver cannot guess.

    `data/plan_images.json` has carried an "overrides" key since the file was
    created and nothing ever read it. It is read now, because the automatic
    paths need a record link the export set does not always have: a content
    drop lands in BOOK weeks before its COBJ is re-exported, and until then a
    brand new plan resolves no created record at all, so there is no name to
    match its art on. An entry here is a stopgap for that window, not a
    permanent fixture — once the record link exists the automatic path finds
    the same file and the entry becomes a no-op. `--audit-overrides` lists the
    ones that have gone redundant so they can be deleted.

    A key is any of: the plan's row id ("PLAN_008E0698"), its FormID, its
    EditorID, or its full name. Matching is case-insensitive.

    A value is one of:
      "/wp-content/…/x.avif"  an absolute URL, used verbatim
      "folder/stem"           a staged stem in a NAMED folder (use this when
                              the art sits in a different page's folder than
                              the row routes to)
      "stem"                  a staged stem in the row's own folder
      ""                      suppress art for this row entirely
    """
    key = os.path.abspath(config_path)
    if key in _OVERRIDE_CACHE:
        return _OVERRIDE_CACHE[key]
    out = {}
    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
        for k, v in (cfg.get("overrides") or {}).items():
            out[str(k).strip().lower()] = (v or "").strip()
    if verbose and out:
        print(f"  {len(out):4d} image overrides read from {config_path}",
              file=sys.stderr)
    _OVERRIDE_CACHE[key] = out
    return out


def override_keys(item):
    """Every spelling read_overrides() will answer to for this row."""
    plan = item.get("plan_item") or {}
    return [str(k).strip().lower() for k in (
        item.get("id"), plan.get("formid"), plan.get("edid"), item.get("name"),
    ) if k]


def apply_override(item, value, staged, folder):
    """Turn an override value into (images, image_source), or None to skip it.

    A stem is only accepted when the .avif is actually staged, exactly like the
    automatic path — an override is allowed to be wrong about which file it
    wants, but it is never allowed to point the front end at a 404.
    """
    if value.startswith("/"):
        return [value], "override"
    if "/" in value:
        fld, _, stem = value.rpartition("/")
        stem = stem.lower()
        if stem in (staged.get(fld) or set()):
            return [PLAN_IMG_BASE + fld + "/" + stem + ".avif"], "override"
        return None
    stem = value.lower()
    if stem and stem in (staged.get(folder) or set()):
        return [stem], "override"
    return None


def scan_staging(avif_root, config_path=CONFIG, verbose=True):
    """Scan the local staging root and persist the stem list into the config.

    `avif_root` is the folder that mirrors the server — one subfolder per page,
    same names. Run this whenever art is added or removed, then commit the
    config so CI builds the same rows this machine does.
    """
    staged = {}
    for folder in FOLDERS:
        path = os.path.join(avif_root, folder)
        # The local staging copy is sometimes spelled with spaces
        # ("legacy nuclear winter") where the server folder has hyphens.
        if not os.path.isdir(path) and os.path.isdir(os.path.join(avif_root, folder.replace("-", " "))):
            path = os.path.join(avif_root, folder.replace("-", " "))
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


# A paint is not a weapon mod for this purpose. It changes nothing about how the
# gun works, and a picture of a receiver on "Plan: Western Spirit Paint" tells
# the reader something untrue about what they are looking at. Paints get no
# stand-in until there is a picture worth standing in for them.
# Paints and skins are NOT mods - they get their own picture, never the mod
# box. Bounded by "not a letter" rather than \b: EditorIDs join words with
# underscores, which \b treats as word characters, so \bpaint\b missed
# mod_AssaultronBlade_Weapon_Paint_TheGutter (The Gutter got the mod box).
_RX_COSMETIC = re.compile(r"(?<![a-z])(paints?|skins?|camo|wrap)(?![a-z])|modelswap|appearance",
                          re.I)


def generic_kind(item):
    """The class of plan this row belongs to, for GENERIC_ART, or ""."""
    cnam = item.get("cnam") or {}
    name = item.get("name") or ""
    edid = (cnam.get("edid") or "") + " " + name
    if _RX_NOT_WEAPON_MOD.search(edid) or _RX_COSMETIC.search(edid):
        return ""
    sig = (cnam.get("sig") or "").upper()
    if sig == "OMOD":
        return "weapon-mod"
    # A plan that builds something else (an ACTI, FURN, STAT ...) is never a
    # weapon mod, whatever its name says - "Plan: Radioactive Barrel" builds the
    # E05_RadioactiveBarrel ACTI and was getting the mod box off the word
    # "Barrel" (fixed Oct 2026).
    if sig:
        return ""
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
    cobj = item.get("cobj") or {}
    plan = item.get("plan_item") or {}
    out = []
    # Texture names the builder already resolved for this row (legacy_nw.py
    # writes the Nuclear Winter entitlement's ETDI here). Staging folders keep
    # the game's own texture name, so this is the likeliest spelling of all.
    for s in ((item.get("art_stems") or {}).get("main") or []):
        out.append(str(s).strip().lower())
    # Every record this plan touches, strongest link first: the thing it makes,
    # the recipe that makes it, then the plan book itself. A file dropped in by
    # hand is usually named after the record it is a PICTURE of, which is the
    # created record — so cnam has to be tried before the plan's own EditorID.
    for edid in ((cnam.get("edid") or ""), (cobj.get("edid") or ""),
                 (plan.get("edid") or "")):
        e = edid.strip().lower()
        if not e:
            continue
        out += [e + "_l", e]          # _l is the transparent tile convention
    name = _RX_PLAN_PREFIX.sub("", item.get("name") or "").strip().lower()
    if name:
        out.append(re.sub(r"[^a-z0-9]+", "_", name).strip("_"))
        # Hand-cropped Inspect renders are saved as hyphen slugs
        # ("bos-knight-uniform", "undershirt-jeans"), the same spelling the
        # reward pages' own folders use.
        out.append(re.sub(r"[^a-z0-9]+", "-", name).strip("-"))
        # Headlamp colours share one picture per helmet: "T-45 Headlamp Blue",
        # "... Purple", "... Vault Boy" all show the same T-45 helmet, so only
        # one file is kept (t-45-headlamp.avif). Tried last, so a colour that
        # ever gets its own render still wins.
        m = re.match(r"^(.*\bheadlamp)\b", name)
        if m:
            out.append(re.sub(r"[^a-z0-9]+", "-", m.group(1)).strip("-"))
        # Regional recipe variants ("Healing Salve (Ash Heap)") look the same
        # in every region, so one file serves them all (healing-salve.avif).
        base = re.sub(r"\s*\([^)]*\)\s*$", "", name)
        if base and base != name:
            out.append(re.sub(r"[^a-z0-9]+", "-", base).strip("-"))
    seen, uniq = set(), []
    for s in out:
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def _elsewhere(staged, home):
    """stem -> folder, for every stem staged in exactly ONE folder but home.

    Cross-folder rescue exists because a row's page is decided by what the
    thing IS to a player and the art is filed by whoever staged it, and the two
    disagree honestly: the Slasher power armour paints are `recipe` rows (the
    plan's record bucket) whose pictures were quite sensibly dropped in
    power-armour/, and the gold shovel paint is a `recipes` row whose picture
    is in weapons/.

    A stem present in two folders is NOT rescued. Two pages staging the same
    filename means the name does not identify a picture, and guessing which one
    a row wants is how the wrong item ends up on a row — worse than the empty
    slot this is trying to fill.
    """
    seen = {}
    for folder, stems in staged.items():
        if folder == home:
            continue
        for stem in stems:
            seen[stem] = None if stem in seen else folder
    return seen


def attach(items, idx, staged, folder_override="", stats=None, overrides=None):
    """Set `image_dir` and `images` on every row. Returns the stats dict.

    `images` is what the renderer draws, in order. An entry starting with "/"
    is an absolute URL used as-is (art another page already hosts); anything
    else is a stem inside this page's own folder.

    Resolution order, first hit wins (the hosted-first rule — see hosted_url):

      1. an override      — the manual map, always the last word
      2. published art    — a picture another page already serves, then the
                            verified season-upload manifests
      3. staged, another page's folder, when the stem is unambiguous
      4. staged, own folder
      5. the generic class picture (weapon mods only)

    """
    stats = stats if stats is not None else {}
    overrides = read_overrides() if overrides is None else overrides
    xfolder = {}
    legacy = getattr(idx, "legacy", None)
    for item in items:
        if legacy:
            legacy.tag(item)
        folder = folder_override or page_folder(item)
        item["image_dir"] = folder or (item.get("type") or "")

        # 1. Override. Checked before the index because the whole point of the
        #    map is to beat a resolver that got it wrong, not only to fill a
        #    gap where the resolver found nothing.
        ov = next((overrides[k] for k in override_keys(item) if k in overrides), None)
        if ov is not None:
            if ov == "":
                item["images"] = []
                item["image_source"] = ""
                _bump(stats, folder, "suppressed")
                continue
            got = apply_override(item, ov, staged, folder)
            if got:
                item["images"], item["image_source"] = got
                _bump(stats, folder, "override")
                continue
            print(f"  WARNING: override for {item.get('id') or item.get('name')}"
                  f" -> {ov!r} is not staged; falling through", file=sys.stderr)

        url, source = idx.lookup(item)
        if not url:
            # Same hosted-first rule, the season-upload manifests this time:
            # the entitlement a Legacy NW plan replaces and its texture names.
            ent = (item.get("nw_entitlement") or {}).get("edid") or ""
            url, source = hosted_url(
                idx, edids=[ent] if ent else [],
                textures=(item.get("art_stems") or {}).get("main") or [])
        if url:
            item["images"] = [url]
            item["image_source"] = source
            _bump(stats, folder, source)
            continue

        stems = candidate_stems(item)
        pool = staged.get(folder) or set()

        # 3. The same picture, already uploaded under ANOTHER page's folder.
        #    Asked before this page's own folder (hosted-first rule above), so a
        #    picture that is already on the server is never uploaded twice.
        #    Emitted as an absolute URL rather than a bare stem, because a bare
        #    stem is read by the front end against THIS row's folder and would
        #    404 there. dspImgURL() appends .avif to a bare stem and passes an
        #    absolute entry through verbatim, so the extension is written here.
        if folder not in xfolder:
            xfolder[folder] = _elsewhere(staged, folder)
        other = xfolder[folder]
        hit = next((s for s in stems if other.get(s)), "")
        if hit:
            item["images"] = [PLAN_IMG_BASE + other[hit] + "/" + hit + ".avif"]
            item["image_source"] = "staged-elsewhere"
            _bump(stats, folder, "staged-elsewhere")
            continue

        # 4. This page's own folder.
        hit = next((s for s in stems if s in pool), "")
        if hit:
            item["images"] = [hit] + staged_extras(item, hit, pool)
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


MAX_EXTRAS = 3


def staged_extras(item, hit, pool):
    """The colour variants staged beside a row's main picture.

    A skin or paint set ships one render (`foo_l`) plus a frame per colour
    (`foo_c1`, `foo_c2` …). The row's Item Image shows every one that is
    actually staged, main render first — never a stem that is not staged, so
    nothing 404s. The texture list legacy_nw.py resolved is tried first, then
    the `_cN` siblings of the hit itself.
    """
    base = re.sub(r"_l$", "", hit)
    want = list((item.get("art_stems") or {}).get("extra") or [])
    want += [f"{base}_c{n}" for n in range(1, 10)]
    out, seen = [], {hit}
    for s in want:
        s = str(s).strip().lower()
        if s and s in pool and s not in seen:
            seen.add(s)
            out.append(s)
    # Item Image shows 1-4 pictures (checklist style guide): main + 3 extras.
    return out[:MAX_EXTRAS]


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
