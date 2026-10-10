#!/usr/bin/env python3
r"""
image_index.py - the ONE place every builder asks "which picture does this item use?"

The site keeps one shared image library and every reward page pulls from it, so
nothing is uploaded twice. src/build_image_index.py reads the server listing
(data/server_listing.tsv, written by tools/list_server_images.py) and writes
dist/image_index.json. This module reads that file and answers for any item.

LOOKUP ORDER (same for every page, first hit wins)

  1. SHARED LIBRARY    the item's picture in the shared folders, by FormID, then
                       by EditorID:
                         guide-images/plan-checklist/<type>/   (plans, apparel, weapons ...)
                         guide-images/titles/  atom-shop/  camp-items/
                         season_images/
                       Anything Babylon / Nuclear Winter resolves to the legacy
                       Nuclear Winter folder and NEVER to an event-folder copy.
  2. OWN EVENT FOLDER  the URLs the page's builder proposed for this item (its
                       own guide-images/<category>/<event>/ folder), kept only
                       if the file is really on the server, plus a file named
                       after the item's FormID in that folder.
  3. ANOTHER PAGE      the same item already uploaded to some other event folder
                       (reused, never uploaded again).
  4. PLACEHOLDER       titles  -> the blank "HELLO MY ... IS" name tag for that
                                  title's affix
                       weapon mod plans -> the yellow mod box
                       anything else -> nothing (the renderer draws its own
                                  dashed placeholder)

Skins and paints are not mods and never get the mod box: the weapon-mod set in
the index comes from plan_images.generic_kind(), which already excludes them.

Every URL handed back is on the server according to the listing, so a page can
never point at a 404. When no listing has been committed yet the module stays
out of the way: builders keep exactly the URLs they proposed.

USE FROM A BUILDER

    import image_index
    image_index.apply(page_obj, event_dirs=["/wp-content/uploads/guide-images/seasonal-events/treasure-hunters/"])

apply() walks any JSON structure and fixes every item dict it finds (one with a
formid/formId and an imageUrl/images field). See apply() for the details.
"""

from __future__ import annotations

import json
import os
import re
import sys
from urllib.parse import quote, unquote

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_PATH = os.path.join(REPO_ROOT, "dist", "image_index.json")
LISTING_PATH = os.path.join(REPO_ROOT, "data", "server_listing.tsv")

UPLOADS = "/wp-content/uploads/"
PLAN_IMG_BASE = UPLOADS + "guide-images/plan-checklist/"
IMAGE_EXT = (".avif", ".webp", ".png", ".jpg", ".jpeg", ".gif")

# ── folder classes ─────────────────────────────────────────────────────────
NW_PREFIX = "guide-images/plan-checklist/legacy-nuclear-winter/"
LIBRARY_PREFIXES = (
    "guide-images/plan-checklist/",
    "guide-images/titles/",
    "guide-images/atom-shop/",
    "guide-images/camp-items/",
    "season_images/",
)
# The site's reward-page categories. guide-images/<one of these>/<event>/ is an
# event folder.
EVENT_CATEGORIES = {
    "activities", "public-events", "seasonal-events", "daily-ops", "mutated-events",
    "infestations", "expos", "raids", "bounty-hunting", "treasure-maps",
    "score-challenges",
}
# Sub-folders of an event folder that hold the event's OWN art - guide figures,
# maps, gallery shots, checklists, the cover. Never treated as a reward picture,
# never offered to another page, never listed as a duplicate to delete.
_EVENT_OWN_ART = re.compile(
    r"(^|/)(gallery|maps?|guide[-_ ]?images|guide[-_ ]?photos|reward[-_ ]?checklists|"
    r"locations|covers?)(/|$)|(^|/)cover\.[a-z0-9]+$", re.I)

# Pictures that stand in for missing art. A builder may still propose one (the
# mod box, a blank name tag, the old Invaders title tag); it is never treated as
# the item's own picture, so real art found anywhere always beats it, and the
# right placeholder for the item is chosen at the end instead.
PLACEHOLDER_ALIASES = (
    "guide-images/seasonal-events/invaders-from-beyond/reward-images/player-title-invader.avif",
)
_BLANK_TAG = re.compile(r"(^|/)guide-images/titles/.*blank\.[a-z]+$", re.I)

FID_RE = re.compile(r"^[0-9a-f]{8}$", re.I)

# A title reward is often the title's RECIPE record, not the title:
# PlayerTitle_Recipe_Suffix_Surveyor unlocks PlayerTitles_Suffix_Surveyor.
_TITLE_EDID = re.compile(r"(player|camp)titles?_(?:recipe_)?(prefix|suffix)_", re.I)


def title_edids(edid):
    """The title record EditorIDs a title-recipe EditorID stands for."""
    e = str(edid or "")
    if not _TITLE_EDID.search(e):
        return []
    out = []
    for cand in (re.sub(r"(Title)s?_Recipe_", r"\1s_", e, flags=re.I),
                 re.sub(r"_Recipe_", "_", e, flags=re.I)):
        if cand != e and cand not in out:
            out.append(cand)
    return out
# <FormID>.avif, <FormID>_go.avif (folded outfit render), <FormID>_c1.avif ...
# (carousel views).
FID_FILE_RE = re.compile(r"^(?P<fid>[0-9a-f]{8})(?P<suffix>_go|_c\d+)?$", re.I)


FID_KEYS = ("formid", "formId", "formID", "form_id", "id")


def item_fid(d):
    """The item's FormID from whichever key its page uses, or ""."""
    for k in FID_KEYS:
        v = d.get(k)
        if isinstance(v, str) and FID_RE.match(v.strip()):
            return v.strip().upper()
    return ""


def norm_rel(path):
    """Any spelling of an uploads path -> 'guide-images/x/y.avif' (unquoted).

    Accepts full URLs, '/wp-content/uploads/...', Windows backslashes and paths
    that are already relative. Returns "" for anything outside uploads.
    """
    s = unquote(str(path or "").strip()).replace("\\", "/")
    if not s:
        return ""
    i = s.lower().find("wp-content/uploads/")
    if i >= 0:
        s = s[i + len("wp-content/uploads/"):]
    elif s.startswith("/") or "://" in s:
        return ""
    return s.lstrip("/")


def to_url(rel):
    """Relative uploads path -> site URL. Spaces are encoded, nothing else is."""
    return UPLOADS + quote(rel, safe="/-_.~()'!,&+@=")


def tier_of(rel):
    low = rel.lower()
    if low.startswith(NW_PREFIX):
        return "nw"
    if low.startswith(LIBRARY_PREFIXES):
        return "library"
    parts = low.split("/")
    if len(parts) >= 3 and parts[0] == "guide-images" and parts[1] in EVENT_CATEGORIES:
        return "event"
    return "other"


def is_event_own_art(rel):
    """True for gallery / map / guide / cover files inside an event folder."""
    parts = rel.split("/")
    return bool(_EVENT_OWN_ART.search("/".join(parts[3:]) if len(parts) > 3 else parts[-1]))


def is_image(rel):
    return rel.lower().endswith(IMAGE_EXT)


# ── the server listing ─────────────────────────────────────────────────────

def read_listing(path=LISTING_PATH):
    """[(rel, bytes)] for every image file in the listing, or None if absent.

    Reads the TSV tools/list_server_images.py writes, and is forgiving about
    anything else pasted in: one path or URL per line works too.
    """
    if not os.path.exists(path):
        return None
    out = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line or line.startswith("#") or line.lower().startswith("path\t"):
                continue
            cols = line.split("\t")
            rel = norm_rel(cols[0])
            if not rel or not is_image(rel):
                continue
            try:
                size = int(cols[1]) if len(cols) > 1 and cols[1] else 0
            except ValueError:
                size = 0
            out.append((rel, size))
    return out


# ── the index ──────────────────────────────────────────────────────────────

class ImageIndex:
    """dist/image_index.json, loaded once."""

    def __init__(self, data=None):
        data = data or {}
        self.data = data
        self.by_fid = data.get("byFormId") or {}
        self.by_edid = data.get("byEdid") or {}
        self.weapon_mod = set(x.upper() for x in data.get("weaponModFormIds") or [])
        self.titles = data.get("titles") or {}
        self.placeholders = data.get("placeholders") or {}
        files = data.get("files")
        self.have_listing = files is not None
        self.files = set()
        self.files_ci = {}
        for folder, names in (files or {}).items():
            for n in names:
                rel = folder + n
                self.files.add(rel)
                self.files_ci.setdefault(rel.lower(), rel)

    # existence ------------------------------------------------------------
    def find(self, url):
        """The listing's spelling of url ('rel'), or "" if it is not on the
        server. With no listing loaded, every uploads URL counts as present."""
        rel = norm_rel(url)
        if not rel:
            return ""
        if not self.have_listing:
            return rel
        if rel in self.files:
            return rel
        # .webp in older data where the server copy is .avif (AVIF is the only
        # copy - see asset_paths), and case drift from Windows-made names.
        for cand in (rel, re.sub(r"\.webp$", ".avif", rel, flags=re.I)):
            hit = self.files_ci.get(cand.lower())
            if hit:
                return hit
        return ""

    def exists(self, url):
        return bool(self.find(url))

    def present(self, url):
        """url itself when it is on the server (kept verbatim so builders'
        spelling does not churn), the corrected URL when only the case or
        extension differs, else ""."""
        rel = self.find(url)
        if not rel:
            return ""
        if norm_rel(url) == rel:
            return url if str(url).startswith("/") else to_url(rel)
        return to_url(rel)

    # library ----------------------------------------------------------------
    def entry(self, fid="", edid=""):
        fid = str(fid or "").strip().upper()
        e = self.by_fid.get(fid) if fid else None
        if not e and edid:
            for cand in [edid] + title_edids(edid):
                e = self.by_edid.get(str(cand).strip().lower())
                if e:
                    break
        return e

    def fid_files(self, folder_url, fid):
        """[main, extras...] for a file named after the FormID inside one folder."""
        if not self.have_listing or not fid:
            return []
        folder = norm_rel(folder_url).rstrip("/") + "/"
        fid = fid.upper()
        main, extras = "", []
        for ext in (".avif", ".webp", ".png", ".jpg"):
            for stem in (fid, fid.lower()):
                for suffix in ("_go", ""):
                    rel = self.files_ci.get((folder + stem + suffix + ext).lower())
                    if rel and not main:
                        main = rel
        if not main:
            return []
        # Carousel views: <FormID>_c1, _c2 ... (the mannequin view of an outfit
        # goes here, never as the main picture).
        for n in range(1, 10):
            rel = self.files_ci.get("{}{}_c{}.avif".format(folder, fid, n).lower())
            if rel:
                extras.append(rel)
        return [to_url(main)] + [to_url(r) for r in extras]

    # placeholders -------------------------------------------------------------
    def is_placeholder(self, url):
        rel = norm_rel(url).lower()
        if not rel:
            return False
        mod_box = norm_rel(self.placeholders.get("weaponMod") or "").lower()
        return (rel == mod_box or rel in PLACEHOLDER_ALIASES or bool(_BLANK_TAG.search(rel))
                or rel == "guide-images/plan-checklist/weapons/weapon_mod.avif")

    def title_info(self, fid="", edid="", name=""):
        t = self.titles.get(str(fid or "").upper())
        for cand in ([edid] + title_edids(edid)) if edid else []:
            t = t or self.titles.get(str(cand).lower())
        if t:
            return t
        m = _TITLE_EDID.search(edid or "")
        if m:
            return {"type": "camp" if m.group(1).lower().startswith("camp") else "player",
                    "affix": m.group(2).lower()}
        m = re.match(r"^\s*(player|camp|c\.a\.m\.p\.)\s+title\b", name or "", re.I)
        if m:
            return {"type": "player" if m.group(1).lower() == "player" else "camp", "affix": "both"}
        return None

    def placeholder(self, fid="", edid="", name=""):
        t = self.title_info(fid, edid, name)
        if t:
            ph = self.placeholders.get("title") or {}
            url = ((ph.get(t.get("type") or "player") or {}).get(t.get("affix") or "both")
                   or (ph.get("player") or {}).get(t.get("affix") or "both")
                   or (ph.get("player") or {}).get("both"))
            if url:
                return url, "title-blank"
        if str(fid or "").upper() in self.weapon_mod and self.placeholders.get("weaponMod"):
            return self.placeholders["weaponMod"], "weapon-mod"
        return "", ""

    # the lookup -----------------------------------------------------------------
    def resolve(self, fid="", edid="", name="", current=(), event_dirs=(), max_images=4):
        """(urls, source) for one item - see the module docstring for the order.

        current     the URLs the builder proposed, in its own order (event-folder
                    slugs, curated overrides, gallery extras, hosted art it found
                    by name ...). Each is kept only if it is on the server.
        event_dirs  this page's own reward-image folder(s), for FormID-named files.
        """
        fid = str(fid or "").strip().upper()
        cur = []
        for u in current or ():
            p = self.present(u) if u else ""
            if p and p not in cur:
                cur.append(p)
        e = self.entry(fid, edid)

        def done(urls, source):
            seen, out = set(), []
            for u in urls:
                k = norm_rel(u).lower()
                if u and k not in seen:
                    seen.add(k)
                    out.append(u)
            return out[:max_images], source

        if e and e.get("tier") == "nw":
            # Nuclear Winter: the legacy folder only, never an event copy.
            keep = [u for u in cur if tier_of(norm_rel(u)) in ("nw", "library")]
            return done([e["url"]] + (e.get("extras") or []) + keep, "nw")

        if e and e.get("tier") == "library":
            # Library first. The builder's own primary is dropped when it is an
            # event-folder copy (the same picture twice); its extra views (the
            # mannequin carousel, front/back) are kept after the library art.
            rest = list(cur)
            if rest and tier_of(norm_rel(rest[0])) == "event":
                rest = rest[1:]
            return done([e["url"]] + (e.get("extras") or []) + rest, "library")

        own = []
        for d in event_dirs or ():
            own += self.fid_files(d, fid)
        if own or cur:
            return done(own + cur, "event")

        if e:                                   # the item, on another page
            return done([e["url"]] + (e.get("extras") or []), "reused")

        ph, src = self.placeholder(fid, edid, name)
        if ph:
            return [ph], src
        return [], "missing"

    # walking a page ---------------------------------------------------------------
    def apply(self, obj, event_dirs=(), page="", stats=None, misses=None,
              fill_missing=False, skip=None):
        """Fix every item dict inside obj in place. Returns stats.

        An item dict has a FormID (formid / formId / formID) and a name, and
        carries imageUrl and/or images. Its current URLs are fed to resolve() as
        the builder's proposal; the answer replaces them. Shapes handled:

          images: [absolute URLs]                    seasonal / mutated pages
          images: [stems] + image_dir                plan_master rows (Daily Ops)
          imageUrl: url                              flat rewards, titles

        fill_missing=True also gives an imageUrl to item dicts that have no
        image field at all (activities, public events, treasure maps).
        skip(d) -> True leaves a dict alone.

        misses (a list) collects {page, name, formid, edid} for every item left
        with no picture at all - the missing-images report.
        """
        stats = stats if stats is not None else {}
        seen = set()
        self._walk(obj, event_dirs, page, stats, misses, fill_missing, skip, seen)
        return stats

    def _walk(self, node, event_dirs, page, stats, misses, fill, skip, seen):
        if isinstance(node, list):
            for v in node:
                self._walk(v, event_dirs, page, stats, misses, fill, skip, seen)
            return
        if not isinstance(node, dict):
            return
        if id(node) in seen:                    # byPage stores one page under 3 keys
            return
        seen.add(id(node))
        fid = item_fid(node)
        has_img = "imageUrl" in node or "images" in node
        if fid and node.get("name") and (has_img or fill) and not (skip and skip(node)):
            self._fix(node, fid, event_dirs, page, stats, misses)
        for k, v in node.items():
            if isinstance(v, (dict, list)) and k not in ("images", "imageUrl"):
                self._walk(v, event_dirs, page, stats, misses, fill, skip, seen)

    def _fix(self, d, fid, event_dirs, page, stats, misses):
        current = []
        imgs = d.get("images")
        if isinstance(imgs, list):
            for s in imgs:
                s = str(s or "")
                if not s:
                    continue
                if s.startswith("/"):
                    current.append(s)
                elif d.get("image_dir"):
                    current.append(PLAN_IMG_BASE + d["image_dir"] + "/" + s + ".avif")
        # imageUrl is normally images[0]; resolve() drops the repeat.
        if d.get("imageUrl"):
            current.append(d["imageUrl"])
        # Placeholders are not item art: let resolve() decide whether this row
        # still gets one, and which.
        current = [u for u in current if not self.is_placeholder(u)]
        urls, source = self.resolve(fid, d.get("edid") or "", d.get("name") or "",
                                    current, event_dirs)
        if "images" in d:
            d["images"] = urls
        if "imageUrl" in d:
            prev = d.get("imageUrl")
            d["imageUrl"] = urls[0] if urls else (None if prev is None else "")
        elif "images" not in d and urls:
            d["imageUrl"] = urls[0]           # fill_missing: only when there is one
        stats[source] = stats.get(source, 0) + 1
        if not urls and misses is not None:
            misses.append({"page": page, "name": d.get("name") or "",
                           "formid": fid.upper(), "edid": d.get("edid") or ""})


_CACHE = {}


def load(path=INDEX_PATH, verbose=True):
    """The index, or an empty one (a no-op resolver) if it has not been built."""
    path = os.path.abspath(path)
    if path in _CACHE:
        return _CACHE[path]
    data = {}
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError) as exc:
            if verbose:
                print("  [image_index] WARNING: {} unreadable ({}) - images left as the "
                      "builder proposed".format(path, exc), file=sys.stderr)
    elif verbose:
        print("  [image_index] no {} yet - run src/build_image_index.py; images left as "
              "the builder proposed".format(os.path.relpath(path, REPO_ROOT)), file=sys.stderr)
    idx = ImageIndex(data)
    _CACHE[path] = idx
    return idx


def apply(obj, event_dirs=(), page="", stats=None, misses=None, fill_missing=False,
          skip=None, index=None):
    """Module-level shortcut: load the index and fix obj in place."""
    idx = index or load()
    return idx.apply(obj, event_dirs=event_dirs, page=page, stats=stats, misses=misses,
                     fill_missing=fill_missing, skip=skip)


def report(tag, stats, stream=sys.stdout):
    if stats:
        print("[{}] images: {}".format(tag, ", ".join(
            "{} {}".format(v, k) for k, v in sorted(stats.items()))), file=stream)


def listing_stems(folder_rel, listing_path=LISTING_PATH):
    """Lower-cased .avif stems in one server folder (recursive), from the
    listing, or None when there is no listing. plan_images uses this so a plan
    checklist only ever emits a stem that is really uploaded."""
    rows = _listing_cached(listing_path)
    if rows is None:
        return None
    folder = folder_rel.strip("/").lower() + "/"
    out = set()
    for rel, _size in rows:
        low = rel.lower()
        if low.startswith(folder) and low.endswith(".avif"):
            out.add(os.path.splitext(os.path.basename(low))[0])
    return out


def listing_subfolders(prefix, listing_path=LISTING_PATH):
    """First-level folder names under prefix in the listing, or None when there
    is no listing (plan_images uses it to cover page folders such as
    apparel-without-plans that are not in its own folder list)."""
    rows = _listing_cached(listing_path)
    if rows is None:
        return None
    pre = prefix.strip("/").lower() + "/"
    out = set()
    for rel, _size in rows:
        low = rel.lower()
        if low.startswith(pre):
            rest = rel[len(pre):]
            if "/" in rest:
                out.add(rest.split("/", 1)[0])
    return out


_LISTING = {}


def _listing_cached(path):
    path = os.path.abspath(path)
    if path not in _LISTING:
        _LISTING[path] = read_listing(path)
    return _LISTING[path]


# ══════════════════════════════════════════════════════════════════════════
# PAGE HANDLERS - one per page family. The builders call these right before
# they write their JSON, and src/apply_image_index.py calls the very same
# functions on the JSON already in dist/ (no rebuild needed). Each returns
# (stats, misses); misses is the page's to-do list for the missing report,
# limited to the rows a reader sees on that page's checklist.
# ══════════════════════════════════════════════════════════════════════════

SEASONAL_BASE = UPLOADS + "guide-images/seasonal-events/"
MUTATED_BASE = UPLOADS + "guide-images/mutated-events/reward-images/"
DAILY_OPS_BASE = UPLOADS + "guide-images/daily-ops/"
TREASURE_MAPS_BASE = UPLOADS + "guide-images/treasure-maps/"


def _folders_of(urls):
    out = []
    for u in urls:
        rel = norm_rel(u)
        if rel and tier_of(rel) == "event":
            f = UPLOADS + rel.rsplit("/", 1)[0] + "/"
            if f not in out:
                out.append(f)
    return out


def _page_urls(obj, out):
    if isinstance(obj, dict):
        for k in ("imageUrl",):
            if isinstance(obj.get(k), str):
                out.append(obj[k])
        if isinstance(obj.get("images"), list):
            out += [u for u in obj["images"] if isinstance(u, str) and u.startswith("/")]
        for v in obj.values():
            if isinstance(v, (dict, list)):
                _page_urls(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _page_urls(v, out)
    return out


def seasonal_page(page, slug, image_dir="", index=None):
    """A seasonal event page (Treasure Hunter, Holiday, Halloween, Fasnacht ...).

    image_dir is the event's reward folder under guide-images/seasonal-events/
    (the builder's EVENTS[...]["imageDir"], else the eventSlug). The page's
    existing event-folder URLs add any other folder it already uses (the
    Fasnacht atom-shop masks).
    """
    idx = index or load()
    dirs = []
    d = (image_dir or page.get("eventSlug") or "").strip("/")
    if d:
        dirs.append(SEASONAL_BASE + d + "/")
    dirs += [f for f in _folders_of(_page_urls(page, [])) if f not in dirs]
    stats = idx.apply(page, event_dirs=dirs, page=slug)
    misses = []
    for r in page.get("rewards") or []:
        if r.get("isTrackable") is False or (r.get("images") or r.get("imageUrl")):
            continue
        fid = item_fid(r)
        if fid:
            misses.append({"page": "seasonal-events/" + slug, "name": r.get("name") or "", "formid": fid,
                           "edid": r.get("edid") or "", "kind": r.get("sig") or r.get("kind") or "", "folder": dirs[0] if dirs else ""})
    return stats, misses


def mutated_page(page, slug, index=None):
    idx = index or load()
    stats = idx.apply(page, event_dirs=[MUTATED_BASE], page=slug)
    misses = [{"page": "mutated-events/" + slug, "name": r.get("name") or "", "formid": item_fid(r),
               "edid": r.get("edid") or "", "kind": r.get("sig") or r.get("kind") or "", "folder": MUTATED_BASE}
              for r in page.get("checklist") or []
              if item_fid(r) and not (r.get("images") or r.get("imageUrl"))]
    return stats, misses


def daily_ops_page(data, slug="daily-ops-all-rewards", index=None):
    """Daily Ops All Rewards: the checklist rows copy plan_master's images +
    image_dir; they come back as absolute URLs, which the renderer passes
    through verbatim."""
    idx = index or load()
    chk = data.get("checklist") or {}
    stats = idx.apply(chk, event_dirs=[DAILY_OPS_BASE], page=slug)
    misses = []
    for g in chk.get("groups") or []:
        for r in g.get("items") or []:
            if item_fid(r) and not r.get("images"):
                misses.append({"page": "daily-ops/" + slug, "name": r.get("name") or "",
                               "formid": item_fid(r), "edid": r.get("edid") or "",
                               "kind": r.get("sig") or r.get("kind") or "",
                               "folder": DAILY_OPS_BASE})
    return stats, misses


def unique_rewards_page(page, slug, category, index=None):
    """Activities / public events: imageUrl on the Unique Rewards items (the
    renderers already carry item.imageUrl through for those rows)."""
    idx = index or load()
    folder = UPLOADS + "guide-images/{}/{}/".format(category, re.sub(r"-all-rewards$", "", slug))
    ad = page.get("activityData") or {}
    lists = [ad.get("uniqueEventRewards") or [], page.get("conditionalRewards") or []]
    stats, misses = {}, []
    for lst in lists:
        idx.apply(lst, event_dirs=[folder], page=slug, stats=stats, fill_missing=True)
        for r in lst:
            if isinstance(r, dict) and item_fid(r) and not r.get("imageUrl"):
                misses.append({"page": category + "/" + slug, "name": r.get("name") or "",
                               "formid": item_fid(r), "edid": r.get("edid") or "",
                               "kind": r.get("sig") or r.get("kind") or "",
                               "folder": folder})
    return stats, misses


_TM_SIGS = {"BOOK", "ARMO"}


def treasure_maps_page(data, index=None):
    """Treasure maps: imageUrl on the plan and apparel rewards in every pool."""
    idx = index or load()
    skip = lambda d: (d.get("sig") or "").upper() not in _TM_SIGS        # noqa: E731
    stats = {}
    for key in ("shared_reward_pools", "regions", "teammate_reward", "pint_sized_phantoms"):
        if key in data:
            idx.apply(data[key], event_dirs=[TREASURE_MAPS_BASE], page="treasure-maps",
                      stats=stats, fill_missing=True, skip=skip)
    misses, seen = [], set()

    def walk(o):
        if isinstance(o, dict):
            fid = item_fid(o)
            if fid and o.get("name") and not skip(o) and not o.get("imageUrl") and fid not in seen:
                seen.add(fid)
                misses.append({"page": "treasure-maps/treasure-maps", "name": o["name"], "formid": fid,
                               "edid": o.get("edid") or "", "kind": o.get("sig") or "",
                               "folder": TREASURE_MAPS_BASE})
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    for key in ("shared_reward_pools", "regions", "teammate_reward", "pint_sized_phantoms"):
        walk(data.get(key))
    return stats, misses
