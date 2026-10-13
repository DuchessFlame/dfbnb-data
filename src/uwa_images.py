"""Pictures for the Unique Weapons & Armour page (Duchess, 12 Oct 2026).

Every unique gets the picture already on the server under its own name, read
from data/server_listing.tsv so the page never points at a file that is not
uploaded. Nothing is hand-mapped unless a name cannot be matched at all.

Matching, first hit wins:
  1. guide-images/plan-checklist/weapons/<slug>.avif, or <slug>-paint.avif
  2. a weapons-folder file whose name is the item's name once spaces,
     apostrophes and a trailing -paint / -skin are ignored
     ("Foundations Vengeance" -> foundation-s-vengeance-paint.avif)
  3. a weapons-folder file that starts or ends with the item's name, for names
     of 8+ letters and only when exactly one file fits
     ("Red Terror" -> mg42-red-terror-paint.avif)
  3b. <slug>.avif in any other guide-images folder, when exactly one exists
     (the Cursed weapons live in seasonal-events/treasure-hunters/)
  4. Scoreboard art named score_s<N>_weapons_<name>.avif in season_images
     ("Head Hunter" -> score_s17_weapons_headhunter.avif)
  5. NAME_ALIAS - names the files spell differently
  6. SAME_LOOK_AS_BASE - uniques that look exactly like their base weapon in
     game (the wiki shows the base picture too). They use the base weapon's
     picture, flagged imageSameAsBase so the Weapon page, which already lists
     them under that weapon, does not repeat it.
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LISTING = os.path.join(ROOT, "data", "server_listing.tsv")
WEAP_DIR = "guide-images/plan-checklist/weapons/"
URL_ROOT = "/wp-content/uploads/"

NAME_ALIAS = {
    "Gunthers Revolver": "gunther-s-paint",
    "Slasher Axe": "sdow_mod_custom_slasheraxe_appearance",
    # The wiki shows Zeta's Revenge as the Alien Blaster in its Steel skin.
    "Zeta's Revenge": "guide-images/atom-shop/request-item-images/ATX_Skin_WeaponSkin_AlienBlaster_Steel.avif",
}

SAME_LOOK_AS_BASE = {
    "Black Diamond": "ski-sword",
    "Camden Whacker": "commie-whacker",
    "Commanders Charge": "revolutionary-war-sword",
    "Crushing Blow": "m79-grenade-launcher",
    "Daisycutter": "fatman",
    "Final Word": "50-cal-machine-gun",
    "Grant's Saber": "chinese-officer-sword",
    "Nailer": "cultist-blade",
    "Oath Breaker": "war-glaive",
    "Peace Maker": "auto-grenade-launcher",
    "Rose's Syringer": "syringer",
    "Salt Of The Earth": "double-barrel-shotgun",
    "The Action Hero": "50-cal-machine-gun",
    "The Farmhand": "super-sledge",
    "The Guarantee": "fatman",
    "Vox Syringer": "syringer",
    "Xerxos": "gamma-gun",
}

_RX_SEASON = re.compile(r"^season_images/[^/]+/score_s\d+_weapons_(.+)\.avif$", re.I)


def _slug(s):
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", (s or "").lower())).strip("-")


def _compact(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _core(stem):
    return _compact(re.sub(r"[-_](paint|skin)$", "", stem, flags=re.I))


def load_listing(path=LISTING):
    weap, season, other = {}, {}, {}
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("#"):
                    continue
                p = line.split("\t", 1)[0].strip()
                if not p.lower().endswith(".avif"):
                    continue
                if p.startswith(WEAP_DIR) and "/" not in p[len(WEAP_DIR):]:
                    weap[p[len(WEAP_DIR):-5]] = p
                elif p.startswith("guide-images/"):
                    other.setdefault(p.rsplit("/", 1)[-1][:-5].lower(), []).append(p)
                m = _RX_SEASON.match(p)
                if m:
                    season[m.group(1)] = p
    except OSError:
        pass
    return weap, season, other


def find(name, weap, season, other=None):
    """(listing path, same_as_base) for one unique, or (None, False)."""
    s = _slug(name)
    for c in (s, s + "-paint"):
        if c in weap:
            return weap[c], False
    full = _compact(name)
    short = _compact(re.sub(r"^the\s+", "", name or "", flags=re.I))
    hits = [p for st, p in weap.items() if _core(st) in (full, short)]
    if len(hits) == 1:
        return hits[0], False
    if other and len(other.get(s) or []) == 1:
        return other[s][0], False
    if len(short) >= 8:
        hits = [p for st, p in weap.items()
                if _core(st).startswith(short) or _core(st).endswith(short)]
        if len(hits) == 1:
            return hits[0], False
        hits = [p for tok, p in season.items() if _compact(tok).endswith(short)]
        if len(hits) == 1:
            return hits[0], False
    alias = NAME_ALIAS.get(name)
    if alias and alias in weap:
        return weap[alias], False
    if alias and "/" in alias and any(alias in v for v in (other or {}).values()):
        return alias, False
    base = SAME_LOOK_AS_BASE.get(name)
    if base and base in weap:
        return weap[base], True
    return None, False


def attach(items, path=LISTING):
    """Set `image` (absolute URL) on every unique the server has a picture for."""
    weap, season, other = load_listing(path)
    n = {"own": 0, "base": 0, "none": 0}
    for it in items:
        p, same = find(it.get("name") or "", weap, season, other)
        # renamed to its in-game name (uwa_names): the picture may still be
        # filed under the old one (strike-breaker.avif for Flatliner)
        for alt in ([] if p else list(it.get("_altNames") or []) + list(it.get("aka") or [])):
            p, same = find(alt, weap, season, other)
            if p:
                break
        if p:
            it["image"] = URL_ROOT + p
            if same:
                it["imageSameAsBase"] = True
            else:
                it.pop("imageSameAsBase", None)
            n["base" if same else "own"] += 1
        else:
            it["image"] = ""
            it.pop("imageSameAsBase", None)
            n["none"] += 1
    print(f"  images: own={n['own']}, same as base={n['base']}, none={n['none']}")
    return n
