"""challenge_required.py - EDID -> required count for every challenge, read from the
built dist/challenges/challenges.json (build_challenges_json_v3.py resolves the
count from the CHAL record and its count GLOB). Used by the Score Challenges
keyword pages so each Used For line reads "<challenge> (<count>)" (Duchess,
17 Nov 2026). A missing file just means no counts, never a failed build."""
import json
import os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _walk(items):
    for c in items or []:
        if isinstance(c, dict):
            yield c
            yield from _walk(c.get("children"))


def load(pts=False):
    path = os.path.join(REPO, "dist", *(["pts"] if pts else []), "challenges", "challenges.json")
    if pts and not os.path.exists(path):
        path = os.path.join(REPO, "dist", "challenges", "challenges.json")
    try:
        pages = json.load(open(path, encoding="utf-8")).get("pages") or {}
    except Exception:
        return {}
    out = {}
    for pl in pages.values():
        items = pl if isinstance(pl, list) else (pl.get("challenges") or pl.get("items") or [])
        for c in _walk(items):
            if c.get("edid") and c.get("required") not in (None, ""):
                out.setdefault(c["edid"], c["required"])
    return out



# ── Mini season label from the challenge EDID (Duchess, 17 Nov 2026) ────────
# A mini-season challenge carries its year and season in the EDID:
#   ATX_DE2025_Halloween_Week2_...  /  DE2026_Halloween_...
# so a line that ran in two years reads "... (30) · Halloween 2025 Mini Season" and
# "... (30) · Halloween 2026 Mini Season". The season name is matched to the site's
# own Mini Seasons page titles (tsv/guide_index.tsv); EDID shorthands that can't be
# matched by spelling are in SEASON_ALIASES.
import csv
import re

_DE_RE = re.compile(r"^(?:ATX_)?DE(20\d\d)_([A-Za-z]+?)(?:_|$)")
SEASON_ALIASES = {
    "MMMFE": "Marshal Mallow's Marvelous Fishing Excursion",
    "WeaponsExpert": "RIP Daring: Weapons Expert Extraordinaire",
    "RipDaring": "RIP Daring & the Cryptid Hunt",
    "SockHop": "Summer Sock Hop",
}
_TITLES = None


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def _titles():
    global _TITLES
    if _TITLES is None:
        _TITLES = {}
        try:
            for r in csv.DictReader(open(os.path.join(REPO, "tsv", "guide_index.tsv"),
                                         encoding="utf-8"), delimiter="\t"):
                url = r.get("url") or ""
                if re.fullmatch(r"/df/mini-seasons/[^/]+/", url):
                    _TITLES[_norm(r.get("title"))] = r.get("title")
        except Exception:
            pass
    return _TITLES


def season_label(edid):
    m = _DE_RE.match(str(edid or ""))
    if not m:
        return ""
    year, tok = m.group(1), m.group(2)
    name = SEASON_ALIASES.get(tok) or _titles().get(_norm(tok)) \
        or re.sub(r"(?<=[a-z])(?=[A-Z])", " ", tok)
    return f"{name} {year} Mini Season"
