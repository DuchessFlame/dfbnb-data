#!/usr/bin/env python3
r"""
rate_format.py — the ONE way a drop / spawn chance is printed on the DF/BnB site.

Duchess, 11 Oct 2026: "show 4 numbers past the decimal point on all drop rates
on the website". Two decimals hid the real number on every rare drop — the
Tattered Field Jacket's 0.0078% read as 0.01%, and anything under 0.005% read
as a flat "0%", which looks like "can't drop" when it can.

Rules (shared by every builder, and mirrored by dfbnbFmtRate() in the theme JS):
  * up to 4 decimals, trailing zeros trimmed:  0.000078 -> "0.0078%",
    0.15 -> "15%", 0.3333333 -> "33.3333%"
  * a real chance smaller than 0.0001% is "<0.0001%", never "0%"
  * a computed 0 is "0%"; 99.99995% and up is "100%"

Display only. Nothing here does rate maths (drop-rate-engine / rng76 own that),
and nothing in rng76.py changes.
"""

PLACES = 4
_FLOOR = 10 ** -PLACES          # smallest percentage that can be printed


def fmt_pct(pct, places=PLACES):
    """A 0-100 percentage -> display string. None -> ""."""
    if pct is None or pct == "":
        return ""
    try:
        v = float(pct)
    except (TypeError, ValueError):
        return str(pct)
    if v <= 0:
        return "0%"
    if v >= 100 - 0.5 * 10 ** -places:
        return "100%"
    if v < 10 ** -places:
        return f"<{10 ** -places:.{places}f}%"
    s = f"{v:.{places}f}".rstrip("0").rstrip(".")
    return f"{s or '0'}%"


def fmt_rate(p, places=PLACES):
    """A 0-1 probability -> display string. None -> ""."""
    if p is None or p == "":
        return ""
    try:
        v = float(p)
    except (TypeError, ValueError):
        return str(p)
    return fmt_pct(v * 100.0, places)


def keep(p):
    """Round a stored 0-1 probability so 4-decimal percentages survive it.
    round(p, 6) kept only 2 significant figures on a 0.0078% drop."""
    return None if p is None else round(float(p), 10)


if __name__ == "__main__":
    for p in (0, 1e-9, 7.8e-05, 0.0003125, 0.15, 1 / 3, 0.99999, 1.0):
        print(p, fmt_rate(p))
