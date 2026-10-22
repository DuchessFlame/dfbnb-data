#!/usr/bin/env python3
r"""
note_markup.py — read a Fallout 76 note's text the way the game lays it out.

Notes (BOOK DESC) are written in a small Flash-HTML dialect:
    <p align="center"><font face="$Typewriter_Font" size="15">...</font></p>
    <font face='$76HandwrittenNeat_Font' color="#851A07">Look for him.</font>
plus <u> <b> <i> <br> and real line breaks (the ALLYDLG _LoreNotes export
keeps them as a literal \n; the BOOK export flattens them).

parse(text) -> blocks
    [{"align": "left|center|right",
      "runs": [{"text": "...", "font": "typewriter", "size": 15,
                "color": "#851A07", "u": 1, "b": 1, "i": 1}]}]
    A new block starts at every <p> and at every change of alignment.
    A run's text keeps its line breaks ("\n").
plain(blocks) -> str   the words only, blank line between paragraphs.

Fonts are folded into a few families the page and the picture maker know:
typewriter, handwritten, handwritten-neat, script, terminal, main.
The game's own markup is broken in places (size="15''>, </font></u> out of
order) - the parser is forgiving: unknown or unbalanced tags never lose text.
"""
from __future__ import annotations

import html
import re

FONT_FAMILY = [
    (r"typewriter", "typewriter"),
    (r"terminal|debug", "terminal"),
    (r"76handwrittenneat", "handwritten-neat"),
    (r"handwritten|illiterate", "handwritten"),
    (r"evascript|leiascript|script", "script"),
    (r"brody", "handwritten"),
    (r"main", "main"),
]

_TAG = re.compile(r"<\s*(/?)\s*([a-zA-Z]+)([^>]*)>")
_ATTR = re.compile(r"""([a-zA-Z]+)\s*=\s*(?:"([^"]*)"?|'([^']*)'?|([^\s>]+))""")


def family(face):
    f = re.sub(r"[^a-z0-9]", "", (face or "").lower())
    for rx, fam in FONT_FAMILY:
        if re.search(rx, f):
            return fam
    return "main"


def _attrs(s):
    out = {}
    for m in _ATTR.finditer(s or ""):
        v = m.group(2) if m.group(2) is not None else (m.group(3) if m.group(3) is not None else m.group(4))
        out[m.group(1).lower()] = (v or "").strip().strip("'\"")
    return out


def parse(text):
    s = (text or "").replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
    style = [{}]            # font stack
    flags = {"u": 0, "b": 0, "i": 0}
    align = "left"
    blocks = []

    def cur_block(new=False):
        if new or not blocks or blocks[-1]["align"] != align:
            blocks.append({"align": align, "runs": []})
        return blocks[-1]

    def emit(t):
        if not t:
            return
        t = html.unescape(t)
        st = style[-1]
        run = {"text": t, "font": st.get("font", "main")}
        if st.get("size"):
            run["size"] = st["size"]
        if st.get("color"):
            run["color"] = st["color"]
        for k in ("u", "b", "i"):
            if flags[k] > 0:
                run[k] = 1
        b = cur_block()
        if b["runs"]:
            last = b["runs"][-1]
            if {k: v for k, v in last.items() if k != "text"} == {k: v for k, v in run.items() if k != "text"}:
                last["text"] += t
                return
        b["runs"].append(run)

    pos = 0
    for m in _TAG.finditer(s):
        emit(s[pos:m.start()])
        pos = m.end()
        close, tag, rest = m.group(1) == "/", m.group(2).lower(), m.group(3)
        if tag == "font":
            if close:
                if len(style) > 1:
                    style.pop()
            else:
                a = _attrs(rest)
                st = dict(style[-1])
                if a.get("face"):
                    st["font"] = family(a["face"])
                if a.get("size"):
                    try:
                        st["size"] = int(re.sub(r"[^0-9]", "", a["size"]) or 0) or st.get("size")
                    except ValueError:
                        pass
                if a.get("color") and re.match(r"^#?[0-9a-fA-F]{6}$", a["color"]):
                    st["color"] = "#" + a["color"].lstrip("#").upper()
                style.append(st)
        elif tag == "p":
            if close:
                align = "left"
                cur_block(new=True)
            else:
                a = _attrs(rest)
                align = (a.get("align") or "left").lower()
                if align not in ("left", "center", "right"):
                    align = "left"
                cur_block(new=True)
        elif tag in ("u", "b", "i"):
            flags[tag] = max(0, flags[tag] + (-1 if close else 1))
        elif tag == "br":
            emit("\n")
        elif tag == "li" and not close:
            emit("\n• ")
        # anything else (<Alias>, <Token>, <textformat>, <img>) is dropped
    emit(s[pos:])

    # tidy: drop empty runs/blocks, trim blank lines at block edges
    out = []
    for b in blocks:
        runs = [r for r in b["runs"] if r["text"]]
        if not runs:
            continue
        runs[0]["text"] = runs[0]["text"].lstrip("\n")
        runs[-1]["text"] = runs[-1]["text"].rstrip()
        runs = [r for r in runs if r["text"]]
        for r in runs:
            r["text"] = re.sub(r"\n{3,}", "\n\n", re.sub(r"[ \t]+\n", "\n", r["text"]))
        if runs and "".join(r["text"] for r in runs).strip():
            out.append({"align": b["align"], "runs": runs})
    return out


def plain(blocks):
    paras = []
    for b in blocks:
        t = "".join(r["text"] for r in b["runs"]).strip()
        if t:
            paras.append(t)
    return re.sub(r"\n{3,}", "\n\n", "\n\n".join(paras)).strip()


if __name__ == "__main__":
    import json
    import sys
    print(json.dumps(parse(sys.stdin.read()), indent=1, ensure_ascii=False))
