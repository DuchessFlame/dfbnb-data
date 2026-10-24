"""chal_names.py - the ONE place a challenge's display name is made.

Duchess, 24 Oct 2026: a challenge named as a way to get something must show
how many it asks for - "Find more Abraxodyne Intel (145)". Nine Abraxodyne
challenges share that FULL; only the count (CHAL TNAM) tells them apart.

Every builder that prints a challenge as a source, condition or unlock calls
display() / Index.name() instead of reading CHAL FULL itself, so a new page
gets the count without anyone remembering to add it.

    import chal_names
    idx = chal_names.Index(path_to_CHAL_Export)      # or Index.newest(tsv_dir)
    idx.name("0085299C")                             # by FormID
    idx.name_by_edid("Burn_Challenge_..._09")        # by EditorID
    chal_names.display("Find more Abraxodyne Intel", 145)

Not for the challenge checklist pages themselves: they show the count in
their own Required Count column.
"""
import csv
import glob
import os
import re

csv.field_size_limit(10 ** 9)


def display(full, count):
    """'FULL (n)' when the challenge asks for more than one and the name does
    not already say the number; otherwise FULL unchanged."""
    full = (full or "").strip()
    try:
        n = int(float(str(count).replace(",", "").strip() or 0))
    except (TypeError, ValueError):
        n = 0
    if not full or n <= 1:
        return full
    if re.search(rf"(?<![\d,]){n:,}(?![\d,])|(?<!\d){n}(?!\d)", full):
        return full
    return f"{full} ({n:,})"


def _fid(v):
    v = (v or "").strip().strip('"').upper()
    v = v.split(":")[0]
    return v.zfill(8) if re.fullmatch(r"[0-9A-F]{1,8}", v) else v


class Index:
    """CHAL FormID / EditorID -> display name (with count)."""

    def __init__(self, path=None):
        self.by_fid, self.by_edid, self.count = {}, {}, {}
        if path and os.path.exists(path):
            with open(path, encoding="utf-8", errors="replace", newline="") as f:
                for r in csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE):
                    fid = _fid(r.get("FormID") or r.get("CHAL_FormID"))
                    ed = (r.get("EDID") or r.get("CHAL_EDID") or "").strip().strip('"')
                    full = (r.get("FULL") or r.get("CHAL_FULL") or "").strip().strip('"')
                    n = r.get("TNAM") or 0
                    try:
                        n = int(float(str(n).strip() or 0))
                    except ValueError:
                        n = 0
                    if fid:
                        self.count[fid] = n
                        self.by_fid[fid] = display(full, n)
                    if ed:
                        self.by_edid[ed.lower()] = display(full, n)

    @classmethod
    def newest(cls, tsv_dir, pattern="CHAL_Export_*.tsv"):
        files = [p for p in glob.glob(os.path.join(tsv_dir, pattern))
                 if "_Rewards" not in p and "_Refs" not in p]
        return cls(max(files, key=os.path.getmtime) if files else None)

    def name(self, fid, full=None):
        """Display name for a CHAL FormID; `full` is used (with the count) when
        the caller already has the plain name from somewhere else."""
        f = _fid(fid)
        if full is not None:
            return display(full, self.count.get(f, 0))
        return self.by_fid.get(f, "")

    def name_by_edid(self, edid):
        return self.by_edid.get((edid or "").strip().lower(), "")

    def with_count(self, full, fid=None, edid=None):
        """Add the count to a name a caller already printed."""
        if fid:
            return display(full, self.count.get(_fid(fid), 0))
        if edid:
            n = self.by_edid.get((edid or "").strip().lower(), "")
            return n or full
        return full
