"""
ench_rows.py — read an ENCH_Export row the same way whichever version of
"!!!Wordpress - ExportENCHToTSV.pas" wrote it.

Two layouts exist in the wild (both under the SAME header):

  CORRECT (script fixed 19 Sep 2026):
      Effect_N_MGEF_FID | Effect_N_MGEF_EID | Effect_N_Magnitude | Effect_N_Area | Effect_N_Duration | Effect_N_CURV_EID
      00123456          | SomeEffect        | 50.0               | 0             | 10                | SomeCurve

  OLD / SHIFTED (exports made with the earlier script, e.g. October 2026):
      "00123456:SomeEffect" | 50.0 | 0 | 10 | SomeCurve | (blank)
      i.e. FormID and EditorID joined in the first cell and every later
      value one column to the left.

normalize(row) rewrites a shifted row into the correct layout (in place, and
returns it). A correct row is returned untouched. Every reader of the ENCH
export calls this first, so re-exporting with the fixed script can't break a
page and an old export still reads right.
"""

_FIELDS = ("MGEF_FID", "MGEF_EID", "Magnitude", "Area", "Duration", "CURV_EID")


def normalize(row, max_effects=40):
    if not row:
        return row
    for i in range(1, max_effects + 1):
        k = f"Effect_{i}_MGEF_FID"
        if k not in row:
            break
        fid = (row.get(k) or "").strip()
        if ":" not in fid:
            continue   # correct layout (or empty slot)
        old = [row.get(f"Effect_{i}_{f}", "") or "" for f in _FIELDS]
        f_id, e_id = fid.split(":", 1)
        row[f"Effect_{i}_MGEF_FID"] = f_id.strip()
        row[f"Effect_{i}_MGEF_EID"] = e_id.strip()
        row[f"Effect_{i}_Magnitude"] = old[1]
        row[f"Effect_{i}_Area"] = old[2]
        row[f"Effect_{i}_Duration"] = old[3]
        row[f"Effect_{i}_CURV_EID"] = old[4]
    return row


if __name__ == "__main__":
    # self-check with the October-2026 shape of Bloodied
    old = {"Effect_1_MGEF_FID": "001EC037:Legendary_DamageInverseHealth_Description",
           "Effect_1_MGEF_EID": "0.000000", "Effect_1_Magnitude": "0", "Effect_1_Area": "0",
           "Effect_1_Duration": "", "Effect_1_CURV_EID": ""}
    new = dict(old, Effect_1_MGEF_FID="001EC037", Effect_1_MGEF_EID="Legendary_DamageInverseHealth_Description",
               Effect_1_Magnitude="0.000000", Effect_1_Area="0", Effect_1_Duration="0", Effect_1_CURV_EID="")
    assert normalize(dict(old)) == new, normalize(dict(old))
    assert normalize(dict(new)) == new
    print("ench_rows ok")
