import argparse, json, os, re

def norm_slashes(p: str) -> str:
    p = (p or "").strip().replace("\\", "/")
    p = re.sub(r"^/+","", p)
    return p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # TSV is often cp1252-ish from xEdit exports
    with open(args.tsv, "r", encoding="latin1", errors="replace") as f:
        header = f.readline().rstrip("\n")
        cols = header.split("\t")

        def idx(name):
            try: return cols.index(name)
            except ValueError: return -1

        i_edid = idx("EDID")
        i_etip = idx("ETIP")
        i_etdi = idx("ETDI")

        if i_edid < 0 or i_etip < 0 or i_etdi < 0:
            raise SystemExit("TSV missing required columns: EDID, ETIP, ETDI")

        items = []
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")

            edid = (parts[i_edid] if i_edid < len(parts) else "").strip()
            etip = (parts[i_etip] if i_etip < len(parts) else "").strip()
            etdi = (parts[i_etdi] if i_etdi < len(parts) else "").strip()

            # Only entries that actually have an image path + filename
            if not edid or not etip or not etdi:
                continue

            # entitlement key = lowercase EDID (matches your webp naming convention)
            ent = edid.strip().lower()

            # combine ETIP + ETDI into a single DDS path
            dds = norm_slashes(etip) + norm_slashes(etdi)

            # drop leading "Textures/" so it becomes relative to extracted textures root
            if dds.lower().startswith("textures/"):
                dds = dds[9:]  # len("Textures/") == 9

            # normalize case and slashes
            dds = dds.replace("/", "\\")
            items.append({
                "entitlement": ent,
                "dds": dds
            })

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as out:
        json.dump(items, out, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()