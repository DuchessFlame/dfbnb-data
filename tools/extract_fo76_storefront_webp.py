import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

def norm(p: str) -> str:
    return (p or "").replace("\\", "/").strip().lower()

def run(cmd):
    return subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def find_ba2_files(data_dir: Path):
    return sorted(data_dir.glob("*.ba2"))

def load_manifest(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def bsarch_list(bsarch: Path, ba2: Path):
    # Try common list forms
    for cmd in (
        [str(bsarch), "list", str(ba2)],
        [str(bsarch), "-list", str(ba2)],
        [str(bsarch), "--list", str(ba2)],
    ):
        try:
            cp = run(cmd)
            out = []
            for line in (cp.stdout or "").splitlines():
                s = line.strip()
                if not s:
                    continue
                if "/" in s or "\\" in s:
                    parts = s.replace("\\", "/").split()
                    pathish = max(parts, key=lambda x: x.count("/"))
                    out.append(norm(pathish))
            if out:
                return out
        except Exception:
            continue
    return []

def bsarch_extract_one(bsarch: Path, ba2: Path, internal_path: str, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    ip = internal_path.replace("\\", "/")

    candidates = [
        [str(bsarch), "extract", str(ba2), ip, str(out_dir)],
        [str(bsarch), "-extract", str(ba2), ip, str(out_dir)],
        [str(bsarch), "-extract", "-f", str(ba2), "-e", ip, "-o", str(out_dir)],
        [str(bsarch), "--extract", "-f", str(ba2), "-e", ip, "-o", str(out_dir)],
    ]
    target_name = Path(ip).name.lower()

    for cmd in candidates:
        try:
            run(cmd)
            for h in out_dir.rglob("*"):
                if h.is_file() and h.name.lower() == target_name:
                    return h
        except Exception:
            continue
    return None

def dds_to_png(texconv: Path, dds_path: Path, png_dir: Path) -> Path:
    png_dir.mkdir(parents=True, exist_ok=True)
    run([str(texconv), "-ft", "png", "-y", "-o", str(png_dir), str(dds_path)])
    out_png = png_dir / (dds_path.stem + ".png")
    if not out_png.exists():
        raise RuntimeError(f"texconv did not output {out_png}")
    return out_png

def png_to_webp(cwebp: Path, png_path: Path, webp_path: Path):
    webp_path.parent.mkdir(parents=True, exist_ok=True)
    run([str(cwebp), "-lossless", str(png_path), "-o", str(webp_path)])
    if not webp_path.exists():
        raise RuntimeError(f"cwebp did not output {webp_path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="dist/titles_images_manifest.json")
    ap.add_argument("--data-dir", required=True, help="Fallout 76 Data folder (contains *.ba2)")
    ap.add_argument("--bsarch", required=True, help="Path to bsarch.exe")
    ap.add_argument("--texconv", required=True, help="Path to texconv.exe")
    ap.add_argument("--cwebp", required=True, help="Path to cwebp.exe")
    ap.add_argument("--export-dir", required=True, help="export folder root")
    args = ap.parse_args()

    manifest = load_manifest(Path(args.manifest))
    tasks = manifest.get("tasks") or []
    if not tasks:
        print("[OK] No tasks in manifest.")
        return

    data_dir = Path(args.data_dir)
    bsarch = Path(args.bsarch)
    texconv = Path(args.texconv)
    cwebp = Path(args.cwebp)
    export_root = Path(args.export_dir)
    out_store = export_root / "storefront"
    out_store.mkdir(parents=True, exist_ok=True)

    ba2s = find_ba2_files(data_dir)
    if not ba2s:
        raise SystemExit(f"No .ba2 found in {data_dir}")

    # Collect all DDS paths needed
    want = set()
    for t in tasks:
        for p in (t.get("ddsPaths") or []):
            if p:
                want.add(norm(p))
    want = sorted(want)

    # Build DDS -> BA2 map by listing BA2 contents once
    dds_to_ba2 = {}
    for ba2 in ba2s:
        listed = bsarch_list(bsarch, ba2)
        if not listed:
            continue
        s = set(listed)
        for p in want:
            if p in s and p not in dds_to_ba2:
                dds_to_ba2[p] = ba2

    missing = [p for p in want if p not in dds_to_ba2]
    if missing:
        print("[WARN] Missing DDS paths (not found in any BA2). Showing first 30:")
        for p in missing[:30]:
            print("  -", p)

    made = 0
    skipped = 0

    with tempfile.TemporaryDirectory(prefix="fo76_storefront_") as td:
        td = Path(td)
        dds_dir = td / "dds"
        png_dir = td / "png"

        for t in tasks:
            ent_edids = t.get("entitlementEdids") or []
            dds_paths = [norm(p) for p in (t.get("ddsPaths") or []) if p]

            if not ent_edids or not dds_paths:
                continue

            for ent in ent_edids:
                ent_lower = str(ent).strip().lower()
                if not ent_lower:
                    continue

                out_webp = out_store / f"{ent_lower}.webp"
                if out_webp.exists():
                    skipped += 1
                    continue

                extracted = None
                for dp in dds_paths:
                    ba2 = dds_to_ba2.get(dp)
                    if not ba2:
                        continue
                    extracted = bsarch_extract_one(bsarch, ba2, dp, dds_dir)
                    if extracted and extracted.exists():
                        break

                if not extracted:
                    print(f"[MISS] {ent_lower} (no DDS extracted)")
                    continue

                try:
                    png = dds_to_png(texconv, extracted, png_dir)
                    png_to_webp(cwebp, png, out_webp)
                    print(f"[OK] {out_webp.name}")
                    made += 1
                except Exception as e:
                    print(f"[FAIL] {ent_lower}: {e}")

    print(f"[DONE] made={made} skipped={skipped} export={export_root}")

if __name__ == "__main__":
    main()
