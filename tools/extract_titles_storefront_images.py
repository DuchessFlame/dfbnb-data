#!/usr/bin/env python3
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# -----------------------------
# Helpers
# -----------------------------

def norm_path(p: str) -> str:
    return (p or "").strip().replace("\\", "/").lower()

def run(cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)

def looks_like_merge_markers(text: str) -> bool:
    return ("<<<<<<<" in text) or ("=======" in text) or (">>>>>>>" in text)

# -----------------------------
# BA2 indexing via BSArch
# -----------------------------

LIST_CMD_VARIANTS = [
    # variant A (seen in some builds)
    lambda bsarch, ba2: [bsarch, str(ba2), "-list"],
    # variant B (some builds expect "list" verb)
    lambda bsarch, ba2: [bsarch, "list", str(ba2)],
    # variant C (some builds use "-dump")
    lambda bsarch, ba2: [bsarch, str(ba2), "-dump"],
]

def bsarch_list_files(bsarch_exe: str, ba2_path: Path) -> Optional[List[str]]:
    """
    Tries multiple known BSArch listing syntaxes. Returns normalized archive paths (lowercase, /).
    """
    for make_cmd in LIST_CMD_VARIANTS:
        cmd = make_cmd(bsarch_exe, ba2_path)
        try:
            cp = run(cmd, check=True)
            out = (cp.stdout or "") + "\n" + (cp.stderr or "")
            if not out.strip():
                continue
            # Heuristic: pull anything that looks like a path inside an archive
            # Typical lines contain "textures/....dds" etc.
            lines = []
            for line in out.splitlines():
                s = line.strip()
                if not s:
                    continue
                s2 = s.replace("\\", "/")
                if "textures/" in s2.lower() or s2.lower().endswith(".dds"):
                    # grab from first occurrence of "textures/" if present
                    idx = s2.lower().find("textures/")
                    if idx != -1:
                        s2 = s2[idx:]
                    lines.append(norm_path(s2))
            # de-dupe
            uniq = []
            seen = set()
            for x in lines:
                if x and x not in seen:
                    seen.add(x)
                    uniq.append(x)
            if uniq:
                return uniq
        except Exception:
            continue
    return None

def build_ba2_index(bsarch_exe: str, data_dir: Path, cache_path: Path) -> Dict[str, List[str]]:
    """
    Builds { archive_path: [file1, file2, ...] } and caches it.
    """
    ensure_dir(cache_path.parent)

    ba2_files = sorted(data_dir.glob("*.ba2"))
    if not ba2_files:
        die(f"No .ba2 files found in: {data_dir}")

    index: Dict[str, List[str]] = {}
    for i, ba2 in enumerate(ba2_files, start=1):
        print(f"[INDEX] ({i}/{len(ba2_files)}) Listing: {ba2.name}")
        files = bsarch_list_files(bsarch_exe, ba2)
        if files is None:
            print(f"[WARN] Could not list {ba2.name} with BSArch. Skipping.")
            continue
        index[str(ba2)] = files

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    return index

def load_ba2_index(bsarch_exe: str, data_dir: Path, cache_path: Path, rebuild: bool) -> Dict[str, List[str]]:
    if (not rebuild) and cache_path.exists():
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return build_ba2_index(bsarch_exe, data_dir, cache_path)

def find_archive_for_path(index: Dict[str, List[str]], wanted: str) -> Optional[str]:
    w = norm_path(wanted)
    if not w:
        return None
    for arch, files in index.items():
        # files are already normalized
        if w in files:
            return arch
    return None

# -----------------------------
# Extraction + Conversion
# -----------------------------

def bsarch_extract_one(bsarch_exe: str, ba2_path: Path, out_dir: Path) -> None:
    """
    Extract the whole archive to out_dir using a known common command.
    This is the safest universally: bsarch.exe unpack <archive> <folder>
    """
    ensure_dir(out_dir)
    cmd = [bsarch_exe, "unpack", str(ba2_path), str(out_dir)]
    try:
        run(cmd, check=True)
        return
    except subprocess.CalledProcessError as e:
        # fallback attempt: some builds use "unpack" without verb placement differences
        # but we keep it minimal: show stderr for debugging
        die(f"BSArch unpack failed for {ba2_path.name}\nSTDOUT:\n{e.stdout}\nSTDERR:\n{e.stderr}")

def texconv_dds_to_png(texconv_exe: str, dds_path: Path, png_out_dir: Path) -> Path:
    ensure_dir(png_out_dir)
    # texconv writes to output dir with same base name
    cmd = [texconv_exe, "-ft", "png", "-y", "-o", str(png_out_dir), str(dds_path)]
    cp = run(cmd, check=True)
    _ = (cp.stdout or "") + (cp.stderr or "")

    png_path = png_out_dir / (dds_path.stem + ".png")
    if not png_path.exists():
        die(f"texconv did not produce PNG: {png_path}")
    return png_path

def cwebp_png_to_webp(cwebp_exe: str, png_path: Path, webp_path: Path) -> None:
    ensure_dir(webp_path.parent)
    # Lossless preserves alpha. -z 9 is max effort.
    cmd = [cwebp_exe, "-lossless", "-z", "9", str(png_path), "-o", str(webp_path)]
    cp = run(cmd, check=True)
    _ = (cp.stdout or "") + (cp.stderr or "")

    if not webp_path.exists():
        die(f"cwebp did not produce WEBP: {webp_path}")

# -----------------------------
# Manifest processing
# -----------------------------

def load_manifest(manifest_path: Path) -> dict:
    if not manifest_path.exists():
        die(f"Manifest not found: {manifest_path}")
    text = manifest_path.read_text(encoding="utf-8", errors="replace")
    if looks_like_merge_markers(text):
        die("Manifest contains merge markers. Fix it before running.")
    return json.loads(text)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="Path to dist/titles_images_manifest.json")
    ap.add_argument("--data-dir", required=True, help="Fallout 76 Data folder containing *.ba2")
    ap.add_argument("--tools-dir", required=True, help="Folder containing bsarch.exe, texconv.exe, cwebp.exe")
    ap.add_argument("--export-dir", required=True, help="Output folder (export/)")
    ap.add_argument("--cache", default="ba2_index.json", help="Cache filename stored under export-dir/_cache/")
    ap.add_argument("--rebuild-index", action="store_true", help="Force rebuild BA2 index")
    ap.add_argument("--keep-temp", action="store_true", help="Keep temp folders for debugging")
    args = ap.parse_args()

    manifest_path = Path(args.manifest).resolve()
    data_dir = Path(args.data_dir).resolve()
    tools_dir = Path(args.tools_dir).resolve()
    export_dir = Path(args.export_dir).resolve()

    bsarch_exe = str((tools_dir / "bsarch.exe").resolve())
    texconv_exe = str((tools_dir / "texconv.exe").resolve())
    cwebp_exe = str((tools_dir / "cwebp.exe").resolve())

    for exe in [bsarch_exe, texconv_exe, cwebp_exe]:
        if not Path(exe).exists():
            die(f"Missing tool: {exe}")

    if not data_dir.exists():
        die(f"Data dir does not exist: {data_dir}")

    ensure_dir(export_dir)
    cache_dir = export_dir / "_cache"
    ensure_dir(cache_dir)
    index_path = cache_dir / args.cache

    mf = load_manifest(manifest_path)
    tasks = mf.get("tasks") or []
    if not tasks:
        print("[INFO] Manifest tasks is empty. Nothing to do.")
        return 0

    print(f"[INFO] Loaded manifest tasks: {len(tasks)}")
    print(f"[INFO] Loading BA2 index (cache: {index_path.name})")
    index = load_ba2_index(bsarch_exe, data_dir, index_path, rebuild=args.rebuild_index)

    # temp working dirs
    temp_root = export_dir / "_temp"
    ensure_dir(temp_root)
    unpack_root = temp_root / "unpack"
    png_root = temp_root / "png"
    ensure_dir(unpack_root)
    ensure_dir(png_root)

    storefront_dir = export_dir / "storefront"
    ensure_dir(storefront_dir)

    # We only need storefront images right now:
    # export/storefront/<entm_edid_lower>.webp
    done = 0
    skipped = 0

    # Keep a memo: if we already extracted an archive once, do not re-unpack it.
    unpacked_archives: Dict[str, Path] = {}

    for t in tasks:
        ent_ids = t.get("entitlementEdids") or []
        dds_paths = t.get("ddsPaths") or []

        if not ent_ids or not dds_paths:
            skipped += 1
            continue

        # Deterministic: for each entitlement EDID, write one webp.
        # If multiple DDS candidates exist, pick first that we can resolve.
        for ent in ent_ids:
            ent_l = (str(ent) or "").strip().lower()
            if not ent_l:
                continue

            out_webp = storefront_dir / f"{ent_l}.webp"
            if out_webp.exists():
                continue

            picked_dds = None
            picked_arch = None

            for dds in dds_paths:
                arch = find_archive_for_path(index, dds)
                if not arch:
                    continue
                picked_dds = norm_path(dds)
                picked_arch = arch
                break

            if not picked_dds or not picked_arch:
                print(f"[MISS] {ent_l} -> no BA2 match for any ddsPaths")
                skipped += 1
                continue

            arch_path = Path(picked_arch)

            # Unpack archive once
            if picked_arch not in unpacked_archives:
                out_folder = unpack_root / arch_path.stem
                print(f"[UNPACK] {arch_path.name} -> {out_folder}")
                bsarch_extract_one(bsarch_exe, arch_path, out_folder)
                unpacked_archives[picked_arch] = out_folder

            unpack_folder = unpacked_archives[picked_arch]
            dds_on_disk = unpack_folder / Path(picked_dds)
            if not dds_on_disk.exists():
                # Sometimes BSArch preserves original case in folders. Try a case-insensitive walk fallback.
                found = None
                wanted = picked_dds.replace("/", os.sep).lower()
                for p in unpack_folder.rglob("*"):
                    if p.is_file() and p.suffix.lower() == ".dds":
                        rel = str(p.relative_to(unpack_folder)).replace("\\", "/").lower()
                        if rel == picked_dds:
                            found = p
                            break
                        if rel.replace("/", os.sep) == wanted:
                            found = p
                            break
                if found:
                    dds_on_disk = found

            if not dds_on_disk.exists():
                print(f"[MISS] {ent_l} -> extracted archive but DDS not found: {picked_dds}")
                skipped += 1
                continue

            # Convert DDS -> PNG -> WEBP
            png = texconv_dds_to_png(texconv_exe, dds_on_disk, png_root)
            cwebp_png_to_webp(cwebp_exe, png, out_webp)

            done += 1
            print(f"[OK] {ent_l}.webp")

    print(f"[DONE] created={done} skipped/missed={skipped}")

    if not args.keep_temp:
        shutil.rmtree(temp_root, ignore_errors=True)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())