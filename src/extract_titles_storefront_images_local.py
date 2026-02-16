#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def norm_rel_path(p: str) -> str:
    """
    Normalize a manifest DDS path to a canonical key we can match against the extracted tree.
    Manifest paths are like: textures/atx/storefront/.../file.dds
    We normalize to: textures/atx/storefront/.../file.dds (lowercase, forward slashes)
    """
    return (p or "").strip().replace("\\", "/").lstrip("/").lower()


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def run(cmd: List[str]) -> subprocess.CompletedProcess:
    """
    Run a subprocess and raise a readable error if it fails.
    """
    try:
        return subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.CalledProcessError as e:
        out = (e.stdout or "").strip()
        err = (e.stderr or "").strip()
        msg = f"Command failed:\n  {' '.join(cmd)}"
        if out:
            msg += f"\n\nSTDOUT:\n{out}"
        if err:
            msg += f"\n\nSTDERR:\n{err}"
        die(msg)


def load_manifest(path: Path) -> dict:
    if not path.exists():
        die(f"Manifest not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"Failed to parse manifest JSON: {path}\n{e}")


def build_extracted_dds_index(extracted_textures_root: Path) -> Dict[str, Path]:
    """
    Scan extracted_textures_root (which should be ...\\textures) and build a map:
      "textures/.../file.dds" -> absolute Path(on disk)
    Matching is case-insensitive via normalization.
    """
    if not extracted_textures_root.exists():
        die(f"Extracted textures folder does not exist: {extracted_textures_root}")

    root = extracted_textures_root
    # If user passed ...\\textures, then relative paths are "atx/...".
    # We add "textures/" prefix to make it match manifest style.
    index: Dict[str, Path] = {}

    dds_files = list(root.rglob("*.dds"))
    if not dds_files:
        print(f"[WARN] No .dds files found under: {root}")

    for f in dds_files:
        # Skip placeholders / bad extracts
        try:
            if f.stat().st_size == 0:
                continue
        except Exception:
            continue

        rel = f.relative_to(root).as_posix()  # can be "textures/textures/atx/..." if root is higher
        rel = rel.replace("\\", "/").lower()

        # Strip redundant leading "textures/" segments so we end up with exactly one.
        while rel.startswith("textures/"):
            rel = rel[len("textures/"):]

        key = norm_rel_path("textures/" + rel)  # final canonical key: "textures/atx/.../file.dds"
        if key not in index:
            index[key] = f

    return index

def build_filename_index(extracted_root: Path) -> Dict[str, Path]:
    """
    Build filename-only index:
      "file.dds" -> full path
    Lowercase keys.
    """
    idx: Dict[str, Path] = {}
    for f in extracted_root.rglob("*.dds"):
        try:
            if f.stat().st_size == 0:
                continue
        except Exception:
            continue

        name = f.name.lower()
        if name not in idx:
            idx[name] = f
    return idx

def choose_dds_for_entitlement(ent_idx: int, ent_total: int, dds_paths: List[str], dds_index: Dict[str, Path]) -> Tuple[Optional[str], Optional[Path]]:
    """
    Deterministic pairing rules:

    If len(entitlementEdids) == len(ddsPaths):
      - Pair by index (ent[i] uses ddsPaths[i])
      - Fallback: if that exact one missing, try the others in order.

    Else:
      - Try the DDS at same index if it exists
      - Else try all DDS candidates in order
    """
    normed = [norm_rel_path(p) for p in dds_paths if p]

    if ent_total == len(normed) and ent_idx < len(normed):
        primary = normed[ent_idx]
        if primary in dds_index:
            return primary, dds_index[primary]
        for p in normed:
            if p in dds_index:
                return p, dds_index[p]
        return None, None

    if ent_idx < len(normed):
        primary = normed[ent_idx]
        if primary in dds_index:
            return primary, dds_index[primary]

    for p in normed:
        if p in dds_index:
            return p, dds_index[p]

    return None, None


def texconv_dds_to_png(texconv_exe: Path, dds_path: Path, png_dir: Path) -> Path:
    """
    Convert DDS -> PNG using texconv, preserving alpha.
    texconv writes output file into png_dir with same basename.
    """
    ensure_dir(png_dir)

    cmd = [
        str(texconv_exe),
        "-ft", "png",
        "-y",
        "-o", str(png_dir),
        str(dds_path),
    ]
    run(cmd)

    out_png = png_dir / (dds_path.stem + ".png")
    if not out_png.exists():
        die(f"texconv did not output PNG as expected: {out_png}")
    return out_png


def cwebp_png_to_webp(cwebp_exe: Path, png_path: Path, webp_path: Path) -> None:
    """
    Convert PNG -> WEBP lossless (preserves alpha).
    """
    ensure_dir(webp_path.parent)

    cmd = [
        str(cwebp_exe),
        "-lossless",
        "-z", "9",
        str(png_path),
        "-o", str(webp_path),
    ]
    run(cmd)

    if not webp_path.exists():
        die(f"cwebp did not output WEBP as expected: {webp_path}")


def main() -> int:
    ap = argparse.ArgumentParser(description="FO76 Titles Storefront DDS -> WEBP (no BA2 logic, uses pre-extracted textures).")
    ap.add_argument("--manifest", required=True, help="Path to dist/titles_images_manifest.json")
    ap.add_argument("--extracted-textures", required=True, help=r'Path to extracted "textures" folder (e.g. D:\FO76_Extracted_Textures\textures)')
    ap.add_argument("--tools-dir", required=True, help="Folder containing texconv.exe and cwebp.exe")
    ap.add_argument("--export-dir", required=True, help="Repo export folder root (export/). WEBP goes to export/storefront/")
    ap.add_argument("--keep-temp", action="store_true", help="Keep temp PNGs for debugging")
    args = ap.parse_args()

    manifest_path = Path(args.manifest).resolve()
    extracted_textures_root = Path(args.extracted_textures).resolve()
    tools_dir = Path(args.tools_dir).resolve()
    export_root = Path(args.export_dir).resolve()

    texconv_exe = tools_dir / "texconv.exe"
    cwebp_exe = tools_dir / "cwebp.exe"

    if not texconv_exe.exists():
        die(f"Missing texconv.exe at: {texconv_exe}")
    if not cwebp_exe.exists():
        die(f"Missing cwebp.exe at: {cwebp_exe}")

    mf = load_manifest(manifest_path)
    tasks = mf.get("tasks") or []
    if not tasks:
        print("[OK] Manifest has no tasks. Nothing to do.")
        return 0

    print(f"[INFO] Manifest tasks: {len(tasks)}")
    print(f"[INFO] Scanning extracted DDS under: {extracted_textures_root}")

    dds_index = build_extracted_dds_index(extracted_textures_root)
    filename_index = build_filename_index(extracted_textures_root)

    print(f"[INFO] Indexed DDS files (path-based): {len(dds_index)}")
    print(f"[INFO] Indexed DDS files (filename-based): {len(filename_index)}")

    out_store = export_root / "storefront"
    ensure_dir(out_store)

    temp_root = export_root / "_temp_storefront"
    png_dir = temp_root / "png"
    ensure_dir(png_dir)

    created = 0
    skipped = 0
    missing = 0

    missing_examples: List[str] = []

    for t in tasks:
        ent_edids = t.get("entitlementEdids") or []
        dds_paths = t.get("ddsPaths") or []

        if not ent_edids or not dds_paths:
            continue

        ent_total = len(ent_edids)

        for i, ent in enumerate(ent_edids):
            ent_lower = (str(ent) or "").strip().lower()
            if not ent_lower:
                continue

            out_webp = out_store / f"{ent_lower}.webp"
            if out_webp.exists():
                skipped += 1
                continue

            chosen_key, chosen_dds = choose_dds_for_entitlement(i, ent_total, dds_paths, dds_index)

            # Fallback: match by entitlement filename if path-based match failed
            if not chosen_dds:
                candidate_name = f"{ent_lower}.dds"
                chosen_dds = filename_index.get(candidate_name)
                if chosen_dds:
                    chosen_key = candidate_name

            if not chosen_dds:
                missing += 1
                if len(missing_examples) < 25:
                    missing_examples.append(f"{ent_lower} -> (no extracted match)")
                continue

            png = texconv_dds_to_png(texconv_exe, chosen_dds, png_dir)
            cwebp_png_to_webp(cwebp_exe, png, out_webp)

            created += 1
            print(f"[OK] created {out_webp.name}  (from {chosen_key})")

    if not args.keep_temp:
        shutil.rmtree(temp_root, ignore_errors=True)

    print("")
    print("[SUMMARY]")
    print(f"  created : {created}")
    print(f"  skipped : {skipped} (already existed)")
    print(f"  missing : {missing} (no DDS match found)")

    if missing_examples:
        print("")
        print("[MISSING EXAMPLES] (first 25)")
        for m in missing_examples:
            print(f"  - {m}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
