#!/usr/bin/env python3
r"""
list_server_images.py - write a listing of every file under wp-content/uploads.

WHY
The image index (src/build_image_index.py) needs to know what is really on the
server. The site refuses folder browsing (403) and there is no full local
mirror, so this walks the server the same way FileZilla does and writes one
plain-text listing that is committed to the repo:

    data/server_listing.tsv      path<TAB>bytes<TAB>modified

Paths are relative to wp-content/uploads ("guide-images/titles/x.avif"). Run it
again after every upload batch, then rebuild the index.

HOW TO RUN (PowerShell, from the dfbnb-data folder)
    pip install paramiko                       # once
    python tools\list_server_images.py --filezilla "<site name>"

--filezilla reads the host, port, protocol and user name of a saved FileZilla
site (Site Manager) from %APPDATA%\FileZilla\sitemanager.xml. It never reads the
saved password: you are asked for it, it is used for this one connection, and
it is not written anywhere. Run with just --filezilla (no name) to list the
saved site names.

Or give the connection by hand:
    python tools\list_server_images.py --host example.sftp.wpengine.com --port 2222 --user myuser

Defaults: every top-level folder under uploads EXCEPT the WordPress media year
folders (2019, 2020 ... - thousands of auto-generated thumbnails the guide pages
never use). Add --years to include them. --only guide-images season_images
limits the walk to the named top-level folders.
"""

from __future__ import annotations

import argparse
import datetime
import getpass
import os
import posixpath
import re
import stat
import sys
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
DEFAULT_OUT = os.path.join(REPO, "data", "server_listing.tsv")
YEAR_DIR = re.compile(r"^\d{4}$")
# Plugin / cache folders that never hold guide art. Skipped unless --only
# names them.
SKIP_TOP = {"cache", "wpforms", "elementor", "backwpup", "backups", "ai1wm-backups",
            "wp-file-manager-pro", "updraft", "wc-logs", "woocommerce_uploads",
            "et-cache", "litespeed", "sucuri"}


# ── FileZilla site manager (host / port / protocol / user only) ────────────

def filezilla_sites():
    path = os.path.join(os.environ.get("APPDATA", ""), "FileZilla", "sitemanager.xml")
    if not os.path.exists(path):
        return path, []
    root = ET.parse(path).getroot()
    out = []

    def walk(node, prefix):
        for child in node:
            if child.tag == "Folder":
                name = (child.text or "").strip()
                walk(child, prefix + name + "/")
            elif child.tag == "Server":
                get = lambda t: (child.findtext(t) or "").strip()      # noqa: E731
                out.append({
                    "name": prefix + (get("Name") or get("Host")),
                    "host": get("Host"),
                    "port": int(get("Port") or 0) or None,
                    "protocol": get("Protocol") or "0",
                    "user": get("User"),
                    "remote_dir": get("RemoteDir"),
                })
    servers = root.find("Servers")
    if servers is not None:
        walk(servers, "")
    return path, out


# ── walkers ────────────────────────────────────────────────────────────────

class SftpWalker:
    def __init__(self, host, port, user, password):
        try:
            import paramiko
        except ImportError:
            sys.exit("paramiko is not installed - run:  pip install paramiko")
        self._t = paramiko.Transport((host, port or 22))
        self._t.connect(username=user, password=password)
        self.sftp = paramiko.SFTPClient.from_transport(self._t)

    def isdir(self, path):
        try:
            return stat.S_ISDIR(self.sftp.stat(path).st_mode)
        except OSError:
            return False

    def entries(self, path):
        for a in self.sftp.listdir_attr(path):
            is_dir = stat.S_ISDIR(a.st_mode or 0)
            yield a.filename, is_dir, a.st_size or 0, a.st_mtime or 0

    def close(self):
        self.sftp.close()
        self._t.close()


class FtpWalker:
    def __init__(self, host, port, user, password, tls):
        import ftplib
        self.ftp = ftplib.FTP_TLS() if tls else ftplib.FTP()
        self.ftp.connect(host, port or 21, timeout=60)
        self.ftp.login(user, password)
        if tls:
            self.ftp.prot_p()

    def isdir(self, path):
        try:
            cur = self.ftp.pwd()
            self.ftp.cwd(path)
            self.ftp.cwd(cur)
            return True
        except Exception:                                    # noqa: BLE001
            return False

    def entries(self, path):
        for name, facts in self.ftp.mlsd(path, facts=["type", "size", "modify"]):
            kind = facts.get("type", "")
            if kind in ("cdir", "pdir"):
                continue
            mod = facts.get("modify", "")
            ts = 0
            if mod:
                try:
                    ts = datetime.datetime.strptime(mod[:14], "%Y%m%d%H%M%S").replace(
                        tzinfo=datetime.timezone.utc).timestamp()
                except ValueError:
                    ts = 0
            yield name, kind == "dir", int(facts.get("size") or 0), ts

    def close(self):
        try:
            self.ftp.quit()
        except Exception:                                    # noqa: BLE001
            pass


def find_uploads(w, hint=""):
    """The uploads folder: --root if given, else the usual spellings."""
    tries = [hint] if hint else []
    tries += ["/wp-content/uploads", "wp-content/uploads",
              "/public_html/wp-content/uploads", "public_html/wp-content/uploads",
              "/htdocs/wp-content/uploads"]
    for t in tries:
        if t and w.isdir(t):
            return t
    sys.exit("Could not find wp-content/uploads on the server - pass --root with its path "
             "(the folder FileZilla shows when you open wp-content/uploads).")


def walk(w, root, only, years):
    files = []
    stack = []
    for name, is_dir, size, mtime in w.entries(root):
        if not is_dir:
            files.append((name, size, mtime))
            continue
        if only and name not in only:
            continue
        if not only and (name.lower() in SKIP_TOP or (YEAR_DIR.match(name) and not years)):
            continue
        stack.append(name)
    while stack:
        rel = stack.pop()
        try:
            listing = list(w.entries(posixpath.join(root, rel)))
        except Exception as exc:                             # noqa: BLE001
            print("  could not list {}: {}".format(rel, exc), file=sys.stderr)
            continue
        for name, is_dir, size, mtime in listing:
            child = rel + "/" + name
            if is_dir:
                stack.append(child)
            else:
                files.append((child, size, mtime))
                if len(files) % 1000 == 0:
                    print("  {} files so far ...".format(len(files)), file=sys.stderr)
    return files


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # nargs="?": PowerShell drops an empty "" argument, so a bare --filezilla
    # (no name) is how the saved site names are listed.
    ap.add_argument("--filezilla", metavar="SITE", nargs="?", const="",
                    help="saved FileZilla site name (on its own: list the saved names)")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    ap.add_argument("--user")
    ap.add_argument("--protocol", choices=["sftp", "ftp", "ftps"], help="default: from FileZilla, else sftp")
    ap.add_argument("--root", default="", help="path of wp-content/uploads on the server")
    ap.add_argument("--only", nargs="*", default=[], help="top-level upload folders to walk")
    ap.add_argument("--years", action="store_true", help="include WordPress media year folders")
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    host, port, user, proto, remote_dir = args.host, args.port, args.user, args.protocol, ""
    if args.filezilla is not None:
        path, sites = filezilla_sites()
        if not sites:
            sys.exit("No saved sites found in {}".format(path))
        if args.filezilla == "":
            print("Saved FileZilla sites:")
            for s in sites:
                print("  {}   ({}@{})".format(s["name"], s["user"], s["host"]))
            return
        want = args.filezilla.lower()
        hits = [s for s in sites if s["name"].lower() == want] or \
               [s for s in sites if want in s["name"].lower()]
        if len(hits) != 1:
            sys.exit("Site '{}' matched {} saved sites - run with just --filezilla to see the names."
                     .format(args.filezilla, len(hits)))
        s = hits[0]
        host = host or s["host"]
        port = port or s["port"]
        user = user or s["user"]
        remote_dir = s["remote_dir"]
        if not proto:
            proto = {"1": "sftp", "3": "ftps", "4": "ftps"}.get(s["protocol"], "ftp")
    if not host or not user:
        sys.exit("Need --filezilla <site> or --host and --user.")
    proto = proto or "sftp"

    password = getpass.getpass("Password for {}@{} (not saved): ".format(user, host))
    print("Connecting ({}) ...".format(proto), file=sys.stderr)
    if proto == "sftp":
        # WP Engine's SFTP is on 2222, everyone else's on 22.
        w = SftpWalker(host, port or (2222 if "wpengine" in host.lower() else 22), user, password)
    else:
        w = FtpWalker(host, port, user, password, tls=(proto == "ftps"))
    password = None
    try:
        # FileZilla's RemoteDir is stored in its own encoded form, so it is not
        # used; the usual spellings in find_uploads() cover WP Engine and cPanel.
        root = find_uploads(w, args.root)
        print("Walking {} ...".format(root), file=sys.stderr)
        files = walk(w, root, set(args.only), args.years)
    finally:
        w.close()

    files.sort(key=lambda f: f[0].lower())
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    tmp = args.out + ".part"
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write("# dfbnb server listing - written by tools/list_server_images.py\n")
        f.write("# generated\t{}\n".format(now))
        f.write("# root\t/wp-content/uploads\n")
        f.write("path\tbytes\tmodified\n")
        for rel, size, mtime in files:
            when = datetime.datetime.fromtimestamp(mtime, datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ") if mtime else ""
            f.write("{}\t{}\t{}\n".format(rel.replace("\t", " "), size, when))
    os.replace(tmp, args.out)
    print("Wrote {} files to {}".format(len(files), args.out))
    print("Next:  python src\\build_image_index.py")


if __name__ == "__main__":
    main()
