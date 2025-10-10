#!/usr/bin/env python3
"""download.py

Downloader and extractor for the Dark Ecology dataset archives.

Usage examples:
    python3 scripts/download_data.py --out data --5min
    python3 scripts/download_data.py --out data --daily --5min --profiles 2012-2022 --extract
    python3 scripts/download_data.py --out data --all --extract

Requires: Python 3.8+, 'requests' and 'tqdm'
Install dependencies: python3 -m pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import shutil
import tarfile
import subprocess
import requests
import sys
import os
from tqdm import tqdm
from pathlib import Path
from typing import Dict, List


DOWNLOAD_LINKS: Dict[str, str] = {
    "5min": "https://zenodo.org/records/13345266/files/5min.tar.bz2",
    "daily": "https://zenodo.org/records/13345266/files/daily.tar.bz2",
    "scans": "https://zenodo.org/records/13345266/files/scans.tar.bz2",
    "profiles_1995": "https://zenodo.org/records/13345174/files/profiles_1995.tar.bz2",
    "profiles_1996": "https://zenodo.org/records/13345174/files/profiles_1996.tar.bz2",
    "profiles_1997": "https://zenodo.org/records/13345174/files/profiles_1997.tar.bz2",
    "profiles_1998": "https://zenodo.org/records/13345174/files/profiles_1998.tar.bz2",
    "profiles_1999": "https://zenodo.org/records/13345174/files/profiles_1999.tar.bz2",
    "profiles_2000": "https://zenodo.org/records/13345202/files/profiles_2000.tar.bz2",
    "profiles_2001": "https://zenodo.org/records/13345202/files/profiles_2001.tar.bz2",
    "profiles_2002": "https://zenodo.org/records/13345202/files/profiles_2002.tar.bz2",
    "profiles_2003": "https://zenodo.org/records/13345202/files/profiles_2003.tar.bz2",
    "profiles_2004": "https://zenodo.org/records/13345202/files/profiles_2004.tar.bz2",
    "profiles_2005": "https://zenodo.org/records/13345204/files/profiles_2005.tar.bz2",
    "profiles_2006": "https://zenodo.org/records/13345204/files/profiles_2006.tar.bz2",
    "profiles_2007": "https://zenodo.org/records/13345204/files/profiles_2007.tar.bz2",
    "profiles_2008": "https://zenodo.org/records/13345204/files/profiles_2008.tar.bz2",
    "profiles_2009": "https://zenodo.org/records/13345204/files/profiles_2009.tar.bz2",
    "profiles_2010": "https://zenodo.org/records/13345206/files/profiles_2010.tar.bz2",
    "profiles_2011": "https://zenodo.org/records/13345206/files/profiles_2011.tar.bz2",
    "profiles_2012": "https://zenodo.org/records/13345206/files/profiles_2012.tar.bz2",
    "profiles_2013": "https://zenodo.org/records/13345206/files/profiles_2013.tar.bz2",
    "profiles_2014": "https://zenodo.org/records/13345206/files/profiles_2014.tar.bz2",
    "profiles_2015": "https://zenodo.org/records/13345210/files/profiles_2015.tar.bz2",
    "profiles_2016": "https://zenodo.org/records/13345210/files/profiles_2016.tar.bz2",
    "profiles_2017": "https://zenodo.org/records/13345210/files/profiles_2017.tar.bz2",
    "profiles_2018": "https://zenodo.org/records/13345210/files/profiles_2018.tar.bz2",
    "profiles_2019": "https://zenodo.org/records/13345210/files/profiles_2019.tar.bz2",
    "profiles_2020": "https://zenodo.org/records/13345214/files/profiles_2020.tar.bz2",
    "profiles_2021": "https://zenodo.org/records/13345214/files/profiles_2021.tar.bz2",
    "profiles_2022": "https://zenodo.org/records/13345214/files/profiles_2022.tar.bz2",
}

# Explicit UMass mirror links (same filenames, different base URL)
UMASS_DOWNLOAD_LINKS: Dict[str, str] = {
    "5min": "https://doppler.cs.umass.edu/darkecodata/1.0.0/5min.tar.bz2",
    "daily": "https://doppler.cs.umass.edu/darkecodata/1.0.0/daily.tar.bz2",
    "scans": "https://doppler.cs.umass.edu/darkecodata/1.0.0/scans.tar.bz2",
    "profiles_1995": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_1995.tar.bz2",
    "profiles_1996": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_1996.tar.bz2",
    "profiles_1997": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_1997.tar.bz2",
    "profiles_1998": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_1998.tar.bz2",
    "profiles_1999": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_1999.tar.bz2",
    "profiles_2000": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2000.tar.bz2",
    "profiles_2001": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2001.tar.bz2",
    "profiles_2002": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2002.tar.bz2",
    "profiles_2003": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2003.tar.bz2",
    "profiles_2004": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2004.tar.bz2",
    "profiles_2005": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2005.tar.bz2",
    "profiles_2006": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2006.tar.bz2",
    "profiles_2007": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2007.tar.bz2",
    "profiles_2008": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2008.tar.bz2",
    "profiles_2009": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2009.tar.bz2",
    "profiles_2010": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2010.tar.bz2",
    "profiles_2011": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2011.tar.bz2",
    "profiles_2012": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2012.tar.bz2",
    "profiles_2013": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2013.tar.bz2",
    "profiles_2014": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2014.tar.bz2",
    "profiles_2015": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2015.tar.bz2",
    "profiles_2016": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2016.tar.bz2",
    "profiles_2017": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2017.tar.bz2",
    "profiles_2018": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2018.tar.bz2",
    "profiles_2019": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2019.tar.bz2",
    "profiles_2020": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2020.tar.bz2",
    "profiles_2021": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2021.tar.bz2",
    "profiles_2022": "https://doppler.cs.umass.edu/darkecodata/1.0.0/profiles_2022.tar.bz2",
}


def download(url: str, target: Path, force: bool = False) -> Path:
    """Download a URL to target with progress bar.

    Download into a temporary ".part" file and atomically rename to the final
    filename only after the download completes successfully. Partial files are
    removed on error/interrupt so only fully downloaded files remain.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and not force:
        print(f"Skipping download for {target}: file exists")
        return target

    tmp = target.with_suffix(target.suffix + ".part")
    # remove stale tmp if present
    if tmp.exists():
        try:
            tmp.unlink()
        except Exception:
            pass

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        cl = r.headers.get("content-length")
        try:
            total = int(cl) if cl is not None else None
        except Exception:
            total = None

        if total:
            print(f"Downloading {url} -> {target} ({total} bytes)")
        else:
            print(f"Downloading {url} -> {target} (size unknown)")

        try:
            with open(tmp, "wb") as fh, tqdm(total=total, unit="B", unit_scale=True, desc=target.name) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
                        pbar.update(len(chunk))
        except KeyboardInterrupt:
            # cleanup partial file
            try:
                tmp.unlink()
            except Exception:
                pass
            raise
        except Exception:
            try:
                tmp.unlink()
            except Exception:
                pass
            raise

    # atomic move into place
    try:
        os.replace(str(tmp), str(target))
    except Exception:
        # fallback
        try:
            if target.exists():
                target.unlink()
            tmp.replace(target)
        except Exception as e:
            try:
                tmp.unlink()
            except Exception:
                pass
            raise RuntimeError(f"Failed to move downloaded file into place: {e}") from e

    return target


def safe_extract_tarball(tar_path: Path, dest_dir: Path, force: bool = False) -> None:
    print(f"Extracting {tar_path} -> {dest_dir}")

    # Prefer system 'tar' for extraction if available for speed. Output streamed to terminal.
    tar_cmd = shutil.which("tar")
    if tar_cmd:
        dest_dir.mkdir(parents=True, exist_ok=True)
        try:
            flags = "xjvf"
            if not force:
                flags += "k"
            subprocess.run([tar_cmd] + [flags] + [str(tar_path), "-C", str(dest_dir)], check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"'tar' extraction failed: {e}") from e
        return

    # Fallback: pure-Python extraction
    with tarfile.open(tar_path, "r:bz2") as tf:
        members = tf.getmembers()
        # basic safety: prevent absolute paths and path traversal
        for member in members:
            member_path = dest_dir.joinpath(member.name)
            if not str(member_path.resolve()).startswith(str(dest_dir.resolve())):
                raise RuntimeError(f"Unsafe path in tarball: {member.name}")

        dest_dir.mkdir(parents=True, exist_ok=True)
        for member in members:
            member_path = dest_dir.joinpath(member.name)
            if member_path.exists() and not force:
                continue
            print(member.name)
            tf.extract(member, path=dest_dir)


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download and optionally extract Dark Ecology dataset archives")
    p.add_argument("--out", default="data", help="Output directory (default: data)")
    p.add_argument("--all", action="store_true", help="Download all known items (may be very large)")
    # convenience flags
    p.add_argument("--profiles", type=str, help="Comma-separated years and ranges to download, e.g. '2012' or '2012,2014,2016' or '2012-2022'")
    p.add_argument("--scans", action="store_true", help="Download scans archive")
    p.add_argument("--5min", dest="five_min", action="store_true", help="Download 5-minute archive")
    p.add_argument("--daily", action="store_true", help="Download daily archive")
    p.set_defaults(extract=True)
    p.add_argument("--no-extract", action="store_false", dest="extract", help="Don't extract .tar.bz2 archives after download")
    p.add_argument("--delete-archives", action="store_true", help="Delete .tar.bz2 archives after extraction (default: keep)")
    p.add_argument("--dry-run", action="store_true", help="Show which URLs would be downloaded and exit (no network activity)")
    p.add_argument("--force", action="store_true", help="Overwrite existing files.")
    p.add_argument("--mirror", choices=["zenodo", "umass"], default="zenodo", help="Which mirror to download from (default: zenodo)")
    # --urls removed; use the convenience flags or --all
    return p.parse_args(argv)


def parse_profiles_arg(arg: str) -> List[int]:
    """Parse profile years specification like '2012', '2012,2014', or '2012-2022'.

    Returns a sorted list of unique years (ints). Raises ValueError on bad input.
    """
    years = set()
    parts = [p.strip() for p in arg.split(",") if p.strip()]
    for part in parts:
        if "-" in part:
            lo_hi = part.split("-")
            if len(lo_hi) != 2:
                raise ValueError(f"Bad range: {part}")
            lo = int(lo_hi[0])
            hi = int(lo_hi[1])
            if lo > hi:
                raise ValueError(f"Bad range (lo > hi): {part}")
            for y in range(lo, hi + 1):
                years.add(y)
        else:
            years.add(int(part))
    return sorted(years)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    to_download: List[str] = []  # URLs to download

    # pick mirror map
    link_map = DOWNLOAD_LINKS if args.mirror == "zenodo" else UMASS_DOWNLOAD_LINKS

    if args.all:
        for v in link_map.values():
            to_download.append(v)

    # assemble list of things to download
    if getattr(args, "five_min", False):
        to_download.append(link_map["5min"])
    if getattr(args, "scans", False):
        to_download.append(link_map["scans"])
    if getattr(args, "daily", False):
        to_download.append(link_map["daily"])
    if args.profiles:
        try:
            years = parse_profiles_arg(args.profiles)
        except ValueError as e:
            print(f"Bad --profiles argument: {e}")
            return 2
        for y in years:
            key = f"profiles_{y}"
            if key in link_map:
                to_download.append(link_map[key])
            else:
                print(f"No known archive for year {y}; check README for exact Zenodo URL")

    if not to_download:
        print("No download items specified. Use --profiles, --scans, --5min, --daily or --all. See --help for details.")
        return 2

    if args.dry_run:
        print("Dry run: the following URLs would be downloaded:")
        for url in to_download:
            print(url)
        return 0

    # perform downloads
    for url in to_download:
        # rewrite URL for selected mirror if necessary
        if args.mirror == "umass":
            fname = url.split("/")[-1]
            url = f"https://doppler.cs.umass.edu/darkecodata/1.0.0/{fname}"
        filename = url.split("/")[-1]
        target = out_dir.joinpath(filename)
        try:
            download(url, target, force=args.force)
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            continue

        if args.extract and filename.endswith(".tar.bz2"):
            # extract into the directory that contains the downloaded archive
            try:
                safe_extract_tarball(target, out_dir, force=args.force)
                # By default, remove archive after successful extraction unless user asks to keep it
                if args.delete_archives:
                    try:
                        target.unlink()
                        print(f"Removed {target}")
                    except Exception:
                        print(f"Warning: failed to remove {target}")
            except Exception as e:
                print(f"Failed to extract {target}: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
