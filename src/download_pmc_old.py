#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download PMC OA (oa_comm/txt) bulk packages listed in an "Index of ..." HTML.

Features:
- Parse local HTML (pmc-oa_comm.html)
- Filter targets: *.tar.gz and *.filelist.(csv|txt)
- Optional filter: baseline / incr
- Parallel downloads (asyncio + aiohttp)
- Retry with exponential backoff
- Resume via HTTP Range if a partial file exists (best-effort)

Base URL default:
  https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/txt/
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import aiohttp


DEFAULT_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/txt/"


# ---------- HTML link extractor ----------
class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        for k, v in attrs:
            if k.lower() == "href" and v:
                self.hrefs.append(v)


def parse_hrefs_from_html(html_path: Path) -> List[str]:
    parser = LinkExtractor()
    text = html_path.read_text(encoding="utf-8", errors="ignore")
    parser.feed(text)
    return parser.hrefs


# ---------- filtering ----------
TAR_RE = re.compile(r"\.tar\.gz$", re.IGNORECASE)
FILELIST_RE = re.compile(r"\.filelist\.(csv|txt)$", re.IGNORECASE)

def is_target(name: str) -> bool:
    return bool(TAR_RE.search(name) or FILELIST_RE.search(name))

def classify_baseline_incr(name: str) -> str:
    # returns "baseline", "incr", or "other"
    if ".baseline." in name:
        return "baseline"
    if ".incr." in name:
        return "incr"
    return "other"

def normalize_filename_from_href(href: str) -> str:
    # href could be "oa_comm_txt....tar.gz" or "/pub/pmc/...."
    return href.split("/")[-1].strip()

def href_to_url(href: str, base_url: str) -> Optional[str]:
    href = href.strip()
    if not href or href.lower().startswith("javascript:"):
        return None
    # skip parent directory and unrelated links
    if "Parent Directory" in href or href.endswith("/pub/pmc/oa_bulk/oa_comm/"):
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    # If absolute path like /pub/pmc/..., attach scheme+host from base_url
    if href.startswith("/"):
        # base_url is like https://ftp.ncbi.nlm.nih.gov/pub/pmc/...
        m = re.match(r"^(https?://[^/]+)", base_url)
        if not m:
            return None
        return m.group(1) + href
    # relative filename
    return base_url.rstrip("/") + "/" + href.lstrip("/")


# ---------- downloader ----------
@dataclass
class Job:
    url: str
    out_path: Path
    kind: str  # baseline / incr / other


async def head_content_length(session: aiohttp.ClientSession, url: str) -> Optional[int]:
    try:
        async with session.head(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status >= 400:
                return None
            cl = r.headers.get("Content-Length")
            return int(cl) if cl and cl.isdigit() else None
    except Exception:
        return None


async def download_one(
    session: aiohttp.ClientSession,
    job: Job,
    chunk_size: int = 1024 * 1024,
    retries: int = 5,
) -> None:
    job.out_path.parent.mkdir(parents=True, exist_ok=True)

    # Resume if possible
    existing = job.out_path.exists()
    existing_size = job.out_path.stat().st_size if existing else 0

    # If already complete (best-effort via HEAD)
    remote_size = await head_content_length(session, job.url)
    if remote_size is not None and existing_size == remote_size and remote_size > 0:
        print(f"[SKIP] {job.out_path.name} (already complete)")
        return

    headers = {}
    mode = "wb"
    if existing_size > 0:
        headers["Range"] = f"bytes={existing_size}-"
        mode = "ab"

    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=60, sock_read=300)
            async with session.get(job.url, headers=headers, timeout=timeout) as r:
                # If server doesn't support Range, it may return 200 instead of 206
                if r.status in (200, 206):
                    # If we asked for Range but got 200, restart from scratch to avoid corruption
                    if "Range" in headers and r.status == 200:
                        print(f"[WARN] {job.out_path.name}: Range not honored; restarting")
                        mode = "wb"
                        headers.pop("Range", None)
                        existing_size = 0

                    total = r.headers.get("Content-Length")
                    total_str = f"{int(total)/1e6:.1f}MB" if total and total.isdigit() else "unknown"
                    print(f"[DL]   {job.out_path.name} ({job.kind}, +{total_str})")

                    with open(job.out_path, mode) as f:
                        async for chunk in r.content.iter_chunked(chunk_size):
                            if chunk:
                                f.write(chunk)
                    return
                else:
                    raise RuntimeError(f"HTTP {r.status}")
        except Exception as e:
            last_err = e
            wait = min(2 ** attempt, 30)
            print(f"[RETRY] {job.out_path.name}: {e} (attempt {attempt+1}/{retries}) -> sleep {wait}s")
            await asyncio.sleep(wait)

    raise RuntimeError(f"Failed: {job.url} ({job.out_path}) last_err={last_err}")


async def run_jobs(jobs: List[Job], workers: int) -> None:
    connector = aiohttp.TCPConnector(limit=workers, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(workers)

        async def sem_task(job: Job) -> None:
            async with sem:
                await download_one(session, job)

        tasks = [asyncio.create_task(sem_task(j)) for j in jobs]
        # gather with better error reporting
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for t in done:
            exc = t.exception()
            if exc:
                for p in pending:
                    p.cancel()
                raise exc
        await asyncio.gather(*pending, return_exceptions=True)


def build_jobs(html_path: Path, outdir: Path, base_url: str, only: str) -> List[Job]:
    hrefs = parse_hrefs_from_html(html_path)
    jobs: List[Job] = []

    for href in hrefs:
        name = normalize_filename_from_href(href)
        if not name or name in ("", "Parent Directory"):
            continue
        if not is_target(name):
            continue

        kind = classify_baseline_incr(name)
        if only != "all":
            if kind != only:
                continue

        url = href_to_url(href, base_url)
        if not url:
            continue

        jobs.append(Job(url=url, out_path=outdir / name, kind=kind))

    # sort stable: filelists first then tar.gz (optional but convenient)
    def key(j: Job) -> Tuple[int, str]:
        is_tar = 1 if j.out_path.name.endswith(".tar.gz") else 0
        return (is_tar, j.out_path.name)

    return sorted(jobs, key=key)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", type=str, required=True, help="local index html, e.g., pmc-oa_comm.html")
    ap.add_argument("--outdir", type=str, required=True, help="output directory")
    ap.add_argument("--base-url", type=str, default=DEFAULT_BASE_URL, help="base URL for relative hrefs")
    ap.add_argument("--workers", type=int, default=16, help="concurrent downloads")
    ap.add_argument("--only", type=str, default="all", choices=["all", "baseline", "incr"],
                    help="download only baseline or incr or all")
    args = ap.parse_args()

    html_path = Path(args.html)
    if not html_path.exists():
        print(f"HTML not found: {html_path}", file=sys.stderr)
        sys.exit(1)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    jobs = build_jobs(html_path, outdir, args.base_url, args.only)
    if not jobs:
        print("No targets found in HTML (tar.gz / filelist.csv / filelist.txt).", file=sys.stderr)
        sys.exit(2)

    print(f"Base URL: {args.base_url}")
    print(f"Targets : {len(jobs)} files")
    print(f"Outdir  : {outdir.resolve()}")
    print(f"Workers : {args.workers}")
    print(f"Only    : {args.only}")
    print("-" * 60)

    try:
        asyncio.run(run_jobs(jobs, args.workers))
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)

    print("\nDone.")


if __name__ == "__main__":
    main()
