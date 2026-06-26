#!/usr/bin/env python3
"""
Crawl web resources for Text2KGBench source acquisition.

Fetches items through a URL pattern, caches responses in RocksDB, and optionally
exports bodies to files for text-source bundling.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import requests
from tqdm import tqdm

from pyodibel.cache import CachedHttpResponse, HttpFetchCache, RocksDbCache

CrawlStatus = Literal["cached", "fetched", "failed"]


@dataclass
class CrawlItemResult:
    item: str
    url: str
    status: CrawlStatus
    status_code: int | None = None
    content_type: str | None = None
    export_path: str | None = None
    error: str | None = None


@dataclass
class CrawlReport:
    items: list[CrawlItemResult] = field(default_factory=list)

    @property
    def cached(self) -> int:
        return sum(1 for item in self.items if item.status == "cached")

    @property
    def fetched(self) -> int:
        return sum(1 for item in self.items if item.status == "fetched")

    @property
    def failed(self) -> int:
        return sum(1 for item in self.items if item.status == "failed")


def resolve_url(pattern: str, item: str) -> str:
    encoded = quote(item, safe="")
    return pattern.format(item=item, title=item, uri=item, encoded=encoded)


def _hash_item(item: str) -> str:
    return hashlib.md5(item.encode("utf-8")).hexdigest()


def _extension_for_content_type(content_type: str) -> str:
    normalized = content_type.split(";", 1)[0].strip().lower()
    mapping = {
        "text/plain": "txt",
        "text/html": "html",
        "application/xml": "xml",
        "text/xml": "xml",
        "application/json": "json",
    }
    return mapping.get(normalized, "bin")


def _export_response(
    item: str,
    response: CachedHttpResponse,
    export_dir: Path,
) -> Path:
    export_dir.mkdir(parents=True, exist_ok=True)
    stem = _hash_item(item)
    ext = _extension_for_content_type(response.content_type)
    body_path = export_dir / f"{stem}.{ext}"
    meta_path = export_dir / f"{stem}.meta.json"

    body_path.write_bytes(response.body)
    meta_path.write_text(
        json.dumps(
            {
                "item": item,
                "url": response.url,
                "content_type": response.content_type,
                "fetched_at": response.fetched_at,
                "body_path": str(body_path),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return body_path


def _fetch_url(
    url: str,
    *,
    timeout: float,
    user_agent: str,
) -> CachedHttpResponse:
    http_response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": user_agent},
    )
    return HttpFetchCache.build_response(
        url,
        status_code=http_response.status_code,
        content_type=http_response.headers.get("Content-Type", ""),
        body=http_response.content,
    )


def load_items(items_path: str) -> list[str]:
    if items_path == "-":
        lines = sys.stdin.read().splitlines()
    else:
        lines = Path(items_path).read_text(encoding="utf-8").splitlines()

    items: list[str] = []
    for line in lines:
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        items.append(value)
    if not items:
        raise ValueError(f"No items found in {items_path}")
    return items


def crawl_items(
    items: list[str],
    pattern: str,
    cache_dir: str,
    *,
    export_dir: str | None = None,
    force: bool = False,
    timeout: float = 30.0,
    delay: float = 0.0,
    user_agent: str = "ODIBEL-Text2KGBench/0.1",
) -> CrawlReport:
    report = CrawlReport()
    export_root = Path(export_dir) if export_dir else None

    with RocksDbCache(cache_dir) as store:
        cache = HttpFetchCache(store)
        for item in tqdm(items, desc="Crawl"):
            url = resolve_url(pattern, item)
            result = CrawlItemResult(item=item, url=url, status="failed")

            try:
                response: CachedHttpResponse | None = None
                if not force:
                    response = cache.get(url)

                if response is not None:
                    result.status = "cached"
                    result.status_code = response.status_code
                    result.content_type = response.content_type
                else:
                    response = _fetch_url(url, timeout=timeout, user_agent=user_agent)
                    if response.is_cacheable():
                        cache.put(response)
                        result.status = "fetched"
                    else:
                        result.status = "failed"
                        result.error = f"HTTP {response.status_code}"
                    result.status_code = response.status_code
                    result.content_type = response.content_type
                    if delay > 0:
                        time.sleep(delay)

                if response is not None and response.is_cacheable() and export_root is not None:
                    export_path = _export_response(item, response, export_root)
                    result.export_path = str(export_path)
            except requests.RequestException as exc:
                result.status = "failed"
                result.error = str(exc)

            report.items.append(result)

    return report


def _print_summary(report: CrawlReport) -> None:
    print(f"Items:   {len(report.items)}")
    print(f"Cached:  {report.cached}")
    print(f"Fetched: {report.fetched}")
    print(f"Failed:  {report.failed}")
    if report.failed:
        print()
        print("Failures:")
        for item in report.items:
            if item.status != "failed":
                continue
            detail = item.error or f"HTTP {item.status_code}"
            print(f"  {item.item}: {detail}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch web resources and cache them in RocksDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "items_path",
        nargs="?",
        help="Newline-delimited items file, or '-' for stdin",
    )
    parser.add_argument("--pattern", help="URL template with {item}, {title}, {uri}, or {encoded}")
    parser.add_argument("--cache-dir", help="RocksDB cache directory")
    parser.add_argument("--export-dir", default=None, help="Optional directory for exported bodies")
    parser.add_argument("--force", action="store_true", help="Bypass cache and re-fetch")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--delay", type=float, default=0.0, help="Seconds to wait after each fetch")
    parser.add_argument("--user-agent", default="ODIBEL-Text2KGBench/0.1")
    return parser


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    args = _build_parser().parse_args(argv)

    if not args.items_path or not args.pattern or not args.cache_dir:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env"))

    items_path = args.items_path or os.getenv("ITEMS_PATH")
    pattern = args.pattern or os.getenv("URL_PATTERN")
    cache_dir = args.cache_dir or os.getenv("CACHE_DIR")
    export_dir = args.export_dir or os.getenv("EXPORT_DIR")

    if not items_path:
        raise ValueError("items_path not provided and ITEMS_PATH not set")
    if not pattern:
        raise ValueError("--pattern not provided and URL_PATTERN not set")
    if not cache_dir:
        raise ValueError("--cache-dir not provided and CACHE_DIR not set")

    items = load_items(items_path)
    report = crawl_items(
        items,
        pattern,
        cache_dir,
        export_dir=export_dir,
        force=args.force,
        timeout=args.timeout,
        delay=args.delay,
        user_agent=args.user_agent,
    )
    _print_summary(report)


if __name__ == "__main__":
    main()
