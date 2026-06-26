import json
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from pyodibel.cache import CachedHttpResponse, HttpFetchCache, RocksDbCache


class TestRocksDbCache:
    def test_put_get_contains(self):
        with tempfile.TemporaryDirectory() as tmp:
            with RocksDbCache(tmp) as cache:
                cache.put(b"key", b"value")
                assert cache.contains(b"key")
                assert cache.get(b"key") == b"value"
                assert cache.get(b"missing") is None


class TestHttpFetchCache:
    def test_round_trip(self):
        response = CachedHttpResponse(
            url="https://example.org/test",
            status_code=200,
            content_type="text/plain",
            body=b"hello",
            fetched_at=123.0,
        )
        raw = response.to_bytes()
        restored = CachedHttpResponse.from_bytes(raw)
        assert restored == response

    def test_store_and_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            with RocksDbCache(tmp) as store:
                cache = HttpFetchCache(store)
                response = CachedHttpResponse(
                    url="https://example.org/page",
                    status_code=200,
                    content_type="application/xml",
                    body=b"<xml/>",
                    fetched_at=456.0,
                )
                cache.put(response)
                loaded = cache.get("https://example.org/page")
                assert loaded == response

    def test_skips_non_cacheable(self):
        with tempfile.TemporaryDirectory() as tmp:
            with RocksDbCache(tmp) as store:
                cache = HttpFetchCache(store)
                cache.put(
                    CachedHttpResponse(
                        url="https://example.org/missing",
                        status_code=404,
                        content_type="text/plain",
                        body=b"",
                        fetched_at=1.0,
                    )
                )
                assert cache.get("https://example.org/missing") is None


class TestResolveUrl:
    def test_placeholder_substitution(self):
        from resources.text2kgbench.crawl import resolve_url

        pattern = (
            "https://en.wikipedia.org/w/api.php?titles={item}"
            "&encoded={encoded}&title={title}&uri={uri}"
        )
        url = resolve_url(pattern, "Titanic_(1997_film)")
        assert "titles=Titanic_(1997_film)" in url
        assert "title=Titanic_(1997_film)" in url
        assert "uri=Titanic_(1997_film)" in url
        assert "encoded=Titanic_%281997_film%29" in url
