import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from pyodibel.cache import RocksDbCache
from resources.text2kgbench.crawl import crawl_items, load_items


WIKI_PATTERN = (
    "https://en.wikipedia.org/w/api.php?format=xml&action=query"
    "&prop=extracts&exintro=&explaintext=&titles={item}"
)


class TestCrawlItems:
    def test_cache_hit_on_second_run(self):
        items = ["Example_Item"]
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/xml"}
        mock_response.content = b"<api><extract>Example text</extract></api>"

        with tempfile.TemporaryDirectory() as cache_dir:
            with patch("resources.text2kgbench.crawl.requests.get", return_value=mock_response) as get:
                first = crawl_items(items, WIKI_PATTERN, cache_dir)
                second = crawl_items(items, WIKI_PATTERN, cache_dir)

            assert first.fetched == 1
            assert first.cached == 0
            assert second.cached == 1
            assert second.fetched == 0
            assert get.call_count == 1

    def test_force_refetches(self):
        items = ["Example_Item"]
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/xml"}
        mock_response.content = b"<api><extract>Example text</extract></api>"

        with tempfile.TemporaryDirectory() as cache_dir:
            with patch("resources.text2kgbench.crawl.requests.get", return_value=mock_response) as get:
                crawl_items(items, WIKI_PATTERN, cache_dir)
                crawl_items(items, WIKI_PATTERN, cache_dir, force=True)

            assert get.call_count == 2

    def test_export_writes_files(self):
        items = ["Example_Item"]
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/xml"}
        mock_response.content = b"<api><extract>Example text</extract></api>"

        with tempfile.TemporaryDirectory() as cache_dir, tempfile.TemporaryDirectory() as export_dir:
            with patch("resources.text2kgbench.crawl.requests.get", return_value=mock_response):
                report = crawl_items(
                    items,
                    WIKI_PATTERN,
                    cache_dir,
                    export_dir=export_dir,
                )

            assert report.fetched == 1
            assert report.items[0].export_path is not None
            export_path = report.items[0].export_path
            assert export_path.endswith(".xml")
            assert Path(export_path).read_bytes() == mock_response.content
            meta_files = list(Path(export_dir).glob("*.meta.json"))
            assert len(meta_files) == 1
            meta = json.loads(meta_files[0].read_text(encoding="utf-8"))
            assert meta["item"] == "Example_Item"

    def test_failed_request(self):
        items = ["Broken_Item"]
        with tempfile.TemporaryDirectory() as cache_dir:
            with patch(
                "resources.text2kgbench.crawl.requests.get",
                side_effect=requests.RequestException("network down"),
            ):
                report = crawl_items(items, WIKI_PATTERN, cache_dir)

        assert report.failed == 1
        assert report.items[0].error == "network down"


class TestLoadItems:
    def test_skips_comments_and_blanks(self, tmp_path):
        path = tmp_path / "items.txt"
        path.write_text("# comment\n\nAlpha\n  \nBeta\n", encoding="utf-8")
        assert load_items(str(path)) == ["Alpha", "Beta"]


@pytest.mark.network
def test_wikipedia_live_fetch():
    from resources.text2kgbench.crawl import resolve_url

    item = "Titanic_(1997_film)"
    url = resolve_url(WIKI_PATTERN, item)
    with tempfile.TemporaryDirectory() as cache_dir:
        try:
            report = crawl_items([item], WIKI_PATTERN, cache_dir, timeout=15.0)
        except requests.RequestException:
            pytest.skip("network unavailable")

        if report.failed:
            pytest.skip("Wikipedia request failed (network or rate limit)")

        with RocksDbCache(cache_dir) as store:
            from pyodibel.cache import HttpFetchCache

            cached = HttpFetchCache(store).get(url)

        assert cached is not None
        assert b"Titanic is a 1997" in cached.body
