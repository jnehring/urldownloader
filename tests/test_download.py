from __future__ import annotations

import threading
from pathlib import Path
import sys

import pandas as pd
import pytest
import requests

from src import download


def test_sqlite_cache_integrity(tmp_path):
    cache_path = tmp_path / "cache.sqlite"
    cache = download.HtmlCache(cache_path)
    try:
        cache.set("https://example.com", "<html>first</html>", status_code=200)
        assert cache.get("https://example.com") == "<html>first</html>"

        # Verify upsert behavior keeps data current.
        cache.set("https://example.com", "<html>updated</html>", status_code=200)
        assert cache.get("https://example.com") == "<html>updated</html>"
    finally:
        cache.close()


def test_parallel_worker_logic(monkeypatch, tmp_path):
    cache = download.HtmlCache(tmp_path / "cache.sqlite")
    try:
        seen_threads = set()

        def fake_process_url(url, cache, timeout_seconds=20, stoplist_language="English"):
            seen_threads.add(threading.get_ident())
            return f"cleaned:{url}"

        monkeypatch.setattr(download, "process_url", fake_process_url)

        df = pd.DataFrame({"url": [f"https://example.com/{i}" for i in range(12)]})
        out = download.process_dataframe(
            df=df,
            url_col="url",
            text_col="text",
            parallel_workers=4,
            cache=cache,
        )

        assert len(out) == 12
        assert out["text"].str.startswith("cleaned:https://example.com/").all()
        # More than one thread should be used for a meaningful parallel run.
        assert len(seen_threads) > 1
    finally:
        cache.close()


def test_limit_processes_only_first_n_rows(monkeypatch, tmp_path):
    cache = download.HtmlCache(tmp_path / "cache.sqlite")
    try:
        def fake_process_url(url, cache, timeout_seconds=20, stoplist_language="English"):
            return f"cleaned:{url}"

        monkeypatch.setattr(download, "process_url", fake_process_url)

        df = pd.DataFrame({"url": [f"https://example.com/{i}" for i in range(10)]})
        out = download.process_dataframe(
            df=df,
            url_col="url",
            text_col="text",
            parallel_workers=2,
            cache=cache,
            limit=3,
        )

        assert len(out) == 3
        assert out["url"].tolist() == [
            "https://example.com/0",
            "https://example.com/1",
            "https://example.com/2",
        ]
        assert out["text"].tolist() == [
            "cleaned:https://example.com/0",
            "cleaned:https://example.com/1",
            "cleaned:https://example.com/2",
        ]
    finally:
        cache.close()


def test_download_and_parse_html_fixture(monkeypatch, tmp_path):
    cache = download.HtmlCache(tmp_path / "cache.sqlite")
    fixture_path = Path(__file__).parent / "html" / "1.html"
    fixture_html = fixture_path.read_text(encoding="utf-8")

    class DummyResponse:
        status_code = 200

        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    try:
        def fake_get(url, timeout=20):
            assert url == "https://example.com/article"
            return DummyResponse(fixture_html)

        monkeypatch.setattr(download.requests, "get", fake_get)

        text = download.process_url(
            "https://example.com/article",
            cache=cache,
            stoplist_language="Spanish",
        )

        assert isinstance(text, str)
        assert len(text) > 200
        assert "Karina García" in text
    finally:
        cache.close()


def test_cli_accepts_stoplist_language_underscore_alias(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["download.py", "--stoplist_language", "Spanish", "--test-url", "https://example.com"],
    )
    args = download.parse_args()
    assert args.stoplist_language == "Spanish"


@pytest.mark.parametrize("status_code", [404, 500])
def test_http_error_handling_leaves_empty_text(monkeypatch, tmp_path, status_code):
    cache = download.HtmlCache(tmp_path / "cache.sqlite")
    try:
        def fake_download_html(url, timeout_seconds=20):
            response = requests.Response()
            response.status_code = status_code
            http_error = requests.HTTPError(f"{status_code} error")
            http_error.response = response
            raise http_error

        monkeypatch.setattr(download, "download_html", fake_download_html)

        # Failing URL should not crash and should return an empty text string.
        text = download.process_url("https://example.com/fail", cache=cache)
        assert text == ""
    finally:
        cache.close()
