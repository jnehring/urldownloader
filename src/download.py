"""URL Downloader & Cleaner CLI utility.

Reads URLs from an XLSX file, downloads HTML with a persistent SQLite cache,
extracts clean text using jusText, and writes results back to XLSX.
"""

from __future__ import annotations

import argparse
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import justext
import pandas as pd
import requests
from tqdm import tqdm


DEFAULT_TIMEOUT_SECONDS = 20


class HtmlCache:
    """Thread-safe SQLite-backed cache for downloaded HTML pages."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = str(db_path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS html_cache (
                    url TEXT PRIMARY KEY,
                    html TEXT NOT NULL,
                    status_code INTEGER,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.commit()

    def get(self, url: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT html FROM html_cache WHERE url = ?",
                (url,),
            ).fetchone()
        return row[0] if row else None

    def set(self, url: str, html: str, status_code: Optional[int] = None) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO html_cache (url, html, status_code, fetched_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(url) DO UPDATE SET
                    html = excluded.html,
                    status_code = excluded.status_code,
                    fetched_at = CURRENT_TIMESTAMP
                """,
                (url, html, status_code),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def extract_clean_text(html: str, stoplist_language: str = "English") -> str:
    """Extract non-boilerplate text blocks from HTML using jusText."""
    paragraphs = justext.justext(html, justext.get_stoplist(stoplist_language))
    cleaned = [p.text.strip() for p in paragraphs if not p.is_boilerplate and p.text.strip()]
    return "\n".join(cleaned)


def download_html(url: str, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> tuple[str, int]:
    """Download a URL and return (HTML, status_code)."""
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.text, response.status_code


def process_url(
    url: str,
    cache: HtmlCache,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    stoplist_language: str = "English",
) -> str:
    """Get clean text for a URL using cache first, then network fallback.

    Returns an empty string for any download or extraction failure.
    """
    if not isinstance(url, str) or not url.strip():
        return ""

    normalized_url = url.strip()

    cached_html = cache.get(normalized_url)
    html = cached_html
    if html is None:
        try:
            html, status_code = download_html(normalized_url, timeout_seconds=timeout_seconds)
            cache.set(normalized_url, html, status_code=status_code)
        except requests.RequestException:
            return ""

    try:
        return extract_clean_text(html, stoplist_language=stoplist_language)
    except Exception:
        return ""


def process_dataframe(
    df: pd.DataFrame,
    url_col: str,
    text_col: str,
    parallel_workers: int,
    cache: HtmlCache,
    limit: int = -1,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    stoplist_language: str = "English",
) -> pd.DataFrame:
    """Process URLs in a DataFrame and return a new DataFrame with text column."""
    if url_col not in df.columns:
        raise ValueError(f"URL column '{url_col}' not found in input file.")
    if parallel_workers < 1:
        raise ValueError("--parallel must be >= 1.")
    if limit < -1:
        raise ValueError("--limit must be -1 (all rows), 0, or a positive integer.")

    working_df = df if limit <= 0 else df.head(limit)

    urls = working_df[url_col].tolist()
    results = [""] * len(urls)

    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        future_to_index = {
            executor.submit(
                process_url,
                url,
                cache,
                timeout_seconds,
                stoplist_language,
            ): idx
            for idx, url in enumerate(urls)
        }
        with tqdm(
            total=len(future_to_index),
            desc="Processing URLs",
            unit="url",
            bar_format=(
                "{l_bar}{bar}| {n_fmt}/{total_fmt} "
                "[elapsed: {elapsed} < eta: {remaining}, {rate_inv_fmt}]"
            ),
        ) as progress:
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result()
                except Exception:
                    # Requirement: leave text empty if processing fails.
                    results[idx] = ""
                progress.update(1)

    output_df = working_df.copy()
    output_df[text_col] = results
    return output_df


def run_single_url_debug(
    test_url: str,
    cache: HtmlCache,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    stoplist_language: str = "English",
) -> int:
    """Run debug mode: process one URL and print extracted text to stdout."""
    text = process_url(
        test_url,
        cache=cache,
        timeout_seconds=timeout_seconds,
        stoplist_language=stoplist_language,
    )
    print(text)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download URLs and extract clean text.")
    parser.add_argument("-i", "--input", help="Path to source XLSX file.")
    parser.add_argument(
        "-o",
        "--output",
        default="output.xlsx",
        help="Path for resulting XLSX file.",
    )
    parser.add_argument(
        "-u",
        "--url-col",
        default="url",
        help="Column name that contains URLs.",
    )
    parser.add_argument(
        "-t",
        "--text-col",
        default="text",
        help="Output column name for cleaned text.",
    )
    parser.add_argument(
        "-p",
        "--parallel",
        type=int,
        default=4,
        help="Number of concurrent download workers.",
    )
    parser.add_argument(
        "-d",
        "--test-url",
        help="Download a single URL and print extracted text.",
    )
    parser.add_argument(
        "--cache-db",
        default="cache.sqlite",
        help="Path to SQLite cache database.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--stoplist-language",
        "--stoplist_language",
        default="English",
        help="jusText stoplist language.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Process only the first N rows. Values <= 0 process all rows.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cache = HtmlCache(args.cache_db)
    try:
        if args.test_url:
            return run_single_url_debug(
                args.test_url,
                cache=cache,
                timeout_seconds=args.timeout,
                stoplist_language=args.stoplist_language,
            )

        if not args.input:
            raise ValueError("--input is required unless --test-url is provided.")

        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        df = pd.read_excel(input_path, sheet_name=0)
        output_df = process_dataframe(
            df=df,
            url_col=args.url_col,
            text_col=args.text_col,
            parallel_workers=args.parallel,
            cache=cache,
            limit=args.limit,
            timeout_seconds=args.timeout,
            stoplist_language=args.stoplist_language,
        )
        output_df.to_excel(args.output, index=False)
        return 0
    finally:
        cache.close()


if __name__ == "__main__":
    raise SystemExit(main())
