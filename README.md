# URL Downloader & Cleaner

Python CLI utility to read URLs from an Excel file, download/cached HTML, clean content with jusText, and write results back to Excel.

## Requirements

Python 3.9+

```bash
pip install -r requirements.txt
```

## Usage

Run from the repository root:

```bash
python src/download.py -i data.xlsx
```

### CLI Parameters

- `--input` / `-i`: Path to input XLSX (required unless `--test-url` is used).
- `--output` / `-o`: Output XLSX path (default: `output.xlsx`).
- `--url-col` / `-u`: URL column name in first sheet (default: `url`).
- `--text-col` / `-t`: Output column for cleaned text (default: `text`).
- `--parallel` / `-p`: Number of parallel workers (default: `4`).
- `--limit`: Process only first N rows from input and write only N rows to output when `N > 0` (default: `-1`, meaning all rows).
- `--test-url` / `-d`: Debug mode for processing one URL only.
- `--cache-db`: SQLite cache path (default: `cache.sqlite`).
- `--timeout`: Request timeout in seconds (default: `20`).
- `--stoplist-language` / `--stoplist_language`: jusText stoplist language (default: `English`).

## Parallel Processing

Increase throughput by setting `--parallel`:

```bash
python src/download.py -i data.xlsx -p 10 -o processed_data.xlsx
```

The progress bar shows:
- processed URLs / total
- ETA
- speed in seconds per item

## Debug Mode

Process one URL and print the extracted text to stdout:

```bash
python src/download.py --test-url "https://example.com/article"
```

This mode still uses the same SQLite cache.

## Tests

Run the test suite:

```bash
pytest -q
```
