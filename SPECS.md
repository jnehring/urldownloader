# Software Specification: URL Downloader & Cleaner (download.py)

## 1\. Overview

The **URL Downloader & Cleaner** is a Python-based CLI utility that extracts clean text from a list of web addresses provided in an Excel file. It features a persistent SQLite cache to minimize redundant network traffic and uses jusText for boilerplate removal. This version includes advanced features for high-performance parallel downloading and real-time progress monitoring.

## 2\. Functional Requirements

* **File Input & Processing:**  
  * The tool must read the first sheet of a provided .xlsx file.  
  * It must identify the URL column (default "url") and allow configuration via CLI.  
* **Persistent Caching:**  
  * All HTML content must be stored in a .sqlite database.  
  * The system must check the cache before initiating any network request.  
* **Parallel Execution:**  
  * The system must support parallel URL downloads to increase throughput.  
* **Progress Monitoring:**  
  * The CLI must display a dynamic progress bar.  
  * The bar must show:  
    * The number of URLs processed vs. the total.  
    * The estimated time remaining (ETA).  
    * The processing speed (seconds per download).  
* **Error Handling:**  
  * The system must be resilient to network or parsing failures.  
  * If a URL fails to download or process, the corresponding "text" column entry must be left empty rather than crashing the application.  
* **Extraction & Output:**  
  * Content must be stripped of boilerplate using jusText.  
  * The output .xlsx must contain all original data plus a new "text" column (configurable name).  
* **Debug Mode:**  
  * The system must include a dedicated mode to download and process a **single URL** to verify jusText extraction results without processing a full file.

## 3\. CLI Parameters & Example Calls

| Parameter | Short | Description | Default |
| :---- | :---- | :---- | :---- |
| \--input | \-i | Path to the source XLSX file. | (Required) |
| \--output | \-o | Path for the resulting XLSX file. | output.xlsx |
| \--url-col | \-u | Name of the column containing URLs. | url |
| \--text-col | \-t | Name of the new column for cleaned text. | text |
| \--parallel | \-p | Number of concurrent download workers. | 4 |
| \--test-url | \-d | Download a single URL and print text to console. | N/A |

#### **Example Usage**

**Standard Production Run:**

Running with 10 parallel threads and a custom output name.

Bash  
python download.py \-i data.xlsx \-p 10 \-o processed\_data.xlsx

**Single URL Debugging:**

Testing how jusText handles a specific page without needing an Excel file.

Bash  
python download.py \--test-url "https://example.com/article"

**Custom Column Mapping:**

Bash  
python download.py \-i list.xlsx \-u "Source\_URL" \-t "Main\_Body\_Text"

## 4\. Technical Stack

* **Language:** Python 3.x  
* **Core Libraries:** pandas, openpyxl, requests, jusText, sqlite3.  
* **Concurrency:** concurrent.futures (ThreadPoolExecutor).  
* **UI/Progress:** tqdm (for the progress bar and ETA logic).  
* **Testing:** pytest.

## 5\. Documentation & Quality Assurance

* **README.md:** Must document parallel settings and debug mode.  
* **Unit Tests:** pytest suites must verify the SQLite cache integrity, the parallel worker logic, and the error handling for 404/500 HTTP errors.

