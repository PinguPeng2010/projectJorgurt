# `jorgurt`

A Python web-crawling and URL indexing project built around a shared SQLite database.

**Gurt** crawls large URL sets efficiently and stores crawl state and discovered data in SQLite. **Jorge** consumes that shared crawl data and builds a searchable index from it.

The project is designed to run for long periods, distribute crawler work across multiple processes, and keep a persistent queue of discovered URLs in `crawler.db`.

## Components

### Gurt — crawler

Gurt is the multiprocessing crawler. It:

- crawls pages from seed URLs
- discovers URLs and stores crawl data as it works
- keeps per-domain and global crawl state in SQLite
- distributes work across multiple processes and worker threads
- logs crawl activity and per-process stats
- exposes a live terminal monitor for throughput and queue activity
- can rebalance queued work across processes based on current load

### Jorge — indexing and search

Jorge is the URL indexing and search component. It consumes data already stored by Gurt in the shared SQLite database, turns it into searchable index data, and provides the basis for searching crawled URLs and page content.

Jorge does not need to repeat Gurt's discovery work: Gurt can write relevant crawl data to the appropriate shared database tables as it crawls.

## Architecture

```text
Seed URLs
    │
    ▼
┌─────────┐
│  Gurt   │  discovers URLs and stores crawl data
│ crawler │
└────┬────┘
     │
     ▼
┌─────────────────────┐
│ Shared SQLite DB    │
│ crawler.db          │
│ - crawl state       │
│ - discovered URLs   │
│ - crawl data        │
│ - index data        │
└────┬────────────────┘
     │
     ▼
┌─────────┐
│  Jorge  │  consumes and indexes crawl data
│ indexer │
└────┬────┘
     │
     ▼
Searchable URL and page index
```

Gurt and Jorge share the database, but their responsibilities remain separate:

- **Gurt** owns URL discovery, crawl queue state, and collection of crawl data.
- **Jorge** owns consuming that data and creating or updating searchable index data.
- Both components should use clearly defined database tables and state transitions to avoid conflicting writes.

## Features

- SQLite-backed URL tracking, crawl state, and indexing data
- process-based crawler concurrency with thread workers
- shared database pipeline between crawling and indexing
- live Rich-based monitoring panel
- automatic seed insertion from a folder of seed files
- rate and status logging for crawl health
- optional watch scripts to monitor database growth over time

> jorgurt is intended for long-running crawling and indexing jobs. It is best suited to a dedicated server or VM with enough RAM and stable storage.

## Requirements

Install the project dependencies:

```bash
python -m pip install -r requirements.txt
```

The dependencies in `requirements.txt` include:

- `httpx`
- `bs4`
- `rich`
- `windows-curses`

## Project layout

```text
jorgurt/
├── gurt.py              # Gurt crawler entry point
├── crawl.py             # crawling logic and queue management
├── jorge.py             # Jorge indexing/search entry point
├── jorge/               # Jorge indexing and search components
├── seeds/               # seed URL files
├── shell/               # monitoring scripts
├── logs/                # runtime logs
├── crawler.db           # shared SQLite database created at runtime
├── requirements.txt
├── README.md
└── LICENSE
```

## Running the crawler

The Gurt crawler script is:

```bash
python gurt.py [options] procs workers
```

### Positional arguments

- `procs`: number of processes to run
- `workers`: number of worker operations per process

### Options

```bash
gurt.py [-h] [-s FOLDER] [-f NUM] [-q NUM] [-m] [-d NUM] procs workers
```

- `-h`, `--help`: show help
- `-s`, `--seeds FOLDER`: folder containing seed files. Defaults to `seeds/`
- `-f`, `--fetch NUM`: number of URLs to fetch from the database at a time. Default: `200`
- `-q`, `--queue NUM`: size of the in-memory URL queue. Default: `50000`
- `-m`, `--monitor`: show the live monitor in the terminal
- `-d`, `--delta NUM`: load-balancing tolerance around the mean queue size. Default: `5000`

## Seed files

The crawler expects a directory of seed files. Each file is treated as a seed pack and assigned to a processing unit.

Example:

```text
seeds/
├── seed_pack_1.txt
├── seed_pack_2.txt
├── seed_pack_3.txt
└── seed_pack_4.txt
```

Each file should contain one URL per line:

```text
https://example.com
https://example.org
https://docs.example.com
```

> Make sure the number of seed files is at least as large as the number of processes you plan to run.

## Example

```bash
python gurt.py -s seeds -f 200 -q 50000 -m 4 4 8
```

This starts:

- 4 processes
- 4 worker operations per process
- 8 crawlers
- a live monitor
- seed files from the `seeds/` folder

## Jorge indexing

Jorge reads the crawl data that Gurt has already stored in `crawler.db` and uses it to build searchable index data.

This keeps the pipeline efficient:

```text
Gurt discovers and stores data
            ↓
Jorge reads and indexes it
            ↓
Search uses Jorge's indexed data
```

Jorge-related database tables should be treated as part of the shared database contract. Gurt may populate relevant data while crawling, and Jorge should consume and index it without duplicating URL discovery.

## Monitoring

The project includes a simple monitor script to watch how the database grows over time.

### Linux/macOS

```bash
bash shell/watchCrawler.sh
```

### Windows

```bash
shell\watchCrawler.bat
```

The script polls the SQLite database every 2 seconds and prints the current URL count and the growth rate.

## Logging

The crawler writes logs to:

- `logs/crawler.log`
- `logs/error.log`

These are useful for checking crawl progress, failures, and unexpected issues.

## Important notes

- Respect `robots.txt` and site terms of service.
- Long-running crawls can consume significant disk space, memory, and network bandwidth.
- A dedicated host or VPS is recommended for sustained crawling.
- Current stable is v1.3.0.

## License

See the repository's license file for usage terms. Uses the Mozilla Public License 2.0.