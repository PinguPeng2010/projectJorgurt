# `gurt`

A multiprocessing web crawler built in Python for crawling large URL sets efficiently while storing crawl state in SQLite.

It is designed to run for a long time, distribute work across multiple processes, and keep a persistent queue of discovered URLs in `crawler.db`.

## What it does

- crawls pages from seed URLs
- keeps per-domain and global crawl state in SQLite
- distributes work across multiple processes and worker threads
- logs crawl activity and per-process stats
- exposes a live terminal monitor for throughput and queue activity
- can rebalance queued work across processes based on current load

## Features

- SQLite-backed URL tracking and queue state
- process-based concurrency with thread workers
- live Rich-based monitoring panel
- automatic seed insertion from a folder of seed files
- rate and status logging for crawl health
- optional watch scripts to monitor database growth over time

> projectGurt is intended for long-running crawling jobs. It is best suited to a dedicated server or VM with enough RAM and stable storage.

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
projectGurt/
├── gurt.py              # main entry point
├── crawl.py            # crawling logic and queue management
├── seeds/              # seed URL files
├── shell/              # monitoring scripts
├── logs/               # runtime logs
├── crawler.db          # SQLite database created at runtime
├── requirements.txt
├── README.md
└── LICENSE
```

## Running the crawler

The main script is:

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

- Respect robots.txt and site terms of service.
- Long-running crawls can consume significant disk space, memory, and network bandwidth.
- A dedicated host or VPS is recommended for sustained crawling.
- Current stable is v1.3.0

## License

See the repository's license file for usage terms.
Uses the Mozilla Public License 2.0

