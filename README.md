# projectGurt

- ***`gurt`*** is a web crawler built in python, built with `nuitka`.
- It uses a depth first approach to crawling, and uses multiprocessing, and threading.
- 



## Features
 - Stores urls in a database `crawler.db`
 - Lots of logging for each crawler set
 - Kind of fast.
 - Has a script `watchCrawler.sh` or `watchCrawler.bat` to see urls added every 2 seconds and the rate of crawling

 > ***`gurt`*** is designed to take a long time. This should ideally be done on a server with minimm 2GB of RAM. This is ideal for a headless Pi 4, or a VPS

## Requirements

- ***`gurt`*** uses the following libraries:
- `httpx`
- `sqlite3`
- `bs4`
- `urllib`
- `logging`
- `datetime`
- `asyncio`
- `threading`
- `multiproccessing`
- `queue`
- `time`
- `rich`
- `argparse`

- To install dependencies, `requirements.txt` is provided:

``` bash
python -m pip install -r requirements.txt
```

## Executing

- ***`gurt`*** has two ways to run, an executable, or native python.

- Usage:

``` bash
gurt [-h] [-s FOLDER] [-f NUM] [-q NUM] [-m] [-d NUM] procs workers crawlers
```

### Positional arguments:
-  `procs`                   Number of processes to run
-  `workers`                 Number of workers operations
-  `crawlers`                Number of crawlers.

### Options:
-  `-h`, `--help`            show help message
-  `-s`, `--seeds` `FOLDER`  Folder where seeds are stored. Defaults to seeds/
-  `-f`, `--fetch` `NUM`     Number of urls to fetch from the db. Defaults to 200
-  `-q`, `--queue` `NUM`     Size of url queue. Defaults to 50000
-  `-m`, `--monitor`         Shows the monitor. Do not set when running as a service.
-  `-d`, `--delta` `NUM`     Delta that the load balancer should keep to the mean. Defaults to 5000

