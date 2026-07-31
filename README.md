# projectGurt

- ***`gurt`*** is a frontier based web crawler built in python
- It uses a depth first approach to crawling, and uses multiprocessing, and threading.
- Each process has 2 threaded crawlers, with one seed set.

- At a rate of 1 crawler, ***`gurt`*** can access upwards of 30k urls in 9 hours.

## Features
 - Stores urls in a database `crawler.db`
 - Lots of logging for each crawler set
 - Kind of fast.
 - Has a script `watchCrawler.sh` to see urls added every 10 seconds and the rate of crawling

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

- To install dependencies, `requirements.txt` is provided:

``` bash
python -m pip install -r requirements.txt
```


## Testing

- For testing, run:
``` bash
python3 webCrawler.py
```

> When changing the crawler code, note that `visitables` is a ***`deque`*** type, and can have duplicates, unlike the ***`set`*** type.

- This code has custom exceptions. If you need to make them, put them in the format:
``` python
class Exception(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        logging.error(f'Exception: {message}')
```

