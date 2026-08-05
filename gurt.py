from multiprocessing import Process, Queue, freeze_support
from crawl import startThreads
import sqlite3
from datetime import datetime, timezone
import argparse
import curses
from collections import deque
from os import listdir, path
import logging

parser = argparse.ArgumentParser()

parser.add_argument(
    "procs",
    type=int,
    help='Number of processes to run'
)

parser.add_argument(
    "workers",
    type=int,
    help="Number of workers operations"
)

parser.add_argument(
    "-s",
    "--seeds",
    type=str,
    metavar='FOLDER',
    default='seeds',
    help='Folder where seeds are stored. Defaults to seeds/'

)

parser.add_argument(
    "-f",
    "--fetch",
    type=int,
    metavar='NUM',
    default=200,
    help='Number of urls to fetch from the db. Defaults to 100'
)

parser.add_argument(
    "-q",
    "--queue",
    type=int,
    metavar='NUM',
    default=50000,
    help='Size of url queue. Defaults to 50000'
)

parser.add_argument(
    "crawlers",
    type=int,
    metavar='NUM',
    help='Number of crawlers.'
)

parser.add_argument(
    "--balance-delta",
    type=int,
    metavar='NUM',
    help='Delta that the load balancer should keep to the mean.'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# info +warning log

infoLog = logging.FileHandler('logs/crawler.log')
infoLog.setLevel(logging.INFO)
infoLog.addFilter(lambda r: r.levelno < logging.ERROR)
infoLog.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s'))
logger.addHandler(infoLog)


class SeedException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)

args = parser.parse_args()


STATS_KEYS = (
    'requests',
    'notFound',
    'rateLimited',
    'forbidden',
    'badResponses',
    'errors',
    'success',
)


def renderMonitor(stdscr, log_lines, totals):
    stdscr.erase()
    height, width = stdscr.getmaxyx()
    log_area = max(1, height - 2)

    for idx, line in enumerate(list(log_lines)[-log_area:]):
        try:
            stdscr.addnstr(idx, 0, line, max(0, width - 1))
        except curses.error:
            pass

    footer = ' | '.join(f'{key}={totals.get(key, 0)}' for key in STATS_KEYS)
    stdscr.addnstr(height - 1, 0, footer, max(0, width - 1))
    stdscr.refresh()


def statsMonitor(statsQueue: Queue, logQueue: Queue):
    totals = {key: 0 for key in STATS_KEYS}
    lastSeen = {}
    log_lines = deque(maxlen=500)

    def run(stdscr):
        stdscr.nodelay(True)
        stdscr.keypad(True)

        while True:
            try:
                msg = statsQueue.get(timeout=0.1)
                if msg == 'STOP':
                    break
                if isinstance(msg, dict):
                    process_id = msg.get('crawler_id')
                    current = msg.get('stats', {})
                    previous = lastSeen.setdefault(process_id, {})
                    for key in STATS_KEYS:
                        current_value = current.get(key, 0)
                        previous_value = previous.get(key, 0)
                        delta = current_value - previous_value
                        if delta:
                            totals[key] += delta
                        previous[key] = current_value
            except Exception:
                pass

            try:
                log_msg = logQueue.get(timeout=0.05)
                if log_msg == 'STOP':
                    break
                if isinstance(log_msg, str):
                    log_lines.append(log_msg)
            except Exception:
                pass

            renderMonitor(stdscr, log_lines, totals)

    curses.wrapper(run)


def initDb() -> None:
    conn = sqlite3.connect('crawler.db')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS urls (
            url TEXT PRIMARY KEY,
            proc INTEGER NOT NULL,
            state TEXT,
            timestamp TEXT NOT NULL,
            title TEXT
        )
    ''')

    conn.commit()
    conn.close()    

def seedInsert(seedPacks: list, seedloc: str, procs) -> None:
    conn = sqlite3.connect('crawler.db', timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()

    batch = []
    
    for i in range(procs):
        with open(f'{seedloc}/{seedPacks[i]}' ,'r') as p:
            seeds = [line.strip() for line in p]

        for seed in seeds:
            batch.append((
                seed,
                i,
                "READY",
                datetime.now(timezone.utc).isoformat(),
                'seed'
            ))

    cur.executemany("""
        INSERT INTO urls (url, proc, state, timestamp, title)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(url) DO NOTHING
    """, batch)

    conn.commit()
    conn.close()

    

def dbWriter(queue: Queue):
    conn = sqlite3.connect('crawler.db', timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()

    batch = []
    BATCH_SIZE = 100

    while True:
        msg = queue.get()

        if msg == "STOP":
            break

        batch.append((
            msg[0],
            msg[1],
            msg[2],
            datetime.now(timezone.utc).isoformat(),
            msg[3]
        ))

        if len(batch) >= BATCH_SIZE:
            print("DB WRITER: starting transaction", flush=True)

            cur.executemany('''
                INSERT INTO urls (url, proc, state, timestamp, title)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    proc = excluded.proc,
                    state = excluded.state,
                    timestamp = excluded.timestamp,
                    title = excluded.title
            ''', batch)
            print("DB WRITER: committing", flush=True)

            conn.commit()
            print("DB WRITER: committed", flush=True)

            batch.clear()

    # Write anything left over
    if batch:
        cur.executemany('''
            INSERT INTO urls (url, proc, state, timestamp, title)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                proc = excluded.proc,
                state = excluded.state,
                timestamp = excluded.timestamp,
                title = excluded.title
        ''', batch)

        conn.commit()

    conn.close()
def launch(procs: int, crawlers: int, seedloc: str, asyncs: int, fetch: int, size: int):
    initDb()

    # get seeds
    try:
        if seedloc is not None:
            if not path.exists(seedloc):
                raise SeedException(f'The folder: {seedloc} was not found')

            else:
                seedPacks: list[str] = listdir(seedloc)
                if len(seedPacks) < procs:
                    raise SeedException(f'The number of seed packs is less than the number of processes: packs: {len(seedPacks)}, procs: {procs}')


    except OSError:
        raise OSError(f'The seed location: {seedloc}, wasnt found')

    seedInsert(seedPacks, seedloc, procs) # type: ignore

    seeds = []
    for pack in seedPacks: # type: ignore
        with open(f'{seedloc}/{pack}' ,'r') as p:
            seed = [line.strip() for line in p]
            seeds.append(seed)

    statsQueue = Queue()
    logQueue = Queue()
    dbQueue = Queue(maxsize=10000)
    visitableQueue = Queue(maxsize=size)

    monitor = Process(target=statsMonitor, args=(statsQueue, logQueue,))
    monitor.start()

    writer = Process(target=dbWriter, args=(dbQueue,))
    writer.start()

    processes = []
    for i in range(procs):
        proc = Process(target=startThreads, args=(crawlers, size, dbQueue, statsQueue, logQueue, i, visitableQueue, seeds[i], asyncs, fetch,))

        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()

    dbQueue.put('STOP')
    statsQueue.put('STOP')
    logQueue.put('STOP')
    writer.join()
    monitor.join()
    print(f'All processes finished')

if __name__ == "__main__":

    freeze_support()

    launch(
        args.procs,
        args.crawlers,
        args.seeds,
        args.worker,
        args.fetch,
        args.queue
    )

