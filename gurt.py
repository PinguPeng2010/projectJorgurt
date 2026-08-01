from multiprocessing import Process, Queue, freeze_support
from crawl import startThreads
import sqlite3
from datetime import datetime, timezone
import argparse
import curses
from collections import deque
from os import listdir, path

parser = argparse.ArgumentParser()

parser.add_argument(
    "procs",
    type=int,
    help='Number of processes to run'
)

parser.add_argument(
    "asyncs",
    type=int,
    help="Number of async operations"
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
    "-c",
    "--crawlers",
    type=int,
    metavar='NUM',
    default=2,
    help='Number of crawlers. Defaults to 2'
)



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
            timestamp TEXT NOT NULL,
            title TEXT
        )
    ''')

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
            datetime.now(timezone.utc).isoformat(),
            msg[1]
        ))

        if len(batch) >= BATCH_SIZE:
            cur.executemany('''
                INSERT INTO urls (url, timestamp, title)
                VALUES (?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    timestamp = excluded.timestamp,
                    title = excluded.title
            ''', batch)

            conn.commit()
            batch.clear()

    # Write anything left over
    if batch:
        cur.executemany('''
            INSERT INTO urls (url, timestamp, title)
            VALUES (?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                timestamp = excluded.timestamp,
                title = excluded.title
        ''', batch)

        conn.commit()

    conn.close()
def launch(procs, crawlers, seedloc, asyncs):
    initDb()

    # get seeds
    try:
        if seedloc is not None:
            if not path.exists(seedloc):
                raise SeedException(f'The folder: {seedloc} was not found')

            else:
                seedPacks: list[str] = listdir(seedloc)
                if not len(seedPacks) == procs:
                    raise SeedException(f'The number of seed packs is different to the number of processes: packs: {len(seedPacks)}, procs: {procs}')


    except OSError:
        raise OSError(f'The seed location: {seedloc}, wasnt found')

    seeds = []
    for pack in seedPacks: # type: ignore
        with open(f'{seedloc}/{pack}' ,'r') as p:
            seed = [line.strip() for line in p]
            seeds.append(seed)

    statsQueue = Queue()
    logQueue = Queue()
    queue = Queue()

    monitor = Process(target=statsMonitor, args=(statsQueue, logQueue,))
    monitor.start()

    writer = Process(target=dbWriter, args=(queue,))
    writer.start()

    processes = []
    for i in range(procs):
        proc = Process(target=startThreads, args=(crawlers, queue, statsQueue, logQueue, i, seeds[i], asyncs,))

        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()

    queue.put('STOP')
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
        args.asyncs
    )

