from multiprocessing import Process, Queue, freeze_support, Event
from crawl import startThreads
import sqlite3
from datetime import datetime, timezone
import argparse
from collections import deque
from os import listdir, path
import logging
from time import sleep, monotonic

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

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
    help='Number of urls to fetch from the db. Defaults to 200'
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
    help='Number of crawlers.'
)

parser.add_argument(
    "-m",
    "--monitor",
    action='store_true',
    help='Shows the monitor. Do not set when running as a service.'
)

parser.add_argument(
    "-d",
    "--delta",
    type=int,
    metavar='NUM',
    default=5000,
    help='Delta that the load balancer should keep to the mean. Defaults to 5000'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# info +warning log

infoLog = logging.FileHandler('logs/crawler.log', encoding='utf-8')
infoLog.setLevel(logging.INFO)
infoLog.addFilter(lambda r: r.levelno < logging.ERROR)
infoLog.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s'))
logger.addHandler(infoLog)


class SeedException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)



STATS_KEYS = (
    'requests',
    'notFound',
    'rateLimited',
    'forbidden',
    'badResponses',
    'errors',
    'success',
)


def styleLogLine(line: str) -> Text:
    text = Text(line)
    lowered = line.lower()

    if 'start' in lowered or 'started' in lowered or 'starting' in lowered:
        text.stylize('bold #d9b3ff')

    if 'http://' in lowered or 'https://' in lowered:
        text.stylize('bold green')

    return text


def renderStats(totals, urlCount):
    stats_table = Table(show_header=False, box=None, expand=True)
    stats_table.add_column('Metric', style='bold cyan')
    stats_table.add_column('Value', justify='right')

    for key in STATS_KEYS:
        stats_table.add_row(key, str(totals.get(key, 0)))

    requests = totals.get('requests', 0)
    success = totals.get('success', 0)
    successRate = (success / requests * 100) if requests else 0.0
    stats_table.add_row('% success', f'{successRate:.1f}%')

    stats_table.add_row('urls in db', str(urlCount) if urlCount is not None else '...')

    return Panel(
        stats_table,
        title='[bold white]Crawler Stats[/bold white]',
        border_style='bright_blue',
    )


def renderLog(log_lines):
    log_table = Table.grid(expand=True)

    for line in list(log_lines)[-12:]:
        log_table.add_row(styleLogLine(line))

    return Panel(
        log_table,
        title='[bold white]Recent Activity[/bold white]',
        border_style='bright_green',
    )


def statsMonitor(statsQueue: Queue, logQueue: Queue):
    totals = {key: 0 for key in STATS_KEYS}
    lastSeen = {}
    log_lines = deque(maxlen=500)
    console = Console()

    URL_COUNT_INTERVAL = 2.0
    urlCount = None
    lastUrlCountAt = 0.0

    try:
        countConn = sqlite3.connect('file:crawler.db?mode=ro', uri=True, timeout=1)
    except sqlite3.Error:
        countConn = None

    with Live(console=console, refresh_per_second=4, transient=False) as live:
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

            now = monotonic()
            if countConn is not None and now - lastUrlCountAt >= URL_COUNT_INTERVAL:
                try:
                    row = countConn.execute("SELECT COUNT(*) FROM urls").fetchone()
                    urlCount = row[0] if row else urlCount
                except sqlite3.Error as e:
                    logging.warning(f'MONITOR: url count query failed: {e}')
                lastUrlCountAt = now

            live.update(
                Panel(
                    Group(renderStats(totals, urlCount), renderLog(log_lines)),
                    title='[bold #d9b3ff]Crawler Monitor[/bold #d9b3ff]',
                    border_style='magenta',
                )
            )

    if countConn is not None:
        countConn.close()


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

def loadBalancer(procs: int, stop, delta: int):
    while True:
        try:
            sleep(60)
            if stop.is_set():
                break
            with sqlite3.connect('crawler.db', timeout=30) as conn:
                conn.execute('BEGIN IMMEDIATE')
                rows = conn.execute("""
                    SELECT proc, COUNT(*) AS ready_count
                    FROM urls
                    WHERE state = 'READY'
                    GROUP BY proc
                    """).fetchall()

                urlCount: dict[int, int] = {i: 0 for i in range(procs)}

                urlCount.update(dict(rows))

                mean = sum(urlCount.values()) // procs

                for p in range(procs):
                    if urlCount[p] > mean + delta:
                        smallest = min(urlCount.values())
                        move = (urlCount[p] - smallest) // 2

                        moveFrom = p

                        for q in range(procs):
                            if urlCount[q] == smallest:
                                moveTo = q
                                break

                        break
                else:
                    continue
                conn.execute("""
                    UPDATE urls
                    SET proc = ?
                    WHERE rowid IN (
                        SELECT rowid
                        FROM urls
                        WHERE proc = ?
                        AND state = 'READY'
                        LIMIT ?
                    )
                """, (moveTo, moveFrom, move)) # type: ignore

                conn.commit()
            logging.info(f'BALANCER moved {move} urls from proc: {moveFrom} to proc: {moveTo}') # type: ignore
        except sqlite3.Error as e:
            logging.critical(f"Balancer had an error: {e}")

            
    
            


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
def launch(procs: int, crawlers: int, seedloc: str, asyncs: int, fetch: int, size: int, delta: int, showMonitor: bool):
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

    stopEvent = Event()
    statsQueue = Queue()
    logQueue = Queue()
    dbQueue = Queue(maxsize=10000)
    visitableQueue = Queue(maxsize=size)

    if showMonitor:
        monitor = Process(target=statsMonitor, args=(statsQueue, logQueue,))
        monitor.start()

    balancer = Process(target=loadBalancer, args=(procs, stopEvent, delta,))
    balancer.start()

    writer = Process(target=dbWriter, args=(dbQueue,))
    writer.start()

    processes = []
    for i in range(procs):
        proc = Process(target=startThreads, args=(crawlers, size, dbQueue, statsQueue, logQueue, i, visitableQueue, seeds[i], asyncs, fetch,))

        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()
    stopEvent.set()
    balancer.join()
    dbQueue.put('STOP')
    statsQueue.put('STOP')
    logQueue.put('STOP')
    writer.join()
    if showMonitor:
        monitor.join() # type: ignore
    print(f'All processes finished')

if __name__ == "__main__":

    freeze_support()
    args = parser.parse_args()
    launch(
        args.procs,
        args.crawlers,
        args.seeds,
        args.workers,
        args.fetch,
        args.queue,
        args.delta,
        args.monitor
    )