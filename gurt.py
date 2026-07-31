from multiprocessing import Process, Queue
from crawl import startThreads
import sqlite3
from datetime import datetime, timezone
import argparse
from os import listdir, path

parser = argparse.ArgumentParser()

parser.add_argument(
    "proc",
    type=int,
    help='Number of processes to run'
)

parser.add_argument(
    "-s",
    "--seeds",
    type=str,
    metavar='FOLDER',
    help='Folder where seeds are stored. Defaults to /seeds/'

)

parser.add_argument(
    "-c",
    "--crawlers",
    type=int,
    metavar='NUM',
    help='Number of crawlers. Defaults to 1'
)

parser.add_argument(
    "-v",
    "--verbose",
    action='count',
    default=0,
    help='Provide outputs to console of sites visited (-v, -vv, -vvv)'
)

class SeedException(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)

args = parser.parse_args()

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

    while True:
        msg: tuple[str, str] | str = queue.get()

        if msg == "STOP":
            break

        url, title = msg

        cur.execute('''
            INSERT INTO urls (url, timestamp, title)
            VALUES (?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                timestamp = excluded.timestamp,
                title = excluded.title
        ''', (url, datetime.now(timezone.utc).isoformat(), title))

        conn.commit()

    conn.close()

def launch(procs, crawlers, seedloc, verbose):

    # get seeds
    if seedloc is not None:
        if not path.exists(seedloc):
            raise SeedException(f'The folder: {seedloc} was not found')

        else:
            seedPacks: list[str] = listdir(seedloc)
            if not len(seedPacks) == procs:
                raise SeedException(f'The number of seed packs is different to the number of processes: packs: {len(seeds)}, procs: {procs}')

    else:
        seedPacks: list[str] = listdir(seedloc)
        if not len(seedPacks) == procs:
            raise SeedException(f'The number of seed packs is different to the number of processes: packs: {len(seeds)}, procs: {procs}')
        

    queue = Queue()

    writer = Process(target=dbWriter, args=(queue,))
    writer.start()
    
    processes = []
    for i in range(procs):
        proc = Process(target=startThreads, args=(crawlers, queue, i, seeds[i]))

        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()

    queue.put('STOP')
    writer.join()
    print(f'All processes finished')

initDb()

# with open(f'seeds/{args.seeds}', 'r') as s:
#     seeds = [line.strip() for line in s]