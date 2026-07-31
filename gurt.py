from multiprocessing import Process, Queue
from crawl import startThreads
import sqlite3
from datetime import datetime, timezone
import argparse
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


    queue = Queue()

    writer = Process(target=dbWriter, args=(queue,))
    writer.start()
    
    processes = []
    for i in range(procs):
        proc = Process(target=startThreads, args=(crawlers, queue, i, seeds[i], asyncs))

        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()

    queue.put('STOP')
    writer.join()
    print(f'All processes finished')

initDb()

launch(
    args.procs,
    args.crawlers,
    args.seeds,
    args.asyncs
)

