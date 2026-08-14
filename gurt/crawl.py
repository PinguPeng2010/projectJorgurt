import sqlite3
from selectolax.parser import HTMLParser
from urllib.parse import urlparse, urljoin
import logging
import asyncio
import httpx
from multiprocessing import Queue
from time import monotonic
from pathlib import Path
from zstandard import ZstdCompressor

compressor =ZstdCompressor(level=3)

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "../logs"
DB_PATH = BASE_DIR / "../data/jorgurt.db"

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

infoLog = logging.FileHandler(LOG_DIR / "gurt-crawler.log", encoding='utf-8')
infoLog.setLevel(logging.INFO)
infoLog.addFilter(lambda r: r.levelno < logging.ERROR)
infoLog.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s'))
logger.addHandler(infoLog)

errorLog = logging.FileHandler(LOG_DIR / "gurt-error.log", encoding='utf-8')
errorLog.setLevel(logging.ERROR)
errorLog.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s'))
logger.addHandler(errorLog)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

STATS_INTERVAL_SECONDS = 2.0


class RequestException(Exception):
    pass

class EmptyResponse(Exception):
    pass


class CrawlerState:

    __slots__ = (
        'proc', 'visited', 'visitable_set', 'robots_cache',
        'requests', 'notFound', 'rateLimited', 'forbidden',
        'badResponses', 'errors', 'success', 'last_emit',
    )

    def __init__(self, proc: int, seeds: list[str]):
        self.proc = proc
        self.visited: set[str] = set()
        self.visitable_set: set[str] = set(seeds)
        self.robots_cache: dict[str, str | None] = {}
        self.requests = 0
        self.notFound = 0
        self.rateLimited = 0
        self.forbidden = 0
        self.badResponses = 0
        self.errors = 0
        self.success = 0
        self.last_emit = 0.0

    def snapshot(self) -> dict[str, int]:
        return {
            'requests': self.requests,
            'notFound': self.notFound,
            'rateLimited': self.rateLimited,
            'forbidden': self.forbidden,
            'badResponses': self.badResponses,
            'errors': self.errors,
            'success': self.success,
        }


def emitStats(statsQueue: Queue, state: CrawlerState, force: bool = False) -> None:
    if statsQueue is None:
        return
    now = monotonic()
    if not force and now - state.last_emit < STATS_INTERVAL_SECONDS:
        return
    state.last_emit = now
    statsQueue.put({'crawler_id': state.proc, 'stats': state.snapshot()})


def log(logQueue: Queue, message: str) -> None:
    if logQueue is not None:
        logQueue.put(message)
    else:
        logging.info(message)


def compressText(text: str) -> bytes:
    compressed: bytes = compressor.compress(text.encode("utf-8"))
    return compressed


async def getNewUrls(visitable: asyncio.Queue, proc: int, fetch: int, done: asyncio.Event, logQueue: Queue) -> None:

    log(logQueue, f'GRABBER STARTED: PROC: {proc}')

    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    nullPulls = 0
    try:
        while True:
            if visitable.qsize() > 20:
                await asyncio.sleep(0.05)
                continue

            def _grab():
                conn.execute('BEGIN IMMEDIATE')
                rows = conn.execute("""
                    SELECT url FROM urls
                    WHERE state = ? AND proc = ?
                    LIMIT ?
                """, ("READY", proc, fetch)).fetchall()
                urls = [row[0] for row in rows]
                for url in urls:
                    conn.execute("UPDATE urls SET state = ? WHERE url = ?", ("CLAIMED", url))
                conn.commit()
                return urls

            try:
                urls = await asyncio.to_thread(_grab)
            except sqlite3.Error as e:
                conn.rollback()
                logging.critical(f'GRABBER proc {proc} db error: {e}')
                await asyncio.sleep(0.5)
                continue

            if not urls:
                nullPulls += 1
                await asyncio.sleep(0.05)
                if nullPulls >= 100:
                    log(logQueue, f'Proc: {proc} finished.')
                    done.set()
                    return
                continue

            nullPulls = 0
            for url in urls:
                await visitable.put(url)
    finally:
        conn.close()


async def findRobots(domain: str, session: httpx.AsyncClient, robots_cache: dict) -> str | None:
    if not domain:
        return None

    parsed = urlparse(domain)
    host = f'{parsed.scheme}://{parsed.netloc}' if parsed.scheme and parsed.netloc else domain

    if host in robots_cache:
        return robots_cache[host]

    try:
        response = await session.get(host + '/robots.txt', timeout=10, follow_redirects=True)
        if response.status_code >= 400:
            logging.warning(f'No usable robots.txt for {host} (HTTP {response.status_code})')
            robots_cache[host] = None
            return None
        robots_cache[host] = response.text
        return response.text
    except asyncio.CancelledError:
        raise
    except Exception:
        logging.critical(f'Error getting robots.txt for {domain}')
        robots_cache[host] = None
        return None


def getDisallowed(robots: str | None, domain: str) -> set:
    if not robots:
        return set()

    lines = [line.strip() for line in robots.splitlines() if line.strip()]
    disallowed = []
    right_agent = False

    for line in lines:
        lower_line = line.lower()
        if lower_line.startswith('user-agent:'):
            agent = line.split(':', 1)[1].strip()
            right_agent = agent == '*'
            continue
        if not right_agent:
            continue
        if lower_line.startswith('disallow:'):
            value = line.split(':', 1)[1].strip()
            disallowed.append(domain + value if value else domain)

    return set(disallowed)


async def visitSite(site: str, session: httpx.AsyncClient, state: CrawlerState) -> tuple[str | None, int | None]:
    try:
        response = await session.get(site, timeout=10)
        state.requests += 1

        if response.status_code == 404:
            state.notFound += 1
        elif response.status_code == 429:
            state.rateLimited += 1
        elif response.status_code == 403:
            state.forbidden += 1
        elif response.status_code >= 400:
            state.badResponses += 1

        if response.status_code >= 400:
            logging.warning(f'Site: {site} returned HTTP {response.status_code}')
            return None, response.status_code

        if not response.text:
            logging.warning(f'Site: {site} returned an empty body')
            return None, response.status_code

        state.success += 1
        return response.text, response.status_code

    except asyncio.CancelledError:
        raise
    except Exception:
        state.errors += 1
        logging.critical(f'Error requesting {site}')
        return None, None


def parseLinks(html: str, site: str, disallowed: set[str]) -> tuple[str, list[str]]:

    tree = HTMLParser(html)

    title_node = tree.css_first('title')
    title = title_node.text(strip=True) if title_node else None
    if not title:
        title = site

    links = []
    for a in tree.css('a[href]'):
        href = a.attributes.get('href')
        if not href:
            continue
        resolved = urljoin(site, href)
        parsed = urlparse(resolved)
        if parsed.scheme not in ('http', 'https') or parsed.fragment:
            continue
        if resolved in disallowed:
            continue
        links.append(resolved)

    return title, links


async def worker(workerID: int, visitable: asyncio.Queue, dbQueue: Queue, statsQueue: Queue,
                  logQueue: Queue, session: httpx.AsyncClient, done: asyncio.Event,
                  proc: int, state: CrawlerState) -> None:
    while True:
        if done.is_set() and visitable.empty():
            break
        try:
            site: str = await asyncio.wait_for(visitable.get(), timeout=1)
        except asyncio.TimeoutError:
            continue

        try:
            if site in state.visited:
                continue

            log(logQueue, f'{proc}.{workerID}: {site}')

            robots = await findRobots(site, session, state.robots_cache)
            disallowed = getDisallowed(robots, site)
            text, _status = await visitSite(site, session, state)
            if text is not None:
                title, links = await asyncio.to_thread(parseLinks, text, site, disallowed)
                await asyncio.to_thread(dbQueue.put, ('pages', site, compressText(text), title))
                for link in links:
                    if link not in state.visitable_set:
                        state.visitable_set.add(link)
                        await asyncio.to_thread(dbQueue.put, ('urls', link, proc, 'READY', title))

                await asyncio.to_thread(dbQueue.put, ('urls', site, proc, 'FINISHED', title))
            else:
                # visitSite already logged + counted the specific HTTP error;
                # just mark it done and move on
                await asyncio.to_thread(dbQueue.put, ('urls', site, proc, 'FINISHED', None))

            state.visited.add(site)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Genuine unexpected error — log, count, and move on
            state.errors += 1
            logging.critical(f'Error handling {site}: {e}, proc: {proc}, worker: {workerID}')
            await asyncio.to_thread(dbQueue.put, ('urls', site, proc, 'FINISHED', None))
            state.visited.add(site)
        finally:
            emitStats(statsQueue, state)


async def runCrawlerProcess(workers: int, queueSize: int, dbQueue: Queue, statsQueue: Queue,
                             logQueue: Queue, proc: int, seeds: list[str], fetch: int) -> None:
    state = CrawlerState(proc, seeds)
    visitable: asyncio.Queue = asyncio.Queue(maxsize=queueSize)
    done = asyncio.Event()

    # Connection pool scales with worker count instead of a fixed
    # 100 regardless of how many workers you actually run.
    limits = httpx.Limits(max_connections=workers + 20, max_keepalive_connections=workers)

    async with httpx.AsyncClient(limits=limits, timeout=10, follow_redirects=True) as session:
        grabber = asyncio.create_task(getNewUrls(visitable, proc, fetch, done, logQueue))
        workerTasks = [
            asyncio.create_task(worker(i, visitable, dbQueue, statsQueue, logQueue, session, done, proc, state))
            for i in range(workers)
        ]
        await asyncio.gather(grabber, *workerTasks, return_exceptions=True)

    emitStats(statsQueue, state, force=True)


def runProcess(workers: int, queueSize: int, dbQueue: Queue, statsQueue: Queue,
               logQueue: Queue, proc: int, seeds: list[str], fetch: int) -> None:

    asyncio.run(runCrawlerProcess(workers, queueSize, dbQueue, statsQueue, logQueue, proc, seeds, fetch))