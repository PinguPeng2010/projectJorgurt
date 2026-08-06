import sqlite3
from bs4 import BeautifulSoup as bs
from bs4 import XMLParsedAsHTMLWarning
from urllib.parse import urlparse, urljoin
import logging
from datetime import datetime, timezone
import asyncio
import httpx
from threading import Thread, Event
from multiprocessing import Queue
from time import monotonic, sleep
from queue import Empty, Full
import warnings
from pathlib import Path

# Change the architecture

# On startup, gurt adds seeds to db, with schema url, proc_id, status, time, title.

# On process startup, each one starts a thread that checks if the url queue has a size less than 20.
# If yes, then query the db, and add 200 urls to the queue, changing the url's status in the db to CLAIMED
# This means that the workers do not add new urls to the queue.
# The workers instead put the urls in the db queue, along with the crawled url, with status READY.
# When urls are crawled, they are put into the db with the status FINISHED

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

requests: int = 0
notFound: int = 0
rateLimited: int = 0
forbidden: int = 0
badResponses: int = 0
errors: int = 0
success: int = 0

STATS_INTERVAL_SECONDS = 2.0


def getStatsSnapshot() -> dict[str, int]:
    return {
        'requests': requests,
        'notFound': notFound,
        'rateLimited': rateLimited,
        'forbidden': forbidden,
        'badResponses': badResponses,
        'errors': errors,
        'success': success,
    }


def emitStats(stats_queue: Queue, crawler_id: int | None = None) -> None:
    if stats_queue is None:
        return

    stats_queue.put({
        'crawler_id': crawler_id,
        'stats': getStatsSnapshot(),
    })


def maybeEmitStats(stats_queue: Queue, crawler_id: int | None, last_emit: dict[int, float], interval: float = STATS_INTERVAL_SECONDS) -> float:
    now = monotonic()
    previous = last_emit.get(crawler_id if crawler_id is not None else -1, 0.0)
    if now - previous >= interval:
        emitStats(stats_queue, crawler_id)
        last_emit[crawler_id if crawler_id is not None else -1] = now
        return now
    return previous



disallowed: set = set() # dione by robots.txt
visited: set = set()
robots_cache: dict[str, str | None] = {}

# with open(f'seeds/{args.seeds}', 'r') as s:
#     seeds = [line.strip() for line in s]

seeds = []
visitableSet: set[str] = set()

# log time

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
DB_PATH = BASE_DIR / "crawler.db"

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# info +warning log

infoLog = logging.FileHandler(LOG_DIR / "crawler.log", encoding='utf-8')
infoLog.setLevel(logging.INFO)
infoLog.addFilter(lambda r: r.levelno < logging.ERROR)
infoLog.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s'))
logger.addHandler(infoLog)

# error log
errorLog = logging.FileHandler(LOG_DIR / "error.log", encoding='utf-8')
errorLog.setLevel(logging.ERROR)
errorLog.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s'))
logger.addHandler(errorLog)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
# custom exceptions

class RequestException(Exception): # Site returned an error code
    def __init__(self, message: str):
        global errors
        super().__init__(message)
        logging.error(f'RequestException: {message}')
        errors += 1

class EmptyResponse(Exception):
    def __init__(self, message: str):
        global errors
        super().__init__(message)
        logging.warning(f'EmptyResponse: {message}')
        errors += 1



class RobotsNotFound(Exception): # The site doesnt have robots.txt
    def __init__(self, message: str, status: int):
        super().__init__(message)
        logging.warning(f'RobotsNotFound: {message}, code: {status}')

def getNewUrls(visitable: Queue, proc: int, finishedEvent: Event, fetch: int=200):
    # queries the db, so needs the visitable queue 
    logging.info(f'GRABBER STARTED: PROC: {proc}')      
    nullPulls = 0
    while True:
        if finishedEvent.is_set():
            break

        if visitable.qsize() > 20: # if the queue is less than 20, it needs refilling
            sleep(0.05)
            continue
        conn = sqlite3.connect(DB_PATH, timeout=30)

        try:
            conn.execute('BEGIN IMMEDIATE')
            rows = conn.execute("""
                SELECT url
                FROM urls
                WHERE state = ?
                    AND proc = ?
                LIMIT ?
            """, ("READY", proc, fetch)).fetchall()
            urls = [row[0] for row in rows] # type: ignore
            if len(urls) == 0:
                nullPulls += 1
                sleep(0.05)
                if nullPulls >= 100:
                    logging.info(f'Proc: {proc} finished.')
                    finishedEvent.set()
                    break
            else:
                nullPulls = 0
            for url in urls:
                conn.execute("""
                    UPDATE urls
                    SET state = ?
                    WHERE url = ?
                """, ("CLAIMED", url))
            conn.commit()

            for url in urls:
                while True:
                    if finishedEvent.is_set():
                        return
                    try:
                        visitable.put(url, timeout=0.25)
                        break
                    except Full:
                        sleep(0.05)
            
        except:
            conn.rollback()
            raise
        finally:
            conn.close()



def getDbVisitedUrls() -> set[str]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute('SELECT url FROM urls')
    urls = {row[0] for row in cursor.fetchall()}
    conn.close()
    return urls

async def findRobots(domain: str, crawlerID: int, session) -> str | None:
    if not domain:
        return None

    parsed = urlparse(domain)
    host = (
        f'{parsed.scheme}://{parsed.netloc}'
        if parsed.scheme and parsed.netloc
        else domain
    )

    if host in robots_cache:
        return robots_cache[host]

    robotURL = host + '/robots.txt'

    try:
        response = await session.get(
            robotURL,
            timeout=10,
            follow_redirects=True
        )

        if response.status_code >= 400:
            logging.warning(
                f'No usable robots.txt for {host} '
                f'(HTTP {response.status_code}), crawler: {crawlerID}'
            )
            robots_cache[host] = None
            return None

        robots_cache[host] = response.text
        return response.text

    except asyncio.CancelledError:
        raise

    except Exception:
        logging.critical(
            f'Error getting robots.txt for {domain}, '
            f'crawler: {crawlerID}'
        )
        robots_cache[host] = None
        return None

def getDisallowed(robots, domain) -> set:
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
            if value:
                disallowed.append(domain + value)
            else:
                disallowed.append(domain)
    return set(disallowed)


def markCrawled(site: str) -> bool:
    try:
        global visited
        visited.add(site)
        return True
    except Exception as e:
        logging.error(f'Site: {site} could not be marked as crawled: {e}')
        return False


async def visitSite(site: str, crawlerID, session) -> tuple[str | None, int | None]:
    global requests, notFound, rateLimited, forbidden, badResponses, success

    try:
        response = await session.get(site, timeout=10)
        requests += 1

        if response.status_code == 404:
            notFound += 1
        elif response.status_code == 429:
            rateLimited += 1
        elif response.status_code == 403:
            forbidden += 1
        elif response.status_code >= 400:
            badResponses += 1

        if response.status_code >= 400:
            logging.warning(
                f'Site: {site} returned HTTP {response.status_code}, '
                f'crawler: {crawlerID}'
            )
            return None, response.status_code

        if not response.text:
            logging.warning(
                f'Site: {site} returned an actually empty body, '
                f'crawler: {crawlerID}'
            )
            return None, response.status_code

        success += 1
        return response.text, response.status_code

    except asyncio.CancelledError:
        raise

    except Exception:
        logging.critical(
            f'Error requesting {site}, crawler: {crawlerID}'
        )
        return None, None


def getAllLinks(html: str, disallowed: set[str], site: str, dbQueue: Queue, proc) -> tuple[bool, str] | None:
    global visitableSet
    parsedHtml = bs(html, 'lxml')
    aTags = parsedHtml.find_all('a')
    title_tag = parsedHtml.title

    title = None

    if title_tag and title_tag.string:
        title = title_tag.string.strip()

    if not title:
        title = site
    
    for tag in aTags:
        href = tag.get('href')

        if not href:
            continue

        resolved = urljoin(site, str(href))
        parsed = urlparse(resolved)

        if not isDisallowed(disallowed, resolved):
            if parsed.scheme in ('http', 'https') and not parsed.fragment:
                if resolved not in visitableSet:
                    visitableSet.add(resolved)
                    dbQueue.put((resolved, proc, 'READY', title))
    return (True, title)


def isFinished(site: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        'SELECT 1 FROM urls WHERE url = ? AND state = ? LIMIT 1',
        (site, 'FINISHED')
    )
    result = cursor.fetchone()
    conn.close()
    return result is not None

def getPageTitle(html: str) -> str | None:
    parsed_html = bs(html, 'html.parser')
    title_tag = parsed_html.title
    if title_tag and title_tag.string:
        return title_tag.string.strip()
    return None


def isDisallowed(disallowed: set[str], link: str) -> bool:
    try:
        if link in disallowed:
            return True
        return False

    except Exception as e:
        logging.critical(f'Something went wrong with identifyDisallowed: {e}')
        return False





async def worker(crawlerID: int, dbQueue: Queue, statsQueue: Queue, logQueue: Queue, visitable: Queue, session, finishedEvent: Event, proc: int):
    last_emit = {}
    while True:
        try:
            if finishedEvent.is_set():
                break

            site: str = await asyncio.to_thread(visitable.get, True, 1)
        except Empty:
            logging.error('There was nothing in the queue')
            if finishedEvent.is_set():
                break
            continue
        try:
            if site in visited:
                continue
            if isFinished(site):
                continue
            if logQueue is not None:
                logQueue.put(f'{crawlerID}: {site}')
            else:
                logging.info(f'{crawlerID}: {site}')

            robots: str | None = await findRobots(site, crawlerID, session)
            disallowed: set[str] = getDisallowed(robots, site)
            text, status = await visitSite(site, crawlerID, session)
            if text is not None:
                if status is None:
                    raise RequestException(f'There was a request error with site: {site}')
                pageData = getAllLinks(text, disallowed, site, dbQueue, proc)

                if pageData is None:
                    result = False
                    title = ''
                else:
                    result = pageData[0]
                    title = pageData[1]

                if not result:
                    
                    raise EmptyResponse(f'Could not get all links for site: {site}, crawler: {crawlerID}')

                dbQueue.put((site, proc, 'FINISHED', title))
                markCrawled(site)
            else:
                title = None
                dbQueue.put((site, proc, 'FINISHED', title))
                markCrawled(site)

                raise EmptyResponse(f'Site: {site} provided an empty response, , crawler: {crawlerID}')

        except EmptyResponse:
            pass
        except Exception as e:
            logging.critical(f'When accessing site: {site}, the following exception occured: {e}, proc: {proc}, crawler: {crawlerID}')
            dbQueue.put((site, proc, 'FINISHED', None))
            markCrawled(site)

        finally:
            if statsQueue is not None:
                maybeEmitStats(statsQueue, crawlerID, last_emit)


async def crawl(visitable: Queue, crawlerID: int, queue: Queue, stats_queue: Queue, log_queue: Queue, asyncs: int, seeds: list[str], finishedEvent: Event, proc, fetch):

    global visitableSet
    visitableSet = set(seeds)

    async with httpx.AsyncClient(
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=100
        ),
        timeout=10,
        follow_redirects=True
    ) as session:
        tasks = [
            asyncio.create_task(
                worker(
                    crawlerID, 
                    queue, 
                    stats_queue, 
                    log_queue, 
                    visitable, session, 
                    finishedEvent, 
                    proc,
                )
            )
            for _ in range(asyncs)
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

def runCrawler(*args) -> None:
    logging.info('in run crawler')
    asyncio.run(crawl(*args))

def startThreads(threadsNum: int, size: int, dbQueue: Queue, stats_queue: Queue, log_queue: Queue, process: int, visitable: Queue, seeds: list[str], asyncs: int, fetch: int):

    threads = []
    finishedEvent = Event()

    urlGrabber = Thread(
        target=getNewUrls,
        args=(visitable, process, finishedEvent, fetch,)
    )

    if log_queue is not None:
        log_queue.put(f'GRABBER STARTING: proc {process}')
    else:
        logging.info(f'GRABBER STARTING: proc {process}')
    urlGrabber.start()
    for t in range(threadsNum):
        if log_queue is not None:
            log_queue.put(f'STARTING THREAD {t}, PROC: {process}')
        else:
            logging.info(f'STARTING THREAD {t}, PROC: {process}')
        thread = Thread(
            target=runCrawler,
            args=(visitable, t, dbQueue, stats_queue, log_queue, asyncs, seeds, finishedEvent, process, fetch,),
            name=f"CrawlerThread-{t}"
        )
        thread.start()
        if log_queue is not None:
            log_queue.put(f'STARTED THREAD {t}, PROC: {process}')
        else:
            logging.info(f'STARTED THREAD {t}, PROC: {process}')

        threads.append(thread)

    urlGrabber.join()
    for thread in threads:
        thread.join()