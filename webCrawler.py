import requests
import sqlite3
from bs4 import BeautifulSoup as bs
from urllib.parse import urlparse, urljoin
import logging
from datetime import datetime
from requests.adapters import HTTPAdapter
from collections import deque

disallowed: set = set() # dione by robots.txt
visited: set = set()
robots_cache: dict[str, str | None] = {}

seeds = ['https://bbc.co.uk', 'https://en.wikipedia.org/wiki/Main_Page', 'https://github.com/explore', 'https://medium.com/explore-topics', 'https://www.theverge.com/',
          'https://stackoverflow.com/questions', 'https://slashdot.org', 'https://arstechnica.com/', 'https://old.reddit.com/'
          'https://www.reuters.com']

visitable: deque[str] = deque(seeds) # when i parse stuff, i put all urls in here, and try and got through the whole list until empty


# Visit seed 1. Find all links, then go through them all, until, no more. Then Seed 2

# Visit seed1/robots.txt. If exists, find all disallowed.
#Then start requesting, and adding to set.
# If one of the seeds is disallowed, remove.
# Then start visiting.

#TODO tommorow bc i cba
# TODO: get each crawler to spawn a 2 threads
# TODO: get crawler 1 to access deque first, then set var to false and the other way for crawler 2
# TODO: get each crawler to do their own thing!!!!

# log time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

http_session = requests.Session()
http_session.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=100))
http_session.mount('https://', HTTPAdapter(pool_connections=100, pool_maxsize=100))
request_count = 0


def get_http_session() -> requests.Session:
    global http_session, request_count

    request_count += 1
    if request_count > 200:
        http_session.close()
        http_session = requests.Session()
        http_session.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=100))
        http_session.mount('https://', HTTPAdapter(pool_connections=100, pool_maxsize=100))
        request_count = 1

    return http_session

# info +warning log

infoLog = logging.FileHandler('crawler.log')
infoLog.setLevel(logging.INFO)
infoLog.addFilter(lambda r: r.levelno < logging.ERROR)
infoLog.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s'))
logger.addHandler(infoLog)

# error log
errorLog = logging.FileHandler('error.log')
errorLog.setLevel(logging.ERROR)
infoLog.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s'))
logger.addHandler(errorLog)


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


def saveUrl(url: str, title: str | None = None) -> None:
    conn = sqlite3.connect('crawler.db')
    conn.execute('''
        INSERT INTO urls (url, timestamp, title)
        VALUES (?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            timestamp = excluded.timestamp,
            title = excluded.title
    ''', (url, datetime.utcnow().isoformat(), title))
    conn.commit()
    conn.close()


def getDbVisitedUrls() -> set[str]:
    conn = sqlite3.connect('crawler.db')
    cursor = conn.execute('SELECT url FROM urls')
    urls = {row[0] for row in cursor.fetchall()}
    conn.close()
    return urls


class RequestException(Exception): # Site returned an error code
    def __init__(self, message: str):
        super().__init__(message)
        logging.error(f'RequestException: {message}')

class EmptyResponse(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        logging.warning(f'EmptyResponse: {message}')


class UrlDisallowed(Exception): # For URLs disallowed by robots.txt
    def __init__(self, message: str):
        super().__init__(message)
        logging.error(f'UrlDisallowed: {message}')


class RobotsNotFound(Exception): # The site doesnt have robots.txt
    def __init__(self, message: str):
        super().__init__(message)
        logging.warning(f'RobotsNotFound: {message}')



def findRobots(domain: str) -> str | None: # This is for the toplevel domain
    if not domain:
        return None

    parsed = urlparse(domain)
    host = f'{parsed.scheme}://{parsed.netloc}' if parsed.scheme and parsed.netloc else domain

    if host in robots_cache:
        return robots_cache[host]

    robotURL = host + '/robots.txt'
    try:
        session = get_http_session()
        response = session.get(robotURL, timeout=10, allow_redirects=False)
        if response.status_code >= 400:
            raise RobotsNotFound(f'Site: {domain} has no robots.txt')

        robots = response.text
        robots_cache[host] = robots
        return robots
    except Exception:
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


def visitSite(site: str) -> str | None:
    try:
        session = get_http_session()
        response = session.get(site, timeout=10)
        if response.status_code >= 400:
            raise RequestException(f'Site: {site} retuned a code of {response.status_code}: {response.reason}')

        elif response.status_code >= 300:
            logging.warning(f'Redirect: Site: {site} ')
        return response.text

    except Exception:
        return None


def getAllLinks(html: str, disallowed: set[str], site: str) -> tuple[bool, str] | None:
    global visitable
    parsedHtml = bs(html, 'lxml')
    aTags = parsedHtml.find_all('a')

    for tag in aTags:
        href = tag.get('href')
        if not href:
            continue
        resolved = urljoin(site, str(href))
        parsed = urlparse(resolved)
        if not isDisallowed(disallowed, resolved):
            if parsed.scheme in ('http', 'https') and not parsed.fragment:
                visitable.append(resolved)
    title_tag = parsedHtml.title

    if title_tag and title_tag.string:
        title =  title_tag.string.strip()

    if title: #type: ignore
        return (True, title)
    
    else:
        return None

def isInDb(site: str) -> bool:
    conn = sqlite3.connect('crawler.db')
    cursor = conn.execute(
        'SELECT 1 FROM urls WHERE url = ? LIMIT 1',
        (site,)
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
        logging.error(f'Something went wrong with identifyDisallowed: {e}')
        return False



initDb()

while visitable:    
    site = visitable.popleft()
    if site in visited:
        continue
    if isInDb(site):
        continue
    print(site)
    try:
        robots = findRobots(site)
        disallowed = getDisallowed(robots, site)
        text = visitSite(site)
        if text is not None:
            pageData = getAllLinks(text, disallowed, site)
            if pageData is None:
                result = False
                title = ''
            else:
                result = pageData[0]
                title = pageData[1]
            if not result:
                raise EmptyResponse(f'Could not get all links for site: {site}')

            saveUrl(site, title)
            markCrawled(site)
        else:
            title = None
            saveUrl(site, title)
            markCrawled(site)
            raise EmptyResponse(f'Site: {site} provided an empty response')

    except EmptyResponse:
        pass
    except Exception as e:
        logging.error(f'When accessing site: {site}, the following exception occured: {e}')
        markCrawled(site)
logging.info(f'Crawler finished at {datetime.now()}')
    
