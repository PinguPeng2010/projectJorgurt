import requests
import sqlite3
from bs4 import BeautifulSoup as bs
from urllib.parse import urlparse
import logging
from datetime import datetime

disallowed: set = set() # dione by robots.txt
visited: set = set()
robots_cache: dict[str, str | None] = {}

seeds = ['https://bbc.co.uk']

visitable: set = set(seeds) # when i parse stuff, i put all urls in here, and try and got through the whole list until empty


# Visit seed 1. Find all links, then go through them all, until, no more. Then Seed 2

# Visit seed1/robots.txt. If exists, find all disallowed.
#Then start requesting, and adding to set.
# If one of the seeds is disallowed, remove.
# Then start visiting.


# log time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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


def init_db() -> None:
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


def save_url(url: str, title: str | None = None) -> None:
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


def get_db_visited_urls() -> set[str]:
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



def removeVisited(visited, visitable) -> set[str]:
    db_visited = get_db_visited_urls()
    combined = set(visited) | db_visited

    for visit in combined:
        if visit in visitable:
            visitable.discard(visit)

    return visitable

def findRobots(domain: str) -> str | None: # This is for the toplevel domain
    if not domain:
        return None

    parsed = urlparse(domain)
    host = f'{parsed.scheme}://{parsed.netloc}' if parsed.scheme and parsed.netloc else domain

    if host in robots_cache:
        return robots_cache[host]

    robotURL = host + '/robots.txt'
    try:
        response = requests.get(robotURL, timeout=10)
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


def getCode(site: str) -> tuple[str, str]:
    response = requests.get(site, timeout=10)
    return (str(response.status_code)[0], response.reason)


def markCrawled(site: str) -> bool:
    try:
        global visited, visitable
        visitable.discard(site)
        visited.add(site)
        logging.info(f'Crawled: Site: {site} was crawled')
        return True
    except Exception as e:
        logging.error(f'Site: {site} could not be marked as crawled: {e}')
        return False


def visitSite(site: str) -> str | None:
    try:
        response = requests.get(site, timeout=10)
        if response.status_code >= 400:
            raise RequestException(f'Site: {site} retuned a code of {response.status_code}: {response.reason}')

        if response.status_code >= 300:
            logging.warning(f'Redirect: Site: {site} ')
        return response.text

    except Exception:
        return None


def getAllLinks(html: str) -> set[str]: 
    parsedHtml = bs(html, 'html.parser')
    aTags = parsedHtml.find_all('a')
    links: set[str] = set()

    for tag in aTags:
        href = tag['href']
        links.add(str(href)) # Every link in the pagesssss

    return links


def getPageTitle(html: str) -> str | None:
    parsed_html = bs(html, 'html.parser')
    title_tag = parsed_html.title
    if title_tag and title_tag.string:
        return title_tag.string.strip()
    return None


def identifyIfDisallowed(disallowed: set[str], links: set[str]) -> set[str] | None:
    try:
        tempLinks: set = set()
        for link in disallowed:
            if link.endswith('/'):
                for l in links:
                    if not l.startswith(link):
                        tempLinks.add(l)
                links = tempLinks
                tempLinks: set = set()

            else:
                if link in links:
                    links.discard(link)
        return links

    except Exception as e:
        logging.error(f'Something went wrong with identifyDisallowed: {e}')



init_db()

while visitable:
    
    site = visitable.pop()
    print(site)
    try:
        robots = findRobots(site)
        disallowed = getDisallowed(robots, site)
        text = visitSite(site)
        title = getPageTitle(text) if text is not None else None
        if text is not None:
            links = getAllLinks(text)
            links = identifyIfDisallowed(disallowed, links)
            if links is not None:
                for link in links:
                    if not urlparse(link).fragment:
                        visitable.add(link)
                removeVisited(visited, visitable)
            save_url(site, title)
            markCrawled(site)
        else:
            save_url(site, title)
            markCrawled(site)
            raise EmptyResponse(f'Site: {site} provided an empty response')

    except EmptyResponse:
        pass
    except Exception as e:
        logging.error(f'When accessing site: {site}, the following exception occured: {e}')
        markCrawled(site)
    
