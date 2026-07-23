import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
import logging

disallowed: set = set() # dione by robots.txt
currentURL = '' # for the robots.txt. 

visited: set = set()
visitable: set = set() # when i parse stuff, i put all urls in here, and try and got through the whole list until empty

seeds = []


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


class RequestException(Exception): # Site returned an error code
    def __init__(self, message: str):
        super().__init__(message)
        logging.error(f'RequestException: {message}')

class UrlDisallowed(Exception): # For URLs disallowed by robots.txt
    def __init__(self, message: str):
        super().__init__(message)
        logging.error(f'UrlDisallowed: {message}')

class RobotsNotFound(Exception): # The site doesnt have robots.txt
    def __init__(self, message):
        super().__init__(message: str)
        logging.warning(f'RobotsNotFound: {message}')

def findRobots(site: str) -> str:
    try:
        if getCode(site)[0].startswith('4') or getCode(site)[0].startswith('5'):
            raise RobotsNotFound('Site: site has no robots.txt')
        

    except:
        pass


def getCode(site: str) -> tuple[str, str]:
    code = requests.get(site).status_code
    reason = requests.get(site).reason
    return (str(code), reason)

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
def visitSite(site: str) -> str:
    try:
        code = requests.get(site).status_code
        if str(code).startswith('4') or str(code).startswith('5'):
            raise RequestException(f'Site: {site} retuned a code of {code}: {requests.get(site).reason}')

        if str(code).startswith('3'):
            logging.warning(f'Redirect: Site: {site} ')
        req = requests.request('GET', site)
        text = req.text
        return text

    except:
        raise RequestException(f'Site: {site} could not be accessed, and returned code')

def getAllLinks(html: str) -> bool: # bool to say it did it or not
    parsedHtml = bs(html, 'html.parser')


def identifyIfDisallowed() -> bool: # also to say if it did it
    pass


print(visitSite('https://bbc.co.uk'))