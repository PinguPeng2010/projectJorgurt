from zstandard import ZstdDecompressor
from pathlib import Path
import sqlite3
from multiprocessing import Queue
decompressor = ZstdDecompressor()

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "../data/jorgurt.db" 


# jorge stuff
# Each page needs to be taken from the db.
# Then decompress the page from pages table
# Parse the text from each page, and 