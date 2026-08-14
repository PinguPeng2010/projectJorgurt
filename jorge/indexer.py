from zstandard import ZstdDecompressor
from pathlib import Path

decompressor = ZstdDecompressor()

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "../data/gurt.db" 


def getUrls():
    