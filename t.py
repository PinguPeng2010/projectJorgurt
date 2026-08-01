import sqlite3

conn = sqlite3.connect("crawler.db")
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM urls")
print(cursor.fetchone())