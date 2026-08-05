#!/bin/bash

DB="../crawler.db"
LAST=0

while true; do
    COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM urls;")
    RATE=$((COUNT - LAST))
    echo "$(date '+%H:%M:%S')  URLs: $COUNT  (+$RATE in last 2s)"
    LAST=$COUNT
    sleep 2
done