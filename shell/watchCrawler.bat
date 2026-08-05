@echo off
set "DB=..\crawler.db"
set "LAST=0"

:loop
for /f %%A in ('py -m sqlite3 "%DB%" "SELECT COUNT(*) FROM urls;"') do set "COUNT=%%A"

set /a RATE=COUNT-LAST

for /f "tokens=1-3 delims=: " %%A in ("%time%") do (
    set "TIME=%%A:%%B:%%C"
)

echo %TIME%  URLs: %COUNT%  (+%RATE% in last 2s)

set "LAST=%COUNT%"

timeout /t 2 /nobreak >nul

goto loop