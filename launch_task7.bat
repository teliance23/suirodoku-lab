@echo off
REM ============================================================
REM  Launch Task 7 Workers — 15 parallel processes
REM  Each worker handles ~59 orbits (884 total / 15)
REM  Logs go to dashboard + local files
REM ============================================================

set N_WORKERS=15

echo.
echo  ==========================================
echo   TASK 7: Launching %N_WORKERS% workers
echo   884 orbits / %N_WORKERS% = ~59 per worker
echo   Logs: log_task7_wNN.txt + dashboard
echo  ==========================================
echo.

REM Clear old worker checkpoints (comment out to resume)
echo Clearing old worker checkpoints...
del /q checkpoints\task7_w*.json 2>nul
del /q checkpoints\task7_w*.tmp 2>nul

REM Launch workers
for /L %%i in (0,1,14) do (
    echo Starting worker %%i / %N_WORKERS% ...
    start /min "Task7_W%%i" cmd /c "python worker_task7.py %%i %N_WORKERS% > log_task7_w%%i.txt 2>&1"
)

echo.
echo  All %N_WORKERS% workers launched!
echo  Monitor: type log_task7_w0.txt
echo  Dashboard: http://localhost:5000 (Task 7 logs)
echo  Workers endpoint: http://localhost:5000/api/task7/workers
echo.
pause
