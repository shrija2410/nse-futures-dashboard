@echo off
setlocal
cd /d "%~dp0"
title Live NSE Stock Futures Dashboard V6
color 0A
echo ============================================================
echo   LIVE NSE STOCK FUTURES DASHBOARD V6
echo ============================================================
echo.
set "PYEXE="
for /f "tokens=2,*" %%A in ('reg query "HKCU\Software\Python\PythonCore" /s /v ExecutablePath 2^>nul ^| findstr /i "ExecutablePath"') do if not defined PYEXE set "PYEXE=%%B"
if not defined PYEXE if exist "%LocalAppData%\Programs\Python\Python313\python.exe" set "PYEXE=%LocalAppData%\Programs\Python\Python313\python.exe"
if not defined PYEXE if exist "%LocalAppData%\Programs\Python\Python312\python.exe" set "PYEXE=%LocalAppData%\Programs\Python\Python312\python.exe"
if not defined PYEXE if exist "%LocalAppData%\Programs\Python\Python311\python.exe" set "PYEXE=%LocalAppData%\Programs\Python\Python311\python.exe"
if not defined PYEXE if exist "%ProgramFiles%\Python313\python.exe" set "PYEXE=%ProgramFiles%\Python313\python.exe"
if not defined PYEXE (
 echo Python was not found.
 echo Please install Python 3.11+ from python.org, then run this file again.
 pause
 exit /b 1
)
echo Python: %PYEXE%
"%PYEXE%" --version
echo.
echo No third-party Python packages are required.
echo Starting dashboard. First NSE refresh may take 20-60 seconds...
echo Keep this window open while using the dashboard.
echo.
"%PYEXE%" server.py
if errorlevel 1 (
 echo.
 echo Dashboard stopped with an error. See the message above.
)
pause
