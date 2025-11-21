@echo off
echo ========================================
echo   Starting Cloud P2P Backend Servers
echo ========================================
echo.

:: Set RUST_LOG for logging
set RUST_LOG=info

:: Kill any existing servers on these ports
echo Cleaning up old processes...
for /f "tokens=5" %%a in ('netstat -aon ^| find ":8001" ^| find "LISTENING"') do taskkill /F /PID %%a >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| find ":8002" ^| find "LISTENING"') do taskkill /F /PID %%a >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| find ":8003" ^| find "LISTENING"') do taskkill /F /PID %%a >nul 2>&1

timeout /t 2 /nobreak >nul

echo.
echo Starting Server 1 on port 8001...
start "Server-1" cmd /k "cargo run --bin server -- 8001 server1 127.0.0.1:8002 127.0.0.1:8003"

timeout /t 3 /nobreak

echo Starting Server 2 on port 8002...
start "Server-2" cmd /k "cargo run --bin server -- 8002 server2 127.0.0.1:8001 127.0.0.1:8003"

timeout /t 3 /nobreak

echo Starting Server 3 on port 8003...
start "Server-3" cmd /k "cargo run --bin server -- 8003 server3 127.0.0.1:8001 127.0.0.1:8002"

echo.
echo ========================================
echo   All servers started!
echo   - Server 1: localhost:8001
echo   - Server 2: localhost:8002
echo   - Server 3: localhost:8003
echo ========================================
echo.
echo Press any key to stop all servers...
pause >nul

:: Stop all servers
echo Stopping servers...
taskkill /FI "WINDOWTITLE eq Server-1" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Server-2" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Server-3" /F >nul 2>&1

echo All servers stopped.
pause