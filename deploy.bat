@echo off
chcp 932 >nul 2>&1
cd /d %~dp0

echo ============================================
echo   Boatrace Gamagori AI - Deploy
echo ============================================
echo.

echo [1/3] Staging files...
git add app.py config.py scorer.py race_scraper.py backtester.py ml_optimizer.py result_tracker.py requirements.txt .gitignore .streamlit/config.toml _ml_result.json deploy.bat
if errorlevel 1 goto :ERROR_ADD
echo       OK
echo.

echo [2/3] Creating commit...
for /f "tokens=1-3 delims=/ " %%a in ('date /t') do set D=%%a-%%b-%%c
for /f "tokens=1-2 delims=: " %%a in ('time /t') do set T=%%a:%%b
git commit -m "deploy: %D% %T%"
echo.

echo [3/3] Pushing to GitHub...
git push origin master
if errorlevel 1 goto :ERROR_PUSH
echo       OK
echo.

echo ============================================
echo   Deploy complete!
echo   https://share.streamlit.io
echo   Repo: sasabe-reiya/boatrace-gamagori-ai
echo   Branch: master / File: app.py
echo ============================================
echo.
pause
exit /b 0

:ERROR_ADD
echo [ERROR] git add failed.
pause
exit /b 1

:ERROR_PUSH
echo [ERROR] git push failed. Check your network.
pause
exit /b 1
