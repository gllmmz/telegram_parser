@echo off
cd /d "%~dp0"

:loop
echo [%date% %time%] Starting bot.py >> bot.log
python -u bot.py >> bot.log 2>&1
echo [%date% %time%] bot.py exited, restarting in 5s... >> bot.log
timeout /t 5 /nobreak >nul
goto loop
