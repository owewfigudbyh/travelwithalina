@echo off
REM Скрипт запуска бота в production режиме (Windows)
REM Для 500+ пользователей

echo ===================================================
echo   TRAVEL WITH ALINA - Production Server
echo   Оптимизировано для 500+ пользователей
echo ===================================================
echo.

REM Проверка установки waitress
python -c "import waitress" 2>nul
if errorlevel 1 (
    echo [!] Устанавливаем waitress...
    pip install waitress
)

echo [OK] Запускаем production сервер...
echo.
echo Параметры:
echo   - Threads: 16 (одновременных запросов)
echo   - Port: 5000
echo   - Host: 0.0.0.0 (все интерфейсы)
echo.

waitress-serve --host=0.0.0.0 --port=5000 --threads=16 --channel-timeout=120 app:app

