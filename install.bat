@echo off
REM Автоматическая установка всех зависимостей для бота
REM Travel With Alina - Production Setup

echo ===================================================
echo   TRAVEL WITH ALINA - Установка зависимостей
echo ===================================================
echo.

echo [1/3] Проверяем Python...
python --version
if errorlevel 1 (
    echo [!] Python не найден! Установите Python 3.8+
    pause
    exit /b 1
)

echo.
echo [2/3] Обновляем pip...
python -m pip install --upgrade pip

echo.
echo [3/3] Устанавливаем зависимости из requirements.txt...
pip install -r requirements.txt

echo.
echo ===================================================
echo   ✅ УСТАНОВКА ЗАВЕРШЕНА!
echo ===================================================
echo.
echo Теперь запустите бота:
echo   python start_production.bat
echo.
pause

