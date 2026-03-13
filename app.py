#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Facebook Bot для Travel With Alina
Автоматический помощник для подбора туров с ИИ (OpenAI)
"""

import os
# Принудительная UTF-8 кодировка для Windows консоли
os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

import re
import json
import logging
import asyncio
import aiohttp
import requests as req_lib
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from enum import Enum
from dotenv import load_dotenv
import pytz  # Для работы с часовыми поясами

# Загружаем .env
load_dotenv()

# Импорт TravelMarket парсера
try:
    from travelmarket_parser import travelmarket
except ImportError:
    travelmarket = None

# Импорт FlightPowers API
try:
    from flightpowers_parser import flightpowers
except ImportError:
    flightpowers = None

# Импорт единого селектора туров
try:
    from tour_selector import TourSelector as _TourSelector
    _tour_selector = _TourSelector()
    TOUR_SELECTOR_AVAILABLE = True
except ImportError:
    _tour_selector = None
    TOUR_SELECTOR_AVAILABLE = False


# ============================================================================
# TOURVISOR API - реальные запросы к отелям
# ============================================================================

TOURVISOR_LOGIN = os.getenv('TOURVISOR_LOGIN', 'travelwithalina.online@gmail.com')
TOURVISOR_PASSWORD = os.getenv('TOURVISOR_PASSWORD', 'ntr65XZVFyzc')
TV_BASE_URL = "http://tourvisor.ru/xml"

# ID регионов TourVisor
TV_REGIONS = {
    "sharm":    "6",   # Шарм-Эль-Шейх
    "hurghada": "5",   # Хургада + Эль Гуна
    "turkey":   None,  # Вся Турция (несколько регионов)
    "antalya":  "4",   # Анталья регион (страна 4=Турция)
    "spain":    None,  # Вся Испания (несколько регионов)
}

# ID туроператоров TourVisor (проверено через API):
# 125=Join UP!, 13=Anex, 11=Coral, 23=Russian Express, 90=Kompas(KZ), 92=Meridian Express
# 12=Pegas НЕ включаем
TV_OPERATORS_EGYPT  = "125,13,11,23,90"
TV_OPERATORS_TURKEY = "125,13,11,23,90"
TV_OPERATORS_SPAIN  = "125,13,11,23,90"

# ============================================================================
# ОБЯЗАТЕЛЬНЫЕ ОТЕЛИ ЕГИПТА (только эти показываем пользователю)
# ============================================================================

EGYPT_REQUIRED_SHARM = [
    # Полный список отелей Шарм-Эль-Шейх
    "The Grand Hotel Sharm El Sheikh",
    "Grand Hotel Sharm El Sheikh",
    "Sharm Grand Plaza Resort",
    "Aurora Oriental Resort",
    "Oriental Resort",
    "Parrotel Beach Resort",
    "Radisson Blu Resort",
    "Island View Resort",
    "Sunrise Island View Resort",
    "Dreams Beach",
    "Amphoras Beach",
    "Naama Bay Hotel & Resort",
    "Tropitel Naama Bay",
    "Jaz Mirabel Park",
    "Jaz Mirabel Park & Club",
    "Naama Bay Promenade Beach Resort",
    "Naama Bay Promenade",
    "Sharm El Sheikh Marriott Red Sea Resort",
    "Marriott Red Sea Resort",
    "Xperience Sea Breeze Resort",
    "Sea Breeze Resort",
    "Jaz Belvedere",
    "Jaz Mirabel Beach",
    "V Hotel Sharm El Sheikh",
    "Pyramisa Beach Resort Sharm El Sheikh",
    "Nubian Island",
    "Nubian Village",
    "Maritim Jolie Ville Resort & Casino",
    "Maritim Jolie Ville",
    "Reef Oasis Blue Bay",
    "Park Regency Sharm El Sheikh",
    "Hyatt Regency Sharm",
    "Park Regency",
    "Sunrise Grand Select Montemare Resort",
    "Sunrise Grand Select Montemare",
    "Montemare",
    "Sunrise Diamond Beach Resort",
    "Sunrise Diamond Beach",
    "Sultan Gardens Resort",
    "Grand Rotana Resort & Spa",
    "Grand Rotana Resort",
    "Savoy Sharm El Sheikh",
    "Stella Di Mare Beach Hotel & Spa",
    "Stella Di Mare Beach",
    "Steigenberger Alcazar",
    "Rixos Premium Seagate",
    "Rixos Sharm",
    "Rixos Radamis",
    "Sunrise White Hills Resort",
    "Sunrise White Hills",
    "Meraki",
    "Sheraton Sharm Main Building",
    "Sheraton Sharm",
    "Reef Oasis Beach Resort",
]

EGYPT_REQUIRED_HURGHADA = [
    # Полный список отелей Хургада + Эль Гуна
    "Rewaya Inn Resort",
    "Hawaii Paradise Aqua Park Resort",
    "Hawaii Paradise Aqua Park",
    "Amwaj Beach Club Abu Soma",
    "Amwaj Beach Club",
    "Pickalbatros Beach Club",
    "Royal Lagoons Resort & Aqua Park",
    "Royal Lagoons Resort",
    "Amc Royal Hotel",
    "AMC Royal Hotel",
    "Rewaya Majestic Resort",
    "Serenity Alpha Beach",
    "Serenity Makadi Beach",
    "Titanic Beach Spa & Aqua Park",
    "Titanic Beach Spa",
    "Stella Makadi Beach Resort & Spa",
    "Stella Makadi Beach Resort",
    "Titanic Palace Resort & Spa",
    "Titanic Palace Resort",
    "Desert Rose Resort",
    "Hilton Plaza",
    "The V Luxury Resort",
    "V Luxury Resort",
    "Sunrise Sentido Mamlouk Palace Resort",
    "Mamlouk Palace Resort",
    "Sentido Mamlouk",
    "Xanadu Makadi Bay",
    "Titanic Royal Hotel",
    "Sunrise Royal Makadi Resort",
    "Sunrise Royal Makadi",
    "Steigenberger Al Dau Beach",
    "Steigenberger Ras Soma",
    "Rixos Premium Magawish",
    "Serenity Alma Resort",
    "Serenity Fun City",
    "Movenpick Resort & Spa El Gouna",
    "Movenpick El Gouna",
    "Movenpick Spa El Gouna",
    "Creek Hotel & Residences El Gouna",
    "Creek Hotel El Gouna",
    "Panorama Bungalows El Gouna",
    "Panorama Bungalows",
    "Sheraton Miramar Resort",
    "Sheraton Miramar",
    "Doubletree By Hilton Mangroovy El Gouna Resort",
    "Doubletree Mangroovy El Gouna",
    "Doubletree Mangroovy",
    "Mangroovy El Gouna",
]


def _is_required_egypt_hotel(hotel_name: str, required_list: list) -> bool:
    """Проверяет, входит ли отель в список обязательных (нечёткое совпадение)"""
    import re as _re
    def _clean(s):
        s = s.lower()
        s = _re.sub(r'\s*\(ex\..*?\)', '', s, flags=_re.IGNORECASE)
        s = _re.sub(r'\s*\(ex\s+.*?\)', '', s, flags=_re.IGNORECASE)
        s = _re.sub(r'\d+\*', '', s)
        s = _re.sub(r'[^a-zа-яё0-9\s]', ' ', s)
        s = _re.sub(r'\s+', ' ', s).strip()
        return s

    cleaned_name = _clean(hotel_name)
    for req in required_list:
        cleaned_req = _clean(req)
        # Совпадение если одно входит в другое (минимум 8 символов совпадения)
        if len(cleaned_req) >= 8 and len(cleaned_name) >= 8:
            if cleaned_req in cleaned_name or cleaned_name in cleaned_req:
                return True
        # Проверяем первые 3 слова
        words_req = cleaned_req.split()[:3]
        words_name = cleaned_name.split()[:3]
        if len(words_req) >= 2 and words_req == words_name:
            return True
    return False


# Трансфер (EUR, фикс)
TRANSFER_PRICES = {
    "sharm": 420, "hurghada": 420,
    "turkey": 490, "antalya": 490,
    "spain": 600, "barcelona": 600, "mallorca": 530, "tenerife": 530,
}


# ============================================================================
# КЭШИРОВАНИЕ ИМЕН ПОЛЬЗОВАТЕЛЕЙ
# ============================================================================

# Словарь для хранения имен пользователей: {user_id: {name, first_name, last_name}}
USER_NAMES_CACHE = {}

def save_user_name(user_id: str, name: str = None, first_name: str = None, last_name: str = None):
    """Сохраняет имя пользователя в кэш"""
    if name or first_name or last_name:
        USER_NAMES_CACHE[user_id] = {
            'name': name or first_name,
            'first_name': first_name,
            'last_name': last_name
        }
        logger.info(f"[Cache] 💾 Сохранено имя для {user_id}: {name or first_name}")

def get_user_name(user_id: str) -> dict:
    """Получает имя из кэша"""
    return USER_NAMES_CACHE.get(user_id, {})


def extract_name_from_message(message: str) -> str:
    """
    Пытается извлечь имя пользователя из сообщения.
    Ищет паттерны: "меня зовут", "я", "мое имя", "это" + имя
    """
    message_lower = message.lower().strip()
    
    # Паттерны на разных языках
    patterns = [
        r'(?:меня зовут|я)\s+([А-ЯЁA-Z][а-яёa-z]{2,15})',
        r'(?:мене звати|мене)\s+([А-ЯЁA-Z][а-яёa-z]{2,15})',
        r'(?:my name is|i am|i\'m)\s+([A-Z][a-z]{2,15})',
        r'(?:me llamo|soy)\s+([A-Z][a-z]{2,15})',
        r'^([А-ЯЁA-Z][а-яёa-z]{2,15})$',  # Просто имя одним словом
    ]
    
    for pattern in patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            name = match.group(1).capitalize()
            # Проверяем что это не служебное слово или команда
            blacklist = [
                'да', 'нет', 'хочу', 'буду', 'могу', 'можна', 'так', 'yes', 'no',
                'начать', 'старт', 'start', 'почати', 'привет', 'hello', 'hi',
                'здравствуйте', 'добрый', 'good', 'вітаю', 'уже',  # уже - не имя!
            ]
            if name.lower() not in blacklist:
                return name
    
    return ""


# ============================================================================
# ПРОВЕРКА ВРЕМЕНИ СУТОК
# ============================================================================

def is_night_time() -> bool:
    """
    Проверяет, является ли текущее время ночным (23:00 - 08:00).
    Используется европейское время (Copenhagen/Kiev timezone).
    """
    try:
        # Используем европейский часовой пояс (Copenhagen для большинства клиентов)
        europe_tz = pytz.timezone('Europe/Copenhagen')
        now = datetime.now(europe_tz)
        hour = now.hour
        
        # Ночное время: с 23:00 до 08:00
        is_night = hour >= 23 or hour < 8
        
        if is_night:
            logger.info(f"🌙 Ночное время: {now.strftime('%H:%M')} (Copenhagen)")
        
        return is_night
    except Exception as e:
        logger.error(f"❌ Ошибка проверки времени: {e}")
        return False  # В случае ошибки - не считаем ночью


# ============================================================================
# TELEGRAM УВЕДОМЛЕНИЯ ДЛЯ МЕНЕДЖЕРОВ
# ============================================================================

TELEGRAM_BOT_TOKEN = "8437409742:AAHX8mZHW2iiOBxWm1o_H2NmuAb4IU9d6YQ"
TELEGRAM_GROUP_ID = "-5218779320"

def send_telegram_notification(user_id: str, user_message: str, bot_response: str = ""):
    """Отправляет уведомление в Telegram группу менеджеров"""
    try:
        text = f"🔔 <b>ВОПРОС ОТ КЛИЕНТА</b>\n\n"
        text += f"👤 User ID: <code>{user_id}</code>\n"
        text += f"💬 Сообщение: {user_message}\n"
        if bot_response:
            text += f"\n🤖 Ответ бота: {bot_response[:200]}"
        text += f"\n\n⚠️ Требуется внимание менеджера!"

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_GROUP_ID,
            "text": text,
            "parse_mode": "HTML"
        }
        r = req_lib.post(url, json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"[Telegram] ✅ Уведомление отправлено для user {user_id}")
        else:
            logger.error(f"[Telegram] ❌ Ошибка отправки: {r.status_code}")
    except Exception as e:
        logger.error(f"[Telegram] ❌ Исключение: {e}")


def _tv_request(method: str, params: dict) -> dict:
    """Синхронный запрос к TourVisor API"""
    params['format'] = 'json'
    params['authlogin'] = TOURVISOR_LOGIN
    params['authpass'] = TOURVISOR_PASSWORD
    url = f"{TV_BASE_URL}{method}"
    try:
        r = req_lib.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.error(f"[TV] request error: {e}")
    return {}


def _tv_search_hotels(country_id: str, region_id: str, date_from: str,
                      nights: int, adults: int, children: int = 0,
                      operators: str = TV_OPERATORS_EGYPT,
                      destination_key: str = "egypt") -> List[dict]:
    """
    Делает реальный запрос к TourVisor и возвращает список отелей с ценами.
    Возвращает список dict: {name, stars, region, price_usd, price_eur, meal, operator, tour_id, flydate}
    """
    logger.info(f"[TV] ══════════════════════════════════════════════")
    logger.info(f"[TV] 🔍 Запрос отелей: country={country_id}, region={region_id}")
    logger.info(f"[TV] 📅 Дата: {date_from}, ночей={nights}, взрослых={adults}")

    search_params = {
        'departure': '99',          # Без перелёта
        'country': country_id,
        'datefrom': date_from,
        'dateto': date_from,
        'nightsfrom': nights,
        'nightsto': nights,
        'adults': adults,
        'stars': 5,
        'rating': 4,
        'mealbetter': 5,            # AI и лучше
        'operator': operators,
        'currency': 1,              # USD (потом конвертируем)
    }

    if children:
        search_params['child'] = children

    if region_id:
        search_params['regions'] = region_id

    # Для Турции добавляем несколько регионов
    if destination_key == "turkey":
        search_params['regions'] = "4,12,11,14,13,5,3"  # Анталья, Кемер, Аланья, Сиде, Белек, Бодрум, Мармарис
        search_params.pop('stars', None)  # убираем stars — добавляем starsbetter
        search_params['starsbetter'] = 4

    if destination_key == "tenerife":
        # Тенерифе: starsbetter=3 = 3* и выше (stars+starsbetter=3 = только 3*)
        # НЕ фильтруем питание в API — делаем вручную ниже
        search_params.pop('stars', None)
        search_params['starsbetter'] = 3
        search_params.pop('mealbetter', None)
        search_params.pop('rating', None)
        search_params.pop('operator', None)  # берём всех, фильтруем ниже

    if destination_key == "spain":
        search_params['regions'] = "38,37,41,42"       # Коста Брава, Коста Дорада, Майорка, Тенерифе
        search_params.pop('mealbetter', None)
        search_params['meal'] = 3                       # HB (полупансион)
        search_params.pop('stars', None)
        search_params['starsbetter'] = 3

    # Создаём запрос
    result = _tv_request('/search.php', search_params)
    if 'error' in result:
        logger.error(f"[TV] Ошибка: {result.get('error')}")
        return []
    if 'result' not in result or 'requestid' not in result.get('result', {}):
        logger.error(f"[TV] Нет requestid: {result}")
        return []

    request_id = result['result']['requestid']
    logger.info(f"[TV] RequestID: {request_id} — ожидаем результаты...")

    total_found = 0
    hotels_raw = []
    # Ждём завершения — до 30 попыток
    for attempt in range(30):
        time.sleep(2)
        status_result = _tv_request('/result.php', {'requestid': request_id, 'onpage': 200, 'page': 1})
        if 'data' not in status_result:
            continue
        data = status_result['data']
        state = data.get('status', {}).get('state', '')
        found = data.get('status', {}).get('hotelsfound', 0)
        progress = data.get('status', {}).get('progress', 0)
        logger.info(f"[TV] Прогресс: {progress}% | {state} | найдено: {found}")
        if state == 'finished':
            total_found = found
            hotels_raw = data.get('result', {}).get('hotel', [])
            if isinstance(hotels_raw, dict):
                hotels_raw = [hotels_raw]
            break

    # Пагинация — берём все страницы
    if total_found > 200:
        p2 = _tv_request('/result.php', {'requestid': request_id, 'onpage': 200, 'page': 2})
        page2 = p2.get('data', {}).get('result', {}).get('hotel', [])
        if isinstance(page2, dict):
            page2 = [page2]
        hotels_raw += page2
        logger.info(f"[TV] Страница 2: +{len(page2)} отелей")

    if total_found > 400:
        p3 = _tv_request('/result.php', {'requestid': request_id, 'onpage': 200, 'page': 3})
        page3 = p3.get('data', {}).get('result', {}).get('hotel', [])
        if isinstance(page3, dict):
            page3 = [page3]
        hotels_raw += page3
        logger.info(f"[TV] Страница 3: +{len(page3)} отелей")

    logger.info(f"[TV] ✅ Завершено! Получено {len(hotels_raw)} отелей (всего найдено: {total_found})")
    return _parse_tv_hotels(hotels_raw)


def _get_eur_rate() -> float:
    """Получает текущий курс USD/EUR из API"""
    try:
        r = req_lib.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
        rates = r.json().get('rates', {})
        return rates.get('EUR', 0.92)
    except Exception:
        return 0.92


def _parse_tv_hotels(hotels_raw) -> List[dict]:
    """Парсит сырые данные отелей из TourVisor"""
    # Если API вернул один отель как dict — оборачиваем в список
    if isinstance(hotels_raw, dict):
        hotels_raw = [hotels_raw]
    if not hotels_raw:
        return []

    usd_to_eur = _get_eur_rate()
    result = []
    for hotel in hotels_raw:
        tours_data = hotel.get('tours', {}).get('tour', [])
        # Если один тур — dict, оборачиваем в список
        if isinstance(tours_data, dict):
            tours_data = [tours_data]
        if not tours_data:
            continue

        # Фильтруем только нужных операторов
        ALLOWED_OPERATOR_CODES = {'125', '13', '11', '23', '90', '92'}

        def is_allowed_operator(tour_item):
            op_code = str(tour_item.get('operatorcode', ''))
            return op_code in ALLOWED_OPERATOR_CODES

        allowed_tours = [t for t in tours_data if is_allowed_operator(t)]
        # Если нет туров от нужных операторов — пропускаем этот отель
        if not allowed_tours:
            continue
        use_tours = allowed_tours

        # priceue — цена ТОЛЬКО ОТЕЛЯ в USD (без перелёта)
        # price — полная цена тура WITH перелётом (не используем)
        def _get_hotel_price(tour):
            p = tour.get('priceue', 0) or 0
            if p <= 0:
                # fallback: берём price и вычитаем примерную стоимость перелёта
                p_full = tour.get('price', 0) or 0
                p = max(0, p_full - 500)  # грубый fallback
            return p

        min_hotel_price = min((_get_hotel_price(t) for t in use_tours), default=0)
        if min_hotel_price <= 0:
            continue
        best_tour = min(use_tours, key=lambda t: _get_hotel_price(t))

        price_usd = _get_hotel_price(best_tour)
        # TourVisor с currency=1 → priceue в USD
        price_eur = round(price_usd * usd_to_eur)

        tour_id = best_tour.get('tourid', '')
        result.append({
            'name': hotel.get('hotelname', ''),
            'stars': hotel.get('hotelstars', 5),
            'region': hotel.get('regionname', ''),
            'price_usd': price_usd,
            'price_eur': price_eur,
            'meal': best_tour.get('meal', 'AI'),
            'operator': best_tour.get('operatorname', ''),
            'tour_id': tour_id,
            'flydate': best_tour.get('flydate', ''),
            'nights': best_tour.get('nights', 7),
        })
    result.sort(key=lambda x: x['price_eur'])
    return result

# ═══════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ (совместимо с Vercel и локальным запуском)
# ═══════════════════════════════════════════════════════════════

import sys as _sys

# Консольный хэндлер с правильной кодировкой для Windows
if hasattr(_sys.stdout, 'reconfigure'):
    try:
        _sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(_sys.stdout)  # Всегда пишем в stdout
    ]
)

# Дополнительно: если НЕ на Vercel, добавляем файловый handler
if not os.environ.get('VERCEL') and not os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    try:
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        file_handler = logging.FileHandler(
            os.path.join(log_dir, 'facebook_bot.log'),
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
        logging.info("✅ Логирование в файл включено")
    except Exception as e:
        # Игнорируем ошибки создания файла на read-only системах
        logging.info(f"⚠️ Логирование только в консоль: {e}")
else:
    # На Vercel используем /tmp (единственная доступная для записи папка)
    try:
        log_dir = '/tmp/logs'
        os.makedirs(log_dir, exist_ok=True)
        
        file_handler = logging.FileHandler(
            os.path.join(log_dir, 'facebook_bot.log'),
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
        logging.info("✅ Логирование в /tmp/logs (Vercel)")
    except Exception as e:
        logging.info(f"⚠️ Логирование только в консоль: {e}")
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
        logging.info("✅ Логирование в файл включено")
    except Exception as e:
        # Игнорируем ошибки создания файла на read-only системах
        logging.info(f"⚠️ Логирование только в консоль (файл недоступен): {e}")

logging.info("🚀 Logging initialized")


import io as _io
_stream = _io.TextIOWrapper(_sys.stdout.buffer, encoding='utf-8', errors='replace') if hasattr(_sys.stdout, 'buffer') else _sys.stdout
_console_handler = logging.StreamHandler(_stream)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Настройка базового логирования (только консоль)
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        _console_handler,  # Только консоль в basicConfig
    ]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Добавляем файловый handler в зависимости от окружения
if os.environ.get('VERCEL') or os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    # На Vercel/AWS Lambda используем /tmp (единственная доступная для записи папка)
    try:
        log_dir = '/tmp/logs'
        os.makedirs(log_dir, exist_ok=True)
        
        _file_handler = logging.FileHandler(
            os.path.join(log_dir, 'facebook_bot.log'),
            encoding='utf-8'
        )
        _file_handler.setLevel(logging.INFO)
        _file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(_file_handler)
        logger.info("✅ Логирование в /tmp/logs (Vercel/Lambda)")
    except Exception as e:
        logger.info(f"⚠️ Файловое логирование недоступно: {e}")
else:
    # Локально используем ./logs
    try:
        log_dir = 'logs'
        os.makedirs(log_dir, exist_ok=True)
        
        _file_handler = logging.FileHandler(
            os.path.join(log_dir, 'facebook_bot.log'),
            encoding='utf-8'
        )
        _file_handler.setLevel(logging.INFO)
        _file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(_file_handler)
        logger.info("✅ Логирование в ./logs")
    except Exception as e:
        logger.info(f"⚠️ Файловое логирование недоступно: {e}")

logger.info("🚀 Logging initialized")

# API ключи
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv('FACEBOOK_PAGE_ACCESS_TOKEN', '')
FACEBOOK_VERIFY_TOKEN = os.getenv('FACEBOOK_VERIFY_TOKEN', 'travel_with_alina_bot')


def check_conversation_history(user_id: str) -> bool:
    """
    Проверяет есть ли история переписки у пользователя через Facebook Graph API.
    
    Возвращает:
        True - если это НОВЫЙ пользователь (нет старых сообщений ИЛИ последнее >2 недель назад)
        False - если есть недавняя история (старый пользователь, писал недавно)
    
    ЛОГИКА:
    - Нет сообщений → НОВЫЙ (True)
    - Последнее сообщение >2 недель назад → НОВЫЙ (True)  
    - Последнее сообщение <2 недель назад → СТАРЫЙ (False)
    """
    try:
        if not FACEBOOK_PAGE_ACCESS_TOKEN:
            logger.warning("⚠️ PAGE_ACCESS_TOKEN не установлен, пропускаем проверку истории")
            return True  # По умолчанию считаем новым если токена нет

        # Запрос к Graph API для получения истории сообщений
        url = f"https://graph.facebook.com/v22.0/{user_id}/conversations"
        params = {
            "access_token": FACEBOOK_PAGE_ACCESS_TOKEN,
            "fields": "messages.limit(5){created_time,from,message}",
        }

        resp = req_lib.get(url, params=params, timeout=5)
        if resp.status_code != 200:
            logger.warning(f"⚠️ Facebook API error {resp.status_code}: {resp.text[:200]}")
            return True  # При ошибке считаем новым

        data = resp.json()
        conversations = data.get("data", [])

        # Если нет переписок - это точно новый пользователь
        if not conversations:
            logger.info(f"✅ [{user_id}] Нет истории переписок - НОВЫЙ пользователь")
            return True

        # Проверяем последние 5 сообщений в первой переписке
        first_conv = conversations[0]
        messages_data = first_conv.get("messages", {}).get("data", [])

        if not messages_data:
            logger.info(f"✅ [{user_id}] Нет сообщений в переписке - НОВЫЙ пользователь")
            return True

        # Ищем САМОЕ СТАРОЕ сообщение из последних
        current_time = datetime.now()
        oldest_message_age_days = 0
        
        for msg in messages_data:
            try:
                created_time = msg.get("created_time", "")
                msg_time = datetime.strptime(created_time, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
                age_days = (current_time - msg_time).total_seconds() / (60 * 60 * 24)
                
                if age_days > oldest_message_age_days:
                    oldest_message_age_days = age_days
                    
            except Exception as e:
                logger.warning(f"⚠️ Ошибка парсинга времени сообщения: {e}")
                continue

        # ПРАВИЛО: Если последнее сообщение старше 14 дней (2 недели) - считаем НОВЫМ клиентом
        INACTIVE_DAYS_THRESHOLD = 14  # 2 недели
        
        if oldest_message_age_days > INACTIVE_DAYS_THRESHOLD:
            logger.info(f"✅ [{user_id}] Последнее сообщение {oldest_message_age_days:.0f} дней назад (>{INACTIVE_DAYS_THRESHOLD} дней) - считаем НОВЫМ")
            return True
        elif oldest_message_age_days > 0.003:  # >5 минут
            logger.info(f"❌ [{user_id}] Последнее сообщение {oldest_message_age_days:.1f} дней назад (<{INACTIVE_DAYS_THRESHOLD} дней) - СТАРЫЙ клиент")
            return False
        else:
            logger.info(f"✅ [{user_id}] Все сообщения свежие - НОВЫЙ пользователь")
            return True

    except Exception as e:
        logger.error(f"❌ Ошибка проверки истории для {user_id}: {e}")
        return True  # При ошибке считаем новым


def get_facebook_user_info(user_id: str) -> dict:
    """
    Пытается получить информацию о пользователе несколькими способами:
    1. Из кэша (если сохранено ранее)
    2. Messenger Profile API (first_name) - иногда работает
    3. Извлечение имени из текста сообщений
    
    Возвращает: {name, first_name, last_name} или {}
    """
    try:
        # Способ 1: Проверяем в кэше
        cached = get_user_name(user_id)
        if cached and cached.get('name'):
            logger.debug(f"[FB] 💾 Имя из кэша: {cached['name']}")
            return cached
        
        # Способ 2: Пробуем Messenger Profile API
        if not FACEBOOK_PAGE_ACCESS_TOKEN:
            return {}
        
        # Пробуем получить через Graph API
        url = f"https://graph.facebook.com/v22.0/{user_id}"
        params = {
            "fields": "first_name,last_name,name",
            "access_token": FACEBOOK_PAGE_ACCESS_TOKEN
        }
        
        try:
            response = req_lib.get(url, params=params, timeout=3)
            if response.status_code == 200:
                data = response.json()
                if data.get('first_name') or data.get('name'):
                    name = data.get('name') or data.get('first_name', '')
                    logger.info(f"[FB] ✅ Получено имя через API: {name}")
                    
                    # Сохраняем в кэш
                    save_user_name(
                        user_id, 
                        name,
                        data.get('first_name', ''),
                        data.get('last_name', '')
                    )
                    return data
            else:
                # Не логируем ошибку, это нормально для Page-Scoped ID
                logger.debug(f"[FB] API недоступен для {user_id}, используем ID")
        except:
            pass
        
        # Возвращаем пустой dict - это не критично
        return {}
        
    except Exception as e:
        logger.error(f"[FB] ❌ Ошибка получения информации: {e}")
        return {}


def send_telegram_notification(user_id: str, message: str, user_name: str = None):
    """Отправляет уведомление в Telegram группу о вопросе от клиента"""
    try:
        # Если имя не передано, получаем из Facebook
        if not user_name:
            user_info = get_facebook_user_info(user_id)
            user_name = user_info.get('name') or user_info.get('first_name', '')
            if user_info.get('last_name'):
                user_name = f"{user_name} {user_info.get('last_name')}"
        
        # Формируем отображаемое имя
        if user_name and user_name.strip():
            user_display = f"👤 {user_name}"
        else:
            user_display = f"👤 Пользователь (ID: {user_id})"
        
        notification_text = (
            f"🔔 <b>ВОПРОС ОТ КЛИЕНТА</b>\n\n"
            f"{user_display}\n"
            f"💬 Сообщение: <i>{message}</i>\n\n"
            f"📎 ID: <code>{user_id}</code>\n"
            f"🕒 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_GROUP_ID,
            "text": notification_text,
            "parse_mode": "HTML"
        }

        response = HTTP_SESSION.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            logger.info(f"[Telegram] ✅ Уведомление отправлено для {user_id}")
        else:
            logger.error(f"[Telegram] ⚠️ Ошибка отправки: {response.status_code}")
    except Exception as e:
        logger.error(f"[Telegram] ❌ Ошибка отправки: {e}")


async def analyze_user_intent(message: str) -> dict:
    """
    Анализирует намерение пользователя с помощью OpenAI API.
    
    Возвращает:
        {
            "is_travel_related": bool,  # True если вопрос связан с бронированием туров/полетов
            "intent": str,               # "booking" | "other" | "greeting"
            "confidence": float,         # Уверенность в анализе (0.0-1.0)
            "language": str              # "ru" | "en" | "da" | "sv" и т.д.
        }
    """
    try:
        if not OPENAI_API_KEY:
            logger.warning("⚠️ OPENAI_API_KEY не установлен, пропускаем анализ намерения")
            # По умолчанию считаем что это о путешествиях
            return {"is_travel_related": True, "intent": "booking", "confidence": 0.5, "language": "ru"}

        system_prompt = """Ты помощник турагентства, который анализирует намерения и язык пользователя.

Определи:
1. Связано ли сообщение с НОВЫМ бронированием тура/путешествия (вопросы о ценах, направлениях, датах, отелях)
2. Или это ДРУГОЙ вопрос (о существующей брони, документах, оплате, возврате, проблемах, встрече в аэропорту)
3. Язык сообщения (ru=русский, uk=украинский, en=английский, pl=польский, da=датский, sv=шведский, no=норвежский, de=немецкий, fi=финский)

Ответь ТОЛЬКО в формате JSON:
{
    "is_travel_related": true/false,
    "intent": "booking" или "other" или "greeting",
    "confidence": 0.0-1.0,
    "language": "ru" или "uk" или "en" или "pl" или "da" и т.д.
}

ПРИМЕРЫ:

Сообщение: "Hi, I want to go to Turkey in June"
→ {"is_travel_related": true, "intent": "booking", "confidence": 0.95, "language": "en"}

Сообщение: "Gdzie są moje dokumenty?"
→ {"is_travel_related": false, "intent": "other", "confidence": 0.9, "language": "pl"}

Сообщение: "Хочу слетать в Египет на двоих, бюджет 3000 евро"
→ {"is_travel_related": true, "intent": "booking", "confidence": 0.98, "language": "ru"}

Сообщение: "Хочу полетіти в Туреччину з сім'єю"
→ {"is_travel_related": true, "intent": "booking", "confidence": 0.95, "language": "uk"}

Сообщение: "Привіт, коли отримаю ваучер?"
→ {"is_travel_related": false, "intent": "other", "confidence": 0.9, "language": "uk"}

Сообщение: "When will I get my voucher?"
→ {"is_travel_related": false, "intent": "other", "confidence": 0.9, "language": "en"}

Сообщение: "Привет"
→ {"is_travel_related": false, "intent": "greeting", "confidence": 0.95, "language": "ru"}

Сообщение: "Gdzie jest mój voucher?"
→ {"is_travel_related": false, "intent": "other", "confidence": 0.9, "language": "pl"}

ВАЖНО: 
- Если человек спрашивает о НОВОМ туре/путешествии → is_travel_related=true
- Если спрашивает о готовой брони/документах/проблемах → is_travel_related=false
- Всегда определяй язык сообщения
- Украинский (uk): буквы і, є, ї, слова як, хочу полетіти
- Польский (pl): буквы ą, ę, ł, ń, ś, ź, ż, слова gdzie, kiedy, chcę"""

        payload = {
            "model": "gpt-5.1",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": message}
            ],
            "temperature": 0.3,
            "max_completion_tokens": 150
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"❌ OpenAI API error {response.status}: {error_text}")
                    return {"is_travel_related": True, "intent": "booking", "confidence": 0.5, "language": "ru"}

                data = await response.json()
                content = data["choices"][0]["message"]["content"].strip()
                
                # Убираем markdown форматирование если есть
                if content.startswith("```json"):
                    content = content[7:]
                if content.startswith("```"):
                    content = content[3:]
                if content.endswith("```"):
                    content = content[:-3]
                content = content.strip()
                
                result = json.loads(content)
                logger.info(f"🤖 Анализ: intent={result.get('intent')}, туры={result.get('is_travel_related')}, язык={result.get('language')}, уверенность={result.get('confidence')}")
                return result

    except Exception as e:
        logger.error(f"❌ Ошибка анализа намерения: {e}")
        # При ошибке считаем что это о путешествиях (безопаснее)
        return {"is_travel_related": True, "intent": "booking", "confidence": 0.5, "language": "ru"}


# ============================================================================
# СОСТОЯНИЯ ДИАЛОГА
# ============================================================================

class ConversationState(Enum):
    NEW = "new"
    GREETED = "greeted"
    ASKED_CITY_DATE = "asked_city_date"      # Спросили город + дату
    ASKED_PEOPLE = "asked_people"            # Спросили количество людей
    ASKED_CHILDREN = "asked_children"        # Спросили про детей
    ASKED_BUDGET = "asked_budget"            # Спросили бюджет / требования
    ASKED_DETAILS = "asked_details"          # Старое состояние (совместимость)
    ASKED_REQUIREMENTS = "asked_requirements"
    SEARCHING = "searching"                  # Идёт поиск туров
    SENT_OPTIONS = "sent_options"
    WAITING_PASSPORT = "waiting_passport"
    MANAGER_CALLED = "manager_called"
    BOT_DISABLED = "bot_disabled"


# ============================================================================
# СКРИПТЫ ПРОДАЖ (МНОГОЯЗЫЧНЫЕ)
# ============================================================================

SCRIPTS = {
    # Русский
    "greeting_ru": """Добрый день! Менеджер Алина, онлайн продаж компании Travel With Alina🤍

У нас персональное отношение к нашим туристам🙂✈""",

    "ask_question_ru": """Какой у вас вопрос? 😊""",
    
    "ask_city_date_ru": """Скажите пожалуйста, откуда вылет нужен и когда хотите поехать?🔥🔥🔥""",

    "transfer_to_manager_ru": """Спасибо за ваше сообщение! 😊

В ближайшее время мы вам ответим 🙏""",

    "searching_tours_ru": """В ближайшее время подберу для вас 5-7 хороших вариантов, ожидайте🙂""",
    
    "ask_budget_ru": """💰 Есть пожелания к бюджету или отелю? (Если нет - просто напишите "нет")""",

    # Английский
    "greeting_en": """Good day! Manager Alina from Travel With Alina🤍

We have a personal approach to each of our tourists🙂✈""",

    "ask_question_en": """What is your question? 😊""",
    
    "ask_city_date_en": """Could you please tell me, where is your departure from and when would you like to travel?🔥🔥🔥""",
    
    "transfer_to_manager_en": """Thank you for your message! 😊

We will get back to you soon 🙏""",

    "searching_tours_en": """I will find 5-7 great options for you soon, please wait🙂""",
    
    "ask_budget_en": """💰 Any preferences for budget or hotel? (If not - just write "no")""",

    # Датский
    "greeting_da": """God dag! Manager Alina fra Travel With Alina🤍

Vi har en personlig tilgang til alle vores turister🙂✈""",

    "ask_question_da": """Hvad er dit spørgsmål? 😊""",
    
    "ask_city_date_da": """Kan du venligst fortælle mig, hvor er din afrejse fra og hvornår vil du gerne rejse?🔥🔥🔥""",
    
    "transfer_to_manager_da": """Tak for din besked! 😊

Vi vender tilbage til dig snart 🙏""",

    "searching_tours_da": """Jeg finder 5-7 gode muligheder for dig snart, vent venligst🙂""",
    
    "ask_budget_da": """💰 Nogen præferencer for budget eller hotel? (Hvis ikke - bare skriv "nej")""",

    # Украинский
    "greeting_uk": """Добрий день! Менеджер Аліна, онлайн продажів компанії Travel With Alina🤍

У нас персональне ставлення до наших туристів🙂✈""",

    "ask_question_uk": """Яке у вас питання? 😊""",
    
    "ask_city_date_uk": """Скажіть будь ласка, звідки виліт потрібен і коли хочете поїхати?🔥🔥🔥""",
    
    "transfer_to_manager_uk": """Дякую за ваше повідомлення! 😊

Найближчим часом ми вам відповімо 🙏""",

    "searching_tours_uk": """Найближчим часом підберу для вас 5-7 гарних варіантів, очікуйте🙂""",
    
    "ask_budget_uk": """💰 Є побажання до бюджету чи готелю? (Якщо немає - просто напишіть "немає")""",

    # Польский
    "greeting_pl": """Dzień dobry! Menadżer Alina z Travel With Alina🤍

Mamy indywidualne podejście do każdego naszego turysty🙂✈""",

    "ask_question_pl": """Jakie jest twoje pytanie? 😊""",
    
    "ask_city_date_pl": """Proszę powiedzieć, skąd jest wasz wylot i kiedy chcecie jechać?🔥🔥🔥""",
    
    "transfer_to_manager_pl": """Dziękuję za wiadomość! 😊

Wkrótce się do Ciebie odezwiemy 🙏""",

    "searching_tours_pl": """Wkrótce znajdę dla Ciebie 5-7 dobrych opcji, proszę czekać🙂""",
    
    "ask_budget_pl": """💰 Jakieś preferencje dotyczące budżetu lub hotelu? (Jeśli nie - po prostu napisz "nie")""",

    # Старые скрипты для совместимости
    "greeting": """Добрый день! Менеджер Алина, онлайн продаж компании Travel With Alina🤍

У нас персональное отношение к нашим туристам🙂✈""",

    "ask_city_date": """Скажите пожалуйста, откуда вылет нужен и когда хотите поехать?🔥🔥🔥""",
    
    "ask_budget": """💰 Есть пожелания к бюджету или отелю? (Если нет - просто напишите "нет")""",
    
    "ask_people": """👥 Сколько человек едет?""",

    "ask_city_only": """📍 Укажите пожалуйста город вылета 😊

Мы работаем с вылетами из: Копенгагена, Биллунда, Орхуса, Ольборга, Осло, Стокгольма, Гётеборга, Мальмё, Хельсинки, Таллина""",

    # Дополнительные скрипты
    "price_question": """Цена зависит от отеля и дат вылета 😊 Давайте я подберу вам варианты, и вы увидите конкретные цены!""",
    "expensive": """Я понимаю 😊 Давайте попробую найти более бюджетные варианты для вас!""",
    "booking_info": """Для бронирования нужны только копии паспортов всех туристов 📄""",
    "insurance": """Медицинская страховка входит в стоимость тура ✅""",
    "about_company": """Мы - Travel With Alina 🤍 Онлайн агентство с персональным подходом к каждому клиенту ✈️""",
    "hotel_photos": """Фотографии отелей пришлю вместе с вариантами туров! 📸""",
    "passport_received": """Отлично! Паспорта получены ✅""",
    "what_included": """В стоимость тура входит: перелёт, проживание, трансфер, страховка ✅""",
    "visa_info": """Виза не нужна для большинства направлений 🎫""",
    "need_passport_for_booking": """Для бронирования нужны копии паспортов 📄""",
    "tenerife_info": """Тенерифе - прекрасный остров с круглогодичным летом! ☀️""",
    "expensive_followup": """Понимаю! Давайте подберу более доступные варианты 💰""",
}


def get_script(script_key: str, language: str = "ru") -> str:
    """
    Возвращает скрипт на нужном языке.
    Если нет перевода - возвращает русский вариант.
    """
    # Пытаемся найти скрипт с языком
    script_with_lang = f"{script_key}_{language}"
    if script_with_lang in SCRIPTS:
        return SCRIPTS[script_with_lang]
    
    # Если нет перевода - возвращаем русский
    script_ru = f"{script_key}_ru"
    if script_ru in SCRIPTS:
        return SCRIPTS[script_ru]
    
    # Если нет и русского - возвращаем базовый
    if script_key in SCRIPTS:
        return SCRIPTS[script_key]
    
    # Совсем не нашли
    logger.warning(f"⚠️ Скрипт '{script_key}' не найден для языка '{language}'")
    return ""


# ============================================================================
# ХРАНИЛИЩЕ ДИАЛОГОВ
# ============================================================================

class ConversationStorage:
    def __init__(self):
        self.conversations: Dict[str, Dict] = {}

    def get_conversation(self, user_id: str) -> Dict:
        if user_id not in self.conversations:
            self.conversations[user_id] = {
                "state": ConversationState.NEW,
                "created_at": datetime.now(),
                "last_message_at": datetime.now(),
                "last_bot_reply_at": None,  # Когда бот последний раз отвечал
                "departure_city": None,
                "destination": None,
                "adults": None,
                "children": None,   # None = ещё не уточняли; 0 = детей нет
                "child_ages": [],
                "departure_date": None,
                "departure_month": None,
                "date_approximate": False,
                "nights": 7,
                "budget": None,
                "requirements": None,
                "chat_history": [],
                "repeat_count": 0,  # Счётчик повторений вопросов
                "is_new_user": True,  # Флаг нового пользователя
                "first_contact": True,  # Первый контакт с ботом
                "language": "ru",  # Язык пользователя (ru, en, uk, pl, da, sv, no, de, fi)
            }
        return self.conversations[user_id]

    def update_conversation(self, user_id: str, **kwargs):
        conv = self.get_conversation(user_id)
        conv.update(kwargs)
        conv["last_message_at"] = datetime.now()

    def set_state(self, user_id: str, state: ConversationState):
        self.update_conversation(user_id, state=state)

    def add_message(self, user_id: str, role: str, content: str):
        conv = self.get_conversation(user_id)
        conv["chat_history"].append({"role": role, "content": content})
        if len(conv["chat_history"]) > 20:
            conv["chat_history"] = conv["chat_history"][-20:]


storage = ConversationStorage()


# ============================================================================
# АНАЛИЗ ИСТОРИИ ДИАЛОГА (ПОСЛЕДНИЕ 20 СООБЩЕНИЙ)
# ============================================================================

def get_facebook_conversation_history(user_id: str, limit: int = 10) -> List[Dict]:
    """
    🔥 УЛУЧШЕННАЯ ВЕРСИЯ: Получает последние N сообщений из Facebook Graph API.
    
    Сначала пытается получить реальную историю из Facebook API,
    если не получается - использует локальное хранилище.
    
    Возвращает список: [{'role': 'user'/'bot', 'text': '...', 'timestamp': datetime}, ...]
    """
    try:
        # Шаг 1: Пытаемся получить реальную историю из Facebook Graph API
        if FACEBOOK_PAGE_ACCESS_TOKEN:
            try:
                # Запрос к Facebook Graph API
                url = f"https://graph.facebook.com/v18.0/{user_id}/conversations"
                params = {
                    "access_token": FACEBOOK_PAGE_ACCESS_TOKEN,
                    "fields": f"messages.limit({limit}){{created_time,from,message}}",
                }
                
                resp = req_lib.get(url, params=params, timeout=5)
                
                if resp.status_code == 200:
                    data = resp.json()
                    conversations = data.get("data", [])
                    
                    if conversations:
                        first_conv = conversations[0]
                        messages_data = first_conv.get("messages", {}).get("data", [])
                        
                        if messages_data:
                            # Преобразуем в нужный формат
                            history = []
                            for msg in reversed(messages_data):  # Reverse для правильного порядка
                                message_text = msg.get("message", "")
                                from_data = msg.get("from", {})
                                from_id = from_data.get("id", "")
                                created_time = msg.get("created_time", "")
                                
                                if not message_text:
                                    continue
                                
                                # Определяем роль: user или bot (по ID отправителя)
                                role = "user" if from_id == user_id else "bot"
                                
                                # Парсим timestamp
                                try:
                                    timestamp = datetime.strptime(created_time, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
                                except:
                                    timestamp = datetime.now()
                                
                                history.append({
                                    'role': role,
                                    'text': message_text,
                                    'timestamp': timestamp
                                })
                            
                            logger.info(f"[CONTEXT] {user_id}: ✅ Получено {len(history)} сообщений из Facebook API")
                            return history
                else:
                    logger.warning(f"[CONTEXT] {user_id}: ⚠️ Facebook API error {resp.status_code}")
            
            except Exception as api_error:
                logger.warning(f"[CONTEXT] {user_id}: ⚠️ Ошибка Facebook API: {api_error}")
        
        # Шаг 2: Fallback - используем локальное хранилище
        conv = storage.get_conversation(user_id)
        chat_history = conv.get('chat_history', [])
        
        if not chat_history:
            logger.info(f"[CONTEXT] {user_id}: История пуста")
            return []
        
        # Преобразуем в нужный формат
        history = []
        for msg in chat_history[-limit:]:
            role = msg.get('role', 'user')
            text = msg.get('content', '')
            
            if not text:
                continue
            
            history.append({
                'role': role,
                'text': text,
                'timestamp': datetime.now()
            })
        
        logger.info(f"[CONTEXT] {user_id}: 📦 Получено {len(history)} сообщений из локального хранилища")
        return history
        
    except Exception as e:
        logger.error(f"[CONTEXT] Ошибка при получении истории: {e}", exc_info=True)
        return []


def analyze_conversation_context(user_id: str, current_message: str, conversation_history: List[Dict]) -> Dict:
    """
    🤖 УМНЫЙ AI-АНАЛИЗАТОР КОНТЕКСТА через OpenAI GPT.
    
    Анализирует последние 20 сообщений и решает через ИИ:
    1. Нужно ли отвечать на текущее сообщение
    2. Это вопрос, благодарность или просто подтверждение
    3. Повторяется ли бот/пользователь
    
    Возвращает:
    {
        'should_respond': bool,  # Нужно ли отвечать
        'reason': str,           # Причина решения
        'context_summary': str,  # Краткое резюме контекста
        'duplicate_detected': bool,  # Обнаружен дубликат
        'bot_already_responded': bool,  # Бот уже ответил на похожее
    }
    """
    
    result = {
        'should_respond': True,
        'reason': 'new_message',
        'context_summary': '',
        'duplicate_detected': False,
        'bot_already_responded': False,
    }
    
    if not conversation_history:
        logger.info(f"[CONTEXT] {user_id}: Нет истории — отвечаем")
        return result
    
    # === 1. БЫСТРАЯ ПРОВЕРКА НА ДУБЛИКАТЫ (без AI) ===
    recent_user_messages = [
        msg['text'] for msg in conversation_history[-10:]
        if msg['role'] == 'user'
    ]
    
    current_msg_lower = current_message.lower().strip()
    duplicate_count = sum(1 for msg in recent_user_messages if msg.lower().strip() == current_msg_lower)
    
    if duplicate_count >= 2:
        result['should_respond'] = False
        result['reason'] = 'duplicate_message'
        result['duplicate_detected'] = True
        logger.info(f"[CONTEXT] {user_id}: Дубликат сообщения (повторов: {duplicate_count}) — НЕ отвечаем")
        return result
    
    # === 2. БЫСТРАЯ ПРОВЕРКА НА ЭМОДЗИ И ОЧЕНЬ КОРОТКИЕ (без AI) ===
    if len(current_message.strip()) <= 2 and not current_msg_lower in ['да', 'нет', 'ok', 'ок']:
        result['should_respond'] = False
        result['reason'] = 'too_short'
        logger.info(f"[CONTEXT] {user_id}: Слишком короткое — НЕ отвечаем")
        return result
    
    # Эмодзи-реакции (без AI)
    emoji_reactions = ['👍', '👎', '❤️', '😊', '🔥', '✅', '👌', '🙏']
    if any(emoji in current_message for emoji in emoji_reactions) and len(current_message.strip()) <= 5:
        result['should_respond'] = False
        result['reason'] = 'emoji_reaction'
        logger.info(f"[CONTEXT] {user_id}: Эмодзи-реакция — НЕ отвечаем")
        return result
    
    # === 3. 🤖 УМНЫЙ AI-АНАЛИЗ через OpenAI GPT ===
    # Используем AI только для неоднозначных случаев
    try:
        ai_decision = _ai_analyze_should_respond(user_id, current_message, conversation_history)
        if ai_decision:
            result.update(ai_decision)
            logger.info(f"[AI-CONTEXT] {user_id}: AI решение — {'ОТВЕЧАЕМ' if result['should_respond'] else 'МОЛЧИМ'} ({result['reason']})")
            return result
    except Exception as e:
        logger.error(f"[AI-CONTEXT] {user_id}: Ошибка AI-анализа: {e}")
        # При ошибке AI — продолжаем обычную логику (отвечаем)
    
    # Формируем резюме контекста
    last_5 = conversation_history[-5:]
    summary_parts = []
    for msg in last_5:
        role_emoji = "👤" if msg['role'] == 'user' else "🤖"
        summary_parts.append(f"{role_emoji} {msg['text'][:50]}")
    result['context_summary'] = " | ".join(summary_parts)
    
    logger.info(f"[CONTEXT] {user_id}: Решение — ОТВЕЧАЕМ (default)")
    return result


def _ai_analyze_should_respond(user_id: str, current_message: str, conversation_history: List[Dict]) -> Optional[Dict]:
    """
    🤖 AI-анализатор: решает нужно ли отвечать на сообщение.
    
    Использует OpenAI GPT для умного анализа контекста.
    Возвращает None при ошибке или словарь с решением.
    """
    if not OPENAI_API_KEY:
        return None
    
    try:
        # Формируем контекст для AI (последние 5 сообщений)
        context_messages = []
        for msg in conversation_history[-5:]:
            role_name = "Клиент" if msg['role'] == 'user' else "Бот"
            context_messages.append(f"{role_name}: {msg['text']}")
        
        context_text = "\n".join(context_messages) if context_messages else "Нет предыдущего контекста"
        
        # Промпт для AI
        system_prompt = """Ты — умный анализатор диалога ТУРИСТИЧЕСКОГО АГЕНТСТВА (подбор туров и путёвок).

Твоя задача: решить, нужно ли ТУРИСТИЧЕСКОМУ БОТУ отвечать на сообщение клиента.

БОТ НЕ ДОЛЖЕН ОТВЕЧАТЬ если:
1. ❌ Клиент просто благодарит (спасибо, дякую, thanks, ок, окей, хорошо, понятно, отлично, супер, класс)
2. ❌ Клиент просто подтверждает без вопроса (ага, угу, да, так, yes, good, fine, ok)
3. ❌ Клиент прощается (пока, bye, до свидания, до зустрічі)
4. ❌ Клиент написал что-то очень короткое без смысла (а, э, хм, ммм)
5. ❌ Клиент уже ОТКАЗАЛСЯ от подбора (уже нашёл, уже купил, уже забронировал, не нужно, не интересно)
6. ❌ Клиент благодарит за ПРОШЕДШИЙ отдых (спасибо за отдых, отель был супер, всё понравилось)
7. ❌ Клиент пишет что-то НЕ ПРО ТУРЫ (погода сегодня, как дела, что делаешь)

БОТ ДОЛЖЕН ОТВЕЧАТЬ если:
1. ✅ Клиент задаёт вопрос про туры (есть "?" или вопросительные слова: что, где, когда, как, сколько, почему)
2. ✅ Клиент называет направление/страну (Турция, Египет, Испания, Греция, Тенерифе и т.д.)
3. ✅ Клиент называет город вылета (Копенгаген, Биллунд, Осло и т.д.)
4. ✅ Клиент называет даты или месяцы поездки (май, июнь, 15 апреля, через неделю)
5. ✅ Клиент уточняет детали тура (количество человек, с детьми, бюджет, звёзды отеля)
6. ✅ Клиент выражает сомнение или просит помощь (не знаю, помогите, посоветуйте)
7. ✅ Клиент продолжает обсуждение подбора тура

ПРИМЕРЫ:
- "Спасибо" → НЕ отвечать (благодарность)
- "Ок, хорошо" → НЕ отвечать (подтверждение)
- "Пока" → НЕ отвечать (прощание)
- "Уже нашёл, спасибо" → НЕ отвечать (отказ)
- "Турция в мае" → ОТВЕЧАТЬ (обсуждение тура)
- "Сколько стоит?" → ОТВЕЧАТЬ (вопрос)
- "2 взрослых" → ОТВЕЧАТЬ (уточнение деталей)
- "Копенгаген" → ОТВЕЧАТЬ (город вылета)

Отвечай ТОЛЬКО в JSON формате:
{
  "should_respond": true/false,
  "reason": "краткая причина на русском (5-10 слов)"
}"""

        user_prompt = f"""Контекст диалога:
{context_text}

Новое сообщение клиента: "{current_message}"

Нужно ли боту отвечать? Ответь в JSON формате."""

        # Запрос к OpenAI
        import requests as req
        response = req.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-5.1",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.3,
                "max_completion_tokens": 150
            },
            timeout=10
        )
        
        if response.status_code != 200:
            logger.error(f"[AI-CONTEXT] OpenAI API error: {response.status_code}")
            return None
        
        ai_response = response.json()
        content = ai_response["choices"][0]["message"]["content"].strip()
        
        # Парсим JSON ответ
        import json
        # Убираем возможные markdown блоки ```json ... ```
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        
        ai_decision = json.loads(content)
        
        return {
            'should_respond': ai_decision.get('should_respond', True),
            'reason': ai_decision.get('reason', 'AI analysis'),
            'context_summary': f"AI: {ai_decision.get('reason', '')}",
            'duplicate_detected': False,
            'bot_already_responded': False,
        }
        
    except Exception as e:
        logger.error(f"[AI-CONTEXT] Ошибка при AI-анализе: {e}", exc_info=True)
        return None


# ============================================================================
# ОСНОВНОЙ БОТ
# ============================================================================

class TravelBot:
    def __init__(self):
        self.storage = storage

        self.departure_cities = {
            # ДАНИЯ
            "копенгаген": "Copenhagen", "copenhagen": "Copenhagen",
            "копенгагена": "Copenhagen", "копенгагене": "Copenhagen", "копенгагену": "Copenhagen",
            "fra copenhagen": "Copenhagen", "fra kobenhavn": "Copenhagen",  # Датские варианты
            "биллунд": "Billund", "billund": "Billund",
            "биллунда": "Billund", "биллунде": "Billund", "биллунду": "Billund",
            # Одно Л — частая опечатка
            "билунд": "Billund", "билунда": "Billund", "билунде": "Billund", "билунду": "Billund",
            "fra billund": "Billund",  # Датский вариант
            "орхус": "Aarhus", "aarhus": "Aarhus", "arhus": "Aarhus",  # arhus - частая опечатка
            "орхуса": "Aarhus", "орхусе": "Aarhus", "архус": "Aarhus", "архуса": "Aarhus",
            "fra aarhus": "Aarhus", "fra arhus": "Aarhus",  # Датское "fra" = "from"
            "ольборг": "Aalborg", "aalborg": "Aalborg", "alborg": "Aalborg",  # Варианты написания
            "ольборга": "Aalborg", "ольборге": "Aalborg",
            "fra aalborg": "Aalborg", "fra alborg": "Aalborg",  # Датские варианты
            # НОРВЕГИЯ
            "осло": "Oslo", "oslo": "Oslo",
            "fra oslo": "Oslo",  # Норвежский вариант
            # ШВЕЦИЯ
            "стокгольм": "Stockholm", "stockholm": "Stockholm",
            "стокгольма": "Stockholm", "стокгольме": "Stockholm", "стокгольму": "Stockholm",
            "from stockholm": "Stockholm", "fra stockholm": "Stockholm",  # Английский/датский
            "гетеборг": "Goteborg", "goteborg": "Goteborg",
            "гётеборг": "Goteborg", "göteborg": "Goteborg",
            "гетеборга": "Goteborg", "гётеборга": "Goteborg", "гетеборге": "Goteborg",
            "from goteborg": "Goteborg", "from göteborg": "Goteborg",  # Английские варианты
            "мальмё": "Copenhagen", "мальме": "Copenhagen", "malmo": "Copenhagen", "malmö": "Copenhagen",
            "мальмо": "Copenhagen",
            "from malmo": "Copenhagen", "from malmö": "Copenhagen",  # Английские варианты
            # ФИНЛЯНДИЯ
            "хельсинки": "Helsinki", "helsinki": "Helsinki",
            "хельсинке": "Helsinki",
            "from helsinki": "Helsinki",  # Английский вариант
            "таллин": "Tallinn", "tallinn": "Tallinn", "таллинн": "Tallinn",
            "таллине": "Tallinn", "таллина": "Tallinn",
            "from tallinn": "Tallinn",  # Английский вариант
        }

        # Список поддерживаемых городов на разных языках
        self.supported_cities_text_ru = """К сожалению, из этого города у нас нет вылетов 😔

Мы работаем с вылетами из:

🇩🇰 **Дания:** Копенгаген, Биллунд, Орхус, Ольборг
🇳🇴 **Норвегия:** Осло
🇸🇪 **Швеция:** Стокгольм, Гётеборг, Мальмё
🇫🇮 **Финляндия:** Хельсинки, Таллин

Напишите один из этих городов, и я подберу для вас туры! 😊"""

        self.supported_cities_text_en = """Unfortunately, we don't have flights from this city 😔

We work with departures from:

🇩🇰 **Denmark:** Copenhagen, Billund, Aarhus, Aalborg
🇳🇴 **Norway:** Oslo
🇸🇪 **Sweden:** Stockholm, Gothenburg, Malmö
🇫🇮 **Finland:** Helsinki, Tallinn

Please enter one of these cities and I'll find tours for you! 😊"""

        self.destinations = {
            "шарм": "sharm", "sharm": "sharm", "шарм-эль-шейх": "sharm",
            "хургада": "hurghada", "hurghada": "hurghada",
            "тенерифе": "tenerife", "tenerife": "tenerife",
            "египет": "egypt", "egypt": "egypt",
            "анталья": "antalya", "antalya": "antalya", "анталия": "antalya",
            "турция": "turkey", "turkey": "turkey",
            "барселона": "barcelona", "barcelona": "barcelona",
            "майорка": "mallorca", "mallorca": "mallorca", "мальорка": "mallorca",
            "испания": "spain", "spain": "spain",
        }

    def detect_language(self, text: str) -> str:
        """Определяет язык сообщения (ru, uk, en, pl, da и др.)"""
        text_lower = text.lower()
        
        # Украинский - специфические буквы і, є, ї
        ukrainian_chars = ['і', 'є', 'ї', 'ґ']
        if any(char in text_lower for char in ukrainian_chars):
            return "uk"
        
        # Польский - специфические буквы
        polish_chars = ['ą', 'ę', 'ł', 'ń', 'ś', 'ź', 'ż']
        if any(char in text_lower for char in polish_chars):
            return "pl"
        
        # Украинские слова-маркеры (расширенный список)
        ukrainian_words = [
            'хочу полетіти', 'хочу поїхати', 'будь ласка', 'дякую', 'вітаю', 'привіт',
            'якщо', 'щось', 'якщо щось', 'якийсь', 'скільки', 'коли',
            'туреччину', 'єгипет', 'шарм-эль-шейх', 'хургаду'
        ]
        if any(word in text_lower for word in ukrainian_words):
            return "uk"
        
        # Польские слова-маркеры
        polish_words = ['chce', 'prosze', 'dziekuje', 'gdzie', 'kiedy', 'witam']
        if any(word in text_lower for word in polish_words):
            return "pl"
        
        # Проверяем наличие кириллицы
        cyrillic_count = len(re.findall(r'[а-яА-ЯёЁ]', text))
        latin_count = len(re.findall(r'[a-zA-Z]', text))

        if cyrillic_count > latin_count:
            return "ru"  # По умолчанию русский для кириллицы
        
        return "en"  # По умолчанию английский для латиницы

    async def analyze_message_with_context(self, user_id: str, message: str) -> Dict:
        """
        Умный анализ сообщения с учетом истории диалога через GPT-5.1
        """
        try:
            # ПРОВЕРЯЕМ КЭШ
            cache_key = get_cache_key(message, "context_analysis")
            cached = get_gpt_cache(cache_key)
            if cached:
                logger.info(f"[{user_id}] 💾 Контекст из кэша")
                return cached
            
            conv = self.storage.get_conversation(user_id)
            chat_history = conv.get("chat_history", [])
            
            # Определяем основной язык
            all_messages = " ".join([m["content"] for m in chat_history[-5:] if m["role"] == "user"])
            if all_messages:
                main_lang = self.detect_language(all_messages + " " + message)
            else:
                main_lang = self.detect_language(message)
            
            # Упрощенный промпт для скорости
            system_prompt = f"""Ты ИИ помощник турагентства.

Сообщение клиента: "{message}"

Верни JSON:
{{
    "action": "greet" или "continue_dialog" или "call_manager",
    "language": "{main_lang}",
    "context": "краткое описание"
}}

Если первое сообщение (привет, добрый день) → "greet"
Если вопрос по турам → "continue_dialog"  
Если про готовую бронь → "call_manager"
"""

            payload = {
                "model": "gpt-5.1",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": message}
                ],
                "temperature": 0.3,
                "max_completion_tokens": 150
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENAI_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=8)  # Уменьшен timeout
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"❌ GPT error {response.status}: {error_text[:200]}")
                        return {"action": "greet", "response": "", "language": main_lang, "context": "error"}

                    data = await response.json()
                    content = data["choices"][0]["message"]["content"].strip()
                    
                    # Убираем markdown
                    if content.startswith("```"):
                        content = content.split("```")[1] if "```" in content else content
                        if content.startswith("json"):
                            content = content[4:]
                    
                    result = json.loads(content.strip())
                    logger.info(f"[{user_id}] 🤖 GPT: action={result.get('action')}, lang={result.get('language')}")
                    
                    # Сохраняем в кэш
                    save_gpt_cache(cache_key, result)
                    
                    return result
        
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON parse error: {e}")
            return {"action": "greet", "response": "", "language": "ru", "context": "json_error"}
        except asyncio.TimeoutError:
            logger.error(f"❌ GPT timeout (>8 сек)")
            return {"action": "greet", "response": "", "language": "ru", "context": "timeout"}
        except Exception as e:
            logger.error(f"❌ analyze_message_with_context error: {e}")
            return {"action": "greet", "response": "", "language": "ru", "context": "error"}
        """
        Умный анализ сообщения с учетом истории диалога через GPT-4o
        
        Возвращает:
        {
            "action": str,  # "greet", "continue_dialog", "call_manager", "answer_question"
            "response": str,  # Рекомендуемый ответ на языке пользователя
            "language": str,  # Определенный язык (ru/uk/en/pl и т.д.)
            "context": str  # Что понял бот из контекста
        }
        """
        try:
            # Получаем историю диалога
            conv = self.storage.get_conversation(user_id)
            chat_history = conv.get("chat_history", [])
            
            # Определяем основной язык диалога (из истории)
            all_messages = " ".join([m["content"] for m in chat_history if m["role"] == "user"])
            if all_messages:
                main_lang = self.detect_language(all_messages + " " + message)
            else:
                main_lang = self.detect_language(message)
            
            # Формируем контекст для GPT-4o
            history_text = ""
            if chat_history:
                for msg in chat_history[-10:]:  # Последние 10 сообщений
                    role = "👤 Клиент" if msg["role"] == "user" else "🤖 Бот"
                    history_text += f"\n{role}: {msg['content'][:100]}"
            
            # Системный промпт для GPT-4o
            system_prompt = f"""Ты — умный помощник турагентства Travel With Alina.

Твоя задача — проанализировать сообщение клиента с учетом истории диалога и дать правильный ответ.

ВАЖНО:
1. Если это ПЕРВОЕ сообщение от клиента — всегда отправляй приветствие
2. Если диалог УЖЕ НАЧАТ (есть история) — НЕ здоровайся снова, продолжай диалог
3. Если клиент спрашивает о готовом туре/брони/документах — вызывай менеджера
4. Отвечай ТОЛЬКО на том языке, на котором общается клиент ({main_lang})
5. Анализируй ВСЮ историю, чтобы понять контекст

История диалога:{history_text if history_text else " (диалог пустой, это первое сообщение)"}

Новое сообщение клиента: "{message}"

Верни JSON:
{{
    "action": "greet" или "continue_dialog" или "call_manager" или "answer_question",
    "response": "Твой ответ на языке клиента",
    "language": "{main_lang}",
    "context": "Что ты понял из контекста (кратко)"
}}

Примеры:

1. Первое сообщение "Привет":
→ {{"action": "greet", "response": "Добрый день! Менеджер Алина...", "language": "ru", "context": "Первый контакт"}}

2. В диалоге написали "а как забронировать?":
→ {{"action": "answer_question", "response": "Для бронирования...", "language": "ru", "context": "Вопрос о бронировании в процессе подбора"}}

3. Спросили "где мои документы?":
→ {{"action": "call_manager", "response": "", "language": "ru", "context": "Вопрос о готовой брони"}}

4. В диалоге написали "2 взрослых":
→ {{"action": "continue_dialog", "response": "👥 2 взр. — понял! ...", "language": "ru", "context": "Ответ на вопрос бота о количестве людей"}}"""

            payload = {
                "model": "gpt-5.1",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"История: {history_text}\n\nНовое сообщение: {message}"}
                ],
                "temperature": 0.3,
                "max_completion_tokens": 500
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENAI_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"❌ GPT-4o error {response.status}: {error_text}")
                        return {"action": "continue_dialog", "response": "", "language": main_lang, "context": "error"}

                    data = await response.json()
                    content = data["choices"][0]["message"]["content"].strip()
                    
                    # Убираем markdown
                    if content.startswith("```json"):
                        content = content[7:]
                    if content.startswith("```"):
                        content = content[3:]
                    if content.endswith("```"):
                        content = content[:-3]
                    
                    result = json.loads(content.strip())
                    logger.info(f"[{user_id}] 🤖 GPT-4o: action={result.get('action')}, context={result.get('context')}")
                    return result
        
        except Exception as e:
            logger.error(f"❌ analyze_message_with_context error: {e}")
            return {"action": "continue_dialog", "response": "", "language": "ru", "context": "error"}

    async def analyze_user_answer(self, user_id: str, message: str, expected_info: str) -> Dict:
        """
        Анализирует ответ пользователя через GPT-4o
        
        Args:
            user_id: ID пользователя
            message: Сообщение пользователя
            expected_info: Что ожидаем от пользователя ("city", "date", "people", "children", "budget")
        
        Returns:
            {
                "understood": bool,  # Понял ли бот ответ
                "response": str,  # Вежливый ответ если не понял
                "should_cancel": bool,  # Хочет ли пользователь отменить
                "extracted_data": dict  # Извлеченные данные
            }
        """
        try:
            conv = self.storage.get_conversation(user_id)
            user_lang = conv.get("language", "ru")
            
            # Системный промпт для анализа
            system_prompt = f"""Ты — ИИ помощник турагентства Travel With Alina 🤖

ВОПРОС БОТА: {expected_info}
- "city" = город вылета (Copenhagen, Billund, Aarhus, Aalborg, Oslo, Stockholm, etc.)
- "date" = месяц или дата
- "people" = сколько взрослых
- "children" = есть ли дети
- "budget" = бюджет

ОТВЕТ КЛИЕНТА: "{message}"

Верни JSON:
{{
    "understood": true/false,
    "response": "КОРОТКИЙ ответ (макс 2 предложения) если не понял. ОБЯЗАТЕЛЬНО: 1) скажи что ты ИИ помощник 🤖, 2) что нужно указать, 3) про кнопку Отмена",
    "should_cancel": true/false,
    "extracted_data": {{}}
}}

ШАБЛОНЫ ОТВЕТОВ (используй ИМЕННО их):

city не понял:
"Я ИИ помощник 🤖 Укажите город вылета. Если хотите вернуться к менеджеру - нажмите Отмена."

date не понял:
"Я ИИ помощник 🤖 Укажите месяц вылета. Для возврата к менеджеру - нажмите Отмена."

people не понял:
"Я ИИ помощник 🤖 Напишите сколько взрослых. Нажмите Отмена для возврата к менеджеру."

НЕ перечисляй примеры городов!
Язык: {user_lang}"""

            payload = {
                "model": "gpt-5.1",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Ожидается: {expected_info}\nОтвет клиента: {message}"}
                ],
                "temperature": 0.3,
                "max_completion_tokens": 300
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENAI_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status != 200:
                        logger.error(f"❌ GPT-4o error {response.status}")
                        return {"understood": True, "response": "", "should_cancel": False, "extracted_data": {}}

                    data = await response.json()
                    content = data["choices"][0]["message"]["content"].strip()
                    
                    # Убираем markdown
                    if content.startswith("```json"):
                        content = content[7:]
                    if content.startswith("```"):
                        content = content[3:]
                    if content.endswith("```"):
                        content = content[:-3]
                    
                    result = json.loads(content.strip())
                    logger.info(f"[{user_id}] 🤖 GPT анализ: understood={result.get('understood')}, cancel={result.get('should_cancel')}")
                    return result
        
        except Exception as e:
            logger.error(f"❌ analyze_user_answer error: {e}")
            return {"understood": True, "response": "", "should_cancel": False, "extracted_data": {}}

    def _save_responses_to_history(self, user_id: str, responses: List[str]):
        """Сохраняет все ответы бота в историю чата и обновляет время последнего ответа"""
        has_real_response = False
        for response in responses:
            if response:  # Игнорируем пустые ответы
                self.storage.add_message(user_id, "assistant", response)
                has_real_response = True
        
        # Обновляем время последнего ответа бота (для защиты от спама)
        if has_real_response:
            conv = self.storage.get_conversation(user_id)
            conv["last_bot_reply_at"] = datetime.now()
            logger.info(f"[{user_id}] 💾 Сохранено время последнего ответа бота")

    def get_supported_cities_text(self, lang: str = "ru") -> str:
        """Возвращает текст о поддерживаемых городах на нужном языке"""
        if lang == "en":
            return self.supported_cities_text_en
        return self.supported_cities_text_ru

    def extract_travel_info(self, message: str, user_id: str) -> Dict:
        message_lower = message.lower()
        extracted = {}

        # Город вылета
        city_found = False
        for city_ru, city_en in self.departure_cities.items():
            if city_ru in message_lower:
                extracted["departure_city"] = city_en
                city_found = True
                break

        # Проверяем, упомянул ли пользователь какой-то город (но не наш)
        if not city_found:
            # Список распространённых городов которые мы НЕ поддерживаем
            unsupported_cities = [
                # Россия
                "москв", "санкт-петербург", "питер", "спб", "екатеринбург", "новосибирск",
                "казань", "сочи", "краснодар", "ростов", "самара", "уфа", "красноярск",
                # Украина
                "киев", "одесса", "харьков", "львов", "днепр", "запорожье",
                # Беларусь
                "минск", "гомель", "брест", "витебск",
                # Польша
                "варшав", "краков", "вроцлав", "гданьск", "познань",
                # Германия
                "берлин", "мюнхен", "франкфурт", "гамбург", "дюссельдорф", "кёльн",
                # Другие страны
                "париж", "лондон", "амстердам", "брюссель", "цюрих", "женева",
                "рига", "вильнюс", "прага", "вена", "будапешт", "бухарест",
                "софия", "загреб", "белград", "тбилиси", "баку", "ереван",
                "алматы", "астана", "ташкент", "бишкек", "дубай", "стамбул",
                "каир", "анкара", "тегеран", "дели", "бангкок", "пекин",
            ]
            for city in unsupported_cities:
                if city in message_lower:
                    extracted["unsupported_city"] = city
                    logger.info(f"Detected unsupported city: {city}")
                    break

        # Направление
        for dest_ru, dest_en in self.destinations.items():
            if dest_ru in message_lower:
                extracted["destination"] = dest_en
                break

        # Количество взрослых — ищем явное упоминание взрослых
        adults_match = None
        
        # Сначала проверяем текстовые числа (два, три, четыре)
        text_numbers = {
            'один': 1, 'одна': 1, 'одного': 1,
            'два': 2, 'двое': 2, 'двух': 2, 'двум': 2,
            'три': 3, 'трое': 3, 'троих': 3,
            'четыре': 4, 'четверо': 4, 'четырёх': 4,
            'пять': 5, 'пятеро': 5,
        }
        
        for word, num in text_numbers.items():
            if f'{word} взрослых' in message_lower or f'{word} взросл' in message_lower:
                adults_match = num
                break
        
        # Если не нашли текстовое, ищем цифровое
        if adults_match is None:
            for pattern in [r'(\d+)\s*взросл']:
                m = re.search(pattern, message_lower)
                if m:
                    adults_match = int(m.group(1))
                break

        # Количество детей — расширенные паттерны
        children_match = None
        children_patterns = [
            r'(\d+)\s*реб[её]нк',   # 1 ребёнка / ребёнку
            r'(\d+)\s*реб[её]нок',  # 1 ребёнок
            r'(\d+)\s*реб[её]н',    # общий: реб[её]н*
            r'(\d+)\s*дет[еёй]',    # 2 детей
            r'(\d+)\s*дети',        # дети
            r'(\d+)\s*ребят',       # ребята
            r'(\d+)\s*kid',         # kids
            r'(\d+)\s*child',       # children
        ]
        for pattern in children_patterns:
            m = re.search(pattern, message_lower)
            if m:
                children_match = int(m.group(1))
                break

        # Один ребёнок без числа (просто «ребёнок» / «ребенок»)
        if children_match is None:
            if re.search(r'\bреб[её]нок\b|\bреб[её]нка\b|\bреб[её]ночек\b|\bмалыш\b', message_lower):
                children_match = 1
            elif re.search(r'\bдвое\s*детей\b|\bдвое\s*деток\b', message_lower):
                children_match = 2
        
        # Проверяем "детей нет" / "дети нет" / "без детей"
        if 'детей нет' in message_lower or 'дети нет' in message_lower or 'без детей' in message_lower:
            extracted["children"] = 0
            children_match = 0

        # Если написано "2 взрослых и 1 ребёнок" — оба значения найдены
        if adults_match is not None:
            extracted["adults"] = adults_match
        if children_match is not None:
            extracted["children"] = children_match

        # Если нет явных "взрослых" — ищем общее число людей (без детей)
        if adults_match is None and children_match is None:
            for pattern in [r'(\d+)\s*человек', r'на\s*(\d+)', r'(\d+)\s*чел\b']:
                m = re.search(pattern, message_lower)
                if m:
                    extracted["adults"] = int(m.group(1))
                    break

        # Возраст детей — ищем "N лет" / "N года" (только детские возрасты < 18)
        age_matches = re.findall(r'(\d+)\s*(?:лет|год[а-я]*)', message_lower)
        if age_matches:
            ages = [int(a) for a in age_matches if int(a) < 18]
            if ages:
                extracted["child_ages"] = ages
                # Если возрасты нашли, но детей явно не указали — считаем по кол-ву возрастов
                if children_match is None and not extracted.get("children"):
                    extracted["children"] = len(ages)
                # Если найденных возрастов больше чем указанных детей — уточняем
                elif children_match is not None and len(ages) > children_match:
                    extracted["children"] = len(ages)

        # Количество ночей
        nights_patterns = [
            r'(\d+)\s*ноч',  # "7 ночей", "10 ночи"
            r'на\s*(\d+)\s*дн',  # "на 7 дней", "на 10 дней"
            r'(\d+)\s*дн[ея]й\s+отдых',  # "7 дней отдых"
            r'(\d+)\s*дн[ея]й\s+тур',  # "7 дней тур"
        ]
        for pattern in nights_patterns:
            match = re.search(pattern, message_lower)
            if match:
                nights = int(match.group(1))
                if 3 <= nights <= 21:
                    extracted["nights"] = nights
                break

        # Конкретная дата (13 марта, 15 февраля, середина марта и т.д.)
        date_patterns = [
            r'(\d{1,2})\s*(?:числа?\s*)?(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)',
            r'(\d{1,2})[./](\d{1,2})',  # 13.03 или 13/03
            r'(\d{1,2})[./](\d{1,2})[./](\d{2,4})',  # 13.03.2026
        ]

        months_map = {
            'январ': 1, 'феврал': 2, 'март': 3, 'апрел': 4,
            'май': 5, 'мая': 5, 'июн': 6, 'июл': 7, 'август': 8,
            'сентябр': 9, 'октябр': 10, 'ноябр': 11, 'декабр': 12,
            # Английские варианты
            'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
            'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6,
            'july': 7, 'jul': 7, 'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
            'october': 10, 'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12,
        }

        # Пробуем найти дату в формате DD.MM или DD/MM
        date_match_numeric = re.search(r'(\d{1,2})[./](\d{1,2})', message_lower)
        if date_match_numeric:
            day = int(date_match_numeric.group(1))
            month = int(date_match_numeric.group(2))
            if 1 <= day <= 31 and 1 <= month <= 12:
                extracted["departure_date"] = datetime(2026, month, day)
                extracted["departure_month"] = month
                extracted["date_approximate"] = False  # точная дата

        # Пробуем найти конкретную дату типа "13 марта"
        if not extracted.get("departure_date"):
            date_match = re.search(date_patterns[0], message_lower)
            if date_match:
                day = int(date_match.group(1))
                month_str = date_match.group(2)
                for month_name, month_num in months_map.items():
                    if month_str.startswith(month_name[:3]):
                        extracted["departure_date"] = datetime(2026, month_num, day)
                        extracted["departure_month"] = month_num
                        extracted["date_approximate"] = False  # точная дата
                        break

        # "середина марта", "начало апреля", "конец мая"
        if not extracted.get("departure_date"):
            period_match = re.search(r'(начал|середин|конец|конц)\w*\s+(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)', message_lower)
            if period_match:
                period = period_match.group(1)
                month_str = period_match.group(2)
                for month_name, month_num in months_map.items():
                    if month_str.startswith(month_name[:3]):
                        if "начал" in period:
                            day = 5
                        elif "середин" in period:
                            day = 15
                        else:  # конец
                            day = 25
                        extracted["departure_date"] = datetime(2026, month_num, day)
                        extracted["departure_month"] = month_num
                        # Дата приблизительная — нужно переспросить точное число
                        extracted["date_approximate"] = True
                        break

        # Просто месяц (без числа) — дата приблизительная, ищем лучшую дату
        if not extracted.get("departure_month"):
            # Ищем по префиксу (чтобы "марте", "апреле", "апрель", "March" — всё работало)
            MONTH_PREFIXES = [
                ('январ', 1), ('феврал', 2),
                # Март — все варианты: март/марта/марте/марте/марту/march
                ('март', 3), ('марта', 3), ('марте', 3), ('марту', 3), ('march', 3), ('mar', 3),
                # Апрель
                ('апрел', 4), ('april', 4), ('apr', 4),
                # Май
                ('мая', 5), ('май', 5), ('may', 5),
                # Июнь
                ('июн', 6), ('june', 6), ('jun', 6),
                # Июль
                ('июл', 7), ('july', 7), ('jul', 7),
                # Август
                ('август', 8), ('august', 8), ('aug', 8),
                # Сентябрь
                ('сентябр', 9), ('september', 9), ('sep', 9),
                # Октябрь
                ('октябр', 10), ('october', 10), ('oct', 10),
                # Ноябрь
                ('ноябр', 11), ('november', 11), ('nov', 11),
                # Декабрь
                ('декабр', 12), ('december', 12), ('dec', 12),
            ]
            # Сортируем по длине (длинные раньше, чтобы "марта" перед "март")
            MONTH_PREFIXES.sort(key=lambda x: -len(x[0]))
            for prefix, month_num in MONTH_PREFIXES:
                if prefix in message_lower:
                    extracted["departure_date"] = datetime(2026, month_num, 15)
                    extracted["departure_month"] = month_num
                    extracted["date_approximate"] = True
                    break

        # Бюджет
        budget_match = re.search(r'(\d+)\s*(?:евро|euro|eur|€)', message_lower)
        if budget_match:
            extracted["budget"] = int(budget_match.group(1))

        # Первая линия
        if "перв" in message_lower and "лини" in message_lower:
            extracted["requirements"] = "первая линия"

        return extracted

    def get_quick_response(self, message: str) -> Optional[str]:
        message_lower = message.lower().strip()

        if message_lower in ["цена", "цена?", "сколько", "сколько?", "стоимость", "стоимость?"]:
            return SCRIPTS["price_question"]
        if "дорого" in message_lower and len(message_lower) < 20:
            return SCRIPTS["expensive"]
        if "бронирован" in message_lower or "забронировать" in message_lower:
            return SCRIPTS["booking_info"]
        if "страховк" in message_lower:
            return SCRIPTS["insurance"]
        if "о компании" in message_lower or "кто вы" in message_lower:
            return SCRIPTS["about_company"]
        if "фото" in message_lower and "отел" in message_lower:
            return SCRIPTS["hotel_photos"]
        if "паспорт" in message_lower:
            return SCRIPTS["passport_received"]
        return None

    def is_winter_season(self, month: int) -> bool:
        return month >= 11 or month <= 4

    async def process_message(self, user_id: str, message: str) -> List[str]:
        conv = self.storage.get_conversation(user_id)
        state = conv["state"]
        responses = []
        message_lower = message.lower().strip()

        logger.info(f"[{user_id}] State: {state.value}, Msg: {message[:50]}...")

        # =================================================================
        # 🔥 AI-АНАЛИЗ КОНТЕКСТА ОТКЛЮЧЕН - используем простую логику с кнопкой
        # =================================================================

        # =================================================================
        # КОМАНДЫ МЕНЕДЖЕРА - ПРОВЕРЯЕМ В САМОМ НАЧАЛЕ!
        # =================================================================
        # Эти команды должны работать ВСЕГДА, независимо от состояния бота
        
        # 1. Менеджер берёт чат на себя (бот выходит)
        manager_takeover_keywords = ["стоп бот", "stopbot", "я беру", "менеджер здесь"]
        if any(keyword in message_lower for keyword in manager_takeover_keywords):
            logger.info(f"[{user_id}] 🛑 Менеджер взял чат — бот отключен")
            send_telegram_notification(user_id, f"🛑 Менеджер взял чат на себя\nКоманда: {message}", "")
            self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
            return []  # Бот молчит

        # 2. Команда отключения с объяснением для клиента
        # "просим прощения" ИЛИ "прошу прощения" + "тестируем"
        if ("просим прощения" in message_lower or "прошу прощения" in message_lower) and "тестируем" in message_lower:
            logger.info(f"[{user_id}] 🛑 Менеджер взял чат (с объяснением) — бот отключен")
            send_telegram_notification(user_id, f"🛑 Менеджер взял чат с объяснением\n💬 Сообщение: {message}", "")
            self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
            # НЕ отвечаем - менеджер сам написал объяснение клиенту
            return []

        # 3. Менеджер возвращает бота в чат (НОВАЯ КОМАНДА)
        # "добрый день менеджер" или "good day manager" - менеджер входит в чат, бот включается
        manager_greeting_keywords = [
            "добрый день менеджер", "доброго дня менеджер", "здравствуйте менеджер",
            "good day manager", "hello manager", "hi manager"
        ]
        if any(keyword in message_lower for keyword in manager_greeting_keywords):
            logger.info(f"[{user_id}] ✅ Менеджер поздоровался - бот ВКЛЮЧАЕТСЯ")
            send_telegram_notification(user_id, f"✅ Менеджер вернул бота\nКоманда: {message}", "")
            self.storage.set_state(user_id, ConversationState.GREETED)
            # НЕ отвечаем - менеджер сам поздоровался
            return []

        # 4. Команды прямого управления ботом
        manager_restore_keywords = ["бот вернись", "botback", "включи бота", "активируй бота", "старт бот", "startbot"]
        if any(keyword in message_lower for keyword in manager_restore_keywords):
            logger.info(f"[{user_id}] ✅ Менеджер вернул бота в чат")
            self.storage.set_state(user_id, ConversationState.GREETED)
            responses.append("Скажите пожалуйста, откуда вылет нужен и когда хотите поехать?🔥🔥🔥")
            return responses

        # 5. Старая команда активации (для совместимости)
        if "менеджер алина" in message_lower and "travel with alina" in message_lower:
            logger.info(f"[{user_id}] ✅ Менеджер вернул бота (старая команда)")
            self.storage.set_state(user_id, ConversationState.GREETED)
            responses.append("Скажите пожалуйста, откуда вылет нужен и когда хотите поехать?🔥🔥🔥")
            return responses

        # =================================================================
        # АВТОМАТИЧЕСКОЕ ИЗВЛЕЧЕНИЕ ИМЕНИ ИЗ СООБЩЕНИЯ
        # =================================================================
        # Пробуем извлечь имя если его еще нет в кэше
        cached_name = get_user_name(user_id)
        if not cached_name.get('name'):
            # Пытаемся извлечь имя из текста сообщения
            extracted_name = extract_name_from_message(message)
            if extracted_name:
                save_user_name(user_id, name=extracted_name, first_name=extracted_name)
                logger.info(f"[{user_id}] 📝 Извлечено имя из сообщения: {extracted_name}")

        # =================================================================
        # 🔥 ФИЛЬТРАЦИЯ БЛАГОДАРНОСТЕЙ ОТКЛЮЧЕНА - используем простую логику с кнопкой
        # =================================================================

        # =================================================================
        # УМНАЯ ФИЛЬТРАЦИЯ: Вопросы по существующим броням
        # =================================================================
        # Если клиент задаёт вопросы НЕ про подбор тура — вызываем менеджера
        booking_questions_keywords = [
            # Документы для готовой брони
            "где докум", "когда отправ", "когда переш", "где ваучер", "где билет",
            "где мои докум", "когда прид", "когда пришл",
            # Время/рейс (уже забронированный)
            "время вылета", "рейс перенес", "рейс отменил", "изменил время",
            "во сколько вылет", "который час вылет",
            # Оплата/деньги по конкретной брони
            "уже оплатил", "когда оплачивать", "счёт не прише", "счет не прише",
            "оплата прош", "деньги переве",
            # Вопросы по готовой брони
            "где мой тур", "когда вылет", "какой номер брони", "подтверд брони",
            "мой заказ", "моя бронь", "номер брони", "статус брони",
            # Встреча/трансфер
            "кто встретит", "где встреч", "трансфер будет",
        ]

        is_booking_question = any(keyword in message_lower for keyword in booking_questions_keywords)

        # Если это вопрос по брони И диалог уже начат (не новый пользователь)
        if is_booking_question and state != ConversationState.NEW:
            logger.info(f"[{user_id}] Обнаружен вопрос по брони — вызываем менеджера")

            # Отправляем уведомление в Telegram группу (ТИХО)
            send_telegram_notification(user_id, message)

            self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
            # Нейтральный автоответчик - клиент думает что это менеджер
            user_lang = conv.get("language", "ru")
            responses.append(get_script("transfer_to_manager", user_lang))
            return responses

        # =================================================================
        # FAQ — Простые вопросы (бот отвечает сам, не передаёт менеджеру)
        # =================================================================
        
        # Получаем язык пользователя
        user_lang = conv.get("language", "ru")
        
        faq_patterns = {
            # Что входит в тур
            "что_входит": ["что входит", "что включ", "включено в тур", "включено в стоимость", "what's included", "what included", "що включено", "co jest wliczone"],
            # Виза
            "виза": ["нужна виза", "нужен ли виза", "нужна ли виза", "visa required", "need visa", "потрібна віза", "czy potrzebna wiza"],
            # Документы для НОВОГО бронирования
            "документы_бронь": ["что нужно для брон", "какие документ для брон", "как забронировать", "what need book", "documents booking", "які документи", "jakie dokumenty"],
            # Страховка
            "страховка": ["что покрывает страховк", "медицинская страховк", "insurance cover", "страхування", "ubezpieczenie"],
            # Фото отеля
            "фото_отель": ["фото отел", "как посмотреть отел", "где посмотреть отел", "описание отел", "hotel photo", "фото готелю", "zdjęcia hotelu"],
            # О компании
            "о_компании": ["кто вы", "расскаж о компании", "о вашей компании", "about company", "про компанію", "o firmie"],
            # Тенерифе
            "тенерифе": ["есть тенерифе", "туры на тенерифе", "tenerife tour", "є тенеріфе", "teneryfa"],
            # Цена/дорого
            "дорого": ["дорого", "expensive", "почему так дорог", "дорого", "dlaczego tak drogo"],
        }

        for faq_key, patterns in faq_patterns.items():
            if any(pat in message_lower for pat in patterns):
                logger.info(f"[{user_id}] FAQ вопрос: {faq_key} (язык: {user_lang})")

                # Получаем ответ на языке пользователя через AI
                faq_response = self._get_faq_response(faq_key, user_lang)
                if faq_response:
                    responses.append(faq_response)
                    return responses
                
                # Fallback на старые скрипты если нет AI
                if faq_key == "что_входит" and "what_included" in SCRIPTS:
                    responses.append(SCRIPTS["what_included"])
                    return responses
                elif faq_key == "виза" and "visa_info" in SCRIPTS:
                    responses.append(SCRIPTS["visa_info"])
                    return responses
                elif faq_key == "документы_бронь" and "need_passport_for_booking" in SCRIPTS:
                    responses.append(SCRIPTS["need_passport_for_booking"])
                    return responses
                elif faq_key == "страховка" and "insurance" in SCRIPTS:
                    responses.append(SCRIPTS["insurance"])
                    return responses
                elif faq_key == "фото_отель" and "hotel_photos" in SCRIPTS:
                    responses.append(SCRIPTS["hotel_photos"])
                    return responses
                elif faq_key == "о_компании" and "about_company" in SCRIPTS:
                    responses.append(SCRIPTS["about_company"])
                    return responses
                elif faq_key == "тенерифе" and "tenerife_info" in SCRIPTS:
                    responses.append(SCRIPTS["tenerife_info"])
                    return responses
                elif faq_key == "дорого" and "expensive_followup" in SCRIPTS:
                    responses.append(SCRIPTS["expensive_followup"])
                    return responses

        # Словари для красивых названий
        city_names_ru = {
            "Copenhagen": "Копенгаген", "Billund": "Биллунд", "Aarhus": "Орхус",
            "Aalborg": "Ольборг", "Oslo": "Осло", "Stockholm": "Стокгольм",
            "Goteborg": "Гётеборг", "Helsinki": "Хельсинки", "Tallinn": "Таллин"
        }
        months_ru_v = {1:"январе",2:"феврале",3:"марте",4:"апреле",5:"мае",6:"июне",
                       7:"июле",8:"августе",9:"сентябре",10:"октябре",11:"ноябре",12:"декабре"}

        def _date_str(conv_data):
            m = conv_data.get("departure_month")
            d = conv_data.get("departure_date")
            if d and hasattr(d, 'day') and d.day not in (5, 10, 15, 20, 25):
                return d.strftime("%d.%m.%Y")
            return f"в {months_ru_v.get(m, str(m))}" if m else ""

        def _do_reset():
            # ВАЖНО: запоминаем, был ли пользователь новым ДО сброса
            old_conv = self.storage.conversations.get(user_id, {})
            was_new_user = old_conv.get("is_new_user", True)
            had_any_state = old_conv.get("state") != ConversationState.NEW if old_conv else False
            last_reply = old_conv.get("last_bot_reply_at")  # Сохраняем время последнего ответа

            self.storage.conversations[user_id] = {
                "state": ConversationState.NEW,
                "created_at": datetime.now(),
                "last_message_at": datetime.now(),
                "last_bot_reply_at": last_reply,  # СОХРАНЯЕМ время последнего ответа!
                "departure_city": None, "destination": None,
                "adults": None, "children": None, "child_ages": [],
                "departure_date": None, "departure_month": None,
                "date_approximate": False,
                "nights": 7, "budget": None, "requirements": None,
                "chat_history": [],
                "repeat_count": 0,
                "is_new_user": was_new_user and not had_any_state,  # False если уже был диалог
                "first_contact": old_conv.get("first_contact", True),  # Сохраняем первый контакт
                "language": "ru",  # Язык пользователя (ru, en, uk, pl, da, sv, no, de, fi)
            }
            logger.info(f"[{user_id}] RESET - диалог сброшен (was_new={was_new_user}, had_state={had_any_state})")

        # =================================================================
        # ПРОВЕРКА: Если бот отключен менеджером — молчим
        # (Проверяем ПЕРЕД "начать", чтобы клиент не мог перезапустить бота)
        # =================================================================
        if state == ConversationState.BOT_DISABLED:
            logger.info(f"[{user_id}] 🔇 Бот отключен менеджером — игнорируем сообщение '{message[:30]}...'")
            return []  # Молчим полностью
        
        # =================================================================
        # 🔘 ОБРАБОТКА СОСТОЯНИЯ GREETED (после приветствия, ждём кнопку "Начать")
        # =================================================================
        # Это должно быть ПЕРЕД всеми другими проверками!
        if state == ConversationState.GREETED:
            # Проверяем отказ клиента
            cancel_keywords = [
                "нет", "не хочу", "отмена", "отменить", "не надо", "не нужно",
                "no", "cancel", "no thanks", "ні", "немає", "не хочу",
                "nie", "anuluj", "nej", "ingen"
            ]
            
            if any(keyword in message_lower for keyword in cancel_keywords):
                logger.info(f"[{user_id}] ❌ Клиент отказался - бот отключается БЕЗ ответа")
                send_telegram_notification(
                    user_id,
                    f"❌ Клиент отказался от подбора тура\n💬 Сообщение: '{message}'\n\nБот отключен (молчит)",
                    ""
                )
                self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
                return []  # МОЛЧИМ, не отвечаем клиенту
            
            # Проверяем, нажал ли пользователь кнопку "Начать" или написал "начать"
            start_keywords = ["начать", "старт", "start", "почати", "rozpocznij", "begynd"]
            if any(keyword in message_lower for keyword in start_keywords):
                logger.info(f"[{user_id}] ✅ Пользователь нажал 'Начать' - начинаем подбор")
                # Сбрасываем флаг кнопки
                conv["_send_with_button"] = False
                # Начинаем подбор тура - спрашиваем откуда вылет С КНОПКОЙ ОТМЕНЫ
                user_lang = conv.get("language", "ru")
                
                # Устанавливаем флаг что нужно отправить с кнопкой отмены
                conv["_send_with_cancel_button"] = True
                conv["_cancel_button_message"] = get_script("ask_city_date", user_lang)
                
                # Текст кнопки отмены на языке клиента
                cancel_labels = {
                    "ru": "Отмена",
                    "en": "Cancel",
                    "uk": "Скасувати",
                    "pl": "Anuluj",
                    "da": "Annullere"
                }
                conv["_cancel_button_label"] = cancel_labels.get(user_lang, "Cancel")
                
                responses.append("")  # Пустой ответ, реальное сообщение отправится через webhook с кнопкой
                self.storage.set_state(user_id, ConversationState.ASKED_CITY_DATE)
                return responses
            else:
                # Игнорируем все другие сообщения
                logger.info(f"[{user_id}] 🔇 Состояние GREETED - игнорируем сообщение '{message[:50]}'")
                return []

        # =================================================================
        # ПРОВЕРКА: Клиент на этапе бронирования — бот молчит
        # =================================================================
        booking_states = [
            ConversationState.WAITING_PASSPORT,
            ConversationState.MANAGER_CALLED,
            ConversationState.SENT_OPTIONS
        ]
        
        if state in booking_states:
            logger.info(f"[{user_id}] 💼 Клиент на этапе бронирования (state={state.value}) — бот молчит")
            logger.info(f"[{user_id}] 📝 Сообщение: '{message}'")
            
            # Уведомляем менеджера если это важное сообщение
            if len(message) > 10:  # Не уведомляем о коротких сообщениях типа "ок"
                send_telegram_notification(
                    user_id,
                    f"💼 Клиент на этапе бронирования пишет:\n'{message}'\n\n"
                    f"Состояние: {state.value}\n"
                    f"Бот молчит - требуется менеджер",
                    ""
                )
            
            return []  # Бот не отвечает

        # =================================================================
        # 🔒 ЗАЩИТА ОТ СПАМА: Бот отвечал недавно - НЕ отвечать снова
        # =================================================================
        # Проверяем: отвечал ли бот этому клиенту в последние 24 часа
        last_bot_reply = conv.get("last_bot_reply_at")
        if last_bot_reply and state == ConversationState.NEW:
            hours_since_reply = (datetime.now() - last_bot_reply).total_seconds() / 3600
            
            # Если бот отвечал менее 24 часов назад - считаем СТАРЫМ клиентом
            if hours_since_reply < 24:
                logger.info(f"[{user_id}] 🔒 Бот уже отвечал {hours_since_reply:.1f}ч назад - НЕ отвечаем снова")
                send_telegram_notification(
                    user_id,
                    f"🔒 Клиент пишет снова ({hours_since_reply:.1f}ч после последнего ответа бота):\n"
                    f"'{message}'\n\n"
                    f"Бот НЕ отвечает - передано менеджеру",
                    ""
                )
                self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
                return []

        # =================================================================
        # УМНАЯ ОБРАБОТКА ПЕРВОГО СООБЩЕНИЯ (БЕЗ "начать")
        # =================================================================
        reset_words = ["начать", "старт", "start", "заново", "сначала", "reset"]
        is_reset = (message_lower in reset_words or message_lower.strip() in reset_words)

        # Приветственные слова
        greeting_words = ["привет", "здравствуйте", "добрый день", "доброе утро", "добрый вечер",
                         "hello", "hi", "hey", "hej", "hei", "добрий день", "dzień dobry", "god dag"]
        is_greeting = any(word in message_lower for word in greeting_words)

        # Проверяем - это первый контакт?
        first_contact = conv.get("first_contact", True)

        # ═══════════════════════════════════════════════════════════════
        # 🤖 УМНАЯ ЛОГИКА: Анализ через GPT-4o с учетом истории
        # ═══════════════════════════════════════════════════════════════
        if state == ConversationState.NEW:
            logger.info(f"[{user_id}] 🤖 Анализ сообщения через GPT-4o...")
            
            # Умный анализ с учетом истории
            ai_analysis = await self.analyze_message_with_context(user_id, message)
            
            action = ai_analysis.get("action", "greet")
            ai_response = ai_analysis.get("response", "")
            user_lang = ai_analysis.get("language", "ru")
            context = ai_analysis.get("context", "")
            
            logger.info(f"[{user_id}] 🤖 AI: {action} | {context[:50]}")
            
            # Сохраняем язык
            self.storage.update_conversation(user_id, language=user_lang)
            
            # Действия по результатам анализа
            if action == "greet" and first_contact:
                # Первое сообщение - отправляем приветствие НА ЯЗЫКЕ КЛИЕНТА
                conv["first_contact"] = False
                
                # Получаем тексты на языке клиента
                greeting_key = f"greeting_{user_lang}" if f"greeting_{user_lang}" in SCRIPTS else "greeting_ru"
                greeting_msg = SCRIPTS.get(greeting_key, SCRIPTS["greeting_ru"])
                
                # Текст для кнопки на языке клиента
                button_texts = {
                    "ru": "Новинка🔥🔥🔥Для индивидуального подбора тура нашим ИИ помощником Travel with Alina с вылетом из стран Скандинавии, нажмите начать",
                    "en": "New feature🔥🔥🔥For individual tour selection by our AI assistant Travel with Alina with departure from Scandinavian countries, press start",
                    "uk": "Новинка🔥🔥🔥Для індивідуального підбору туру нашим ІІ помічником Travel with Alina з вильотом зі країн Скандинавії, натисніть почати",
                    "pl": "Nowość🔥🔥🔥Do indywidualnego wyboru wycieczki przez naszego asystenta AI Travel with Alina z wylotem z krajów skandynawskich, naciśnij start",
                    "da": "Nyhed🔥🔥🔥Til individuel turvalg af vores AI-assistent Travel with Alina med afgang fra skandinaviske lande, tryk start"
                }
                button_msg = button_texts.get(user_lang, button_texts["ru"])
                
                # Текст самой кнопки
                button_labels = {
                    "ru": "Начать",
                    "en": "Start",
                    "uk": "Почати",
                    "pl": "Start",
                    "da": "Start"
                }
                button_label = button_labels.get(user_lang, "Start")
                
                responses.append(greeting_msg)
                
                conv["_send_with_button"] = True
                conv["_button_message"] = button_msg
                conv["_button_label"] = button_label  # Сохраняем текст кнопки
                
                self.storage.set_state(user_id, ConversationState.GREETED)
                return responses
            
            elif action == "call_manager":
                # Вызываем менеджера
                logger.info(f"[{user_id}] 📞 AI рекомендует вызвать менеджера")
                send_telegram_notification(user_id, f"AI: {context}\n\n💬 {message}")
                self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
                
                # Отправляем клиенту что менеджер ответит
                if ai_response:
                    responses.append(ai_response)
                else:
                    responses.append("Спасибо за ваше сообщение! В ближайшее время мы вам ответим 🙏")
                return responses
            
            elif action == "answer_question" and ai_response:
                # Отвечаем на вопрос напрямую
                responses.append(ai_response)
                return responses
            
            elif action == "continue_dialog":
                # Продолжаем нормальный диалог (извлечение данных и т.д.)
                # Переходим к обычной логике ниже
                pass
            
            else:
                # Не первый контакт но state=NEW - что-то странное
                if not first_contact:
                    logger.warning(f"[{user_id}] ⚠️ state=NEW но first_contact=False")
                    send_telegram_notification(user_id, f"⚠️ Странное состояние\n💬 {message}")
                    self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
                    return []

        # =================================================================
        # 🔥 ОБРАБОТКА КОМАНДЫ "НАЧАТЬ" ТОЛЬКО В СОСТОЯНИИ GREETED
        # =================================================================
        # Команда "начать" теперь обрабатывается ТОЛЬКО в состоянии GREETED (см. выше)
        # Для старых пользователей - передаём менеджеру

        # =================================================================
        # УМНЫЙ АНАЛИЗ НАМЕРЕНИЯ (через AI)
        # =================================================================
        # ❌ ОТКЛЮЧЕНО: AI-анализ намерений мешает нормальному диалогу!
        # Проблема: бот ждёт ответ на конкретный вопрос (город, дату, людей),
        # а AI думает что "2 взрослых" или "Нет" - это НЕ про туры и вызывает менеджера.
        # 
        # Анализ намерений нужен только для ПРОИЗВОЛЬНЫХ сообщений,
        # когда бот НЕ задавал конкретный вопрос.
        # 
        # Пока просто отключаем этот блок.
        
        # if state not in [ConversationState.NEW, ConversationState.GREETED]:
        #     logger.info(f"[{user_id}] 🤖 Анализируем намерение сообщения...")
        #     intent_result = await analyze_user_intent(message)
        #     ...
        #     (код закомментирован)

        # Сохраняем сообщение в историю
        self.storage.add_message(user_id, "user", message)

        # Определяем язык
        user_lang = self.detect_language(message)
        self.storage.update_conversation(user_id, language=user_lang)

        # Извлекаем данные из сообщения
        travel_info = self.extract_travel_info(message, user_id)

        # Сохраняем все данные, включая unsupported_city (но не в travel_info)
        unsupported_city = travel_info.pop("unsupported_city", None)
        
        if travel_info:
            # Не перезаписываем точную дату приблизительной (только если уже есть точная дата!)
            existing_approx = conv.get("date_approximate", True)
            existing_month  = conv.get("departure_month")
            if (travel_info.get("date_approximate") == True
                    and existing_month is not None
                    and existing_approx == False):
                # Уже есть точная дата — не затираем её приблизительной
                travel_info.pop("departure_date", None)
                travel_info.pop("departure_month", None)
                travel_info.pop("date_approximate", None)
            self.storage.update_conversation(user_id, **travel_info)
            logger.info(f"[{user_id}] Extracted: {travel_info}")

        # Если обнаружен неподдерживаемый город - помечаем его
        if unsupported_city:
            logger.info(f"[{user_id}] ⚠️ Unsupported city detected: {unsupported_city}")
            conv["unsupported_city"] = unsupported_city

        # Рефрешим conv после обновления
        conv = self.storage.get_conversation(user_id)

        # =================================================================
        # ПОШАГОВЫЙ ДИАЛОГ
        # =================================================================

        # ── ШАГ 0: Новый пользователь ────────────────────────────────────
        if state == ConversationState.NEW:
            # Не должны сюда попадать - уже обработано выше
            logger.warning(f"[{user_id}] ⚠️ Попали в state=NEW после проверок")
            return []

        # ── ШАГ 1: Ждём город + дату ─────────────────────────────────────
        elif state in (ConversationState.ASKED_CITY_DATE, ConversationState.ASKED_DETAILS):
            # 🤖 УМНЫЙ АНАЛИЗ ЧЕРЕЗ GPT-4o
            logger.info(f"[{user_id}] 🤖 Анализируем ответ через GPT-4o...")
            analysis = await self.analyze_user_answer(user_id, message, "city_and_date")
            
            # Проверяем хочет ли отменить
            if analysis.get("should_cancel"):
                logger.info(f"[{user_id}] ❌ Клиент отменил подбор")
                send_telegram_notification(
                    user_id,
                    f"❌ Клиент отменил подбор тура на этапе ввода города/даты\n💬 Сообщение: '{message}'",
                    ""
                )
                self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
                return []  # МОЛЧИМ
            
            # Проверяем понял ли бот ответ
            if not analysis.get("understood"):
                # НЕ ПОНЯЛ - отправляем вежливое уточнение С КНОПКОЙ ОТМЕНЫ
                user_lang = conv.get("language", "ru")
                polite_response = analysis.get("response", "Извините, я не совсем понял ваш ответ 😊")
                
                conv["_send_with_cancel_button"] = True
                conv["_cancel_button_message"] = polite_response
                cancel_labels = {"ru": "Отмена", "en": "Cancel", "uk": "Скасувати", "pl": "Anuluj", "da": "Annullere"}
                conv["_cancel_button_label"] = cancel_labels.get(user_lang, "Cancel")
                
                responses.append("")  # Пустой, реальное сообщение с кнопкой отправится через webhook
                return responses
            
            # ПОНЯЛ - продолжаем обычную логику
            has_city  = conv.get("departure_city")
            has_month = conv.get("departure_month")

            months_ru_gen = {1:"январе",2:"феврале",3:"марте",4:"апреле",5:"мае",6:"июне",
                             7:"июле",8:"августе",9:"сентябре",10:"октябре",11:"ноябре",12:"декабре"}

            if has_city and not has_month:
                # Есть город, нет месяца — спрашиваем дату С КНОПКОЙ ОТМЕНЫ
                city_ru = city_names_ru.get(has_city, has_city)
                user_lang = conv.get("language", "ru")
                
                date_question = f"✈️ Отлично! Вылет из {city_ru} 😊\n\n📅 В каком месяце планируете?"
                
                conv["_send_with_cancel_button"] = True
                conv["_cancel_button_message"] = date_question
                cancel_labels = {"ru": "Отмена", "en": "Cancel", "uk": "Скасувати", "pl": "Anuluj", "da": "Annullere"}
                conv["_cancel_button_label"] = cancel_labels.get(user_lang, "Cancel")
                
                responses.append("")  # Пустой, реальное сообщение с кнопкой
                return responses
            
            elif has_city and has_month:
                # Есть и город и месяц — переходим к следующему шагу
                city_ru = city_names_ru.get(has_city, has_city)
                dep = conv.get("departure_date")
                date_approx = conv.get("date_approximate", True)
                has_adults_now = conv.get("adults")
                if dep and not date_approx and dep.day not in (1, 5, 10, 15, 20, 25):
                    date_info = dep.strftime("%d.%m.%Y")
                else:
                    date_info = f"в {months_ru_gen.get(has_month, str(has_month))}" if has_month else ""

                if has_adults_now and has_adults_now > 0:
                    # Уже знаем количество людей
                    has_children_now = conv.get("children")  # None = не спрашивали ещё
                    people_confirm = f"{has_adults_now} взр."
                    if has_children_now is not None and has_children_now >= 0:
                        if has_children_now > 0:
                            ages_str = ""
                            child_ages = conv.get("child_ages", [])
                            if child_ages:
                                ages_str = " (" + ", ".join(f"{a} лет" for a in child_ages[:3]) + ")"
                            people_confirm += f" + {has_children_now} реб.{ages_str}"
                            if has_children_now >= 3:
                                people_confirm += " → 2 номера"
                        # Дети уже уточнены — идём к бюджету
                        self.storage.set_state(user_id, ConversationState.ASKED_BUDGET)
                        user_lang = conv.get("language", "ru")
                        responses.append(
                            f"Отлично! ✈️ Вылет из {city_ru}, {date_info}, {people_confirm} — понял! 😊\n\n"
                            + get_script("ask_budget", user_lang)
                        )
                    else:
                        # Дети не уточнены — спрашиваем про них
                        self.storage.set_state(user_id, ConversationState.ASKED_CHILDREN)
                        responses.append(
                            f"Отлично! ✈️ Вылет из {city_ru}, {date_info}, {has_adults_now} взр. — понял! 😊\n\n"
                            f"👶 Едут ли дети? Если да — напишите сколько и возраст.\n"
                            f"Если нет — просто напишите «нет» 😊"
                        )
                else:
                    responses.append(
                        f"Отлично! ✈️ Вылет из {city_ru}, {date_info} — понял! 😊\n\n"
                        + SCRIPTS["ask_people"]
                    )
                    self.storage.set_state(user_id, ConversationState.ASKED_PEOPLE)

            elif has_city and not has_month:
                # Есть город, нет месяца — один короткий уточняющий вопрос
                city_ru = city_names_ru.get(has_city, has_city)
                responses.append(
                    f"✈️ Вылет из {city_ru} — отлично! 😊\n\n"
                    f"📅 В каком месяце планируете?"
                )
                # Остаёмся в ASKED_CITY_DATE

            elif not has_city and has_month:
                # Есть месяц, нет города — один короткий уточняющий вопрос
                dep = conv.get("departure_date")
                date_approx = conv.get("date_approximate", True)
                if dep and not date_approx and dep.day not in (1, 5, 10, 15, 20, 25):
                    date_info = dep.strftime("%d.%m.%Y")
                else:
                    date_info = f"в {months_ru_gen.get(has_month, str(has_month))}"
                responses.append(
                    f"📅 {date_info.capitalize()} — понял! 😊\n\n"
                    f"✈️ Откуда будет вылет? (Копенгаген, Биллунд, Орхус, Ольборг, Осло, Стокгольм, Гётеборг, Хельсинки...)"
                )
                # Остаёмся в ASKED_CITY_DATE

            else:
                # Ничего не поняли — НЕ повторяем вопрос, передаём менеджеру
                logger.info(f"[{user_id}] ⚠️ Не удалось понять ответ - вызываем менеджера")

                send_telegram_notification(
                    user_id,
                    f"❌ Бот не смог понять ответ клиента\n"
                    f"💬 Сообщение: '{message}'\n"
                    f"📊 Ожидалось: город и дата вылета\n"
                    f"Требуется помощь менеджера!",
                    ""
                )

                self.storage.set_state(user_id, ConversationState.BOT_DISABLED)
                # Нейтральный автоответчик - не раскрываем что это бот
                user_lang = conv.get("language", "ru")
                responses.append(get_script("transfer_to_manager", user_lang))
                return responses

        # ── ШАГ 2: Ждём количество людей ─────────────────────────────────
        elif state == ConversationState.ASKED_PEOPLE:
            has_adults   = conv.get("adults")
            has_children = conv.get("children")  # None = не спрашивали ещё
            has_city     = conv.get("departure_city")
            has_month    = conv.get("departure_month")

            # Проверяем: явно ли упомянуто количество людей в текущем сообщении
            people_mentioned_in_msg = bool(
                re.search(r'взрослых?\s*([1-9]|10)', message_lower)
                or re.search(r'количество\s*взрослых?\s*([1-9]|10)', message_lower)
                or re.search(r'(\d+)\s*взросл', message_lower)
            )
            no_people_in_msg = bool(
                re.search(r'\bнет\s*взрослых?\b', message_lower)
                or re.search(r'\bтолько\s*дети\b', message_lower)
                or re.search(r'\bбез\s*взрослых?\b', message_lower)
                or 'детей нет' in message_lower
                or 'дети нет' in message_lower
                or 'без детей' in message_lower
                or message_lower.strip() in ["нет", "только дети", "без взрослых", "взрослые", "двое взрослых", "двое"]
            )

            # Если не извлекли взрослых из сообщения — ищем число напрямую
            if not has_adults or has_adults <= 0:
                # Сначала ищем текстовые числа (два, три, четыре)
                text_numbers = {
                    'один': 1, 'одна': 1, 'одного': 1, 'одному': 1,
                    'два': 2, 'двое': 2, 'двух': 2, 'двум': 2, 'двоих': 2,
                    'три': 3, 'трое': 3, 'троих': 3, 'трём': 3,
                    'четыре': 4, 'четверо': 4, 'четырёх': 4, 'четырем': 4,
                    'пять': 5, 'пятеро': 5,
                    'шесть': 6, 'шестеро': 6,
                    'семь': 7, 'семеро': 7,
                    'восемь': 8, 'восьмеро': 8,
                }
                
                for word, num in text_numbers.items():
                    if f'{word} взрослых' in message_lower or f'{word} взросл' in message_lower:
                        has_adults = num
                        self.storage.update_conversation(user_id, adults=has_adults)
                        break
                
                # Если не нашли текстовое, ищем цифровое
                if not has_adults or has_adults <= 0:
                    adult_match = re.search(r'(\d+)\s*взросл', message_lower)
                    if adult_match:
                        has_adults = int(adult_match.group(1))
                        self.storage.update_conversation(user_id, adults=has_adults)
                    else:
                        num_match = re.search(r'\b([1-9]|10)\b', message_lower)
                        if num_match:
                            n = int(num_match.group(1))
                            self.storage.update_conversation(user_id, adults=n)
                            has_adults = n
                conv = self.storage.get_conversation(user_id)
                has_adults = conv.get("adults")
                has_children = conv.get("children")
                logger.info(f"[{user_id}] Обновлено: adults={has_adults}, children={has_children}")

            if has_adults and has_adults > 0:
                children_count = has_children if has_children is not None else 0

                # Если дети явно упомянуты в этом же сообщении или уже есть в conv
                if people_mentioned_in_msg or (has_children is not None and has_children >= 0) or no_people_in_msg:
                    if no_people_in_msg and not people_mentioned_in_msg:
                        children_count = 0
                        self.storage.update_conversation(user_id, children=0)
                    # Идём сразу к бюджету
                    self.storage.set_state(user_id, ConversationState.ASKED_BUDGET)
                    people_confirm = f"{has_adults} взр."
                    if children_count > 0:
                        child_ages_now = conv.get("child_ages", [])
                        ages_str = ""
                        if child_ages_now:
                            ages_str = " (" + ", ".join(f"{a} лет" for a in child_ages_now[:3]) + ")"
                        people_confirm += f" + {children_count} реб.{ages_str}"
                        if children_count >= 3:
                            people_confirm += " → 2 номера"
                    user_lang = conv.get("language", "ru")
                    responses.append(f"👥 {people_confirm} — понял! 😊\n\n" + get_script("ask_budget", user_lang))
                else:
                    # Дети не упомянуты — спрашиваем отдельно
                    self.storage.set_state(user_id, ConversationState.ASKED_CHILDREN)
                    responses.append(
                        f"👥 {has_adults} взр. — понял! 😊\n\n"
                        f"👶 Едут ли дети? Если да — напишите сколько и возраст.\n"
                        f"Если нет — просто напишите «нет» 😊"
                    )
            else:
                # Не поняли количество — переспрашиваем кратко
                responses.append(
                    "Сколько человек едет? 😊"
                )

        # ── ШАГ 2б: Ждём ответ про детей ────────────────────────────────
        elif state == ConversationState.ASKED_CHILDREN:
            has_adults = conv.get("adults") or 2
            # Извлекаем детей из ответа
            children_count = 0
            no_children = bool(
                re.search(r'\bбез\s*дет[еёйи]|\bтолько\s*взросл|\bне\s*будет\s*дет[еёй]|\bдетей\s*нет\b', message_lower)
                or message_lower.strip() in ["нет", "без детей", "только взрослые", "нет дети", "не будет детей", "без ребёнка", "без ребенка", "взрослые", "двое", "no", "no kids", "no children"]
            )
            if no_children:
                children_count = 0
                self.storage.update_conversation(user_id, children=0)
            else:
                # Ищем количество детей
                ch_match = re.search(r'(\d+)\s*реб[её]н', message_lower)
                if not ch_match:
                    ch_match = re.search(r'(\d+)\s*дет[еёй]', message_lower)
                if ch_match:
                    children_count = int(ch_match.group(1))
                    self.storage.update_conversation(user_id, children=children_count)
                else:
                    # Ищем возраста детей — если нашли, считаем по количеству возрастов
                    age_found = re.findall(r'(\d+)\s*(?:лет|год)', message_lower)
                    ages_children = [int(a) for a in age_found if int(a) < 18]
                    if ages_children:
                        children_count = len(ages_children)
                        self.storage.update_conversation(user_id, children=children_count, child_ages=ages_children)
                    elif re.search(r'реб[её]нок|реб[её]нка|ребёночек|малыш', message_lower):
                        children_count = 1
                        self.storage.update_conversation(user_id, children=1)
                    else:
                        # Одно число — может быть количество детей
                        single_num = re.search(r'\b([1-5])\b', message_lower)
                        if single_num:
                            children_count = int(single_num.group(1))
                            self.storage.update_conversation(user_id, children=children_count)

            conv = self.storage.get_conversation(user_id)
            children_count = conv.get("children") or 0
            user_lang = conv.get("language", "ru")
            logger.info(f"[{user_id}] Дети: children={children_count}")

            self.storage.set_state(user_id, ConversationState.ASKED_BUDGET)
            people_confirm = f"{has_adults} взр."
            if children_count > 0:
                child_ages_now = conv.get("child_ages", [])
                ages_str = ""
                if child_ages_now:
                    ages_str = " (" + ", ".join(f"{a} лет" for a in child_ages_now[:3]) + ")"
                people_confirm += f" + {children_count} реб.{ages_str}"
                if children_count >= 3:
                    people_confirm += " → подберём 2 номера"
            responses.append(f"👥 {people_confirm} — понял! 😊\n\n" + get_script("ask_budget", user_lang))

        # ── ШАГ 3: Ждём бюджет / требования ─────────────────────────────
        elif state == ConversationState.ASKED_BUDGET:
            # Сохраняем требования/бюджет если есть
            budget_match = re.search(r'(\d+)\s*(?:евро|euro|eur|€)', message.lower())
            if budget_match:
                self.storage.update_conversation(user_id, budget=int(budget_match.group(1)))
            if "перв" in message.lower() and "лини" in message.lower():
                self.storage.update_conversation(user_id, requirements="первая линия")
            elif message.strip() and message.lower().strip() not in ["нет", "no", "не знаю", "без разницы", "-"]:
                self.storage.update_conversation(user_id, requirements=message.strip())
            
            # Обновляем conv после сохранения данных
            conv = self.storage.get_conversation(user_id)
            
            # ПРОВЕРКА: Если город неподдерживаемый - вызываем менеджера
            if conv.get("unsupported_city"):
                bad_city = conv.get("unsupported_city")
                logger.info(f"[{user_id}] ⚠️ Все данные собраны, но город неподдерживаемый: {bad_city}")
                
                # Формируем информацию о запросе
                destination = conv.get("destination", "не указано")
                adults = conv.get("adults", 0)
                children = conv.get("children", 0)
                month = conv.get("departure_month")
                nights = conv.get("nights", 7)
                
                request_summary = (
                    f"👤 User ID: {user_id}\n"
                    f"🌍 Направление: {destination}\n"
                    f"📍 Город вылета: {bad_city} (НЕ ПОДДЕРЖИВАЕТСЯ)\n"
                    f"👥 Туристов: {adults} взр."
                )
                if children > 0:
                    request_summary += f" + {children} дет."
                if month:
                    months_ru = {1:"январь",2:"февраль",3:"март",4:"апрель",5:"май",6:"июнь",
                                7:"июль",8:"август",9:"сентябрь",10:"октябрь",11:"ноябрь",12:"декабрь"}
                    request_summary += f"\n📅 Месяц: {months_ru.get(month, str(month))}"
                request_summary += f"\n🌙 Ночей: {nights}\n\n"
                request_summary += f"Полное сообщение: {message}"
                
                # Отправляем уведомление менеджеру
                send_telegram_notification(
                    user_id,
                    f"❗ ЗАПРОС С НЕПОДДЕРЖИВАЕМЫМ ГОРОДОМ\n\n{request_summary}\n\n⚠️ Требуется помощь менеджера!"
                )
                
                # Клиенту отвечаем что подбираем варианты
                user_lang = conv.get("language", "ru")
                responses.append(get_script("searching_tours", user_lang))
                
                # Отключаем бота - дальше только менеджер
                self.storage.set_state(user_id, ConversationState.MANAGER_CALLED)
                return responses
            
            # Немедленно возвращаем "Ищу туры" — тяжелый поиск запустится в webhook потоке
            self.storage.set_state(user_id, ConversationState.SEARCHING)
            # Специальный флаг — означает что надо запустить поиск в этом же потоке
            conv['_run_search'] = True
            user_lang = conv.get("language", "ru")
            return [get_script("searching_tours", user_lang)]

        # ── Идёт поиск ────────────────────────────────────────────────
        elif state == ConversationState.SEARCHING:
            pass

        # ── После выдачи вариантов (ASKED_REQUIREMENTS = устаревший, объединяем) ──
        elif state == ConversationState.ASKED_REQUIREMENTS:
            if message.strip():
                self.storage.update_conversation(user_id, requirements=message)
            self.storage.set_state(user_id, ConversationState.SEARCHING)
            conv = self.storage.get_conversation(user_id)
            conv['_run_search'] = True
            user_lang = conv.get("language", "ru")
            return [get_script("searching_tours", user_lang)]

        elif state == ConversationState.SENT_OPTIONS:
            msg_lower = message.lower().strip()

            # ─── Слова НЕ являющиеся запросом нового поиска ─────────────────
            _skip_research_words = [
                "паспорт", "бронир", "беру", "берём", "берем", "оплат", "купить", "заказать",
                "дорого", "дорог", "букинг", "booking", "что входит", "что включ", "виза", "страховк",
                "трансфер", "рекоменд", "расскажи", "подробн", "фото", "отзыв",
                "что нужно для брон", "как забронировать", "как оформ", "как купить", "что для бронирования",
            ]
            _is_service_msg = any(w in msg_lower for w in _skip_research_words)

            # ─── Новый поиск с другой датой / месяцем ────────────────────────
            # "а на май?", "а до 10 июля?", "а в июне?", "покажи на август"
            _new_date_extracted = self.extract_travel_info(message, user_id)
            _has_new_date = (
                _new_date_extracted.get("departure_date") is not None
                or _new_date_extracted.get("departure_month") is not None
            )

            # Сигналы запроса нового поиска с другой датой
            _new_search_signals = [
                "а на ", "на май", "на июн", "на июл", "на август", "на сентябр", "на октябр",
                "на ноябр", "на декабр", "на январ", "на феврал", "на март", "на апрел",
                "в мае", "в июне", "в июле", "в августе", "в сентябре", "в октябре",
                "покажи на", "найди на", "подбери на", "что есть на", "есть на",
                "до 10", "до 15", "до 20", "до 25", "до 30",
                "вернуться до", "вернуться к", "уехать до", "выехать до",
                "следующий месяц", "следующем месяце",
                "другие даты", "другие числа", "поменяйте дату", "другую дату",
                "may", "june", "july", "august",
            ]
            _has_search_signal = any(sig in msg_lower for sig in _new_search_signals)

            # Также проверяем новый город вылета
            _new_city_extracted = _new_date_extracted.get("departure_city")
            _has_new_city = _new_city_extracted is not None and _new_city_extracted != conv.get("departure_city")

            if (_has_new_date or _has_search_signal or _has_new_city) and not _is_service_msg:
                # Обновляем параметры и делаем новый поиск
                if _has_new_city:
                    self.storage.update_conversation(user_id, departure_city=_new_city_extracted)

                if _new_date_extracted.get("departure_date"):
                    self.storage.update_conversation(
                        user_id,
                        departure_date=_new_date_extracted["departure_date"],
                        departure_month=_new_date_extracted.get("departure_month"),
                        date_approximate=_new_date_extracted.get("date_approximate", False),
                    )
                elif _new_date_extracted.get("departure_month"):
                    self.storage.update_conversation(
                        user_id,
                        departure_date=datetime(2026, _new_date_extracted["departure_month"], 15),
                        departure_month=_new_date_extracted["departure_month"],
                        date_approximate=True,
                    )
                elif _has_search_signal and not _has_new_date:
                    # Сигнал новой даты но не распознали — просим уточнить
                    responses.append("Подскажите пожалуйста — на какой месяц или дату хотите варианты? 😊")
                    return responses

                # Сохраняем остальные параметры (город, взрослые, дети — не меняем!)
                self.storage.set_state(user_id, ConversationState.SEARCHING)
                conv = self.storage.get_conversation(user_id)
                conv['_run_search'] = True
                user_lang = conv.get("language", "ru")
                return [get_script("searching_tours", user_lang)]

            # ─── Что нужно для бронирования ─────────────────────────────────
            if any(w in msg_lower for w in [
                "что нужно", "как забронировать", "что для бронирования", "как оформить",
                "как купить тур", "как заказать тур", "порядок", "процесс",
                "что надо для брон", "что надо сделать",
            ]):
                responses.append(SCRIPTS["booking_info"])

            # ─── Готов бронировать / паспорта ───────────────────────────────
            elif any(w in msg_lower for w in ["паспорт", "скидываю паспорт", "скину паспорт", "вот паспорт"]):
                responses.append(SCRIPTS["need_passport_for_booking"])
                self.storage.set_state(user_id, ConversationState.WAITING_PASSPORT)

            elif any(w in msg_lower for w in ["бронируем", "бронировать", "беру", "берём", "берем", "оплатить", "оплата", "забронировать", "заказать"]):
                responses.append(SCRIPTS["booking_info"])
                self.storage.set_state(user_id, ConversationState.WAITING_PASSPORT)

            # ─── Согласие ────────────────────────────────────────────────────
            elif msg_lower in ["да", "да!", "окей", "ок", "хорошо", "подходит", "ok", "yes", "норм", "идёт", "идет"]:
                responses.append("Отлично! 🎉\n\n" + SCRIPTS["booking_info"])
                self.storage.set_state(user_id, ConversationState.WAITING_PASSPORT)

            # ─── ДОРОГО ──────────────────────────────────────────────────────
            elif any(w in msg_lower for w in ["дорого", "дорог", "дорогов", "expensive", "too much", "много денег", "не могу столько"]):
                responses.append(SCRIPTS["expensive_followup"])

            # ─── Сравнение с другими ─────────────────────────────────────────
            elif any(w in msg_lower for w in ["букинг", "booking", "другой сайт", "видел дешевле", "видела дешевле", "нашёл дешевле", "нашла дешевле", "самостоятельно"]):
                responses.append(SCRIPTS["compare_price"])

            # ─── Другие варианты / не то ─────────────────────────────────────
            elif any(w in msg_lower for w in ["другие", "другой вариант", "что-то ещё", "ещё варианты", "покажи ещё", "больше вариантов"]):
                responses.append("Конечно, могу подобрать другие варианты 😊\n\nЧто именно не подошло? Цена, отель, или хотите другое направление?")

            elif any(w in msg_lower for w in ["не подходит", "не то", "не нравится", "не устраивает"]):
                responses.append("Понял 😊 Скажите — что именно не подошло? Цена, конкретный отель, или хотите совсем другое направление?\n\nПостараюсь подобрать то, что вам понравится!")

            # ─── "нет" отдельно — уточняем что не устраивает ────────────────
            elif msg_lower.strip() in ["нет", "нет.", "нет!", "no"]:
                responses.append("Понял 😊 Что именно не подошло — цена, даты или отель? Подберу другие варианты!")

            # ─── Вопрос о конкретном отеле ──────────────────────────────────
            elif any(w in msg_lower for w in ["расскажи", "подробнее", "об отеле", "про отель", "отзывы", "описание", "фото", "смотреть"]):
                responses.append(SCRIPTS["hotel_photos"])

            # ─── Что включено в стоимость ────────────────────────────────────
            elif any(w in msg_lower for w in ["что входит", "что включено", "что в цену", "входит ли", "включено ли", "что в стоимость"]):
                responses.append(SCRIPTS["what_included"])

            # ─── Виза / документы ────────────────────────────────────────────
            elif any(w in msg_lower for w in ["виза", "документы нужны", "какие документы", "passport", "visa"]):
                responses.append(SCRIPTS["visa_info"])

            # ─── Страховка ───────────────────────────────────────────────────
            elif "страховк" in msg_lower:
                responses.append(SCRIPTS["insurance"])

            # ─── Тенерифе / другие направления ──────────────────────────────
            elif any(w in msg_lower for w in ["тенерифе", "tenerife", "майорка", "mallorca", "испания", "spain", "барселона", "barcelona"]):
                responses.append(SCRIPTS["tenerife_info"])
                # Определяем конкретное направление
                if "тенерифе" in msg_lower or "tenerife" in msg_lower:
                    new_dest = "tenerife"
                elif "майорка" in msg_lower or "mallorca" in msg_lower or "мальорка" in msg_lower:
                    new_dest = "mallorca"
                elif "барселона" in msg_lower or "barcelona" in msg_lower:
                    new_dest = "barcelona"
                else:
                    new_dest = "spain"
                self.storage.update_conversation(user_id, destination=new_dest)
                self.storage.set_state(user_id, ConversationState.ASKED_BUDGET)

            elif any(w in msg_lower for w in ["турция", "turkey", "анталья", "анталия", "кемер", "алания", "аланья"]):
                responses.append("Отличный выбор — Турция! 🇹🇷\n\nУ нас есть туры в Анталью, Кемер, Аланью, Сиде, Белек, Бодрум.\nНапишите примерные даты и количество человек, подберу варианты 😊")
                self.storage.update_conversation(user_id, destination="turkey")
                self.storage.set_state(user_id, ConversationState.ASKED_BUDGET)

            elif any(w in msg_lower for w in ["другие направления", "куда ещё", "что ещё есть", "другие страны"]):
                responses.append(SCRIPTS["other_countries"])

            # ─── Для бронирования что нужно ─────────────────────────────────
            elif any(w in msg_lower for w in ["что нужно", "как забронировать", "как оплатить", "как заказать", "как купить", "процесс", "порядок"]):
                responses.append(SCRIPTS["need_passport_for_booking"])

            # ─── Вопросы о трансфере ─────────────────────────────────────────
            elif any(w in msg_lower for w in ["трансфер", "встретят", "встреча", "аэропорт", "довезут"]):
                responses.append("Да, трансфер включён в стоимость! 🚗\n\nВас встретят у выхода из аэропорта с табличкой и отвезут прямо в отель. После отдыха трансфер также отвезёт обратно в аэропорт 😊")

            # ─── Рекомендация отеля ──────────────────────────────────────────
            elif any(w in msg_lower for w in ["рекомендуете", "посоветуйте", "лучший", "лучший отель", "какой лучше", "какой выбрать"]):
                responses.append("Рекомендую обратить внимание на отели с рейтингом 4.5+ — там питание разнообразное, сервис хороший, туристы очень довольны 😊\n\nЕсли скажете ваши предпочтения (спокойный отдых / активный / семейный / для двоих) — подберу что-то более точечно!")

            # ─── Вопрос о детях ──────────────────────────────────────────────
            elif any(w in msg_lower for w in ["дети", "ребёнок", "ребенок", "kids", "child"]):
                responses.append("Для детей действуют скидки в зависимости от возраста 😊\n\nНапишите возраст ребёнка/детей, и я пересчитаю стоимость!")

            # ─── FAQ: Что нужно для бронирования ────────────────────────────
            elif any(w in msg_lower for w in ["как забронировать", "что нужно для бронирования", "как оформить", "как заказать тур", "процесс бронирования", "порядок оформления"]):
                responses.append(SCRIPTS["booking_info"])

            # ─── FAQ: Часто задаваемые вопросы туристов ─────────────────────
            elif any(w in msg_lower for w in ["часто спрашивают", "вопросы туристов", "что спрашивают", "faq"]):
                responses.append(
                    "❓ Часто задаваемые вопросы:\n\n"
                    "📋 *Что нужно для бронирования?*\nФото 1-й страницы паспорта, оплата по ссылке в евро или гривне с любой карты.\n\n"
                    "📦 *Что входит в стоимость?*\nАвиаперелёт, трансфер, проживание, питание, страховка.\n\n"
                    "🛡 *Страховка покрывает?*\nМедпомощь, эвакуацию, компенсацию при задержке рейса.\n\n"
                    "🏨 *Как посмотреть отель?*\nВведите название на booking.com — там фото, отзывы, описание.\n\n"
                    "🚗 *Трансфер включён?*\nДа! Встретят у самолёта и отвезут прямо в отель 😊"
                )

            # ─── FAQ: О компании ─────────────────────────────────────────────
            elif any(w in msg_lower for w in ["расскажите о компании", "что за компания", "о вас", "кто вы", "ваша компания"]):
                responses.append(SCRIPTS["about_company"])

            # ─── Неизвестное направление ─────────────────────────────────
            elif any(w in msg_lower for w in ["риф", "reef", "мальдив", "бали", "тайланд", "таиланд", "thailand", "греция", "кипр", "хорватия", "черногория", "монтенегро"]):
                responses.append(
                    "К сожалению, это направление сейчас не в нашем каталоге 😔\n\n"
                    "Мы специализируемся на турах в Египет (Шарм-Эль-Шейх, Хургада), Турцию и Испанию/Тенерифе "
                    "с вылетами из Скандинавии и Финляндии.\n\n"
                    "Могу подобрать отличный вариант по одному из этих направлений — там потрясающие пляжи и отличное соотношение цены и качества! 😊"
                )

            # ─── Подтверждение / вопросов нет ───────────────────────────────
            elif any(w in msg_lower for w in ["понятно", "ясно", "хорошо", "спасибо", "thanks", "thank you", "ок", "окей"]) and len(msg_lower) < 20:
                responses.append("Отлично! 😊 Если решите бронировать или появятся вопросы — пишите, всегда помогу! ✈️🌴")

            # ─── Неизвестный вопрос — умный дефолт ──────────────────────────
            else:
                # Пробуем openai если ключ есть
                ai_response = self._get_ai_response(user_id, message)
                if ai_response:
                    responses.append(ai_response)
                else:
                    responses.append(
                        "Конечно, отвечу на ваш вопрос! 😊\n\n"
                        "Если хотите уточнить детали по отелю, стоимости, бронированию или датам — "
                        "пишите, всегда помогу найти лучший вариант для вас! ✈️🌴"
                    )

        elif state == ConversationState.WAITING_PASSPORT:
            msg_lower = message.lower()
            if any(w in msg_lower for w in ["паспорт", "фото", "документ", "скидываю", "скину", "вот", "прикрепляю"]):
                responses.append(SCRIPTS["passport_received"])
                self.storage.set_state(user_id, ConversationState.MANAGER_CALLED)

                # Уведомляем менеджеров о получении паспортов
                send_telegram_notification(user_id, "✅ Клиент отправил паспорта для бронирования!")

                logger.info(f"[{user_id}] ⚠️ МЕНЕДЖЕР НУЖЕН - паспорта получены!")
            elif any(w in msg_lower for w in ["вопрос", "уточнить", "?"]):
                ai_resp = self._get_ai_response(user_id, message)
                responses.append(ai_resp if ai_resp else "Слушаю вас 😊 Задавайте ваш вопрос!")
            else:
                responses.append("Жду фото первой страницы загранпаспорта 😊\n\nЕсли есть вопросы — пишите, отвечу на всё!")

        elif state == ConversationState.MANAGER_CALLED:
            # Менеджер уже подключён / паспорта получены — бот продолжает отвечать на вопросы
            msg_lower = message.lower().strip()
            ai_resp = self._get_ai_response(user_id, message)
            if ai_resp:
                responses.append(ai_resp)
            elif any(w in msg_lower for w in ["спасибо", "thank", "ок", "хорошо", "понятно"]):
                responses.append("Отлично! 😊 В ближайшее время с вами свяжутся для подтверждения бронирования. Приятного отдыха! ✈️🌴")
            else:
                responses.append("Спасибо! Менеджер скоро свяжется с вами для подтверждения бронирования 😊\n\nЕсли появятся вопросы — пишите!")

        # Сохраняем все ответы в историю чата перед возвратом
        self._save_responses_to_history(user_id, responses)
        
        return responses

    def _get_faq_response(self, faq_key: str, language: str) -> Optional[str]:
        """Возвращает FAQ ответ на нужном языке"""
        faq_responses = {
            "что_входит": {
                "ru": "✈️ В стоимость тура входит:\n\n• Авиаперелёт туда и обратно\n• Трансфер аэропорт-отель-аэропорт\n• Проживание в отеле\n• Питание (завтрак, полупансион или всё включено - зависит от отеля)\n• Медицинская страховка\n\nДополнительно оплачивается:\n• Виза (если требуется)\n• Экскурсии по желанию",
                "uk": "✈️ У вартість туру входить:\n\n• Авіапереліт туди і назад\n• Трансфер аеропорт-готель-аеропорт\n• Проживання в готелі\n• Харчування (сніданок, напівпансіон або все включено - залежить від готелю)\n• Медична страховка\n\nДодатково оплачується:\n• Віза (якщо потрібна)\n• Екскурсії за бажанням",
                "en": "✈️ The tour price includes:\n\n• Round-trip airfare\n• Airport-hotel-airport transfer\n• Hotel accommodation\n• Meals (breakfast, half board or all-inclusive - depends on hotel)\n• Medical insurance\n\nExtra charges:\n• Visa (if required)\n• Optional excursions",
                "pl": "✈️ Cena wycieczki obejmuje:\n\n• Przelot tam i z powrotem\n• Transfer lotnisko-hotel-lotnisko\n• Zakwaterowanie w hotelu\n• Wyżywienie (śniadanie, półpansion lub all inclusive - zależy od hotelu)\n• Ubezpieczenie medyczne\n\nDodatkowo płatne:\n• Wiza (jeśli wymagana)\n• Opcjonalne wycieczki",
            },
            "виза": {
                "ru": "🛂 Информация о визах:\n\n• Египет: виза по прибытию ($25)\n• Турция: виза не нужна для граждан многих стран\n• Испания: Шенгенская виза (если у вас нет)\n\nМожем помочь с оформлением! 😊",
                "uk": "🛂 Інформація про візи:\n\n• Єгипет: віза по прибуттю ($25)\n• Туреччина: віза не потрібна для громадян багатьох країн\n• Іспанія: Шенгенська віза (якщо у вас немає)\n\nМожемо допомогти з оформленням! 😊",
                "en": "🛂 Visa information:\n\n• Egypt: visa on arrival ($25)\n• Turkey: no visa required for many countries\n• Spain: Schengen visa (if you don't have one)\n\nWe can help with visa arrangements! 😊",
                "pl": "🛂 Informacje o wizach:\n\n• Egipt: wiza po przyjeździe ($25)\n• Turcja: wiza nie jest wymagana dla wielu krajów\n• Hiszpania: wiza Schengen (jeśli nie masz)\n\nMożemy pomóc w załatwieniu wizy! 😊",
            },
            "страховка": {
                "ru": "🏥 Медицинская страховка покрывает:\n\n• Неотложную медицинскую помощь\n• Госпитализацию\n• Транспортировку в больницу\n• Репатриацию\n\nВключена в стоимость тура! 😊",
                "uk": "🏥 Медична страховка покриває:\n\n• Невідкладну медичну допомогу\n• Госпіталізацію\n• Транспортування до лікарні\n• Репатріацію\n\nВключена в вартість туру! 😊",
                "en": "🏥 Medical insurance covers:\n\n• Emergency medical care\n• Hospitalization\n• Transportation to hospital\n• Repatriation\n\nIncluded in tour price! 😊",
                "pl": "🏥 Ubezpieczenie medyczne obejmuje:\n\n• Nagłą pomoc medyczną\n• Hospitalizację\n• Transport do szpitala\n• Repatriację\n\nWliczone w cenę wycieczki! 😊",
            },
        }
        
        if faq_key in faq_responses and language in faq_responses[faq_key]:
            return faq_responses[faq_key][language]
        elif faq_key in faq_responses and "ru" in faq_responses[faq_key]:
            return faq_responses[faq_key]["ru"]  # Fallback на русский
        
        return None  # Если нет перевода - вернет None и будет использован старый SCRIPTS

    def _get_alternative_destinations(self, user_id: str, current_destination: str = None) -> str:
        """
        Генерирует предложение альтернативных направлений после показа результатов.
        
        Args:
            user_id: ID пользователя
            current_destination: Текущее направление (sharm, hurghada, turkey, tenerife, spain)
        
        Returns:
            Текст с предложением 2-3 альтернативных направлений
        """
        conv = self.storage.get_conversation(user_id)
        user_lang = conv.get("language", "ru")
        
        # Определяем альтернативы в зависимости от текущего направления
        alternatives = []
        
        if current_destination in ["sharm", "hurghada", "egypt"]:
            # Если показали Египет - предлагаем Турцию и Испанию/Тенерифе
            alternatives = [
                {"key": "turkey", "names": {
                    "ru": "🇹🇷 Турция",
                    "uk": "🇹🇷 Туреччина",
                    "en": "🇹🇷 Turkey",
                    "pl": "🇹🇷 Turcja",
                    "da": "🇹🇷 Tyrkiet",
                }},
                {"key": "tenerife", "names": {
                    "ru": "🇪🇸 Тенерифе (Испания)",
                    "uk": "🇪🇸 Тенеріфе (Іспанія)",
                    "en": "🇪🇸 Tenerife (Spain)",
                    "pl": "🇪🇸 Teneryfa (Hiszpania)",
                    "da": "🇪🇸 Tenerife (Spanien)",
                }}
            ]
        elif current_destination in ["turkey", "antalya"]:
            # Если показали Турцию - предлагаем Египет и Испанию
            alternatives = [
                {"key": "egypt", "names": {
                    "ru": "🇪🇬 Египет (Шарм, Хургада)",
                    "uk": "🇪🇬 Єгипет (Шарм, Хургада)",
                    "en": "🇪🇬 Egypt (Sharm, Hurghada)",
                    "pl": "🇪🇬 Egipt (Sharm, Hurghada)",
                    "da": "🇪🇬 Egypten (Sharm, Hurghada)",
                }},
                {"key": "tenerife", "names": {
                    "ru": "🇪🇸 Тенерифе",
                    "uk": "🇪🇸 Тенеріфе",
                    "en": "🇪🇸 Tenerife",
                    "pl": "🇪🇸 Teneryfa",
                    "da": "🇪🇸 Tenerife",
                }}
            ]
        elif current_destination in ["tenerife", "spain", "mallorca", "barcelona"]:
            # Если показали Испанию - предлагаем Турцию и Египет
            alternatives = [
                {"key": "turkey", "names": {
                    "ru": "🇹🇷 Турция",
                    "uk": "🇹🇷 Туреччина",
                    "en": "🇹🇷 Turkey",
                    "pl": "🇹🇷 Turcja",
                    "da": "🇹🇷 Tyrkiet",
                }},
                {"key": "egypt", "names": {
                    "ru": "🇪🇬 Египет",
                    "uk": "🇪🇬 Єгипет",
                    "en": "🇪🇬 Egypt",
                    "pl": "🇪🇬 Egipt",
                    "da": "🇪🇬 Egypten",
                }}
            ]
        else:
            # По умолчанию предлагаем ТОП-3
            alternatives = [
                {"key": "turkey", "names": {
                    "ru": "🇹🇷 Турция",
                    "uk": "🇹🇷 Туреччина",
                    "en": "🇹🇷 Turkey",
                    "pl": "🇹🇷 Turcja",
                    "da": "🇹🇷 Tyrkiet",
                }},
                {"key": "egypt", "names": {
                    "ru": "🇪🇬 Египет",
                    "uk": "🇪🇬 Єгипет",
                    "en": "🇪🇬 Egypt",
                    "pl": "🇪🇬 Egipt",
                    "da": "🇪🇬 Egypten",
                }},
                {"key": "tenerife", "names": {
                    "ru": "🇪🇸 Тенерифе",
                    "uk": "🇪🇸 Тенеріфе",
                    "en": "🇪🇸 Tenerife",
                    "pl": "🇪🇸 Teneryfa",
                    "da": "🇪🇸 Tenerife",
                }}
            ]
        
        # Формируем текст на нужном языке
        headers = {
            "ru": "\n━━━━━━━━━━━━━━━━━━━━━━\n💡 Могу также предложить:\n━━━━━━━━━━━━━━━━━━━━━━\n",
            "uk": "\n━━━━━━━━━━━━━━━━━━━━━━\n💡 Можу також запропонувати:\n━━━━━━━━━━━━━━━━━━━━━━\n",
            "en": "\n━━━━━━━━━━━━━━━━━━━━━━\n💡 I can also offer:\n━━━━━━━━━━━━━━━━━━━━━━\n",
            "pl": "\n━━━━━━━━━━━━━━━━━━━━━━\n💡 Mogę również zaproponować:\n━━━━━━━━━━━━━━━━━━━━━━\n",
            "da": "\n━━━━━━━━━━━━━━━━━━━━━━\n💡 Jeg kan også tilbyde:\n━━━━━━━━━━━━━━━━━━━━━━\n",
        }
        
        footers = {
            "ru": "\n❓ Хотите посмотреть варианты по другим направлениям?\nПросто напишите интересующую страну! 😊",
            "uk": "\n❓ Бажаєте подивитися варіанти по інших напрямках?\nПросто напишіть цікаву країну! 😊",
            "en": "\n❓ Would you like to see options for other destinations?\nJust write the country you're interested in! 😊",
            "pl": "\n❓ Chcesz zobaczyć opcje dla innych kierunków?\nPo prostu napisz interesujący Cię kraj! 😊",
            "da": "\n❓ Vil du se muligheder for andre destinationer?\nBare skriv det land, du er interesseret i! 😊",
        }
        
        header = headers.get(user_lang, headers["ru"])
        footer = footers.get(user_lang, footers["ru"])
        
        # Собираем список альтернатив
        alt_list = []
        for alt in alternatives[:2]:  # Берем только 2 альтернативы
            name = alt["names"].get(user_lang, alt["names"]["ru"])
            alt_list.append(f"  • {name}")
        
        result = header + "\n".join(alt_list) + footer
        
        logger.info(f"[{user_id}] 💡 Генерируем альтернативы: {[a['key'] for a in alternatives[:2]]}")
        
        return result

    def _get_ai_response(self, user_id: str, message: str) -> Optional[str]:
        """Получаем умный ответ от OpenAI для нестандартных вопросов"""
        if not OPENAI_API_KEY:
            return None
        try:
            conv = self.storage.get_conversation(user_id)
            history = conv.get("chat_history", [])[-6:]  # последние 6 сообщений
            user_language = conv.get("language", "ru")  # Язык пользователя

            # Определяем язык для промпта
            language_instructions = {
                "ru": "Отвечай по-русски",
                "uk": "Відповідай українською мовою",
                "en": "Answer in English",
                "pl": "Odpowiadaj po polsku",
                "da": "Svar på dansk",
                "sv": "Svara på svenska",
                "no": "Svar på norsk",
                "de": "Antworte auf Deutsch",
                "fi": "Vastaa suomeksi",
            }
            
            lang_instruction = language_instructions.get(user_language, "Отвечай по-русски")

            system_prompt = (
                f"Ты — Алина, менеджер по продажам турагентства Travel With Alina. "
                f"Ты помогаешь подобрать туры в Египет (Шарм-Эль-Шейх, Хургаду), Турцию и Испанию/Тенерифе "
                f"с вылетами из Дании, Норвегии, Швеции и Финляндии. "
                f"{lang_instruction}, коротко, дружелюбно и профессионально. "
                f"Цель — помочь клиенту и довести его до покупки тура. "
                f"Если клиент говорит 'дорого' — объясни что входит в цену. "
                f"Если спрашивает про другие направления — расскажи что у нас есть. "
                f"Не придумывай цены — говори что подберёшь варианты. "
                f"Всегда оставайся позитивной и мотивируй к покупке."
            )

            messages_for_ai = [{"role": "system", "content": system_prompt}]
            for h in history:
                messages_for_ai.append(h)
            messages_for_ai.append({"role": "user", "content": message})

            import requests as req
            resp = req.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "gpt-5.1", "messages": messages_for_ai, "max_completion_tokens": 300, "temperature": 0.7},
                timeout=15
            )
            if resp.status_code == 200:
                content = resp.json()["choices"][0]["message"]["content"].strip()
                self.storage.add_message(user_id, "assistant", content)
                logger.info(f"[AI] Ответ на языке {user_language}: {content[:50]}...")
                return content
        except Exception as e:
            logger.error(f"[AI] OpenAI error: {e}")
        return None

    def _do_search(self, user_id: str, conv: dict) -> List[str]:
        """Выполняет реальный поиск туров через TourSelector и возвращает список сообщений"""
        month = conv.get("departure_month") or datetime.now().month
        is_winter = self.is_winter_season(month)
        from_city = conv.get("departure_city") or "Copenhagen"
        adults   = conv.get("adults") or 2
        children = conv.get("children") or 0
        destination    = conv.get("destination")
        nights         = conv.get("nights") or 7
        departure_date = conv.get("departure_date")

        # Если destination явно задан как НЕ египетское направление → всегда лето
        SUMMER_DESTINATIONS = {"tenerife", "spain", "mallorca", "barcelona", "turkey", "antalya"}
        if destination in SUMMER_DESTINATIONS:
            is_winter = False

        # Дата ВСЕГДА приблизительная если пользователь не указал точный день
        now = datetime.now()
        if not departure_date:
            year = now.year
            if month < now.month:
                year += 1
            try:
                departure_date = datetime(year, month, 15)
            except Exception:
                departure_date = now + timedelta(days=30)
            date_approximate = True
        else:
            # Дата указана - проверяем не в прошлом ли она
            if departure_date < now - timedelta(days=1):
                try:
                    departure_date = departure_date.replace(year=departure_date.year + 1)
                except Exception:
                    departure_date = now + timedelta(days=30)
            # Если дата точная (не середина месяца) - используем точную дату
            date_approximate = conv.get("date_approximate", False)

        # ── ЛОГИКА НОМЕРОВ: ─────────────────────────────────────────────────
        # 0–2 детей → 1 поиск: adults + children в 1 номере (API: adults=adults, child=children)
        # 3 детей   → 2 поиска: Номер1(1взр+1реб) + Номер2(1взр+2реб)
        # ──────────────────────────────────────────────────────────────────────
        if children >= 3:
            # Два отдельных поиска для двух номеров
            logger.info(
                f"[{user_id}] 🔍 3+ детей → 2 номера: "
                f"Ном.1=1взр+1реб, Ном.2=1взр+2реб | {from_city}, {departure_date.strftime('%d.%m.%Y')}"
            )
            all_msgs = []
            room_configs = [
                (1, 1, "🛏 НОМЕР 1: 1 взр. + 1 реб."),
                (1, 2, "🛏 НОМЕР 2: 1 взр. + 2 реб."),
            ]
            for room_adults, room_children, room_label in room_configs:
                logger.info(f"[{user_id}] Поиск для {room_label}...")
                try:
                    sel = _tour_selector
                    if sel is None:
                        from tour_selector import TourSelector as _TS
                        sel = _TS()
                    msgs = sel.find_tours(
                        departure_city=from_city,
                        people=room_adults + room_children,
                        adults=room_adults,
                        children=room_children,
                        departure_date=departure_date,
                        nights=nights,
                        is_winter=is_winter,
                        destination=destination,
                        date_is_approximate=date_approximate,
                    )
                    if isinstance(msgs, list) and msgs:
                        header_msg = f"{'='*40}\n{room_label}\n{'='*40}"
                        all_msgs.append(header_msg)
                        all_msgs.extend(msgs)
                    elif isinstance(msgs, str):
                        all_msgs.append(f"{room_label}\n{msgs}")
                except Exception as e:
                    logger.error(f"[{user_id}] Ошибка поиска {room_label}: {e}", exc_info=True)

            if all_msgs:
                intro = (
                    f"👨‍👩‍👧‍👦 Вы едёте {adults} взр. + {children} дет. — подбираем 2 отдельных номера:\n"
                    f"• Номер 1: 1 взр. + 1 реб.\n"
                    f"• Номер 2: 1 взр. + 2 реб.\n"
                    f"Цены указаны за каждый номер отдельно 👇"
                )
                return [intro] + all_msgs
            return ["К сожалению, туры для указанных дат не найдены. Попробуйте другую дату или направление."]

        else:
            # Обычный поиск: все в одном номере, adults и children передаём раздельно в API
            people = adults + children
            ppl_str = f"{adults} взр." + (f" + {children} реб." if children else "")
            logger.info(
                f"[{user_id}] 🔍 ПОИСК: {from_city}, {ppl_str} (итого {people} чел.), "
                f"{'ПРИБЛИЗИТЕЛЬНАЯ~' if date_approximate else 'используем точную дату'}{departure_date.strftime('%d.%m.%Y')}, "
                f"{'зима' if is_winter else 'лето'}, dest={destination}"
            )
            logger.info(f"[{user_id}] date_approximate={date_approximate} → {'ищем лучшую дату за месяц' if date_approximate else 'используем точную дату'}")
            logger.info(f"[{user_id}] Начинаем подбор туров через TourSelector...")

            try:
                sel = _tour_selector
                if sel is None:
                    from tour_selector import TourSelector as _TS
                    sel = _TS()
                msgs = sel.find_tours(
                    departure_city=from_city,
                    people=people,
                    adults=adults,
                    children=children,
                    departure_date=departure_date,
                    nights=nights,
                    is_winter=is_winter,
                    destination=destination,
                    date_is_approximate=date_approximate,
                )
                logger.info(f"[{user_id}] TourSelector вернул {len(msgs) if isinstance(msgs, list) else 1} сообщений")
                if isinstance(msgs, list) and msgs:
                    return msgs
                if isinstance(msgs, str):
                    return [msgs]
                return ["К сожалению, туры для указанных дат не найдены. Попробуйте другую дату или направление."]
            except Exception as e:
                logger.error(f"[{user_id}] Ошибка при поиске туров: {e}", exc_info=True)
                return ["Произошла ошибка при поиске туров, попробуйте, пожалуйста, позже."]

    def generate_tour_options(self, from_city: str, adults: int, children: int,
                             is_winter: bool, destination: str = None,
                             nights: int = 7, budget: int = None,
                             departure_date: datetime = None,
                             date_approximate: bool = False) -> list:
        """Генерация вариантов туров через tour_selector (единый модуль)"""

        people = adults + children

        if not departure_date:
            departure_date = datetime.now() + timedelta(days=21)
            date_approximate = True

        logger.info(
            f"[BOT→SELECTOR] {from_city}, {people} чел., {departure_date.strftime('%d.%m.%Y')}, "
            f"{'зима' if is_winter else 'лето'}, dest={destination}"
        )

        # =====================================================================
        # ЗИМА → ЕГИПЕТ (рейсы ищутся внутри _generate_egypt_from_api)
        # =====================================================================
        if is_winter:
            return self._generate_egypt_from_api(
                people_text, people, 0, 0, from_city,
                nights, budget, flight_date_str, tv_date
            )
        # =====================================================================
        # ЛЕТО → ТУРЦИЯ и/или ИСПАНИЯ
        # =====================================================================
        else:
            search_date_api = departure_date.strftime("%Y-%m-%d")
            country_search = "turkey" if destination in ["antalya", "turkey"] else "spain"

            # Поиск рейсов для лета
            flight_eur = 0
            if flightpowers:
                try:
                    if country_search == "turkey":
                        flights = flightpowers.search_turkey_flights(from_city, search_date_api, nights, people)
                    else:
                        flights = flightpowers.search_spain_flights(from_city, search_date_api, nights, people)
                    if flights:
                        flight_eur = int(flights[0].get('price_per_person_eur', 0))
                        logger.info(f"[FLIGHT] FlightPowers {country_search}: {flight_eur} EUR/чел ✅")
                except Exception as e:
                    logger.error(f"[FLIGHT] FlightPowers ❌: {e}")

            if not flight_eur:
                fallback = {
                    "Copenhagen": {"turkey": 200, "spain": 160},
                    "Billund":    {"turkey": 185, "spain": 150},
                    "Aalborg":    {"turkey": 185, "spain": 150},
                    "Aarhus":     {"turkey": 185, "spain": 150},
                    "Oslo":       {"turkey": 215, "spain": 175},
                    "Stockholm":  {"turkey": 225, "spain": 185},
                    "Goteborg":   {"turkey": 215, "spain": 175},
                    "Helsinki":   {"turkey": 235, "spain": 195},
                    "Tallinn":    {"turkey": 245, "spain": 205},
                }
                flight_eur = fallback.get(from_city, fallback["Copenhagen"]).get(country_search, 200)
                logger.info(f"[FLIGHT] Fallback {country_search}: {flight_eur} EUR/чел")

            flight_total = flight_eur * people

            if destination in ["antalya", "turkey"]:
                return self._generate_country_from_api(
                    "turkey", people_text, people, flight_eur, flight_total,
                    from_city, nights, budget, flight_date_str, tv_date
                )
            elif destination in ["barcelona", "mallorca", "spain"]:
                return self._generate_country_from_api(
                    "spain", people_text, people, flight_eur, flight_total,
                    from_city, nights, budget, flight_date_str, tv_date
                )
            else:
                return self._generate_summer_both_from_api(
                    people_text, people, flight_eur, flight_total,
                    from_city, nights, budget, flight_date_str, tv_date
                )

    @staticmethod
    def _split_message(text: str, max_len: int = 1900) -> List[str]:
        """Разбивает длинное сообщение на части по max_len символов, разбивая по строкам"""
        if len(text) <= max_len:
            return [text]
        parts = []
        current = ""
        for line in text.split('\n'):
            addition = (line + '\n')
            if len(current) + len(addition) > max_len:
                if current:
                    parts.append(current.rstrip('\n'))
                current = addition
            else:
                current += addition
        if current.strip():
            parts.append(current.rstrip('\n'))
        return parts if parts else [text[:max_len]]

    def _generate_egypt_from_api(self, people_text, people, flight_eur, flight_total,
                                  from_city, nights, budget, flight_date_str, tv_date) -> List[str]:
        """Египет: Шарм + Хургада — только обязательные отели, каждый отдельным сообщением"""
        transfer = TRANSFER_PRICES["sharm"]  # 420 EUR

        logger.info(f"[EGYPT] ═══════════════════════════════════════════════")
        logger.info(f"[EGYPT] 🔍 Поиск рейсов FlightPowers (oneway): {from_city} → Египет")

        # ── 1. Конвертируем дату ──────────────────────────────────────────────
        try:
            dep_dt = datetime.strptime(tv_date, "%d.%m.%Y")
            dep_iso = dep_dt.strftime("%Y-%m-%d")
        except Exception:
            dep_iso = datetime.now().strftime("%Y-%m-%d")

        # ── 2. Рейсы через tour_selector.search_flight (FP + TravelMarket) ───
        try:
            from tour_selector import search_flight as _search_flight
            flight_result = _search_flight(from_city, "egypt", dep_dt, nights, people)
            flight_sharm_eur    = flight_result.get("price_sharm",    flight_result["price_per_person"])
            flight_hurghada_eur = flight_result.get("price_hurghada", flight_result["price_per_person"])
            logger.info(f"[EGYPT] ✅ Рейс SSH={flight_sharm_eur} EUR/чел | HRG={flight_hurghada_eur} EUR/чел | источник: {flight_result.get('source')}")
        except Exception as e:
            logger.error(f"[EGYPT] search_flight ошибка: {e}")
            FALLBACK_FLIGHT = {
                "Copenhagen": 175, "Billund": 160, "Aalborg": 160,
                "Aarhus": 160, "Oslo": 190, "Stockholm": 200,
                "Goteborg": 190, "Helsinki": 210, "Tallinn": 220,
            }
            flight_sharm_eur    = FALLBACK_FLIGHT.get(from_city, 175)
            flight_hurghada_eur = flight_sharm_eur

        logger.info(f"[EGYPT] Рейс: Шарм={flight_sharm_eur} EUR/чел, Хургада={flight_hurghada_eur} EUR/чел")

        # ── 3. Отели Шарм-Эль-Шейх ───────────────────────────────────────────
        logger.info(f"[EGYPT] Поиск отелей Шарм: {tv_date}, {nights} ночей, {people} чел.")
        sharm_all = _tv_search_hotels(
            country_id="1", region_id=TV_REGIONS["sharm"],
            date_from=tv_date, nights=nights, adults=people,
            operators=TV_OPERATORS_EGYPT, destination_key="sharm"
        )
        # Фильтруем только обязательные отели Шарм
        sharm_hotels = [h for h in sharm_all if _is_required_egypt_hotel(h['name'], EGYPT_REQUIRED_SHARM)]
        logger.info(f"[EGYPT] Шарм: всего найдено API={len(sharm_all)}, в нашем списке={len(sharm_hotels)}")
        for h in sharm_all:
            in_list = _is_required_egypt_hotel(h['name'], EGYPT_REQUIRED_SHARM)
            logger.info(f"[EGYPT] Шарм {'✅' if in_list else '❌'} {h['name']}: {h.get('price_eur',0)} EUR/ном | {h.get('meal','')}")

        # ── 4. Отели Хургады + Эль Гуна ───────────────────────────────────────
        hurghada_all = _tv_search_hotels(
            country_id="1", region_id=TV_REGIONS["hurghada"],
            date_from=tv_date, nights=nights, adults=people,
            operators=TV_OPERATORS_EGYPT, destination_key="hurghada"
        )
        # Фильтруем только обязательные отели Хургады
        hurghada_hotels = [h for h in hurghada_all if _is_required_egypt_hotel(h['name'], EGYPT_REQUIRED_HURGHADA)]
        logger.info(f"[EGYPT] Хургада: всего {len(hurghada_all)}, обязательных: {len(hurghada_hotels)}")

        flight_sharm_total    = flight_sharm_eur * people
        flight_hurghada_total = flight_hurghada_eur * people

        # ── 5. Формируем сообщения компактно — ВСЕ отели из списка ──────────
        header = (
            f"🏖 ЕГИПЕТ — {nights} ночей ({people_text})\n"
            f"✈️ Вылет из {from_city} ~ {flight_date_str}\n"
        )

        hurghada_block = ""
        if hurghada_hotels:
            hurghada_block = "\n━━━ 🌊 ХУРГАДА + ЭЛЬ ГУНА ━━━\n\n"
            for i, h in enumerate(hurghada_hotels, 1):
                hotel_price = h['price_eur']
                total = flight_hurghada_total + hotel_price + transfer
                flydate = h.get('flydate', flight_date_str)
                logger.info(f"[EGYPT] Хургада {i}. {h['name']}: рейс {flight_hurghada_total} + отель {hotel_price} + трансфер {transfer} = {total} EUR")
                hurghada_block += (
                    f"{i}. {h['name']} {h['stars']}⭐\n"
                    f"   🍽 {h['meal']} | 📅 {flydate} | 💰 {total} EUR\n\n"
                )

        sharm_block = ""
        if sharm_hotels:
            sharm_block = "\n━━━ 🏖 ШАРМ-ЭЛЬ-ШЕЙХ ━━━\n\n"
            offset = len(hurghada_hotels) if hurghada_hotels else 0
            for i, h in enumerate(sharm_hotels, offset + 1):
                hotel_price = h['price_eur']
                total = flight_sharm_total + hotel_price + transfer
                flydate = h.get('flydate', flight_date_str)
                logger.info(f"[EGYPT] Шарм {i}. {h['name']}: рейс {flight_sharm_total} + отель {hotel_price} + трансфер {transfer} = {total} EUR")
                sharm_block += (
                    f"{i}. {h['name']} {h['stars']}⭐\n"
                    f"   🍽 {h['meal']} | 📅 {flydate} | 💰 {total} EUR\n\n"
                )

        count_sent = len(hurghada_hotels) + len(sharm_hotels)
        logger.info(f"[EGYPT] ✅ Итого отелей: {count_sent}")

        if count_sent == 0:
            return [
                header + "\n❌ Отели из нашего списка не найдены для указанных дат.\n"
                "Попробуйте другую дату (обычно туры появляются за 2-4 месяца до вылета)."
            ]

        full_text = header + hurghada_block + sharm_block
        return self._split_message(full_text)

    def _generate_country_from_api(self, dest_key: str, people_text, people,
                                    flight_eur, flight_total, from_city,
                                    nights, budget, flight_date_str, tv_date) -> List[str]:
        """Турция или Испания — реальные цены из TourVisor + FlightPowers"""
        transfer = TRANSFER_PRICES.get(dest_key, 490)

        country_names = {"turkey": "ТУРЦИЯ 🇹🇷", "spain": "ИСПАНИЯ 🇪🇸"}
        country_ids = {"turkey": "4", "spain": "14"}

        country_id = country_ids[dest_key]
        operators = TV_OPERATORS_TURKEY if dest_key == "turkey" else TV_OPERATORS_SPAIN

        # ── 1. Реальная цена рейса через FlightPowers ──────────────────────
        real_flight_eur = flight_eur  # fallback

        if flightpowers:
            try:
                if dest_key == "turkey":
                    fp_flights = flightpowers.search_turkey_flights(from_city, dep_iso, nights, people)
                else:
                    fp_flights = flightpowers.search_spain_flights(from_city, dep_iso, nights, people)
                if fp_flights:
                    fp_price = int(fp_flights[0].get('price_per_person_eur', 0))
                    if fp_price > 0:
                        real_flight_eur = fp_price
                        logger.info(f"[{dest_key.upper()}] FlightPowers: {real_flight_eur} EUR/чел ✅")
            except Exception as e:
                logger.error(f"[{dest_key.upper()}] FlightPowers ошибка: {e}")

        real_flight_total = real_flight_eur * people

        logger.info(f"[{dest_key.upper()}] ═══════════════════════════════════════════════")
        logger.info(f"[{dest_key.upper()}] 🔍 Запрос отелей в TourVisor...")

        hotels = _tv_search_hotels(
            country_id=country_id, region_id=None,
            date_from=tv_date, nights=nights, adults=people,
            operators=operators, destination_key=dest_key
        )
        logger.info(f"[{dest_key.upper()}] Найдено {len(hotels)} отелей")

        result = (
            f"🏖 {country_names[dest_key]} - {nights} ночей ({people_text})\n"
            f"✈️ Вылет из {from_city} ~ {flight_date_str}\n"
            f"(перелёт {real_flight_eur} EUR/чел + отель + трансфер {transfer} EUR)\n\n"
        )

        # Показываем больше отелей по Турции
        TOP_N = 20 if dest_key == "turkey" else 10
        for i, h in enumerate(hotels[:TOP_N], 1):
            total = real_flight_total + h['price_eur'] + transfer
            logger.info(f"[{dest_key.upper()}] {i}. {h['name']}: рейс {real_flight_total} + отель {h['price_eur']} + трансфер {transfer} = {total} EUR")
            result += f"{i}. {h['name']} {h['stars']}⭐\n"
            result += f"📍 {h['region']} | 💰 {total} EUR | 🍽 {h['meal']}\n\n"

        logger.info(f"[{dest_key.upper()}] ✅ Готово, {min(len(hotels), TOP_N)} отелей отправлено!")

        if not hotels:
            result += "_Отели не найдены для указанных дат._\n"

        return self._split_message(result)

    def _generate_summer_both_from_api(self, people_text, people, flight_eur, flight_total,
                                        from_city, nights, budget, flight_date_str, tv_date) -> List[str]:
        """Лето: Турция + Испания — реальные цены из TourVisor + FlightPowers"""
        logger.info(f"[SUMMER] ═══════════════════════════════════════════════")
        logger.info(f"[SUMMER] 🔍 Запрос отелей Турция + Испания...")

        # ── 1. Рейсы через FlightPowers ────────────────────────────────────
        try:
            dep_dt = datetime.strptime(tv_date, "%d.%m.%Y")
            dep_iso = dep_dt.strftime("%Y-%m-%d")
        except Exception:
            dep_iso = datetime.now().strftime("%Y-%m-%d")

        turkey_flight_eur = flight_eur   # fallback
        spain_flight_eur  = flight_eur   # fallback

        if flightpowers:
            try:
                fp_turkey = flightpowers.search_turkey_flights(from_city, dep_iso, nights, people)
                if fp_turkey:
                    p = int(fp_turkey[0].get('price_per_person_eur', 0))
                    if p > 0:
                        turkey_flight_eur = p
                        logger.info(f"[SUMMER] FlightPowers Турция: {turkey_flight_eur} EUR/чел ✅")
            except Exception as e:
                logger.error(f"[SUMMER] FlightPowers Турция ошибка: {e}")

            try:
                fp_spain = flightpowers.search_spain_flights(from_city, dep_iso, nights, people)
                if fp_spain:
                    p = int(fp_spain[0].get('price_per_person_eur', 0))
                    if p > 0:
                        spain_flight_eur = p
                        logger.info(f"[SUMMER] FlightPowers Испания: {spain_flight_eur} EUR/чел ✅")
            except Exception as e:
                logger.error(f"[SUMMER] FlightPowers Испания ошибка: {e}")

        turkey_flight_total = turkey_flight_eur * people
        spain_flight_total  = spain_flight_eur * people

        # ── 2. Отели ───────────────────────────────────────────────────────
        turkey_hotels = _tv_search_hotels(
            country_id="4", region_id=None,
            date_from=tv_date, nights=nights, adults=people,
            operators=TV_OPERATORS_TURKEY, destination_key="turkey"
        )
        spain_hotels = _tv_search_hotels(
            country_id="14", region_id=None,
            date_from=tv_date, nights=nights, adults=people,
            operators=TV_OPERATORS_SPAIN, destination_key="spain"
        )

        turkey_transfer = TRANSFER_PRICES["turkey"]
        spain_transfer  = TRANSFER_PRICES["spain"]

        # Показываем больше отелей: Турция - 20, Испания - 10
        TURKEY_TOP_N = 20
        SPAIN_TOP_N = 10

        header = (
            f"🌞 ЛЕТНИЕ НАПРАВЛЕНИЯ ({people_text}, {nights} ночей)\n"
            f"✈️ Вылет из {from_city} ~ {flight_date_str}\n"
            f"(цена включает перелёт + отель + трансфер)\n\n"
        )

        turkey_block  = f"━━━ 🇹🇷 ТУРЦИЯ ━━━\n\n"
        for i, h in enumerate(turkey_hotels[:TURKEY_TOP_N], 1):
            total = turkey_flight_total + h['price_eur'] + turkey_transfer
            logger.info(f"[TURKEY] {i}. {h['name']}: рейс {turkey_flight_total} + отель {h['price_eur']} + трансфер {turkey_transfer} = {total} EUR")
            turkey_block += f"{i}. {h['name']} {h['stars']}⭐\n"
            turkey_block += f"📍 {h['region']} | 💰 {total} EUR | 🍽 {h['meal']}\n\n"

        offset = len(turkey_hotels[:TURKEY_TOP_N])
        spain_block  = f"\n━━━ 🇪🇸 ИСПАНИЯ ━━━\n\n"
        for i, h in enumerate(spain_hotels[:SPAIN_TOP_N], offset + 1):
            total = spain_flight_total + h['price_eur'] + spain_transfer
            logger.info(f"[SPAIN] {i}. {h['name']}: рейс {spain_flight_total} + отель {h['price_eur']} + трансфер {spain_transfer} = {total} EUR")
            spain_block += f"{i}. {h['name']} {h['stars']}⭐\n"
            spain_block += f"📍 {h['region']} | 💰 {total} EUR | 🍽 {h['meal']}\n\n"

        total_count = len(turkey_hotels[:TURKEY_TOP_N]) + len(spain_hotels[:SPAIN_TOP_N])
        logger.info(f"[SUMMER] ✅ Готово! Турция: {len(turkey_hotels[:TURKEY_TOP_N])}, Испания: {len(spain_hotels[:SPAIN_TOP_N])}, итого: {total_count}")

        if total_count == 0:
            return [header + "_Отели не найдены для указанных дат._"]

        full_text = header + turkey_block + spain_block
        return self._split_message(full_text)


# Глобальный бот — КОНЕЦ КЛАССА TravelBot
# Глобальный бот
bot = TravelBot()


async def handle_message(sender_id: str, message_text: str) -> List[str]:
    """Обработчик сообщений"""
    logger.info(f"[FB:{sender_id}] IN: {message_text}")
    responses = await bot.process_message(sender_id, message_text)
    for r in responses:
        logger.info(f"[FB:{sender_id}] OUT: {r[:50]}...")
    return responses


# ============================================================================
# FLASK СЕРВЕР ДЛЯ FACEBOOK WEBHOOK
# ============================================================================

import requests
from flask import Flask, request as flask_request

app = Flask(__name__)

VERIFY_TOKEN = 'travel_with_alina_bot'


def send_facebook_message(recipient_id: str, text: str) -> bool:
    """Отправка сообщения в Facebook, разбиваем текст на части, чтобы не превышать лимит API"""
    if not FACEBOOK_PAGE_ACCESS_TOKEN:
        logger.error("FACEBOOK_PAGE_ACCESS_TOKEN not set!")
        return False

    logger.info(f"[{recipient_id}] Отправляем сообщение (длина: {len(text)} символов)")

    # Используем метод класса для разбиения на части и безопасной отправки
    parts = bot._split_message(text, max_len=1900)
    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={FACEBOOK_PAGE_ACCESS_TOKEN}"
    all_ok = True
    for i, part in enumerate(parts, 1):
        payload = {
            "recipient": {"id": recipient_id},
            "message": {"text": part},
            "messaging_type": "RESPONSE"
        }
        try:
            logger.info(f"[{recipient_id}] Отправка части {i}/{len(parts)}: {len(part)} символов")
            r = requests.post(url, json=payload, timeout=30)
            if r.status_code == 200:
                logger.info(f"[{recipient_id}] Message part sent OK ({len(part)} chars)")
            else:
                logger.error(f"[{recipient_id}] Send error: {r.status_code} - {r.text}")
                logger.error(f"[{recipient_id}] Payload: {json.dumps(payload, ensure_ascii=False)}")
                all_ok = False
        except Exception as e:
            logger.error(f"[{recipient_id}] Error sending message: {e}")
            all_ok = False
    return all_ok


def send_facebook_message_with_button(recipient_id: str, text: str, button_text: str = "Начать", button_payload: str = "START_TOUR_SELECTION") -> bool:
    """Отправка сообщения с кнопкой Quick Reply в Facebook"""
    if not FACEBOOK_PAGE_ACCESS_TOKEN:
        logger.error("FACEBOOK_PAGE_ACCESS_TOKEN not set!")
        return False

    logger.info(f"[{recipient_id}] Отправляем сообщение с кнопкой '{button_text}'")

    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={FACEBOOK_PAGE_ACCESS_TOKEN}"
    
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "text": text,
            "quick_replies": [
                {
                    "content_type": "text",
                    "title": button_text,
                    "payload": button_payload
                }
            ]
        },
        "messaging_type": "RESPONSE"
    }
    
    try:
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code == 200:
            logger.info(f"[{recipient_id}] ✅ Сообщение с кнопкой отправлено")
            return True
        else:
            logger.error(f"[{recipient_id}] ❌ Send error: {r.status_code} - {r.text}")
            return False
    except Exception as e:
        logger.error(f"[{recipient_id}] ❌ Error sending message with button: {e}")
        return False


def send_message_with_cancel_button(recipient_id: str, text: str, cancel_text: str = "Отмена") -> bool:
    """Отправка сообщения с кнопкой ОТМЕНА"""
    if not FACEBOOK_PAGE_ACCESS_TOKEN:
        logger.error("FACEBOOK_PAGE_ACCESS_TOKEN not set!")
        return False

    logger.info(f"[{recipient_id}] Отправляем сообщение с кнопкой '{cancel_text}'")

    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={FACEBOOK_PAGE_ACCESS_TOKEN}"
    
    payload = {
        "recipient": {"id": recipient_id},
        "message": {
            "text": text,
            "quick_replies": [
                {
                    "content_type": "text",
                    "title": cancel_text,
                    "payload": "CANCEL_SELECTION"
                }
            ]
        },
        "messaging_type": "RESPONSE"
    }
    
    try:
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code == 200:
            logger.info(f"[{recipient_id}] ✅ Сообщение с кнопкой отмены отправлено")
            return True
        else:
            logger.error(f"[{recipient_id}] ❌ Send error: {r.status_code} - {r.text}")
            return False
    except Exception as e:
        logger.error(f"[{recipient_id}] ❌ Error sending message with cancel button: {e}")
        return False


@app.route('/', methods=['GET'])
def home():
    return "Travel With Alina Bot is running!"


@app.route('/webhook', methods=['GET'])
def verify_webhook():
    """Верификация webhook"""
    mode = flask_request.args.get('hub.mode')
    token = flask_request.args.get('hub.verify_token')
    challenge = flask_request.args.get('hub.challenge')

    print(f"\n{'='*50}")
    print(f"VERIFY WEBHOOK REQUEST!")
    print(f"Mode: {mode}")
    print(f"Token: {token}")
    print(f"Challenge: {challenge}")
    print(f"{'='*50}\n")

    logger.info(f"Webhook verify: mode={mode}, token={token}")

    if mode == 'subscribe' and token == VERIFY_TOKEN:
        logger.info("Webhook verified OK!")
        print(">>> VERIFIED OK!")
        return challenge, 200

    print(">>> VERIFICATION FAILED!")
    return 'Forbidden', 403


@app.route('/webhook', methods=['POST'])
def handle_webhook():
    """Обработка сообщений от Facebook"""
    import json
    import time
    import threading

    # Кэш обработанных сообщений (для дедупликации)
    if not hasattr(handle_webhook, 'processed_messages'):
        handle_webhook.processed_messages = {}
    if not hasattr(handle_webhook, 'last_cleanup'):
        handle_webhook.last_cleanup = time.time()
    # Блокировки по user_id — не позволяем двум потокам одновременно обрабатывать 1 юзера
    if not hasattr(handle_webhook, 'user_locks'):
        handle_webhook.user_locks = {}
    if not hasattr(handle_webhook, 'user_locks_mutex'):
        handle_webhook.user_locks_mutex = threading.Lock()

    # Чистим старые записи каждые 10 минут
    current_time = time.time()
    if current_time - handle_webhook.last_cleanup > 600:
        # Удаляем записи старше 15 минут
        cutoff = current_time - 900
        handle_webhook.processed_messages = {
            k: v for k, v in handle_webhook.processed_messages.items()
            if v > cutoff
        }
        handle_webhook.last_cleanup = current_time

    # Получаем данные (логируем минимально)
    try:
        data = flask_request.get_json()
    except Exception as e:
        logger.error(f"JSON parse error: {e}")
        return 'OK', 200

    if data.get('object') == 'page':
        for entry in data.get('entry', []):
            for event in entry.get('messaging', []):
                sender_id = event.get('sender', {}).get('id')

                if 'message' in event:
                    msg = event['message']
                    msg_id = msg.get('mid', '')
                    msg_timestamp = event.get('timestamp', 0)

                    # Создаём уникальный ключ: только msg_id (он глобально уникален)
                    cache_key = msg_id if msg_id else f"{sender_id}_{msg_timestamp}"

                    # Проверяем дубликат
                    if cache_key in handle_webhook.processed_messages:
                        continue

                    # 🔥 ФИЛЬТРАЦИЯ СТАРЫХ СООБЩЕНИЙ
                    if msg_timestamp > 0:
                        # 1. Игнорируем сообщения которые были ДО запуска бота
                        if hasattr(handle_webhook, 'bot_start_time'):
                            bot_start_ms = int(handle_webhook.bot_start_time * 1000)
                            if msg_timestamp < bot_start_ms:
                                logger.info(f"[{sender_id}] ⏰ Игнорируем сообщение ДО запуска бота")
                                continue
                        
                        # 2. Игнорируем сообщения старше 5 минут
                        current_ms = int(time.time() * 1000)
                        msg_age_sec = (current_ms - msg_timestamp) / 1000
                        if msg_age_sec > 300:
                            logger.info(f"[{sender_id}] ⏰ Игнорируем старое сообщение ({msg_age_sec:.0f}с)")
                            continue

                    # Добавляем в кэш обработанных СРАЗУ (до старта потока)
                    handle_webhook.processed_messages[cache_key] = current_time

                    if msg.get('is_echo'):
                        continue

                    text = msg.get('text', '')
                    logger.info(f"[{sender_id}] 📩 {text[:80]}")  # Короткий лог

                    if text and sender_id:
                        # Получаем/создаём блокировку для этого user_id
                        with handle_webhook.user_locks_mutex:
                            if sender_id not in handle_webhook.user_locks:
                                handle_webhook.user_locks[sender_id] = threading.Lock()
                            user_lock = handle_webhook.user_locks[sender_id]

                        def process_in_thread(sid, txt, lock):
                            acquired = lock.acquire(blocking=False)
                            if not acquired:
                                return
                            try:
                                loop2 = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop2)
                                responses2 = loop2.run_until_complete(
                                    bot.process_message(sid, txt)
                                )
                                loop2.close()

                                # Отправляем первую часть ответов немедленно
                                logger.info(f"[{sid}] Готово, отправляем {len(responses2)} сообщений")
                                for resp in responses2:
                                    if resp:  # Пропускаем пустые ответы
                                        logger.info(f"[{sid}] ОТПРАВКА: {resp[:80]}...")
                                        ok = send_facebook_message(sid, resp)
                                        logger.info(f"[{sid}] Результат: {'OK' if ok else 'FAIL'}")
                                        time.sleep(0.5)
                                
                                # 🔥 Проверяем нужно ли отправить сообщение с кнопкой "Начать"
                                conv_data = bot.storage.get_conversation(sid)
                                if conv_data.get('_send_with_button'):
                                    button_msg = conv_data.get('_button_message', '')
                                    button_label = conv_data.get('_button_label', 'Start')
                                    if button_msg:
                                        logger.info(f"[{sid}] 🔘 Отправляем сообщение с кнопкой '{button_label}'")
                                        send_facebook_message_with_button(sid, button_msg, button_text=button_label, button_payload="START_TOUR_SELECTION")
                                        conv_data['_send_with_button'] = False
                                        time.sleep(0.5)
                                
                                # 🔥 Проверяем нужно ли отправить сообщение с кнопкой "Отмена"
                                if conv_data.get('_send_with_cancel_button'):
                                    cancel_msg = conv_data.get('_cancel_button_message', '')
                                    cancel_label = conv_data.get('_cancel_button_label', 'Cancel')
                                    if cancel_msg:
                                        logger.info(f"[{sid}] ❌ Отправляем сообщение с кнопкой '{cancel_label}'")
                                        send_message_with_cancel_button(sid, cancel_msg, cancel_text=cancel_label)
                                        conv_data['_send_with_cancel_button'] = False
                                        time.sleep(0.5)

                                # Проверяем нужно ли запустить поиск туров
                                if conv_data.pop('_run_search', False):
                                    logger.info(f"[{sid}] 🔍 Запускаем поиск туров...")
                                    tour_msgs = bot._do_search(sid, conv_data)
                                    bot.storage.set_state(sid, ConversationState.SENT_OPTIONS)
                                    # Отправляем результаты поиска
                                    for tmsg in tour_msgs:
                                        logger.info(f"[{sid}] ОТПРАВКА (тур): {tmsg[:80]}...")
                                        send_facebook_message(sid, tmsg)
                                        time.sleep(0.5)
                                    
                                    # ✨ НОВОЕ: Добавляем предложение альтернативных направлений
                                    current_destination = conv_data.get("destination")
                                    if current_destination:
                                        alternatives_text = bot._get_alternative_destinations(sid, current_destination)
                                        logger.info(f"[{sid}] 💡 Отправляем альтернативные направления")
                                        send_facebook_message(sid, alternatives_text)
                                        time.sleep(0.5)
                                    
                                    # Сообщение о том, что входит в стоимость
                                    inclusion_msg = (
                                        "✅ В стоимость тура входит:\n\n"
                                        "• Авиаперелёт туда и обратно ✈️\n"
                                        "• Трансфер аэропорт-отель-аэропорт 🚌\n"
                                        "• Проживание в отеле 🏨\n"
                                        "• Питание (указано для каждого отеля) 🍽\n"
                                        "• Медицинская страховка 🏥\n\n"
                                        "💙 В наличии также более 100 отелей по ценам выше и ниже! "
                                        "Эти отели я отправляю по отзывам туристов - всем очень нравятся 😊"
                                    )
                                    send_facebook_message(sid, inclusion_msg)
                                    time.sleep(0.5)
                                    
                                    # Финальное сообщение
                                    send_facebook_message(sid, SCRIPTS["after_selection"])
                                    logger.info(f"[{sid}] ✅ Поиск завершён, результаты отправлены")

                                    # ── Follow-up через 4 часа (в дневное время) ──────────
                                    def _follow_up_thread(follow_sid):
                                        """Через 4 часа проверяем — если пользователь не ответил и не забронировал, отправляем follow_up"""
                                        try:
                                            time.sleep(4 * 3600)  # ждём 4 часа
                                            # Проверяем текущий час (локальное время сервера)
                                            hour_now = datetime.now().hour
                                            if hour_now < 8 or hour_now >= 22:
                                                # Ночью не пишем — ждём до 10 утра
                                                delta = 10 - hour_now if hour_now < 10 else (24 - hour_now + 10)
                                                time.sleep(delta * 3600)
                                            # Проверяем что пользователь всё ещё в SENT_OPTIONS (не ответил)
                                            fu_conv = bot.storage.get_conversation(follow_sid)
                                            fu_state = fu_conv.get("state")
                                            if fu_state == ConversationState.SENT_OPTIONS:
                                                logger.info(f"[{follow_sid}] 📲 Follow-up отправляем...")
                                                send_facebook_message(follow_sid, SCRIPTS["follow_up"])
                                            else:
                                                logger.info(f"[{follow_sid}] Follow-up пропуск — состояние {fu_state}")
                                        except Exception as fe:
                                            logger.error(f"[{follow_sid}] Follow-up ошибка: {fe}")

                                    ft = threading.Thread(target=_follow_up_thread, args=(sid,), daemon=True)
                                    ft.start()

                            except Exception as ex:
                                logger.error(f"[{sid}] Thread error: {ex}", exc_info=True)
                            finally:
                                lock.release()
                        t = threading.Thread(target=process_in_thread, args=(sender_id, text, user_lock), daemon=True)
                        t.start()
                        logger.info(f"[{sender_id}] Запущен поток обработки")

    return 'OK', 200


@app.route('/status', methods=['GET'])
def status():
    return {
        "status": "running",
        "token_set": bool(FACEBOOK_PAGE_ACCESS_TOKEN),
        "verify_token": VERIFY_TOKEN
    }


# ============================================================================
# ЗАПУСК
# ============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        # Тестовый режим
        async def test():
            print("=" * 70)
            print("TEST Facebook Bot - Travel With Alina")
            print("=" * 70)

            # ТЕСТ 1: Стандартный сценарий
            print("\n>>> ТЕСТ 1: Стандартный сценарий")
            user = "test_" + datetime.now().strftime("%H%M%S")
            messages = [
                "Привет!",
                "Из Копенгагена, 2 взрослых и 1 ребенок 9 лет",
                "Хотим в Египет в феврале",
            ]
            for msg in messages:
                print(f"\nUser: {msg}")
                responses = await handle_message(user, msg)
                for r in responses:
                    # Безопасный вывод эмодзи
                    safe_r = r.encode('ascii', 'replace').decode('ascii')[:100]
                    print(f"Bot: {safe_r}...")

            # ТЕСТ 2: Мальме отдельно
            print("\n" + "=" * 70)
            print(">>> ТЕСТ 2: Мальме по шагам")
            user2 = "test_malmo_" + datetime.now().strftime("%H%M%S")
            messages2 = [
                "начать",
                "мальме",  # Только город
                "2 взрослых в марте",  # Люди и дата
            ]
            for msg in messages2:
                print(f"\nUser: {msg}")
                responses = await handle_message(user2, msg)
                for r in responses:
                    safe_r = r.encode('ascii', 'replace').decode('ascii')[:120]
                    print(f"Bot: {safe_r}...")

            print("\nTest done!")
        asyncio.run(test())
    else:
        # Запуск сервера
        print("=" * 50)
        print("TRAVEL WITH ALINA - Facebook Bot Server")
        print("=" * 50)
        print("Webhook URL: http://localhost:5000/webhook")
        print("Verify Token:", VERIFY_TOKEN)
        print("Page Token:", "SET" if FACEBOOK_PAGE_ACCESS_TOKEN else "NOT SET")
        print("=" * 50)
        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
