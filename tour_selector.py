#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
tour_selector.py — единый модуль подбора туров для Travel With Alina

Логика:
  1. Получаем параметры от пользователя (город, люди, дата, сезон)
  2. Ищем рейс: FlightPowers (Google Flights) + TravelMarket — берём дешевле
  3. Ищем отели: TourVisor API (без перелёта)
  4. Считаем итог: рейс + отель + трансфер
  5. Возвращаем готовый список сообщений для Facebook

Использование:
    from tour_selector import TourSelector
    selector = TourSelector()
    messages = selector.find_tours(
        departure_city="Copenhagen",
        people=2,
        departure_date=datetime(2026, 3, 15),
        nights=7,
        is_winter=True,   # до мая — Египет, лето — Турция+Испания
    )
    for msg in messages:
        send_to_facebook(msg)
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# TOURVISOR API
# ─────────────────────────────────────────────────────────────────────────────
TV_LOGIN    = os.getenv("TOURVISOR_LOGIN",    "travelwithalina.online@gmail.com")
TV_PASSWORD = os.getenv("TOURVISOR_PASSWORD", "ntr65XZVFyzc")
TV_BASE     = "http://tourvisor.ru/xml"

# ─────────────────────────────────────────────────────────────────────────────
# TOURVISOR REST API (новый JWT-токен, v1) — используется для Турции
# ─────────────────────────────────────────────────────────────────────────────
TV_REST_TOKEN = os.getenv(
    "TOURVISOR_JWT",
    "eyJhbGciOiJFZERTQSIsImtpZCI6IjIwMzNkMzE0LTdlNTktNDAyYS05ZDBmLWZlMjMzYzhiODFkOCIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJ0di1hcGktYXV0aCIsInN1YiI6IjEyNzk0MSIsImF1ZCI6WyJzZWFyY2giXSwiZXhwIjoxNzkxNDg2MDAwLCJuYmYiOjE3NzI0MzgwNjgsImlhdCI6MTc3MjQzODA2OCwianRpIjoiMDE5Y2FkOGEtOWM1MS03YmZlLTgwMDgtYzI4NTQ0YTYxZGE3In0."
    "QCgL1fkEsrAAwHVbCrQbQ2Vs6Oo-7-ctDCirxUbvLi64bmby5Fuf-YODPPz47xQs1BuquS3bz8xekVR2dUnXCA"
)
TV_REST_BASE    = "https://api.tourvisor.ru/search/api/v1"
TV_REST_HEADERS = {
    "Authorization": f"Bearer {TV_REST_TOKEN}",
    "Accept": "application/json",
}

# Курорты Анталийского побережья для нового REST API
TV_REST_TURKEY_REGIONS = [19, 20, 21, 22, 23]  # Аланья, Анталья, Белек, Кемер, Сиде

# Операторы Турции (только эти): Anex=13, Coral=11, Join UP!=125, Kompas=90
TV_REST_TURKEY_OPS = [13, 11, 125, 90]

# Ключевые слова операторов для клиентской фильтрации (Russian Express убран)
_TV_REST_OP_KEYWORDS = ["anex", "анекс", "coral", "корал", "join up", "join up!", "джойн", "kompas", "компас"]

# Коды стран TourVisor
TV_COUNTRY = {"egypt": "1", "turkey": "4", "spain": "14"}

# Коды регионов TourVisor
TV_REGION = {
    "sharm":     "6",    # Шарм-Эль-Шейх
    "hurghada":  "5",    # Хургада + Эль Гуна
    "antalya":   "4",    # Анталья (страна Турция)
    "alanya":    "11",   # Аланья
    "belek":     "3",    # Белек
    "kemer":     "5",    # Кемер (Турция)
    "side":      "13",   # Сиде
    "tenerife":  "101",  # Тенерифе (Испания) — проверено через /xml/list.php?type=region
}

# Туроператоры (проверено debug_operators.py)
TV_OPS_EGYPT    = "125,13,11,90,23"   # Join UP!=125, Anex=13, Coral=11, Kompas=90, Russian Express=23
TV_OPS_TURKEY   = "125,13,11,90,23"
TV_OPS_SPAIN    = "125,13,11,90,23"
TV_OPS_TENERIFE = ""      # Тенерифе: все операторы (параметр `operator` НЕ передаём)

# Трансфер на двоих (EUR)
TRANSFER = {
    "egypt":    420,
    "sharm":    420,
    "hurghada": 420,
    "turkey":   490,
    "antalya":  490,
    "spain":    600,
    "mallorca": 530,
    "tenerife": 530,
}

# ─────────────────────────────────────────────────────────────────────────────
# КОДЫ АЭРОПОРТОВ
# ─────────────────────────────────────────────────────────────────────────────
AIRPORT_CODES = {
    "Copenhagen": "CPH",  "Billund": "BLL",   "Aalborg": "AAL",
    "Aarhus":     "AAR",  "Oslo":    "OSL",   "Stockholm": "ARN",
    "Goteborg":   "GOT",  "Helsinki":"HEL",   "Tallinn":   "TLL",
    "Malmo":      "CPH",  # Мальмё → Копенгаген
}

# TravelMarket: ID аэропортов и стран
TM_DEPARTURE_IDS = {
    "Billund":    "499708,503274,503278",  # Ютландия: Billund+Aarhus+Aalborg — ищем вместе
    "Aarhus":     "499708,503274,503278",  # Ютландия: все три рядом
    "Aalborg":    "499708,503274,503278",  # Ютландия: все три рядом
    "Copenhagen": "500055",    # CPH — Копенгаген
    "Malmo":      "500055",    # Мальмё → CPH
    "Oslo":       "499714",
    "Stockholm":  "499716",
    "Goteborg":   "499715",
    "Helsinki":   "499717",
    "Tallinn":    "499718",
}
TM_COUNTRY_IDS = {
    "egypt":  "500297",
    "turkey": "500320",   # Tyrkiet = Турция
    "spain":  "500311",   # Spanien = Испания
}

# Fallback цены на перелёт (EUR/чел oneway) если оба API не ответили
# Реальные примерные цены чартеров/лоукостеров в одну сторону
FLIGHT_FALLBACK = {
    "Copenhagen": {"egypt": 175, "turkey": 200, "spain": 160, "tenerife": 200, "mallorca": 165, "spain_barcelona": 155},
    "Billund":    {"egypt": 160, "turkey": 185, "spain": 150, "tenerife": 185, "mallorca": 150, "spain_barcelona": 145},
    "Aarhus":     {"egypt": 160, "turkey": 185, "spain": 150, "tenerife": 185, "mallorca": 150, "spain_barcelona": 145},
    "Aalborg":    {"egypt": 160, "turkey": 185, "spain": 150, "tenerife": 185, "mallorca": 150, "spain_barcelona": 145},
    "Oslo":       {"egypt": 190, "turkey": 215, "spain": 175, "tenerife": 220, "mallorca": 180, "spain_barcelona": 170},
    "Stockholm":  {"egypt": 200, "turkey": 225, "spain": 185, "tenerife": 230, "mallorca": 190, "spain_barcelona": 180},
    "Goteborg":   {"egypt": 190, "turkey": 215, "spain": 175, "tenerife": 220, "mallorca": 180, "spain_barcelona": 170},
    "Helsinki":   {"egypt": 210, "turkey": 235, "spain": 195, "tenerife": 240, "mallorca": 200, "spain_barcelona": 190},
    "Tallinn":    {"egypt": 220, "turkey": 245, "spain": 205, "tenerife": 250, "mallorca": 210, "spain_barcelona": 200},
    "Malmo":      {"egypt": 175, "turkey": 200, "spain": 160, "tenerife": 200, "mallorca": 165, "spain_barcelona": 155},
}

# Максимально разумная цена рейса от FlightPowers (EUR/чел roundtrip).
# Если API вернул больше — скорее всего неверные данные или стыковочный рейс.
FP_MAX_ONEWAY_EUR = {
    "egypt":   450,
    "turkey":  600,
    "spain":   500,
    "tenerife":600,
    "mallorca":500,
}

# ─────────────────────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────────────────────────

def _get_usd_eur_rate() -> float:
    """Получает актуальный курс USD→EUR из открытого API"""
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
        return r.json().get("rates", {}).get("EUR", 0.92)
    except Exception:
        return 0.92


# ─────────────────────────────────────────────────────────────────────────────
# БЕЛЫЙ СПИСОК ОТЕЛЕЙ ТУРЦИИ (Анталийское побережье)
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_TURKEY = [
    "grand kolibri prestige", "telatiye resort", "port river hotel",
    "lake & river side", "lake river side", "kahya resort", "alarcha hotels",
    "kirbiyik resort", "crystal paraiso aqua", "a good life utopia",
    "crystal admiral aqua", "crystal aura aqua", "justiniano deluxe",
    "vikingen infinity", "utopia beach club", "justiniano club park",
    "grand park lara", "kaya side", "utopia resort", "noxinn deluxe",
    "quattro beach resort", "crystal de luxe", "fame residence kemer",
    "gypsophila holiday", "megasaray westbeach", "baia salima kemer",
    "loceanica beach", "utopia world hotel", "quattro family club",
    "crystal flora pearl", "bosphorus sorgun", "crystal prestige pearl",
    "zena resort", "club hotel phaselis rose", "pirates beach club",
    "sidera kirman", "baia lara hotel", "calyptus kirman", "juju premier palace",
    "leodikya kirman", "sidemarin kirman", "aqi pegasos resort", "arycanda kirman",
    "limak limra", "fame residence lara", "rubi platinum", "kirman belazur",
    "rixos downtown antalya", "dobedan world palace", "megasaray club belek",
    "nirvana cosmopolitan", "ic hotels green palace", "akka antedon",
    "mirage park resort", "titanic deluxe golf", "nirvana mediterranean",
    "titanic deluxe lara", "rixos park belek", "kaya palazzo golf",
    "rixos premium tekirova", "rixos premium belek", "ng phaselis bay",
    "cullinan golf resort", "maxx royal kemer", "papillon ayscha",
    "papillon belvil", "papillon zeugma", "regnum carya",
    "titanic mardan palace", "venezia palace deluxe",
]

# Псевдонимы — старые/альтернативные названия → ключевое слово белого списка
TURKEY_ALIASES: dict = {
    "amara premier palace": "juju premier palace",
    "kirman leodikya":      "leodikya kirman",
    "alara park hotel":     "utopia resort",
    "crystal paraiso verde":"crystal paraiso aqua",
    "crystal admiral resort":"crystal admiral aqua",
    "crystal aura beach":   "crystal aura aqua",
    "crystal de luxe resort":"crystal de luxe",
    "crystal flora beach":  "crystal flora pearl",
    "crystal prestige elite":"crystal prestige pearl",
    "harrington park resort":"megasaray westbeach",
    "baia kemer club":      "baia salima kemer",
    "alva donna world":     "dobedan world palace",
    "majesty mirage":       "mirage park resort",
    "riu":                  "kaya palazzo golf",
    "ic hotels residence":  "ic hotels green palace",
    "kirman belazur":       "kirman belazur",
}


def _clean_name(name: str) -> str:
    """Убирает спецсимволы и лишние пробелы из названия отеля для дедупликации."""
    import re
    return re.sub(r"[^a-zа-я0-9 ]", "", name.lower()).strip()


def _is_in_list(name: str, lst: list, aliases: dict = None) -> bool:
    """
    Проверяет, есть ли название отеля в белом/чёрном списке.
    aliases — словарь {альтернативное_слово: ключевое_слово_в_lst}
    """
    n = name.lower()
    # Прямое совпадение
    for item in lst:
        if item.lower() in n:
            return True
    # Через псевдонимы
    if aliases:
        for alias, canonical in aliases.items():
            if alias.lower() in n:
                for item in lst:
                    if item.lower() in canonical.lower() or canonical.lower() in item.lower():
                        return True
    return False


def _tv_rest_safe_get(url: str, params=None, timeout: int = 60, retries: int = 3):
    """GET к новому REST API с повторными попытками при таймауте"""
    for attempt in range(retries):
        try:
            return requests.get(url, params=params, headers=TV_REST_HEADERS, timeout=timeout)
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout):
            if attempt < retries - 1:
                logger.warning(f"[TV_REST] Таймаут, повтор {attempt+2}/{retries}...")
                time.sleep(5)
            else:
                raise
        except Exception as e:
            logger.error(f"[TV_REST] Ошибка: {e}")
            raise


def _tv_rest_op_allowed(op_obj) -> bool:
    """Проверяет, входит ли оператор в разрешённый список (Anex/Coral/Join UP!/Kompas)"""
    if not op_obj:
        return False
    if isinstance(op_obj, dict):
        name = ((op_obj.get("russianName") or "") + " " + (op_obj.get("name") or "")).lower()
    else:
        name = str(op_obj).lower()
    return any(kw in name for kw in _TV_REST_OP_KEYWORDS)


def search_hotels_turkey_rest(date_from_str: str, nights: int, adults: int,
                               children_ages: list = None,
                               whitelist: list = None,
                               whitelist_aliases: dict = None,
                               top: int = 20) -> List[Dict]:
    """
    Поиск отелей Турции через новый REST API TourVisor (JWT).
    Параметры:
      date_from_str  — дата вылета в формате "DD.MM.YYYY"
      nights         — количество ночей (7)
      adults         — число взрослых
      children_ages  — список возрастов детей (например [9])
      whitelist      — список разрешённых отелей (REQUIRED_TURKEY)
      whitelist_aliases — синонимы (TURKEY_ALIASES)
      top            — максимум отелей в результате
    Возвращает список dict с полями: name, stars, region, price_eur, meal, operator, tour_id, tour_url, flydate
    """
    if children_ages is None:
        children_ages = []
    if whitelist is None:
        whitelist = []
    if whitelist_aliases is None:
        whitelist_aliases = {}

    # Конвертируем дату DD.MM.YYYY → YYYY-MM-DD для нового API
    try:
        dt_obj = datetime.strptime(date_from_str, "%d.%m.%Y")
        date_api = dt_obj.strftime("%Y-%m-%d")
        # dateTo: добавляем 14 дней гибкости для большего охвата отелей
        date_to_obj = dt_obj + timedelta(days=14)
        date_to_api = date_to_obj.strftime("%Y-%m-%d")
    except Exception:
        date_api    = date_from_str
        date_to_api = date_from_str

    # Строим параметры запроса
    base_params = [
        ("departureId", 99),
        ("countryId",   4),          # Турция
        ("dateFrom",    date_api),
        ("dateTo",      date_to_api),
        ("nightsFrom",  nights),
        ("nightsTo",    nights),
        ("adults",      min(adults, 9)),    # REST API принимает не более 9 взрослых
        ("meal",        7),           # AI и лучше
        ("hotelCategory", 5),         # от 5 звёзд
        ("hotelRating", 4),           # 4.0+ — как в my_test.py для правильной фильтрации
        ("currency",    "CU"),        # у.е. → ответ в EUR для Турции
        ("onlyCharter", "false"),
    ]
    # Операторы: Anex=13, Coral=11, Join UP!=125, Kompas=90 — передаём в API
    for op_id in TV_REST_TURKEY_OPS:
        base_params.append(("operatorIds", op_id))
    # Курорты Анталийского побережья
    for reg_id in TV_REST_TURKEY_REGIONS:
        base_params.append(("regionIds", reg_id))
    # Дети
    for age in children_ages:
        base_params.append(("childs", age))

    logger.info(f"[TV_REST] 🇹🇷 Запуск поиска Турции: {date_api}—{date_to_api}, {adults} взр., {nights} ночей")

    # ── Шаг 1: запуск поиска ───────────────────────────────────────────────
    try:
        r = _tv_rest_safe_get(f"{TV_REST_BASE}/tours/search", params=base_params, timeout=30)
    except Exception as e:
        logger.error(f"[TV_REST] Не удалось запустить поиск: {e}")
        return []

    if r.status_code != 200:
        logger.error(f"[TV_REST] Поиск вернул {r.status_code}: {r.text[:200]}")
        return []

    search_id = r.json().get("searchId")
    if not search_id:
        logger.error(f"[TV_REST] searchId не получен: {r.json()}")
        return []

    logger.info(f"[TV_REST] searchId={search_id}")

    # ── Шаг 2: ожидание завершения ────────────────────────────────────────
    for attempt in range(40):
        time.sleep(3)
        try:
            st = _tv_rest_safe_get(
                f"{TV_REST_BASE}/tours/search/{search_id}/status",
                params={"operatorStatus": "false"}, timeout=60
            )
        except Exception as e:
            logger.warning(f"[TV_REST] Статус ошибка: {e}")
            continue
        if st.status_code != 200:
            continue
        sd = st.json()
        progress = sd.get("progress", 0)
        status   = sd.get("status", "")
        logger.info(f"[TV_REST] [{attempt*3}с] progress={progress}% | {status}")
        if progress >= 100 or status in ("finished", "complete"):
            break

    # ── Шаг 3: продолжение поиска (больше туров, как в my_test.py) ──────────
    for cont_num in range(1, 4):
        try:
            cont = _tv_rest_safe_get(
                f"{TV_REST_BASE}/tours/search/{search_id}/continue", timeout=60
            )
            if cont.status_code == 200:
                rc = cont.json().get("requestCount", "?")
                logger.info(f"[TV_REST] 🔄 Продолжение {cont_num}: requestCount={rc}")
                time.sleep(10)
            else:
                logger.warning(f"[TV_REST] Продолжение {cont_num}: HTTP {cont.status_code}")
                break
        except Exception as e:
            logger.warning(f"[TV_REST] Продолжение {cont_num} не удалось: {e}")

    # ── Шаг 4: получение результатов (проверка + доп. продолжения если мало) ──
    try:
        res_check = _tv_rest_safe_get(
            f"{TV_REST_BASE}/tours/search/{search_id}",
            params={"limit": 200}, timeout=60
        )
        if res_check.status_code == 200:
            check_count = len(res_check.json() or [])
            # Если нашли меньше 30 отелей — делаем ещё продолжения
            if check_count < 30:
                logger.info(f"[TV_REST] Нашли только {check_count} отелей, делаем доп. продолжения...")
                for cont_num in range(4, 7):
                    try:
                        cont = _tv_rest_safe_get(
                            f"{TV_REST_BASE}/tours/search/{search_id}/continue", timeout=60
                        )
                        if cont.status_code == 200:
                            rc = cont.json().get("requestCount", "?")
                            logger.info(f"[TV_REST] 🔄 Доп. продолжение {cont_num}: requestCount={rc}")
                            time.sleep(10)
                        else:
                            break
                    except Exception as e:
                        logger.warning(f"[TV_REST] Доп. продолжение {cont_num} не удалось: {e}")
    except Exception:
        pass

    try:
        res = _tv_rest_safe_get(
            f"{TV_REST_BASE}/tours/search/{search_id}",
            params={"limit": 200}, timeout=60
        )
    except Exception as e:
        logger.error(f"[TV_REST] Не удалось получить результаты: {e}")
        return []

    if res.status_code != 200:
        logger.error(f"[TV_REST] Результаты вернули {res.status_code}")
        return []

    hotels_raw = res.json() or []
    logger.info(f"[TV_REST] Получено {len(hotels_raw)} отелей от API")

    # ── Шаг 5: фильтрация ─────────────────────────────────────────────────
    result = []
    seen = set()

    for h in hotels_raw:
        name = h.get("name", "?")

        # Белый список — только если он задан
        if whitelist:
            if not _is_in_list(name, whitelist, aliases=whitelist_aliases):
                continue

        # Дедупликация
        name_key = _clean_name(name)[:30]
        if name_key in seen:
            continue
        seen.add(name_key)

        tours = h.get("tours", [])
        if not tours:
            continue

        # Выбираем лучший тур нужного оператора
        # Сначала фильтруем туры по целевой дате (±3 дня от date_api)
        best_tour  = None
        best_price = float("inf")

        # Целевая дата (для приоритета)
        try:
            target_date = datetime.strptime(date_api, "%Y-%m-%d").date()
        except Exception:
            target_date = None

        for t in tours:
            if not _tv_rest_op_allowed(t.get("operator")):
                continue
            p = t.get("price", 0) or 0
            if p <= 0:
                continue

            # Проверяем дату тура (если задана целевая дата)
            if target_date:
                tour_date_str = t.get("date", "")
                if tour_date_str:
                    try:
                        tour_date = datetime.strptime(tour_date_str.split("T")[0], "%Y-%m-%d").date()
                        # Берём туры на целевую дату ±3 дня
                        if abs((tour_date - target_date).days) <= 3:
                            if p < best_price:
                                best_price = p
                                best_tour  = t
                    except Exception:
                        pass
            else:
                # Если целевой даты нет — берём просто минимум
                if p < best_price:
                    best_price = p
                    best_tour  = t

        if not best_tour:
            continue

        # Название питания
        meal_obj = best_tour.get("meal", {})
        if isinstance(meal_obj, dict):
            meal_name = meal_obj.get("name", "") or meal_obj.get("russianName", "—")
        else:
            meal_name = str(meal_obj) if meal_obj else "—"

        # Название оператора
        op_obj = best_tour.get("operator", {})
        if isinstance(op_obj, dict):
            op_name = op_obj.get("russianName", "") or op_obj.get("name", "—")
        else:
            op_name = str(op_obj) if op_obj else "—"

        tour_id  = str(best_tour.get("id", ""))
        tour_url = (f"https://tourvisor.ru/search.php#tvtourid={tour_id}"
                    if tour_id else "")

        region_obj  = h.get("region", {})
        region_name = region_obj.get("name", "") if isinstance(region_obj, dict) else ""

        # Цена уже в EUR (REST API для Турции возвращает CU≈EUR)
        price_eur = round(best_price)

        logger.info(
            f"[TV_REST] ✅ {name} [{region_name}]: "
            f"{price_eur} EUR | 🍽{meal_name} | 👷{op_name}"
        )

        result.append({
            "name":     name,
            "stars":    h.get("category", 5),
            "region":   region_name,
            "price_eur": price_eur,
            "meal":     meal_name,
            "operator": op_name,
            "tour_id":  tour_id,
            "tour_url": tour_url,
            "flydate":  best_tour.get("date", date_from_str),
            "nights":   nights,
        })

    result.sort(key=lambda x: x["price_eur"])
    logger.info(f"[TV_REST] После фильтрации: {len(result)} отелей (Anex/Coral/JoinUP/Kompas)")

    # ── Умный выбор: по одному отелю от каждого ключевого слова белого списка ──
    # При наличии whitelist — строго только отели из белого списка, по 1 на каждое ключевое слово
    if whitelist and result:
        used_hotel_names = set()  # нормализованные имена уже выбранных отелей
        selected = []

        for kw in whitelist:
            kw_lower = kw.lower()
            for h in result:  # result отсортирован по цене (дешёвые первые)
                name_norm = _clean_name(h["name"])[:30]
                if name_norm in used_hotel_names:
                    continue
                if kw_lower in h["name"].lower():
                    selected.append(h)
                    used_hotel_names.add(name_norm)
                    break  # следующее ключевое слово

        # Финальная сортировка по цене
        selected.sort(key=lambda x: x["price_eur"])
        logger.info(f"[TV_REST] Умная выборка: {len(selected)} отелей (по 1 от каждого из белого списка)")
        return selected

    return result[:top]


# Курорты Испании: Коста Брава=69, Коста Дель Маресме=70, Коста Дорада=71
TV_REST_SPAIN_BCN_REGIONS = [69, 70, 71]
# Майорка=102
TV_REST_MALLORCA_REGIONS = [102]
# Тенерифе=101
TV_REST_TENERIFE_REGIONS = [101]
# Операторы Испании BCN: Coral=11, Anex=13, Kompas=90, Join UP!=125
TV_REST_SPAIN_OPS = [13, 11, 125, 90]
# Операторы Майорки: Anex=13, Coral=11, JoinUP=125, Kompas=90, Pegas=12, TezTour=35, Biblio=4
TV_REST_MALLORCA_OPS = [13, 11, 125, 90, 12, 35, 4]

# Разрешённые операторы для Майорки (клиентская фильтрация)
_TV_REST_MALLORCA_OP_KEYWORDS = [
    "anex", "анекс", "coral", "корал", "join up", "джойн", "kompas", "компас",
    "tui", "тui", "pegas", "пегас", "tez", "тез", "biblio", "библио",
]


def search_hotels_spain_rest(date_from_str: str, nights: int, adults: int,
                              children_ages: list = None,
                              destination_key: str = "spain_bcn",
                              top: int = 50) -> List[Dict]:
    """
    Поиск отелей Испании / Майорки / Тенерифе через новый REST API TourVisor (JWT).
    destination_key: 'spain_bcn' | 'mallorca' | 'tenerife'
    Возвращает список dict: {name, stars, region, price_eur, meal, operator, tour_id, tour_url, flydate}
    """
    if children_ages is None:
        children_ages = []

    # Конвертируем дату DD.MM.YYYY → YYYY-MM-DD
    try:
        dt_obj = datetime.strptime(date_from_str, "%d.%m.%Y")
        date_api = dt_obj.strftime("%Y-%m-%d")
        # dateTo: добавляем 14 дней гибкости для большего охвата отелей
        date_to_obj = dt_obj + timedelta(days=14)
        date_to_api = date_to_obj.strftime("%Y-%m-%d")
    except Exception:
        date_api    = date_from_str
        date_to_api = date_from_str

    # Выбираем курорты и параметры по направлению
    if destination_key == "mallorca":
        region_ids        = TV_REST_MALLORCA_REGIONS
        meal_min          = 4        # HB и лучше
        stars_min         = 3
        rating_min        = 0        # любой рейтинг (иначе мало отелей)
        label             = "Майорка"
        # Майорка: не передаём операторов в API (иначе мало отелей), но фильтруем на клиенте
        ops_to_use        = []
        client_op_filter  = _TV_REST_MALLORCA_OP_KEYWORDS   # расширенный список для Майорки
    elif destination_key == "tenerife":
        region_ids        = TV_REST_TENERIFE_REGIONS
        meal_min          = 4        # HB и лучше
        stars_min         = 4        # 4★+ для Тенерифе
        rating_min        = 3        # 3.5+
        label             = "Тенерифе"
        ops_to_use        = []       # все операторы
        client_op_filter  = None     # None = все операторы
    else:  # spain_bcn: Коста Брава + Маресме + Дорада
        region_ids        = TV_REST_SPAIN_BCN_REGIONS
        meal_min          = 4        # HB и лучше
        stars_min         = 3
        rating_min        = 0        # любой рейтинг — больше отелей
        label             = "Испания (Коста Брава/Маресме/Дорада)"
        ops_to_use        = TV_REST_SPAIN_OPS  # [13, 11, 125, 90]
        client_op_filter  = _TV_REST_OP_KEYWORDS

    base_params = [
        ("departureId",   99),
        ("countryId",     14),          # Испания
        ("dateFrom",      date_api),
        ("dateTo",        date_to_api),
        ("nightsFrom",    nights),
        ("nightsTo",      nights),
        ("adults",        min(adults, 9)),   # REST API принимает не более 9 взрослых
        ("meal",          meal_min),    # HB(4) и лучше
        ("hotelCategory", stars_min),   # от 3★
        ("hotelRating",   rating_min),  # 3.5+
        ("currency",      "CU"),
        ("onlyCharter",   "false"),
    ]
    for op_id in ops_to_use:
        base_params.append(("operatorIds", op_id))
    for reg_id in region_ids:
        base_params.append(("regionIds", reg_id))
    for age in children_ages:
        base_params.append(("childs", age))

    logger.info(f"[TV_REST_SPAIN] 🇪🇸 {label}: {date_api}—{date_to_api}, {adults} взр., {nights} ночей")

    # ── Шаг 1: запуск поиска ───────────────────────────────────────────────
    try:
        r = _tv_rest_safe_get(f"{TV_REST_BASE}/tours/search", params=base_params, timeout=30)
    except Exception as e:
        logger.error(f"[TV_REST_SPAIN] Не удалось запустить поиск: {e}")
        return []

    if r.status_code != 200:
        logger.error(f"[TV_REST_SPAIN] Поиск вернул {r.status_code}: {r.text[:200]}")
        return []

    search_id = r.json().get("searchId")
    if not search_id:
        logger.error(f"[TV_REST_SPAIN] searchId не получен: {r.json()}")
        return []

    logger.info(f"[TV_REST_SPAIN] searchId={search_id}")

    # ── Шаг 2: ожидание завершения ────────────────────────────────────────
    for attempt in range(40):
        time.sleep(3)
        try:
            st = _tv_rest_safe_get(
                f"{TV_REST_BASE}/tours/search/{search_id}/status",
                params={"operatorStatus": "false"}, timeout=60
            )
        except Exception as e:
            logger.warning(f"[TV_REST_SPAIN] Статус ошибка: {e}")
            continue
        if st.status_code != 200:
            continue
        sd = st.json()
        progress = sd.get("progress", 0)
        status   = sd.get("status", "")
        logger.info(f"[TV_REST_SPAIN] [{attempt*3}с] progress={progress}% | {status}")
        if progress >= 100 or status in ("finished", "complete"):
            break

    # ── Шаг 3: продолжение поиска (больше туров, как в my_test.py) ─────────
    for cont_num in range(1, 4):
        try:
            cont = _tv_rest_safe_get(
                f"{TV_REST_BASE}/tours/search/{search_id}/continue", timeout=60
            )
            if cont.status_code == 200:
                rc = cont.json().get("requestCount", "?")
                logger.info(f"[TV_REST_SPAIN] 🔄 Продолжение {cont_num}: requestCount={rc}")
                time.sleep(10)
            else:
                logger.warning(f"[TV_REST_SPAIN] Продолжение {cont_num}: HTTP {cont.status_code}")
                break  # дальше смысла нет
        except Exception as e:
            logger.warning(f"[TV_REST_SPAIN] Продолжение {cont_num} не удалось: {e}")

    # ── Шаг 4: получение результатов (промежуточная проверка — если мало, ещё continue) ──
    try:
        res_check = _tv_rest_safe_get(
            f"{TV_REST_BASE}/tours/search/{search_id}",
            params={"limit": 200}, timeout=60
        )
        if res_check.status_code == 200:
            check_count = len(res_check.json() or [])
            # Если нашли меньше 20 отелей — делаем ещё 3 продолжения
            min_expected = 20 if destination_key == "spain_bcn" else 10
            if check_count < min_expected:
                logger.info(f"[TV_REST_SPAIN] Нашли только {check_count} отелей, делаем доп. продолжения...")
                for cont_num in range(4, 7):
                    try:
                        cont = _tv_rest_safe_get(
                            f"{TV_REST_BASE}/tours/search/{search_id}/continue", timeout=60
                        )
                        if cont.status_code == 200:
                            rc = cont.json().get("requestCount", "?")
                            logger.info(f"[TV_REST_SPAIN] 🔄 Доп. продолжение {cont_num}: requestCount={rc}")
                            time.sleep(10)
                        else:
                            break
                    except Exception as e:
                        logger.warning(f"[TV_REST_SPAIN] Доп. продолжение {cont_num} не удалось: {e}")
    except Exception:
        pass

    try:
        res = _tv_rest_safe_get(
            f"{TV_REST_BASE}/tours/search/{search_id}",
            params={"limit": 200}, timeout=60
        )
    except Exception as e:
        logger.error(f"[TV_REST_SPAIN] Не удалось получить результаты: {e}")
        return []

    if res.status_code != 200:
        logger.error(f"[TV_REST_SPAIN] Результаты вернули {res.status_code}")
        return []

    hotels_raw = res.json() or []
    logger.info(f"[TV_REST_SPAIN] Получено {len(hotels_raw)} отелей от API")

    # ── Шаг 5: фильтрация ─────────────────────────────────────────────────
    # Допустимые питания: 1=RO, 2=BB, 3=BB+, 4=HB, 5=FB, 7=AI, 9=UAI
    # Для Майорки и Испании BCN — HB и выше
    ALLOWED_MEALS = {4, 5, 7, 9}   # HB и выше для всех

    result = []
    seen = set()

    for h in hotels_raw:
        name = h.get("name", "?")
        region_obj  = h.get("region", {})

        # Дедупликация
        name_key = _clean_name(name)[:30]
        if name_key in seen:
            continue
        seen.add(name_key)

        tours = h.get("tours", [])
        if not tours:
            continue

        def _op_allowed_local(op_obj, keywords):
            if keywords is None:
                return True  # None = все операторы разрешены
            if not op_obj:
                return False
            if isinstance(op_obj, dict):
                n = ((op_obj.get("russianName") or "") + " " + (op_obj.get("name") or "")).lower()
            else:
                n = str(op_obj).lower()
            return any(kw in n for kw in keywords)

        # Выбираем лучший тур нужного оператора c нужным питанием
        best_tour  = None
        best_price = float("inf")

        # Целевая дата (для приоритета)
        try:
            target_date = datetime.strptime(date_api, "%Y-%m-%d").date()
        except Exception:
            target_date = None

        for t in tours:
            # Фильтр операторов (используем client_op_filter для конкретного направления)
            if not _op_allowed_local(t.get("operator"), client_op_filter):
                continue
            # Фильтр питания
            meal_obj = t.get("meal", {})
            meal_id  = meal_obj.get("id", 0) if isinstance(meal_obj, dict) else 0
            if meal_id not in ALLOWED_MEALS:
                continue
            p = t.get("price", 0) or 0
            if p <= 0:
                continue

            # Проверяем дату тура (если задана целевая дата)
            if target_date:
                tour_date_str = t.get("date", "")
                if tour_date_str:
                    try:
                        tour_date = datetime.strptime(tour_date_str.split("T")[0], "%Y-%m-%d").date()
                        # Берём туры на целевую дату ±3 дня
                        if abs((tour_date - target_date).days) <= 3:
                            if p < best_price:
                                best_price = p
                                best_tour  = t
                    except Exception:
                        pass
            else:
                # Если целевой даты нет — берём просто минимум
                if p < best_price:
                    best_price = p
                    best_tour  = t

        if not best_tour:
            continue

        # Название питания
        meal_obj  = best_tour.get("meal", {})
        meal_name = meal_obj.get("name", "") or meal_obj.get("russianName", "—") if isinstance(meal_obj, dict) else str(meal_obj)

        # Название оператора
        op_obj  = best_tour.get("operator", {})
        op_name = op_obj.get("russianName", "") or op_obj.get("name", "—") if isinstance(op_obj, dict) else str(op_obj)

        tour_id  = str(best_tour.get("id", ""))
        tour_url = (f"https://tourvisor.ru/search.php#tvtourid={tour_id}"
                    if tour_id else "")

        region_name = region_obj.get("name", "") if isinstance(region_obj, dict) else ""
        price_eur   = round(best_price)

        logger.info(
            f"[TV_REST_SPAIN] ✅ {name} [{region_name}]: "
            f"{price_eur} EUR | 🍽{meal_name} | 👷{op_name}"
        )

        result.append({
            "name":      name,
            "stars":     h.get("category", 3),
            "region":    region_name,
            "price_eur": price_eur,
            "meal":      meal_name,
            "operator":  op_name,
            "tour_id":   tour_id,
            "tour_url":  tour_url,
            "flydate":   best_tour.get("date", date_from_str),
            "nights":    nights,
        })

    result.sort(key=lambda x: x["price_eur"])
    logger.info(f"[TV_REST_SPAIN] После фильтрации: {len(result)} отелей ({label})")
    return result[:top]


def _tv_api(method: str, params: dict, retries: int = 3) -> dict:
    """Запрос к TourVisor API с повторными попытками при таймауте"""
    params.update({"format": "json", "authlogin": TV_LOGIN, "authpass": TV_PASSWORD})
    for attempt in range(retries):
        try:
            r = requests.get(f"{TV_BASE}{method}", params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                logger.warning(f"[TV] Таймаут, повтор {attempt+1}/{retries}...")
                time.sleep(3)
            else:
                logger.error(f"[TV] Превышено число попыток ({retries}): таймаут")
        except Exception as e:
            logger.error(f"[TV] {e}")
            break
    return {}


def _split_message(text: str, max_len: int = 1900) -> List[str]:
    """Разбивает длинный текст на части по max_len символов по строкам"""
    parts = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > max_len:
            if current.strip():
                parts.append(current.strip())
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        parts.append(current.strip())
    return parts if parts else [text[:max_len]]


# ─────────────────────────────────────────────────────────────────────────────
# ШАГ 1: ПОИСК РЕЙСОВ
# ─────────────────────────────────────────────────────────────────────────────

# Глобальный парсер — Selenium открывается один раз и переиспользуется
_tm_parser = None

def _get_tm_parser():
    """Возвращает глобальный экземпляр TravelMarketParser (Selenium открыт один раз)"""
    global _tm_parser
    if _tm_parser is None:
        from travelmarket_parser import TravelMarketParser
        _tm_parser = TravelMarketParser()
        logger.info("[TravelMarket] Создан глобальный парсер")
    return _tm_parser


def _tm_parse(parser, url: str) -> list:
    """Парсит URL: сначала requests, если 0 результатов — Selenium"""
    flights = parser.parse_with_requests(url)
    if not flights:
        logger.info(f"[TravelMarket] requests=0, пробуем Selenium: {url[:80]}...")
        flights = parser.parse_with_selenium(url)
    return flights or []


def _search_travelmarket_flight(departure_city: str, country: str,
                                 departure_date: datetime) -> Dict:
    """
    Парсим TravelMarket (nFlexDays=28, точная дата).
    Возвращает {'sharm': EUR, 'hurghada': EUR, 'generic': EUR} или None.
    Использует Selenium если доступен, иначе пробует requests.
    """
    try:
        parser = _get_tm_parser()

        dep_date_str = departure_date.strftime("%Y-%m-%d")
        flex = 28
        all_flights = []

        if country == "egypt":
            # Раздельный поиск по Хургаде и Шарму
            url_hrg = parser.build_url_hurghada(departure_city, dep_date_str, flex_days=flex)
            fl_hrg = _tm_parse(parser, url_hrg)
            for f in fl_hrg:
                f["destination"] = "Hurghada"
            all_flights += fl_hrg

            url_ssh = parser.build_url_sharm(departure_city, dep_date_str, flex_days=flex)
            fl_ssh = _tm_parse(parser, url_ssh)
            for f in fl_ssh:
                f["destination"] = "Sharm El Sheikh"
            all_flights += fl_ssh

            logger.info(f"[TravelMarket] Хургада: {len(fl_hrg)}, Шарм: {len(fl_ssh)} рейсов")
        elif country == "tenerife":
            # Для Тенерифе используем специальный URL с lSubAreaIDs=502666
            url_ten = parser.build_url_tenerife(departure_city, dep_date_str, flex_days=flex)
            logger.info(f"[TravelMarket] Тенерифе URL: {url_ten[:100]}...")
            fl_ten = _tm_parse(parser, url_ten)
            for f in fl_ten:
                f["destination"] = "Tenerife"
            all_flights += fl_ten
            logger.info(f"[TravelMarket] Тенерифе: {len(fl_ten)} рейсов")
        elif country in ("turkey", "antalya"):
            # Турция: Анталья, Белек, Аланья, Сиде (lPlaceIDs=74347,74373,74641,209164)
            url_tr = parser.build_url_turkey(departure_city, dep_date_str, flex_days=flex)
            logger.info(f"[TravelMarket] Турция URL: {url_tr[:100]}...")
            fl_tr = _tm_parse(parser, url_tr)
            for f in fl_tr:
                f["destination"] = "Turkey"
            all_flights += fl_tr
            logger.info(f"[TravelMarket] Турция: {len(fl_tr)} рейсов")
        elif country in ("mallorca",):
            # Майорка (lSubAreaIDs=501354)
            url_mal = parser.build_url_mallorca(departure_city, dep_date_str, flex_days=flex)
            logger.info(f"[TravelMarket] Майорка URL: {url_mal[:100]}...")
            fl_mal = _tm_parse(parser, url_mal)
            for f in fl_mal:
                f["destination"] = "Mallorca"
            all_flights += fl_mal
            logger.info(f"[TravelMarket] Майорка: {len(fl_mal)} рейсов")
        elif country in ("spain", "spain_barcelona", "barcelona", "costa_brava"):
            # Барселона + Коста Брава + Коста Дорада
            url_bcn = parser.build_url_spain_barcelona(departure_city, dep_date_str, flex_days=flex)
            logger.info(f"[TravelMarket] Испания(BCN+CB+CD) URL: {url_bcn[:100]}...")
            fl_bcn = _tm_parse(parser, url_bcn)
            for f in fl_bcn:
                f["destination"] = "Spain"
            all_flights += fl_bcn
            logger.info(f"[TravelMarket] Испания(BCN+CB+CD): {len(fl_bcn)} рейсов")
        else:
            url = parser.build_url(departure_city, country, dep_date_str,
                                   nights_min=7, nights_max=9,
                                   domain="dk", flex_days=flex)
            logger.info(f"[TravelMarket] Поиск: {url[:80]}...")
            all_flights = _tm_parse(parser, url)

        flights = all_flights

        if not flights:
            return {}

        # Актуальный курс DKK→EUR
        try:
            import requests as req_lib
            r = req_lib.get("https://api.exchangerate-api.com/v4/latest/DKK", timeout=5)
            dkk_eur = r.json().get("rates", {}).get("EUR", 1/7.47)
        except Exception:
            dkk_eur = 1/7.47

        result = {}
        for f in flights:
            p_dkk = f.get("price_per_person_dkk", 0)
            if not p_dkk:
                continue
            p_eur = round(p_dkk * dkk_eur)
            dest = f.get("destination", "").lower()
            if "sharm" in dest:
                if "sharm" not in result or p_eur < result["sharm"]:
                    result["sharm"] = p_eur
            elif "hurghada" in dest:
                if "hurghada" not in result or p_eur < result["hurghada"]:
                    result["hurghada"] = p_eur
            else:
                if "generic" not in result or p_eur < result["generic"]:
                    result["generic"] = p_eur

        if result:
            logger.info(f"[TravelMarket] Результат: {result}")
        return result
    except Exception as e:
        logger.warning(f"[TravelMarket] Ошибка: {e}")
        return {}


# Максимальная разумная цена roundtrip на человека (EUR/чел) — выше вероятно ошибка или регулярный рейс
# Цена = total_roundtrip_USD / adults * USD_to_EUR
FP_MAX_ONEWAY_EUR = {
    "egypt":  350,   # Чартеры в Египет roundtrip ~200–320 EUR/чел
    "turkey": 450,   # Чартеры в Турцию roundtrip ~200–400 EUR/чел
    "spain":  450,   # Чартеры в Испанию roundtrip ~180–400 EUR/чел
}


def find_best_flight_date(departure_city: str, country: str,
                           month: int, year: int, nights: int, people: int) -> Dict:
    """
    Ищет лучшую (самую дешёвую) дату вылета за месяц.
    Алгоритм:
      1. TravelMarket nFlexDays=28 от 1-го числа месяца — самый быстрый способ,
         возвращает все рейсы за месяц с датами и ценами.
      2. Если TravelMarket не дал результат — FlightPowers для 3-4 дат (1,8,15,22).
    Возвращает тот же формат что и search_flight, плюс 'best_date' datetime.
    """
    import calendar
    from datetime import date as _date

    logger.info(f"[BEST_DATE] 🔍 Поиск лучшей даты: {departure_city} → {country}, {month:02d}/{year}")

    today = datetime.now().date()
    _, last_day = calendar.monthrange(year, month)

    # Начало месяца (или первый доступный день)
    month_start_day = 1
    for d in range(1, last_day + 1):
        if _date(year, month, d) >= today + timedelta(days=3):
            month_start_day = d
            break

    month_start_str = datetime(year, month, month_start_day).strftime("%Y-%m-%d")

    best_date = None
    best_price_sharm = None
    best_price_hrg = None
    best_generic = None
    # Раздельные лучшие даты для каждого направления
    best_date_sharm = None
    best_date_hrg   = None

    # ── ШАГ 1: TravelMarket nFlexDays=28 — раздельно Хургада и Шарм ──────────
    try:
        parser = _get_tm_parser()

        # Актуальный курс DKK→EUR (получаем один раз)
        try:
            r = requests.get("https://api.exchangerate-api.com/v4/latest/DKK", timeout=5)
            dkk_eur = r.json().get("rates", {}).get("EUR", 1/7.47)
        except Exception:
            dkk_eur = 1/7.47

        def _parse_date_str(d_str, yr, mo, fallback_day):
            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(d_str, fmt)
                except Exception:
                    pass
            return datetime(yr, mo, fallback_day)

        def _best_from_flights(flights_list):
            """Находит лучшую (минимальную) цену и дату из списка рейсов.
            Возвращает (best_datetime, best_price_eur) или (None, None)."""
            best_d = None
            best_p = None
            for f in flights_list:
                dep_date_raw = f.get("departure_date", "")
                if not dep_date_raw:
                    continue
                p_dkk = f.get("price_per_person_dkk") or 0
                if p_dkk:
                    p_eur = round(float(p_dkk) * dkk_eur)
                else:
                    p_eur_raw = f.get("price_per_person_eur") or 0
                    if not p_eur_raw:
                        continue
                    p_eur = round(float(p_eur_raw))
                if best_p is None or p_eur < best_p:
                    best_p = p_eur
                    best_d = _parse_date_str(dep_date_raw, year, month, month_start_day)
            return best_d, best_p

        if country == "egypt":
            # Хургада: lPlaceIDs=59338
            url_hrg = parser.build_url_hurghada(departure_city, month_start_str, flex_days=28)
            logger.info(f"[BEST_DATE] TravelMarket Хургада URL: {url_hrg[:100]}...")
            flights_hrg = _tm_parse(parser, url_hrg) or []
            if flights_hrg:
                logger.info(f"[BEST_DATE] TravelMarket Хургада: {len(flights_hrg)} рейсов")
                best_date_hrg, best_price_hrg = _best_from_flights(flights_hrg)
                if best_date_hrg:
                    logger.info(f"[BEST_DATE] Хургада лучшая дата: {best_date_hrg.strftime('%d.%m.%Y')} — {best_price_hrg} EUR/чел")

            # Шарм: lPlaceIDs=59303
            url_ssh = parser.build_url_sharm(departure_city, month_start_str, flex_days=28)
            logger.info(f"[BEST_DATE] TravelMarket Шарм URL: {url_ssh[:100]}...")
            flights_ssh = _tm_parse(parser, url_ssh) or []
            if flights_ssh:
                logger.info(f"[BEST_DATE] TravelMarket Шарм: {len(flights_ssh)} рейсов")
                best_date_sharm, best_price_sharm = _best_from_flights(flights_ssh)
                if best_date_sharm:
                    logger.info(f"[BEST_DATE] Шарм лучшая дата: {best_date_sharm.strftime('%d.%m.%Y')} — {best_price_sharm} EUR/чел")

            # best_date — глобальная (с минимальной ценой из двух направлений)
            if best_date_hrg and best_date_sharm:
                if best_price_hrg <= best_price_sharm:
                    best_date = best_date_hrg
                else:
                    best_date = best_date_sharm
            elif best_date_hrg:
                best_date = best_date_hrg
            elif best_date_sharm:
                best_date = best_date_sharm

            if best_date:
                logger.info(
                    f"[BEST_DATE] TravelMarket ✅ "
                    f"Шарм={best_date_sharm.strftime('%d.%m.%Y') if best_date_sharm else '?'} {best_price_sharm} EUR | "
                    f"Хургада={best_date_hrg.strftime('%d.%m.%Y') if best_date_hrg else '?'} {best_price_hrg} EUR"
                )

        elif country in ("turkey", "antalya"):
            # Турция: Анталья, Белек, Аланья, Сиде
            url_tr = parser.build_url_turkey(departure_city, month_start_str, flex_days=28)
            logger.info(f"[BEST_DATE] TravelMarket Турция URL: {url_tr[:100]}...")
            flights_tr = _tm_parse(parser, url_tr) or []
            if flights_tr:
                logger.info(f"[BEST_DATE] TravelMarket Турция: {len(flights_tr)} рейсов")
                best_date, best_generic = _best_from_flights(flights_tr)
                if best_date:
                    logger.info(f"[BEST_DATE] Турция лучшая дата: {best_date.strftime('%d.%m.%Y')} — {best_generic} EUR/чел")

        elif country == "mallorca":
            # Майорка
            url_mal = parser.build_url_mallorca(departure_city, month_start_str, flex_days=28)
            logger.info(f"[BEST_DATE] TravelMarket Майорка URL: {url_mal[:100]}...")
            flights_mal = _tm_parse(parser, url_mal) or []
            if flights_mal:
                logger.info(f"[BEST_DATE] TravelMarket Майорка: {len(flights_mal)} рейсов")
                best_date, best_generic = _best_from_flights(flights_mal)
                if best_date:
                    logger.info(f"[BEST_DATE] Майорка лучшая дата: {best_date.strftime('%d.%m.%Y')} — {best_generic} EUR/чел")

        elif country in ("spain", "spain_barcelona", "barcelona", "costa_brava"):
            # Испания: Барселона + Коста Брава + Коста Дорада
            url_bcn = parser.build_url_spain_barcelona(departure_city, month_start_str, flex_days=28)
            logger.info(f"[BEST_DATE] TravelMarket Испания(BCN+CB+CD) URL: {url_bcn[:100]}...")
            flights_bcn = _tm_parse(parser, url_bcn) or []
            if flights_bcn:
                logger.info(f"[BEST_DATE] TravelMarket Испания(BCN+CB+CD): {len(flights_bcn)} рейсов")
                best_date, best_generic = _best_from_flights(flights_bcn)
                if best_date:
                    logger.info(f"[BEST_DATE] Испания лучшая дата: {best_date.strftime('%d.%m.%Y')} — {best_generic} EUR/чел")

        else:
            url = parser.build_url(departure_city, country, month_start_str,
                                   nights_min=7, nights_max=9, domain="dk", flex_days=28)
            logger.info(f"[BEST_DATE] TravelMarket URL: {url[:90]}...")
            flights_tm = _tm_parse(parser, url) or []
            if flights_tm:
                best_date, best_generic = _best_from_flights(flights_tm)
                if best_date:
                    logger.info(f"[BEST_DATE] TravelMarket ✅ {best_date.strftime('%d.%m.%Y')} — {best_generic} EUR/чел")

    except Exception as e:
        logger.warning(f"[BEST_DATE] TravelMarket ошибка: {e}", exc_info=True)

    # ── ШАГ 2: FlightPowers если TravelMarket ничего не нашёл ───────────────
    if best_date is None:
        try:
            from flightpowers_parser import FlightPowersAPI, AIRPORT_CODES as FP_AIRPORTS
            fp_api = FlightPowersAPI()
            from_code = FP_AIRPORTS.get(departure_city, "CPH")
        except Exception as e:
            logger.error(f"[BEST_DATE] FlightPowers недоступен: {e}")
            fallback_date = datetime(year, month, month_start_day)
            return {**_flight_fallback(departure_city, country, people, fallback_date, nights),
                    "best_date": fallback_date}

        max_price = FP_MAX_ONEWAY_EUR.get(country, 450)
        # Проверяем 4 даты в месяце: ~1, 8, 15, 22
        sample_days = []
        for day in [1, 8, 15, 22]:
            d = _date(year, month, day)
            if d <= _date(year, month, last_day) and d >= today + timedelta(days=3):
                sample_days.append(d)
        if not sample_days:
            for day in range(month_start_day, last_day + 1):
                d = _date(year, month, day)
                if d >= today + timedelta(days=3):
                    sample_days = [d]
                    break

        logger.info(f"[BEST_DATE] FlightPowers проверяем: {[str(d) for d in sample_days]}")

        if country == "egypt":
            for d in sample_days:
                dep_str = d.strftime("%Y-%m-%d")
                try:
                    res_ssh = fp_api.search_oneway(from_code, "SSH", dep_str)
                    p_ssh = fp_api.parse_flight_price(res_ssh, adults=people) if res_ssh else None
                    p_ssh = round(p_ssh) if (p_ssh and 0 < p_ssh <= max_price) else None

                    res_hrg = fp_api.search_oneway(from_code, "HRG", dep_str)
                    p_hrg = fp_api.parse_flight_price(res_hrg, adults=people) if res_hrg else None
                    p_hrg = round(p_hrg) if (p_hrg and 0 < p_hrg <= max_price) else None

                    prices = [p for p in [p_ssh, p_hrg] if p]
                    if prices:
                        score = min(prices)
                        current_best = min(best_price_sharm or 9999, best_price_hrg or 9999)
                        if best_date is None or score < current_best:
                            best_date = datetime.combine(d, datetime.min.time())
                            best_price_sharm = p_ssh
                            best_price_hrg = p_hrg
                            logger.info(f"[BEST_DATE] FP ✅ {dep_str}: SSH={p_ssh} HRG={p_hrg} EUR/чел")
                except Exception as e:
                    logger.warning(f"[BEST_DATE] FP ошибка {dep_str}: {e}")
        else:
            dest_map = {"turkey": "AYT", "spain": "PMI"}
            dest_code = dest_map.get(country, "AYT")
            best_fp_price = None
            for d in sample_days:
                dep_str = d.strftime("%Y-%m-%d")
                ret_str = (d + timedelta(days=nights)).strftime("%Y-%m-%d")
                try:
                    res = fp_api.search_roundtrip(from_code, dest_code, dep_str, ret_str)
                    price = fp_api.parse_flight_price(res, adults=people) if res else None
                    if price and 0 < price <= max_price:
                        p = round(price)
                        if best_fp_price is None or p < best_fp_price:
                            best_fp_price = p
                            best_date = datetime.combine(d, datetime.min.time())
                            logger.info(f"[BEST_DATE] FP ✅ {dep_str}: {p} EUR/чел")
                except Exception as e:
                    logger.warning(f"[BEST_DATE] FP ошибка {dep_str}: {e}")
            if best_date and best_fp_price:
                return {
                    "price_per_person": best_fp_price, "price_sharm": best_fp_price,
                    "price_hurghada": best_fp_price, "price_total": best_fp_price * people,
                    "source": "flightpowers", "details": f"{from_code}→{country} {best_fp_price} EUR/чел",
                    "best_date": best_date, "date_sharm": best_date, "date_hrg": best_date,
                }

    # ── Если ничего не нашли — fallback ──────────────────────────────────────
    if best_date is None and best_date_sharm is None and best_date_hrg is None:
        fallback_date = datetime(year, month, max(month_start_day, 10))
        logger.warning(f"[BEST_DATE] Нет результатов → fallback {fallback_date.strftime('%d.%m.%Y')}")
        fb = _flight_fallback(departure_city, country, people, fallback_date, nights)
        return {**fb, "best_date": fallback_date, "date_sharm": fallback_date, "date_hrg": fallback_date}

    # Если нашли только одно из двух направлений — дублируем цену, НО дата остаётся своей
    if best_price_sharm and not best_price_hrg:
        best_price_hrg = best_price_sharm
        if best_date_hrg is None:
            best_date_hrg = best_date_sharm
    elif best_price_hrg and not best_price_sharm:
        best_price_sharm = best_price_hrg
        if best_date_sharm is None:
            best_date_sharm = best_date_hrg

    if not best_price_sharm:
        fb = _flight_fallback(departure_city, country, people, best_date or datetime(year, month, month_start_day), nights)
        best_price_sharm = fb["price_sharm"]
        best_price_hrg   = fb["price_hurghada"]

    # Если раздельные даты не нашлись — используем общую best_date
    if best_date is None:
        best_date = best_date_sharm or best_date_hrg or datetime(year, month, month_start_day)
    if best_date_sharm is None:
        best_date_sharm = best_date
    if best_date_hrg is None:
        best_date_hrg = best_date

    logger.info(
        f"[BEST_DATE] ✅ Итог: "
        f"Шарм={best_date_sharm.strftime('%d.%m.%Y')} {best_price_sharm} EUR | "
        f"Хургада={best_date_hrg.strftime('%d.%m.%Y')} {best_price_hrg} EUR"
    )
    return {
        "price_per_person": min(best_price_sharm, best_price_hrg),
        "price_sharm":    best_price_sharm,
        "price_hurghada": best_price_hrg,
        "price_total":    min(best_price_sharm, best_price_hrg) * people,
        "source":  "travelmarket+flightpowers",
        "details": f"SSH={best_price_sharm} HRG={best_price_hrg} EUR/чел",
        "best_date":   best_date,
        "date_sharm":  best_date_sharm,
        "date_hrg":    best_date_hrg,
    }


def _flight_fallback(departure_city: str, country: str, people: int,
                     dep_date: datetime, nights: int) -> Dict:
    """Возвращает fallback цены на перелёт"""
    city_fb = FLIGHT_FALLBACK.get(departure_city, FLIGHT_FALLBACK["Copenhagen"])
    p = city_fb.get(country, 335)
    return {
        "price_per_person": p,
        "price_sharm":      p,
        "price_hurghada":   p,
        "price_total":      p * people,
        "source":           "fallback",
        "details":          f"fallback {p} EUR/чел",
    }


def search_flight(departure_city: str, country: str,
                  departure_date: datetime, nights: int,
                  people: int) -> Dict:
    """
    Ищет рейс:
      1. FlightPowers (Google Flights oneway) — если цена <= FP_MAX_REASONABLE_PRICE
      2. TravelMarket (чартеры, nFlexDays=28) — для Биллунда/Орхуса/Ольборга особенно важно
    Берёт меньшую из двух цен.
    Для Египта возвращает раздельные цены по Шарм/Хургада.

    Returns dict:
      price_per_person   – EUR/чел (oneway), лучшая цена
      price_sharm        – EUR/чел для Шарм (только egypt)
      price_hurghada     – EUR/чел для Хургада (только egypt)
      source             – 'flightpowers' / 'travelmarket' / 'fallback'
    """
    logger.info(f"[FLIGHT] ═══ Поиск рейса: {departure_city} → {country}, {departure_date.strftime('%d.%m.%Y')}")

    dep_date_str = departure_date.strftime("%Y-%m-%d")
    from_code = AIRPORT_CODES.get(departure_city, "CPH")

    # ── РЕЗУЛЬТАТЫ по направлению (для Египта) ────────────────────────────────
    fp_sharm    = None
    fp_hurghada = None
    tm_sharm    = None
    tm_hurghada = None
    fp_generic  = None
    tm_generic  = None

    # ── 1. FlightPowers ───────────────────────────────────────────────────────
    try:
        from flightpowers_parser import FlightPowersAPI
        fp_api = FlightPowersAPI()

        if country == "egypt":
            fp_flights = fp_api.search_egypt_flights(departure_city, dep_date_str, nights, people)
            max_price = FP_MAX_ONEWAY_EUR.get("egypt", 450)
            for f in fp_flights:
                dest = f.get("destination", "")
                p    = f.get("price_per_person_eur", 0)
                # Принимаем только разумные цены (не дороже cap)
                if p and 0 < p <= max_price:
                    if "Sharm" in dest:
                        fp_sharm = p if fp_sharm is None else min(fp_sharm, p)
                    elif "Hurghada" in dest:
                        fp_hurghada = p if fp_hurghada is None else min(fp_hurghada, p)
                elif p and p > max_price:
                    logger.warning(f"[FlightPowers] Цена {p} EUR для {dest} слишком высокая (cap={max_price}), игнорируем")
            logger.info(f"[FlightPowers] SSH={fp_sharm} EUR | HRG={fp_hurghada} EUR")

        elif country == "turkey":
            fp_flights = fp_api.search_turkey_flights(departure_city, dep_date_str, nights, people)
            max_price = FP_MAX_ONEWAY_EUR.get("turkey", 400)
            if fp_flights:
                prices = [f.get("price_per_person_eur", 0) for f in fp_flights
                          if 0 < f.get("price_per_person_eur", 0) <= max_price]
                if prices:
                    fp_generic = int(min(prices))
                    logger.info(f"[FlightPowers] Turkey: {fp_generic} EUR/чел")

        else:  # spain / tenerife / mallorca
            fp_flights = fp_api.search_spain_flights(departure_city, dep_date_str, nights, people)
            max_price = FP_MAX_ONEWAY_EUR.get("spain", 400)
            if fp_flights:
                prices = [f.get("price_per_person_eur", 0) for f in fp_flights
                          if 0 < f.get("price_per_person_eur", 0) <= max_price]
                if prices:
                    fp_generic = int(min(prices))
                    logger.info(f"[FlightPowers] Spain/Tenerife: {fp_generic} EUR/чел")

    except Exception as e:
        logger.warning(f"[FlightPowers] Ошибка: {e}")

    # ── 2. TravelMarket (только если FlightPowers не нашёл или нашёл дорого) ──
    # TravelMarket особенно важен для Биллунда/Орхуса/Ольборга (чартеры)
    need_tm = False
    if country == "egypt" and (fp_sharm is None or fp_hurghada is None):
        need_tm = True
    elif country in ("tenerife", "mallorca", "spain_barcelona", "turkey"):
        need_tm = True  # Эти направления — всегда ищем через TravelMarket (чартеры)
    elif country not in ("egypt",) and fp_generic is None:
        need_tm = True

    # Также всегда пробуем TravelMarket для сравнения если город — скандинавский не-CPH
    charter_cities = {"Billund", "Aarhus", "Aalborg", "Oslo", "Stockholm", "Goteborg", "Helsinki", "Tallinn"}
    if departure_city in charter_cities:
        need_tm = True

    if need_tm:
        logger.info(f"[TravelMarket] Запускаем поиск (departure={departure_city})")
        tm_result = _search_travelmarket_flight(departure_city, country, departure_date)
        tm_sharm    = tm_result.get("sharm")
        tm_hurghada = tm_result.get("hurghada")
        tm_generic  = tm_result.get("generic")
        if tm_sharm or tm_hurghada:
            logger.info(f"[TravelMarket] SSH={tm_sharm} EUR | HRG={tm_hurghada} EUR")
        elif tm_generic:
            logger.info(f"[TravelMarket] generic={tm_generic} EUR")

    # ── 3. Выбираем лучшую цену ───────────────────────────────────────────────
    if country == "egypt":
        # Для Шарм: берём минимум из FlightPowers и TravelMarket
        candidates_sharm    = [x for x in [fp_sharm, tm_sharm] if x and x > 0]
        candidates_hurghada = [x for x in [fp_hurghada, tm_hurghada] if x and x > 0]

        sharm_price    = int(min(candidates_sharm))    if candidates_sharm    else None
        hurghada_price = int(min(candidates_hurghada)) if candidates_hurghada else None

        # Если найдено только одно направление — дублируем цену на оба
        if sharm_price and not hurghada_price:
            hurghada_price = sharm_price
        elif hurghada_price and not sharm_price:
            sharm_price = hurghada_price

        # Если вообще ничего не найдено — используем fallback
        if not sharm_price and not hurghada_price:
            city_fb = FLIGHT_FALLBACK.get(departure_city, FLIGHT_FALLBACK["Copenhagen"])
            sharm_price    = city_fb.get("egypt", 335)
            hurghada_price = city_fb.get("egypt", 335)
            source_final = "fallback"
        else:
            # Для каждого направления выберем источник: FlightPowers или TravelMarket
            def choose_source(fp_val, tm_val):
                if fp_val and tm_val:
                    return "flightpowers" if fp_val <= tm_val else "travelmarket"
                if fp_val:
                    return "flightpowers"
                if tm_val:
                    return "travelmarket"
                return "fallback"

            src_sharm = choose_source(fp_sharm, tm_sharm)
            src_hrg   = choose_source(fp_hurghada, tm_hurghada)

            # Лучший источник для итоговой минимальной цены
            if sharm_price <= hurghada_price:
                source_final = src_sharm
            else:
                source_final = src_hrg

        best_price = int(min(sharm_price, hurghada_price))
        logger.info(f"[FLIGHT] ✅ Шарм={sharm_price} EUR | Хургада={hurghada_price} EUR | Источник: {source_final}")

        return {
            "price_per_person": best_price,
            "price_sharm":      int(sharm_price),
            "price_hurghada":   int(hurghada_price),
            "price_total":      best_price * people,
            "source":           source_final,
            "details":          f"SSH={sharm_price} HRG={hurghada_price} EUR/чел",
        }

    else:
        # Турция / Испания
        candidates = [x for x in [fp_generic, tm_generic] if x and x > 0]
        if candidates:
            best_price = int(min(candidates))
            source_final = "TravelMarket" if (tm_generic and min(candidates) == tm_generic) else "FlightPowers"
        else:
            city_fb = FLIGHT_FALLBACK.get(departure_city, FLIGHT_FALLBACK["Copenhagen"])
            best_price = city_fb.get(country, 350)
            source_final = "fallback"
            logger.warning(f"[FLIGHT] Fallback: {best_price} EUR/чел")

        logger.info(f"[FLIGHT] ✅ {country}: {best_price} EUR/чел ({source_final})")

        return {
            "price_per_person": best_price,
            "price_sharm":      best_price,
            "price_hurghada":   best_price,
            "price_total":      best_price * people,
            "source":           source_final,
            "details":          f"{from_code}→{country.upper()} {best_price} EUR/чел",
        }


# ─────────────────────────────────────────────────────────────────────────────
# СПИСКИ ОБЯЗАТЕЛЬНЫХ ОТЕЛЕЙ ЕГИПТА
# ─────────────────────────────────────────────────────────────────────────────

# Список обязательных отелей Шарм — используем ТОЛЬКО первичные имена (не синонимы)
# чтобы избежать ложных совпадений
REQUIRED_SHARM = [
    "The Grand Hotel Sharm El Sheikh",
    "Sharm Grand Plaza Resort",
    "Aurora Oriental Resort",
    "Parrotel Beach Resort",
    "Island View Resort",
    "Dreams Beach",
    "Amphoras Beach",
    "Naama Bay Hotel & Resort",
    "Jaz Mirabel Park",
    "Naama Bay Promenade Beach Resort",
    "Xperience Sea Breeze Resort",
    "Jaz Belvedere",
    "Jaz Mirabel Beach",
    "V Hotel Sharm El Sheikh",
    "Nubian Island",
    "Nubian Village",
    "Maritim Jolie Ville Resort & Casino",
    "Reef Oasis Blue Bay",
    "Park Regency Sharm El Sheikh",
    "Sunrise Grand Select Montemare Resort",
    "Sunrise Diamond Beach Resort",
    "Sultan Gardens Resort",
    "Grand Rotana Resort & Spa",
    "Savoy Sharm El Sheikh",
    "Stella Di Mare Beach Hotel & Spa",
    "Steigenberger Alcazar",
    "Rixos Premium Seagate",
    "Rixos Sharm El Sheikh",
    "Rixos Radamis",
    "Sunrise White Hills Resort",
    "Meraki Resort",
    "Sheraton Sharm",
    "Reef Oasis Beach Resort",
]

# Синонимы — alternate names для матчинга (отображение: alternate → canonical из списка)
SHARM_ALIASES = {
    "grand hotel sharm": "The Grand Hotel Sharm El Sheikh",
    "oriental resort": "Aurora Oriental Resort",
    "radisson blu resort sharm": "Parrotel Beach Resort",
    "sunrise island view": "Island View Resort",
    "tropitel naama bay": "Naama Bay Hotel & Resort",
    "jaz mirabel park & club": "Jaz Mirabel Park",
    "naama bay promenade": "Naama Bay Promenade Beach Resort",
    "pyramisa beach resort sharm": "V Hotel Sharm El Sheikh",
    "maritim jolie ville": "Maritim Jolie Ville Resort & Casino",
    "park regency sharm": "Park Regency Sharm El Sheikh",
    "hyatt regency ssh": "Park Regency Sharm El Sheikh",
    "sunrise grand select montemare": "Sunrise Grand Select Montemare Resort",
    "montemare resort": "Sunrise Grand Select Montemare Resort",
    "sultan gardens": "Sultan Gardens Resort",
    "grand rotana": "Grand Rotana Resort & Spa",
    "savoy sharm": "Savoy Sharm El Sheikh",
    "stella di mare beach": "Stella Di Mare Beach Hotel & Spa",
    "rixos premium seagate": "Rixos Premium Seagate",
    "rixos radamis": "Rixos Radamis",
    "rixos sharm el sheikh adults": "Rixos Sharm El Sheikh",
    "sunrise white hills": "Sunrise White Hills Resort",
    "meraki resort sharm": "Meraki Resort",
    "sheraton sharm main": "Sheraton Sharm",
    "reef oasis blue bay": "Reef Oasis Blue Bay",
    "reef oasis beach": "Reef Oasis Beach Resort",
    "sunrise diamond beach": "Sunrise Diamond Beach Resort",
    "sea breeze resort": "Xperience Sea Breeze Resort",
}

REQUIRED_HURGHADA = [
    "Rewaya Inn Resort",
    "Amwaj Beach Club Abu Soma",
    "Hawaii Paradise Aqua Park Resort",
    "Royal Lagoons Resort & Aqua Park",
    "Amc Royal Hotel",
    "Rewaya Majestic Resort",
    "Serenity Alpha Beach",
    "Titanic Beach Spa & Aqua Park",
    "Stella Makadi Beach Resort & Spa",
    "Titanic Palace Resort & Spa",
    "Desert Rose Resort",
    "Hilton Plaza",
    "The V Luxury Resort",
    "Sunrise Sentido Mamlouk Palace Resort",
    "Xanadu Makadi Bay",
    "Titanic Royal Hotel",
    "Sunrise Royal Makadi Resort",
    "Steigenberger Al Dau Beach",
    "Steigenberger Ras Soma",
    "Rixos Premium Magawish",
    "Serenity Alma Resort",
    "Movenpick Resort & Spa El Gouna",
    "Creek Hotel & Residences El Gouna",
    "Panorama Bungalows El Gouna",
    "Sheraton Miramar Resort",
    "Doubletree By Hilton Mangroovy El Gouna Resort",
]

HURGHADA_ALIASES = {
    "rewaya inn": "Rewaya Inn Resort",
    "hawaii paradise aqua park": "Hawaii Paradise Aqua Park Resort",
    "amwaj beach club": "Amwaj Beach Club Abu Soma",
    "pickalbatros beach club abu soma": "Amwaj Beach Club Abu Soma",
    "royal lagoons": "Royal Lagoons Resort & Aqua Park",
    "amc royal": "Amc Royal Hotel",
    "rewaya majestic": "Rewaya Majestic Resort",
    "serenity alpha beach": "Serenity Alpha Beach",
    "serenity makadi beach": "Serenity Alpha Beach",
    "titanic beach spa": "Titanic Beach Spa & Aqua Park",
    "stella makadi beach": "Stella Makadi Beach Resort & Spa",
    "titanic palace": "Titanic Palace Resort & Spa",
    "desert rose resort": "Desert Rose Resort",
    "hilton plaza": "Hilton Plaza",
    "v luxury resort": "The V Luxury Resort",
    "mamlouk palace": "Sunrise Sentido Mamlouk Palace Resort",
    "sentido mamlouk": "Sunrise Sentido Mamlouk Palace Resort",
    "xanadu makadi": "Xanadu Makadi Bay",
    "titanic royal": "Titanic Royal Hotel",
    "sunrise royal makadi": "Sunrise Royal Makadi Resort",
    "steigenberger al dau": "Steigenberger Al Dau Beach",
    "steigenberger ras soma": "Steigenberger Ras Soma",
    "rixos premium magawish": "Rixos Premium Magawish",
    "serenity alma": "Serenity Alma Resort",
    "serenity fun city": "Serenity Alma Resort",
    "movenpick resort & spa el gouna": "Movenpick Resort & Spa El Gouna",
    "movenpick el gouna": "Movenpick Resort & Spa El Gouna",
    "creek hotel & residences el gouna": "Creek Hotel & Residences El Gouna",
    "creek hotel el gouna": "Creek Hotel & Residences El Gouna",
    "panorama bungalows": "Panorama Bungalows El Gouna",
    "sheraton miramar": "Sheraton Miramar Resort",
    "doubletree mangroovy": "Doubletree By Hilton Mangroovy El Gouna Resort",
    "mangroovy el gouna": "Doubletree By Hilton Mangroovy El Gouna Resort",
}


# ─────────────────────────────────────────────────────────────────────────────
# СПИСОК ОБЯЗАТЕЛЬНЫХ ОТЕЛЕЙ ТУРЦИИ — полные названия (для совместимости)
# Основной список ключевых слов — REQUIRED_TURKEY выше (строка ~161)
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_TURKEY_FULL = [
    "Grand Kolibri Prestige & Spa",
    "Telatiye Resort Hotel",
    "Port River Hotel & Spa",
    "Lake & River Side Hotel & Spa",
    "Kahya Resort Aqua & Spa",
    "Alarcha Hotels & Resorts",
    "Kirbiyik Resort Hotel",
    "Crystal Paraiso Aqua Collection",
    "A Good Life Utopia Family Resort",
    "Crystal Admiral Aqua Collection",
    "Crystal Aura Aqua Collection",
    "Justiniano Deluxe Resort",
    "Vikingen Infinity Resort",
    "Utopia Beach Club",
    "Justiniano Club Park Conti",
    "Grand Park Lara",
    "Kaya Side",
    "Utopia Resort & Residence",
    "Noxinn Deluxe Hotel",
    "Quattro Beach Resort & Spa",
    "Crystal De Luxe Comfort Collection",
    "Fame Residence Kemer & Spa",
    "Gypsophila Holiday Village",
    "Megasaray Westbeach Antalya",
    "Baia Salima Kemer",
    "Loceanica Beach Resort Hotel",
    "Utopia World Hotel",
    "Quattro Family Club Dem Hotel",
    "Crystal Flora Pearl Collection",
    "Bosphorus Sorgun",
    "Crystal Prestige Pearl Collection",
    "Zena Resort Hotel",
    "Club Hotel Phaselis Rose",
    "Pirates Beach Club",
    "Sidera Kirman Premium",
    "Baia Lara Hotel",
    "Calyptus Kirman Premium",
    "Juju Premier Palace",
    "Leodikya Kirman Premium",
    "Sidemarin Kirman Premium",
    "Aqi Pegasos Resort",
    "Arycanda Kirman Premium",
    "Limak Limra Hotel & Resort",
    "Fame Residence Lara & Spa",
    "Rubi Platinum Spa Resort & Suites",
    "Kirman Belazur Resort & Spa",
    "Rixos Downtown Antalya",
    "Dobedan World Palace",
    "Megasaray Club Belek",
    "Nirvana Cosmopolitan",
    "Ic Hotels Green Palace & Villas",
    "Akka Antedon Hotel",
    "Mirage Park Resort",
    "Titanic Deluxe Golf Belek",
    "Nirvana Mediterranean Excellence",
    "Titanic Deluxe Lara",
    "Rixos Park Belek",
    "Kaya Palazzo Golf Resort",
    "Rixos Premium Tekirova",
    "Rixos Premium Belek",
    "Ng Phaselis Bay",
    "Cullinan Golf Resort Belek",
    "Maxx Royal Kemer Resort",
    "Papillon Ayscha Hotel",
    "Papillon Belvil Hotel",
    "Papillon Zeugma Relaxury",
    "Regnum Carya",
    "Titanic Mardan Palace",
    "Venezia Palace Deluxe Resort",
]

TURKEY_ALIASES = {
    "grand kolibri": "Grand Kolibri Prestige & Spa",
    "telatiye resort": "Telatiye Resort Hotel",
    "port river": "Port River Hotel & Spa",
    "lake & river": "Lake & River Side Hotel & Spa",
    "lake river side": "Lake & River Side Hotel & Spa",
    "kahya resort": "Kahya Resort Aqua & Spa",
    "alarcha": "Alarcha Hotels & Resorts",
    "kirbiyik resort": "Kirbiyik Resort Hotel",
    "dinler hotel": "Kirbiyik Resort Hotel",
    "crystal paraiso": "Crystal Paraiso Aqua Collection",
    "a good life utopia": "A Good Life Utopia Family Resort",
    "good life utopia": "A Good Life Utopia Family Resort",
    "crystal admiral": "Crystal Admiral Aqua Collection",
    "crystal aura": "Crystal Aura Aqua Collection",
    "justiniano deluxe": "Justiniano Deluxe Resort",
    "vikingen infinity": "Vikingen Infinity Resort",
    "utopia beach club": "Utopia Beach Club",
    "justiniano club park": "Justiniano Club Park Conti",
    "grand park lara": "Grand Park Lara",
    "kaya side": "Kaya Side",
    "utopia resort & residence": "Utopia Resort & Residence",
    "alara park": "Utopia Resort & Residence",
    "noxinn deluxe": "Noxinn Deluxe Hotel",
    "quattro beach": "Quattro Beach Resort & Spa",
    "crystal de luxe": "Crystal De Luxe Comfort Collection",
    "fame residence kemer": "Fame Residence Kemer & Spa",
    "gypsophila": "Gypsophila Holiday Village",
    "megasaray westbeach": "Megasaray Westbeach Antalya",
    "harrington park resort": "Megasaray Westbeach Antalya",
    "baia salima kemer": "Baia Salima Kemer",
    "baia kemer": "Baia Salima Kemer",
    "loceanica beach": "Loceanica Beach Resort Hotel",
    "utopia world": "Utopia World Hotel",
    "quattro family club": "Quattro Family Club Dem Hotel",
    "crystal flora": "Crystal Flora Pearl Collection",
    "bosphorus sorgun": "Bosphorus Sorgun",
    "crystal prestige": "Crystal Prestige Pearl Collection",
    "zena resort": "Zena Resort Hotel",
    "club hotel phaselis rose": "Club Hotel Phaselis Rose",
    "phaselis rose": "Club Hotel Phaselis Rose",
    "pirates beach club": "Pirates Beach Club",
    "sidera kirman": "Sidera Kirman Premium",
    "baia lara": "Baia Lara Hotel",
    "calyptus kirman": "Calyptus Kirman Premium",
    "juju premier": "Juju Premier Palace",
    "amara premier": "Juju Premier Palace",
    "leodikya kirman": "Leodikya Kirman Premium",
    "kirman leodikya": "Leodikya Kirman Premium",
    "sidemarin kirman": "Sidemarin Kirman Premium",
    "aqi pegasos resort": "Aqi Pegasos Resort",
    "arycanda kirman": "Arycanda Kirman Premium",
    "limak limra": "Limak Limra Hotel & Resort",
    "fame residence lara": "Fame Residence Lara & Spa",
    "rubi platinum": "Rubi Platinum Spa Resort & Suites",
    "kirman belazur": "Kirman Belazur Resort & Spa",
    "rixos downtown antalya": "Rixos Downtown Antalya",
    "dobedan world": "Dobedan World Palace",
    "alva donna world": "Dobedan World Palace",
    "megasaray club belek": "Megasaray Club Belek",
    "nirvana cosmopolitan": "Nirvana Cosmopolitan",
    "ic hotels green palace": "Ic Hotels Green Palace & Villas",
    "ic hotels residence": "Ic Hotels Green Palace & Villas",
    "akka antedon": "Akka Antedon Hotel",
    "mirage park resort": "Mirage Park Resort",
    "majesty mirage": "Mirage Park Resort",
    "titanic deluxe golf": "Titanic Deluxe Golf Belek",
    "nirvana mediterranean": "Nirvana Mediterranean Excellence",
    "titanic deluxe lara": "Titanic Deluxe Lara",
    "rixos park belek": "Rixos Park Belek",
    "kaya palazzo": "Kaya Palazzo Golf Resort",
    "rixos premium tekirova": "Rixos Premium Tekirova",
    "rixos premium belek": "Rixos Premium Belek",
    "ng phaselis": "Ng Phaselis Bay",
    "cullinan golf": "Cullinan Golf Resort Belek",
    "maxx royal kemer": "Maxx Royal Kemer Resort",
    "papillon ayscha": "Papillon Ayscha Hotel",
    "papillon belvil": "Papillon Belvil Hotel",
    "papillon zeugma": "Papillon Zeugma Relaxury",
    "regnum carya": "Regnum Carya",
    "titanic mardan": "Titanic Mardan Palace",
    "venezia palace": "Venezia Palace Deluxe Resort",
    # дополнительные алиасы для надёжного матча
    "akra kemer": "Akka Antedon Hotel",          # не тот отель — оставим для совместимости
    "baia lara": "Baia Lara Hotel",
    "barut collection": "Akka Antedon Hotel",    # AKRA KEMER BARUT → отдельный от белого списка
    "kahya resort": "Kahya Resort Aqua & Spa",
    "port river hotel": "Port River Hotel & Spa",
    "lake river side hotel": "Lake & River Side Hotel & Spa",
    "lake & river side hotel": "Lake & River Side Hotel & Spa",
    "grand kolibri prestige": "Grand Kolibri Prestige & Spa",
    "loceanica beach resort": "Loceanica Beach Resort Hotel",
    "quattro beach resort": "Quattro Beach Resort & Spa",
    "quattro family": "Quattro Family Club Dem Hotel",
    "bosphorus sorgun": "Bosphorus Sorgun",
    "rubi platinum spa": "Rubi Platinum Spa Resort & Suites",
    "kirman belazur": "Kirman Belazur Resort & Spa",
    "rixos downtown": "Rixos Downtown Antalya",
    "rixos park": "Rixos Park Belek",
    "rixos premium": "Rixos Premium Belek",
    "kaya palazzo golf": "Kaya Palazzo Golf Resort",
    "ng phaselis bay": "Ng Phaselis Bay",
    "cullinan golf resort": "Cullinan Golf Resort Belek",
    "maxx royal": "Maxx Royal Kemer Resort",
    "papillon ayscha hotel": "Papillon Ayscha Hotel",
    "papillon belvil hotel": "Papillon Belvil Hotel",
    "papillon zeugma": "Papillon Zeugma Relaxury",
    "regnum carya": "Regnum Carya",
    "titanic mardan palace": "Titanic Mardan Palace",
    "venezia palace deluxe": "Venezia Palace Deluxe Resort",
    "fame residence": "Fame Residence Kemer & Spa",
    "gypsophila holiday": "Gypsophila Holiday Village",
    "megasaray westbeach": "Megasaray Westbeach Antalya",
    "megasaray club": "Megasaray Club Belek",
    "limak limra hotel": "Limak Limra Hotel & Resort",
    "arycanda kirman": "Arycanda Kirman Premium",
    "sidemarin kirman": "Sidemarin Kirman Premium",
    "calyptus kirman": "Calyptus Kirman Premium",
    "sidera kirman": "Sidera Kirman Premium",
    "leodikya kirman": "Leodikya Kirman Premium",
    "aqi pegasos": "Aqi Pegasos Resort",
    "juju premier palace": "Juju Premier Palace",
    "dobedan world palace": "Dobedan World Palace",
    "ic hotels green": "Ic Hotels Green Palace & Villas",
    "titanic deluxe": "Titanic Deluxe Golf Belek",
    "nirvana mediterranean excellence": "Nirvana Mediterranean Excellence",
    "nirvana cosmopolitan": "Nirvana Cosmopolitan",
    "akka antedon": "Akka Antedon Hotel",
    "mirage park": "Mirage Park Resort",
    "club hotel phaselis": "Club Hotel Phaselis Rose",
    "pirates beach": "Pirates Beach Club",
    "zena resort hotel": "Zena Resort Hotel",
    "crystal prestige": "Crystal Prestige Pearl Collection",
    "crystal flora beach": "Crystal Flora Pearl Collection",
    "crystal de luxe": "Crystal De Luxe Comfort Collection",
    "crystal aura beach": "Crystal Aura Aqua Collection",
    "crystal admiral resort": "Crystal Admiral Aqua Collection",
    "crystal paraiso verde": "Crystal Paraiso Aqua Collection",
    "vikingen infinity": "Vikingen Infinity Resort",
    "justiniano deluxe resort": "Justiniano Deluxe Resort",
    "justiniano club": "Justiniano Club Park Conti",
    "utopia world hotel": "Utopia World Hotel",
    "utopia beach": "Utopia Beach Club",
    "utopia resort": "Utopia Resort & Residence",
    "noxinn deluxe": "Noxinn Deluxe Hotel",
    "grand park lara": "Grand Park Lara",
    "kaya side hotel": "Kaya Side",
    "alarcha hotels": "Alarcha Hotels & Resorts",
    "kirbiyik resort hotel": "Kirbiyik Resort Hotel",
    "telatiye resort hotel": "Telatiye Resort Hotel",
    "a good life utopia": "A Good Life Utopia Family Resort",
    "good life utopia family": "A Good Life Utopia Family Resort",
}


def _clean_name(name: str) -> str:
    """Нормализует название отеля для сравнения"""
    import re
    name = re.sub(r'\s*\(Ex\..*?\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*\(EX\..*?\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+adults only.*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*\d+\*', '', name)
    name = re.sub(r'[&/]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    return name.lower().strip()


def _sig_words_ts(name: str) -> list:
    """Значимые слова (≥4 символов) для сопоставления"""
    import re
    return [w for w in re.split(r'\W+', name.lower()) if len(w) >= 4]


def _is_in_list(hotel_name: str, required_list: list,
                aliases: Optional[dict] = None) -> bool:
    """
    Проверяет, входит ли отель в список обязательных.
    Расширенные правила:
      1. Точное совпадение нормализованных имён.
      2. По алиасам — ВСЕ значимые слова алиаса есть в API-названии.
      3. По белому списку:
         а) ВСЕ значимые слова (≥4 симв) эталона есть в API-названии (строгий).
         б) Если эталон ≥ 3 слов — достаточно первых 2 уникальных значимых слов (мягкий).
    """
    cleaned_api = _clean_name(hotel_name)
    sig_api     = _sig_words_ts(cleaned_api)
    sig_api_set = set(sig_api)
    len_api     = len(sig_api)

    # 1. Точное совпадение
    for req in required_list:
        if _clean_name(req) == cleaned_api:
            return True

    # 2. По алиасам — все значимые слова алиаса в API
    if aliases:
        for alias_key in aliases:
            alias_clean = _clean_name(alias_key)
            sig_alias   = _sig_words_ts(alias_clean)
            if len(sig_alias) < 2:
                continue
            if all(w in sig_api_set for w in sig_alias) and len_api >= len(sig_alias):
                return True

    # 3а. Строгое совпадение — все значимые слова эталона в API
    for req in required_list:
        cleaned_r = _clean_name(req)
        sig_r     = _sig_words_ts(cleaned_r)
        if len(sig_r) < 2:
            continue
        if all(w in sig_api_set for w in sig_r) and len_api >= len(sig_r):
            return True

    # 3б. Мягкое совпадение — первые 2 значимых слова эталона совпадают с API
    for req in required_list:
        cleaned_r = _clean_name(req)
        sig_r     = _sig_words_ts(cleaned_r)
        if len(sig_r) < 3:
            continue
        # Берём уникальные слова — пропускаем "hotel", "resort", "spa" и т.п.
        SKIP_WORDS = {"hotel", "resort", "beach", "club", "park", "suites",
                      "suite", "collection", "premium", "aqua"}
        key_words = [w for w in sig_r if w not in SKIP_WORDS]
        if len(key_words) >= 2:
            # Первые 2 ключевых слова должны быть в API-названии
            if all(w in sig_api_set for w in key_words[:2]):
                return True

    # 3в. Матч по уникальному бренду — если первое ключевое слово уникальное (длина ≥6)
    # и второе слово тоже совпадает → принимаем
    BRAND_WORDS = {"rixos", "papillon", "titanic", "nirvana", "maxx", "regnum",
                   "venezia", "limak", "bosphorus", "loceanica", "telatiye",
                   "alarcha", "kirbiyik", "gypsophila", "vikingen", "noxinn",
                   "quattro", "justiniano", "megasaray", "cullinan", "arycanda",
                   "sidemarin", "calyptus", "sidera", "leodikya", "dobedan",
                   "utopia", "crystal"}
    for req in required_list:
        cleaned_r = _clean_name(req)
        sig_r     = _sig_words_ts(cleaned_r)
        # Если первое значимое слово — уникальный бренд и он есть в API → принимаем
        if sig_r and sig_r[0] in BRAND_WORDS and sig_r[0] in sig_api_set:
            # Нужно ещё совпадение второго слова (если есть) чтобы избежать ложных
            if len(sig_r) >= 2:
                if sig_r[1] in sig_api_set:
                    return True
            else:
                return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# ШАГ 2: ПОИСК ОТЕЛЕЙ
# ─────────────────────────────────────────────────────────────────────────────

_MEAL_CODE_MAP = {
    # Реальные коды TourVisor API: RO=2, BB=3, HB=4, FB=5, AI=7, UAI=9
    2: 'RO', 3: 'BB', 4: 'HB', 5: 'FB', 7: 'AI', 9: 'UAI',
    '2': 'RO', '3': 'BB', '4': 'HB', '5': 'FB', '7': 'AI', '9': 'UAI',
}

def _meal_name(code) -> str:
    """Конвертирует числовой/строковый код питания TourVisor в название"""
    if not code:
        return ""
    if isinstance(code, str) and not code.isdigit():
        return code  # уже строковое название
    return _MEAL_CODE_MAP.get(code, str(code))


def search_hotels_tv(country_id: str, region_id: Optional[str],
                     date_from: str, nights: int, adults: int,
                     operators: str, destination_key: str,
                     children: int = 0) -> List[Dict]:
    """
    Запрашивает отели из TourVisor (без перелёта).
    Для Турции использует новый REST API (JWT).
    Для Египта фильтрует только по спискам REQUIRED_SHARM / REQUIRED_HURGHADA.
    Возвращает список dict: {name, stars, region, price_eur, meal, operator, tour_id, flydate}
    """

    # ── ТУРЦИЯ: используем новый REST API ────────────────────────────────────
    if destination_key == "turkey":
        children_ages = []
        if children > 0:
            children_ages = [9] * children
        return search_hotels_turkey_rest(
            date_from_str=date_from,
            nights=nights,
            adults=adults,
            children_ages=children_ages,
            whitelist=None,
            whitelist_aliases=None,
            top=50,
        )

    # ── ИСПАНИЯ / МАЙОРКА / ТЕНЕРИФЕ: используем новый REST API ─────────────
    if destination_key in ("spain_bcn", "mallorca", "tenerife", "spain"):
        children_ages = [9] * children if children > 0 else []
        _dest = destination_key if destination_key != "spain" else "spain_bcn"
        return search_hotels_spain_rest(
            date_from_str=date_from,
            nights=nights,
            adults=adults,
            children_ages=children_ages,
            destination_key=_dest,
            top=20,
        )

    usd_eur = _get_usd_eur_rate()

    params = {
        "departure":  "99",        # Без перелёта
        "country":    country_id,
        "datefrom":   date_from,
        "dateto":     date_from,
        "nightsfrom": nights,
        "nightsto":   nights,
        "adults":     adults,
        "operator":   operators,
        "currency":   1,           # USD → конвертируем в EUR сами (currency=2 = BYN!)
    }
    if children > 0:
        params["child"] = children

    # Снимаем operator из base params — установим индивидуально ниже
    params.pop("operator", None)

    def _extend_date(base_date_str: str, days: int = 28) -> str:
        try:
            from datetime import datetime as _dtt, timedelta as _tdd
            _d = _dtt.strptime(base_date_str, "%d.%m.%Y")
            return (_d + _tdd(days=days)).strftime("%d.%m.%Y")
        except Exception:
            return base_date_str

    if destination_key == "turkey_xml_unused":
        # Турция: запрашиваем 4★+ (API вернёт больше данных),
        # фильтрацию 5★ и AI делаем в Python по белому списку
        params["operator"]  = "125,13,11,90,23"   # Join UP, Anex, Coral, Kompas, Russian Express
        params["stars"]     = 4                    # 4+ — чтобы API вернул больше отелей
        params["regular"]   = 1
        params["onpage"]    = 300
        params["dateto"]    = _extend_date(date_from, 28)
        params.pop("meal", None)          # убираем фильтр питания — фильтруем сами
        params.pop("mealbetter", None)
        params.pop("region", None)
    elif destination_key == "tenerife":
        # Тенерифе: от 4★, HB+, регион 101
        params["region"]    = "101"
        params["stars"]     = 4                    # от 4★ (4★, 5★)
        params["operator"]  = "13,11,125,90,23"    # Anex, Coral, Join UP, Kompas, Russian Express
        params["regular"]   = 1
        params["onpage"]    = 200
        params["dateto"]    = _extend_date(date_from, 28)
        params.pop("meal", None)
        params.pop("mealbetter", None)
        params.pop("starsbetter", None)
        params.pop("rating", None)
    elif destination_key == "mallorca":
        # Майорка: от 3★, HB+, регион 102
        params["region"]    = region_id or "102"
        params["stars"]     = 3
        params["operator"]  = "13,11,125,90,23"    # + Russian Express
        params["regular"]   = 1
        params["onpage"]    = 200
        params["dateto"]    = _extend_date(date_from, 28)
        params.pop("meal", None)
        params.pop("mealbetter", None)
        params.pop("starsbetter", None)
    elif destination_key == "spain_bcn":
        # Коста Брава(69) + Коста Дель Маресме(70) + Коста Дорада(71): от 3★, HB+
        params["region"]    = "69,70,71"
        params["stars"]     = 3
        params["operator"]  = "13,11,125,90,23"    # + Russian Express
        params["regular"]   = 1
        params["onpage"]    = 200
        params["dateto"]    = _extend_date(date_from, 28)
        params.pop("meal", None)
        params.pop("mealbetter", None)
        params.pop("starsbetter", None)
    elif destination_key == "spain":
        params["region"]    = region_id or "69,70,71"
        params["stars"]     = 3
        params["operator"]  = "13,11,125,90,23"
        params["onpage"]    = 200
        params["dateto"]    = _extend_date(date_from, 28)
        params.pop("meal", None)
        params.pop("mealbetter", None)
        params.pop("starsbetter", None)
    else:
        # Египет — 5★ AI
        params["operator"]   = operators
        params["stars"]      = 5
        params["meal"]       = 7         # AI
        params["mealbetter"] = 7
        params["onpage"]     = 200
        if region_id:
            params["region"] = region_id

    logger.info(f"[TV] Запрос: country={country_id} region={region_id} date={date_from}")
    req = _tv_api("/search.php", params)

    if "error" in req or "result" not in req:
        logger.error(f"[TV] Ошибка запроса: {req}")
        return []

    request_id = req["result"]["requestid"]
    logger.info(f"[TV] RequestID: {request_id}")

    hotels_raw = []
    collected_ids = set()
    stable_count = 0
    prev_count = 0

    # Ждём результаты: до 60 попыток по 2 секунды (120 секунд)
    # Логика как в search_tenerife_hotels.py — собираем постепенно и ждём стабилизации
    for attempt in range(60):
        time.sleep(2)
        res = _tv_api("/result.php", {
            "requestid": request_id,
            "onpage": 200,
            "page": 1,
            "order": "price",
        })
        if not res:
            continue

        # API возвращает data.result для отелей и data.result.state для статуса
        data_result = res.get("data", {}).get("result", {})
        state  = data_result.get("state", "")
        found  = int(data_result.get("hotelsFound", 0))
        hotels = data_result.get("hotel", [])
        if isinstance(hotels, dict):
            hotels = [hotels]

        # Добавляем новые отели
        for h in hotels:
            hid = h.get("hotelcode")
            if hid and hid not in collected_ids:
                collected_ids.add(hid)
                hotels_raw.append(h)

        elapsed = (attempt + 1) * 2
        logger.info(f"[TV] {elapsed}с | state={state} | found={found} | collected={len(hotels_raw)}")

        if state == "finished":
            logger.info(f"[TV] Поиск завершён. Отелей: {found}")
            break

        # Стабилизация: если 5 итераций подряд количество не меняется — выходим
        if len(hotels_raw) == prev_count and len(hotels_raw) > 0:
            stable_count += 1
            if stable_count >= 5:
                logger.info(f"[TV] Данные стабильны {stable_count*2}с — завершаем сбор.")
                break
        else:
            stable_count = 0
        prev_count = len(hotels_raw)

    logger.info(f"[TV] Получено {len(hotels_raw)} отелей от API")

    # Определяем список обязательных отелей
    if destination_key == "egypt" and region_id == "6":
        required_list = REQUIRED_SHARM
        aliases_map = SHARM_ALIASES
    elif destination_key == "egypt" and region_id == "5":
        required_list = REQUIRED_HURGHADA
        aliases_map = HURGHADA_ALIASES
    elif destination_key == "turkey":
        required_list = REQUIRED_TURKEY
        aliases_map = TURKEY_ALIASES
    else:
        required_list = None   # Для Испании/Тенерифе/Майорки не фильтруем по списку
        aliases_map = {}

    # Операторы: для Тенерифе — Anex=13, Coral=11, Join UP=125, Kompas=90, Russian Express=23
    if destination_key == "tenerife":
        ALLOWED_OPS = {"13", "11", "125", "90", "23"}  # Anex, Coral, Join UP, Kompas, Russian Express
    else:
        ALLOWED_OPS = {"125", "13", "11", "90", "23"}  # Стандартные операторы

    # Коды питания HB и выше
    MEAL_ORDER = {"RO": 0, "BB": 1, "HB": 2, "FB": 3, "AI": 4, "UAI": 5}
    MEAL_CODES_STR = {
        "1": "RO", "2": "BB", "3": "HB", "4": "FB", "5": "AI", "6": "UAI",
        "ro": "RO", "bb": "BB", "hb": "HB", "fb": "FB", "ai": "AI", "uai": "UAI",
    }

    result = []
    seen_names = set()  # дедупликация
    for hotel in hotels_raw:
        hotel_name = hotel.get("hotelname", "")

        # Фильтр по спискам обязательных отелей (Египет + Турция)
        if required_list is not None:
            if not _is_in_list(hotel_name, required_list, aliases=aliases_map):
                continue

        # ── КЛЮЧЕВОЙ ФИЛЬТР для Тенерифе: только regioncode=101 ──
        if destination_key == "tenerife":
            rcode = int(hotel.get("regioncode", 0))
            if rcode != 101:
                logger.debug(f"[TV] Пропускаем {hotel_name} — regioncode={rcode} (не Тенерифе)")
                continue

        # ── ФИЛЬТР для Испании (Коста Брава/Маресме/Дорада): только regioncode 69, 70, 71 ──
        if destination_key == "spain_bcn":
            rcode = int(hotel.get("regioncode", 0))
            if rcode not in (69, 70, 71):
                logger.debug(f"[TV] Пропускаем {hotel_name} — regioncode={rcode} (не Коста Брава/Маресме/Дорада)")
                continue

        # ── ФИЛЬТР для Майорки: только regioncode=102 ──
        if destination_key == "mallorca":
            rcode = int(hotel.get("regioncode", 0))
            if rcode != 102:
                logger.debug(f"[TV] Пропускаем {hotel_name} — regioncode={rcode} (не Майорка)")
                continue

        # ── ФИЛЬТР для Турции: только 5 звёзд из белого списка ──
        if destination_key == "turkey":
            try:
                h_stars = int(hotel.get("hotelstars", 0))
            except Exception:
                h_stars = 0
            if h_stars < 5:
                logger.debug(f"[TV] Пропускаем {hotel_name} — {h_stars}★ (нужно 5★)")
                continue

        # Дедупликация по имени
        name_key = _clean_name(hotel_name)[:30]
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        tours = hotel.get("tours", {}).get("tour", [])
        if isinstance(tours, dict):
            tours = [tours]

        # Фильтр по операторам
        allowed = [t for t in tours if str(t.get("operatorcode", "")) in ALLOWED_OPS]

        if not allowed:
            continue

        # Таблица рангов питания: RO=0, BB=1, HB=2, FB=3, AI=4, UAI=5
        MEAL_ORDER_TV = {
            "RO": 0, "BB": 1, "HB": 2, "FB": 3, "AI": 4, "UAI": 5,
            "2": 0, "3": 1, "4": 2, "5": 3, "7": 4, "9": 5,
            2: 0, 3: 1, 4: 2, 5: 3, 7: 4, 9: 5,
        }
        def _meal_rank(t):
            m = t.get("meal", "")
            key = m.upper().strip() if isinstance(m, str) else m
            return MEAL_ORDER_TV.get(key, -1)

        if destination_key == "tenerife":
            # Тенерифе: предпочитаем HB и лучше, но если нет — берём что есть
            allowed_hb = [t for t in allowed if _meal_rank(t) >= 2]  # >= HB
            if allowed_hb:
                allowed = allowed_hb
            # Если нет HB+ — оставляем allowed как есть (не пропускаем отель)

        if destination_key in ("spain_bcn", "mallorca", "spain"):
            # Испания (Коста Брава/Дорада/Маресме/Майорка): минимум HB
            allowed_hb = [t for t in allowed if _meal_rank(t) >= 2]  # >= HB
            if allowed_hb:
                allowed = allowed_hb
            else:
                continue  # нет HB+ туров — пропускаем отель

        if destination_key in ("egypt", "turkey"):
            AI_MEALS_EG = {
                7, '7', 9, '9',
                'AI', 'UAI', 'All Inclusive', 'All inclusive', 'ALLINCLUSIVE',
                'all inclusive', 'AllInclusive',
            }
            allowed_ai_eg = [t for t in allowed if t.get("meal") in AI_MEALS_EG]
            if allowed_ai_eg:
                allowed = allowed_ai_eg
            # Для Турции: если AI туров нет — берём любые (не ломаем подборку)
            elif destination_key == "turkey":
                pass  # оставляем allowed как есть


        best = min(allowed, key=lambda t: t.get("priceue", 0) or t.get("price", 999999))

        # currency=1 → priceue = USD за номер → конвертируем в EUR
        price_usd = best.get("priceue", 0) or 0
        if not price_usd or price_usd <= 0:
            continue
        price_eur = round(price_usd * usd_eur)

        tour_id = best.get("tourid", "")
        tour_url = (f"https://tourvisor.ru/search.php#tvtourid={tour_id}"
                    if tour_id else f"https://tourvisor.ru/hotel/{hotel.get('hotelcode', '')}")

        logger.info(
            f"[TV] ✅ {hotel_name} [{hotel.get('regionname','')}]: "
            f"{price_usd} USD = {price_eur} EUR | "
            f"meal={best.get('meal','')} | op={best.get('operatorname','')} | "
            f"regioncode={hotel.get('regioncode','')}"
        )

        result.append({
            "name":      hotel_name,
            "stars":     hotel.get("hotelstars", 5),
            "region":    hotel.get("regionname", ""),
            "price_eur": price_eur,    # EUR за номер (за всех людей)
            "meal":      _meal_name(best.get("meal", "")),
            "operator":  best.get("operatorname", ""),
            "tour_id":   tour_id,
            "tour_url":  tour_url,
            "flydate":   best.get("flydate", date_from),
            "nights":    best.get("nights", nights),
        })

    result.sort(key=lambda x: x["price_eur"])
    logger.info(f"[TV] После фильтрации операторов: {len(result)} отелей")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# ШАГ 3: ФОРМАТИРОВАНИЕ ИТОГОВОГО СООБЩЕНИЯ
# ─────────────────────────────────────────────────────────────────────────────

def format_hotel_line(idx: int, h: dict, flight_pp: int, transfer: int, people: int, fallback_date: str = "") -> str:
    """Одна строка с отелем: итоговая цена за ВСЕХ (рейс + отель + трансфер).
    price_eur — цена отеля за НОМЕР (за всех людей, как возвращает TourVisor).
    flight_pp — цена рейса на 1 человека (oneway).
    """
    hotel_total_eur = h["price_eur"]              # EUR за номер (за всех)
    flight_total    = flight_pp * people           # рейс за всех
    total           = flight_total + hotel_total_eur + transfer
    flydate = h.get("flydate", "") or fallback_date
    logger.info(
        f"[RESULT] {idx}. {h['name']}: "
        f"рейс {flight_pp} EUR/чел x {people} = {flight_total} + отель {hotel_total_eur} + трансфер {transfer} = {total} EUR"
    )
    name      = h.get("name", "")
    stars     = h.get("stars", 5)
    region    = h.get("region", "")
    meal      = h.get("meal", "AI")
    stars_int = min(int(stars), 5)
    region_str = f" | 📍{region}" if region else ""
    return f"{idx}. {name} {'⭐'*stars_int}{region_str}\n   🍽 {meal} | 📅 {flydate} | 💰 {total} EUR\n\n"


def filter_no_stars_downgrade(hotels: List[Dict], flight_pp: int, transfer: int, people: int) -> List[Dict]:
    """
    Фильтрует список отелей: убирает ситуацию «понижение звёзд при росте цены».
    Правило: если есть более высокозвёздочный отель с МЕНЬШЕЙ или РАВНОЙ итоговой ценой,
    то менее звёздочный отель с такой же или большей ценой — не показываем.

    Пример: Best Delta 4★ за 1500 EUR → Vala Beach 3★ за 1520 EUR — убрать,
            потому что 3★ не должен стоить >= цены 4★.

    Алгоритм:
      1. Считаем итоговую цену (hotel_eur + flight*people + transfer) для каждого отеля.
      2. Для каждого уровня звёзд (5,4,3,2) находим минимальную итоговую цену.
      3. Убираем отели у которых итоговая цена >= минимальной итоговой цены
         любого отеля с БÓЛЬШИМ числом звёзд.
    """
    if not hotels:
        return hotels

    def total_price(h):
        return h["price_eur"] + flight_pp * people + transfer

    # Минимальная итоговая цена по каждой звёздности
    min_price_by_stars: Dict[int, int] = {}
    for h in hotels:
        stars = int(h.get("stars", 3) or 3)
        tp = total_price(h)
        if stars not in min_price_by_stars or tp < min_price_by_stars[stars]:
            min_price_by_stars[stars] = tp

    result = []
    for h in hotels:
        stars = int(h.get("stars", 3) or 3)
        tp = total_price(h)
        # Убираем отель ТОЛЬКО если более высокозвёздочный стоит СТРОГО ДЕШЕВЛЕ
        # (при равной цене — оставляем оба, не убиваем 4★ если 5★ стоит столько же)
        should_hide = False
        for higher_stars, higher_min_price in min_price_by_stars.items():
            if higher_stars > stars and higher_min_price < tp:
                should_hide = True
                logger.info(
                    f"[STARS_FILTER] Убран {h['name']} {stars}★ "
                    f"({tp} EUR) — есть {higher_stars}★ от {higher_min_price} EUR"
                )
                break
        if not should_hide:
            result.append(h)

    if len(result) < len(hotels):
        logger.info(
            f"[STARS_FILTER] Убрано {len(hotels) - len(result)} отелей "
            f"с нерациональным соотношением звёзд/цены"
        )
    return result


def build_egypt_messages(departure_city: str, flight: dict,
                          hurghada_hotels: List[dict], sharm_hotels: List[dict],
                          people: int, nights: int,
                          dep_date_hrg_str: str, dep_date_sharm_str: str = None,
                          top_per_region: int = 999,
                          tenerife_hotels: List[dict] = None,
                          tenerife_flight: dict = None,
                          dep_date_ten_str: str = None) -> List[str]:
    """Строит список сообщений для Египта (сначала Хургада, потом Шарм).
    Использует раздельные цены и даты перелёта для каждого направления.
    dep_date_hrg_str   — дата вылета на Хургаду
    dep_date_sharm_str — дата вылета на Шарм (если None — совпадает с Хургадой)
    """
    if dep_date_sharm_str is None:
        dep_date_sharm_str = dep_date_hrg_str

    fp_sharm    = flight.get("price_sharm",    flight["price_per_person"])
    fp_hurghada = flight.get("price_hurghada", flight["price_per_person"])
    transfer    = TRANSFER["egypt"]
    people_str  = f"{people} чел."

    # Применяем фильтр «не понижаем звёзды по цене»
    hurghada_hotels = filter_no_stars_downgrade(hurghada_hotels, fp_hurghada, transfer, people)
    sharm_hotels    = filter_no_stars_downgrade(sharm_hotels,    fp_sharm,    transfer, people)

    has_tenerife = bool(tenerife_hotels)
    header = (
        f"🏖 {nights} ночей ({people_str})\n"
        f"✈️ Вылет из {departure_city}\n\n"
    )

    # ── ХУРГАДА ПЕРВОЙ ──
    hurghada_block = f"━━━ 🌊 ХУРГАДА + ЭЛЬ ГУНА ━━━\n"
    hurghada_block += f"📅 Вылет: {dep_date_hrg_str}\n\n"
    idx = 0
    for h in hurghada_hotels[:top_per_region]:
        idx += 1
        hurghada_block += format_hotel_line(idx, h, fp_hurghada, transfer, people, fallback_date=dep_date_hrg_str)

    if not hurghada_hotels:
        hurghada_block += "Туры не найдены\n"

    # ── ШАРМ ВТОРЫМ ──
    sharm_block = f"\n━━━ 🏖 ШАРМ-ЭЛЬ-ШЕЙХ ━━━\n"
    sharm_block += f"📅 Вылет: {dep_date_sharm_str}\n\n"
    for h in sharm_hotels[:top_per_region]:
        idx += 1
        sharm_block += format_hotel_line(idx, h, fp_sharm, transfer, people, fallback_date=dep_date_sharm_str)

    if not sharm_hotels:
        sharm_block += "Туры не найдены\n"

    # ── ТЕНЕРИФЕ ТРЕТЬИМ (если есть) ──
    tenerife_block = ""
    if tenerife_hotels:
        ten_flight = tenerife_flight or flight
        fp_ten = ten_flight.get("price_per_person", flight["price_per_person"])
        transfer_ten = TRANSFER["tenerife"]
        ten_date_str = dep_date_ten_str or dep_date_hrg_str
        tenerife_hotels = filter_no_stars_downgrade(tenerife_hotels, fp_ten, transfer_ten, people)
        tenerife_block = f"\n━━━ 🌴 ТЕНЕРИФЕ (Испания) ━━━\n"
        tenerife_block += f"📅 Вылет: {ten_date_str}\n\n"
        for h in tenerife_hotels[:top_per_region]:
            idx += 1
            tenerife_block += format_hotel_line(idx, h, fp_ten, transfer_ten, people, fallback_date=ten_date_str)

    full_text = header + hurghada_block + sharm_block + tenerife_block
    return _split_message(full_text)


def build_country_messages(country_label: str, departure_city: str, flight: dict,
                            hotels: List[dict], people: int, nights: int,
                            dep_date_str: str, dest_key: str,
                            top: int = 50) -> List[str]:
    """Строит список сообщений для Турции или Испании"""
    fp = flight["price_per_person"]
    transfer = TRANSFER.get(dest_key, 490)

    # Применяем фильтр «не понижаем звёзды по цене»
    hotels = filter_no_stars_downgrade(hotels, fp, transfer, people)

    header = (
        f"🏖 {country_label} - {nights} ночей ({people} чел.)\n"
        f"✈️ Вылет из {departure_city} ~ {dep_date_str}\n\n"
    )

    body = ""
    for i, h in enumerate(hotels[:top], 1):
        body += format_hotel_line(i, h, fp, transfer, people, fallback_date=dep_date_str) + "\n"

    return _split_message(header + body)


# ─────────────────────────────────────────────────────────────────────────────
# ГЛАВНЫЙ КЛАСС
# ─────────────────────────────────────────────────────────────────────────────

class TourSelector:
    """
    Главный класс — принимает параметры от пользователя,
    ищет рейсы + отели, возвращает готовые строки сообщений.
    """

    def find_tours(
        self,
        departure_city: str,
        people: int,
        departure_date: datetime,
        nights: int = 7,
        is_winter: bool = True,   # True = до мая (Египет), False = лето (Турция/Испания)
        destination: Optional[str] = None,  # 'egypt'/'turkey'/'spain'/'antalya'/'mallorca' etc
        date_is_approximate: bool = False,  # True = только месяц указан, ищем лучшую дату
        adults: Optional[int] = None,       # Явное число взрослых (если None — берём из people)
        children: int = 0,                  # Число детей
    ) -> List[str]:
        """
        Основной метод подбора туров.
        Если date_is_approximate=True — ищет лучшую дату за весь месяц.
        adults/children передаются раздельно в TourVisor API.
        Возвращает список строк — готовых сообщений для отправки в Facebook.
        """
        # Разбираем состав группы
        if adults is None:
            # Старый вызов: people = всего людей (взр+дети вместе)
            _adults   = people - children
            _children = children
        else:
            _adults   = adults
            _children = children
            people    = adults + children  # итого для расчёта рейса/трансфера
        month = departure_date.month
        year  = departure_date.year

        # Уточняем сезон
        # Если destination явно задан как НЕ египетское направление — всегда лето
        SUMMER_DEST = {"tenerife", "spain", "mallorca", "barcelona", "turkey", "antalya"}
        if destination in SUMMER_DEST:
            season_winter = False
        elif month < 5 or month >= 11:
            # Январь–апрель, ноябрь–декабрь → Египет (Шарм + Хургада + Тенерифе)
            season_winter = True
        elif month >= 5:
            # Май–октябрь → Лето: Турция + Барселона + Майорка
            season_winter = False
        else:
            season_winter = is_winter

        logger.info(
            f"[SELECTOR] 🔍 {departure_city}, {people} чел., "
            f"{'~' if date_is_approximate else ''}{departure_date.strftime('%d.%m.%Y')}, "
            f"{'ЗИМА→Египет' if season_winter else 'ЛЕТО→Турция/Испания'}"
        )

        country_key = "egypt" if season_winter else (
            "turkey" if destination in ("turkey", "antalya") else "spain"
        )

        # Если дата приблизительная — ищем лучшую дату через TravelMarket
        prefetched_flight = None
        date_sharm = None
        date_hrg   = None
        if date_is_approximate and season_winter:
            logger.info(f"[SELECTOR] Дата приблизительная — ищем лучшую дату в {month:02d}/{year}")
            try:
                best_result = find_best_flight_date(departure_city, country_key, month, year, nights, people)
                actual_date = best_result.get("best_date", departure_date)
                date_sharm  = best_result.get("date_sharm", actual_date)
                date_hrg    = best_result.get("date_hrg",   actual_date)
                prefetched_flight = {k: v for k, v in best_result.items() if k not in ("best_date", "date_sharm", "date_hrg")}
                logger.info(f"[SELECTOR] Лучшая дата: Шарм={date_sharm.strftime('%d.%m.%Y') if date_sharm else '?'} Хургада={date_hrg.strftime('%d.%m.%Y') if date_hrg else '?'} | Рейс: {best_result.get('price_per_person')} EUR/чел")
            except Exception as e:
                logger.warning(f"[SELECTOR] Ошибка поиска лучшей даты: {e} — используем {departure_date.strftime('%d.%m.%Y')}")
                actual_date = departure_date
        elif date_is_approximate and not season_winter and destination not in (None, "tenerife"):
            # Летние направления (Турция/Майорка/Испания): ищем лучшую дату через TravelMarket + FlightPowers
            logger.info(f"[SELECTOR] {destination}, дата приблизительная — ищем лучшую дату в {month:02d}/{year}")
            import calendar as _cal_summer
            _, last_day_sum = _cal_summer.monthrange(year, month)
            from datetime import date as _date_sum
            today_sum = datetime.now().date()
            ms_day_sum = 1
            for _d in range(1, last_day_sum + 1):
                if _date_sum(year, month, _d) >= today_sum + timedelta(days=3):
                    ms_day_sum = _d
                    break
            ms_str_sum = datetime(year, month, ms_day_sum).strftime("%Y-%m-%d")

            # Определяем направление
            if destination in ("turkey", "antalya"):
                dest_tm = "turkey"
                fp_dest_code = "AYT"
            elif destination == "mallorca":
                dest_tm = "mallorca"
                fp_dest_code = "PMI"
            else:  # spain, barcelona
                dest_tm = "spain_barcelona"
                fp_dest_code = "BCN"

            _max_fp_sum = FP_MAX_ONEWAY_EUR.get("turkey" if dest_tm == "turkey" else "spain", 320)
            best_d_sum = None
            best_p_sum = None  # лучшая цена из TravelMarket
            fp_p_sum   = None  # цена FlightPowers

            # TravelMarket
            try:
                parser = _get_tm_parser()
                if dest_tm == "turkey":
                    url_sum = parser.build_url_turkey(departure_city, ms_str_sum, flex_days=28)
                elif dest_tm == "mallorca":
                    url_sum = parser.build_url_mallorca(departure_city, ms_str_sum, flex_days=28)
                else:
                    url_sum = parser.build_url_spain_barcelona(departure_city, ms_str_sum, flex_days=28)
                logger.info(f"[SELECTOR] TravelMarket {dest_tm} URL: {url_sum[:100]}...")
                flights_sum = _tm_parse(parser, url_sum) or []
                if flights_sum:
                    try:
                        r_dkk_sum = requests.get("https://api.exchangerate-api.com/v4/latest/DKK", timeout=5)
                        dkk_eur_sum = r_dkk_sum.json().get("rates", {}).get("EUR", 1/7.47)
                    except Exception:
                        dkk_eur_sum = 1/7.47
                    for f in flights_sum:
                        dep_raw = f.get("departure_date", "")
                        p_dkk = f.get("price_per_person_dkk") or 0
                        if p_dkk and dep_raw:
                            p_eur = round(float(p_dkk) * dkk_eur_sum)
                            if best_p_sum is None or p_eur < best_p_sum:
                                best_p_sum = p_eur
                                for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
                                    try:
                                        best_d_sum = datetime.strptime(dep_raw, fmt)
                                        break
                                    except Exception:
                                        pass
                    if best_p_sum:
                        logger.info(f"[SELECTOR] TravelMarket {dest_tm}: {best_p_sum} EUR/чел ({best_d_sum.strftime('%d.%m.%Y') if best_d_sum else '?'})")
            except Exception as e:
                logger.warning(f"[SELECTOR] TravelMarket {destination} ошибка: {e}")

            # FlightPowers — ищем на лучшую дату TravelMarket (если нашли), иначе перебираем
            try:
                from flightpowers_parser import FlightPowersAPI
                fp_api2 = FlightPowersAPI()
                from_code_sum = AIRPORT_CODES.get(departure_city, "CPH")
                # Собираем даты: сначала лучшая из TravelMarket, потом равномерные точки
                import calendar as _cal_s2
                _, _last_s2 = _cal_s2.monthrange(year, month)
                from datetime import date as _d_cls2
                _today_s2 = datetime.now().date()
                _sample_s2 = []
                if best_d_sum:
                    _sample_s2.append(best_d_sum.strftime("%Y-%m-%d"))
                for _day in [1, 8, 15, 22]:
                    try:
                        _d2 = _d_cls2(year, month, min(_day, _last_s2))
                        if _d2 >= _today_s2 + timedelta(days=3):
                            _s2 = _d2.strftime("%Y-%m-%d")
                            if _s2 not in _sample_s2:
                                _sample_s2.append(_s2)
                    except Exception:
                        pass
                _sample_s2 = _sample_s2[:3]  # не более 3 дат
                for _dep_s2 in _sample_s2:
                    _ret_s2 = (datetime.strptime(_dep_s2, "%Y-%m-%d") + timedelta(days=nights)).strftime("%Y-%m-%d")
                    try:
                        res_fp_sum = fp_api2.search_roundtrip(from_code_sum, fp_dest_code, _dep_s2, _ret_s2, adults=people)
                        if res_fp_sum:
                            p_fp_sum_candidate = fp_api2.parse_flight_price(res_fp_sum, adults=people)
                            if p_fp_sum_candidate and 0 < p_fp_sum_candidate <= _max_fp_sum:
                                p_fp_sum_candidate = round(p_fp_sum_candidate)
                                if fp_p_sum is None or p_fp_sum_candidate < fp_p_sum:
                                    fp_p_sum = p_fp_sum_candidate
                                    logger.info(f"[SELECTOR] FlightPowers {dest_tm} ({_dep_s2}): {fp_p_sum} EUR/чел")
                    except Exception as _fe2:
                        logger.debug(f"[SELECTOR] FlightPowers {destination} {_dep_s2} ошибка: {_fe2}")
            except Exception as e:
                logger.debug(f"[SELECTOR] FlightPowers {destination} ошибка: {e}")

            # Выбираем лучшую цену
            candidates_sum = [p for p in [best_p_sum, fp_p_sum] if p and p > 0]
            if candidates_sum:
                final_p_sum = min(candidates_sum)
                src_sum = "FlightPowers" if (fp_p_sum and final_p_sum == fp_p_sum and (best_p_sum is None or fp_p_sum < best_p_sum)) else "TravelMarket"
                logger.info(f"[SELECTOR] {dest_tm} итог: TM={best_p_sum} FP={fp_p_sum} → лучший={final_p_sum} ({src_sum})")
                # Если TravelMarket нашёл дату — используем её; иначе начало месяца
                actual_date = best_d_sum if best_d_sum else datetime(year, month, ms_day_sum)
                prefetched_flight = {
                    "price_per_person": final_p_sum, "price_sharm": final_p_sum,
                    "price_hurghada": final_p_sum, "price_total": final_p_sum * people,
                    "source": src_sum, "details": f"{dest_tm} {final_p_sum} EUR/чел",
                }
            else:
                actual_date = departure_date

        elif date_is_approximate and destination == "tenerife":
            # Тенерифе: ищем лучшую дату через TravelMarket + FlightPowers
            logger.info(f"[SELECTOR] Тенерифе, дата приблизительная — ищем лучшую дату в {month:02d}/{year}")
            import calendar
            _, last_day = calendar.monthrange(year, month)
            today = datetime.now().date()
            month_start_day = 1
            from datetime import date as _date
            for d in range(1, last_day + 1):
                if _date(year, month, d) >= today + timedelta(days=3):
                    month_start_day = d
                    break
            month_start_str = datetime(year, month, month_start_day).strftime("%Y-%m-%d")

            best_d_ten = None
            best_p_ten = None  # TravelMarket цена
            fp_p_ten   = None  # FlightPowers цена

            # TravelMarket
            try:
                parser = _get_tm_parser()
                url_ten = parser.build_url_tenerife(departure_city, month_start_str, flex_days=28)
                logger.info(f"[SELECTOR] TravelMarket Тенерифе URL: {url_ten[:100]}...")
                flights_ten = _tm_parse(parser, url_ten) or []
                if flights_ten:
                    try:
                        r_dkk = requests.get("https://api.exchangerate-api.com/v4/latest/DKK", timeout=5)
                        dkk_eur = r_dkk.json().get("rates", {}).get("EUR", 1/7.47)
                    except Exception:
                        dkk_eur = 1/7.47
                    for f in flights_ten:
                        dep_raw = f.get("departure_date", "")
                        p_dkk = f.get("price_per_person_dkk") or 0
                        if p_dkk and dep_raw:
                            p_eur = round(float(p_dkk) * dkk_eur)
                            if best_p_ten is None or p_eur < best_p_ten:
                                best_p_ten = p_eur
                                for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
                                    try:
                                        best_d_ten = datetime.strptime(dep_raw, fmt)
                                        break
                                    except Exception:
                                        pass
                    if best_p_ten:
                        logger.info(f"[SELECTOR] TravelMarket Тенерифе: {best_p_ten} EUR/чел ({best_d_ten.strftime('%d.%m.%Y') if best_d_ten else '?'})")
            except Exception as e:
                logger.warning(f"[SELECTOR] TravelMarket Тенерифе ошибка: {e}")

            # FlightPowers: TFS roundtrip
            try:
                from flightpowers_parser import FlightPowersAPI
                fp_api_ten = FlightPowersAPI()
                from_code_ten = AIRPORT_CODES.get(departure_city, "CPH")
                fp_ret_str_ten = (datetime(year, month, month_start_day) + timedelta(days=nights)).strftime("%Y-%m-%d")
                res_tfs = fp_api_ten.search_roundtrip(from_code_ten, "TFS", month_start_str, fp_ret_str_ten)
                if res_tfs:
                    p_tfs = fp_api_ten.parse_flight_price(res_tfs, adults=people)
                    if p_tfs and 0 < p_tfs <= FP_MAX_ONEWAY_EUR.get("spain", 320):
                        fp_p_ten = round(p_tfs)
                        logger.info(f"[SELECTOR] FlightPowers Тенерифе: {fp_p_ten} EUR/чел")
            except Exception as e:
                logger.debug(f"[SELECTOR] FlightPowers Тенерифе ошибка: {e}")

            # Берём лучшую цену
            candidates_ten = [p for p in [best_p_ten, fp_p_ten] if p and p > 0]
            if candidates_ten:
                final_p_ten = min(candidates_ten)
                src_ten = "FlightPowers" if (fp_p_ten and final_p_ten == fp_p_ten and (best_p_ten is None or fp_p_ten < best_p_ten)) else "TravelMarket"
                logger.info(f"[SELECTOR] Тенерифе итог: TM={best_p_ten} FP={fp_p_ten} → {final_p_ten} ({src_ten})")
                actual_date = best_d_ten if best_d_ten else datetime(year, month, month_start_day)
                prefetched_flight = {
                    "price_per_person": final_p_ten,
                    "price_sharm": final_p_ten,
                    "price_hurghada": final_p_ten,
                    "price_total": final_p_ten * people,
                    "source": src_ten,
                    "details": f"Тенерифе {final_p_ten} EUR/чел",
                }
            else:
                actual_date = departure_date
        else:
            actual_date = departure_date

        dep_date_str  = actual_date.strftime("%d.%m.%Y")
        tv_date_str   = actual_date.strftime("%d.%m.%Y")

        # ── ЗИМА: Египет ──────────────────────────────────────────────────────
        if season_winter:
            return self._egypt_tours(departure_city, people, actual_date,
                                     nights, tv_date_str, dep_date_str,
                                     prefetched_flight=prefetched_flight,
                                     date_sharm=date_sharm, date_hrg=date_hrg,
                                     children=_children)

        # ── ЛЕТО: конкретное направление ──────────────────────────────────────
        if destination in ("turkey", "antalya"):
            return self._turkey_tours(departure_city, people, actual_date,
                                      nights, tv_date_str, dep_date_str,
                                      children=_children)
        if destination in ("spain", "barcelona", "mallorca", "tenerife"):
            dest_key = "mallorca" if destination == "mallorca" else (
                "tenerife" if destination == "tenerife" else "spain"
            )
            if destination == "tenerife":
                return self._tenerife_tours(departure_city, people, actual_date,
                                           nights, tv_date_str, dep_date_str,
                                           prefetched_flight=prefetched_flight,
                                           children=_children)
            else:
                return self._spain_tours(departure_city, people, actual_date,
                                        nights, tv_date_str, dep_date_str, dest_key,
                                        children=_children)

        # Без конкретного направления — все летние: Турция + Барселона/Коста Брава + Майорка
        return self._summer_all(departure_city, people, actual_date,
                                nights, tv_date_str, dep_date_str, children=_children)

    # ── ЕГИПЕТ ────────────────────────────────────────────────────────────────
    def _egypt_tours(self, city, people, dep_date, nights, tv_date, dep_date_str,
                     prefetched_flight: Optional[Dict] = None,
                     date_sharm: Optional[datetime] = None,
                     date_hrg:   Optional[datetime] = None,
                     children: int = 0):
        # Рейс — FlightPowers + TravelMarket (или уже найденный)
        if prefetched_flight:
            flight = prefetched_flight
            logger.info(f"[EGYPT] ✈️ Используем найденную цену (best date search)")
        else:
            flight = search_flight(city, "egypt", dep_date, nights, people)
        fp_sharm    = flight.get("price_sharm",    flight["price_per_person"])
        fp_hurghada = flight.get("price_hurghada", flight["price_per_person"])
        transfer    = TRANSFER["egypt"]

        # Определяем даты для каждого направления
        tv_date_sharm = date_sharm.strftime("%d.%m.%Y") if date_sharm else tv_date
        tv_date_hrg   = date_hrg.strftime("%d.%m.%Y")   if date_hrg   else tv_date
        dep_date_sharm_str = tv_date_sharm
        dep_date_hrg_str   = tv_date_hrg

        logger.info(
            f"[EGYPT] ✈️ Шарм={fp_sharm} EUR/чел ({dep_date_sharm_str}) | "
            f"Хургада={fp_hurghada} EUR/чел ({dep_date_hrg_str}) | источник: {flight.get('source')}"
        )

        # Отели Хургада — фильтруем по REQUIRED_HURGHADA
        logger.info(f"[EGYPT] Ищем Хургаду + Эль Гуна ({tv_date_hrg})...")
        hurghada = search_hotels_tv(
            country_id="1", region_id="5",
            date_from=tv_date_hrg, nights=nights, adults=people - children,
            operators=TV_OPS_EGYPT, destination_key="egypt",
            children=children
        )
        logger.info(f"[EGYPT] Хургада: всего {len(hurghada)}, обязательных: {len(hurghada)}")
        for i, h in enumerate(hurghada, 1):
            total = fp_hurghada * people + h["price_eur"] + transfer
            logger.info(f"[EGYPT] Хургада {i}. {h['name']}: рейс {fp_hurghada}/чел × {people} + отель {h['price_eur']} + трансфер {transfer} = {total} EUR")

        # Отели Шарм — фильтруем по REQUIRED_SHARM
        logger.info(f"[EGYPT] Ищем Шарм-Эль-Шейх ({tv_date_sharm})...")
        sharm = search_hotels_tv(
            country_id="1", region_id="6",
            date_from=tv_date_sharm, nights=nights, adults=people - children,
            operators=TV_OPS_EGYPT, destination_key="egypt",
            children=children
        )
        logger.info(f"[EGYPT] Шарм: всего {len(sharm)}, обязательных: {len(sharm)}")
        for i, h in enumerate(sharm, 1):
            total = fp_sharm * people + h["price_eur"] + transfer
            logger.info(f"[EGYPT] Шарм {i}. {h['name']}: рейс {fp_sharm}/чел × {people} + отель {h['price_eur']} + трансфер {transfer} = {total} EUR")

        logger.info(f"[EGYPT] ✅ Итого: Хургада={len(hurghada)}, Шарм={len(sharm)}, готово к отправке!")

        # ── ТЕНЕРИФЕ — ищем лучшую дату + рейс + отели ──
        tenerife = []
        tenerife_flight_res = None
        dep_date_ten_str = dep_date_sharm_str  # по умолчанию та же дата
        best_ten_date = None
        try:
            logger.info(f"[TENERIFE] Ищем лучший рейс на Тенерифе из {city}...")
            parser = _get_tm_parser()
            month_ten = dep_date.month
            year_ten  = dep_date.year
            import calendar as _cal
            _, last_day_ten = _cal.monthrange(year_ten, month_ten)
            from datetime import date as _date_cls
            today_d = datetime.now().date()
            ms_day = 1
            for d in range(1, last_day_ten + 1):
                if _date_cls(year_ten, month_ten, d) >= today_d + timedelta(days=3):
                    ms_day = d
                    break
            ms_str = datetime(year_ten, month_ten, ms_day).strftime("%Y-%m-%d")
            url_ten = parser.build_url_tenerife(city, ms_str, flex_days=28)
            logger.info(f"[TENERIFE] TravelMarket URL: {url_ten[:100]}...")
            flights_ten = _tm_parse(parser, url_ten) or []
            if flights_ten:
                try:
                    r_dkk2 = requests.get("https://api.exchangerate-api.com/v4/latest/DKK", timeout=5)
                    dkk_eur2 = r_dkk2.json().get("rates", {}).get("EUR", 1/7.47)
                except Exception:
                    dkk_eur2 = 1/7.47
                best_ten_price = None
                for f in flights_ten:
                    dep_raw = f.get("departure_date", "")
                    p_dkk = f.get("price_per_person_dkk") or 0
                    if p_dkk and dep_raw:
                        p_eur = round(float(p_dkk) * dkk_eur2)
                        if best_ten_price is None or p_eur < best_ten_price:
                            best_ten_price = p_eur
                            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
                                try:
                                    best_ten_date = datetime.strptime(dep_raw, fmt)
                                    break
                                except Exception:
                                    pass

                # Сравниваем с FlightPowers
                try:
                    from flightpowers_parser import FlightPowersAPI
                    _fp_ten_api = FlightPowersAPI()
                    _from_ten = AIRPORT_CODES.get(city, "CPH")
                    _fp_ten_ret = (datetime(year_ten, month_ten, ms_day) + timedelta(days=nights)).strftime("%Y-%m-%d")
                    _fp_ten_res = _fp_ten_api.search_roundtrip(_from_ten, "TFS", ms_str, _fp_ten_ret)
                    if _fp_ten_res:
                        _fp_ten_p = _fp_ten_api.parse_flight_price(_fp_ten_res, adults=people)
                        if _fp_ten_p and 0 < _fp_ten_p <= FP_MAX_ONEWAY_EUR.get("spain", 320):
                            _fp_ten_p = round(_fp_ten_p)
                            logger.info(f"[TENERIFE] FlightPowers: {_fp_ten_p} EUR/чел (TM={best_ten_price})")
                            if best_ten_price is None or _fp_ten_p < best_ten_price:
                                best_ten_price = _fp_ten_p
                                logger.info(f"[TENERIFE] FlightPowers дешевле → {best_ten_price} EUR/чел")
                except Exception as _e:
                    logger.debug(f"[TENERIFE] FlightPowers ошибка: {_e}")

                if best_ten_date and best_ten_price:
                    dep_date_ten_str = best_ten_date.strftime("%d.%m.%Y")
                    tenerife_flight_res = {
                        "price_per_person": best_ten_price,
                        "price_sharm": best_ten_price,
                        "price_hurghada": best_ten_price,
                        "price_total": best_ten_price * people,
                        "source": "travelmarket",
                        "details": f"Тенерифе {best_ten_price} EUR/чел",
                    }
                    logger.info(f"[TENERIFE] ✅ Лучший рейс: {dep_date_ten_str} — {best_ten_price} EUR/чел")
                else:
                    tenerife_flight_res = search_flight(city, "tenerife", dep_date, nights, people)
                    dep_date_ten_str = dep_date_sharm_str
            else:
                tenerife_flight_res = search_flight(city, "tenerife", dep_date, nights, people)
                dep_date_ten_str = dep_date_sharm_str

            tv_date_ten = dep_date_ten_str
            logger.info(f"[TENERIFE] Ищем отели (4*+, HB+) на {tv_date_ten}...")
            tenerife = search_hotels_tv(
                country_id="14", region_id="101",
                date_from=tv_date_ten, nights=nights, adults=people,
                operators="13,11,125,90,23", destination_key="tenerife",
                children=children
            )
            logger.info(f"[TENERIFE] Найдено: {len(tenerife)} отелей")
        except Exception as e:
            logger.warning(f"[TENERIFE] Ошибка при поиске Тенерифе: {e}")

        # Показываем максимум 20 отелей на регион
        return build_egypt_messages(city, flight, hurghada, sharm, people, nights,
                                    dep_date_hrg_str, dep_date_sharm_str, top_per_region=20,
                                    tenerife_hotels=tenerife,
                                    tenerife_flight=tenerife_flight_res,
                                    dep_date_ten_str=dep_date_ten_str)

    # ── ТУРЦИЯ ────────────────────────────────────────────────────────────────
    def _turkey_tours(self, city, people, dep_date, nights, tv_date, dep_date_str,
                      prefetched_flight: Optional[Dict] = None, children: int = 0):
        if prefetched_flight:
            flight = prefetched_flight
        else:
            flight = search_flight(city, "turkey", dep_date, nights, people)

        logger.info("[TURKEY] Ищем отели...")
        hotels = search_hotels_tv(
            country_id="4", region_id=None,
            date_from=tv_date, nights=nights, adults=people - children,
            operators=TV_OPS_TURKEY, destination_key="turkey",
            children=children
        )

        logger.info(f"[TURKEY] ✅ Найдено: {len(hotels)} отелей")
        return build_country_messages("ТУРЦИЯ", city, flight, hotels,
                                      people, nights, dep_date_str, "turkey", top=20)

    # ── ИСПАНИЯ / ТЕНЕРИФЕ ────────────────────────────────────────────────────
    def _spain_tours(self, city, people, dep_date, nights, tv_date, dep_date_str,
                     dest_key="spain", prefetched_flight: Optional[Dict] = None,
                     children: int = 0):
        if prefetched_flight:
            flight = prefetched_flight
        else:
            flight = search_flight(city, "spain", dep_date, nights, people)

        if dest_key == "tenerife":
            # Тенерифе: регион 101, 4*+, HB+, рейтинг 3.5+
            logger.info("[TENERIFE] Ищем отели Тенерифе (region=101, 4*+, HB+, рейтинг 3.5+)...")
            hotels = search_hotels_tv(
                country_id="14", region_id="101",
                date_from=tv_date, nights=nights, adults=people,
                operators="13,11,125,90,23", destination_key="tenerife",
                children=children
            )
            label = "ТЕНЕРИФЕ 🌴"
        else:
            logger.info("[SPAIN] Ищем отели...")
            if dest_key == "mallorca":
                spain_region = "102"
                spain_dest   = "mallorca"
            else:
                spain_region = "69,70,71"
                spain_dest   = "spain_bcn"
            hotels = search_hotels_tv(
                country_id="14", region_id=spain_region,
                date_from=tv_date, nights=nights, adults=people,
                operators=TV_OPS_SPAIN, destination_key=spain_dest,
                children=children
            )
            label = {"mallorca": "МАЙОРКА"}.get(dest_key, "ИСПАНИЯ")

        transfer_key = dest_key if dest_key in TRANSFER else "spain"
        logger.info(f"[SPAIN/{dest_key.upper()}] ✅ Найдено: {len(hotels)} отелей")
        return build_country_messages(f"ИСПАНИЯ / {label}", city, flight, hotels,
                                      people, nights, dep_date_str, transfer_key, top=20)

    # ── ИСПАНИЯ / ТЕНЕРИФЕ ────────────────────────────────────────────────────
    def _tenerife_tours(self, city, people, dep_date, nights, tv_date, dep_date_str,
                       prefetched_flight: Optional[Dict] = None, children: int = 0):
        """Поиск туров на Тенерифе"""
        if prefetched_flight:
            flight = prefetched_flight
        else:
            # Ищем рейс: передаём country="tenerife" чтобы TravelMarket использовал lSubAreaIDs=502666
            flight = search_flight(city, "tenerife", dep_date, nights, people)

        logger.info("[TENERIFE] Ищем отели (region=101, 4*+, HB+, рейтинг 3.5+)...")
        hotels = search_hotels_tv(
            country_id="14", region_id="101",  # Испания, Тенерифе
            date_from=tv_date, nights=nights, adults=people - children,
            operators="13,11,125,90,23", destination_key="tenerife",
            children=children
        )

        transfer_key = "tenerife"
        logger.info(f"[TENERIFE] ✅ Найдено: {len(hotels)} отелей")
        return build_country_messages("ИСПАНИЯ / ТЕНЕРИФЕ 🌴", city, flight, hotels,
                                       people, nights, dep_date_str, transfer_key, top=20)

    # ── ЛЕТО: ТУРЦИЯ + БАРСЕЛОНА/КОСТА БРАВА/КОСТА ДОРАДА + МАЙОРКА ─────────
    def _summer_all(self, city, people, dep_date, nights, tv_date, dep_date_str, children: int = 0):
        """Все летние направления: Турция + Барселона (Коста Брава/Дорада) + Майорка"""
        logger.info(f"[SUMMER_ALL] Ищем все летние направления для {city}...")

        # ── Ищем лучшие даты за месяц через TravelMarket для каждого направления ──
        month = dep_date.month
        year  = dep_date.year
        import calendar as _cal2
        _, last_day_s = _cal2.monthrange(year, month)
        from datetime import date as _date_cls2
        today_s = datetime.now().date()
        ms_day_s = 1
        for _d in range(1, last_day_s + 1):
            if _date_cls2(year, month, _d) >= today_s + timedelta(days=3):
                ms_day_s = _d
                break
        ms_str_s = datetime(year, month, ms_day_s).strftime("%Y-%m-%d")

        try:
            r_dkk3 = requests.get("https://api.exchangerate-api.com/v4/latest/DKK", timeout=5)
            dkk_eur3 = r_dkk3.json().get("rates", {}).get("EUR", 1/7.47)
        except Exception:
            dkk_eur3 = 1/7.47

        def _best_flight(flights_list):
            best_d, best_p = None, None
            for f in flights_list:
                dep_raw = f.get("departure_date", "")
                p_dkk = f.get("price_per_person_dkk") or 0
                if p_dkk and dep_raw:
                    p_eur = round(float(p_dkk) * dkk_eur3)
                    if best_p is None or p_eur < best_p:
                        best_p = p_eur
                        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
                            try:
                                best_d = datetime.strptime(dep_raw, fmt)
                                break
                            except Exception:
                                pass
            return best_d, best_p

        # TravelMarket результаты
        best_d_tr = best_d_bcn = best_d_mal = None
        best_p_tr = best_p_bcn = best_p_mal = None

        try:
            parser = _get_tm_parser()

            # Турция
            url_tr2 = parser.build_url_turkey(city, ms_str_s, flex_days=28)
            fl_tr2 = _tm_parse(parser, url_tr2) or []
            best_d_tr, best_p_tr = _best_flight(fl_tr2)
            logger.info(f"[SUMMER_ALL] Турция TravelMarket: {len(fl_tr2)} рейсов, лучший: {best_d_tr} {best_p_tr} EUR/чел")

            # Барселона + Коста Брава + Коста Дорада
            url_bcn2 = parser.build_url_spain_barcelona(city, ms_str_s, flex_days=28)
            fl_bcn2 = _tm_parse(parser, url_bcn2) or []
            best_d_bcn, best_p_bcn = _best_flight(fl_bcn2)
            logger.info(f"[SUMMER_ALL] Барселона TravelMarket: {len(fl_bcn2)} рейсов, лучший: {best_d_bcn} {best_p_bcn} EUR/чел")

            # Майорка
            url_mal2 = parser.build_url_mallorca(city, ms_str_s, flex_days=28)
            fl_mal2 = _tm_parse(parser, url_mal2) or []
            best_d_mal, best_p_mal = _best_flight(fl_mal2)
            logger.info(f"[SUMMER_ALL] Майорка TravelMarket: {len(fl_mal2)} рейсов, лучший: {best_d_mal} {best_p_mal} EUR/чел")

        except Exception as e:
            logger.warning(f"[SUMMER_ALL] TravelMarket ошибка: {e}")

        # ── FlightPowers: сравниваем и берём лучшую цену ──────────────────────
        # Логика: если TravelMarket нашёл лучшую дату → FlightPowers ищет на ту же дату.
        # Если TravelMarket не нашёл → FlightPowers перебирает несколько дат и берёт минимум.
        fp_p_tr = fp_p_bcn = fp_p_mal = None
        fp_d_tr = fp_d_bcn = fp_d_mal = None  # даты от FlightPowers
        fp_link_tr = fp_link_bcn = fp_link_mal = ""   # ссылки на покупку рейса
        from_code_fp = AIRPORT_CODES.get(city, "CPH")

        # Вспомогательная функция: перебирает даты и возвращает минимальную цену
        def _fp_best_roundtrip(fp_api_inst, from_c, to_c, tm_best_date, month_y, month_m, ms_day, nights_n, ppl):
            """
            Ищет лучший roundtrip рейс FlightPowers.
            Перебираем ВСЕ пятницы и субботы месяца (основные дни вылетов)
            + дату TravelMarket (если есть).
            Возвращает (price_per_person_eur, best_date, buy_link) или (None, None, '').
            """
            from datetime import date as _d_cls
            import calendar as _cal_fp
            _, _last = _cal_fp.monthrange(month_y, month_m)
            today_fp = datetime.now().date()
            _max_fp = FP_MAX_ONEWAY_EUR.get("turkey" if to_c == "AYT" else "spain", 450)

            best_price = None
            best_date  = None  # ИСПРАВЛЕНО: объявляем переменную
            best_link  = ""

            # Список дат для перебора:
            # 1. Дата TravelMarket (если нашёл чартер)
            # 2. ВСЕ ДНИ месяца (чтобы найти САМЫЙ ДЕШЁВЫЙ рейс)
            sample_dates = []
            if tm_best_date:
                sample_dates.append(tm_best_date.strftime("%Y-%m-%d"))

            # Добавляем ВСЕ дни месяца (кроме первых 3 дней от сегодня)
            for day in range(1, _last + 1):
                try:
                    d_obj = _d_cls(month_y, month_m, day)
                    if d_obj >= today_fp + timedelta(days=3):
                        s = d_obj.strftime("%Y-%m-%d")
                        if s not in sample_dates:
                            sample_dates.append(s)
                except Exception:
                    pass

            logger.info(f"[FP_BEST] Перебираем {len(sample_dates)} дат для {from_c}→{to_c}")

            for dep_s in sample_dates:
                ret_s = (datetime.strptime(dep_s, "%Y-%m-%d") + timedelta(days=nights_n)).strftime("%Y-%m-%d")
                try:
                    res = fp_api_inst.search_roundtrip(from_c, to_c, dep_s, ret_s, adults=ppl)
                    if res:
                        info = fp_api_inst.get_best_flight_info(res, adults=ppl)
                        if info and info.get("price_per_person_eur", 0) > 0:
                            p = round(info["price_per_person_eur"])
                            if 0 < p <= _max_fp:
                                if best_price is None or p < best_price:
                                    best_price = p
                                    best_date  = datetime.strptime(dep_s, "%Y-%m-%d")  # Сохраняем дату
                                    best_link  = info.get("buy_link", "")
                                    logger.info(f"[FP_BEST] ✅ {from_c}→{to_c} {dep_s}: {p} EUR/чел")
                except Exception as _fe:
                    logger.debug(f"[FP_BEST] {from_c}→{to_c} {dep_s} ошибка: {_fe}")

            if best_price:
                logger.info(f"[FP_BEST] 🏆 BEST {from_c}→{to_c}: {best_price} EUR/чел на {best_date.strftime('%Y-%m-%d') if best_date else 'N/A'}")
            return best_price, best_date, best_link

        try:
            from flightpowers_parser import FlightPowersAPI
            fp_api = FlightPowersAPI()

            # Турция: AYT roundtrip — используем дату TravelMarket если есть
            try:
                fp_p_tr, fp_d_tr, fp_link_tr = _fp_best_roundtrip(
                    fp_api, from_code_fp, "AYT",
                    best_d_tr, year, month, ms_day_s, nights, people
                )
                if fp_p_tr:
                    logger.info(f"[SUMMER_ALL] FlightPowers Турция: {fp_p_tr} EUR/чел")
            except Exception as e:
                logger.debug(f"[SUMMER_ALL] FlightPowers Турция ошибка: {e}")

            # Барселона: BCN roundtrip — используем дату TravelMarket если есть
            try:
                fp_p_bcn, fp_d_bcn, fp_link_bcn = _fp_best_roundtrip(
                    fp_api, from_code_fp, "BCN",
                    best_d_bcn, year, month, ms_day_s, nights, people
                )
                if fp_p_bcn:
                    logger.info(f"[SUMMER_ALL] FlightPowers Барселона: {fp_p_bcn} EUR/чел")
            except Exception as e:
                logger.debug(f"[SUMMER_ALL] FlightPowers Барселона ошибка: {e}")

            # Майорка: PMI roundtrip — используем дату TravelMarket если есть
            try:
                fp_p_mal, fp_d_mal, fp_link_mal = _fp_best_roundtrip(
                    fp_api, from_code_fp, "PMI",
                    best_d_mal, year, month, ms_day_s, nights, people
                )
                if fp_p_mal:
                    logger.info(f"[SUMMER_ALL] FlightPowers Майорка: {fp_p_mal} EUR/чел")
            except Exception as e:
                logger.debug(f"[SUMMER_ALL] FlightPowers Майорка ошибка: {e}")

        except Exception as e:
            logger.warning(f"[SUMMER_ALL] FlightPowers недоступен: {e}")

        # Fallback цены из таблицы
        _fb_table = FLIGHT_FALLBACK.get(city, FLIGHT_FALLBACK.get("Copenhagen", {}))

        # ── Выбираем лучшую цену: min(TravelMarket, FlightPowers) ─────────────
        def _pick_best_price_and_date(tm_price, tm_date, fp_price, fp_date, fp_link, label, fb_key, fb_date):
            """Берём минимальную цену из двух источников + обновляем дату.
            Возвращает (best_price, best_date, flight_link)"""
            candidates = []
            if tm_price and tm_price > 0:
                candidates.append(("TM", tm_price, tm_date, ""))
            if fp_price and fp_price > 0:
                candidates.append(("FP", fp_price, fp_date, fp_link))

            if not candidates:
                fb = _fb_table.get(fb_key, 200)
                logger.info(f"[SUMMER_ALL] {label}: TM={tm_price} FP={fp_price} → fallback={fb} EUR/чел")
                return fb, fb_date, ""

            # Сортируем по цене (дешевле сначала)
            candidates.sort(key=lambda x: x[1])
            best_src, best_price, best_date_cand, best_link = candidates[0]

            # Если дата не найдена → используем fallback
            if not best_date_cand:
                best_date_cand = fb_date

            logger.info(f"[SUMMER_ALL] {label}: TM={tm_price} FP={fp_price} → лучший={best_price} ({best_src}) на {best_date_cand.strftime('%Y-%m-%d') if best_date_cand else 'N/A'}")
            return best_price, best_date_cand, best_link

        # Fallback даты для FlightPowers (если он не вернул дату)
        fp_fallback_date = datetime(year, month, ms_day_s)
        import calendar as _cal3
        _, _last_day = _cal3.monthrange(year, month)
        _mal_day = min(10, _last_day)
        if _date_cls2(year, month, _mal_day) < datetime.now().date() + timedelta(days=3):
            _mal_day = ms_day_s
        fp_mallorca_fallback_date = datetime(year, month, _mal_day)

        best_p_tr_final, best_d_tr, best_link_tr   = _pick_best_price_and_date(
            best_p_tr, best_d_tr, fp_p_tr, fp_d_tr, fp_link_tr, "Турция", "turkey", fp_fallback_date)
        best_p_bcn_final, best_d_bcn, best_link_bcn = _pick_best_price_and_date(
            best_p_bcn, best_d_bcn, fp_p_bcn, fp_d_bcn, fp_link_bcn, "Барселона", "spain_barcelona", fp_fallback_date)
        best_p_mal_final, best_d_mal, best_link_mal = _pick_best_price_and_date(
            best_p_mal, best_d_mal, fp_p_mal, fp_d_mal, fp_link_mal, "Майорка", "mallorca", fp_mallorca_fallback_date)

        # Обновляем переменные финальными ценами и ссылками
        best_p_tr  = best_p_tr_final
        best_p_bcn = best_p_bcn_final
        best_p_mal = best_p_mal_final

        # Формируем объекты рейсов (с ссылками)
        def _make_flight(best_p, flight_link, city_key, fallback_country):
            if best_p:
                return {"price_per_person": best_p, "price_sharm": best_p,
                        "price_hurghada": best_p, "price_total": best_p * people,
                        "source": "travelmarket+flightpowers",
                        "buy_link": flight_link or ""}
            fb = FLIGHT_FALLBACK.get(city, FLIGHT_FALLBACK["Copenhagen"])
            p = fb.get(fallback_country, 200)
            return {"price_per_person": p, "price_sharm": p, "price_hurghada": p,
                    "price_total": p * people, "source": "fallback", "buy_link": ""}

        turkey_flight  = _make_flight(best_p_tr,  best_link_tr,  city, "turkey")
        bcn_flight     = _make_flight(best_p_bcn, best_link_bcn, city, "spain_barcelona")
        mallorca_flight = _make_flight(best_p_mal, best_link_mal, city, "mallorca")

        tr_date  = (best_d_tr  or dep_date).strftime("%d.%m.%Y")
        bcn_date = (best_d_bcn or dep_date).strftime("%d.%m.%Y")
        mal_date = (best_d_mal or dep_date).strftime("%d.%m.%Y")


        # ── Отели Турции (новый REST API) ──
        logger.info(f"[SUMMER_ALL] Ищем отели Турции ({tr_date})...")
        _children_ages_tr = [10] * children if children else []
        turkey_hotels = search_hotels_turkey_rest(
            date_from_str=tr_date, nights=nights, adults=people - children,
            children_ages=_children_ages_tr,
            whitelist=REQUIRED_TURKEY, whitelist_aliases=TURKEY_ALIASES,
            top=20,
        )
        logger.info(f"[SUMMER_ALL] Турция: {len(turkey_hotels)} отелей")

        # ── Отели Барселона + Коста Брава + Коста Дорада (новый REST API) ──
        logger.info(f"[SUMMER_ALL] Ищем отели Барселоны/Косты Бравы/Дорады ({bcn_date})...")
        _children_ages_sp = [10] * children if children else []
        barcelona_hotels = search_hotels_spain_rest(
            date_from_str=bcn_date, nights=nights, adults=people - children,
            children_ages=_children_ages_sp, destination_key="spain_bcn", top=50
        )
        logger.info(f"[SUMMER_ALL] Барселона/Коста Брава/Дорада: {len(barcelona_hotels)} отелей")

        # ── Отели Майорки (новый REST API) ──
        logger.info(f"[SUMMER_ALL] Ищем отели Майорки ({mal_date})...")
        mallorca_hotels = search_hotels_spain_rest(
            date_from_str=mal_date, nights=nights, adults=people - children,
            children_ages=_children_ages_sp, destination_key="mallorca", top=50
        )
        logger.info(f"[SUMMER_ALL] Майорка: {len(mallorca_hotels)} отелей")

        t_fp = turkey_flight["price_per_person"]
        b_fp = bcn_flight["price_per_person"]
        m_fp = mallorca_flight["price_per_person"]
        t_tr = TRANSFER["turkey"]
        b_tr = TRANSFER.get("spain", 600)
        m_tr = TRANSFER.get("mallorca", 530)

        # Применяем фильтр «не понижаем звёзды по цене»
        turkey_hotels    = filter_no_stars_downgrade(turkey_hotels,    t_fp, t_tr, people)
        barcelona_hotels = filter_no_stars_downgrade(barcelona_hotels, b_fp, b_tr, people)
        mallorca_hotels  = filter_no_stars_downgrade(mallorca_hotels,  m_fp, m_tr, people)

        _adults_lbl = people - children
        _ppl_lbl    = f"{_adults_lbl} взр." + (f" + {children} реб." if children else "") + f" ({people} чел.)" if children else f"{people} чел."
        header = (
            f"🌞 ЛЕТНИЕ НАПРАВЛЕНИЯ ({_ppl_lbl}, {nights} ночей)\n"
            f"✈️ Вылет из {city}\n\n"
        )

        turkey_block = (
            f"━━━ 🇹🇷 ТУРЦИЯ ━━━\n"
            f"📅 Вылет: {tr_date}\n\n"
        )
        idx = 0
        for h in turkey_hotels:  # все отели Турции из белого списка
            idx += 1
            turkey_block += format_hotel_line(idx, h, t_fp, t_tr, people, fallback_date=tr_date) + "\n"
        if not turkey_hotels:
            turkey_block += "Туры не найдены\n"

        bcn_block = (
            f"\n━━━ 🇪🇸 БАРСЕЛОНА / КОСТА БРАВА / КОСТА ДОРАДА ━━━\n"
            f"📅 Вылет: {bcn_date}\n\n"
        )
        for h in barcelona_hotels[:20]:  # максимум 20 отелей Испании
            idx += 1
            bcn_block += format_hotel_line(idx, h, b_fp, b_tr, people, fallback_date=bcn_date) + "\n"
        if not barcelona_hotels:
            bcn_block += "Туры не найдены\n"

        mallorca_block = (
            f"\n━━━ 🏝 МАЙОРКА ━━━\n"
            f"📅 Вылет: {mal_date}\n\n"
        )
        for h in mallorca_hotels[:20]:  # максимум 20 отелей Майорки
            idx += 1
            mallorca_block += format_hotel_line(idx, h, m_fp, m_tr, people, fallback_date=mal_date) + "\n"
        if not mallorca_hotels:
            mallorca_block += "Туры не найдены\n"

        return _split_message(header + turkey_block + bcn_block + mallorca_block)

    # ── ЛЕТО: ТУРЦИЯ + ИСПАНИЯ (устарело, для совместимости) ─────────────────
    def _summer_both(self, city, people, dep_date, nights, tv_date, dep_date_str, children: int = 0):
        turkey_flight = search_flight(city, "turkey", dep_date, nights, people)
        spain_flight  = search_flight(city, "spain",  dep_date, nights, people)

        logger.info("[SUMMER] Ищем отели Турции...")
        turkey_hotels = search_hotels_tv(
            country_id="4", region_id=None,
            date_from=tv_date, nights=nights, adults=people - children,
            operators=TV_OPS_TURKEY, destination_key="turkey"
        )

        logger.info("[SUMMER] Ищем отели Испании...")
        spain_hotels = search_hotels_tv(
            country_id="14", region_id="69,70,71",
            date_from=tv_date, nights=nights, adults=people - children,
            operators=TV_OPS_SPAIN, destination_key="spain_bcn"
        )

        t_fp = turkey_flight["price_per_person"]
        s_fp = spain_flight["price_per_person"]
        t_tr = TRANSFER["turkey"]
        s_tr = TRANSFER["spain"]

        header = (
            f"🌞 ЛЕТНИЕ НАПРАВЛЕНИЯ ({people} чел., {nights} ночей)\n"
            f"✈️ Вылет из {city} ~ {dep_date_str}\n\n"
        )
        turkey_block = (
            f"━━━ 🇹🇷 ТУРЦИЯ ━━━\n\n"
        )
        idx = 0
        for h in turkey_hotels:
            idx += 1
            turkey_block += format_hotel_line(idx, h, t_fp, t_tr, people) + "\n"

        spain_block = (
            f"\n━━━ 🇪🇸 ИСПАНИЯ ━━━\n\n"
        )
        for h in spain_hotels:
            idx += 1
            spain_block += format_hotel_line(idx, h, s_fp, s_tr, people) + "\n"

        return _split_message(header + turkey_block + spain_block)


# Глобальный экземпляр для импорта в facebook_bot.py
selector = TourSelector()


# ─────────────────────────────────────────────────────────────────────────────
# ТЕСТ — запуск напрямую
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import io as _io
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    print("=" * 70)
    print("TOUR SELECTOR — тест")
    print("=" * 70)

    sel = TourSelector()

    # ── ТЕСТ 1: Египет из Копенгагена в марте ─────────────────────────────────
    print("\n[ТЕСТ 1] Египет, Копенгаген, март 2026, 2 взрослых, 7 ночей")
    print("-" * 70)
    msgs = sel.find_tours(
        departure_city="Copenhagen",
        people=2,
        departure_date=datetime(2026, 3, 15),
        nights=7,
        is_winter=True,
    )
    print(f"\nГотово! Получено {len(msgs)} сообщений:\n")
    for i, m in enumerate(msgs, 1):
        print(f"── Сообщение {i} ({len(m)} символов) ──────────────")
        print(m)
        print()

    # ── ТЕСТ 2: Лето из Биллунда в июне ──────────────────────────────────────
    print("\n[ТЕСТ 2] Лето, Биллунд, июнь 2026, 2 взрослых, 7 ночей")
    print("-" * 70)
    msgs2 = sel.find_tours(
        departure_city="Billund",
        people=2,
        departure_date=datetime(2026, 6, 10),
        nights=7,
        is_winter=False,
    )
    print(f"\nГотово! Получено {len(msgs2)} сообщений:\n")
    for i, m in enumerate(msgs2, 1):
        print(f"── Сообщение {i} ({len(m)} символов) ──────────────")
        print(m)
        print()

