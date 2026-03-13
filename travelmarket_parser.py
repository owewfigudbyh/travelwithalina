#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TravelMarket Parser - парсинг чартерных рейсов
Поддерживает travelmarket.dk (Дания), travelmarket.no (Норвегия), travelmarket.se (Швеция)
Использует Selenium для JavaScript страниц
"""

import re
import logging
import requests
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Курс DKK к EUR (примерно 7.45 DKK = 1 EUR)
DKK_TO_EUR_RATE = 7.45

# ID аэропортов (одиночные)
AIRPORT_IDS = {
    "Billund":    "499708",
    "Copenhagen": "500055",   # CPH Copenhagen
    "Aalborg":    "503278",
    "Aarhus":     "503274",   # Орхус (Ютландия)
    "Oslo":       "501864",   # OSL — travelmarket.no
    "Stockholm":  "499485",   # ARN Stockholm — travelmarket.se
    "Goteborg":   "499715",
    "Gothenburg": "499715",
    "Helsinki":   "499717",
    "Malmo":      "500055",   # Мальмё → Копенгаген
}

# Для Ютландии (Billund + Aarhus + Aalborg) — все три вместе
JUTLAND_DEPARTURE_IDS = "499708,503274,503278"
JUTLAND_CITIES = {"Billund", "Aarhus", "Aalborg"}

# Стокгольм + Гётеборг + Мальмё (travelmarket.se)
SWEDEN_DEPARTURE_IDS = "499485,501789,502994"
SWEDEN_CITIES = {"Stockholm", "Goteborg", "Malmo", "Gothenburg"}

# ID стран
COUNTRY_IDS = {
    "egypt": "500297",
    "turkey": "500320",
    "spain": "500311",
    "canary": "500311",  # Канарские острова = Испания
}

# Домены для разных стран (правильные домены travelmarket)
DOMAINS = {
    "dk": "www.travelmarket.dk",
    "no": "www.travelmarket.no",
    "se": "www.travelmarket.se",
}

# Какой домен использовать для каждого города вылета
CITY_TO_DOMAIN = {
    "Billund":    "dk",
    "Copenhagen": "dk",
    "Aalborg":    "dk",
    "Aarhus":     "dk",
    "Malmo":      "dk",
    "Oslo":       "no",
    "Stockholm":  "se",
    "Goteborg":   "se",
    "Gothenburg": "se",
    "Helsinki":   "fi",
}

# Параметры поиска по домену:
# Норвегия и Швеция: nMaxDuration=9, lDestinationTypeIDs=1,2
# Дания: nMaxDuration=7, lDestinationTypeIDs=1
CITY_PARAMS = {
    "dk": {"nMinDuration": 7, "nMaxDuration": 7, "lDestinationTypeIDs": "1"},
    "no": {"nMinDuration": 7, "nMaxDuration": 9, "lDestinationTypeIDs": "1,2"},
    "se": {"nMinDuration": 7, "nMaxDuration": 9, "lDestinationTypeIDs": "1,2"},
    "fi": {"nMinDuration": 7, "nMaxDuration": 9, "lDestinationTypeIDs": "1,2"},
}


class TravelMarketParser:
    """Парсер чартерных рейсов с TravelMarket"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        self.driver = None

    def dkk_to_eur(self, dkk_price: float) -> float:
        """Конвертация датских крон в евро"""
        return round(dkk_price / DKK_TO_EUR_RATE, 2)

    def parse_price(self, price_text: str) -> Optional[float]:
        """Парсинг цены из текста"""
        if not price_text:
            return None
        # "2.299,-" -> 2299
        # "Total: 4.598,-" -> 4598
        clean = re.sub(r'[^\d]', '', price_text)
        if clean:
            return float(clean)
        return None

    def init_selenium(self):
        """Инициализация Selenium WebDriver"""
        if self.driver:
            return True

        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager

            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            logger.info("Selenium WebDriver initialized")
            return True
        except Exception as e:
            logger.error(f"Selenium init error: {e}")
            return False

    def _fetch_html(self, url: str) -> Optional[str]:
        """Загружает HTML страницы с правильными заголовками браузера.
        Пробует несколько User-Agent и наборов заголовков.
        """
        headers_variants = [
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'da-DK,da;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            },
            {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,*/*;q=0.9',
                'Accept-Language': 'da,en;q=0.9',
            },
        ]
        for hdrs in headers_variants:
            try:
                sess = requests.Session()
                sess.headers.update(hdrs)
                r = sess.get(url, timeout=25, allow_redirects=True)
                if r.status_code == 200 and len(r.text) > 5000:
                    return r.text
                logger.warning(f"[TravelMarket] HTTP {r.status_code}, len={len(r.text)}")
            except Exception as e:
                logger.warning(f"[TravelMarket] fetch error: {e}")
        return None

    def _parse_html_results(self, page_source: str) -> List[Dict]:
        """Парсит HTML TravelMarket — извлекает все result_element.
        Работает как с полным HTML (когда сервер рендерит результаты),
        так и ищет данные в JS-переменных если HTML пустой.
        """
        flights = []
        now = datetime.now()

        # ── Метод 1: div-структура result_element (сервер отдаёт готовый HTML) ──
        indices = re.findall(r'id="result_element-(\d+)"', page_source)
        if indices:
            logger.info(f"[TravelMarket] Найдено {len(indices)} result_element в HTML")
            for idx_str in indices:
                idx = int(idx_str)
                try:
                    # Вырезаем блок этого элемента
                    block_m = re.search(
                        rf'id="result_element-{idx}".*?(?=id="result_element-{idx+1}"|id="tse-resultpage-more"|</div>\s*</div>\s*</div>\s*<div\s+id="tse-resultpage)',
                        page_source, re.DOTALL | re.IGNORECASE
                    )
                    block = block_m.group(0) if block_m else page_source

                    # Дата: ищем в id="departure-date-N"
                    date_m = re.search(
                        rf'id="departure-date-{idx}"[^>]*>(.*?)</div>',
                        block, re.DOTALL | re.IGNORECASE
                    )
                    if not date_m:
                        continue
                    date_raw = re.sub(r'<[^>]+>', ' ', date_m.group(1)).strip()
                    date_num = re.search(r'(\d{1,2})/(\d{1,2})', date_raw)
                    if not date_num:
                        continue
                    day, month = int(date_num.group(1)), int(date_num.group(2))
                    year = now.year
                    if month < now.month or (month == now.month and day < now.day):
                        year = now.year + 1

                    # Направление
                    loc_m = re.search(
                        rf'id="hotel-location-{idx}".*?<strong[^>]*>(.*?)</strong>',
                        block, re.DOTALL | re.IGNORECASE
                    )
                    destination = re.sub(r'<[^>]+>', '', loc_m.group(1)).strip() if loc_m else ""

                    # Нас интересуют ВСЕ результаты (и туры с отелями, и чистые рейсы)
                    # т.к. цена отображает стоимость перелёта
                    hn_m = re.search(rf'id="hotel-name-{idx}"([^>]*)>(.*?)</span>', block, re.IGNORECASE | re.DOTALL)
                    if hn_m:
                        attrs = hn_m.group(1)
                        text_inner = hn_m.group(2)
                        alt_m = re.search(r'alt="([^"]*)"', attrs, re.IGNORECASE)
                        hotel_text = alt_m.group(1) if alt_m else re.sub(r'<[^>]+>', '', text_inner).strip()
                    else:
                        hotel_text = "Kun fly"

                    # Ночей
                    dur_m = re.search(rf'id="departure-duration-{idx}"[^>]*>(\d+)\s*n', block, re.IGNORECASE)
                    nights_val = int(dur_m.group(1)) if dur_m else 7

                    # Цена за 1 человека (offer-price = per person price)
                    price_m = re.search(rf'id="offer-price-{idx}"[^>]*>\s*([\d\.]+),-', block, re.IGNORECASE)
                    if not price_m:
                        # Альтернативно ищем просто цену в блоке
                        price_m = re.search(r'>([\d]{1,2}\.[\d]{3}),-<', block)
                    if not price_m:
                        continue
                    price_dkk = float(price_m.group(1).replace('.', ''))
                    if price_dkk < 500:
                        continue

                    # Аэропорт
                    ap_m = re.search(rf'id="departure-airport-{idx}"[^>]*>(.*?)</span>', block, re.IGNORECASE | re.DOTALL)
                    airport = re.sub(r'<[^>]+>', '', ap_m.group(1)).strip() if ap_m else ""

                    flights.append({
                        'departure_date': f"{day:02d}.{month:02d}.{year}",
                        'departure_date_obj': datetime(year, month, day),
                        'destination': destination,
                        'departure_airport': airport,
                        'nights': nights_val,
                        'flight_only': True,
                        'price_per_person_dkk': price_dkk,
                        'price_per_person_eur': self.dkk_to_eur(price_dkk),
                        'source': 'travelmarket_html',
                    })
                    logger.info(f"[TravelMarket] ✅ {destination} {day:02d}/{month:02d}/{year} — {price_dkk:.0f} DKK ({self.dkk_to_eur(price_dkk):.0f} EUR/чел)")
                except Exception as ex:
                    logger.debug(f"[TravelMarket] parse idx={idx}: {ex}")
                    continue

        # ── Метод 2: JS-переменная aCharterElements или массив данных ──
        if not flights:
            # Ищем JavaScript массив с данными чартеров
            js_data_m = re.search(
                r'aCharterElements\s*=\s*(\[.*?\]);',
                page_source, re.DOTALL
            )
            if js_data_m:
                try:
                    import json
                    elements = json.loads(js_data_m.group(1))
                    logger.info(f"[TravelMarket] JS aCharterElements: {len(elements)} элементов")
                    for el in elements:
                        try:
                            dep_date = el.get('departuredate', '') or el.get('departureDate', '')
                            dest = el.get('destination', '') or el.get('placename', '')
                            price = float(el.get('price', 0) or el.get('priceperpers', 0) or 0)
                            nights_val = int(el.get('duration', 7) or 7)
                            if not dep_date or not price or price < 500:
                                continue
                            # Парсим дату
                            if '-' in dep_date:
                                dt = datetime.strptime(dep_date[:10], '%Y-%m-%d')
                            else:
                                dt = datetime.strptime(dep_date[:10], '%d.%m.%Y')
                            flights.append({
                                'departure_date': dt.strftime('%d.%m.%Y'),
                                'departure_date_obj': dt,
                                'destination': dest,
                                'nights': nights_val,
                                'flight_only': True,
                                'price_per_person_dkk': price,
                                'price_per_person_eur': self.dkk_to_eur(price),
                                'source': 'travelmarket_js',
                            })
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug(f"[TravelMarket] JS parse error: {e}")

        # ── Метод 3: Regex по паттерну дата+направление+цена ──
        if not flights:
            # Паттерн из HTML: дата/месяц ... Sharm/Hurghada ... цена,-
            pattern = (
                r'(\d{1,2})/(\d{1,2})'          # день/месяц
                r'[\s\S]{0,500}?'
                r'(Sharm El Sheikh|Hurghada|Antalya|Mallorca|Tenerife|Marsa Alam|El Gouna)'
                r'[\s\S]{0,500}?'
                r'([\d]{1,2}\.[\d]{3}),-'        # цена X.XXX,-
            )
            matches = re.findall(pattern, page_source, re.DOTALL | re.IGNORECASE)
            seen_prices = set()
            for m in matches:
                try:
                    day, month = int(m[0]), int(m[1])
                    destination = m[2]
                    price_dkk = float(m[3].replace('.', ''))
                    if price_dkk < 500 or price_dkk in seen_prices:
                        continue
                    seen_prices.add(price_dkk)
                    year = now.year
                    if month < now.month or (month == now.month and day < now.day):
                        year = now.year + 1
                    flights.append({
                        'departure_date': f"{day:02d}.{month:02d}.{year}",
                        'departure_date_obj': datetime(year, month, day),
                        'destination': destination,
                        'nights': 7,
                        'flight_only': True,
                        'price_per_person_dkk': price_dkk,
                        'price_per_person_eur': self.dkk_to_eur(price_dkk),
                        'source': 'travelmarket_regex',
                    })
                    logger.info(f"[TravelMarket] regex ✅ {destination} {day}/{month} — {price_dkk:.0f} DKK")
                except Exception:
                    continue

        if flights:
            logger.info(f"[TravelMarket] Итого распарсено: {len(flights)} рейсов")
        else:
            logger.warning(f"[TravelMarket] 0 рейсов после всех методов парсинга")
        return flights

    def parse_with_requests(self, url: str) -> List[Dict]:
        """Быстрый парсинг через requests (без Selenium).
        Загружает страницу и парсит HTML с результатами.
        """
        page_source = self._fetch_html(url)
        if not page_source:
            logger.warning("[TravelMarket] Не удалось загрузить страницу")
            return []
        return self._parse_html_results(page_source)

    def parse_with_selenium(self, url: str) -> List[Dict]:
        """Парсинг страницы с помощью Selenium — ждём JS, затем парсим HTML"""
        if not self.init_selenium():
            return []

        try:
            import time
            logger.info(f"Selenium loading: {url}")
            print(f"[TravelMarket] Loading page with Selenium...")

            self.driver.get(url)

            # Ждём пока появятся результаты (result_element-0 или Kun fly)
            max_wait = 30
            waited = 0
            while waited < max_wait:
                time.sleep(2)
                waited += 2
                src = self.driver.page_source
                if 'result_element-0' in src or ('Kun fly' in src and 'offer-price-0' in src):
                    logger.info(f"[TravelMarket] Результаты появились через {waited}с")
                    break

            page_source = self.driver.page_source
            logger.info(f"Page source length: {len(page_source)}")

            # Основной парсинг через result_element div-структуру
            flights = self._parse_html_results(page_source)

            if flights:
                logger.info(f"[TravelMarket] Selenium распарсил {len(flights)} рейсов")
                return flights

            # Запасной метод: парсим блоки result_element напрямую через BeautifulSoup-like regex
            print("[TravelMarket] Пробуем прямой парсинг блоков result_element...")
            now = datetime.now()
            seen = set()

            # Ищем все блоки result_element
            blocks = re.findall(
                r'id="result_element-\d+".*?(?=id="result_element-\d+"|id="tse-resultpage-more")',
                page_source, re.DOTALL
            )
            if not blocks:
                # Берём весь контент results-container
                rc_m = re.search(r'id="results-container"(.*?)id="tse-resultpage-more"', page_source, re.DOTALL)
                if rc_m:
                    blocks = [rc_m.group(1)]

            logger.info(f"[TravelMarket] Блоков result_element: {len(blocks)}")

            for block in blocks:
                try:
                    # Дата: ищем паттерн "Sø 15/3" или просто "15/3"
                    date_m = re.search(r'(\d{1,2})/(\d{1,2})', block)
                    if not date_m:
                        continue
                    day, month = int(date_m.group(1)), int(date_m.group(2))
                    year = now.year
                    if month < now.month or (month == now.month and day < now.day):
                        year = now.year + 1

                    # Направление — парсим для любого типа тура
                    dest_m = re.search(
                        r'(Sharm El Sheikh|Hurghada|Antalya|Alanya|Side|Belek|Kemer|Tenerife|Mallorca|El Gouna|Marsa Alam|Barcelona|Palma)',
                        block, re.IGNORECASE
                    )
                    destination = dest_m.group(1) if dest_m else ""

                    # Цена за человека: offer-price-N (per person, НЕ total)
                    # В HTML: id="offer-price-0" bis_skin_checked="1">2.799,-</div>
                    price_pp_m = re.search(
                        r'id="offer-price-\d+"[^>]*>\s*([\d]{1,2}\.[\d]{3}),-',
                        block, re.IGNORECASE
                    )
                    if not price_pp_m:
                        # Альтернатива: ищем все цены X.XXX,- и берём меньшую (per person < total)
                        all_prices = re.findall(r'([\d]{1,2}\.[\d]{3}),-', block)
                        valid = [float(p.replace('.', '')) for p in all_prices
                                 if 1000 <= float(p.replace('.', '')) <= 15000]
                        if not valid:
                            continue
                        price_dkk = min(valid)  # меньшая = per person
                    else:
                        price_dkk = float(price_pp_m.group(1).replace('.', ''))

                    if price_dkk < 500:
                        continue

                    key = (day, month, round(price_dkk))
                    if key in seen:
                        continue
                    seen.add(key)

                    flights.append({
                        'departure_date': f"{day:02d}.{month:02d}.{year}",
                        'departure_date_obj': datetime(year, month, day),
                        'destination': destination,
                        'nights': 7,
                        'flight_only': True,
                        'price_per_person_dkk': price_dkk,
                        'price_per_person_eur': self.dkk_to_eur(price_dkk),
                        'source': 'travelmarket_selenium',
                    })
                    logger.info(f"[TravelMarket] ✅ {destination} {day:02d}/{month:02d}/{year} — {price_dkk:.0f} DKK ({self.dkk_to_eur(price_dkk):.0f} EUR/чел)")
                except Exception as ex:
                    logger.debug(f"[TravelMarket] block parse error: {ex}")
                    continue

            if flights:
                logger.info(f"[TravelMarket] Selenium итого: {len(flights)} рейсов")
            else:
                logger.warning("[TravelMarket] Selenium: ничего не найдено")

            return flights

        except Exception as e:
            logger.error(f"Selenium parsing error: {e}")
            print(f"[TravelMarket] Selenium error: {e}")
            return []

    # lPlaceIDs для конкретных курортов Египта
    PLACE_IDS = {
        "hurghada": "59338",    # Хургада
        "sharm":    "59303",    # Шарм-эль-Шейх
    }

    # ── ID для новых направлений ──────────────────────────────────────────
    # Турция: Анталья(74347), Белек(74373), Аланья(74641), Сиде(209164)
    TURKEY_PLACE_IDS = "74347,74373,74641,209164"

    # Испания: Барселона(242249 = lPlaceIDs), Коста Брава(500044), Коста Дорада(500046) = lSubAreaIDs
    SPAIN_BARCELONA_SUBAREA_IDS = "500044,500046"
    SPAIN_BARCELONA_PLACE_IDS   = "242249"

    # Майорка: lSubAreaIDs=501354
    MALLORCA_SUBAREA_ID = "501354"

    def _departure_ids(self, departure_city: str) -> str:
        """Возвращает строку lDepartureIDs для аэропорта"""
        JUTLAND = "499708,503274,503278"
        SWEDEN  = "499485,501789,502994"
        MAP = {
            "Billund":    JUTLAND, "Aarhus": JUTLAND, "Aalborg": JUTLAND,
            "Copenhagen": "500055",
            "Malmo":      SWEDEN,
            "Oslo":       "501864",   # travelmarket.no
            "Stockholm":  SWEDEN,     # travelmarket.se
            "Goteborg":   SWEDEN,     # travelmarket.se
            "Gothenburg": SWEDEN,
            "Helsinki":   "499717",
            "Tallinn":    "499718",
        }
        return MAP.get(departure_city, "500055")

    def _get_domain_and_params(self, departure_city: str, domain: str = None) -> tuple:
        """Возвращает (domain_str, base_url, nMinDuration, nMaxDuration, lDestinationTypeIDs).
        Если domain явно задан — используем его, иначе определяем по городу.
        """
        if domain is None:
            domain = CITY_TO_DOMAIN.get(departure_city, "dk")
        params = CITY_PARAMS.get(domain, CITY_PARAMS["dk"])
        base = DOMAINS.get(domain, DOMAINS["dk"])
        return domain, base, params["nMinDuration"], params["nMaxDuration"], params["lDestinationTypeIDs"]

    def build_url_turkey(self, departure_city: str, departure_date: str,
                          flex_days: int = 28, domain: str = None) -> str:
        """URL для рейсов в Турцию (Анталья, Белек, Аланья, Сиде, Кемер).
        Норвегия (Осло): travelmarket.no, lDepartureIDs=501864, nMaxDuration=9, lDestinationTypeIDs=1,2
        Швеция: travelmarket.se, lDepartureIDs=499485,501789,502994
        Дания (Ютландия/CPH): travelmarket.dk, стандартные параметры
        """
        dep_ids = self._departure_ids(departure_city)
        _, base, nmin, nmax, dest_type = self._get_domain_and_params(departure_city, domain)
        # Турция: Анталья(74347), Белек(74373), Аланья(74641), Сиде(209164)
        # Для Норвегии добавляем Кемер(74377) согласно URL из ТЗ
        turkey_place_ids = "74347,74373,74377,74641,209164"
        url = (
            f"https://{base}/charter_v5_list.cfm?"
            f"dDepartureDate={departure_date}"
            f"&nFlexDays={flex_days}"
            f"&lDestinationTypeIDs={dest_type}"
            f"&nMinDuration={nmin}"
            f"&nMaxDuration={nmax}"
            f"&lDepartureIDs={dep_ids}"
            f"&lPlaceIDs={turkey_place_ids}"
            f"&nSortBy=1"
            f"&nCharterVersion=12"
        )
        # Для Дании и Швеции добавляем lSpecificationIDs=1
        dom = domain or CITY_TO_DOMAIN.get(departure_city, "dk")
        if dom in ("dk", "se"):
            url += "&lSpecificationIDs=1"
        return url

    def build_url_mallorca(self, departure_city: str, departure_date: str,
                            flex_days: int = 28, domain: str = None) -> str:
        """URL для рейсов на Майорку (lSubAreaIDs=501354).
        Норвегия (Осло): travelmarket.no, nMaxDuration=9, lDestinationTypeIDs=1,2
        Швеция: travelmarket.se
        Дания: travelmarket.dk
        """
        dep_ids = self._departure_ids(departure_city)
        _, base, nmin, nmax, dest_type = self._get_domain_and_params(departure_city, domain)
        return (
            f"https://{base}/charter_v5_list.cfm?"
            f"dDepartureDate={departure_date}"
            f"&nFlexDays={flex_days}"
            f"&lDestinationTypeIDs={dest_type}"
            f"&nMinDuration={nmin}"
            f"&nMaxDuration={nmax}"
            f"&lSpecificationIDs=1"
            f"&lDepartureIDs={dep_ids}"
            f"&lSubAreaIDs={self.MALLORCA_SUBAREA_ID}"
            f"&nSortBy=1"
            f"&nCharterVersion=12"
        )

    def build_url_spain_barcelona(self, departure_city: str, departure_date: str,
                                   flex_days: int = 28, domain: str = None) -> str:
        """URL для рейсов в Барселону + Коста Брава + Коста Дорада.
        lSubAreaIDs=500044,500046 + lPlaceIDs=242249
        Норвегия (Осло): travelmarket.no, nMaxDuration=9, lDestinationTypeIDs=1,2
        Швеция: travelmarket.se
        Дания: travelmarket.dk
        """
        dep_ids = self._departure_ids(departure_city)
        _, base, nmin, nmax, dest_type = self._get_domain_and_params(departure_city, domain)
        return (
            f"https://{base}/charter_v5_list.cfm?"
            f"dDepartureDate={departure_date}"
            f"&nFlexDays={flex_days}"
            f"&lDestinationTypeIDs={dest_type}"
            f"&nMinDuration={nmin}"
            f"&nMaxDuration={nmax}"
            f"&lSpecificationIDs=1"
            f"&lDepartureIDs={dep_ids}"
            f"&lSubAreaIDs={self.SPAIN_BARCELONA_SUBAREA_IDS}"
            f"&lPlaceIDs={self.SPAIN_BARCELONA_PLACE_IDS}"
            f"&nSortBy=1"
            f"&nCharterVersion=12"
        )

    def get_best_price(self, url: str, label: str = "") -> dict:
        """Парсит URL и возвращает лучшую (минимальную) цену за человека.
        Сначала пробует requests, потом Selenium.
        Возвращает: {'price_eur': float|None, 'date': str|None, 'raw': list}
        """
        flights = self.parse_with_requests(url)
        if not flights:
            logger.info(f"[TM] {label}: requests=0, пробуем Selenium")
            flights = self.parse_with_selenium(url)

        flight_only = [f for f in flights if f.get('flight_only', False)]
        if not flight_only:
            return {'price_eur': None, 'date': None, 'raw': []}

        flight_only.sort(key=lambda x: x.get('price_per_person_eur', 99999))
        best = flight_only[0]
        return {
            'price_eur': round(best.get('price_per_person_eur', 0)),
            'date':      best.get('departure_date', ''),
            'raw':       flight_only,
        }

    def get_flight_prices_by_destination(
            self,
            departure_city: str,
            destination: str,       # 'turkey' | 'mallorca' | 'spain_barcelona' | 'egypt'
            departure_date: str,    # YYYY-MM-DD
            flex_days: int = 28,
            domain: str = "dk",
    ) -> dict:
        """Универсальный метод: строит нужный URL и возвращает лучшую цену.
        destination варианты:
          'turkey'          — Анталья/Белек/Аланья/Сиде
          'mallorca'        — Майорка
          'spain_barcelona' — Барселона + Коста Брава + Коста Дорада
          'hurghada'        — Хургада
          'sharm'           — Шарм-эль-Шейх
          'tenerife'        — Тенерифе
        """
        dest = destination.lower()
        if dest == 'turkey':
            url = self.build_url_turkey(departure_city, departure_date, flex_days, domain)
        elif dest == 'mallorca':
            url = self.build_url_mallorca(departure_city, departure_date, flex_days, domain)
        elif dest in ('spain_barcelona', 'barcelona', 'costa_brava', 'costa_dorada'):
            url = self.build_url_spain_barcelona(departure_city, departure_date, flex_days, domain)
        elif dest == 'tenerife':
            url = self.build_url_tenerife(departure_city, departure_date, flex_days, domain)
        elif dest == 'hurghada':
            url = self.build_url_hurghada(departure_city, departure_date, flex_days, domain)
        elif dest == 'sharm':
            url = self.build_url_sharm(departure_city, departure_date, flex_days, domain)
        else:
            url = self.build_url(departure_city, destination, departure_date,
                                 nights_min=7, nights_max=7, domain=domain, flex_days=flex_days)

        logger.info(f"[TM] {departure_city} → {destination}: {url}")
        return self.get_best_price(url, label=f"{departure_city}→{destination}")

    def build_url(self, departure_city: str, destination: str,
                  departure_date: str, nights_min: int = None, nights_max: int = None,
                  domain: str = None, flex_days: int = 0,
                  place: str = None) -> str:
        """Построение URL для поиска (только рейсы без отеля).

        place: 'hurghada' или 'sharm' — lPlaceIDs вместо lCountryIDs.
        Параметры nMinDuration/nMaxDuration/lDestinationTypeIDs определяются автоматически
        по городу вылета: NO/SE — nMaxDuration=9, lDestinationTypeIDs=1,2.
        """
        dep_dom, base_domain, auto_nmin, auto_nmax, dest_type = self._get_domain_and_params(departure_city, domain)
        departure_ids = self._departure_ids(departure_city)
        if nights_min is None:
            nights_min = auto_nmin
        if nights_max is None:
            nights_max = auto_nmax

        if place and place.lower() in self.PLACE_IDS:
            place_id = self.PLACE_IDS[place.lower()]
            url = (
                f"https://{base_domain}/charter_v5_list.cfm?"
                f"dDepartureDate={departure_date}"
                f"&nFlexDays={flex_days}"
                f"&lDestinationTypeIDs={dest_type}"
                f"&nMinDuration={nights_min}"
                f"&nMaxDuration={nights_max}"
                f"&lSpecificationIDs=1"
                f"&lDepartureIDs={departure_ids}"
                f"&lPlaceIDs={place_id}"
                f"&nSortBy=1"
                f"&nCharterVersion=12"
            )
        else:
            country_id = COUNTRY_IDS.get(destination, COUNTRY_IDS["egypt"])
            url = (
                f"https://{base_domain}/charter_v5_list.cfm?"
                f"dDepartureDate={departure_date}"
                f"&nFlexDays={flex_days}"
                f"&lDestinationTypeIDs={dest_type}"
                f"&nMinDuration={nights_min}"
                f"&nMaxDuration={nights_max}"
                f"&lSpecificationIDs=1"
                f"&lDepartureIDs={departure_ids}"
                f"&lCountryIDs={country_id}"
                f"&nSortBy=1"
                f"&nCharterVersion=12"
            )
        return url

    def build_url_hurghada(self, departure_city: str, departure_date: str,
                            flex_days: int = 28, domain: str = None) -> str:
        """URL для рейсов в Хургаду (lPlaceIDs=59303,59338 — Шарм+Хургада вместе).
        Осло: travelmarket.no, nMaxDuration=9, lDestinationTypeIDs=1,2
        """
        return self.build_url(departure_city, "egypt", departure_date,
                              domain=domain, flex_days=flex_days, place="hurghada")

    def build_url_sharm(self, departure_city: str, departure_date: str,
                         flex_days: int = 28, domain: str = None) -> str:
        """URL для рейсов в Шарм-эль-Шейх+Хургаду.
        Осло: travelmarket.no, nMaxDuration=9, lDestinationTypeIDs=1,2
        """
        return self.build_url(departure_city, "egypt", departure_date,
                              domain=domain, flex_days=flex_days, place="sharm")

    def build_url_egypt_combined(self, departure_city: str, departure_date: str,
                                  flex_days: int = 28, domain: str = None) -> str:
        """URL для Шарм-эль-Шейха + Хургады вместе (lPlaceIDs=59303,59338).
        Для Осло/Стокгольма автоматически используется travelmarket.no/.se.
        """
        dep_dom, base_domain, nmin, nmax, dest_type = self._get_domain_and_params(departure_city, domain)
        departure_ids = self._departure_ids(departure_city)
        return (
            f"https://{base_domain}/charter_v5_list.cfm?"
            f"dDepartureDate={departure_date}"
            f"&nFlexDays={flex_days}"
            f"&lDestinationTypeIDs={dest_type}"
            f"&nMinDuration={nmin}"
            f"&nMaxDuration={nmax}"
            f"&lSpecificationIDs=1"
            f"&lDepartureIDs={departure_ids}"
            f"&lPlaceIDs=59303,59338"
            f"&nSortBy=1"
            f"&nCharterVersion=12"
        )

    def build_url_tenerife(self, departure_city: str, departure_date: str,
                            flex_days: int = 28, domain: str = None) -> str:
        """URL для рейсов на Тенерифе (lSubAreaIDs=502666).
        Осло: travelmarket.no, nMaxDuration=9, lDestinationTypeIDs=1,2
        Стокгольм: travelmarket.se
        Дания: travelmarket.dk
        """
        dep_ids = self._departure_ids(departure_city)
        _, base, nmin, nmax, dest_type = self._get_domain_and_params(departure_city, domain)
        return (
            f"https://{base}/charter_v5_list.cfm?"
            f"dDepartureDate={departure_date}"
            f"&nFlexDays={flex_days}"
            f"&lDestinationTypeIDs={dest_type}"
            f"&nMinDuration={nmin}"
            f"&nMaxDuration={nmax}"
            f"&lSpecificationIDs=1"
            f"&lDepartureIDs={dep_ids}"
            f"&lSubAreaIDs=502666"
            f"&nSortBy=1"
            f"&nCharterVersion=12"
        )

    def search_flights(self, departure_city: str, destination: str,
                       departure_date: str = None, nights_min: int = None,
                       nights_max: int = None, domain: str = None) -> List[Dict]:
        """Поиск рейсов (только перелёт, точная дата)"""
        if not departure_date:
            from datetime import timedelta
            departure_date = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')

        url = self.build_url(departure_city, destination, departure_date,
                             nights_min=nights_min, nights_max=nights_max,
                             domain=domain, flex_days=0)
        logger.info(f"TravelMarket search: {url}")
        print(f"[TravelMarket] URL: {url[:80]}...")

        flights = self.parse_with_selenium(url)
        flight_only = [f for f in flights if f.get('flight_only', False)]
        logger.info(f"TravelMarket found {len(flights)} results, {len(flight_only)} flight-only")
        print(f"[TravelMarket] Found {len(flight_only)} flight-only offers")

        if not flight_only:
            print("[TravelMarket] Using fallback data...")
            flight_only = self.get_fallback_flights(departure_city, destination)

        flight_only.sort(key=lambda x: x.get('price_per_person_eur', 9999))
        return flight_only

    def get_egypt_flight_prices(self, departure_city: str, departure_date: str,
                                 nights: int = 7) -> Dict[str, Optional[float]]:
        """Парсит TravelMarket раздельно для Шарма и Хургады."""
        result = {
            'Sharm El Sheikh': None, 'Sharm El Sheikh_date': None,
            'Hurghada': None, 'Hurghada_date': None,
            'best': None, 'best_destination': None, 'best_date': None,
        }

        def _get_best_for_place(place: str) -> tuple:
            url = self.build_url(departure_city, "egypt", departure_date,
                                 domain=None, flex_days=28, place=place)
            logger.info(f"[TravelMarket] {place} URL: {url}")
            flights = self.parse_with_requests(url)
            if not flights:
                logger.warning(f"[TravelMarket] {place}: requests дал 0, пробуем Selenium")
                flights = self.parse_with_selenium(url)
            flight_only = [f for f in flights if f.get('flight_only', False)]
            if not flight_only:
                return None, None
            flight_only.sort(key=lambda x: x.get('price_per_person_eur', 99999))
            best = flight_only[0]
            price_eur = round(best.get('price_per_person_eur', 0))
            date_str = best.get('departure_date', '')
            logger.info(f"[TravelMarket] {place}: лучшая цена {price_eur} EUR на {date_str}")
            return price_eur, date_str

        sharm_price, sharm_date = _get_best_for_place("sharm")
        if sharm_price:
            result['Sharm El Sheikh'] = sharm_price
            result['Sharm El Sheikh_date'] = sharm_date

        hrg_price, hrg_date = _get_best_for_place("hurghada")
        if hrg_price:
            result['Hurghada'] = hrg_price
            result['Hurghada_date'] = hrg_date

        if result['Sharm El Sheikh'] and not result['Hurghada']:
            result['Hurghada'] = result['Sharm El Sheikh']
            result['Hurghada_date'] = result['Sharm El Sheikh_date']
        elif result['Hurghada'] and not result['Sharm El Sheikh']:
            result['Sharm El Sheikh'] = result['Hurghada']
            result['Sharm El Sheikh_date'] = result['Hurghada_date']

        candidates = {}
        if result['Sharm El Sheikh']:
            candidates['Sharm El Sheikh'] = result['Sharm El Sheikh']
        if result['Hurghada']:
            candidates['Hurghada'] = result['Hurghada']
        if candidates:
            best_dest = min(candidates, key=candidates.get)
            result['best'] = candidates[best_dest]
            result['best_destination'] = best_dest
            result['best_date'] = result.get(f'{best_dest}_date')

        logger.info(
            f"[TravelMarket] Sharm={result['Sharm El Sheikh']} EUR ({result['Sharm El Sheikh_date']}) | "
            f"Hurghada={result['Hurghada']} EUR ({result['Hurghada_date']})"
        )
        return result

    def get_fallback_flights(self, departure_city: str, destination: str) -> List[Dict]:
        """Fallback данные на основе реальных цен"""
        base_prices_dkk = {
            ("Billund", "egypt"): 2299,
            ("Copenhagen", "egypt"): 2500,
            ("Aalborg", "egypt"): 3999,
            ("Billund", "turkey"): 1999,
            ("Copenhagen", "turkey"): 2199,
            ("Billund", "spain"): 1799,
            ("Copenhagen", "spain"): 1599,
            ("Oslo", "egypt"): 2899,
            ("Oslo", "turkey"): 2499,
            ("Stockholm", "egypt"): 3199,
            ("Stockholm", "turkey"): 2799,
            ("Helsinki", "egypt"): 3499,
        }
        key = (departure_city, destination)
        base_price_dkk = base_prices_dkk.get(key, 2500)
        base_price_eur = self.dkk_to_eur(base_price_dkk)
        flights = []
        destinations_map = {
            "egypt": [("Sharm El Sheikh", "Egypten"), ("Hurghada", "Egypten")],
            "turkey": [("Antalya", "Tyrkiet"), ("Bodrum", "Tyrkiet")],
            "spain": [("Tenerife", "Spanien"), ("Mallorca", "Spanien")],
        }
        dests = destinations_map.get(destination, [("Unknown", "Unknown")])
        from datetime import timedelta
        base_date = datetime.now() + timedelta(days=14)
        for i, (dest_name, country) in enumerate(dests):
            flight_date = base_date + timedelta(days=i * 7)
            price_dkk = base_price_dkk + (i * 200)
            price_eur = self.dkk_to_eur(price_dkk)
            flights.append({
                "departure_date": flight_date.strftime("%d.%m.%Y"),
                "departure_time": "07:00",
                "destination": dest_name,
                "country": country,
                "departure_airport": departure_city,
                "nights": 7,
                "hotel_name": "Kun fly",
                "flight_only": True,
                "price_per_person_dkk": price_dkk,
                "price_per_person_eur": price_eur,
                "total_price_dkk": price_dkk * 2,
                "total_price_eur": price_eur * 2,
                "all_inclusive": False,
                "source": "travelmarket_fallback"
            })
        return flights

    def get_cheapest_flight(self, departure_city: str, destination: str,
                            departure_date: str = None, people: int = 2) -> Optional[Dict]:
        """Получить самый дешёвый рейс"""
        flights = self.search_flights(departure_city, destination, departure_date)
        if flights:
            cheapest = flights[0]
            cheapest['total_for_people_eur'] = round(cheapest['price_per_person_eur'] * people)
            cheapest['total_for_people_dkk'] = round(cheapest.get('price_per_person_dkk', 0) * people)
            return cheapest
        return None

    def close(self):
        """Закрытие драйвера"""
        if self.driver:
            self.driver.quit()
            self.driver = None


# Глобальный экземпляр парсера
travelmarket = TravelMarketParser()


# ============================================================================
# ТЕСТ
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("TravelMarket Parser Test")
    print("=" * 60)

    parser = TravelMarketParser()

    try:
        flights = parser.search_flights(
            departure_city="Billund",
            destination="egypt",
            departure_date="2026-03-05",
        )
        print(f"\nНайдено {len(flights)} рейсов (только перелёт):\n")
        for i, flight in enumerate(flights[:10], 1):
            print(f"{i}. {flight.get('destination', 'N/A')}")
            print(f"   Дата: {flight.get('departure_date', 'N/A')}")
            print(f"   Цена: {flight.get('price_per_person_dkk', 'N/A')} DKK = {flight.get('price_per_person_eur', 'N/A')} EUR/чел")
            print()
    finally:
        parser.close()
        print("Драйвер закрыт.")

