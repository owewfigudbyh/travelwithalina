#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
FlightPowers API - поиск рейсов через Google Flights Live API (RapidAPI)
"""

import json
import logging
import http.client
from datetime import datetime, timedelta
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# RapidAPI ключ
RAPIDAPI_KEY = "473c42b155msh01a62509602eb76p12b718jsn47d41ee523ea"
RAPIDAPI_HOST = "google-flights-live-api.p.rapidapi.com"

# Коды аэропортов
AIRPORT_CODES = {
    # Скандинавия
    "Copenhagen": "CPH",
    "Billund": "BLL",
    "Aalborg": "AAL",
    "Aarhus": "AAR",
    "Oslo": "OSL",
    "Stockholm": "ARN",
    "Goteborg": "GOT",
    "Helsinki": "HEL",
    "Tallinn": "TLL",
    "Malmo": "CPH",  # Мальмё использует Копенгаген

    # Египет
    "Sharm El Sheikh": "SSH",
    "Hurghada": "HRG",

    # Турция
    "Antalya": "AYT",
    "Istanbul": "IST",
    "Bodrum": "BJV",

    # Испания
    "Barcelona": "BCN",
    "Mallorca": "PMI",
    "Tenerife": "TFS",
    "Madrid": "MAD",
}

# Направления по странам
DESTINATIONS = {
    "egypt": ["SSH", "HRG"],  # Шарм, Хургада
    "turkey": ["AYT"],  # Анталья
    "spain": ["BCN", "PMI", "TFS"],  # Барселона, Майорка, Тенерифе
}


class FlightPowersAPI:
    """Поиск рейсов через Google Flights Live API"""

    def __init__(self):
        self.api_key = RAPIDAPI_KEY
        self.api_host = RAPIDAPI_HOST

    def search_oneway(self, from_airport: str, to_airport: str,
                      departure_date: str) -> Optional[Dict]:
        """
        Поиск рейса в одну сторону

        Args:
            from_airport: код аэропорта вылета (CPH, BLL, etc)
            to_airport: код аэропорта прилёта (SSH, HRG, etc)
            departure_date: дата в формате YYYY-MM-DD
        """
        try:
            conn = http.client.HTTPSConnection(self.api_host)

            payload = json.dumps({
                "departure_date": departure_date,
                "from_airport": from_airport,
                "to_airport": to_airport
            })

            headers = {
                'x-rapidapi-key': self.api_key,
                'x-rapidapi-host': self.api_host,
                'Content-Type': 'application/json'
            }

            logger.info(f"[FlightPowers] Searching: {from_airport} -> {to_airport} on {departure_date}")

            conn.request("POST", "/api/google_flights/oneway/v1", payload, headers)
            res = conn.getresponse()
            data = res.read()

            result = json.loads(data.decode("utf-8"))

            if 'error' in result:
                logger.error(f"[FlightPowers] API Error: {result.get('error')}")
                return None

            return result

        except Exception as e:
            logger.error(f"[FlightPowers] Request error: {e}")
            return None

    def search_roundtrip(self, from_airport: str, to_airport: str,
                         departure_date: str, return_date: str,
                         adults: int = 1) -> Optional[Dict]:
        """
        Поиск рейса туда-обратно (roundtrip).

        Документация API:
          passengers: [1]*adults — правильный формат (1=adult).
          API возвращает total_price_as_number — цена за ВСЕХ пассажиров.
          max_departure_stops / max_return_stops = 0 → только прямые рейсы.
        """
        try:
            import requests as _req
            url = f"https://{self.api_host}/api/google_flights/roundtrip/v1"
            headers = {
                'x-rapidapi-key': self.api_key,
                'x-rapidapi-host': self.api_host,
                'Content-Type': 'application/json'
            }
            payload = {
                "departure_date": departure_date,
                "return_date": return_date,
                "from_airport": from_airport,
                "to_airport": to_airport,
                "passengers": [1] * adults,   # [1,1] для 2 взрослых
                "max_departure_stops": 0,      # только прямые рейсы туда
                "max_return_stops": 0,         # только прямые рейсы обратно
            }

            logger.info(f"[FlightPowers] Roundtrip: {from_airport} -> {to_airport}, {departure_date} - {return_date}, {adults} adults, passengers={payload['passengers']}")

            resp = _req.post(url, json=payload, headers=headers, timeout=60)
            result = resp.json()

            if isinstance(result, dict) and 'error' in result:
                logger.error(f"[FlightPowers] API Error: {result.get('error')}")
                return None

            return result

        except Exception as e:
            logger.error(f"[FlightPowers] Request error: {e}")
            return None

    def parse_flight_price(self, api_response, adults: int = 1) -> Optional[float]:
        """Извлечение минимальной цены EUR из ответа API.
        Roundtrip API: total_price_as_number — цена за ВСЕХ пассажиров (USD).
        Возвращает цену за ОДНОГО человека в EUR.
        """
        info = self.get_best_flight_info(api_response, adults=adults)
        return info.get("price_per_person_eur") if info else None

    def get_best_flight_info(self, api_response, adults: int = 1) -> Optional[Dict]:
        """Извлекает лучший вариант из ответа API.
        Возвращает dict с ключами:
          price_per_person_eur — EUR за 1 чел
          buy_link             — ссылка на покупку (Google Flights)
          departure_date       — дата вылета
          return_date          — дата возврата
          airline              — авиакомпания
          departure_time       — время вылета
          return_time          — время вылета обратно
        или None если ничего не нашлось.
        """
        if not api_response:
            return None

        # Актуальный курс USD -> EUR
        try:
            import requests as req
            r = req.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
            USD_TO_EUR = r.json().get("rates", {}).get("EUR", 0.85)
        except Exception:
            USD_TO_EUR = 0.86

        try:
            # API всегда возвращает список
            items = api_response if isinstance(api_response, list) else []

            if not items:
                logger.warning(f"[FlightPowers] Пустой ответ API или неправильный формат")
                return None

            logger.info(f"[FlightPowers] API вернул {len(items)} вариантов для анализа")

            # Фильтруем только прямые рейсы (0 пересадок)
            direct_flights = []
            for item in items:
                # Проверяем количество пересадок
                total_stops = item.get("total_stops", -1)
                dep_stops = item.get("departure_flight_stops", -1)
                ret_stops = item.get("return_flight_stops", -1)

                # Берём только прямые (0 пересадок в обе стороны)
                if total_stops == 0 or (dep_stops == 0 and ret_stops == 0):
                    direct_flights.append(item)

            if not direct_flights:
                logger.warning(f"[FlightPowers] Нет прямых рейсов среди {len(items)} вариантов")
                # Если нет прямых, берём все (на случай если фильтр сработал неправильно)
                direct_flights = items

            logger.info(f"[FlightPowers] Прямых рейсов: {len(direct_flights)}")

            # Ищем минимальную цену среди прямых рейсов
            best_item = None
            best_price_usd = None

            for item in direct_flights:
                # Извлекаем цену (total_price_as_number = цена за ВСЕХ пассажиров в USD)
                price_total_usd = item.get("total_price_as_number")

                if price_total_usd and isinstance(price_total_usd, (int, float)) and price_total_usd > 0:
                    if best_price_usd is None or price_total_usd < best_price_usd:
                        best_price_usd = price_total_usd
                        best_item = item

            if not best_item or not best_price_usd:
                logger.warning(f"[FlightPowers] Не удалось найти валидную цену")
                return None

            # Делим на количество пассажиров (API отдаёт цену за ВСЕХ)
            per_person_usd = best_price_usd / max(adults, 1)
            per_person_eur = round(per_person_usd * USD_TO_EUR, 0)  # Округляем до целых EUR

            logger.info(
                f"[FlightPowers] Roundtrip: ${best_price_usd} за {adults} чел "
                f"→ ${per_person_usd:.1f}/чел = {per_person_eur} EUR/чел"
            )

            return {
                "price_per_person_eur": int(per_person_eur),
                "buy_link":       best_item.get("buy_link", ""),
                "departure_date": best_item.get("departure_date", ""),
                "return_date":    best_item.get("return_date", ""),
                "airline":        best_item.get("departure_flight_airline", ""),
                "departure_time": best_item.get("departure_flight_departure_description", ""),
                "return_time":    best_item.get("return_flight_departure_description", ""),
                "total_stops":    best_item.get("total_stops", 0),
            }

        except Exception as e:
            logger.error(f"[FlightPowers] Price parsing error: {e}")

        return None

    def search_egypt_flights(self, departure_city: str, departure_date: str,
                             nights: int = 7, people: int = 2) -> List[Dict]:
        """
        Поиск рейсов в Египет (Шарм + Хургада) — ONEWAY (в одну сторону).
        Цена oneway = стоимость перелёта туда (без обратного).

        Returns:
            Список рейсов с ценами (price_per_person_eur = цена oneway за 1 чел в EUR)
        """
        flights = []

        from_code = AIRPORT_CODES.get(departure_city, "CPH")

        # Ищем в оба египетских аэропорта — ONEWAY
        for dest_code in ["SSH", "HRG"]:
            dest_name = "Sharm El Sheikh" if dest_code == "SSH" else "Hurghada"

            logger.info(f"[FlightPowers] Oneway: {from_code} -> {dest_code} on {departure_date}")

            result = self.search_oneway(from_code, dest_code, departure_date)

            if result:
                price_eur = self.parse_flight_price(result)
                if price_eur and price_eur > 0:
                    flights.append({
                        "destination": dest_name,
                        "destination_code": dest_code,
                        "departure_airport": departure_city,
                        "departure_code": from_code,
                        "departure_date": departure_date,
                        "return_date": None,
                        "nights": nights,
                        "price_per_person_eur": round(price_eur),
                        "total_price_eur": round(price_eur * people),
                        "currency": "EUR",
                        "source": "flightpowers",
                        "is_roundtrip": False,
                    })
                    logger.info(f"[FlightPowers] {dest_name}: {round(price_eur)} EUR/чел (oneway)")
                else:
                    logger.warning(f"[FlightPowers] No price for {dest_name}")
            else:
                logger.warning(f"[FlightPowers] No result for {dest_name}")

        # Сортируем по цене
        flights.sort(key=lambda x: x.get('price_per_person_eur', 9999))

        return flights

    def search_turkey_flights(self, departure_city: str, departure_date: str,
                              nights: int = 7, people: int = 2) -> List[Dict]:
        """Поиск рейсов в Турцию — ROUNDTRIP (туда+обратно), только прямые"""
        flights = []

        from_code = AIRPORT_CODES.get(departure_city, "CPH")
        dep_dt = datetime.strptime(departure_date, "%Y-%m-%d")
        return_date = (dep_dt + timedelta(days=nights)).strftime("%Y-%m-%d")

        result = self.search_roundtrip(from_code, "AYT", departure_date, return_date, adults=people)

        if result:
            info = self.get_best_flight_info(result, adults=people)
            if info and info.get("price_per_person_eur", 0) > 0:
                price = info["price_per_person_eur"]
                flights.append({
                    "destination": "Antalya",
                    "destination_code": "AYT",
                    "departure_airport": departure_city,
                    "departure_code": from_code,
                    "departure_date": departure_date,
                    "return_date": return_date,
                    "nights": nights,
                    "price_per_person_eur": round(price),
                    "total_price_eur": round(price * people),
                    "currency": "EUR",
                    "source": "flightpowers",
                    "is_roundtrip": True,
                    "buy_link": info.get("buy_link", ""),
                    "airline": info.get("airline", ""),
                    "departure_time": info.get("departure_time", ""),
                })
                logger.info(f"[FlightPowers] ✅ Antalya: {round(price)} EUR/чел (roundtrip {departure_date}→{return_date}, link={'YES' if info.get('buy_link') else 'NO'})")

        return flights

    def search_spain_flights(self, departure_city: str, departure_date: str,
                             nights: int = 7, people: int = 2) -> List[Dict]:
        """Поиск рейсов в Испанию — ROUNDTRIP (туда+обратно), только прямые"""
        flights = []

        from_code = AIRPORT_CODES.get(departure_city, "CPH")
        dep_dt = datetime.strptime(departure_date, "%Y-%m-%d")
        return_date = (dep_dt + timedelta(days=nights)).strftime("%Y-%m-%d")

        spain_dests = [("BCN", "Barcelona"), ("PMI", "Mallorca"), ("TFS", "Tenerife")]

        for dest_code, dest_name in spain_dests:
            result = self.search_roundtrip(from_code, dest_code, departure_date, return_date, adults=people)

            if result:
                info = self.get_best_flight_info(result, adults=people)
                if info and info.get("price_per_person_eur", 0) > 0:
                    price = info["price_per_person_eur"]
                    flights.append({
                        "destination": dest_name,
                        "destination_code": dest_code,
                        "departure_airport": departure_city,
                        "departure_code": from_code,
                        "departure_date": departure_date,
                        "return_date": return_date,
                        "nights": nights,
                        "price_per_person_eur": round(price),
                        "total_price_eur": round(price * people),
                        "currency": "EUR",
                        "source": "flightpowers",
                        "is_roundtrip": True,
                        "buy_link": info.get("buy_link", ""),
                        "airline": info.get("airline", ""),
                        "departure_time": info.get("departure_time", ""),
                    })
                    logger.info(f"[FlightPowers] ✅ {dest_name}: {round(price)} EUR/чел (roundtrip {departure_date}→{return_date})")

        flights.sort(key=lambda x: x.get('price_per_person_eur', 9999))
        return flights


# Глобальный экземпляр
flightpowers = FlightPowersAPI()


# ============================================================================
# ТЕСТ
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print("=" * 60)
    print("FlightPowers API Test")
    print("=" * 60)

    api = FlightPowersAPI()

    # Тест поиска рейсов в Египет из Копенгагена
    print("\n🔍 Поиск рейсов: Copenhagen -> Egypt (март 2026)")
    print("-" * 40)

    flights = api.search_egypt_flights(
        departure_city="Copenhagen",
        departure_date="2026-03-15",
        nights=7,
        people=2
    )

    if flights:
        print(f"\n✅ Найдено {len(flights)} рейсов:\n")
        for f in flights:
            print(f"  {f['destination']}: {f['price_per_person_eur']} EUR/чел (в одну сторону)")
            print(f"    Дата вылета: {f['departure_date']}")
            print(f"    Маршрут: {f['departure_code']} -> {f['destination_code']}")
            print()
    else:
        print("\n❌ Рейсы не найдены")

    # Тест одного рейса
    print("\n" + "=" * 60)
    print("Тест одиночного запроса (roundtrip)")
    print("=" * 60)

    result = api.search_roundtrip("CPH", "SSH", "2026-03-15", "2026-03-22")
    if result:
        print(f"\nОтвет API:")
        print(json.dumps(result, indent=2, ensure_ascii=False)[:1000])

        price = api.parse_flight_price(result)
        print(f"\nИзвлечённая цена: {price} EUR")
    else:
        print("\n❌ Нет ответа от API")

