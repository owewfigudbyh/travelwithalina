import asyncio
import logging
import os
import io
import json
import copy
import re
import typing
import secrets
import traceback

from urllib.parse import urlencode

from playwright.async_api     import async_playwright
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
from playwright.async_api._generated import Playwright as AsyncPlaywright
from playwright.async_api._generated import Browser, BrowserContext

from datetime import datetime as DT
from datetime import date, timedelta

from cache import AsyncLRU, AsyncTTL

from modules.currency_convert           import converter as CurrencyConverter
from airline_codes.travel_market_update import departures_ids, places_ids, find_travelmarket_country_by_tourvisor_code
from airline_codes.find_airline_codes   import find_airline_codes_and_replace_origin
from airline_codes.travel_market_update import find_alternative_origins, find_alternative_destinations
from modules.AviaSalesAPI               import aviasales

# https://www.travelmarket.dk/charter_v5_list.cfm?ddeparturedate=2025-10-08&nflexdays=14&ldepartureids=499708&lplaceids=59338&nminduration=7&nmaxduration=11&lspecificationids=1&ldestinationtypeids=1,2

class TravelMarketAPI:
    def __init__(self):
        self.base_urls = {
            "Sweden": "https://www.travelmarket.se",    # Швеция
            "Denmark": "https://www.travelmarket.dk",   # Дания
            "Norway": "https://www.travelmarket.no",    # Норвегия
            "Finland": "https://www.travelmarket.fi",   # Финляндия
        }
        self.country_currency = {"Denmark": "DKK", "Norway": "NOK", "Sweden": "SEK", "Finland": "EUR"}

        self.playwright: AsyncPlaywright = None
        self.browser: Browser = None
        self.context: BrowserContext = None

        self.DATE_RE = re.compile(r'(\d{1,2})\s*/\s*(\d{1,2})')

    def extract_day_month(self, text: str, zero_pad: bool = False) -> typing.Optional[str]:
        if not text:
            return None
        m = self.DATE_RE.search(text)
        if not m:
            return None
        day = int(m.group(1))
        month = int(m.group(2))
        if not (1 <= day <= 31 and 1 <= month <= 12):
            return None
        if zero_pad:
            return f"{day:02d}/{month:02d}"
        return f"{day}/{month}"

    def daymonth_to_date(self, daymonth: str, prefer_future: bool = True) -> typing.Optional[date]:
        try:
            d, m = map(int, daymonth.split("/"))
            year = DT.now().year
            dt = date(year, m, d)
            if prefer_future and dt < DT.now().date():
                dt = date(year + 1, m, d)
            return dt
        except Exception:
            return None

    async def _ensure_browser(self):
        if not self.playwright:
            self.playwright = await async_playwright().start()
        if not self.browser:
            self.browser = await self.playwright.chromium.launch(headless=True)
        if not self.context:
            self.context = await self.browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
        logging.info("Browser started automatically")

    async def stop(self):
        if self.context:
            await self.context.close()
            self.context = None
        if self.browser:
            await self.browser.close()
            self.browser = None
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None
        logging.info("Browser stopped")

    @AsyncTTL(time_to_live=300)
    async def parse_ticket_price(self, search_parameters: dict, country: typing.Literal["Denmark", "Norway", "Sweden", "Finland"], currency: str, people: int) -> typing.Optional[typing.Tuple[int, date, date, str]]:
        url = f"{self.base_urls[country]}/charter_v5_list.cfm"

        final_url = f"{url}?{urlencode(search_parameters)}"
        logging.info(f"URL для поиска на TravelMarket: {final_url}")

        # await page.screenshot(path="screenshot.png", full_page=True)

        price = None
        departure_date = None
        arrival_date = None
        destination = None
        try:

            await self._ensure_browser()
            page = await self.context.new_page()
            await page.goto(final_url)

            await page.wait_for_load_state("domcontentloaded")

            # ".tse__charter__result__element__info__price--total--price"
            price_element = await page.wait_for_selector(".tse__charter__result__element__info__price--price", timeout=5000)
            if price_element:
                price_text = await price_element.inner_text()
                if price_text:

                    # Ждём появления блока с датой вылета   
                    dep_elem = await page.wait_for_selector(
                        ".tse__charter__result__element__info--date", timeout=500
                    )
                    
                    dur_elem = await page.wait_for_selector(
                        ".tse__charter__result__element__info--duration", timeout=500
                    )

                    dest_elem = await page.wait_for_selector(
                        ".tse__charter__result__element__info--location", timeout=500
                    )

                    price = int(re.sub(r"[^\d]", "", price_text))

                    if dep_elem:
                        dep_text: str = (await dep_elem.inner_text()).strip()
                        departure_date = self.daymonth_to_date(self.extract_day_month(dep_text.splitlines()[0]), prefer_future=True)

                    if dur_elem:
                        dur_text: str = (await dur_elem.inner_text()).strip()
                        duration = int(re.sub(r"[^\d]", "", dur_text))
                        arrival_date = departure_date + timedelta(days=duration)

                    if dest_elem:
                        dest_text: str = (await dest_elem.inner_text()).strip()
                        destination = dest_text.splitlines()[0]
                    
    

        except PlaywrightTimeoutError:
            logging.warning(f"Price element not found | Returned None\n{traceback.format_exc()}")
        finally:
            await page.close()

        if not price:
            return (None, None, None, None, final_url)

        # конвертация валюты
        base_currency = self.country_currency.get(country, "EUR")

        price = await CurrencyConverter.convert(base_currency, currency, price)
        if people > 1:
            price = price * people

        return (destination, int(price), departure_date, arrival_date, final_url)

    @AsyncTTL(time_to_live=600)
    async def search_ticket(
        self,
        user_id: str,
        departure_airport: str,
        inbound_airport: str,
        country_code: typing.Optional[str],
        regioncode: str,
        departure_date: typing.Union[str, DT],
        exact_departure_date: bool = True,
        nightsfrom: int = 7,
        nightsto: int = 11,
        currency: str = "EUR",
        passengers: typing.List[typing.Dict[str, int]] = None
    ) -> typing.Optional[typing.Dict[str, date]]:
        
        departure_airport = copy.deepcopy(departure_airport)
        inbound_airport = copy.deepcopy(inbound_airport)
        country_code = copy.deepcopy(country_code)
        regioncode = copy.deepcopy(regioncode)
        departure_date = copy.deepcopy(departure_date)
        exact_departure_date = copy.deepcopy(exact_departure_date)
        nightsfrom = copy.deepcopy(nightsfrom)
        nightsto = copy.deepcopy(nightsto)
        currency = copy.deepcopy(currency)
        passengers = copy.deepcopy(passengers)

        travelmarket_country_code = None
        result = None
        
        if regioncode in ['71', '70', '69'] and country_code == '14':
            logging.info(f"Используем код BCN (Барселона) для {inbound_airport}")
            inbound_airport = 'BCN'
        elif not inbound_airport:
            travelmarket_country_code = await find_travelmarket_country_by_tourvisor_code(country_code)
            if not travelmarket_country_code:
                return None
            inbound_airport = None
        else:
            inbound_airport = await aviasales.search_city(inbound_airport)
        if not inbound_airport:
            if not travelmarket_country_code:
                return None 
        
        if isinstance(departure_date, DT):
            departure_date = departure_date.strftime('%Y-%m-%d')

        airline_codes, replace_origin, only_direct_flights = await find_airline_codes_and_replace_origin(departure_date, inbound_airport)

        if replace_origin:
            logging.info(f"Замена аэропорта вылета из {departure_airport} на {replace_origin} из-за параметра replace_origin")
            departure_airport = replace_origin

        if passengers:
            people = 0
            for passenger_info in passengers:
                if passenger_info.get("travelerType") != "HELD_INFANT":
                    people += 1
        else:
            people = 1

        dep = departures_ids.get(departure_airport, {})
        inbound_id = places_ids.get(inbound_airport) if inbound_airport else None

        if not dep or not inbound_id:
            if (not country_code and not inbound_id) or (not dep):
                logging.warning(f"Не найден ID для аэропортов {departure_airport} | {inbound_airport} | Поиск отменен.")
                return None

        departure_id, country = dep.get("departureId"), dep.get("country")
        if not departure_id or not country:
            logging.warning(f"Не найден ID для аэропортов {departure_airport} | {inbound_airport} | Поиск отменен.")
            return None

        if isinstance(departure_date, DT):
            departure_date = departure_date.strftime('%Y-%m-%d')

        # Основные параметры поиска
        search_parameters = {
            "ddeparturedate": departure_date,
            "nflexdays": "0" if exact_departure_date else "3",
            "ldepartureids": departure_id,
            "lplaceids": inbound_id,
            "nminduration": str(nightsfrom),
            "nmaxduration": str(nightsto),
            # "lspecificationids": "1",
        }

        if travelmarket_country_code:
            search_parameters.pop("lplaceids", None)
            search_parameters["lcountryids"] = travelmarket_country_code

        if not travelmarket_country_code and not search_parameters.get("lplaceids"):
            logging.warning(f"Нет ни страны, ни места назначения для поиска {departure_airport} → {inbound_airport} | Поиск отменен.")
            return None

        try:
            ticket_destination, price, ticket_departure_date, ticket_arrival_date, travelmarket_search_url = await self.parse_ticket_price(search_parameters, country, currency, people)
        except Exception as e:
            price, ticket_departure_date, ticket_arrival_date, travelmarket_search_url = None, None, None, "Отсутствует"
            logging.error(f"Ошибка поиска для {departure_airport} → {inbound_airport}: {e}")

        if price:
            logging.info(f"Найдена цена {price} {currency} за {people} человека")
            with io.StringIO() as buffer:
                buffer.write("Параметры поиска и сам билет:\n\n")
                buffer.write(json.dumps(search_parameters, ensure_ascii=False, indent=4))
                buffer.write("\n\nСсылка на поиск на TravelMarket:\n")
                buffer.write(travelmarket_search_url)
                buffer.write("\n\n")
                buffer.write(json.dumps({"price": price, "currency": currency, "source": "travelmarket", "departure_date": str(ticket_departure_date) if ticket_departure_date else "", "arrival_date": str(ticket_arrival_date) if ticket_arrival_date else "", "alternative": False}, ensure_ascii=False, indent=4))
                buffer.seek(0)

            result = {
                "price": price,
                "currency": currency,
                "source": "travelmarket",
                "departure_date": ticket_departure_date,
                "arrival_date": ticket_arrival_date,
                "alternative": False,
                "from": departure_airport,
                "to": inbound_airport if inbound_airport else await aviasales.search_city(ticket_destination)
            }
        
        # пробуем alternative origins/destinations
        alternative_origins = await find_alternative_origins(departure_airport)
        alternative_destinations = await find_alternative_destinations(inbound_airport)

        # всегда включаем оригинальные origin и destination
        all_origins = [departure_airport] + (alternative_origins or [])
        all_destinations = [inbound_airport] + (alternative_destinations or [])

        cheapest_result = None
        cheapest_result_search_parameters = None
        cheapest_search_url = None
        ticket_search_url = None

        if search_parameters.get("lcountryids"):
            search_parameters.pop("lcountryids", None)

        for alt_origin in all_origins:
            for alt_dest in all_destinations:
                if alt_origin == departure_airport and alt_dest == inbound_airport:
                    continue
                
                logging.info(f"Ищем с {alt_origin} → {alt_dest}")

                dep = departures_ids.get(alt_origin, {})
                inbound_id = places_ids.get(alt_dest) if alt_dest else None

                if not dep or not inbound_id:
                    logging.warning(f"Не найден ID для аэропортов {alt_origin} | {alt_dest} | Поиск отменен.")
                    continue

                departure_id, country = dep.get("departureId"), dep.get("country")
                if not departure_id or not country:
                    logging.warning(f"Не найден ID для аэропортов {alt_origin} | {alt_dest} | Поиск отменен.")
                    continue

                search_parameters["ldepartureids"] = departure_id
                search_parameters["lplaceids"] = inbound_id

                if not search_parameters.get("lcountryids") and not search_parameters.get("lplaceids"):
                    logging.warning(f"Нет ни страны, ни места назначения для поиска {alt_origin} → {alt_dest} | Поиск отменен.")
                    continue

                try:
                    ticket_destination, price, ticket_departure_date, ticket_arrival_date, ticket_search_url = await self.parse_ticket_price(search_parameters, country, currency, people)
                except Exception as e:
                    logging.error(f"Ошибка поиска для {alt_origin} → {alt_dest}: {e}")
                    continue

                if not price:
                    continue

                if not cheapest_result or price < cheapest_result["price"]:
                    cheapest_result = {
                        "price": price,
                        "currency": currency,
                        "source": "travelmarket",
                        "departure_date": ticket_departure_date,
                        "arrival_date": ticket_arrival_date,
                        "alternative": True if alternative_destinations else False,
                        "from": alt_origin,
                        "to": alt_dest
                    }
                    cheapest_result_search_parameters = search_parameters
                    cheapest_search_url = ticket_search_url
                    logging.info(
                        f"Новый самый дешёвый альтернативный вариант: {cheapest_result['price']} {cheapest_result['currency']} "
                        f"({alt_origin} → {alt_dest})"
                    )

        with io.StringIO() as buffer:
            buffer.write("Параметры поиска" + (" и сам билет:" if cheapest_result else "") + "\n\n")
            buffer.write(json.dumps(cheapest_result_search_parameters or search_parameters, ensure_ascii=False, indent=4))
            if cheapest_result:
                buffer.write("\n\n")
                cheapest_result_copy = cheapest_result.copy()
                cheapest_result_copy["departure_date"] = str(cheapest_result_copy["departure_date"]) if cheapest_result_copy["departure_date"] else ""
                cheapest_result_copy["arrival_date"] = str(cheapest_result_copy["arrival_date"]) if cheapest_result_copy["arrival_date"] else ""
                buffer.write(json.dumps(cheapest_result_copy, ensure_ascii=False, indent=4))
            if cheapest_search_url:
                buffer.write("\n\nСсылка на поиск на TravelMarket:\n")
                buffer.write(cheapest_search_url)
            elif ticket_search_url:
                buffer.write("\n\nПоследняя ссылка на поиск на TravelMarket:\n")
                buffer.write(ticket_search_url)
            buffer.seek(0)

        if result and not cheapest_result:
            return result
        elif cheapest_result and not result:
            return cheapest_result
        elif cheapest_result and result:
            cheapest_result_price_for_comparison = (await CurrencyConverter.convert(cheapest_result['currency'], "EUR", cheapest_result['price'])) + 300
            result_price_for_comparison = await CurrencyConverter.convert(result['currency'], "EUR", result['price'])
            if cheapest_result_price_for_comparison < result_price_for_comparison:
                return cheapest_result
            else:
                return result


travelmarket = TravelMarketAPI()