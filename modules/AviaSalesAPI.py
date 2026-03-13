import aiohttp
import asyncio
import logging
import os
import typing
import secrets
import io
import json
import copy

from datetime import datetime as DT

from airline_codes.find_airline_codes import find_airline_codes_and_replace_origin, find_fallback_airline_codes
from airline_codes.travel_market_update import find_alternative_origins, find_alternative_destinations
from hotel_codes.find_hotel_codes import find_airport_codes
from airline_codes.travel_market_update import departures_ids as travelmarket_departures_ids

from modules.currency_convert import converter as CurrencyConverter

from amadeus import Client as AmadeusClient
from amadeus import ResponseError as AmadeusResponseError

from cache import AsyncLRU, AsyncTTL

class AviaSalesAPI:
    def __init__(self):
        self.api_key = os.getenv('AVIASALES_API_KEY')
        self.base_url = os.getenv('AVIASALES_BASE_URL')
        self.cached_airport_codes = []
        self.cached_city_codes = []

    async def execute_command(self, method: str, params: dict = None, base_url: str = None, request_type: typing.Literal["POST", "GET"] = 'POST') -> typing.Union[dict, None]:
        if params is None:
            params = {}
        params['token'] = self.api_key
        url = f"{base_url or self.base_url}{method}"
        logging.info(f"Выполнение команды: {url} с параметрами: {params}")
        async with aiohttp.ClientSession() as session:
            async with session.request(request_type, url, params=params) as response:
                try:
                    return await response.json(encoding='utf-8')
                except aiohttp.ContentTypeError:
                    logging.error(f"Failed to parse JSON response: {await response.text()}")
                    return None
                
    @AsyncLRU(maxsize=1024)
    async def find_airport_city_name(self, airport_code: str) -> typing.Union[dict, None]:
        if not self.cached_airport_codes:
            self.cached_airport_codes = await self.execute_command('/data/en/airports.json', request_type='GET')
            if not self.cached_airport_codes:
                return None
            
        city_code = None
            
        for airport in self.cached_airport_codes:
            if airport['code'] == airport_code and airport["iata_type"] == "airport":
                city_code = airport["city_code"]
                break

        if city_code:
            if not self.cached_city_codes:
                self.cached_city_codes = await self.execute_command('/data/ru/cities.json', request_type='GET')
                if not self.cached_city_codes:
                    return None
                
            for city in self.cached_city_codes:
                if city['code'] == city_code and city["has_flightable_airport"]:
                    return city["name"]

        return

    @AsyncLRU(maxsize=1024)
    async def search_city(self, city_name: str) -> typing.Union[dict, None]:

        result = await self.execute_command(
            '/places2', 
            {
                'types[]': 'city',
                'locale': 'ru',
                'term': city_name
            },
            base_url="https://autocomplete.travelpayouts.com"
        )

        return result[0].get('code') if result else None
    
    # Фильтрация результатов согласно требованиям
    async def validate_response(self, response: typing.Union[typing.Dict, typing.List], city_code: str, destination_code: str) -> typing.Optional[dict]:
        for flight in response:
            # Проверяем, что аэропорты вылета и прилета совпадают в обе стороны
            if len(flight['itineraries']) != 2:
                continue
                
            outbound = flight['itineraries'][0]
            inbound = flight['itineraries'][1]
            
            if (outbound['segments'][0]['departure']['iataCode'] != inbound['segments'][-1]['arrival']['iataCode'] or
                outbound['segments'][-1]['arrival']['iataCode'] != inbound['segments'][0]['departure']['iataCode']):
                continue
                
            # Проверяем количество пересадок
            if len(outbound['segments']) > 2 or len(inbound['segments']) > 2:
                continue
                
            # Проверяем пересадки и авиакомпании
            valid_connection = True
            
            if len(outbound['segments']) == 2:
                first_arrival = DT.fromisoformat(outbound['segments'][0]['arrival']['at'].replace('Z', '+00:00'))
                second_departure = DT.fromisoformat(outbound['segments'][1]['departure']['at'].replace('Z', '+00:00'))
                connection_time = (second_departure - first_arrival).total_seconds() / 3600
                if (connection_time > 6 or 
                    outbound['segments'][0]['carrierCode'] != outbound['segments'][1]['carrierCode']):
                    valid_connection = False
                    
            if len(inbound['segments']) == 2:
                first_arrival = DT.fromisoformat(inbound['segments'][0]['arrival']['at'].replace('Z', '+00:00'))
                second_departure = DT.fromisoformat(inbound['segments'][1]['departure']['at'].replace('Z', '+00:00'))
                connection_time = (second_departure - first_arrival).total_seconds() / 3600
                if (connection_time > 6 or 
                    inbound['segments'][0]['carrierCode'] != inbound['segments'][1]['carrierCode']):
                    valid_connection = False
            
            if not valid_connection:
                continue

            logging.info(f"Найденный рейс: {flight}")
            return {
                "price": float(flight['price']['total']),
                "currency": flight['price']['currency'],
                "itineraries": flight['itineraries'],
                "source": "amadeus",
                "alternative": False,
                "from": city_code,
                "to": destination_code
            }
        return None
    
    @AsyncTTL(time_to_live=600)
    async def _search_ticket_cached(self, user_id: str, city_code: str, date_from: DT, date_to: DT, destination_code: str, currency: str = 'RUB', direct: bool = False, exact_departure_date: bool = False, exact_arrival_date: bool = False, passengers: typing.List[typing.Dict[str, int]] = None) -> typing.Optional[dict]:

        if destination_code == "TCI" and city_code in travelmarket_departures_ids.keys():
            logging.info(f"Пропускаем поиск билетов из {city_code} в TCI, так как это вылет из скандинавской страны и направление Тенерифе недоступно через Amadeus.")
            return None
        logging.info(f"Поиск билетов из {city_code} в {destination_code} от {date_from} до {date_to} с валютой {currency}, прямые перелеты {direct}")

        airline_codes, replace_origin, only_direct_flights = await find_airline_codes_and_replace_origin(city_code, destination_code)

        if not airline_codes:
            airline_codes = await find_fallback_airline_codes(destination_code)

        if only_direct_flights:
            logging.info("Ищутся только прямые рейсы из-за параметра direct_only")
            direct = True

        if replace_origin:
            logging.info(f"Замена аэропорта вылета из {city_code} на {replace_origin} из-за параметра replace_origin")
            city_code = replace_origin

        search_parameters = {
            "currencyCode": currency,
            "originDestinations": [
                {
                    "id": "1",
                    "originLocationCode": city_code,
                    "destinationLocationCode": destination_code,
                    "departureDateTimeRange": {
                        "date": date_from
                    },
                },
                {
                    "id": "2",
                    "originLocationCode": destination_code,
                    "destinationLocationCode": city_code,
                    "departureDateTimeRange": {
                        "date": date_to
                    },
                }
            ],
            "travelers": [],
            "sources": ["GDS"],
            "searchCriteria": {
                "flightFilters": {
                    "connectionRestriction": {
                        "maxNumberOfConnections": 2,
                        "nonStopPreferred": direct
                    }
                }
            }
        }

        if not passengers:
            search_parameters["travelers"].append(
                {
                    "id": "1",
                    "travelerType": "ADULT"
                }
            )

        if passengers:
            for passenger_id, passenger_info in enumerate(passengers, 1):
                passenger_info["id"] = str(passenger_id)
                search_parameters["travelers"].append(passenger_info)

        if not exact_departure_date:
            search_parameters["originDestinations"][0]["departureDateTimeRange"]["dateWindow"] = "I3D"
        if not exact_arrival_date:
            search_parameters["originDestinations"][1]["departureDateTimeRange"]["dateWindow"] = "I3D"

        if airline_codes:
            search_parameters["searchCriteria"]["flightFilters"]["carrierRestrictions"] = {
                "includedCarrierCodes": airline_codes
            }

        logging.info(f"Параметры поиска: {search_parameters}")

        response = (await asyncio.to_thread(amadeus_client.shopping.flight_offers_search.post, search_parameters)).data

        result = await self.validate_response(response, city_code, destination_code)

        if result:
            with io.StringIO() as buffer:
                buffer.write("Параметры поиска и сам билет:\n\n")
                buffer.write(json.dumps(search_parameters, ensure_ascii=False, indent=4))
                buffer.write("\n\n")
                buffer.write(json.dumps(result, ensure_ascii=False, indent=4))
                buffer.seek(0)
            logging.info(f"Найден результат: {result}")
        
        # пробуем alternative origins/destinations
        alternative_origins = await find_alternative_origins(city_code)
        alternative_destinations = await find_alternative_destinations(destination_code)

        # всегда включаем оригинальные origin и destination
        all_origins = [city_code] + (alternative_origins or [])
        all_destinations = [destination_code] + (alternative_destinations or [])

        cheapest_result = None
        cheapest_result_search_parameters = None

        for alt_origin in all_origins:
            for alt_dest in all_destinations:
                logging.info(f"Ищем с {alt_origin} → {alt_dest}")

                search_parameters["originDestinations"][0]["originLocationCode"] = alt_origin
                search_parameters["originDestinations"][0]["destinationLocationCode"] = alt_dest

                search_parameters["originDestinations"][1]["originLocationCode"] = alt_dest
                search_parameters["originDestinations"][1]["destinationLocationCode"] = alt_origin

                try:
                    response = (await asyncio.to_thread(
                        amadeus_client.shopping.flight_offers_search.post, search_parameters
                    )).data
                except Exception as e:
                    logging.error(f"Ошибка поиска для {alt_origin} → {alt_dest}: {e}")
                    continue

                result = await self.validate_response(response, alt_origin, alt_dest)
                if not result:
                    continue
                if alternative_destinations:
                    result["alternative"] = True

                if not cheapest_result or result["price"] < cheapest_result["price"]:
                    cheapest_result = result
                    cheapest_result_search_parameters = search_parameters
                    logging.info(
                        f"Новый самый дешёвый вариант: {cheapest_result['price']} {cheapest_result['currency']} "
                        f"({alt_origin} → {alt_dest})"
                    )

        with io.StringIO() as buffer:
            buffer.write("Параметры поиска" + (" и сам билет:" if cheapest_result else "") + "\n\n")
            buffer.write(json.dumps(cheapest_result_search_parameters or search_parameters, ensure_ascii=False, indent=4))
            if cheapest_result:
                buffer.write("\n\n")
                buffer.write(json.dumps(cheapest_result, ensure_ascii=False, indent=4))
            buffer.seek(0)

        logging.info(f"Самый дешёвый найденный альтернативный вариант: {cheapest_result}")

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
    
    async def search_ticket(self, user_id: str, city_code: str, country_code: typing.Optional[str], destination: str, regioncode: str, date_from: typing.Union[str, DT], date_to: typing.Union[str, DT], currency: str = 'RUB', direct: bool = False, exact_departure_date: bool = False, exact_arrival_date: bool = False, passengers: typing.List[typing.Dict[str, int]] = None) -> typing.Optional[dict]:
        
        city_code = copy.deepcopy(city_code)
        country_code = copy.deepcopy(country_code)
        destination = copy.deepcopy(destination)
        regioncode = copy.deepcopy(regioncode)
        date_from = copy.deepcopy(date_from)
        date_to = copy.deepcopy(date_to)
        currency = copy.deepcopy(currency)
        direct = copy.deepcopy(direct)
        exact_departure_date = copy.deepcopy(exact_departure_date)
        exact_arrival_date = copy.deepcopy(exact_arrival_date)
        passengers = copy.deepcopy(passengers)
        
        if regioncode in ['71', '70', '69'] and country_code == '14':
            destination_code = 'BCN'
            logging.info(f"Используем код {destination_code} (Барселона) для {destination}")
        elif not destination:
            found_airport_codes = await find_airport_codes(country_code)
            if not found_airport_codes:
                return None
            destination_code = secrets.choice(found_airport_codes)
        else:
            destination_code = await self.search_city(destination)
        if not destination_code:
            return None
        if isinstance(date_from, DT):
            date_from = date_from.strftime('%Y-%m-%d')
        if isinstance(date_to, DT):
            date_to = date_to.strftime('%Y-%m-%d')

        return await self._search_ticket_cached(
            user_id, city_code, date_from, date_to, destination_code, currency, direct, exact_departure_date, exact_arrival_date, passengers
        )
            

amadeus_client = AmadeusClient(
    client_id=os.getenv('AMADEUS_CLIENT_ID'), 
    client_secret=os.getenv('AMADEUS_CLIENT_SECRET'),
    hostname="production"
)
        
aviasales = AviaSalesAPI()