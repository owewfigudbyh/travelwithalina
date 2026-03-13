import logging
import os
import aiohttp
import io
import copy
import json
import asyncio
import traceback
import random

from datetime import datetime as DT
from datetime import timedelta
from pytz import timezone
from dateutil.parser import parse as duparse

from fuzzywuzzy import fuzz
from typing import Dict, Optional, Literal, Union, Tuple, List

from modules.currency_convert import converter as CurrencyConverter
from modules.database import get_used_hotels, add_used_hotels
from modules.AviaSalesAPI import aviasales
from modules.TravelMarketAPI import travelmarket

from hotel_codes.find_hotel_codes import if_ru_or_by_city, find_hotel_codes, find_aviasales_tax, find_other_regions_of_country, find_offer_regions
from airline_codes.travel_market_update import departures_ids as travelmarket_departures_ids

pattern_perplexity = '''Ты должен создать описание отеля {hotel_name} по следующим пунктам:
Питание
Расположение
Развлечения
Удобства
Для кого подходит
Подробно расписать в нескольких пунктах плюсы и минусы отеля
Например:
Отель "REEF OASIS BEACH RESORT" 4* находится в районе Шарм-Эль-Шейх и предлагает отличное соотношение цены и качества. Вот немного информации об этом отеле:
Питание: В отеле действует система "Все включено", предлагающая разнообразные блюда в формате шведского стола.
Расположение: Отель расположен на первой береговой линии, что обеспечивает легкий доступ к пляжу.
Удобства: Гости могут насладиться бассейнами, спа-центром и фитнес-залом, которые доступны на территории отеля.
Развлечения: В отеле проводятся развлекательные мероприятия и шоу-программы для всех возрастов.
Для кого подходит отель: Идеально подходит для семейных пар, а также для пар, ищущих спокойный отдых с красочными видами на море.
Плюсы: Отличное расположение в бухте с красивым коралловым рифом, большая зеленая территория, разнообразное питание, хорошая анимация, наличие аквапарка и нескольких бассейнов.

Минусы: Старый номерной фонд, требующий обновления, иногда недостаточное количество лежаков на пляже в высокий сезон, шумно в номерах близко к ресторанам, значительный подъем от пляжа к основной территории

Примечания:
В итогом ответе тебе нельзя упомянать никакие источники, откуда ты брал информацию'''

recommended_destinations = [{"country": {"name": "Испания", "code": "14"}, "cities": ["Тенерифе", "Гран-Канария", "Фуэртевентура"]}, {"country": {"name": "Египет", "code": "1"}, "cities": ["Шарм-эш-Шейх", "Хургада"]}]

def try_parsing_date(text):
    for fmt in ('%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            return DT.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('No valid date format found for %s' % text)

def is_summer_season(date: DT) -> bool:

    month = date.month
    day = date.day
    
    if month > 5 and month < 10:
        return True
    elif month == 5:
        return True
    elif month == 10:
        return day == 1
    else:
        return False

class TourVisorAPI:

    def __init__(self):
        self.login = os.getenv('TOURVISOR_LOGIN')
        self.password = os.getenv('TOURVISOR_PASSWORD')
        self.base_url = os.getenv('TOURVISOR_BASE_URL')
        self._country_cache = {}
        self._regions_cache = {}
        self._departure_cache = {'Абакан': '53', 'Актау': '74', 'Актобе': '73', 'Алматы': '60', 'Анапа': '63', 'Архангельск': '29', 'Астана': '59', 'Астрахань': '40', 'Атырау': '75', 'Баку': '109', 'Барнаул': '25', 'Без перелета': '99', 'Белгород': '32', 'Бишкек': '80', 'Благовещенск': '36', 'Братск': '45', 'Брест': '70', 'Брянск': '38', 'Бухара': '126', 'Варшава': '108', 'Вильнюс': '72', 'Винница': '86', 'Витебск': '71', 'Владивосток': '23', 'Владикавказ': '46', 'Волгоград': '27', 'Воронеж': '26', 'Гданьск': '113', 'Геленджик': '116', 'Гомель': '69', 'Горно-Алтайск': '124', 'Гродно': '83', 'Грозный': '96', 'Днепр': '87', 'Екатеринбург': '3', 'Ереван': '127', 'Запорожье': '88', 'Ивано-Франковск': '89', 'Иваново': '103', 'Ижевск': '64', 'Иркутск': '22', 'Казань': '10', 'Калининград': '17', 'Калуга': '95', 'Караганды': '76', 'Катовице': '114', 'Каунас': '107', 'Кемерово': '15', 'Киев': '58', 'Киров': '104', 'Кишинев': '122', 'Костанай': '68', 'Краснодар': '11', 'Красноярск': '12', 'Кривой Рог': '90', 'Курган': '119', 'Курск': '47', 'Кызылорда': '77', 'Липецк': '115', 'Львов': '91', 'Магнитогорск': '48', 'Махачкала': '94', 'Мин.Воды': '39', 'Минск': '57', 'Могилев': '82', 'Москва': '1', 'Мурманск': '30', 'Н.Новгород': '8', 'Наб.Челны': '61', 'Нальчик': '54', 'Нижневартовск': '34', 'Нижнекамск': '19', 'Николаев': '110', 'Новокузнецк': '16', 'Новосибирск': '9', 'Новый Уренгой': '67', 'Ноябрьск': '123', 'Одесса': '93', 'Омск': '21', 'Оренбург': '28', 'Орск': '49', 'П.Камчатский': '43', 'Павлодар': '66', 'Пенза': '65', 'Пермь': '2', 'Петрозаводск': '117', 'Петропавловск': '98', 'Псков': '118', 'Рига': '81', 'Ровно': '105', 'Ростов-на-Дону': '18', 'С.Петербург': '5', 'Самара': '7', 'Самарканд': '125', 'Саранск': '101', 'Саратов': '31', 'Семей': '121', 'Симферополь': '62', 'Сочи': '56', 'Ставрополь': '55', 'Сургут': '13', 'Сыктывкар': '41', 'Таллин': '111', 'Тараз': '97', 'Ташкент': '106', 'Томск': '52', 'Туркестан': '120', 'Тюмень': '14', 'Улан-Удэ': '42', 'Ульяновск': '50', 'Уральск': '84', 'Усть-Каменогорск': '78', 'Уфа': '4', 'Фергана': '128', 'Хабаровск': '20', 'Ханты-Мансийск': '35', 'Харьков': '33', 'Херсон': '92', 'Чебоксары': '51', 'Челябинск': '6', 'Череповец': '102', 'Черновцы': '112', 'Чита': '44', 'Шымкент': '79', 'Ю.Сахалинск': '24', 'Якутск': '37', 'Ярославль': '85'}
        self._meal_cache = {}
        # self._cache_hits = {'countries': 0, 'departures': 0, 'meals': 0}
        self._cache_hits = {'countries': 0, 'meals': 0, 'regions': 0}
        self._hotels_cache = {}
        self._hotels_cache_hits = {}
        self.MAX_CACHE_HITS = 100

    @staticmethod
    async def get_hotels_by_region(country_name: str, region_name: str, return_format: Literal["list", "ids"] = "list") -> Union[Tuple[List[Dict], Optional[str]], Tuple[str, Optional[str]]]:
        """
        Получает список всех отелей в указанном регионе страны.
        
        Args:
            country_name (str): Название страны
            region_name (str): Название региона
            return_format (str): Формат возврата - "list" для списка словарей или "ids" для строки с ID через запятую
            
        Returns:
            Union[Tuple[List[Dict], Optional[str]], Tuple[str, Optional[str]]]: 
                Кортеж из списка отелей (или строки с ID) и сообщения об ошибке (если есть)
        """
        try:
            # Получаем код страны
            country_code = await tourvisor.get_country_code(country_name)
            if not country_code:
                return ([] if return_format == "list" else ""), f"Страна '{country_name}' не найдена"
            
            # Получаем код региона
            region_code = await tourvisor.get_region_code(region_name)
            if not region_code:
                return ([] if return_format == "list" else ""), f"Регион '{region_name}' не найден в стране '{country_name}'"
            
            # Обновляем кэш отелей для этой страны
            await tourvisor.refresh_hotels_cache(country_code)
            
            # Получаем данные о всех отелях в стране
            data = await tourvisor.execute_command('/list.php', {'type': 'hotel', 'hotcountry': country_code})

            logging.info(f"Данные о отелях в стране: {data}")
            
            if not data or 'lists' not in data or 'hotels' not in data['lists'] or 'hotel' not in data['lists']['hotels']:
                return ([] if return_format == "list" else ""), "Не удалось получить данные об отелях"
            
            # Фильтруем отели по региону
            hotels_in_region = []
            hotel_ids = []

            logging.info(f"Список отелей в стране: {data['lists']['hotels']['hotel']}")
            
            for hotel in data['lists']['hotels']['hotel']:
                if 'region' in hotel and hotel['region'] == region_code:
                    hotels_in_region.append({
                        'id': hotel['id'],
                        'name': hotel['name'],
                        'stars': hotel.get('stars', 'Не указано'),
                        'rating': hotel.get('rating', 'Не указано'),
                        'regionname': hotel.get('regionname', region_name)
                    })
                    hotel_ids.append(hotel['id'])
            
            if not hotels_in_region:
                return ([] if return_format == "list" else ""), f"В регионе '{region_name}' не найдено отелей"
            
            if return_format == "ids":
                return ",".join(hotel_ids), None
            else:
                return hotels_in_region, None
            
        except Exception as e:
            logging.error(f"Ошибка при получении списка отелей: {str(e)}")
            return ([] if return_format == "list" else ""), f"Произошла ошибка: {str(e)}"

    @staticmethod
    async def get_all_hotels_ids_by_region(country_name: str, region_name: str) -> str:
        """
        Получает строку с ID всех отелей в указанном регионе через запятую.
        
        Args:
            country_name (str): Название страны
            region_name (str): Название региона
            
        Returns:
            str: Строка с ID отелей через запятую (например, "1,2,3,4,5")
        """
        hotel_ids, error = await TourVisorAPI.get_hotels_by_region(country_name, region_name, return_format="ids")
        if error:
            logging.error(f"Ошибка при получении ID отелей: {error}")
            return ""
        return hotel_ids

    @staticmethod
    async def get_all_regions_in_country(country_name: str) -> Tuple[List[Dict], Optional[str]]:
        """
        Получает список всех регионов в указанной стране.
        
        Args:
            country_name (str): Название страны
            
        Returns:
            Tuple[List[Dict], Optional[str]]: Кортеж из списка регионов и сообщения об ошибке (если есть)
        """
        try:
            # Получаем код страны
            country_code = await tourvisor.get_country_code(country_name)
            if not country_code:
                return [], f"Страна '{country_name}' не найдена"
            
            # Обновляем кэш регионов
            await tourvisor.refresh_cache('regions')
            
            # Получаем данные о всех регионах
            data = await tourvisor.execute_command('/list.php', {'type': 'region', 'country': country_code})
            
            if not data or 'lists' not in data or 'regions' not in data['lists'] or 'region' not in data['lists']['regions']:
                return [], "Не удалось получить данные о регионах"
            
            regions = []
            for region in data['lists']['regions']['region']:
                if region.get('countrycode') == country_code:
                    regions.append({
                        'id': region['id'],
                        'name': region['name']
                    })
            
            if not regions:
                return [], f"В стране '{country_name}' не найдено регионов"
                
            return regions, None
            
        except Exception as e:
            logging.error(f"Ошибка при получении списка регионов: {str(e)}")
            return [], f"Произошла ошибка: {str(e)}"
        
    @staticmethod
    async def get_subregions_in_region(country_name: str, region_code: str):
        """
        Получает список всех подрегионов в указанном регионе.

        Args:
            country_name (str): Название страны
            region_name (str): Название региона

        """

        data = await tourvisor.execute_command('/list.php', {'type': 'subregion', 'regcountry': country_name})

        if data and 'lists' in data and 'subregions' in data['lists'] and 'subregion' in data['lists']['subregions']:
            return [(subregion['id'], subregion['name']) for subregion in data['lists']['subregions']['subregion'] if subregion['parentregion'] == region_code]
        
        
    async def execute_command(self, method, params=None):
        if params is None:
            params = {}
        params['format'] = 'json'
        params['authlogin'] = self.login
        params['authpass'] = self.password
        url = f"{self.base_url}{method}"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params) as response:
                try:
                    return await response.json(encoding='utf-8')
                except aiohttp.ContentTypeError:
                    logging.error(f"Failed to parse JSON response: {await response.text()}")
                    return None
            
    async def refresh_hotels_cache(self, country_id: str):
        data = await self.execute_command('/list.php', {'type': 'hotel', 'hotcountry': country_id})
        if data and 'lists' in data and 'hotels' in data['lists'] and 'hotel' in data['lists']['hotels']:
            self._hotels_cache[country_id] = {
                hotel['name']: hotel['id']
                for hotel in data['lists']['hotels']['hotel']
            }
            self._hotels_cache_hits[country_id] = 0

    async def refresh_cache(self, cache_type: str):
        if cache_type == 'countries':
            data = await self.execute_command('/list.php', {'type': 'country'})
            self._country_cache = {
                country['name']: country['id'] 
                for country in data['lists']['countries']['country']
            }
            self._cache_hits['countries'] = 0
            
        # elif cache_type == 'departures':
        #     data = await self.execute_command('/list.php', {'type': 'departure'})
        #     self._departure_cache = {
        #         dep['name']: dep['id'] 
        #         for dep in data['lists']['departures']['departure']
        #     }
        #     self._cache_hits['departures'] = 0

        elif cache_type == 'meals':
            data = await self.execute_command('/list.php', {'type': 'meal'})
            self._meal_cache = {
                meal['russianfull']: meal['id']
                for meal in data['lists']['meals']['meal']
            }
            self._cache_hits['meals'] = 0

        elif cache_type == 'regions':
            data = await self.execute_command('/list.php', {'type': 'region'})
            self._regions_cache = {
                region['name']: region['id']
                for region in data['lists']['regions']['region']
            }
            self._cache_hits['regions'] = 0

    async def get_hotel_rating(self, hotel_id: str, country_id: str) -> Optional[str]:
        data = await self.execute_command('/list.php', {'type': 'hotel', 'hotcountry': country_id})
        if data and 'lists' in data and 'hotels' in data['lists'] and 'hotel' in data['lists']['hotels']:
            for hotel in data['lists']['hotels']['hotel']:
                if hotel['id'] == hotel_id:
                    return hotel['rating']
        return 'Не найдено'

    async def get_hotel_code(self, hotel_name: str, country_id: str) -> Union[list[str], Optional[str]]:
        if not hotel_name or not country_id:
            return []
            
        if (country_id not in self._hotels_cache or 
            country_id not in self._hotels_cache_hits or 
            self._hotels_cache_hits[country_id] >= self.MAX_CACHE_HITS):
            await self.refresh_hotels_cache(country_id)
        
        self._hotels_cache_hits[country_id] = self._hotels_cache_hits.get(country_id, 0) + 1
        
        hotels_dict = self._hotels_cache.get(country_id, {})
        
        matching_hotels = []
        
        for cached_name, hotel_id in hotels_dict.items():
            if hotel_name.upper() in cached_name.upper():
                if hotel_name.upper() == cached_name.upper():
                    return hotel_id
                else:
                    matching_hotels.append((cached_name, hotel_id))

        if len(matching_hotels) == 1:
            return matching_hotels[0][1]
        if len(matching_hotels) > 1:
            return [ho[0] for ho in matching_hotels]
        return None
    
    async def get_hotel_codes(self, hotel_name: str, country_id: str) -> list[str]:
        if not hotel_name or not country_id:
            return []
            
        if (country_id not in self._hotels_cache or 
            country_id not in self._hotels_cache_hits or 
            self._hotels_cache_hits[country_id] >= self.MAX_CACHE_HITS):
            await self.refresh_hotels_cache(country_id)
        
        self._hotels_cache_hits[country_id] = self._hotels_cache_hits.get(country_id, 0) + 1
        
        hotels_dict = self._hotels_cache.get(country_id, {})
        
        matching_hotels = []
        
        for cached_name, hotel_id in hotels_dict.items():
            if hotel_name.upper() in cached_name.upper():
                matching_hotels.append(hotel_id)
                
        return ",".join(matching_hotels)

    async def get_country_code(self, country_name: str) -> Optional[str]:
        if not self._country_cache or self._cache_hits['countries'] >= self.MAX_CACHE_HITS:
            await self.refresh_cache('countries')
        
        self._cache_hits['countries'] += 1
        
        if not country_name:
            return None
            
        # Точное совпадение
        if country_name in self._country_cache:
            logging.info(f"Точное совпадение страны найдено: {country_name} -> {self._country_cache[country_name]}")
            return self._country_cache[country_name]
            
        # Нечеткий поиск
        best_match = None
        best_ratio = 0
        
        for cached_name in self._country_cache.keys():
            ratio = fuzz.ratio(country_name.lower(), cached_name.lower())
            if ratio > best_ratio and ratio > 80:  # Порог схожести 80%
                best_ratio = ratio
                best_match = cached_name

        logging.info(f"Нечеткий поиск страны: {country_name} -> {best_match} с коэффициентом {best_ratio}")
                
        return self._country_cache.get(best_match)

    async def get_departure_code(self, city_name: str) -> Optional[str]:
        # if not self._departure_cache or self._cache_hits['departures'] >= self.MAX_CACHE_HITS:
        #     await self.refresh_cache('departures')
            
        # self._cache_hits['departures'] += 1
        
        if not city_name:
            return None
            
        # Точное совпадение
        if city_name in self._departure_cache:
            return self._departure_cache[city_name]
            
        # Нечеткий поиск
        best_match = None
        best_ratio = 0
        
        for cached_name in self._departure_cache.keys():
            ratio = fuzz.ratio(city_name.lower(), cached_name.lower())
            if ratio > best_ratio and ratio > 80:  # Порог схожести 80%
                best_ratio = ratio
                best_match = cached_name
                
        return self._departure_cache.get(best_match)
    
    async def get_meal_code(self, meal_name: str) -> Optional[str]:
        if not self._meal_cache or self._cache_hits['meals'] >= self.MAX_CACHE_HITS:
            await self.refresh_cache('meals')

        self._cache_hits['meals'] += 1

        if not meal_name:
            return None

        # Точное совпадение
        if meal_name in self._meal_cache:
            return self._meal_cache[meal_name]

        # Нечеткий поиск
        best_match = None
        best_ratio = 0

        for cached_name in self._meal_cache.keys():
            if meal_name.lower() in cached_name.lower():
                return self._meal_cache[cached_name]
            ratio = fuzz.ratio(meal_name.lower(), cached_name.lower())
            if ratio > best_ratio and ratio > 80:  # Порог схожести 80%
                best_ratio = ratio
                best_match = cached_name

        return self._meal_cache.get(best_match)
    
    async def get_region_code(self, region_name: str) -> Optional[str]:
        if not self._regions_cache or self._cache_hits['regions'] >= self.MAX_CACHE_HITS:
            await self.refresh_cache('regions')
        
        self._cache_hits['regions'] += 1

        if not region_name:
            return None

        # Точное совпадение
        if region_name in self._regions_cache:
            return self._regions_cache[region_name]
        
        # Нечеткий поиск
        best_match = None
        best_ratio = 0

        for cached_name in self._regions_cache.keys():
            ratio = fuzz.ratio(region_name.lower(), cached_name.lower())
            if ratio > best_ratio and ratio > 80:  # Порог схожести 80%
                best_ratio = ratio
                best_match = cached_name
        
        return self._regions_cache.get(best_match)
        
    async def search_tours(self, city: str, country: str, nightsfrom: int = 7, nightsto: int = 10, date_from: str = None, date_to: str = None, adults: int = 2, child: int = 0, stars: int = None, starsbetter: int = None, meal: str = None, mealbetter: str = None, rating: Literal[0, 2, 3, 4, 5] = 4, what_time: int = 1, currency: str = None, price_to: int = None, price_from: int = None, services: str = None, child_age: list = None, user_id: str = None, hotel_name: str = None, region: str = None, direct: bool = None, exact_nights: bool = False, exact_departure_date: bool = False, arrival_date: str = None, exact_arrival_date: bool = False) -> Union[tuple[dict], dict, str]:
        now = DT.now()
        # if date_to:
        #     uple = DT.strptime(date_to, '%d.%m.%Y') + timedelta(days=3)
        #     if uple <= now:
        #         date_to = (now + timedelta(days=3)).strftime('%d.%m.%Y')
        #     else:
        #         date_to = uple.strftime('%d.%m.%Y')
        # if date_from:
        #     upre = DT.strptime(date_from, '%d.%m.%Y') - timedelta(days=3)
        #     if upre >= now:
        #         date_from = upre.strftime('%d.%m.%Y')
        #     else:
        #         date_from = now.strftime('%d.%m.%Y')
        # if nightsfrom:
        #     if nightsfrom > 9:
        #         nightsfrom = nightsfrom - 3
        #     else:
        #         if not nightsto:
        #             nightsto = nightsfrom + 3
        # if nightsto and nightsfrom:
        #     if nightsto != nightsfrom + 3:
        #         if nightsto <= 27:
        #             nightsto = nightsto + 3
        # elif nightsto:
        #     if nightsto <= 27:
        #         nightsto = nightsto + 3
        if not date_from:
            date_from = (now + timedelta(days=1)).strftime('%d.%m.%Y')
        if not date_to:
            date_to = (now + timedelta(days=8)).strftime('%d.%m.%Y')
        city_str = city
        city = await self.get_departure_code(city)
        aviasales_city_code = None
        aviasales_flight = None
        country_str = country
        country = await self.get_country_code(country)
        region_str = region
        request_data = {}
        if region:
            region = await self.get_region_code(region)
            if region:
                if is_summer_season(DT.now()):
                    if country == '1':
                        if region == '6':
                            request_data['subregions'] = '529'
                        elif region == '5':
                            request_data['subregions'] = '526,525,555'
                        else:
                            request_data['regions'] = region
                    else:
                        request_data['regions'] = region
                else:
                    request_data['regions'] = region
        if not country:
            return "По данной стране нельзя найти тур"
        if adults:
            request_data['adults'] = adults
        if child:
            request_data['child'] = child
        if child_age:
            for index, age in enumerate(child_age, 1):
                request_data[f'childage{index}'] = age
        if not city:
            aviasales_city_code = await aviasales.search_city(city_str)
            if not aviasales_city_code:
                return "По данному городу нельзя найти тур"
            departure_date = try_parsing_date(date_from)
            if arrival_date:
                inbound_date = try_parsing_date(arrival_date)
            else:
                inbound_date = copy.deepcopy(departure_date) + timedelta(days=nightsfrom if exact_nights else random.randint(nightsfrom, nightsto))
            passengers = []
            if adults:
                for i in range(adults):
                    passengers.append({
                        "travelerType": "ADULT",
                    })
            if child_age:
                for age in child_age:
                    if 12 < age:
                        passengers.append({
                            "travelerType": "ADULT",
                        })
                    elif 2 < age < 12:
                        passengers.append({
                            "travelerType": "CHILD",
                        })
                    elif age < 2:
                        passengers.append({
                            "travelerType": "HELD_INFANT",
                            "associatedAdultId": "1"
                        })

            aviasales_flight = await aviasales.search_ticket(user_id, aviasales_city_code, country, region_str, region, departure_date, inbound_date, direct=direct if direct != None else False, currency=currency, exact_departure_date=True if exact_departure_date else False, exact_arrival_date=True if (exact_arrival_date or exact_nights) else False, passengers=passengers)
            travelmarket_flight = None

            if aviasales_city_code in travelmarket_departures_ids.keys():
                travelmarket_flight = await travelmarket.search_ticket(user_id, aviasales_city_code, region_str, country, region, departure_date, exact_departure_date=exact_departure_date, nightsfrom=nightsfrom, nightsto=nightsto, currency=currency, passengers=passengers)

            if travelmarket_flight:
                if not aviasales_flight:
                    aviasales_flight = travelmarket_flight
                else:
                    avia_price = aviasales_flight['price']
                    tm_price = travelmarket_flight['price']

                    avia_currency = aviasales_flight['currency']
                    tm_currency = travelmarket_flight['currency']

                    if avia_currency != tm_currency:
                        tm_price = await CurrencyConverter.convert(
                            tm_currency, avia_currency, tm_price
                        )
                        tm_currency = avia_currency

                    if aviasales_flight.get("alternative", False):
                        avia_price += await CurrencyConverter.convert("EUR", avia_currency, 300)

                    if travelmarket_flight.get("alternative", False):
                        tm_price += await CurrencyConverter.convert("EUR", avia_currency, 300)

                    if tm_price < avia_price:
                        aviasales_flight = travelmarket_flight

            if not aviasales_flight:
                return "По данному городу нельзя найти тур"
            logging.info("Поиск ведётся по Aviasales")

            # Обновляем даты и количество ночей на основе найденного билета
            if aviasales_flight["source"] == "amadeus" and 'itineraries' in aviasales_flight and len(aviasales_flight['itineraries']) >= 2:
                # Получаем дату вылета туда
                outbound_date = None
                for segment in aviasales_flight['itineraries'][0]['segments']:
                    if 'departure' in segment and 'at' in segment['departure']:
                        outbound_date = segment['departure']['at']
                        break
                
                # Получаем дату вылета обратно
                inbound_date = None
                for segment in aviasales_flight['itineraries'][1]['segments']:
                    if 'departure' in segment and 'at' in segment['departure']:
                        inbound_date = segment['departure']['at']
                        break
                
                if outbound_date and inbound_date:
                    # Преобразуем строки дат в объекты datetime
                    outbound_dt = DT.strptime(outbound_date.split('T')[0], '%Y-%m-%d')
                    inbound_dt = DT.strptime(inbound_date.split('T')[0], '%Y-%m-%d')
                    
                    # Вычисляем количество ночей
                    nights = (inbound_dt - outbound_dt).days
                    
                    # Обновляем параметры поиска
                    date_from = outbound_dt.strftime('%d.%m.%Y')
                    date_to = outbound_dt.strftime('%d.%m.%Y')  # Ищем туры только на дату вылета
                    nightsfrom = max(1, nights)  # Минимум 1 ночь
                    nightsto = nightsfrom
                    
                    logging.info(f"Обновлены параметры поиска на основе билета: date_from={date_from}, date_to={date_to}, nights={nights}")

            elif aviasales_flight["source"] == "travelmarket":
                # Получаем дату вылета туда и обратно
                if aviasales_flight.get('departure_date') and aviasales_flight.get('arrival_date'):
                    outbound_dt = aviasales_flight['departure_date']
                    inbound_dt = aviasales_flight['arrival_date']

                    # Вычисляем количество ночей
                    nights = (inbound_dt - outbound_dt).days

                    # Обновляем параметры поиска
                    date_from = outbound_dt.strftime('%d.%m.%Y')
                    date_to = outbound_dt.strftime('%d.%m.%Y')  # Ищем туры только на дату вылета
                    nightsfrom = max(1, nights)  # Минимум 1 ночь
                    nightsto = nightsfrom

                    logging.info(f"Обновлены параметры поиска на основе билета: date_from={date_from}, date_to={date_to}, nights={nights}")

            if not aviasales_flight.get("alternative") or not region:
                region_airport_code = await aviasales.search_city(region_str) if region_str else None
                logging.info(f"Город вылета: {city_str} ({aviasales_city_code}), город прилёта: {region_str} ({region_airport_code}), аэропорт прилёта билета: {aviasales_flight['to']}")
                if region_airport_code != aviasales_flight["to"]:
                    region_str = await aviasales.find_airport_city_name(aviasales_flight["to"])
                    if region_str:
                        logging.info(f"Обновлен город прилёта: {region_str}")
                        region = await self.get_region_code(region_str)
                        if region:
                            if is_summer_season(DT.now()):
                                if country == '1':
                                    if region == '6':
                                        request_data['subregions'] = '529'
                                    elif region == '5':
                                        request_data['subregions'] = '526,525,555'
                                    else:
                                        request_data['regions'] = region
                                else:
                                    request_data['regions'] = region
                            else:
                                request_data['regions'] = region
                            logging.info(f"Регион обновлён на {region_str} ({region})")


        by_and_ru_city = await if_ru_or_by_city(city) if city else False
        request_data['departure'] = city if city else '99'
        request_data['country'] = country
        # Если страна Мальдивы (8) - туроператор "Русский Экспресс" (23)
        if country == '8':
            request_data['operator'] = '23'
        else:
            request_data['operator'] = '13,11,90'

        if price_from or price_to:
            if price_from:
                request_data['pricefrom'] = await CurrencyConverter.find_diapason(currency, price_from, 'minus')
            if price_to:
                if not price_from:
                    request_data['priceto'] = await CurrencyConverter.find_diapason(currency, price_to, 'plus')
                    request_data['pricefrom'] = await CurrencyConverter.find_diapason(currency, price_to, 'minus')
                else:
                    request_data['priceto'] = await CurrencyConverter.find_diapason(currency, price_to, 'plus')
            if aviasales_flight:
                request_data['pricefrom'] = int(request_data['pricefrom'] - await CurrencyConverter.convert(aviasales_flight['currency'], currency, aviasales_flight['price']))
                request_data['priceto'] = int(request_data['priceto'] - await CurrencyConverter.convert(aviasales_flight['currency'], currency, aviasales_flight['price']))
        currency_str = "EUR"
        if currency:
            currency_str = currency
            if currency in ['RUB', 'USD', 'EUR', 'BYN', 'KZT']:
                if currency == 'RUB':
                    currency = 0
                elif currency in ['USD', 'EUR']:
                    currency = 1
                elif currency == 'BYN':
                    currency = 2
                elif currency == 'KZT':
                    currency = 3
                request_data['currency'] = currency
            else:
                return "Неверный код валюты. Выберите одну из доступных валют: RUB, USD, EUR, BYN, KZT"
        already_used_codes, used_at = await get_used_hotels(str(user_id))
        logging.info("Уже использованные коды отелей: " + str(already_used_codes))
        if nightsfrom:
            if nightsfrom < 1:
                nightsfrom = 1
            request_data['nightsfrom'] = nightsfrom
        if nightsto:
            if nightsto < 1:
                nightsto = 4
            if nightsto > 30:
                nightsto = 30
            request_data['nightsto'] = nightsto
        if date_from:
            request_data['datefrom'] = date_from
        if date_to:
            request_data['dateto'] = date_to
        if not (stars or starsbetter):
            if country == '14':
                request_data['starsbetter'] = '2'
            elif region in ['151', '101', '298']:
                request_data['stars'] = '3'
            elif country == '8':
                request_data['stars'] = '3'
            else:
                request_data['starsbetter'] = '3'
        if stars:
            request_data['stars'] = stars
        if starsbetter:
            request_data['starsbetter'] = starsbetter
        if meal:
            Y = await self.get_meal_code(meal)
            if Y:
                request_data['meal'] = Y
        if not meal:
            if country in ['1', '4']:
                request_data['meal'] = '7'
            elif region in ['151', '101', '298']:
                request_data['meal'] = '3'
            elif country == '8':
                request_data['meal'] = '3'
            else:
                request_data['meal'] = '4'
        offer_regions = await find_offer_regions(country, region)
        if offer_regions:
            request_data['regions'] = offer_regions
        if mealbetter:
            Z = await self.get_meal_code(mealbetter)
            if Z:
                request_data['mealbetter'] = Z
        if not mealbetter:
            if region in ['151', '101', '298']:
                request_data['mealbetter'] = '3'
            elif country == '8':
                request_data['mealbetter'] = '3'
        if rating:
            if rating in [0, 2, 3, 4, 5]:
                request_data['rating'] = rating
        if not rating:
            if region in ['151', '101', '298']:
                request_data['rating'] = 4
            elif country == '8':
                request_data['rating'] = 4
        # if country == '1':
        #     request_data['hideregular'] = 1
        if not hotel_name:
            result_hotels, result_hotel_codes = await find_hotel_codes(city, country, region, True if child else False)
            if result_hotels:
                if result_hotel_codes:
                    request_data['hotels'] = result_hotel_codes
            else:
                return result_hotel_codes
        else:
            request_data['hotels'] = await self.get_hotel_codes(hotel_name, country)
            if not request_data['hotels']:
                return f"Отель {hotel_name} не был найден, необходимо уточнить у клиента страну, где находится отель, если же страна {country_str} указана верно, необходимо вызвать менеджера посредством вызова функции call_manager "
        if services:
            request_data['services'] = services

        with io.StringIO() as buffer:
            buffer.write("Параметры запроса в Tourvisor:\n\n")
            buffer.write(json.dumps(request_data, ensure_ascii=False, indent=4))
            buffer.seek(0)

        req = await self.execute_command('/search.php', request_data)

        logging.info(f"Запрос на поиск туров: {request_data}")

        logging.info(f"Ответ на запрос поиска туров: {req}")

        request_id = req['result']['requestid']

        while True:
            await asyncio.sleep(1)
            result = (await self.execute_command('/result.php', {'requestid': request_id}))['data']
            if result['status']['state'] == 'finished':
                break
            
        logging.info(f"Ответ на поиск туров: {result}")

        if not result['status']['hotelsfound']:
            return "Туры не найдены"
        
        massive_result = {}
        massive_int = 0

        now_used_codes = []

        if aviasales_flight:
            aviasales_tax, aviasales_tax_currency = await find_aviasales_tax(country, region)

        for hotel in result['result']['hotel']:
            if massive_int >= 9 and request_data.get('hotels') == None:
                break
            if hotel_name == None and request_data.get('hotels') == None:
                if str(hotel['hotelcode']) in already_used_codes:
                    continue
                else:
                    now_used_codes.append(str(hotel['hotelcode']))
            min_price = min(tour['priceue'] for tour in hotel['tours']['tour'])
            tour = next(tour for tour in hotel['tours']['tour'] if tour['priceue'] == min_price)
            tour_price_str = ""
            if aviasales_flight:
                # --- NEW: явный и надёжный расчёт итоговой цены ---
                try:
                    # число взрослых: из тура (tour) или из request_data, иначе 2
                    adults_count = int(tour.get('adults') or request_data.get('adults') or 2)
                except Exception:
                    adults_count = 2
                # Переводим цену перелёта (за 1 человека) в EUR
                flight_price_eur = await CurrencyConverter.convert(aviasales_flight['currency'], 'EUR', aviasales_flight['price'])
                # Цена от TourVisor в поле tour['price'] — обычно за НОМЕР; конвертируем в EUR
                hotel_price_raw = tour.get('price', 0)
                hotel_price_currency = tour.get('currency') or aviasales_flight.get('currency') or 'EUR'
                hotel_price_eur = await CurrencyConverter.convert(hotel_price_currency, 'EUR', hotel_price_raw)
                # Таксы/доплаты (если есть) переводим в EUR
                tax_price_eur = 0
                if 'aviasales_tax' in locals() and aviasales_tax:
                    tax_price_eur = await CurrencyConverter.convert(aviasales_tax_currency, 'EUR', aviasales_tax)
                # Общая цена: перелёт (за всех) + отель (за номер) + таксы/трансфер
                total_price_eur = int(round(flight_price_eur * adults_count + hotel_price_eur + tax_price_eur))
                tour_price_str = f"{total_price_eur} EUR"
                # Сохраняем сырые значения для дальнейшей отладки
                redi_price_raw = total_price_eur
            else:
                # если нет данных по авиаперелёту — просто конвертируем цену тура в EUR
                hotel_price_raw = tour.get('price', 0)
                hotel_price_currency = tour.get('currency') or 'EUR'
                converted = await CurrencyConverter.convert(hotel_price_currency, 'EUR', hotel_price_raw)
                tour_price_str = f"{int(round(converted))} EUR"
                redi_price_raw = int(round(converted))


