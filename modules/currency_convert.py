import aiohttp
import datetime
import typing
import logging

from cache import AsyncTTL

class AsyncRealTimeCurrencyConverter():
    def __init__(self):
        self.url = 'https://open.er-api.com/v6/latest/USD'
        self.currencies = {}
        self.next_update = None

    async def init_data(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                self.data = await response.json()
                self.currencies = self.data['rates']
                self.next_update = datetime.datetime.fromtimestamp(self.data['time_next_update_unix'] + 1, datetime.timezone.utc)

    @AsyncTTL(time_to_live=600)
    async def convert(self, from_currency: str, to_currency: str, amount: typing.Union[float, int]):

        from_currency = from_currency.upper()
        to_currency = to_currency.upper()

        if from_currency == to_currency:
            return amount

        # Инициализация при первом запуске
        if not self.currencies:
            await self.init_data()
        
        # Проверка необходимости обновления
        if datetime.datetime.now(datetime.timezone.utc) >= self.next_update:
            await self.update_data()

        start_amount = amount
            
        if from_currency != 'USD':
            amount = amount / self.currencies[from_currency] 
    
        amount = round(amount * self.currencies[to_currency], 2)
        logging.info(f"Сконвертировано {start_amount} {from_currency} -> {amount} {to_currency}")
        return amount
    
    async def find_diapason(self, from_currency: str, amount: typing.Union[float, int], direction: typing.Literal['plus', 'minus'] = 'plus'):
        
        from_currency = from_currency.upper()

        # Инициализация при первом запуске
        if not self.currencies:
            await self.init_data()
        
        # Проверка необходимости обновления
        if datetime.datetime.now(datetime.timezone.utc) >= self.next_update:
            await self.update_data()

        start_amount = amount
            
        # Конвертируем в USD
        if from_currency != 'USD':
            amount = amount / self.currencies[from_currency] 

        # Добавляем или вычитаем 700 USD
        if direction == 'plus':
            amount = amount + 700
        else:
            amount = amount - 700

        # Конвертируем обратно в исходную валюту
        if from_currency != 'USD':
            amount = round(amount * self.currencies[from_currency], 2)

        logging.info(f"Диапазон для {start_amount} {from_currency}: {amount} {from_currency}")
        return amount
    
    async def update_data(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                self.data = await response.json()
                self.currencies = self.data['rates']
                self.next_update = datetime.datetime.fromtimestamp(self.data['time_next_update_unix'] + 1, datetime.timezone.utc)

converter = AsyncRealTimeCurrencyConverter()