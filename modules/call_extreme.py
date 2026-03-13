import os
import aiohttp
import logging

# Функция для оповещения об ошибке
async def call_extreme(error: str):
    admin_id = os.getenv('EXTREME_ID')
    
    message = f"В телеграм тур визор боте произошла ошибка:\n{error}"
    
    async with aiohttp.ClientSession() as session:
        telegram_url = f"https://api.telegram.org/bot{os.getenv('EXTREME_TELEGRAM_TOKEN')}/sendMessage"
        params = {
            "chat_id": admin_id,
            "text": message
        }
        async with session.post(telegram_url, json=params) as response:
            if not response.status == 200:
                logging.error(f"Failed to send error to admin: {await response.text()}")
            return await response.json()