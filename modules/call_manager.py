import os
import aiohttp
import logging

async def call_manager(message: str):
    
    async with aiohttp.ClientSession() as session:
        telegram_url = f"https://api.telegram.org/bot{os.getenv('MANAGER_BOT_TOKEN')}/sendMessage"
        params = {
            "chat_id": os.getenv('MANAGER_GROUP_ID'),
            "text": message,
            "parse_mode": "HTML"
        }
        async with session.post(telegram_url, json=params) as response:
            if not response.status == 200:
                logging.error(f"Failed to send error to manager group: {await response.text()}")
            return await response.json()