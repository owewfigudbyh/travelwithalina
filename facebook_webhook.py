#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Facebook Messenger Webhook Server
Travel With Alina Bot
"""

import os
import json
import logging
import asyncio
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Импортируем бота
from facebook_bot import bot, handle_message

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/facebook_webhook.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Facebook настройки
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv('FACEBOOK_PAGE_ACCESS_TOKEN', '')
FACEBOOK_VERIFY_TOKEN = 'travel_with_alina_bot'  # Этот токен вводишь в Facebook

app = Flask(__name__)


def send_facebook_message(recipient_id: str, message_text: str):
    """Отправка сообщения в Facebook Messenger"""

    if not FACEBOOK_PAGE_ACCESS_TOKEN:
        logger.error("FACEBOOK_PAGE_ACCESS_TOKEN не установлен!")
        return False

    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={FACEBOOK_PAGE_ACCESS_TOKEN}"

    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": message_text},
        "messaging_type": "RESPONSE"
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)

        if response.status_code == 200:
            logger.info(f"[FB:{recipient_id}] Сообщение отправлено")
            return True
        else:
            logger.error(f"[FB:{recipient_id}] Ошибка отправки: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        logger.error(f"[FB:{recipient_id}] Ошибка: {e}")
        return False


def send_typing_indicator(recipient_id: str, action: str = "typing_on"):
    """Показать индикатор набора текста"""

    if not FACEBOOK_PAGE_ACCESS_TOKEN:
        return

    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={FACEBOOK_PAGE_ACCESS_TOKEN}"

    payload = {
        "recipient": {"id": recipient_id},
        "sender_action": action
    }

    try:
        requests.post(url, json=payload, timeout=10)
    except:
        pass


@app.route('/', methods=['GET'])
def home():
    """Главная страница"""
    return "🤖 Travel With Alina Bot is running!"


@app.route('/webhook', methods=['GET'])
def verify_webhook():
    """Верификация webhook для Facebook"""

    mode = request.args.get('hub.mode')
    token = request.args.get('hub.verify_token')
    challenge = request.args.get('hub.challenge')

    logger.info(f"Webhook verification: mode={mode}, token={token}")

    if mode == 'subscribe' and token == FACEBOOK_VERIFY_TOKEN:
        logger.info("Webhook verified successfully!")
        return challenge, 200
    else:
        logger.error("Webhook verification failed!")
        return 'Forbidden', 403


@app.route('/webhook', methods=['POST'])
def handle_webhook():
    """Обработка входящих сообщений от Facebook"""

    data = request.get_json()
    logger.info(f"Webhook received: {json.dumps(data, ensure_ascii=False)[:500]}")

    if data.get('object') == 'page':
        for entry in data.get('entry', []):
            for messaging_event in entry.get('messaging', []):

                sender_id = messaging_event.get('sender', {}).get('id')

                # Проверяем что это сообщение от пользователя
                if 'message' in messaging_event:
                    message = messaging_event['message']

                    # Игнорируем эхо наших сообщений
                    if message.get('is_echo'):
                        continue

                    message_text = message.get('text', '')

                    if message_text and sender_id:
                        logger.info(f"[FB:{sender_id}] Получено: {message_text[:50]}...")

                        # Показываем что печатаем
                        send_typing_indicator(sender_id, "typing_on")

                        # Обрабатываем сообщение
                        try:
                            # Запускаем асинхронную обработку
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            responses = loop.run_until_complete(
                                bot.process_message(sender_id, message_text)
                            )
                            loop.close()

                            # Отправляем ответы
                            for response in responses:
                                if response:
                                    # Разбиваем длинные сообщения
                                    if len(response) > 2000:
                                        parts = [response[i:i+2000] for i in range(0, len(response), 2000)]
                                        for part in parts:
                                            send_facebook_message(sender_id, part)
                                    else:
                                        send_facebook_message(sender_id, response)

                        except Exception as e:
                            logger.error(f"[FB:{sender_id}] Ошибка обработки: {e}")
                            send_facebook_message(sender_id, "Произошла ошибка, попробуйте ещё раз 😊")

                        finally:
                            send_typing_indicator(sender_id, "typing_off")

    return 'OK', 200


@app.route('/status', methods=['GET'])
def status():
    """Статус бота"""
    return jsonify({
        "status": "running",
        "bot": "Travel With Alina",
        "facebook_token_set": bool(FACEBOOK_PAGE_ACCESS_TOKEN),
        "verify_token": FACEBOOK_VERIFY_TOKEN
    })


if __name__ == '__main__':
    print("=" * 60)
    print("TRAVEL WITH ALINA - Facebook Bot Server")
    print("=" * 60)
    print(f"Webhook URL: http://localhost:5001/webhook")
    print(f"Verify Token: {FACEBOOK_VERIFY_TOKEN}")
    print(f"Page Token: {'SET' if FACEBOOK_PAGE_ACCESS_TOKEN else 'NOT SET'}")
    print("=" * 60)

    if not FACEBOOK_PAGE_ACCESS_TOKEN:
        print("\nWARNING: Set FACEBOOK_PAGE_ACCESS_TOKEN in .env!")

    print("\nStarting server...")
    app.run(host='0.0.0.0', port=5001, debug=True)

