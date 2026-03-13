import aiosqlite
import logging
import json
import os
import uuid

from datetime import datetime as DT
from datetime import timedelta
from pytz import timezone
from typing import Literal, Union, List, Tuple, Optional
from openai import AsyncOpenAI

DATABASE_NAME = 'database.db'

async def execute(query: str, result_format: Literal['fetchall', 'fetchone'] = None, args: tuple = tuple()) -> Union[list, tuple]:
    async with aiosqlite.connect(DATABASE_NAME) as con:
        async with con.execute(query, args) as cur:
            result = await getattr(cur, result_format)() if result_format else None
        await con.commit()
    return result

async def executemany(query: str, result_format: Literal['fetchall', 'fetchone'] = None, args: tuple = tuple()) -> None:
    async with aiosqlite.connect(DATABASE_NAME) as con:
        async with con.executemany(query, args) as cur:
            result = await getattr(cur, result_format)() if result_format else None
        await con.commit()
    return result

async def start_db():
    # await execute("""
    #     CREATE TABLE IF NOT EXISTS user_threads (
    #         thread_id varchar NOT NULL,
    #         user_id varchar NOT NULL,
    #         used_hotel_codes varchar,
    #         used_at datetime
    #     );
    # """)
    await execute("""
        CREATE TABLE IF NOT EXISTS user_dialogs (
            user_id varchar NOT NULL,
            last_run_id varchar,
            full_dialog varchar,
            used_hotel_codes varchar,
            used_at datetime
        );
    """)
    await execute("""
        CREATE TABLE IF NOT EXISTS banned_users (
            user_id varchar NOT NULL,
            ban_until datetime NOT NULL
        );
    """)
    await execute("""
        CREATE TABLE IF NOT EXISTS temporary_banned_users (
            user_id varchar NOT NULL,
            ban_until datetime NOT NULL
        );
    """)

# async def get_thread(client: AsyncOpenAI, user_id: str, name: str = None) -> str:
#     user_id = str(user_id)
#     result = await execute("SELECT thread_id FROM user_threads WHERE user_id = ?", 'fetchone', (user_id,))
#     if result:
#         return result[0]
#     else:
#         thread_id = (await client.beta.threads.create()).id
#         await client.beta.threads.messages.create(
#             thread_id=thread_id,
#             role="user",
#             content=f"Время написания: {DT.now().strftime('%Y-%m-%d %H:%M:%S')}\nID пользователя: {user_id}\Имя пользователя: {name if name else 'Не указано'}"
#         )
#         await execute("INSERT OR IGNORE INTO user_threads (thread_id, user_id) VALUES (?, ?)", args=(thread_id, user_id))
#         return thread_id
    
# async def delete_thread(user_id: str):
#     user_id = str(user_id)
#     await execute("DELETE FROM user_threads WHERE user_id = ?", args=(user_id,))


async def ban_user(user_id: str) -> None:
    user_id = str(user_id)
    await execute("INSERT OR IGNORE INTO banned_users (user_id, ban_until) VALUES (?, ?)", args=(user_id, DT.now(timezone('Europe/Moscow')) + timedelta(days=30)))

async def unban_user(user_id: str) -> None:
    user_id = str(user_id)
    await execute("DELETE FROM banned_users WHERE user_id = ?", args=(user_id,))

async def check_if_user_banned(user_id: str) -> bool:
    user_id = str(user_id)
    result = await execute("SELECT * FROM banned_users WHERE user_id = ?", 'fetchone', (user_id,))
    if not result:
        return False
    ban_until = DT.fromisoformat(result[1])
    if DT.now(timezone('Europe/Moscow')) >= ban_until:
        await execute("DELETE FROM banned_users WHERE user_id = ?", args=(user_id,))
        return False
    else:
        return True
    
async def get_used_hotels(user_id: str) -> tuple[list, DT]:
    user_id = str(user_id)
    now = DT.now(timezone('Europe/Moscow'))
    result = await execute("SELECT used_hotel_codes, used_at FROM user_dialogs WHERE user_id = ?", 'fetchone', (user_id,))
    if result:
        if result[0]:
            last_used = DT.fromisoformat(result[1])
            if now - last_used > timedelta(days=1):
                await execute('UPDATE user_dialogs SET used_hotel_codes = ?, used_at = ?', args=(None, now))
                return ([], now)
            return (result[0].split(), last_used)
        else:
            return ([], now)
    else:
        return ([], now)
    
async def add_used_hotels(user_id: str, hotel_codes: list) -> None:
    user_id = str(user_id)
    now = DT.now(timezone('Europe/Moscow'))
    already_used_codes, last_used = await get_used_hotels(user_id)
    if already_used_codes:
        hotel_codes.extend(already_used_codes)
    hotel_codes = " ".join(hotel_codes)
    await execute('UPDATE user_dialogs SET used_hotel_codes = ?, used_at = ?', args=(hotel_codes, now))

async def delete_used_hotels(user_id: str) -> None:
    user_id = str(user_id)
    await execute('UPDATE user_dialogs SET used_hotel_codes = ?, used_at = ?', args=(None, DT.now(timezone('Europe/Moscow'))))

def format_datetime_ru(dt: DT) -> str:
    weekdays = {
        'Monday': 'Понедельник',
        'Tuesday': 'Вторник', 
        'Wednesday': 'Среда',
        'Thursday': 'Четверг',
        'Friday': 'Пятница',
        'Saturday': 'Суббота',
        'Sunday': 'Воскресенье'
    }
    weekday_en = dt.strftime('%A')
    try:
        weekday_ru = weekdays[weekday_en]
    except KeyError:
        weekday_ru = weekday_en
    return f"{dt.strftime('%Y.%m.%d')} {weekday_ru} {dt.strftime('%H:%M')}"

async def get_prompt_id(assistant_name: Literal["default"]) -> str:
    if assistant_name == "default":
        return os.getenv("PROMPT_ID")
    elif assistant_name == "embedding":
        return os.getenv("EMBEDDING_PROMPT_ID")

async def append_json_dialog(chat_id: str, new_message: dict, update_run_id: bool = False) -> Tuple[List[Optional[dict]], Optional[str]]:
    res = await execute("SELECT full_dialog, last_run_id FROM user_dialogs WHERE user_id = ?", 'fetchone', (chat_id,))
    full_dialog: List[dict] = []
    last_run_id: str = res[1] if res else None
    updated_run_id = str(uuid.uuid4()) if update_run_id else None
    if not res:
        full_dialog.append(new_message)
        await execute("INSERT INTO user_dialogs (user_id, full_dialog, last_run_id) VALUES (?, ?, ?)", args=(chat_id, json.dumps(full_dialog), updated_run_id))
    else:
        full_dialog = json.loads(res[0])
        full_dialog.append(new_message)
        if update_run_id:
            await execute("UPDATE user_dialogs SET full_dialog = ?, last_run_id = ? WHERE user_id = ?", args=(json.dumps(full_dialog), updated_run_id, chat_id))
        else:
            await execute("UPDATE user_dialogs SET full_dialog = ? WHERE user_id = ?", args=(json.dumps(full_dialog), chat_id))
    logging.info(f"Dialog parameters for chat_id {chat_id} | Full dialog: {full_dialog}")
    return (full_dialog, updated_run_id or last_run_id)

async def clear_json_dialog(chat_id: str):
    res = await execute("SELECT full_dialog, last_run_id FROM user_dialogs WHERE user_id = ?", 'fetchone', (chat_id,))
    if res:
        await execute("UPDATE user_dialogs SET full_dialog = ?, last_run_id = ? WHERE user_id = ?", args=(json.dumps([]), None, chat_id))
    else:
        await execute("INSERT INTO user_dialogs (user_id, full_dialog, last_run_id) VALUES (?, ?, ?)", args=(chat_id, json.dumps([]), None))
    logging.info(f"Cleared dialog for chat_id {chat_id}")

async def get_last_run_id(chat_id: str) -> Optional[str]:
    res = await execute("SELECT last_run_id FROM user_dialogs WHERE user_id = ?", 'fetchone', (chat_id,))
    return res[0] if res else None

async def check_if_user_exists(user_id: str) -> bool:
    user_id = str(user_id)
    result = await execute("SELECT user_id FROM user_dialogs WHERE user_id = ?", 'fetchone', (user_id,))
    return bool(result)