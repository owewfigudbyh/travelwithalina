import logging
import json
import os
import aiohttp
import copy
import httpx
import asyncio
import traceback

from io                             import BytesIO
from typing                         import Optional, Union, Literal, Dict
from datetime                       import datetime as DT
from pytz                           import timezone   

from telebot.util                   import smart_split

from modules.TourVisorAPI           import tourvisor
from modules.database               import append_json_dialog, get_prompt_id, format_datetime_ru, get_last_run_id, clear_json_dialog
from modules.AviaSalesAPI           import aviasales
from modules.VerboxAPI              import VerboxBot
from modules.call_manager           import call_manager
from modules.database               import ban_user

from openai                         import AsyncOpenAI
from openai.types.responses         import Response

from pinecone                       import Pinecone, ServerlessSpec

from io                             import BytesIO
from typing                         import Optional, Union

from whatsapp_api_client_python.API import GreenAPI

TOURS_MESSAGE = "Подобрала для вас список отелей в {region} {date_str} {nights_str} ночей:\n\n"

FINAL_TOURS_MESSAGE = "💙 В наличии также более 100 отелей по ценам выше и ниже, эти всегда отправляю по отзывам туристов, всем очень нравятся)\nВсе делаю сама, чтобы Вы отдыхали без забот 😊\nВ стоимость тура входит: авиаперелёт, трансфер, питание {meal_type}, проживание и страхование"

FOOD_DICTIONARY = {"RO": "Без питания", "BB": "Завтрак шведский стол", "HB": "Завтрак и ужин шведский стол", "FB": "Завтрак, обед и ужин шведский стол", "AI": "Все включено шведский стол", "UAI": "Ультра всё включено шведский стол"}


pinecone_client = Pinecone(api_key=os.getenv("PINECONE_API_KEY"), environment=os.getenv("PINECONE_ENVIRONMENT"))  # например 'us-east-1-aws'

INDEX_NAME   = os.getenv("PINECONE_INDEX_NAME")
DIMENSION    = int(os.getenv("PINECONE_DIMENSION"))               # для text-embedding-3-large
PC_REGION    = os.getenv("PINECONE_PC_REGION")                    # region из консоли
PC_CLOUD     = os.getenv("PINECONE_CLOUD")

if INDEX_NAME not in pinecone_client.list_indexes().names():
        pinecone_client.create_index(
            name=INDEX_NAME,
            dimension=DIMENSION,
            metric='euclidean',
            spec=ServerlessSpec(
                cloud=PC_CLOUD,
                region=PC_REGION
            )
        )

index = pinecone_client.Index(INDEX_NAME)

async def upsert_to_pinecone(vec_id: str, vec: list[float], meta: dict) -> None:
    await asyncio.to_thread(
        lambda: index.upsert(vectors=[{
            "id": vec_id,
            "values": vec,
            "metadata": meta
        }])
    )

async def create_embedding(openai_client: AsyncOpenAI, text: str) -> list[float]:
    rsp = await openai_client.embeddings.create(
        input=text,
        model="text-embedding-3-large"
    )
    return rsp.data[0].embedding

async def find_hints_for_user(local_client: Union[VerboxBot, GreenAPI], openai_client: AsyncOpenAI, chat_id: int, name: str, text: str):
    try:

        user_q = (await make_and_run_response(
            local_client,
            openai_client,
            [{"role": "user", "content": text}],
            "embedding",
            chat_id=chat_id,
            name=name
        )).output_text

        if user_q == '[ignore]':
            logging.info("[CHATGPT] Игнорирую embedding запроса")
            return None

        # 1) embedding запроса
        query_vec = await create_embedding(openai_client, user_q)

        # 2) первые 10 совпадений в Pinecone
        pine_res = await asyncio.to_thread(
            lambda v: index.query(vector=v, top_k=10, include_metadata=True),
            query_vec
        )

        if not pine_res["matches"]:
            logging.info("[PINECONE] Нет совпадений")
            return None

        # 3) фильтрация по score > 0.6
        filtered_matches = [m for m in pine_res["matches"] if m["score"] > 0.6]
        if not filtered_matches:
            logging.info(f"[PINECONE] Совпадения: {pine_res['matches']}")
            logging.info("[PINECONE] Нет совпадений с score > 0.6")
            return None

        logging.info("[PINECONE] Отфильтрованные совпадения с score > 0.6:")
        for i, m in enumerate(filtered_matches, start=1):
            q = m["metadata"]["question"]
            a = m["metadata"]["answer"]
            sc = m["score"]
            logging.info(f"{i:2d}. score={sc:.4f} │ Q: {q} │ A: {a}")
        logging.info("-" * 80)

        # 4) готовим документы для Cohere (текст = вопрос + ответ)
        documents = [
            f"Q: {m['metadata']['question']}\nA: {m['metadata']['answer']}"
            for m in filtered_matches
        ]

        # 5) Cohere rerank‑v3.5

        # proxy_url = os.getenv("COHERE_PROXY")
        api_key = os.getenv("COHERE_API_KEY")

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": "rerank-v3.5",
            "query": user_q,
            "documents": documents,
            "top_n": len(documents)
        }

        async def rerank_with_proxy():
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    "https://api.cohere.com/v2/rerank",
                    headers=headers,
                    data=json.dumps(payload)
                )
                resp.raise_for_status()
                return resp.json()["results"]

        try:
            rerank_resp = await rerank_with_proxy()
        except Exception as e:
            logging.error(f"[COHERE] Ошибка при реранке: {e}")
            logging.error(traceback.format_exc())
            return None


        logging.info("[COHERE] Реранк результаты:")

        results = getattr(rerank_resp, "results", rerank_resp)

        for item in results:
            if hasattr(item, "relevance_score"):
                idx = item.index
                score = item.relevance_score
            elif isinstance(item, dict):
                idx = item["index"]
                score = item["relevance_score"]
            else:
                idx, score, *_ = item

            snippet = documents[idx].replace("\n", " │ ")[:100]
            logging.info(f"{idx:2d}: score={score:.4f} │ {snippet} …")

        logging.info("-" * 80)

        # 6) лучший по реранку и его оригинальный match
        best_item = results[0]

        if isinstance(best_item, dict):
            best_idx = best_item["index"]
            best_score = best_item["relevance_score"]
        elif hasattr(best_item, "index") and hasattr(best_item, "relevance_score"):
            best_idx = best_item.index
            best_score = best_item.relevance_score
        else:
            best_idx = best_item[0]
            best_score = best_item[1]

        best_match = filtered_matches[best_idx]

        # 7) если score < 0.75 — сообщаем, что ничего подходящего не нашли
        if best_score < 0.75:
            logging.info(f"[COHERE] Лучший результат score={best_score:.4f} < 0.75 | {best_idx}")
            logging.info("[PINECONE] Нет подходящих совпадений с score > 0.75")
            return None

        # 8) Формируем системный контекст для GPT
        system_hint = (
            f"Вопрос: {best_match['metadata']['question']}\n\n"
            f"Ответ: {best_match['metadata']['answer']}\n"
        )

        return system_hint
    except:
        return None
    
async def get_audio_transcription(openai_client: AsyncOpenAI, url: str, fileName: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            buffer = BytesIO(await response.read())
            buffer.name = fileName
            buffer.seek(0)

            response = await openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=buffer,
            )

            buffer.close()

            return response.text

def convert_phone_number(wa_number: str):
    phone_number = "+" + copy.copy(wa_number)
    phone_number = phone_number.replace("@c.us", "")
    return phone_number


async def add_tours_message(local_client: Union[VerboxBot, GreenAPI], chat_id: str, region: str, date_from: str, date_to: str, nightsfrom: str, nightsto: str, united_result: dict):
    try:
    
        await asyncio.sleep(30)
        if nightsfrom == nightsto:
            nights_str = f"на {nightsfrom}"
        else:
            nights_str = f"от {nightsfrom} до {nightsto}"
        if date_from == date_to:
            date_str = f"на дату {date_from}"
        else:
            date_str = f"с {date_from} по {date_to}"
        message = TOURS_MESSAGE.format(region=region, date_str=date_str, nights_str=nights_str)
        if united_result.get("aviasales_flight"):
            aviasales_flight = united_result.get("aviasales_flight")
            from_region = await aviasales.find_airport_city_name(aviasales_flight['from'])
            to_region = await aviasales.find_airport_city_name(aviasales_flight['to'])
            if to_region and from_region:
                message += f"Направление: {from_region or aviasales_flight['from']} → {to_region or aviasales_flight['to']}\n\n"
        meal_type = None
        tour_result = united_result.get("main_result")
        for value in tour_result.values():
            if isinstance(value, dict):
                value = [value]
            for tour in value:
                tour_str = f"{tour['hotelname']} {tour['price']} ({tour['nights']} ночей на {tour['flydate']})"
                if tour.get('hotel_link'):
                    tour_str += f" - {tour['hotel_link']}"
                tour_str += f" - {tour['regionname']}"
                tour_str += "\n"
                message += tour_str
                if meal_type == None:
                    meal_type = tour['meal']
        if united_result.get("second_result"):
            message += "\n\nМожете также посмотреть другие туры в стране:\n\n"
            for value in united_result.get("second_result").values():
                if isinstance(value, dict):
                    value = [value]
                for tour in value:
                    tour_str = f"{tour['hotelname']} {tour['price']} ({tour['nights']} ночей на {tour['flydate']})"
                    if tour.get('hotel_link'):
                        tour_str += f" - {tour['hotel_link']}"
                    tour_str += f" - {tour['regionname']}"
                    tour_str += "\n"
                    message += tour_str
                    if meal_type == None:
                        meal_type = tour['meal']

        message += "\n\n"
        if not meal_type:
            message += FINAL_TOURS_MESSAGE.format(meal_type='Неизвестно')
        if meal_type:
            message += FINAL_TOURS_MESSAGE.format(meal_type=FOOD_DICTIONARY[meal_type.upper()])

        # for key in FOOD_DICTIONARY.keys():
        #     message = message.replace(key, FOOD_DICTIONARY[key])
        #     message = message.replace(key.lower(), FOOD_DICTIONARY[key])

        await append_json_dialog(chat_id=chat_id, new_message={"role": "assistant", "content": message})

        for chunked_message in smart_split(message, 2000):
            if isinstance(local_client, GreenAPI):
                local_client.sending.sendMessage(chat_id, chunked_message)
            else:
                client_id, search_id = chat_id.split("_")
                await local_client.send_message(client_id, search_id, chunked_message)

        if united_result.get("recommended_destinations"):
            await append_json_dialog(chat_id=chat_id, new_message={"role": "assistant", "content": "Также сделала для вас подборки туров по направлениям, которые мы сейчас рекомендуем:"})
            if isinstance(local_client, GreenAPI):
                local_client.sending.sendMessage(chat_id, "Также сделала для вас подборки туров по направлениям, которые мы сейчас рекомендуем:")
            else:
                client_id, search_id = chat_id.split("_")
                await local_client.send_message(client_id, search_id, "Также сделала для вас подборки туров по направлениям, которые мы сейчас рекомендуем:")

            for recommended_result in united_result.get("recommended_destinations", []):
                recommended_region = None
                message = ""

                for value in recommended_result.get("result").values():
                    if isinstance(value, dict):
                        value = [value]
                    for tour in value:
                        tour_str = f"{tour['hotelname']} {tour['price']} ({tour['nights']} ночей на {tour['flydate']})"
                        if not recommended_region:
                            recommended_region = tour['regionname']
                        if tour.get('hotel_link'):
                            tour_str += f" - {tour['hotel_link']}"
                        tour_str += f" - {tour['regionname']}"
                        tour_str += "\n"
                        message += tour_str

                if recommended_region == "Тенерифе":
                    message += "\n\n💙 В наличии также более 100 отелей по ценам выше и ниже, эти всегда отправляю по отзывам туристов, всем очень нравятся)\nВсе делаю сама, чтобы Вы отдыхали без забот 😊\nВ стоимость тура входит: авиаперелёт, трансфер, питание завтраки и ужины, шведский стол, проживание и страхование"
                        
                await append_json_dialog(chat_id=chat_id, new_message={"role": "assistant", "content": f"Подборка по {recommended_region}:\n\n{message}"})
                for chunked_message in smart_split(f"Подборка по {recommended_region}:\n\n{message}", 2000):
                    if isinstance(local_client, GreenAPI):
                        local_client.sending.sendMessage(chat_id, chunked_message)
                    else:
                        client_id, search_id = chat_id.split("_")
                        await local_client.send_message(client_id, search_id, chunked_message)
            
    except Exception as e:
        logging.error(f"Ошибка при добавлении сообщения с турами: {e}")

# Функция для того, чтобы ждать ответа от OpenAI
async def make_and_run_response(
    local_client: Union[VerboxBot, GreenAPI],
    openai_client: AsyncOpenAI,
    message_list: list, 
    assistant_name: Literal["default", "embedding"] = "default",
    chat_id: int = None,
    name: str = None,
    last_run_id: str = None,
    retry_count: int = 0, 
    max_retries: int = 5
) -> Optional[Response]:

    if await get_last_run_id(chat_id) != last_run_id and assistant_name != "embedding":
        return None  # Если за время выполнения произошёл новый запрос, не отправляем ответ на старый

    if retry_count >= max_retries:
        logging.error(f"Max retries reached for chat_id: {chat_id}. Returning None.")
        return None

    try:

        search_hint = None

        if assistant_name != "embedding":
            if message_list[-1].get("role") == "user":
                if isinstance(message_list[-1]["content"], str):
                    search_hint = await find_hints_for_user(local_client, openai_client, chat_id, name, message_list[-1]["content"])

        input_messages = []

        prompt_id = await get_prompt_id(assistant_name)

        prompt_dict = {"id": prompt_id}
        
        if assistant_name != "embedding":
            prompt_dict["variables"] = { 
                            "date": format_datetime_ru(DT.now(timezone('Asia/Novokuznetsk'))),
                            "hint": search_hint if search_hint else "..."
                        }

        logging.info(f"Waiting for response: {message_list} | Retry count: {retry_count}")

        response = await openai_client.responses.create(
                        input=message_list,
                        prompt=prompt_dict)

        output_types_list = [output.type for output in response.output]
        reasoning_enabled = "reasoning" in output_types_list and "code_interpreter_call" not in output_types_list

        for tool_call in response.output:

            if tool_call.type == "reasoning" and reasoning_enabled and assistant_name != "embedding":
                input_messages.append({"type": "item_reference", "id": tool_call.id})
                await append_json_dialog(chat_id=chat_id, new_message={"type": "item_reference", "id": tool_call.id})
                continue

            if tool_call.type == "message" and reasoning_enabled and assistant_name != "embedding":
                input_messages.append({"type": "item_reference", "id": tool_call.id})
                await append_json_dialog(chat_id=chat_id, new_message={"type": "item_reference", "id": tool_call.id})
                continue

            if tool_call.type != "function_call":
                continue

            try:

                input_messages.append(dict(tool_call))

                if assistant_name != "embedding":
                    await append_json_dialog(chat_id=chat_id, new_message=dict(tool_call))

                function_name = tool_call.name
                arguments = json.loads(tool_call.arguments)

                logging.info(f"Processing tool call: {function_name} with arguments: {arguments}")

                if function_name == 'call_manager':
                    text_notification = f"Клиент в {'WhatsApp' if convert_phone_number(chat_id) == name else 'Facebook'}"
                    if name:
                        text_notification += f" <code>{name}</code>"
                    text_notification += f" вызывает менеджера:\n\n<code>{arguments.get('reason', 'Причина не указана')}</code>"
                    await call_manager(text_notification)
                    await ban_user(chat_id)
                    input_messages.append({
                        "type": "function_call_output",
                        "call_id": tool_call.call_id,
                        "output": 'success'
                    })
                    await append_json_dialog(chat_id=chat_id, new_message={"type": "function_call_output", "call_id": tool_call.call_id, "output": 'success'})

                elif function_name in ['tour_searching', 'tour_search', 'tour_search_by_specific_hotel']:
                    arguments['user_id'] = str(chat_id)
                    search_result = await tourvisor.search_tours(**arguments)
                    if isinstance(search_result, str):
                        input_messages.append({
                            "type": "function_call_output",
                            "call_id": tool_call.call_id,
                            "output": search_result
                        })
                        await append_json_dialog(chat_id=chat_id, new_message={"type": "function_call_output", "call_id": tool_call.call_id, "output": search_result})
                    elif isinstance(search_result, dict):
                        asyncio.create_task(add_tours_message(local_client, chat_id, arguments.get('region') or arguments.get('country'), arguments.get('date_from'), arguments.get('date_to'), arguments.get('nightsfrom'), arguments.get('nightsto'), search_result))
                        input_messages.append({
                            "type": "function_call_output",
                            "call_id": tool_call.call_id,
                            "output": 'Пользователю скоро вышлется ответ'
                        })
                        await append_json_dialog(chat_id=chat_id, new_message={"type": "function_call_output", "call_id": tool_call.call_id, "output": 'Пользователю скоро вышлется ответ'})

                elif function_name == 'get_info_about_hotel':
                    result = await tourvisor.get_info_about_hotel(**arguments)
                    input_messages.append({
                        "type": "function_call_output",
                        "call_id": tool_call.call_id,
                        "output": result
                    })
                    await append_json_dialog(chat_id=chat_id, new_message={"type": "function_call_output", "call_id": tool_call.call_id, "output": result})

            except Exception as e:
                logging.error(f"Error processing tool call: {traceback.format_exc()}\n\n{arguments}")
                input_messages.append({
                    "type": "function_call_output",
                    "call_id": tool_call.call_id,
                    "output": f'Ошибка: {str(e)}'
                })
                if assistant_name != "embedding":
                    await append_json_dialog(chat_id=chat_id, new_message={"type": "function_call_output", "call_id": tool_call.call_id, "output": f'Ошибка: {str(e)}'})

        if input_messages:
            if "function_call_output" in [msg.get("type") for msg in input_messages]:

                message_list.extend(input_messages)

                logging.info(f"Tool outputs: {input_messages}")

                return await make_and_run_response(
                    local_client=local_client,
                    openai_client=openai_client,
                    message_list=message_list,
                    assistant_name=assistant_name,
                    chat_id=chat_id,
                    name=name,
                    last_run_id=last_run_id,
                    retry_count=retry_count,
                    max_retries=max_retries
                )
        
        if await get_last_run_id(chat_id) != last_run_id and assistant_name != "embedding":
            return None  # Если за время выполнения произошёл новый запрос, не отправляем ответ на старый

        if assistant_name != "embedding":
            await append_json_dialog(chat_id=chat_id, new_message={"role": "assistant", "content": response.output_text})

        logging.info(f"Final response: {response.output_text}")
        return response

    except Exception as e:
        logging.error(f"Error in make_and_run_response (attempt {retry_count + 1}/{max_retries}): {traceback.format_exc()}")

        if "No tool output found for function call" in traceback.format_exc():
            logging.error("Detected 'No tool output found for function call' error")
            await clear_json_dialog(chat_id)
            raise TimeoutError("Cleared dialog due to tool output error.")
        
        # Экспоненциальная задержка перед повтором
        delay = min(2 ** retry_count, 30)  # Максимум 30 секунд
        logging.info(f"Retrying in {delay} seconds...")
        await asyncio.sleep(delay)
        
        return await make_and_run_response(
            local_client=local_client,
            openai_client=openai_client,
            message_list=message_list,
            assistant_name=assistant_name,
            chat_id=chat_id,
            name=name,
            last_run_id=last_run_id,
            retry_count=retry_count+1,
            max_retries=max_retries
        )