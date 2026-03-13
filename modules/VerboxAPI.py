import logging
import os
import aiohttp
import io

BASE_URL = 'https://admin.verbox.ru/json/v1.0/'

class VerboxBot:
    def __init__(self):
        self.token = os.getenv('VERBOX_TOKEN')
        self.operator_login = os.getenv('VERBOX_OPERATOR_LOGIN')
        
    async def execute_Command(self, method_url: str, clientId: str, searchId: int, data: dict, virtual_operator: bool = False):
        data["client"] = {
            "clientId": clientId,
            "searchId": int(searchId),
        }
        data["operator"] = {
            "login": self.operator_login,
            "virtual": virtual_operator
        }
        logging.info(f"Executing command {method_url} with data: {data}")
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BASE_URL}{method_url}", json=data, headers={"Content-Type": "application/json", "X-Token": self.token}) as response:
                logging.info(await response.json())
                return await response.json()

    async def send_message(self, clientId: str, searchId: str, message: str, disableTextInput: bool = False, virtual_operator: bool = False, attachments: list = []):
        data = {
            "message": {
                "text": message,
                "disableTextInput": disableTextInput
            },
            "markAsRead": True,
            "countInOperatorStatistic": True
        }
        if attachments:
            data["message"]["attachments"] = attachments
        return await self.execute_Command('chat/message/sendToClient', clientId, searchId, data, virtual_operator=virtual_operator)

    async def send_typing_status(self, clientId: str, searchId: str):
        return await self.execute_Command('chat/message/sendTypingToClient', clientId, searchId, {})
    
    async def upload_file(self, file_url: str, filename: str):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BASE_URL}fileshare/upload/uploadFromLink", json={"url": file_url, "fileName": filename}, headers={"Content-Type": "application/json", "X-Token": self.token}) as response:
                resp = await response.json()
                logging.info(resp)
                if resp['success']:
                    return resp['result']['fileId']
                else:
                    raise Exception(str(resp['error']))
                
    async def get_operators(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BASE_URL}chat/operator/getList", headers={"Content-Type": "application/json", "X-Token": self.token}) as response:
                return await response.json()
            
    async def get_message_history(self, clientId: str, searchId: str) -> tuple[int, list]:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BASE_URL}chat/message/getClientMessageList", json={"client": {
                "clientId": clientId,
                "searchId": searchId
            }, "orderDirection": "desc"}, headers={"Content-Type": "application/json", "X-Token": self.token}) as response:
                data = await response.json()
        if data['success']:
            return data['result']['count'], data['result']['items']
        else:
            raise Exception(str(data['error']))
                

verbox = VerboxBot()