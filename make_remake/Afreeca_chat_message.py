import ssl 
import base
import certifi
import asyncio
import websockets
from time import time
from os import environ
from json import loads
from requests import get
from datetime import datetime
from dataclasses import dataclass
from supabase import create_client
from Afreeca_live_message import afreeca_getChannelStateData, afreeca_getLink

@dataclass
class afreecaChatData:
    afreeca_chat_msg_List: list
    sock: websockets.connect
    processed_messages: list

class AfreecaChat:
    def __init__(self):
        self.ssl_context = self.create_ssl_context()
        self.F = "\x0c"
        self.ESC = "\x1b\t"
        self.PING_PACKET = f'{self.ESC}000000000100{self.F}'
        # self.stream_end_time = {}  # 각 스트리머의 방송 종료 시간을 저장할 딕셔너리

    @staticmethod
    def create_ssl_context():
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(certifi.where())
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    async def connect_to_chat(self, init: base.initVar, afreecaID):
        while True:
            try:
                if init.chat_json[afreecaID]: self.change_afreeca_chat_json(init, afreecaID, afreeca_chat_TF= False)
                if init.afreeca_titleData.loc[afreecaID,'live_state'] == "CLOSE" or check_is_passwordDict(init, afreecaID):
                    await asyncio.sleep(5)
                    continue

                channel_data = afreeca_getChannelStateData(init.afreeca_titleData.loc[afreecaID, 'chatChannelId'], init.afreecaIDList["afreecaID"][afreecaID])
                # channel_data = afreeca_getChannelStateData(BNO, BID)
                _, TITLE, if_adult_channel, CHDOMAIN, CHATNO, FTK, BJID, CHPT = channel_data
                if TITLE is None: 
                    await asyncio.sleep(1)
                    continue

                if if_adult_channel is False:
                    await asyncio.sleep(15)
                    continue
                
                async with websockets.connect(
                    f"wss://{CHDOMAIN}:{CHPT}/Websocket/{init.afreecaIDList['afreecaID'][afreecaID]}",
                    # f"wss://{CHDOMAIN}:{CHPT}/Websocket/{BID}",
                    subprotocols=['chat'],
                    ssl=self.ssl_context,
                    ping_interval=None
                ) as sock:
                    afreecaChat = afreecaChatData(
                        afreeca_chat_msg_List = [],
                        sock = sock,
                        processed_messages = []
                    )
                    CONNECT_PACKET = f'{self.ESC}000100000600{self.F*3}16{self.F}'
                    JOIN_PACKET = f'{self.ESC}0002{self.calculate_byte_size(CHATNO):06}00{self.F}{CHATNO}{self.F*5}'
                    
                    await afreecaChat.sock.send(CONNECT_PACKET)
                    time = init.afreeca_titleData.loc[afreecaID,'update_time']
                    time = datetime(int(time[:4]),int(time[5:7]),int(time[8:10]),int(time[11:13]),int(time[14:16]),int(time[17:19]))
                    chatChannelId = init.afreeca_titleData.loc[afreecaID, 'chatChannelId']
                    if (datetime.now() - time).seconds < 60: asyncio.create_task(base.async_errorPost(f"{afreecaID} 연결 완료 {chatChannelId}", errorPostBotURL=environ['chat_post_url']))
                    else: print(f"{datetime.now()} {afreecaID} 연결 완료 {chatChannelId}")
                    await asyncio.sleep(2)
                    await afreecaChat.sock.send(JOIN_PACKET)

                    ping_task = asyncio.create_task(self.ping(init, afreecaChat, afreecaID))
                    receive_task = asyncio.create_task(self.receive_messages(init, afreecaChat, afreecaID))

                    await asyncio.gather(ping_task, receive_task)

            except Exception as e:
                print(f"{datetime.now()} ERROR: {afreecaID} {e}")
                await asyncio.sleep(5)

    async def ping(self, init: base.initVar, afreecaChat: afreecaChatData, afreecaID):
        start_timer = None
        while not init.chat_json[afreecaID]:
            if start_timer == None: 
                start_timer = asyncio.create_task(base.timer(60))
            if start_timer and start_timer._result == "CLOSE": 
                await afreecaChat.sock.send(self.PING_PACKET)
                start_timer = None
            await asyncio.sleep(0.5)
    
    async def receive_messages(self, init: base.initVar, afreecaChat: afreecaChatData, afreecaID):
        time_str = init.afreeca_titleData.loc[afreecaID,'update_time']
        time = datetime(
            int(time_str[:4]), int(time_str[5:7]), int(time_str[8:10]),
            int(time_str[11:13]), int(time_str[14:16]), int(time_str[17:19])
        )

        wait_time = max(600 - (datetime.now() - time).seconds, 60) if (datetime.now() - time).seconds < 600 else 3
        start_timer = asyncio.create_task(base.timer(wait_time))

        TIMEOUT_SECONDS = 3
        MAX_RETRIES = 60
        RECONNECT_THRESHOLD = 4  

        close_timer = None
        count = 0

        while True:
            live_state = init.afreeca_titleData.loc[afreecaID,'live_state']

            if live_state == "CLOSE" and close_timer is None:
                close_timer = asyncio.create_task(base.should_terminate(afreecaChat.sock, afreecaID))
            elif close_timer and live_state == "OPEN":
                self.change_afreeca_chat_json(init, afreecaID)
                asyncio.create_task(base.async_errorPost(f"{afreecaID}: 재연결을 위해 정상적으로 종료되었습니다.", errorPostBotURL=environ['chat_post_url']))
                break

            if close_timer and close_timer._result == "CLOSE":
                self.change_afreeca_chat_json(init, afreecaID)
                asyncio.create_task(base.async_errorPost(f"{afreecaID}: 연결이 정상적으로 종료되었습니다.", errorPostBotURL=environ['chat_post_url']))
                break

            if (start_timer._result != "CLOSE" and count > RECONNECT_THRESHOLD) or init.chat_json[afreecaID] or count > MAX_RETRIES:
                self.change_afreeca_chat_json(init, afreecaID)
                asyncio.create_task(base.async_errorPost(f"afreeca chat reconnect {afreecaID}", errorPostBotURL=environ['chat_post_url']))
                break

            try:
                data = await asyncio.wait_for(
                    afreecaChat.sock.recv(), 
                    timeout=TIMEOUT_SECONDS
                )
                count = await self.decode_message(init, afreecaChat, data, afreecaID, count)

                await self.postChat(init, afreecaChat)
            except asyncio.TimeoutError:
                count += 1
                continue
            except websockets.exceptions.ConnectionClosed:
                asyncio.create_task(base.async_errorPost(f"{afreecaID}: 연결이 정상적으로 종료되었습니다.", errorPostBotURL=environ['chat_post_url']))
                self.change_afreeca_chat_json(init, afreecaID)
                break
            except Exception as e: 
                asyncio.create_task(base.async_errorPost(f"{afreecaID} afreeca chat test except {e}"))
                self.change_afreeca_chat_json(init, afreecaID)
                break

    @staticmethod
    async def decode_message(init: base.initVar, afreecaChat: afreecaChatData, bytes_data, afreecaID, count):
        parts = bytes_data.split(b'\x0c')
        messages = [part.decode('utf-8', errors='ignore') for part in parts]
        
        if len(messages) < 7 or messages[1] in ['-1', '', '1'] or len(messages[2]) == 0 or messages[2] in ["1"] or ("fw" in messages[2]):
            try:
                if fafreeca_chat_TF(messages): 
                    asyncio.create_task(base.async_errorPost(f"아프리카 chat recv messages {messages}", errorPostBotURL=environ['afreeca_chat_log_url']))
                else:
                    return 0
            except: 
                asyncio.create_task(base.async_errorPost(f"아프리카 chat decode_message2 {messages}", errorPostBotURL=environ['afreeca_chat_log_url']))
            return count
        
        count = 0
        user_id, chat, nickname = messages[2], messages[1], messages[6]

        user_id = user_id.split("(")[0]

        if nickname not in [*init.afreeca_chatFilter["channelName"]] or user_id not in [*init.afreeca_chatFilter["channelID"]]: 
            return 0
        
        try: 
            stateData = loads(get(afreeca_getLink(user_id), headers=base.getChzzkHeaders(), timeout=3).text)
            user_nick = stateData['station']['user_nick']
            _, _, thumbnail_url = base.afreeca_getChannelOffStateData(stateData, stateData["station"]["user_id"])
        except Exception as e:
            base.errorPost(f"{datetime.now()} error station {e}")
            return 0

        if nickname != user_nick:
            return 0
        
        # 메시지 중복 체크
        message_id = f"{chat}_{time()}"
        if message_id not in afreecaChat.processed_messages:
            afreecaChat.processed_messages.append(message_id)
            if len(afreecaChat.processed_messages) > 20: 
                afreecaChat.processed_messages.pop(0)

            afreecaChat.afreeca_chat_msg_List.append([nickname, chat, afreecaID, thumbnail_url])
            afreecaName = init.afreecaIDList.loc[afreecaID, 'channelName']
            print(f"{datetime.now()} [채팅 - {afreecaName}] {nickname}: {chat}")
        else:
            print(f"{datetime.now()} 중복 메시지 무시: {chat}")
        return 0

    async def postChat(self, init: base.initVar, afreecaChat: afreecaChatData): #send to chatting message
        try:
            post_msg_count = 0
            while afreecaChat.afreeca_chat_msg_List and post_msg_count < 1:
                chatDic = self.addChat(afreecaChat)
                tasks = []
                for chatDicKey in chatDic:
                    list_of_urls = await self.make_chat_list_of_urls(init, chatDic, chatDicKey)
                    if list_of_urls:
                        task = asyncio.create_task(base.async_post_message(list_of_urls))
                        tasks.append(task)
                
                if tasks:
                    await asyncio.gather(*tasks)
                    print(f"{datetime.now()} post chat")
                    post_msg_count += 1
        except Exception as e:
            base.errorPost(f"error postChat: {str(e)}")

    async def make_chat_list_of_urls(self, init: base.initVar, chatDic, chatDicKey):
        name, channelID, thumbnail_url = chatDicKey

        try:
            message = self.make_thumbnail_url(init, name, chatDic[name, channelID, thumbnail_url], channelID, thumbnail_url)

            if init.DO_TEST:
                # return [(environ['errorPostBotURL'], message)]
                return []
            
            return [
                (discordWebhookURL, message)
                for discordWebhookURL in init.userStateData['discordURL']
                if init.userStateData.loc[discordWebhookURL, "chat_user_json"] and 
                name in init.userStateData.loc[discordWebhookURL, "chat_user_json"].get(channelID, [])
            ]
        except KeyError as e:
            base.errorPost(f"KeyError in make_chat_list_of_urls: {str(e)}")
        except AttributeError as e:
            base.errorPost(f"AttributeError in make_chat_list_of_urls: {str(e)}")
        except Exception as e:
            base.errorPost(f"Unexpected error in make_chat_list_of_urls: {str(e)}")
        
        return []
    
    def make_thumbnail_url(self, init: base.initVar, name, chat, channelID, thumbnail_url):
        return {'content'   : chat,
                "username"  : name + " >> " + init.afreecaIDList.loc[channelID, 'channelName'],
                "avatar_url": thumbnail_url}

    def addChat(init: base.initVar, afreecaChat: afreecaChatData): # 같은 사람이 빠르게 여러번 채팅을 입력했다면 합치기
        try:
            chatDic = {}
            afreeca_chatList = afreecaChat.afreeca_chat_msg_List[:]
            for name, chat, channelID, thumbnail_url in afreeca_chatList:
                key = (name, channelID, thumbnail_url)
                if key not in chatDic:
                    if len(chatDic) != 0:
                        return chatDic
                    chatDic[key] = str(chat)
                    afreecaChat.afreeca_chat_msg_List = afreecaChat.afreeca_chat_msg_List[1:]
                else:
                    chatDic[key] += f"\n{chat}"
                    afreecaChat.afreeca_chat_msg_List = afreecaChat.afreeca_chat_msg_List[1:]
            
        except Exception as e:
            base.errorPost(f"error addChat {e}")
            return chatDic
        return chatDic

    @staticmethod
    def calculate_byte_size(string):
        return len(string.encode('utf-8')) + 6

    def change_afreeca_chat_json(self, init: base.initVar, afreecaID, afreeca_chat_TF = True):
        init.chat_json[afreecaID] = afreeca_chat_TF
        supabase = create_client(environ['supabase_url'], environ['supabase_key'])
        supabase.table('date_update').upsert({"idx": 0, "chat_json": init.chat_json}).execute()

def fafreeca_chat_TF(messages):
    # 기본 제외 조건들을 리스트로 정의
    excluded_values = {'-1', '1', '', '0', '2', '4'}
    
    if len(messages) == 1:
        return 0
     
    # 첫 번째 검사: messages[1] 확인
    if (messages[1] in excluded_values or 
        '|' in messages[1] or 
        'fw' in messages[2] or 
        'CHALLENGE_GIFT' in messages[1]):
        return 0
        
    # 두 번째 검사: 특정 문자열 체크
    if ('png' in str(messages) or 
        'https://smartstore.naver.com' in str(messages) or 
        '씨발' in str(messages)):
        return 0
        
    # 세 번째 검사: messages[2] 확인 및 messages[7] 체크
    if (messages[2] in {'-1', '1', '', '0'} or 
        '|' in messages[2] or 
        (len(messages) >= 7 and '|' in messages[7])):
        return 0
        
    return 1

def check_is_passwordDict(init: base.initVar, afreecaID):
    stateData = loads(get(afreeca_getLink(init.afreecaIDList["afreecaID"][afreecaID]), headers=base.getChzzkHeaders(), timeout=3).text)
    return stateData['broad'].get('is_password',{False})

async def main():
    init = base.initVar()
    chat = AfreecaChat()

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--streamer_id', type=str, default='charmel')
    # args = parser.parse_args()

    while True:
        await chat.connect_to_chat(init)

if __name__ == "__main__":
    asyncio.run(main())
