import asyncio
import aiohttp
import chzzk_api
import websockets
from os import environ
from requests import get
from datetime import datetime
from urllib.parse import unquote
from json import loads, dumps, JSONDecodeError
from dataclasses import dataclass
from supabase import create_client
from cmd_type import CHZZK_CHAT_CMD
from base import getChzzkHeaders, getChzzkCookie, async_post_message, async_errorPost, should_terminate, timer, initVar, if_after_time

@dataclass
class chzzkChatData:
    chzzk_chat_msg_List: list
    sock: websockets.connect
    chzzk_chat_count: int = 40000
    Join_count: int = 0
    joinChatCount: int = 200
    sid: str = ""
    cid: str = ""
    accessToken: str = ""
    extraToken: str = ""
    chzzkID: str = ""
    
    def __post_init__(self):
        # 이벤트 객체 초기화
        self.chat_event = asyncio.Event()

class chzzk_chat_message:
    async def chatMsg(self, init: initVar, chzzkID):
        while True:
            
            if init.chat_json[chzzkID]: change_chzzk_chat_json(init, chzzkID, False)
            if init.chzzk_titleData.loc[chzzkID,'live_state'] == "CLOSE":
                await asyncio.sleep(5)
                continue
            
            async with websockets.connect('wss://kr-ss3.chat.naver.com/chat', subprotocols=['chat'], ping_interval=None) as sock:
                try:
                    chzzkChat = chzzkChatData(
                        chzzk_chat_msg_List = [],
                        sock = sock,
                        cid = init.chzzk_titleData.loc[chzzkID, 'chatChannelId'],
                        chzzkID = chzzkID
                    )
                    
                    # if init.chat_json[chzzkID]: change_chzzk_chat_json(init, chzzkID, False)
                    await connect(chzzkChat, if_chzzk_Join(init, chzzkChat))
                    
                    ping_task = asyncio.create_task(self.ping(init, chzzkChat))
                    receive_task = asyncio.create_task(self.receive_message(init, chzzkChat))

                    await asyncio.gather(ping_task, receive_task)
                except Exception as e:
                    asyncio.create_task(async_errorPost(f"error chatMsg {e}"))
                    change_chzzk_chat_json(init, chzzkChat.chzzkID)
                
    async def receive_message(self, init: initVar, chzzkChat: chzzkChatData):
        close_timer = None
        # 메시지 큐 추가 - 메시지를 저장할 대기열
        message_queue = asyncio.Queue()
        # 메시지 수신 및 처리를 위한 태스크
        receiver_task = None
        processor_task = None

        try:
            # 메시지 수신 및 처리 태스크 시작
            receiver_task = asyncio.create_task(self.message_receiver(init, chzzkChat, message_queue))
            processor_task = asyncio.create_task(self.message_processor(init, chzzkChat, message_queue))
            chat_processor = asyncio.create_task(self.postChat(init, chzzkChat))
                        
            
            while True:
                try:
                    live_state = init.chzzk_titleData.loc[chzzkChat.chzzkID, 'live_state']

                    # 방송 상태 처리
                    if live_state == "CLOSE" and close_timer is None:
                        close_timer = asyncio.create_task(should_terminate(chzzkChat.sock, chzzkChat.chzzkID))
                    elif (live_state == "OPEN" and close_timer) or if_chzzk_Join(init, chzzkChat):
                        change_chzzk_chat_json(init, chzzkChat.chzzkID)
                        close_timer = None
                        if not init.DO_TEST:
                            asyncio.create_task(async_errorPost(
                                f"{chzzkChat.chzzkID}: 재연결을 위해 종료되었습니다.",
                                errorPostBotURL=environ['chat_post_url']
                            ))
                        break

                    # 종료 조건 체크
                    if close_timer and close_timer._result == "CLOSE":
                        change_chzzk_chat_json(init, chzzkChat.chzzkID)
                        if not init.DO_TEST:
                            asyncio.create_task(async_errorPost(
                                f"{chzzkChat.chzzkID}: 연결이 정상적으로 종료되었습니다.",
                                errorPostBotURL=environ['chat_post_url']
                            ))
                        break
                    
                    # 주기적으로 상태 확인
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    asyncio.create_task(async_errorPost(f"error receive_message {e}"))
        finally:
            # 종료 시 태스크 정리
            if receiver_task and not receiver_task.done():
                receiver_task.cancel()
            if processor_task and not processor_task.done():
                processor_task.cancel()
            if chat_processor and not chat_processor.done():
                chat_processor.cancel()

    async def message_receiver(self, init: initVar, chzzkChat: chzzkChatData, message_queue: asyncio.Queue):
        """지속적으로 웹소켓에서 메시지를 수신하여 큐에 추가하는 함수"""
        while True:
            try:
                # WebSocket 연결 상태 확인
                if chzzkChat.sock.closed:
                    chzzkChat.Join_count += 200
                    await asyncio.sleep(1)
                    continue
                
                # 논블로킹 방식으로 메시지 수신 시도
                try:
                    raw_message = await asyncio.wait_for(chzzkChat.sock.recv(), timeout=2)
                    chzzkChat.Join_count = 0
                    message_data = loads(raw_message)
                    
                    # 메시지 큐에 추가
                    await message_queue.put(message_data)
                    
                except asyncio.TimeoutError:
                    if init.chzzk_titleData.loc[chzzkChat.chzzkID, 'live_state'] == "OPEN":
                        chzzkChat.Join_count += 1
                    await asyncio.sleep(0.05)
                    continue
                    
                except (JSONDecodeError, ConnectionError, RuntimeError, websockets.exceptions.ConnectionClosed) as e:
                    if init.chzzk_titleData.loc[chzzkChat.chzzkID, 'live_state'] == "OPEN":
                        chzzkChat.Join_count += 1
                        print(f"{datetime.now()} Join_count{chzzkChat.chzzkID} 2.{chzzkChat.Join_count}.{e}")
                        
                        # 연결 관련 오류 처리 개선
                        if (isinstance(e, (JSONDecodeError, ConnectionError, websockets.exceptions.ConnectionClosed)) 
                            or str(e) == "socket is already closed." 
                            or "no close frame received or sent" in str(e)):
                            chzzkChat.Join_count += 200
                            try:
                                await chzzkChat.sock.close()
                            except:
                                pass
                    await asyncio.sleep(0.5)  # 에러 발생 시 약간 더 긴 대기 시간
                    continue
                    
            except Exception as e:
                print(f"{datetime.now()} Error in message_receiver: {e}")
                asyncio.create_task(async_errorPost(f"Error in message_receiver: {e}"))
                await asyncio.sleep(1)  # 예외 발생 시 잠시 대기

    async def message_processor(self, init: initVar, chzzkChat: chzzkChatData, message_queue: asyncio.Queue):
        """큐에서 메시지를 가져와 처리하는 함수"""
        while True:
            try:
                # 큐에서 메시지 가져오기
                raw_message = await message_queue.get()
                
                try:
                    chat_cmd = raw_message['cmd']

                    if chat_cmd == CHZZK_CHAT_CMD['ping']: 
                        await chzzkChat.sock.send(dumps(CHZZK_CHAT_DICT(chzzkChat, "pong")))
                        continue

                    # 채팅 타입 결정
                    chat_type = {
                        CHZZK_CHAT_CMD['chat']: '채팅',
                        CHZZK_CHAT_CMD['request_chat']: '채팅',
                        CHZZK_CHAT_CMD['donation']: '후원'
                    }.get(chat_cmd, '모름')

                    # 에러 체크
                    if chat_type != "후원" and raw_message['tid'] is None:
                        bdy = raw_message.get('bdy', {})
                        if message := bdy.get('message'):
                            asyncio.create_task(async_errorPost(str(message)))
                        continue

                    # 임시 제한 처리
                    bdy = raw_message.get('bdy', {})
                    if isinstance(bdy, dict) and bdy.get('type') == 'TEMPORARY_RESTRICT':
                        duration = bdy.get('duration', 30)
                        print(f"{datetime.now()} 임시 제한 상태입니다. {duration}초 동안 대기합니다.")
                        await asyncio.sleep(duration)
                        continue
                    
                    # 메시지 리스트 처리
                    if isinstance(bdy, dict) and 'messageList' in bdy:
                        chat_data = bdy['messageList']
                        chzzk_chat_list = [msg for msg in chat_data]
                    else:
                        chat_data = bdy if isinstance(bdy, list) else [bdy]
                        chzzk_chat_list = [msg for msg in chat_data]

                    if chzzk_chat_list:
                        # 메시지 필터링 처리
                        await self.filter_message(init, chzzkChat, chzzk_chat_list, chat_type)
                        
                        # 채팅 메시지가 추가되었다면 postChat 이벤트 발생
                        if chzzkChat.chzzk_chat_msg_List:
                            # 채팅 메시지가 있을 때만 postChat을 실행하도록 이벤트 설정
                            chzzkChat.chat_event.set()
                    
                except Exception as e:
                    asyncio.create_task(async_errorPost(f"Error processing message: {e}, {str(raw_message)}"))
                
                # 큐 작업 완료 신호
                message_queue.task_done()
                
            except Exception as e:
                print(f"{datetime.now()} Error in message_processor: {e}")
                asyncio.create_task(async_errorPost(f"Error in message_processor: {e}"))
                await asyncio.sleep(0.5)  # 예외 발생 시 잠시 대기
         
    def get_nickname(self, chat_data):
        if chat_data['extras'] is None or loads(chat_data['extras']).get('styleType', {}) in [1, 2, 3]:
            return None
            
        # Handle anonymous users
        user_id = chat_data.get('uid', chat_data.get('userId'))
        if user_id == 'anonymous':
            return '익명의 후원자'
        
        # Parse and validate profile data
        try:
            if '%7D' in chat_data['profile']: chat_data['profile'] = unquote(chat_data['profile'])
            profile_data = loads(chat_data['profile'])
            if isinstance(profile_data, dict) and 'nickname' in profile_data:
                return profile_data['nickname']
        except:
            pass
            
        return '(알 수 없음)'

    async def filter_message(self, init: initVar, chzzkChat: chzzkChatData, chzzk_chat_list, chat_type):
        for chat_data in chzzk_chat_list:
            nickname = self.get_nickname(chat_data)
            try:
                if nickname is None:
                    continue
                if not init.DO_TEST and (chat_type == "후원" or nickname in [*init.chzzk_chatFilter["channelName"]]):
                    asyncio.create_task(print_msg(init, chat_data, chat_type, chzzkChat.chzzkID, nickname))

                if not(nickname in [*init.chzzk_chatFilter["channelName"]]):
                    asyncio.create_task(print_msg(init, chat_data, chat_type, chzzkChat.chzzkID, nickname, post_msg_TF=False))

                if nickname not in [*init.chzzk_chatFilter["channelName"]]: #chzzk_chatFilter에 없는 사람 채팅은 제거
                    return

                if 'msg' in chat_data:
                    msg = chat_data['msg']
                elif 'content' in chat_data:
                    msg = chat_data['content']
                else:
                    continue  # 메시지가 없으면 다음 chat_data로 넘어갑니다.

                if msg and msg[0] in [">"]:
                    msg = "/" + msg
                chzzkChat.chzzk_chat_msg_List.append([nickname, msg, chat_type, chat_data.get('uid') or chat_data.get('userId')])

            except Exception as e:
                asyncio.create_task(async_errorPost(f"error process_message {e}"))

    async def postChat(self, init: initVar, chzzkChat: chzzkChatData):
        # Event 객체가 없으면 생성
        if not hasattr(chzzkChat, 'chat_event'):
            chzzkChat.chat_event = asyncio.Event()
        
        while True:
            # 이벤트가 설정될 때까지 대기 (메시지가 추가될 때)
            await chzzkChat.chat_event.wait()
            
            try:
                if chzzkChat.chzzk_chat_msg_List:
                    # 메시지 처리를 백그라운드 태스크로 분리
                    asyncio.create_task(self.process_chat_messages(init, chzzkChat))
                
                chzzkChat.chat_event.clear()
                
            except Exception as e:
                
                asyncio.create_task(async_errorPost(f"error postChat: {str(e)}"))
                # 오류가 발생해도 이벤트를 초기화
                chzzkChat.chat_event.clear()

    async def process_chat_messages(self, init: initVar, chzzkChat: chzzkChatData):
        try:

            # URL 생성
            list_of_urls = await self.make_chat_list_of_urls(init, chzzkChat)
            
            if list_of_urls:
                # 타임아웃 및 예외 처리를 포함한 메시지 전송
                try:
                    await asyncio.wait_for(
                        async_post_message(list_of_urls), 
                        timeout=30  # 30초 타임아웃
                    )
                except asyncio.TimeoutError:
                    asyncio.create_task(async_errorPost("Message sending timed out"))
                except Exception as e:
                    asyncio.create_task(async_errorPost(f"Error sending messages: {e}"))
            
        except Exception as e:
            asyncio.create_task(async_errorPost(f"Error in process_chat_messages: {e}"))

    async def ping(self, init: initVar, chzzkChat: chzzkChatData):
        start_timer = None
        while not init.chat_json[chzzkChat.chzzkID]:
            try:
                if start_timer == None: start_timer = asyncio.create_task(timer(10))
                if start_timer and start_timer._result == "CLOSE": 
                    await chzzkChat.sock.send(dumps(CHZZK_CHAT_DICT(chzzkChat, "pong")))
                    start_timer = None
            except Exception as e:
                asyncio.create_task(async_errorPost(f"error pong {e}"))
            await asyncio.sleep(0.5)
        
        print(f"{chzzkChat.chzzkID} chat pong 종료")
                
    async def make_chat_list_of_urls(self, init: initVar, chzzkChat: chzzkChatData):
        result_urls = []
        try:
            name, chat, chat_type, uid = chzzkChat.chzzk_chat_msg_List.pop(0)

            chzzkName = init.chzzkIDList.loc[chzzkChat.chzzkID, 'channelName']
            print(f"{datetime.now()} post message [{chat_type} - {chzzkName}]{name}: {chat}")

            message = await self.make_thumbnail_url(init, name, chat, chzzkChat.chzzkID, uid)
            
            if init.DO_TEST:
                # return [(environ['errorPostBotURL'], message)]
                return result_urls
            
            for discordWebhookURL in init.userStateData['discordURL']:
                try:
                    if (init.userStateData.loc[discordWebhookURL, "chat_user_json"] and 
                        name in init.userStateData.loc[discordWebhookURL, "chat_user_json"].get(chzzkChat.chzzkID, [])):
                        result_urls.append((discordWebhookURL, message))
                except (KeyError, AttributeError):
                    # 특정 URL 처리 중 오류가 발생해도 다른 URL 처리는 계속 진행
                    continue
                
            return result_urls
        except Exception as e:
            asyncio.create_task(async_errorPost(f"Error in make_chat_list_of_urls: {type(e).__name__}: {str(e)}"))
            return result_urls

    def addChat(self, chzzkChat: chzzkChatData):

        chatDic = {}
        
        try:
            # 원본 리스트를 직접 순회하면서 처리
            while chzzkChat.chzzk_chat_msg_List:
                # 첫 번째 메시지 가져오기
                name, chat, chat_type, channelID, uid = chzzkChat.chzzk_chat_msg_List[0]
                key = (name, channelID, uid)
                
                # 이미 해당 사용자의 메시지가 있으면 추가, 없으면 새로 생성
                if key in chatDic:
                    chatDic[key] += f"\n{chat}"
                else:
                    chatDic[key] = str(chat)
                
                # 처리한 메시지 제거
                chzzkChat.chzzk_chat_msg_List.pop(0)
                
        except Exception as e:
            asyncio.create_task(async_errorPost(f"addChat 함수 실행 중 오류 발생: {e}"))
        
        return chatDic

    async def make_thumbnail_url(self, init: initVar, name, chat, channelID, uid):
        thumbnail_url = environ['default_thumbnail']  # 변수를 기본값으로 미리 초기화
        
        for attempt in range(3):
            try:
                headers = getChzzkHeaders()
                Cookie = getChzzkCookie()
                
                # response = get(self.chzzk_getLink(uid), headers=headers, cookies=Cookie, timeout=3)
                # if response.status_code != 200:
                #     await asyncio.sleep(0.05)
                #     continue
                thumbnail_url = None
                async with aiohttp.ClientSession() as session:  # aiohttp 사용
                    async with session.get(
                        self.chzzk_getLink(uid), 
                        headers=headers, 
                        cookies=Cookie, 
                        timeout=aiohttp.ClientTimeout(total=5)  # timeout 증가
                    ) as response:
                        if response.status != 200:
                            await asyncio.sleep(0.1)  # 대기 시간 약간 증가
                            continue

                        data = await response.json()
                        thumbnail_url = data["content"]["channelImageUrl"]

                # thumbnail_url = loads(response.text)["content"]["channelImageUrl"]
                if thumbnail_url and thumbnail_url.startswith(("https://nng-phinf.pstatic.net", "https://ssl.pstatic.net")):
                    break
                if thumbnail_url is None or len(thumbnail_url) == 0:
                    thumbnail_url = environ['default_thumbnail']
                    break

                
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                await asyncio.sleep(0.05)
                asyncio.create_task(async_errorPost(f"error make thumbnail url {name}.{chat}.(attempt {attempt + 1}/3): {str(e)}"))
                
            except Exception as e:
                await asyncio.sleep(0.05)
                asyncio.create_task(async_errorPost(f"unexpected error in make thumbnail url: {str(e)}"))
        else:
            thumbnail_url = environ['default_thumbnail']

        return {'content'   : chat,
                "username"  : name + " >> " + init.chzzkIDList.loc[channelID, 'channelName'],
                "avatar_url": thumbnail_url}

    def chzzk_getLink(self, uid):
        return f'https://api.chzzk.naver.com/service/v1/channels/{uid}'
    
  
def change_chzzk_chat_json(init: initVar, chzzkID, chzzkID_chat_TF = True):
    init.chat_json[chzzkID] = chzzkID_chat_TF
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    supabase.table('date_update').upsert({"idx": 0, "chat_json": init.chat_json}).execute()
    
async def connect(chzzkChat: chzzkChatData, TF = 0):
    
    chzzkChat.chzzk_chat_count = 40000
    chzzkChat.accessToken, chzzkChat.extraToken = chzzk_api.fetch_accessToken(chzzkChat.cid, getChzzkCookie())
    
    await chzzkChat.sock.send(dumps(CHZZK_CHAT_DICT(chzzkChat, "connect")))
    sock_response = loads(await chzzkChat.sock.recv())
    chzzkChat.sid = sock_response['bdy']['sid']

    await chzzkChat.sock.send(dumps(CHZZK_CHAT_DICT(chzzkChat, "recentMessageCount", num = 50)))
    sock_response = loads(await chzzkChat.sock.recv())

    # if TF == 2:
    asyncio.create_task(async_errorPost(f"{chzzkChat.chzzkID} 연결 완료 {chzzkChat.cid}", errorPostBotURL=environ['chat_post_url']))
    # else: 
    #     print(f"{datetime.now()} {chzzkID} 연결 완료 {chzzkChat.cid}")

def send(chzzkChat: chzzkChatData, message):
    default_dict = {
        "ver": 2,
        "svcid": "game",
        "cid": chzzkChat.cid,
    }

    extras = {
        "chatType": "STREAMING",
        "emojis": "",
        "osType": "PC",
        "extraToken": chzzkChat.extraToken,
        "streamingChannelId": chzzkChat.cid
    }

    send_dict = {
        "tid": 3,
        "cmd": CHZZK_CHAT_CMD['send_chat'],
        "retry": False,
        "sid": chzzkChat.sid,
        "bdy": {
            "msg": message,
            "msgTypeCode": 1,
            "extras": dumps(extras),
            "msgTime": int(datetime.now().timestamp())
        }
    }

    chzzkChat.sock.send(dumps(dict(send_dict, **default_dict)))

def if_chzzk_Join(init: initVar, chzzkChat: chzzkChatData) -> int:
    try:
        
        if not if_after_time(init.chzzk_titleData.loc[chzzkChat.chzzkID,'update_time']):
            chzzkChat.cid = chzzk_api.fetch_chatChannelId(init.chzzkIDList.loc[chzzkChat.chzzkID, "channel_code"])
            if chzzkChat.cid != init.chzzk_titleData.loc[chzzkChat.chzzkID, 'chatChannelId']:
                change_chatChannelId(init, chzzkChat)
                return 2
        if chzzkChat.Join_count >= chzzkChat.joinChatCount or chzzkChat.chzzk_chat_count == 0 or init.chat_json[chzzkChat.chzzkID]:
            return 1
        
    except Exception as e: asyncio.create_task(async_errorPost(f"error if_chzzk_Join {chzzkChat.chzzkID}.{e}"))
    return 0

def change_chatChannelId(init: initVar, chzzkChat: chzzkChatData):
    idx = {chzzk: i for i, chzzk in enumerate(init.chzzk_titleData["channelID"])}
    init.chzzk_titleData.loc[chzzkChat.chzzkID, 'oldChatChannelId'] = init.chzzk_titleData.loc[chzzkChat.chzzkID, 'chatChannelId']
    init.chzzk_titleData.loc[chzzkChat.chzzkID, 'chatChannelId'] = chzzkChat.cid
    updates = {
        'oldChatChannelId': init.chzzk_titleData.loc[chzzkChat.chzzkID, 'oldChatChannelId'],
        'chatChannelId': init.chzzk_titleData.loc[chzzkChat.chzzkID, 'chatChannelId']}
    init.chzzk_titleData.loc[chzzkChat.chzzkID, updates.keys()] = updates.values()

    supabase_data = {
        "idx": idx[chzzkChat.chzzkID],
        **updates}
    
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    supabase.table('chzzk_titleData').upsert(supabase_data).execute()
       
async def print_msg(init: initVar, chat_data, chat_type, chzzkID, nickname, post_msg_TF=True):
    chzzkName = init.chzzkIDList.loc[chzzkID, 'channelName']
    def format_message(msg_type, nickname, message, time, **kwargs):
        base = f"[{chat_type} - {chzzkName}] {nickname}"
        if msg_type == 'donation':
            return f"{base} ({kwargs.get('amount')}치즈): {message}, {time}"
        return f"{base}: {message}, {time}"
    
    try:
        if chat_type == "후원":
            extras = loads(chat_data['extras'])
            if 'payAmount' in extras:
                message = format_message('donation', nickname, chat_data['msg'], chat_data['msgTime'], amount=extras['payAmount'])
            else:
                return  # 도네이션 금액이 없는 경우 처리하지 않음

        else:
            msg = chat_data['msg'] if chat_type == "채팅" else chat_data['content']
            time = chat_data['msgTime'] if chat_type == "채팅" else chat_data['messageTime']
            time = datetime.fromtimestamp(time/1000)
            
            message = format_message('chat', nickname, msg, time)
            
        if post_msg_TF:
            asyncio.create_task(async_errorPost(message, errorPostBotURL=environ['donation_post_url']))
        else:
            print(f"{datetime.now()} {message}")

    except Exception as e:
        if chat_type == "후원":
            asyncio.create_task(async_errorPost(f"{datetime.now()} it is test {e}.{loads(chat_data['extras'])}"))

async def sendHi(init: initVar, chzzkChat: chzzkChatData, himent):
    if if_chzzk_Join(init, chzzkChat) == 2:
        async with websockets.connect('wss://kr-ss3.chat.naver.com/chat', timeout=3.0) as websocket:
            await connect(chzzkChat)
        asyncio.create_task(async_errorPost(f"send hi {init.chzzkIDList.loc[chzzkChat.chzzkID, 'channelName']} {chzzkChat.cid}"))
        send(chzzkChat, himent)

def onAirChat(chzzkChat: chzzkChatData, message):
    himent = None
    if message != "뱅온!":return
    if chzzkChat.chzzkID == "charmel"   	: himent = "챠하"
    if chzzkChat.chzzkID == "mawang0216"	: himent = "마하"
    if chzzkChat.chzzkID == "bighead033"	: himent = "빅하"
    if chzzkChat.chzzkID == "suisui_again": himent = "싀하"
    if himent: send(chzzkChat, himent)

def offAirChat(chzzkChat: chzzkChatData):
    byement = None
    if chzzkChat.chzzkID =="charmel"   : byement = "챠바"
    if chzzkChat.chzzkID =="mawang0216": byement = "마바"
    if chzzkChat.chzzkID =="bighead033": byement = "빅바"
    if byement: send(chzzkChat, byement)

def chzzk_connect_count(init: initVar, chzzkChat: chzzkChatData, num = 1):
    if chzzkChat.chzzk_chat_count > 0 and init.chzzk_titleData.loc[chzzkChat.chzzkID,"live_state"] == "OPEN": chzzkChat.chzzk_chat_count -= num
    if chzzkChat.chzzk_chat_count < 0: chzzkChat.chzzk_chat_count = 0

def CHZZK_CHAT_DICT(chzzkChat: chzzkChatData, option = "connect", num = 50):
    default_dict = {
        "ver": "2",
        "svcid": "game",
        "cid": chzzkChat.cid,
    }
    if option == "connect":
        send_dict = {
            "cmd": CHZZK_CHAT_CMD['connect'],
            "tid": 1,
            "bdy": {
                "uid": chzzk_api.fetch_userIdHash(getChzzkCookie()),
                "devType": 2001,
                "accTkn": chzzkChat.accessToken,
                "auth": "SEND"
            }
        } 
    elif option == "recentMessageCount":
        send_dict = {
            "cmd": CHZZK_CHAT_CMD['request_recent_chat'],
            "tid": 2,
            "sid": chzzkChat.sid,
            "bdy": {
                "recentMessageCount": num
            }
        }
    elif option == "pong":
        return {
            "ver" : "2",
            "cmd" : CHZZK_CHAT_CMD['pong']
            }
    
    return dict(send_dict, **default_dict)

# async def main():
#     init = await base2.initVar()
#     await base2.discordBotDataVars(init)
#     await base2.userDataVars(init)
#     await base2.firstMessage(init)
#     chzzkID = "1f1d41ca22a06ad13fc40efe5d7f8917"
#     chzzkchat = chzzk_chat_message()
#     await chzzk_joinchat(init)
#     await chzzkchat.getChatList(init, chzzkID)

# if __name__ == "__main__":
#     asyncio.run(main())