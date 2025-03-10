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
from base import errorPost, getChzzkHeaders, getChzzkCookie, async_post_message, async_errorPost, should_terminate, timer, initVar, if_after_time

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
                        cid = init.chzzk_titleData.loc[chzzkID, 'chatChannelId']
                    )
                    
                    # if init.chat_json[chzzkID]: change_chzzk_chat_json(init, chzzkID, False)
                    await connect(chzzkChat, chzzkID, if_chzzk_Join(init, chzzkID, chzzkChat))
                    
                    ping_task = asyncio.create_task(self.ping(init, chzzkChat, chzzkID))
                    receive_task = asyncio.create_task(self.receive_message(init, chzzkChat, chzzkID))

                    await asyncio.gather(ping_task, receive_task)
                except Exception as e:
                    await async_errorPost(f"error chatMsg {e}")
                    change_chzzk_chat_json(init, chzzkID)
                
    async def receive_message(self, init: initVar, chzzkChat: chzzkChatData, chzzkID):
        close_timer = None

        while True:
            try:
                live_state = init.chzzk_titleData.loc[chzzkID, 'live_state']

                # 방송 상태 처리
                if live_state == "CLOSE" and close_timer is None:
                    close_timer = asyncio.create_task(should_terminate(chzzkChat.sock, chzzkID))
                elif (live_state == "OPEN" and close_timer) or if_chzzk_Join(init, chzzkID, chzzkChat):
                    change_chzzk_chat_json(init, chzzkID)
                    close_timer = None
                    if not init.DO_TEST:
                        await async_errorPost(
                            f"{chzzkID}: 재연결을 위해 종료되었습니다.",
                            errorPostBotURL=environ['chat_post_url']
                        )
                    break

                # 종료 조건 체크
                if close_timer and close_timer._result == "CLOSE":
                    change_chzzk_chat_json(init, chzzkID)
                    if not init.DO_TEST:
                        await async_errorPost(
                            f"{chzzkID}: 연결이 정상적으로 종료되었습니다.",
                            errorPostBotURL=environ['chat_post_url']
                        )
                    break

                # 채팅 처리
                await self.getChatList(init, chzzkID, chzzkChat)
                await self.postChat(init, chzzkChat)
            except Exception as e:
                await async_errorPost(f"error receive_message {e}")

    async def getChatList(self, init: initVar, chzzkID, chzzkChat: chzzkChatData):  
        chzzk_connect_count(init, chzzkID, chzzkChat, 1)
        try:
            # raw_message = await self.f_raw_message(init, chzzkID)
            raw_message = await asyncio.wait_for(
                self.f_raw_message(init, chzzkChat, chzzkID), 
                timeout=10
            )
            if not raw_message:
                return
            
            chat_cmd = raw_message['cmd']

            if chat_cmd == CHZZK_CHAT_CMD['ping']: 
                await chzzkChat.sock.send(dumps(CHZZK_CHAT_DICT(chzzkChat, "pong")))
                return

            # 채팅 타입 결정
            chat_type = {
                CHZZK_CHAT_CMD['chat']: '채팅',
                CHZZK_CHAT_CMD['request_chat']: '채팅',
                CHZZK_CHAT_CMD['donation']: '후원'
            }.get(chat_cmd, '모름')

            # 에러 체크
            if chat_cmd != CHZZK_CHAT_CMD['donation'] and raw_message['tid'] is None:
                bdy = raw_message.get('bdy', {})
                if message := bdy.get('message'):
                    errorPost(str(message))
                return

            # 임시 제한 처리
            bdy = raw_message.get('bdy', {})
            if isinstance(bdy, dict) and bdy.get('type') == 'TEMPORARY_RESTRICT':
                duration = bdy.get('duration', 30)
                print(f"{datetime.now()} 임시 제한 상태입니다. {duration}초 동안 대기합니다.")
                await asyncio.sleep(duration)
                return
            
            
            # 메시지 리스트 처리
            if isinstance(bdy, dict) and 'messageList' in bdy:
                chat_data = bdy['messageList']
                chzzk_chat_list = [msg for msg in chat_data]
            else:
                chat_data = bdy if isinstance(bdy, list) else [bdy]
                chzzk_chat_list = [msg for msg in chat_data]

            if chzzk_chat_list:
                await self.process_message(init, chzzkChat, chat_cmd, chzzk_chat_list, chat_type, chzzkID)
            
        except asyncio.TimeoutError:
            print(f"{datetime.now()} Timeout occurred in getChatList for chzzkID: {chzzkID}")
        except Exception as e:
            errorPost(f"error chatMsg3333 {e}.{str(raw_message)}")

    async def f_raw_message(self, init: initVar, chzzkChat: chzzkChatData, chzzkID):
        try:
            # WebSocket 연결 상태 확인 추가
            if chzzkChat.sock.closed:
                chzzkChat.Join_count += 200
                return None
            
            raw_message = await asyncio.wait_for(chzzkChat.sock.recv(), timeout=2)
            chzzkChat.Join_count = 0
            return loads(raw_message)

        except asyncio.TimeoutError:
            if init.chzzk_titleData.loc[chzzkID,'live_state'] == "OPEN":
                chzzkChat.Join_count += 1
                await asyncio.sleep(0.05)
            return None

        except (JSONDecodeError, ConnectionError, RuntimeError, websockets.exceptions.ConnectionClosed) as e:
            if init.chzzk_titleData.loc[chzzkID,'live_state'] == "OPEN":
                chzzkChat.Join_count += 1
                print(f"{datetime.now()} Join_count{chzzkID} 2.{chzzkChat.Join_count}.{e}")
                
                # 연결 관련 오류 처리 개선
                if (isinstance(e, (JSONDecodeError, ConnectionError, websockets.exceptions.ConnectionClosed)) 
                    or str(e) == "socket is already closed." 
                    or "no close frame received or sent" in str(e)):
                    chzzkChat.Join_count += 200
                    try:
                        await chzzkChat.sock.close()
                    except:
                        pass
                    await asyncio.sleep(0.05)
            return None
        except Exception as e:
            if init.chzzk_titleData.loc[chzzkID,'live_state'] == "OPEN":
                chzzkChat.Join_count += 1
                print(f"{datetime.now()} Join_count{chzzkID} 3.{chzzkChat.Join_count}.{e}")
                await asyncio.sleep(0.05)
            return None
            
    async def get_nickname(self, chat_data):
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

    async def process_message(self, init: initVar, chzzkChat: chzzkChatData, chat_cmd, chzzk_chat_list, chat_type, chzzkID):
        for chat_data in chzzk_chat_list:
            nickname = await self.get_nickname(chat_data)
            try:
                if nickname is None:
                    continue
                if not init.DO_TEST and chat_cmd == CHZZK_CHAT_CMD['donation'] or nickname == "ai코딩":
                     asyncio.create_task(print_msg(init, chat_cmd, chat_data, chat_type, chzzkID, nickname))

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
                chzzkChat.chzzk_chat_msg_List.append([nickname, msg, chzzkID, chat_data.get('uid') or chat_data.get('userId')])

                # if not(chat_cmd == CHZZK_CHAT_CMD['donation'] or nickname == "ai코딩"):
                #      asyncio.create_task(print_msg(init, chat_cmd, chat_data, chat_type, chzzkID, nickname, post_msg_TF=False))

            except Exception as e:
                await async_errorPost(f"error process_message {e}")

    async def postChat(self, init: initVar, chzzkChat: chzzkChatData):
        try:
            post_msg_count = 0
            while chzzkChat.chzzk_chat_msg_List and post_msg_count < 1:
                chatDic = self.addChat(chzzkChat)
                tasks = []
                for chatDicKey in chatDic:
                    list_of_urls = await self.make_chat_list_of_urls(init, chatDic, chatDicKey)
                    if list_of_urls:
                        task = asyncio.create_task(async_post_message(list_of_urls))
                        tasks.append(task)
                
                if tasks:
                    await asyncio.gather(*tasks)
                    post_msg_count += 1
        except Exception as e:
            errorPost(f"error postChat: {str(e)}")

    async def ping(self, init: initVar, chzzkChat: chzzkChatData, chzzkID):
        start_timer = None
        while not init.chat_json[chzzkID]:
            try:
                if start_timer == None: start_timer = asyncio.create_task(timer(10))
                if start_timer and start_timer._result == "CLOSE": 
                    await chzzkChat.sock.send(dumps(CHZZK_CHAT_DICT(chzzkChat, "pong")))
                    start_timer = None
            except Exception as e:
                await async_errorPost(f"error pong {e}")
            await asyncio.sleep(0.5)
        
        print(f"{chzzkID} chat pong 종료")
                
    async def make_chat_list_of_urls(self, init: initVar, chatDic, chatDicKey):
        name, channelID, uid = chatDicKey
        
        try:
            message = await self.make_thumbnail_url(init, name, chatDic[name, channelID, uid], channelID, uid)
            
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
            errorPost(f"KeyError in make_chat_list_of_urls: {str(e)}")
        except AttributeError as e:
            errorPost(f"AttributeError in make_chat_list_of_urls: {str(e)}")
        except Exception as e:
            errorPost(f"Unexpected error in make_chat_list_of_urls: {str(e)}")
        
        return []

    def addChat(self, chzzkChat: chzzkChatData): # 같은 사람이 빠르게 여러번 채팅을 입력했다면 합치기
        try:
            chatDic = {}
            chzzk_chatList = chzzkChat.chzzk_chat_msg_List[:]
            for name, chat, channelID, uid in chzzk_chatList:
                key = (name, channelID, uid)
                if key not in chatDic:
                    if len(chatDic) != 0:
                        return chatDic
                    chatDic[key] = str(chat)
                    chzzkChat.chzzk_chat_msg_List = chzzkChat.chzzk_chat_msg_List[1:]
                else:
                    chatDic[key] += f"\n{chat}"
                    chzzkChat.chzzk_chat_msg_List = chzzkChat.chzzk_chat_msg_List[1:]
            
        except Exception as e:
            errorPost(f"error addChat {e}")
            return chatDic
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
                errorPost(f"error make thumbnail url {name}.{chat}.(attempt {attempt + 1}/3): {str(e)}")
                
            except Exception as e:
                await asyncio.sleep(0.05)
                errorPost(f"unexpected error in make thumbnail url: {str(e)}")
        else:
            thumbnail_url = environ['default_thumbnail']

        return {'content'   : chat,
                "username"  : name + " >> " + init.chzzkIDList.loc[channelID, 'channelName'],
                "avatar_url": thumbnail_url}

    def chzzk_getLink(self, chzzkID):
        return f'https://api.chzzk.naver.com/service/v1/channels/{chzzkID}'
    
def change_chzzk_chat_json(init: initVar, chzzkID, chzzkID_chat_TF = True):
    init.chat_json[chzzkID] = chzzkID_chat_TF
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    supabase.table('date_update').upsert({"idx": 0, "chat_json": init.chat_json}).execute()
    
async def connect(chzzkChat: chzzkChatData, chzzkID, TF = 0):
    
    chzzkChat.chzzk_chat_count = 40000
    chzzkChat.accessToken, chzzkChat.extraToken = chzzk_api.fetch_accessToken(chzzkChat.cid, getChzzkCookie())
    
    await chzzkChat.sock.send(dumps(CHZZK_CHAT_DICT(chzzkChat, "connect")))
    sock_response = loads(await chzzkChat.sock.recv())
    chzzkChat.sid = sock_response['bdy']['sid']

    await chzzkChat.sock.send(dumps(CHZZK_CHAT_DICT(chzzkChat, "recentMessageCount", num = 50)))
    sock_response = loads(await chzzkChat.sock.recv())

    # if TF == 2:
    await async_errorPost(f"{chzzkID} 연결 완료 {chzzkChat.cid}", errorPostBotURL=environ['chat_post_url'])
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

def if_chzzk_Join(init: initVar, chzzkID, chzzkChat: chzzkChatData) -> int:
    try:
        
        if not if_after_time(init.chzzk_titleData.loc[chzzkID,'update_time']):
            chzzkChat.cid = chzzk_api.fetch_chatChannelId(init.chzzkIDList.loc[chzzkID, "channel_code"])
            if chzzkChat.cid != init.chzzk_titleData.loc[chzzkID, 'chatChannelId']:
                change_chatChannelId(init, chzzkChat, chzzkID)
                return 2
        if chzzkChat.Join_count >= chzzkChat.joinChatCount or chzzkChat.chzzk_chat_count == 0 or init.chat_json[chzzkID]:
            return 1
        
    except Exception as e: errorPost(f"error if_chzzk_Join {chzzkID}.{e}")
    return 0

def change_chatChannelId(init: initVar, chzzkChat: chzzkChatData, chzzkID):
    idx = {chzzk: i for i, chzzk in enumerate(init.chzzk_titleData["channelID"])}
    init.chzzk_titleData.loc[chzzkID, 'oldChatChannelId'] = init.chzzk_titleData.loc[chzzkID, 'chatChannelId']
    init.chzzk_titleData.loc[chzzkID, 'chatChannelId'] = chzzkChat.cid
    updates = {
        'oldChatChannelId': init.chzzk_titleData.loc[chzzkID, 'oldChatChannelId'],
        'chatChannelId': init.chzzk_titleData.loc[chzzkID, 'chatChannelId']}
    init.chzzk_titleData.loc[chzzkID, updates.keys()] = updates.values()

    supabase_data = {
        "idx": idx[chzzkID],
        **updates}
    
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    supabase.table('chzzk_titleData').upsert(supabase_data).execute()
       
async def print_msg(init: initVar, chat_cmd, chat_data, chat_type, chzzkID, nickname, post_msg_TF=True):
    chzzkName = init.chzzkIDList.loc[chzzkID, 'channelName']
    def format_message(msg_type, nickname, message, **kwargs):
        base = f"[{chat_type} - {chzzkName}] {nickname}"
        if msg_type == 'donation':
            return f"{base} ({kwargs.get('amount')}치즈): {message}"
        return f"{base}: {message}"

    try:
        if chat_cmd == CHZZK_CHAT_CMD['donation']:
            extras = loads(chat_data['extras'])
            if 'payAmount' in extras:
                message = format_message('donation', nickname, chat_data['msg'], amount=extras['payAmount'])
            else:
                return  # 도네이션 금액이 없는 경우 처리하지 않음
                
        else:
            msg = chat_data['msg'] if chat_cmd == CHZZK_CHAT_CMD['chat'] else chat_data['content']
            time = chat_data['msgTime'] if chat_cmd == CHZZK_CHAT_CMD['chat'] else chat_data['messageTime']
            time = datetime.fromtimestamp(time/1000)
            
            message = format_message('chat', nickname, msg)
            if not post_msg_TF:
                message = f"{datetime.now()} {message}, {time}"

        if post_msg_TF:
            await async_errorPost(message, errorPostBotURL=environ['donation_post_url'])
        else:
            print(message)

    except Exception as e:
        if chat_cmd == CHZZK_CHAT_CMD['donation']:
            await async_errorPost(f"{datetime.now()} it is test {e}.{loads(chat_data['extras'])}")

async def sendHi(init: initVar, chzzkID, chzzkChat: chzzkChatData, himent):
    if if_chzzk_Join(init, chzzkID, chzzkChat) == 2:
        async with websockets.connect('wss://kr-ss3.chat.naver.com/chat', timeout=3.0) as websocket:
            await connect(chzzkChat, chzzkID)
        errorPost(f"send hi {init.chzzkIDList.loc[chzzkID, 'channelName']} {chzzkChat.cid}")
        send(chzzkChat, himent)

def onAirChat(chzzkChat: chzzkChatData, chzzkID, message):
    himent = None
    if message != "뱅온!":return
    if chzzkID == "charmel"   	: himent = "챠하"
    if chzzkID == "mawang0216"	: himent = "마하"
    if chzzkID == "bighead033"	: himent = "빅하"
    if chzzkID == "suisui_again": himent = "싀하"
    if himent: send(chzzkChat, himent)

def offAirChat(chzzkChat: chzzkChatData, chzzkID):
    byement = None
    if chzzkID =="charmel"   : byement = "챠바"
    if chzzkID =="mawang0216": byement = "마바"
    if chzzkID =="bighead033": byement = "빅바"
    if byement: send(chzzkChat, byement)

def chzzk_connect_count(init: initVar, chzzkID, chzzkChat, num = 1):
    if chzzkChat.chzzk_chat_count > 0 and init.chzzk_titleData.loc[chzzkID,"live_state"] == "OPEN": chzzkChat.chzzk_chat_count -= num
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