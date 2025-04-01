import asyncio
import chzzk_api
import websockets
from os import environ
from datetime import datetime
from urllib.parse import unquote
from json import loads, dumps, JSONDecodeError
from dataclasses import dataclass, field
from cmd_type import CHZZK_CHAT_CMD
from base import  getChzzkCookie, if_after_time, if_last_chat, initVar, get_message, change_chat_join_state, save_airing_data
from discord_webhook_sender import DiscordWebhookSender, get_list_of_urls, get_json_data

@dataclass
class ChzzkChatData:
    sock: websockets.connect = None
    chzzk_chat_msg_List: list = field(default_factory=list)
    last_chat_time: datetime = field(default_factory=datetime.now)
    chzzk_chat_count: int = 80000
    sid: str = ""
    cid: str = ""
    accessToken: str = ""
    extraToken: str = ""
    channel_id: str = ""
    channel_name: str = ""
    
    def __post_init__(self):
        # 이벤트 객체 초기화
        self.chat_event = asyncio.Event()

class chzzk_chat_message:
    def __init__(self, init_var: initVar, channel_id):
        self.DO_TEST = init_var.DO_TEST
        self.supabase = init_var.supabase
        self.userStateData = init_var.userStateData
        self.chzzkIDList = init_var.chzzkIDList
        self.chzzk_chatFilter = init_var.chzzk_chatFilter
        self.titleData = init_var.chzzk_titleData
        self.chat_json = init_var.chat_json

        channel_name = self.chzzkIDList.loc[channel_id, 'channelName']
        self.data = ChzzkChatData(channel_id=channel_id, channel_name = channel_name)
        self.chat_event = asyncio.Event()
        self.tasks = []

    async def start(self):
        while True:
            if self.chat_json[self.data.channel_id]: 
                change_chat_join_state(self.chat_json, self.data.channel_id, False)
            
            if self.titleData.loc[self.data.channel_id, 'live_state'] == "CLOSE":
                await asyncio.sleep(5)
                continue
            
            try:
                await self._connect_and_run()
            except Exception as e:
                await DiscordWebhookSender._log_error(f"error in chat manager: {e}")
                change_chat_join_state(self.chat_json, self.data.channel_id)
            finally:
                await self._cleanup_tasks()

    async def _connect_and_run(self):
        async with websockets.connect('wss://kr-ss3.chat.naver.com/chat', 
                                    subprotocols=['chat'], 
                                    ping_interval=None) as sock:
            self.data.sock = sock
            self.data.cid = self.titleData.loc[self.data.channel_id, 'chatChannelId']
            
            await self.get_check_channel_id()
            await self.connect()
            message_queue = asyncio.Queue()

            self.tasks = [
                asyncio.create_task(self._ping()),
                asyncio.create_task(self._message_receiver(message_queue)),
                asyncio.create_task(self._message_processor(message_queue)),
                asyncio.create_task(self._post_chat())
            ]
            
            await asyncio.gather(self.tasks[0], self.tasks[1])

    async def _cleanup_tasks(self):
        for task in self.tasks:
            if task and not task.done() and not task.cancelled():
                try:
                    task.cancel()
                    # Optionally wait for task to actually cancel
                    await asyncio.wait([task], timeout=2)
                except Exception as cancel_error:
                    error_logger = DiscordWebhookSender()
                    await error_logger._log_error(f"Error cancelling task for {self.data.channel_id}: {cancel_error}")

    async def _message_receiver(self, message_queue: asyncio.Queue):
        while True:

            # 논블로킹 방식으로 메시지 수신 시도
            try:
                if if_last_chat(self.data.last_chat_time) or self.chat_json[self.data.channel_id]:
                    try: await self.data.sock.close()
                    except: pass

                if self.data.sock.closed:
                    asyncio.create_task(DiscordWebhookSender._log_error(f"{self.data.channel_id} 연결 종료 {self.data.cid}", webhook_url=environ['chat_post_url']))
                    break

                raw_message = await asyncio.wait_for(self.data.sock.recv(), timeout=1)
                self.data.last_chat_time = datetime.now()
                await message_queue.put(loads(raw_message))
                
            except asyncio.TimeoutError:
                continue
                
            except (JSONDecodeError, ConnectionError, RuntimeError, websockets.exceptions.ConnectionClosed) as e:
                if self.titleData.loc[self.data.channel_id, 'live_state'] == "OPEN":
                    asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} last_chat_time{self.data.channel_id} 2.{self.data.last_chat_time}.{e}"))
                    try: await self.data.sock.close()
                    except: pass
                asyncio.create_task(DiscordWebhookSender._log_error(f"Test2 {self.data.channel_id}.{e}{datetime.now()}"))
                continue
                    
            except Exception as e:
                print(f"{datetime.now()} Error details: {type(e)}, {e}")
                asyncio.create_task(DiscordWebhookSender._log_error(f"Detailed error in message_receiver: {type(e)}, {e}"))

    async def _message_processor(self, message_queue: asyncio.Queue):
        def filter_message():
            for chat_data in chzzk_chat_list:
                nickname = get_nickname(chat_data)
                try:
                    if nickname is None:
                        continue
                    if not self.DO_TEST and (chat_type == "후원" or nickname in [*self.chzzk_chatFilter["channelName"]]):
                        asyncio.create_task(print_msg(chat_data, nickname))

                    if not(nickname in [*self.chzzk_chatFilter["channelName"]]):
                        asyncio.create_task(print_msg(chat_data, nickname, post_msg_TF=False))

                    if nickname not in [*self.chzzk_chatFilter["channelName"]]: #chzzk_chatFilter에 없는 사람 채팅은 제거
                        return

                    if 'msg' in chat_data:
                        msg = chat_data['msg']
                    elif 'content' in chat_data:
                        msg = chat_data['content']
                    else:
                        continue  # 메시지가 없으면 다음 chat_data로 넘어갑니다.

                    if msg and msg[0] in [">"]:
                        msg = "/" + msg
                    self.data.chzzk_chat_msg_List.append([nickname, msg, chat_type, chat_data.get('uid') or chat_data.get('userId')])

                except Exception as e:
                    asyncio.create_task(DiscordWebhookSender._log_error(f"error process_message {e}"))

            if self.data.chzzk_chat_msg_List:
                self.data.chat_event.set()
                    
        def get_nickname(chat_data):
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
        
        async def print_msg(chat_data, nickname, post_msg_TF=True):
            # chzzkName = self.chzzkIDList.loc[self.data.channel_id, 'channelName']
            def format_message(msg_type, nickname, message, time, **kwargs):
                base = f"[{chat_type} - {self.data.channel_name}] {nickname}"
                time = datetime.fromtimestamp(time/1000)
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
                    message = format_message('chat', nickname, msg, time)
                    
                if post_msg_TF:
                    asyncio.create_task(DiscordWebhookSender._log_error(message, webhook_url=environ['donation_post_url']))
                else:
                    print(f"{datetime.now()} {message}")

            except Exception as e:
                if chat_type == "후원":
                    asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} it is test {e}.{loads(chat_data['extras'])}"))

        while True:
            try:
                # 큐에서 메시지 가져오기
                raw_message = await message_queue.get()
                
                try:
                    chat_cmd = raw_message['cmd']

                    if chat_cmd == CHZZK_CHAT_CMD['ping']: 
                        await self.data.sock.send(dumps(self._CHZZK_CHAT_DICT("pong")))
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
                            asyncio.create_task(DiscordWebhookSender._log_error(f"message_processor200len.{str(message)[:200]}"))
                        continue

                    # 임시 제한 처리
                    bdy = raw_message.get('bdy', {})
                    if isinstance(bdy, dict) and bdy.get('type') == 'TEMPORARY_RESTRICT':
                        duration = bdy.get('duration', 30)
                        asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} 임시 제한 상태입니다. {duration}초 동안 대기합니다."))
                        await asyncio.sleep(duration)
                        continue
                    
                    # 메시지 리스트 처리
                    if isinstance(bdy, dict) and 'messageList' in bdy:
                        chat_data = bdy['messageList']
                        chzzk_chat_list = [msg for msg in chat_data]
                    else:
                        chat_data = bdy if isinstance(bdy, list) else [bdy]
                        chzzk_chat_list = [msg for msg in chat_data]

                    if chzzk_chat_list: filter_message()
            
                except Exception as e:
                    asyncio.create_task(DiscordWebhookSender._log_error(f"Error processing message: {e}, {str(raw_message)}"))
                
                # 큐 작업 완료 신호
                message_queue.task_done()
                
            except Exception as e:
                print(f"{datetime.now()} Error in message_processor: {e}")
                asyncio.create_task(DiscordWebhookSender._log_error(f"Error in message_processor: {e}"))
                await asyncio.sleep(0.5)  # 예외 발생 시 잠시 대기
         
    async def _post_chat(self):
        while not self.data.sock.closed and self.data.chzzk_chat_msg_List:
            try:
                await self.data.chat_event.wait()
                name, chat, chat_type, uid = self.data.chzzk_chat_msg_List.pop(0)
                profile_image = await self._get_profile_image(uid)
                json_data = get_json_data(name, chat, self.data.channel_name, profile_image)
                
                list_of_urls = get_list_of_urls(self.DO_TEST, self.userStateData, name, self.data.channel_id, self.data.channel_name, json_data, "chat_user_json")
                asyncio.create_task(DiscordWebhookSender().send_messages(list_of_urls))

                print(f"{datetime.now()} post chat")
                self.data.chat_event.clear()
                
            except Exception as e:
                asyncio.create_task(DiscordWebhookSender._log_error(f"error postChat: {str(e)}"))
                self.data.chat_event.clear()

    async def _ping(self):
        ping_interval = 10
        
        try:
            while not self.data.sock.closed:
                # Send ping message
                await self.data.sock.send(dumps(self._CHZZK_CHAT_DICT("pong")))
                
                try:
                    await asyncio.wait_for(asyncio.shield(self.data.sock.wait_closed()), timeout=ping_interval)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    await DiscordWebhookSender._log_error(f"Error during ping wait: {e}")
                    break
                    
        except Exception as e:
            await DiscordWebhookSender._log_error(f"Error in ping function: {e}")
        
        print(f"{self.data.channel_id} chat pong 종료")

    async def connect(self, first_connectTF = 0):
        
        self.data.chzzk_chat_count = 80000
        self.data.accessToken, self.data.extraToken = chzzk_api.fetch_accessToken(self.data.cid, getChzzkCookie())
        
        await self.data.sock.send(dumps(self._CHZZK_CHAT_DICT("connect")))
        sock_response = loads(await self.data.sock.recv())
        self.data.sid = sock_response['bdy']['sid']

        await self.data.sock.send(dumps(self._CHZZK_CHAT_DICT("recentMessageCount", num = 50)))
        sock_response = loads(await self.data.sock.recv())

        messageTime = sock_response["bdy"]["messageList"][-1]["messageTime"]
        self.data.last_chat_time = datetime.fromtimestamp(messageTime/1000)

        asyncio.create_task(DiscordWebhookSender._log_error(f"{self.data.channel_id} 연결 완료 {self.data.cid}", webhook_url=environ['chat_post_url']))

    def _send(self, message):
        default_dict = {
            "ver": 2,
            "svcid": "game",
            "cid": self.data.cid,
        }

        extras = {
            "chatType": "STREAMING",
            "emojis": "",
            "osType": "PC",
            "extraToken": self.data.extraToken,
            "streamingChannelId": self.data.cid
        }

        send_dict = {
            "tid": 3,
            "cmd": CHZZK_CHAT_CMD['send_chat'],
            "retry": False,
            "sid": self.data.sid,
            "bdy": {
                "msg": message,
                "msgTypeCode": 1,
                "extras": dumps(extras),
                "msgTime": int(datetime.now().timestamp())
            }
        }

        self.data.sock.send(dumps(dict(send_dict, **default_dict)))

    def _CHZZK_CHAT_DICT(self, option = "connect", num = 50):
        default_dict = {
            "ver": "2",
            "svcid": "game",
            "cid": self.data.cid,
        }
        if option == "connect":
            send_dict = {
                "cmd": CHZZK_CHAT_CMD['connect'],
                "tid": 1,
                "bdy": {
                    "uid": chzzk_api.fetch_userIdHash(getChzzkCookie()),
                    "devType": 2001,
                    "accTkn": self.data.accessToken,
                    "auth": "SEND"
                }
            } 
        elif option == "recentMessageCount":
            send_dict = {
                "cmd": CHZZK_CHAT_CMD['request_recent_chat'],
                "tid": 2,
                "sid": self.data.sid,
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
    
    async def _get_profile_image(self, uid):
        def chzzk_getLink(uid):
            return f'https://api.chzzk.naver.com/service/v1/channels/{uid}'
        
        profile_image = None

        data = await get_message("chzzk", chzzk_getLink(uid))
        profile_image = data["content"]["channelImageUrl"]

        if profile_image and profile_image.startswith(("https://nng-phinf.pstatic.net", "https://ssl.pstatic.net")):
            return profile_image

        profile_image = environ['default_thumbnail']

        return profile_image
    
    async def get_check_channel_id(self) -> int:
        try:

            self.data.cid = chzzk_api.fetch_chatChannelId(self.chzzkIDList.loc[self.data.channel_id, "channel_code"])
            await self.change_chatChannelId()
            return True
            
        except Exception as e: 
            asyncio.create_task(DiscordWebhookSender._log_error(f"error get_check_channel_id {self.data.channel_id}.{e}"))
        return False

    async def change_chatChannelId(self):
        if self.data.cid != self.titleData.loc[self.data.channel_id, 'chatChannelId']:
            self.titleData.loc[self.data.channel_id, 'oldChatChannelId'] = self.titleData.loc[self.data.channel_id, 'chatChannelId']
            self.titleData.loc[self.data.channel_id, 'chatChannelId'] = self.data.cid
            await save_airing_data(self.titleData, 'chzzk', self.data.channel_id)

    async def sendHi(self, himent):
        if await self.get_check_channel_id():
            asyncio.create_task(DiscordWebhookSender._log_error(f"send hi {self.chzzkIDList.loc[self.data.channel_id, 'channelName']} {self.data.cid}"))
            self._send(himent)

    def onAirChat(self, message):
        himent = None
        if message != "뱅온!":return
        if self.data.channel_id == "charmel"   	: himent = "챠하"
        if self.data.channel_id == "mawang0216"	: himent = "마하"
        if self.data.channel_id == "bighead033"	: himent = "빅하"
        if self.data.channel_id == "suisui_again"  : himent = "싀하"
        if himent: self._send(himent)

    def offAirChat(self):
        byement = None
        if self.data.channel_id =="charmel"   : byement = "챠바"
        if self.data.channel_id =="mawang0216": byement = "마바"
        if self.data.channel_id =="bighead033": byement = "빅바"
        if byement: self._send(byement)

    def chzzk_connect_count(self, num = 1):
        if self.data.chzzk_chat_count > 0 and self.titleData.loc[self.data.channel_id,"live_state"] == "OPEN": self.data.chzzk_chat_count -= num
        if self.data.chzzk_chat_count < 0: self.data.chzzk_chat_count = 0


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