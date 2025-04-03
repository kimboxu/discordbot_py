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
        self.init = init_var
        channel_name = init_var.chzzkIDList.loc[channel_id, 'channelName']
        self.data = ChzzkChatData(channel_id=channel_id, channel_name = channel_name)
        self.chat_event = asyncio.Event()
        self.tasks = []

    async def start(self):
        while True:
            if self.init.chat_json[self.data.channel_id]: 
                await change_chat_join_state(self.init.chat_json, self.data.channel_id, False)
            
            if self.check_live_state_close():
                await asyncio.sleep(5)
                continue
            
            try:
                await self._connect_and_run()
            except Exception as e:
                await DiscordWebhookSender._log_error(f"error in chat manager: {e}")
                await change_chat_join_state(self.init.chat_json, self.data.channel_id)
            finally:
                await self._cleanup_tasks()

    async def _connect_and_run(self):
        async with websockets.connect('wss://kr-ss3.chat.naver.com/chat', 
                                    subprotocols=['chat'], 
                                    ping_interval=None) as sock:
            self.data.sock = sock
            self.data.cid = self.init.chzzk_titleData.loc[self.data.channel_id, 'chatChannelId']
            
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
                if (self.check_live_state_close() and if_last_chat(self.data.last_chat_time)) or self.init.chat_json[self.data.channel_id]:
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
                if self.init.chzzk_titleData.loc[self.data.channel_id, 'live_state'] == "OPEN":
                    asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} last_chat_time{self.data.channel_id} 2.{self.data.last_chat_time}.{e}"))
                    try: await self.data.sock.close()
                    except: pass
                asyncio.create_task(DiscordWebhookSender._log_error(f"Test2 {self.data.channel_id}.{e}{datetime.now()}"))
                continue
                    
            except Exception as e:
                print(f"{datetime.now()} Error details: {type(e)}, {e}")
                asyncio.create_task(DiscordWebhookSender._log_error(f"Detailed error in message_receiver: {type(e)}, {e}"))

    async def _message_processor(self, message_queue: asyncio.Queue):            
        while True:
            try:
                # 큐에서 메시지 가져오기
                raw_message = await message_queue.get()
                
                try:
                    chat_cmd = raw_message['cmd']

                    # 채팅 타입 결정
                    chat_type = self.get_chat_type(chat_cmd)

                    if not await self.check_chat_message(raw_message, chat_type):
                        continue
                    
                    if  not (bdy := await self.check_TEMPORARY_RESTRICT(raw_message)):
                        continue
                    
                    chzzk_chat_list = self.get_chzzk_chat_list(bdy)

                    if chzzk_chat_list: 
                        self.filter_message(chzzk_chat_list, chat_type)
            
                except Exception as e:
                    asyncio.create_task(DiscordWebhookSender._log_error(f"Error processing message: {e}, {str(raw_message)}"))
                
                # 큐 작업 완료 신호
                message_queue.task_done()
                
            except Exception as e:
                print(f"{datetime.now()} Error in message_processor: {e}")
                asyncio.create_task(DiscordWebhookSender._log_error(f"Error in message_processor: {e}"))
                await asyncio.sleep(0.5)  # 예외 발생 시 잠시 대기

    def get_chat_type(self, chat_cmd) -> str:
        # 채팅 타입 결정
        return {
            CHZZK_CHAT_CMD['chat']: '채팅',
            CHZZK_CHAT_CMD['request_chat']: '채팅',
            CHZZK_CHAT_CMD['donation']: '후원',
            CHZZK_CHAT_CMD['ping']: '핑'
        }.get(chat_cmd, '모름')

    async def check_chat_message(self, raw_message, chat_type):
        if chat_type == "핑": 
            await self.data.sock.send(dumps(self._CHZZK_CHAT_DICT("pong")))
            return False
        
        # 에러 체크
        if chat_type != "후원" and raw_message['tid'] is None:
            bdy = raw_message.get('bdy', {})
            if message := bdy.get('message'):
                asyncio.create_task(DiscordWebhookSender._log_error(f"message_processor200len.{str(message)[:200]}"))
            return False

        return True
    
    async def check_TEMPORARY_RESTRICT(self, raw_message):
        # 임시 제한 처리
        bdy = raw_message.get('bdy', {})
        if isinstance(bdy, dict) and bdy.get('type') == 'TEMPORARY_RESTRICT':
            duration = bdy.get('duration', 30)
            asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} 임시 제한 상태입니다. {duration}초 동안 대기합니다."))
            await asyncio.sleep(duration)
            return {}
        return bdy

    def get_chzzk_chat_list(self, bdy):
        if isinstance(bdy, dict) and 'messageList' in bdy:
            chat_data = bdy['messageList']
            chzzk_chat_list = [msg for msg in chat_data]
        else:
            chat_data = bdy if isinstance(bdy, list) else [bdy]
            chzzk_chat_list = [msg for msg in chat_data]
        return chzzk_chat_list

    def filter_message(self, chzzk_chat_list, chat_type):
        for chat_data in chzzk_chat_list:
            try:
                nickname = self.get_nickname(chat_data)

                if nickname is None:
                    continue

                if not self.init.DO_TEST and (chat_type == "후원" or nickname in [*self.init.chzzk_chatFilter["channelName"]]):
                    asyncio.create_task(DiscordWebhookSender._log_error(self.print_msg(chat_data, chat_type), webhook_url=environ['donation_post_url']))

                elif not(nickname in [*self.init.chzzk_chatFilter["channelName"]]):
                    print(f"{datetime.now()} {self.print_msg(chat_data, chat_type)}")

                if nickname not in [*self.init.chzzk_chatFilter["channelName"]]: #chzzk_chatFilter에 없는 사람 채팅은 제거
                    return

                self.data.chzzk_chat_msg_List.append([chat_data, chat_type])

            except Exception as e:
                asyncio.create_task(DiscordWebhookSender._log_error(f"error process_message {e}"))

        if self.data.chzzk_chat_msg_List:
            self.data.chat_event.set()

    async def _post_chat(self):
        while not self.data.sock.closed:
            try:
                if not self.data.chzzk_chat_msg_List: await self.data.chat_event.wait()
                chat_data, chat_type = self.data.chzzk_chat_msg_List.pop(0)

                nickname = self.get_nickname(chat_data)
                chat = self.get_chat(chat_data)
                uid = self.get_uid(chat_data)

                profile_image = await self._get_profile_image(uid)
                json_data = get_json_data(nickname, chat, self.data.channel_name, profile_image)
                
                list_of_urls = get_list_of_urls(self.init.DO_TEST, self.init.userStateData, nickname, self.data.channel_id, json_data, "chat_user_json")
                asyncio.create_task(DiscordWebhookSender().send_messages(list_of_urls))

                print(f"{datetime.now()} post chat {self.print_msg(chat_data, chat_type)}")
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

            self.data.cid = chzzk_api.fetch_chatChannelId(self.init.chzzkIDList.loc[self.data.channel_id, "channel_code"])
            await self.change_chatChannelId()
            return True
            
        except Exception as e: 
            asyncio.create_task(DiscordWebhookSender._log_error(f"error get_check_channel_id {self.data.channel_id}.{e}"))
        return False

    async def change_chatChannelId(self):
        if self.data.cid != self.init.chzzk_titleData.loc[self.data.channel_id, 'chatChannelId']:
            self.init.chzzk_titleData.loc[self.data.channel_id, 'oldChatChannelId'] = self.init.chzzk_titleData.loc[self.data.channel_id, 'chatChannelId']
            self.init.chzzk_titleData.loc[self.data.channel_id, 'chatChannelId'] = self.data.cid
            await save_airing_data(self.init.chzzk_titleData, 'chzzk', self.data.channel_id)

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

    def get_chat(self, chat_data) -> str:
        if 'msg' in chat_data:
            msg = chat_data['msg']
        elif 'content' in chat_data:
            msg = chat_data['content']
        else:
            return None

        if msg and msg[0] in [">"]:
            msg = "/" + msg
        return msg

    def get_uid(self, chat_data) -> str:
        return chat_data.get('uid') or chat_data.get('userId')

    def get_payAmount(self, chat_data, chat_type) -> str:
        if chat_type == "후원": payAmount = loads(chat_data['extras'])['payAmount']
        else: payAmount = None
        return payAmount

    def print_msg(self, chat_data, chat_type) -> str:

        def format_message(msg_type, nickname, message, time, **kwargs):
            base = f"[{chat_type} - {self.data.channel_name}] {nickname}"
            time = datetime.fromtimestamp(time/1000)
            if msg_type == "후원":
                return f"{base} ({kwargs.get('amount')}치즈): {message}, {time}"
            return f"{base}: {message}, {time}"
        
        if chat_type == "후원":
            extras = loads(chat_data['extras'])
            if 'payAmount' in extras:
                message = format_message(chat_type, self.get_nickname(chat_data), chat_data['msg'], chat_data['msgTime'], amount=extras['payAmount'])
            elif 'month' in extras:
                #구독
                nickname = extras["nickname"]
                chat = chat_data['msg']
                msgTime = chat_data['msgTime']
                month = extras["month"]
                tierName = extras["tierName"] #구독 티어 이름
                tierNo = extras["tierNo"]   #구독 티어 
                return  "print_msg 구독 메시지(추후 추가 예정)" # 도네이션 금액이 없는 경우 처리하지 않음
            else:
                # 구독권 선물 이팩트
                return f"print_msg 어떤 메시지인지 현재는 확인X.{self.data.channel_name}.{self.get_nickname(chat_data)}.{extras}"

        else:
            msg = chat_data['msg'] if chat_type == "채팅" else chat_data['content']
            time = chat_data['msgTime'] if chat_type == "채팅" else chat_data['messageTime']
            message = format_message('chat', self.get_nickname(chat_data), msg, time)

        return message

    async def sendHi(self, himent):
        if await self.get_check_channel_id():
            asyncio.create_task(DiscordWebhookSender._log_error(f"send hi {self.init.chzzkIDList.loc[self.data.channel_id, 'channelName']} {self.data.cid}"))
            self._send(himent)

    def onAirChat(self, message):
        himent = None
        if message != "뱅온!":return
        if self.data.channel_id == "charmel"   	: himent = "챠하"
        if self.data.channel_id == "mawang0216"	: himent = "마하"
        if self.data.channel_id == "bighead033"	: himent = "빅하"
        if himent: self._send(himent)

    def offAirChat(self):
        byement = None
        if self.data.channel_id =="charmel"   : byement = "챠바"
        if self.data.channel_id =="mawang0216": byement = "마바"
        if self.data.channel_id =="bighead033": byement = "빅바"
        if byement: self._send(byement)

    def check_live_state_close(self):
        return self.init.chzzk_titleData.loc[self.data.channel_id, 'live_state'] == "CLOSE"


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