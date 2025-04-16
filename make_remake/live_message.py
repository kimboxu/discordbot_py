import base
import asyncio
from json import loads
from typing import Dict
from datetime import datetime
from os import remove, environ
from requests import post
from urllib.request import urlretrieve
from dataclasses import dataclass, field
from discord_webhook_sender import DiscordWebhookSender, get_list_of_urls

from supabase import create_client
from typing import List, Tuple, Dict, Any
import firebase_admin
from firebase_admin import credentials, messaging

@dataclass
class LiveData:
    livePostList: list = field(default_factory=list)
    live: str = ""
    title: str = ""
    view_count: int = 0
    thumbnail_url: str = ""
    profile_image: str = ""
    start_at: Dict[str, str] = field(default_factory=lambda: {
        "openDate": "2025-01-01 00:00:00",
        "closeDate": "2025-01-01 00:00:00"
    })
    state_update_time: Dict[str, str] = field(default_factory=lambda: {
        "openDate": "2025-01-01T00:00:00",
        "closeDate": "2025-01-01T00:00:00",
        "titleChangeDate": "2025-01-01T00:00:00"
})
    
# 기본 라이브 메시지 클래스 - 공통 기능 포함
class base_live_message:
    def __init__(self, init_var: base.initVar, channel_id, platform_name):
        self.DO_TEST = init_var.DO_TEST
        self.userStateData = init_var.userStateData
        self.platform_name = platform_name
        self.channel_id = channel_id

        # 플랫폼별 데이터 초기화
        if platform_name == "chzzk":
            self.id_list = init_var.chzzkIDList
            self.title_data = init_var.chzzk_titleData
        elif platform_name == "afreeca":
            self.id_list = init_var.afreecaIDList
            self.title_data = init_var.afreeca_titleData
        else:
            raise ValueError(f"Unsupported platform: {platform_name}")
        
        self.channel_name = self.id_list.loc[channel_id, 'channelName']
        state_update_time = self.title_data.loc[self.channel_id, 'state_update_time']
        self.data = LiveData(state_update_time = state_update_time)

    async def start(self):
        await self.addMSGList()
        await self.postLive_message()

    async def addMSGList(self):
        try:
            state_data = await self._get_state_data()
                
            if not self._is_valid_state_data(state_data):
                return

            self._update_title_if_needed()

            # 스트림 데이터 얻기
            stream_data = self._get_stream_data(state_data)
            self._update_stream_info(stream_data, state_data)
            await self.save_profile_image()

            # 온라인/오프라인 상태 처리
            if self._should_process_online_status():
                await self._handle_online_status(state_data)
            elif self._should_process_offline_status():
                await self._handle_offline_status(state_data)

        except Exception as e:
            error_msg = f"error get state_data {self.platform_name} live {e}.{self.channel_id}"
            asyncio.create_task(DiscordWebhookSender._log_error(error_msg))
            await base.update_flag('user_date', True)

    def _update_title_if_needed(self):
        if (base.if_after_time(self.data.state_update_time["titleChangeDate"]) and 
            self._get_old_title() != self._get_title()):
            self.title_data.loc[self.channel_id,'title2'] = self.title_data.loc[self.channel_id,'title1']
            asyncio.create_task(base.save_airing_data(self.title_data, self.platform_name, self.channel_id))

    def _get_channel_name(self):
        return self.id_list.loc[self.channel_id, 'channelName']
    
    async def postLive_message(self):
        try:
            if not self.data.livePostList:
                return
            message, json_data = self.data.livePostList.pop(0)

            db_name = self._get_db_name(message)
            self._log_message(message)

            # Get list of device tokens
            token_list = get_user_device_tokens(
                self.DO_TEST,
                self.userStateData,
                self.channel_name,
                self.channel_id,
                json_data,
                db_name
            )
            list_of_urls = get_list_of_urls(
                self.DO_TEST, 
                self.userStateData, 
                self.channel_name, 
                self.channel_id, 
                json_data, 
                db_name
            )
            asyncio.create_task(DiscordWebhookSender().send_messages(list_of_urls))
            # Send notifications
            try:
                asyncio.create_task(NotificationSender().send_notifications(token_list))
            except: print("Test NotificationSender")
            await base.save_airing_data(self.title_data, self.platform_name, self.channel_id)

        except Exception as e:
            print(f"postLiveMSG {e}")
            self.data.livePostList.clear()
    
    def _get_db_name(self, message):
        """메시지에 맞는 DB 이름 반환"""
        if message == "뱅온!":
            return "뱅온 알림"
        elif message == "방제 변경":
            return "방제 변경 알림"
        elif message == "뱅종":
            return "방종 알림"
        return "알림"
    
    def _log_message(self, message):
        """메시지 로깅"""
        now = datetime.now()
        if message == "뱅온!":
            print(f"{now} onLine {self.channel_name} {message}")
        elif message == "방제 변경":
            old_title = self._get_old_title()
            print(f"{now} onLine {self.channel_name} {message}")
            print(f"{now} 이전 방제: {old_title}")
            print(f"{now} 현재 방제: {self.data.title}")
        elif message == "뱅종":
            print(f"{now} offLine {self.channel_name}")
    
    def _get_old_title(self):
        return self.title_data.loc[self.channel_id,'title2']
    
    def _get_title(self):
        return self.title_data.loc[self.channel_id,'title1']

    def onLineTitle(self, message):
        if message == "뱅온!":
            self.title_data.loc[self.channel_id, 'live_state'] = "OPEN"
        self.title_data.loc[self.channel_id,'title2'] = self._get_title()
        self.title_data.loc[self.channel_id,'title1'] = self.data.title

    def onLineTime(self, message):
        if message == "뱅온!":
            self.title_data.loc[self.channel_id,'update_time'] = self.getStarted_at("openDate")

    def offLineTitle(self):
        self.title_data.loc[self.channel_id, 'live_state'] = "CLOSE"

    def ifChangeTitle(self):
        return self.data.title not in [
            str(self._get_title()), 
            str(self._get_old_title())
        ]

    def getMessage(self) -> str:
        # 각 플랫폼별 구현에서 정의 (상태 전환 체크 방식이 다름)
        raise NotImplementedError
    
    # 서브클래스에서 구현해야 하는 메소드들
    async def _get_state_data(self):
        raise NotImplementedError
    
    def _is_valid_state_data(self, state_data):
        raise NotImplementedError
    
    def _get_stream_data(self, state_data):
        raise NotImplementedError
    
    def _update_stream_info(self, stream_data, state_data):
        raise NotImplementedError
    
    def _should_process_online_status(self):
        raise NotImplementedError
    
    def _should_process_offline_status(self):
        raise NotImplementedError
    
    async def _handle_online_status(self, state_data):
        message = self.getMessage()
        json_data = await self.getOnAirJson(message, state_data)

        self.onLineTime(message)
        self.onLineTitle(message)

        self.data.livePostList.append((message, json_data))

        await base.save_profile_data(self.id_list, self.platform_name, self.channel_id)

        if message == "뱅온!": 
            self.title_data.loc[self.channel_id, 'state_update_time']["openDate"] = datetime.now().isoformat()
        self.title_data.loc[self.channel_id, 'state_update_time']["titleChangeDate"] = datetime.now().isoformat()

    async def save_profile_image(self):
        if self.id_list.loc[self.channel_id, 'profile_image'] != self.data.profile_image:
            self.id_list.loc[self.channel_id, 'profile_image'] = self.data.profile_image
            await base.save_profile_data(self.id_list, self.platform_name, self.channel_id)
    
    async def getOnAirJson(self, message, state_data):
        raise NotImplementedError
    
    async def _handle_offline_status(self, state_data):
        raise NotImplementedError
    
    def getStarted_at(self, status: str):
        time_str = self.data.start_at[status]
        time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        return time.isoformat()
    
    def get_channel_url(self):
        raise NotImplementedError
    
    def getViewer_count(self, state_data):
        raise NotImplementedError
    
    async def get_live_thumbnail_image(self, state_data, message=None):
        raise NotImplementedError

# 치지직 구현 클래스
class chzzk_live_message(base_live_message):
    def __init__(self, init_var: base.initVar, chzzk_id):
        super().__init__(init_var, chzzk_id, "chzzk")

    async def _get_state_data(self):
        return await base.get_message(
            "chzzk", 
            base.chzzk_getLink(self.id_list.loc[self.channel_id, "channel_code"])
        )
    
    def _is_valid_state_data(self, state_data):
        try:
            return state_data and state_data["code"] == 200
        except Exception as e:
            if len(state_data) > 200: state_data = state_data[:200]
            asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} _is_valid_state_data.{self.channel_id}.{e}.{state_data}"))
            return False

    def _get_stream_data(self, state_data):
        return base.chzzk_getChannelOffStateData(
            state_data["content"], 
            self.id_list.loc[self.channel_id, "channel_code"], 
            self.id_list.loc[self.channel_id, 'profile_image']
        )
    
    def _update_stream_info(self, stream_data, state_data):
        self.data.start_at["openDate"] = state_data['content']["openDate"]
        self.data.start_at["closeDate"] = state_data['content']["closeDate"]
        self.data.live, self.data.title, self.data.profile_image = stream_data

    def _should_process_online_status(self):
        return ((self.checkStateTransition("OPEN") or 
           (self.ifChangeTitle())) and
           base.if_after_time(self.data.state_update_time["closeDate"], sec=15))

    def _should_process_offline_status(self):
        return (self.checkStateTransition("CLOSE") and 
          base.if_after_time(self.data.state_update_time["openDate"], sec=15))
    
    # async def _handle_online_status(self, state_data):
    #     message = self.getMessage()
    #     json_data = await self.getOnAirJson(message, state_data)

    #     self.onLineTime(message)
    #     self.onLineTitle(message)

    #     self.data.livePostList.append((message, json_data))

    #     await base.save_profile_data(self.id_list, self.platform_name, self.channel_id)

    #     if message == "뱅온!": 
    #         self.title_data.loc[self.channel_id, 'state_update_time']["openDate"] = datetime.now().isoformat()
    #     self.title_data.loc[self.channel_id, 'state_update_time']["titleChangeDate"] = datetime.now().isoformat()

    async def _handle_offline_status(self, state_data):
        message = "뱅종"
        json_data = await self.getOffJson(state_data, message)

        self.offLineTitle()
        self.offLineTime()

        self.data.livePostList.append((message, json_data))

        self.title_data.loc[self.channel_id, 'state_update_time']["closeDate"] = datetime.now().isoformat()
        self.title_data.loc[self.channel_id, 'state_update_time']["titleChangeDate"] = datetime.now().isoformat()
    
    def offLineTime(self):
        self.title_data.loc[self.channel_id,'update_time'] = self.getStarted_at("closeDate")

    def get_channel_url(self): 
        return f'https://chzzk.naver.com/live/{self.id_list.loc[self.channel_id, "channel_code"]}'

    def getViewer_count(self, state_data):
        self.data.view_count = state_data['content']['concurrentUserCount']

    def getMessage(self) -> str: 
        return "뱅온!" if (self.checkStateTransition("OPEN")) else "방제 변경"
    
    def checkStateTransition(self, target_state: str):
        if self.data.live != target_state or self.title_data.loc[self.channel_id, 'live_state'] != ("CLOSE" if target_state == "OPEN" else "OPEN"):
            return False
        return self.getStarted_at(("openDate" if target_state == "OPEN" else "closeDate")) > self.title_data.loc[self.channel_id, 'update_time']
    
    async def get_live_thumbnail_image(self, state_data, message):
        for count in range(20):
            time_difference = (datetime.now() - datetime.fromisoformat(self.title_data.loc[self.channel_id, 'update_time'])).total_seconds()

            if message == "뱅온!" or self.title_data.loc[self.channel_id, 'live_state'] == "CLOSE" or time_difference < 15: 
                thumbnail_image = ""
                break

            thumbnail_image = self.get_thumbnail_image(state_data)
            if thumbnail_image is None:
                print(f"{datetime.now()} wait make thumbnail1 {count}")
                await asyncio.sleep(0.05)
                continue
            break

        else: thumbnail_image = ""
        
        return thumbnail_image
    
    def get_thumbnail_image(self, state_data): 
        try:
            if state_data['content']['liveImageUrl'] is not None:
                self.saveImage(state_data)
                file = {'file': open("explain.png", 'rb')}
                data = {"username": self.channel_name,
                        "avatar_url": self.id_list.loc[self.channel_id, 'profile_image']}
                thumbnail = post(environ['recvThumbnailURL'], files=file, data=data, timeout=3)
                try: remove('explain.png')
                except: pass
                frontIndex = thumbnail.text.index('"proxy_url"')
                thumbnail = thumbnail.text[frontIndex:]
                frontIndex = thumbnail.index('https://media.discordap')
                return thumbnail[frontIndex:thumbnail.index(".png") + 4]
            return None
        except Exception as e:
            asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} wait make thumbnail2 {e}"))
            return None

    def saveImage(self, state_data): 
        urlretrieve(self.getImageURL(state_data), "explain.png")

    def getImageURL(self, state_data) -> str:
        link = state_data['content']['liveImageUrl']
        link = link.replace("{type", "")
        link = link.replace("}.jpg", "0.jpg")
        self.data.thumbnail_url = link
        return link
    
    async def getOnAirJson(self, message, state_data):

        if self.data.live == "CLOSE":
            return self.get_state_data_change_title_json(message)
        
        self.getViewer_count(state_data)
        thumbnail_url = await self.get_live_thumbnail_image(state_data, message)
        
        if message == "뱅온!":
            return self.get_online_state_json(message, thumbnail_url)
        
        return self.get_online_titleChange_state_json(message, thumbnail_url)
    
    def get_online_state_json(self, message, thumbnail_url):
        return {"username": self.channel_name, "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "방제", "value": self.data.title, "inline": True},
                        # {"name": ':busts_in_silhouette: 시청자수',
                        # "value": self.data.view_count, "inline": True}
                        ],
                    "title": f"{self.channel_name} {message}\n",
                "url": self.get_channel_url(),
                # "image": {"url": thumbnail_url},
                "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
                "timestamp": base.changeUTCtime(self.getStarted_at("openDate"))}]}

    def get_online_titleChange_state_json(self, message, thumbnail_url):
        return {"username": self.channel_name, "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "방제", "value": self.data.title, "inline": True},
                        {"name": ':busts_in_silhouette: 시청자수',
                        "value": self.data.view_count, "inline": True}
                        ],
                    "title": f"{self.channel_name} {message}\n",
                "url": self.get_channel_url(),
                "image": {"url": thumbnail_url},
                "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
                "timestamp": base.changeUTCtime(self.getStarted_at("openDate"))}]}

		# return {"username": self.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": self.chzzkIDList.loc[chzzkID, 'profile_image'],
		# 		"embeds": [
		# 			{"color": int(self.chzzkIDList.loc[chzzkID, 'channel_color']),
		# 			"fields": [
		# 				{"name": "이전 방제", "value": str(self.titleData.loc[chzzkID,'title1']), "inline": True},
		# 				{"name": "현재 방제", "value": title, "inline": True},
		# 				{"name": ':busts_in_silhouette: 시청자수',
		# 				"value": viewer_count, "inline": True}],
		# 			"title":  f"{self.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
		# 		"url": url,
		# 		"image": {"url": thumbnail},
		# 		"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
		# 		"timestamp": base.changeUTCtime(started_at)}]}

    def get_state_data_change_title_json(self, message):
        return {"username": self.channel_name, "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "이전 방제", "value": str(self._get_title()), "inline": True},
                        {"name": "현재 방제", "value": self.data.title, "inline": True}],
                    "title": f"{self.channel_name} {message}\n",
                "url": self.get_channel_url()}]}

    async def getOffJson(self, state_data, message):
        thumbnail_url = await self.get_live_thumbnail_image(state_data, message)
        
        return {"username": self.channel_name, "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "title": self.channel_name +" 방송 종료\n",
                "image": {"url": thumbnail_url},
                "footer": { "text": f"방종 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
                "timestamp": base.changeUTCtime(self.getStarted_at("closeDate"))}]}

# 아프리카 구현 클래스
class afreeca_live_message(base_live_message):
    def __init__(self, init_var: base.initVar, channel_id):
        super().__init__(init_var, channel_id, "afreeca")

    async def _get_state_data(self):
        return await base.get_message(
            "afreeca", 
            base.afreeca_getLink(self.id_list.loc[self.channel_id, "afreecaID"])
        )
    
    def _is_valid_state_data(self, state_data):
        try:
            state_data["station"]["user_id"]
            return True
        except:
            return False
    
    def _get_stream_data(self, state_data):
        return base.afreeca_getChannelOffStateData(
            state_data,
            self.id_list.loc[self.channel_id, "afreecaID"],
            self.id_list.loc[self.channel_id, 'profile_image']
        )
    
    def _update_stream_info(self, stream_data, state_data):
        self.update_broad_no(state_data)
        self.data.start_at["openDate"] = state_data["station"]["broad_start"]
        self.data.live, self.data.title, self.data.profile_image = stream_data
        self.id_list.loc[self.channel_id, 'profile_image'] = self.data.profile_image
    
    def update_broad_no(self, state_data):
        if state_data["broad"] and state_data["broad"]["broad_no"] != self.title_data.loc[self.channel_id, 'chatChannelId']:
            self.title_data.loc[self.channel_id, 'oldChatChannelId'] = self.title_data.loc[self.channel_id, 'chatChannelId']
            self.title_data.loc[self.channel_id, 'chatChannelId'] = state_data["broad"]["broad_no"]
    
    def _should_process_online_status(self):
        return ((self.turnOnline() or 
                (self.data.title and self.ifChangeTitle())) and 
                base.if_after_time(self.data.state_update_time["closeDate"], sec=15))
    
    def _should_process_offline_status(self):
        return (self.turnOffline() and
                  base.if_after_time(self.data.state_update_time["openDate"], sec=15))
    
    # async def _handle_online_status(self, state_data):
    #     message = self.getMessage()
    #     json_data = await self.getOnAirJson(message, state_data)
        
    #     self.onLineTime(message)
    #     self.onLineTitle(message)
        
    #     self.data.livePostList.append((message, json_data))
        
    #     await base.save_profile_data(self.id_list, self.platform_name, self.channel_id)

    #     if message == "뱅온!": 
    #         self.title_data.loc[self.channel_id, 'state_update_time']["openDate"] = datetime.now().isoformat()
    #     self.title_data.loc[self.channel_id, 'state_update_time']["titleChangeDate"] = datetime.now().isoformat()
    
    async def _handle_offline_status(self, state_data=None):
        message = "뱅종"
        json_data = self.getOffJson()
        
        self.offLineTitle()

        self.data.livePostList.append((message, json_data))
        
        self.title_data.loc[self.channel_id, 'state_update_time']["closeDate"] = datetime.now().isoformat()
        self.title_data.loc[self.channel_id, 'state_update_time']["titleChangeDate"] = datetime.now().isoformat()
    
    def get_channel_url(self):
        afreecaID = self.id_list.loc[self.channel_id, "afreecaID"]
        bno = self.title_data.loc[self.channel_id, 'chatChannelId']
        return f"https://play.sooplive.co.kr/{afreecaID}/{bno}"
    
    def getViewer_count(self, state_data):
        self.data.view_count = state_data['broad']['current_sum_viewer']
    
    def getMessage(self):
        return "뱅온!" if (self.turnOnline()) else "방제 변경"
    
    def turnOnline(self):
        now_time = self.getStarted_at("openDate")
        old_time = self.title_data.loc[self.channel_id,'update_time']
        return self.data.live == 1 and self.title_data.loc[self.channel_id,'live_state'] == "CLOSE" and now_time > old_time
    
    def turnOffline(self):
        return self.data.live == 0 and self.title_data.loc[self.channel_id,'live_state'] == "OPEN"
    
    async def get_live_thumbnail_image(self, state_data, message=None):
        for count in range(40):
            thumbnail_image = self.get_thumbnail_image()
            if thumbnail_image is None: 
                print(f"{datetime.now()} wait make thumbnail 1 .{count}.{str(self.getImageURL())}")
                await asyncio.sleep(0.05)
                continue
            break
        else: thumbnail_image = ""

        return thumbnail_image
    
    def get_thumbnail_image(self): 
        try:
            self.saveImage()
            file = {'file': open("explain.png", 'rb')}
            data = {"username": self.channel_name,
                    "avatar_url": self.id_list.loc[self.channel_id, 'profile_image']}
            
            thumbnailURL = environ['recvThumbnailURL']

            thumbnail = post(thumbnailURL, files=file, data=data, timeout=3)
            try: remove('explain.png')
            except: pass
            frontIndex = thumbnail.text.index('"proxy_url"')
            thumbnail = thumbnail.text[frontIndex:]
            frontIndex = thumbnail.index('https://media.discordap')
            return thumbnail[frontIndex:thumbnail.index(".png") + 4]
        except:
            return None

    def saveImage(self): 
        urlretrieve(self.getImageURL(), "explain.png")

    def getImageURL(self) -> str:
        link = f"https://liveimg.afreecatv.com/m/{self.title_data.loc[self.channel_id, 'chatChannelId']}"
        self.data.thumbnail_url = link
        return link
    
    async def getOnAirJson(self, message, state_data):
        self.getViewer_count(state_data)
        thumbnail_url = await self.get_live_thumbnail_image(state_data)

        return self.get_online_state_json(message, thumbnail_url)
    
    def get_online_state_json(self, message, thumbnail_url):
        return {"username": self.channel_name, "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "방제", "value": self.data.title, "inline": True},
                        {"name": ':busts_in_silhouette: 시청자수',
                        "value": self.data.view_count, "inline": True}],
                    "title": f"{self.channel_name} {message}\n",
                "url": self.get_channel_url(), 
                "image": {"url": thumbnail_url},
                "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().soop_icon },
                "timestamp": base.changeUTCtime(self.getStarted_at("openDate"))}]}
    
	# def get_online_titleChange_state_json(self, message, title, url, started_at, thumbnail):
	# 	return {"username": self.channel_name, "avatar_url": self.afreecaIDList.loc[self.channel_id, 'profile_image'],\
	# 			"embeds": [
	# 				{"color": int(self.afreecaIDList.loc[self.channel_id, 'channel_color']),
	# 				"fields": [
	# 					{"name": "이전 방제", "value": str(self.titleData.loc[self.channel_id,'title1']), "inline": True},
	# 					{"name": "현재 방제", "value": title, "inline": True}],
	# 				"title":  f"{self.channel_name} {message}\n",\
	# 			"url": url, \
	# 			"image": {"url": thumbnail},
	# 			"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().soop_icon },
	# 			"timestamp": base.changeUTCtime(started_at)}]}

    def getOffJson(self): 
        return {"username": self.channel_name, "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "title": self.channel_name +" 방송 종료\n",
                }]}
    

class NotificationSender:
    def __init__(self, 
                 supabase_url: str = environ.get('supabase_url'),
                 supabase_key: str = environ.get('supabase_key'),
                 firebase_creds_path: str = environ.get('firebase_creds_path')):

        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        
        # Initialize Firebase Admin SDK
        cred = credentials.Certificate(firebase_creds_path)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        
        # Configure settings
        self.MAX_RETRIES = 3
        self.MAX_CONCURRENT = 5
        self.BASE_DELAY = 0.2  # Base delay for exponential backoff
        self.TIMEOUT = 15  # Default timeout for requests

    async def send_notifications(self, notifications: List[Tuple[str, Dict[str, Any]]]) -> List[str]:
        """
        Send notifications to multiple users
        notifications: List of (user_token, notification_data) tuples
        """
        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        
        # Create tasks for each notification
        tasks = [
            asyncio.create_task(
                self._send_notification_with_retry(token, data, semaphore)
            ) 
            for token, data in notifications
        ]
        
        # Collect responses as tasks complete
        responses = []
        for task in asyncio.as_completed(tasks):
            result = await task
            if result is not None:
                responses.append(result)
        
        return responses

    async def _send_notification_with_retry(self, 
                                          token: str, 
                                          data: Dict[str, Any], 
                                          semaphore: asyncio.Semaphore) -> str:
        """Send a single notification with retry logic"""
        async with semaphore:
            for attempt in range(self.MAX_RETRIES):
                try:
                    # Create the message
                    message = messaging.Message(
                        data={
                            'title': data.get('username', 'Notification'),
                            'body': data.get('content', ''),
                            'image': data.get('avatar_url', ''),
                            'channel_name': data.get('username', '').split(' >> ')[-1] if ' >> ' in data.get('username', '') else '',
                            'timestamp': str(datetime.now().timestamp())
                        },
                        token=token
                    )
                    
                    # Send the message
                    response = messaging.send(message)
                    return response
                
                except messaging.ApiCallError as e:
                    # Handle invalid token
                    if 'invalid-argument' in str(e) or 'registration-token-not-registered' in str(e):
                        await self._handle_invalid_token(token)
                        break
                    
                    if attempt == self.MAX_RETRIES - 1:
                        await self._log_error(f"Failed to send notification after {self.MAX_RETRIES} attempts: {e}")
                        break
                    
                    # Exponential backoff
                    await asyncio.sleep(self.BASE_DELAY * (2 ** attempt))
                
                except Exception as e:
                    await self._log_error(f"Unexpected error sending notification: {e}")
                    
                    if attempt == self.MAX_RETRIES - 1:
                        break
                    
                    # Exponential backoff
                    await asyncio.sleep(self.BASE_DELAY * (2 ** attempt))
        
        return None

    async def _handle_invalid_token(self, token: str):
        """Remove invalid device token from database"""
        if not self.supabase_url or not self.supabase_key:
            return

        try:
            supabase = create_client(self.supabase_url, self.supabase_key)
            
            # Find and delete the token
            response = supabase.table('user_devices').delete().eq('token', token).execute()
            await self._log_error(f"Removed invalid token from database: {token}")
        except Exception as e:
            await self._log_error(f"Error removing invalid token: {e}")

    async def _log_error(self, message: str):
        """Log error message"""
        print(f"{datetime.now()} {message}")
        # You could also save errors to a log file or database

def get_user_device_tokens(DO_TEST, user_data, channel_name, channel_id, notification_data, db_name):
    """Get list of device tokens to send notifications to"""
    result_tokens = []
    try:
        if DO_TEST:
            # For testing
            return result_tokens
        
        for user_id, user_info in user_data.items():
            try:
                # Check if user is subscribed to this channel
                subscriptions = user_info.get(db_name, {})
                
                # Handle different data formats
                if isinstance(subscriptions, str):
                    channels = [subscriptions]
                elif isinstance(subscriptions, dict):
                    channels = subscriptions.get(channel_id, [])
                else:
                    channels = []
                
                # If user is subscribed to this channel, add their device tokens
                if channel_name in channels:
                    # Get all device tokens for this user
                    for token in user_info.get('device_tokens', []):
                        result_tokens.append((token, notification_data))
            except (KeyError, AttributeError) as e:
                continue
        
        return result_tokens
    except Exception as e:
        print(f"Error in get_user_device_tokens: {type(e).__name__}: {str(e)}")
        return result_tokens


    
async def main_loop(init: base.initVar):

    while True:
        try:
            if init.count % 2 == 0: await base.userDataVar(init)

            chzzk_live_tasks = [asyncio.create_task(chzzk_live_message(init, channel_id).start()) for channel_id in init.chzzkIDList["channelID"]]
            afreeca_live_tasks = [asyncio.create_task(afreeca_live_message(init, channel_id).start()) for channel_id in init.afreecaIDList["channelID"]]
            
            tasks = [
                *chzzk_live_tasks,
                *afreeca_live_tasks,
            ]

            await asyncio.gather(*tasks)
            await base.fSleep(init)
            base.fCount(init)

        except Exception as e:
            asyncio.create_task(DiscordWebhookSender._log_error(f"Error in main loop: {str(e)}"))
            await asyncio.sleep(1)

async def main():
    # Flet 앱 백그라운드에서 실행
    
    init = base.initVar()
    await base.discordBotDataVars(init)
    await base.userDataVar(init)
    await asyncio.sleep(1)
    
    await asyncio.create_task(main_loop(init))
        
if __name__ == "__main__":
    asyncio.run(main())