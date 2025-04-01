import base
import asyncio
from json import loads
from datetime import datetime
from os import remove, environ
from requests import post
from urllib.request import urlretrieve
from discord_webhook_sender import DiscordWebhookSender, get_list_of_urls

# 기본 라이브 메시지 클래스 - 공통 기능 포함
class base_live_message:
    def __init__(self, init_var: base.initVar, channel_id, platform_name):
        self.init = init_var
        self.DO_TEST = init_var.DO_TEST
        self.userStateData = init_var.userStateData
        self.platform_name = platform_name
        self.channel_id = channel_id
        self.data = base.LiveData()
        
        # 플랫폼별 데이터 초기화
        if platform_name == "chzzk":
            self.id_list = init_var.chzzkIDList
            self.title_data = init_var.chzzk_titleData
        elif platform_name == "afreeca":
            self.id_list = init_var.afreecaIDList
            self.title_data = init_var.afreeca_titleData
        else:
            raise ValueError(f"Unsupported platform: {platform_name}")

    async def start(self):
        await self.addMSGList()
        await self.postLive_massge()

    async def addMSGList(self):
        try:
            state_data = await self._get_state_data()
                
            if not self._is_valid_state_data(state_data):
                return

            self._update_title_if_needed()

            # 스트림 데이터 얻기
            stream_data = self._get_stream_data(state_data)
            self._update_stream_info(stream_data, state_data)

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
        if (base.if_after_time(self.data.change_title_time) and 
            self._get_old_title() != self._get_title()):
            self.title_data.loc[self.channel_id,'title2'] = self.title_data.loc[self.channel_id,'title1']
            asyncio.create_task(base.save_airing_data(self.title_data, self.platform_name, self.channel_id))

    def _get_channel_name(self):
        return self.id_list.loc[self.channel_id, 'channelName']
    
    async def postLive_massge(self):
        try:
            if not self.data.livePostList: 
                return
            message, json_data = self.data.livePostList.pop(0)
            channel_name = self._get_channel_name()

            db_name = self._get_db_name(message)
            self._log_message(message, channel_name)

            list_of_urls = get_list_of_urls(
                self.DO_TEST, 
                self.userStateData, 
                channel_name, 
                self.channel_id, 
                json_data, 
                db_name
            )
            asyncio.create_task(DiscordWebhookSender().send_messages(list_of_urls))
            await base.save_airing_data(self.title_data, self.platform_name, self.channel_id)

        except Exception as e:
            asyncio.create_task(DiscordWebhookSender._log_error(f"postLiveMSG {e}"))
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
    
    def _log_message(self, message, channel_name):
        """메시지 로깅"""
        now = datetime.now()
        if message == "뱅온!":
            print(f"{now} onLine {channel_name} {message}")
        elif message == "방제 변경":
            old_title = self._get_old_title()
            print(f"{now} onLine {channel_name} {message}")
            print(f"{now} 이전 방제: {old_title}")
            print(f"{now} 현재 방제: {self.data.title}")
        elif message == "뱅종":
            print(f"{now} offLine {channel_name}")
    
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
        raise NotImplementedError
    
    async def _handle_offline_status(self, state_data):
        raise NotImplementedError
    
    def getStarted_at(self, status: str):
        time_str = self.data.start_at[status]
        if not time_str or time_str == '0000-00-00 00:00:00': 
            return None
        try:
            time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            return time.isoformat()
        except ValueError:
            return None
    
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
            return state_data["code"] == 200
        except Exception as e:
            asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} _is_valid_state_data.{self.channel_id}.{e}"))
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
        self.id_list.loc[self.channel_id, 'profile_image'] = self.data.profile_image

    def _should_process_online_status(self):
        return ((self.checkStateTransition("OPEN") or 
           (self.ifChangeTitle())) and
           base.if_after_time(self.data.LiveCountEnd, sec=15))

    def _should_process_offline_status(self):
        return (self.checkStateTransition("CLOSE") and 
          base.if_after_time(self.data.LiveCountStart, sec=15))
    
    async def _handle_online_status(self, state_data):
        message = self.getMessage()
        json_data = await self.getOnAirJson(message, state_data)

        self.onLineTime(message)
        self.onLineTitle(message)

        self.data.livePostList.append((message, json_data))

        await base.save_profile_data(self.id_list, 'chzzk', self.channel_id)

        if message == "뱅온!": 
            self.data.LiveCountStart = datetime.now().isoformat()
        self.data.change_title_time = datetime.now().isoformat()

    async def _handle_offline_status(self, state_data):
        message = "뱅종"
        json_data = await self.getOffJson(state_data, message)

        self.offLineTitle()
        self.offLineTime()

        self.data.livePostList.append((message, json_data))

        self.data.LiveCountEnd = datetime.now().isoformat()
        self.data.change_title_time = datetime.now().isoformat()
    
    def offLineTime(self):
        self.title_data.loc[self.channel_id,'update_time'] = self.getStarted_at("closeDate")

    def get_channel_url(self): 
        return f'https://chzzk.naver.com/live/{self.id_list.loc[self.channel_id, "channel_code"]}'

    def getViewer_count(self, state_data):
        return state_data['content']['concurrentUserCount']

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
                data = {"username": self._get_channel_name(),
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
        return link
    
    async def getOnAirJson(self, message, state_data):
        channel_url = self.get_channel_url()
        
        if self.data.live == "CLOSE":
            return self.get_state_data_change_title_json(message, channel_url)
        
        started_at = self.getStarted_at("openDate")
        viewer_count = self.getViewer_count(state_data)
        live_thumbnail_image = await self.get_live_thumbnail_image(state_data, message)
        
        if message == "뱅온!":
            return self.get_online_state_json(
                message, channel_url, started_at, live_thumbnail_image, viewer_count
            )
        
        return self.get_online_titleChange_state_json(
            message, channel_url, started_at, live_thumbnail_image, viewer_count
        )
    
    def get_online_state_json(self, message, url, started_at, thumbnail, viewer_count):
        return {"username": self._get_channel_name(), "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "방제", "value": self.data.title, "inline": True},
                        # {"name": ':busts_in_silhouette: 시청자수',
                        # "value": viewer_count, "inline": True}
                        ],
                    "title": f"{self._get_channel_name()} {message}\n",
                "url": url,
                # "image": {"url": thumbnail},
                "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
                "timestamp": base.changeUTCtime(started_at)}]}

    def get_online_titleChange_state_json(self, message, url, started_at, thumbnail, viewer_count):
        return {"username": self._get_channel_name(), "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "방제", "value": self.data.title, "inline": True},
                        {"name": ':busts_in_silhouette: 시청자수',
                        "value": viewer_count, "inline": True}
                        ],
                    "title": f"{self._get_channel_name()} {message}\n",
                "url": url,
                "image": {"url": thumbnail},
                "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
                "timestamp": base.changeUTCtime(started_at)}]}

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

    def get_state_data_change_title_json(self, message, url):
        return {"username": self._get_channel_name(), "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "이전 방제", "value": str(self._get_title()), "inline": True},
                        {"name": "현재 방제", "value": self.data.title, "inline": True}],
                    "title": f"{self._get_channel_name()} {message}\n",
                "url": url}]}

    async def getOffJson(self, state_data, message):
        started_at = self.getStarted_at("closeDate")
        live_thumbnail_image = await self.get_live_thumbnail_image(state_data, message)
        
        return {"username": self._get_channel_name(), "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "title": self._get_channel_name() +" 방송 종료\n",
                "image": {"url": live_thumbnail_image},
                "footer": { "text": f"방종 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
                "timestamp": base.changeUTCtime(started_at)}]}

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
                base.if_after_time(self.data.LiveCountEnd, sec=15))
    
    def _should_process_offline_status(self):
        return (self.turnOffline() and
                  base.if_after_time(self.data.LiveCountStart, sec=15))
    
    async def _handle_online_status(self, state_data):
        message = self.getMessage()
        json_data = await self.getOnAirJson(message, state_data)
        
        self.onLineTime(message)
        self.onLineTitle(message)
        
        self.data.livePostList.append((message, json_data))
        
        await base.save_profile_data(self.id_list, 'afreeca', self.channel_id)

        if message == "뱅온!": 
            self.data.LiveCountStart = datetime.now().isoformat()
        self.data.change_title_time = datetime.now().isoformat()
    
    async def _handle_offline_status(self, state_data=None):
        message = "뱅종"
        json_data = self.getOffJson()
        
        self.offLineTitle()

        self.data.livePostList.append((message, json_data))
        
        self.data.LiveCountEnd = datetime.now().isoformat()
        self.data.change_title_time = datetime.now().isoformat()
    
    def get_channel_url(self):
        afreecaID = self.id_list.loc[self.channel_id, "afreecaID"]
        bno = self.title_data.loc[self.channel_id, 'chatChannelId']
        return f"https://play.sooplive.co.kr/{afreecaID}/{bno}"
    
    def getViewer_count(self, state_data):
        return state_data['broad']['current_sum_viewer']
    
    def getMessage(self):
        return "뱅온!" if (self.turnOnline()) else "방제 변경"
    
    def turnOnline(self):
        now_time = self.getStarted_at("openDate")
        old_time = self.title_data.loc[self.channel_id,'update_time']
        return self.data.live == 1 and self.title_data.loc[self.channel_id,'live_state'] == "CLOSE" and now_time > old_time
    
    def turnOffline(self):
        return self.data.live == 0 and self.title_data.loc[self.channel_id,'live_state'] == "OPEN"
    
    async def get_live_thumbnail_image(self, state_data, message=None):
        for count in range(20):
            thumbnail_image = self.get_thumbnail_image()
            if thumbnail_image is None: 
                print(f"{datetime.now()} wait make thumbnail 1 .{str(self.getImageURL())}")
                await asyncio.sleep(0.05)
                continue
            break
        else: thumbnail_image = ""

        return thumbnail_image
    
    def get_thumbnail_image(self): 
        try:
            self.saveImage()
            file = {'file': open("explain.png", 'rb')}
            data = {"username": self._get_channel_name(),
                    "avatar_url": self.id_list.loc[self.channel_id, 'profile_image']}
            thumbnail = post(environ['recvThumbnailURL'], files=file, data=data, timeout=3)
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
        return f"https://liveimg.afreecatv.com/m/{self.title_data.loc[self.channel_id, 'chatChannelId']}"
    
    async def getOnAirJson(self, message, state_data):
        channel_url = self.get_channel_url()
        started_at = self.getStarted_at("openDate")
        viewer_count = self.getViewer_count(state_data)
        thumbnail = await self.get_live_thumbnail_image(state_data)

        return self.get_online_state_json(message, channel_url, started_at, thumbnail, viewer_count)
    
    def get_online_state_json(self, message, url, started_at, thumbnail, viewer_count):
        return {"username": self._get_channel_name(), "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "방제", "value": self.data.title, "inline": True},
                        {"name": ':busts_in_silhouette: 시청자수',
                        "value": viewer_count, "inline": True}],
                    "title": f"{self._get_channel_name()} {message}\n",
                "url": url, 
                "image": {"url": thumbnail},
                "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().soop_icon },
                "timestamp": base.changeUTCtime(started_at)}]}
    
	# def get_online_titleChange_state_json(self, message, title, url, started_at, thumbnail):
	# 	return {"username": self._get_channel_name(), "avatar_url": self.afreecaIDList.loc[self.channel_id, 'profile_image'],\
	# 			"embeds": [
	# 				{"color": int(self.afreecaIDList.loc[self.channel_id, 'channel_color']),
	# 				"fields": [
	# 					{"name": "이전 방제", "value": str(self.titleData.loc[self.channel_id,'title1']), "inline": True},
	# 					{"name": "현재 방제", "value": title, "inline": True}],
	# 				"title":  f"{self._get_channel_name()} {message}\n",\
	# 			"url": url, \
	# 			"image": {"url": thumbnail},
	# 			"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().soop_icon },
	# 			"timestamp": base.changeUTCtime(started_at)}]}

    def getOffJson(self): 
        return {"username": self._get_channel_name(), "avatar_url": self.id_list.loc[self.channel_id, 'profile_image'],
                "embeds": [
                    {"color": int(self.id_list.loc[self.channel_id, 'channel_color']),
                    "title": self._get_channel_name() +" 방송 종료\n",
                }]}
    
    