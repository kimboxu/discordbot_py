import base
import asyncio
from datetime import datetime
from os import remove, environ
from requests import post
from urllib.request import urlretrieve
from discord_webhook_sender import DiscordWebhookSender, get_list_of_urls
from abc import ABC, abstractmethod

# 기본 추상 클래스
class BaseLiveMessage(ABC):
    def __init__(self, init_var, channel_id):
        self.init = init_var
        self.DO_TEST = init_var.DO_TEST
        self.supabase = init_var.supabase
        self.userStateData = init_var.userStateData
        self.channel_id = channel_id
        self.data = self._create_data_object()

    @abstractmethod
    def _create_data_object(self):
        """플랫폼별 데이터 객체 생성"""
        pass

    async def start(self):
        """메인 실행 메서드"""
        await self.addMSGList()
        await self.postLive_massge()

    async def addMSGList(self):
        """메시지 리스트에 추가하는 작업"""
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
            await self._log_error(f"Error getting state data: {e}")

    @abstractmethod
    async def _get_state_data(self):
        """플랫폼별 상태 데이터 가져오기"""
        pass
    
    @abstractmethod
    def _is_valid_state_data(self, state_data):
        """상태 데이터가 유효한지 검증"""
        pass
    
    @abstractmethod
    def _update_title_if_needed(self):
        """필요시 타이틀 업데이트"""
        pass
    
    @abstractmethod
    def _get_stream_data(self, state_data):
        """스트림 데이터 얻기"""
        pass
    
    @abstractmethod
    def _update_stream_info(self, stream_data, state_data):
        """스트림 정보 업데이트"""
        pass
    
    @abstractmethod
    def _should_process_online_status(self):
        """온라인 상태를 처리해야 하는지 확인"""
        pass
    
    @abstractmethod
    def _should_process_offline_status(self):
        """오프라인 상태를 처리해야 하는지 확인"""
        pass
    
    @abstractmethod
    async def _handle_online_status(self, state_data):
        """온라인 상태 처리"""
        pass
    
    @abstractmethod
    async def _handle_offline_status(self, state_data=None):
        """오프라인 상태 처리"""
        pass
    
    async def postLive_massge(self):
        """메시지 전송"""
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
            await self._save_airing_data()
            
        except Exception as e:
            await self._log_error(f"Error posting message: {e}")
            self.data.livePostList.clear()
    
    @abstractmethod
    def _get_channel_name(self):
        """채널 이름 가져오기"""
        pass
    
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
    
    @abstractmethod
    def _get_old_title(self):
        """이전 타이틀 가져오기"""
        pass
    
    @abstractmethod
    async def _save_airing_data(self):
        """방송 데이터 저장"""
        pass
    
    async def _log_error(self, message):
        """에러 로깅"""
        return await DiscordWebhookSender._log_error(message)


# CHZZK 구현
class ChzzkLiveMessage(BaseLiveMessage):
    def __init__(self, init_var, chzzk_id):
        self.chzzkIDList = init_var.chzzkIDList
        self.chzzk_titleData = init_var.chzzk_titleData
        super().__init__(init_var, chzzk_id)
        
    def _create_data_object(self):
        return base.LiveData()
        
    async def _get_state_data(self):
        return await base.get_message(
            "chzzk", 
            base.chzzk_getLink(self.chzzkIDList.loc[self.channel_id, "channel_code"])
        )
    
    def _is_valid_state_data(self, state_data):
        return state_data["code"] == 200
    
    def _update_title_if_needed(self):
        if (base.if_after_time(self.data.change_title_time) and 
            self.chzzk_titleData.loc[self.channel_id,'title2'] != self.chzzk_titleData.loc[self.channel_id,'title1']):
            self.chzzk_titleData.loc[self.channel_id,'title2'] = self.chzzk_titleData.loc[self.channel_id,'title1']
    
    def _get_stream_data(self, state_data):
        return base.chzzk_getChannelOffStateData(
            state_data["content"], 
            self.chzzkIDList.loc[self.channel_id, "channel_code"], 
            self.chzzkIDList.loc[self.channel_id, 'channel_thumbnail']
        )
    
    def _update_stream_info(self, stream_data, state_data):
        self.data.start_at["openDate"] = state_data['content']["openDate"]
        self.data.start_at["closeDate"] = state_data['content']["closeDate"]
        self.data.live, self.data.title, self.data.channel_thumbnail_url = stream_data
        self.chzzkIDList.loc[self.channel_id, 'channel_thumbnail'] = self.data.channel_thumbnail_url
    
    def _should_process_online_status(self):
        return ((self.checkStateTransition("OPEN") or 
            (self.ifChangeTitle())) and
            base.if_after_time(self.data.LiveCountEnd, sec = 15))
    
    def _should_process_offline_status(self):
        return (self.checkStateTransition("CLOSE") and 
            base.if_after_time(self.data.LiveCountStart, sec = 15))
    
    async def _handle_online_status(self, state_data):
        message = self.getMessage()
        json_data = await self.getOnAirJson(message, state_data)

        self.onLineTime(message)
        self.onLineTitle(message)

        self.data.livePostList.append((message, json_data))

        await base.save_profile_data(self.chzzkIDList, 'chzzk', self.channel_id, self.supabase)

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
    
    def _get_channel_name(self):
        return self.chzzkIDList.loc[self.channel_id, 'channelName']
    
    def _get_old_title(self):
        return self.chzzk_titleData.loc[self.channel_id,'title2']
    
    async def _save_airing_data(self):
        await base.save_airing_data(self.chzzk_titleData, 'chzzk', self.channel_id, self.supabase)
    
    # 기존 메서드들 (필요시 유지)
    def ifChangeTitle(self):
        return self.data.title not in [str(self.chzzk_titleData.loc[self.channel_id,'title1']), str(self.chzzk_titleData.loc[self.channel_id,'title2'])]
    
    def onLineTitle(self, message):
        if message == "뱅온!":
            self.chzzk_titleData.loc[self.channel_id, 'live_state'] = "OPEN"
        self.chzzk_titleData.loc[self.channel_id,'title2'] = self.chzzk_titleData.loc[self.channel_id,'title1']
        self.chzzk_titleData.loc[self.channel_id,'title1'] = self.data.title
    
    def onLineTime(self, message):
        if message == "뱅온!":
            self.chzzk_titleData.loc[self.channel_id,'update_time'] = self.getStarted_at("openDate")
    
    def offLineTitle(self):
        self.chzzk_titleData.loc[self.channel_id, 'live_state'] = "CLOSE"
    
    def offLineTime(self):
        self.chzzk_titleData.loc[self.channel_id,'update_time'] = self.getStarted_at("closeDate")
    
    def checkStateTransition(self, target_state: str):
        if self.data.live != target_state or self.chzzk_titleData.loc[self.channel_id, 'live_state'] != ("CLOSE" if target_state == "OPEN" else "OPEN"):
            return False
        return self.getStarted_at(("openDate" if target_state == "OPEN" else "closeDate")) > self.chzzk_titleData.loc[self.channel_id, 'update_time']
    
    def getMessage(self) -> str: 
        return "뱅온!" if (self.checkStateTransition("OPEN")) else "방제 변경"
    
    def getStarted_at(self, status: str): 
        time_str = self.data.start_at[status]
        if not time_str or time_str == '0000-00-00 00:00:00': 
            return None
        try:
            time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            return time.isoformat()
        except ValueError:
            return None
        
# 나머지 메서드들도 유사하게 구현
        
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
        
        return self.get_online_titleChange_state_json(message, channel_url, started_at, live_thumbnail_image, viewer_count)
    
    def get_online_state_json(self, message, url, started_at, thumbnail, viewer_count):
        return {"username": self.chzzkIDList.loc[self.channel_id, 'channelName'], "avatar_url": self.chzzkIDList.loc[self.channel_id, 'channel_thumbnail'],
                "embeds": [
                    {"color": int(self.chzzkIDList.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "방제", "value": self.data.title, "inline": True},
                        # {"name": ':busts_in_silhouette: 시청자수',
                        # "value": viewer_count, "inline": True}
                        ],
                    "title":  f"{self.chzzkIDList.loc[self.channel_id, 'channelName']} {message}\n",
                "url": url,
                # "image": {"url": thumbnail},
                "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
                "timestamp": base.changeUTCtime(started_at)}]}
    
    def get_online_titleChange_state_json(self, message, url, started_at, thumbnail, viewer_count):
        return {"username": self.chzzkIDList.loc[self.channel_id, 'channelName'], "avatar_url": self.chzzkIDList.loc[self.channel_id, 'channel_thumbnail'],
        "embeds": [
            {"color": int(self.chzzkIDList.loc[self.channel_id, 'channel_color']),
            "fields": [
                {"name": "방제", "value": self.data.title, "inline": True},
                {"name": ':busts_in_silhouette: 시청자수',
                "value": viewer_count, "inline": True}
                ],
            "title":  f"{self.chzzkIDList.loc[self.channel_id, 'channelName']} {message}\n",
        "url": url,
        "image": {"url": thumbnail},
        "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
        "timestamp": base.changeUTCtime(started_at)}]}
        # return {"username": self.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": self.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
        # 		"embeds": [
        # 			{"color": int(self.chzzkIDList.loc[chzzkID, 'channel_color']),
        # 			"fields": [
        # 				{"name": "이전 방제", "value": str(self.chzzk_titleData.loc[chzzkID,'title1']), "inline": True},
        # 				{"name": "현재 방제", "value": title, "inline": True},
        # 				{"name": ':busts_in_silhouette: 시청자수',
        # 				"value": viewer_count, "inline": True}],
        # 			"title":  f"{self.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
        # 		"url": url,
        # 		"image": {"url": thumbnail},
        # 		"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
        # 		"timestamp": base.changeUTCtime(started_at)}]}

    async def get_live_thumbnail_image(self, state_data, message):
        for count in range(20):
            time_difference = (datetime.now() - datetime.fromisoformat(self.chzzk_titleData.loc[self.channel_id, 'update_time'])).total_seconds()

            if message == "뱅온!" or self.chzzk_titleData.loc[self.channel_id, 'live_state'] == "CLOSE" or time_difference < 15: 
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
    
    def get_state_data_change_title_json(self, message, url):
        return {"username": self.chzzkIDList.loc[self.channel_id, 'channelName'], "avatar_url": self.chzzkIDList.loc[self.channel_id, 'channel_thumbnail'],
                "embeds": [
                    {"color": int(self.chzzkIDList.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "이전 방제", "value": str(self.chzzk_titleData.loc[self.channel_id,'title1']), "inline": True},
                        {"name": "현재 방제", "value": self.data.title, "inline": True}],
                    "title":  f"{self.chzzkIDList.loc[self.channel_id, 'channelName']} {message}\n",
                "url": url}]}
    
    def get_channel_url(self): 
        return f'https://chzzk.naver.com/live/{self.chzzkIDList.loc[self.channel_id, "channel_code"]}'

    def getViewer_count(self, state_data):
        return state_data['content']['concurrentUserCount']

    def get_thumbnail_image(self, state_data): #thumbnail shape do transformation able to send to discord
        try:
            if state_data['content']['liveImageUrl'] is not None:
                self.saveImage(state_data)
                file = {'file'      : open("explain.png", 'rb')}
                data = {"username"  : self.chzzkIDList.loc[self.channel_id, 'channelName'],
                        "avatar_url": self.chzzkIDList.loc[self.channel_id, 'channel_thumbnail']}
                thumbnail  = post(environ['recvThumbnailURL'], files=file, data=data, timeout=3)
                try: remove('explain.png')
                except: pass
                frontIndex = thumbnail.text.index('"proxy_url"')
                thumbnail  = thumbnail.text[frontIndex:]
                frontIndex = thumbnail.index('https://media.discordap')
                return thumbnail[frontIndex:thumbnail.index(".png") + 4]
            return None
        except Exception as e:
            asyncio.create_task(DiscordWebhookSender._log_error(f"{datetime.now()} wait make thumbnail2 {e}"))
            return None
        
    def saveImage(self, state_data): urlretrieve(self.getImageURL(state_data), "explain.png") # save thumbnail image to png

    def getImageURL(self, state_data) -> str:
        link = state_data['content']['liveImageUrl']
        link = link.replace("{type", "")
        link = link.replace("}.jpg", "0.jpg")
        self.chzzk_titleData.loc[self.channel_id,'channelURL'] = link
        return link

# Afreeca 구현
class AfreecaLiveMessage(BaseLiveMessage):
    def __init__(self, init_var, afreeca_id):
        self.afreecaIDList = init_var.afreecaIDList
        self.afreeca_titleData = init_var.afreeca_titleData
        super().__init__(init_var, afreeca_id)
    
    def _create_data_object(self):
        return base.LiveData()
    
    async def _get_state_data(self):
        return await base.get_message(
            "afreeca", 
            base.afreeca_getLink(self.afreecaIDList.loc[self.channel_id, "afreecaID"])
        )
    
    def _is_valid_state_data(self, state_data):
        try:
            state_data["station"]["user_id"]
            return True
        except:
            return False
    
    # 다른 메서드들도 유사하게 구현
    def _update_title_if_needed(self):
        if (base.if_after_time(self.data.change_title_time) and 
            (self.afreeca_titleData.loc[self.channel_id,'title2'] != self.afreeca_titleData.loc[self.channel_id,'title1'])):
            self.afreeca_titleData.loc[self.channel_id,'title2'] = self.afreeca_titleData.loc[self.channel_id,'title1']
    
    def _get_stream_data(self, state_data):
        return base.chzzk_getChannelOffStateData(
            state_data["content"], 
            self.chzzkIDList.loc[self.channel_id, "channel_code"], 
            self.chzzkIDList.loc[self.channel_id, 'channel_thumbnail']
        )
    
    def _update_stream_info(self, stream_data, state_data):
        self.data.start_at["openDate"] = state_data['content']["openDate"]
        self.data.start_at["closeDate"] = state_data['content']["closeDate"]
        self.data.live, self.data.title, self.data.channel_thumbnail_url = stream_data
        self.chzzkIDList.loc[self.channel_id, 'channel_thumbnail'] = self.data.channel_thumbnail_url
    
    def _get_stream_data(self, state_data):
        return base.afreeca_getChannelOffStateData(
            state_data,
            self.afreecaIDList.loc[self.channel_id, "afreecaID"],
            self.afreecaIDList.loc[self.channel_id, 'channel_thumbnail']
        )

    def _update_stream_info(self, stream_data, state_data):
        self.update_broad_no(state_data)
        self.data.start_at["openDate"] = state_data["station"]["broad_start"]
        self.data.live, self.data.title, self.data.channel_thumbnail_url = stream_data
        self.afreecaIDList.loc[self.channel_id, 'channel_thumbnail'] = self.data.channel_thumbnail_url

    def _should_process_online_status(self):
        return ((self.turnOnline() or 
                (self.data.title and self.ifChangeTitle())) and 
                base.if_after_time(self.data.LiveCountEnd, sec = 15))

    def _should_process_offline_status(self):
        return (self.turnOffline() and
                base.if_after_time(self.data.LiveCountStart, sec = 15))

    async def _handle_online_status(self, state_data):
        message = self.getMessage()
        json_data = await self.getOnAirJson(message, state_data)
        
        self.onLineTime(message)
        self.onLineTitle(message)
        
        self.data.livePostList.append((message, json_data))
        
        await base.save_profile_data(self.afreecaIDList, 'afreeca', self.supabase)

        if message == "뱅온!": 
            self.data.LiveCountStart = datetime.now().isoformat()
        self.data.change_title_time = datetime.now().isoformat()

    async def _handle_offline_status(self, state_data):
        message = "뱅종"
        json_data = self.getOffJson()
        
        self.offLineTitle()

        self.data.livePostList.append((message, json_data))
        
        self.data.LiveCountEnd = datetime.now().isoformat()
        self.data.change_title_time = datetime.now().isoformat()

    def update_broad_no(self, state_data):
        if state_data["broad"] and state_data["broad"]["broad_no"] != self.afreeca_titleData.loc[self.channel_id, 'chatChannelId']:
            self.afreeca_titleData.loc[self.channel_id, 'oldChatChannelId'] = self.afreeca_titleData.loc[self.channel_id, 'chatChannelId']
            self.afreeca_titleData.loc[self.channel_id, 'chatChannelId'] = state_data["broad"]["broad_no"]
	
    def _get_channel_name(self):
        return self.afreecaIDList.loc[self.channel_id, 'channelName']
    
    def _get_old_title(self):
        self.afreeca_titleData.loc[self.channel_id,'title2']
    
    async def _save_airing_data(self):
        await base.save_airing_data(self.afreeca_titleData, 'afreeca', self.channel_id, self.supabase)
    
    # 기존 메서드들 (필요시 유지)
    def ifChangeTitle(self):
        return self.data.title not in [str(self.afreeca_titleData.loc[self.channel_id,'title1']), str(self.afreeca_titleData.loc[self.channel_id,'title2'])]
    
    def onLineTitle(self, message):
        if message == "뱅온!":
            self.afreeca_titleData.loc[self.channel_id, 'live_state'] = "OPEN"
        self.afreeca_titleData.loc[self.channel_id,'title2'] = self.afreeca_titleData.loc[self.channel_id,'title1']
        self.afreeca_titleData.loc[self.channel_id,'title1'] = self.data.title
    
    def onLineTime(self, message):
        if message == "뱅온!":
            self.afreeca_titleData.loc[self.channel_id,'update_time'] = self.getStarted_at("openDate")
    
    def offLineTitle(self):
        self.afreeca_titleData.loc[self.channel_id, 'live_state'] = "CLOSE"
    
    def getViewer_count(self, state_data):
        return state_data['broad']['current_sum_viewer'] #get viewer data

    def getMessage(self): return "뱅온!" if (self.turnOnline()) else "방제 변경" #turnOn or change title

    def get_thumbnail_image(self): #thumbnail shape do transformation able to send to discord
        try:
            self.saveImage()
            file = {'file'      : open("explain.png", 'rb')}
            data = {"username"  : self._get_channel_name(),
                    "avatar_url": self.afreecaIDList.loc[self.channel_id, 'channel_thumbnail']}
            thumbnail  = post(environ['recvThumbnailURL'], files=file, data=data, timeout=3)
            try: remove('explain.png')
            except: pass
            frontIndex = thumbnail.text.index('"proxy_url"')
            thumbnail  = thumbnail.text[frontIndex:]
            frontIndex = thumbnail.index('https://media.discordap')
            return thumbnail[frontIndex:thumbnail.index(".png") + 4]
        except:
            return None
    def saveImage(self, state_data): urlretrieve(self.getImageURL(state_data), "explain.png") # save thumbnail image to png

    def getImageURL(self) -> str:
        return f"https://liveimg.afreecatv.com/m/{self.afreeca_titleData.loc[self.channel_id, 'chatChannelId']}"
    
    def turnOnline(self):
        now_time = self.getStarted_at("openDate")
        old_time = self.afreeca_titleData.loc[self.channel_id,'update_time']
        return self.data.live == 1 and self.afreeca_titleData.loc[self.channel_id,'live_state'] == "CLOSE" and now_time > old_time #turn online
    
    def turnOffline(self):
        # now_time = self.getStarted_at(state_data)
        # old_time = self.afreeca_titleData.loc[self.channel_id,'update_time']
        return self.data.live == 0 and self.afreeca_titleData.loc[self.channel_id,'live_state'] == "OPEN" #turn offline
    
    async def getOnAirJson(self, message, state_data):
        channel_url  = self.get_channel_url()
        started_at   = self.getStarted_at("openDate")
        viewer_count = self.getViewer_count(state_data)
        thumbnail = await self.get_live_thumbnail_image(state_data)

        return self.get_online_state_json(message, channel_url, started_at, thumbnail, viewer_count)

    async def get_live_thumbnail_image(self, state_data):
        for count in range(20):
            thumbnail_image = self.get_thumbnail_image(self.channel_id)
            if thumbnail_image is None: 
                print(f"{datetime.now()} wait make thumbnail 1 .{str(self.getImageURL(state_data))}")
                await asyncio.sleep(0.05)
                continue
            break
        else: thumbnail_image = ""

        return thumbnail_image

    def get_online_state_json(self, message, url, started_at, thumbnail, viewer_count):
        return {"username": self._get_channel_name(), "avatar_url": self.afreecaIDList.loc[self.channel_id, 'channel_thumbnail'],\
                "embeds": [
                    {"color": int(self.afreecaIDList.loc[self.channel_id, 'channel_color']),
                    "fields": [
                        {"name": "방제", "value": self.data.title, "inline": True},
                        {"name": ':busts_in_silhouette: 시청자수',
                        "value": viewer_count, "inline": True}],
                    "title":  f"{self._get_channel_name()} {message}\n",\
                "url": url, \
                "image": {"url": thumbnail},
                "footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().soop_icon },
                "timestamp": base.changeUTCtime(started_at)}]}
    
    # def get_online_titleChange_state_json(self, message, title, url, started_at, thumbnail):
    # 	return {"username": self._get_channel_name(), "avatar_url": self.afreecaIDList.loc[self.channel_id, 'channel_thumbnail'],\
    # 			"embeds": [
    # 				{"color": int(self.afreecaIDList.loc[self.channel_id, 'channel_color']),
    # 				"fields": [
    # 					{"name": "이전 방제", "value": str(self.afreeca_titleData.loc[self.channel_id,'title1']), "inline": True},
    # 					{"name": "현재 방제", "value": title, "inline": True}],
    # 				"title":  f"{self._get_channel_name()} {message}\n",\
    # 			"url": url, \
    # 			"image": {"url": thumbnail},
    # 			"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().soop_icon },
    # 			"timestamp": base.changeUTCtime(started_at)}]}

    def getOffJson(self): #offJson
        return {"username": self._get_channel_name(), "avatar_url": self.afreecaIDList.loc[self.channel_id, 'channel_thumbnail'],\
                "embeds": [
                    {"color": int(self.afreecaIDList.loc[self.channel_id, 'channel_color']),
                    "title":  self._get_channel_name() +" 방송 종료\n",\
                # "image": {"url": self.afreecaIDList.loc[self.channel_id, 'offLine_thumbnail']}
                }]}

    def get_channel_url(self):
        afreeca_id = self.afreecaIDList.loc[self.channel_id, "afreecaID"]
        bno = self.afreeca_titleData.loc[self.channel_id, 'chatChannelId']
        return f"https://play.sooplive.co.kr/{afreeca_id}/{bno}"

    def getStarted_at(self, status: str): 
        time_str = self.data.start_at[status]
        if not time_str or time_str == '0000-00-00 00:00:00': 
            return None
        try:
            time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            return time.isoformat()
        except ValueError:
            return None