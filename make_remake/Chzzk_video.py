import asyncio
from os import environ
from datetime import datetime
from supabase import create_client
from discord_webhook_sender import DiscordWebhookSender
from base import getChzzkHeaders, getChzzkCookie, changeUTCtime, get_message, chzzkVideoData, iconLinkData

class chzzk_video:
    def __init__(self, init_var):
        self.DO_TEST = init_var.DO_TEST
        self.chzzkIDList = init_var.chzzkIDList
        self.chzzk_video = init_var.chzzk_video
        self.userStateData = init_var.userStateData
        self.chzzkID = ""
        self.data = chzzkVideoData()


    async def chzzk_video_msg(self):
        await self.check_chzzk_video()
        await self.post_chzzk_video()

    async def check_chzzk_video(self):
        def getChzzkDataList():
            headers = getChzzkHeaders()
            cookie = getChzzkCookie()
        
            return [
                [(f"https://api.chzzk.naver.com/service/v1/channels/{self.chzzkIDList.loc[chzzkID, 'channel_code']}/videos", headers, cookie), chzzkID] 
                for chzzkID in self.chzzkIDList["channelID"]
            ]
        
        list_of_stateData_chzzkID = None
        for _ in range(10):
            try:
                list_of_stateData_chzzkID = await get_message(getChzzkDataList(), "chzzk")
                break
            except:
                await asyncio.sleep(0.05)
        
        if not list_of_stateData_chzzkID:
            return
        
        for stateData_chzzkID in list_of_stateData_chzzkID:
            for stateData, chzzkID, should_process in stateData_chzzkID:
                if not self._should_process_video(stateData, should_process):
                    continue

                try:
                    await self._process_video_data(stateData, chzzkID)
                except Exception as e:
                    asyncio.create_task(DiscordWebhookSender._log_error(f"error get stateData chzzk video.{chzzkID}.{e}."))

    def _should_process_video(self, stateData, should_process):
        return should_process and stateData["code"] == 200

    async def _process_video_data(self, stateData, chzzkID):
        videoNo, videoTitle, publishDate, thumbnailImageUrl, _ = self.getChzzkState(stateData)
        
        # 이미 처리된 비디오 건너뛰기
        a=self.chzzk_video.loc[chzzkID, 'VOD_json']["publishDate"]
        b= self.chzzk_video.loc[chzzkID, 'VOD_json'].values()
        if (publishDate <= self.chzzk_video.loc[chzzkID, 'VOD_json']["publishDate"] or 
            videoNo in self.chzzk_video.loc[chzzkID, 'VOD_json'].values()):
            return

        # 썸네일 URL 검증
        if not thumbnailImageUrl or "https://video-phinf.pstatic.net" not in thumbnailImageUrl:
            await asyncio.sleep(1)
            return

        # 비디오 데이터 처리 및 저장
        json_data = self.getChzzk_video_json(chzzkID, stateData)
        self.data.video_alarm_List.append((chzzkID, json_data, videoTitle))
        await self.chzzk_saveVideoData(chzzkID, videoNo, videoTitle, publishDate)
 
    async def post_chzzk_video(self):
        def ifAlarm(discordWebhookURL):
            return (self.userStateData["치지직 VOD"][discordWebhookURL] and 
                    self.chzzkIDList.loc[chzzkID, 'channelName'] in self.userStateData["치지직 VOD"][discordWebhookURL])
        
        def make_list_of_urls(json_data):
            if self.DO_TEST:
                return [(environ['errorPostBotURL'], json_data)]
                # return []

            return [
                (discordWebhookURL, json_data)
                for discordWebhookURL in self.userStateData['discordURL']
                if ifAlarm(discordWebhookURL)
            ]
        
        try:
            if not self.data.video_alarm_List:
                return
            
            chzzkID, json_data, videoTitle = self.data.video_alarm_List.pop(0)
            channel_name = self.chzzkIDList.loc[chzzkID, 'channelName']
            print(f"{datetime.now()} VOD upload {channel_name} {videoTitle}")

            asyncio.create_task(DiscordWebhookSender().send_messages(make_list_of_urls(json_data)))
            # await async_post_message(make_list_of_urls(json_data))

        except Exception as e:
            asyncio.create_task(DiscordWebhookSender._log_error(f"postLiveMSG {e}"))
            self.data.video_alarm_List.clear()

    def getChzzk_video_json(self, chzzkID, stateData):
        videoNo, videoTitle, publishDate, thumbnailImageUrl, videoCategoryValue = self.getChzzkState(stateData)
        
        videoTitle = "|" + (videoTitle if videoTitle != " " else "                                                  ") + "|"
        
        channel_data = self.chzzkIDList.loc[chzzkID]
        username = channel_data['channelName']
        avatar_url = channel_data['channel_thumbnail']
        video_url = f"https://chzzk.naver.com/{channel_data['channel_code']}/video"
        
        embed = {
            "color": 65443,
            "author": {
                "name": username,
                "url": video_url,
                "icon_url": avatar_url
            },
            "title": videoTitle,
            "url": f"https://chzzk.naver.com/video/{videoNo}",
            "description": f"{username} 치지직 영상 업로드!",
            "fields": [
                {"name": 'Category', "value": videoCategoryValue}
            ],
            "thumbnail": {"url": avatar_url},
            "image": {"url": thumbnailImageUrl},
            "footer": {
                "text": "Chzzk",
                "inline": True,
                "icon_url": iconLinkData.chzzk_icon
            },
            "timestamp": changeUTCtime(publishDate)
        }
        
        return {
            "username": f"[치지직 알림] {username}",
            "avatar_url": avatar_url,
            "embeds": [embed]
        }
    
    def getChzzkState(self, stateData):
        def get_started_at() -> str | None:
            if not data["publishDate"]:
                return None
            try:
                return datetime.fromisoformat(data["publishDate"]).isoformat()
            except ValueError:
                return None
            
        data = stateData["content"]["data"][0]
        return (
            data["videoNo"],
            data["videoTitle"],
            get_started_at(),
            data["thumbnailImageUrl"],
            data["videoCategoryValue"]
        )
    async def chzzk_saveVideoData(self, chzzkID, videoNo, videoTitle, publishDate): #save profile data
        idx = {chzzk: i for i, chzzk in enumerate(self.chzzk_video["channelID"])}
        supabase = create_client(environ['supabase_url'], environ['supabase_key'])
        for _ in range(3):
            try:
                chzzk_video_json = self.chzzk_video.loc[chzzkID, 'VOD_json']

                chzzk_video_json["videoTitle3"] = chzzk_video_json["videoTitle2"]
                chzzk_video_json["videoTitle2"] = chzzk_video_json["videoTitle1"]
                chzzk_video_json["videoTitle1"] = videoTitle

                chzzk_video_json["videoNo3"] 	= chzzk_video_json["videoNo2"]
                chzzk_video_json["videoNo2"] 	= chzzk_video_json["videoNo1"]
                chzzk_video_json["videoNo1"] 	= videoNo

                chzzk_video_json["publishDate"] = publishDate

                
                supabase.table('chzzk_video').upsert({
                    "idx": idx[chzzkID],
                    'VOD_json': self.chzzk_video.loc[chzzkID, 'VOD_json']
                }).execute()
                break
            except Exception as e:
                asyncio.create_task(DiscordWebhookSender._log_error(f"error saving profile data {e}"))
                await asyncio.sleep(0.5)
