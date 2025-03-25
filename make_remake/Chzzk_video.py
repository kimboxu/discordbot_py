import asyncio
from os import environ
from datetime import datetime
from base import getChzzkHeaders, getChzzkCookie, chzzk_saveVideoData, async_errorPost, changeUTCtime, async_post_message, get_message, chzzkVideoData, iconLinkData

class chzzk_video:
    async def chzzk_video(self, init, chzzkVideo: chzzkVideoData):
        await self.check_chzzk_video(init, chzzkVideo)
        await self.post_chzzk_video(init, chzzkVideo)

    async def check_chzzk_video(self, init, chzzkVideo: chzzkVideoData):
        def getChzzkDataList():
            headers = getChzzkHeaders()
            cookie = getChzzkCookie()
        
            return [
                [(f"https://api.chzzk.naver.com/service/v1/channels/{init.chzzkIDList.loc[chzzkID, 'channel_code']}/videos", headers, cookie), chzzkID] 
                for chzzkID in init.chzzkIDList["channelID"]
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
                    await self._process_video_data(init, chzzkVideo, stateData, chzzkID)
                except Exception as e:
                    asyncio.create_task(async_errorPost(f"error get stateData chzzk video.{chzzkID}.{e}."))

    def _should_process_video(self, stateData, should_process):
        return should_process and stateData["code"] == 200

    async def _process_video_data(self, init, chzzkVideo: chzzkVideoData, stateData, chzzkID):
        videoNo, videoTitle, publishDate, thumbnailImageUrl, _ = self.getChzzkState(stateData)
        
        # 이미 처리된 비디오 건너뛰기
        a=init.chzzk_video.loc[chzzkID, 'VOD_json']["publishDate"]
        b= init.chzzk_video.loc[chzzkID, 'VOD_json'].values()
        if (publishDate <= init.chzzk_video.loc[chzzkID, 'VOD_json']["publishDate"] or 
            videoNo in init.chzzk_video.loc[chzzkID, 'VOD_json'].values()):
            return

        # 썸네일 URL 검증
        if not thumbnailImageUrl or "https://video-phinf.pstatic.net" not in thumbnailImageUrl:
            await asyncio.sleep(1)
            return

        # 비디오 데이터 처리 및 저장
        json_data = self.getChzzk_video_json(init, chzzkID, stateData)
        chzzkVideo.video_alarm_List.append((chzzkID, json_data, videoTitle))
        await chzzk_saveVideoData(init, chzzkID, videoNo, videoTitle, publishDate)
 
    async def post_chzzk_video(self, init, chzzkVideo: chzzkVideoData):
        def ifAlarm(discordWebhookURL):
            return (init.userStateData["치지직 VOD"][discordWebhookURL] and 
                    init.chzzkIDList.loc[chzzkID, 'channelName'] in init.userStateData["치지직 VOD"][discordWebhookURL])
        
        def make_list_of_urls(json_data):
            if init.DO_TEST:
                return [(environ['errorPostBotURL'], json_data)]
                # return []

            return [
                (discordWebhookURL, json_data)
                for discordWebhookURL in init.userStateData['discordURL']
                if ifAlarm(discordWebhookURL)
            ]
        
        try:
            if not chzzkVideo.video_alarm_List:
                return
            
            chzzkID, json_data, videoTitle = chzzkVideo.video_alarm_List.pop(0)
            channel_name = init.chzzkIDList.loc[chzzkID, 'channelName']
            print(f"{datetime.now()} VOD upload {channel_name} {videoTitle}")

            await async_post_message(make_list_of_urls(json_data))

        except Exception as e:
            asyncio.create_task(async_errorPost(f"postLiveMSG {e}"))
            chzzkVideo.video_alarm_List.clear()

    def getChzzk_video_json(self, init, chzzkID, stateData):
        videoNo, videoTitle, publishDate, thumbnailImageUrl, videoCategoryValue = self.getChzzkState(stateData)
        
        videoTitle = "|" + (videoTitle if videoTitle != " " else "                                                  ") + "|"
        
        channel_data = init.chzzkIDList.loc[chzzkID]
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