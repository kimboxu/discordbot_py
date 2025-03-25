import asyncio
import aiohttp
from os import environ
from requests import get
from datetime import datetime
from supabase import create_client, Client
from googleapiclient.discovery import build
from base import async_errorPost, subjectReplace, getChzzkHeaders, async_post_message, youtubeVideoData, iconLinkData, initVar
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from googleapiclient.errors import HttpError


from dataclasses import dataclass
from typing import List, Optional

@dataclass
class YouTubeVideo:
    video_title: str
    thumbnail_link: str
    publish_time: str
    video_link: str
    description: str = ""

@dataclass 
class YouTubeVideoBatch:
    videos: List[YouTubeVideo]
    
    def __getitem__(self, idx: int) -> YouTubeVideo:
        return self.videos[idx]
    
    def sort_by_publish_time(self):
        def parse_time(time_str: str) -> datetime:
            return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ")
            
        self.videos.sort(
            key=lambda x: parse_time(x.publish_time),
            reverse=True
        )

class getYoutubeJsonData:
	async def fYoutube(self, init: initVar, youtubeVideo: youtubeVideoData):
		try:
			for _ in range(1):
				youtubeVideo.youtubeChannelIdx = int((youtubeVideo.youtubeChannelIdx + 1) % (init.youtubeData["idx"].max()+1))
				# youtubeChannelID = list(init.youtubeData["YoutubeChannelID"])[youtubeVideo.youtubeChannelIdx]
				youtubeChannelID = init.youtubeData["YoutubeChannelID"].iloc[youtubeVideo.youtubeChannelIdx]
		
				if youtubeChannelID not in youtubeVideo.video_count_check_dict:
					youtubeVideo.video_count_check_dict[youtubeChannelID] = 0

				json_responses = await self.ifYoutubeJson(init, youtubeVideo, youtubeChannelID)

				if json_responses[0] is not None:
					youtubeVideo.video_count_check_dict[youtubeChannelID] = 0

					for json_data in reversed(json_responses):
						if json_data is not None:
							await async_post_message(self.makeList_of_urls(init, json_data, youtubeChannelID))
							print(f'{datetime.now()} {json_data["username"]}: {json_data["embeds"][0]["title"]}')
							await asyncio.sleep(0.5)

					self.saveYoutubeData(init, youtubeVideo, youtubeChannelID) #save video count
			
		except Exception as e:
			if "RetryError" not in str(e):
				asyncio.create_task(async_errorPost(f"fYoutube {youtubeChannelID}: {str(e)}"))
			
	@retry(stop=stop_after_attempt(5), 
		wait=wait_exponential(multiplier=1, min=2, max=5),
		retry=retry_if_exception_type((asyncio.TimeoutError, ConnectionError)))
	async def ifYoutubeJson(self, init: initVar, youtubeVideo: youtubeVideoData, youtubeChannelID: str):
		try:
			youtube = build('youtube', 'v3', 
				   developerKey=youtubeVideo.developerKeyDict[youtubeVideo.TFdeveloperKey], 
				   cache_discovery=False)
			youtubeVideo.TFdeveloperKey = (youtubeVideo.TFdeveloperKey + 1) % len(youtubeVideo.developerKeyDict)
		
			try:
				channel_response = await asyncio.wait_for(
					asyncio.get_event_loop().run_in_executor(
						None,
						youtube.channels().list(
							part='statistics', 
							id=init.youtubeData.loc[youtubeChannelID, "channelCode"]
						).execute
					),
					timeout=3
				)
			except HttpError as e:
				if e.resp.status == 503:
					# 503 에러는 일시적인 서버 문제이므로 무시
					print(f"{datetime.now()} YouTube API 일시적 오류 (채널: {youtubeChannelID})")
					return None, None, None
				raise  # 다른 HTTP 에러는 그대로 발생
			except asyncio.TimeoutError:
				print(f"{datetime.now()} Channel response timeout for {youtubeChannelID}")
				return None, None, None
			except Exception as e:
				asyncio.create_task(async_errorPost(f"error channel_response {e}"))
				return None, None, None

			# 응답이 없거나 items가 비어있는 경우 처리
			if not channel_response or 'items' not in channel_response or not channel_response['items']:
				print(f"{datetime.now()} No valid response for channel {youtubeChannelID}")
				return None, None, None

			video_count = int(channel_response['items'][0]['statistics']['videoCount'])
			current_count = init.youtubeData.loc[youtubeChannelID, "videoCount"]

			if youtubeVideo.video_count_check_dict[youtubeChannelID] > 3:
				current_count += 1
				await self._update_video_count(init, youtubeVideo, youtubeChannelID, current_count)
				youtubeVideo.video_count_check_dict[youtubeChannelID] = 0

			try:
				if current_count < video_count:
					youtubeVideo.video_count_check_dict[youtubeChannelID] += 1
					self.get_youtube_thumbnail_url(init, youtubeChannelID)
					response = youtube.search().list(
						part="id,snippet", 
						channelId=init.youtubeData.loc[youtubeChannelID, "channelCode"], 
						order="date", 
						type="video", 
						maxResults=3
					).execute()

					newVideo, jsonList = await self.ifNewVideo(init, youtubeVideo, youtubeChannelID, response, video_count)
					return self._prepare_response(newVideo, jsonList)
				
				elif current_count > video_count:
					if video_count - current_count < 3:
						current_count -= 1
						await self._update_video_count(init, youtubeVideo, youtubeChannelID, current_count)
					# asyncio.create_task(async_errorPost(f"down video count {youtubeChannelID} {init.youtubeData.loc[youtubeChannelID, 'videoCount'] + 1} - {video_count}"))
				return None, None, None
			except asyncio.TimeoutError:
				raise
			except Exception as e:
				asyncio.create_task(async_errorPost(f"ifYoutubeJson3: {youtubeChannelID}.{str(e)}"))
				return None, None, None
				
		except Exception as e:
			asyncio.create_task(async_errorPost(f"ifYoutubeJson3: {youtubeChannelID}.{str(e)}"))
			return None, None, None
				
	async def _update_video_count(self, init: initVar, youtubeVideo: youtubeVideoData, youtubeChannelID: str, count: int):
		init.youtubeData.loc[youtubeChannelID, "videoCount"] = count
		supabase = create_client(environ['supabase_url'], environ['supabase_key'])
		supabase.table('youtubeData').upsert({
			"idx": youtubeVideo.youtubeChannelIdx,
			"videoCount": int(count)
		}).execute()
		await asyncio.sleep(0.05)

	def _prepare_response(self, newVideo: int, jsonList: list):
		if newVideo == 3:
			return jsonList[0], jsonList[1], jsonList[2]
		if newVideo == 2:
			return jsonList[0], jsonList[1], None
		if newVideo == 1:
			return jsonList[0], None, None
		return None, None, None

	async def ifNewVideo(self, init: initVar, youtubeVideo: youtubeVideoData, youtubeChannelID: str, response, video_count: int):
		try:
			newVideoNum = video_count - init.youtubeData.loc[youtubeChannelID, "videoCount"]
			tasks = [self.getYoutubeVars(response, i) for i in range(3)]
			videos = await asyncio.gather(*tasks)
			batch = YouTubeVideoBatch(videos=list(videos))
			batch.sort_by_publish_time()

			# 새로운 비디오 체크를 위한 두 조건 배열 생성
			TF1 = [i < newVideoNum for i in range(3)]
			TF2 = [video.video_link not in init.youtubeData.loc[youtubeChannelID, "oldVideo"].values() 
				for video in batch.videos[:3]]

			# 새로운 비디오 수 결정
			if all(TF1) and all(TF2):
				newVideo = 3
			elif all(TF1[:2]) and all(TF2[:2]):
				newVideo = 2
			elif TF1[0] and TF2[0] and newVideoNum == 1:
				newVideo = 1
			else:
				newVideo = 0

			# 비디오 설명 가져오기
			for num in range(newVideo):
				batch.videos[num].description = await self.getDescription(
					youtubeVideo,
					str(batch.videos[num].video_link).replace("https://www.youtube.com/watch?v=", "")
				)
				
			jsonList = [self.getYoutubeJson(init, youtubeChannelID, video) 
					for video in batch.videos[:3]]

			if newVideo > 0:
				# 데이터 업데이트
				init.youtubeData.loc[youtubeChannelID, "videoCount"] += newVideoNum
				init.youtubeData.loc[youtubeChannelID, "uploadTime"] = batch.videos[0].publish_time
				
				# oldVideo 링크 업데이트
				old_links = [str(init.youtubeData.loc[youtubeChannelID, "oldVideo"][f"link{i}"]) 
							for i in range(1, 6)]
				new_links = [video.video_link for video in batch.videos[:newVideo]]
				
				updated_links = new_links + old_links[:-newVideo]
				for i, link in enumerate(updated_links[:5], 1):
					init.youtubeData.loc[youtubeChannelID, "oldVideo"][f"link{i}"] = link

			return newVideo, jsonList

		except Exception as e:
			return 0, []

	def makeList_of_urls(self, init: initVar, json, youtubeChannelID: str) -> list:
		if init.DO_TEST: list_of_urls = [(environ['errorPostBotURL'], json)]
		else:
			list_of_urls = [(discordWebhookURL, json) for discordWebhookURL in init.userStateData['discordURL'] if self.ifYoutubeAlarm(init, discordWebhookURL, youtubeChannelID)]
		return list_of_urls

	async def getYoutubeVars(self, response, num) -> YouTubeVideo:
		title = subjectReplace(response['items'][num]['snippet']['title'])
		thumbnail = response['items'][num]['snippet']['thumbnails']["medium"]["url"].replace("mqdefault", "maxresdefault")
		
		# thumbnail URL 체크 로직
		async with aiohttp.ClientSession() as session:
			for i in range(10):
				if i == 4:
					thumbnail = response['items'][num]['snippet']['thumbnails']["medium"]["url"].replace("mqdefault", "sddefault")
				elif i == 9:
					thumbnail = response['items'][num]['snippet']['thumbnails']["medium"]["url"]
					
				async with session.get(thumbnail) as resp:
					if resp.status != 404:
						break
					await asyncio.sleep(0.05)
		
		publish_time = response['items'][num]['snippet']['publishTime']
		video_id = response['items'][num]['id']['videoId']
		video_link = f"https://www.youtube.com/watch?v={video_id}"
		
		return YouTubeVideo(
			video_title=title,
			thumbnail_link=thumbnail,
			publish_time=publish_time,
			video_link=video_link
		)
	
	async def getDescription(self, youtubeVideo: youtubeVideoData, video_id: str) -> str:
		try:
			youtube = build(
				'youtube', 
				'v3', 
				developerKey=youtubeVideo.developerKeyDict[youtubeVideo.TFdeveloperKey], 
				cache_discovery=False
			)
			
			result = await asyncio.wait_for(
				asyncio.get_running_loop().run_in_executor(
					None,
					lambda: youtube.videos().list(
						part='snippet',
						id=video_id
					).execute()
				),
				timeout=5
			)
			
			if not result.get('items'):
				return ""
				
			description = result['items'][0]['snippet']['description']
			return subjectReplace(description.split('\n')[0])
			
		except Exception as e:
			asyncio.create_task(async_errorPost(f"error youtube getDescription {e}"))
			return ""
		
	def getYoutubeJson(self, init: initVar, youtubeChannelID: str, video) -> dict:
		channelID = init.youtubeData.loc[youtubeChannelID, "channelID"]
		
		# 플랫폼별 ID 리스트를 딕셔너리로 관리
		platform_lists = {
			'chzzk': init.chzzkIDList,
			'afreeca': init.afreecaIDList,
			'twitch': init.twitchIDList
		}
		
		# 채널 정보 찾기
		username = None
		avatar_url = None
		for platform_list in platform_lists.values():
			try:
				username = platform_list.loc[channelID, 'channelName']
				avatar_url = platform_list.loc[channelID, 'channel_thumbnail']
				break
			except KeyError:
				continue
				
		if username is None or avatar_url is None:
			asyncio.create_task(async_errorPost(f"Channel information not found for channelID: {channelID}"))
			return
		
		youtube_data = init.youtubeData.loc[youtubeChannelID]
		
		return {
			"username": f" [유튜브 알림] {username}",
			"avatar_url": avatar_url,
			"embeds": [{
				"color": 16711680,
				"author": {
					"name": youtube_data['channelName'],
					"url": f"https://www.youtube.com/@{youtubeChannelID}",
					"icon_url": youtube_data['thumbnail_link']
				},
				"title": video.video_title,
				"url": video.video_link,
				"description": f"{youtube_data['channelName']} 유튜브 영상 업로드!",
				"fields": [{"name": 'Description', "value": video.description}],
				"thumbnail": {"url": youtube_data['thumbnail_link']},
				"image": {"url": video.thumbnail_link},
				"footer": {"text": "YouTube", "inline": True, "icon_url": iconLinkData().youtube_icon},
				"timestamp": video.publish_time
			}]
		}

	def saveYoutubeData(self, init: initVar, youtubeVideo: youtubeVideoData, youtubeChannelID: str):
		data = {
			"idx": youtubeVideo.youtubeChannelIdx,
			"videoCount": int(init.youtubeData.loc[youtubeChannelID, "videoCount"]),
			"uploadTime": init.youtubeData.loc[youtubeChannelID, "uploadTime"],
			"oldVideo": init.youtubeData.loc[youtubeChannelID, "oldVideo"],
			'thumbnail_link': init.youtubeData.loc[youtubeChannelID, 'thumbnail_link']
		}

		for _ in range(3):
			try:
				supabase = create_client(environ['supabase_url'], environ['supabase_key'])
				supabase.table('youtubeData').upsert(data).execute()
			except Exception as e:
				asyncio.create_task(async_errorPost(f"error saving youtube data {e}"))
		
	def ifYoutubeAlarm(self, init: initVar, discordWebhookURL, youtubeChannelID: str) -> bool:
		return (init.userStateData["유튜브 알림"][discordWebhookURL] and 
		  init.youtubeData.loc[youtubeChannelID, 'channelName'] in init.userStateData["유튜브 알림"][discordWebhookURL])
	
	def get_youtube_thumbnail_url(self, init: initVar, youtubeChannelID: str):
		response = get(f"https://www.youtube.com/@{youtubeChannelID}", 
                      headers=getChzzkHeaders())
		
		text = response.text
		start_idx = text.find("https://yt3.googleusercontent.com")
		end_str = "no-rj"
		end_idx = text[start_idx:].find(end_str)
		thumbnail_url = text[start_idx:start_idx + end_idx + len(end_str)]
		if 110 < len(thumbnail_url) < 150:
			init.youtubeData.loc[youtubeChannelID, 'thumbnail_link'] = thumbnail_url
