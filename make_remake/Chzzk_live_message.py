import base
import asyncio
from json import loads
from datetime import datetime
from os import remove, environ
from requests import post, get
from urllib.request import urlretrieve
from discord_webhook_sender import DiscordWebhookSender

class chzzk_live_message():
	def __init__(self, init_var):
		self.init = init_var
		self.DO_TEST = init_var.DO_TEST
		self.userStateData = init_var.userStateData
		self.chzzkIDList = init_var.chzzkIDList
		self.chzzk_titleData = init_var.chzzk_titleData
		self.chzzk_id = ""
		self.data = base.afreecaLiveData()

	async def chzzk_liveMsg(self):
		await self.addMSGList()
		await self.postLiveMSG()

	async def addMSGList(self):
		for _ in range(10):
			try:
				list_of_offState_chzzkID = await base.get_message(self.getChzzkDataList(), "chzzk")
				break
			except:
				await asyncio.sleep(0.1)
		try:
			for offState_chzzkID in list_of_offState_chzzkID:
				for offState, chzzkID, should_process in offState_chzzkID:
					if not should_process: continue

					if base.if_after_time(self.data.change_title_time) and self.chzzk_titleData.loc[chzzkID,'title2'] != self.chzzk_titleData.loc[chzzkID,'title1']:
						self.chzzk_titleData.loc[chzzkID,'title2'] = self.chzzk_titleData.loc[chzzkID,'title1']
						
					if offState["code"] != 200: continue

					stream_data = base.chzzk_getChannelOffStateData(
						offState["content"], 
						self.chzzkIDList.loc[chzzkID, "channel_code"], 
						self.chzzkIDList.loc[chzzkID, 'channel_thumbnail']
					)
					live, title, thumbnail_url = stream_data

					if self._should_process_online_status(live, title, chzzkID, offState):
						await self._handle_online_status(offState, chzzkID, live, title, thumbnail_url)

					elif self._should_process_offline_status(live, chzzkID, offState):
						await self._handle_offline_status(chzzkID, title, offState)

		except Exception as e:
			asyncio.create_task(DiscordWebhookSender()._log_error(f"testerror get stateData chzzk live{e}.{chzzkID}.{str(offState)}"))
			self.chat_json[self.data.chzzkID] = True

	def _should_process_online_status(self, live, title, chzzkID, offState):
		return ((self.checkStateTransition(live, chzzkID, offState, "OPEN") or 
		   (self.ifChangeTitle(title, chzzkID))) and
		   base.if_after_time(self.data.LiveCountEnd, sec = 15) )

	def _should_process_offline_status(self, live, chzzkID, offState):
		return (self.checkStateTransition(live, chzzkID, offState, "CLOSE") and 
		  base.if_after_time(self.data.LiveCountStart, sec = 15))
	
	async def _handle_online_status(self, offState, chzzkID, live, title, thumbnail_url):
		message = self.getMessage(live, chzzkID, offState)
		json = await self.getOnAirJson(chzzkID, message, offState, thumbnail_url, title, live)

		self.onLineTime(offState, chzzkID, message)
		self.onLineTitle(title, chzzkID, message)

		old_title = self.chzzk_titleData.loc[chzzkID,'title2']
		self.data.livePostList.append((chzzkID, message, title, old_title, json))

		await base.save_airing_data(self.init, 'chzzk', chzzkID)
		await base.save_profile_data(self.init, 'chzzk', chzzkID)

		if message == "뱅온!": 
			self.data.LiveCountStart = datetime.now().isoformat()
		self.data.change_title_time = datetime.now().isoformat()

	async def _handle_offline_status(self, chzzkID, title, offState):
		message = "뱅종"
		json = await self.getOffJson(chzzkID, offState, message)

		self.offLineTitle(chzzkID)
		self.offLineTime(chzzkID, offState)
		self.data.livePostList.append((chzzkID, message, title, None, json))

		await base.save_airing_data(self.init, 'chzzk', chzzkID)
		self.data.LiveCountEnd = datetime.now().isoformat()
		self.data.change_title_time = datetime.now().isoformat()
	
	async def postLiveMSG(self):
		try:
			if not self.data.livePostList: 
				return
			chzzkID, message, title, old_title, json = self.data.livePostList.pop(0)
			channel_name = self.chzzkIDList.loc[chzzkID, 'channelName']

			if message in ["뱅온!", "방제 변경"]:
				print(f"{datetime.now()} onLine {channel_name} {message}")
				if message == "방제 변경":
					print(f"{datetime.now()} 이전 방제: {old_title}")
					print(f"{datetime.now()} 현재 방제: {title}")

				list_of_urls = self.make_online_list_of_urls(chzzkID, message, json)
				asyncio.create_task(DiscordWebhookSender().send_messages(list_of_urls))
				# asyncio.create_task(base.async_post_message(list_of_urls))
				
			if message in ["뱅종"]:
				print(f"{datetime.now()} offLine {channel_name}")
				list_of_urls = self.make_offline_list_of_urls(chzzkID, json)
				asyncio.create_task(DiscordWebhookSender().send_messages(list_of_urls))
				# asyncio.create_task(base.async_post_message(list_of_urls))
		except Exception as e:
			asyncio.create_task(DiscordWebhookSender()._log_error(f"postLiveMSG {e}"))
			self.data.livePostList.clear()
	
	def getChzzkDataList(self):
		headers = base.getChzzkHeaders()
		cookie = base.getChzzkCookie()

		return [[(chzzk_getLink(self.chzzkIDList.loc[chzzkID, "channel_code"]), headers, cookie), chzzkID] for chzzkID in self.chzzkIDList["channelID"]]

	def make_online_list_of_urls(self, chzzkID, message, json):
		if self.DO_TEST:
			return [(environ['errorPostBotURL'], json)]

		return [
			(discordWebhookURL, json) 
			for discordWebhookURL in self.userStateData['discordURL']
			if self.ifAlarm(discordWebhookURL, chzzkID, message)
		]

	def make_offline_list_of_urls(self, chzzkID, json):
		if self.DO_TEST: return [(environ['errorPostBotURL'], json)]
		else: return [(discordWebhookURL, json) for discordWebhookURL in self.userStateData['discordURL'] if self.ifOffAlarm(chzzkID, discordWebhookURL)]

	def onLineTitle(self, title, chzzkID, message):
		if message == "뱅온!":
			self.chzzk_titleData.loc[chzzkID, 'live_state'] = "OPEN"
		self.chzzk_titleData.loc[chzzkID,'title2'] = self.chzzk_titleData.loc[chzzkID,'title1']
		self.chzzk_titleData.loc[chzzkID,'title1'] = title

	def onLineTime(self, offState, chzzkID, message):
		if message != "뱅온!":
			return
		self.chzzk_titleData.loc[chzzkID,'update_time'] = self.getStarted_at(offState,"openDate")

	def offLineTitle(self, chzzkID):
		self.chzzk_titleData.loc[chzzkID, 'live_state'] = "CLOSE"

	def offLineTime(self, chzzkID, offState):
		self.chzzk_titleData.loc[chzzkID,'update_time'] = self.getStarted_at(offState,"closeDate")

	def ifAlarm(self, discordWebhookURL, chzzkID, message):#if user recv to online Alarm
		if message == "뱅온!":
			return self.userStateData["뱅온 알림"][discordWebhookURL] and self.chzzkIDList.loc[chzzkID, 'channelName'] in self.userStateData["뱅온 알림"][discordWebhookURL]
		else:
			return self.userStateData[f"방제 변경 알림"][discordWebhookURL] and self.chzzkIDList.loc[chzzkID, 'channelName'] in self.userStateData[f"방제 변경 알림"][discordWebhookURL]

	def ifChangeTitle(self, title, chzzkID):
		return title not in [str(self.chzzk_titleData.loc[chzzkID,'title1']), str(self.chzzk_titleData.loc[chzzkID,'title2'])]

	def ifOffAlarm(self, chzzkID, discordWebhookURL):
		return self.userStateData["방종 알림"][discordWebhookURL] and self.chzzkIDList.loc[chzzkID, 'channelName'] in self.userStateData["방종 알림"][discordWebhookURL]

	async def getOnAirJson(self, chzzkID, message, offState, thumbnail_url, title, live):
		url = self.getURL(self.chzzkIDList.loc[chzzkID, "channel_code"])
		self.chzzkIDList.loc[chzzkID, 'channel_thumbnail'] = thumbnail_url
		
		if live == "CLOSE":
			return self.get_offState_change_title_json(chzzkID, message, title, url)
		
		started_at, viewer_count, thumbnail = await self.getJsonVars(chzzkID, offState, message)
		
		if message == "뱅온!":
			return self.get_online_state_json(
				chzzkID, message, title, url, 
				started_at, thumbnail, viewer_count
			)
		
		return self.get_online_titleChange_state_json(
			chzzkID, message, title, url,
			started_at, thumbnail, viewer_count
		)

	async def getJsonVars(self, chzzkID, offState, message):
		for count in range(20):
			started_at = self.getStarted_at(offState, "openDate")
			viewer_count = self.getViewer_count(offState)
			time_difference = (datetime.now() - datetime.fromisoformat(self.chzzk_titleData.loc[chzzkID, 'update_time'])).total_seconds()
			if message == "뱅온!" or self.chzzk_titleData.loc[chzzkID, 'live_state'] == "CLOSE" or time_difference < 15: 
				thumbnail = ""
				break

			thumbnail = self.getThumbnail(chzzkID, offState)
			if thumbnail is None:
				print(f"{datetime.now()} wait make thumbnail1 {count}")
				await asyncio.sleep(0.05)
				continue
			break

		else: thumbnail = ""
		
		if 'liveImageUrl' in offState.get('content', {}) and offState['content']['liveImageUrl']: 
			self.chzzk_titleData.loc[chzzkID,'channelURL'] = self.getImage(offState)
		return started_at, viewer_count, thumbnail

	def get_online_state_json(self, chzzkID, message, title, url, started_at, thumbnail, viewer_count):
		return {"username": self.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": self.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
				"embeds": [
					{"color": int(self.chzzkIDList.loc[chzzkID, 'channel_color']),
					"fields": [
						{"name": "방제", "value": title, "inline": True},
						# {"name": ':busts_in_silhouette: 시청자수',
						# "value": viewer_count, "inline": True}
						],
					"title":  f"{self.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
				"url": url,
				# "image": {"url": thumbnail},
				"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
				"timestamp": base.changeUTCtime(started_at)}]}

	def get_online_titleChange_state_json(self, chzzkID, message, title, url, started_at, thumbnail, viewer_count):
		return {"username": self.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": self.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
		"embeds": [
			{"color": int(self.chzzkIDList.loc[chzzkID, 'channel_color']),
			"fields": [
				{"name": "방제", "value": title, "inline": True},
				{"name": ':busts_in_silhouette: 시청자수',
				"value": viewer_count, "inline": True}
				],
			"title":  f"{self.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
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

	def get_offState_change_title_json(self, chzzkID, message, title, url):
		return {"username": self.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": self.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
				"embeds": [
					{"color": int(self.chzzkIDList.loc[chzzkID, 'channel_color']),
					"fields": [
						{"name": "이전 방제", "value": str(self.chzzk_titleData.loc[chzzkID,'title1']), "inline": True},
						{"name": "현재 방제", "value": title, "inline": True}],
					"title":  f"{self.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
				"url": url}]}

	async def getOffJson(self, chzzkID, offState, message):
		started_at = self.getStarted_at(offState,"closeDate")
		_, _, thumbnail = await self.getJsonVars(chzzkID, offState, message)
		return {"username": self.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": self.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
				"embeds": [
					{"color": int(self.chzzkIDList.loc[chzzkID, 'channel_color']),
					"title":  self.chzzkIDList.loc[chzzkID, 'channelName'] +" 방송 종료\n",
				"image": {"url": thumbnail},
				"footer": { "text": f"방종 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
				"timestamp": base.changeUTCtime(started_at)}]}

	def getURL(self, chzzkID): 
		return f'https://chzzk.naver.com/live/{chzzkID}'

	def getStarted_at(self, state_data, status: str): 
		time_str = state_data['content'][status]
		if not time_str: 
			return None
		try:
			time = datetime(int(time_str[:4]),int(time_str[5:7]),int(time_str[8:10]),int(time_str[11:13]),int(time_str[14:16]),int(time_str[17:19]))
			return time.isoformat()
		except ValueError:
			return None

	def getViewer_count(self, stateData):
		return stateData['content']['concurrentUserCount']

	def getMessage(self, live: str, chzzkID: str, offState) -> str: 
		return "뱅온!" if (self.checkStateTransition(live, chzzkID, offState, "OPEN")) else "방제 변경"

	def getThumbnail(self, chzzkID: str, stateData): #thumbnail shape do transformation able to send to discord
		try:
			if stateData['content']['liveImageUrl'] is not None:
				self.saveImage(stateData)
				file = {'file'      : open("explain.png", 'rb')}
				data = {"username"  : self.chzzkIDList.loc[chzzkID, 'channelName'],
						"avatar_url": self.chzzkIDList.loc[chzzkID, 'channel_thumbnail']}
				thumbnail  = post(environ['recvThumbnailURL'], files=file, data=data, timeout=3)
				try: remove('explain.png')
				except: pass
				frontIndex = thumbnail.text.index('"proxy_url"')
				thumbnail  = thumbnail.text[frontIndex:]
				frontIndex = thumbnail.index('https://media.discordap')
				return thumbnail[frontIndex:thumbnail.index(".png") + 4]
			return None
		except Exception as e:
			asyncio.create_task(DiscordWebhookSender()._log_error(f"{datetime.now()} wait make thumbnail2 {e}"))
			return None


	def saveImage(self, stateData): urlretrieve(self.getImage(stateData), "explain.png") # save thumbnail image to png

	def getImage(self, stateData) -> str:
		link = stateData['content']['liveImageUrl']
		link = link.replace("{type", "")
		link = link.replace("}.jpg", "0.jpg")
		return link

	def checkStateTransition(self, live: str, chzzkID: str, stateData, target_state: str):
		if live != target_state or self.chzzk_titleData.loc[chzzkID, 'live_state'] != ("CLOSE" if target_state == "OPEN" else "OPEN"):
			return False
		return self.getStarted_at(stateData, ("openDate" if target_state == "OPEN" else "closeDate")) > self.chzzk_titleData.loc[chzzkID, 'update_time']
		
def chzzk_getLink(chzzkID: str): 
	return f"https://api.chzzk.naver.com/service/v2/channels/{chzzkID}/live-detail"