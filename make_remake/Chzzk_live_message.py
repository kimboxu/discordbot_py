import base
import asyncio
from json import loads
from datetime import datetime
from os import remove, environ
from requests import post, get
from urllib.request import urlretrieve

class chzzk_live_message():

	async def chzzk_liveMsg(self, init: base.initVar, chzzkLive: base.chzzkLiveData):
		await self.addMSGList(init, chzzkLive)
		await self.postLiveMSG(init, chzzkLive)

	async def addMSGList(self, init: base.initVar, chzzkLive: base.chzzkLiveData):
		for _ in range(10):
			try:
				list_of_offState_chzzkID = await base.get_message(self.getChzzkDataList(init), "chzzk")
				break
			except:
				await asyncio.sleep(0.1)
		try:
			for offState_chzzkID in list_of_offState_chzzkID:
				for offState, chzzkID, should_process in offState_chzzkID:
					if not should_process: continue

					if base.if_after_time(chzzkLive.change_title_time) and init.chzzk_titleData.loc[chzzkID,'title2'] != init.chzzk_titleData.loc[chzzkID,'title1']:
						init.chzzk_titleData.loc[chzzkID,'title2'] = init.chzzk_titleData.loc[chzzkID,'title1']
						
					if offState["code"] != 200: continue

					stream_data = base.chzzk_getChannelOffStateData(
						offState["content"], 
						init.chzzkIDList.loc[chzzkID, "channel_code"], 
						init.chzzkIDList.loc[chzzkID, 'channel_thumbnail']
					)
					live, title, thumbnail_url = stream_data

					if self._should_process_online_status(init, chzzkLive, live, title, chzzkID, offState):
						await self._handle_online_status(init, chzzkLive, offState, chzzkID, live, title, thumbnail_url)

					elif self._should_process_offline_status(init, chzzkLive, live, chzzkID, offState):
						await self._handle_offline_status(init, chzzkLive, chzzkID, title, offState)

		except Exception as e:
			base.errorPost(f"testerror get stateData chzzk live{e}.{chzzkID}.{str(offState)}")
			if str(offState).find("error") != -1:
				len(1)

	def _should_process_online_status(self, init: base.initVar, chzzkLive: base.chzzkLiveData, live, title, chzzkID, offState):
		return ((self.checkStateTransition(init, live, chzzkID, offState, "OPEN") or 
		   (self.ifChangeTitle(init, title, chzzkID))) and
		   base.if_after_time(chzzkLive.LiveCountEnd, sec = 15) )

	def _should_process_offline_status(self, init: base.initVar, chzzkLive: base.chzzkLiveData, live, chzzkID, offState):
		return (self.checkStateTransition(init, live, chzzkID, offState, "CLOSE") and 
		  base.if_after_time(chzzkLive.LiveCountStart, sec = 15))
	
	async def _handle_online_status(self, init: base.initVar, chzzkLive: base.chzzkLiveData, offState, chzzkID, live, title, thumbnail_url):
		message = self.getMessage(init, live, chzzkID, offState)
		json = await self.getOnAirJson(init, chzzkID, message, offState, thumbnail_url, title, live)

		self.onLineTime(init, offState, chzzkID, message)
		self.onLineTitle(init, title, chzzkID, message)

		old_title = init.chzzk_titleData.loc[chzzkID,'title2']
		chzzkLive.livePostList.append((chzzkID, message, title, old_title, json))

		await base.save_airing_data(init, 'chzzk', chzzkID)
		await base.save_profile_data(init, 'chzzk', chzzkID)

		if message == "뱅온!": 
			chzzkLive.LiveCountStart = datetime.now().isoformat()
		chzzkLive.change_title_time = datetime.now().isoformat()

	async def _handle_offline_status(self, init: base.initVar, chzzkLive: base.chzzkLiveData, chzzkID, title, offState):
		message = "뱅종"
		json = await self.getOffJson(init, chzzkID, offState, message)

		self.offLineTitle(init, chzzkID)
		self.offLineTime(init, chzzkID, offState)
		chzzkLive.livePostList.append((chzzkID, message, title, None, json))

		await base.save_airing_data(init, 'chzzk', chzzkID)
		chzzkLive.LiveCountEnd = datetime.now().isoformat()
		chzzkLive.change_title_time = datetime.now().isoformat()
	
	async def postLiveMSG(self, init: base.initVar, chzzkLive: base.chzzkLiveData):
		try:
			if not chzzkLive.livePostList: 
				return
			chzzkID, message, title, old_title, json = chzzkLive.livePostList.pop(0)
			channel_name = init.chzzkIDList.loc[chzzkID, 'channelName']

			if message in ["뱅온!", "방제 변경"]:
				print(f"{datetime.now()} onLine {channel_name} {message}")
				if message == "방제 변경":
					print(f"{datetime.now()} 이전 방제: {old_title}")
					print(f"{datetime.now()} 현재 방제: {title}")

				list_of_urls = self.make_online_list_of_urls(init, chzzkID, message, json)
				asyncio.create_task(base.async_post_message(list_of_urls))
				
			if message in ["뱅종"]:
				print(f"{datetime.now()} offLine {channel_name}")
				list_of_urls = self.make_offline_list_of_urls(init, chzzkID, json)
				asyncio.create_task(base.async_post_message(list_of_urls))
		except Exception as e:
			base.errorPost(f"postLiveMSG {e}")
			chzzkLive.livePostList.clear()
	
	def getChzzkDataList(self, init: base.initVar):
		headers = base.getChzzkHeaders()
		cookie = base.getChzzkCookie()

		return [[(chzzk_getLink(init.chzzkIDList.loc[chzzkID, "channel_code"]), headers, cookie), chzzkID] for chzzkID in init.chzzkIDList["channelID"]]

	def make_online_list_of_urls(self, init: base.initVar, chzzkID, message, json):
		if init.DO_TEST:
			return [(environ['errorPostBotURL'], json)]

		return [
			(discordWebhookURL, json) 
			for discordWebhookURL in init.userStateData['discordURL']
			if self.ifAlarm(init, discordWebhookURL, chzzkID, message)
		]

	def make_offline_list_of_urls(self, init: base.initVar, chzzkID, json):
		if init.DO_TEST: return [(environ['errorPostBotURL'], json)]
		else: return [(discordWebhookURL, json) for discordWebhookURL in init.userStateData['discordURL'] if self.ifOffAlarm(init, chzzkID, discordWebhookURL)]

	def onLineTitle(self, init: base.initVar, title, chzzkID, message):
		if message == "뱅온!":
			init.chzzk_titleData.loc[chzzkID, 'live_state'] = "OPEN"
		init.chzzk_titleData.loc[chzzkID,'title2'] = init.chzzk_titleData.loc[chzzkID,'title1']
		init.chzzk_titleData.loc[chzzkID,'title1'] = title

	def onLineTime(self, init: base.initVar, offState, chzzkID, message):
		if message != "뱅온!":
			return
		init.chzzk_titleData.loc[chzzkID,'update_time'] = self.getStarted_at(offState,"openDate")

	def offLineTitle(self, init: base.initVar, chzzkID):
		init.chzzk_titleData.loc[chzzkID, 'live_state'] = "CLOSE"

	def offLineTime(self, init: base.initVar, chzzkID, offState):
		init.chzzk_titleData.loc[chzzkID,'update_time'] = self.getStarted_at(offState,"closeDate")

	def ifAlarm(self, init: base.initVar, discordWebhookURL, chzzkID, message):#if user recv to online Alarm
		if message == "뱅온!":
			return init.userStateData["뱅온 알림"][discordWebhookURL] and init.chzzkIDList.loc[chzzkID, 'channelName'] in init.userStateData["뱅온 알림"][discordWebhookURL]
		else:
			return init.userStateData[f"방제 변경 알림"][discordWebhookURL] and init.chzzkIDList.loc[chzzkID, 'channelName'] in init.userStateData[f"방제 변경 알림"][discordWebhookURL]

	def ifChangeTitle(self, init: base.initVar, title, chzzkID):
		return title not in [str(init.chzzk_titleData.loc[chzzkID,'title1']), str(init.chzzk_titleData.loc[chzzkID,'title2'])]

	def ifOffAlarm(self, init: base.initVar, chzzkID, discordWebhookURL):
		return init.userStateData["방종 알림"][discordWebhookURL] and init.chzzkIDList.loc[chzzkID, 'channelName'] in init.userStateData["방종 알림"][discordWebhookURL]

	async def getOnAirJson(self, init: base.initVar, chzzkID, message, offState, thumbnail_url, title, live):
		url = self.getURL(init.chzzkIDList.loc[chzzkID, "channel_code"])
		init.chzzkIDList.loc[chzzkID, 'channel_thumbnail'] = thumbnail_url
		
		if live == "CLOSE":
			return self.get_offState_change_title_json(init, chzzkID, message, title, url)
		
		started_at, viewer_count, thumbnail = await self.getJsonVars(init, chzzkID, offState, message)
		
		if message == "뱅온!":
			return self.get_online_state_json(
				init, chzzkID, message, title, url, 
				started_at, thumbnail, viewer_count
			)
		
		return self.get_online_titleChange_state_json(
			init, chzzkID, message, title, url,
			started_at, thumbnail, viewer_count
		)

	async def getJsonVars(self, init: base.initVar, chzzkID, offState, message):
		for count in range(20):
			started_at = self.getStarted_at(offState, "openDate")
			viewer_count = self.getViewer_count(offState)
			time_difference = (datetime.now() - datetime.fromisoformat(init.chzzk_titleData.loc[chzzkID, 'update_time'])).total_seconds()
			if message == "뱅온!" or init.chzzk_titleData.loc[chzzkID, 'live_state'] == "CLOSE" or time_difference < 15: 
				thumbnail = ""
				break

			thumbnail = self.getThumbnail(init, chzzkID, offState)
			if thumbnail is None:
				print(f"{datetime.now()} wait make thumbnail1 {count}")
				await asyncio.sleep(0.05)
				continue
			break

		else: thumbnail = ""
		
		if 'liveImageUrl' in offState.get('content', {}) and offState['content']['liveImageUrl']: 
			init.chzzk_titleData.loc[chzzkID,'channelURL'] = self.getImage(offState)
		return started_at, viewer_count, thumbnail

	def get_online_state_json(self, init: base.initVar, chzzkID, message, title, url, started_at, thumbnail, viewer_count):
		return {"username": init.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": init.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
				"embeds": [
					{"color": int(init.chzzkIDList.loc[chzzkID, 'channel_color']),
					"fields": [
						{"name": "방제", "value": title, "inline": True},
						# {"name": ':busts_in_silhouette: 시청자수',
						# "value": viewer_count, "inline": True}
						],
					"title":  f"{init.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
				"url": url,
				# "image": {"url": thumbnail},
				"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
				"timestamp": base.changeUTCtime(started_at)}]}

	def get_online_titleChange_state_json(self, init: base.initVar, chzzkID, message, title, url, started_at, thumbnail, viewer_count):
		return {"username": init.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": init.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
		"embeds": [
			{"color": int(init.chzzkIDList.loc[chzzkID, 'channel_color']),
			"fields": [
				{"name": "방제", "value": title, "inline": True},
				{"name": ':busts_in_silhouette: 시청자수',
				"value": viewer_count, "inline": True}
				],
			"title":  f"{init.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
		"url": url,
		"image": {"url": thumbnail},
		"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
		"timestamp": base.changeUTCtime(started_at)}]}
		# return {"username": init.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": init.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
		# 		"embeds": [
		# 			{"color": int(init.chzzkIDList.loc[chzzkID, 'channel_color']),
		# 			"fields": [
		# 				{"name": "이전 방제", "value": str(init.chzzk_titleData.loc[chzzkID,'title1']), "inline": True},
		# 				{"name": "현재 방제", "value": title, "inline": True},
		# 				{"name": ':busts_in_silhouette: 시청자수',
		# 				"value": viewer_count, "inline": True}],
		# 			"title":  f"{init.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
		# 		"url": url,
		# 		"image": {"url": thumbnail},
		# 		"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
		# 		"timestamp": base.changeUTCtime(started_at)}]}

	def get_offState_change_title_json(self, init: base.initVar, chzzkID, message, title, url):
		return {"username": init.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": init.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
				"embeds": [
					{"color": int(init.chzzkIDList.loc[chzzkID, 'channel_color']),
					"fields": [
						{"name": "이전 방제", "value": str(init.chzzk_titleData.loc[chzzkID,'title1']), "inline": True},
						{"name": "현재 방제", "value": title, "inline": True}],
					"title":  f"{init.chzzkIDList.loc[chzzkID, 'channelName']} {message}\n",
				"url": url}]}

	async def getOffJson(self, init: base.initVar, chzzkID, offState, message):
		started_at = self.getStarted_at(offState,"closeDate")
		_, _, thumbnail = await self.getJsonVars(init, chzzkID, offState, message)
		return {"username": init.chzzkIDList.loc[chzzkID, 'channelName'], "avatar_url": init.chzzkIDList.loc[chzzkID, 'channel_thumbnail'],
				"embeds": [
					{"color": int(init.chzzkIDList.loc[chzzkID, 'channel_color']),
					"title":  init.chzzkIDList.loc[chzzkID, 'channelName'] +" 방송 종료\n",
				"image": {"url": thumbnail},
				"footer": { "text": f"방종 시간", "inline": True, "icon_url": base.iconLinkData().chzzk_icon },
				"timestamp": base.changeUTCtime(started_at)}]}

	def getChatFilterName(self, init: base.initVar, name):
		[channelName] = [init.chatFilter["channelName"][i] for i in range(len(list(init.chatFilter["channelID"]))) if init.chatFilter["channelID"][i] == name]
		return channelName

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

	def getMessage(self, init: base.initVar, live: str, chzzkID: str, offState) -> str: 
		return "뱅온!" if (self.checkStateTransition(init, live, chzzkID, offState, "OPEN")) else "방제 변경"

	def getThumbnail(self, init: base.initVar, chzzkID: str, stateData): #thumbnail shape do transformation able to send to discord
		try:
			if stateData['content']['liveImageUrl'] is not None:
				self.saveImage(stateData)
				file = {'file'      : open("explain.png", 'rb')}
				data = {"username"  : init.chzzkIDList.loc[chzzkID, 'channelName'],
						"avatar_url": init.chzzkIDList.loc[chzzkID, 'channel_thumbnail']}
				thumbnail  = post(environ['recvThumbnailURL'], files=file, data=data, timeout=3)
				try: remove('explain.png')
				except: pass
				frontIndex = thumbnail.text.index('"proxy_url"')
				thumbnail  = thumbnail.text[frontIndex:]
				frontIndex = thumbnail.index('https://media.discordap')
				return thumbnail[frontIndex:thumbnail.index(".png") + 4]
			return None
		except Exception as e:
			base.errorPost(f"{datetime.now()} wait make thumbnail2 {e}")
			return None


	def saveImage(self, stateData): urlretrieve(self.getImage(stateData), "explain.png") # save thumbnail image to png

	def getImage(self, stateData) -> str:
		link = stateData['content']['liveImageUrl']
		link = link.replace("{type", "")
		link = link.replace("}.jpg", "0.jpg")
		return link

	def checkStateTransition(self, init: base.initVar, live: str, chzzkID: str, stateData, target_state: str):
		if live != target_state or init.chzzk_titleData.loc[chzzkID, 'live_state'] != ("CLOSE" if target_state == "OPEN" else "OPEN"):
			return False
		return self.getStarted_at(stateData, ("openDate" if target_state == "OPEN" else "closeDate")) > init.chzzk_titleData.loc[chzzkID, 'update_time']
		
def chzzk_getLink(chzzkID: str): 
	return f"https://api.chzzk.naver.com/service/v2/channels/{chzzkID}/live-detail"