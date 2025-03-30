import base
import asyncio
from json import loads
from datetime import datetime
from requests import post, get
from os import remove, environ
from urllib.request import urlretrieve
from discord_webhook_sender import DiscordWebhookSender

class afreeca_live_message():
	def __init__(self, init_var: base.initVar):
		self.DO_TEST = init_var.DO_TEST
		self.userStateData = init_var.userStateData
		self.afreecaIDList = init_var.afreecaIDList
		self.afreeca_titleData = init_var.afreeca_titleData
		self.afreecaID = ""
		self.data = base.afreecaLiveData()

	async def afreeca_liveMsg(self):
		await self.addMSGList()
		await self.postLiveMSG()

	async def addMSGList(self):
		for _ in range(10):
			try:
				list_of_offState_afreecaID = await base.get_message(self.getAfreecaDataList(), "afreeca")
				break
			except:
				await asyncio.sleep(0.1)
		try:
			for offState_afreecaID in list_of_offState_afreecaID:
				for offState, user_id, should_process in offState_afreecaID:
					if not should_process: continue

					if base.if_after_time(self.data.change_title_time) and self.afreeca_titleData.loc[user_id,'title2'] != self.afreeca_titleData.loc[user_id,'title1']:
						self.afreeca_titleData.loc[user_id,'title2'] = self.afreeca_titleData.loc[user_id,'title1']
					
					if not self._is_valid_station_data(offState):
						continue

					stream_data = base.afreeca_getChannelOffStateData(
						offState,
						self.afreecaIDList.loc[user_id, "afreecaID"],
						self.afreecaIDList.loc[user_id, 'channel_thumbnail']
					)
					live, title, thumbnail_url = stream_data

					if self._should_process_online_status(live, title, user_id, offState):
						await self._handle_online_status(offState, user_id, live, title, thumbnail_url)

					elif self._should_process_offline_status(live, user_id):
						await self._handle_offline_status(user_id, title, thumbnail_url)

		except Exception as e:
			asyncio.create_task(DiscordWebhookSender()._log_error(f"error get stateData afreeca live .{user_id}.{e}.{offState}"))
			if  -1 not in [str(offState).find("Error"), str(offState).find("error")]:
				len(1)

	def _is_valid_station_data(self, offState):
		try:
			offState["station"]["user_id"]
			return True
		except:
			return False
			
	def _should_process_online_status(self, live, title, user_id, offState):
		return ((self.turnOnline(live, user_id, offState) or 
				(title and self.ifChangeTitle(title, user_id))) and 
				base.if_after_time(self.data.LiveCountEnd, sec = 15))

	def _should_process_offline_status(self, live, user_id):
		return (self.turnOffline(live, user_id) and
		  		base.if_after_time(self.data.LiveCountStart, sec = 15))

	async def _handle_online_status(self, offState, user_id, live, title, thumbnail_url):
		message = self.getMessage(live, user_id, offState)
		json = await self.getOnAirJson(user_id, message, thumbnail_url, title, offState)
		
		self.onLineTime(offState, user_id, message)
		self.onLineTitle(title, user_id, message)
		
		old_title = self.afreeca_titleData.loc[user_id,'title2']
		self.data.livePostList.append((user_id, message, title, old_title, json))
		
		await base.save_airing_data('afreeca', user_id)
		await base.save_profile_data('afreeca', user_id)
		
		self.data.LiveCountStart = datetime.now().isoformat()
		self.data.change_title_time = datetime.now().isoformat()

	async def _handle_offline_status(self, user_id, title, thumbnail_url):
		message = "뱅종"
		self.afreecaIDList.loc[user_id, 'channel_thumbnail'] = thumbnail_url
		json = self.getOffJson(user_id)
		
		self.offLineTitle(user_id)
		self.data.livePostList.append((user_id, message, title, None, json))
		
		await base.save_airing_data('afreeca', user_id)
		self.data.LiveCountEnd = datetime.now().isoformat()
		self.data.change_title_time = datetime.now().isoformat()
	
	async def postLiveMSG(self):
		try:
			if not self.data.livePostList:
				return
			user_id, message, title, old_title, json = self.data.livePostList.pop(0)
			channel_name = self.afreecaIDList.loc[user_id, 'channelName']

			if message in ["뱅온!", "방제 변경"]:
				print(f"{datetime.now()} onLine {channel_name} {message}")
				if message == "방제 변경":
					print(f"{datetime.now()} 이전 방제: {old_title}")
					print(f"{datetime.now()} 현재 방제: {title}")

				list_of_urls = self.make_online_list_of_urls(user_id, message, json)
				asyncio.create_task(DiscordWebhookSender().send_messages(list_of_urls))
				# asyncio.create_task(base.async_post_message(list_of_urls))
			if message in ["뱅종"]:
				print(f"{datetime.now()} offLine {channel_name}")
				list_of_urls = self.make_offline_list_of_urls(user_id, json)
				asyncio.create_task(DiscordWebhookSender().send_messages(list_of_urls))
				# asyncio.create_task(base.async_post_message(list_of_urls))
		except Exception as e:
			asyncio.create_task(DiscordWebhookSender()._log_error(f"postLiveMSG {e}"))
			self.data.livePostList.clear()

	def getAfreecaDataList(self):
		headers = base.getChzzkHeaders()

		return [[(afreeca_getLink(self.afreecaIDList.loc[user_id, "afreecaID"]), headers), user_id] for user_id in self.afreecaIDList["channelID"]]

	def make_online_list_of_urls(self, user_id, message, json):
		if self.DO_TEST:
			return [(environ['errorPostBotURL'], json)]
		
		return [
			(discordWebhookURL, json) 
			for discordWebhookURL in self.userStateData['discordURL']
			if self.ifAlarm(discordWebhookURL, user_id, message)
		]

	def make_offline_list_of_urls(self, user_id, json):
		if self.DO_TEST:
			return [(environ['errorPostBotURL'], json)]
		else:
			return [(discordWebhookURL, json) for discordWebhookURL in self.userStateData['discordURL'] if self.ifOffAlarm(user_id, discordWebhookURL)]

	def onLineTitle(self, title, user_id, message): #change title. state to online
		if message == "뱅온!": self.afreeca_titleData.loc[user_id,'live_state'] = "OPEN"
		self.afreeca_titleData.loc[user_id,'title2'] = self.afreeca_titleData.loc[user_id,'title1']
		self.afreeca_titleData.loc[user_id,'title1'] = title

	def onLineTime(self, offState, user_id, message): #change time. state to online
		if message == "뱅온!": 
			self.afreeca_titleData.loc[user_id,'update_time'] = self.getStarted_at(offState)

	def offLineTitle(self, user_id): self.afreeca_titleData.loc[user_id,'live_state'] = "CLOSE" # change state to offline  

	def offLineTime(self, user_id, offState): 
		self.afreeca_titleData.loc[user_id,'update_time'] = self.getStarted_at(offState) # change state to offline  

	def ifAlarm(self, discordWebhookURL, user_id, message): #if user recv to online Alarm
		if message == "뱅온!": 
			return self.userStateData["뱅온 알림"][discordWebhookURL] and self.afreecaIDList.loc[user_id, 'channelName'] in self.userStateData["뱅온 알림"][discordWebhookURL]
		else: 
			return self.userStateData[f"방제 변경 알림"][discordWebhookURL] and self.afreecaIDList.loc[user_id, 'channelName'] in self.userStateData[f"방제 변경 알림"][discordWebhookURL]

	def ifChangeTitle(self, title, user_id):
		return title not in [str(self.afreeca_titleData.loc[user_id,'title1']), str(self.afreeca_titleData.loc[user_id,'title2'])] #if title change
	
	def ifOffAlarm(self, user_id, discordWebhookURL):
		try: return self.afreecaIDList.loc[user_id, 'channelName'] in self.userStateData["방종 알림"][discordWebhookURL] #if offline Alarm
		except: return False

	async def getOnAirJson(self, user_id, message, thumbnail_url, title, offState):
		self.afreecaIDList.loc[user_id, 'channel_thumbnail'] = thumbnail_url
		started_at, thumbnail, url, viewer_count = await self.getJsonVars(user_id, offState)
		# if message=="뱅온!":
		return self.get_online_state_json(user_id, message, title, url, started_at, thumbnail, viewer_count)
		# return self.get_online_titleChange_state_json(user_id, message, title, url, started_at, thumbnail)

	async def getJsonVars(self, user_id, offState):
		def getURL(afreecaID, bno):
			return f"https://play.sooplive.co.kr/{afreecaID}/{bno}" #get channel URL
		
		for count in range(100):
			try:
				if not offState or 'broad' not in offState:
					print(f"{datetime.now()} Invalid offState for {user_id}, retrying...")
					try:
						response = get(
							afreeca_getLink(self.afreecaIDList.loc[user_id, "afreecaID"]), 
							headers=base.getChzzkHeaders(), 
							timeout=3
						)
						offState = loads(response.text)
						if not offState or 'broad' not in offState:
							await asyncio.sleep(0.05)
							continue
					except Exception as e:
						print(f"{datetime.now()} Failed to fetch new offState: {e}")
						await asyncio.sleep(0.05)
						continue

				if offState["broad"]["broad_no"] != self.afreeca_titleData.loc[user_id, 'chatChannelId']:
					self.afreeca_titleData.loc[user_id, 'oldChatChannelId'] = self.afreeca_titleData.loc[user_id, 'chatChannelId']
					self.afreeca_titleData.loc[user_id, 'chatChannelId'] = offState["broad"]["broad_no"]

				_, _, thumbnailLink, _, _, _, _, _ = afreeca_getChannelStateData(self.afreeca_titleData.loc[user_id, 'chatChannelId'],self.afreecaIDList.loc[user_id, "afreecaID"])
				url = getURL(self.afreecaIDList.loc[user_id, "afreecaID"], self.afreeca_titleData.loc[user_id, 'chatChannelId'])
				started_at   = self.getStarted_at(offState)
				viewer_count = offState['broad']['current_sum_viewer']

				try:	
					thumbnail = self.getThumbnail(user_id, thumbnailLink)
					if thumbnail is None: 
						print(f"{datetime.now()} wait make thumbnail 1 .{str(thumbnailLink)}")
						if count % 20 == 0: offState = loads(get(afreeca_getLink(self.afreecaIDList.loc[user_id, "afreecaID"]), headers=base.getChzzkHeaders(), timeout=3).text)
						await asyncio.sleep(0.05)
						continue
					break
				except Exception as e:
					asyncio.create_task(DiscordWebhookSender()._log_error(f"{datetime.now()} wait make thumbnail 2 {e}.{str(thumbnailLink)}"))
					if count % 20 == 0: offState = loads(get(afreeca_getLink(self.afreecaIDList.loc[user_id, "afreecaID"]), headers=base.getChzzkHeaders(), timeout=3).text)
					await asyncio.sleep(0.05)

			except Exception as e: 
				asyncio.create_task(DiscordWebhookSender()._log_error(f"error getJsonVars {user_id}.{e}"))
				await asyncio.sleep(0.05)

		else: thumbnail = ""

		return started_at, thumbnail, url, viewer_count

	def get_online_state_json(self, user_id, message, title, url, started_at, thumbnail, viewer_count):
		return {"username": self.afreecaIDList.loc[user_id, 'channelName'], "avatar_url": self.afreecaIDList.loc[user_id, 'channel_thumbnail'],\
				"embeds": [
					{"color": int(self.afreecaIDList.loc[user_id, 'channel_color']),
					"fields": [
						{"name": "방제", "value": title, "inline": True},
						{"name": ':busts_in_silhouette: 시청자수',
						"value": viewer_count, "inline": True}],
					"title":  f"{self.afreecaIDList.loc[user_id, 'channelName']} {message}\n",\
				"url": url, \
				"image": {"url": thumbnail},
				"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().soop_icon },
				"timestamp": base.changeUTCtime(started_at)}]}
	
	def get_online_titleChange_state_json(self, user_id, message, title, url, started_at, thumbnail):
		return {"username": self.afreecaIDList.loc[user_id, 'channelName'], "avatar_url": self.afreecaIDList.loc[user_id, 'channel_thumbnail'],\
				"embeds": [
					{"color": int(self.afreecaIDList.loc[user_id, 'channel_color']),
					"fields": [
						{"name": "이전 방제", "value": str(self.afreeca_titleData.loc[user_id,'title1']), "inline": True},
						{"name": "현재 방제", "value": title, "inline": True}],
					"title":  f"{self.afreecaIDList.loc[user_id, 'channelName']} {message}\n",\
				"url": url, \
				"image": {"url": thumbnail},
				"footer": { "text": f"뱅온 시간", "inline": True, "icon_url": base.iconLinkData().soop_icon },
				"timestamp": base.changeUTCtime(started_at)}]}

	def getOffJson(self, user_id): #offJson
		return {"username": self.afreecaIDList.loc[user_id, 'channelName'], "avatar_url": self.afreecaIDList.loc[user_id, 'channel_thumbnail'],\
				"embeds": [
					{"color": int(self.afreecaIDList.loc[user_id, 'channel_color']),
					"title":  self.afreecaIDList.loc[user_id, 'channelName'] +" 방송 종료\n",\
				# "image": {"url": self.afreecaIDList.loc[user_id, 'offLine_thumbnail']}
				}]}

	def getChatFilterName(self, name):
		[channelName] = [self.chatFilter["channelName"][i] for i in range(len(list(self.chatFilter["channelID"]))) if self.chatFilter["channelID"][i] == name]
		return channelName

	def getName(self, data): #get chat person's name 
		try:	return data[data.index(":") + 1:data.index("!")]
		except: return None

	def getStarted_at(self, state_data): 
		time_str = state_data["station"]["broad_start"]
		if not time_str or time_str == '0000-00-00 00:00:00':
			return None
		try:
			time = datetime(int(time_str[:4]),int(time_str[5:7]),int(time_str[8:10]),int(time_str[11:13]),int(time_str[14:16]),int(time_str[17:19]))
			# time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
			return time.isoformat()
		except ValueError:
			return None

	def getViewer_count(self, stateData):
		return stateData['content']['concurrentUserCount'] #get viewer data

	def getMessage(self, live, user_id, offState): return "뱅온!" if (self.turnOnline(live, user_id, offState)) else "방제 변경" #turnOn or change title

	def getThumbnail(self, user_id, thumbnailLink): #thumbnail shape do transformation able to send to discord
		try:
			urlretrieve(thumbnailLink, "explain.png")
			file = {'file'      : open("explain.png", 'rb')}
			data = {"username"  : self.afreecaIDList.loc[user_id, 'channelName'],
					"avatar_url": self.afreecaIDList.loc[user_id, 'channel_thumbnail']}
			thumbnail  = post(environ['recvThumbnailURL'], files=file, data=data, timeout=3)
			try: remove('explain.png')
			except: pass
			frontIndex = thumbnail.text.index('"proxy_url"')
			thumbnail  = thumbnail.text[frontIndex:]
			frontIndex = thumbnail.index('https://media.discordap')
			return thumbnail[frontIndex:thumbnail.index(".png") + 4]
		except:
			return None

	def turnOnline(self, live, user_id, stateData):
		now_time = self.getStarted_at(stateData)
		old_time = self.afreeca_titleData.loc[user_id,'update_time']
		return live==1 and self.afreeca_titleData.loc[user_id,'live_state'] == "CLOSE" and now_time > old_time #turn online

	def turnOffline(self, live, user_id):
		# now_time = self.getStarted_at(stateData)
		# old_time = self.afreeca_titleData.loc[user_id,'update_time']
		return live==0 and self.afreeca_titleData.loc[user_id,'live_state'] == "OPEN" #turn offline

def afreeca_getLink(afreecaID): return f"https://chapi.sooplive.co.kr/api/{afreecaID}/station"

def afreeca_getChannelStateData(bno, bid):
	url = 'https://live.sooplive.co.kr/afreeca/player_live_api.php'
	data = {
		'bid': bid,
		'bno': bno,
		'type': 'live',
		'confirm_adult': 'false',
		'player_type': 'html5',
		'mode': 'landing',
		'from_api': '0',
		'pwd': '',
		'stream_type': 'common',
		'quality': 'HD'}
	try:
		response = post(f'{url}?bjid={bid}', data=data)
		res = response.json()
	except Exception as e:
		asyncio.create_task(DiscordWebhookSender()._log_error(f"error get player live {str(e)}"))
		return None, None, None, None, None, None, None, None
	live = res["CHANNEL"]["RESULT"]
	title = res["CHANNEL"]["TITLE"]

	adult_channel_state = -6
	if live == adult_channel_state:  # 연령제한 체널로 썸네일링크 못 읽을 경우
		thumbnail_url = f"https://liveimg.afreecatv.com/m/{bno}"
		return live, title, thumbnail_url, None, None, None, None, None
	if live:
		try: int(res['CHANNEL']['BNO'])
		except: 
			asyncio.create_task(DiscordWebhookSender()._log_error(f"error res['CHANNEL']['BNO'] None"))

		thumbnail_url = f"https://liveimg.afreecatv.com/m/{res['CHANNEL']['BNO']}"

		CHDOMAIN = res["CHANNEL"]["CHDOMAIN"].lower()
		CHATNO = res["CHANNEL"]["CHATNO"]
		FTK = res["CHANNEL"]["FTK"]
		BJID = res["CHANNEL"]["BJID"]
		CHPT = str(int(res["CHANNEL"]["CHPT"]) + 1)
	else:
		title = None
		thumbnail_url = None
		CHDOMAIN = None
		CHATNO = None
		FTK = None
		BJID = None
		CHPT = None

	return live, title, thumbnail_url, CHDOMAIN, CHATNO, FTK, BJID, CHPT