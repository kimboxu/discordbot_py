import os
import logging
import asyncio
from json import loads
from aiohttp import ClientSession, TCPConnector, ClientError
import pandas as pd
from requests import post
from timeit import default_timer
from dataclasses import dataclass
from supabase import create_client
from datetime import datetime, timedelta
from dotenv import load_dotenv

class initVar:
	# load_dotenv()
	DO_TEST = False
	
	printCount 		= 100	#every 100 count, print count 
	countTimeList = []
	countTimeList.append(default_timer())	#nomal sleep time
	countTimeList.append(default_timer())	#nomal sleep time
	SEC 			= 1000000  #every 100000 count rejoin
	count 			= 0

	logging.getLogger('httpx').setLevel(logging.WARNING)
	# 모든 로거의 레벨을 높이려면 다음과 같이 할 수 있습니다:
	# logging.getLogger().setLevel(logging.WARNING)
	print("start!")
	
@dataclass
class chzzkLiveData:
	livePostList: list
	change_title_time: str = '2024-01-01 00:00:00'
	LiveCountStart: str = '2024-01-01 00:00:00'
	LiveCountEnd: str = '2024-01-01 00:00:00'

@dataclass
class afreecaLiveData:
	livePostList: list
	change_title_time: str = '2024-01-01 00:00:00'
	LiveCountStart: str = '2024-01-01 00:00:00'
	LiveCountEnd: str = '2024-01-01 00:00:00'

@dataclass
class twitchLiveData:
	livePostList: list
	change_title_time: str = '2024-01-01 00:00:00'
	LiveCountStart: str = '2024-01-01 00:00:00'
	LiveCountEnd: str = '2024-01-01 00:00:00'

@dataclass
class chzzkVideoData:
	video_alarm_List: list

@dataclass
class youtubeVideoData:
	video_count_check_dict: dict
	developerKeyDict: dict
	TFdeveloperKey: int = 0
	youtubeChannelIdx: int = 0

@dataclass
class cafeVarData:
	cafeChannelIdx: int = 0

@dataclass
class iconLinkData:
	chzzk_icon: str = os.environ['CHZZK_ICON']
	afreeca_icon: str = os.environ['AFREECA_ICON']
	soop_icon: str = os.environ['SOOP_ICON']
	black_img: str = os.environ['BLACK_IMG']
	youtube_icon: str = os.environ['YOUTUBE_ICON']
	cafe_icon: str = os.environ['CAFE_ICON']

async def userDataVar(init: initVar):
	try:
		supabase = create_client(os.environ['supabase_url'], os.environ['supabase_key'])
		
		date_update = supabase.table('date_update').select("*").execute()
		update_data = date_update.data[0]

		for attr, value in {
			'youtube_TF': update_data['youtube_TF'],
			'chat_json': update_data['chat_json']
		}.items():
			setattr(init, attr, value)

		if update_data['user_date']:
			userStateData = supabase.table('userStateData').select("*").execute()
			init.userStateData = re_idx(make_list_to_dict(userStateData.data))
			init.userStateData.index = list(init.userStateData['discordURL'])
			await update_flag(supabase, 'user_date', False)

		if update_data['all_date']:
			await discordBotDataVars(init)
			await update_flag(supabase, 'all_date', False)

	except Exception as e:
		error_details = f"Error in userDataVar: {str(e)}"
		if hasattr(e, 'response'):
			error_details += f"\nResponse: {e.response.text}"
		
		if "EOF occurred in violation of protocol" in str(e):
			error_details += "\nSSL connection error occurred"
			
		errorPost(error_details)

async def update_flag(supabase, field, value):
	#플래그 업데이트를 위한 헬퍼 함수
	supabase.table('date_update').upsert({
		"idx": 0,
		field: value
	}).execute()

async def discordBotDataVars(init: initVar):
	while True:
		try:
			supabase = create_client(os.environ['supabase_url'], os.environ['supabase_key'])
			
			# 모든 테이블 이름을 리스트로 정의
			table_names = [
				'userStateData', 'twitch_titleData', 'chzzk_titleData', 
				'afreeca_titleData', 'twitchIDList', 'chzzkIDList', 
				'afreecaIDList', 'youtubeData', 'twitch_chatFilter',
				'chzzk_chatFilter', 'afreeca_chatFilter', 'chzzk_video', 
				'cafeData'
			]
			
			# 모든 테이블의 데이터를 비동기로 가져오기
			tasks = [fetch_data(supabase, name) for name in table_names]
			results = await asyncio.gather(*tasks)
			
			# 결과를 딕셔너리로 변환
			data_dict = {name: re_idx(make_list_to_dict(result.data)) 
						for name, result in zip(table_names, results)}
			
			# init 객체에 데이터 할당
			for name, data in data_dict.items():
				setattr(init, name, data)
			
			# index 설정
			index_mappings = {
				'userStateData': 'discordURL',
				'twitchIDList': 'channelID',
				'chzzkIDList': 'channelID',
				'afreecaIDList': 'channelID',
				'twitch_titleData': 'channelID',
				'chzzk_titleData': 'channelID',
				'afreeca_titleData': 'channelID',
				'youtubeData': 'YoutubeChannelID',
				'cafeData': 'channelID',
				'chzzk_video': 'channelID'
			}
			
			for table_name, index_col in index_mappings.items():
				data = getattr(init, table_name)
				data.index = list(data[index_col])
			
			break
			
		except Exception as e:
			errorPost(f"Error in discordBotDataVars: {e}")
			if init.count != 0: break
			await asyncio.sleep(0.5)

async def fetch_data(supabase, date_name):
	return supabase.table(date_name).select("*").execute()

def re_idx(data):
	return (data
			.astype({'idx': 'int'})
			.sort_values('idx')
			.reset_index(drop=True))

def make_list_to_dict(data):
	if not data:
		return pd.DataFrame()
		
	# Dictionary comprehension을 사용하여 더 간단하게 표현
	return pd.DataFrame({
		key: [item[key] for item in data]
		for key in data[0].keys()
	})

async def async_post_message(arr, max_retries=3, max_concurrent=5):
	semaphore = asyncio.Semaphore(max_concurrent)
	async with ClientSession(connector=TCPConnector(ssl=False)) as session:
		tasks = [send_message_with_retry(session, url, data, max_retries, semaphore) for url, data in arr]
		responses = await asyncio.gather(*tasks, return_exceptions=True)
		return [r for r in responses if not isinstance(r, Exception)]

async def send_message_with_retry(session, url, data, max_retries, semaphore):
	async with semaphore:
		for attempt in range(max_retries):
			try:
				async with session.post(url, json=data, timeout=15) as response:  # Increased timeout
					response.raise_for_status()
					return await response.text()
			except ClientError as e:
				if response.status == 404:
					# Handle 404 error (e.g., delete user data)
					print(f"404 error for URL {url}. Deleting user data...")
					# Add your code to delete user data here
				if attempt == max_retries - 1:
					print(f"Failed to send message to {url} after {max_retries} attempts: {str(e)}")
					return None
				await asyncio.sleep(0.2 * 2**attempt)  # Exponential backoff
			except asyncio.TimeoutError:
				print(f"{datetime.now()} timeout send_message_with_retry {str(data)}")
				if attempt == max_retries - 1:
					return None
				await asyncio.sleep(0.2 * 2**attempt)  # Exponential backoff
			except Exception as e:
				print(f"Unexpected error in send_message_with_retry: {e}")
				if attempt == max_retries - 1:
					return None
				await asyncio.sleep(0.2 * 2**attempt)  # Exponential backoff

def fCount(init: initVar): #function to count
	if init.count >= init.SEC:
		init.count = 0

	if init.count % init.printCount == 0: 
		printCount(init)
	init.count += 1

async def fSleep(init: initVar):
	current_time = default_timer()
	init.countTimeList.append(current_time)
	
	# 리스트 길이 관리를 더 효율적으로 수정
	if len(init.countTimeList) > (init.printCount + 1):
		init.countTimeList.pop(0)
	
	# 시간 차이 계산 및 sleep 시간 결정
	time_diff = current_time - init.countTimeList[-2]
	sleepTime = max(0.01, min(1.00, 1.00 - time_diff))
	
	await asyncio.sleep(sleepTime)
	init.countTimeList[-1] += sleepTime

def get_online_count(data, id_column="channelID"):
	return sum(1 for channel_id in data[id_column] if data.loc[channel_id, "live_state"] == "OPEN")

def printCount(init: initVar):
	# 각 플랫폼의 온라인 카운트 계산
	online_counts = {
		'twitch': get_online_count(init.twitch_titleData),
		'chzzk': get_online_count(init.chzzk_titleData),
		'afreeca': get_online_count(init.afreeca_titleData)
	}
	
	# 모든 플랫폼이 오프라인인지 확인
	all_offline = not any(online_counts.values())
	
	# 시간 관련 데이터 계산
	current_time = datetime.now()
	count = str(init.count).zfill(len(str(init.SEC)))
	elapsed_time = round(init.countTimeList[-1] - init.countTimeList[0], 3)
	
	# 결과 출력
	status_prefix = "All offLine, " if all_offline else ""
	print(f"{status_prefix}{current_time} count {count} TIME {elapsed_time:.1f} SEC")

def changeUTCtime(time_str):
	time = datetime(int(time_str[:4]),int(time_str[5:7]),int(time_str[8:10]),int(time_str[11:13]),int(time_str[14:16]),int(time_str[17:19]))
	time -= timedelta(hours=9)
	return time.isoformat()

def subjectReplace(subject: str) -> str:
	replacements = {
		'&lt;': '<',
		'&gt;': '>',
		'&amp;': '&',
		'&quot;': '"',
		'&#035;': '#',
		'&#35;': '#',
		'&#039;': "'",
		'&#39;': "'"
	}
	
	for old, new in replacements.items():
		subject = subject.replace(old, new)
	
	return subject

def errorPost(msg, errorPostBotURL=os.environ['errorPostBotURL']):
	data = {
		'content': msg,
		'username': "error alarm"
	}
	
	try:
		print(f"{datetime.now()} {msg}")
		response = post(errorPostBotURL, json=data, timeout=3)
		response.raise_for_status()  # HTTP 오류 확인
		
	except Exception as e:
		print(f"{datetime.now()} Post error: {e} {msg}")

async def async_errorPost(msg, errorPostBotURL=os.environ.get('errorPostBotURL')):
	max_retries = 5
	base_delay = 0.3  # 초기 대기 시간 (초)

	for attempt in range(max_retries):
		try:
			data = {'content': msg, "username": "error alarm"}
			print(f"{datetime.now()} {msg}")
			
			if not errorPostBotURL:
				print("Error: errorPostBotURL is not set")
				return

			async with ClientSession() as session:
				async with session.post(errorPostBotURL, json=data, timeout=10) as response:
					response_text = await response.text()
					if response.status == 429:
						retry_after = float(response.headers.get('Retry-After', 1))
						await asyncio.sleep(retry_after)
						continue
					return  # 성공적으로 요청을 보냈거나, 429가 아닌 다른 에러 발생 시 함수 종료

		except asyncio.TimeoutError:
			print(f"{datetime.now()} Timeout error while posting: {msg}")
			return None
		except ClientError as client_error:
			print(f"{datetime.now()} Client error: {client_error} - {msg}")
		except Exception as e:
			print(f"{datetime.now()} Unexpected error: {type(e).__name__}: {e} - {msg}")

		# 재시도 전 대기
		await asyncio.sleep(base_delay * (2 ** attempt))

	print(f"Failed to post message after {max_retries} attempts: {msg}")

def getTwitchHeaders(): 
	twitch_Client_ID = os.environ['twitch_Client_ID']
	twitch_Client_secret = os.environ['twitch_Client_secret']

	oauth_key = post(	"https://id.twitch.tv/oauth2/token?client_id=" +
						twitch_Client_ID + "&client_secret=" +
						twitch_Client_secret +
						"&grant_type=client_credentials")
	authorization = 'Bearer ' + loads(oauth_key.text)["access_token"]
	return {'client-id': twitch_Client_ID, 'Authorization': authorization} #get headers 
def getChzzkHeaders(): return {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'} #get headers 
def getChzzkCookie(): return {'NID_AUT': os.environ['NID_AUT'],'NID_SES':os.environ['NID_SES']} 

async def should_terminate(sock, ID):
	try:
		await asyncio.sleep(300)  # 5분 대기
		
		if not sock.closed:
			await sock.close()
		print(f"{datetime.now()} {ID}: 방송 종료 5분 경과, 연결 종료")
		
	except asyncio.CancelledError:
		# 태스크가 취소된 경우 정상적으로 종료
		raise
	except Exception as e:
		print(f"{datetime.now()} error should_terminate {e}")
	
	return "CLOSE"

def if_after_time(time_str, sec = 300): # 지금 시간이 이전 시간보다 SEC초 만큼 지났는지 확인
    time = datetime(int(time_str[:4]),int(time_str[5:7]),int(time_str[8:10]),int(time_str[11:13]),int(time_str[14:16]),int(time_str[17:19])) + timedelta(seconds=sec)
    return time.isoformat() <= datetime.now().isoformat()

async def timer(time): 
	await asyncio.sleep(time)  # time 초 대기
	return "CLOSE"

async def get_message(arr, platform):
	platform_handlers = {
		"afreeca": afreeca_get_url_json,
		"twitch": afreeca_get_url_json,
		"chzzk": chzzk_get_url_json,
		"cafe": cafe_get_url_json
	}
	
	handler = platform_handlers.get(platform)
	if not handler:
		return []

	async with ClientSession() as session:
		tasks = [handler(session, url) for url in arr]
		responses = await asyncio.gather(*tasks)
		return [responses]

async def chzzk_get_url_json(session, args):
	url, headers, cookies = args[0]
	try:
		async with session.get(url, headers=headers, cookies=cookies, timeout=3) as response:
			return [await response.json(), args[1], True]
	except: return [{}, args[1], False]
	
async def afreeca_get_url_json(session, args):
	url, headers = args[0]
	try:
		async with session.get(url, headers=headers, timeout=3) as response:
			return [await response.json(), args[1], True]
	except: return [{}, args[1], False]

async def cafe_get_url_json(session, args):
	url, params = args[0], args[1]
	headers = getChzzkHeaders()
	try:
		async with session.get(url, params=params, headers=headers, timeout=3) as response:
			return [await response.json(), True]
	except Exception as e: return [{}, False]
	
def twitch_getChannelOffStateData(offStateList, twitchID):
	try:
		for offState in offStateList:
			if offState["broadcaster_login"] == twitchID:
				return (
					offState["is_live"],
					offState["title"],
					offState["thumbnail_url"]
				)
		return None, None, None
	except Exception as e:
		errorPost(f"error getChannelOffStateData twitch {e}")
		return None, None, None

def chzzk_getChannelOffStateData(stateData, chzzkID, channel_thumbnail = ""):
	try:
		if stateData["channel"]["channelId"]==chzzkID:
			return (
				stateData["status"],
				stateData["liveTitle"],
				stateData["channel"]["channelImageUrl"]
			)
		return None, None, channel_thumbnail
	except Exception as e: 
		errorPost(f"error getChannelOffStateData chzzk {e}")
		return None, None, channel_thumbnail

def afreeca_getChannelOffStateData(stateData, afreecaID, channel_thumbnail = ""):
	try:
		if stateData["station"]["user_id"] == afreecaID: 
			live = int(stateData["broad"] is not None)
			title = stateData["broad"]["broad_title"] if live else None
			thumbnail_url = stateData["profile_image"]
			if thumbnail_url.startswith("//"):
				thumbnail_url = f"https:{thumbnail_url}"
			return live, title, thumbnail_url
		return None, None, channel_thumbnail
	except Exception as e: 
		errorPost(f"error getChannelOffStateData afreeca {e}")

def afreeca_getiflive(stateData):
	try:
		thumbnail_url = stateData["profile_image"]
		if thumbnail_url.startswith("//"):
			thumbnail_url = f"https:{thumbnail_url}"
		return thumbnail_url
	except Exception as e:
		errorPost(f"error getChannelOffStateData {e}")
		return None

async def save_airing_data(init: initVar, platform, id_):
	def get_index(channel_list, target_id):
		return {id: idx for idx, id in enumerate(channel_list)}[target_id]
	
	def get_twitch_data():
		return {
				"idx": get_index(init.twitch_titleData["channelID"], id_),
				"live_state": init.twitch_titleData.loc[id_, "live_state"],
				"title1": init.twitch_titleData.loc[id_, "title1"]
		}
	
	def get_chzzk_data():
		return {
				"idx": get_index(init.chzzk_titleData["channelID"], id_),
				"live_state": init.chzzk_titleData.loc[id_, "live_state"],
				"title1": init.chzzk_titleData.loc[id_, "title1"],
				"title2": init.chzzk_titleData.loc[id_, "title2"],
				"update_time": init.chzzk_titleData.loc[id_, "update_time"],
				"channelURL": init.chzzk_titleData.loc[id_, "channelURL"],
				"messageTime": init.chzzk_titleData.loc[id_, "messageTime"]
		}
	
	def get_afreeca_data():
		return {
				"idx": get_index(init.afreeca_titleData["channelID"], id_),
				"live_state": init.afreeca_titleData.loc[id_, "live_state"],
				"title1": init.afreeca_titleData.loc[id_, "title1"],
				"title2": init.afreeca_titleData.loc[id_, "title2"],
				"update_time": init.afreeca_titleData.loc[id_, "update_time"],
				"chatChannelId": init.afreeca_titleData.loc[id_, "chatChannelId"],
				"oldChatChannelId": init.afreeca_titleData.loc[id_, "oldChatChannelId"]
		}

	platform_configs = {
		'twitch': ('twitch_titleData', get_twitch_data),
		'chzzk': ('chzzk_titleData', get_chzzk_data),
		'afreeca': ('afreeca_titleData', get_afreeca_data)
	}

	if platform not in platform_configs:
		errorPost(f"Unsupported platform: {platform}")
		return
	
	table_name, data_func = platform_configs[platform]

	for _ in range(3):
		try:
			supabase = create_client(os.environ['supabase_url'], os.environ['supabase_key'])
			supabase.table(table_name).upsert(data_func()).execute()
			break
		except Exception as e:
			errorPost(f"error saving profile data {e}")
			await asyncio.sleep(0.5)

async def save_profile_data(init: initVar, platform, id):
	# Platform specific configurations
	def get_index(channel_list, target_id):
		return {id: idx for idx, id in enumerate(channel_list)}[target_id]

	def get_twitch_data():
		return {
			"idx": get_index(init.twitchIDList["channelID"], id),
			'channel_thumbnail': init.twitchIDList.loc[id, 'channel_thumbnail']
		}
	
	def get_chzzk_data():
		return {
			"idx": get_index(init.chzzkIDList["channelID"], id),
			'channel_thumbnail': init.chzzkIDList.loc[id, 'channel_thumbnail'],
			'userID': init.chzzkIDList.loc[id, 'userID']
		}
	
	def get_afreeca_data():
		return {
			"idx": get_index(init.afreecaIDList["channelID"], id),
			'channel_thumbnail': init.afreecaIDList.loc[id, 'channel_thumbnail']
		}

	platform_configs = {
		'twitch': ('twitchIDList', get_twitch_data),
		'chzzk': ('chzzkIDList', get_chzzk_data),
		'afreeca': ('afreecaIDList', get_afreeca_data)
	}

	if platform not in platform_configs:
		errorPost(f"Unsupported platform: {platform}")
		return

	table_name, data_func = platform_configs[platform]

	for _ in range(3):
		try:
			supabase = create_client(os.environ['supabase_url'], os.environ['supabase_key'])
			supabase.table(table_name).upsert(data_func()).execute()
			break
		except Exception as e:
			errorPost(f"error saving profile data {e}")
			await asyncio.sleep(0.5)

async def chzzk_saveVideoData(init: initVar, chzzkID, videoNo, videoTitle, publishDate): #save profile data
	idx = {chzzk: i for i, chzzk in enumerate(init.chzzk_video["channelID"])}

	for _ in range(3):
		try:
			chzzk_video_json = init.chzzk_video.loc[chzzkID, 'VOD_json']

			chzzk_video_json["videoTitle3"] = chzzk_video_json["videoTitle2"]
			chzzk_video_json["videoTitle2"] = chzzk_video_json["videoTitle1"]
			chzzk_video_json["videoTitle1"] = videoTitle

			chzzk_video_json["videoNo3"] 	= chzzk_video_json["videoNo2"]
			chzzk_video_json["videoNo2"] 	= chzzk_video_json["videoNo1"]
			chzzk_video_json["videoNo1"] 	= videoNo

			chzzk_video_json["publishDate"] = publishDate

			supabase = create_client(os.environ['supabase_url'], os.environ['supabase_key'])
			supabase.table('chzzk_video').upsert({
				"idx": idx[chzzkID],
				'VOD_json': init.chzzk_video.loc[chzzkID, 'VOD_json']
			}).execute()
			break
		except Exception as e:
			errorPost(f"error saving profile data {e}")
			await asyncio.sleep(0.5)

def saveCafeData(init: initVar, cafeID):
	try:
		# 인덱스 생성을 dict comprehension으로 더 간단하게
		idx = {cafe: i for i, cafe in enumerate(init.cafeData["channelID"])}
		
		# 데이터 준비를 별도로 하여 가독성 향상
		cafe_data = {
			"idx": idx[cafeID],
			"update_time": int(init.cafeData.loc[cafeID, 'update_time']),
			"cafe_json": init.cafeData.loc[cafeID, 'cafe_json'],
			"cafeNameDict": init.cafeData.loc[cafeID, 'cafeNameDict']
		}
		
		# supabase 클라이언트 생성
		supabase = create_client(os.environ['supabase_url'], os.environ['supabase_key'])
		
		# 데이터 저장
		supabase.table('cafeData').upsert(cafe_data).execute()
		
	except Exception as e:
		errorPost(f"error save cafe time {e}")
	
# def post_message(init, arr):
# 	list_of_urls = []
	
# 	for i in range(0, len(arr), init.requestListSize):list_of_urls.append(arr[i: i+init.requestListSize])

# 	for i in range(len(list_of_urls)):
# 		with ThreadPoolExecutor(max_workers=len(list_of_urls[i])) as pool:
# 			list(pool.map(post_url_json,list_of_urls[i]))

# def post_url_json(args): return post(args[0], json=args[1], timeout=30)
