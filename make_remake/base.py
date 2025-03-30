from os import environ
import logging
import asyncio
import aiohttp
from json import loads
from queue import Queue
from aiohttp import ClientSession, TCPConnector, ClientError
import pandas as pd
from requests import post
from timeit import default_timer
from dataclasses import dataclass, field
from supabase import create_client
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from discord_webhook_sender import DiscordWebhookSender

class initVar:
	load_dotenv()
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
	livePostList: list = field(default_factory=list)
	change_title_time: str = '2024-01-01 00:00:00'
	LiveCountStart: str = '2024-01-01 00:00:00'
	LiveCountEnd: str = '2024-01-01 00:00:00'

@dataclass
class afreecaLiveData:
	livePostList: list = field(default_factory=list)
	change_title_time: str = '2024-01-01 00:00:00'
	LiveCountStart: str = '2024-01-01 00:00:00'
	LiveCountEnd: str = '2024-01-01 00:00:00'

@dataclass
class twitchLiveData:
	livePostList: list = field(default_factory=list)
	change_title_time: str = '2024-01-01 00:00:00'
	LiveCountStart: str = '2024-01-01 00:00:00'
	LiveCountEnd: str = '2024-01-01 00:00:00'

@dataclass
class chzzkVideoData:
	video_alarm_List: list = field(default_factory=list)

@dataclass
class youtubeVideoData:
	video_count_check_dict: dict
	developerKeyDict: dict
	TFdeveloperKey: int = 0
	youtubeChannelIdx: int = 0

@dataclass
class iconLinkData:
	chzzk_icon: str = environ['CHZZK_ICON']
	afreeca_icon: str = environ['AFREECA_ICON']
	soop_icon: str = environ['SOOP_ICON']
	black_img: str = environ['BLACK_IMG']
	youtube_icon: str = environ['YOUTUBE_ICON']
	cafe_icon: str = environ['CAFE_ICON']

class AsyncLogger:
    def __init__(self, bot_url, max_workers=3):
        self.bot_url = bot_url
        self.log_queue = Queue()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()

    async def start_logging(self):
        """로깅 프로세스 시작"""
        self._stop_event.clear()
        await self.loop.run_in_executor(
            self.executor, 
            self._process_log_queue
        )

    def _process_log_queue(self):
        """백그라운드 스레드에서 로그 큐 처리"""
        while not self._stop_event.is_set():
            try:
                # 0.1초마다 큐 확인
                if not self.log_queue.empty():
                    message = self.log_queue.get(timeout=0.1)
                    asyncio.run(self._send_log(message))
            except Exception as e:
                print(f"Logging queue processing error: {e}")

    async def _send_log(self, message):
        """로그 메시지 비동기 전송"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    data = {'content': message, "username": "error alarm"}
                    async with session.post(
                        self.bot_url, 
                        json=data, 
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as response:
                        if response.status == 429:
                            retry_after = float(response.headers.get('Retry-After', 1))
                            await asyncio.sleep(retry_after)
                            continue
                        return
            except Exception as e:
                print(f"Log sending attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(0.5 * (2 ** attempt))

    def enqueue_log(self, message):
        """로그 메시지 큐에 추가"""
        self.log_queue.put(message)

    async def stop(self):
        """로깅 프로세스 정지"""
        self._stop_event.set()
        self.executor.shutdown(wait=True)
  

async def userDataVar(init: initVar, supabase):
    try:
        
        # 1. 업데이트 정보 가져오기
        date_update = await asyncio.to_thread(
            lambda: supabase.table('date_update').select("*").execute()
        )
        update_data = date_update.data[0]

        # 단순 속성 설정
        for attr, value in {
            'youtube_TF': update_data['youtube_TF'],
            'chat_json': update_data['chat_json']
        }.items():
            setattr(init, attr, value)

        # 병렬로 필요한 데이터 로드
        tasks = []
        
        if update_data['user_date']:
            tasks.append(load_user_state_data(init, supabase))
            
        if update_data['all_date']:
            tasks.append(discordBotDataVars(init))
            
        # 모든 작업 기다리기
        if tasks:
            await asyncio.gather(*tasks)

    except Exception as e:
        error_details = f"Error in userDataVar: {str(e)}"
        if hasattr(e, 'response'):
            error_details += f"\nResponse: {e.response.text}"
        
        if "EOF occurred in violation of protocol" in str(e):
            error_details += "\nSSL connection error occurred"
            
        asyncio.create_task(DiscordWebhookSender._log_error(error_details))

async def load_user_state_data(init, supabase):
    # 사용자 상태 데이터 로드
    userStateData = await asyncio.to_thread(
        lambda: supabase.table('userStateData').select("*").execute()
    )
    init.userStateData = re_idx(make_list_to_dict(userStateData.data))
    init.userStateData.index = list(init.userStateData['discordURL'])
    
    # 플래그 업데이트
    await update_flag(supabase, 'user_date', False)

async def update_flag(supabase, field, value):
    # 비동기로 플래그 업데이트
    await asyncio.to_thread(
        lambda: supabase.table('date_update').upsert({
            "idx": 0,
            field: value
        }).execute()
    )

# async def cached_query(supabase, table, query_func, cache_key, ttl=60):
# 	# 메모리 캐시 초기화
# 	_cache = {}
# 	_cache_expiry = {}
# 	"""캐싱 메커니즘이 있는 쿼리 실행"""
# 	now = datetime.now()
	
# 	# 캐시가 유효한지 확인
# 	if cache_key in _cache and _cache_expiry[cache_key] > now:
# 		return _cache[cache_key]
	
# 	# 캐시가 없거나 만료된 경우 쿼리 실행
# 	result = await asyncio.to_thread(query_func)
	
# 	# 결과 캐싱
# 	_cache[cache_key] = result
# 	_cache_expiry[cache_key] = now + ttl
	
# 	return result

async def discordBotDataVars(init: initVar):
	while True:
		try:
			supabase = create_client(environ['supabase_url'], environ['supabase_key'])
			
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
			asyncio.create_task(DiscordWebhookSender._log_error((f"Error in discordBotDataVars: {e}")))
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

def getTwitchHeaders(): 
	twitch_Client_ID = environ['twitch_Client_ID']
	twitch_Client_secret = environ['twitch_Client_secret']

	oauth_key = post(	"https://id.twitch.tv/oauth2/token?client_id=" +
						twitch_Client_ID + "&client_secret=" +
						twitch_Client_secret +
						"&grant_type=client_credentials")
	authorization = 'Bearer ' + loads(oauth_key.text)["access_token"]
	return {'client-id': twitch_Client_ID, 'Authorization': authorization} #get headers 
def getChzzkHeaders(): return {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'} #get headers 
def getChzzkCookie(): return {'NID_AUT': environ['NID_AUT'],'NID_SES':environ['NID_SES']} 

# async def should_terminate(sock, ID):
# 	try:
# 		await asyncio.sleep(300)  # 5분 대기
		
# 		if not sock.closed:
# 			await sock.close()
# 		print(f"{datetime.now()} {ID}: 방송 종료 5분 경과, 연결 종료")
		
# 	except asyncio.CancelledError:
# 		# 태스크가 취소된 경우 정상적으로 종료
# 		raise
# 	except Exception as e:
# 		print(f"{datetime.now()} error should_terminate {e}")
	
# 	return "CLOSE"

def if_after_time(time_str, sec = 300): # 지금 시간이 이전 시간보다 SEC초 만큼 지났는지 확인
	time = datetime(int(time_str[:4]),int(time_str[5:7]),int(time_str[8:10]),int(time_str[11:13]),int(time_str[14:16]),int(time_str[17:19])) + timedelta(seconds=sec)
	return time <= datetime.now()

def if_last_chat(last_chat_time: datetime, sec = 300): #마지막 채팅을 읽어온지 sec초가 지났다면 True
    return (last_chat_time + timedelta(seconds=sec)) <= datetime.now()

# async def timer(time): 
# 	await asyncio.sleep(time)  # time 초 대기
# 	return "CLOSE"

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
		asyncio.create_task(DiscordWebhookSender._log_error(f"error getChannelOffStateData twitch {e}"))
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
		asyncio.create_task(DiscordWebhookSender._log_error(f"error getChannelOffStateData chzzk {e}"))
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
		asyncio.create_task(DiscordWebhookSender._log_error(f"error getChannelOffStateData afreeca {e}"))

def afreeca_getiflive(stateData):
	try:
		thumbnail_url = stateData["profile_image"]
		if thumbnail_url.startswith("//"):
			thumbnail_url = f"https:{thumbnail_url}"
		return thumbnail_url
	except Exception as e:
		asyncio.create_task(DiscordWebhookSender._log_error(f"error getChannelOffStateData {e}"))
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
		asyncio.create_task(DiscordWebhookSender._log_error(f"Unsupported platform: {platform}"))
		return
	
	table_name, data_func = platform_configs[platform]

	for _ in range(3):
		try:
			supabase = create_client(environ['supabase_url'], environ['supabase_key'])
			supabase.table(table_name).upsert(data_func()).execute()
			break
		except Exception as e:
			asyncio.create_task(DiscordWebhookSender._log_error(f"error saving profile data {e}"))
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
		asyncio.create_task(DiscordWebhookSender._log_error(f"Unsupported platform: {platform}"))
		return

	table_name, data_func = platform_configs[platform]

	for _ in range(3):
		try:
			supabase = create_client(environ['supabase_url'], environ['supabase_key'])
			supabase.table(table_name).upsert(data_func()).execute()
			break
		except Exception as e:
			asyncio.create_task(DiscordWebhookSender._log_error(f"error saving profile data {e}"))
			await asyncio.sleep(0.5)
