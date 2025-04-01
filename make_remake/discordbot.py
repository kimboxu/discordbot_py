from os import environ
import base
import asyncio
from time import time
from datetime import datetime
from supabase import create_client
from Twitch_live_message import twitch_live_message
# from Chzzk_live_message import chzzk_live_message
# from Afreeca_live_message import afreeca_live_message
from Chzzk_chat_message import chzzk_chat_message
from Afreeca_chat_message import afreeca_chat_message
from Chzzk_video import chzzk_video
from getCafePostTitle import getCafePostTitle
from getYoutubeJsonData import getYoutubeJsonData
from discord_webhook_sender import DiscordWebhookSender

from live_message import chzzk_live_message, afreeca_live_message

    
async def main_loop(init: base.initVar):

    while True:
        try:
            if init.count % 2 == 0: await base.userDataVar(init)

            cafe_tasks = [asyncio.create_task(getCafePostTitle(init, channel_id).start()) for channel_id in init.cafeData["channelID"]]
            chzzk_video_tasks = [asyncio.create_task(chzzk_video(init, channel_id).start()) for channel_id in init.chzzkIDList["channelID"]]
            chzzk_live_tasks = [asyncio.create_task(chzzk_live_message(init, channel_id).start()) for channel_id in init.chzzkIDList["channelID"]]
            afreeca_live_tasks = [asyncio.create_task(afreeca_live_message(init, channel_id).start()) for channel_id in init.afreecaIDList["channelID"]]
            
            tasks = [
                *chzzk_live_tasks,
                *afreeca_live_tasks,
                *chzzk_video_tasks,
                *cafe_tasks
            ]

            await asyncio.gather(*tasks)
            await base.fSleep(init)
            base.fCount(init)

        except Exception as e:
            asyncio.create_task(DiscordWebhookSender._log_error(f"Error in main loop: {str(e)}"))
            await asyncio.sleep(1)


async def fyoutube(init: base.initVar):
    await asyncio.sleep(2)
    youtubeVideo = base.youtubeVideoData(
        video_count_check_dict={},
        developerKeyDict = environ['developerKeyList'].split(","))
    
    while True:
        try:
            if not init.youtube_TF: await asyncio.sleep(3);continue
            start_time = time()
            await asyncio.create_task(getYoutubeJsonData().fYoutube(init, youtubeVideo))
            end_time = time()
            sleepTime = 0
            if end_time - start_time < 3.00: sleepTime = 3.00 - (end_time - start_time)
            await asyncio.sleep(sleepTime)
        except Exception as e: 
            print(f"{datetime.now()} error fyoutube {e}")
            await asyncio.sleep(3)

async def chzzk_chatf(init: base.initVar):
    await asyncio.sleep(2)
    
    tasks = {}  # 채널 ID별 실행 중인 task를 관리할 딕셔너리

    while True:
        try:
            # 기존 실행 중인 태스크를 유지하면서, 새로운 채널이 추가되면 실행
            for channel_id in init.chzzkIDList["channelID"]:
                if channel_id not in tasks or tasks[channel_id].done():
                    chat_instance = chzzk_chat_message(init, channel_id)
                    tasks[channel_id] = asyncio.create_task(chat_instance.start())

            await asyncio.sleep(1)  # 5초마다 체크 (필요하면 조절 가능)
        
        except Exception as e:
            print(f"{datetime.now()} error chzzk_chatf {e}")
            await asyncio.sleep(1)

async def afreeca_chatf(init: base.initVar):
    await asyncio.sleep(2)
    
    tasks = {}  # 채널 ID별 실행 중인 task를 관리할 딕셔너리

    while True:
        try:
            # 기존 실행 중인 태스크를 유지하면서, 새로운 채널이 추가되면 실행
            for channel_id in init.afreecaIDList["channelID"]:
                if channel_id not in tasks or tasks[channel_id].done():
                    chat_instance = afreeca_chat_message(init, channel_id)
                    tasks[channel_id] = asyncio.create_task(chat_instance.start())

            await asyncio.sleep(1)  # 5초마다 체크 (필요하면 조절 가능)
        
        except Exception as e:
            print(f"{datetime.now()} error afreeca_chatf {e}")
            await asyncio.sleep(1)
 
async def main():
    init = base.initVar()
    await base.discordBotDataVars(init)
    await base.userDataVar(init)
    await asyncio.sleep(1)
    
    test = [
            asyncio.create_task(main_loop(init)),
            asyncio.create_task(afreeca_chatf(init)),
            asyncio.create_task(chzzk_chatf(init)),
            asyncio.create_task(fyoutube(init)),
            ]
    
    await asyncio.gather(*test)
        
if __name__ == "__main__":
    asyncio.run(main())
