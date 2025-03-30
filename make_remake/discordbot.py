from os import environ
import base
import asyncio
from time import time
from datetime import datetime
from supabase import create_client
from Twitch_live_message import twitch_live_message
from Chzzk_live_message import chzzk_live_message
from Afreeca_live_message import afreeca_live_message
from Chzzk_chat_message import chzzk_chat_message
from Afreeca_chat_message import AfreecaChat
from Chzzk_video import chzzk_video
from getCafePostTitle import getCafePostTitle
from getYoutubeJsonData import getYoutubeJsonData
from discord_webhook_sender import DiscordWebhookSender

    
async def main_loop(init: base.initVar):
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    c_chzzk_live_message = chzzk_live_message(init)
    c_afreeca_live_message = afreeca_live_message(init)
    c_chzzk_video = chzzk_video(init)
    c_cafe = getCafePostTitle(init)

    while True:
        try:
            if init.count % 2 == 0: await base.userDataVar(init, supabase)
            tasks = [
                asyncio.create_task(c_chzzk_live_message.chzzk_liveMsg()),
                asyncio.create_task(c_afreeca_live_message.afreeca_liveMsg()),
                asyncio.create_task(c_cafe.fCafeTitle()),
                asyncio.create_task(c_chzzk_video.chzzk_video_msg()),
            ]

            await asyncio.gather(*tasks)
            await base.fSleep(init)
            base.fCount(init)

        except Exception as e:
            asyncio.create_task(DiscordWebhookSender()._log_error(f"Error in main loop: {str(e)}"))
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
    while True:
        try:
            test = [chzzk_chat_message(init, chzzkID).start() for chzzkID in init.chzzkIDList["channelID"]] 
            await asyncio.gather(*test)
        except Exception as e: print(f"{datetime.now()} error chzzk_chatf {e}");await asyncio.sleep(1)

async def afreeca_chatf(init: base.initVar):
    await asyncio.sleep(2)
    while True:
        try:
            test = [AfreecaChat(init, afreecaID).start() for afreecaID in init.afreecaIDList["channelID"]] 
            await asyncio.gather(*test)
        except Exception as e: print(f"{datetime.now()} error afreeca_chatf {e}");await asyncio.sleep(1)
        
async def main():
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    init = base.initVar()
    await base.discordBotDataVars(init)
    await base.userDataVar(init, supabase)
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
