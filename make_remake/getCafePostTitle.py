import asyncio
from os import environ
from json import loads
from time import gmtime
from requests import get
from datetime import datetime
from urllib.parse import quote
from dataclasses import dataclass
from Chzzk_live_message import chzzk_getLink
from Afreeca_live_message import afreeca_getLink
from discord_webhook_sender import DiscordWebhookSender
from base import getChzzkHeaders, saveCafeData, subjectReplace, getChzzkCookie, afreeca_getChannelOffStateData, chzzk_getChannelOffStateData, get_message, cafeVarData, iconLinkData, initVar

@dataclass
class CafePostData:
    cafe_link: str
    menu_id: str
    menu_name: str
    subject: str
    image: str
    write_timestamp: str
    channelID: str
    writer_nickname: str
    
class getCafePostTitle:
    async def fCafeTitle(self, init: initVar, cafeVar: cafeVarData):
        for channelID in init.cafeData["channelID"]:
            try:
                await self.getCafeDataDic(init, channelID, cafeVar)
                await self.postCafe(init, channelID, cafeVar)
                    
            except Exception as e:
                asyncio.create_task(DiscordWebhookSender()._log_error(f"error cafe {channelID}.{e}"))

    async def getCafeDataDic(self, init: initVar, channelID, cafeVar: cafeVarData)-> list:

        try:
            list_of_urls = self.get_article_list(init.cafeData.loc[channelID, 'cafeNum']) 
            response_list = await get_message(list_of_urls, "cafe")
        except:
            return

        cafe_json_ref_articles = init.cafeData.loc[channelID, 'cafe_json']["refArticleId"]
        max_ref_article = max(cafe_json_ref_articles)
        update_time = int(init.cafeData.loc[channelID, 'update_time'])
        cafe_name_dict = init.cafeData.loc[channelID, "cafeNameDict"]
        channelID = init.cafeData.loc[channelID, 'cafeID']

        for response in response_list:
            for request, TF in response:
                if not TF: 
                    continue
                
                try:
                    for Article in request['message']['result']['articleList'][::-1]:
                        if Article["writerNickname"] not in cafe_name_dict.keys():
                            continue

                        if not (Article["refArticleId"] > max_ref_article and 
                                Article['writeDateTimestamp'] > update_time):
                            continue

                        Article["subject"] = subjectReplace(Article["subject"])
                        init.cafeData.loc[channelID, 'update_time'] = max(init.cafeData.loc[channelID, 'update_time'], Article["writeDateTimestamp"])

                        # Update cafe_json refArticleId list
                        if len(cafe_json_ref_articles) >= 10:
                            cafe_json_ref_articles[:-1] = cafe_json_ref_articles[1:]
                            cafe_json_ref_articles[-1] = Article["refArticleId"]
                        else:
                            cafe_json_ref_articles.append(Article["refArticleId"])

                        # CafePostData 객체 생성
                        cafe_post = CafePostData(
                            cafe_link=f"https://cafe.naver.com/{channelID}/{Article['refArticleId']}",
                            menu_id=Article["menuId"],
                            menu_name=Article["menuName"],
                            subject=Article["subject"],
                            image=Article.get("representImage", ""),
                            write_timestamp=Article["writeDateTimestamp"],
                            channelID=channelID,
                            writer_nickname=Article["writerNickname"]
                        )
                        cafeVar.message_list.append(cafe_post)
                except:
                    continue

    def get_article_list(self, cafe_unique_id: str, page_num: int = 1) -> list:
        BASE_URL = "https://apis.naver.com/cafe-web/cafe2/ArticleListV2dot1.json"
        
        params = {
            'search.queryType': 'lastArticle',
            'ad': 'False',
            'search.clubid': str(cafe_unique_id),
            'search.page': str(page_num)
        }
        
        return [(BASE_URL, params)]
    
    async def postCafe(self, init: initVar, channelID: list, cafeVar: cafeVarData):
        try:
            def make_list_of_cafe(json_data, writerNickname):
                if init.DO_TEST:
                    return [(environ['errorPostBotURL'], json_data)]
                
                return [
                    (discordURL, json_data)
                    for discordURL in init.userStateData['discordURL']
                    if init.userStateData.loc[discordURL, "cafe_user_json"] and 
                    writerNickname in init.userStateData.loc[discordURL, "cafe_user_json"].get(channelID, [])
                ]

            
            for post_data in cafeVar.message_list:
                json_data = self.create_cafe_json(init, post_data)
                print(f"{datetime.now()} {post_data.writer_nickname} post cafe")
                
                asyncio.create_task(DiscordWebhookSender().send_messages(make_list_of_cafe(json_data, post_data.writer_nickname)))
                
            saveCafeData(init, channelID)
            
        except Exception as e:
            asyncio.create_task(DiscordWebhookSender()._log_error(f"error postCafe {e}"))

    def create_cafe_json(self, init: initVar, post_data: CafePostData) -> dict:
        def getTime(timestamp):
            tm = gmtime(timestamp/1000)
            return f"{tm.tm_year}-{tm.tm_mon:02d}-{tm.tm_mday:02d}T{tm.tm_hour:02d}:{tm.tm_min:02d}:{tm.tm_sec:02d}Z"
    
        cafe_info = init.cafeData.loc[post_data.channelID]
        menu_url = (f"https://cafe.naver.com/{cafe_info['cafeID']}"
                f"?iframe_url=/ArticleList.nhn%3F"
                f"search.clubid={int(cafe_info['cafeNum'])}"
                f"%26search.menuid={post_data.menu_id}")

        embed = {
            "author": {
                "name": post_data.menu_name,
                "url": menu_url,
            },
            "color": 248125,
            "title": post_data.subject,
            "url": post_data.cafe_link,
            "thumbnail": {
                "url": quote(post_data.image, safe='/%:@&=+$,!?*\'()')
            },
            "footer": {
                "text": "cafe",
                "inline": True,
                "icon_url": iconLinkData().cafe_icon
            },
            "timestamp": getTime(post_data.write_timestamp)
        }

        return {
            "username": f"[카페 알림] {cafe_info['cafeName']} - {post_data.writer_nickname}",
            "avatar_url": self.get_cafe_thumbnail_url(init, post_data.channelID, post_data.writer_nickname),
            "embeds": [embed]
        }
    
    def get_cafe_thumbnail_url(self, init: initVar, channelID: str, writerNickname: str) -> str:
        def _get_afreeca_thumbnail(user_id, current_thumbnail):
            response = get(afreeca_getLink(user_id), headers=getChzzkHeaders(), timeout=3)
            _, _, thumbnail_url = afreeca_getChannelOffStateData(
                loads(response.text), 
                user_id, 
                current_thumbnail
            )
            return thumbnail_url

        def _get_chzzk_thumbnail(user_id, current_thumbnail):
            response = get(
                chzzk_getLink(user_id), 
                headers=getChzzkHeaders(), 
                cookies=getChzzkCookie(), 
                timeout=3
            )
            _, _, thumbnail_url = chzzk_getChannelOffStateData(
                loads(response.text)["content"], 
                user_id, 
                current_thumbnail
            )
            return thumbnail_url
        
        cafe_info = init.cafeData.loc[channelID, "cafeNameDict"][writerNickname]
        platform, user_id, current_thumbnail = cafe_info[1], cafe_info[0], cafe_info[2]
        try:
            if platform == "afreeca": 
                thumbnail_url = _get_afreeca_thumbnail(user_id, current_thumbnail)
            else: 
                thumbnail_url = _get_chzzk_thumbnail(user_id, current_thumbnail)

            init.cafeData.loc[channelID, "cafeNameDict"][writerNickname][2] = thumbnail_url
            return thumbnail_url

        except Exception as e: 
            asyncio.create_task(DiscordWebhookSender()._log_error(f"error CateJson file {e}"))
            return current_thumbnail
    
# 사용 예:
# async def main():
#     init = ... # 초기화 객체
#     cafe_post_title = getCafePostTitle()
#     await cafe_post_title.fCafeTitle(init)

# asyncio.run(main())
