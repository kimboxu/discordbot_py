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
from base import getChzzkHeaders, saveCafeData, async_errorPost, async_post_message, subjectReplace, getChzzkCookie, afreeca_getChannelOffStateData, chzzk_getChannelOffStateData, get_message, cafeVarData, iconLinkData, initVar

@dataclass
class CafePostData:
    cafe_link: str
    menu_id: str
    menu_name: str
    subject: str
    image: str
    write_timestamp: str
    cafe_id: str
    writer_nickname: str
    
class getCafePostTitle:
    async def fCafeTitle(self, init: initVar, cafeVar: cafeVarData):
        for _ in range(1):
            try:
                cafeVar.cafeChannelIdx = (cafeVar.cafeChannelIdx + 1) % len(list(init.cafeData["channelID"]))
                cafeID = list(init.cafeData["channelID"])[cafeVar.cafeChannelIdx]
                cafe_posts = await self.getCafeDataDic(init, cafeID)
                
                if cafe_posts:
                    await self.postCafe(init, cafe_posts, cafeID)
                    
            except Exception as e:
                asyncio.create_task(async_errorPost(f"error cafe {cafeID}.{e}"))

    async def getCafeDataDic(self, init: initVar, cafeID)-> list:
        cafe_posts = []  

        try:
            list_of_urls = self.get_article_list(init.cafeData.loc[cafeID, 'cafeNum']) 
            response_list = await get_message(list_of_urls, "cafe")
        except:
            return []

        newTime = 0   
        cafe_json_ref_articles = init.cafeData.loc[cafeID, 'cafe_json']["refArticleId"]
        max_ref_article = max(cafe_json_ref_articles)
        update_time = int(init.cafeData.loc[cafeID, 'update_time'])
        cafe_name_dict = init.cafeData.loc[cafeID, "cafeNameDict"]
        cafe_id = init.cafeData.loc[cafeID, 'cafeID']

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
                        newTime = max(newTime, Article["writeDateTimestamp"])

                        # Update cafe_json refArticleId list
                        if len(cafe_json_ref_articles) >= 10:
                            cafe_json_ref_articles[:-1] = cafe_json_ref_articles[1:]
                            cafe_json_ref_articles[-1] = Article["refArticleId"]
                        else:
                            cafe_json_ref_articles.append(Article["refArticleId"])

                        # CafePostData 객체 생성
                        cafe_post = CafePostData(
                            cafe_link=f"https://cafe.naver.com/{cafe_id}/{Article['refArticleId']}",
                            menu_id=Article["menuId"],
                            menu_name=Article["menuName"],
                            subject=Article["subject"],
                            image=Article.get("representImage", ""),
                            write_timestamp=Article["writeDateTimestamp"],
                            cafe_id=cafeID,
                            writer_nickname=Article["writerNickname"]
                        )
                        cafe_posts.append(cafe_post)
                except:
                    continue

        if newTime:
            init.cafeData.loc[cafeID, 'update_time'] = newTime

        return cafe_posts
    
    def get_article_list(self, cafe_unique_id: str, page_num: int = 1) -> list:
        BASE_URL = "https://apis.naver.com/cafe-web/cafe2/ArticleListV2dot1.json"
        
        params = {
            'search.queryType': 'lastArticle',
            'ad': 'False',
            'search.clubid': str(cafe_unique_id),
            'search.page': str(page_num)
        }
        
        return [(BASE_URL, params)]
    
    async def postCafe(self, init: initVar, cafe_posts: list, channelID: list):
        try:
            async def process_cafe_item(post_data: CafePostData):
                json_data = self.create_cafe_json(init, post_data)
                print(f"{datetime.now()} {post_data.writer_nickname} post cafe")
                
                return await async_post_message(
                    make_list_of_cafe(json_data, post_data.writer_nickname)
                )
            
            def make_list_of_cafe(json_data, writerNickname):
                if init.DO_TEST:
                    return [(environ['errorPostBotURL'], json_data)]
                
                return [
                    (discordURL, json_data)
                    for discordURL in init.userStateData['discordURL']
                    if init.userStateData.loc[discordURL, "cafe_user_json"] and 
                    writerNickname in init.userStateData.loc[discordURL, "cafe_user_json"].get(channelID, [])
                ]

            # 모든 항목을 동시에 처리
            if cafe_posts:
                await asyncio.gather(
                    *(process_cafe_item(post) for post in cafe_posts)
                )
                
            saveCafeData(init, channelID)
            
        except Exception as e:
            asyncio.create_task(async_errorPost(f"error postCafe {e}"))

    def create_cafe_json(self, init: initVar, post_data: CafePostData) -> dict:
        def getTime(timestamp):
            tm = gmtime(timestamp/1000)
            return f"{tm.tm_year}-{tm.tm_mon:02d}-{tm.tm_mday:02d}T{tm.tm_hour:02d}:{tm.tm_min:02d}:{tm.tm_sec:02d}Z"
    
        cafe_info = init.cafeData.loc[post_data.cafe_id]
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
            "avatar_url": self.get_cafe_thumbnail_url(init, post_data.cafe_id, post_data.writer_nickname),
            "embeds": [embed]
        }
    
    def get_cafe_thumbnail_url(self, init: initVar, cafeID: str, writerNickname: str) -> str:
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
        
        cafe_info = init.cafeData.loc[cafeID, "cafeNameDict"][writerNickname]
        platform, user_id, current_thumbnail = cafe_info[1], cafe_info[0], cafe_info[2]
        try:
            if platform == "afreeca": 
                thumbnail_url = _get_afreeca_thumbnail(user_id, current_thumbnail)
            else: 
                thumbnail_url = _get_chzzk_thumbnail(user_id, current_thumbnail)

            init.cafeData.loc[cafeID, "cafeNameDict"][writerNickname][2] = thumbnail_url
            return thumbnail_url

        except Exception as e: 
            asyncio.create_task(async_errorPost(f"error CateJson file {e}"))
            return current_thumbnail
    
# 사용 예:
# async def main():
#     init = ... # 초기화 객체
#     cafe_post_title = getCafePostTitle()
#     await cafe_post_title.fCafeTitle(init)

# asyncio.run(main())
