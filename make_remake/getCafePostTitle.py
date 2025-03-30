import asyncio
from os import environ
from json import loads
from time import gmtime
from requests import get
from datetime import datetime
from urllib.parse import quote
from supabase import create_client
from dataclasses import dataclass
from Chzzk_live_message import chzzk_getLink
from Afreeca_live_message import afreeca_getLink
from discord_webhook_sender import DiscordWebhookSender, get_cafe_list_of_urls
from base import getChzzkHeaders, subjectReplace, getChzzkCookie, afreeca_getChannelOffStateData, chzzk_getChannelOffStateData, get_message, iconLinkData, initVar
@dataclass
class CafePostData:
    cafe_link: str
    menu_id: str
    menu_name: str
    subject: str
    image: str
    write_timestamp: str
    writer_nickname: str
    
class getCafePostTitle:
    def __init__(self, init_var: initVar):
        self.DO_TEST: bool = init_var.DO_TEST
        self.userStateData = init_var.userStateData
        self.cafeData = init_var.cafeData
        self.channelID: str = ""

    async def start(self, channel_id):
        try:
            self.message_list: list = []
            self.channelID = channel_id
            await self.getCafeDataDic()
            await self.postCafe()
                
        except Exception as e:
            asyncio.create_task(DiscordWebhookSender()._log_error(f"error cafe {self.channelID}.{e}"))

    # async def getCafeDataDic(self):
    #     try:
    #         response_list = await get_message(self.get_article_list() , "cafe")
    #     except:
    #         return

    #     cafe_json_ref_articles = self.cafeData.loc[self.channelID, 'cafe_json']["refArticleId"]
    #     max_ref_article = max(cafe_json_ref_articles)
    #     update_time = int(self.cafeData.loc[self.channelID, 'update_time'])
    #     cafe_name_dict = self.cafeData.loc[self.channelID, "cafeNameDict"]

    #     for response in response_list:
    #         for request, is_success in response:
    #             if not is_success: 
    #                 continue
                
    #             try:
    #                 for Article in request['message']['result']['articleList'][::-1]:
    #                     if Article["writerNickname"] not in cafe_name_dict.keys():
    #                         continue

    #                     if not (Article["refArticleId"] > max_ref_article and 
    #                             Article['writeDateTimestamp'] > update_time):
    #                         continue

    #                     Article["subject"] = subjectReplace(Article["subject"])
    #                     self.cafeData.loc[self.channelID, 'update_time'] = max(self.cafeData.loc[self.channelID, 'update_time'], Article["writeDateTimestamp"])

    #                     # Update cafe_json refArticleId list
    #                     if len(cafe_json_ref_articles) >= 10:
    #                         cafe_json_ref_articles[:-1] = cafe_json_ref_articles[1:]
    #                         cafe_json_ref_articles[-1] = Article["refArticleId"]
    #                     else:
    #                         cafe_json_ref_articles.append(Article["refArticleId"])

    #                     # CafePostData 객체 생성
    #                     cafe_post = CafePostData(
    #                         cafe_link=f"https://cafe.naver.com/{self.channelID}/{Article['refArticleId']}",
    #                         menu_id=Article["menuId"],
    #                         menu_name=Article["menuName"],
    #                         subject=Article["subject"],
    #                         image=Article.get("representImage", ""),
    #                         write_timestamp=Article["writeDateTimestamp"],
    #                         writer_nickname=Article["writerNickname"]
    #                     )
    #                     self.message_list.append(cafe_post)
    #             except:
    #                 continue


    async def getCafeDataDic(self):
        try:
            response_list = await get_message(self.get_article_list(), "cafe")
        except Exception as e:
            asyncio.create_task(DiscordWebhookSender()._log_error(f"카페 데이터 조회 중 오류 발생: {e}"))
            return
        
        cafe_json = self.cafeData.loc[self.channelID, 'cafe_json']
        cafe_json_ref_articles = cafe_json["refArticleId"]
        max_ref_article = max(cafe_json_ref_articles) if cafe_json_ref_articles else 0
        update_time = int(self.cafeData.loc[self.channelID, 'update_time'])
        cafe_name_dict = self.cafeData.loc[self.channelID, "cafeNameDict"]
        
        for response in response_list:
            for request, is_success in response:
                if not is_success:
                    continue
                
                try:
                    self._process_article_list(request, cafe_name_dict, max_ref_article, update_time, cafe_json_ref_articles)
                except Exception as e:
                    asyncio.create_task(DiscordWebhookSender()._log_error(f"게시글 처리 중 오류 발생: {e}"))
                    continue
        
        return

    def _process_article_list(self, request, cafe_name_dict, max_ref_article, update_time, cafe_json_ref_articles):
        """
        Args:
            request: API 요청 결과
            cafe_name_dict: 필터링할 작성자 닉네임 딕셔너리
            max_ref_article: 가장 최근의 현재까지의 게시글 ID
            update_time: 마지막 업데이트 시간
            cafe_json_ref_articles: 게시글 ID 목록들
        """
        article_list = request.get('message', {}).get('result', {}).get('articleList', [])
        
        for article in reversed(article_list):
            # 필터링 조건 확인
            if article["writerNickname"] not in cafe_name_dict:
                continue
                
            if not (article["refArticleId"] > max_ref_article and article['writeDateTimestamp'] > update_time):
                continue
            
            # 게시글 제목 정리
            article["subject"] = subjectReplace(article["subject"])
            
            # 최신 업데이트 시간 갱신
            self.cafeData.loc[self.channelID, 'update_time'] = max(
                self.cafeData.loc[self.channelID, 'update_time'], 
                article["writeDateTimestamp"]
            )
            
            # 참조 게시글 ID 목록 업데이트 (최대 10개 유지)
            self._update_ref_article_ids(cafe_json_ref_articles, article["refArticleId"])
            
            # 카페 게시글 데이터 객체 생성 및 추가
            self._add_cafe_post(article)

    def _update_ref_article_ids(self, ref_article_ids, new_id):
        """
        Args:
            ref_article_ids: 참조 게시글 ID 목록
            new_id: 추가할 새 게시글 ID
        """
        if len(ref_article_ids) >= 10:
            ref_article_ids[:-1] = ref_article_ids[1:]
            ref_article_ids[-1] = new_id
        else:
            ref_article_ids.append(new_id)

    def _add_cafe_post(self, article):
        """
        Args:
            article: 게시글 데이터
        """
        cafe_post = CafePostData(
            cafe_link=f"https://cafe.naver.com/{self.channelID}/{article['refArticleId']}",
            menu_id=article["menuId"],
            menu_name=article["menuName"],
            subject=article["subject"],
            image=article.get("representImage", ""),
            write_timestamp=article["writeDateTimestamp"],
            writer_nickname=article["writerNickname"]
        )
        self.message_list.append(cafe_post)

    def get_article_list(self, page_num: int = 1) -> list:
        BASE_URL = "https://apis.naver.com/cafe-web/cafe2/ArticleListV2dot1.json"
        
        params = {
            'search.queryType': 'lastArticle',
            'ad': 'False',
            'search.clubid': str(self.cafeData.loc[self.channelID, 'cafeNum']),
            'search.page': str(page_num)
        }
        
        return [(BASE_URL, params)]
    
    async def postCafe(self):
        try:
            if not self.message_list:
                return
            
            tasks = []
            for post_data in self.message_list:
                json_data = self.create_cafe_json(post_data)
                print(f"{datetime.now()} {post_data.writer_nickname} post cafe")
                task = DiscordWebhookSender().send_messages(get_cafe_list_of_urls(self.DO_TEST, self.userStateData, self.channelID, json_data, post_data.writerNickname))
                tasks.append(task)
            
            if tasks:
                await asyncio.gather(*tasks)
            
            self.saveCafeData()
            
        except Exception as e:
            asyncio.create_task(DiscordWebhookSender()._log_error(f"error postCafe {e}"))

    def create_cafe_json(self, post_data: CafePostData) -> dict:
        def getTime(timestamp):
            tm = gmtime(timestamp/1000)
            return f"{tm.tm_year}-{tm.tm_mon:02d}-{tm.tm_mday:02d}T{tm.tm_hour:02d}:{tm.tm_min:02d}:{tm.tm_sec:02d}Z"
    
        cafe_info = self.cafeData.loc[self.channelID]
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
            "avatar_url": self.get_cafe_thumbnail_url(post_data.writer_nickname),
            "embeds": [embed]
        }
        
    async def get_cafe_thumbnail_url(self, writerNickname: str) -> str:
        # 미리 정의된 플랫폼별 API 요청 구성
        platform_config = {
            "afreeca": {
                "get_link": afreeca_getLink,
                "process_data": afreeca_getChannelOffStateData,
                "needs_cookies": False
            },
            "chzzk": {
                "get_link": chzzk_getLink,
                "process_data": chzzk_getChannelOffStateData,
                "needs_cookies": True,
                "content_path": lambda data: data["content"]
            }
        }
        
        try:
            # 작성자 정보 가져오기
            if writerNickname not in self.cafeData.loc[self.channelID, "cafeNameDict"]:
                return environ['default_thumbnail']  # 기본 썸네일
                
            cafe_info = self.cafeData.loc[self.channelID, "cafeNameDict"][writerNickname]
            platform, user_id, current_thumbnail = cafe_info[1], cafe_info[0], cafe_info[2]
            
            # 플랫폼 설정 가져오기
            if platform not in platform_config:
                return current_thumbnail
                
            config = platform_config[platform]
            
            # API 요청 준비
            headers = getChzzkHeaders()
            request_kwargs = {
                "headers": headers,
                "timeout": 3
            }
            
            # 쿠키가 필요한 경우 추가
            if config["needs_cookies"]:
                request_kwargs["cookies"] = getChzzkCookie()
            
            # API 요청 실행
            response = await asyncio.to_thread(
                get, 
                config["get_link"](user_id), 
                **request_kwargs
            )
            
            # 응답 데이터 처리
            data = loads(response.text)
            if "content_path" in config:
                data = config["content_path"](data)
                
            # 썸네일 URL 추출
            _, _, thumbnail_url = config["process_data"](
                data,
                user_id,
                current_thumbnail
            )
            
            # 새 썸네일 URL 저장
            self.cafeData.loc[self.channelID, "cafeNameDict"][writerNickname][2] = thumbnail_url
            return thumbnail_url

        except Exception as e:
            error_msg = f"카페 썸네일 가져오기 실패 (작성자: {writerNickname}, 플랫폼: {platform if 'platform' in locals() else 'unknown'}): {str(e)}"
            asyncio.create_task(DiscordWebhookSender()._log_error(error_msg))
            
            # 오류 발생 시 기존 썸네일 반환
            return current_thumbnail if 'current_thumbnail' in locals() else None
            
    def saveCafeData(self):
        try:
            # 인덱스 생성을 dict comprehension으로 더 간단하게
            idx = {cafe: i for i, cafe in enumerate(self.cafeData["channelID"])}
            
            # 데이터 준비를 별도로 하여 가독성 향상
            cafe_data = {
                "idx": idx[self.channelID],
                "update_time": int(self.cafeData.loc[self.channelID, 'update_time']),
                "cafe_json": self.cafeData.loc[self.channelID, 'cafe_json'],
                "cafeNameDict": self.cafeData.loc[self.channelID, 'cafeNameDict']
            }
            
            # supabase 클라이언트 생성
            supabase = create_client(environ['supabase_url'], environ['supabase_key'])
            
            # 데이터 저장
            supabase.table('cafeData').upsert(cafe_data).execute()
            
        except Exception as e:
            asyncio.create_task(DiscordWebhookSender()._log_error(f"error save cafe time {e}"))
    
# 사용 예:
# async def main():
#     init = ... # 초기화 객체
#     cafe_post_title = getCafePostTitle()
#     await cafe_post_title.fCafeTitle(init)

# asyncio.run(main())
