import asyncio
import os
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any

from aiohttp import ClientSession, ClientError, TCPConnector
from supabase import create_client

class DiscordWebhookSender:
    def __init__(self, 
                 supabase_url: str = os.environ.get('supabase_url'),
                 supabase_key: str = os.environ.get('supabase_key'),
                 error_webhook_url: str = os.environ.get('errorPostBotURL')):

        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.error_webhook_url = error_webhook_url
        
        # Configure retry and concurrency settings
        self.MAX_RETRIES = 3
        self.MAX_CONCURRENT = 5
        self.BASE_DELAY = 0.2  # Base delay for exponential backoff
        self.TIMEOUT = 15  # Default timeout for requests

    async def send_messages(self, messages: List[Tuple[str, Dict[str, Any]]]) -> List[str]:

        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        
        async with ClientSession(connector=TCPConnector(ssl=False)) as session:
            tasks = [
                asyncio.create_task(
                    self._send_message_with_retry(session, url, data, semaphore)
                ) 
                for url, data in messages
            ]
            
            # Collect responses as tasks complete
            responses = []
            for task in asyncio.as_completed(tasks):
                result = await task
                if result is not None:
                    responses.append(result)
            
            return responses

    async def _send_message_with_retry(self, 
                                       session: ClientSession, 
                                       url: str, 
                                       data: Dict[str, Any], 
                                       semaphore: asyncio.Semaphore) -> Optional[str]:
        async with semaphore:
            for attempt in range(self.MAX_RETRIES):
                try:
                    async with session.post(url, json=data, timeout=self.TIMEOUT) as response:
                        response.raise_for_status()
                        return await response.text()
                
                except ClientError as e:
                    # Handle specific error scenarios
                    if response.status == 404:
                        await self._handle_404_error(url)
                    
                    if attempt == self.MAX_RETRIES - 1:
                        await self._handle_persistent_failure(url, e)
                    
                    # Exponential backoff
                    await asyncio.sleep(self.BASE_DELAY * (2 ** attempt))
                
                except asyncio.TimeoutError:
                    print(f"{datetime.now()} Timeout for {url}: {str(data)}")
                    
                    if attempt == self.MAX_RETRIES - 1:
                        return None
                    
                    # Exponential backoff
                    await asyncio.sleep(self.BASE_DELAY * (2 ** attempt))
                
                except Exception as e:
                    print(f"Unexpected error sending message: {e}")
                    
                    if attempt == self.MAX_RETRIES - 1:
                        return None
                    
                    # Exponential backoff
                    await asyncio.sleep(self.BASE_DELAY * (2 ** attempt))
        
        return None

    async def _handle_404_error(self, url: str):
        try:
            await self._delete_user_state_data(url)
            await self._log_error(f"Deleted user data for non-existent URL: {url}")
        except Exception as e:
            await self._log_error(f"Error handling 404: {e}")

    async def _handle_persistent_failure(self, url: str, error: Exception):

        try:
            await self._delete_user_state_data(url)
            await self._log_error(f"Persistent failure for {url}: {str(error)}")
        except Exception as e:
            await self._log_error(f"Error handling persistent failure: {e}")

    async def _delete_user_state_data(self, url: str):
        if not self.supabase_url or not self.supabase_key:
            return

        supabase = create_client(self.supabase_url, self.supabase_key)
        
        try:
            # Fetch and delete user state data
            userStateData = supabase.table('userStateData').select("*").execute()
            matching_rows = [row for row in userStateData.data if row.get('discordURL') == url]
            
            if matching_rows:
                idx = matching_rows[0].get('idx')
                supabase.table('userStateData').delete().eq('idx', idx).execute()
                
                # Optional: Update flag if needed
                # await self._update_flag(supabase, 'all_date', True)
        except Exception as e:
            await self._log_error(f"Error deleting user state data: {e}")

    async def _log_error(init, message: str, webhook_url = os.environ.get('errorPostBotURL')):

        try:
            async with ClientSession() as session:
                data = {'content': message, "username": "Error Alarm"}
                print(f"{datetime.now()} {message}")
                async with session.post(webhook_url, json=data, timeout=10) as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 1))
                        await asyncio.sleep(retry_after)
        except Exception as e:
            print(f"Failed to log error to webhook: {e}")

def get_chat_list_of_urls(init, name, chat, thumbnail_url, channel_name):
    result_urls = []
    try:
        def make_thumbnail_url():
            return {'content'   : chat,
                    "username"  : name + " >> " + channel_name,
                    "avatar_url": thumbnail_url}

        if init.DO_TEST:
            # return [(environ['errorPostBotURL'], message)]
            return result_urls
        
        for discordWebhookURL in init.userStateData['discordURL']:
            try:
                if (init.userStateData.loc[discordWebhookURL, "chat_user_json"] and 
                    name in init.userStateData.loc[discordWebhookURL, "chat_user_json"].get(channel_name, [])):
                    result_urls.append((discordWebhookURL, make_thumbnail_url()))
            except (KeyError, AttributeError):
                # 특정 URL 처리 중 오류가 발생해도 다른 URL 처리는 계속 진행
                continue
            
        return result_urls
    except Exception as e:
        asyncio.create_task(DiscordWebhookSender()._log_error(f"Error in get_chat_list_of_urls: {type(e).__name__}: {str(e)}"))
        return result_urls
    
def get_cafe_list_of_urls(init, json_data, writerNickname):
    result_urls = []
    try:
        if init.DO_TEST:
            # return [(environ['errorPostBotURL'], json_data)]
            return result_urls
        

        for discordWebhookURL in init.userStateData['discordURL']:
                try:
                    if ((init.userStateData.loc[discordWebhookURL, "cafe_user_json"] and
                    writerNickname in init.userStateData.loc[discordWebhookURL, "cafe_user_json"].get(init.channelID, []))):
                        result_urls.append((discordWebhookURL, json_data))
                except (KeyError, AttributeError):
                    # 특정 URL 처리 중 오류가 발생해도 다른 URL 처리는 계속 진행
                    continue
                
        return result_urls
    except Exception as e:
        asyncio.create_task(DiscordWebhookSender()._log_error(f"Error in get_cafe_list_of_urls: {type(e).__name__}: {str(e)}"))
        return result_urls