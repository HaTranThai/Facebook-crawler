from __future__ import annotations
from typing import Optional, Dict, List, Any, TYPE_CHECKING, ClassVar
from bs4 import BeautifulSoup
from datetime import datetime
from pytz import timezone
from requests_handler import RequestHandler
from dataclasses import dataclass, field
import asyncio


@dataclass
class UserInfo:
    user_id: str = ""
    name: str = ""
    handle: str = ""
    url: str = ""

@dataclass 
class CommentInfo:
    author: UserInfo = field(default_factory=UserInfo)
    date: str = ""
    url: str = ""
    comment_id: str = ""
    content: str = ""
    count_like: int = 0
    count_dislike: int = 0

@dataclass
class GeneralInfo:
    post_id: str = ""
    url: str = ""
    post_type: str = ""
    title: str = ""
    content: str = ""
    author: UserInfo = field(default_factory=UserInfo)
    date: str = ""
    media: str = ""
    count_like: int = 0
    count_dislike: int = 0
    count_comment: int = 0
    comments: List[CommentInfo] = field(default_factory=list)
    count_share: int = 0
    count_view: int = 0

class FacebookScraper:
    url: Optional[str]
    
    def __init__(self, 
            proxies=None,
        ):
        self.request_handler = RequestHandler(proxies=proxies)
        self.general_info: Dict[str, Optional[str]] = {}

    async def scrape(self, url: str) -> Optional[Dict[str, Optional[str]]]:
        try:
            if 'm.facebook.com' in url:
                url = url.replace('m.facebook.com', 'www.facebook.com')
            html_content = self.request_handler.fetch_html(url)
            # Kiểm tra URL thực tế sau khi tải trang
            actual_url = self.request_handler.get_final_url()
            # Sử dụng URL thực tế để xác định loại nội dung
            if 'login' not in actual_url:
                if '/posts/' in actual_url or 'permalink.php' in actual_url: 
                    self.general_info = await self.extract_general_info_post(html_content, url=actual_url)
                elif any(x in actual_url for x in ["/videos/", "/watch/"]):
                    try:
                        self.general_info = await self.extract_general_info_video(html_content)
                        # Nếu không lấy được dữ liệu từ video, thử reel
                        if not self.general_info:
                            print("Video extraction failed, trying reel extraction...")
                            self.general_info = await self.extract_general_info_reel(html_content)
                    except Exception as e:
                        print(f"Video extraction error: {e}, trying reel extraction...")
                        self.general_info = await self.extract_general_info_reel(html_content)
                elif '/reel/' in actual_url:
                    self.general_info = await self.extract_general_info_reel(html_content)
                else:
                    print(f"Unsupported URL: {actual_url} (original: {url})")
                    return {
                        "success": False,
                        "message": "Unsupported URL"
                    }
            else:
                if '/posts/' in actual_url or 'permalink.php' in actual_url: 
                    self.general_info = await self.extract_general_info_post(html_content)
                elif any(x in url for x in ["/videos/", "/watch/"]):
                    try:
                        self.general_info = await self.extract_general_info_video(html_content)
                        # Nếu không lấy được dữ liệu từ video, thử reel
                        if not self.general_info:
                            print("Video extraction failed, trying reel extraction...")
                            self.general_info = await self.extract_general_info_reel(html_content)
                    except Exception as e:
                        print(f"Video extraction error: {e}, trying reel extraction...")
                        self.general_info = await self.extract_general_info_reel(html_content)
                elif '/reel/' in url:
                    self.general_info = await self.extract_general_info_reel(html_content)
                else:
                    print(f"Unsupported URL: {url} (original: {url})")
                    return {
                        "success": False,
                        "message": "Unsupported URL"
                    }
            return {
                "success": True,
                "data": self.general_info
            }
        except Exception as e:
            print(f"Error in scrape: {e}")
            return {
                "success": False,
                "message": f"Error in scrape: {e}"
            }

    async def extract_general_info_post(self, html_content: BeautifulSoup, url: str) -> Optional[GeneralInfo]:
        general_info = GeneralInfo(post_type="post")
        try:
            # Try to extract title from RunWWW
            try:
                general_info_json = self.request_handler.parse_json_from_html(html_content, "RunWWW")
                requires = general_info_json.get("require", [])
                if not requires:
                    title = ''
                else:
                    requires = requires[0][3][0].get("__bbox", {}).get("require", [])

                    relay_prefetched_stream_cache = None
                    for item in requires:
                        if "CometPlatformRootClient" in item:
                            relay_prefetched_stream_cache = item
                            break

                    if relay_prefetched_stream_cache:
                        title = relay_prefetched_stream_cache[3][0].get('initialRouteInfo', {}).get('route', {}).get('meta', {}).get('title', '')
                    else:
                        title = ''
            except Exception as e:
                title = ''
            
            try:
                general_info_json = self.request_handler.parse_json_from_html(html_content, "CometEmoji.react")
            except Exception as e:
                print(f"Error parsing JSON: {e}")
                general_info_json = self.request_handler.parse_json_from_html(html_content, "CometEmoji.react")

            requires = general_info_json.get("require", [])
            if not requires:
                raise ValueError("Missing 'require' key in JSON data.")
            requires = requires[0][3][0].get("__bbox", {}).get("require", [])

            for item in requires:
                if 'RelayPrefetchedStreamCache' in item:
                    relay_prefetched_stream_cache = item
                    break
                else:
                    relay_prefetched_stream_cache = None

            content = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {}).get('node', {}).get('comet_sections', {}).get('content', {})
            if content:
                text = content.get('story', {}).get('message', {}).get('text', []) if content.get('story', {}).get('message', {}) else None
                id = content.get('story', {}).get('post_id', [])
                url = content.get('story', {}).get('wwwURL', '')
                user = content.get('story', {}).get('actors', [])[0]
                user_id = user.get('id', [])
                user_name = user.get('name', [])
                user_url = "https://www.facebook.com/" + user_id
            else:
                text = id = url = user_id = user_name = user_url = ""
                count_like = count_dislike = comment_count = share_count = 0

            feedback = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {}).get('node', {}).get('comet_sections', []).get('feedback', []).get('story', []).get('story_ufi_container', []).get('story', []).get('feedback_context', []).get('feedback_target_with_context', [])
            if feedback:
                id_comment = feedback.get('id', '')
                comment_count = feedback.get('comment_list_renderer', {}).get('feedback', []).get('comment_rendering_instance', {}).get('comments', {}).get('total_count', {})
                count_like = feedback.get('comet_ufi_summary_and_actions_renderer', {}).get('feedback', {}).get('reaction_count', {}).get('count', 0)
                reaction_details = feedback.get('comet_ufi_summary_and_actions_renderer', {}).get('feedback', {}).get('top_reactions', {}).get('edges', [])
                count_dislike = next((r['reaction_count'] for r in reaction_details if r['node']['localized_name'] == 'Phẫn nộ'), 0)
                share_count = int(feedback.get('comet_ufi_summary_and_actions_renderer', {}).get('feedback', {}).get('share_count', 0).get('count', 0))
                all_comments_json = self.request_handler.fetch_all_comments(id_comment)
                comments = []
                for comment_json in all_comments_json:
                    comment_info = await self.extract_comment(comment_json)
                    comments.append(comment_info)
            else:
                comment_count = count_like = count_dislike = share_count = 0

            created_time = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {}).get('node', {}).get('comet_sections', {}).get('context_layout', {}).get('story', {}).get('comet_sections', {}).get('metadata', {})[0].get('story', {}).get('creation_time', None)
            if created_time:
                vn_timezone = timezone("Asia/Ho_Chi_Minh")
                created_time = datetime.fromtimestamp(created_time, tz=vn_timezone)
                created_time = created_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                created_time = None

            medias = content.get('story', {}).get('attachments', {})[0].get('styles', {}).get('attachment', {}) if content.get('story', {}).get('attachments', {}) else {}
            if medias.get('all_subattachments', {}).get('nodes', []):
                medias = [media.get('media', {}).get('image', {}).get('uri', '') for media in medias.get('all_subattachments', {}).get('nodes', [])]
            elif medias.get('media', {}).get('placeholder_image', {}):
                medias = [medias.get('media', {}).get('placeholder_image', {}).get('uri', '')]
            else:
                medias = []

            general_info.post_id = id or ""
            general_info.url = url or ""
            general_info.post_type = 'post'
            general_info.title = title or ""
            general_info.content = text or ""
            general_info.author = UserInfo(user_id=user_id or "", name=user_name or "", handle="", url=user_url or "")
            general_info.date = created_time or ""
            general_info.count_like = count_like or 0
            general_info.count_dislike = count_dislike or 0
            general_info.count_comment = comment_count or 0
            general_info.count_share = share_count or 0
            general_info.count_view = 0
            general_info.comments = comments or []
            general_info.media = ""
            return general_info
        except Exception as e:
            print(f"Error extracting general info: {e}")
            return None

    async def extract_general_info_video(self, html_content: BeautifulSoup) -> Optional[GeneralInfo]:
        general_info = GeneralInfo(post_type="video")
        try:
            # First try to get basic info from CometTooltipCompatibilityComponent
            general_info_json = self.request_handler.parse_json_from_html(html_content, "reaction_count")
            requires = general_info_json.get("require", [])
            if not requires:
                raise ValueError("Missing 'require' key in JSON data.")
            requires = requires[0][3][0].get("__bbox", {}).get("require", [])

            for item in requires:
                if 'RelayPrefetchedStreamCache' in item:
                    relay_prefetched_stream_cache = item
                    break
                else:
                    relay_prefetched_stream_cache = None

            content = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {})
            if content:
                id = content.get('id', [])
                title = content.get('title', {}).get('text', '')
                commnet_count = int(content.get('feedback', {}).get('total_comment_count', 0))
                id_comment = content.get('feedback', {}).get('id', '')
                count_like = content.get('feedback', {}).get('reaction_count', {}).get('count', 0)
                count_view = content.get('feedback', {}).get('video_view_count_renderer', {}).get('feedback', {}).get('play_count', 0)
                text = content.get('creation_story', {}).get('message', {}).get('text', '') if content.get('creation_story', {}).get('message', {}) else None
                user_name = content.get('owner', {}).get('owner_as_page', {}).get('name', '')
                user_id = content.get('owner', {}).get('owner_as_page', {}).get('id', '')
                user_url = "https://www.facebook.com/" + user_id
                all_comments_json = self.request_handler.fetch_all_comments(id_comment)
                comments = []
                for comment_json in all_comments_json:
                    comment_info = await self.extract_comment(comment_json)
                    comments.append(comment_info)
            else:
                id = title = text = user_name = user_id = user_url = ""
                commnet_count = count_like = count_view = count_dislike = share_count = 0

            # Get creation time and URL
            general_info_json = self.request_handler.parse_json_from_html(html_content, "creation_time")
            requires = general_info_json.get("require", [])
            if not requires:
                raise ValueError("Missing 'require' key in JSON data.")
            requires = requires[0][3][0].get("__bbox", {}).get("require", [])

            for item in requires:
                if 'RelayPrefetchedStreamCache' in item:
                    relay_prefetched_stream_cache = item
                    break
                else:
                    relay_prefetched_stream_cache = None
            created_time = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {}).get('attachments', {})[0].get('media').get('creation_story', {}).get('comet_sections', {}).get('metadata', {})[0].get('story', {}).get('creation_time', None)
            if created_time:
                vn_timezone = timezone("Asia/Ho_Chi_Minh")
                created_time = datetime.fromtimestamp(created_time, tz=vn_timezone)
                created_time = created_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                created_time = None
            url = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {}).get('attachments', {})[0].get('media').get('creation_story', {}).get('comet_sections', {}).get('metadata', {})[0].get('story', {}).get('url', None)

            general_info_json = self.request_handler.parse_json_from_html(html_content, "VideoPlayerShakaPerformanceLoggerConfig")
            requires = general_info_json.get("require", [])
            if not requires:
                raise ValueError("Missing 'require' key in JSON data.")
            requires = requires[0][3][0].get("__bbox", {}).get("require", [])

            for item in requires:
                if 'RelayPrefetchedStreamCache' in item:
                    relay_prefetched_stream_cache = item
                    break
                else:
                    relay_prefetched_stream_cache = None
                    
            media = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {}).get('video', {}).get('story', {}).get('attachments', {})[0].get('media', {}).get('videoDeliveryLegacyFields', {}).get('browser_native_hd_url', {})
                
            general_info.post_id = id or ""
            general_info.url = url or ""
            general_info.post_type = 'video'
            general_info.title = title or ""
            general_info.content = text or ""
            general_info.author = UserInfo(user_id=user_id or "", name=user_name or "", handle="", url=user_url or "")
            general_info.date = created_time or ""
            general_info.count_like = count_like or 0
            general_info.count_dislike = 0
            general_info.count_comment = commnet_count or 0
            general_info.count_share = 0
            general_info.count_view = count_view or 0
            general_info.comments = comments or []
            general_info.media = media or ""
            
            return general_info
        except Exception as e:
            print(f"Error extracting general info: {e}")
            return None
        
    async def extract_general_info_reel(self, html_content: BeautifulSoup) -> Optional[GeneralInfo]:
        general_info = GeneralInfo(post_type="reel")
        try:
            general_info_json = self.request_handler.parse_json_from_html(html_content, "VideoPlayerShakaPerformanceLoggerConfig")
            requires = general_info_json.get("require", [])
            if not requires:
                raise ValueError("Missing 'require' key in JSON data.")
            requires = requires[0][3][0].get("__bbox", {}).get("require", [])

            for item in requires:
                if 'RelayPrefetchedStreamCache' in item:
                    relay_prefetched_stream_cache = item
                    break
                else:
                    relay_prefetched_stream_cache = None

            content = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {}).get('video', {})
            if content:
                id = content.get('id', '')
                media = content.get('creation_story', {}).get('short_form_video_context', {}).get('playback_video', {}).get('videoDeliveryLegacyFields', {}).get('browser_native_hd_url', '')
                created_time = content.get('creation_story', {}).get('creation_time', None)
                if created_time:
                    vn_timezone = timezone("Asia/Ho_Chi_Minh")
                    created_time = datetime.fromtimestamp(created_time, tz=vn_timezone)
                    created_time = created_time.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    created_time = None
                text = content.get('creation_story', {}).get('message', {}).get('text', '') if content.get('creation_story', {}).get('message', {}) else None
            else:
                id = media = text = created_time = ""
                count_like = comment_count = share_count = 0

            general_info_json = self.request_handler.parse_json_from_html(html_content, "share_count_reduced")
            requires = general_info_json.get("require", [])
            if not requires:
                raise ValueError("Missing 'require' key in JSON data.")
            requires = requires[0][3][0].get("__bbox", {}).get("require", [])

            for item in requires:
                if 'RelayPrefetchedStreamCache' in item:
                    relay_prefetched_stream_cache = item
                    break
                else:
                    relay_prefetched_stream_cache = None

            data_video = relay_prefetched_stream_cache[3][1].get("__bbox", {}).get("result", {}).get("data", {})
            if data_video:
                url = data_video.get('short_form_video_context', {}).get('shareable_url', {}) if data_video.get('short_form_video_context') else data_video.get('attachments', {})[0].get('media', {}).get('shareable_url', {})
                user_name = data_video.get('short_form_video_context', {}).get('video_owner', {}).get('name', '') if data_video.get('short_form_video_context') else data_video.get('attachments', {})[0].get('media', {}).get('owner', {}).get('name', '')
                user_id = data_video.get('short_form_video_context', {}).get('video_owner', {}).get('id', '') if data_video.get('short_form_video_context') else data_video.get('attachments', {})[0].get('media', {}).get('owner', {}).get('id', '')
                user_url = "https://www.facebook.com/" + user_id
                count_like = data_video.get('fb_reel_react_button', {}).get('story', {}).get('feedback', {}).get('unified_reactors', {}).get('count', 0)
                comment_count = int(data_video.get('feedback', {}).get('total_comment_count', 0))
                id_comment = data_video.get('feedback', {}).get('id', '')
                share_count = int(data_video.get('feedback', {}).get('share_count_reduced', 0))
                all_comments_json = self.request_handler.fetch_all_comments(id_comment)
                comments = []
                for comment_json in all_comments_json:
                    comment_info = await self.extract_comment(comment_json)
                    comments.append(comment_info)
            else:
                url = user_name = user_id = user_url = ""
                count_like = comment_count = share_count = 0
            
            general_info.post_id = id or ""
            general_info.url = url or ""
            general_info.post_type = 'reel'
            general_info.title = text or ""
            general_info.content = text or ""
            general_info.author = UserInfo(user_id=user_id or "", name=user_name or "", handle="", url=user_url or "")
            general_info.date = created_time or ""
            general_info.count_like = count_like or 0
            general_info.count_dislike = 0
            general_info.count_comment = comment_count or 0
            general_info.count_share = share_count or 0
            general_info.count_view = 0
            general_info.comments = comments or []
            general_info.media = media or ""
            return general_info
        except Exception as e:
            print(f"Error extracting general info: {e}")
            return None

    async def extract_comment(self, comment_json: Dict[str, Any]) -> Optional[CommentInfo]:
        try:
            node = comment_json.get('node', {})
            
            # Extract comment ID
            comment_id = node.get('legacy_fbid', '')
            
            # Extract and format creation time
            created_time = node.get('created_time')
            if created_time:
                vn_timezone = timezone("Asia/Ho_Chi_Minh")
                created_time = datetime.fromtimestamp(created_time, tz=vn_timezone)
                created_time = created_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                created_time = ''
            
            # Extract comment URL
            comment_url = node.get('feedback', {}).get('url', '')
            
            # Extract comment content
            content = node.get('body', {}).get('text', '') if node.get('body') is not None else ''
            
            # Extract reaction counts
            feedback = node.get('feedback', {})
            total_reactors = int(feedback.get('reactors', {}).get('count_reduced', 0))
            
            # Check for anger reaction (ID: 444813342392137)
            top_reactions = feedback.get('top_reactions', {}).get('edges', [])
            anger_reaction = None
            for reaction in top_reactions:
                if reaction.get('node', {}).get('id') == '444813342392137':
                    anger_reaction = reaction
                    break
            
            if anger_reaction:
                count_dislike = int(anger_reaction.get('reaction_count', 0))
                count_like = total_reactors - count_dislike
            else:
                count_like = total_reactors
                count_dislike = 0
            
            # Extract author information
            author = node.get('author', {})
            user_id = author.get('id', '')
            name = author.get('name', '')
            author_url = author.get('url', '')
            
            # Create UserInfo object
            user_info = UserInfo(
                user_id=user_id,
                name=name,
                handle="",
                url=author_url
            )
            
            # Create and return CommentInfo object
            comment_info = CommentInfo(
                author=user_info,
                date=created_time,
                url=comment_url,
                comment_id=comment_id,
                content=content,
                count_like=count_like,
                count_dislike=count_dislike
            )
            
            return comment_info
            
        except Exception as e:
            print(f"Error extracting comment: {e}")
            return None