import requests
import re
import time
from typing import Dict, List, Any
import os
import random
from dataclasses import dataclass, field
from typing import Optional, Any, List

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

    
def load_user_agents():
    USER_AGENT = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.92',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    ]

    # Load the list of valid user agents from the install folder.
    # The search order is:
    #   * user_agents.txt.gz
    #   * user_agents.txt
    #   * default user agent
    try:
        install_folder = os.path.abspath(os.path.split(__file__)[0])
        try:
            user_agents_file = os.path.join(install_folder, 'user_agents.txt.gz')
            import gzip
            fp = gzip.open(user_agents_file, 'rb')
            try:
                user_agents_list = [_.decode('utf-8').strip() for _ in fp.readlines()]
            finally:
                fp.close()
                del fp
        except Exception:
            user_agents_file = os.path.join(install_folder, 'user_agents.txt')
            with open(user_agents_file, 'r') as fp:
                user_agents_list = [_.strip() for _ in fp.readlines()]
    except Exception:
        user_agents_list = USER_AGENT

    return user_agents_list

USER_AGENTS = load_user_agents()

class FacebookScraperAPI:
    def __init__(self, proxies=None, access_token=None, cookie=None):
        self.session = requests.Session()
        self.logger = self.setup_logger()
        self.proxies = proxies
        self.set_proxies()
        self.user_agents = USER_AGENTS
        self.cookie = cookie
        self.access_token = access_token
        # Store the random headers once for reuse
        self.headers = self.get_random_headers()

    def setup_logger(self):
        import logging
        logger = logging.getLogger("FacebookScraper")
        
        # Remove all existing handlers to avoid duplicates
        if logger.hasHandlers():
            logger.handlers.clear()
            
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def set_proxies(self):
        if self.proxies:
            self.session.proxies.update(self.proxies)

    def check_facebook_error(self, response) -> dict:
        if response.status_code != 200:
            try:
                data = response.json()
                if "error" in data:
                    return {"success": False, "error": {"message": data["error"]["message"]}}
                else:
                    return {
                        "success": False,
                        "error": {
                            "message": f"Unknown error. Status code: {response.status_code}",
                            "detail": data
                        }
                    }
            except Exception as e:
                return {
                    "success": False,
                    "error": {
                        "message": f"Cannot parse error JSON. Status code: {response.status_code}",
                        "detail": str(e)
                    }
                }
        try:
            data = response.json()
            if "error" in data:
                return {"success": False, "error": {"message": data["error"]["message"]}}
            return {"success": True, "data": data}
        except Exception as e:
            return {
                "success": False,
                "error": {
                    "message": "Failed to parse JSON from response although status_code=200",
                    "detail": str(e)
                }
            }

    def get_random_headers(self):
        """Generate random headers for requests based on OS and browser combinations"""
        # Define OS and browser combinations
        os_browser_combinations = {
            'windows': {
                'chrome': {
                    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept_language': 'en-US,en;q=0.9',
                    'sec_ch_ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    'sec_ch_ua_mobile': '?0',
                    'sec_ch_ua_platform': '"Windows"',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                },
                'firefox': {
                    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'accept_language': 'en-US,en;q=0.5',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                },
                'edge': {
                    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.92',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept_language': 'en-US,en;q=0.9',
                    'sec_ch_ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    'sec_ch_ua_mobile': '?0',
                    'sec_ch_ua_platform': '"Windows"',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                }
            },
            'macos': {
                'chrome': {
                    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept_language': 'en-US,en;q=0.9',
                    'sec_ch_ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    'sec_ch_ua_mobile': '?0',
                    'sec_ch_ua_platform': '"macOS"',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                },
                'safari': {
                    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'accept_language': 'en-US,en;q=0.9',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                }
            },
            'linux': {
                'chrome': {
                    'user_agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept_language': 'en-US,en;q=0.9',
                    'sec_ch_ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    'sec_ch_ua_mobile': '?0',
                    'sec_ch_ua_platform': '"Linux"',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                },
                'firefox': {
                    'user_agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'accept_language': 'en-US,en;q=0.5',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                }
            },
            'mobile': {
                'android_chrome': {
                    'user_agent': 'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept_language': 'en-US,en;q=0.9',
                    'sec_ch_ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    'sec_ch_ua_mobile': '?1',
                    'sec_ch_ua_platform': '"Android"',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                },
                'ios_safari': {
                    'user_agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'accept_language': 'en-US,en;q=0.9',
                    'sec_fetch_dest': 'document',
                    'sec_fetch_mode': 'navigate',
                    'sec_fetch_site': 'none',
                    'sec_fetch_user': '?1',
                    'upgrade_insecure_requests': '1'
                }
            }
        }

        # Randomly select OS and browser combination
        os_choice = random.choice(list(os_browser_combinations.keys()))
        browser_choice = random.choice(list(os_browser_combinations[os_choice].keys()))
        
        # Get the headers for the selected combination
        headers = os_browser_combinations[os_choice][browser_choice].copy()
        
        # Add common headers
        headers.update({
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0'
        })

        # Add cookie if available
        if self.cookie:
            headers['cookie'] = self.cookie

        return headers

    def get_access_token(self) -> str:
        if self.access_token:
            return self.access_token  # Return cached token if available
        
        self.logger.info("Getting access token ...")

        url = 'https://business.facebook.com/business_locations'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
        }
        headers['cookie'] = self.cookie

        response = self.session.get(url, headers=headers, verify=False)

        if response.status_code == 200:
            search_token = re.search(r'(EAAG\w+)', response.text)
            if search_token and search_token.group(1):
                self.access_token = search_token.group(1)  # Cache the token
                self.logger.info("Access token retrieved successfully.")
                return self.access_token

        self.logger.error("Cannot find access token. Maybe your cookie is invalid!")
        return None

    def scrape_facebook_detail(self, page_id: str, post_id: str, post_type: str) -> dict:
        try:
            if not self.access_token:
                self.access_token = self.get_access_token()
            if not self.access_token:
                return {"success": False, "message": "Cannot get access token"}

            fields_map = {
                "post": "id,from,message,reactions.summary(total_count),comments.summary(true),shares.summary(total_count),updated_time",
                "group": "id,from,message,reactions.summary(total_count),comments.summary(true),shares.summary(total_count),created_time",
                "video": "id,from,message,source,reactions.summary(total_count),comments.summary(true),shares.summary(total_count),created_time",
                "watch": "id,from,message,source,reactions.summary(total_count),comments.summary(true),shares.summary(total_count),created_time",
                "reel": "id,from,message,source,reactions.summary(total_count),comments.summary(true),shares.summary(total_count),created_time",
                "photo": "id,from,album,images,name,reactions.summary(total_count),comments.summary(true),shares.summary(total_count),created_time"
            }
            fields = fields_map.get(post_type, "id,reactions.summary(total_count),comments.summary(true),shares.summary(total_count),created_time")
            if post_type == "groups":
                url = f"https://graph.facebook.com/{post_id}?fields={fields}&limit=2000&access_token={self.access_token}"
            else:
                url = f"https://graph.facebook.com/{page_id}_{post_id}?fields={fields}&limit=2000&access_token={self.access_token}"
            
            # Use the stored headers instead of generating new ones
            headers = self.headers.copy()
            headers['cookie'] = self.cookie
            
            response = self.session.get(url, headers=headers)
            time.sleep(random.uniform(3, 7))

            # Kiểm tra lỗi
            check_result = self.check_facebook_error(response)
            if not check_result["success"]:
                self.logger.error(f"Cannot get data for post {post_id}. Error: {check_result['error']}")
                return {"success": False, "error": check_result["error"]["message"]}

            post_data = check_result["data"]
            # Bổ sung url "thô" và post_type
            post_data["url"] = f"https://facebook.com/{post_id}"
            post_data["post_type"] = post_type
            return {"success": True, "data": post_data}

        except Exception as e:
            self.logger.error(f"Failed to scrape post: {e}")
            return {"success": False, "message": f"Failed to scrape post: {e}"}

    def extract_facebook_ids(self, url: str):
        patterns = {
            "groups": r"(?<=/groups/)([a-zA-Z0-9]+)(?:/posts/([a-zA-Z0-9]+)|/?\?multi_permalinks=([a-zA-Z0-9]+))?",
            "video": r"facebook\.com/([^/]+)(?:/videos/.+/(\d+)/?$|/videos/([a-zA-Z0-9]+))",
            "watch": r"watch(?:\/)?\?v=([a-zA-Z0-9_-]+)",
            "reel": r"reel/([a-zA-Z0-9]+)",
            "photo": r"photo(?:\.php)?\?fbid=([a-zA-Z0-9]+)",
            "post": r"facebook\.com/([^/]+)(?:/posts/.+/(\d+)/?$|/posts/([a-zA-Z0-9]+)|/permalink.php\?story_fbid=([a-zA-Z0-9]+))"
        }

        session = requests.Session()

        if self.proxies:
            session.proxies.update(self.proxies)

        headers = self.get_random_headers()
        session.headers.update(headers)

        extracted_ids = {}

        # Kiểm tra group
        if "groups" in url:
            match = re.search(patterns["groups"], url)
            if match:
                if not match.group(1).isdigit():
                    response = session.get(f"https://www.facebook.com/groups/{match.group(1)}", timeout=10)
                    group_id = re.findall(r'"groupID":"(\d+)"', response.text)[0]
                else:
                    group_id = match.group(1)
                post_id = match.group(2)
                multi_permalink = match.group(3)
                extracted_ids["groups"] = {"group_id": group_id}
                if post_id:
                    extracted_ids["groups"]["post_id"] = post_id
                if multi_permalink:
                    extracted_ids["groups"]["multi_permalink"] = multi_permalink

            return extracted_ids

        # Kiểm tra video
        if "video" in url:
            match = re.search(patterns["video"], url)
            if match:
                if not match.group(1).isdigit():
                    response = session.get(f"https://www.facebook.com/{match.group(1)}/about_profile_transparency", timeout=10)
                    page_match = re.findall(r'"pageID":"(\d+)"', response.text)
                    if page_match:
                        page_id = page_match[0]
                    else:
                        response = session.get(url, headers=headers)
                        page_match = re.findall(r'"props":\{"actorID":(\d+),"pageID":null|"props":\{"pageID":"(\d+)"', response.text)
                        if page_match:
                            page_id = page_match[0][0] if page_match[0][0] else page_match[0][1]
                else:
                    page_id = match.group(1)
                video_id = match.group(2) if match.group(2) else match.group(3)
                extracted_ids["video"] = {"page_id": page_id}
                if video_id:
                    extracted_ids["video"]["video_id"] = video_id

            return extracted_ids    

        # Kiểm tra post
        if "post" in url:
            match = re.search(patterns["post"], url)
            if match:
                if not match.group(1).isdigit():
                    response = session.get(f"https://www.facebook.com/{match.group(1)}/about_profile_transparency", timeout=10)
                    page_match = re.findall(r'"pageID":"(\d+)"', response.text)
                    if page_match:
                        page_id = page_match[0]
                    else:
                        response = session.get(url, headers=headers)
                        page_match = re.findall(r'"props":\{"actorID":(\d+),"pageID":null|"props":\{"pageID":"(\d+)"', response.text)
                        if page_match:
                            page_id = page_match[0][0] if page_match[0][0] else page_match[0][1]
                else:
                    page_id = match.group(1)
                post_id = match.group(2) if match.group(2) else match.group(3)
                extracted_ids["post"] = {"page_id": page_id}
                if post_id:
                    extracted_ids["post"]["post_id"] = post_id
                if match.group(4):
                    extracted_ids["post"]["multi_permalink"] = match.group(4)

            return extracted_ids

        # Kiểm tra watch, reel, photo
        if "watch" in url or "reel" in url or "photo" in url:
            response = session.get(url, headers=headers)
            page_id = re.findall(r'"owner":{"__typename":"User","id":"(\d+)"', response.text)[0]
            if "watch" in patterns:
                match = re.search(patterns["watch"], url)
                if match:
                    video_id = match.group(1)
                    extracted_ids["watch"] = {"page_id": page_id}
                    if video_id:
                        extracted_ids["watch"]["video_id"] = video_id

            if "reel" in patterns:
                match = re.search(patterns["reel"], url)
                if match:
                    reel_id = match.group(1)
                    extracted_ids["reel"] = {"page_id": page_id}
                    if reel_id:
                        extracted_ids["reel"]["reel_id"] = reel_id

            if "photo" in patterns:
                match = re.search(patterns["photo"], url)
                if match:
                    photo_id = match.group(1)
                    extracted_ids["photo"] = {"page_id": page_id}
                    if photo_id:
                        extracted_ids["photo"]["photo_id"] = photo_id

        # # trường hợp share link
        # if "share" in url:
        #     data = {
        #         'link': url,
        #     }

        #     response = requests.post('https://id.traodoisub.com/api.php', data=data)
        #     id = response.json()['id']
        #     extracted_ids["post"] = {"post_id": id}

        return extracted_ids

    def standardize_data(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Chuyển post_data thô của Facebook (tuỳ loại post) sang định dạng chuẩn.
        """
        post_type = post_data.get("post_type", "")
        standardized = {}

        # ID, URL, post_type
        standardized["id"] = post_data.get("id")
        standardized["url"] = post_data.get("url")
        standardized["post_type"] = post_type

        # Tùy loại post mà lấy text
        if post_type in ["post", "group", "watch", "video", "reel"]:
            standardized["text"] = post_data.get("message", "")
        elif post_type == "photo":
            standardized["text"] = post_data.get("name", "")
        else:
            standardized["text"] = ""  # Mặc định

        # from_user
        standardized["from_user"] = {
            "id": post_data.get("from", {}).get("id", ""),
            "name": post_data.get("from", {}).get("name", ""),
            "url": f"https://www.facebook.com/{post_data.get('from', {}).get('id', '')}"
        }

        # count_reactions
        count_reactions = post_data.get("reactions", {}).get("summary")
        count_like = count_reactions.get("total_count") if count_reactions else 0
        standardized["count_like"] = count_like

        if post_data.get("reactions"):
            reactions = post_data["reactions"]["data"]
            angry_reactions = [r for r in reactions if r["type"] == "ANGRY"]
            standardized["count_dislike"] = len(angry_reactions)
            standardized["count_like"] -= standardized["count_dislike"]  # Subtract angry from total likes

        
        # Standardize reactions data (likes, love, etc.)
        # reactions = post_data.get("reactions", {}).get("data", [])
        # reaction_types = ["LIKE", "LOVE", "WOW", "HAHA", "SAD", "ANGRY"]
        # standardized["reactions"] = {reaction: 0 for reaction in reaction_types}
        # for reaction in reactions:
        #     reaction_type = reaction.get("type")
        #     if reaction_type in standardized["reactions"]:
        #         standardized["reactions"][reaction_type] += 1

        # comments.summary(true) -> total_count
        comment_summary = post_data.get("comments", {}).get("summary")
        comment_count = comment_summary.get("total_count") if comment_summary else 0
        standardized["comment_count"] = comment_count

        # shares.count -> total share count
        share_count = post_data.get("shares", {}).get("count", 0)
        standardized["share_count"] = share_count

        # created_time (group thường có created_time, post page thường updated_time)
        standardized["created_time"] = post_data.get("created_time") or post_data.get("updated_time")

        # media: Nếu là photo -> list image; nếu là video/watch/reel -> source
        media = None
        if post_type in ["video", "watch", "reel"]:
            media = post_data.get("source")  # link video
        elif post_type == "photo":
            media = post_data.get("images", [])
        standardized["media"] = media

        # Comments
        standardized["comments"] = post_data.get("comments", {}).get("data", [])
        for comment in standardized["comments"]:
            comment["count_like"] = comment.get("likes", {})
            comment["count_dislike"] = 0

            if "likes" in comment:
                del comment["likes"]

            comment['url'] = f"https://www.facebook.com/{comment['id']}"           

        

        return standardized

    def scrape_info(self, url: str):
        extracted_ids = self.extract_facebook_ids(url)
        if not extracted_ids:
            return {"success": False, "message": "Could not extract IDs from url."}
        
        # Xác định post_id & post_type
        if 'groups' in extracted_ids:
            group_id = extracted_ids['groups']['group_id']
            post_id = (
                f"{group_id}_{extracted_ids['groups']['post_id']}"
                if 'post_id' in extracted_ids['groups']
                else f"{group_id}_{extracted_ids['groups']['multi_permalink']}"
            )
            post_type = 'groups'
        else:
            post_type = (
                'post' if 'post' in extracted_ids else
                'video' if 'video' in extracted_ids else
                'watch' if 'watch' in extracted_ids else
                'reel' if 'reel' in extracted_ids else
                'photo' if 'photo' in extracted_ids else
                None
            )
            page_id = extracted_ids[post_type]['page_id']
            post_id = extracted_ids[post_type].get('post_id') or extracted_ids[post_type].get('video_id') or extracted_ids[post_type].get('reel_id') or extracted_ids[post_type].get('photo_id')

        # Lấy dữ liệu post thô
        try:
            if post_type == "groups":
                post_data = self.scrape_facebook_detail(None, post_id, post_type)
            else:
                post_data = self.scrape_facebook_detail(page_id, post_id, post_type)
        except Exception as e:
            self.logger.error(f"Failed to scrape post: {e}")
            return {"success": False, "message": f"Failed to scrape post: {e}"}
        
        if not post_data["success"]:
            return post_data
        
        # Chuẩn hóa dữ liệu
        standardized_data = self.standardize_data(post_data["data"])
        
        return {"success": True, "data": standardized_data}

    def scrape_post_page_group(self, url: str):
        self.access_token = self.get_access_token()
        if not self.access_token:
            return {"success": False, "message": "Cannot get access token"}
            
        fields = "id,from,type,message,reactions.summary(total_count),shares.summary(total_count),created_time,source,permalink_url,comments.limit(1000).fields(id,message,created_time,from,full_picture,permalink_url)"
        
        session = requests.Session()
        session.headers.update(self.get_random_headers())
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            "cookie": self.cookie
        }

        if "groups" in url:
            print("Scraping group posts...")
            group_id = [p for p in url.split('/') if p][-1]

            print(f"Extracted group identifier: {group_id}")
            if not group_id.isdigit():
                headers_tmp = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cookie": self.cookie
                }
                html = requests.get(url, headers=headers_tmp, cookies={'cookie': self.cookie}, proxies=self.proxies).text
                match = re.findall(r'"groupID":"(\d+)"', html)
                group_id = match[0]
                # print(f"Resolved group ID: {group_id}")

            url = f"https://graph.facebook.com/v19.0/{group_id}/feed?fields={fields}&limit=10&access_token={self.access_token}"

            # print("Fetching posts from URL:", url)
            response = self.session.get(url, timeout=30, headers=headers, cookies={'cookie': self.cookie}, proxies=self.proxies)
            # print(response.status_code)

            if "data" not in response.json():
                return {"success": False, "message": "No data found."}
            
            posts = response.json().get('data', [])
            for post in posts:
                from_data = post.get('from', {}) 
                from_user = UserInfo(
                    user_id=str(from_data.get('id', '')),
                    name=from_data.get('name', ''),
                    url=f"https://www.facebook.com/{from_data.get('id', '')}" if from_data.get('id') else "",
                    handle=""
                )

                reactions_data = post.get('reactions', {})
                comments_container = post.get('comments', {})
                shares_data = post.get('shares', {})
                
                post_info = GeneralInfo(
                    post_id=str(post.get('id', '')),
                    url=post.get('permalink_url', ''),
                    post_type=post.get('type', ''),
                    title=post.get('message', post.get('story', '')),
                    content=post.get('message', post.get('story', '')),
                    media=post.get('source', []),
                    author=from_user,
                    date=post.get('created_time', ''),
                    count_like=reactions_data.get('summary', {}).get('total_count', 0),
                    count_dislike=0,
                    count_comment=comments_container.get('summary', {}).get('total_count', 
                                                                        comments_container.get('count', 0)),
                    count_share=shares_data.get('count', 0)
                )

                comments = comments_container.get('data', [])
                
                for comment in comments:
                    comment_author_data = comment.get('from', {})
                    
                    comment_info = CommentInfo(
                        author=UserInfo(
                            user_id=str(comment_author_data.get('id', '')),
                            name=comment_author_data.get('name', ''),
                            url=f"https://www.facebook.com/{comment_author_data.get('id', '')}" if comment_author_data.get('id') else "",
                            handle=""
                        ),
                        date=comment.get('created_time', ''),
                        url=comment.get('permalink_url', f"https://www.facebook.com/{comment.get('id', '')}"),
                        
                        comment_id=str(comment.get('id', '')),
                        content=comment.get('message', ''),
                        count_like=comment.get('like_count', 0),
                        count_dislike=0
                    )
                    post_info.comments.append(comment_info)
                # print(f"Scraped post ID: {post_info.post_id}")

                yield {"success": True, "data": post_info}
            
        else:
            print("Scraping page posts...")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
            }
            page_id = re.search(r"https://www\.facebook\.com/([^/?#]+)", url).group(1)
            response = requests.get(f"https://www.facebook.com/{page_id}/about_profile_transparency", headers=headers, timeout=10)
            page_match = re.findall(r'"pageID":"(\d+)"|"delegate_page":{"id":"(\d+)"', response.text)
            if page_match:
                page_id = page_match[0][0]
            else:
                response = requests.get(url, headers=headers, timeout=10)
                page_match = re.findall(r'"props":\{"actorID":(\d+),"pageID":null|"props":\{"pageID":"(\d+)"|"delegate_page":{"id":"(\d+)"', response.text)
                if page_match:
                    page_id = page_match[0][0] if page_match[0][0] else page_match[0][1]
            url = f"https://graph.facebook.com/v19.0/{page_id}/feed?fields={fields}&limit=10&access_token={self.access_token}"
            
        
            response = self.session.get(url, timeout=30, headers=headers, cookies={'cookie': self.cookie}, proxies=self.proxies)
            print(response.status_code)
            if "data" not in response.json():
                return {"success": False, "message": "No data found."}
        
            posts = response.json()['data']
            for post in posts:
                try:
                    # Create UserInfo object
                    from_user = UserInfo(
                        user_id=str(post['from']['id']),
                        name=post['from']['name'],
                        url="https://www.facebook.com/" + str(post['from']['id']),
                        handle=""
                    )

                    # Create GeneralInfo object
                    post_info = GeneralInfo(
                        post_id=str(post['id']),
                        url=post['permalink_url'],
                        post_type=post['type'],
                        title=post.get('message', ''),
                        content=post.get('message', ''),
                        author=from_user,
                        date=post['created_time'],
                        media=post.get('source', []),
                        count_like=post['reactions'].get('summary', {}).get('total_count', 0) if 'reactions' in post else 0,
                        count_dislike=0,
                        count_comment=post['comments'].get('count', 0) if 'comments' in post else 0,
                        count_share=post['shares'].get('count', 0) if 'shares' in post else 0
                    )

                    # Create CommentInfo object
                    comments_data = post.get('comments', {})
                    comments = comments_data.get('data', [])
                    for comment in comments:
                        comment_info = CommentInfo(
                            author=UserInfo(
                                user_id=str(comment['from']['id']),
                                name=comment['from']['name'],
                                url="https://www.facebook.com/" + str(comment['from']['id']),
                                handle=""
                            ),
                            date=comment['created_time'],
                            url=f"https://www.facebook.com/{comment['id']}",
                            comment_id=str(comment['id']),
                            content=comment['message'],
                            count_like=comment.get('like_count', 0),
                            count_dislike=0
                        )
                        post_info.comments.append(comment_info)

                    yield {"success": True, "data": post_info}
                    
                except Exception as e:
                    self.logger.error(f"Error processing post {post.get('id', 'unknown')}: {e}")
                    yield {"success": False, "message": f"Error processing post: {e}", "post_id": post.get('id', 'unknown')}
