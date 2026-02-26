import requests
import re
import time
from typing import Dict, List, Any
import os
from dotenv import load_dotenv
import random

os.environ.pop('COOKIE', None)
load_dotenv()
COOKIE = os.getenv('COOKIE')

class FacebookScraper:
    def __init__(self, proxies=None):
        self.cookie = COOKIE
        self.session = requests.Session()
        self.logger = self.setup_logger()
        self.access_token = None
        self.proxies = proxies
        self.set_proxies()
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.92',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (iPad; CPU OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1'
        ]

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
        """Generate random headers for requests"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Pragma': 'no-cache'
        }

    def get_access_token(self) -> str:
        if self.access_token:
            return self.access_token  # Return cached token if available
        
        self.logger.info("Getting access token ...")

        url = 'https://business.facebook.com/business_locations'
        headers = self.get_random_headers()
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

    def scrape_facebook_detail(self, post_id: str, post_type: str = "post") -> dict:
        try:
            if not self.access_token:
                self.access_token = self.get_access_token()
            if not self.access_token:
                return {"success": False, "message": "Cannot get access token"}

            fields_map = {
                "post": "id,from,message,likes.summary(true),comments.summary(true),updated_time",
                "group": "id,from,message,comments.summary(true),created_time",
                "video": "id,from,description,source,likes.summary(true),comments.summary(true),created_time",
                "watch": "id,from,description,source,likes.summary(true),comments.summary(true),created_time",
                "reel": "id,from,description,source,likes.summary(true),comments.summary(true),created_time",
                "photo": "id,from,album,images,name,likes.summary(true),comments.summary(true),created_time"
            }
            fields = fields_map.get(post_type, "id,likes.summary(true),comments.summary(true),created_time")

            url = f"https://graph.facebook.com/{post_id}?fields={fields}&access_token={self.access_token}"
            headers = self.get_random_headers()
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

    def scrape_facebook_comments(self, post_id: str) -> dict:
        """
        Trả về comments ở dạng "thô", chưa chuẩn hoá.
        """
        try:
            if not self.access_token:
                self.access_token = self.get_access_token()
            if not self.access_token:
                return {"success": False, "message": "Cannot get access token"}

            headers = self.get_random_headers()
            headers['cookie'] = self.cookie
            comments = self.fetch_comments(post_id, self.access_token, headers)
            # Nếu comments trả về dạng {"success": False, "error": {...}} thì ta trả về luôn
            if isinstance(comments, dict) and comments.get("success") == False:
                return comments

            return {"success": True, "data": comments}
        except Exception as e:
            self.logger.error(f"Failed to scrape comments: {e}")
            return {"success": False, "message": f"Failed to scrape comments: {e}"}

    def fetch_comments(self,
                       post_id: str,
                       access_token: str,
                       headers: Dict[str, str],
                       is_reply: bool = False) -> List[Dict[str, Any]]:
        """
        Hàm đệ quy để fetch comment + replies.
        Nếu phát hiện lỗi từ Facebook (VD hết quota, token die...), trả về dict {"success": False, "error": {...}}.
        Bên ngoài chỉ cần kiểm tra isinstance(kết_quả, dict) and kết_quả["success"] == False.
        """
        comments = []
        url = f"https://graph.facebook.com/v18.0/{post_id}/comments"
        params = {
            'fields': 'id,message,from,created_time,reactions.summary(total_count),comment_count',
            'access_token': access_token,
            'limit': 2000 if not is_reply else 100 # Lấy tối đa 2000 comments, 100 replies
        }

        while url:
            response = self.session.get(url, params=params, headers=headers)
            check_result = self.check_facebook_error(response)
            if not check_result["success"]:
                # Trả về luôn lỗi
                return check_result

            data = check_result["data"]

            # Tạm dừng
            time.sleep(random.uniform(3, 7))

            for comment in data.get('data', []):
                comment_data = {
                    "id": comment.get("id"),
                    "url": f"https://facebook.com/{comment.get('id')}",
                    "message": comment.get("message"),
                    "from": comment.get('from', {}).get('name', 'Unknown'),
                    "created_time": comment.get("created_time"),
                    "count_like": comment.get("reactions", {}).get("summary", {}).get("total_count", 0),
                    "count_dislike": 0,  # Sẽ gán sau
                    "comment_count": comment.get("comment_count", 0),
                    "replies": []
                }

                # Điếm số angry reactions
                if comment.get("reactions"):
                    reactions = comment["reactions"]["data"]
                    angry_reactions = [r for r in reactions if r["type"] == "ANGRY"]
                    comment_data["count_dislike"] = len(angry_reactions)
                    comment_data["count_like"] -= comment_data["count_dislike"]  # Subtract angry from total likes

                comments.append(comment_data)

            paging = data.get('paging', {})
            url = paging.get('next')
            # Để tránh gắn params nhiều lần
            params = {}

        return comments
    
    def fetch_reactions_totals(self, post_id: str) -> dict:
        """
        Fetch the total count of likes (count_like) and angry reactions (count_dislike) for the given post.
        """
        try:            
            if not self.access_token:
                self.access_token = self.get_access_token()
            if not self.access_token:
                return {"success": False, "message": "Cannot get access token"}

            # Initialize the reactions dictionary
            reactions = {"count_like": 0, "count_dislike": 0}

            # Fetch count_like
            url_like = f"https://graph.facebook.com/v18.0/{post_id}?fields=reactions.summary(total_count)"
            params_like = {"access_token": self.access_token}
            headers = self.get_random_headers()
            headers['cookie'] = self.cookie
            response_like = self.session.get(url_like, params=params_like, headers=headers)
            time.sleep(random.uniform(3, 7))

            check_like = self.check_facebook_error(response_like)
            if not check_like["success"]:
                self.logger.error(f"Failed to fetch count_like: {check_like['error']}")
                return {"success": False, "error": check_like["error"]["message"]}

            data_like = check_like["data"]
            summary_like = data_like.get("reactions", {}).get("summary", {})
            count_like = summary_like.get("total_count", 0)
            reactions["count_like"] = count_like

            return {"success": True, "data": reactions}

        except Exception as e:
            self.logger.error(f"Error fetching reactions: {e}")
            return {"success": False, "message": f"Error fetching reactions: {e}"}

    def extract_facebook_ids(self, url: str):
        patterns = {
            "groups": r"(?<=/groups/)([a-zA-Z0-9]+)(?:/posts/([a-zA-Z0-9]+)|/?\?multi_permalinks=([a-zA-Z0-9]+))?",
            "video": r"videos/.+/(\d+)/?$|videos/([a-zA-Z0-9]+)",
            "watch": r"watch/\?v=([a-zA-Z0-9]+)",
            "reel": r"reel/([a-zA-Z0-9]+)",
            "photo": r"photo\?fbid=([a-zA-Z0-9]+)",
            "post": r"posts/.+/(\d+)/?$|posts/([a-zA-Z0-9]+)|permalink.php\?story_fbid=([a-zA-Z0-9]+)"
        }

        extracted_ids = {}

        # Kiểm tra group
        if "groups" in patterns:
            match = re.search(patterns["groups"], url)
            if match:
                if not match.group(1).isdigit():
                    response = self.session.get(f"https://www.facebook.com/groups/{match.group(1)}", timeout=10)
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

        # Kiểm tra các pattern khác
        for key, pattern in patterns.items():
            if key == "groups":
                continue
            match = re.search(pattern, url)
            if match:
                if key == "post":
                    extracted_ids[key] = match.group(1) if match.group(1) else match.group(2) if match.group(2) else match.group(3)
                elif key == "video":
                    extracted_ids[key] = match.group(1) if match.group(1) else match.group(2)
                else:
                    extracted_ids[key] = match.group(1)
                return extracted_ids

        return extracted_ids

    def standardize_post_data(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
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
        if post_type in ["post", "group"]:
            standardized["text"] = post_data.get("message", "")
        elif post_type in ["video", "watch", "reel"]:
            standardized["text"] = post_data.get("description", "")
        elif post_type == "photo":
            standardized["text"] = post_data.get("name", "")
        else:
            standardized["text"] = ""  # Mặc định

        # from (user/page)
        standardized["from"] = post_data.get("from")

        # likes.summary(true) -> total_count
        like_summary = post_data.get("likes", {}).get("summary")
        like_count = like_summary.get("total_count") if like_summary else 0
        standardized["like_count"] = like_count

        # comments.summary(true) -> total_count
        comment_summary = post_data.get("comments", {}).get("summary")
        comment_count = comment_summary.get("total_count") if comment_summary else 0
        standardized["comment_count"] = comment_count

        # created_time (group thường có created_time, post page thường updated_time)
        standardized["created_time"] = post_data.get("created_time") or post_data.get("updated_time")

        # media: Nếu là photo -> list image; nếu là video/watch/reel -> source
        media = None
        if post_type in ["video", "watch", "reel"]:
            media = post_data.get("source")  # link video
        elif post_type == "photo":
            media = post_data.get("images", [])
        standardized["media"] = media

        # Comments sẽ gán sau
        standardized["comments"] = []

        return standardized

    def standardize_comment_data(self, comments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Chuẩn hoá danh sách comment và replies.
        """
        standardized_comments = []
        for cmt in comments:
            # Kiểm tra nếu cmt là {"success": False, "error": ...} thì dừng
            # (trường hợp hiếm, vì ta thường đã return sớm ở fetch_comments)
            if isinstance(cmt, dict) and cmt.get("success") == False:
                # Ở đây tuỳ ý, bạn có thể append 1 comment "báo lỗi" 
                # hoặc dừng hẳn. Tuỳ logic.
                continue

            item = {
                "id": cmt.get("id"),
                "url": cmt.get("url"),
                "text": cmt.get("message", ""),
                "from": cmt.get("from"),
                "created_time": cmt.get("created_time"),
                "count_like": cmt.get("count_like", 0),
                "count_dislike": cmt.get("count_dislike", 0),
                "comment_count": cmt.get("comment_count", 0),
                "replies": []
            }
            # Xử lý replies (đệ quy)
            if cmt.get("replies") and isinstance(cmt["replies"], list):
                item["replies"] = self.standardize_comment_data(cmt["replies"])

            standardized_comments.append(item)
        return standardized_comments

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
            post_type = 'group'
        else:
            post_type = (
                'post' if 'post' in extracted_ids else
                'video' if 'video' in extracted_ids else
                'photo' if 'photo' in extracted_ids else
                'watch' if 'watch' in extracted_ids else
                'reel' if 'reel' in extracted_ids else
                None
            )
            post_id = extracted_ids.get(post_type)

        if not post_id or not post_type:
            return {"success": False, "message": "URL not supported or cannot find post_id."}

        # Lấy dữ liệu post thô
        try:
            post_data = self.scrape_facebook_detail(post_id, post_type)
        except Exception as e:
            self.logger.error(f"Failed to scrape post: {e}")
            return {"success": False, "message": f"Failed to scrape post: {e}"}

        if not post_data["success"]:
            # Có error -> trả về
            return post_data

        # Lấy reactions thô
        try:
            if post_type == "group":
                reactions = self.fetch_reactions_totals(post_id)
            else:
                # Kiểm tra xem "from" có tồn tại không
                from_data = post_data["data"].get("from")
                if from_data and "id" in from_data:
                    object_id = from_data["id"] + "_" + post_id
                else:
                    object_id = post_id  # Nếu không có "from", fallback về post_id gốc

                reactions = self.fetch_reactions_totals(object_id)
        except Exception as e:
            self.logger.error(f"Failed to fetch reactions: {e}")
            reactions = {"success": False, "message": f"Failed to fetch reactions: {e}"}

        # Lấy danh sách comments thô
        try:
            if post_type == "group":
                comments_data = self.scrape_facebook_comments(post_id)
            else:
                # Kiểm tra xem "from" có tồn tại không
                from_data = post_data["data"].get("from")
                if from_data and "id" in from_data:
                    object_id = from_data["id"] + "_" + post_id
                else:
                    object_id = post_id  # Nếu không có "from", fallback về post_id gốc

                comments_data = self.scrape_facebook_comments(object_id)
        except Exception as e:
            self.logger.error(f"Failed to scrape comments: {e}")
            comments_data = {"success": False, "message": f"Failed to scrape comments: {e}"}

        # Nếu comment trả về lỗi
        if not comments_data["success"]:
            return comments_data

        # Chuẩn hoá dữ liệu post
        standardized_post = self.standardize_post_data(post_data["data"])

        # Thêm reactions vào standardized_post
        if reactions and reactions.get("success"):
            standardized_post["count_like"] = reactions["data"]["count_like"]
            standardized_post["count_dislike"] = reactions["data"]["count_dislike"]
            # Xoá trường "like_count" mặc định do standardize_post_data tạo ra (nếu muốn)
            if "like_count" in standardized_post:
                del standardized_post["like_count"]
        else:
            # Nếu không lấy được reactions
            standardized_post["count_like"] = 0
            standardized_post["count_dislike"] = 0
            if "like_count" in standardized_post:
                del standardized_post["like_count"]
        standardized_post = {k: standardized_post[k] for k in ["id", "url", "post_type", "text", "from", "created_time", "media", "count_like", "count_dislike", "comment_count", "comments"]}

        # Chuẩn hoá danh sách comments
        if comments_data["success"]:
            standardized_comments = self.standardize_comment_data(comments_data["data"])
            standardized_post["comments"] = standardized_comments
        else:
            # Nếu không lấy được comments
            standardized_post["comments"] = []

        return {"success": True, "data": standardized_post}

# if __name__ == "__main__":
#     scraper = FacebookScraper()
#     data = scraper.scrape_info(url)