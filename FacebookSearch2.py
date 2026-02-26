import json
import re
import time
import uuid  # hiện chưa dùng, nhưng giữ lại để không thay đổi môi trường
from typing import Dict, List, Optional
import os

import requests

from utils.logger import setup_loggers

os.makedirs(f'logs', exist_ok=True)
log_files = [
    'logs/auth.log',      # Cho các hoạt động xác thực/đăng nhập
    'logs/search.log',    # Cho các hoạt động tìm kiếm và xử lý URL
    'logs/crawl.log',     # Cho các hoạt động crawl và kết quả
    'logs/error.log',      # Cho các lỗi nghiêm trọng
    'logs/debug.log'      # Cho các thông tin debug chi tiết
]
loggers = setup_loggers(log_files)
debug_logger = loggers['debug']

class FacebookSearch:
    def __init__(
        self,
        cookies: Optional[str] = None,
        proxies: Optional[Dict[str, str]] = None,
        keyword: str = "",
    ):
        self.session = requests.Session()
        self.keyword = keyword

        self.proxies = proxies or {}
        if self.proxies:
            self.session.proxies.update(self.proxies)

        self.cookies = self._parse_cookies(cookies) if cookies else {}
        if self.cookies:
            self.session.cookies.update(self.cookies)

        self.session.headers.update({
            "Accept-Encoding": "gzip, deflate",
        })

    # ------------------------------------------------------------
    # Cookie & dynamic values
    # ------------------------------------------------------------
    def _parse_cookies(self, cookies_str: str) -> Dict[str, str]:
        """
        Parse cookie string dạng 'a=1; b=2' thành dict.
        """
        cookies_dict: Dict[str, str] = {}
        for cookie in cookies_str.split(";"):
            cookie = cookie.strip()
            if "=" in cookie:
                name, value = cookie.split("=", 1)
                cookies_dict[name.strip()] = value.strip()
        return cookies_dict

    def _extract_dynamic_values_and_queryid(self, keyword: str) -> Optional[Dict[str, str]]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-CH-UA": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

        try:
            # Dùng session + proxies + cookies
            response = self.session.get(
                "https://www.facebook.com/search/top",
                params={"q": keyword},
                headers=headers,
                timeout=30,
            )
            html = response.text

            # fb_dtsg
            fb_dtsg = None
            dtsg_matches = re.findall(
                r'"DTSGInitialData".*?"token":"([^"]+)"',
                html,
            )
            if dtsg_matches:
                fb_dtsg = dtsg_matches[0]

            # queryid
            queryid = None
            pattern = (
                r'"queryName":"SearchCometResultsInitialResultsParallelFetchQuery".*?"queryID":"(\d+)"'
            )
            match = re.search(pattern, html)
            if match:
                queryid = match.group(1)
            else:
                all_queryids = re.findall(pattern, html)
                if all_queryids:
                    queryid = all_queryids[0]

            # haste_session
            haste_session = None
            haste_session_match = re.search(
                r'"haste_session"\s*:\s*"([^"]+)"',
                html,
            )
            if haste_session_match:
                haste_session = haste_session_match.group(1)

            # accountId
            account_id = None
            accountid_match = re.search(
                r'"accountId"\s*:\s*"(\d+)"',
                html,
            )
            if accountid_match:
                account_id = accountid_match.group(1)

            if fb_dtsg is None or queryid is None or haste_session is None or account_id is None:
                debug_logger.warning(
                    f"[{keyword}] Missing dynamic values "
                    f"(fb_dtsg={fb_dtsg}, queryid={queryid}, "
                    f"haste_session={haste_session}, accountId={account_id})"
                )
                return None
            
            # debug_logger.info(
            #     f"[{keyword}] Extracted dynamic values: "
            #     f"fb_dtsg={fb_dtsg}, queryid={queryid}, "
            #     f"haste_session={haste_session}, accountId={account_id}"
            # )

            return {
                "fb_dtsg": fb_dtsg,
                "queryid": queryid,
                "haste_session": haste_session,
                "accountId": account_id,
            }

        except Exception as e:
            debug_logger.exception(f"Error extracting dynamic values for keyword '{keyword}': {e}")
            return None

    # ------------------------------------------------------------
    # GraphQL fetch posts
    # ------------------------------------------------------------
    def fetch_posts(self, keyword: str, max_posts: int = 50, sleep_time: float = 1.0):
        """
        Gọi API GraphQL để lấy danh sách posts cho keyword.
        Giữ nguyên cấu trúc variables, headers, data như code gốc.
        """
        dynamic_values = self._extract_dynamic_values_and_queryid(keyword)
        if not dynamic_values:
            debug_logger.warning(f"[{keyword}] Skip fetch_posts because dynamic_values is None")
            return []

        headers = {
            "accept": "*/*",
            "accept-language": "vi,en-US;q=0.9,en;q=0.8",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://www.facebook.com",
            "priority": "u=1, i",
            "referer": "https://www.facebook.com",
            "sec-ch-prefers-color-scheme": "dark",
            "sec-ch-ua": (
                '"Microsoft Edge";v="141", "Not?A_Brand";v="8", '
                '"Chromium";v="141"'
            ),
            "sec-ch-ua-full-version-list": (
                '"Microsoft Edge";v="141.0.3537.71", '
                '"Not?A_Brand";v="8.0.0.0", '
                '"Chromium";v="141.0.7390.66"'
            ),
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": '""',
            "sec-ch-ua-platform": '"macOS"',
            "sec-ch-ua-platform-version": '"26.0.1"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
            ),
            "x-asbd-id": "359341",
            "x-fb-friendly-name": "SearchCometResultsInitialResultsQuery",
            "x-fb-lsd": "foyADmWNX18MEahONUaofK",
        }

        count_per_page = 10
        cursor = None
        all_posts = []
        total_fetched = 0

        variables_template = {
            "allow_streaming": False,
            "args": {
                "callsite": "COMET_GLOBAL_SEARCH",
                "config": {
                    "exact_match": False,
                    "high_confidence_config": None,
                    "intercept_config": None,
                    "sts_disambiguation": None,
                    "watch_config": None,
                },
                "context": {
                    "bsid": "d5cc7418-c2f8-4880-8cca-d1caf7c95f33",
                    "tsid": "0.2930380711253795",
                },
                "experience": {
                    "client_defined_experiences": ["ADS_PARALLEL_FETCH"],
                    "encoded_server_defined_params": None,
                    "fbid": None,
                    "type": "POSTS_TAB",
                },
                "filters": ['{"name":"recent_posts","args":""}'],
                "text": keyword,
            },
            "count": count_per_page,
            "feedLocation": "SEARCH",
            "feedbackSource": 23,
            "fetch_filters": True,
            "focusCommentID": None,
            "locale": None,
            "privacySelectorRenderLocation": "COMET_STREAM",
            "renderLocation": "search_results_page",
            "scale": 2,
            "stream_initial_count": 0,
            "useDefaultActor": False,
            "__relay_internal__pv__GHLShouldChangeAdIdFieldNamerelayprovider": True,
            "__relay_internal__pv__GHLShouldChangeSponsoredDataFieldNamerelayprovider": True,
            "__relay_internal__pv__FBReels_enable_view_dubbed_audio_type_gkrelayprovider": True,
            "__relay_internal__pv__CometUFICommentAvatarStickerAnimatedImagerelayprovider": False,
            "__relay_internal__pv__IsWorkUserrelayprovider": False,
            "__relay_internal__pv__FBReels_deprecate_short_form_video_context_gkrelayprovider": True,
            "__relay_internal__pv__FeedDeepDiveTopicPillThreadViewEnabledrelayprovider": False,
            "__relay_internal__pv__CometImmersivePhotoCanUserDisable3DMotionrelayprovider": False,
            "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider": False,
            "__relay_internal__pv__IsMergQAPollsrelayprovider": False,
            "__relay_internal__pv__FBReels_enable_meta_ai_label_gkrelayprovider": True,
            "__relay_internal__pv__FBReelsMediaFooter_comet_enable_reels_ads_gkrelayprovider": True,
            "__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider": False,
            "__relay_internal__pv__CometUFIShareActionMigrationrelayprovider": True,
            "__relay_internal__pv__CometUFI_dedicated_comment_routable_dialog_gkrelayprovider": False,
            "__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider": True,
            "__relay_internal__pv__FBReelsIFUTileContent_reelsIFUPlayOnHoverrelayprovider": True,
            "__relay_internal__pv__GroupsCometGYSJFeedItemHeightrelayprovider": 206,
            "__relay_internal__pv__StoriesShouldIncludeFbNotesrelayprovider": False,
        }

        while total_fetched < max_posts:
            variables = variables_template.copy()
            variables["count"] = min(count_per_page, max_posts - total_fetched)
            if cursor:
                variables["cursor"] = cursor

            data = {
                "av": dynamic_values["accountId"],
                "__aaid": "0",
                "__user": dynamic_values["accountId"],
                "__a": "1",
                "__req": "19",
                "__hs": dynamic_values["haste_session"],
                "dpr": "2",
                "__ccg": "EXCELLENT",
                "__rev": "1028355510",
                "__s": "zps5wz:cbwkif:lwtcpz",
                "__hsi": "7560990823119959060",
                "__dyn": (
                    "7xeUmK1ixt0mUyEqxemh0noeEb8nwgUao4ubyQdwSwAyUco2qwJyE24wJwpUe8hwaG1sw9u0LVEtwMw6ywMwto886C11wBz83"
                    "WwgEcEhwGxu782lwv89kbxS1FwnE6a1awhUC7Udo5qfK0zEkxe2GewGwkUe9obrwh8lwUwgojUlDw-wUwxwjFovUaU3VwLyEb"
                    "UGdG0HE88cA0z8c84q58jyUaUcojxK2B08-269wkopg6C13xecwBwWzUfHDzUiBG2OUqwjVqwLwHwGwto461wweW2K3W6Eqwl8"
                ),
                "__csr": (
                    "gd_7NccgH3nd11b5NdT8JPiq3_POimxH99soV7kAhnPOlvZRshA9H8WQKGQWaiIADrnnrhKgWyK8ZapXLyid4mnCWmTjGqV9bG"
                    "VpkmbWLmAO6Q4kBiKjVpKimeAy8riUDAKAFbXKVGy8GiEhKEWbVUKdDy9J7yFAcjJ4Kq3deQbGbzumm6k5-m6o-U8oixuvzby8"
                    "GES4pU9U8oiGdCwFxWi5UfoaU98tzE422mfxmmquFEyeU8Ey9yFEeqwyxC2y4XzUTyK4U8poiyK4HxCiE985F0Fwi8C17BwQCw"
                    "WwXBxV0QwFx28Dwxxeu8xq1exW1awm9oW0F8cU26wgEC1PwpE-0va0-RxO0rYK7k1PwaG9pAF8qwq8km10wUw2PEGKE0fiEcU1"
                    "l403rG05QU1vU03c6wCw9e0bC811w778m9w12m0hxx21cxgM3JXw3C8Qw6Waz81IS1iw5Cw4vxW0eUw1Bm0kyaz8y0iu1UwbO0"
                    "4CU7a0iK06Ziw19ucw81wXw11d07kw7Yyo39Dlwem6E0zu03lS0i68Q2e02u-0j-0Q8"
                ),
                "__hsdp": (
                    "gdk5a2qE7qitFqq78Ocj8Cl8r8x2i2iEiAl8d9dH4EcEnezEmyE8AkG3Wd22MJqfckGq1Pd5gKeNezq5gztNQoqga_i3FqIlEf"
                    "gxli6DCV9emtajEqbt9yCSoHqaMIsGh2Ez9EKCz88MwzYVO96BhGpxAYgh5FEzJPaIkzEahihoCtX184kIUJ448y5hMB14B8g"
                    "Ay8GA5Qyd4y4FPGy2bhiKimp1u4UV2LBymya4ope8p8x128qBiCF4kGgxh25CkEo8jr8RGAUwxBl8kG5MxADzpohx6bxujGbky"
                    "GxgEX9qFaqWzp6lkajIg48Pxbceghponxy6GDoF2QCl1LAcql1u9mi62289Qu2R9m2eiaxXCyUkwVgF1mfokF2o4Dwi87ai4U"
                    "5ObDAwJzVqwXgkwn49x1ahxm48zzl832FGxmayu5EO0x89U9ECh0aO0tzwh8jwsA99o4FwOwrE3zxmmu2C8wOxq7CA1QF6zHw"
                    "5-wBxK0a_w8i4U2Fw29E1HU4W0AU3xwfG05iU0yO093wei0akw9-1gx60l-4o0AW0jS0tC2O6o6O0t6066U4G0Po1lEfU4G0hq"
                    "1lxu0jG0jm3W0bDwm8"
                ),
                "__hblp": (
                    "1qfgf8vDwaWdpUpwTg13US4pA264A583oxu9zUiwnobo2sxvK1gweC2y8xu2m3u0yFEkwtoSbz8uxGfKUeEG68SmVEW261zxa"
                    "1wK6o3vxKfwIwyw9J0de3S687m2SEOm0IEC598nxC1gG229y9VUO0x89UEwa82IwZwe-0H4322i0x852320la5o423a3lG0zU"
                    "2Ewdvwgo15E17U2DwLxO4oqwi862bwdy0N41VU1082Kwea1-wEwVwfGEb8couw48w961Lwb61rw6NwJl06Qwho1iEpwdW2O16"
                    "x61uw41w6iwpUC2S1gx60l-4o2Zw5jwYw9-EqU5W7E1R-2O6odWwLU6S3mq0iq0m-awnU1koiwuk11yoS2m18wgEgw4LwnE7C1"
                    "dBwNwgEy0S8fU4G0w82lwlonwaa0BUlxy2a0REO3W0bDwiES0gC"
                ),
                "__sjsp": (
                    "gdk5a2qE7qitFqq78Ocj8Cl8r8x2i4N2EiAl8d9dH4EcEnezEZEG295C9vd6cQIkgn5mDsClWqkjq1fd5n4N8X4WdEl2dT7h"
                    "xF0H-MWmH5q_7N7i5qngnxCaIob-maChS3134A8BAwpUfpbwUU6my0m-7EeEjAy4ex-2a266Ea9EbHxaAUixd1XhAqax68hpQ"
                    "EV219ElwFwzAWzy166AFEa4ezE37gng5to550oE-7K1Jg1O8K0zQ585N1F7o1G80Sd0"
                ),
                "__comet_req": "15",
                "fb_dtsg": dynamic_values["fb_dtsg"],
                "jazoest": "25488",
                "lsd": "EDQOkuJlc29JwfK-NlfTw2",
                "__spin_r": "1028355510",
                "__spin_b": "trunk",
                "__spin_t": "1760430360",
                "__crn": "comet.fbweb.CometSearchGlobalSearchDefaultTabRoute",
                "fb_api_caller_class": "RelayModern",
                "fb_api_req_friendly_name": "SearchCometResultsInitialResultsQuery",
                "server_timestamps": "true",
                "variables": json.dumps(variables),
                "doc_id": dynamic_values["queryid"],
            }

            response = self.session.post(
                "https://www.facebook.com/api/graphql/",
                headers=headers,
                data=data,
            )

            # debug_logger.info(f"Status Code: {response.status_code} for keyword: {keyword}")

            try:
                decoder = json.JSONDecoder()
                data_res = decoder.raw_decode(response.text, 0)[0].get("data")
            except Exception as e:
                debug_logger.exception(f"Error decoding JSON response for keyword '{keyword}': {e}")
                break

            try:
                search_results = data_res["serpResponse"]["results"]["edges"]
                page_info = data_res["serpResponse"]["results"]["page_info"]
            except Exception as e:
                debug_logger.exception(f"Error extracting posts/page_info for keyword '{keyword}': {e}")
                break

            all_posts.extend(search_results)
            total_fetched += len(search_results)

            debug_logger.info(f"[{keyword}] Fetched {total_fetched} posts so far.")

            if page_info.get("has_next_page") and page_info.get("end_cursor"):
                cursor = page_info["end_cursor"]
                time.sleep(sleep_time)
            else:
                break

        return all_posts

    # ------------------------------------------------------------
    # Helper static method
    # ------------------------------------------------------------
    @staticmethod
    def get_post_url(post: dict) -> str:
        """
        Lấy permalink_url từ cấu trúc post GraphQL.
        """
        try:
            return (
                post.get("rendering_strategy", {})
                .get("view_model", {})
                .get("click_model", {})
                .get("story", {})
                .get("permalink_url", "URL không tìm thấy")
            )
        except Exception as e:
            return f"Lỗi khi lấy URL: {str(e)}"


# if __name__ == "__main__":
#     my_cookies = (
#         "c_user=61573970626104; "
#         "xs=5:hFy7ChnPLYeLXw:2:1742350510:-1:-1; "
#         "fr=07YKSiNWnUid0xe1y.AWXaOuz7a4FHxojRsYjtBcTe2LH807r3BmHbuw.Bn2iii..AAA.0.0.Bn2iii.AWU_MIpkygM; "
#         "datr=USjaZ2oasqsHJgcnrErwQyLY"
#     )

#     proxy_url = "http://sp08v1-18231:FAGEH@sp08v1-30.proxysystem.net:18231"
#     proxies = {
#         "http": proxy_url,
#         "https": proxy_url,
#     }

#     fb_search = FacebookSearch(my_cookies, proxies=proxies)

#     posts = fb_search.fetch_posts("hieuthuhai", max_posts=3)
#     print(f"Tổng số bài lấy được: {len(posts)}")

#     for post in posts:
#         url = FacebookSearch.get_post_url(post)
#         print(url)
