import time
import requests
from typing import List, Set

class GoogleSearchFacebook:
    def __init__(self, keyword: str):
        self.keyword = keyword

    def get_url_google_search(
        self,
        num_results: int = 50,
        time_range: str = "day",
        region: str = "vn",
        target_count: int = 20,   # số lượng URL muốn đạt
        max_retry: int = 10,      # số lần search tối đa
        delay: float = 1.5        # delay mỗi lần search
    ) -> List[str]:
        collected: Set[str] = set()  # lưu URL không trùng lặp

        for attempt in range(1, max_retry + 1):
            try:
                headers = {"accept": "application/json"}
                params = {
                    "site": "www.facebook.com",
                    "keyword": self.keyword,
                    "num_results": num_results,
                    "time_range": time_range,
                    "region": region,
                }

                response = requests.get(
                    "http://192.167.117.36:2003/api/v1/search",
                    params=params,
                    headers=headers,
                    timeout=20,   
                )
                response.raise_for_status()
                data = response.json()

                for item in data.get("data", []):
                    url_str = item.get("url", "") 

                    if not url_str:
                        continue

                    if "groups" in url_str:
                        continue

                    if any(x in url_str for x in ["/posts/", "/videos/", "/watch/"]):
                        clean_url = url_str.split("?", 1)[0]
                        collected.add(clean_url)

                if len(collected) >= target_count:
                    return list(collected)

                time.sleep(delay)

            except Exception as e:
                print(f"[ERROR] Attempt {attempt}: {e}")

        return list(collected)