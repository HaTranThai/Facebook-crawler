import time
import requests

class GoogleSearchFacebook:
    def __init__(self, keyword: str):
        self.keyword = keyword

    def get_url_google_search(
        self,
        num_results: int = 50,
        time_range: str = 'day',
        region: str = 'vn',
        target_count: int = 20,   # số lượng URL muốn đạt
        max_retry: int = 20,      # số lần search tối đa
        delay: float = 1.5        # delay mỗi lần search
    ):
        collected = set()  # lưu URL không trùng lặp

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
                    headers=headers
                )
                response.raise_for_status()
                data = response.json()

                # lọc kết quả
                for item in data.get("data", []):
                    url_str = item["url"]

                    if "groups" in url_str:
                        continue
                    
                    if any(x in url_str for x in ["/posts/", "/videos/", "/watch/"]):
                        clean_url = url_str.split("?")[0]
                        collected.add(clean_url)

                print(f"[{attempt}] Đã thu thập được {len(collected)} URL…")

                # ĐỦ SỐ LƯỢNG → RETURN
                if len(collected) >= target_count:
                    print(f"🎉 Đủ {target_count} URL – dừng search.")
                    return list(collected)

                time.sleep(delay)

            except Exception as e:
                print(f"[ERROR] Attempt {attempt}: {e}")

        print("⚠️ Không đủ URL theo yêu cầu, trả về tất cả URL thu được.")
        return list(collected)


if __name__ == "__main__":
    keyword = "HDPE"
    google_search = GoogleSearchFacebook(keyword)

    urls = google_search.get_url_google_search(
        num_results=50,
        time_range='day',
        region='vn',
        target_count=20,   # cần đủ 20 link
        max_retry=15,      # thử tối đa 15 lượt
        delay=2            # mỗi lần cách nhau 2s
    )

    print("\nKẾT QUẢ CUỐI CÙNG:")
    for i, u in enumerate(urls, 1):
        print(f"{i:02d}. {u}")
