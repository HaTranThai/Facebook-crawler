from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Optional
import time
import random
import asyncio
import requests

if TYPE_CHECKING:
    from FacebookSession import FacebookSession

class FacebookSearch:
    parent: ClassVar[FacebookSession]

    keyword: Optional[str]

    as_dict: dict

    def __init__(self, keyword: Optional[str] = None):
        self.keyword = keyword

    async def get_url_google_search(self, num_results: int = 50, time_range: str = 'day', region: str = 'vn'):
        try:
            headers = {
                'accept': 'application/json',
            }
            params = {
                'site': 'www.facebook.com',
                'keyword': self.keyword,
                'num_results': num_results,
                'time_range': time_range,
                # 'cutoff_time': 1,
                'region': region,
            }

            response = requests.get('http://192.167.117.36:2003/search/', params=params, headers=headers)
            data = response.json()
            filtered_results = []
            for url in data.get("results", []):
                url_str = url["url"]
                # Bỏ qua URL nếu chứa "groups"
                if "groups" in url_str:
                    continue
                # Chỉ lấy các URL posts, videos, reels
                if any(x in url_str for x in ["/posts/", "/videos/", "/watch/"]):
                    filtered_results.append(url_str.split("?")[0])

            return list(set(filtered_results))
        except Exception as e:
            print(f"Error: {e}")
            return []

    async def search_by_keyword(self, count: int = 10, **kwargs):
        """
        Playwright version of search: scrolls, hovers <a>, extracts hrefs, returns list of URLs.
        Optimized to reduce hover lag and improve scrolling performance.
        """
        found = 0
        url_list = set()
        processed_elements = set()  # Track processed elements to avoid re-hovering
        self.keyword = self.keyword.replace("#", "%23")
        self.page = self.parent.session.page
        await self.page.set_viewport_size({'width': 1920, 'height': 1080})
        await self.page.goto(f"https://www.facebook.com/search/posts?q={self.keyword}&filters=eyJyZWNlbnRfcG9zdHM6MCI6IntcIm5hbWVcIjpcInJlY2VudF9wb3N0c1wiLFwiYXJnc1wiOlwiXCJ9In0%3D")
        await asyncio.sleep(random.uniform(1, 3))

        scroll_distance = 1000  # Reduced scroll distance for smoother scrolling
        previous_urls_count = 0
        start_time = time.time()
        timeout = 180  # seconds

        while found < count:
            if time.time() - start_time > timeout:
                if hasattr(self, 'error_logger'):
                    self.error_logger.warning("Đã vượt quá thời gian tìm kiếm (200 seconds). Dừng tìm kiếm.")
                break

            # Find elements
            try:
                await asyncio.sleep(1)  # Reduced sleep time
                elements = await self.page.query_selector_all('xpath=//div[contains(@id, "r") and count(span) >= 3]/span[position() = last()-2]')
                
                # Filter out already processed elements
                new_elements = []
                for element in elements:
                    try:
                        # Get element position as unique identifier
                        bounding_box = await element.bounding_box()
                        if bounding_box:
                            element_id = f"{bounding_box['x']}_{bounding_box['y']}"
                            if element_id not in processed_elements:
                                new_elements.append((element, element_id))
                    except:
                        continue

                # Process new elements only
                urls = await self._process_elements_batch(new_elements, processed_elements)
                
                if urls:
                    for url in urls:
                        if url not in url_list:
                            url_list.add(url)
                    if len(url_list) > previous_urls_count:
                        previous_urls_count = len(url_list)
                
                # --- Thêm xử lý reels elements ---
                reels_elements = await self.page.query_selector_all("xpath=//a[contains(translate(@aria-label, 'REELS', 'reels'), 'reels')]")
                reels_urls = await self._process_reels_elements(reels_elements)
                for url in reels_urls:
                    if url not in url_list:
                        url_list.add(url)
                # --- Kết thúc thêm ---

            except Exception as e:
                if hasattr(self, 'error_logger'):
                    self.error_logger.error(f"Lỗi khi lấy URL: {e}")

            # Check if we have enough URLs before scrolling
            if len(url_list) >= count:
                if hasattr(self, 'search_logger'):
                    self.search_logger.info(f"Đã tìm đủ {count} URL và tổng số URL là {len(url_list)}")
                break

            # Smooth scroll with multiple small steps
            await self._smooth_scroll(self.page, scroll_distance)

        return list(url_list)

    async def _process_elements_batch(self, elements_with_ids, processed_elements):
        """
        Process elements one by one, not in batch, to ensure each link is handled sequentially.
        """
        urls = set()
        for element, element_id in elements_with_ids:
            result = await self._process_single_element(element, element_id, processed_elements)
            if isinstance(result, str) and result:
                urls.add(result)
            await asyncio.sleep(0.2)  # Small pause between each element
        return urls

    async def _process_single_element(self, element, element_id, processed_elements, delay=0):
        """
        Process a single element with optimized hovering
        """
        try:
            # Add small delay to stagger hover operations
            if delay > 0:
                await asyncio.sleep(delay)
                
            # Check if element is still in viewport before hovering
            bounding_box = await element.bounding_box()
            if not bounding_box or bounding_box['y'] < 0:
                return None
                
            link_element = await element.query_selector('a')
            if link_element is None:
                return None
                
            # Quick hover with increased wait time
            await link_element.hover()
            await asyncio.sleep(0.5)
            
            await self.page.mouse.move(0, 0)
            await asyncio.sleep(0.1)

            url = await link_element.get_attribute('href')
            processed_elements.add(element_id)
            
            if url and ("/posts/" in url or "/videos/" in url or "/watch/" in url):
                url = url.split("?")[0]
                return url
                
        except Exception:
            pass
        
        return None

    async def _smooth_scroll(self, page, total_distance):
        """
        Perform smooth scrolling in smaller increments
        """
        steps = 4  # Break scroll into 4 steps
        step_distance = total_distance // steps
        
        for i in range(steps):
            try:
                await page.evaluate(f"window.scrollBy(0, {step_distance});")
                await asyncio.sleep(0.5)  # Small pause between scroll steps
            except Exception as e:
                if hasattr(self, 'error_logger'):
                    self.error_logger.error(f"Lỗi khi cuộn trang: {e}")
                break
                
        await asyncio.sleep(0.3)  # Final pause after scrolling

    async def _process_reels_elements(self, reels_elements):
        urls = set()
        for element in reels_elements:
            try:
                url = await element.get_attribute('href')
                if not url.startswith("http"):
                    url = "https://www.facebook.com/" + url.lstrip("/")
                if url and "/reel/" in url:
                    url = url.split("?")[0]
                    urls.add(url)
            except Exception:
                continue
        return urls

    def __repr__(self):
        return self.__str__()
    
    def __str__(self):
        return f"FacebookSearch(keyword={self.keyword})"