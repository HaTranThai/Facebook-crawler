import requests
import json
import random
from bs4 import BeautifulSoup
import os
import sys
import time

# Custom exceptions
class RequestTimeoutError(Exception):
    """Raised when a request times out"""
    pass

class RequestError(Exception):
    """Raised when there's an error with the request"""
    pass

class JSONParseError(Exception):
    """Raised when JSON parsing fails"""
    pass

class DataNotFoundError(Exception):
    """Raised when expected data is not found"""
    pass

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

class RequestHandler:
    def __init__(self, proxies=None):
        self.user_agents = USER_AGENTS
        self.session = requests.Session()
        self.proxies = proxies
        self.set_proxies()
        self.response = None

    def set_proxies(self):
        if self.proxies:
            self.session.proxies.update(self.proxies)

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
            }
        }

        # Randomly select OS and browser combination
        os_choice = random.choice(list(os_browser_combinations.keys()))
        browser_choice = random.choice(list(os_browser_combinations[os_choice].keys()))
        
        # Get the headers for the selected combination
        headers = os_browser_combinations[os_choice][browser_choice].copy()
        
        # Add common headers
        headers.update({
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0'
        })

        return headers
    
    def fetch_html(self, url):
        """
        Get the content of a Facebook page
        Args:
            url (str): The Facebook URL to scrape
        Returns:
            requests.Response: The response object from the request
        """
        try:
            headers = self.get_random_headers()
            self.response = requests.get(url, headers=headers, timeout=30)
            self.response.raise_for_status()
            return BeautifulSoup(self.response.text, 'html.parser')
        except requests.Timeout:
            print(f"Request timed out for URL [{url}]")
            raise RequestTimeoutError(f"Request timed out for URL [{url}]")
        except Exception as e:
            print(f"Error fetching the page [{url}]: {e}")
            raise RequestError(f"Error fetching the page [{url}]: {e}")

    def parse_json_from_html(self, html_content: BeautifulSoup, key_to_find: str) -> dict:
        """
        Parses JSON data from HTML by extracting the relevant script block.

        Args:
            html_content (str): The raw HTML content of the page.
            key_to_find (str): The key to look for in the script.

        Returns:
            dict: The parsed JSON object.

        Raises:
            DataNotFoundError: If no valid data is found.
            JSONParseError: If JSON parsing fails.
        """
        try:
            parser = html_content
            for script in parser.find_all('script', type='application/json'):
                if script.string and key_to_find in script.string:
                    json_data = json.loads(script.string)
                    return json_data
            print(f"No valid data found for key '{key_to_find}' in the HTML page.")
            raise DataNotFoundError(f"No valid data found for key '{key_to_find}' in the HTML page.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for key '{key_to_find}': {e}")
            raise JSONParseError(f"Error decoding JSON for key '{key_to_find}': {e}")
        except Exception as e:
            print(f"Unexpected error parsing JSON for key '{key_to_find}': {e}")
            raise JSONParseError(f"Unexpected error parsing JSON for key '{key_to_find}': {e}")

    def get_final_url(self):
        """
        Returns the final URL after any redirects.
        
        Returns:
            str: The final URL.
        """
        if hasattr(self, 'response') and self.response:
            return self.response.url
        return None

    def fetch_all_comments(self, post_id, delay=1):
        comments = []
        after_cursor = None
        while True:
            # cookies = {
            #     'datr': 'UsbsaJDcJw5fuHgdM7jUjn7z',
            #     'sb': 'U8bsaOddZvLbEeoRdtu61Xgx',
            #     'wd': '952x1015',
            #     'ps_l': '1',
            #     'ps_n': '1',
            # }
            cookies = {
                'datr': 'gWVJZqROEYbyiKkplq-Vwwdd',
                'sb': 'gWVJZm-1I8usm1e8btXEpv2f',
                'wd': '1018x695',
                'ps_l': '1',
                'ps_n': '1',
            }

            headers = {
                'accept': '*/*',
                'accept-language': 'vi',
                "accept-encoding": "gzip, deflate",
                'content-type': 'application/x-www-form-urlencoded',
                'origin': 'https://www.facebook.com',
                'priority': 'u=1, i',
                'referer': 'https://www.facebook.com/',
                'sec-ch-prefers-color-scheme': 'dark',
                'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                'sec-ch-ua-full-version-list': '"Microsoft Edge";v="141.0.3537.71", "Not?A_Brand";v="8.0.0.0", "Chromium";v="141.0.7390.66"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-model': '""',
                'sec-ch-ua-platform': '"macOS"',
                'sec-ch-ua-platform-version': '"26.0.1"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
                'x-asbd-id': '359341',
                'x-fb-friendly-name': 'CommentsListComponentsPaginationQuery',
                'x-fb-lsd': 'AdFNt0BQuYA',
            }

            variables = {
                "commentsAfterCount": 50,                  
                "commentsAfterCursor": after_cursor,       
                "commentsBeforeCount": None,
                "commentsBeforeCursor": None,
                "commentsIntentToken": None,
                "feedLocation": "POST_PERMALINK_DIALOG",
                "focusCommentID": None,
                "scale": 2,
                "useDefaultActor": False,
                "id": post_id,
                "__relay_internal__pv__CometUFICommentAvatarStickerAnimatedImagerelayprovider": False,
                "__relay_internal__pv__IsWorkUserrelayprovider": False
            }

            data = {
                'av': '0',
                '__aaid': '0',
                '__user': '0',
                '__a': '1',
                '__req': '8',
                '__hs': '20374.HYP:comet_loggedout_pkg.2.1...0',
                'dpr': '2',
                '__ccg': 'EXCELLENT',
                '__rev': '1028334556',
                '__s': 'g9rbqy:q2lusi:opvhmi',
                '__hsi': '7560635964831994350',
                '__dyn': '7xeUmwlEnwn8yEqxemh0no6u5U4e1Nxt3odEc8co2qwJyE24wJwpUe8hw2nVE4W0qa321Rw8G11wBz83WwgEcEhwGwQw9m1YwBgao6C1uwoE2iyo5m1mzXw8W58jwGzE2ZwJK14xm3y1lUlwhE2FBwxw5wwLyES0QEcU2ZwhEkxe3u362-2B0bK1hxG1FwgWx21JwkEnxyEb8uwjUy2-2K0UE620ui',
                '__csr': 'g8cbgwJ8p5NZEiXl9R8AhunQCmWiuFvZbKp4pFbFGK8DUyrBhoxoKpbDgxu7oyh4iyGDzF8KhemElKqi5qWF28pjl2XG9hUaeay8HxeHyawwwIxq9GU8q8uaDyoCbAwQy8K9oWeRyEvypEy1iwOyE5a1uw4MU0YHy8pyE0tgw_VuLw1kl00zPg0Xci48cA3m56068E8p86u07Do1RU3Mw8i0qR004a9wb60qJm0Io0Ie9o1CU4y0j5w1kO0S81GE1UQ0v2E2iwLw7EwaK0So8EO0-83Kw3cU1G407qy0j81IU0mHw2UFojz81L6022-1zwLw6nzE6m0cwg28w11-0uh0ro1943-6U1nU0Wy0n-0p3xe0nty8J065wdi08cz8S2Jw1hl0so0AiUe81jEfU88gwb5xi6E',
                '__hsdp': 'glA1sJ_I29j2Qo19saggMP2A6sUn88gl5ScOuHx62e9oyl95yel1N3QQlxpkAukFgii0gokAgO7org5u4xUnGUpgdUGm1hggwuE5-Hg1o81dEdE9gw0BpUaQ0xS3d2o2NEQx81i85YM1aV98K8yo3owo8myo0EO0ke784S3C0SU6a0-U1eo461YwdC1FBZ0ru14wp80YC0arwUw2w81pUeE3Rw4Tw',
                '__hblp': '0aa0B60zU6i1aBgK3CUK1nwKxS18wrU6a3Gm0TE3Nwdq0jq1qxa0CUaQ0xU420K9p81i8pwLw4SzU4-5U3qwxwmE1Q81n84S68swjoeo2mwh81nE1eo460OU2awlo22yU4i1Awb60Uo2bw929wlo4S1Gw5IwlEfEy4oR0CwhUe8W4PobE0yu0mu3G0H85q0WUfo',
                '__sjsp': 'glA1sJ_IcNkfQQku1sAiPT3IcNaEQjfA3AUng8h18X9WK3Zy9kA49k74fjho2Yg2vw',
                '__comet_req': '15',
                'lsd': 'AdFNt0BQuYA',
                'jazoest': '2895',
                '__spin_r': '1028334556',
                '__spin_b': 'trunk',
                '__spin_t': '1760347738',
                '__crn': 'comet.fbweb.CometSinglePostDialogRoute',
                'fb_api_caller_class': 'RelayModern',
                'fb_api_req_friendly_name': 'CommentsListComponentsPaginationQuery',
                'server_timestamps': 'true',
                'variables': json.dumps(variables),
                'doc_id': '24572584042352300',
            }
            resp = requests.post('https://www.facebook.com/api/graphql/', cookies=cookies, headers=headers, data=data, proxies=self.proxies)
            
            try:
                decoder = json.JSONDecoder()
                res_json = decoder.raw_decode(resp.text, 0)[0]
            except Exception as e:
                print("Decode error:", e)
                break
            try:
                data_node = res_json.get('data', {}).get('node', {})
                if not data_node:
                    print(f"[WARN] Không có 'data' trong phản hồi cho post_id={post_id}")
                    print("Response snippet:", resp.text[:200])
                    break

                comments_data = data_node.get('comment_rendering_instance_for_feed_location', {}).get('comments', {})
                if not comments_data:
                    print(f"[WARN] Không có comments trong node cho post_id={post_id}")
                    break

                nodes = comments_data.get('edges', [])
                comments.extend(nodes)
                # Kiểm tra nếu còn cursor - next page
                page_info = res_json['data']['node']['comment_rendering_instance_for_feed_location']['comments'].get('page_info')
                if page_info and page_info.get('has_next_page') and page_info.get('end_cursor'):
                    after_cursor = page_info['end_cursor']
                    time.sleep(delay)
                else:
                    break
            except Exception as e:
                print("Không thể lấy thêm comment:", e)
                break
        return comments
