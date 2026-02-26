import os
import asyncio
import time
import json
import psutil
import cld3
from datetime import datetime
from pytz import timezone
from collections import defaultdict
from FacebookSession import FacebookSession
from FacebookScraper2 import FacebookScraperAPI
from FacebookSearch2 import FacebookSearch
from utils_postgres import PostgresFbAccounts
import boto3
from utils.logger import setup_loggers
from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from GoogleSearch import GoogleSearchFacebook

os.makedirs("logs", exist_ok=True)
log_files = [
    "logs/auth.log",
    "logs/search.log",
    "logs/crawl.log",
    "logs/error.log",
    "logs/debug.log",
]
loggers = setup_loggers(log_files)

from langdetect import detect_langs
import cld3
import re

VIETNAMESE_CHARS = "ăâđêôơưĂÂĐÊÔƠƯ"
VI_STOPWORDS = set(
    [
        "và",
        "của",
        "là",
        "cho",
        "một",
        "các",
        "được",
        "với",
        "khi",
        "đã",
        "này",
        "rất",
        "trong",
        "có",
        "đến",
        "vì",
        "như",
        "còn",
        "thì",
    ]
)


def clean_text_vi(text: str) -> str:
    """
    Làm sạch văn bản (phục vụ nhận diện tiếng Việt): loại bỏ link, mention/hashtag,
    ký tự đặc biệt và chuẩn hoá khoảng trắng.

    Input:
        text (str): Chuỗi cần làm sạch.

    Output:
        str: Chuỗi sau khi làm sạch.
    """
    # Xóa link, emoji, mention, ký tự control
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    text = re.sub(r"[@#]\S+", "", text)
    text = re.sub(r"[^\w\sÀ-ỹà-ỹ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_vietnamese_text(text: str) -> bool:
    """
    Xác định text có phải tiếng Việt hay không bằng cách kết hợp:
    - cld3 (xác suất ngôn ngữ)
    - langdetect (xác suất ngôn ngữ)
    - heuristic ký tự tiếng Việt + stopwords

    Input:
        text (str): Chuỗi cần kiểm tra.

    Output:
        bool: True nếu được xem là tiếng Việt, ngược lại False.
    """
    text = clean_text_vi(text)
    if len(text) < 10:
        return False

    try:
        res = cld3.get_language(text)
        if res and res.language == "vi" and res.probability > 0.65:
            return True
    except Exception:
        pass

    try:
        langs = detect_langs(text)
        for lang_prob in langs:
            if lang_prob.lang == "vi" and lang_prob.prob > 0.3:
                return True
    except Exception:
        pass

    vi_chars = sum(ch in VIETNAMESE_CHARS for ch in text)
    vi_stop = sum(1 for w in text.lower().split() if w in VI_STOPWORDS)
    ratio = vi_chars / max(1, len(text))
    if vi_chars >= 2 or vi_stop >= 2 or ratio > 0.01:
        return True
    return False


def check_ngrams_in_data(query, data):
    """
    Kiểm tra keyword/query có xuất hiện trong content của dữ liệu scrape hay không.

    Input:
        query (str): Keyword cần match.
        data (dict): Kết quả scrape (kỳ vọng có data['data'].content).

    Output:
        bool: True nếu query nằm trong content, ngược lại False.
    """
    text_lower = data["data"].content.lower() if data["data"].content else ""

    found_any = False

    query = query.lower()

    if query in text_lower:
        found_any = True
        return found_any

    return found_any


def build_proxies_from_row(proxy_row: dict) -> dict | None:
    """
    Build dict proxies từ một row proxy trong DB.

    Input:
        proxy_row (dict): {'proxy_ip':..., 'proxy_port':..., 'proxy_username':..., 'proxy_password':...}

    Output:
        dict|None: {"http": proxy_url, "https": proxy_url} nếu hợp lệ, ngược lại None.
    """
    """
    proxy_row: {'proxy_ip':..., 'proxy_port':..., 'proxy_username':..., 'proxy_password':...}
    """
    if not proxy_row:
        return None

    ip = proxy_row.get("proxy_ip")
    port = proxy_row.get("proxy_port")
    user = proxy_row.get("proxy_username")
    pwd = proxy_row.get("proxy_password")

    if not ip or not port:
        return None

    if user and pwd:
        proxy_url = f"http://{user}:{pwd}@{ip}:{port}"
    else:
        proxy_url = f"http://{ip}:{port}"

    return {"http": proxy_url, "https": proxy_url}


def format_facebook_data_structure(serializable_data, keyword):
    """
    Chuẩn hoá dữ liệu facebook sau khi scrape về JSON schema thống nhất để lưu MinIO.

    Input:
        serializable_data (dict): Dữ liệu đã được convert sang JSON-serializable.
        keyword (str): Keyword (hoặc url) gắn với dữ liệu.

    Output:
        dict: Payload chuẩn hoá gồm platform, keyword, post, comments.
    """
    post_data = serializable_data["data"]
    return {
        "platform": "facebook",
        "keyword": keyword,
        "post": {
            "author": {
                "user_id": post_data.get("author", {}).get("user_id", ""),
                "name": post_data.get("author", {}).get("name", ""),
                "handle": post_data.get("author", {}).get("handle", ""),
                "url": post_data.get("author", {}).get("url", ""),
            },
            "post_id": post_data.get("post_id", ""),
            "date": post_data.get("date", ""),
            "url": post_data.get("url", ""),
            "title": post_data.get("title", ""),
            "content": post_data.get("content", ""),
            "media": post_data.get("media", ""),
            "count_comment": post_data.get("count_comment", 0),
            "count_like": post_data.get("count_like", 0),
            "count_dislike": post_data.get("count_dislike", 0),
            "count_view": post_data.get("count_view", 0),
            "count_share": post_data.get("count_share", 0),
        },
        "comments": [
            {
                "author": {
                    "user_id": comment.get("author", {}).get("user_id", ""),
                    "name": comment.get("author", {}).get("name", ""),
                    "handle": comment.get("author", {}).get("handle", ""),
                    "url": comment.get("author", {}).get("url", ""),
                },
                "date": comment.get("date", ""),
                "url": comment.get("url", ""),
                "comment_id": comment.get("comment_id", ""),
                "content": comment.get("content", ""),
                "count_like": comment.get("count_like", 0),
                "count_dislike": comment.get("count_dislike", 0),
            }
            for comment in post_data.get("comments", [])
        ],
    }


async def main(search_query, sendtodone=None):
    """
    Crawl dữ liệu facebook theo keyword:
    - Lấy proxy từ DB
    - Lấy danh sách URL (GoogleSearchFacebook và/hoặc FacebookSearch)
    - Scrape từng URL, filter theo keyword, lưu JSON lên MinIO
    - Gửi trạng thái DONE lên Kafka topic service_status
    - In report hiệu suất

    Input:
        search_query (str): Keyword cần crawl.
        sendtodone (datetime|float|None): Mốc thời gian để tính sendtodone; None thì lấy now.

    Output:
        int|bool: Số URL lưu thành công (int) hoặc False nếu fail sớm (proxy/search lỗi).
    """
    crawl_logger = loggers["crawl"]
    error_logger = loggers["error"]
    account_logger = loggers["auth"]
    debug_logger = loggers["debug"]

    metrics = PerformanceMetrics()

    count_url = 0
    postgres = PostgresFbAccounts()
    postgres.connect()
    receivetodone = datetime.now(timezone("Asia/Ho_Chi_Minh"))
    start_time = time.time()
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )

    list_url_by_search = []
    list_url_by_google = []
    list_url = []

    proxy_row = postgres.get_random_proxy()
    proxies = build_proxies_from_row(proxy_row)

    if not proxies:
        error_logger.error("[PROXY] Không lấy được proxy từ DB (get_random_proxy trả về None hoặc thiếu ip/port)")
        postgres.close()
        return False

    # account = postgres.get_random_accounts(task_type="search", limit=1)
    # if not account:
    #     error_logger.error("Không có account có thể sử dụng")
    #     return False

    # account = account[0]

    # proxy_url = f"http://{account['proxy_username']}:{account['proxy_password']}@{account['proxy_ip']}:{account['proxy_port']}"
    # proxies = {"http": proxy_url, "https": proxy_url}

    try:
        # fb_search = FacebookSearch(cookies=account['cookie'], proxies=proxies, keyword=search_query)
        # posts = fb_search.fetch_posts(search_query, max_posts=20)
        posts = []

        gg_search = GoogleSearchFacebook(search_query)
        urls = gg_search.get_url_google_search(
            num_results=50,
            time_range="day",
            region="vn",
            target_count=20,
            max_retry=10,
            delay=2,
        )
        # urls = []

    except Exception as e:
        error_logger.error(f"Lỗi khi tìm kiếm bài viết cho từ khóa {search_query}: {str(e)}")
        postgres.close()
        return False

    # if posts == []:
    #     error_logger.error("Tài khoản {} bị block".format(account['uid']))
    #     postgres.update_account_status(account['uid'], "blocked")
    #     postgres.close()
    #     return False
    # else:
    list_url = [FacebookSearch.get_post_url(post) for post in posts]
    loggers["search"].info(
        f"Keyword {search_query}: Url from google search: {len(urls)} | Url from facebook search: {len(list_url)}"
    )
    list_url.extend(urls)
    # postgres.increment_account_usage(account['uid'])

    from FacebookSession import FacebookScraper

    scraper = FacebookScraper(proxies=proxies)

    for url in list_url:
        crawl_start = time.time()
        try:
            data = await scraper.scrape(url)
            if not data.get("data"):
                error_logger.error(f"Failed to scrape {url}, skipping...")
                metrics.record_page_processed(success=False)
                continue
            crawl_latency = time.time() - crawl_start
            metrics.record_crawl_latency(crawl_latency)

            if check_ngrams_in_data(search_query, data):
                processing_start = time.time()
                try:

                    def to_serializable(obj):
                        if hasattr(obj, "to_dict"):
                            return obj.to_dict()
                        elif hasattr(obj, "__dict__"):
                            return {k: to_serializable(v) for k, v in obj.__dict__.items()}
                        elif isinstance(obj, list):
                            return [to_serializable(i) for i in obj]
                        elif isinstance(obj, dict):
                            return {k: to_serializable(v) for k, v in obj.items()}
                        else:
                            return obj

                    serializable_data = to_serializable(data)

                    data_to_insert = format_facebook_data_structure(serializable_data, search_query)
                    object_key = f"mention-{search_query.replace(' ', '_')}-normal-{data_to_insert['post']['post_id']}"
                    s3.put_object(
                        Bucket=os.getenv("MINIO_BUCKET_NAME"),
                        Key=object_key,
                        Body=json.dumps(data_to_insert),
                        ContentType="application/json",
                    )
                    crawl_logger.info(f"Đã lưu dữ liệu vào MinIO: {object_key}")
                    count_url += 1

                    processing_lag = time.time() - processing_start
                    metrics.record_processing_lag(processing_lag)
                    metrics.record_page_processed(success=True)

                except Exception as e:
                    error_logger.error(f"Lỗi khi xử lý và lưu dữ liệu cho URL {url}: {str(e)}")
                    metrics.record_page_processed(success=False)
                    continue
            else:
                crawl_logger.info(f"Check n-grams không khớp cho URL {url}, bỏ qua.")
                metrics.record_page_processed(success=False)

        except Exception as e:
            error_logger.error(f"Lỗi khi crawl URL {url}: {str(e)}")
            metrics.record_page_processed(success=False)
            if "proxy" in str(e).lower() or "connection" in str(e).lower():
                metrics.record_proxy_failure()
            continue

        if metrics.pages_processed % 5 == 0:
            metrics.record_system_usage()

    crawl_logger.info(f"Đã xử lý {count_url} URL")

    if not sendtodone:
        sendtodone = datetime.now(timezone("Asia/Ho_Chi_Minh"))

    receivetodone = (datetime.now(timezone("Asia/Ho_Chi_Minh")) - receivetodone).total_seconds()
    sendtodone_seconds = (
        (datetime.now(timezone("Asia/Ho_Chi_Minh")) - sendtodone).total_seconds()
        if isinstance(sendtodone, datetime)
        else float(sendtodone)
    )

    data_kafka = {
        "platform": "facebook" if os.getenv("KAFKA_TOPIC") == "keyword" else "facebook-update",
        "crawl_value": search_query,
        "status": "done",
        "info": "normal",
        "count": count_url,
        "sendtodone": sendtodone_seconds,
        "receivetodone": receivetodone,
        "time_done": datetime.now(timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
    }

    producer = KafkaProducer(
        bootstrap_servers=["keyword-broker-1:9093", "keyword-broker-2:9093"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        future = producer.send("service_status", data_kafka)
        record_metadata = future.get(timeout=10)
        crawl_logger.info(f"Đã gửi thông báo về Kafka topic service_status: offset {record_metadata.offset}")
    except Exception as e:
        error_logger.error(f"Lỗi khi gửi thông báo Kafka: {str(e)}")
    finally:
        producer.close()

    metrics.print_evaluation(search_query, crawl_logger)
    postgres.close()

    return count_url


async def main_link(url, sendtodone=None):
    """
    Crawl dữ liệu theo một URL cụ thể:
    - Lấy account + proxy từ DB
    - Dùng FacebookScraperAPI scrape group/page hoặc 1 post
    - Lưu JSON lên MinIO
    - Gửi trạng thái DONE lên Kafka topic service_status

    Input:
        url (str): Link cần crawl.
        sendtodone (datetime|float|None): Mốc thời gian để tính sendtodone; None thì lấy now.

    Output:
        int|bool: Số record lưu thành công (int) hoặc False nếu không có account.
    """
    crawl_logger = loggers["crawl"]
    error_logger = loggers["error"]
    account_logger = loggers["auth"]

    # Khởi tạo performance metrics
    metrics = PerformanceMetrics()

    count_url = 0
    postgres = PostgresFbAccounts()
    postgres.connect()
    receivetodone = datetime.now(timezone("Asia/Ho_Chi_Minh"))
    start_time = time.time()
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )
    account = postgres.get_random_accounts(task_type="search", limit=1)
    if not account:
        error_logger.error("Không có account có thể sử dụng")
        return False

    account = account[0]

    proxy_url = f"http://{account['proxy_username']}:{account['proxy_password']}@{account['proxy_ip']}:{account['proxy_port']}"
    proxies = {"http": proxy_url, "https": proxy_url}
    print("Using account:", account["uid"])

    list_url_by_search = []
    list_url_by_google = []
    list_url = []
    session_created = False

    print("Using account:", account["uid"])
    scraper = FacebookScraperAPI(proxies=proxies, access_token=account["token"], cookie=account["cookie"])
    parts = [p for p in url.split("/") if p]
    if len(parts) <= 5:
        for post in scraper.scrape_post_page_group(url):
            try:
                # print(post.get('success'))
                data_obj = post.get("data") if isinstance(post, dict) else post

                # Lấy title
                title_content = getattr(data_obj, "title", None) or getattr(data_obj, "content", None) or ""
                # if not title_content:
                #     print(f"Skipping post không có nội dung: {url}")
                #     continue

                # # Check ngôn ngữ
                # if not is_vietnamese_text(title_content):
                #     print(f"Skipping non-Vietnamese post: {url}")
                #     continue

                # Chuyển object sang JSON-serializable dict
                def to_serializable(obj):
                    if hasattr(obj, "to_dict"):
                        return obj.to_dict()
                    elif hasattr(obj, "__dict__"):
                        return {k: to_serializable(v) for k, v in obj.__dict__.items()}
                    elif isinstance(obj, list):
                        return [to_serializable(i) for i in obj]
                    elif isinstance(obj, dict):
                        return {k: to_serializable(v) for k, v in obj.items()}
                    else:
                        return obj

                serializable_data = to_serializable(data_obj)
                serializable_data = {"data": to_serializable(data_obj)}
                data_to_insert = format_facebook_data_structure(serializable_data, url)
                object_key = f"mention-{url.split('/')[-1]}-link-{data_to_insert['post']['post_id']}"
                s3.put_object(
                    Bucket=os.getenv("MINIO_BUCKET_NAME"),
                    Key=object_key,
                    Body=json.dumps(data_to_insert),
                    ContentType="application/json",
                )
                crawl_logger.info(f"Đã lưu dữ liệu vào MinIO: {object_key}")
                count_url += 1
            except Exception as e:
                error_logger.error(f"Lỗi khi xử lý và lưu dữ liệu cho URL {url}: {str(e)}")
                continue
    else:
        post = scraper.scrape_info(url)
        if post.get("success"):
            # Check if title content is in Vietnamese
            title_content = post.get("data", {}).get("title", "") if post.get("data") else ""
            # If title is empty, use content instead
            if not title_content:
                title_content = post.get("data", {}).get("content", "") if post.get("data") else ""
            if not is_vietnamese_text(title_content):
                crawl_logger.info(f"Skipping non-Vietnamese single post: {url}")
            else:
                data_to_insert = format_facebook_data_structure(post, url)
                object_key = f"mention-{url.split('/')[-1]}-link-{data_to_insert['post']['post_id']}"
                s3.put_object(
                    Bucket=os.getenv("MINIO_BUCKET_NAME"),
                    Key=object_key,
                    Body=json.dumps(data_to_insert),
                    ContentType="application/json",
                )
                crawl_logger.info(f"Đã lưu dữ liệu vào MinIO: {object_key}")
                count_url += 1
        else:
            error_logger.error(f"Lỗi khi xử lý và lưu dữ liệu cho URL {url}: {post.get('message')}")
            count_url += 1

    # --- FORMAT KAFKA DONE ---
    if not sendtodone:
        sendtodone = datetime.now(timezone("Asia/Ho_Chi_Minh"))

    receivetodone = (datetime.now(timezone("Asia/Ho_Chi_Minh")) - receivetodone).total_seconds()
    sendtodone_seconds = (
        (datetime.now(timezone("Asia/Ho_Chi_Minh")) - sendtodone).total_seconds()
        if isinstance(sendtodone, datetime)
        else float(sendtodone)
    )

    data_kafka = {
        "platform": "facebook" if os.getenv("KAFKA_TOPIC") == "keyword" else "facebook-update",
        "crawl_value": url,
        "status": "done",
        "info": "link",
        "count": count_url,
        "sendtodone": sendtodone_seconds,
        "receivetodone": receivetodone,
        "time_done": datetime.now(timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Send to Kafka
    producer = KafkaProducer(
        bootstrap_servers=["keyword-broker-1:9093", "keyword-broker-2:9093"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        future = producer.send("service_status", data_kafka)
        record_metadata = future.get(timeout=10)
        crawl_logger.info(f"Đã gửi thông báo về Kafka topic service_status: offset {record_metadata.offset}")
    except Exception as e:
        error_logger.error(f"Lỗi khi gửi thông báo Kafka: {str(e)}")
    finally:
        producer.close()

    postgres.increment_account_usage(account["uid"])
    postgres.close()

    return count_url


class KafkaConsumerHandler:
    def __init__(self):
        """
        Khởi tạo Kafka consumer/producer:
        - consumer đọc message từ topic cấu hình trong env
        - producer dùng để gửi message begintwork / service_status
        - cấu hình commit offset thủ công

        Input:
            None

        Output:
            None
        """
        self.consumer = KafkaConsumer(
            os.getenv("KAFKA_TOPIC"),
            bootstrap_servers=os.getenv("KAFKA_HOST"),
            auto_offset_reset="latest",
            enable_auto_commit=False,
            group_id="facebook-group",
            max_poll_records=1,
            max_poll_interval_ms=600000,
            session_timeout_ms=60000,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=["keyword-broker-1:9093", "keyword-broker-2:9093"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.error_logger = loggers["error"]

    def calculate_begintowork_time(self, timestamp_str):
        """
        Calculate thời gian chênh lệch (giây) từ timestamp trong message đến thời điểm hiện tại.

        Input:
            timestamp_str (str|None): Timestamp (ISO8601 hoặc "%Y-%m-%d %H:%M:%S").

        Output:
            float: Số giây chênh lệch; 0.0 nếu timestamp_str rỗng hoặc parse lỗi.
        """
        """Calculate the time difference from timestamp to now in seconds"""
        try:
            if timestamp_str:
                # Try to parse ISO 8601 format first (with timezone)
                try:
                    from datetime import datetime as dt

                    timestamp_dt = dt.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                except ValueError:
                    # Fallback to the original format
                    timestamp_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                    # Convert to timezone-aware datetime
                    timestamp_dt = timezone("Asia/Ho_Chi_Minh").localize(timestamp_dt)

                # Get current time
                current_time = datetime.now(timezone("Asia/Ho_Chi_Minh"))
                # Calculate difference in seconds
                time_diff = (current_time - timestamp_dt).total_seconds()
                return time_diff
            else:
                return 0.0
        except Exception as e:
            self.error_logger.error(f"Error calculating begintowork time: {str(e)}")
            return 0.0

    def send_begintowork_message(self, keyword, stage="new", begintowork_seconds=0.0):
        """
        Gửi message lên topic 'begintwork' để ghi nhận thời gian bắt đầu xử lý keyword/link.

        Input:
            keyword (str): keyword hoặc link.
            stage (str): 'new' hoặc 'update'.
            begintowork_seconds (float): Thời gian chênh lệch (giây).

        Output:
            None
        """
        """Send a message to the 'begintwork' topic when starting to process a keyword"""
        try:
            begintowork_data = {
                "keyword": keyword,
                "begintowork": begintowork_seconds,
                "platforms": "facebook",
                "stage": stage,
                "timestamp": datetime.now(timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
            }

            # Send to 'begintwork' topic
            future = self.producer.send("begintwork", begintowork_data)
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            self.error_logger.info(
                f"Sent begintowork message for keyword '{keyword}' to topic 'begintwork' at offset {record_metadata.offset} (begintowork: {begintowork_seconds:.2f}s)"
            )

        except Exception as e:
            self.error_logger.error(f"Error sending begintowork message for keyword '{keyword}': {str(e)}")

    def process_message(self, message, sendtodone):
        """
        Parse và xử lý 1 message Kafka:
        - đọc JSON payload
        - xác định keyword hoặc keyword_link
        - gọi main() hoặc main_link()
        - trả về True/False để quyết định commit offset

        Input:
            message: Kafka message object.
            sendtodone (datetime|float): Mốc thời gian để tính sendtodone.

        Output:
            bool: True nếu xử lý OK và result > 0, ngược lại False.
        """
        try:
            data = json.loads(message.value.decode("utf-8"))
            self.error_logger.info(f"Platforms in message: {data.get('platforms', [])}")

            if os.getenv("KAFKA_TOPIC") == "keyword":
                start_time = data.get("timestamp")
                stage = "new"
            else:
                start_time = data.get("start_time")
                stage = "update"

            # keyword
            if data.get("keyword"):
                kw = data.get("keyword")
                begintowork_seconds = self.calculate_begintowork_time(start_time)
                self.send_begintowork_message(kw, stage, begintowork_seconds)

                result = asyncio.run(main(kw, sendtodone=sendtodone))
                return (result is not False) and (int(result) > 0)

            # keyword_link
            if data.get("keyword_link"):
                link = data.get("keyword_link")
                begintowork_seconds = self.calculate_begintowork_time(start_time)
                self.send_begintowork_message(link, stage, begintowork_seconds)

                result = asyncio.run(main_link(link, sendtodone=sendtodone))
                return (result is not False) and (int(result) > 0)

            self.error_logger.error(f"No keyword or keyword_link found in message: {data}")
            return False

        except (json.JSONDecodeError, Exception) as e:
            self.error_logger.error(f"Error processing message: {e}")
            return False

    def start_consuming(self):
        """
        Vòng lặp poll Kafka:
        - đọc message theo batch nhỏ
        - xử lý 1 message mỗi lần
        - retry theo offset tối đa 3 lần; fail đủ thì commit để skip
        - tự reconnect khi gặp lỗi fatal

        Input:
            None

        Output:
            None
        """
        sendtodone = datetime.now(timezone("Asia/Ho_Chi_Minh"))
        empty_poll_count = 0
        max_empty_polls = 5

        # Đếm số lần retry theo message offset (topic, partition, offset)
        retry_counts = defaultdict(int)
        max_retries = 3

        while True:
            try:
                message_pack = self.consumer.poll(timeout_ms=1000, max_records=3)
                found_message = False

                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        found_message = True

                        tp = TopicPartition(message.topic, message.partition)
                        key = (message.topic, message.partition, message.offset)

                        ok = self.process_message(message, sendtodone)

                        if ok:
                            # Thành công => commit + reset retry count
                            offsets = {tp: OffsetAndMetadata(message.offset + 1, None)}
                            self.consumer.commit(offsets)
                            self.error_logger.info("Offsets committed successfully.")
                            retry_counts.pop(key, None)

                        else:
                            # Fail => tăng retry
                            retry_counts[key] += 1
                            n = retry_counts[key]

                            if n >= max_retries:
                                # QUAN TRỌNG: fail >= 3 lần => commit luôn để bỏ qua keyword
                                self.error_logger.error(
                                    f"Failed {n} times for offset {message.offset}. "
                                    f"Committing offset to skip this message."
                                )
                                offsets = {tp: OffsetAndMetadata(message.offset + 1, None)}
                                self.consumer.commit(offsets)
                                retry_counts.pop(key, None)
                            else:
                                # Chưa đủ 3 lần => không commit, seek để retry
                                self.error_logger.error(
                                    f"Not committing offset {message.offset} (attempt {n}/{max_retries}). Will retry."
                                )
                                self.consumer.seek(tp, message.offset)
                                time.sleep(2)

                        break  # chỉ xử lý 1 message
                    break  # chỉ 1 partition

                if not found_message:
                    empty_poll_count += 1
                    if empty_poll_count >= max_empty_polls:
                        break
                else:
                    empty_poll_count = 0

            except KeyboardInterrupt:
                self.error_logger.info("Consumer stopped by user.")
                break

            except Exception as e:
                self.error_logger.error(f"Fatal error in consumer: {e}")
                try:
                    self.consumer.close()
                    self.producer.close()
                except Exception:
                    pass

                self.error_logger.info("Kafka consumer and producer closed. Will reconnect in 5 seconds...")
                time.sleep(5)

                self.consumer = KafkaConsumer(
                    os.getenv("KAFKA_TOPIC"),
                    bootstrap_servers=os.getenv("KAFKA_HOST"),
                    auto_offset_reset="latest",
                    enable_auto_commit=False,
                    group_id="facebook-group",
                    max_poll_records=1,
                    max_poll_interval_ms=600000,
                    session_timeout_ms=60000,
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_HOST"),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )


class PerformanceMetrics:
    def __init__(self):
        """
        Lưu trữ và tính toán các chỉ số hiệu suất crawl:
        số trang, success/fail, latency, CPU/RAM, lỗi proxy...

        Input:
            None

        Output:
            None
        """
        self.start_time = time.time()
        self.pages_processed = 0
        self.pages_successful = 0
        self.pages_failed = 0
        self.proxy_failures = 0
        self.total_crawl_time = 0
        self.total_processing_time = 0
        self.cpu_usage = []
        self.ram_usage = []
        self.crawl_latencies = []
        self.processing_lag = []

    def record_page_processed(self, success=True):
        """
        Ghi nhận 1 page đã được xử lý (tăng counters success/fail).

        Input:
            success (bool): True nếu thành công, False nếu thất bại.

        Output:
            None
        """
        self.pages_processed += 1
        if success:
            self.pages_successful += 1
        else:
            self.pages_failed += 1

    def record_crawl_latency(self, latency):
        """
        Ghi nhận độ trễ crawl (giây) cho 1 URL.

        Input:
            latency (float): Thời gian crawl (seconds).

        Output:
            None
        """
        self.crawl_latencies.append(latency)
        self.total_crawl_time += latency

    def record_processing_lag(self, lag):
        """
        Ghi nhận độ trễ xử lý (giây) cho 1 URL (serialize + lưu).

        Input:
            lag (float): Thời gian xử lý (seconds).

        Output:
            None
        """
        self.processing_lag.append(lag)
        self.total_processing_time += lag

    def record_proxy_failure(self):
        """
        Tăng bộ đếm lỗi proxy.

        Input:
            None

        Output:
            None
        """
        self.proxy_failures += 1

    def record_system_usage(self):
        """
        Ghi nhận mức sử dụng CPU/RAM hiện tại.

        Input:
            None

        Output:
            None
        """
        cpu_percent = psutil.cpu_percent(interval=1)
        ram_percent = psutil.virtual_memory().percent
        self.cpu_usage.append(cpu_percent)
        self.ram_usage.append(ram_percent)

    def get_evaluation_report(self, keyword):
        """
        Tạo dict report tổng hợp các metrics hiệu suất.

        Input:
            keyword (str): Keyword đang xử lý.

        Output:
            dict: Report metrics (time, pages/min, success rate, latency, CPU/RAM, proxy failures...).
        """
        total_time = time.time() - self.start_time
        minutes = total_time / 60

        # Tính toán các metrics
        pages_per_minute = self.pages_processed / minutes if minutes > 0 else 0
        success_rate = (self.pages_successful / self.pages_processed * 100) if self.pages_processed > 0 else 0
        avg_crawl_latency = sum(self.crawl_latencies) / len(self.crawl_latencies) if self.crawl_latencies else 0
        avg_processing_lag = sum(self.processing_lag) / len(self.processing_lag) if self.processing_lag else 0
        avg_cpu = sum(self.cpu_usage) / len(self.cpu_usage) if self.cpu_usage else 0
        avg_ram = sum(self.ram_usage) / len(self.ram_usage) if self.ram_usage else 0
        proxy_failure_rate = (self.proxy_failures / self.pages_processed * 100) if self.pages_processed > 0 else 0

        report = {
            "keyword": keyword,
            "total_time_minutes": round(minutes, 2),
            "pages_processed": self.pages_processed,
            "pages_successful": self.pages_successful,
            "pages_failed": self.pages_failed,
            "pages_per_minute": round(pages_per_minute, 2),
            "success_rate_percent": round(success_rate, 2),
            "avg_crawl_latency_seconds": round(avg_crawl_latency, 2),
            "avg_processing_lag_seconds": round(avg_processing_lag, 2),
            "avg_cpu_usage_percent": round(avg_cpu, 2),
            "avg_ram_usage_percent": round(avg_ram, 2),
            "proxy_failures": self.proxy_failures,
            "proxy_failure_rate_percent": round(proxy_failure_rate, 2),
            "data_yield": self.pages_successful,
            "timestamp": datetime.now(timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"),
        }

        return report

    def print_evaluation(self, keyword, crawl_logger):
        """
        In report hiệu suất ra logger crawl.

        Input:
            keyword (str): Keyword đang xử lý.
            crawl_logger: Logger để ghi log.

        Output:
            None
        """
        report = self.get_evaluation_report(keyword)

        crawl_logger.info("\n" + "=" * 80)
        crawl_logger.info(f"📊 ĐÁNH GIÁ HIỆU SUẤT CRAWLER - KEYWORD: {keyword}")
        crawl_logger.info("=" * 80)
        crawl_logger.info(f"⏱️  Tổng thời gian xử lý: {report['total_time_minutes']} phút")
        crawl_logger.info(f"📄 Tổng số trang xử lý: {report['pages_processed']}")
        crawl_logger.info(f"✅ Trang thành công: {report['pages_successful']}")
        crawl_logger.info(f"❌ Trang thất bại: {report['pages_failed']}")
        crawl_logger.info(f"🚀 Tốc độ crawl: {report['pages_per_minute']} trang/phút")
        crawl_logger.info(f"📈 Tỷ lệ thành công: {report['success_rate_percent']}%")
        crawl_logger.info(f"⏳ Độ trễ crawl trung bình: {report['avg_crawl_latency_seconds']} giây")
        crawl_logger.info(f"💾 Độ trễ xử lý DB trung bình: {report['avg_processing_lag_seconds']} giây")
        crawl_logger.info(f"🖥️  CPU trung bình: {report['avg_cpu_usage_percent']}%")
        crawl_logger.info(f"🧠 RAM trung bình: {report['avg_ram_usage_percent']}%")
        crawl_logger.info(f"🌐 Lỗi proxy: {report['proxy_failures']} ({report['proxy_failure_rate_percent']}%)")
        crawl_logger.info(f"📊 Sản lượng dữ liệu: {report['data_yield']} records")
        crawl_logger.info("=" * 80 + "\n")


if __name__ == "__main__":
    kafka_consumer = KafkaConsumerHandler()
    while True:
        try:
            kafka_consumer.start_consuming()
        except KeyboardInterrupt:
            print("Shutting down gracefully...")
            break
