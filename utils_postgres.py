import psycopg2
from psycopg2 import OperationalError
from typing import List, Dict, Optional
import random

class PostgresFbAccounts:
    """
    Class quản lý kết nối và truy vấn dữ liệu từ bảng fb_accounts trong PostgreSQL.
    """
    def __init__(self, host="postgres", port=5432, user="postgres", password="postgres", dbname="postgres"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.connection = None

    def connect(self):
        """Kết nối đến database"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
            return True
        except OperationalError as e:
            print(f"Lỗi kết nối: {e}")
            return False

    def close(self):
        """Đóng kết nối"""
        if self.connection:
            self.connection.close()

    def get_random_proxy(self) -> Dict:
        """
        Lấy ngẫu nhiên một proxy từ bảng proxies
        
        Returns:
            Dict: Thông tin proxy
        """
        if not self.connection:
            if not self.connect():
                return None

        try:
            with self.connection.cursor() as cursor:
                query = """
                    SELECT p.ip as proxy_ip, p.port as proxy_port, 
                           p.username as proxy_username, p.password as proxy_password
                    FROM proxies p
                    ORDER BY RANDOM()
                    LIMIT 1
                """
                cursor.execute(query)
                proxy = cursor.fetchone()
                return proxy
        except Exception as e:
            print(f"Lỗi khi lấy proxy: {e}")
            return None

    def get_random_accounts(self, task_type: str, limit: int = 1) -> List[Dict]:
        """
        Lấy ngẫu nhiên các account có thể sử dụng theo task_type
        
        Args:
            task_type (str): Loại task ('crawl' hoặc 'search')
            limit (int): Số lượng account cần lấy
            
        Returns:
            List[Dict]: Danh sách account với thông tin proxy
        """
        if not self.connection:
            if not self.connect():
                return []

        try:
            with self.connection.cursor() as cursor:
                query = """
                    WITH available_account AS (
                        SELECT a.*, 
                            CASE 
                                WHEN a.last_used_at < CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN true
                                WHEN a.usage_count < 7 THEN true
                                ELSE false
                            END as can_use
                        FROM accounts a
                        WHERE a.status = 'active'
                        AND a.task_type = %s
                    )
                    SELECT a.uid, a.password, a.email, a.cookie, a.token, a.two_fa,
                        p.ip as proxy_ip, p.port as proxy_port, 
                        p.username as proxy_username, p.password as proxy_password
                    FROM available_account a
                    LEFT JOIN account_proxy_mapping apm ON a.uid = apm.account_uid
                    LEFT JOIN proxies p ON apm.proxy_id = p.id
                    WHERE a.can_use = true
                    ORDER BY a.usage_count ASC, a.last_used_at ASC
                    LIMIT %s;
                """
                cursor.execute(query, (task_type, limit))
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results
        except Exception as e:
            print(f"Lỗi khi lấy account: {e}")
            return []
        
    def update_default_account(self, account_uid: int) -> bool:
        """
        Cập nhật trạng thái inactive và số lần sử dụng +1
        
        Args:
            account_uid (int): UID của account
                
        Returns:
            bool: True nếu cập nhật thành công
        """
        if not self.connection:
            if not self.connect():
                return False
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE accounts SET status = 'inactive', usage_count = usage_count + 1 WHERE uid = %s",
                    (account_uid,)
                )
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Lỗi khi cập nhật trạng thái: {e}")
            return False

    def update_account_status(self, account_uid: int, status: str) -> bool:
        """
        Cập nhật trạng thái của account
        
        Args:
            account_uid (int): UID của account
            status (str): Trạng thái mới ('active', 'inactive', 'blocked')
            
        Returns:
            bool: True nếu cập nhật thành công
        """
        if not self.connection:
            if not self.connect():
                return False

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE accounts SET status = %s WHERE uid = %s",
                    (status, account_uid)
                )
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Lỗi khi cập nhật trạng thái: {e}")
            return False

    def increment_account_usage(self, account_uid: int) -> bool:
        """
        Tăng số lần sử dụng của account
        
        Args:
            account_uid (int): UID của account
            
        Returns:
            bool: True nếu cập nhật thành công
        """
        if not self.connection:
            if not self.connect():
                return False

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT increment_account_usage(%s)", (account_uid,))
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Lỗi khi tăng số lần sử dụng: {e}")
            return False

    def assign_new_proxy(self, account_uid: int) -> bool:
        """
        Gán proxy mới cho account
        
        Args:
            account_uid (int): UID của account
            
        Returns:
            bool: True nếu gán thành công
        """
        if not self.connection:
            if not self.connect():
                return False

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT assign_new_proxy(%s)", (account_uid,))
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Lỗi khi gán proxy mới: {e}")
            return False

    def reset_account_usage(self) -> bool:
        """
        Reset số lần sử dụng cho các account đã quá 24h
        
        Returns:
            bool: True nếu reset thành công
        """
        if not self.connection:
            if not self.connect():
                return False

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT reset_account_usage()")
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Lỗi khi reset số lần sử dụng: {e}")
            return False

    def update_account_cookie(self, account_uid: int, new_cookie: str) -> bool:
        """
        Cập nhật cookie mới cho account
        
        Args:
            account_uid (int): UID của account
            new_cookie (str): Cookie mới
            
        Returns:
            bool: True nếu cập nhật thành công
        """
        if not self.connection:
            if not self.connect():
                return False

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE accounts SET cookie = %s, last_used_at = CURRENT_TIMESTAMP WHERE uid = %s",
                    (new_cookie, account_uid)
                )
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Lỗi khi cập nhật cookie: {e}")
            return False