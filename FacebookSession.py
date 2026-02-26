import os
import time
import random
import asyncio
from dotenv import load_dotenv
import dataclasses
from playwright.async_api import async_playwright, TimeoutError
from typing import Any
import logging
from stealth import stealth_async
from FacebookSearch import FacebookSearch
from FacebookScraperTemp import FacebookScraper
from utils_postgres import PostgresFbAccounts
# from .helpers import random_choice

load_dotenv()

@dataclasses.dataclass
class FacebookPlaywrightSession:
    context: Any = None
    page: Any = None
    proxy: str = None
    account: dict = None
    base_url: str = "https://www.facebook.com"

class FacebookAccountManager:
    def __init__(self):
        self.postgres = PostgresFbAccounts()
        self.postgres.connect()
        self.banned_accounts = set()

    def get_next_available_account(self, task_type="search"):
        """Get next available account that isn't banned and hasn't exceeded usage limits"""
        accounts = self.postgres.get_random_accounts(task_type=task_type, limit=1)
        if not accounts:
            raise Exception("No available accounts left - all accounts are banned or exceeded limits")
        return accounts[0]

    def update_account_usage(self, account_id):
        """Update the usage count for an account"""
        self.postgres.increment_account_usage(account_id)

    def handle_banned_account(self, account_id):
        """Handle banned account by updating its status in database"""
        try:
            self.postgres.update_account_status(account_id, "blocked")
            return True
        except Exception as e:
            return False

    def handle_account_ban(self, account_id, task_type="search"):
        """Handle banned account and get a new one"""
        self.banned_accounts.add(account_id)
        self.handle_banned_account(account_id)
        return self.get_next_available_account(task_type)

class FacebookSession:
    search = FacebookSearch
    scraper = FacebookScraper
    account_manager = FacebookAccountManager
    
    def __init__(self, logging_level: int = logging.WARN, logger_name: str = None):
        self.session = None

        if logger_name is None:
            logger_name = __name__

        self.__create_logger(logger_name, logging_level)

        FacebookSearch.parent = self
        FacebookScraper.parent = self
        self.account_manager = FacebookAccountManager()

    def __create_logger(self, logger_name: str, logging_level: int):
        """Create a logger for the class."""
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging_level)
        
        # Clear existing handlers to prevent duplicate logging
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Only add handler if logger doesn't have any handlers
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
            # Prevent log messages from being passed to the root logger
            self.logger.propagate = False

    async def create_session(
        self,
        url: str = "https://www.facebook.com",
        proxy: str = None,
        browser: str = "chromium",
        sleep_after: int = 1,
        timeout: int = 60000,  # Increase default timeout to 60 seconds
        headless: bool = False,  # Default to show browser window
        override_browser_args: list = None,
        executable_path: str = None,
        suppress_resource_load_types: list = None,
        show_browser: bool = True,  # New parameter to control browser display
        account: dict = None,
    ):
        """Create a Facebook session using Playwright."""
        self.playwright = await async_playwright().start()
        self.browser = None  # Initialize browser attribute

        # Convert proxy string to object format if provided
        proxy_config = None
        if proxy:
            # Parse proxy string format: host:port:username:password
            proxy_parts = proxy.split(':')

            if len(proxy_parts) == 4:
                # Format: host:port:username:password
                host, port, username, password = proxy_parts
                proxy_config = {
                    "server": f"http://{host}:{port}",
                    "username": username,
                    "password": password
                }
            elif len(proxy_parts) == 2:
                # Format: host:port
                host, port = proxy_parts
                proxy_config = {
                    "server": f"http://{host}:{port}"
                }
            else:
                # Assume it's already a full URL
                proxy_config = {"server": proxy}

        if browser == "chromium":
            # Set up browser arguments for better display
            if override_browser_args is None:
                override_browser_args = []
            
            stealth_args = [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
                "--disable-blink-features=AutomationControlled",  # Hide automation indicators
                "--exclude-switches=enable-automation",  # Remove automation flags
                "--disable-extensions-except=",
                "--disable-plugins-discovery",
                "--disable-default-apps",
                "--no-first-run",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                "--disable-features=TranslateUI",
                "--disable-ipc-flooding-protection",
                "--disable-background-networking",
                "--disable-sync",
                "--metrics-recording-only",
                "--no-report-upload",
                "--disable-breakpad",
                "--disable-crash-reporter",
                "--disable-gpu-sandbox",
                "--use-gl=swiftshader",  # Software rendering
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"  # Real user agent
            ]
            
            # Add stealth arguments to override args
            override_browser_args.extend(stealth_args)
            
            # Use show_browser parameter to control headless mode
            browser_headless = not show_browser
            self.browser = await self.playwright.chromium.launch(
                headless=browser_headless, 
                args=override_browser_args, 
                executable_path=executable_path, 
                proxy=proxy_config
            )
        elif browser == "firefox":
            browser_headless = not show_browser
            self.browser = await self.playwright.firefox.launch(
                headless=browser_headless,
                args=override_browser_args, 
                executable_path=executable_path
            )
        elif browser == "webkit":
            browser_headless = not show_browser
            self.browser = await self.playwright.webkit.launch(
                headless=browser_headless,
                args=override_browser_args, 
                executable_path=executable_path
            )
        else:
            raise ValueError("Invalid browser argument passed")

        # Create context with additional stealth properties
        context_options = {
            "proxy": proxy_config,
            "ignore_https_errors": True,
            "viewport": {"width": 1366, "height": 768},  # Common resolution
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "locale": "en-US",
            "timezone_id": "America/New_York",
            "permissions": ["geolocation"],
            "extra_http_headers": {
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        }
        
        context = await self.browser.new_context(**context_options)

        page = await context.new_page()

        # Inject JavaScript to hide automation indicators
        await page.add_init_script("""
            // Hide webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Mock chrome object
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
            
            // Override permission query
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // Hide automation indicators
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        """)

        await stealth_async(page)

        if suppress_resource_load_types is not None:
            await page.route(
                "**/*",
                lambda route, request: route.abort()
                if request.resource_type in suppress_resource_load_types
                else route.continue_(),
            )

        # Set the navigation timeout
        page.set_default_navigation_timeout(timeout)

        try:
            self.logger.info(f"Navigating to {url}...")
            await page.goto(url, wait_until="domcontentloaded")
            self.logger.info("Page loaded successfully")
        except TimeoutError as e:
            self.logger.error(f"Timeout error navigating to {url}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error navigating to {url}: {e}")
            raise

        x, y = random.randint(0, 50), random.randint(0, 50)
        a, b = random.randint(1, 50), random.randint(100, 200)

        await page.mouse.move(x, y)
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except TimeoutError:
            self.logger.warning("Network idle timeout, continuing anyway...")
        await page.mouse.move(a, b)

        try:
            if account is None:
                account = self.account_manager.get_next_available_account(task_type="search")
        except Exception as e:
            self.logger.error(f"Error getting next available account: {e}")
            return None
        
        if account is not None:
            try:
                cookies_list = account['cookie'].split(";")
                for cookie in cookies_list:
                    cookie = cookie.strip()
                    if '=' in cookie:
                        name, value = cookie.split("=", 1)
                        name = name.strip()
                        value = value.strip()
                        if name and value:  # Only add non-empty cookies
                            await page.context.add_cookies([{
                                "name": name, 
                                "value": value, 
                                "url": url, 
                                'secure': True if url.startswith('https') else False
                            }])
            except Exception as e:
                self.logger.error(f"Error adding cookies: {e}")
        else:
            self.logger.warning("No cookies provided, session may not be authenticated.")

        await page.reload()
        
        # Check if cookie is still valid - if not, try to login with credentials
        if not await self.is_cookie_valid(page):
            self.logger.warning("Cookie appears to be invalid, attempting fallback login...")
            
            if account and 'uid' in account and 'password' in account:
                login_success, new_cookie = await self.login_with_credentials(page, account['uid'], account['password'])
                
                if login_success and new_cookie:
                    self.logger.info("Fallback login successful, updating cookie in database")
                    # Update cookie in database
                    if self.account_manager.postgres.update_account_cookie(account['uid'], new_cookie):
                        # Update local account object
                        account['cookie'] = new_cookie
                        self.logger.info("Cookie updated successfully in database")
                    else:
                        self.logger.error("Failed to update cookie in database")
                else:
                    self.logger.error("Fallback login failed")
                    if account and 'uid' in account:
                        account = self.account_manager.handle_account_ban(account['uid'], task_type="search")
                    return None
            else:
                self.logger.error("No uid/password available for fallback login")
                return None
        
        # Check for banned account AFTER all login attempts are completed
        current_url = page.url
        if any(x in current_url for x in ["facebook.com/checkpoint", "facebook.com/confirmemail"]):
            if account and 'uid' in account:
                self.logger.error(f"Account {account['uid']} is banned - redirected to checkpoint page")
            else:
                self.logger.error("Account is banned - redirected to checkpoint page")
            if account and 'uid' in account:
                account = self.account_manager.handle_account_ban(account['uid'], task_type="search")
            return None
        
        # Check for ban indicators in page content
        try:
            await page.wait_for_load_state("domcontentloaded")
            # Use locator instead of query_selector_all to handle navigation better
            locator = page.locator('xpath=//div[@role="main"]//span[@dir="auto"]')
            if await self.is_account_banned_with_locator(locator, current_url):
                if account and 'uid' in account:
                    self.logger.error(f"Account {account['uid']} is banned")
                else:
                    self.logger.error("Account is banned")
                if account and 'uid' in account:
                    account = self.account_manager.handle_account_ban(account['uid'], task_type="search")
                return None
        except Exception as e:
            self.logger.warning(f"Error checking for banned content (likely due to navigation): {e}")
            # If we can't check elements due to context destruction, just check URL
            if await self.is_account_banned([], current_url):
                if account and 'uid' in account:
                    self.logger.error(f"Account {account['uid']} is banned (URL check)")
                else:
                    self.logger.error("Account is banned (URL check)")
                if account and 'uid' in account:
                    account = self.account_manager.handle_account_ban(account['uid'], task_type="search")
                return None
        
        # Final check after potential login
        if account and 'uid' in account:
            self.account_manager.update_account_usage(account['uid'])
        
        session = FacebookPlaywrightSession(
            context=context,
            page=page,
            account=account,
            proxy=proxy,
            base_url=url,
        )
        self.session = session
        self.logger.info(f"Session created successfully for {url}")
        if sleep_after:
            time.sleep(sleep_after)
        return session
    
    async def is_account_banned(self, elements: list, page_url: str = None) -> bool:
        """Check if account is banned based on response content or checkpoint URL"""
        # Check if URL contains checkpoint (primary ban indicator)
        if page_url and "facebook.com/checkpoint" in page_url:
            return True
            
        ban_indicators = [
            "Your account has been disabled",
            "You can't use Facebook at the moment",
            "Your account has been locked",
            "We've detected suspicious activity",
            "You're Temporarily Blocked",
            "Chúng tôi đã đình chỉ tài khoản của bạn",
            "hãy xác nhận bạn là người thật để sử dụng tài khoản của mình",
            "tài khoản của bạn đã bị khóa",
            "confirm you're human to use your account"
        ]
        
        # Check elements for ban indicators (if elements are provided and valid)
        for element in elements[:3]:
            try:
                text = await element.text_content()
                if text:
                    for indicator in ban_indicators:
                        if indicator.lower() in text.lower():
                            return True
            except Exception as e:
                # Element may be stale due to navigation - skip this element
                continue
        return False

    async def is_account_banned_with_locator(self, locator, page_url: str = None) -> bool:
        """Check if account is banned using locator instead of elements to handle navigation better"""
        # Check if URL contains checkpoint (primary ban indicator)
        if page_url and "facebook.com/checkpoint" in page_url:
            return True
            
        ban_indicators = [
            "Your account has been disabled",
            "You can't use Facebook at the moment",
            "Your account has been locked",
            "We've detected suspicious activity",
            "You're Temporarily Blocked",
            "Chúng tôi đã đình chỉ tài khoản của bạn",
            "hãy xác nhận bạn là người thật để sử dụng tài khoản của mình",
            "tài khoản của bạn đã bị khóa",
            "confirm you're human to use your account"
        ]
        
        try:
            # Wait for locator to be available with timeout
            await locator.first.wait_for(state="attached", timeout=5000)
            
            # Get count of matching elements
            count = await locator.count()
            
            # Check first 3 elements for ban indicators
            for i in range(min(count, 3)):
                try:
                    element = locator.nth(i)
                    text = await element.text_content(timeout=3000)
                    if text:
                        for indicator in ban_indicators:
                            if indicator.lower() in text.lower():
                                return True
                except Exception as e:
                    # Element may not be available - continue to next
                    continue
                    
        except Exception as e:
            # If locator fails completely, fall back to URL check only
            self.logger.warning(f"Could not check elements with locator: {e}")
            
        return False

    async def is_cookie_valid(self, page) -> bool:
        """
        Kiểm tra xem cookie có còn hoạt động không bằng cách kiểm tra các dấu hiệu đăng nhập
        
        Args:
            page: Playwright page object
            
        Returns:
            bool: True nếu cookie còn hoạt động, False nếu hết hạn
        """
        try:
            current_url = page.url
            
            # Kiểm tra URL có chứa login hay không
            if 'login' in current_url or 'checkpoint' in current_url:
                return False
            
            # Kiểm tra sự tồn tại của các element đăng nhập
            login_selectors = [
                '//input[@name="email"]',
                '//input[@type="email"]',
                '//input[contains(@placeholder, "Email")]',
                '//input[contains(@placeholder, "email")]',
                '//div[@data-testid="royal_email"]'
            ]
            
            for selector in login_selectors:
                try:
                    element = await page.query_selector(f'xpath={selector}')
                    if element and await element.is_visible():
                        self.logger.info("Login form detected - cookie invalid")
                        return False
                except:
                    continue
            
            # Kiểm tra sự tồn tại của navigation menu (dấu hiệu đã đăng nhập)
            logged_in_selectors = [
                '//div[@role="navigation"]',
                '//div[@data-testid="blue_bar"]',
                '//a[@href="/me"]',
                '//div[@aria-label="Your profile"]'
            ]
            
            for selector in logged_in_selectors:
                try:
                    element = await page.query_selector(f'xpath={selector}')
                    if element and await element.is_visible():
                        self.logger.info("Navigation detected - cookie valid")
                        return True
                except:
                    continue
            
            # Kiểm tra text content để xác định trạng thái đăng nhập
            page_content = await page.content()
            if any(text in page_content.lower() for text in ['log in', 'sign up', 'create account']):
                return False
            
            self.logger.info("Cookie appears to be valid")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking cookie validity: {e}")
            return False

    async def clear_all_cookies(self, page):
        """
        Xóa tất cả cookies để tránh xung đột khi đăng nhập bằng credentials
        """
        try:
            # Clear all cookies from the context
            await page.context.clear_cookies()
            self.logger.info("All cookies cleared successfully")
        except Exception as e:
            self.logger.error(f"Error clearing cookies: {e}")

    async def login_with_credentials(self, page, uid: str, password: str) -> tuple[bool, str]:
        """
        Login with credentials using human-like behavior to avoid detection
        """
        await self.clear_all_cookies(page)

        try:
            # Convert uid and password to strings to avoid type errors
            uid_str = str(uid)
            password_str = str(password)
            
            self.logger.info(f"Attempting human-like login with uid: {uid_str}")
            
            # Navigate to login page with random delay
            await page.goto("https://www.facebook.com/", wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))  # Random wait
            
            # Add some random mouse movements
            await self._simulate_human_mouse_movement(page)
            
            await page.wait_for_load_state("networkidle", timeout=10000)
            await asyncio.sleep(random.uniform(1, 2))

            # Wait for and fill email field with human-like typing
            email_selector = '//input[@name="email"]'
            email_filled = False
            try:
                email_element = await page.wait_for_selector(f'xpath={email_selector}', timeout=10000)
                if email_element:
                    # Click on the field first (human behavior)
                    await email_element.click()
                    await asyncio.sleep(random.uniform(0.5, 1))
                    
                    # Clear field slowly
                    await email_element.fill("")
                    await asyncio.sleep(random.uniform(0.3, 0.6))
                    
                    # Type email character by character with random delays
                    await self._type_like_human(email_element, uid_str)
                    email_filled = True
                    
                    # Random mouse movement after typing
                    await self._simulate_human_mouse_movement(page)
                    
            except Exception as e:
                self.logger.error(f"Error filling email field: {e}")
                pass
            
            if not email_filled:
                self.logger.error("Could not find uid input field")
                return False, ""
            
            # Wait before moving to password
            await asyncio.sleep(random.uniform(1, 2))
            
            # Fill password field
            password_selector = '//input[@name="pass"]'
            password_filled = False
            try:
                password_element = await page.wait_for_selector(f'xpath={password_selector}', timeout=5000)
                if password_element:
                    # Click on password field
                    await password_element.click()
                    await asyncio.sleep(random.uniform(0.5, 1))
                    
                    # Clear and type password
                    await password_element.fill("")
                    await asyncio.sleep(random.uniform(0.2, 0.4))
                    
                    # Type password with human-like delays
                    await self._type_like_human(password_element, password_str)
                    password_filled = True
                    
                    # Random mouse movement
                    await self._simulate_human_mouse_movement(page)
                    
            except Exception as e:
                self.logger.error(f"Error filling password field: {e}")
                pass            
            
            if not password_filled:
                self.logger.error("Could not find password input field")
                return False, ""
            
            # Wait before clicking login (human behavior)
            await asyncio.sleep(random.uniform(1, 3))
            
            # Click login button
            login_selector = '//button[@name="login"]'
            login_clicked = False
            try:
                login_button = await page.wait_for_selector(f'xpath={login_selector}', timeout=5000)
                if login_button:
                    # Move mouse to button area first
                    bbox = await login_button.bounding_box()
                    if bbox:
                        await page.mouse.move(
                            bbox['x'] + random.uniform(10, bbox['width'] - 10),
                            bbox['y'] + random.uniform(5, bbox['height'] - 5)
                        )
                        await asyncio.sleep(random.uniform(0.2, 0.5))
                    
                    self.logger.info("Clicking login button")
                    await login_button.click()
                    login_clicked = True
            except Exception as e:
                self.logger.error(f"Error clicking login button: {e}")
                pass            
            
            if not login_clicked:
                self.logger.error("Could not find login button")
                return False, ""
            
            # Wait for response
            await asyncio.sleep(random.uniform(3, 5))
            
            # Get current URL after login attempt
            current_url = page.url
            
            # Check if login was successful
            if any(indicator in current_url for indicator in ['checkpoint', 'login', 'recover']):
                self.logger.error(f"Login failed - redirected to: {current_url}")
                return False, ""
            
            # Check if we're on main Facebook page or any authenticated page
            if 'facebook.com' in current_url and not any(indicator in current_url for indicator in ['login', 'checkpoint']):
                # Get new cookies
                cookies = await page.context.cookies()
                cookie_string = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
                
                self.logger.info("Login successful - extracted new cookies")
                return True, cookie_string
            else:
                self.logger.error(f"Login failed - unexpected URL: {current_url} for uid: {uid_str}")
                return False, ""
                
        except Exception as e:
            self.logger.error(f"Error during login: {e}")
            return False, ""

    async def _simulate_human_mouse_movement(self, page):
        """Simulate human-like mouse movements"""
        try:
            # Get viewport size
            viewport = page.viewport_size
            if not viewport:
                viewport = {'width': 1366, 'height': 768}
            
            # Random mouse movements
            for _ in range(random.randint(1, 3)):
                x = random.randint(50, viewport['width'] - 50)
                y = random.randint(50, viewport['height'] - 50)
                
                # Move with some curve (not straight line)
                current_pos = await page.evaluate("() => ({ x: window.mouseX || 0, y: window.mouseY || 0 })")
                
                # Move in steps to simulate human movement
                steps = random.randint(5, 10)
                for step in range(steps):
                    progress = step / steps
                    intermediate_x = current_pos.get('x', 0) + (x - current_pos.get('x', 0)) * progress
                    intermediate_y = current_pos.get('y', 0) + (y - current_pos.get('y', 0)) * progress
                    
                    # Add some randomness to the path
                    intermediate_x += random.uniform(-5, 5)
                    intermediate_y += random.uniform(-5, 5)
                    
                    await page.mouse.move(intermediate_x, intermediate_y)
                    await asyncio.sleep(random.uniform(0.01, 0.03))
                
                await asyncio.sleep(random.uniform(0.1, 0.3))
                
        except Exception as e:
            self.logger.warning(f"Error in mouse simulation: {e}")

    async def _type_like_human(self, element, text: str):
        """Type text with human-like delays and patterns"""
        try:
            for i, char in enumerate(text):
                await element.type(char)
                
                # Variable typing speed - slower for first few characters
                if i < 3:
                    delay = random.uniform(0.1, 0.25)
                else:
                    delay = random.uniform(0.05, 0.15)
                
                # Occasional longer pauses (like thinking)
                if random.random() < 0.1:  # 10% chance
                    delay += random.uniform(0.2, 0.5)
                
                await asyncio.sleep(delay)
                
        except Exception as e:
            self.logger.warning(f"Error in human typing simulation: {e}")
            # Fallback to regular fill
            await element.fill(text)

    async def close_session(self):
        """Close the current session."""
        if self.session:
            try:
                await self.session.page.close()
                await self.session.context.close()
                self.logger.info("Session closed successfully.")
            except Exception as e:
                self.logger.warning(f"Error closing session: {e}")
            finally:
                self.session = None  # Clear session reference after closing
        # Remove the warning log when no session to close - it's normal behavior

    async def stop_playwright(self):
        """Stop the Playwright instance."""
        try:
            if hasattr(self, 'browser') and self.browser:
                await self.browser.close()
                self.browser = None
            if hasattr(self, 'playwright') and self.playwright:
                await self.playwright.stop()
                self.playwright = None
                self.logger.info("Playwright stopped successfully.")
        except Exception as e:
            self.logger.warning(f"Error stopping Playwright: {e}")
        # Remove the warning log when no Playwright instance - it's normal behavior

    def _get_session(self):
        """Get the current session."""
        if self.session is None:
            raise ValueError("No session created. Please create a session first.")
        return self.session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close_session()
        await self.stop_playwright()