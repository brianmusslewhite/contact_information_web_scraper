import concurrent.futures
import logging
import random
import time
import os
import urllib.robotparser
from functools import lru_cache


from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import validators


def set_up_driver():
    try:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        ]
        user_agent = random.choice(user_agents)
        path_to_ublock_origin = 'chrome_extensions/ublockorigin.crx'
        path_to_https_everywhere = 'chrome_extensions/httpseverywhere.crx'
        path_to_decentraleyes = 'chrome_extensions/decentraleyes.crx'

        options = webdriver.ChromeOptions()
        options.add_extension(path_to_ublock_origin)
        options.add_extension(path_to_https_everywhere)
        options.add_extension(path_to_decentraleyes)
        options.add_argument(f'user-agent={user_agent}')
        options.add_argument('--incognito')
        options.add_argument('--disable-plugins-discovery')
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-third-party-cookies")
        options.add_argument("--disable-hyperlink-auditing")
        options.add_argument("--disable-credit-card-autofill")
        options.add_argument("--disable-file-system")
        options.add_argument("--disable-features=OutOfBlinkCors,LegacySymantecCert")
        options.add_argument("--disable-smooth-scrolling")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-accelerated-2d-canvas")
        options.add_argument("--disable-accelerated-video-decode")
        options.add_argument("--enable-features=NetworkService")
        options.add_argument("--feature-policy=geolocation 'none'")
        options.add_argument('--headless')
        options.add_argument("--site-per-process")
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument("--enable-low-res-tiling")
        options.add_argument("--disable-webgl")
        options.add_argument('--disable-webrtc')
        options.add_argument("--disable-print-preview")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument('--enable-strict-powerful-feature-restrictions')
        options.add_argument("--lang=en-US")

        prefs = {
            "safebrowsing.enabled": True,
            "safebrowsing.disable_download_protection": False,
            "download.prompt_for_download": False,
            "download.default_directory": "/dev/null",
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "profile.default_content_setting_values.automatic_downloads": 2,
            "profile.password_manager_enabled": False,
            "credentials_enable_service": False,
            "autofill.enabled": False,
            "profile.autofill.disable": True,
            "profile.block_third_party_cookies": True,
            "local_storage_enabled": False,
            "appcache_enabled": False,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_settings.stylesheets": 2,
            "profile.default_content_settings.fonts": 2,
            "profile.default_content_settings.encrypted_media": 2,
            "profile.default_content_setting_values.mixed_content": 2,
            "profile.default_content_setting_values.geolocation": 2,
            "profile.default_content_setting_values.media_stream_camera": 2,
            "profile.default_content_setting_values.media_stream_mic": 2,
            "profile.managed_default_content_settings.media_stream": 2,
            "profile.default_content_setting_values.notifications": 2,
        }
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=options)
        # driver.set_page_load_timeout(80)
        # driver.set_script_timeout(80)
        # driver.implicitly_wait(80)
        return driver
    except Exception as e:
        if driver:
            driver.quit()
        raise e


def get_gigablast_search_results(search_queries, clicks=0, timeout=30):
    all_urls = []
    workers = int(os.cpu_count())
    logging.info(f"Starting with queries: {search_queries}")
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        logging.debug(f"Executor created with {workers} workers.")
        future_to_query = {executor.submit(get_gigablast_search_results_worker, query, clicks, timeout): query for query in search_queries}
        logging.debug("Submitted all search results to the executor.")
        for future in concurrent.futures.as_completed(future_to_query):
            urls = future.result()
            if urls:
                all_urls.extend(urls)
    return all_urls


def get_gigablast_search_results_worker(query, clicks=0, timeout=30):
    url = f"https://gigablast.org/search/?q={query.replace(' ', '%20')}"
    
    if not is_allowed(url):
        logging.debug(f"Access denied by robots.txt for URL: {url}")
        return []
    
    driver = set_up_driver()
    try:
        time.sleep(random.uniform(1, 5))

        logging.debug(f"Fetching search results from: {url}")
        driver.get(url)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Check if jQuery is available on the page and wait for AJAX if so
        jquery_loaded = driver.execute_script("return typeof jQuery != 'undefined'")
        if jquery_loaded:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script('return jQuery.active == 0')
            )

        if clicks > 0:
            for click in range(clicks):
                try:
                    more_results_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CLASS_NAME, 'result--more__btn'))
                    )
                    current_results = driver.find_elements(By.CLASS_NAME, 'searpList')
                    num_results_before = len(current_results)
                    
                    logging.debug(f"Click {click+1}/{clicks} for: {url}")
                    more_results_button.click()
                    
                    WebDriverWait(driver, 10).until(
                        lambda d: len(d.find_elements(By.CLASS_NAME, 'searpList')) > num_results_before
                    )
                    time.sleep(random.uniform(1, 3))
                except Exception as e:
                    logging.error(f"No more results button found or failed to click: {e}")
                    break

        search_results = driver.page_source
    finally:
        driver.quit()

    soup = BeautifulSoup(search_results, 'html.parser')
    links = soup.find_all('a', attrs={'data-target': True})
    urls = [link['data-target'] for link in links if link['data-target'].startswith('http') and "anon.toorgle.com" not in link['data-target']]

    return list(set(urls))


@lru_cache()
def is_allowed(url, user_agent='Mozilla/5.0'):
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(urllib.parse.urljoin(url, 'robots.txt'))
    parser.read()
    return parser.can_fetch(user_agent, url)


class InvalidURLException(Exception):
    """Exception raised for invalid URLs."""
    pass

class AccessDeniedException(Exception):
    """Exception raised for access denied by robots.txt."""
    pass


def fetch_html(url, timeout=60):
    driver = set_up_driver()
    try:
        logging.debug(f"Fetching url: {url}")
        if not validators.url(url):
            raise InvalidURLException(f"Invalid url: {url}")
        if not is_allowed(url):
            raise AccessDeniedException(f"Access denied by robots.txt for URL: {url}")
        
        driver.get(url)
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")

        # Check if jQuery is available and handle AJAX with a maximum wait limit
        jquery_loaded = driver.execute_script("return typeof jQuery != 'undefined'")
        if jquery_loaded:
            start_time = time.time()
            while time.time() - start_time < timeout:
                ajax_active = driver.execute_script('return jQuery.active')
                if ajax_active == 0:
                    break
                time.sleep(0.5)
            else:
                raise ValueError(f"Timed out waiting for AJAX calls to complete at {url}")

        return driver.page_source
    except Exception as e:
        raise e
    finally:
        driver.quit()


def fetch_html_with_retries(url, timeout=60, retries=3):
    attempt = 0
    while attempt < retries:
        html_content = fetch_html(url, timeout)
        if html_content:
            return html_content
        attempt += 1
        logging.info(f"Retrying ({attempt}/{retries}) for URL: {url}")
    return ""