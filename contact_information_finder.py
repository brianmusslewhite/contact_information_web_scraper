import logging
import os
import random
import re
import time
import threading
import multiprocessing
import urllib.robotparser
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from datetime import datetime
from functools import lru_cache

import pandas as pd
import phonenumbers
import validators
from bs4 import BeautifulSoup
from email_validator import validate_email, EmailNotValidError
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def get_gigablast_search_results(query, clicks=0, timeout=30):
    url = f"https://gigablast.org/search/?q={query.replace(' ', '%20')}"
    if not is_allowed(url):
        logging.debug(f"Access denied by robots.txt for URL: {url}")
        return []
    
    driver = set_up_driver()
    time.sleep(random.uniform(1, 3))

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
        for _ in range(clicks):
            try:
                more_results_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, 'result--more__btn'))
                )
                current_results = driver.find_elements(By.CLASS_NAME, 'searpList')
                num_results_before = len(current_results)
                
                more_results_button.click()
                
                WebDriverWait(driver, 10).until(
                    lambda d: len(d.find_elements(By.CLASS_NAME, 'searpList')) > num_results_before
                )
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                logging.error(f"No more results button found or failed to click: {e}")
                break

    search_results = driver.page_source
    driver.quit()

    soup = BeautifulSoup(search_results, 'html.parser')
    links = soup.find_all('a', attrs={'data-target': True})
    urls = [link['data-target'] for link in links if link['data-target'].startswith('http') and "anon.toorgle.com" not in link['data-target']]

    return list(set(urls))


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
        driver.set_page_load_timeout(80)
        driver.set_script_timeout(80)
        driver.implicitly_wait(80)
        return driver
    except Exception as e:
        logging.error(f"Error setting up Chrome driver: {e}")
        raise


@lru_cache()
def is_allowed(url, user_agent='Mozilla/5.0'):
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(urllib.parse.urljoin(url, 'robots.txt'))
    parser.read()
    return parser.can_fetch(user_agent, url)


def fetch_html(url, timeout=60):
    logging.debug(f"Fetching url: {url}")
    if not validators.url(url):
        logging.warning(f"Invalid url: {url}")
        return ""

    if not is_allowed(url):
        logging.debug(f"Access denied by robots.txt for URL: {url}")
        return ""

    driver = set_up_driver()
    html_content = ""

    def load_url(driver, url):
        logging.debug(f"Starting getting: {url}")
        driver.get(url)
        logging.debug(f"Finished getting: {url}")
    
    try:
        load_thread = threading.Thread(target=load_url, args=(driver, url))
        load_thread.start()
        load_thread.join(timeout)
        if load_thread.is_alive():
            logging.warning(f"Timeout exceeded while fetching URL: {url}")
            return ""
        
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        # Check if jQuery is available and handle AJAX with a maximum wait limit
        jquery_loaded = driver.execute_script("return typeof jQuery != 'undefined'")
        if jquery_loaded:
            logging.debug(f"jQuery detected in: {url}")
            start_time = time.time()
            while time.time() - start_time < timeout:
                ajax_active = driver.execute_script('return jQuery.active')
                if ajax_active == 0:
                    logging.debug(f"AJAX finished loading in: {url}")
                    break
                time.sleep(0.5)
            else:
                logging.warning(f"Timed out waiting for AJAX calls to complete at {url}")
        
        html_content = driver.page_source
        logging.debug(f"Successfully fetched HTML for {url}")
    except TimeoutException:
        logging.warning(f"Timeout while waiting for page elements on {url}")
    except NoSuchElementException:
        logging.warning(f"Required element not found on {url}")
    except WebDriverException as e:
        logging.warning(f"WebDriver error encountered on {url}: {e}")
    except Exception as e:
        logging.warning(f"An unexpected error occurred on {url}: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logging.error(f"Error quitting driver: {e}")
    return html_content


def proximity_based_extraction(soup, url):
    logging.debug(f"Starting proximety based extraction for: {url}")
    contacts = []
    seen_data = set()

    phone_regex = r'\(?\b[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b'
    email_regex = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    # address_regex = r'\b\d{1,5}\s(?:\b\w+\b\s?){0,4}(Street|St|Drive|Dr|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Court|Ct|Way|Plaza|Plz|Terrace|Terr|Circle|Cir|Trail|Trl|Parkway|Pkwy|Commons|Cmns|Square|Sq)\b'
    website_regex = r'https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,}'
    name_regex = r"(Mr\.|Mrs\.|Ms\.|Dr\.|Capt\.|Captain)\s+([A-Z][\w'-]+)\s+([A-Z][\w'-]+)?"

    potential_blocks = soup.find_all(['div', 'p', 'footer', 'section', 'td', 'span', 'article', 'header', 'aside', 'li'])
    try:
        for block in potential_blocks:
            text = ' '.join(block.stripped_strings)

            phones = tuple(re.findall(phone_regex, text, re.IGNORECASE))
            emails = tuple(re.findall(email_regex, text, re.IGNORECASE))
            # addresses = tuple(re.findall(address_regex, text, re.IGNORECASE))
            websites = tuple(re.findall(website_regex, text, re.IGNORECASE))
            names = tuple(re.findall(name_regex, text))

            if phones or emails:
                contact_details = {
                    'phone1': phones[0] if len(phones) > 0 else '',
                    'phone2': phones[1] if len(phones) > 1 else '',
                    'email1': emails[0] if len(emails) > 0 else '',
                    'email2': emails[1] if len(emails) > 1 else '',
                    # 'address1': addresses[0] if len(addresses) > 0 else '',
                    # 'address2': addresses[1] if len(addresses) > 1 else '',
                    'website1': websites[0] if len(websites) > 0 else '',
                    'website2': websites[1] if len(websites) > 1 else '',
                }

                for i, name in enumerate(names[:2]):
                    contact_details[f'salutation{i+1}'] = name[0]
                    contact_details[f'first_name{i+1}'] = name[1]
                    contact_details[f'last_name{i+1}'] = name[2] if len(name) > 2 else ''
                
                contact_details['source'] = url

                # Remove duplicates
                contact_id = frozenset(contact_details.items())
                if contact_id not in seen_data:
                    seen_data.add(contact_id)
                    contacts.append(contact_details)
        logging.debug(f"Contacts found in {url}, {contacts}")
        return contacts
    except Exception as e:
        logging.warning(f"Error: {e}, processing a blocks in url: {url}")
    return[]


def process_url(url):
    logging.debug(f"Processing URL: {url}")
    try:
        time.sleep(0.0001)
        html_content = fetch_html(url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            contacts = proximity_based_extraction(soup, url)
            return contacts
        else:
            logging.debug(f"No html content for: {url}")
        return []
    except Exception as e:
        logging.warning(f"Error processing URL {url}: {e}")
    return []


def save_to_csv(contacts, filename):
    if contacts.empty:
        logging.critical("No contacts to save in csv.")
        return
    try:
        contacts.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}. Total unique contacts: {len(contacts)}")
    except Exception as e:
        logging.critical(f"Saving to CSV failed with error: {e}")


def clean_contact_information(all_contacts):
    def standardize_phone(phone, region='US'):
        if pd.isna(phone) or phone == '':
            return ""
        try:
            phone_number = phonenumbers.parse(phone, region)
            if phonenumbers.is_valid_number(phone_number):
                return phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.NATIONAL)
            else:
                return ""
        except phonenumbers.NumberParseException:
            logging.warning(f"Failed to standardize: {phone}")
            return ""

    def standardize_email(email):
        if pd.isna(email) or email == '':
            return ""
        try:
            v = validate_email(email)
            return v.email
        except EmailNotValidError as e:
            logging.warning(f"Invalid email: {email}, error: {e}")
            return ""
    
    if not all_contacts:
        logging.warning("No contact information provided for cleaning.")
        return pd.DataFrame()

    contact_info = pd.DataFrame(all_contacts)
    logging.info(f"Cleaning contact information. Length before cleaning: {len(contact_info)}")
    
    # clean phone
    contact_info['phone1'] = contact_info['phone1'].apply(standardize_phone)
    contact_info['phone2'] = contact_info['phone2'].apply(standardize_phone)
    
    # clean email
    contact_info['email1'] = contact_info['email1'].apply(standardize_email)
    contact_info['email2'] = contact_info['email2'].apply(standardize_email)
    
    # clean address
    # to-do

    # remove exact duplicates
    contact_info.drop_duplicates(inplace=True)

    # remove partial duplicates
    # to-do

    logging.info(f"Cleaning complete. Length after cleaning: {len(contact_info)}")
    return contact_info


def setup_paths_and_logging(search_queries):
    current_datetime = datetime.now()
    current_formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    current_date = current_datetime.strftime("%Y_%m_%d")
    current_time = current_datetime.strftime("%H-%M-%S")

    results_path = os.path.join("Results", current_date)
    os.makedirs(results_path, exist_ok=True)
    
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    log_filepath = os.path.join(results_path, f"{search_queries[0].replace(' ', '_')}_{current_date}_{current_time}.log")
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(console_handler)
    
    logging.info("Logging started")

    for logger_name in ['selenium', 'urllib3', 'http.client']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.ERROR)

    csv_filename = f"{current_formatted_datetime}_{search_queries[0].replace(' ', '-')}.csv"
    csv_filepath = os.path.join(results_path, csv_filename)
    logging.info(f"CSV Filepath: {csv_filepath}")
    
    return csv_filepath


if __name__ == "__main__":
    search_queries = [
        "Texas saltwater fishing guides",
        # "Best Texas saltwater fishing",
        # "Texas saltwater fishing guides contact information",
        # "Saltwater fishing guides in Texas",
        # "Texas saltwater fishing charters contact details",
        # "Fishing guide services Texas Gulf Coast",
        # "Texas coast fishing guides contact info",
        "Galveston saltwater fishing guides contact",
        "Corpus Christi saltwater fishing charters contact",
        "Port Aransas fishing guides contact information",
        "South Padre Island fishing guides contact details",
        "Rockport Texas saltwater fishing guides contact info",
        "Texas saltwater fishing guides Yelp",
        # "Texas fishing charters TripAdvisor",
        # "Saltwater fishing guides Texas Google Maps",
        # "Texas fishing guides directory",
        # "Best saltwater fishing guides in Texas",
        # "Texas Professional Fishing Guides Association",
        # "Texas fishing guides association members contact",
        # "Texas Parks and Wildlife fishing guides list",
        # "Texas fishing guides yellow pages",
        # "Texas saltwater fishing guides Facebook",
        # "Texas fishing guides Instagram",
        # "Fishing forums Texas saltwater guides",
        # "Texas fishing groups contact information",
    ]

    csv_filepath = setup_paths_and_logging(search_queries)
    all_urls = []
    all_contacts = []
    
    logging.info(f"Starting with queries: {search_queries}")
    with ThreadPoolExecutor(max_workers=len(search_queries)) as executor:
        future_to_query = {executor.submit(get_gigablast_search_results, query, clicks=0): query for query in search_queries}
        for future in as_completed(future_to_query):
            urls = future.result()
            if urls:
                all_urls.extend(urls)
    
    logging.info(f"Collected {len(all_urls)} urls from all queries. Starting to process.")
    start_time = time.time()
    all_contacts = []

    try:
        logging.debug("Starting URL processing with ThreadPoolExecutor.")
        workers = 1 # int(os.cpu_count() * 2.5)
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            logging.debug(f"Executor created with {workers} workers.")
            future_to_url = {executor.submit(process_url, url): url for url in all_urls}
            logging.debug("Submitted all URLs to the executor.")

            processed_count = 0
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                logging.debug(f"Processing result for URL: {url}")
                try:
                    contacts = future.result(timeout=80)
                    if contacts:
                        all_contacts.extend(contacts)
                        logging.debug(f"Added contacts from: {url}")
                    else:
                        logging.debug(f"No contacts found for: {url}")
                except concurrent.futures.TimeoutError:
                    logging.warning(f"Processing {url} timed out.")
                except Exception as e:
                    logging.error(f"An error occurred while processing {url}: {e}")
                finally:
                    processed_count += 1
                    logging.info(f"Processed {processed_count}/{len(all_urls)} URLs")

        logging.info("Finished processing all URLs")
    except Exception as e:
        logging.critical("Failed processing URLs!", exc_info=True)


    cleaned_contacts = clean_contact_information(all_contacts)
    save_to_csv(cleaned_contacts, csv_filepath)
