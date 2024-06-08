import concurrent.futures
import collections
import logging
import os
import threading
from datetime import datetime

from bs4 import BeautifulSoup
from url_normalize import url_normalize
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

from data_processing import proximity_based_extraction, clean_contact_information, save_to_csv
from web_interface import get_gigablast_search_results, fetch_html, InvalidURLException, AccessDeniedException


class URLProcessingManager:
    def __init__(self, initial_urls):
        self.url_queue = collections.deque()
        self.all_urls = set()
        self.total_count = 0
        self.processed_count = 0
        self.count_lock = threading.Lock()

        for url in initial_urls: self.add_url(url)

    def clean_url(self, url):
        try:
            normalized_url = url_normalize(url)
            parsed_url = urlparse(normalized_url)
            query_params = parse_qs(parsed_url.query)
            query_params.pop('sid', None)
            new_query = urlencode(query_params, doseq=True)
            cleaned_url = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                new_query,
                parsed_url.fragment
            ))
        except Exception as e:
            raise e
        return cleaned_url

    def add_url(self, url):
        with self.count_lock:
            try:
                normal_url = self.clean_url(url)
                if normal_url not in self.all_urls:
                    self.url_queue.append(normal_url)
                    self.all_urls.add(normal_url)
                    self.total_count += 1
                    logging.debug(f"Added: {url}")
                    if url != normal_url:
                        logging.debug(f"Cleaned URL\nBefore cleaning: {url}\nAfter  cleaning: {normal_url}")
                else:
                    logging.debug(f"Did not add, already added: {normal_url}")
            except Exception as e:
                logging.warning(f"Failed to add: {url}, because of {e}")

    def get_next_url(self):
        with self.count_lock:
            if self.url_queue:
                return self.url_queue.popleft()
            return None

    def increment_processed(self):
        with self.count_lock:
            self.processed_count += 1
            self.log_progress()
            return self.processed_count

    def log_progress(self):
        if self.processed_count % 50 == 0:
            logging.info(f"Processed {self.processed_count}/{self.total_count} URLs")
        else:
            logging.debug(f"Processed {self.processed_count}/{self.total_count} URLs")


def setup_paths_and_logging(search_queries):
    current_datetime = datetime.now()
    current_formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    current_date = current_datetime.strftime("%Y_%m_%d")
    current_time = current_datetime.strftime("%H-%M-%S")

    results = "Results"
    results_path = os.path.join(results, current_date)
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
    logging.info(f"CSV filepath: {csv_filepath}")

    urls_filename = f"{search_queries[0].replace(' ', '-')}.txt"
    urls_filepath = os.path.join(results, urls_filename)
    
    return csv_filepath, urls_filepath


def get_urls(search_queries, clicks, urls_filepath, use_test_urls):
    if use_test_urls:
        logging.debug(f"use_test_urls = {use_test_urls}, URL filepath: {urls_filepath}")
        if os.path.exists(urls_filepath):
            logging.debug("Saved URL file exists, now loading")
            with open(urls_filepath, 'r') as file:
                all_urls = [line.strip() for line in file.readlines()]
        else:
            logging.debug("Saved URL file does not exists, fetching results and saving")
            all_urls = get_gigablast_search_results(search_queries, clicks=clicks)
            with open(urls_filepath, 'w') as file:
                for url in all_urls:
                    file.write(url + "\n")
                logging.debug("Wrote URLs to text file")
                print(all_urls)
    else:
        all_urls = get_gigablast_search_results(search_queries, clicks=clicks)
    logging.info(f"Collected {len(all_urls)} URLs")
    return all_urls


def process_url(url, manager):
    try:
        logging.debug(f"Starting to process: {url}")
        html_content = fetch_html(url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            contacts = proximity_based_extraction(soup, url, manager)
            return contacts
        else:
            logging.debug(f"No html content: {url}")
            return []
    except Exception as e:
        raise e


def get_contact_info_from_urls(workers, manager):
    all_contacts = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            logging.debug(f"Executor created with {workers} workers")
            futures_to_urls = {}
            while manager.url_queue or futures_to_urls:
                while manager.url_queue and len(futures_to_urls) < workers:
                    url = manager.get_next_url()
                    if url:
                        future = executor.submit(process_url, url, manager)
                        futures_to_urls[future] = url

                done_futures = [f for f in futures_to_urls if f.done()]
                for future in done_futures:
                    url = futures_to_urls.pop(future)
                    try:
                        contacts = future.result()
                        if contacts:
                            all_contacts.extend(contacts)
                            logging.debug(f"Added contacts: {url}")
                        else:
                            logging.debug(f"No contacts found: {url}")
                    except InvalidURLException as e:
                        logging.debug(e)
                    except AccessDeniedException as e:
                        logging.debug(e)
                    except concurrent.futures.TimeoutError:
                        logging.warning(f"Timeout occurred processing: {url}")
                    except Exception as e:
                        logging.warning(f"Error retrieving result: {url}: {str(e)}")
                    finally:
                        manager.increment_processed()
    except Exception as e:
        logging.critical(f"Function get_contact_info_from_urls failure! {e}")
    return all_contacts


def find_contact_info(search_queries, clicks=0, use_test_urls=False):
    csv_filepath, urls_filepath = setup_paths_and_logging(search_queries)
    all_urls = get_urls(search_queries, clicks, urls_filepath, use_test_urls)
    
    manager = URLProcessingManager(all_urls)
    all_contacts = get_contact_info_from_urls(int(os.cpu_count()*2.5), manager)

    cleaned_contacts = clean_contact_information(all_contacts)
    save_to_csv(cleaned_contacts, csv_filepath)


if __name__ == "__main__":
    search_queries = [
        "Texas saltwater fishing guides long",
        "Best Texas saltwater fishing",
        "Texas saltwater fishing guides contact information",
        "Saltwater fishing guides in Texas",
        "Texas saltwater fishing charters contact details",
        "Fishing guide services Texas Gulf Coast",
        "Texas coast fishing guides contact info",
        "Galveston saltwater fishing guides contact",
        "Corpus Christi saltwater fishing charters contact",
        "Port Aransas fishing guides contact information",
        "South Padre Island fishing guides contact details",
        "Rockport Texas saltwater fishing guides contact info",
        "Texas saltwater fishing guides Yelp",
        "Texas fishing charters TripAdvisor",
        "Saltwater fishing guides Texas Google Maps",
        "Texas fishing guides directory",
        "Best saltwater fishing guides in Texas",
        "Texas Professional Fishing Guides Association",
        "Texas fishing guides association members contact",
        "Texas Parks and Wildlife fishing guides list",
        "Texas fishing guides yellow pages",
        "Texas saltwater fishing guides Facebook",
        "Texas fishing guides Instagram",
        "Fishing forums Texas saltwater guides",
        "Texas fishing groups contact information",
    ]

find_contact_info(search_queries, clicks=5, use_test_urls=True)
