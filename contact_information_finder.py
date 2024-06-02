import concurrent.futures
import logging
import os
import queue
import time
from concurrent.futures import as_completed
from datetime import datetime

from bs4 import BeautifulSoup

from data_processing import proximity_based_extraction, clean_contact_information, save_to_csv
from web_interface import get_gigablast_search_results, fetch_html


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


def process_url(url):
    logging.debug(f"Processing URL: {url}")
    try:
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


def find_contact_info(search_queries):
    csv_filepath = setup_paths_and_logging(search_queries)
    all_urls = queue.Queue()
    all_contacts = []
    
    logging.info(f"Starting with queries: {search_queries}")
    with concurrent.futures.ProcessPoolExecutor(max_workers=len(search_queries)) as executor:
        future_to_query = {executor.submit(get_gigablast_search_results, query, clicks=0): query for query in search_queries}
        for future in as_completed(future_to_query):
            urls = future.result()
            if urls:
                for url in urls:
                    all_urls.put(url)
    logging.info(f"Collected {all_urls.qsize()} urls from all queries. Starting to process.")

    try:
        logging.debug("Starting URL processing with ThreadPoolExecutor.")
        workers = int(3*os.cpu_count())
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            logging.debug(f"Executor created with {workers} workers.")
            future_to_url = {executor.submit(process_url, url): url for url in list(all_urls.queue)}
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

                processed_count += 1
                if processed_count % 50 == 0:
                    logging.info(f"Processed {processed_count}/{all_urls.qsize()} URLs")
                else:
                    logging.debug(f"Processed {processed_count}/{all_urls.qsize()} URLs")

        logging.info("Finished processing all URLs")
    except Exception as e:
        logging.critical("Failed processing URLs!", exc_info=True)
    finally:
        executor.shutdown(wait=True)

    cleaned_contacts = clean_contact_information(all_contacts)
    save_to_csv(cleaned_contacts, csv_filepath)


if __name__ == "__main__":
    search_queries = [
        "Texas saltwater fishing guides",
        # "Best Texas saltwater fishing",
        # "Texas saltwater fishing guides contact information",
        # "Saltwater fishing guides in Texas",
        # "Texas saltwater fishing charters contact details",
        # "Fishing guide services Texas Gulf Coast",
        # "Texas coast fishing guides contact info",
        # "Galveston saltwater fishing guides contact",
        # "Corpus Christi saltwater fishing charters contact",
        # "Port Aransas fishing guides contact information",
        # "South Padre Island fishing guides contact details",
        # "Rockport Texas saltwater fishing guides contact info",
        # "Texas saltwater fishing guides Yelp",
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

find_contact_info(search_queries)
