import concurrent.futures
import logging
import os
from datetime import datetime

from bs4 import BeautifulSoup

from data_processing import proximity_based_extraction, clean_contact_information, save_to_csv
from web_interface import get_gigablast_search_results, fetch_html, InvalidURLException, AccessDeniedException


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
    try:
        logging.debug(f"Processing URL: {url}")
        html_content = fetch_html(url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            contacts = proximity_based_extraction(soup, url)
            return contacts
        else:
            logging.debug(f"No html content for: {url}")
            return []
    except InvalidURLException as e:
        logging.debug(e)
        return []
    except AccessDeniedException as e:
        logging.debug(e)
        return []
    except concurrent.futures.TimeoutError:
        logging.debug(f"Processing {url} timed out.")
        return []
    except Exception as e:
        logging.warning(f"Error in process_url for {url}: {e}")
        return []


def find_contact_info(search_queries):
    csv_filepath = setup_paths_and_logging(search_queries)
    workers = int(os.cpu_count())
    all_contacts = []

    all_urls = get_gigablast_search_results(search_queries)
    logging.info(f"Collected {len(all_urls)} urls from all queries. Starting to process.")

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_url = {executor.submit(process_url, url): url for url in all_urls}
        logging.debug(f"Executor created with {workers} workers and all URLs submitted")

        processed_count = 0
        for future in concurrent.futures.as_completed(future_to_url, timeout=80):
            url = future_to_url[future]
            logging.debug(f"Processing result for URL: {url}")

            contacts = future.result()
            if contacts:
                all_contacts.extend(contacts)
                logging.debug(f"Added contacts from: {url}")
            else:
                logging.debug(f"No contacts found for: {url}")

            processed_count += 1
            if processed_count % 50 == 0:
                logging.info(f"Processed {processed_count}/{len(all_urls)} URLs")
            else:
                logging.debug(f"Processed {processed_count}/{len(all_urls)} URLs")

    cleaned_contacts = clean_contact_information(all_contacts)
    save_to_csv(cleaned_contacts, csv_filepath)


if __name__ == "__main__":
    search_queries = [
        "Texas saltwater fishing guides",
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

find_contact_info(search_queries)
