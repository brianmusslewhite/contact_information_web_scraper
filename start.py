import csv
import logging
import re
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup


def get_gigablast_search_results(query):
    url = f"https://gigablast.org/search/?q={query.replace(' ', '%20')}"

    search_results = fetch_html(url)
    soup = BeautifulSoup(search_results, 'html.parser')
    links = soup.find_all('a', attrs={'data-target': True})
    urls = [link['data-target'] for link in links if link['data-target'].startswith('http') and "anon.toorgle.com" not in link['data-target']]

    return list(set(urls))


def set_up_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    driver = webdriver.Chrome(options=options)
    return driver


def is_allowed(url, user_agent='Mozilla/5.0'):
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(urllib.parse.urljoin(url, 'robots.txt'))
    parser.read()
    return parser.can_fetch(user_agent, url)


def fetch_html(url, timeout=10):
    if not is_allowed(url):
        logging.warning(f"Access denied by robots.txt for URL: {url}")
        return []
    
    driver = set_up_driver()
    try:
        driver.get(url)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Check if jQuery is available on the page and wait for AJAX if so
        jquery_loaded = driver.execute_script("return typeof jQuery != 'undefined'")
        if jquery_loaded:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script('return jQuery.active == 0')
            )
        html_content = driver.page_source
        logging.info(f"Successfully fetched HTML for {url}")
    except Exception as e:
        logging.error(f"Error fetching {url}: {str(e)}")
        html_content = ""
    finally:
        driver.quit()
    return html_content


def find_contact_details(text):
    phones = re.findall(r'\(?\b[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b', text, re.IGNORECASE)
    emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', text, re.IGNORECASE)
    return phones, emails


def proximity_based_extraction(soup, url):
    contacts = []
    seen_data = set()  # Track all seen combinations
    base_url = urlparse(url).netloc

    # Regular expressions for different types of data
    phone_regex = r'\(?\b[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b'
    email_regex = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    address_regex = r'\d{1,5} [\w\s]{1,31}(Street|St|Drive|Dr|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Court|Ct)\b'
    website_regex = r'https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,}'
    name_regex = r"(Mr\.|Mrs\.|Ms\.|Dr\.|Capt\.|Captain)\s+([A-Z][\w'-]+)\s+([A-Z][\w'-]+)?"

    potential_blocks = soup.find_all(['div', 'p', 'footer', 'section', 'td', 'span', 'article', 'header', 'aside', 'li'])

    for block in potential_blocks:
        text = ' '.join(block.stripped_strings)

        # Extract data from text using regular expressions
        phones = tuple(re.findall(phone_regex, text, re.IGNORECASE))
        emails = tuple(re.findall(email_regex, text, re.IGNORECASE))
        addresses = tuple(re.findall(address_regex, text, re.IGNORECASE))
        websites = tuple(re.findall(website_regex, text, re.IGNORECASE))
        names = tuple(re.findall(name_regex, text))

        # Collect all data found in a block if any data is present
        if phones or emails or addresses:
            contact_details = {
                'phone': phones,
                'email': emails,
                'address': addresses,
                'additional_websites': websites,
                'name': names,
                'source': url,
            }

            # Create a unique identifier for each contact block to avoid duplicates
            contact_id = frozenset(contact_details.items())
            if contact_id not in seen_data:
                seen_data.add(contact_id)
                contacts.append(contact_details)

    return contacts


def save_to_csv(contacts, filename):
    if not contacts:
        return

    with open(filename, 'w', newline='') as file:
        fieldnames = list(contacts[0].keys())
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for contact in contacts:
            writer.writerow(contact)


def remove_exact_duplicates(contacts):
    seen = set()
    unique_contacts = []

    for contact in contacts:
        contact_frozenset = frozenset(contact.items())
        if contact_frozenset not in seen:
            seen.add(contact_frozenset)
            unique_contacts.append(contact)
    return unique_contacts


def process_url(url):
    try:
        logging.info(f"Processing URL: {url}")
        html_content = fetch_html(url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            contacts = proximity_based_extraction(soup, url)
            return contacts
        return []
    except Exception as e:
        logging.error(f"Error processing URL {url}: {e}")
        return []


if __name__ == "__main__":
    current_datetime = datetime.now()
    current_formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    current_date = current_datetime.strftime("%Y_%m_%d")
    search_queries = [
        "Texas saltwater fishing guides",
        "Top saltwater fishing charters in Texas",
        "Best Texas guides for saltwater fishing",
        "Licensed saltwater fishing guides in Texas Gulf Coast",
        "Professional saltwater fishing charters Texas",
        "Texas coast fishing guide services",
        "Affordable saltwater fishing guides in Texas",
        "Texas saltwater fishing trip reviews",
        "Certified saltwater fishing guides near Corpus Christi Texas",
        "Texas saltwater fishing guide directories"
    ]
    logging.basicConfig(filename=f'{search_queries[0]}_{current_date}.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info(f"Starting with search queries: {search_queries}")
    filename = f"{current_formatted_datetime}_{search_queries[0].replace(' ', '_')}.csv"
    all_contacts = []

    for query in search_queries:
        logging.info(f"Starting web search for: {query}")
        urls = get_gigablast_search_results(query)
        if urls:
            logging.info(f"Got urls for: {query}")
        else:
            logging.info(f"URLs are empty for: {query}")
            continue

        with ThreadPoolExecutor(max_workers=min(50,len(urls))) as executor:
            future_to_url = {executor.submit(process_url, url): url for url in urls}
            for future in as_completed(future_to_url):
                contacts = future.result()
                if contacts:
                    all_contacts.extend(contacts)
                    logging.info(f"Completed processing: {future_to_url[future]}")
            
    all_contacts = remove_exact_duplicates(all_contacts)
    save_to_csv(all_contacts, filename)
    logging.info(f"Data saved to {filename}. Total unique contacts: {len(all_contacts)}")
