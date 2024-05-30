import csv
import logging
import re
import time
import random
import requests
from datetime import datetime
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup


HEADERS = [
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}
]


def get_google_search_results(query, num_pages):
    urls = []
    for page in range(num_pages):
        logging.info(f"Searching page {page+1}/{num_pages} of search {query}")
        url = f'https://www.google.com/search?q={query}&start={page*10}'
        response = requests.get(url, headers=random.choice(HEADERS))
        soup = BeautifulSoup(response.text, 'html.parser')

        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            if 'U&url=' in href:
                try:
                    actual_url = href.split('U&url=')[1].split('&')[0]
                    actual_url = requests.utils.unquote(actual_url)
                    if actual_url.startswith('http'):
                        urls.append(actual_url)
                    else:
                        logging.info(f"Skipping non-HTTP URL: {actual_url}")
                except IndexError:
                    logging.warning(f"Skipping malformed URL: {href}")
        if num_pages > 1 and page !=num_pages-1:
            sleep_time = 2
            logging.info(f"Sleeping for {sleep_time}s")
            time.sleep(sleep_time)
    return urls


def set_up_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run in headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('blink-settings=imagesEnabled=false')  # Disables images
    options.add_argument('--disable-gpu')  # Disables GPU hardware acceleration
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})  # Another way to block images
    driver = webdriver.Chrome(options=options)
    return driver


def fetch_html(driver, url):
    driver.get(url)
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        html_content = driver.page_source
        logging.info(f"Successfully fetched HTML for {url}")
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        html_content = ""
    return html_content


def find_contact_details(text):
    phones = re.findall(r'\(?\b[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b', text, re.IGNORECASE)
    emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', text, re.IGNORECASE)
    return phones, emails


def proximity_based_extraction(soup, url):
    contacts = []
    seen_data = set()  # Track all seen combinations

    base_url = urlparse(url).netloc
    potential_blocks = soup.find_all(['div', 'p', 'footer', 'section'])

    for block in potential_blocks:
        text = ' '.join(block.stripped_strings)
        # Extract phone numbers from text
        phones = set(re.findall(r'\(?\b[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b', text, re.IGNORECASE))
        
        # Extract emails from text and href attributes of <a> tags
        emails = set(re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', text, re.IGNORECASE))
        for a in block.find_all('a', href=True):
            mailto = a['href']
            if 'mailto:' in mailto:
                email = mailto.split('mailto:')[1].split('?')[0]  # Split on 'mailto:' and '?' for parameters
                emails.add(email)
        
        # Determine pairing logic
        if len(phones) == 1 and len(emails) == 1:
            phone = next(iter(phones))
            email = next(iter(emails))
            contact_id = (phone, email)
            if contact_id not in seen_data:
                seen_data.add(contact_id)
                contacts.append({'website': base_url, 'email': email, 'phone': phone})
        else:
            # If multiple or no direct pair, add them independently
            for phone in phones:
                contact_id = (phone, '')
                if contact_id not in seen_data:
                    seen_data.add(contact_id)
                    contacts.append({'website': base_url, 'email': '', 'phone': phone})
            for email in emails:
                contact_id = ('', email)
                if contact_id not in seen_data:
                    seen_data.add(contact_id)
                    contacts.append({'website': base_url, 'email': email, 'phone': ''})

    return contacts


def save_to_csv(contacts, filename):
    with open(filename, 'w', newline='') as file:
        fieldnames = ['website', 'email', 'phone']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for contact in contacts:
            writer.writerow(contact)


def remove_exact_duplicates(contacts):
    seen = set()
    unique_contacts = []

    for contact in contacts:
        # Convert dictionary to a frozenset of its items
        contact_frozenset = frozenset(contact.items())
        if contact_frozenset not in seen:
            seen.add(contact_frozenset)
            unique_contacts.append(contact)

    return unique_contacts


if __name__ == "__main__":
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=f'scraping_logs.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    search_queries = [
        "Texas saltwater fishing guides",
        "saltwater fishing charters in Texas",
        "Texas coastal fishing guides",
        "best saltwater fishing guides Texas",
        "licensed saltwater fishing guides in Texas"
    ]

    logging.info(f"Starting search for queries: {search_queries}")

    driver = set_up_driver()
    all_contacts = []

    try:
        for query in search_queries:
            logging.info(f"Starting Google search for: {query}")
            urls = get_google_search_results(query, num_pages=10)
            logging.info(f"Got urls for: {query}")
            # urls = ["https://www.texasfishingguides.org/saltwater_fishing_guides_aransas_pass.html"]

            for url in urls:
                logging.info(f"Searching: {url}")
                html_content = fetch_html(driver, url)
                soup = BeautifulSoup(html_content, 'html.parser')
                contacts = proximity_based_extraction(soup, url)
                all_contacts.extend(contacts)
    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")
            
    driver.quit()
    all_contacts = remove_exact_duplicates(all_contacts)

    filename = f"{current_time}_{query.replace(' ', '_')}.csv"
    save_to_csv(all_contacts, filename)
    logging.info(f"Data saved to {filename}. Total unique contacts: {len(all_contacts)}")
