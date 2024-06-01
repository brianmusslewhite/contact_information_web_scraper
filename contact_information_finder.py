import logging
import re
import os
import pandas as pd
import phonenumbers
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email_validator import validate_email, EmailNotValidError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup


def get_gigablast_search_results(query):
    # To-do: Modify this to retrieve more results by clicking the more button
    url = f"https://gigablast.org/search/?q={query.replace(' ', '%20')}"
    logging.debug(f"Fetching search results from: {url}")

    search_results = fetch_html(url)
    soup = BeautifulSoup(search_results, 'html.parser')
    links = soup.find_all('a', attrs={'data-target': True})
    urls = [link['data-target'] for link in links if link['data-target'].startswith('http') and "anon.toorgle.com" not in link['data-target']]

    return list(set(urls))


def set_up_driver():
    logging.debug("Setting up Selenium WebDriver")
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')

    # Disable file downloads and images
    prefs = {
        "download.prompt_for_download": False,
        "download.default_directory": "/dev/null",
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_settings.popups": 0,
        "profile.managed_default_content_settings.images": 2
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    return driver


def is_allowed(url, user_agent='Mozilla/5.0'):
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(urllib.parse.urljoin(url, 'robots.txt'))
    parser.read()
    return parser.can_fetch(user_agent, url)


def fetch_html(url, timeout=10):
    if not is_allowed(url):
        logging.debug(f"Access denied by robots.txt for URL: {url}")
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
        logging.debug(f"Successfully fetched HTML for {url}")
    except Exception as e:
        logging.error(f"Error fetching {url}: {str(e)}")
        html_content = ""
    finally:
        driver.quit()
    return html_content


def proximity_based_extraction(soup, url):
    contacts = []
    seen_data = set()

    phone_regex = r'\(?\b[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b'
    email_regex = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    address_regex = r'\b\d{1,5}\s(?:\b\w+\b\s?){0,4}(Street|St|Drive|Dr|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Court|Ct|Way|Plaza|Plz|Terrace|Terr|Circle|Cir|Trail|Trl|Parkway|Pkwy|Commons|Cmns|Square|Sq)\b'
    website_regex = r'https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,}'
    name_regex = r"(Mr\.|Mrs\.|Ms\.|Dr\.|Capt\.|Captain)\s+([A-Z][\w'-]+)\s+([A-Z][\w'-]+)?"

    potential_blocks = soup.find_all(['div', 'p', 'footer', 'section', 'td', 'span', 'article', 'header', 'aside', 'li'])
    for block in potential_blocks:
        text = ' '.join(block.stripped_strings)

        phones = tuple(re.findall(phone_regex, text, re.IGNORECASE))
        emails = tuple(re.findall(email_regex, text, re.IGNORECASE))
        addresses = tuple(re.findall(address_regex, text, re.IGNORECASE))
        websites = tuple(re.findall(website_regex, text, re.IGNORECASE))
        names = tuple(re.findall(name_regex, text))

        if phones or emails:
            contact_details = {
                'phone1': phones[0] if len(phones) > 0 else '',
                'phone2': phones[1] if len(phones) > 1 else '',
                'email1': emails[0] if len(emails) > 0 else '',
                'email2': emails[1] if len(emails) > 1 else '',
                'address1': addresses[0] if len(addresses) > 0 else '',
                'address2': addresses[1] if len(addresses) > 1 else '',
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
    return contacts


def save_to_csv(contacts, filename):
    if contacts.empty:
        logging.warning("No contacts to save in csv.")
        return
    try:
        contacts.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}. Total unique contacts: {len(contacts)}")
    except Exception as e:
        logging.error(f"Saving to CSV failed with error: {e}")


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


def clean_contact_information(all_contacts):
    contact_info = pd.DataFrame(all_contacts)
    
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

    return contact_info


def process_url(url):
    try:
        logging.debug(f"Processing URL: {url}")
        html_content = fetch_html(url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            contacts = proximity_based_extraction(soup, url)
            return contacts
        return []
    except Exception as e:
        logging.error(f"Error processing URL {url}: {e}")
        return []


def setup_paths_and_logging(search_queries):
    current_datetime = datetime.now()
    current_formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    current_date = current_datetime.strftime("%Y_%m_%d")

    results_path = os.path.join("Results", current_date)
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    log_filepath = os.path.join(results_path, f"{search_queries[0].replace(' ', '_')}_{current_date}.log")
    logging.basicConfig(filename=log_filepath, filemode='a', format=log_format, level=logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.root.addHandler(console_handler)
    logging.info("Logging started")

    csv_filename = f"{current_formatted_datetime}_{search_queries[0].replace(' ', '-')}.csv"
    csv_filepath = os.path.join(results_path, csv_filename)
    logging.info(f"CSV Filepath: {csv_filepath}")
    return csv_filepath


if __name__ == "__main__":
    search_queries = [
        "Texas saltwater fishing guides",
    ]

    csv_filepath = setup_paths_and_logging(search_queries)
    all_contacts = []
    
    logging.info(f"Starting with queries: {search_queries}")
    for query in search_queries:
        logging.info(f"Starting query: {query}")
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
                    logging.debug(f"Completed processing: {future_to_url[future]}")
    
    cleaned_contacts = clean_contact_information(all_contacts)
    save_to_csv(cleaned_contacts, csv_filepath)
