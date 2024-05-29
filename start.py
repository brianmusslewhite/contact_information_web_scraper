import requests
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import re
from bs4 import BeautifulSoup, NavigableString, Tag
import re


HEADER = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

def get_google_search_results(query, num_pages=1):

    urls = []
    for page in range(num_pages):
        url = f'https://www.google.com/search?q={query}&start={page*10}'
        response = requests.get(url, headers=HEADER)
        
        # Print the raw HTML content for debugging
        # print(f"Response content for page {page}:\n{response.text[:1000]}...\n")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all 'a' tags
        links = soup.find_all('a', href=True)
        # print(f"Found {len(links)} 'a' elements with href attribute")
        
        for link in links:
            href = link['href']
            # Print out the href for debugging
            # print(f"Link href: {href}")
            if 'U&url=' in href:
                try:
                    # Extract the actual URL
                    actual_url = href.split('U&url=')[1].split('&')[0]
                    # Decode the URL to handle any URL encoding
                    actual_url = requests.utils.unquote(actual_url)
                    # Verify if the extracted URL is valid
                    if actual_url.startswith('http'):
                        urls.append(actual_url)
                        # print(f"Extracted URL: {actual_url}")
                    else:
                        print(f"Skipping non-HTTP URL: {actual_url}")
                except IndexError:
                    print(f"Skipping malformed URL: {href}")
        
        time.sleep(2)  # Be polite and avoid getting blocked
    return urls

query = "Fishing Guides in Texas"
urls = get_google_search_results(query, num_pages=1)
# urls = ["https://www.nychealthandhospitals.org/doctors/"]
print("Got urls")

def set_up_driver():
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')  # Run in headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    return driver

def fetch_html(driver, url):
    driver.get(url)
    html_content = driver.page_source
    # print("HTML Content:", html_content[:1000])  # Print the first 1000 characters for inspection
    return html_content

def parse_html(html_content):
    return BeautifulSoup(html_content, 'lxml')

def find_phone_numbers_and_chunks(soup):
    phone_pattern = re.compile(r'\+?\d[\d\s\-\(\)]{7,}\d')
    chunks = []
    
    for text in soup.find_all(string=phone_pattern):
        phone_number = phone_pattern.search(text)
        if phone_number:
            print("Found phone number:", phone_number.group())  # Debugging line
            top_level_div = find_top_level_div(text)
            if top_level_div:
                chunks.append((top_level_div, phone_number.group()))
    
    return chunks

def find_top_level_div(element):
    while element:
        if isinstance(element, Tag) and element.name == 'div':
            # print("Found top-level div:", element)  # Debugging line
            return element
        element = element.parent
    return None

def extract_from_chunk(chunk):
    text = chunk.get_text()

    # Regular expression patterns for names, phone numbers, emails, and addresses
    name_pattern = re.compile(r'\b[A-Z][a-z]*\s[A-Z][a-z]*\b')  # Simple pattern for First Last names
    phone_pattern = re.compile(r'\+?\d[\d\s\-\(\)]{7,}\d')  # Pattern for phone numbers
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')  # Pattern for emails
    address_pattern = re.compile(r'\d+\s[A-Za-z]+\s[A-Za-z]+')  # Basic pattern for addresses

    names = name_pattern.findall(text)
    phone_numbers = phone_pattern.findall(text)
    emails = email_pattern.findall(text)
    addresses = address_pattern.findall(text)

    # Combine extracted information into a dictionary for clarity
    contact_info = {
        'names': names,
        'phone_numbers': phone_numbers,
        'emails': emails,
        'addresses': addresses
    }

    return contact_info


def extract_contact_info(driver, url):
    html_content = fetch_html(driver, url)
    soup = parse_html(html_content)

    chunks_and_phones = find_phone_numbers_and_chunks(soup)
    
    contacts = []
    for chunk, phone in chunks_and_phones:
        contact_info = extract_from_chunk(chunk)
        print(contact_info)
        contacts.append(contact_info)
    
    return contacts


driver = set_up_driver()
for url in urls:
    contacts = extract_contact_info(driver, url)
driver.quit()
print(contacts)