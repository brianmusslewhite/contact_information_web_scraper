import logging
import re
import urllib.parse
from urllib.parse import urlparse

import pandas as pd
import phonenumbers
from email_validator import validate_email, EmailNotValidError


def get_base_url(full_url):
    parsed_url = urlparse(full_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return base_url


def find_contact_us_links(soup, url, manager):
    base_url = get_base_url(url)
    contact_pattern = re.compile(r'\b(contact|reach out|get in touch|contact us|contact me|reach us)\b', re.IGNORECASE)

    a_tags = soup.find_all('a')
    for tag in a_tags:
        # Check both the text and the title attribute for matching the contact pattern
        link_text = tag.text.strip() if tag.text else ''
        title_attr = tag.get('title', '').strip()
        
        # Search in both the visible text and the title attribute of the tag
        if contact_pattern.search(link_text) or contact_pattern.search(title_attr):
            href = tag.get('href')
            
            # Check if href is valid and not empty
            if href and not href.startswith('#') and not href.startswith('mailto:'):
                full_url = urllib.parse.urljoin(base_url, href)
                manager.add_url(full_url)


def proximity_based_extraction(soup, url, manager):
    try:
        logging.debug(f"Starting proximety based extraction for: {url}")
        contacts = []
        seen_data = set()

        find_contact_us_links(soup, url, manager)

        phone_regex = r'\(?\b[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b'
        email_regex = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
        name_regex = r"(Mr\.|Mrs\.|Ms\.|Capt\.|Captain)\s+([A-Z][\w'-]+)\s+([A-Z][\w'-]+)?"

        potential_blocks = soup.find_all(['div', 'p', 'footer', 'section', 'td', 'span', 'article', 'header', 'aside', 'li'])
        for block in potential_blocks:
            text = ' '.join(block.stripped_strings)

            phones = tuple(re.findall(phone_regex, text, re.IGNORECASE))
            emails = tuple(re.findall(email_regex, text, re.IGNORECASE))
            names = tuple(re.findall(name_regex, text))

            if phones or emails:
                contact_details = {
                    'phone': phones[0] if phones else '',
                    'email': emails[0] if emails else '',
                    'salutation': names[0][0] if names else '',
                    'first_name': names[0][1] if names else '',
                    'last_name': names[0][2] if len(names) > 0 and len(names[0]) > 2 else '',
                    'source': url
                }

                # Remove duplicates
                contact_id = frozenset(contact_details.items())
                if contact_id not in seen_data:
                    seen_data.add(contact_id)
                    contacts.append(contact_details)

        logging.debug(f"Contacts found in {url}, {contacts}")
        return contacts
    except Exception as e:
        raise e


def clean_contact_information(all_contacts):
    try:
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
                logging.debug(f"Invalid email: {email}, error: {e}")
                return ""
        
        if not all_contacts:
            logging.critical("No contact information provided for cleaning.")
            return pd.DataFrame()

        contact_info = pd.DataFrame(all_contacts)
        logging.info(f"Cleaning contact information. Length before cleaning: {len(contact_info)}")
        
        # clean phone
        contact_info['phone'] = contact_info['phone'].apply(standardize_phone)
        
        # clean email
        contact_info['email'] = contact_info['email'].apply(standardize_email)

        # remove exact duplicates
        contact_info.drop_duplicates(inplace=True)

        # remove partial duplicates
        # to-do

        logging.info(f"Cleaning complete. Length after cleaning: {len(contact_info)}")
    except Exception as e:
        logging.critical(f"Error cleaning contacts: {e}")
        return all_contacts
    return contact_info


def save_to_csv(contacts, filename):
    if contacts.empty:
        logging.critical("No contacts to save in csv.")
        return
    try:
        contacts.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}. Total unique contacts: {len(contacts)}")
    except Exception as e:
        logging.critical(f"Saving to CSV failed with error: {e}")
