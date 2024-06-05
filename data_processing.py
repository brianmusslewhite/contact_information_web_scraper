import logging
import re

import pandas as pd
import phonenumbers
from email_validator import validate_email, EmailNotValidError


def proximity_based_extraction(soup, url, manager):
    try:
        logging.debug(f"Starting proximety based extraction for: {url}")
        contacts = []
        seen_data = set()

        phone_regex = r'\(?\b[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b'
        email_regex = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
        # address_regex = r'\b\d{1,5}\s(?:\b\w+\b\s?){0,4}(Street|St|Drive|Dr|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Court|Ct|Way|Plaza|Plz|Terrace|Terr|Circle|Cir|Trail|Trl|Parkway|Pkwy|Commons|Cmns|Square|Sq)\b'
        website_regex = r'https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,}'
        name_regex = r"(Mr\.|Mrs\.|Ms\.|Cpt\.|Capt\.|Captain)\s+([A-Z][\w'-]+)\s+([A-Z][\w'-]+)?"

        potential_blocks = soup.find_all(['div', 'p', 'footer', 'section', 'td', 'span', 'article', 'header', 'aside', 'li'])
        for block in potential_blocks:
            text = ' '.join(block.stripped_strings)

            phones = tuple(re.findall(phone_regex, text, re.IGNORECASE))
            emails = tuple(re.findall(email_regex, text, re.IGNORECASE))
            # addresses = tuple(re.findall(address_regex, text, re.IGNORECASE))
            # websites = tuple(re.findall(website_regex, text, re.IGNORECASE))
            names = tuple(re.findall(name_regex, text))

            if phones or emails:
                contact_details = {
                    'phone1': phones[0] if len(phones) > 0 else '',
                    # 'phone2': phones[1] if len(phones) > 1 else '',
                    'email1': emails[0] if len(emails) > 0 else '',
                    # 'email2': emails[1] if len(emails) > 1 else '',
                    # 'address1': addresses[0] if len(addresses) > 0 else '',
                    # 'address2': addresses[1] if len(addresses) > 1 else '',
                    # 'website1': websites[0] if len(websites) > 0 else '',
                    # 'website2': websites[1] if len(websites) > 1 else '',
                }

                for i, name in enumerate(names[:1]):
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
        # if contacts:
        #     contacts = contacts[:5]
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
        contact_info['phone1'] = contact_info['phone1'].apply(standardize_phone)
        # contact_info['phone2'] = contact_info['phone2'].apply(standardize_phone)
        
        # clean email
        contact_info['email1'] = contact_info['email1'].apply(standardize_email)
        # contact_info['email2'] = contact_info['email2'].apply(standardize_email)
        
        # clean address
        # to-do

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
