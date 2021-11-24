"""The script scrapes descriptions from the Ukrainian Startup Fund for each
project and then crawles the Web in search for contact data of the projects.
"""
import logging
import time
import re
from urllib.parse import urlparse
import os

from bs4 import BeautifulSoup

from utils.http_request import HttpRequest, PROXY_TYPE_FREE, PROXY_TYPE_TOR
from utils.scraping_utils import (
    FATAL_ERROR_STR,

    setup_logging,
    fix_filename,
    remove_umlauts,
    clean_text,

    save_last_page,
    load_last_page,

    save_items_csv,
    load_items_csv,

    save_items_json,
    load_items_json,
)

CSV_FILENAME = 'items.csv'
JSON_FILENAME = 'items.json'

COLUMNS = [
    'title',
    'description',
    'industry',
    'url',
    'emails',
    'phones',
]

BASE_URL = 'https://usf.com.ua/wp-admin/admin-ajax.php'
CATEGORY_ID = '78'

TIME_DELAY = 1

# Ony files with these extensions are treated as webpages by the crawler
HTML_EXTENSIONS = [
    '.htm', '.html', '.asp', '.aspx', '.cgi', '.php', '.pl', '.py'
]

EMAIL_RE = re.compile(r'\b([A-Za-z0-9._+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,6})\b')

PHONE_RE = re.compile(r'\D(\+380\d{9})\D')

MAX_RECURSION_DEPTH = 1

def get_page_count() -> int:
    payload = {
        'action': 'loadmore_count_post',
        'category_id': CATEGORY_ID,
    }
    r = HttpRequest().post(BASE_URL, data=payload)
    if r == None or not r.text.isdigit():
        return None
    return int(r.text)

def get_page_html(page: int) -> str:
    payload = {
        'action': 'loadmore',
        'category_id': CATEGORY_ID,
        'page': str(page),
    }
    r = HttpRequest().post(BASE_URL, data=payload)
    if r == None:
        return None
    return r.text

def scrape_items(html: str) -> list:
    items = []
    soup = BeautifulSoup(html, 'lxml')
    for div in soup.find_all('div', class_='modal-content-box'):
        item = {
            'title': '',
            'description': '',
            'industry': '',
            'url': '',
            'emails': '',
            'phones': '',
        }

        item['title'] = div.find('div', class_='modal_title').get_text()
        item['description'] = clean_text(
            div.find('span', class_='card_item_description').get_text())

        ul = div.find_next_sibling('ul')
        item['industry'] = ul.find('div', class_='mr-td-industry').get_text()

        a = ul.find('a')
        if a != None:
            item['url'] = a.get('href', '')

        items.append(item)

    return items

def scrape_all_items() -> list:
    items = []

    for page in range(get_page_count()):
        logging.info(f'Scraping page {page + 1}')
        items.extend(scrape_items(get_page_html(page)))
        time.sleep(TIME_DELAY)

    return items

def get_html(url: str) -> str:
    r = HttpRequest().get(url)
    if r == None:
        return None
    return r.text

def get_host_url(url: str) -> str:
    return '{}://{}'.format(urlparse(url).scheme, urlparse(url).netloc)

# The function returns the list of the links to html pages only
def get_internal_links(soup: BeautifulSoup, url: str) -> list:
    host_url = get_host_url(url)

    internal_links = []

    for link in soup.find_all('a', href=re.compile(f'^(/|{host_url})')):
        absolute_link = link.get('href')
        if absolute_link:
            if absolute_link.startswith('/'):
                absolute_link = host_url + absolute_link

            # Mediafiles and such stuff should be skipped
            ext = os.path.splitext(urlparse(absolute_link).path)[1]
            if ext and ext not in HTML_EXTENSIONS:
                continue

            if absolute_link not in internal_links:
                internal_links.append(absolute_link)

    return internal_links

# The emails parameter serves both for input and output
# The function returns nothing
def find_distinct_emails(text: str, emails: list):
    for match in re.findall(EMAIL_RE, text):
        if match not in emails:
            emails.append(match)

# The phones parameter serves both for input and output
# The function returns nothing
def find_distinct_phones(text: str, phones: list):
    for match in re.findall(PHONE_RE, text):
        if match not in phones:
            phones.append(match)

# The links, emails and phones parameters serve both for input and output
# The function returns nothing
def crawl(url: str, links: list, emails: list, phones: list, depth: int = 0):
    logging.info(f'Crawling page {url}')

    html = get_html(url)
    if not html:
        return

    find_distinct_emails(html, emails)
    find_distinct_phones(re.sub(r'\s+|-|\(|\)', '', html), phones)

    if depth >= MAX_RECURSION_DEPTH:
        return

    soup = BeautifulSoup(html, 'lxml')
    for link in get_internal_links(soup, url):
        if link not in links:
            links.append(link)
            crawl(link, links, emails, phones, depth = depth + 1)

def scrape_contact_data(url: str) -> dict:
    logging.info(f'Collecting contact data for site {url}')
    links = [get_host_url(url), get_host_url(url) + '/']
    emails = []
    phones = []
    crawl(url, links, emails, phones)

    return {
        'emails': '; '.join(emails),
        'phones': '; '.join(phones),
    }

def append_contact_data(items: list):
    for i, item in enumerate(items):
        if items[i]['url']:
            contact_data = scrape_contact_data(items[i]['url'])
            items[i]['emails'] = contact_data['emails']
            items[i]['phones'] = contact_data['phones']

def main():
    setup_logging()
    logging.info('Starting scraping process.')
    items = scrape_all_items()
    if items == None:
        logging.error(FATAL_ERROR_STR)
        return

    logging.info('Initial data has been collected. '
                 'Now searching for contact information.')
    append_contact_data(items)

    logging.info('Scraping process complete. Now saving the results.')
    if not save_items_csv(items, COLUMNS, CSV_FILENAME):
        logging.error(FATAL_ERROR_STR)
        return
    logging.info('Saving complete.')

if __name__ == '__main__':
    main()
