#!/usr/bin/env python3
"""
This module provides functions to scrape book data from
the Books.toscrape website.
It includes functionality to extract book links, parse book details,
and save results.
"""

import re
import json
import time
from itertools import islice
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Generator, Callable, Dict, Any
import requests
import schedule
from tqdm import tqdm
from bs4 import BeautifulSoup


def get_soup(base_url: str) -> Optional[BeautifulSoup]:
    """
    Fetch HTML content from a URL and parse it into a BeautifulSoup
    object.

    Args:
        base_url (str): The URL to fetch HTML content from.

    Returns:
        Optional[BeautifulSoup]:
            - BeautifulSoup object if request is successful
            - None if an error occurs

    Raises:
        requests.RequestException:
            If there's an issue with the HTTP request
            (handled internally, returns None instead of raising).

    Example:
        >>> soup = get_soup("https://example.com")
        >>> if soup:
        ...     title = soup.find('title')
        ...     print(title.text)
    """
    try:
        with requests.get(base_url, timeout=20) as r:
            r.raise_for_status()
            soup = BeautifulSoup(r.content, 'lxml')
    except requests.RequestException as e:
        print(f"Error fetching {base_url}:", e)
        soup = None

    return soup


def get_books_links(
        base_url: str,
        _raw_url: Optional[str] = None) -> Generator[str, None, None]:
    """
    Generates book links from an online catalog.

    The function iterates through catalog pages, extracts book links
    and handles pagination. Automatically adjusts paths by adding
    'catalogue/' to relative URLs when necessary.

    Args:
        base_url (str): Base catalog URL to start parsing from
        _raw_url (Optional[str]): Raw URL for edge case testing.
                                  If not provided, uses base_url + '/'

    Yields:
        str: Full URLs of book links

    Note:
        _raw_url is needed for edge case testing. See usage example.

    Examples:
        >>> for book_link in get_books_links(
                "http://books.toscrape.com"):
        ...     print(book_link)

        >>> b_url = 'https://books.toscrape.com/catalogue/page-31.html'
        >>> r_url = 'https://books.toscrape.com/'
        >>> res = get_books_links(base_url=b_url, _raw_url=r_url)
        >>> print(list(res))
        ... # displaying content 'res' starting from page 31
    """
    in_catalogue: Callable[[str], str] = (
        lambda link: '' if 'catalogue' in link else 'catalogue/')

    if not _raw_url:
        _raw_url = base_url + '/'

    while base_url:
        soup = get_soup(base_url)
        if not soup:
            break

        for link in soup.find_all('a', title=True):
            href = link.get('href', '')
            yield _raw_url + in_catalogue(href) + href

        next_page = soup.find('li', class_='next')
        if next_page and next_page.a:
            next_page = next_page.a['href']
            base_url = _raw_url + in_catalogue(next_page) + next_page
        else:
            base_url = None


def get_book_data(book_url: str) -> Dict[str, Any]:
    """
    Extracts detailed information about a book from its product page.

    This function parses the HTML content of a book page and extracts
    various details including title, price, availability, rating,
    description, and product information such as UPC, product type,
    tax details, etc.

    Args:
        book_url (str): The URL of the book's product page to scrape.

    Returns:
        Dict[str, Any]: A dictionary containing book data with the
        following structure:
            - 'title' (str): Book title
            - 'price' (str): Book price in format '£X.XX'
            - 'in stock' (str): Availability information
            - 'rating' (int): Numeric rating from 1 to 5
            - 'product description' (str): Book description text
            - 'product information' (Dict[str, str]): Detailed product
              info including:
                    'UPC'; 'product Type'; 'price (excl. tax)';
                    'price (incl. tax)'; 'tax'; 'availability';
                    'number of reviews'

    Raises:
        Exceptions related to HTML parsing originate from BeautifulSoup
        operations.

    Note:
        Returns an empty dictionary if the page cannot be loaded or
        parsed. Converts star rating classes ('One', 'Two', etc.)
        to numeric values.

    Example:
        >>> book_data = get_book_data(
                "http://books.toscrape.com/catalogue/"
                + "a-light-in-the-attic_1000/index.html"
            )
        >>> print(book_data['title'])
        'A Light in the Attic'
        >>> print(book_data['price'])
        '£51.77'
        >>> print(book_data['rating'])
        3
    """
    book_data: Dict[str, Any] = {}
    ratings: Dict[str, int] = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4,
                               'Five': 5}
    re_price: re.Pattern = re.compile(r'£\d+\.\d{2}')
    re_availability: re.Pattern = re.compile(r'\((.*?)\)')
    soup: Optional[BeautifulSoup] = get_soup(book_url)

    if soup:
        p_main = soup.find('div', class_='col-sm-6 product_main')
        info_table = soup.find('table').find_all('tr')
        desc = soup.find('div', id='product_description')

        book_data = {
            'title': p_main.find('h1').text.strip(),
            'price': (re_price
                        .search(p_main
                                .find('p', class_='price_color')
                                .text)
                        .group()),
            'in stock': re_availability.search(p_main.text).group(1),
            'rating': (
                ratings.get(
                    p_main.find('p', class_='star-rating')['class'][-1],
                    None)
                ),
            'product description': (desc
                                    .find_next_sibling('p')
                                    .text
                                    .strip()) if desc else '',
            'product information': {
                'UPC': info_table[0].td.text,
                'product Type': info_table[1].td.text,
                'price (excl. tax)': (
                    re_price.search(info_table[2].td.text).group()),
                'price (incl. tax)': (
                    re_price.search(info_table[3].td.text).group()),
                'tax': re_price.search(info_table[4].td.text).group(),
                'availability': (
                    re_availability.search(info_table[5].td.text).group(1)),
                'number of reviews': info_table[6].td.text
            }
        }

    return book_data


def scrape_books(
        base_url: str = '',
        _raw_url: Optional[str] = None,
        batch_size: int = 200,
        is_save: bool = False) -> Dict[str, Any]:
    """
    Scrapes book data from an online catalog using parallel processing.

    This function iterates through all pages of a book catalog, extracts
    book URLs, and then concurrently scrapes detailed information for
    each book. It features progress tracking, batch processing,
    and optional data persistence.

    Args:
        base_url (str): The starting URL of the book catalog to scrape.
        _raw_url (Optional[str]): Internal parameter for edge case
                                  handling of URL formatting.
        batch_size (int): Number of books to process in each batch
                          (default: 200).
        is_save (bool): If True, saves the scraped data to
                        'books_data.txt' (default: False).

    Returns:
        Dict[str, Any]: A dictionary where keys are book URLs and
                        values are dictionaries containing detailed book
                        information.

    Raises:
        Exceptions related to HTML parsing originate from BeautifulSoup
        operations.

    Note:
        Uses ThreadPoolExecutor for concurrent scraping with up
        to 100 workers.
        Includes progress visualization with tqdm.
        Only saves data if both is_save=True and initial page parsing
        succeeds.

    Example:
        >>> # Basic usage
        >>> books = scrape_books("http://books.toscrape.com")
        >>> print(f"Scraped {len(books)} books")

        >>> # With saving to file
        >>> books = scrape_books(
        ...     base_url="http://books.toscrape.com",
        ...     is_save=True
        ... )
        >>> # Data will be saved to 'books_data.txt'
    """

    books: Dict[str, Any] = {}
    links: Generator[str, None, None] = get_books_links(base_url, _raw_url)
    soup: Optional[BeautifulSoup] = get_soup(base_url)

    if soup:
        strong_tags = soup.find_all('strong')
        total = int(strong_tags[0].text) - int(strong_tags[1].text) + 1

        with tqdm(total=total, desc='Scrape books', ncols=100) as pbar:
            while True:
                batch = list(islice(links, batch_size))
                if not batch:
                    break

                with ThreadPoolExecutor(max_workers=70) as executor:
                    results = executor.map(get_book_data, batch)
                    books.update(zip(batch, results))
                pbar.update(len(batch))

    if is_save and soup:
        file_name = './artifacts/books_data.txt'
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(books, f, ensure_ascii=False, indent=4)
        print(f"The data has been saved to file '{file_name}'!")

    return books


if __name__ == '__main__':
    schedule.every().day.at("19:00:00", 'Europe/Moscow').do(
        scrape_books, base_url='https://books.toscrape.com/', is_save=True)

    while True:
        schedule.run_pending()
        if time.localtime().tm_min <= 5:
            time.sleep(3000)
        else:
            time.sleep(1)
