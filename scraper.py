#!/usr/bin/env python3
"""
Web Scraping Module for Books.toscrape.com

This module provides a comprehensive solution for scraping book data from
the Books.toscrape website. It includes functionality to:
- Extract book links from catalog pages with pagination support
- Parse detailed book information from individual product pages
- Process data concurrently for improved performance
- Schedule automated scraping tasks
- Save results to JSON format

The module handles various edge cases including URL normalization,
error handling, and progress tracking.

Key Features:
    - Concurrent scraping with ThreadPoolExecutor
    - Progress visualization with tqdm
    - Flexible scheduling with schedule library
    - Robust error handling for network requests
    - Configurable batch processing

Example:
    >>> from books_scraper import scrape_books
    >>> books_data = scrape_books(
    ...     base_url='https://books.toscrape.com/',
    ...     is_save=True
    ... )
    >>> print(f"Scraped {len(books_data)} books")
"""

import re
import select
import sys
import json
import time
from itertools import islice
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Generator, Callable, Dict, Any
import requests
import schedule
from tqdm import tqdm
from bs4 import BeautifulSoup


def get_soup(session: requests.Session,
             base_url: str) -> Optional[BeautifulSoup]:
    """
    Fetch HTML content from a URL and parse it into a BeautifulSoup
    object.

    Args:
        session (requests.Session): The requests session to use for the
                                    HTTP request.
        base_url (str): The URL to fetch HTML content from.

    Returns:
        Optional[BeautifulSoup]:
            - BeautifulSoup object if request is successful
            - None if an error occurs during HTML parsing

    Raises:
        requests.RequestException: If there's an issue with the HTTP
                                   request (connection error, timeout,
                                   HTTP error, etc.)

    Note:
        HTTP-related exceptions are propagated to the caller for
        handling.
        Returns None only for HTML parsing issues, not for HTTP errors.

    Example:
        >>> import requests
        >>> session = requests.Session()
        >>> try:
        ...     soup = get_soup(session, "https://example.com")
        ...     if soup:
        ...         title = soup.find('title')
        ...         print(title.text)
        ... except requests.RequestException as e:
        ...     print(f"Request failed: {e}")
        >>> session.close()
    """
    response = session.get(base_url, timeout=20)
    response.raise_for_status()
    return BeautifulSoup(response.content, 'lxml')


def get_books_links(
        session: requests.Session,
        base_url: str,
        _raw_url: Optional[str] = None) -> Generator[str, None, None]:
    """
    Generates book links from an online catalog.

    The function iterates through catalog pages, extracts book links
    and handles pagination. Automatically adjusts paths by adding
    'catalogue/' to relative URLs when necessary.

    Args:
        session (requests.Session): The requests session to use for HTTP
                                    requests
        base_url (str): Base catalog URL to start parsing from
        _raw_url (Optional[str]): Raw URL for edge case testing.
                                  If not provided, uses base_url + '/'

    Yields:
        str: Full URLs of book links

    Note:
        _raw_url is needed for edge case testing when starting from
        specific pages. See usage example.

    Examples:
        Basic usage:
            >>> with requests.Session() as session:
            ...     for book_link in get_books_links(
                        session, "http://books.toscrape.com"
                    ):
            ...         print(book_link)

        Edge case - starting from specific page:
            >>> b_url = ('https://books.toscrape.com/'
                         +'catalogue/page-31.html')
            >>> r_url = 'https://books.toscrape.com/'
            >>> with requests.Session() as session:
            ...     res = list(get_books_links(session,
                                               base_url=b_url,
                                               _raw_url=r_url))
            ...     print(f"Found {len(res)} books start with page 31")
    """
    in_catalogue: Callable[[str], str] = lambda link: (
        '' if 'catalogue' in link else 'catalogue/'
    )

    if not _raw_url:
        _raw_url = base_url.rstrip('/') + '/'

    while base_url:
        soup = get_soup(session, base_url)
        if not soup:
            break

        for link in soup.find_all('a', title=True):
            href = link.get('href', '')
            if href:
                yield _raw_url + in_catalogue(href) + href

        next_page = soup.find('li', class_='next')
        if next_page and next_page.a:
            next_page = next_page.a['href']
            base_url = _raw_url + in_catalogue(next_page) + next_page
        else:
            base_url = None


def get_book_data(session: requests.Session, book_url: str) -> Dict[str, Any]:
    """
    Extracts detailed information about a book from its product page.

    This function parses the HTML content of a book page and extracts
    various details including title, price, availability, rating,
    description, and product information such as UPC, product type,
    tax details, etc.

    Args:
        session (requests.Session): The requests session to use for HTTP
                                    requests
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

    Note:
        Returns an empty dictionary if the page cannot be loaded or
        parsed.
        Converts star rating classes ('One', 'Two', etc.) to numeric
        values.
        Some fields may be missing or empty if the corresponding HTML
        elements are not found.
        The 'product description' field may be an empty string.

    Raises:
        No exceptions are raised externally; all errors are handled
        internally by returning an empty dictionary. Internal parsing
        errors may occur if the HTML structure differs from expected.

    Example:
        >>> session = requests.Session()
        >>> book_data = get_book_data(
        ...     session,
        ...     "http://books.toscrape.com/catalogue/"
        ...     + "a-light-in-the-attic_1000/index.html"
        ... )
        >>> session.close()
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
    soup: Optional[BeautifulSoup] = get_soup(session, book_url)

    p_main: BeautifulSoup
    info_table: BeautifulSoup
    desc: BeautifulSoup
    if soup:
        p_main = soup.find('div', class_='col-sm-6 product_main')
        info_table = soup.find('table')
        desc = soup.find('div', id='product_description')

    if soup and p_main and info_table:
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
                'UPC': info_table.find_all('tr')[0].td.text,
                'product Type': info_table.find_all('tr')[1].td.text,
                'price (excl. tax)': (
                    re_price.search(info_table.find_all('tr')[2].td.text)
                    .group()),
                'price (incl. tax)': (
                    re_price.search(info_table.find_all('tr')[3].td.text)
                    .group()),
                'tax': (re_price
                        .search(info_table.find_all('tr')[4].td.text)
                        .group()),
                'availability': (re_availability
                                 .search(info_table.find_all('tr')[5].td.text)
                                 .group(1)),
                'number of reviews': info_table.find_all('tr')[6].td.text
            }
        }

    return book_data


def scrape_books(
        base_url: str = '',
        _raw_url: Optional[str] = None,
        batch_size: int = 200,
        is_save: bool = False,
        file_name: str = './artifacts/books_data.txt') -> Dict[str, Any]:
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
        is_save (bool): If True, saves the scraped data to a file
                        (default: False).
        file_name (str): Path where to save the resulting file when
                         is_save is True
                         (default: './artifacts/books_data.txt').

    Returns:
        Dict[str, Any]: A dictionary where keys are book URLs
                        and values are dictionaries containing
                        detailed book information.
                        Returns empty dict if no books found.

    Note:
        Uses ThreadPoolExecutor for concurrent scraping.
        Includes progress visualization with tqdm.
        Saves data only if is_save=True and books data is available.

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

        >>> # Edge case - starting from specific page:
        >>> books = scrape_books(
        ...     base_url=('https://books.toscrape.com'
        ...              + '/catalogue/page-31.html'),
        ...     _raw_url='https://books.toscrape.com/',
        ...     is_save=True
        ... )
        >>> # Data will be saved to 'books_data.txt'
    """
    books: Dict[str, Any] = {}

    try:
        with requests.Session() as session:
            links = get_books_links(session, base_url, _raw_url)
            soup = get_soup(session, base_url)

            if soup:
                strong_tags = soup.find_all('strong')
                total = int(strong_tags[0].text) - int(strong_tags[1].text) + 1

                with tqdm(total=total, desc='Scrape books', ncols=100) as pbar:
                    with ThreadPoolExecutor(max_workers=70) as executor:
                        while True:
                            batch = list(islice(links, batch_size))
                            if not batch:
                                break

                            books.update(
                                zip(batch, executor.map(
                                    lambda url: get_book_data(session, url),
                                    batch))
                            )
                            pbar.update(len(batch))

    except requests.RequestException as e:
        print(f"Error during scraping {base_url}: {e}")

    finally:
        if is_save:
            if books:
                with open(file_name, 'w', encoding='utf-8') as f:
                    json.dump(books, f, ensure_ascii=False, indent=4)
                print(f"The data has been saved to file '{file_name}'!")
            else:
                print("No data to save - books dictionary is empty")

    return books


if __name__ == '__main__':
    schedule.every().day.at("19:00:00", 'Europe/Moscow').do(
        scrape_books, base_url='https://books.toscrape.com/', is_save=True)

    try:
        print('Task Scheduler has started.')
        print('To stop, type S/s and press Enter.')

        while True:
            schedule.run_pending()
            if select.select([sys.stdin], [], [], 0.1)[0]:
                if sys.stdin.readline().strip().lower() in ['S', 's']:
                    print("Stopping scheduler...")
                    break
            time.sleep(1)

    except KeyboardInterrupt:
        print('\nThe scraping process has terminated!')
