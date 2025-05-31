import logging
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


def _detect_material(text):
    materials = {
        'polyester': ['polyester', 'poly'],
        'nylon': ['nylon'],
        'cotton': ['cotton'],
        'PVC': ['PVC', 'vinyl']
    }
    text_lower = text.lower()
    for mat, keywords in materials.items():
        if any(kw in text_lower for kw in keywords):
            return mat
    return None


def _detect_vehicle_type(text):
    types = {
        'SUV': ['SUV', 'sport utility'],
        'sedan': ['sedan'],
        'hatchback': ['hatchback', 'hatch'],
        'universal': ['universal', 'all cars', 'fit all']
    }
    text_upper = text.upper()
    for v_type, keywords in types.items():
        if any(kw.upper() in text_upper for kw in keywords):
            return v_type
    return None


def _extract_size(text):
    size_match = re.search(r'(\d+)\s*cm\s*[x×]\s*(\d+)\s*cm', text)
    if size_match:
        return f"{size_match.group(1)}x{size_match.group(2)}cm"
    return None


class CarCoverScraper:

    def __init__(self):
        self.config = {
            'base_url': 'https://www.olx.in',
            'search_term': 'car-cover',
            'max_threads': 4,
            'request_timeout': 10,
            'delay_range': (1, 3),  # More conservative delays for OLX
            'output_formats': ['csv', 'parquet']
        }

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        })
        logging.basicConfig(level=logging.INFO)

    @staticmethod
    def _extract_cover_specs(description):

        specs = {
            'material': _detect_material(description),
            'vehicle_type': _detect_vehicle_type(description),
            'is_waterproof': bool(re.search(r'water\s*proof|rain\s*proof', description, re.I)),
            'has_uv_protection': bool(re.search(r'UV|ultraviolet|sun\s*protect', description, re.I)),
            'size': _extract_size(description)
        }
        return specs

    def _scrape_listing_page(self, page_url):

        try:
            response = self.session.get(page_url, timeout=self.config['request_timeout'])
            soup = BeautifulSoup(response.text, 'html.parser')

            description = soup.find('div', {'data-aut-id': 'itemDescription'}).text if soup.find('div', {
                'data-aut-id': 'itemDescription'}) else ""
            specs = self._extract_cover_specs(description)

            time.sleep(random.uniform(*self.config['delay_range']))
            return specs

        except Exception as e:
            logging.warning(f"Error scraping {page_url}: {str(e)}")
            return None

    def scrape(self, pages=3):

        results = []

        for page in range(1, pages + 1):
            try:
                url = f"{self.config['base_url']}/items/q-{self.config['search_term']}?page={page}"
                response = self.session.get(url, timeout=self.config['request_timeout'])
                soup = BeautifulSoup(response.text, 'html.parser')

                listings = soup.find_all('li', {'data-aut-id': 'itemBox'})

                with ThreadPoolExecutor(max_workers=self.config['max_threads']) as executor:
                    # Process listings in parallel
                    listing_data = []
                    for listing in listings:
                        title = listing.find('span', {'data-aut-id': 'itemTitle'}).text.strip()
                        price = listing.find('span', {'data-aut-id': 'itemPrice'}).text.strip()
                        location = listing.find('span', {'data-aut-id': 'item-location'}).text.strip()
                        link = urljoin(self.config['base_url'], listing.find('a', {'data-aut-id': 'itemAd'})['href'])

                        # Get detailed specs in parallel
                        specs = executor.submit(self._scrape_listing_page, link).result()

                        if specs:
                            listing_data.append({
                                'title': title,
                                'price': price,
                                'location': location,
                                'url': link,
                                **specs,
                                'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })

                results.extend(listing_data)
                logging.info(f"Scraped page {page} with {len(listing_data)} listings")

                time.sleep(random.uniform(*self.config['delay_range']))

            except Exception as e:
                logging.error(f"Error on page {page}: {str(e)}")
                continue

        self._save_results(results)
        return results

    def _save_results(self, data):
     
        df = pd.DataFrame(data)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if 'csv' in self.config['output_formats']:
            csv_file = f"car_covers_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
            logging.info(f"Saved CSV: {csv_file}")

        if 'parquet' in self.config['output_formats']:
            parquet_file = f"car_covers_{timestamp}.parquet"
            df.to_parquet(parquet_file, engine='pyarrow')
            logging.info(f"Saved Parquet: {parquet_file}")


if __name__ == "__main__":
    scraper = CarCoverScraper()
    car_cover_data = scraper.scrape(pages=2)
    print(f"Total car covers scraped: {len(car_cover_data)}")