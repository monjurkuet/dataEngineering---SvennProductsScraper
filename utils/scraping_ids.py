import os
import asyncio
import aiohttp
import requests
import json
from bs4 import BeautifulSoup
import random
from utils import USER_AGENT_LIST, logger

class ScrapingIds:
    def __init__(self, category_l1, category_l2):
        self.__category_l1 = category_l1
        self.__category_l2 = category_l2
        self.url_base = f"https://www.byggmakker.no/kategori/{category_l1}/{category_l2}?page="
        self.headers = {"User-Agent": random.choice(USER_AGENT_LIST)}
        self.data_folder = os.path.join(os.path.dirname(__file__), '..', 'data', f'{category_l1}', f'{category_l2}')
        self.create_data_folder()

    def create_data_folder(self):
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
            logger.info(f"Created directory: {self.data_folder}")

    async def fetch_products(self, session, page):
        url = self.url_base + str(page)
        try:
            async with session.get(url, headers=self.headers) as response:
                response.raise_for_status()
                response_text = await response.text()
                soup = BeautifulSoup(response_text, 'html.parser')
                product_cards = soup.find_all('div', {'class': 'product-card__container', 'data-cy': 'product-card-container'})
                products_on_page = []
                for product in product_cards:
                    id = product.find('a').get('data-product_id')
                    link = 'https://www.byggmakker.no' + product.find('a').get('href')
                    products_on_page.append({
                        'id': id,
                        'link': link
                    })
                return products_on_page
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching products EANS for {self.__category_l1}/{self.__category_l2}: {e}")
            return []

    async def scraping_ids(self, pages):
        all_products = []
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [self.fetch_products(session, page) for page in range(1, pages + 1)]
            results = await asyncio.gather(*tasks)
            for result in results:
                all_products.extend(result)

        # Save all_products to a JSON file
        json_path = os.path.join(self.data_folder, 'products_ids.json')
        with open(json_path, 'w') as json_file:
            json.dump(all_products, json_file)

        logger.info(f"Products saved to {self.data_folder} products_ids.json")

    def get_pages(self):
        url = self.url_base + '1'
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            pages_elem = soup.find_all('span', class_='pagination__pages--total')
            self.pages = int(pages_elem[-1].text.strip())
            return self.pages
        except requests.RequestException as e:
            logger.error(f"Error fetching number of pages from {self.__category_l1}/{self.__category_l2}: {e}")
            return 0
