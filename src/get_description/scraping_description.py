import aiohttp
import json
import asyncio
import os
from utils import USER_AGENT_LIST, logger
import random


class DescriptionEans:

    def __init__(self, category_l1, category_l2):
        self.url_base = "https://www.byggmakker.no/api/product/"
        self.headers = {"User-Agent": random.choice(USER_AGENT_LIST)}
        self.selected_keys = ['ean', 'id', 'name', 'brandName',
                              'measurements', 'images', 'categories', 'relatedEans']
        self.__category_l1 = category_l1
        self.__category_l2 = category_l2
        self.folder_path = os.path.join('data', category_l1, category_l2)
        self.all_products_info = []  # Initialize an empty list to store all products info
        self.create_folders()

    def create_folders(self):
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
            logger.info(f"Created directory: {self.folder_path}")

    async def fetch_product(self, product, semaphore):
        async with semaphore:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                url = self.url_base + product
                try:
                    async with session.get(url) as response:
                        json_raw = await response.json()
                        clean_json_data = {
                            key: json_raw[key] for key in self.selected_keys if key in json_raw}
                        # Append the clean_json_data to the all_products_info list
                        self.all_products_info.append(clean_json_data)
                except Exception as e:
                    logger.error(
                        f"Error fetching product description {product} from {url}: {e}")

    async def scraping_description(self, product_ids):
        logger.info(
            f"Scraping description for {self.__category_l1}/{self.__category_l2} started.")
        # Limit the number of concurrent requests
        semaphore = asyncio.Semaphore(20)
        tasks = [self.fetch_product(product, semaphore)
                 for product in product_ids]
        await asyncio.gather(*tasks)

        # After fetching all products, write the aggregated data to a single JSON file
        file_path = os.path.join(self.folder_path, "product_description.json")
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(self.all_products_info, json_file,
                      indent=4, ensure_ascii=False)

        logger.info(
            f"Scraping description for {self.__category_l1}/{self.__category_l2} finished.")
