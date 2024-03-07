import os
import aiohttp
import json
import asyncio
import random
from utils import USER_AGENT_LIST, logger


class AvailabilityEans():

    def __init__(self, category_l1, category_l2) -> None:
        # URL base and headers
        self.url_base = "https://www.byggmakker.no/api/availability/"
        self.headers = {"User-Agent": random.choice(USER_AGENT_LIST)}
        self.__category_l1 = category_l1
        self.__category_l2 = category_l2
        self.selected_keys = ['ean', 'ean', 'id', 'name', 'brandName',
                              'measurements', 'images', 'categories', 'relatedEans']
        self.folder_path = os.path.join(
            'data', category_l1, category_l2, 'availability')
        self.create_folders()

    def create_folders(self):
        # Create folder path if it doesn't exist
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
                        # Save availability to a JSON file
                        file_path = os.path.join(
                            self.folder_path, f"{product}_availability.json")
                        with open(file_path, 'w', encoding='utf-8') as json_file:
                            json.dump(json_raw, json_file,
                                      indent=4, ensure_ascii=False)
                except Exception as e:
                    logger.error(
                        f"Error fetching product {product} from {url}: {e}")

    async def scraping_availability(self, product_ids):
        logger.info(
            f"Scraping availability for {self.__category_l1}/{self.__category_l2} started.")
        print(
            f"Scraping availability for {self.__category_l1}/{self.__category_l2} started.")
        semaphore = asyncio.Semaphore(20)  # Semaphore with a limit of 20
        tasks = [self.fetch_product(product, semaphore)
                 for product in product_ids]
        await asyncio.gather(*tasks)
        logger.info(
            f"Scraping availability for {self.__category_l1}/{self.__category_l2} finished.")
        print(
            f"Scraping availability for {self.__category_l1}/{self.__category_l2} finished.")
