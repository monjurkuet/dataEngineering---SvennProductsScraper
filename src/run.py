from utils.scraping_ids import ScrapingIds, logger
from src.get_availability.scraping_availability import AvailabilityEans
from src.get_description.scraping_description import DescriptionEans
from src.get_prices.scraping_prices import PricesEans

import asyncio
import json


class ScrapeData():
    def __init__(self, category_l1, category_l2):
        self.category_l1 = category_l1
        self.category_l2 = category_l2

    def run(self):
        # initializing ids scraper object for specific category/subcategory
        scrap_ids = ScrapingIds(self.category_l1, self.category_l2)
        logger.info(
            f"Scraping routine for {self.category_l1}/{self.category_l2} started.")
        print(
            f"Scraping routine for {self.category_l1}/{self.category_l2} started.")

        # getting number of pages to scrape
        pages = scrap_ids.get_pages()

        # async code to scrape all ids (eans) for specific category/subcategory
        loop = asyncio.get_event_loop()
        loop.run_until_complete(scrap_ids.scraping_ids(pages))

        # Reading products id JSON file
        with open(f'data/{self.category_l1}/{self.category_l2}/products_ids.json', 'r') as json_file:
            products = json.load(json_file)

        product_ids = [product['id'] for product in products]

        # initializing description scraper object for specific category/subcategory
        description_scraper = DescriptionEans(
            self.category_l1, self.category_l2)

        # async code to scrape description for all ids (eans) on a specific category/subcategory
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            description_scraper.scraping_description(product_ids))

        # initializing availability scraper object for specific category/subcategory
        availability_scraper = AvailabilityEans(
            self.category_l1, self.category_l2)

        # async code to scrape availability for all ids (eans) on a specific category/subcategory
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            availability_scraper.scraping_availability(product_ids))

        # initializing prices scraper object for specific category/subcategory
        prices_scraper = PricesEans(
            self.category_l1, self.category_l2)  # Provide category names
        loop = asyncio.get_event_loop()
        loop.run_until_complete(prices_scraper.scraping_prices())

        logger.info(
            f"Scraping routine for {self.category_l1}/{self.category_l2} finished.")
        print(
            f"Scraping routine for {self.category_l1}/{self.category_l2} finished.")
