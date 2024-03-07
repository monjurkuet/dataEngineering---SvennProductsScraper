import json
import os
import datetime
from os.path import join, dirname
from pymongo import MongoClient
from dotenv import load_dotenv
from models import Product, Variant, Store, ProductDescription, ProductPrices, StoreInfo
from typing import List

DB_NAME = "byggmakker"
COLLECTION_NAME = "products"


def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def get_mongodb_uri():
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    MONGODB_URI = os.getenv("MONGODB_URI")
    return MONGODB_URI


def get_product_collection(MONGODB_URI):
    """
    Connect to the MongoDB database and return the collection for products

    Args:
        MONGODB_URI: str, the URI for the MongoDB database

    Returns:
        collection: pymongo.collection.Collection, the collection for products
    """
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection


def process_product(product_ids, product_descriptions, product_prices: List[dict], store_info) -> list[Product]:
    products_list = []
    for product in product_ids:
        ean = product["id"]
        # Dict to store processed product
        processed_product = dict({})
        processed_product["ean_codes"] = [ean]
        # find all corresponding product descriptions
        product_description = ProductDescription.model_validate(next(filter(lambda x: x["ean"] == ean, product_descriptions), None))
        processed_product["nobb_codes"] = [product_description.id]
        processed_product["base_name"] = product_description.name
        processed_product["brand"] = product_description.brandName
        processed_product["base_images"] = product_description.images
        processed_product["base_category"] = [category.name for category in product_description.categories]
        processed_product["base_unit"] = product_description.measurements.netContent.unit
        processed_product["base_price_unit"] = ""

        # populate variants array
        procesed_variants = []
        # product_prices_variants = list(map(lambda y: ProductPrices.model_validate(y) ,filter(lambda x: x[0]["ean"] == ean, product_prices)))
        product_prices_variants = []

        for product_price in product_prices:
            for product_entry in product_price:
                if product_entry["ean"] == ean:
                    product_prices_variants.append(ProductPrices.model_validate(product_entry))

        for product_variant in product_prices_variants:
            variant = dict({})
            variant["brand"] = next(filter(lambda x: x["ean"] == ean, product_descriptions))["brandName"]
            variant["url_product"] = next(filter(lambda x: x["id"] == ean, product_ids))["link"]
            variant["retail_unit"] = product_variant.salesUnitLocalized
            variant["retail_price_unit"] = product_variant.comparisonPriceUnit
            variant["ean_codes"] = [ean]
            variant["nobb_codes"] = [product_description.id]
            variant["categories"] = [category.name for category in product_description.categories]
            variant["stores"] = []
            stores = list(map(lambda y: StoreInfo.model_validate(y), filter(lambda x: x["id"] == product_variant.storeId, store_info)))
            for store in stores:
                temp_store = dict({})
                temp_store["storeId"] = store.id
                temp_store["store_name"] = store.name
                temp_store["price"] = product_variant.price
                temp_store["scraped_at"] = "" 
                variant["stores"].append(Store.model_validate(temp_store))
            procesed_variants.append(Variant.model_validate(variant))
        processed_product["variants"] = procesed_variants
        processed_product["created"] = datetime.datetime.now().astimezone().isoformat()
        processed_product["updated"] = datetime.datetime.now().astimezone().isoformat()

        products_list.append(Product.model_validate(processed_product))
    return products_list

        

def main():
    # load json files
    print("Loading json files...")
    store_info = load_json("store_info.json")
    product_descriptions = load_json("data/gulv/laminatgulv/product_description.json")
    product_ids = load_json("data/gulv/laminatgulv/products_ids.json")
    product_prices = load_json("data/gulv/laminatgulv/prices/product_prices.json")
    print("Done\n")


    # process product entries
    print("Processing product entries...")
    products_list = process_product(product_ids, product_descriptions, product_prices, store_info)
    products_list = list(map(lambda x: x.model_dump(), products_list))
    print("Done\n")

    # connect to mongodb and get products collection
    print("Reading MONGODB_URI from '.env' file...")
    MONGODB_URI = get_mongodb_uri()
    print("Connecting to mongodb ...")
    collection = get_product_collection(MONGODB_URI)
    print(f"Database name: {DB_NAME}")
    print(f"Collection name: {COLLECTION_NAME}")
    print("Connected successfully!\n")
    print("Inserting products into collection...")
    collection.insert_many(products_list)
    print("Done\n")


if __name__ == "__main__":
    main()
