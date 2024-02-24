import json
from pymongo import MongoClient
from datetime import datetime
import os

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['construction_products']
collection = db['products']

#file directories
current_dir = os.getcwd()
DATA_DIRECTORY=os.path.join(current_dir, 'data')

target_files=['product_description.json','products_ids.json','product_prices.json']
file_names = [os.path.join(root, filename) for root, _, files in os.walk(DATA_DIRECTORY) for filename in files if filename in target_files]

product_description=next((item for item in file_names if 'product_description' in item), None)
products_ids=next((item for item in file_names if 'products_ids' in item), None)
product_prices=next((item for item in file_names if 'product_prices' in item), None)


# Load data from JSON files
with open('store_info.json') as f:
    store_info = json.load(f)
with open(product_description) as f:
    product_description = json.load(f)
with open(products_ids) as f:
    products_ids = json.load(f)
with open(product_prices) as f:
    product_prices = json.load(f)

# Process and integrate data
for product_id in products_ids:
    # Use EAN codes as the primary identifier
    ean_codes = products_ids[product_id]['ean_codes']

    # Extract base product information
    base_name = product_description[product_id]['name']
    brand = product_description[product_id]['brandName']
    base_images = product_description[product_id]['images']

    # Extract variant information
    variants = []
    for store in store_info:
        if product_id in store_info[store]:
            variant = store_info[store][product_id]
            variant['retailer'] = store
            variant['price'] = product_prices[store][product_id]
            variants.append(variant)

    # Create or update document in MongoDB
    doc = collection.find_one({'ean_codes': {'$in': ean_codes}})
    if doc:
        # Update existing document
        doc['updated'] = datetime.now()
        doc['variants'].extend(variants)
        collection.replace_one({'_id': doc['_id']}, doc)
    else:
        # Create new document
        doc = {
            'created': datetime.now(),
            'updated': datetime.now(),
            'base_name': base_name,
            'brand': brand,
            'base_images': base_images,
            'ean_codes': ean_codes,
            'variants': variants
        }
        collection.insert_one(doc)
