# Scraped Data Integration for Byggmakker Products

This repository contains JSON files with scraped data from Byggmakker products, intended for integration into a MongoDB collection. The data files are as follows:

- `store_info.json`
- `product_description.json`
- `products_ids.json`
- `Product_prices.json`

## Integration Instructions

### Product Identification

- Use `ean_codes` from `products_ids.json` as the primary identifier for documents in the MongoDB collection to match data from the different files. This is an array since each product can possibly have multiple EANs.

### Base Product Information

- From `product_description.json`, extract `name`, `brandName`, and `images` to populate `base_name`, `brand`, and `base_images` in the document.
- `base_category`, `base_unit`, and `base_price_unit` are derived from `categories` and `measurements` in the same file.

### Variants

- Each document should include a `variants` array, combining data from:
  - `store_info.json` for store-specific information.
  - `product_prices.json` for pricing information at different retailers.
- The example variant (maxbo) not from this dataset illustrates that multiple sources will contribute to variants, ensuring future data additions are accounted for.

### Updating Documents

- When updating, ensure new information is merged with existing data without overwriting or losing pre-existing fields not included in the update. This is very important.

### Document Creation and Update Timestamps

- Set `created` timestamp upon initial document creation.
- Update `updated` timestamp with the current time on each document update and `scraped_at` field to keep track of when the data was updated.

## Dictionary

```json
{
  "created": "Date the document was created in the database.",
  "updated": "Date the document was last updated in the database.",
  "base_name": "The base name or common name of the product, derived from product descriptions.",
  "base_category": "The primary category or categories the product belongs to, indicating its general classification.",
  "base_unit": "The base unit of measurement for the product, indicating how it is sold or measured (e.g., 'Pakke').",
  "base_price_unit": "The unit of measurement for the base price (e.g., 'M2' for per square meter).",
  "nobb_codes": "Array of NOBB codes, a unique identifier used in the construction industry to identify products.",
  "ean_codes": "Array of EAN codes, the global standard barcoding for product identification.",
  "base_images": [
    {
      "url": "URL to the product's image.",
      "type": "The type of image (e.g., 'PRODUCT' for main product images)."
    }
  ],
  "variants": [
    {
      "retailer": "The retailer or store selling the product. This is fixed value, Byggmakker.",
      "brand": "The brand of the product.",
      "url_product": "URL to the product page on the retailer's website. Derived from products_ids",
      "retail_unit": "The unit of measurement used by the retailer for this product.",
      "retail_price_unit": "The unit of measurement for the retail price (e.g., 'M2').",
      "ean_codes": "Array of EAN codes specific to this variant of the product.",
      "nobb_codes": "Array of NOBB codes specific to this variant of the product.",
      "categories": "Array of categories this variant belongs to.",
      "stores": [
        {
          "storeId": "Unique identifier for the store. Derived from store_info or Prices",
          "store_name": "Name of the store. Derived from store_info",
          "price": "Price of the product at this store.",
          "scraped_at": "Date and time when the price was scraped/updated."
        }
      ]
    }
  ]
}
```

# Scraping tool

This module was created in order to scrape data from a home improvement retailer from Norway.

## Usage

- Clone this repo to your local machine
- Create a python virtual environment
- Install necessary auxiliary libs

```
pip install -r requirements.txt
```

- Choose category level 1 and category level 2
  ![Tutorial image 1](images/tuto_image.jpg)
- Open 'main.py' file, instantiate a ScrapeData object and perform a .run() method on that object

```
from src.run import ScrapeData

if __name__ == '__main__':
    scraper = ScrapeData('trelast', 'terrassebord')
    scraper.run()
```

- Wait for the scraping proccess! Data scraped will be found on filepath: /data/category_level1/category_level2
- Logs can be found on filepath /logs/scraping.log

## Code logic

By instatiating a ScrapeData('category_l1', 'category_l2') object and applying a .run() on it, the following workflow is initiated:

1. ScrapingIds() object is instantiated in order to identify how many listing pages this category/subcategory contains and loop trough them in order to parse all individual 'eans'. A file is created on filepath /data/category_l1/category_l2/products_ids.json. Code can be found on /utils/scraping_ids.py
2. DescriptionEans() object is instantiated to fetch the detailed description of each individual 'ean' found on previous step. Data is stored on filepath /data/category_l1/category_l2/description/{ean}\_description.json. Code can be found on /src/get_description/scraping_description.py
3. AvailabilityEans() object is instantiated to fetch the detailed availability of each individual 'ean' found on first step. Data is stored on filepath /data/category_l1/category_l2/availability/{ean}\_availability.json. Code can be found on /src/get_availability/scraping_availability.py
4. PriceEans() object is instantiated to fetch the detailed prices of each individual 'ean' found on first step. Data is stored on filepath /data/category_l1/category_l2/prices/{ean}\_prices.json. Code can be found on /src/get_prices/scraping_prices.py
5. All errors and important milestones of the code is logged on /logs/scraping.log

# Inserting Product Data into MongoDB

## Running the python script
- Clone this repo to your local machine
- Create a python virtual environment
- Install necessary auxiliary libs

```
pip install -r requirements.txt
```

- Run the python script
```
python3 sync_mongodb.py
```

### Note: 
- The script expects a .env file with `MONGODB_URI` specified
- This script currently processes data from the `.json` files, creates documents, and inserts them into your collection. The current version *does not* handle merging data with existing documents in mongodb
