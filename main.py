import json
import pymongo
from pymongo import MongoClient
from datetime import datetime
import os
import pandas as pd

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['construction_products']
collection = db['products']
# Use EAN codes as the primary identifier
# Check if the unique index already exists
collection.create_index([('ean_codes', 1)], unique=True)

#file directories
current_dir = os.getcwd()
DATA_DIRECTORY=os.path.join(current_dir, 'data')

target_files=['product_description.json','products_ids.json','product_prices.json']
file_names = [os.path.join(root, filename) for root, _, files in os.walk(DATA_DIRECTORY) for filename in files if filename in target_files]

store_info='store_info.json'
product_description=next((item for item in file_names if 'product_description' in item), None)
products_ids=next((item for item in file_names if 'products_ids' in item), None)
product_prices=next((item for item in file_names if 'product_prices' in item), None)

# Load data from JSON files
for eachfile in [store_info,product_description,products_ids,product_prices]:
    filevar=os.path.split(eachfile)[1].split('.')[0]
    with open(eachfile, encoding='utf-8') as f:
        vars()[filevar]= json.load(f)

#create and process dataframes
df_product_prices = [pd.json_normalize(sublist) for sublist in product_prices]
df_product_prices = [df.dropna(axis=1, how='all') for df in df_product_prices]
df_product_prices = pd.concat(df_product_prices, ignore_index=True)


df_store_info=pd.DataFrame(store_info)

df_product_prices_store_info=pd.merge(df_product_prices, df_store_info, left_on='storeId', right_on='id', how='left')

df_products_ids=pd.DataFrame(products_ids)

df_product_prices_store_info_product_link=pd.merge(df_product_prices_store_info, df_products_ids, left_on='ean', right_on='id', how='left')

df_product_description=pd.json_normalize(product_description)

df_product_description['categories']=df_product_description['categories'].apply(lambda x: [category['name'] for category in x])      
df_product_description['images']=df_product_description['images'].apply(lambda x: [{'url':image['url'],'type':'product'} for image in x if image['type']=='PRODUCT']) 

df_product_prices_store_info_product_link_description=pd.merge(df_product_prices_store_info_product_link, df_product_description, left_on='ean', right_on='ean', how='left')

# group dataframe by ean

grouped =df_product_prices_store_info_product_link_description.groupby('ean')
grouped_dict=[]
for ean, group_df in grouped:
    grouped_dict.append(group_df.to_dict(orient='records'))

#process data
full_data=[]
for each_dict in grouped_dict:
    base_name=each_dict[0]['name_y']
    base_category=each_dict[0]['categories']
    base_unit=each_dict[0]['salesUnitLocalized']
    base_price_unit=each_dict[0]['salesUnit']
    ean_codes=[each_dict[0]['ean']]
    base_images=each_dict[0]['images']
    #process variant
    variants=[]
    retailer='Byggmakker'
    brand=each_dict[0]['brandName']
    url_product=each_dict[0]['link']
    retail_unit=each_dict[0]['salesUnitLocalized']
    retail_price_unit=each_dict[0]['salesUnit']
    ean_codes=[each_dict[0]['ean']]
    categories=each_dict[0]['categories']
    #process stores
    stores=[]
    for each_store in each_dict:
        storeId=each_store['storeId']
        store_name=each_store['name_x']
        price=each_store['price']
        scraped_at=datetime.now()
        store_dict={'storeId':storeId,'store_name':store_name,'price':price,'scraped_at':scraped_at}
        stores.append(store_dict)
    variant_data={'retailer':retailer,'brand':brand,'url_product':url_product,'retail_unit':retail_unit,
                  'retail_price_unit':retail_price_unit,'ean_codes':ean_codes,'categories':categories,'stores':stores}
    variants.append(variant_data)
    # final product data
    product_data={'base_name':base_name,'base_category':base_category,'base_unit':base_unit,'base_price_unit':base_price_unit
                   ,'ean_codes':ean_codes,'base_images':base_images,'variants':variants }
    full_data.append(product_data)
    

# send data to mongodb
for each_data in full_data:
    ean_codes = each_data['ean_codes']
    # Create or update document in MongoDB
    try:
        each_data['created']=datetime.now()
        each_data['updated']=datetime.now()
        collection.insert_one(each_data)
    except pymongo.errors.DuplicateKeyError:
        del each_data['_id']    # Exclude _id field from update
        del each_data['created']
        each_data['updated']=datetime.now()
        collection.update_one({"ean_codes":ean_codes}, {'$set': each_data})
