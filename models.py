from typing import List
from pydantic import BaseModel, Field
# from datetime import datetime

class Image(BaseModel):
    url: str = Field(..., description="URL to the product's image")
    type: str = Field(..., description="The type of image (e.g., 'PRODUCT' for main product images).")

class StoreInfo(BaseModel):
    id: str
    name: str


class ProductDescCategories(BaseModel):
    salesCategoryIdentifier: str
    name: str
    url: str

class NetContent(BaseModel):
    unit: str
    value: float

class Dimension(BaseModel):
    unit: str
    value: float
    unitCode: str
    unitLocalized: str

class GrossDimension(BaseModel):
    width: Dimension
    height: Dimension
    length: Dimension

class Measurements(BaseModel):
    netContent: NetContent
    grossDimensions: GrossDimension
    

class ProductDescription(BaseModel):
    ean: str
    id: str
    name: str
    brandName: str
    measurements: Measurements
    images: List[Image]
    categories: List[ProductDescCategories]
    relatedEans: List[str]


class Store(BaseModel):
    storeId: str = Field(
        ..., description="Unique identifier for the store.Derived from store_info or Prices")
    store_name: str = Field(..., description="The name of the store. Derived from store_info")
    price: float = Field(..., description="Price of the product at the store")
    scraped_at: str = Field( ..., description="Date and time when the price was scraped/updated")


class Variant(BaseModel):
    retailer: str = Field("Byggmakker", description="The retailer or store selling the product. This is fixed value, Byggmakker")
    brand: str = Field(..., description="The brand of the product")
    url_product: str = Field( ..., description="URL to the product page on the retailer's website. Derived from products_ids")
    retail_unit: str = Field( ..., description="The unit of measurement used by the retailer for this product")
    retail_price_unit: str = Field(..., description="The unit of measurement for the retail price (e.g., 'M2')")
    ean_codes: List[str] = Field( ..., description="Array of EAN codes specific to this variant of the product")
    nobb_codes: List[str] = Field( ..., description="Array of NOBB codes specific to this variant of the product")
    categories: List[str] = Field(..., description="Array of categories this variant belongs to.")
    stores: List[Store]

class ProductPrices(BaseModel):
      ean: str
      type: str
      basePrice: float
      salesUnitLocalized: str
      unitAmount: float
      campaignPrice: float | None = 0
      scales: List[str]
      comparisonPrice: float = 0.0
      comparisonPriceUnit: str
      comparisonPriceUnitLocalized: str
      displayCodePCU: int
      priceValidUntil: str
      qualifier: str
      price: float
      basePriceUnit: str
      basePriceUnitLocalized: str
      salesUnit: str
      vatPercentage: float
      campaignId: str
      campaignTag: str = ""
      storeId: str


class Product(BaseModel):
    created: str = Field(... ,description="Date the document was created in the database.")
    updated: str = Field( ..., description="Date the document was last updated in the database.")
    base_name: str = Field( ..., description="The base name or common name of the product, derived from product descriptions.")
    base_category: List[str] = Field( ..., description="The primary category or categories the product belongs to, indicating its general classification.")
    base_unit: str = Field(..., description="The base unit of measurement for the product, indicating how it is sold or measured (e.g., 'Pakke').")
    base_price_unit: str = Field( ..., description="The unit of measurement for the base price (e.g., 'M2' for per square meter).")
    nobb_codes: List[str] = Field( ..., description="Array of NOBB codes, a unique identifier used in the construction industry to identify products")
    ean_codes: List[str] = Field( ..., description="Array of EAN codes, the global standard barcoding for product identification.")
    base_images: List[Image]
    variants: List[Variant]

