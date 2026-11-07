from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
DOCS_DIR = PROJECT_ROOT / "docs"

KAGGLE_DATASETS = {
    "olist": "olistbr/brazilian-ecommerce",
    "retailrocket": "retailrocket/ecommerce-dataset",
}

OLIST_TABLES = {
    "customers": {
        "file": "olist_customers_dataset.csv",
        "pk": ["customer_id"],
        "fk": {},
    },
    "orders": {
        "file": "olist_orders_dataset.csv",
        "pk": ["order_id"],
        "fk": {"customer_id": ("customers", "customer_id")},
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "pk": ["order_id", "order_item_id"],
        "fk": {
            "order_id": ("orders", "order_id"),
            "product_id": ("products", "product_id"),
            "seller_id": ("sellers", "seller_id"),
        },
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "pk": ["product_id"],
        "fk": {
            "product_category_name": ("category_translation", "product_category_name"),
        },
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "pk": ["seller_id"],
        "fk": {},
    },
    "payments": {
        "file": "olist_order_payments_dataset.csv",
        "pk": ["order_id", "payment_sequential"],
        "fk": {"order_id": ("orders", "order_id")},
    },
    "reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "pk": ["review_id"],
        "fk": {"order_id": ("orders", "order_id")},
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "pk": [],
        "fk": {},
    },
    "category_translation": {
        "file": "product_category_name_translation.csv",
        "pk": ["product_category_name"],
        "fk": {},
    },
}

OLIST_STRING_COLUMNS = {
    "customer_zip_code_prefix",
    "seller_zip_code_prefix",
    "geolocation_zip_code_prefix",
}

OLIST_DATETIME_COLUMNS = {
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "shipping_limit_date",
    "review_creation_date",
    "review_answer_timestamp",
}
