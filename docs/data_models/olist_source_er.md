# Olist — ER-диаграмма источника

Связи между таблицами в исходных CSV, проверенные скриптом `src/exploration/profile_olist.py`.
Кардинальности — наблюдаемые значения, а не гарантии из документации.

```mermaid
erDiagram
    CUSTOMERS ||--|| ORDERS : "customer_id (1:1)"
    ORDERS ||--o{ ORDER_ITEMS : "order_id (0..21)"
    ORDERS ||--o{ PAYMENTS : "order_id (0..29)"
    ORDERS ||--o{ REVIEWS : "order_id (0..3)"
    PRODUCTS ||--o{ ORDER_ITEMS : "product_id"
    SELLERS ||--o{ ORDER_ITEMS : "seller_id"
    CATEGORY_TRANSLATION |o--o{ PRODUCTS : "product_category_name"
    GEOLOCATION }o..o{ CUSTOMERS : "zip_code_prefix (многие ко многим)"
    GEOLOCATION }o..o{ SELLERS : "zip_code_prefix (многие ко многим)"

    CUSTOMERS {
        string customer_id PK
        string customer_unique_id "реальный человек"
        string customer_zip_code_prefix
        string customer_city
        string customer_state
    }
    ORDERS {
        string order_id PK
        string customer_id FK
        string order_status
        timestamp order_purchase_timestamp
        timestamp order_approved_at
        timestamp order_delivered_carrier_date
        timestamp order_delivered_customer_date
        date order_estimated_delivery_date
    }
    ORDER_ITEMS {
        string order_id PK,FK
        int order_item_id PK
        string product_id FK
        string seller_id FK
        timestamp shipping_limit_date
        decimal price
        decimal freight_value
    }
    PAYMENTS {
        string order_id PK,FK
        int payment_sequential PK
        string payment_type
        int payment_installments
        decimal payment_value
    }
    REVIEWS {
        string review_id PK
        string order_id PK,FK
        int review_score
        string review_comment_title
        string review_comment_message
        date review_creation_date
        timestamp review_answer_timestamp
    }
    PRODUCTS {
        string product_id PK
        string product_category_name FK
        int product_weight_g
        int product_length_cm
        int product_height_cm
        int product_width_cm
    }
    SELLERS {
        string seller_id PK
        string seller_zip_code_prefix
        string seller_city
        string seller_state
    }
    GEOLOCATION {
        string geolocation_zip_code_prefix
        double geolocation_lat
        double geolocation_lng
        string geolocation_city
        string geolocation_state
    }
    CATEGORY_TRANSLATION {
        string product_category_name PK
        string product_category_name_english
    }
```

## Ловушки размножения строк

Если в одном запросе соединить `orders` больше чем с одной из таблиц `order_items`, `payments`, `reviews`,
строки размножаются: заказ с 3 товарами и 2 платежами превращается в 6 строк, и `sum(price)` удваивается.
Поэтому в целевой модели у каждой гранулярности своя таблица фактов (`fact_order_items`, `fact_payments`,
`fact_reviews`, `fact_orders`), а связываются они только через уже агрегированные показатели заказа.

У `geolocation` нет ключа: перед любым соединением её нужно свести к одной строке на почтовый индекс.
