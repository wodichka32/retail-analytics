import argparse
import json
from datetime import date, datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.transformations.olist.silver import UNKNOWN, region_of
from src.utils.config import GOLD_DIR, SILVER_DIR
from src.utils.quality import column_sum, duplicate_keys, null_keys, orphans
from src.utils.spark import get_spark

SILVER = SILVER_DIR / "olist"
CORE = GOLD_DIR / "olist" / "core"
VALIDATION_PATH = CORE / "_validation.json"

AMOUNT = T.DecimalType(14, 2)
DATE_START = date(2016, 9, 1)
DATE_END = date(2018, 12, 31)
ANALYSIS_START = date(2017, 1, 1)
ANALYSIS_END = date(2018, 8, 31)

PRIMARY_KEYS = {
    "dim_date": ["date_key"],
    "dim_geography": ["geo_key"],
    "dim_product": ["product_key"],
    "dim_seller": ["seller_key"],
    "fact_orders": ["order_id"],
    "dim_customer": ["customer_key"],
    "fact_order_items": ["order_id", "order_item_id"],
    "fact_payments": ["order_id", "payment_sequential"],
    "fact_reviews": ["review_id", "order_id"],
}

FOREIGN_KEYS = [
    ("fact_orders", "customer_key", "dim_customer", "customer_key"),
    ("fact_orders", "purchase_date_key", "dim_date", "date_key"),
    ("fact_orders", "delivery_geo_key", "dim_geography", "geo_key"),
    ("fact_order_items", "order_id", "fact_orders", "order_id"),
    ("fact_order_items", "product_key", "dim_product", "product_key"),
    ("fact_order_items", "seller_key", "dim_seller", "seller_key"),
    ("fact_order_items", "customer_key", "dim_customer", "customer_key"),
    ("fact_order_items", "purchase_date_key", "dim_date", "date_key"),
    ("fact_order_items", "delivery_geo_key", "dim_geography", "geo_key"),
    ("fact_payments", "order_id", "fact_orders", "order_id"),
    ("fact_payments", "customer_key", "dim_customer", "customer_key"),
    ("fact_payments", "purchase_date_key", "dim_date", "date_key"),
    ("fact_reviews", "order_id", "fact_orders", "order_id"),
    ("fact_reviews", "customer_key", "dim_customer", "customer_key"),
    ("fact_reviews", "review_creation_date_key", "dim_date", "date_key"),
    ("dim_customer", "current_geo_key", "dim_geography", "geo_key"),
    ("dim_seller", "geo_key", "dim_geography", "geo_key"),
]


def date_key(c: Column) -> Column:
    return F.date_format(c, "yyyyMMdd").cast("int")


def hours_between(start: Column, end: Column) -> Column:
    return (F.unix_timestamp(end.cast("timestamp")) - F.unix_timestamp(start.cast("timestamp"))) / 3600.0


def money_sum(col: str) -> Column:
    return F.sum(col).cast(AMOUNT)


def zero_amount() -> Column:
    return F.lit(0).cast(AMOUNT)


def with_geo_key(df: DataFrame, zip_col: str, out_col: str, dim_geography: DataFrame) -> DataFrame:
    keys = dim_geography.filter(~F.col("is_unknown")).select(F.col("geo_key").alias("_geo"))
    return (
        df.join(keys, df[zip_col] == keys["_geo"], "left")
        .withColumn(out_col, F.coalesce(F.col("_geo"), F.lit(UNKNOWN)))
        .drop("_geo")
    )


def build_dim_date(spark: SparkSession) -> DataFrame:
    days = spark.sql(
        f"select explode(sequence(date'{DATE_START}', date'{DATE_END}', interval 1 day)) as date")
    dow = (F.dayofweek("date") + 5) % 7 + 1
    return days.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("iso_week"),
        dow.alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        (dow >= 6).alias("is_weekend"),
        F.col("date").between(F.lit(ANALYSIS_START), F.lit(ANALYSIS_END)).alias("is_in_analysis_window"),
    )


def build_dim_geography(geolocation: DataFrame, spark: SparkSession) -> DataFrame:
    known = geolocation.select(
        F.col("zip_code_prefix").alias("geo_key"), "city", "state", "region", "lat", "lng",
        F.lit(False).alias("is_unknown"),
    )
    unknown = spark.range(1).select(
        F.lit(UNKNOWN).alias("geo_key"), F.lit(UNKNOWN).alias("city"), F.lit(UNKNOWN).alias("state"),
        F.lit(UNKNOWN).alias("region"), F.lit(None).cast("double").alias("lat"),
        F.lit(None).cast("double").alias("lng"), F.lit(True).alias("is_unknown"),
    )
    return known.unionByName(unknown)


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        F.col("product_id").alias("product_key"),
        F.col("product_category_name").alias("category_pt"),
        F.col("product_category_name_english").alias("category_en"),
        F.col("product_name_length").alias("name_length"),
        F.col("product_description_length").alias("description_length"),
        F.col("product_photos_qty").alias("photos_qty"),
        F.col("product_weight_g").alias("weight_g"),
        F.col("product_length_cm").alias("length_cm"),
        F.col("product_height_cm").alias("height_cm"),
        F.col("product_width_cm").alias("width_cm"),
        (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")).alias("volume_cm3"),
        "is_category_missing",
        "has_invalid_measurements",
    )


def build_dim_seller(sellers: DataFrame, dim_geography: DataFrame) -> DataFrame:
    df = with_geo_key(sellers, "seller_zip_code_prefix", "geo_key", dim_geography)
    return df.select(
        F.col("seller_id").alias("seller_key"),
        F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        "geo_key",
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
        region_of(F.col("seller_state")).alias("region"),
    )


def order_item_totals(items: DataFrame) -> DataFrame:
    return items.groupBy("order_id").agg(
        F.count("*").alias("items_count"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.countDistinct("seller_id").alias("distinct_sellers"),
        money_sum("price").alias("items_value"),
        money_sum("freight_value").alias("freight_value"),
    )


def order_payment_totals(payments: DataFrame) -> DataFrame:
    totals = payments.groupBy("order_id").agg(
        money_sum("payment_value").alias("payment_value"),
        F.count("*").alias("payments_count"),
        F.max("payment_installments").alias("max_installments"),
    )
    by_type = payments.groupBy("order_id", "payment_type").agg(F.sum("payment_value").alias("type_value"))
    w = Window.partitionBy("order_id").orderBy(F.desc("type_value"), F.asc("payment_type"))
    main_type = (
        by_type.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
        .select("order_id", F.col("payment_type").alias("main_payment_type"))
    )
    return totals.join(main_type, "order_id")


def build_fact_orders(orders: DataFrame, customers: DataFrame, items: DataFrame, payments: DataFrame,
                      reviews: DataFrame, dim_geography: DataFrame) -> DataFrame:
    customer_cols = customers.select(
        "customer_id",
        F.col("customer_unique_id").alias("customer_key"),
        F.col("customer_zip_code_prefix").alias("delivery_zip_code_prefix"),
        F.col("customer_city").alias("delivery_city"),
        F.col("customer_state").alias("delivery_state"),
    )
    latest_review = reviews.filter("is_latest_for_order").select("order_id", "review_score")

    df = (
        orders.join(customer_cols, "customer_id", "left")
        .join(order_item_totals(items), "order_id", "left")
        .join(order_payment_totals(payments), "order_id", "left")
        .join(latest_review, "order_id", "left")
    )
    df = with_geo_key(df, "delivery_zip_code_prefix", "delivery_geo_key", dim_geography)

    purchase = F.col("order_purchase_timestamp")
    delivered = F.col("order_delivered_customer_date")
    estimated = F.col("order_estimated_delivery_date")
    consistent = ~F.col("is_timestamp_inconsistent")
    valid = F.col("is_valid_order")
    by_customer = Window.partitionBy("customer_key", "is_valid_order")

    return df.select(
        "order_id",
        "customer_id",
        "customer_key",
        date_key(purchase).alias("purchase_date_key"),
        "delivery_geo_key",
        "delivery_zip_code_prefix",
        "delivery_city",
        "delivery_state",
        region_of(F.col("delivery_state")).alias("delivery_region"),
        "order_status",
        "is_valid_order",
        purchase.alias("purchase_ts"),
        F.col("order_approved_at").alias("approved_ts"),
        F.col("order_delivered_carrier_date").alias("delivered_carrier_ts"),
        delivered.alias("delivered_customer_ts"),
        estimated.alias("estimated_delivery_date"),
        F.coalesce("items_count", F.lit(0)).alias("items_count"),
        F.coalesce("distinct_products", F.lit(0)).alias("distinct_products"),
        F.coalesce("distinct_sellers", F.lit(0)).alias("distinct_sellers"),
        F.coalesce("items_value", zero_amount()).alias("items_value"),
        F.coalesce("freight_value", zero_amount()).alias("freight_value"),
        (F.coalesce("items_value", zero_amount()) + F.coalesce("freight_value", zero_amount()))
        .cast(AMOUNT).alias("order_value"),
        F.coalesce("payment_value", zero_amount()).alias("payment_value"),
        F.coalesce("payments_count", F.lit(0)).alias("payments_count"),
        "main_payment_type",
        "max_installments",
        "review_score",
        F.when(consistent, hours_between(purchase, F.col("order_approved_at"))).alias("approval_hours"),
        F.when(consistent & delivered.isNotNull(), hours_between(purchase, delivered) / 24.0)
        .alias("delivery_days"),
        F.when(delivered.isNotNull(), F.datediff(estimated, F.to_date(delivered)))
        .alias("delivery_vs_estimate_days"),
        F.when(delivered.isNotNull(), F.to_date(delivered) > estimated).alias("is_late"),
        F.when(valid, F.row_number().over(by_customer.orderBy(purchase, "order_id")))
        .alias("customer_order_seq"),
        F.when(valid, F.dense_rank().over(by_customer.orderBy(purchase))).alias("customer_purchase_seq"),
        F.col("items_count").isNotNull().alias("has_items"),
        F.col("payments_count").isNotNull().alias("has_payment"),
        F.col("review_score").isNotNull().alias("has_review"),
        "is_timestamp_inconsistent",
        "has_status_date_conflict",
    )


def build_dim_customer(fact_orders: DataFrame) -> DataFrame:
    latest = Window.partitionBy("customer_key").orderBy(F.desc("purchase_ts"), F.desc("order_id"))
    address = (
        fact_orders.withColumn("rn", F.row_number().over(latest)).filter("rn = 1")
        .select(
            "customer_key",
            F.col("delivery_zip_code_prefix").alias("current_zip_code_prefix"),
            F.col("delivery_geo_key").alias("current_geo_key"),
            F.col("delivery_city").alias("current_city"),
            F.col("delivery_state").alias("current_state"),
            F.col("delivery_region").alias("current_region"),
        )
    )
    valid = F.col("is_valid_order")
    valid_date = F.when(valid, F.to_date("purchase_ts"))
    stats = fact_orders.groupBy("customer_key").agg(
        F.count("*").alias("total_orders_count"),
        F.sum(valid.cast("int")).alias("orders_count"),
        F.coalesce(F.max("customer_purchase_seq"), F.lit(0)).alias("purchase_occasions_count"),
        F.min(valid_date).alias("first_purchase_date"),
        F.max(valid_date).alias("last_purchase_date"),
    )
    return (
        address.join(stats, "customer_key")
        .withColumn("cohort_month", F.date_format("first_purchase_date", "yyyy-MM"))
        .withColumn("is_repeat_customer", F.col("purchase_occasions_count") >= 2)
    )


def order_attributes(fact_orders: DataFrame) -> DataFrame:
    return fact_orders.select("order_id", "customer_key", "purchase_date_key", "delivery_geo_key",
                              "delivery_state", "is_valid_order")


def build_fact_order_items(items: DataFrame, fact_orders: DataFrame) -> DataFrame:
    return items.join(order_attributes(fact_orders), "order_id").select(
        "order_id",
        "order_item_id",
        F.col("product_id").alias("product_key"),
        F.col("seller_id").alias("seller_key"),
        "customer_key",
        "purchase_date_key",
        "delivery_geo_key",
        "delivery_state",
        F.col("shipping_limit_date").alias("shipping_limit_ts"),
        "price",
        "freight_value",
        "is_valid_order",
        "is_shipping_limit_outlier",
    )


def build_fact_payments(payments: DataFrame, fact_orders: DataFrame) -> DataFrame:
    return payments.join(order_attributes(fact_orders), "order_id").select(
        "order_id", "payment_sequential", "customer_key", "purchase_date_key", "payment_type",
        "payment_installments", "payment_value", "is_valid_order",
    )


def build_fact_reviews(reviews: DataFrame, fact_orders: DataFrame) -> DataFrame:
    return reviews.join(fact_orders.select("order_id", "customer_key"), "order_id").select(
        "review_id",
        "order_id",
        "customer_key",
        date_key(F.col("review_creation_date")).alias("review_creation_date_key"),
        "review_score",
        "has_title",
        "has_comment",
        hours_between(F.col("review_creation_date"), F.col("review_answer_timestamp")).alias("answer_hours"),
        "is_latest_for_order",
    )


def validate(silver: dict[str, DataFrame], gold: dict[str, DataFrame]) -> dict:
    tables = {}
    for name, keys in PRIMARY_KEYS.items():
        df = gold[name]
        tables[name] = {
            "rows": df.count(),
            "duplicate_keys": duplicate_keys(df, keys),
            "null_keys": null_keys(df, keys),
        }

    foreign_keys = []
    for child, col, parent, parent_col in FOREIGN_KEYS:
        foreign_keys.append({
            "child": f"{child}.{col}",
            "parent": f"{parent}.{parent_col}",
            "nulls": gold[child].filter(F.col(col).isNull()).count(),
            "orphans": orphans(gold[child], col, gold[parent], parent_col),
        })

    fo, foi, fp = gold["fact_orders"], gold["fact_order_items"], gold["fact_payments"]
    reconciliation = [
        ("fact_orders rows = silver orders", tables["fact_orders"]["rows"], silver["orders"].count()),
        ("fact_order_items rows = silver order_items", tables["fact_order_items"]["rows"],
         silver["order_items"].count()),
        ("fact_payments rows = silver payments", tables["fact_payments"]["rows"], silver["payments"].count()),
        ("fact_reviews rows = silver reviews", tables["fact_reviews"]["rows"], silver["reviews"].count()),
        ("dim_customer rows = distinct customer_unique_id", tables["dim_customer"]["rows"],
         silver["customers"].select("customer_unique_id").distinct().count()),
        ("sum(items.price) = sum(orders.items_value)", column_sum(foi, "price"), column_sum(fo, "items_value")),
        ("sum(items.freight) = sum(orders.freight_value)", column_sum(foi, "freight_value"),
         column_sum(fo, "freight_value")),
        ("sum(payments.value) = sum(orders.payment_value)", column_sum(fp, "payment_value"),
         column_sum(fo, "payment_value")),
        ("latest reviews = orders with review", gold["fact_reviews"].filter("is_latest_for_order").count(),
         fo.filter("has_review").count()),
    ]
    checks = [{"check": c, "left": str(a), "right": str(b), "ok": a == b} for c, a, b in reconciliation]

    errors = [f"{n}: primary key violated" for n, t in tables.items() if t["duplicate_keys"] or t["null_keys"]]
    errors += [f"{fk['child']} -> {fk['parent']}: nulls={fk['nulls']} orphans={fk['orphans']}"
               for fk in foreign_keys if fk["nulls"] or fk["orphans"]]
    errors += [f"{c['check']}: {c['left']} != {c['right']}" for c in checks if not c["ok"]]
    return {"tables": tables, "foreign_keys": foreign_keys, "reconciliation": checks, "errors": errors}


def write_table(df: DataFrame, name: str, spark: SparkSession) -> DataFrame:
    path = str(CORE / name)
    df.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def run() -> dict:
    spark = get_spark("gold-core-olist")
    started = datetime.now(timezone.utc).replace(microsecond=0)
    CORE.mkdir(parents=True, exist_ok=True)
    silver_names = ["customers", "sellers", "products", "orders", "order_items", "payments", "reviews",
                    "geolocation"]
    silver = {n: spark.read.parquet(str(SILVER / n)) for n in silver_names}

    try:
        gold: dict[str, DataFrame] = {}
        steps = [
            ("dim_date", lambda: build_dim_date(spark)),
            ("dim_geography", lambda: build_dim_geography(silver["geolocation"], spark)),
            ("dim_product", lambda: build_dim_product(silver["products"])),
            ("dim_seller", lambda: build_dim_seller(silver["sellers"], gold["dim_geography"])),
            ("fact_orders", lambda: build_fact_orders(
                silver["orders"], silver["customers"], silver["order_items"], silver["payments"],
                silver["reviews"], gold["dim_geography"])),
            ("dim_customer", lambda: build_dim_customer(gold["fact_orders"])),
            ("fact_order_items", lambda: build_fact_order_items(silver["order_items"], gold["fact_orders"])),
            ("fact_payments", lambda: build_fact_payments(silver["payments"], gold["fact_orders"])),
            ("fact_reviews", lambda: build_fact_reviews(silver["reviews"], gold["fact_orders"])),
        ]
        for name, build in steps:
            gold[name] = write_table(build(), name, spark)
            print(f"built {name}")
        report = validate(silver, gold)
    finally:
        spark.stop()

    report = {"source": "olist", "started_at": started.isoformat(),
              "status": "OK" if not report["errors"] else "FAILED", **report}
    VALIDATION_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    for name, t in report["tables"].items():
        print(f"{name:18} rows={t['rows']:>9,}  dup_keys={t['duplicate_keys']}  null_keys={t['null_keys']}")
    bad_fk = [fk for fk in report["foreign_keys"] if fk["nulls"] or fk["orphans"]]
    print(f"foreign keys: {len(report['foreign_keys'])} checked, {len(bad_fk)} failed")
    for c in report["reconciliation"]:
        print(f"{'OK  ' if c['ok'] else 'FAIL'} {c['check']}: {c['left']} / {c['right']}")
    if report["errors"]:
        raise SystemExit("gold core olist failed:\n" + "\n".join(report["errors"]))
    print(f"gold core olist OK, report {VALIDATION_PATH}")
    return report


def main() -> None:
    argparse.ArgumentParser(description="Build the Olist Gold star schema from Silver").parse_args()
    run()


if __name__ == "__main__":
    main()
