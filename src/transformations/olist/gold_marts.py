import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.transformations.olist.gold_core import AMOUNT, ANALYSIS_END, ANALYSIS_START, CORE, zero_amount
from src.utils.config import GOLD_DIR
from src.utils.quality import duplicate_keys
from src.utils.spark import get_spark

MARTS = GOLD_DIR / "olist" / "marts"
MANIFEST_PATH = MARTS / "_manifest.json"

ANALYSIS_MONTHS = (ANALYSIS_START.strftime("%Y-%m"), ANALYSIS_END.strftime("%Y-%m"))

MART_KEYS = {
    "gold_sales_daily": ["date_key"],
    "gold_sales_category": ["year_month", "category_en"],
    "gold_customer_metrics": ["customer_key"],
    "gold_product_metrics": ["product_key"],
    "gold_payment_metrics": ["year_month", "payment_type"],
    "gold_logistics_metrics": ["year_month", "delivery_state"],
    "gold_rfm_segments": ["customer_key"],
}


def ratio(num: Column, den: Column, scale: int = 4) -> Column:
    return F.when(den > 0, F.round(num.cast("double") / den, scale))


def amount_ratio(num: Column, den: Column) -> Column:
    return F.when(den > 0, (num / den).cast(AMOUNT))


def month_in_window(c: Column) -> Column:
    return c.between(*ANALYSIS_MONTHS)


def with_month(df: DataFrame, dim_date: DataFrame) -> DataFrame:
    months = dim_date.select(F.col("date_key").alias("purchase_date_key"), "year_month")
    return df.join(months, "purchase_date_key")


def build_sales_daily(fact_orders: DataFrame, dim_date: DataFrame) -> DataFrame:
    valid = fact_orders.filter("is_valid_order")
    daily = valid.groupBy(F.col("purchase_date_key").alias("date_key")).agg(
        F.count("*").alias("orders"),
        F.countDistinct("customer_key").alias("customers"),
        F.sum("items_count").alias("items"),
        F.sum("items_value").cast(AMOUNT).alias("revenue"),
        F.sum("freight_value").cast(AMOUNT).alias("freight"),
    )
    bounds = fact_orders.agg(F.min("purchase_date_key"), F.max("purchase_date_key")).first()
    days = dim_date.filter(F.col("date_key").between(bounds[0], bounds[1])).select(
        "date_key", "date", "year_month", "day_of_week", "is_weekend", "is_in_analysis_window")
    return (
        days.join(daily, "date_key", "left")
        .fillna(0, ["orders", "customers", "items"])
        .withColumn("revenue", F.coalesce("revenue", zero_amount()))
        .withColumn("freight", F.coalesce("freight", zero_amount()))
        .withColumn("aov", amount_ratio(F.col("revenue"), F.col("orders")))
    )


def build_sales_category(fact_order_items: DataFrame, dim_product: DataFrame, dim_date: DataFrame) -> DataFrame:
    items = with_month(fact_order_items.filter("is_valid_order"), dim_date).join(
        dim_product.select(F.col("product_key"), "category_en"), "product_key")
    by_month = Window.partitionBy("year_month")
    return (
        items.groupBy("year_month", "category_en").agg(
            F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("items"),
            F.countDistinct("product_key").alias("products"),
            F.sum("price").cast(AMOUNT).alias("revenue"),
            F.sum("freight_value").cast(AMOUNT).alias("freight"),
        )
        .withColumn("avg_item_price", amount_ratio(F.col("revenue"), F.col("items")))
        .withColumn("revenue_share", ratio(F.col("revenue"), F.sum("revenue").over(by_month), 6))
        .withColumn("revenue_rank", F.dense_rank().over(by_month.orderBy(F.desc("revenue"))))
        .withColumn("is_in_analysis_window", month_in_window(F.col("year_month")))
    )


def build_customer_metrics(fact_orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    valid = fact_orders.filter("is_valid_order")
    stats = valid.groupBy("customer_key").agg(
        F.sum("items_value").cast(AMOUNT).alias("revenue"),
        F.sum("freight_value").cast(AMOUNT).alias("freight"),
        F.sum("items_count").alias("items"),
        F.round(F.avg("review_score"), 3).alias("avg_review_score"),
        F.round(F.avg(F.col("is_late").cast("int")), 4).alias("late_order_share"),
        F.countDistinct("main_payment_type").alias("payment_types_used"),
    )
    span = F.datediff("last_purchase_date", "first_purchase_date")
    return (
        dim_customer.filter("orders_count > 0").join(stats, "customer_key")
        .select(
            "customer_key", "current_state", "current_region", "cohort_month",
            "first_purchase_date", "last_purchase_date", "orders_count", "purchase_occasions_count",
            "is_repeat_customer", "items", "revenue", "freight",
            amount_ratio(F.col("revenue"), F.col("orders_count")).alias("aov"),
            span.alias("customer_lifespan_days"),
            F.when(F.col("purchase_occasions_count") > 1,
                   F.round(span / (F.col("purchase_occasions_count") - 1), 1)).alias("avg_days_between_purchases"),
            "avg_review_score", "late_order_share", "payment_types_used",
        )
    )


def build_product_metrics(fact_order_items: DataFrame, fact_orders: DataFrame, dim_product: DataFrame) -> DataFrame:
    items = fact_order_items.filter("is_valid_order")
    sales = items.groupBy("product_key").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("units"),
        F.countDistinct("seller_key").alias("sellers"),
        F.countDistinct("customer_key").alias("customers"),
        F.sum("price").cast(AMOUNT).alias("revenue"),
        F.sum("freight_value").cast(AMOUNT).alias("freight"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.to_date(F.min("purchase_date_key").cast("string"), "yyyyMMdd").alias("first_sale_date"),
        F.to_date(F.max("purchase_date_key").cast("string"), "yyyyMMdd").alias("last_sale_date"),
    )
    reviews = (
        items.select("product_key", "order_id").distinct()
        .join(fact_orders.select("order_id", "review_score"), "order_id")
        .groupBy("product_key").agg(
            F.round(F.avg("review_score"), 3).alias("avg_review_score"),
            F.count("review_score").alias("reviewed_orders"),
        )
    )
    total = Window.partitionBy()
    return (
        sales.join(reviews, "product_key", "left")
        .join(dim_product.select("product_key", "category_en", "weight_g", "volume_cm3", "photos_qty"),
              "product_key")
        .withColumn("revenue_share", ratio(F.col("revenue"), F.sum("revenue").over(total), 8))
        .withColumn("revenue_rank", F.dense_rank().over(total.orderBy(F.desc("revenue"))))
        .withColumn("units_rank", F.dense_rank().over(total.orderBy(F.desc("units"))))
    )


def build_payment_metrics(fact_payments: DataFrame, dim_date: DataFrame) -> DataFrame:
    payments = with_month(fact_payments.filter("is_valid_order"), dim_date)
    by_month = Window.partitionBy("year_month")
    return (
        payments.groupBy("year_month", "payment_type").agg(
            F.count("*").alias("payments"),
            F.countDistinct("order_id").alias("orders"),
            F.sum("payment_value").cast(AMOUNT).alias("payment_value"),
            F.round(F.avg("payment_value"), 2).alias("avg_payment_value"),
            F.round(F.avg("payment_installments"), 2).alias("avg_installments"),
            F.sum((F.col("payment_installments") > 1).cast("int")).alias("installment_payments"),
        )
        .withColumn("value_share", ratio(F.col("payment_value"), F.sum("payment_value").over(by_month), 6))
        .withColumn("is_in_analysis_window", month_in_window(F.col("year_month")))
    )


def build_logistics_metrics(fact_orders: DataFrame, dim_date: DataFrame) -> DataFrame:
    delivered = with_month(
        fact_orders.filter("is_valid_order and delivery_days is not null"), dim_date)
    return (
        delivered.groupBy("year_month", "delivery_state", "delivery_region").agg(
            F.count("*").alias("delivered_orders"),
            F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
            F.round(F.median("delivery_days"), 2).alias("median_delivery_days"),
            F.round(F.percentile_approx("delivery_days", 0.9), 2).alias("p90_delivery_days"),
            F.sum(F.col("is_late").cast("int")).alias("late_orders"),
            F.round(F.avg("delivery_vs_estimate_days"), 2).alias("avg_days_before_estimate"),
            F.round(F.avg("review_score"), 3).alias("avg_review_score"),
            F.round(F.avg("freight_value"), 2).alias("avg_freight"),
        )
        .withColumn("late_rate", ratio(F.col("late_orders"), F.col("delivered_orders")))
        .withColumn("is_in_analysis_window", month_in_window(F.col("year_month")))
    )


def rfm_segment(r: Column, f: Column, m: Column) -> Column:
    return (
        F.when((f >= 2) & (r >= 4), "champions")
        .when((f >= 2) & (r == 3), "loyal")
        .when(f >= 2, "at_risk")
        .when((r >= 4) & (m >= 4), "new_high_value")
        .when(r >= 4, "new")
        .when(r == 3, "needs_attention")
        .when(m >= 4, "hibernating_high_value")
        .otherwise("hibernating")
    )


def build_rfm_segments(fact_orders: DataFrame) -> DataFrame:
    valid = fact_orders.filter("is_valid_order")
    reference = valid.agg(F.date_add(F.max(F.to_date("purchase_ts")), 1)).first()[0]
    base = valid.groupBy("customer_key").agg(
        F.max(F.to_date("purchase_ts")).alias("last_purchase_date"),
        F.max("customer_purchase_seq").alias("frequency"),
        F.sum("items_value").cast(AMOUNT).alias("monetary"),
    ).withColumn("recency_days", F.datediff(F.lit(reference), "last_purchase_date"))

    everyone = Window.partitionBy()
    r = 6 - F.ntile(5).over(everyone.orderBy("recency_days", "customer_key"))
    m = F.ntile(5).over(everyone.orderBy("monetary", "customer_key"))
    f = F.least(F.col("frequency"), F.lit(5))
    return (
        base.withColumn("r_score", r)
        .withColumn("f_score", f)
        .withColumn("m_score", m)
        .withColumn("rfm_code", F.concat_ws("", "r_score", "f_score", "m_score"))
        .withColumn("segment", rfm_segment(F.col("r_score"), F.col("f_score"), F.col("m_score")))
        .withColumn("reference_date", F.lit(reference))
    )


def write_mart(df: DataFrame, name: str, spark: SparkSession) -> DataFrame:
    path = str(MARTS / name)
    df.coalesce(1).write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def run() -> dict:
    spark = get_spark("gold-marts-olist")
    started = datetime.now(timezone.utc).replace(microsecond=0)
    MARTS.mkdir(parents=True, exist_ok=True)
    core = {n: spark.read.parquet(str(CORE / n)) for n in
            ["dim_date", "dim_product", "dim_customer", "fact_orders", "fact_order_items", "fact_payments"]}
    fo, foi = core["fact_orders"], core["fact_order_items"]

    try:
        steps = {
            "gold_sales_daily": lambda: build_sales_daily(fo, core["dim_date"]),
            "gold_sales_category": lambda: build_sales_category(foi, core["dim_product"], core["dim_date"]),
            "gold_customer_metrics": lambda: build_customer_metrics(fo, core["dim_customer"]),
            "gold_product_metrics": lambda: build_product_metrics(foi, fo, core["dim_product"]),
            "gold_payment_metrics": lambda: build_payment_metrics(core["fact_payments"], core["dim_date"]),
            "gold_logistics_metrics": lambda: build_logistics_metrics(fo, core["dim_date"]),
            "gold_rfm_segments": lambda: build_rfm_segments(fo),
        }
        marts = {}
        for name, build in steps.items():
            out = write_mart(build(), name, spark)
            marts[name] = {"rows": out.count(), "columns": out.columns,
                           "duplicate_keys": duplicate_keys(out, MART_KEYS[name])}
            print(f"{name:24} rows={marts[name]['rows']:>7,}  dup_keys={marts[name]['duplicate_keys']}")
    finally:
        spark.stop()

    errors = [f"{n}: duplicate keys" for n, m in marts.items() if m["duplicate_keys"]]
    manifest = {"source": "olist", "started_at": started.isoformat(), "keys": MART_KEYS,
                "marts": marts, "errors": errors, "status": "OK" if not errors else "FAILED"}
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if errors:
        raise SystemExit("gold marts olist failed:\n" + "\n".join(errors))
    print(f"gold marts olist OK, manifest {MANIFEST_PATH}")
    return manifest


def main() -> None:
    argparse.ArgumentParser(description="Build Olist Gold marts from the star schema").parse_args()
    run()


if __name__ == "__main__":
    main()
