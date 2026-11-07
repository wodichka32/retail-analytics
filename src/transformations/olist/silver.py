import argparse
import json
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.utils.config import BRONZE_DIR, SILVER_DIR
from src.utils.quality import boolean_flag_counts, duplicate_keys, null_keys, orphans
from src.utils.spark import get_spark

BRONZE = BRONZE_DIR / "olist"
SILVER = SILVER_DIR / "olist"
QUALITY_PATH = SILVER / "_quality.json"

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
MONEY = T.DecimalType(12, 2)
UNKNOWN = "unknown"
INVALID_ORDER_STATUSES = ["canceled", "unavailable"]
SHIPPING_LIMIT_MAX_DAYS = 60
BRAZIL_LAT = (-34.0, 6.0)
BRAZIL_LNG = (-74.0, -34.0)

ACCENTS_FROM = "áàâãäéèêëíìîïóòôõöúùûüçñ" + "º£´³"
ACCENTS_TO = "aaaaaeeeeiiiiooooouuuucn"

MISSING_TRANSLATIONS = {
    "pc_gamer": "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_and_food_preparers",
}

STATE_REGION = {
    **dict.fromkeys(["AC", "AM", "AP", "PA", "RO", "RR", "TO"], "Norte"),
    **dict.fromkeys(["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"], "Nordeste"),
    **dict.fromkeys(["DF", "GO", "MS", "MT"], "Centro-Oeste"),
    **dict.fromkeys(["ES", "MG", "RJ", "SP"], "Sudeste"),
    **dict.fromkeys(["PR", "RS", "SC"], "Sul"),
}

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "sellers": ["seller_id"],
    "category_translation": ["product_category_name"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id", "order_id"],
    "geolocation": ["zip_code_prefix"],
}

FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("reviews", "order_id", "orders", "order_id"),
    ("products", "product_category_name", "category_translation", "product_category_name"),
]

ZIP_COVERAGE = [
    ("customers", "customer_zip_code_prefix"),
    ("sellers", "seller_zip_code_prefix"),
]

Cast = Callable[[Column], Column]


def to_ts(c: Column) -> Column:
    return F.to_timestamp_ntz(c, F.lit(TS_FORMAT))


def to_date(c: Column) -> Column:
    return F.to_date(to_ts(c))


def to_int(c: Column) -> Column:
    return c.cast("double").cast("int")


def to_money(c: Column) -> Column:
    return c.cast(MONEY)


def to_double(c: Column) -> Column:
    return c.cast("double")


def clean_text(c: Column) -> Column:
    no_accents = F.regexp_replace(F.translate(F.lower(c), ACCENTS_FROM, ACCENTS_TO), r"\p{M}", "")
    cleaned = F.trim(F.regexp_replace(no_accents, r"\s+", " "))
    return F.when(cleaned == "", None).otherwise(cleaned)


def clean_city(c: Column) -> Column:
    first_part = F.trim(F.split(clean_text(c), r"\s*[/\\,]\s*|\s+-\s+").getItem(0))
    return F.when(first_part.rlike(r"@|^[0-9 ]*$"), None).otherwise(first_part)


def clean_code(c: Column) -> Column:
    return F.upper(F.trim(c))


def zip5(c: Column) -> Column:
    return F.lpad(F.trim(c), 5, "0")


def blank_to_null(c: Column) -> Column:
    t = F.trim(c)
    return F.when(t == "", None).otherwise(t)


def apply_casts(df: DataFrame, casts: dict[str, Cast]) -> tuple[DataFrame, dict[str, int]]:
    if not casts:
        return df, {}
    checks = [
        F.sum((F.col(c).isNotNull() & fn(F.col(c)).isNull()).cast("int")).alias(c)
        for c, fn in casts.items()
    ]
    failures = {c: int(v or 0) for c, v in df.agg(*checks).first().asDict().items()}
    for c, fn in casts.items():
        df = df.withColumn(c, fn(F.col(c)))
    return df, {c: v for c, v in failures.items() if v}


def region_of(state: Column) -> Column:
    mapping = F.create_map(*[F.lit(x) for kv in STATE_REGION.items() for x in kv])
    return F.coalesce(mapping[state], F.lit(UNKNOWN))


def most_frequent(df: DataFrame, key: str, value: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.desc("n"), F.asc(value))
    return (
        df.groupBy(key, value).agg(F.count("*").alias("n"))
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select(key, value)
    )


def build_customers(b: DataFrame) -> tuple[DataFrame, dict]:
    df = b.select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        zip5(F.col("customer_zip_code_prefix")).alias("customer_zip_code_prefix"),
        clean_city(F.col("customer_city")).alias("customer_city"),
        clean_code(F.col("customer_state")).alias("customer_state"),
        "_batch_id",
    ).dropDuplicates(["customer_id"])
    return df, {"parse_failures": {}}


def build_sellers(b: DataFrame) -> tuple[DataFrame, dict]:
    df = b.select(
        F.trim("seller_id").alias("seller_id"),
        zip5(F.col("seller_zip_code_prefix")).alias("seller_zip_code_prefix"),
        clean_city(F.col("seller_city")).alias("seller_city"),
        clean_code(F.col("seller_state")).alias("seller_state"),
        "_batch_id",
    ).dropDuplicates(["seller_id"])
    return df, {"parse_failures": {}}


def build_category_translation(b: DataFrame, spark: SparkSession) -> tuple[DataFrame, dict]:
    source = b.select(
        F.lower(F.trim("product_category_name")).alias("product_category_name"),
        F.lower(F.trim("product_category_name_english")).alias("product_category_name_english"),
        F.lit(False).alias("is_manual"),
    )
    manual_rows = [(pt, en, True) for pt, en in {**MISSING_TRANSLATIONS, UNKNOWN: UNKNOWN}.items()]
    manual = spark.createDataFrame(manual_rows, source.schema)
    existing = source.select("product_category_name")
    df = source.unionByName(manual.join(existing, "product_category_name", "left_anti"))
    return df.dropDuplicates(["product_category_name"]), {"parse_failures": {}}


def build_products(b: DataFrame, translation: DataFrame) -> tuple[DataFrame, dict]:
    renamed = b.withColumnsRenamed({
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
    })
    int_cols = [
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    ]
    df, failures = apply_casts(renamed, {c: to_int for c in int_cols})
    category = F.coalesce(blank_to_null(F.lower("product_category_name")), F.lit(UNKNOWN))
    dims = ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    df = (
        df.withColumn("product_id", F.trim("product_id"))
        .withColumn("is_category_missing", F.col("product_category_name").isNull())
        .withColumn("product_category_name", category)
        .join(translation.select("product_category_name", "product_category_name_english"),
              "product_category_name", "left")
        .withColumn("has_invalid_measurements",
                    F.exists(F.array(*[F.col(c) for c in dims]), lambda x: x.isNull() | (x <= 0)))
        .select("product_id", "product_category_name", "product_category_name_english", *int_cols,
                "is_category_missing", "has_invalid_measurements", "_batch_id")
        .dropDuplicates(["product_id"])
    )
    return df, {"parse_failures": failures}


def build_orders(b: DataFrame) -> tuple[DataFrame, dict]:
    ts_cols = ["order_purchase_timestamp", "order_approved_at",
               "order_delivered_carrier_date", "order_delivered_customer_date"]
    casts: dict[str, Cast] = {c: to_ts for c in ts_cols}
    casts["order_estimated_delivery_date"] = to_date
    df, failures = apply_casts(b, casts)

    purchase, approved, carrier, delivered = (F.col(c) for c in ts_cols)
    status = F.lower(F.trim("order_status"))
    inconsistent = (approved < purchase) | (carrier < purchase) | (delivered < carrier) | (delivered < purchase)
    conflict = ((status == "delivered") & delivered.isNull()) | ((status != "delivered") & delivered.isNotNull())

    df = (
        df.withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("order_status", status)
        .withColumn("is_valid_order", ~status.isin(INVALID_ORDER_STATUSES))
        .withColumn("is_timestamp_inconsistent", F.coalesce(inconsistent, F.lit(False)))
        .withColumn("has_status_date_conflict", F.coalesce(conflict, F.lit(False)))
        .select("order_id", "customer_id", "order_status", *ts_cols, "order_estimated_delivery_date",
                "is_valid_order", "is_timestamp_inconsistent", "has_status_date_conflict", "_batch_id")
        .dropDuplicates(["order_id"])
    )
    return df, {"parse_failures": failures}


def build_order_items(b: DataFrame, orders: DataFrame) -> tuple[DataFrame, dict]:
    df, failures = apply_casts(b, {
        "order_item_id": to_int,
        "shipping_limit_date": to_ts,
        "price": to_money,
        "freight_value": to_money,
    })
    purchase = orders.select("order_id", "order_purchase_timestamp")
    outlier = F.datediff("shipping_limit_date", "order_purchase_timestamp") > SHIPPING_LIMIT_MAX_DAYS
    df = (
        df.withColumn("order_id", F.trim("order_id"))
        .withColumn("product_id", F.trim("product_id"))
        .withColumn("seller_id", F.trim("seller_id"))
        .join(purchase, "order_id", "left")
        .withColumn("is_shipping_limit_outlier", F.coalesce(outlier, F.lit(False)))
        .select("order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date",
                "price", "freight_value", "is_shipping_limit_outlier", "_batch_id")
        .dropDuplicates(["order_id", "order_item_id"])
    )
    return df, {"parse_failures": failures}


def build_payments(b: DataFrame) -> tuple[DataFrame, dict]:
    df, failures = apply_casts(b, {
        "payment_sequential": to_int,
        "payment_installments": to_int,
        "payment_value": to_money,
    })
    df = (
        df.withColumn("order_id", F.trim("order_id"))
        .withColumn("payment_type", F.lower(F.trim("payment_type")))
        .select("order_id", "payment_sequential", "payment_type", "payment_installments",
                "payment_value", "_batch_id")
        .dropDuplicates(["order_id", "payment_sequential"])
    )
    return df, {"parse_failures": failures}


def build_reviews(b: DataFrame) -> tuple[DataFrame, dict]:
    df, failures = apply_casts(b, {
        "review_score": to_int,
        "review_creation_date": to_date,
        "review_answer_timestamp": to_ts,
    })
    latest = Window.partitionBy("order_id").orderBy(
        F.desc("review_answer_timestamp"), F.desc("review_creation_date"), F.desc("review_id"))
    df = (
        df.withColumn("review_id", F.trim("review_id"))
        .withColumn("order_id", F.trim("order_id"))
        .withColumn("review_comment_title", blank_to_null(F.col("review_comment_title")))
        .withColumn("review_comment_message", blank_to_null(F.col("review_comment_message")))
        .dropDuplicates(["review_id", "order_id"])
        .withColumn("has_title", F.col("review_comment_title").isNotNull())
        .withColumn("has_comment", F.col("review_comment_message").isNotNull())
        .withColumn("is_latest_for_order", F.row_number().over(latest) == 1)
        .select("review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
                "review_creation_date", "review_answer_timestamp", "has_title", "has_comment",
                "is_latest_for_order", "_batch_id")
    )
    return df, {"parse_failures": failures}


def build_geolocation(b: DataFrame) -> tuple[DataFrame, dict]:
    df, failures = apply_casts(b, {"geolocation_lat": to_double, "geolocation_lng": to_double})
    df = df.select(
        zip5(F.col("geolocation_zip_code_prefix")).alias("zip_code_prefix"),
        F.col("geolocation_lat").alias("lat"),
        F.col("geolocation_lng").alias("lng"),
        clean_city(F.col("geolocation_city")).alias("city"),
        clean_code(F.col("geolocation_state")).alias("state"),
    )
    inside = F.col("lat").between(*BRAZIL_LAT) & F.col("lng").between(*BRAZIL_LNG)
    points = df.filter(inside)
    stats = df.agg(
        F.count("*").alias("source_rows"),
        F.sum((~inside).cast("int")).alias("rows_outside_brazil"),
        F.countDistinct("zip_code_prefix").alias("source_zip_prefixes"),
    ).first().asDict()

    coords = points.groupBy("zip_code_prefix").agg(
        F.median("lat").alias("lat"),
        F.median("lng").alias("lng"),
        F.count("*").alias("points_count"),
        F.countDistinct("city").alias("city_variants"),
    )
    result = (
        coords.join(most_frequent(points, "zip_code_prefix", "city"), "zip_code_prefix")
        .join(most_frequent(points, "zip_code_prefix", "state"), "zip_code_prefix")
        .withColumn("region", region_of(F.col("state")))
        .select("zip_code_prefix", "city", "state", "region", "lat", "lng", "points_count", "city_variants")
    )
    return result, {"parse_failures": failures, **stats}


def read_bronze(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.parquet(str(BRONZE / name))


def write_silver(df: DataFrame, name: str, spark: SparkSession) -> DataFrame:
    path = str(SILVER / name)
    df.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def run() -> dict:
    spark = get_spark("silver-olist")
    started = datetime.now(timezone.utc).replace(microsecond=0)
    SILVER.mkdir(parents=True, exist_ok=True)
    bronze = {n: read_bronze(spark, n) for n in PRIMARY_KEYS}
    report: dict = {"source": "olist", "started_at": started.isoformat(), "tables": {}}

    try:
        built: dict[str, DataFrame] = {}
        steps = [
            ("category_translation", lambda: build_category_translation(bronze["category_translation"], spark)),
            ("products", lambda: build_products(bronze["products"], built["category_translation"])),
            ("customers", lambda: build_customers(bronze["customers"])),
            ("sellers", lambda: build_sellers(bronze["sellers"])),
            ("orders", lambda: build_orders(bronze["orders"])),
            ("order_items", lambda: build_order_items(bronze["order_items"], built["orders"])),
            ("payments", lambda: build_payments(bronze["payments"])),
            ("reviews", lambda: build_reviews(bronze["reviews"])),
            ("geolocation", lambda: build_geolocation(bronze["geolocation"])),
        ]
        for name, build in steps:
            df, info = build()
            out = write_silver(df, name, spark)
            built[name] = out
            keys = PRIMARY_KEYS[name]
            entry = {
                "rows_in": bronze[name].count(),
                "rows_out": out.count(),
                "parse_failures": info.pop("parse_failures"),
                "duplicate_keys": duplicate_keys(out, keys),
                "null_keys": null_keys(out, keys),
                "flags": boolean_flag_counts(out),
                **info,
            }
            report["tables"][name] = entry
            print(f"{name:22} in={entry['rows_in']:>9,}  out={entry['rows_out']:>9,}  "
                  f"parse_failures={sum(entry['parse_failures'].values())}  dup_keys={entry['duplicate_keys']}")

        report["foreign_keys"] = [
            {"child": f"{c}.{cc}", "parent": f"{p}.{pc}", "orphans": orphans(built[c], cc, built[p], pc)}
            for c, cc, p, pc in FOREIGN_KEYS
        ]
        geo_zips = built["geolocation"].select(F.col("zip_code_prefix"))
        report["zip_coverage"] = []
        for table, col in ZIP_COVERAGE:
            total = built[table].count()
            missing = orphans(built[table], col, geo_zips, "zip_code_prefix")
            report["zip_coverage"].append({
                "column": f"{table}.{col}", "rows": total, "missing_in_geolocation": missing,
                "coverage_pct": round((total - missing) / total * 100, 2),
            })
    finally:
        spark.stop()

    errors = []
    for name, t in report["tables"].items():
        if t["parse_failures"]:
            errors.append(f"{name}: parse failures {t['parse_failures']}")
        if t["duplicate_keys"] or t["null_keys"]:
            errors.append(f"{name}: primary key violated")
        if name != "geolocation" and name != "category_translation" and t["rows_in"] != t["rows_out"]:
            errors.append(f"{name}: rows {t['rows_in']} -> {t['rows_out']}")
    errors += [f"orphans {fk['child']} -> {fk['parent']}: {fk['orphans']}"
               for fk in report["foreign_keys"] if fk["orphans"]]
    report["errors"] = errors
    report["status"] = "OK" if not errors else "FAILED"
    QUALITY_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    for fk in report["foreign_keys"]:
        print(f"fk {fk['child']:40} -> {fk['parent']:45} orphans={fk['orphans']}")
    for z in report["zip_coverage"]:
        print(f"zip coverage {z['column']:35} {z['coverage_pct']}%")
    if errors:
        raise SystemExit("silver olist failed:\n" + "\n".join(errors))
    print(f"silver olist: {len(report['tables'])} tables OK, report {QUALITY_PATH}")
    return report


def main() -> None:
    argparse.ArgumentParser(description="Build the Olist Silver layer from Bronze").parse_args()
    run()


if __name__ == "__main__":
    main()
