import argparse
import csv
import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import BRONZE_DIR, OLIST_TABLES, RAW_DIR
from src.utils.spark import get_spark

SOURCE = "olist"
SOURCE_DIR = RAW_DIR / SOURCE
TARGET_DIR = BRONZE_DIR / SOURCE
MANIFEST_PATH = TARGET_DIR / "_manifest.json"


def count_csv_records(path) -> int:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def read_raw(spark: SparkSession, path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .csv(str(path))
    )


def add_metadata(df: DataFrame, file_name: str, ingested_at: datetime, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source", F.lit(SOURCE))
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_ingestion_timestamp", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_table(spark: SparkSession, name: str, ingested_at: datetime, batch_id: str) -> dict:
    file_name = OLIST_TABLES[name]["file"]
    src = SOURCE_DIR / file_name
    dst = TARGET_DIR / name

    raw = read_raw(spark, src)
    source_columns = raw.columns
    add_metadata(raw, file_name, ingested_at, batch_id).write.mode("overwrite").parquet(str(dst))

    expected = count_csv_records(src)
    written = spark.read.parquet(str(dst)).count()
    status = "OK" if expected == written else "MISMATCH"
    print(f"{status:8} {name:22} csv={expected:>9,}  parquet={written:>9,}")
    return {
        "table": name,
        "source_file": file_name,
        "source_bytes": src.stat().st_size,
        "columns": source_columns,
        "csv_records": expected,
        "parquet_rows": written,
        "status": status,
    }


def run(tables: list[str]) -> list[dict]:
    spark = get_spark("bronze-olist")
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
    batch_id = ingested_at.strftime("%Y%m%dT%H%M%SZ")
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    try:
        results = [ingest_table(spark, t, ingested_at, batch_id) for t in tables]
    finally:
        spark.stop()

    manifest = {
        "source": SOURCE,
        "batch_id": batch_id,
        "ingestion_timestamp": ingested_at.isoformat(),
        "tables": results,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    failed = [r["table"] for r in results if r["status"] != "OK"]
    if failed:
        raise SystemExit(f"row count mismatch: {failed}")
    print(f"bronze {SOURCE}: {len(results)} tables, batch {batch_id}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw Olist CSV files into the Bronze layer as Parquet")
    parser.add_argument("--tables", nargs="+", choices=sorted(OLIST_TABLES), default=list(OLIST_TABLES))
    args = parser.parse_args()
    run(args.tables)


if __name__ == "__main__":
    main()
