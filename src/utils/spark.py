import os
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession


def _ensure_windows_env() -> None:
    if os.name != "nt":
        return
    hadoop_home = os.environ.get("HADOOP_HOME") or str(Path.home() / "hadoop")
    if Path(hadoop_home, "bin", "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = hadoop_home
        bin_dir = str(Path(hadoop_home, "bin"))
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def get_spark(app_name: str = "retail-analytics") -> SparkSession:
    _ensure_windows_env()
    os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).parent))
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.master(os.environ.get("RETAIL_SPARK_MASTER", "local[*]"))
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
