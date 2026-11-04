"""Tao SparkSession co Delta Lake. Day la noi DUY NHAT tao SparkSession.

Muon chay tren Databricks: chi can sua ham get_spark (tra ve `spark` co san).
"""

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# hai jar nay da co san trong pyspark; tai lai co the treo tren mang cham
JARS_EXCLUDES = ("org.apache.hadoop:hadoop-client-api,"
                 "org.apache.hadoop:hadoop-client-runtime")


def get_spark(app_name="sales_lakehouse"):
    """Tra ve SparkSession local[*] da bat Delta (dung lai neu da co)."""
    # dam bao driver va worker dung cung trinh thong dich Python
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.excludes", JARS_EXCLUDES)
        .config("spark.ui.enabled", "false")
        # gio metadata hien thi theo UTC cho de so sanh giua cac may
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
