# app/data.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pathlib import Path
import sys

@dataclass
class DataBundle:
    orders: DataFrame
    op_train: DataFrame
    op_prior: DataFrame
    products: DataFrame
    aisles: DataFrame
    departments: DataFrame


def create_spark(app_name: str = "InstacartSpark") -> SparkSession:
    python_exe = sys.executable  # <-- python della tua venv

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        # IMPORTANTISSIMO su Windows: forza python worker
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        .getOrCreate()
    )



def load_data(
    spark: SparkSession,
    base_path: str = "/mnt/data",
    create_temp_views: bool = False
) -> DataBundle:
    """
    Carica i CSV Instacart
    base_path: cartella dove si trovano i csv (qui: /mnt/data).
    """

    def read_csv(name: str) -> DataFrame:
        path = (Path(base_path) / name).resolve()
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(path))
        )

    orders = read_csv("orders.csv")
    op_train = read_csv("order_products__train.csv")
    op_prior = read_csv("order_products__prior.csv")
    products = read_csv("products.csv")
    aisles = read_csv("aisles.csv")
    departments = read_csv("departments.csv")

    # ---- Casting "di sicurezza" (molto utile per join / aggregazioni) ----
    orders = (
        orders
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("user_id", col("user_id").cast("int"))
        .withColumn("eval_set", col("eval_set").cast("string"))
        .withColumn("order_number", col("order_number").cast("int"))
        .withColumn("order_dow", col("order_dow").cast("int"))
        .withColumn("order_hour_of_day", col("order_hour_of_day").cast("int"))
        .withColumn("days_since_prior_order", col("days_since_prior_order").cast("double"))
    )

    op_train = (
        op_train
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("add_to_cart_order", col("add_to_cart_order").cast("int"))
        .withColumn("reordered", col("reordered").cast("int"))
    )

    op_prior = (
        op_prior
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("add_to_cart_order", col("add_to_cart_order").cast("int"))
        .withColumn("reordered", col("reordered").cast("int"))
    )

    products = (
        products
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("aisle_id", col("aisle_id").cast("int"))
        .withColumn("department_id", col("department_id").cast("int"))
    )

    aisles = aisles.withColumn("aisle_id", col("aisle_id").cast("int"))
    departments = departments.withColumn("department_id", col("department_id").cast("int"))


    if create_temp_views:
        orders.createOrReplaceTempView("orders")
        op_train.createOrReplaceTempView("op_train")
        op_prior.createOrReplaceTempView("op_prior")
        products.createOrReplaceTempView("products")
        aisles.createOrReplaceTempView("aisles")
        departments.createOrReplaceTempView("departments")

    return DataBundle(
        orders=orders,
        op_train=op_train,
        op_prior=op_prior,
        products=products,
        aisles=aisles,
        departments=departments,
    )
