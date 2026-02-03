# app/main.py
from __future__ import annotations
from pathlib import Path
from app.data import create_spark, load_data
from app import queries as q
from pyspark import StorageLevel


def main() -> None:
    spark = create_spark("InstacartAggregations")
    spark.sparkContext.setLogLevel("WARN")

    BASE_PATH = Path(__file__).resolve().parent.parent / "data"

    data = load_data(
        spark,
        base_path=str(BASE_PATH),
        create_temp_views=False
    )

    data.products.cache()
    data.departments.cache()
    data.aisles.cache()

    print("\n=== KPI OVERVIEW ===")
    q.kpi_overview_from_splits(data.orders, data.op_prior, data.op_train).show(truncate=False)

    print("\n=== TOP 10 PRODUCTS ===")
    q.top_products_from_splits(data.op_prior, data.op_train, data.products, n=10).show(truncate=False)

    print("\n=== SALES BY DEPARTMENT (with %) ===")
    q.sales_by_department_from_splits(
        data.op_prior, data.op_train, data.products, data.departments
    ).show(30, truncate=False)

    print("\n=== ORDERS BY DAY OF WEEK ===")
    q.orders_by_dow(data.orders).show(truncate=False)

    print("\n=== ORDERS BY HOUR ===")
    q.orders_by_hour(data.orders).show(truncate=False)

    print("\n=== GLOBAL REORDER RATE ===")
    q.reorder_rate_global_from_splits(data.op_prior, data.op_train).show(truncate=False)

    print("\n=== TOP REORDERED PRODUCTS (min_count=200) ===")
    q.top_reordered_products_from_splits(
        data.op_prior, data.op_train, data.products, n=10, min_count=200
    ).show(truncate=False)

    print("\n=== REORDER RATE BY DEPARTMENT ===")
    q.reorder_rate_by_department_from_splits(
        data.op_prior, data.op_train, data.products, data.departments
    ).show(30, truncate=False)

    print("\n=== AVG ITEMS PER ORDER BY DOW ===")
    q.avg_items_per_order_by_dow_from_splits(data.orders, data.op_prior, data.op_train).show(truncate=False)

    print("\n=== BASKET SIZE BUCKETS ===")
    q.basket_size_buckets_from_splits(data.op_prior, data.op_train).show(truncate=False)

    print("\n=== AVG ORDERS PER USER ===")
    q.avg_orders_per_user(data.orders).show(truncate=False)

    print("\n=== TOP USERS BY ORDERS ===")
    q.top_users_by_orders(data.orders, n=10).show(truncate=False)

    print("\n=== ORDERS COUNT BY EVAL_SET ===")
    q.orders_count_by_eval_set(data.orders).show(truncate=False)

    print("\n=== ITEMS SOLD BY EVAL_SET (SPARK ONLY) ===")
    q.items_sold_by_eval_set_from_splits(data.op_prior, data.op_train).show(truncate=False)

    print("\n=== AVG ITEMS PER ORDER BY EVAL SET ===")
    q.avg_items_per_order_by_eval_set_from_splits(data.op_prior, data.op_train).show(truncate=False)

    print("\n=== REORDER RATE BY EVAL_SET ===")
    q.reorder_rate_by_eval_set_from_splits(data.op_prior, data.op_train).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
