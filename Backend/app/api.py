# app/api.py
from __future__ import annotations
from pyspark.sql.functions import col

from pathlib import Path
from fastapi import FastAPI, Query
from Backend.app.data import create_spark, load_data
from Backend.app import queries as q
import time
from starlette.requests import Request

app = FastAPI(title="Instacart Aggregations (Spark)")

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Process-Time-ms"] = f"{elapsed_ms:.2f}"
    return response


spark = create_spark("InstacartAggregationsAPI")
spark.sparkContext.setLogLevel("WARN")

BASE_PATH = Path(__file__).resolve().parent.parent / "data"
data = load_data(spark, base_path=str(BASE_PATH), create_temp_views=False)

# cache dimension tables come fai già in main.py


data.products.cache()
data.departments.cache()
data.aisles.cache()

def df_to_json(df, limit: int = 1000):
    # per query aggregate i risultati sono piccoli => collect ok
    return [r.asDict(recursive=True) for r in df.limit(limit).collect()]

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/kpi")
def kpi():
    return df_to_json(q.kpi_overview_from_splits(data.orders, data.op_prior, data.op_train))

@app.get("/top-products")
def top_products(n: int = Query(10, ge=1, le=100)):
    df = q.top_products_from_splits(data.op_prior, data.op_train, data.products, n=n)
    return df_to_json(df)

@app.get("/orders-by-dow")
def orders_by_dow():
    return df_to_json(q.orders_by_dow(data.orders))

@app.get("/orders-by-hour")
def orders_by_hour():
    return df_to_json(q.orders_by_hour(data.orders))

@app.get("/sales-by-department")
def sales_by_department():
    df = q.sales_by_department_from_splits(data.op_prior, data.op_train, data.products, data.departments)
    return df_to_json(df, limit=200)

@app.get("/reorder-rate-by-department")
def reorder_rate_by_department():
    df = q.reorder_rate_by_department_from_splits(data.op_prior, data.op_train, data.products, data.departments)
    return df_to_json(df, limit=200)

@app.get("/top-reordered-products")
def top_reordered_products(
    n: int = Query(10, ge=1, le=100),
    min_count: int = Query(200, ge=1, le=1000000),
):
    df = q.top_reordered_products_from_splits(data.op_prior, data.op_train, data.products, n=n, min_count=min_count)
    return df_to_json(df)

@app.get("/reorder-rate-global")
def reorder_rate_global():
    return df_to_json(q.reorder_rate_global_from_splits(data.op_prior, data.op_train))

@app.get("/avg-items-by-dow")
def avg_items_by_dow():
    return df_to_json(q.avg_items_per_order_by_dow_from_splits(data.orders, data.op_prior, data.op_train))

@app.get("/basket-size-buckets")
def basket_size_buckets():
    return df_to_json(q.basket_size_buckets_from_splits(data.op_prior, data.op_train))

@app.get("/avg-orders-per-user")
def avg_orders_per_user():
    return df_to_json(q.avg_orders_per_user(data.orders))

@app.get("/top-users-by-orders")
def top_users_by_orders(n: int = Query(10, ge=1, le=100)):
    return df_to_json(q.top_users_by_orders(data.orders, n=n))

@app.get("/orders-count-by-eval-set")
def orders_count_by_eval_set():
    return df_to_json(q.orders_count_by_eval_set(data.orders))

@app.get("/items-sold-by-eval-set")
def items_sold_by_eval_set():
    return df_to_json(q.items_sold_by_eval_set_from_splits(data.op_prior, data.op_train))

@app.get("/avg-items-per-order-by-eval-set")
def avg_items_per_order_by_eval_set():
    return df_to_json(q.avg_items_per_order_by_eval_set_from_splits(data.op_prior, data.op_train))

@app.get("/reorder-rate-by-eval-set")
def reorder_rate_by_eval_set():
    return df_to_json(q.reorder_rate_by_eval_set_from_splits(data.op_prior, data.op_train))

@app.get("/kmeans-user-clusters")
def kmeans_user_clusters(
    k: int = Query(5, ge=2, le=20),
    seed: int = 42,
    limit: int = Query(1000, ge=1, le=50000),
    cluster: int | None = None,   # opzionale, utile per frontend
):
    df = q.kmeans_user_clusters(data.orders, data.op_prior, data.op_train, data.products, k=k, seed=seed)
    if cluster is not None:
        df = df.filter(col("cluster") == cluster)
    return df_to_json(df, limit=limit)


@app.get("/kmeans-user-cluster-profile")
def kmeans_user_cluster_profile(
    k: int = Query(5, ge=2, le=20),
    seed: int = 42,
):
    df = q.kmeans_user_cluster_profile(data.orders, data.op_prior, data.op_train, data.products, k=k, seed=seed)
    return df_to_json(df, limit=200)


@app.get("/")
def root():
    return {"message": "API running. Try /health or /docs"}
