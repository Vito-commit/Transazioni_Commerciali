from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, countDistinct, lit, sum, when
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import approx_count_distinct
from pyspark import StorageLevel


# -----------------------
# Helpers (interni)
# -----------------------

def _op_min(op: DataFrame) -> DataFrame:
    # tieni solo le colonne utili per molte query
    return op.select("order_id", "product_id", "reordered")

# -----------------------
# KPI / Overview (NO op_all) KPI generali in una riga.
# -----------------------
def kpi_overview_from_splits(orders: DataFrame, op_prior: DataFrame, op_train: DataFrame) -> DataFrame:

    num_orders_df = orders.agg(count("*").alias("num_orders"))

    op = _op_min(op_prior).unionByName(_op_min(op_train))
    num_items_df = op.agg(count("*").alias("num_items"))
    distinct_orders_df = op.select("order_id").distinct().agg(count("*").alias("distinct_orders_in_op"))

    joined = num_orders_df.crossJoin(num_items_df).crossJoin(distinct_orders_df)
    return (
        joined
        .withColumn("avg_items_per_order", col("num_items") / col("distinct_orders_in_op"))
        .drop("distinct_orders_in_op")
    )

# -----------------------
# Prodotti
# -----------------------

# -----------------------
# top N prodotti più acquistati
# -----------------------

def top_products_from_splits(op_prior: DataFrame, op_train: DataFrame, products: DataFrame, n: int = 10) -> DataFrame:
    op = _op_min(op_prior).select("product_id").unionByName(_op_min(op_train).select("product_id"))

    return (
        op.join(products, "product_id")
        .groupBy("product_id", "product_name")
        .agg(count("*").alias("times_bought"))
        .orderBy("times_bought", ascending=False)
        .limit(n)
    )

# -----------------------
# Percentuale media di riacquisto sull’intero dataset
# -----------------------

def reorder_rate_global_from_splits(op_prior: DataFrame, op_train: DataFrame) -> DataFrame:
    op = _op_min(op_prior).selectExpr("cast(reordered as double) as reordered") \
        .unionByName(_op_min(op_train).selectExpr("cast(reordered as double) as reordered"))
    return op.agg(avg(col("reordered")).alias("reorder_rate"))

# -----------------------
# Top N prodotti con reorder rate più alto (ma solo prodotti “significativi”)
# -----------------------

def top_reordered_products_from_splits(
    op_prior: DataFrame, op_train: DataFrame, products: DataFrame, n: int = 10, min_count: int = 200
) -> DataFrame:
    op = _op_min(op_prior).select("product_id", "reordered").unionByName(_op_min(op_train).select("product_id", "reordered"))

    stats = (
        op.groupBy("product_id")
        .agg(
            count("*").alias("times_bought"),
            avg(col("reordered")).alias("reorder_rate"),
        )
        .filter(col("times_bought") >= lit(min_count))
    )

    return (
        stats.join(products, "product_id")
        .select("product_id", "product_name", "times_bought", "reorder_rate")
        .orderBy(col("reorder_rate").desc(), col("times_bought").desc())
        .limit(n)
    )

# -----------------------
# Dipartimenti
# -----------------------

# -----------------------
# Volume vendite (in numero item) per dipartimento + percentuale sul totale
# -----------------------

def sales_by_department_from_splits(op_prior: DataFrame, op_train: DataFrame, products: DataFrame, departments: DataFrame) -> DataFrame:


    op = _op_min(op_prior).select("product_id").unionByName(_op_min(op_train).select("product_id"))

    dep = (
        op.join(products, "product_id")
        .join(departments, "department_id")
        .groupBy("department_id", "department")
        .agg(count("*").alias("items_sold"))
    )

    total = dep.agg(sum(col("items_sold")).alias("total_sold"))
    return (
        dep.crossJoin(total)
        .withColumn("pct", col("items_sold") * lit(100.0) / col("total_sold"))
        .drop("total_sold")
        .orderBy("items_sold", ascending=False)
    )

# -----------------------
# Reorder rate per dipartimento + numero di items osservati
# -----------------------

def reorder_rate_by_department_from_splits(op_prior: DataFrame, op_train: DataFrame, products: DataFrame, departments: DataFrame) -> DataFrame:
    op = _op_min(op_prior).select("product_id", "reordered").unionByName(_op_min(op_train).select("product_id", "reordered"))

    return (
        op.join(products, "product_id")
        .join(departments, "department_id")
        .groupBy("department_id", "department")
        .agg(
            avg(col("reordered")).alias("reorder_rate"),
            count("*").alias("items")
        )
        .orderBy(col("reorder_rate").desc(), col("items").desc())
    )

# -----------------------
# Tempo (orders)
# -----------------------

# -----------------------
#  Numero di ordini per giorno della settimana (order_dow)
# -----------------------

def orders_by_dow(orders: DataFrame) -> DataFrame:
    return orders.groupBy("order_dow").agg(count("*").alias("num_orders")).orderBy(col("order_dow").asc())

# -----------------------
#  Numero di ordini per ora del giorno (order_hour_of_day)
# -----------------------

def orders_by_hour(orders: DataFrame) -> DataFrame:
    return orders.groupBy("order_hour_of_day").agg(count("*").alias("num_orders")).orderBy(col("order_hour_of_day").asc())

# -----------------------
#  Media dimensione carrello per giorno della settimana
# -----------------------

def avg_items_per_order_by_dow_from_splits(orders: DataFrame, op_prior: DataFrame, op_train: DataFrame) -> DataFrame:
    # compattiamo per order_id prima di fare union (molto più leggero)
    prior_items = op_prior.groupBy("order_id").count().withColumnRenamed("count", "num_items")
    train_items = op_train.groupBy("order_id").count().withColumnRenamed("count", "num_items")

    items_per_order = prior_items.unionByName(train_items)

    return (
        items_per_order
        .join(orders.select("order_id", "order_dow"), on="order_id", how="inner")
        .groupBy("order_dow")
        .agg(avg("num_items").alias("avg_items"))
        .orderBy("order_dow")
    )

# -----------------------
#  Distribuzione degli ordini per fascia di numero prodotti.
# -----------------------

def basket_size_buckets_from_splits(op_prior: DataFrame, op_train: DataFrame) -> DataFrame:
    prior = op_prior.groupBy("order_id").agg(count("*").alias("items"))
    train = op_train.groupBy("order_id").agg(count("*").alias("items"))
    items_per_order = prior.unionByName(train)

    bucketed = items_per_order.withColumn(
        "bucket",
        when(col("items") <= 5, lit("1-5"))
        .when(col("items") <= 10, lit("6-10"))
        .when(col("items") <= 20, lit("11-20"))
        .otherwise(lit("21+"))
    )

    return (
        bucketed.withColumn(
            "bucket_order",
            when(col("bucket") == "1-5", lit(1))
            .when(col("bucket") == "6-10", lit(2))
            .when(col("bucket") == "11-20", lit(3))
            .otherwise(lit(4))
        )
        .groupBy("bucket", "bucket_order")
        .agg(count("*").alias("num_orders"))
        .orderBy(col("bucket_order").asc())
        .drop("bucket_order")
    )


# -----------------------
#  Media numero di ordini per utente
# -----------------------

def avg_orders_per_user(orders: DataFrame) -> DataFrame:
    """
    Media ordini per utente.
    """
    per_user = orders.groupBy("user_id").agg(count("*").alias("num_orders"))
    return per_user.agg(avg(col("num_orders")).alias("avg_orders_per_user"))

# -----------------------
# Top N utenti con più ordini.
# -----------------------

def top_users_by_orders(orders: DataFrame, n: int = 10) -> DataFrame:
    """
    Top N utenti per numero di ordini.
    """
    return (
        orders.groupBy("user_id")
        .agg(count("*").alias("num_orders"))
        .orderBy(col("num_orders").desc())
        .limit(n)
    )

# -----------------------
# Quanti ordini ci sono per eval_set (prior/train)
# -----------------------

def orders_count_by_eval_set(orders: DataFrame) -> DataFrame:
    return (
        orders.groupBy("eval_set")
        .agg(count("*").alias("num_orders"))
        .orderBy(col("num_orders").desc())
    )

# -----------------------
# Numero totale di righe (items) in prior vs train
# -----------------------

def items_sold_by_eval_set_from_splits(op_prior: DataFrame, op_train: DataFrame) -> DataFrame:
    prior_items = (
        op_prior.select(lit("prior").alias("eval_set"))
        .groupBy("eval_set").count()
        .withColumnRenamed("count", "num_items")
    )
    train_items = (
        op_train.select(lit("train").alias("eval_set"))
        .groupBy("eval_set").count()
        .withColumnRenamed("count", "num_items")
    )
    return prior_items.unionByName(train_items).orderBy(col("num_items").desc())

# -----------------------
# Media items per ordine separata per prior e train
# -----------------------

def avg_items_per_order_by_eval_set_from_splits(op_prior: DataFrame, op_train: DataFrame) -> DataFrame:
    prior = (
        op_prior.groupBy("order_id").count()
        .withColumnRenamed("count", "num_items")
        .agg(avg("num_items").alias("avg_items"))
        .withColumn("eval_set", lit("prior"))
    )
    train = (
        op_train.groupBy("order_id").count()
        .withColumnRenamed("count", "num_items")
        .agg(avg("num_items").alias("avg_items"))
        .withColumn("eval_set", lit("train"))
    )
    return prior.unionByName(train).select("eval_set", "avg_items").orderBy("eval_set")

# -----------------------
# Reorder rate separato per prior e train
# -----------------------

def reorder_rate_by_eval_set_from_splits(op_prior: DataFrame, op_train: DataFrame) -> DataFrame:
    prior = (
        op_prior.selectExpr("cast(reordered as double) as reordered")
        .agg(avg("reordered").alias("reorder_rate"))
        .withColumn("eval_set", lit("prior"))
    )
    train = (
        op_train.selectExpr("cast(reordered as double) as reordered")
        .agg(avg("reordered").alias("reorder_rate"))
        .withColumn("eval_set", lit("train"))
    )
    return prior.unionByName(train).select("eval_set", "reorder_rate").orderBy("eval_set")

# -----------------------
# ML (K-Means) - User Segmentation
# -----------------------

def user_features_for_kmeans(
    orders: DataFrame,
    op_prior: DataFrame,
    op_train: DataFrame,
    products: DataFrame,
    max_users: int | None = None, seed: int = 42,
) -> DataFrame:
    """
    Feature per utente (1 riga per user_id) su cui fare clustering.
    Versione ottimizzata per evitare OOM:
    - orders light
    - op filtrato agli order_id presenti
    - una sola groupBy per order_id
    """

    # orders "light" (evita colonne inutili)
    orders_l = orders.select(
        "order_id", "user_id", "days_since_prior_order", "order_hour_of_day"
    )

    # --- LIMITA GLI UTENTI PRIMA DELLE FEATURE PESANTI ---
    if max_users is not None:
        max_users = int(max_users)

        # prendo un set di utenti e filtro orders_l a quei soli user_id
        users = (
            orders_l
            .select("user_id")
            .distinct()
            .sample(withReplacement=False, fraction=0.3, seed=seed)  # aiuta a non far "scansionare tutto"
            .limit(max_users)
        )
        orders_l = orders_l.join(users, on="user_id", how="inner")

    # order_ids realmente utili
    order_ids = orders_l.select("order_id").distinct()

    op_train = op_train.join(order_ids, on="order_id", how="inner")
    op_prior = op_prior.join(order_ids, on="order_id", how="inner")

    # union prior/train ma filtrata agli order_id utili (CHIAVE per evitare OOM)
    op = op_prior.select("order_id", "product_id", "reordered") \
        .unionByName(op_train.select("order_id", "product_id", "reordered")
        )

    # base orders per user
    user_orders = (
        orders_l.groupBy("user_id")
        .agg(
            count(lit(1)).alias("num_orders"),
            avg("days_since_prior_order").alias("avg_days_between_orders"),
            avg("order_hour_of_day").alias("avg_hour"),
        )
    )

    # UNA sola aggregazione per order_id (più leggera)
    per_order_stats = (
        op.groupBy("order_id")
        .agg(
            count(lit(1)).alias("num_items"),
            avg(col("reordered").cast("double")).alias("reorder_rate_order"),
        )
    )

    per_order = (
        orders_l.select("order_id", "user_id")
        .join(per_order_stats, "order_id", "inner")
    )

    user_basket = (
        per_order.groupBy("user_id")
        .agg(
            avg("num_items").alias("avg_items_per_order"),
            avg("reorder_rate_order").alias("reorder_rate_user"),
        )
    )

    # products light (spesso piccolo: se vuoi puoi broadcastarlo)
    products_l = products.select("product_id", "department_id")

    user_dept_div = (
        orders_l.select("order_id", "user_id")
        .join(op.select("order_id", "product_id"), "order_id", "inner")
        .join(products_l, "product_id", "left")
        .groupBy("user_id")
        .agg(approx_count_distinct("department_id").alias("dept_diversity"))
    )

    feats = (
        user_orders.join(user_basket, "user_id", "left")
        .join(user_dept_div, "user_id", "left")
        .fillna(0)
    )

    return feats.select(
        "user_id",
        col("num_orders").cast("double").alias("num_orders"),
        col("avg_days_between_orders").cast("double").alias("avg_days_between_orders"),
        col("avg_hour").cast("double").alias("avg_hour"),
        col("avg_items_per_order").cast("double").alias("avg_items_per_order"),
        col("reorder_rate_user").cast("double").alias("reorder_rate_user"),
        col("dept_diversity").cast("double").alias("dept_diversity"),
    )



def kmeans_user_clusters(
    orders: DataFrame,
    op_prior: DataFrame,
    op_train: DataFrame,
    products: DataFrame,
    k: int = 5,
    seed: int = 42,
    min_orders: int = 5,
    max_users: int = 3000,
    sample_fraction: float = 0.05,
) -> DataFrame:

    feats = user_features_for_kmeans(
    orders, op_prior, op_train, products,
    max_users=max_users, seed=seed
    )


    # utenti “attivi”
    feats = feats.filter(col("num_orders") >= lit(min_orders))

    # evita orderBy (sort globale = OOM)
    sampled = feats.sample(withReplacement=False, fraction=float(sample_fraction), seed=seed)
    feats = sampled  # limit senza sort

    # shuffle più piccolo (solo per questa query)
    feats.sparkSession.conf.set("spark.sql.shuffle.partitions", "8")
    feats = feats.repartition(8)

    feature_cols = [
        "num_orders",
        "avg_days_between_orders",
        "avg_hour",
        "avg_items_per_order",
        "reorder_rate_user",
        "dept_diversity",
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)


    kmeans = KMeans(
        k=k,
        seed=seed,
        featuresCol="features",
        predictionCol="cluster",
        maxIter=15,
        initMode="k-means||",
    )


    feats = feats.persist(StorageLevel.MEMORY_AND_DISK)
    _ = feats.count()


    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(feats)

    return model.transform(feats).select("user_id", "cluster")

def kmeans_user_cluster_profile(
    orders: DataFrame,
    op_prior: DataFrame,
    op_train: DataFrame,
    products: DataFrame,
    k: int = 5,
    seed: int = 42,
    min_orders: int = 5,
    max_users: int = 3000,
    sample_fraction: float = 0.05,
) -> DataFrame:
    feats = user_features_for_kmeans(
        orders, op_prior, op_train, products,
        max_users=max_users, seed=seed
    )

    feats = feats.filter(col("num_orders") >= lit(min_orders))

    # evita orderBy (sort globale = OOM)
    sampled = feats.sample(withReplacement=False, fraction=float(sample_fraction), seed=seed)
    feats = sampled

    feats.sparkSession.conf.set("spark.sql.shuffle.partitions", "8")
    feats = feats.repartition(8)

    feature_cols = [
        "num_orders",
        "avg_days_between_orders",
        "avg_hour",
        "avg_items_per_order",
        "reorder_rate_user",
        "dept_diversity",
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)

    kmeans = KMeans(
        k=k,
        seed=seed,
        featuresCol="features",
        predictionCol="cluster",
        maxIter=15,
        initMode="k-means||",
    )

    feats = feats.persist(StorageLevel.MEMORY_AND_DISK)
    _ = feats.count()

    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(feats)

    clustered = model.transform(feats)

    return (
        clustered.groupBy("cluster")
        .agg(
            count(lit(1)).alias("users"),
            avg("num_orders").alias("avg_num_orders"),
            avg("avg_items_per_order").alias("avg_basket_size"),
            avg("reorder_rate_user").alias("avg_reorder_rate"),
            avg("dept_diversity").alias("avg_dept_diversity"),
            avg("avg_days_between_orders").alias("avg_days_between"),
            avg("avg_hour").alias("avg_hour"),
        )
        .orderBy("cluster")
    )