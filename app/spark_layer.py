from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as fsum
from pyspark import StorageLevel

_spark = None
_fact = None

def get_spark() -> SparkSession:
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .appName("spark-jobs-api")
            .master("local[*]")
            # shuffle più piccolo in locale
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.default.parallelism", "8")
            # memoria (con 16 GB RAM: 6g o 8g vanno bene)
            .config("spark.driver.memory", "6g")
            .config("spark.executor.memory", "6g")
            # evita risultati enormi al driver
            .config("spark.driver.maxResultSize", "1g")
            .getOrCreate()
        )
    return _spark

_fact = None

def get_fact(data_dir: str = "data"):
    global _fact
    if _fact is not None:
        return _fact

    spark = get_spark()

    aisles = spark.read.option("header", True).option("inferSchema", True).csv(f"{data_dir}/aisles.csv")
    departments = spark.read.option("header", True).option("inferSchema", True).csv(f"{data_dir}/departments.csv")
    products = spark.read.option("header", True).option("inferSchema", True).csv(f"{data_dir}/products.csv")
    orders = spark.read.option("header", True).option("inferSchema", True).csv(f"{data_dir}/orders.csv")
    op_train = spark.read.option("header", True).option("inferSchema", True).csv(f"{data_dir}/order_products__train.csv")
    op_prior = spark.read.option("header", True).option("inferSchema", True).csv(f"{data_dir}/order_products__prior.csv")

    # Cache/persist intelligenti (piccoli joinati spesso)
    products = products.select("product_id", "product_name", "aisle_id", "department_id").cache()
    departments = departments.select("department_id", "department").cache()
    aisles = aisles.select("aisle_id", "aisle").cache()

    # Orders "light" (meno colonne, utile per quasi tutto)
    orders = orders.select(
        "order_id", "user_id", "eval_set",
        "order_dow", "order_hour_of_day", "days_since_prior_order"
    ).persist(StorageLevel.MEMORY_AND_DISK)

    # OP minimizzati (sono grandi → MEMORY_AND_DISK)
    op_train = op_train.select("order_id", "product_id", "reordered").persist(StorageLevel.MEMORY_AND_DISK)
    op_prior = op_prior.select("order_id", "product_id", "reordered").persist(StorageLevel.MEMORY_AND_DISK)

    # Materializza una volta sola
    _ = products.count()
    _ = departments.count()
    _ = aisles.count()
    _ = orders.count()
    _ = op_train.count()
    _ = op_prior.count()

    _fact = {
        "aisles": aisles,
        "departments": departments,
        "products": products,
        "orders": orders,
        "op_train": op_train,
        "op_prior": op_prior
    }
    return _fact