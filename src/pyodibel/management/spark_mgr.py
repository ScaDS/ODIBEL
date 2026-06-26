import os

from pyspark import SparkContext
from pyspark.sql import SparkSession


def _is_local_master(master: str) -> bool:
    return master.startswith("local")


def _normalize_optional(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not value or value.lower() == "none":
        return None
    return value


def get_spark_session(
    app_name: str,
    master: str | None = None,
    executor_memory: str = "16g",
    driver_memory: str = "8g",
    shuffle_partitions: int = 210,
    local_dir: str | None = None,
    adaptive_enabled: bool = True,
    skew_join_enabled: bool = True,
):
    # active_context = SparkContext._active_spark_context
    # submitted = os.getenv("PYSPARK_SUBMIT_VERSION") is not None
    # effective_master = _normalize_optional(master) or _normalize_optional(os.getenv("MASTER"))
    # effective_local_dir = _normalize_optional(local_dir) or _normalize_optional(
    #     os.getenv("SPARK_LOCAL_DIR")
    # )

    builder = SparkSession.builder.appName(app_name)

    # # Only set master when explicitly provided. Under spark-submit, or when a
    # # SparkContext already exists, leave master unset and inherit configuration.
    # if active_context is None and effective_master is not None:
    #     builder = builder.master(effective_master)
    #     if _is_local_master(effective_master):
    #         executor_memory = None
    #         builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")
    # elif active_context is None and effective_master is None and not submitted:
    #     builder = builder.master("local[*]")
    #     executor_memory = None
    #     builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")
    #     if effective_local_dir is None:
    #         effective_local_dir = "/tmp/spark"

    builder = (
        builder
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(adaptive_enabled))
        .config("spark.sql.adaptive.skewJoin.enabled", str(skew_join_enabled))
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
    )

    # if effective_local_dir is not None:
    #     builder = builder.config("spark.local.dir", effective_local_dir)

    if executor_memory:
        builder = builder.config("spark.executor.memory", executor_memory)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark master: {spark.sparkContext.master}")

    return spark
