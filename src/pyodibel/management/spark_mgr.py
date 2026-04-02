from pyspark.sql import SparkSession

def get_spark_session(
    app_name: str, 
    master: str = "local[*]", 
    executor_memory: str = "16g",
    driver_memory: str = "8g", 
    shuffle_partitions: int = 210, 
    local_dir: str = "/tmp/spark",
    adaptive_enabled: bool = True,
    skew_join_enabled: bool = True
):
    if master == "local[*]":
        executor_memory = None

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.local.dir", local_dir)
        .config("spark.sql.adaptive.enabled", str(adaptive_enabled))
        .config("spark.sql.adaptive.skewJoin.enabled", str(skew_join_enabled))
    )
    
    # In cluster mode, this matters. In local mode, it's effectively irrelevant.
    if executor_memory:
        builder = builder.config("spark.executor.memory", executor_memory)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark