from pyspark.sql import SparkSession
from data_loader import load_datasets

spark = (
    SparkSession.builder
    .appName("BigDataProject")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

datasets = load_datasets(spark, "/data")

for name, df in datasets.items():
    df.count()
    print(f"\n{'='*60}")
    print(f" {name.upper()} — {df.count():,} rows, {len(df.columns)} columns")
    print(f"{'='*60}")
    df.printSchema()
