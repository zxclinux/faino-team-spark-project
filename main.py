from pyspark.sql import SparkSession
from preprocessing import print_general_stats, print_numeric_stats

DATA_DIR = "/data"
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
    print_general_stats(name, df)
    print_numeric_stats(name, df)
    df.count()
    print(f"\n{'='*60}")
    print(f" {name.upper()} — {df.count():,} rows, {len(df.columns)} columns")
    print(f"{'='*60}")
    df.printSchema()
