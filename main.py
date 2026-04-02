from pyspark.sql import SparkSession
from data_loader import load_datasets
from stats import print_general_stats, print_numeric_stats
from preprocessing import (
    preprocess_business,
    preprocess_review,
    preprocess_tip,
    preprocess_user,
)
from questions import QUESTIONS, OUTPUT_DIR

spark = (
    SparkSession.builder
    .appName("BigDataProject")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

datasets = load_datasets(spark, "/data")

print("\n" + "=" * 60)
print(" RAW DATA STATISTICS")
print("=" * 60)
for name, df in datasets.items():
    print_general_stats(name, df)
    print_numeric_stats(name, df)

datasets["review"] = preprocess_review(datasets["review"])
datasets["tip"] = preprocess_tip(datasets["tip"])
datasets["user"] = preprocess_user(datasets["user"])
datasets["business"] = preprocess_business(datasets["business"], datasets["checkin"])

del datasets["checkin"]

print("\n" + "=" * 60)
print(" PREPROCESSED DATA STATISTICS")
print("=" * 60)
for name, df in datasets.items():
    print_general_stats(name, df)
    print_numeric_stats(name, df)

for q in QUESTIONS:
    print(f"\n[{q.author}] {q.content}")
    result = q.query(datasets)
    result.explain(True)
    result.show(20, truncate=False)
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{OUTPUT_DIR}/{q.author}/{q.output_name}"
    )
