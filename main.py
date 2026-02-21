import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

DATA_DIR = "/data"

spark = (
    SparkSession.builder
    .appName("BigDataProject")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

business = spark.read.json(f"{DATA_DIR}/yelp_academic_dataset_business.json")
review = spark.read.json(f"{DATA_DIR}/yelp_academic_dataset_review.json")
user = spark.read.json(f"{DATA_DIR}/yelp_academic_dataset_user.json")
checkin = spark.read.json(f"{DATA_DIR}/yelp_academic_dataset_checkin.json")
tip = spark.read.json(f"{DATA_DIR}/yelp_academic_dataset_tip.json")

datasets = {
    "business": business,
    "review": review,
    "user": user,
    "checkin": checkin,
    "tip": tip,
}

for name, df in datasets.items():
    print(f"\n{'='*60}")
    print(f" {name.upper()} — {df.count():,} rows, {len(df.columns)} columns")
    print(f"{'='*60}")
    df.printSchema()
    df.show(5, truncate=50)