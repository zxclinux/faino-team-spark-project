from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# business-level categories most often seen in the dataset become 0/1 flags
TOP_CATEGORIES_N = 20

# right-skewed counters (see stats.md) are fed to models as log1p
LOG_COLS = ["review_count", "checkin_count", "tip_count", "total_useful"]

NUMERIC_FEATURES = [
    "latitude",
    "longitude",
    "is_open",
    "has_free_wifi",
    "open_days_count",
    "categories_count",
    "log_review_count",
    "log_checkin_count",
    "log_tip_count",
    "log_total_useful",
    "avg_useful",
    "avg_funny",
    "avg_cool",
    "review_years_span",
]

CATEGORICAL_FEATURES = ["state"]


def _top_categories(business_df: DataFrame, n: int) -> list:
    rows = (
        business_df
        .select(F.explode("categories").alias("category"))
        .groupBy("category")
        .count()
        .orderBy(F.desc("count"))
        .limit(n)
        .collect()
    )
    return [r["category"] for r in rows]


def _category_col(category: str) -> str:
    slug = "".join(ch if ch.isalnum() else "_" for ch in category.lower())
    return f"cat_{slug}"


def build_business_features(datasets: dict):
    """Build one row per business with model features and label = business.stars.

    avg(review.stars) is deliberately left out: it is almost the target itself (leakage).

    Returns:
        (DataFrame, numeric_feature_cols, categorical_feature_cols)
    """
    business_df = datasets["business"]
    review_df = datasets["review"]
    tip_df = datasets["tip"]

    # review reactions per business — describe engagement, not the rating itself
    review_stats = review_df.groupBy("business_id").agg(
        F.sum("useful").alias("total_useful"),
        F.avg("useful").alias("avg_useful"),
        F.avg("funny").alias("avg_funny"),
        F.avg("cool").alias("avg_cool"),
        (F.year(F.max("date")) - F.year(F.min("date"))).alias("review_years_span"),
    )
    tip_counts = tip_df.groupBy("business_id").agg(F.count("*").alias("tip_count"))

    df = (
        business_df
        .join(review_stats, on="business_id", how="left")
        .join(tip_counts, on="business_id", how="left")
    )

    df = (
        df
        .withColumn("label", F.col("stars"))
        .withColumn("is_open", F.col("is_open").cast("int"))
        .withColumn("has_free_wifi", F.coalesce(F.col("has_free_wifi"), F.lit(False)).cast("int"))
        # size(null) returns -1 in Spark, so nulls are handled explicitly
        .withColumn(
            "open_days_count",
            F.when(F.col("open_days").isNull(), F.lit(0)).otherwise(F.size("open_days")),
        )
        .withColumn("categories_count", F.size("categories"))
        .fillna(0, subset=["tip_count", "total_useful", "avg_useful", "avg_funny",
                           "avg_cool", "review_years_span"])
    )

    for c in LOG_COLS:
        df = df.withColumn(f"log_{c}", F.log1p(F.col(c).cast("double")))

    category_cols = []
    for category in _top_categories(business_df, TOP_CATEGORIES_N):
        col_name = _category_col(category)
        df = df.withColumn(col_name, F.array_contains("categories", category).cast("int"))
        category_cols.append(col_name)

    numeric_cols = NUMERIC_FEATURES + category_cols
    df = df.select("business_id", "label", *CATEGORICAL_FEATURES, *numeric_cols)
    df = df.dropna(subset=["label", "latitude", "longitude"])

    return df, numeric_cols, CATEGORICAL_FEATURES
