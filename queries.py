from pyspark.sql import functions as F
from pyspark.sql.window import Window


def q1_top3_restaurants_per_state(datasets: dict):
    """Топ-3 ресторани за к-стю відгуків у кожному штаті (лише відкриті)."""
    business_df = datasets["business"]
    review_df = datasets["review"]

    restaurants = business_df.filter(
        (F.col("is_open") == True)
        & F.array_contains(F.col("categories"), "Restaurants")
    )
    review_counts = review_df.groupBy("business_id").agg(
        F.count("*").alias("actual_review_count"),
    )
    joined = restaurants.join(review_counts, on="business_id")

    w = Window.partitionBy("state").orderBy(F.desc("actual_review_count"))
    ranked = joined.withColumn("rank", F.row_number().over(w))
    return ranked.filter(F.col("rank") <= 3).select(
        "state", "name", "business_id", "actual_review_count", "rank",
    )


def q2_highest_rated_per_state(datasets: dict):
    """Бізнес з найвищим рейтингом у кожному штаті (>100 відгуків)."""
    business_df = datasets["business"]

    filtered = business_df.filter(F.col("review_count") > 100)

    w = Window.partitionBy("state").orderBy(
        F.desc("stars"), F.desc("review_count"),
    )
    ranked = filtered.withColumn("rank", F.row_number().over(w))
    return ranked.filter(F.col("rank") == 1).select(
        "state", "name", "business_id", "stars", "review_count", "rank",
    )


def q3_weekend_vs_weekday_rating(datasets: dict):
    """Середній рейтинг: працюють у вихідні vs лише будні."""
    business_df = datasets["business"]

    with_flag = business_df.withColumn(
        "works_weekends",
        F.array_contains(F.col("open_days"), "Saturday")
        | F.array_contains(F.col("open_days"), "Sunday"),
    )
    weekday_only = with_flag.filter(
        (F.col("works_weekends") == False) & F.col("open_days").isNotNull()
    )
    weekend = with_flag.filter(F.col("works_weekends") == True)

    result_weekend = weekend.agg(
        F.avg("stars").alias("avg_stars"),
        F.count("*").alias("biz_count"),
    ).withColumn("group", F.lit("works_weekends"))

    result_weekday = weekday_only.agg(
        F.avg("stars").alias("avg_stars"),
        F.count("*").alias("biz_count"),
    ).withColumn("group", F.lit("weekday_only"))

    return result_weekend.unionByName(result_weekday)


def q4_wifi_vs_no_wifi_rating(datasets: dict):
    """Середній рейтинг: безкоштовний WiFi vs без WiFi."""
    business_df = datasets["business"]

    return business_df.groupBy("has_free_wifi").agg(
        F.avg("stars").alias("avg_stars"),
        F.count("*").alias("biz_count"),
    ).orderBy("has_free_wifi")


def q5_checkins_vs_rating(datasets: dict):
    """Залежність між кількістю чекінів і середнім рейтингом бізнесу."""
    business_df = datasets["business"]

    bucketed = business_df.withColumn(
        "checkin_bucket",
        F.when(F.col("checkin_count") == 0, "0")
        .when(F.col("checkin_count") <= 10, "1-10")
        .when(F.col("checkin_count") <= 50, "11-50")
        .when(F.col("checkin_count") <= 200, "51-200")
        .when(F.col("checkin_count") <= 1000, "201-1000")
        .otherwise("1000+"),
    )
    return bucketed.groupBy("checkin_bucket").agg(
        F.avg("stars").alias("avg_stars"),
        F.count("*").alias("biz_count"),
        F.avg("checkin_count").alias("avg_checkins"),
    ).orderBy("avg_checkins")


def q6_rating_discrepancy(datasets: dict):
    """Бізнеси з найбільшою розбіжністю між офіційним і середнім рейтингом відгуків."""
    business_df = datasets["business"]
    review_df = datasets["review"]

    avg_review = review_df.groupBy("business_id").agg(
        F.avg("stars").alias("avg_review_stars"),
        F.count("*").alias("num_reviews"),
    )

    joined = business_df.join(avg_review, on="business_id")
    with_diff = joined.withColumn(
        "discrepancy",
        F.round(F.abs(F.col("stars") - F.col("avg_review_stars")), 2),
    )

    w = Window.orderBy(F.desc("discrepancy"))
    ranked = with_diff.withColumn("rank", F.row_number().over(w))
    return ranked.filter(F.col("rank") <= 20).select(
        "rank", "name", "business_id", "state",
        "stars", "avg_review_stars", "discrepancy", "num_reviews",
    )
