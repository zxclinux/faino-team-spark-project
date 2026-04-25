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

def q7_active_reviewers_vs_all_avg_rating(datasets: dict):
    """Середній рейтинг від активних оглядачів (>100 reviews) vs загальний середній."""
    review_df = datasets["review"]
    user_df = datasets["user"]

    active_users = user_df.filter(F.col("review_count") > 100).select("user_id")

    active_reviews = review_df.join(active_users, on="user_id", how="inner")

    active_result = active_reviews.agg(
        F.avg("stars").alias("avg_rating"),
        F.count("*").alias("review_count"),
    ).withColumn("group", F.lit("active_reviewers"))

    all_result = review_df.agg(
        F.avg("stars").alias("avg_rating"),
        F.count("*").alias("review_count"),
    ).withColumn("group", F.lit("all_reviews"))

    return active_result.unionByName(all_result)


def q8_elite_vs_non_elite_rating(datasets: dict):
    """Порівняння середнього рейтингу від elite vs non-elite користувачів."""
    review_df = datasets["review"]
    user_df = datasets["user"]

    users = user_df.withColumn(
        "user_group",
        F.when(F.size(F.col("elite")) > 0, "elite").otherwise("non_elite"),
    ).select("user_id", "user_group")

    joined = review_df.join(users, on="user_id", how="inner")

    return joined.groupBy("user_group").agg(
        F.avg("stars").alias("avg_rating"),
        F.count("*").alias("review_count"),
    ).orderBy("user_group")


def q9_median_friends_vs_rest_rating(datasets: dict):
    """Користувачі з друзями вище медіани vs решта."""
    
    review_df = datasets["review"]
    user_df = datasets["user"]

    users = user_df.withColumn(
        "friends_count",
        F.size(F.col("friends"))
    ).select("user_id", "friends_count")

    w = Window.partitionBy().rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )

    users_with_median = users.withColumn(
        "median_friends",
        F.percentile_approx("friends_count", 0.5).over(w)
    )

    above_median = users_with_median.filter(
        F.col("friends_count") > F.col("median_friends")
    ).select("user_id").withColumn(
        "group", F.lit("above_median")
    )

    rest_users = users_with_median.filter(
        F.col("friends_count") <= F.col("median_friends")
    ).select("user_id").withColumn(
        "group", F.lit("median_or_below")
    )

    segmented_users = above_median.unionByName(rest_users)

    joined = segmented_users.join(review_df, on="user_id", how="inner")

    return joined.groupBy("group").agg(
        F.avg("stars").alias("avg_rating"),
        F.count("*").alias("review_count")
    ).orderBy("group")


def q10_business_moving_avg_last10_reviews(datasets: dict):
    """Moving average рейтингу по останніх 10 відгуках для кожного бізнесу."""
    review_df = datasets["review"]

    w = Window.partitionBy("business_id").orderBy("date").rowsBetween(-9, 0)

    return review_df.withColumn(
        "moving_avg_last_10",
        F.round(F.avg("stars").over(w), 3),
    ).select(
        "business_id",
        "review_id",
        "date",
        "stars",
        "moving_avg_last_10",
    )


def q11_active_users_no_useful_votes(datasets: dict):
    """Користувачі з >100 reviews, які жодного разу не отримали useful."""
    review_df = datasets["review"]
    user_df = datasets["user"]

    active_users = user_df.filter(F.col("review_count") > 100).select(
        "user_id", "name", "review_count"
    )

    useful_stats = review_df.groupBy("user_id").agg(
        F.sum("useful").alias("total_useful"),
        F.count("*").alias("actual_reviews"),
    )

    joined = active_users.join(useful_stats, on="user_id", how="left").fillna(0)

    return joined.filter(F.col("total_useful") == 0).select(
        "user_id",
        "name",
        "review_count",
        "actual_reviews",
        "total_useful",
    ).orderBy(F.desc("review_count"))


def q12_friends_vs_avg_useful_votes(datasets: dict):
    """Зв'язок між друзями користувача та середнім useful на його відгуки."""
    review_df = datasets["review"]
    user_df = datasets["user"]

    users = user_df.withColumn(
        "friends_count",
        F.size(F.col("friends")),
    ).select("user_id", "friends_count")

    review_stats = review_df.groupBy("user_id").agg(
        F.avg("useful").alias("avg_useful_per_review"),
        F.count("*").alias("review_count"),
    )

    joined = users.join(review_stats, on="user_id", how="inner")

    bucketed = joined.withColumn(
        "friends_bucket",
        F.when(F.col("friends_count") == 0, "0")
        .when(F.col("friends_count") <= 5, "1-5")
        .when(F.col("friends_count") <= 20, "6-20")
        .when(F.col("friends_count") <= 50, "21-50")
        .when(F.col("friends_count") <= 100, "51-100")
        .otherwise("100+"),
    )

    return bucketed.groupBy("friends_bucket").agg(
        F.avg("avg_useful_per_review").alias("avg_useful"),
        F.count("*").alias("users_count"),
    )
