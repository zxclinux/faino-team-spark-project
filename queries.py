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

    users = user_df.select("user_id", "friends_count")

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

    users = user_df.select("user_id", "friends_count")

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


def q13_monthly_review_growth(ds):
    review, business = ds["review"], ds["business"]

    top_cities = (
        business.groupBy("city")
        .agg(F.sum("review_count").alias("total_reviews"))
        .orderBy(F.desc("total_reviews"))
        .limit(10)
    )

    joined = (
        review
        .join(business.select("business_id", "city"), on="business_id")
        .join(top_cities.select("city"), on="city")
        .filter(F.col("date").isNotNull())
    )

    monthly = (
        joined
        .withColumn("year_month", F.date_format("date", "yyyy-MM"))
        .groupBy("city", "year_month")
        .agg(F.count("*").alias("review_count"))
    )

    w = Window.partitionBy("city").orderBy("year_month")
    return (
        monthly
        .withColumn("prev_month_count", F.lag("review_count").over(w))
        .withColumn(
            "mom_growth_pct",
            F.round(
                (F.col("review_count") - F.col("prev_month_count"))
                / F.col("prev_month_count") * 100,
                2,
            ),
        )
        .orderBy("city", "year_month")
    )


def q14_review_seasonality(ds):
    review = ds["review"]

    monthly = (
        review
        .filter(F.col("date").isNotNull())
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .groupBy("year", "month")
        .agg(F.count("*").alias("review_count"))
    )

    w = Window.partitionBy("year").orderBy(F.desc("review_count"))
    return (
        monthly
        .withColumn("month_rank", F.rank().over(w))
        .orderBy("year", "month_rank")
    )


def q15_useful_votes_by_stars(ds):
    review, business = ds["review"], ds["business"]

    extreme = (
        business
        .filter((F.col("stars") == 5.0) | (F.col("stars") == 1.0))
        .select("business_id", F.col("stars").alias("business_stars"))
    )

    return (
        review
        .join(extreme, on="business_id")
        .groupBy("business_stars")
        .agg(
            F.round(F.avg("useful"), 2).alias("avg_useful"),
            F.round(F.avg("funny"), 2).alias("avg_funny"),
            F.round(F.avg("cool"), 2).alias("avg_cool"),
            F.count("*").alias("total_reviews"),
        )
        .orderBy("business_stars")
    )


def q16_star_distribution_by_state(ds):
    open_biz = ds["business"].filter(F.col("is_open") == True)

    return (
        open_biz
        .groupBy("state", "stars")
        .agg(F.count("*").alias("business_count"))
        .orderBy("state", "stars")
    )


def q17_city_size_vs_stars(ds):
    open_biz = ds["business"].filter(F.col("is_open") == True)

    return (
        open_biz
        .groupBy("state", "city")
        .agg(
            F.round(F.avg("stars"), 2).alias("avg_stars"),
            F.count("*").alias("business_count"),
        )
        .filter(F.col("business_count") >= 10)
        .orderBy(F.desc("business_count"))
    )


def q18_popular_user_categories(ds):
    review, user, business = ds["review"], ds["user"], ds["business"]

    popular = user.filter(F.col("fans") > 10).select("user_id")

    return (
        review
        .join(popular, on="user_id")
        .join(business.select("business_id", "categories"), on="business_id")
        .select(F.explode("categories").alias("category"))
        .groupBy("category")
        .agg(F.count("*").alias("review_count"))
        .orderBy(F.desc("review_count"))
    )


def q19_top5_businesses_per_city(datasets: dict):
    """Топ-5 бізнесів за зірковим рейтингом у кожному місті."""
    business_df = datasets["business"]

    w = Window.partitionBy("city").orderBy(
        F.desc("stars"), F.desc("review_count"),
    )
    ranked = business_df.withColumn("rank", F.row_number().over(w))
    return ranked.filter(F.col("rank") <= 5).select(
        "city", "rank", "name", "business_id", "stars", "review_count",
    )


def q20_rating_trend_first_vs_last_year(datasets: dict):
    """Тренд рейтингу бізнесу: середня оцінка за перший рік відгуків vs останній."""
    business_df = datasets["business"]
    review_df = datasets["review"]

    yearly = (
        review_df
        .withColumn("year", F.year("date"))
        .groupBy("business_id", "year")
        .agg(F.avg("stars").alias("year_avg_stars"))
    )

    # at least 2 distinct years to define a trend
    multi_year = yearly.groupBy("business_id").agg(
        F.countDistinct("year").alias("years_covered"),
    ).filter(F.col("years_covered") >= 2)

    yearly = yearly.join(multi_year, on="business_id")

    w_full = (
        Window.partitionBy("business_id")
        .orderBy("year")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    with_endpoints = (
        yearly
        .withColumn("first_year", F.first("year").over(w_full))
        .withColumn("last_year", F.last("year").over(w_full))
        .withColumn("first_year_avg", F.first("year_avg_stars").over(w_full))
        .withColumn("last_year_avg", F.last("year_avg_stars").over(w_full))
    )

    per_biz = with_endpoints.select(
        "business_id", "first_year", "last_year",
        "first_year_avg", "last_year_avg", "years_covered",
    ).dropDuplicates(["business_id"])

    per_biz = per_biz.withColumn(
        "trend", F.round(F.col("last_year_avg") - F.col("first_year_avg"), 2),
    )

    return (
        per_biz.join(business_df.select("business_id", "name", "state"), on="business_id")
        .orderBy(F.desc(F.abs(F.col("trend"))))
        .select(
            "name", "business_id", "state",
            "first_year", "first_year_avg",
            "last_year", "last_year_avg",
            "trend", "years_covered",
        )
    )


def q21_top_categories_by_tips(datasets: dict):
    """Категорії бізнесу, що отримують найбільше порад (tips)."""
    business_df = datasets["business"]
    tip_df = datasets["tip"]

    tips_per_biz = tip_df.groupBy("business_id").agg(
        F.count("*").alias("tip_count"),
    )
    joined = business_df.join(tips_per_biz, on="business_id")
    exploded = joined.select(
        F.explode("categories").alias("category"),
        "tip_count",
    )
    return (
        exploded.groupBy("category")
        .agg(F.sum("tip_count").alias("total_tips"))
        .orderBy(F.desc("total_tips"))
        .limit(20)
    )


def q22_top10_categories_by_avg_rating(datasets: dict):
    """Топ-10 категорій бізнесу за середнім рейтингом (≥50 бізнесами)."""
    business_df = datasets["business"]

    exploded = business_df.select(
        F.explode("categories").alias("category"),
        "stars",
    )
    return (
        exploded.groupBy("category")
        .agg(
            F.avg("stars").alias("avg_stars"),
            F.count("*").alias("biz_count"),
        )
        .filter(F.col("biz_count") >= 50)
        .orderBy(F.desc("avg_stars"))
        .limit(10)
    )


def q23_top10_users_by_useful_votes(datasets: dict):
    """Топ-10 користувачів, чиї відгуки отримали найбільше голосів useful."""
    review_df = datasets["review"]
    user_df = datasets["user"]

    useful_per_user = review_df.groupBy("user_id").agg(
        F.sum("useful").alias("total_useful_on_reviews"),
        F.count("*").alias("review_count_actual"),
    )
    return (
        useful_per_user.join(
            user_df.select("user_id", "name", "fans"), on="user_id",
        )
        .orderBy(F.desc("total_useful_on_reviews"))
        .limit(10)
        .select(
            "user_id", "name", "total_useful_on_reviews",
            "review_count_actual", "fans",
        )
    )


def q24_elite_users_most_funny_reviews(datasets: dict):
    """Elite-користувачі з найбільшою кількістю funny-відгуків."""
    review_df = datasets["review"]
    user_df = datasets["user"]

    elite_users = user_df.filter(F.size("elite") > 0).select("user_id", "name")

    funny_reviews = review_df.filter(F.col("funny") > 0).select(
        "user_id", F.col("funny").alias("review_funny"),
    )

    return (
        elite_users.join(funny_reviews, on="user_id")
        .groupBy("user_id", "name")
        .agg(
            F.count("*").alias("funny_review_count"),
            F.sum("review_funny").alias("total_funny_votes"),
        )
        .orderBy(F.desc("funny_review_count"))
        .limit(20)
    )
