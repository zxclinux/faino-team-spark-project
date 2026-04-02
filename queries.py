from pyspark.sql import functions as F
from pyspark.sql.window import Window


def monthly_review_growth(ds):
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


def review_seasonality(ds):
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


def useful_votes_by_stars(ds):
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


def star_distribution_by_state(ds):
    open_biz = ds["business"].filter(F.col("is_open") == True)

    return (
        open_biz
        .groupBy("state", "stars")
        .agg(F.count("*").alias("business_count"))
        .orderBy("state", "stars")
    )


def city_size_vs_stars(ds):
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


def popular_user_categories(ds):
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


def top_tip_users(ds):
    tip, user = ds["tip"], ds["user"]

    tip_counts = (
        tip.groupBy("user_id")
        .agg(F.count("*").alias("tip_count"))
    )

    return (
        tip_counts
        .join(user, on="user_id")
        .select(
            "user_id", "name", "tip_count",
            "review_count", "fans", "average_stars", "friends_count",
        )
        .orderBy(F.desc("tip_count"))
        .limit(10)
    )
