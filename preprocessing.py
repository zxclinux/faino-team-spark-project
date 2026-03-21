from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def preprocess_review(df: DataFrame) -> DataFrame:
    # 3) date -> DateType
    df = df.withColumn("date", F.to_date("date"))

    # 4) drop text (not in schema, but just in case), drop unused raw
    # review schema has no text column — already excluded in schemas.py

    # 5) deduplicate by review_id
    df = df.dropDuplicates(["review_id"])

    return df


def preprocess_tip(df: DataFrame) -> DataFrame:
    # 3) date -> DateType
    df = df.withColumn("date", F.to_date("date"))

    # 4) drop text — not needed for any analytical question
    df = df.drop("text")

    # 5) deduplicate by (user_id, business_id, date)
    df = df.dropDuplicates(["user_id", "business_id", "date"])

    return df


def preprocess_user(df: DataFrame) -> DataFrame:
    # 3) yelping_since -> DateType
    df = df.withColumn("yelping_since", F.to_date("yelping_since"))

    # 3) elite -> array of ints (years), e.g. "2015,2016,2017" -> [2015, 2016, 2017]
    df = df.withColumn(
        "elite",
        F.when(
            (F.col("elite").isNull()) | (F.trim(F.col("elite")) == ""),
            F.array().cast("array<int>"),
        ).otherwise(
            F.transform(
                F.split(F.trim(F.col("elite")), ",\\s*"),
                lambda x: x.cast("int"),
            )
        ),
    )

    # 3) friends -> friends_count
    df = df.withColumn(
        "friends_count",
        F.when(
            (F.col("friends").isNull()) | (F.col("friends") == "None"),
            F.lit(0),
        ).otherwise(F.size(F.split(F.col("friends"), ",\\s*"))),
    )

    # 4) drop raw friends string, compliment_* columns
    compliment_cols = [c for c in df.columns if c.startswith("compliment_")]
    df = df.drop("friends", *compliment_cols)

    # 5) deduplicate by user_id
    df = df.dropDuplicates(["user_id"])

    return df


def preprocess_business(df: DataFrame, checkin_df: DataFrame) -> DataFrame:
    # 3) is_open -> BooleanType
    df = df.withColumn("is_open", F.col("is_open").cast("boolean"))

    # 3) categories string -> array, e.g. "Restaurants, Food" -> ["Restaurants", "Food"]
    df = df.withColumn(
        "categories",
        F.when(
            F.col("categories").isNotNull(),
            F.transform(
                F.split(F.col("categories"), ",\\s*"),
                lambda x: F.trim(x),
            ),
        ),
    )

    # 5) missing categories — fill with the most frequent category-set of the same state
    #    strategy: for each state, find the most common first category among businesses
    #    that DO have categories, then assign it as a single-element array
    state_mode = (
        df.filter(F.col("categories").isNotNull())
        .withColumn("first_cat", F.col("categories")[0])
        .groupBy("state", "first_cat")
        .agg(F.count("*").alias("cnt"))
    )
    w = Window.partitionBy("state").orderBy(F.desc("cnt"))
    state_mode = (
        state_mode.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("state").alias("_state"),
            F.col("first_cat").alias("_fill_cat"),
        )
    )
    df = df.join(state_mode, df["state"] == state_mode["_state"], "left")
    df = df.withColumn(
        "categories",
        F.when(F.col("categories").isNull(), F.array(F.col("_fill_cat"))).otherwise(
            F.col("categories")
        ),
    )
    df = df.drop("_state", "_fill_cat")

    # 3) attributes -> extract wifi as boolean (True = free wifi)
    #    values seen: "u'free'", "'free'", "free", "u'no'", "'no'", "no",
    #                 "u'paid'", "'paid'", "paid", "None", null
    df = df.withColumn("wifi", F.col("attributes")["WiFi"])
    df = df.withColumn(
        "has_free_wifi",
        F.lower(F.regexp_replace(F.col("wifi"), "[u'\"\\s]", "")).isin(
            "free"
        ),
    )
    df = df.drop("wifi")

    # 3) hours map -> array of day names when the business is open
    #    e.g. {"Monday": "9:0-17:0", "Tuesday": "9:0-17:0"} -> ["Monday", "Tuesday"]
    df = df.withColumn(
        "open_days",
        F.when(F.col("hours").isNotNull(), F.map_keys(F.col("hours"))).otherwise(
            F.array().cast("array<string>")
        ),
    )

    # 4) drop raw attributes and hours maps (parsed into has_free_wifi + open_days)
    df = df.drop("attributes", "hours")

    # 3) merge checkin_count from checkin table
    checkin_counts = checkin_df.withColumn(
        "checkin_count",
        F.size(F.split(F.col("date"), ",\\s*")),
    ).select("business_id", "checkin_count")

    # deduplicate checkin just in case
    checkin_counts = checkin_counts.dropDuplicates(["business_id"])

    df = df.join(checkin_counts, on="business_id", how="left")
    df = df.withColumn(
        "checkin_count",
        F.coalesce(F.col("checkin_count"), F.lit(0)),
    )

    # 5) deduplicate by business_id
    df = df.dropDuplicates(["business_id"])

    return df
