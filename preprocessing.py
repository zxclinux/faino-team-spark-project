from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def preprocess_review(df: DataFrame) -> DataFrame:
    # str "2018-01-09" -> DateType
    df = df.withColumn("date", F.to_date("date"))
    return df

def preprocess_tip(df: DataFrame) -> DataFrame:
    # str "2018-01-09" -> DateType
    df = df.withColumn("date", F.to_date("date"))
    # text is not used in any analytical question
    df = df.drop("text")
    return df

def preprocess_user(df: DataFrame) -> DataFrame:
    # str "2012-03-14" -> DateType
    df = df.withColumn("yelping_since", F.to_date("yelping_since"))

    # "2015,2016,2017" -> [2015, 2016, 2017]; null/"" -> empty array
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

    # comma-separated user_ids -> count; "None"/null -> 0
    df = df.withColumn(
        "friends_count",
        F.when(
            (F.col("friends").isNull()) | (F.col("friends") == "None"),
            F.lit(0),
        ).otherwise(F.size(F.split(F.col("friends"), ",\\s*"))),
    )

    # drop raw friends string and all 11 compliment_* columns (uninformative)
    compliment_cols = [c for c in df.columns if c.startswith("compliment_")]
    df = df.drop("friends", *compliment_cols)

    return df

def preprocess_business(df: DataFrame, checkin_df: DataFrame) -> DataFrame:
    # 0/1 -> true/false
    df = df.withColumn("is_open", F.col("is_open").cast("boolean"))

    # "Restaurants, Food, Italian" -> ["Restaurants", "Food", "Italian"]
    # drop ~0.07% rows where categories is null — too few to impute meaningfully
    df = df.withColumn("categories", F.split(F.col("categories"), ",\\s*"))
    df = df.filter(F.col("categories").isNotNull())

    # attributes["WiFi"] has Python 2 repr artifacts: u'free', u'no', u'paid'
    # strip u' prefix and ' suffix, then compare to "free" -> boolean
    df = df.withColumn(
        "has_free_wifi",
        F.regexp_replace(F.col("attributes")["WiFi"], "^u?'|'$", "") == "free",
    )

    # {"Monday": "9:0-17:0", ...} -> ["Monday", ...]; null stays null
    df = df.withColumn("open_days", F.map_keys(F.col("hours")))

    # raw maps replaced by has_free_wifi and open_days
    df = df.drop("attributes", "hours")

    # checkin table has one row per business with comma-separated timestamps
    # count timestamps and merge into business; businesses without checkins get 0
    checkin_counts = (
        checkin_df
        .withColumn("checkin_count", F.size(F.split(F.col("date"), ",\\s*")))
        .select("business_id", "checkin_count")
    )
    df = df.join(checkin_counts, on="business_id", how="left")
    df = df.withColumn("checkin_count", F.coalesce(F.col("checkin_count"), F.lit(0)))

    return df
