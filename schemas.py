from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    MapType,
)


class BusinessSchema:
    SCHEMA = StructType(
        [
            StructField("business_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("stars", DoubleType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("is_open", IntegerType(), True),
            StructField("attributes", MapType(StringType(), StringType(), True), True),
            StructField("categories", StringType(), True),
            StructField("hours", MapType(StringType(), StringType(), True), True),
        ]
    )


class ReviewSchema:
    SCHEMA = StructType(
        [
            StructField("review_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("stars", DoubleType(), True),
            StructField("date", StringType(), True),
            StructField("text", StringType(), True),
            StructField("useful", IntegerType(), True),
            StructField("funny", IntegerType(), True),
            StructField("cool", IntegerType(), True),
        ]
    )


class UserSchema:
    SCHEMA = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("yelping_since", StringType(), True),
            StructField("friends", StringType(), True),
            StructField("useful", IntegerType(), True),
            StructField("funny", IntegerType(), True),
            StructField("cool", IntegerType(), True),
            StructField("fans", IntegerType(), True),
            StructField("elite", StringType(), True),
            StructField("average_stars", DoubleType(), True),
            StructField("compliment_hot", IntegerType(), True),
            StructField("compliment_more", IntegerType(), True),
            StructField("compliment_profile", IntegerType(), True),
            StructField("compliment_cute", IntegerType(), True),
            StructField("compliment_list", IntegerType(), True),
            StructField("compliment_note", IntegerType(), True),
            StructField("compliment_plain", IntegerType(), True),
            StructField("compliment_cool", IntegerType(), True),
            StructField("compliment_funny", IntegerType(), True),
            StructField("compliment_writer", IntegerType(), True),
            StructField("compliment_photos", IntegerType(), True),
        ]
    )


class CheckinSchema:
    SCHEMA = StructType(
        [
            StructField("business_id", StringType(), True),
            StructField("date", StringType(), True),
        ]
    )


class TipSchema:
    SCHEMA = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("date", StringType(), True),
            StructField("compliment_count", IntegerType(), True),
        ]
    )


SCHEMAS = {
    "business": BusinessSchema.SCHEMA,
    "review": ReviewSchema.SCHEMA,
    "user": UserSchema.SCHEMA,
    "checkin": CheckinSchema.SCHEMA,
    "tip": TipSchema.SCHEMA,
}
