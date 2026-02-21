from pyspark.sql import SparkSession
from schemas import BusinessSchema, CheckinSchema, ReviewSchema, TipSchema, UserSchema

SCHEMAS = {
    "business": BusinessSchema.SCHEMA,
    "review": ReviewSchema.SCHEMA,
    "user": UserSchema.SCHEMA,
    "checkin": CheckinSchema.SCHEMA,
    "tip": TipSchema.SCHEMA,
}

def load_datasets(spark: SparkSession, data_dir: str) -> dict:
    datasets = {}
    for name, schema in SCHEMAS.items():
        path = f"{data_dir}/yelp_academic_dataset_{name}.json"
        datasets[name] = spark.read.schema(schema).json(path)
    return datasets