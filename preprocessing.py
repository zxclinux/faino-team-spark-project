from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, DoubleType, LongType, FloatType
from pyspark.sql.functions import col, count, when, isnan


def print_general_stats(name, df):
    row_count = df.count()
    col_count = len(df.columns)

    print(f"\n{'='*60}")
    print(f" {name.upper()} — General Statistics")
    print(f" Rows: {row_count:,} | Columns: {col_count}")
    print(f"{'='*60}")

    df.printSchema()

    null_exprs = []
    for field in df.schema.fields:
        col_name = field.name
        if isinstance(field.dataType, (DoubleType, FloatType)):
            null_exprs.append(
                count(when(col(col_name).isNull() | isnan(col(col_name)), col_name)).alias(col_name)
            )
        else:
            null_exprs.append(
                count(when(col(col_name).isNull(), col_name)).alias(col_name)
            )

    null_counts = df.select(null_exprs).collect()[0]

    print(f" {'Column':<35} {'Type':<15} {'Non-Null':>10} {'Null %':>8}")
    print(f" {'-'*35} {'-'*15} {'-'*10} {'-'*8}")
    for field in df.schema.fields:
        nulls = null_counts[field.name]
        non_null = row_count - nulls
        null_pct = (nulls / row_count * 100) if row_count > 0 else 0
        print(f" {field.name:<35} {str(field.dataType):<15} {non_null:>10,} {null_pct:>7.2f}%")

    df.show(5, truncate=50)


def print_numeric_stats(name, df):
    numeric_types = (IntegerType, DoubleType, LongType, FloatType)
    numeric_cols = [
        field.name for field in df.schema.fields
        if isinstance(field.dataType, numeric_types)
    ]

    if not numeric_cols:
        print(f"\n [{name.upper()}] No numeric columns found.\n")
        return

    print(f"\n{'='*60}")
    print(f" {name.upper()} — Numeric Feature Statistics")
    print(f" Numeric columns: {', '.join(numeric_cols)}")
    print(f"{'='*60}")

    df.select(numeric_cols).summary().show(truncate=False)
