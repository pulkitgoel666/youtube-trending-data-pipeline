import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    when,
    count,
    sum as spark_sum,
    row_number
)

from pyspark.sql.window import Window


# ==========================================
# Initialize Glue
# ==========================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args["JOB_NAME"],
    args
)


# ==========================================
# Read Silver
# ==========================================

print("Reading Silver data")

silver_df = spark.read.parquet(
    "s3://yt-analytics-all/silver/"
)


print("Original rows:")
print(silver_df.count())


# ==========================================
# Cleaning
# ==========================================

print("Cleaning data")


# Remove spaces from text columns

text_columns = [
    "video_id",
    "title",
    "channel_title",
    "description"
]


for c in text_columns:

    if c in silver_df.columns:

        silver_df = silver_df.withColumn(
            c,
            trim(col(c))
        )


# Remove rows with missing important values

required_columns = [
    "video_id",
    "title",
    "channel_title",
    "published_date",
    "views",
    "likes",
    "comments"
]


silver_df = silver_df.dropna(
    subset=required_columns
)



# Convert metrics

silver_df = (
    silver_df
    .withColumn(
        "views",
        col("views").cast("long")
    )
    .withColumn(
        "likes",
        col("likes").cast("long")
    )
    .withColumn(
        "comments",
        col("comments").cast("long")
    )
)



# Fix negative values

silver_df = (
    silver_df
    .withColumn(
        "views",
        when(col("views") < 0, 0)
        .otherwise(col("views"))
    )
    .withColumn(
        "likes",
        when(col("likes") < 0, 0)
        .otherwise(col("likes"))
    )
    .withColumn(
        "comments",
        when(col("comments") < 0, 0)
        .otherwise(col("comments"))
    )
)



# Fix impossible metrics

silver_df = (
    silver_df
    .withColumn(
        "likes",
        when(
            col("likes") > col("views"),
            col("views")
        )
        .otherwise(col("likes"))
    )
    .withColumn(
        "comments",
        when(
            col("comments") > col("views"),
            col("views")
        )
        .otherwise(col("comments"))
    )
)



# ==========================================
# Remove duplicates
# ==========================================

print("Removing duplicates")


window = Window.partitionBy(
    "video_id",
    "extraction_date"
).orderBy(
    col("published_date").desc()
)


silver_df = (
    silver_df
    .withColumn(
        "row_number",
        row_number().over(window)
    )
    .filter(
        col("row_number") == 1
    )
    .drop(
        "row_number"
    )
)



# ==========================================
# Final Validation
# ==========================================

print("Final validation")


errors = []


if silver_df.count() == 0:

    errors.append(
        "No records after cleaning"
    )


for c in required_columns:

    nulls = (
        silver_df
        .filter(
            col(c).isNull()
        )
        .count()
    )

    if nulls > 0:

        errors.append(
            f"{c} still contains NULL values"
        )



if errors:

    print("VALIDATION FAILED")

    for e in errors:
        print(e)

    raise Exception(
        "Gold creation stopped"
    )


print("Validation successful")



# ==========================================
# Write Gold
# ==========================================


print("Writing Gold tables")



# Gold 1: Clean videos


(
    silver_df
    .write
    .mode("overwrite")
    .partitionBy(
        "year",
        "month",
        "day"
    )
    .parquet(
        "s3://yt-analytics-all/gold/videos/"
    )
)



# Gold 2: Channel analytics


gold_channel = (
    silver_df
    .groupBy(
        "channel_title"
    )
    .agg(
        count("video_id")
        .alias("total_videos"),

        spark_sum("views")
        .alias("total_views"),

        spark_sum("likes")
        .alias("total_likes"),

        spark_sum("comments")
        .alias("total_comments")
    )
)


(
    gold_channel
    .write
    .mode("overwrite")
    .parquet(
        "s3://yt-analytics-all/gold/channel_analysis/"
    )
)



# Gold 3: Daily analytics


gold_daily = (
    silver_df
    .groupBy(
        "year",
        "month",
        "day"
    )
    .agg(
        count("video_id")
        .alias("total_videos"),

        spark_sum("views")
        .alias("total_views")
    )
)


(
    gold_daily
    .write
    .mode("overwrite")
    .parquet(
        "s3://yt-analytics-all/gold/daily_analysis/"
    )
)



print("GOLD CREATED SUCCESSFULLY")


job.commit()
