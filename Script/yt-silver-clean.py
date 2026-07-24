import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    when,
    year,
    month,
    dayofmonth,
    row_number,
    to_date
)
from pyspark.sql.window import Window

# -----------------------------
# Initialize Glue
# -----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------
# Read Existing Silver Data
# -----------------------------
silver_df = spark.read.parquet("s3://yt-analytics-all/silver/")

# Remove whitespace
for c in ["video_id", "title", "channel_title"]:
    silver_df = silver_df.withColumn(c, trim(col(c)))

# Correct data types
silver_df = (
    silver_df
    .withColumn("views", col("views").cast("long"))
    .withColumn("likes", col("likes").cast("long"))
    .withColumn("comments", col("comments").cast("long"))
    .withColumn("published_date", to_date(col("published_date")))
)

# Remove NULLs from required columns
silver_df = silver_df.dropna(subset=[
    "video_id",
    "title",
    "channel_title",
    "published_date",
    "views",
    "likes",
    "comments"
])

# Fix negative values
silver_df = (
    silver_df
    .withColumn("views", when(col("views") < 0, 0).otherwise(col("views")))
    .withColumn("likes", when(col("likes") < 0, 0).otherwise(col("likes")))
    .withColumn("comments", when(col("comments") < 0, 0).otherwise(col("comments")))
)

# Likes cannot exceed views
silver_df = silver_df.withColumn(
    "likes",
    when(col("likes") > col("views"), col("views"))
    .otherwise(col("likes"))
)

# Comments cannot exceed views
silver_df = silver_df.withColumn(
    "comments",
    when(col("comments") > col("views"), col("views"))
    .otherwise(col("comments"))
)

# Remove duplicates
window = Window.partitionBy(
    "video_id",
    "extraction_date"
).orderBy(col("published_date").desc())

silver_df = (
    silver_df
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Recreate partition columns
silver_df = (
    silver_df
    .withColumn("year", year(col("published_date")))
    .withColumn("month", month(col("published_date")))
    .withColumn("day", dayofmonth(col("published_date")))
)

# -----------------------------
# Write Clean Data
# -----------------------------
(
    silver_df.write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet("s3://yt-analytics-all/silver_clean/")
)

print("Silver data cleaned successfully.")

job.commit()
