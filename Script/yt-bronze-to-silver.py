import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, year, month, dayofmonth


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# =========================
# READ CSV (OLD DATA)
# =========================

csv_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"",
        "withHeader": True,
        "separator": ","
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://yt-analytics-all/INvideos.csv"],
        "recurse": True
    },
    transformation_ctx="csv_source"
)


# =========================
# READ BRONZE JSON (NEW DATA)
# =========================

bronze_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://yt-analytics-all/bronze/"],
        "recurse": True
    },
    transformation_ctx="bronze_source"
)


# =========================
# APPLY SAME SCHEMA TO CSV
# =========================

csv = ApplyMapping.apply(
    frame=csv_dyf,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "extraction_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "string", "channel_id", "string"),
        ("publish_time", "string", "published_date", "string"),
        ("views", "string", "views", "string"),
        ("likes", "string", "likes", "string"),
        ("comment_count", "string", "comments", "string"),
        ("description", "string", "description", "string")
    ],
    transformation_ctx="csv_mapping"
)


# =========================
# APPLY SAME SCHEMA TO BRONZE
# =========================

bronze = ApplyMapping.apply(
    frame=bronze_dyf,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("extraction_date", "string", "extraction_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("channel_id", "string", "channel_id", "string"),
        ("published_date", "string", "published_date", "string"),
        ("views", "string", "views", "string"),
        ("likes", "string", "likes", "string"),
        ("comments", "string", "comments", "string"),
        ("description", "string", "description", "string")
    ],
    transformation_ctx="bronze_mapping"
)


# =========================
# CONVERT TO DATAFRAME
# =========================

csv_df = csv.toDF()
bronze_df = bronze.toDF()


# =========================
# FIX MISSING DESCRIPTION
# =========================

if "description" not in bronze_df.columns:
    bronze_df = bronze_df.withColumn(
        "description",
        lit("")
    )


# =========================
# FORCE SAME COLUMN ORDER
# =========================

columns = [
    "video_id",
    "extraction_date",
    "title",
    "channel_title",
    "channel_id",
    "published_date",
    "views",
    "likes",
    "comments",
    "description"
]


csv_df = csv_df.select(columns)
bronze_df = bronze_df.select(columns)


# =========================
# UNION
# =========================

final_df = csv_df.unionByName(bronze_df)


# =========================
# CAST FINAL TYPES
# =========================

final_df = (
    final_df
    .withColumn("views", col("views").cast("bigint"))
    .withColumn("likes", col("likes").cast("bigint"))
    .withColumn("comments", col("comments").cast("bigint"))
)


# =========================
# ADD PARTITION COLUMNS
# =========================

final_df = final_df.withColumn(
    "Year",
    year(col("published_date"))
)

final_df = final_df.withColumn(
    "Month",
    month(col("published_date"))
)

final_df = final_df.withColumn(
    "Day",
    dayofmonth(col("published_date"))
)


# =========================
# WRITE SILVER
# =========================

final_dyf = DynamicFrame.fromDF(
    final_df,
    glueContext,
    "final_dyf"
)


sink = glueContext.getSink(
    path="s3://yt-analytics-all/silver/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=["Year", "Month", "Day"],
    enableUpdateCatalog=True,
    transformation_ctx="silver_sink"
)

sink.setCatalogInfo(
    catalogDatabase="yt-data-db",
    catalogTableName="yt-etl-data"
)

sink.setFormat(
    "glueparquet",
    compression="snappy"
)

sink.writeFrame(final_dyf)


job.commit()
