import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, coalesce, to_date, substring,
    year, month, dayofmonth
)
from awsglue.utils import getResolvedOptions

# -----------------------
# Args & Spark
# -----------------------
args = getResolvedOptions(sys.argv, ["SILVER_BASE", "GOLD_BASE"])
silver_base = args["SILVER_BASE"].rstrip("/")
gold_base   = args["GOLD_BASE"].rstrip("/")

spark = (
    SparkSession.builder
    .appName("SilverToGoldGlue")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

print(f"[S2G] SILVER_BASE={silver_base}")
print(f"[S2G] GOLD_BASE  ={gold_base}")

# Paths
dim_media_path   = f"{silver_base}/dim_media"
dim_visitor_path = f"{silver_base}/dim_visitor"  # not used directly now, but keep for future
fact_path        = f"{silver_base}/fact_media_engagement"

gold_media_p   = f"{gold_base}/agg_media_daily_p"
gold_visitor_p = f"{gold_base}/agg_visitor_daily_p"

print(f"[S2G] OUT media_p   -> {gold_media_p}")
print(f"[S2G] OUT visitor_p -> {gold_visitor_p}")

# -----------------------
# Load silver
# -----------------------
dim_media = spark.read.parquet(dim_media_path)
fact      = spark.read.parquet(fact_path)

print("[S2G] SCHEMA fact_media_engagement")
fact.printSchema()
print("[S2G] SCHEMA dim_media")
dim_media.printSchema()

print(f"[S2G] COUNTS silver -> dim_media={dim_media.count()}, fact={fact.count()}")

# -----------------------
# Build reliable event_date
# -----------------------
def to_date_yyyy_mm_dd(colname):
    # '2025-09-19T...' -> take first 10 chars -> to_date()
    return to_date(substring(col(colname), 1, 10))

fact_evt = (
    fact
    .withColumn(
        "event_date",
        coalesce(
            to_date_yyyy_mm_dd("last_active"),
            to_date_yyyy_mm_dd("first_seen")
        )
    )
)

null_before = fact_evt.filter(col("event_date").isNull()).count()
print(f"[S2G] fact rows with NULL event_date BEFORE media fallback: {null_before}")

# Fallback to media.created_at (first 10 chars) when event_date is still null
dim_media_dates = dim_media.select(
    "media_id",
    to_date_yyyy_mm_dd("created_at").alias("media_created_date")
)

fact_evt = (
    fact_evt
    .join(dim_media_dates, on="media_id", how="left")
    .withColumn("event_date", coalesce(col("event_date"), col("media_created_date")))
    .drop("media_created_date")
)

null_after = fact_evt.filter(col("event_date").isNull()).count()
print(f"[S2G] fact rows with NULL event_date AFTER media fallback: {null_after}")

# Optional: show a small sample to confirm
print("[S2G] SAMPLE fact_evt")
for r in fact_evt.limit(5).collect():
    print(r)

# -----------------------
# MEDIA-LEVEL daily aggregation (uses loads & plays directly)
# -----------------------
media_daily = (
    fact_evt
    .groupBy("media_id", "event_date")
    .agg(
        _sum(col("load_count")).alias("total_loads"),
        _sum(col("play_count")).alias("total_plays")
    )
    .withColumn("year",  year(col("event_date")))
    .withColumn("month", month(col("event_date")))
    .withColumn("day",   dayofmonth(col("event_date")))
)

print("[S2G] SCHEMA media_daily")
media_daily.printSchema()
md_cnt = media_daily.count()
print(f"[S2G] media_daily rows: {md_cnt}")

(
    media_daily
    .write.mode("append")
    .partitionBy("year", "month", "day")
    .parquet(gold_media_p)
)
print("[S2G] ✅ Wrote media_daily_p to S3")

# -----------------------
# VISITOR-LEVEL daily aggregation
# (no visitor_email; just visitor_id, event_date, interactions/plays)
# -----------------------
visitor_daily = (
    fact_evt
    .groupBy("visitor_id", "event_date")
    .agg(
        _sum(col("play_count")).alias("media_interactions"),
        _sum(col("play_count")).alias("total_plays")
    )
    .withColumn("year",  year(col("event_date")))
    .withColumn("month", month(col("event_date")))
    .withColumn("day",   dayofmonth(col("event_date")))
)

print("[S2G] SCHEMA visitor_daily")
visitor_daily.printSchema()
vd_cnt = visitor_daily.count()
print(f"[S2G] visitor_daily rows: {vd_cnt}")

(
    visitor_daily
    .write.mode("append")
    .partitionBy("year", "month", "day")
    .parquet(gold_visitor_p)
)
print("[S2G] ✅ Wrote visitor_daily_p to S3")
print("[S2G] DONE")
