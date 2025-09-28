import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count as _count

# Local paths
SILVER_BASE = "mock_output/silver"
GOLD_BASE   = "mock_output/gold"

spark = SparkSession.builder.appName("SilverToGoldLocal").getOrCreate()

# Paths
dim_media_path   = os.path.join(SILVER_BASE, "dim_media")
dim_visitor_path = os.path.join(SILVER_BASE, "dim_visitor")
fact_path        = os.path.join(SILVER_BASE, "fact_media_engagement")

gold_media_path   = os.path.join(GOLD_BASE, "agg_media_daily")
gold_visitor_path = os.path.join(GOLD_BASE, "agg_visitor_daily")

print(">>> Loading Silver parquet...")
df_media   = spark.read.parquet(dim_media_path)
df_visitor = spark.read.parquet(dim_visitor_path)
df_fact    = spark.read.parquet(fact_path)

# Aggregate: media engagement per day
agg_media = (
    df_fact.withColumn("event_date", to_date("first_seen"))
           .groupBy("media_id", "event_date")
           .agg(
               _sum("play_count").alias("total_plays"),
               _sum("load_count").alias("total_loads")
           )
)

# Aggregate: visitor engagement per day
agg_visitor = (
    df_fact.withColumn("event_date", to_date("first_seen"))
           .groupBy("visitor_id", "event_date")
           .agg(
               _count("media_id").alias("media_interactions"),
               _sum("play_count").alias("total_plays")
           )
)

print(">>> Writing Gold outputs...")
agg_media.write.mode("overwrite").parquet(gold_media_path)
agg_visitor.write.mode("overwrite").parquet(gold_visitor_path)

print(f"✅ Gold written locally to {GOLD_BASE}")
spark.stop()
