import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count as _count

# -------------------------------------------------
# Parse local arguments
# -------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--SILVER_BASE", required=True)
parser.add_argument("--GOLD_BASE", required=True)
parser.add_argument("--mode", default="overwrite")
parser.add_argument("--coalesce", type=int, default=1)
args = parser.parse_args()

silver_base = args.SILVER_BASE
gold_base   = args.GOLD_BASE

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()


# -------------------------------------------------
# Paths
# -------------------------------------------------
dim_media_path   = os.path.join(silver_base, "dim_media")
dim_visitor_path = os.path.join(silver_base, "dim_visitor")
fact_path        = os.path.join(silver_base, "fact_media_engagement")

gold_media_path   = os.path.join(gold_base, "agg_media_daily")
gold_visitor_path = os.path.join(gold_base, "agg_visitor_daily")

print(">>> Loading Silver parquet...")
df_media   = spark.read.parquet(dim_media_path)
df_visitor = spark.read.parquet(dim_visitor_path)
df_fact    = spark.read.parquet(fact_path)

# -------------------------------------------------
# Aggregate: media engagement per day
# -------------------------------------------------
agg_media = (
    df_fact.withColumn("event_date", to_date("first_seen"))
           .groupBy("media_id", "event_date")
           .agg(
               _sum("play_count").alias("total_plays"),
               _sum("load_count").alias("total_loads")
           )
)

# -------------------------------------------------
# Aggregate: visitor engagement per day
# -------------------------------------------------
agg_visitor = (
    df_fact.withColumn("event_date", to_date("first_seen"))
           .groupBy("visitor_id", "event_date")
           .agg(
               _count("media_id").alias("media_interactions"),
               _sum("play_count").alias("total_plays"),
               _sum("load_count").alias("total_loads")
           )
)

print(">>> Writing Gold outputs...")
agg_media.write.mode("overwrite").parquet(gold_media_path)
agg_visitor.write.mode("overwrite").parquet(gold_visitor_path)

print(f"✅ Gold written to {gold_base}")
spark.stop()
