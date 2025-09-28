import os
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract, to_date, lit
)

args = getResolvedOptions(sys.argv, ["BRONZE_BASE", "SILVER_BASE"])
bronze_base = args["BRONZE_BASE"].rstrip("/")       # e.g. s3://wistia-video-analytics/bronze OR .../bronze/wistia
silver_base = args["SILVER_BASE"].rstrip("/")       # e.g. s3://wistia-video-analytics/silver

# If caller passed .../bronze (no trailing /wistia), add it (your bucket has /wistia/ under bronze)
if not bronze_base.endswith("/wistia"):
    bronze_root = bronze_base + "/wistia"
else:
    bronze_root = bronze_base

spark = (
    SparkSession.builder
    .appName("BronzeToSilverGlue")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"[B2S] BRONZE_BASE={bronze_base}  (using {bronze_root})")
print(f"[B2S] SILVER_BASE={silver_base}")

# -----------------------------------------------------------------------------
# Helpers to safely parse media_id and dt from the S3 path
# -----------------------------------------------------------------------------
# Path examples (from your screenshots):
#   .../bronze/wistia/medias/dt=2025-09-19/medias_v08dlrgr7v_2025-09-19.ndjson
#   .../bronze/wistia/metadata/dt=2025-09-19/metadata_gskhw4w4lm_2025-09-19.ndjson
#   .../bronze/wistia/visitors/dt=2025-09-19/visitors_v08dlrgr7v_page1_20.ndjson
#
# media_id pattern: the token right after "<kind>_" consisting of [a-z0-9]+
# dt pattern: the partition folder "dt=YYYY-MM-DD"
MEDIA_ID_FROM_PATH = r".*/(?:medias|metadata|visitors)_([a-z0-9]+).*\.ndjson$"
DT_FROM_PATH       = r".*/dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/.*"

def with_ids(df):
    return (
        df
        .withColumn("_source", input_file_name())
        .withColumn("media_id", regexp_extract(col("_source"), MEDIA_ID_FROM_PATH, 1))
        .withColumn("dt", regexp_extract(col("_source"), DT_FROM_PATH, 1))
        .withColumn("event_date", to_date(col("dt")))
    )

# -----------------------------------------------------------------------------
# Read Bronze
# -----------------------------------------------------------------------------
medias_raw = spark.read.json(f"{bronze_root}/medias/dt=*/*.ndjson")
metadata_raw = spark.read.json(f"{bronze_root}/metadata/dt=*/*.ndjson")
visitors_raw = spark.read.json(f"{bronze_root}/visitors/dt=*/*.ndjson")

medias_raw = with_ids(medias_raw)
metadata_raw = with_ids(metadata_raw)
visitors_raw = with_ids(visitors_raw)

print("[B2S] Loaded rows:",
      "medias=", medias_raw.count(),
      "metadata=", metadata_raw.count(),
      "visitors=", visitors_raw.count())

# Quick structural sanity checks
missing_mid_visitors = visitors_raw.filter(col("media_id") == "").count()
if missing_mid_visitors > 0:
    raise RuntimeError(f"[B2S] FATAL: {missing_mid_visitors} visitor rows missing media_id parsed from path.")

# -----------------------------------------------------------------------------
# Build Silver tables
# -----------------------------------------------------------------------------
# dim_media from metadata (richer than medias payload), dedup by media_id
dim_media = (
    metadata_raw.select(
        col("media_id"),
        col("name").alias("title"),
        col("duration").alias("duration_seconds"),
        col("created").alias("created_at"),
        col("updated").alias("updated_at"),
        "status",
        "tags",
        col("project.id").alias("project_id"),
        col("project.name").alias("project_name"),
        col("thumbnail.url").alias("thumbnail_url"),
        col("event_date")
    )
    .dropDuplicates(["media_id"])
)

# dim_visitor from visitors payload – dedup by visitor_id (called visitor_key in bronze)
dim_visitor = (
    visitors_raw.select(
        col("visitor_key").alias("visitor_id"),
        col("visitor_identity.email").alias("email"),
        col("visitor_identity.name").alias("name"),
        col("visitor_identity.org.name").alias("org_name"),
        col("visitor_identity.org.title").alias("org_title"),
        col("user_agent_details.browser").alias("browser"),
        col("user_agent_details.platform").alias("platform"),
        col("user_agent_details.mobile").alias("is_mobile"),
        col("created_at").alias("first_seen"),
        col("last_active_at").alias("last_active")
    )
    .dropDuplicates(["visitor_id"])
)

# fact_media_engagement – visitor-media daily counts
fact_media_engagement = (
    visitors_raw.select(
        "media_id",
        col("visitor_key").alias("visitor_id"),
        col("load_count"),
        col("play_count"),
        col("last_event_key"),
        col("created_at").alias("first_seen"),
        col("last_active_at").alias("last_active"),
        col("event_date")
    )
)

# -----------------------------------------------------------------------------
# Cross-check every media_id in visitors has a sibling in metadata (join sanity)
# -----------------------------------------------------------------------------
unknown_media = (
    fact_media_engagement.select("media_id").distinct()
    .join(dim_media.select("media_id").distinct(), on="media_id", how="left_anti")
    .count()
)
if unknown_media > 0:
    print(f"[B2S][WARN] {unknown_media} media_id(s) present in visitors with no matching metadata. "
          "Continuing, but this indicates incomplete bronze for that date.")

# Snapshot samples to logs
print("[B2S] Samples: dim_media")
dim_media.show(3, truncate=False)
print("[B2S] Samples: dim_visitor")
dim_visitor.show(3, truncate=False)
print("[B2S] Samples: fact_media_engagement")
fact_media_engagement.show(3, truncate=False)

# -----------------------------------------------------------------------------
# Write Silver (overwrite full folders — you can switch to dynamic overwrite later)
# -----------------------------------------------------------------------------
dim_media.write.mode("overwrite").parquet(os.path.join(silver_base, "dim_media"))
dim_visitor.write.mode("overwrite").parquet(os.path.join(silver_base, "dim_visitor"))
# Partition facts by dt for cheap scans
fact_media_engagement.write.mode("overwrite").partitionBy("event_date").parquet(
    os.path.join(silver_base, "fact_media_engagement")
)

print(f"[B2S] ✅ Silver written under {silver_base}")
spark.stop()
