import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Local paths
BRONZE_BASE = "mock_data/bronze"
SILVER_BASE = "mock_output/silver"

spark = SparkSession.builder.appName("BronzeToSilverLocal").getOrCreate()

print(">>> Loading Bronze ndjson...")
df_metadata = spark.read.json(os.path.join(BRONZE_BASE, "metadata/*/*.ndjson"))
df_medias   = spark.read.json(os.path.join(BRONZE_BASE, "medias/*/*.ndjson"))
df_visitors = spark.read.json(os.path.join(BRONZE_BASE, "visitors/*/*.ndjson"))

# Transform to cleaned Silver tables
dim_media = (
    df_metadata
      .select("hashed_id", "name", "duration", "created", "updated", "status", "tags",
              col("project.id").alias("project_id"),
              col("project.name").alias("project_name"),
              col("thumbnail.url").alias("thumbnail_url"))
      .withColumnRenamed("hashed_id", "media_id")
      .withColumnRenamed("name", "title")
      .withColumnRenamed("duration", "duration_seconds")
      .withColumnRenamed("created", "created_at")
      .withColumnRenamed("updated", "updated_at")
)

dim_visitor = (
    df_visitors
      .select(col("visitor_key").alias("visitor_id"),
              col("visitor_identity.email").alias("email"),
              col("visitor_identity.name").alias("name"),
              col("visitor_identity.org.name").alias("org_name"),
              col("visitor_identity.org.title").alias("org_title"),
              col("user_agent_details.browser").alias("browser"),
              col("user_agent_details.platform").alias("platform"),
              col("user_agent_details.mobile").alias("is_mobile"),
              "created_at", "last_active_at")
      .withColumnRenamed("created_at", "first_seen")
      .withColumnRenamed("last_active_at", "last_active")
)

fact_media_engagement = (
    df_visitors
      .select(col("media_id"),
              col("visitor_key").alias("visitor_id"),
              "load_count", "play_count", "last_event_key",
              "created_at", "last_active_at")
      .withColumnRenamed("created_at", "first_seen")
      .withColumnRenamed("last_active_at", "last_active")
)

print(">>> Writing Silver parquet...")
dim_media.write.mode("overwrite").parquet(os.path.join(SILVER_BASE, "dim_media"))
dim_visitor.write.mode("overwrite").parquet(os.path.join(SILVER_BASE, "dim_visitor"))
fact_media_engagement.write.mode("overwrite").parquet(os.path.join(SILVER_BASE, "fact_media_engagement"))

print(f"✅ Silver written locally to {SILVER_BASE}")
spark.stop()
