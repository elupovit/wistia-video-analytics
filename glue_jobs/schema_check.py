import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaCheck").getOrCreate()

# Paths (local or S3 bronze data)
# Local mock data for schema testing
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
bronze_base = os.path.join(repo_root, "mock_data", "wistia", "bronze")


metadata_path = os.path.join(bronze_base, "media_stats/*/*.ndjson")
medias_path   = os.path.join(bronze_base, "medias/*/*.ndjson")
visitors_path = os.path.join(bronze_base, "visitors/*/*.ndjson")


print(">>> Loading Bronze JSONs...")
df_metadata = spark.read.json(metadata_path, multiLine=False)
df_medias   = spark.read.json(medias_path,   multiLine=False)
df_visitors = spark.read.json(visitors_path, multiLine=False)

print("\n>>> Metadata schema")
df_metadata.printSchema()
df_metadata.show(5, truncate=False)

print("\n>>> Medias schema")
df_medias.printSchema()
df_medias.show(5, truncate=False)

print("\n>>> Visitors schema")
df_visitors.printSchema()
df_visitors.show(5, truncate=False)

spark.stop()
