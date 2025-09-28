#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bronze → Silver for Wistia datasets.

Creates 4 Silver tables:
  - dim_media                ← bronze/metadata
  - dim_visitor              ← bronze/visitors
  - fact_media_engagement    ← bronze/visitors   (visitor × media × dt)
  - fact_media_daily_raw     ← bronze/medias     (clean passthrough; for Gold QA)

Usage (local dev):
  spark-submit glue_jobs/bronze_to_silver.py \
    --bronze-base s3://wistia-video-analytics/bronze/wistia \
    --silver-base s3://wistia-video-analytics/silver/wistia \
    --dt 2025-09-16 \
    --format ndjson \
    --mode overwrite \
    --coalesce 1

Or against local copies:
  spark-submit glue_jobs/bronze_to_silver.py \
    --bronze-base mock_data/bronze \
    --silver-base mock_output/silver \
    --format ndjson \
    --mode overwrite \
    --coalesce 1
"""
import argparse
from typing import List, Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

DATASETS = ["medias", "metadata", "visitors"]

# -------------------------
# Spark helpers
# -------------------------
def build_spark(app_name: str = "bronze_to_silver") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def paths_for(base: str, dataset: str, dt: Optional[str]) -> List[str]:
    base = base.rstrip("/")
    if dt:
        return [f"{base}/{dataset}/dt={dt}/*"]
    # fallback: accept both partitioned and flat layouts
    return [f"{base}/{dataset}/dt=*/*", f"{base}/{dataset}/*"]

def read_json(spark: SparkSession, paths: List[str], fmt: str):
    reader = spark.read.option("mode", "PERMISSIVE")
    if fmt == "ndjson":
        return reader.json(paths)
    elif fmt == "json":
        return reader.option("multiline", True).json(paths)
    else:
        raise ValueError("--format must be 'ndjson' or 'json'")

def ensure_dt(df):
    # If dt column is missing, extract from file path "dt=YYYY-MM-DD"
    if "dt" not in df.columns:
        df = df.withColumn(
            "dt",
            F.regexp_extract(F.input_file_name(), r"dt=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1)
        )
    return df

def parse_ts(col):
    # Try multiple ISO-8601 patterns (Z or +/- offset)
    return F.coalesce(
        F.to_timestamp(col),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

def coalesce_str(*cols):
    return F.coalesce(*[F.col(c).cast("string") for c in cols])

# -------------------------
# Normalizers
# -------------------------
def normalize_dim_media(metadata_raw):
    df = ensure_dt(metadata_raw)
    # Prefer hashed_id as the stable business key (matches your samples)
    return (
        df.select(
            F.col("hashed_id").alias("media_id"),
            F.col("name").alias("title"),
            F.col("duration").cast("double").alias("duration_seconds"),
            parse_ts(F.col("created")).alias("created_at"),
            parse_ts(F.col("updated")).alias("updated_at"),
            F.col("status").cast("string").alias("status"),
            F.col("tags").alias("tags"),
            F.col("project.id").cast("string").alias("project_id"),
            F.col("project.name").cast("string").alias("project_name"),
            F.col("thumbnail.url").cast("string").alias("thumbnail_url"),
            F.col("dt"),
        )
        .dropDuplicates(["media_id", "dt"])
    )

def normalize_dim_visitor(visitors_raw):
    df = ensure_dt(visitors_raw)
    # media_id is now injected by Lambda; keep it for lineage but not the PK
    return (
        df.select(
            F.col("visitor_key").alias("visitor_id"),
            F.col("visitor_identity.email").alias("email"),
            F.col("visitor_identity.name").alias("name"),
            F.col("visitor_identity.org.name").alias("org_name"),
            F.col("visitor_identity.org.title").alias("org_title"),
            F.col("user_agent_details.browser").alias("browser"),
            F.col("user_agent_details.platform").alias("platform"),
            F.col("user_agent_details.mobile").alias("is_mobile"),
            parse_ts(F.col("created_at")).alias("first_seen"),
            parse_ts(F.col("last_active_at")).alias("last_active"),
            F.col("dt"),
        )
        .dropDuplicates(["visitor_id", "dt"])
    )

def normalize_fact_media_engagement(visitors_raw):
    df = ensure_dt(visitors_raw)
    # Defensive: if media_id somehow missing in a row, parse from file name
    df = df.withColumn(
        "media_id",
        F.when(
            F.col("media_id").isNotNull() & (F.length("media_id") > 0),
            F.col("media_id")
        ).otherwise(
            F.regexp_extract(F.input_file_name(), r"visitors_([A-Za-z0-9]+)_page", 1)
        )
    )
    return (
        df.select(
            F.col("media_id").cast("string").alias("media_id"),
            F.col("visitor_key").cast("string").alias("visitor_id"),
            F.col("load_count").cast("long").alias("load_count"),
            F.col("play_count").cast("long").alias("play_count"),
            F.col("last_event_key").cast("string").alias("last_event_key"),
            parse_ts(F.col("created_at")).alias("first_seen"),
            parse_ts(F.col("last_active_at")).alias("last_active"),
            F.col("dt"),
        )
        .where(F.col("media_id").isNotNull() & F.col("visitor_id").isNotNull())
        .dropDuplicates(["media_id", "visitor_id", "dt"])
    )

def normalize_fact_media_daily_raw(medias_raw, metadata_dim_for_enrich=None):
    # medias_raw is the aggregate stats per media from /v1/stats/medias/{id}
    df = ensure_dt(medias_raw)
    # Add media_id from file name if not present in JSON (your sample didn't include it)
    df = df.withColumn(
        "media_id",
        F.regexp_extract(F.input_file_name(), r"medias_([A-Za-z0-9]+)", 1)
    )
    out = (
        df.select(
            F.col("media_id").cast("string").alias("media_id"),
            F.col("load_count").cast("long").alias("load_count"),
            F.col("play_count").cast("long").alias("play_count"),
            F.col("play_rate").cast("double").alias("play_rate"),
            F.col("hours_watched").cast("double").alias("hours_watched"),
            F.col("engagement").cast("double").alias("engagement"),
            F.col("visitors").cast("long").alias("visitors"),
            F.col("dt"),
        )
        .where(F.col("media_id").isNotNull())
        .dropDuplicates(["media_id", "dt"])
    )
    # Optional enrichment with title if desired (commented out by default)
    # if metadata_dim_for_enrich is not None:
    #     out = (out.alias("f")
    #            .join(metadata_dim_for_enrich.select("media_id", "title").alias("m"),
    #                  on="media_id", how="left"))
    return out

# -------------------------
# IO
# -------------------------
def write_parquet(df, base: str, table: str, mode: str = "overwrite", coalesce: Optional[int] = None):
    if df is None:
        return
    if coalesce:
        df = df.coalesce(int(coalesce))
    (
        df.write.mode(mode)
        .format("parquet")
        .partitionBy("dt")
        .option("compression", "snappy")
        .save(f"{base.rstrip('/')}/{table}")
    )

# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-base", required=True)
    ap.add_argument("--silver-base", required=True)
    ap.add_argument("--dt", required=False, help="YYYY-MM-DD; if omitted, processes all available")
    ap.add_argument("--format", default="ndjson", choices=["ndjson", "json"])
    ap.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    ap.add_argument("--coalesce", default=None)
    args = ap.parse_args()

    spark = build_spark()

    # Load Bronze frames (tolerate both partitioned and flat local copies)
    metadata_raw = read_json(spark, paths_for(args.bronze_base, "metadata", args.dt), args.format)
    medias_raw   = read_json(spark, paths_for(args.bronze_base, "medias",   args.dt), args.format)
    visitors_raw = read_json(spark, paths_for(args.bronze_base, "visitors", args.dt), args.format)

    # Build Silver
    dim_media   = normalize_dim_media(metadata_raw)
    dim_visitor = normalize_dim_visitor(visitors_raw)
    fact_vis    = normalize_fact_media_engagement(visitors_raw)
    fact_media  = normalize_fact_media_daily_raw(medias_raw)

    # Validations (fail loud if key dims are empty)
    if dim_media.rdd.isEmpty():
        raise RuntimeError("dim_media is empty — check Bronze metadata and --format/--dt.")
    if dim_visitor.rdd.isEmpty():
        raise RuntimeError("dim_visitor is empty — check Bronze visitors and --format/--dt.")
    if fact_vis.rdd.isEmpty():
        raise RuntimeError("fact_media_engagement is empty — confirm visitors contain media_id and dt.")

    # Write
    write_parquet(dim_media,   args.silver_base, "dim_media",              args.mode, args.coalesce)
    write_parquet(dim_visitor, args.silver_base, "dim_visitor",            args.mode, args.coalesce)
    write_parquet(fact_vis,    args.silver_base, "fact_media_engagement",  args.mode, args.coalesce)
    write_parquet(fact_media,  args.silver_base, "fact_media_daily_raw",   args.mode, args.coalesce)

    spark.stop()

if __name__ == "__main__":
    main()
