#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bronze validation script:
Reads raw NDJSON/JSON from Bronze layer (S3 or local),
prints schema + sample rows for medias, metadata, visitors.

Usage (local):
  spark-submit glue_jobs/bronze_validation.py \
    --bronze-base s3://wistia-video-analytics/bronze/wistia \
    --dt 2025-09-16 \
    --format ndjson
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATASETS = ["medias", "metadata", "visitors"]

def build_spark(app_name="bronze_validation"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def paths_for(bronze_base: str, dataset: str, dt: str):
    return [
        f"{bronze_base.rstrip('/')}/{dataset}/dt={dt}/*",
        f"{bronze_base.rstrip('/')}/{dataset}/*"
    ]

def read_json(spark, paths, fmt: str):
    reader = spark.read.option("mode", "PERMISSIVE")
    if fmt == "ndjson":
        return reader.json(paths)
    elif fmt == "json":
        return reader.option("multiline", True).json(paths)
    else:
        raise ValueError("--format must be 'ndjson' or 'json'")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-base", required=True, help="S3 base path for Bronze")
    ap.add_argument("--dt", required=False, help="Partition date (YYYY-MM-DD)")
    ap.add_argument("--dataset", default="all", choices=["all", "medias", "metadata", "visitors"])
    ap.add_argument("--format", default="ndjson", choices=["ndjson", "json"])
    args = ap.parse_args()

    spark = build_spark()
    targets = DATASETS if args.dataset == "all" else [args.dataset]

    for d in targets:
        pths = paths_for(args.bronze_base, d, args.dt or "*")
        df = read_json(spark, pths, args.format)

        # add dt from partition if missing
        if "dt" not in df.columns:
            df = df.withColumn(
                "dt",
                F.regexp_extract(F.input_file_name(), r"dt=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1)
            )

        print("\n======= ", d.upper(), " =======")
        df.printSchema()
        df.show(10, truncate=80)

    spark.stop()

if __name__ == "__main__":
    main()
