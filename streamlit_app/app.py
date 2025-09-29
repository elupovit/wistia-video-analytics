# streamlit_app/app.py
import os
from datetime import date, timedelta

import streamlit as st
import pandas as pd
import boto3

# ==============================
# Page config
# ==============================
st.set_page_config(page_title="Wistia Video Analytics — Gold KPIs", layout="wide")
st.title("📊 Wistia Video Analytics — Gold KPIs from S3 Parquet")
st.caption("Data source: Gold Parquet files in S3 (no Athena)")

# ==============================
# AWS session from Streamlit secrets
# ==============================
AWS_SECRETS = st.secrets["aws"]
session = boto3.Session(
    aws_access_key_id=AWS_SECRETS["aws_access_key_id"],
    aws_secret_access_key=AWS_SECRETS["aws_secret_access_key"],
    region_name=AWS_SECRETS.get("region_name", "us-east-1"),
)
s3 = session.client("s3")

# ==============================
# S3 locations (bucket + prefixes)
# ==============================
BUCKET = st.secrets.get("s3", {}).get("bucket", "wistia-video-analytics")
GOLD_ROOT = st.secrets.get("s3", {}).get("gold_prefix", "gold")
DATASETS = {
    "media": f"{GOLD_ROOT}/agg_media_daily_p",
    "visitor": f"{GOLD_ROOT}/agg_visitor_daily_p",
}

# ==============================
# Helpers for partition discovery
# ==============================
def _ls_prefix(prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix, Delimiter="/")
    prefixes, objects = [], []
    for page in pages:
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("$folder$"):
                continue
            objects.append(obj)
    return prefixes, objects

def _parse_partition_triplet(year_p: str, month_p: str, day_p: str) -> date:
    y = int(year_p.strip("/").split("=")[1])
    m = int(month_p.strip("/").split("=")[1])
    d = int(day_p.strip("/").split("=")[1])
    return date(y, m, d)

@st.cache_data(ttl=300)
def list_available_partition_dates(dataset_prefix: str):
    dates = []
    years, _ = _ls_prefix(f"{dataset_prefix}/")
    for y in years:
        months, _ = _ls_prefix(y)
        for m in months:
            days, _ = _ls_prefix(m)
            for d in days:
                try:
                    dates.append(_parse_partition_triplet(y, m, d))
                except Exception:
                    pass
    return sorted(set(dates))

@st.cache_data(ttl=300)
def list_parquet_keys_for_date(dataset_prefix: str, the_date: date):
    y = f"year={the_date.year}/"
    m = f"month={the_date.month}/"
    d = f"day={the_date.day}/"
    partition_prefix = f"{dataset_prefix}/{y}{m}{d}"
    _, objects = _ls_prefix(partition_prefix)
    return [o["Key"] for o in objects if o["Key"].endswith(".parquet")]

# ==============================
# Loading parquet
# ==============================
def _read_parquet_via_boto(keys):
    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_parquet(obj["Body"])
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _read_parquet_via_s3fs(paths):
    import s3fs  # optional
    return pd.read_parquet(paths)

@st.cache_data(ttl=300)
def load_dataset(dataset_key: str, start: date, end: date, scope: str):
    dataset_prefix = DATASETS[dataset_key]
    available = list_available_partition_dates(dataset_prefix)
    if not available:
        return pd.DataFrame(), available

    if scope == "latest":
        selected_dates = [max(available)]
    else:
        selected_dates = [d for d in available if start <= d <= end] or [max(available)]

    keys = []
    for d in selected_dates:
        keys.extend(list_parquet_keys_for_date(dataset_prefix, d))

    s3_urls = [f"s3://{BUCKET}/{k}" for k in keys]
    try:
        df = _read_parquet_via_s3fs(s3_urls)
    except Exception:
        df = _read_parquet_via_boto(keys)

    if "d" in df.columns:
        df["d"] = pd.to_datetime(df["d"]).dt.date
    for col in ["loads", "plays", "unique_visitors", "seconds_watched"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df, available

# ==============================
# Sidebar filters
# ==============================
with st.sidebar:
    st.header("🔧 Filters")
    scope = st.radio("Load", options=["Date range", "Latest day"], index=0, horizontal=True)
    today = date.today()
    default_start = today - timedelta(days=30)

    if scope == "Date range":
        dr = st.date_input("Date range", value=(default_start, today))
        if isinstance(dr, tuple):
            start_date, end_date = dr
        else:
            start_date, end_date = dr, dr
    else:
        start_date, end_date = today, today

# ==============================
# Load datasets
# ==============================
with st.spinner("Loading gold data from S3..."):
    media_df, _ = load_dataset("media", start_date, end_date, "latest" if scope == "Latest day" else "range")
    visitor_df, _ = load_dataset("visitor", start_date, end_date, "latest" if scope == "Latest day" else "range")

if media_df.empty and visitor_df.empty:
    st.warning("No data found in the selected range.")
    st.stop()

# Media filter
with st.sidebar:
    if "media_id" in media_df.columns:
        media_options = sorted(pd.Series(media_df["media_id"].dropna().unique()).astype(str).tolist())
        selected_media = st.multiselect("Select Media", options=media_options, default=media_options[: min(15, len(media_options))])
        if selected_media:
            media_df = media_df[media_df["media_id"].astype(str).isin(selected_media)]

# ==============================
# KPIs
# ==============================
def _safe_sum(df, col):
    return int(df[col].sum()) if col in df.columns else 0

summary_mask = pd.Series([True] * len(media_df))
if "d" in media_df.columns and scope == "Date range":
    summary_mask &= (media_df["d"] >= start_date) & (media_df["d"] <= end_date)
summary_df = media_df[summary_mask].copy()

total_plays = _safe_sum(summary_df, "plays")
total_loads = _safe_sum(summary_df, "loads")
total_watch_time = _safe_sum(summary_df, "seconds_watched")
avg_play_rate = round(100 * total_plays / total_loads, 2) if total_loads else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Plays", f"{total_plays:,}")
c2.metric("Total Loads", f"{total_loads:,}")
c3.metric("Avg Play Rate", f"{avg_play_rate:.2f}%")
c4.metric("Total Watch Time (sec)", f"{total_watch_time:,}")

# ==============================
# Media KPIs
# ==============================
st.subheader("🎬 Media KPIs")
if {"media_id", "plays", "loads"}.issubset(set(media_df.columns)):
    media_kpis = (
        media_df.groupby("media_id", as_index=False)[["plays", "loads", "seconds_watched"]].sum()
        .assign(play_rate=lambda d: (100 * d["plays"] / d["loads"]).round(2).fillna(0))
        .sort_values("plays", ascending=False)
    )
    st.dataframe(media_kpis, use_container_width=True)
else:
    st.info("Missing expected columns for Media KPIs.")

# ==============================
# Top visitors
# ==============================
st.subheader("🧑‍💻 Top Visitors")
if {"visitor_id", "plays"}.issubset(set(visitor_df.columns)):
    top_visitors = (
        visitor_df.groupby("visitor_id", as_index=False)[["plays", "seconds_watched"]].sum()
        .sort_values("plays", ascending=False)
        .head(25)
    )
    st.dataframe(top_visitors, use_container_width=True)
else:
    st.info("Missing expected columns for Top Visitors.")

# ==============================
# Trends
# ==============================
st.subheader("📈 Trends")
if {"d", "plays"}.issubset(set(media_df.columns)):
    trend = media_df.groupby("d", as_index=False)[["plays", "seconds_watched"]].sum().sort_values("d")
    st.line_chart(trend.set_index("d"))
else:
    st.info("Missing expected columns for Trends.")

# ==============================
# Raw data
# ==============================
with st.expander("Peek media_df"):
    st.write(media_df.head(50))
with st.expander("Peek visitor_df"):
    st.write(visitor_df.head(50))
