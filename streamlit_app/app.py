# streamlit_app/app.py
import io
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
# Secrets / config
# ==============================
AWS = st.secrets["aws"]
S3CFG = st.secrets["s3"]
BUCKET = S3CFG["bucket"]
MEDIA_PREFIX = S3CFG["media_prefix"].rstrip("/") + "/"
VISITOR_PREFIX = S3CFG["visitor_prefix"].rstrip("/") + "/"

AWS_KEY = AWS["aws_access_key_id"]
AWS_SECRET = AWS["aws_secret_access_key"]
AWS_REGION = AWS.get("region_name", "us-east-1")

# boto3 session (for fallback & listing)
session = boto3.Session(
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)
s3_client = session.client("s3")

# ==============================
# Helpers
# ==============================
def _unify_watch_time(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure we have a 'watch_time_sec' column no matter the source schema."""
    if "watch_time" in df.columns:
        df["watch_time_sec"] = pd.to_numeric(df["watch_time"], errors="coerce").fillna(0)
    elif "seconds_watched" in df.columns:
        df["watch_time_sec"] = pd.to_numeric(df["seconds_watched"], errors="coerce").fillna(0)
    else:
        df["watch_time_sec"] = 0
    return df

def _ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    if "d" in df.columns:
        df["d"] = pd.to_datetime(df["d"], errors="coerce")
    return df

def _numericize(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

def _list_parquet_keys(prefix: str):
    """List ALL parquet keys under a prefix (handles pagination)."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".parquet"):
                keys.append(k)
    return keys

def _read_parquet_via_boto(keys):
    """Read a list of parquet S3 keys using boto3 get_object (no s3fs)."""
    frames = []
    for k in keys:
        obj = s3_client.get_object(Bucket=BUCKET, Key=k)
        by = io.BytesIO(obj["Body"].read())
        frames.append(pd.read_parquet(by))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

@st.cache_data(ttl=300)
def load_parquet_dataset(prefix: str) -> pd.DataFrame:
    """
    Preferred: s3fs glob read across year=/month=/day=/ partitions.
    Fallback: boto3 list + get_object.
    """
    # 1) Try s3fs (fast path)
    try:
        import s3fs  # ensure package available
        path = f"s3://{BUCKET}/{prefix}year=*/month=*/day=*/*.parquet"
        df = pd.read_parquet(
            path,
            storage_options={
                "key": AWS_KEY,
                "secret": AWS_SECRET,
                "client_kwargs": {"region_name": AWS_REGION},
            },
        )
        return df
    except Exception as e:
        st.warning(f"s3fs read failed, falling back to boto3. Reason: {type(e).__name__}")

    # 2) Fallback: boto3 list + read
    keys = _list_parquet_keys(prefix)
    if not keys:
        st.error(f"No parquet files found under s3://{BUCKET}/{prefix}")
        return pd.DataFrame()
    return _read_parquet_via_boto(keys)

# ==============================
# Load datasets
# ==============================
with st.spinner("Loading media parquet from S3…"):
    media_df = load_parquet_dataset(MEDIA_PREFIX)
with st.spinner("Loading visitor parquet from S3…"):
    visitor_df = load_parquet_dataset(VISITOR_PREFIX)

if media_df.empty and visitor_df.empty:
    st.error("No data loaded from S3. Verify prefixes and IAM permissions.")
    st.stop()

# Normalize schemas
media_df = _ensure_datetime(_numericize(_unify_watch_time(media_df), ["plays", "loads"]))
visitor_df = _ensure_datetime(_numericize(_unify_watch_time(visitor_df), ["plays"]))

# ==============================
# Sidebar filters
# ==============================
with st.sidebar:
    st.header("🔧 Filters")
    today = date.today()
    default_start = today - timedelta(days=30)
    dr = st.date_input("Date range", value=(default_start, today))
    # Streamlit returns a single date if user picks one date
    start_date, end_date = (dr if isinstance(dr, tuple) else (dr, dr))

    if "media_id" in media_df.columns:
        media_options = sorted(pd.Series(media_df["media_id"].dropna().astype(str).unique()).tolist())
        default_pick = media_options[: min(15, len(media_options))] or media_options
        selected_media = st.multiselect("Select Media", options=media_options, default=default_pick)
    else:
        selected_media = []

# Apply filters
def _apply_media_filters(df: pd.DataFrame) -> pd.DataFrame:
    if "d" in df.columns:
        df = df[(df["d"] >= pd.to_datetime(start_date)) & (df["d"] <= pd.to_datetime(end_date))]
    if selected_media and "media_id" in df.columns:
        df = df[df["media_id"].astype(str).isin(selected_media)]
    return df

def _apply_visitor_filters(df: pd.DataFrame) -> pd.DataFrame:
    if "d" in df.columns:
        df = df[(df["d"] >= pd.to_datetime(start_date)) & (df["d"] <= pd.to_datetime(end_date))]
    return df

media_f = _apply_media_filters(media_df.copy())
visitor_f = _apply_visitor_filters(visitor_df.copy())

# ==============================
# Media KPIs & trends
# ==============================
st.subheader("🎥 Media KPIs")

total_plays = int(media_f["plays"].sum()) if "plays" in media_f.columns else 0
total_loads = int(media_f["loads"].sum()) if "loads" in media_f.columns else 0
avg_play_rate = round(100 * total_plays / total_loads, 2) if total_loads else 0.0
total_watch_time_sec = int(media_f["watch_time_sec"].sum()) if "watch_time_sec" in media_f.columns else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Plays", f"{total_plays:,}")
c2.metric("Total Loads", f"{total_loads:,}")
c3.metric("Avg Play Rate", f"{avg_play_rate:.2f}%")
c4.metric("Total Watch Time (sec)", f"{total_watch_time_sec:,}")

# Trend (plays + watch time)
if {"d", "plays"}.issubset(media_f.columns):
    trend_media = (media_f.groupby("d", as_index=False)[["plays", "watch_time_sec"]]
                   .sum().sort_values("d"))
    st.line_chart(trend_media.set_index("d"))
else:
    st.info("Not enough columns in media data to render trend.")

# Media table
if {"media_id", "plays", "loads"}.issubset(media_f.columns):
    media_kpis = (
        media_f.groupby("media_id", as_index=False)[["plays", "loads", "watch_time_sec"]]
        .sum()
        .assign(play_rate=lambda d: (100 * d["plays"] / d["loads"]).round(2).fillna(0))
        .sort_values("plays", ascending=False)
    )
    st.markdown("#### Media performance")
    st.dataframe(media_kpis, use_container_width=True)
else:
    st.info("Expected columns `media_id`, `plays`, `loads` not all present in media dataset.")

# ==============================
# Visitor KPIs & trends
# ==============================
st.subheader("🧑‍🤝‍🧑 Visitor KPIs")

if "visitor_id" in visitor_f.columns:
    unique_visitors = visitor_f["visitor_id"].nunique()
else:
    unique_visitors = 0
total_visitor_plays = int(visitor_f["plays"].sum()) if "plays" in visitor_f.columns else 0
total_visitor_watch = int(visitor_f["watch_time_sec"].sum()) if "watch_time_sec" in visitor_f.columns else 0
avg_watch_per_visitor = round(total_visitor_watch / unique_visitors, 2) if unique_visitors else 0.0

vc1, vc2, vc3 = st.columns(3)
vc1.metric("Unique Visitors", f"{unique_visitors:,}")
vc2.metric("Visitor Plays", f"{total_visitor_plays:,}")
vc3.metric("Avg Watch / Visitor (sec)", f"{avg_watch_per_visitor:,}")

# Top visitors
if {"visitor_id", "plays"}.issubset(visitor_f.columns):
    top_visitors = (
        visitor_f.groupby("visitor_id", as_index=False)[["plays", "watch_time_sec"]]
        .sum()
        .sort_values(["plays", "watch_time_sec"], ascending=False)
        .head(25)
    )
    st.markdown("#### Top visitors")
    st.dataframe(top_visitors, use_container_width=True)
else:
    st.info("Expected columns `visitor_id`, `plays` not found in visitor dataset.")

# Visitor trend
if {"d", "plays"}.issubset(visitor_f.columns):
    trend_vis = visitor_f.groupby("d", as_index=False)[["plays", "watch_time_sec"]].sum().sort_values("d")
    st.line_chart(trend_vis.set_index("d"))
else:
    st.info("Not enough columns in visitor data to render trend.")

# ==============================
# Inspect
# ==============================
with st.expander("Peek media_df (raw)"):
    st.write(media_df.head(50))
with st.expander("Peek visitor_df (raw)"):
    st.write(visitor_df.head(50))
