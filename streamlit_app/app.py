# streamlit_app/app.py
import streamlit as st
import pandas as pd
import boto3
from datetime import date, timedelta

# ==============================
# Page config
# ==============================
st.set_page_config(page_title="Wistia Video Analytics — Gold KPIs", layout="wide")
st.title("📊 Wistia Video Analytics — Gold KPIs from S3 Parquet")
st.caption("Data source: Gold Parquet files in S3 (no Athena)")

# ==============================
# AWS S3 Session
# ==============================
session = boto3.Session(
    aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
    aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
    region_name=st.secrets["aws"]["region_name"]
)
s3 = session.client("s3")

BUCKET = st.secrets["s3"]["bucket"]
MEDIA_PREFIX = st.secrets["s3"]["media_prefix"]
VISITOR_PREFIX = st.secrets["s3"]["visitor_prefix"]

# ==============================
# Load parquet files
# ==============================
@st.cache_data(ttl=300)
def load_latest_parquet(prefix: str) -> pd.DataFrame:
    """Find and load the latest parquet file under a given S3 prefix."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]
    if not keys:
        st.error(f"No parquet files found under {prefix}")
        return pd.DataFrame()

    # Sort by object key (since year/month/day structure makes later keys > earlier ones)
    latest_key = sorted(keys)[-1]
    obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
    return pd.read_parquet(obj["Body"])

# Load datasets
media_df = load_latest_parquet(MEDIA_PREFIX)
visitor_df = load_latest_parquet(VISITOR_PREFIX)

if media_df.empty or visitor_df.empty:
    st.stop()

# ==============================
# Filters
# ==============================
with st.sidebar:
    st.header("🔧 Filters")
    today = date.today()
    default_start = today - timedelta(days=30)
    dr = st.date_input("Date Range", value=(default_start, today))
    start_date, end_date = dr

    media_options = sorted(media_df["media_id"].dropna().unique().tolist())
    selected_media = st.multiselect("Select Media", options=media_options, default=media_options)

# Apply filters
media_df["d"] = pd.to_datetime(media_df["d"])
filtered_media = media_df[
    (media_df["d"] >= pd.to_datetime(start_date)) &
    (media_df["d"] <= pd.to_datetime(end_date)) &
    (media_df["media_id"].isin(selected_media))
]

# ==============================
# KPIs
# ==============================
total_plays = int(filtered_media["plays"].sum())
total_loads = int(filtered_media["loads"].sum())
avg_play_rate = round(100 * total_plays / total_loads, 2) if total_loads else 0
total_watch_time = int(filtered_media["watch_time"].sum()) if "watch_time" in filtered_media.columns else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Plays", f"{total_plays:,}")
c2.metric("Total Loads", f"{total_loads:,}")
c3.metric("Avg Play Rate", f"{avg_play_rate:.2f}%")
c4.metric("Total Watch Time (sec)", f"{total_watch_time:,}")

# ==============================
# Visitor KPIs
# ==============================
visitor_df["d"] = pd.to_datetime(visitor_df["d"])
filtered_visitors = visitor_df[
    (visitor_df["d"] >= pd.to_datetime(start_date)) &
    (visitor_df["d"] <= pd.to_datetime(end_date))
]

unique_visitors = filtered_visitors["visitor_id"].nunique()
avg_watch_time_per_visitor = round(
    filtered_visitors.groupby("visitor_id")["watch_time"].sum().mean(), 2
) if "watch_time" in filtered_visitors.columns else 0

st.subheader("👥 Visitor Metrics")
vc1, vc2 = st.columns(2)
vc1.metric("Unique Visitors", f"{unique_visitors:,}")
vc2.metric("Avg Watch Time per Visitor (sec)", f"{avg_watch_time_per_visitor:,}")

# ==============================
# Trends
# ==============================
st.subheader("📈 Daily Trends")

daily_trend = (
    filtered_media.groupby("d")[["plays", "loads"]].sum().reset_index()
    if not filtered_media.empty else pd.DataFrame()
)

if not daily_trend.empty:
    st.line_chart(daily_trend.set_index("d"))
else:
    st.info("No trend data available for selected filters.")

# ==============================
# Media Table
# ==============================
st.subheader("🎬 Media-Level Performance")
media_summary = (
    filtered_media.groupby("media_id")
    .agg(
        total_plays=("plays", "sum"),
        total_loads=("loads", "sum"),
        total_watch_time=("watch_time", "sum") if "watch_time" in filtered_media.columns else ("plays", "sum")
    )
    .reset_index()
)
st.dataframe(media_summary)
