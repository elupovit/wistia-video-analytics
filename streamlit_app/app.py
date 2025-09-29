# streamlit_app/app.py
import streamlit as st
import pandas as pd
from datetime import date, timedelta

# ==============================
# Page config
# ==============================
st.set_page_config(page_title="Wistia Video Analytics — Gold KPIs", layout="wide")
st.title("📊 Wistia Video Analytics — Gold KPIs from S3 Parquet")
st.caption("Data source: Gold Parquet files in S3 (no Athena)")

# ==============================
# Config from secrets
# ==============================
BUCKET = st.secrets["s3"]["bucket"]
MEDIA_PREFIX = st.secrets["s3"]["media_prefix"]
VISITOR_PREFIX = st.secrets["s3"]["visitor_prefix"]

AWS_KEY = st.secrets["aws"]["aws_access_key_id"]
AWS_SECRET = st.secrets["aws"]["aws_secret_access_key"]
AWS_REGION = st.secrets["aws"]["region_name"]

# ==============================
# Data loading
# ==============================
@st.cache_data(ttl=300)
def load_parquet_dataset(prefix: str) -> pd.DataFrame:
    """
    Load all parquet files from a prefix (handles year/month/day partitions).
    """
    path = f"s3://{BUCKET}/{prefix}*/*.parquet"
    return pd.read_parquet(
        path,
        storage_options={
            "key": AWS_KEY,
            "secret": AWS_SECRET,
            "client_kwargs": {"region_name": AWS_REGION},
        },
    )

media_df = load_parquet_dataset(MEDIA_PREFIX)
visitor_df = load_parquet_dataset(VISITOR_PREFIX)

# ==============================
# Sidebar filters
# ==============================
with st.sidebar:
    st.header("🔧 Filters")
    today = date.today()
    default_start = today - timedelta(days=30)

    dr = st.date_input("Date range", value=(default_start, today))
    start_date, end_date = dr

    media_options = sorted(media_df["media_id"].dropna().unique().tolist())
    selected_media = st.multiselect(
        "Select Media",
        options=media_options,
        default=media_options,
    )

# ==============================
# Filtered data
# ==============================
media_filtered = media_df[
    (media_df["d"] >= pd.to_datetime(start_date))
    & (media_df["d"] <= pd.to_datetime(end_date))
    & (media_df["media_id"].isin(selected_media))
]

visitor_filtered = visitor_df[
    (visitor_df["d"] >= pd.to_datetime(start_date))
    & (visitor_df["d"] <= pd.to_datetime(end_date))
]

# ==============================
# Media KPIs
# ==============================
st.subheader("🎥 Media KPIs")

total_plays = media_filtered["plays"].sum()
total_loads = media_filtered["loads"].sum()
total_watch_time = media_filtered["watch_time"].sum()
avg_play_rate = round(100 * total_plays / total_loads, 2) if total_loads else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Plays", f"{total_plays:,}")
c2.metric("Total Loads", f"{total_loads:,}")
c3.metric("Avg Play Rate", f"{avg_play_rate:.2f}%")
c4.metric("Total Watch Time (hrs)", f"{total_watch_time/3600:.1f}")

# Trend chart
st.line_chart(
    media_filtered.groupby("d")[["plays", "loads", "watch_time"]].sum()
)

# ==============================
# Visitor KPIs
# ==============================
st.subheader("🧑‍🤝‍🧑 Visitor KPIs")

unique_visitors = visitor_filtered["visitor_id"].nunique()
avg_visits = visitor_filtered.groupby("visitor_id")["sessions"].sum().mean()
total_sessions = visitor_filtered["sessions"].sum()

c1, c2, c3 = st.columns(3)
c1.metric("Unique Visitors", f"{unique_visitors:,}")
c2.metric("Avg Sessions per Visitor", f"{avg_visits:.2f}")
c3.metric("Total Sessions", f"{total_sessions:,}")

# Top visitors
st.markdown("#### Top Visitors (by sessions)")
top_visitors = (
    visitor_filtered.groupby("visitor_id")["sessions"].sum()
    .reset_index()
    .sort_values("sessions", ascending=False)
    .head(10)
)
st.dataframe(top_visitors)

# Visitor trend
st.line_chart(
    visitor_filtered.groupby("d")[["sessions"]].sum()
)
