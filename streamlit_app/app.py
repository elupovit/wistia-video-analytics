# streamlit_app/app.py
import io
from datetime import date, timedelta

import streamlit as st
import pandas as pd
import boto3
from botocore.exceptions import ClientError

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

AWS_KEY = st.secrets.aws_credentials.AWS_ACCESS_KEY_ID
AWS_SECRET = st.secrets.aws_credentials.AWS_SECRET_ACCESS_KEY
AWS_REGION = st.secrets.aws_credentials.AWS_DEFAULT_REGION

# boto3 session/clients (force our creds/region explicitly)
session = boto3.Session(
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)
s3_client = session.client("s3")
sts_client = session.client("sts")

# ==============================
# Quick diagnostics (identity + simple S3 check)
# ==============================
idn = {}
try:
    idn = sts_client.get_caller_identity()
    st.success(
        f"🔐 Using AWS principal: **{idn.get('Arn', 'unknown')}** (Account {idn.get('Account', 'unknown')}), Region **{AWS_REGION}**"
    )
except Exception as e:
    st.error(f"Could not call STS GetCallerIdentity: {type(e).__name__}. Check keys/region in secrets.")
    st.stop()

def list_some_keys(prefix: str, limit: int = 5):
    keys = []
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, PaginationConfig={"MaxItems": limit}):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])
            if len(keys) >= limit:
                break
    except ClientError as e:
        st.error(
            "S3 List failed. Most common causes: wrong keys, wrong region, or bucket policy. "
            f"Error: {e.response.get('Error', {}).get('Code')} — {e.response.get('Error', {}).get('Message')}"
        )
        st.stop()
    return keys[:limit]

with st.expander("S3 connectivity check (debug)"):
    st.write(
        {
            "bucket": BUCKET,
            "region_used": AWS_REGION,
            "media_prefix": MEDIA_PREFIX,
            "visitor_prefix": VISITOR_PREFIX,
            "caller_arn": idn.get("Arn"),
        }
    )
    try:
        mpeek = list_some_keys(MEDIA_PREFIX, limit=3)
        vpeek = list_some_keys(VISITOR_PREFIX, limit=3)
        st.write({"media_keys_sample": mpeek, "visitor_keys_sample": vpeek})
        if not mpeek and not vpeek:
            st.warning("No parquet files found under the configured prefixes.")
    except SystemExit:
        raise
    except Exception as e:
        st.error(f"Unexpected error during S3 check: {type(e).__name__}: {e}")
        st.stop()

# ==============================
# Loader (boto3 only)
# ==============================
def _read_parquet_keys(keys):
    frames = []
    for k in keys:
        try:
            obj = s3_client.get_object(Bucket=BUCKET, Key=k)
            by = io.BytesIO(obj["Body"].read())
            frames.append(pd.read_parquet(by))
        except ClientError as e:
            st.error(f"GetObject failed for {k}: {e.response.get('Error', {}).get('Code')} — {e.response.get('Error', {}).get('Message')}")
            st.stop()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

@st.cache_data(ttl=300)
def load_dataset(prefix: str) -> pd.DataFrame:
    # Pull a reasonable batch to render the app; paginator ensures we get all if you want
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    if not keys:
        return pd.DataFrame()
    return _read_parquet_keys(keys)

def _unify_watch_time(df: pd.DataFrame) -> pd.DataFrame:
    if "watch_time" in df.columns:
        df["watch_time_sec"] = pd.to_numeric(df["watch_time"], errors="coerce").fillna(0)
    elif "seconds_watched" in df.columns:
        df["watch_time_sec"] = pd.to_numeric(df["seconds_watched"], errors="coerce").fillna(0)
    else:
        df["watch_time_sec"] = 0
    return df

def _prep(df: pd.DataFrame, numeric_cols) -> pd.DataFrame:
    if "d" in df.columns:
        df["d"] = pd.to_datetime(df["d"], errors="coerce")
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return _unify_watch_time(df)

# ==============================
# Load data
# ==============================
with st.spinner("Loading media parquet from S3…"):
    media_df = load_dataset(MEDIA_PREFIX)
with st.spinner("Loading visitor parquet from S3…"):
    visitor_df = load_dataset(VISITOR_PREFIX)

if media_df.empty and visitor_df.empty:
    st.error("No data loaded from S3. Verify prefixes and that gold parquet exists.")
    st.stop()

media_df = _prep(media_df, ["plays", "loads"])
visitor_df = _prep(visitor_df, ["plays"])

# ==============================
# Sidebar filters
# ==============================
with st.sidebar:
    st.header("🔧 Filters")
    today = date.today()
    default_start = today - timedelta(days=30)
    dr = st.date_input("Date range", value=(default_start, today))
    start_date, end_date = (dr if isinstance(dr, tuple) else (dr, dr))

    if "media_id" in media_df.columns:
        media_options = sorted(pd.Series(media_df["media_id"].dropna().astype(str).unique()).tolist())
        default_pick = media_options[: min(len(media_options), 15)] or media_options
        selected_media = st.multiselect("Select Media", options=media_options, default=default_pick)
    else:
        selected_media = []

def filt_media(df):
    if "d" in df.columns:
        df = df[(df["d"] >= pd.to_datetime(start_date)) & (df["d"] <= pd.to_datetime(end_date))]
    if selected_media and "media_id" in df.columns:
        df = df[df["media_id"].astype(str).isin(selected_media)]
    return df

def filt_vis(df):
    if "d" in df.columns:
        df = df[(df["d"] >= pd.to_datetime(start_date)) & (df["d"] <= pd.to_datetime(end_date))]
    return df

media_f = filt_media(media_df.copy())
visitor_f = filt_vis(visitor_df.copy())

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

if {"d", "plays"}.issubset(media_f.columns):
    trend_media = (media_f.groupby("d", as_index=False)[["plays", "watch_time_sec"]]
                   .sum().sort_values("d"))
    st.line_chart(trend_media.set_index("d"))

if {"media_id", "plays", "loads"}.issubset(media_f.columns):
    media_kpis = (
        media_f.groupby("media_id", as_index=False)[["plays", "loads", "watch_time_sec"]]
        .sum()
        .assign(play_rate=lambda d: (100 * d["plays"] / d["loads"]).round(2).fillna(0))
        .sort_values("plays", ascending=False)
    )
    st.markdown("#### Media performance")
    st.dataframe(media_kpis, use_container_width=True)

# ==============================
# Visitor KPIs & trends
# ==============================
st.subheader("🧑‍🤝‍🧑 Visitor KPIs")
unique_visitors = visitor_f["visitor_id"].nunique() if "visitor_id" in visitor_f.columns else 0
total_visitor_plays = int(visitor_f["plays"].sum()) if "plays" in visitor_f.columns else 0
total_visitor_watch = int(visitor_f["watch_time_sec"].sum()) if "watch_time_sec" in visitor_f.columns else 0
avg_watch_per_visitor = round(total_visitor_watch / unique_visitors, 2) if unique_visitors else 0.0

vc1, vc2, vc3 = st.columns(3)
vc1.metric("Unique Visitors", f"{unique_visitors:,}")
vc2.metric("Visitor Plays", f"{total_visitor_plays:,}")
vc3.metric("Avg Watch / Visitor (sec)", f"{avg_watch_per_visitor:,}")

if {"visitor_id", "plays"}.issubset(visitor_f.columns):
    top_visitors = (
        visitor_f.groupby("visitor_id", as_index=False)[["plays", "watch_time_sec"]]
        .sum()
        .sort_values(["plays", "watch_time_sec"], ascending=False)
        .head(25)
    )
    st.markdown("#### Top visitors")
    st.dataframe(top_visitors, use_container_width=True)

if {"d", "plays"}.issubset(visitor_f.columns):
    trend_vis = visitor_f.groupby("d", as_index=False)[["plays", "watch_time_sec"]].sum().sort_values("d")
    st.line_chart(trend_vis.set_index("d"))

# ==============================
# Inspect heads
# ==============================
with st.expander("Peek media_df (raw)"):
    st.dataframe(media_df.head(50), use_container_width=True)
with st.expander("Peek visitor_df (raw)"):
    st.dataframe(visitor_df.head(50), use_container_width=True)
