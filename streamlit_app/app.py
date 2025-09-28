import streamlit as st
import pandas as pd
from pyathena.connection import connect
from typing import List, Optional
import os

# Load secrets from Streamlit
ACCESS_KEY = st.secrets["access_key_id"]
SECRET_KEY = st.secrets["secret_access_key"]
REGION = st.secrets["region"]
DB = st.secrets["database"]
WORKGROUP = st.secrets["workgroup"]
S3_STAGING = st.secrets["s3_staging_dir"]

# --------------------------------
# DB connection helper
# --------------------------------
def _conn():
    return connect(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        s3_staging_dir=S3_STAGING,
        region_name=REGION,
        schema_name=DB,
        work_group=WORKGROUP,
    )

# Run SQL query and return DataFrame
@st.cache_data(ttl=1800, show_spinner=False)
def run_sql(sql: str) -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql(sql, c)

# --------------------------------
# Validation query (run once on startup)
# --------------------------------
try:
    test_df = run_sql("SELECT 1")
    st.sidebar.success("✅ Connected to Athena successfully.")
except Exception as e:
    st.sidebar.error(f"❌ Athena connection failed: {e}")
    st.stop()

# --------------------------------
# Helper functions
# --------------------------------
def sql_date_literal(d: pd.Timestamp) -> str:
    return f"DATE '{d.date().isoformat()}'"

def sql_list(values: List[str]) -> str:
    # Safe quote each with single quotes; escape single quotes for SQL
    if not values:
        return "''"
    quoted = []
    for v in values:
        if v is None:
            continue
        safe_v = str(v).replace("'", "''")
        quoted.append(f"'{safe_v}'")
    return ", ".join(quoted)

def where_clause_for_media_dates(start: pd.Timestamp, end: pd.Timestamp, media_ids: Optional[List[str]]) -> str:
    clause = f"BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"
    if media_ids:
        clause += f" AND media_id IN ({sql_list(media_ids)})"
    return "WHERE d " + clause

def where_clause_for_dates_only(start: pd.Timestamp, end: pd.Timestamp) -> str:
    return f"WHERE d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"

# --------------------------------
# Sidebar filters
# --------------------------------
with st.sidebar:
    st.header("🔧 Filters")

    # Date range filter
    start_date = pd.to_datetime(st.date_input("Start Date", pd.to_datetime("2025-08-29")))
    end_date = pd.to_datetime(st.date_input("End Date", pd.to_datetime("2025-09-28")))

    # Media selection filter
    media_opt_sql = f"""
        SELECT DISTINCT media_id
        FROM gold_media_daily_trend_30d
        {where_clause_for_dates_only(start_date, end_date)}
        ORDER BY media_id
    """
    try:
        media_options_df = run_sql(media_opt_sql)
        media_ids = st.multiselect("Select Media (media_id)", media_options_df["media_id"].tolist())
    except Exception as e:
        st.error(f"Failed to load media options: {e}")
        media_ids = []

# --------------------------------
# Main Dashboard
# --------------------------------
st.title("📊 Wistia Video Analytics — Gold KPIs from Athena")
st.caption(f"Data source: Athena / Glue Data Catalog ➡️ {DB}")

# Example query for summary metrics
summary_sql = f"""
    SELECT
        SUM(plays) AS total_plays,
        SUM(loads) AS total_loads,
        AVG(play_rate_pct) AS avg_play_rate
    FROM gold_media_daily
    {where_clause_for_media_dates(start_date, end_date, media_ids)}
"""

try:
    summary_df = run_sql(summary_sql)
    total_plays = int(summary_df["total_plays"].iloc[0]) if not summary_df.empty else 0
    total_loads = int(summary_df["total_loads"].iloc[0]) if not summary_df.empty else 0
    avg_play_rate = round(summary_df["avg_play_rate"].iloc[0], 2) if not summary_df.empty else 0.0

    st.metric("▶️ Total Plays", f"{total_plays:,}")
    st.metric("📥 Total Loads", f"{total_loads:,}")
    st.metric("🎯 Avg Play Rate (%)", f"{avg_play_rate}")
except Exception as e:
    st.error(f"Failed to load summary metrics: {e}")
