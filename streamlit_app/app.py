# streamlit_app/app.py

from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
import streamlit as st
import boto3
from sqlalchemy import create_engine

# ==============================
# Page config
# ==============================
st.set_page_config(page_title="Wistia Video Analytics — Gold KPIs", layout="wide")
st.title("📊 Wistia Video Analytics — Gold KPIs from Athena")
st.caption("Data source: Athena / Glue Data Catalog ➜ **wistia-analytics-gold**")

# ==============================
# AWS Session (local vs Streamlit)
# ==============================
def get_boto3_session():
    """Return a boto3 session (local = profile, cloud = secrets)."""
    try:
        # Local development
        return boto3.Session(profile_name="wistia-dev")
    except Exception:
        # Streamlit Cloud
        return boto3.Session(
            aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
            region_name=st.secrets["aws"]["region_name"],
        )

session = get_boto3_session()

DB         = st.secrets["athena"]["database"]
WORKGROUP  = st.secrets["athena"]["workgroup"]
S3_STAGING = st.secrets["athena"]["s3_staging_dir"]
REGION     = session.region_name

# ==============================
# Athena engine (SQLAlchemy)
# ==============================
@st.cache_resource(show_spinner=False)
def get_engine():
    conn_str = (
        f"awsathena+rest://@athena.{REGION}.amazonaws.com:443/{DB}"
        f"?s3_staging_dir={S3_STAGING}&work_group={WORKGROUP}"
    )
    return create_engine(
        conn_str,
        connect_args={"s3_session": session, "poll_interval": 1}
    )

@st.cache_data(ttl=180, show_spinner=False)
def run_sql(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)

# ==============================
# Validate the connection once
# ==============================
try:
    _ = run_sql("SELECT 1")
    st.sidebar.success("✅ Connected to Athena successfully.")
except Exception as e:
    st.sidebar.error(f"❌ Athena connection failed: {e}")
    st.stop()

# ==============================
# SQL helpers
# ==============================
def sql_date_literal(d: date) -> str:
    return f"DATE '{d.isoformat()}'"

def sql_list(values: List[str]) -> str:
    if not values:
        return "''"
    quoted = []
    for v in values:
        if v is None:
            continue
        quoted.append("'" + str(v).replace("'", "''") + "'")
    return ", ".join(quoted) if quoted else "''"

def where_clause_for_media_dates(start: date, end: date, media_ids: Optional[List[str]]) -> str:
    clause = f"d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"
    if media_ids:
        clause += f" AND media_id IN ({sql_list(media_ids)})"
    return "WHERE " + clause

def where_clause_for_dates_only(start: date, end: date) -> str:
    return f"WHERE d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"

# ==============================
# Sidebar filters
# ==============================
with st.sidebar:
    st.header("🔧 Filters")
    today = date.today()
    default_start = today - timedelta(days=30)

    dr = st.date_input(
        "Date Range",
        value=(default_start, today),
        min_value=today - timedelta(days=365),
        max_value=today,
        help="Filters use the `d` column in the daily trend views."
    )

    if isinstance(dr, (list, tuple)) and len(dr) == 2:
        start_date, end_date = dr
    else:
        start_date, end_date = default_start, today

    if start_date > end_date:
        start_date, end_date = end_date, end_date

    media_opt_sql = f"""
        SELECT DISTINCT media_id
        FROM gold_media_daily_trend_30d
        {where_clause_for_dates_only(start_date, end_date)}
        ORDER BY media_id
    """
    media_options_df = run_sql(media_opt_sql)
    media_options = media_options_df["media_id"].dropna().astype(str).tolist()

    selected_media = st.multiselect(
        "Select Media (media_id)",
        options=media_options,
        default=media_options,
    )

# ==============================
# Queries
# ==============================
summary_sql = f"""
SELECT
    SUM(plays) AS total_plays,
    SUM(loads) AS total_loads,
    ROUND(100.0 * SUM(plays) / NULLIF(SUM(loads), 0), 2) AS avg_play_rate_pct
FROM gold_media_daily_trend_30d
{where_clause_for_media_dates(start_date, end_date, selected_media)}
"""
summary_df = run_sql(summary_sql)

visitors_sql = f"""
SELECT
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    SUM(interactions) AS total_interactions,
    ROUND(1.0 * SUM(interactions) / NULLIF(SUM(plays), 0), 2) AS interactions_per_play
FROM gold_visitor_daily_trend_30d
{where_clause_for_dates_only(start_date, end_date)}
"""
vis_df = run_sql(visitors_sql)

media_leader_sql = f"""
SELECT
    media_id,
    COALESCE(media_title, '') AS media_title,
    SUM(plays) AS plays,
    SUM(loads) AS loads,
    ROUND(100.0 * SUM(plays) / NULLIF(SUM(loads), 0), 2) AS play_rate_pct
FROM gold_media_daily_trend_30d
{where_clause_for_media_dates(start_date, end_date, selected_media)}
GROUP BY media_id, media_title
ORDER BY plays DESC
"""
media_kpis = run_sql(media_leader_sql)

top_visitors_sql = f"""
SELECT
    COALESCE(visitor_email, 'Unknown') AS visitor_email,
    COALESCE(visitor_org_name, 'Unknown') AS visitor_org_name,
    SUM(plays) AS plays
FROM gold_visitor_daily_trend_30d
{where_clause_for_dates_only(start_date, end_date)}
GROUP BY visitor_email, visitor_org_name
ORDER BY plays DESC
LIMIT 10
"""
visitor_kpis = run_sql(top_visitors_sql)

media_trend_sql = f"""
SELECT d, SUM(plays) AS plays
FROM gold_media_daily_trend_30d
{where_clause_for_media_dates(start_date, end_date, selected_media)}
GROUP BY d
ORDER BY d
"""
media_trend = run_sql(media_trend_sql)

visitor_trend_sql = f"""
SELECT d, SUM(plays) AS plays
FROM gold_visitor_daily_trend_30d
{where_clause_for_dates_only(start_date, end_date)}
GROUP BY d
ORDER BY d
"""
visitor_trend = run_sql(visitor_trend_sql)

# ==============================
# Layout
# ==============================
st.markdown("### 📌 Executive Summary")
c1, c2, c3, c4 = st.columns(4)

total_plays     = int(summary_df.get("total_plays",            pd.Series([0])).iloc[0] or 0)
unique_visitors = int(vis_df.get("unique_visitors",            pd.Series([0])).iloc[0] or 0)
avg_play_rate   = float(summary_df.get("avg_play_rate_pct",    pd.Series([0.0])).iloc[0] or 0.0)
inter_per_play  = float(vis_df.get("interactions_per_play",    pd.Series([0.0])).iloc[0] or 0.0)

c1.metric("Total Plays", f"{total_plays:,}")
c2.metric("Unique Visitors", f"{unique_visitors:,}")
c3.metric("Avg Play Rate", f"{avg_play_rate:.2f}%")
c4.metric("Interactions per Play", f"{inter_per_play:.2f}")

st.markdown("### 🎯 Engagement KPIs (by Media)")
left, right = st.columns([1.4, 1.0])

with left:
    if media_kpis.empty:
        st.info("No media rows in the selected range/filters.")
    else:
        st.dataframe(
            media_kpis[["media_title", "media_id", "plays", "loads", "play_rate_pct"]],
            use_container_width=True,
        )

with right:
    if visitor_kpis.empty:
        st.info("No visitor rows in the selected range.")
    else:
        st.markdown("**Top Visitors (by plays)**")
        st.dataframe(
            visitor_kpis[["visitor_email", "visitor_org_name", "plays"]],
            use_container_width=True,
        )

st.markdown("### 📈 Engagement Trends (last selected window)")
t1, t2 = st.columns(2)

with t1:
    st.markdown("**Media Plays Trend**")
    if media_trend.empty:
        st.info("No media trend data.")
    else:
        st.line_chart(media_trend.set_index("d")["plays"], use_container_width=True)

with t2:
    st.markdown("**Visitor Plays Trend**")
    if visitor_trend.empty:
        st.info("No visitor trend data.")
    else:
        st.line_chart(visitor_trend.set_index("d")["plays"], use_container_width=True)

st.success("✅ Dashboard loaded successfully.")
