# streamlit_app/app.py

import pandas as pd
import streamlit as st
from pyathena import connect
from datetime import date, timedelta
from typing import List, Optional

# ================================
# Page config
# ================================
st.set_page_config(page_title="Wistia Video Analytics — Gold KPIs", layout="wide")
st.title("📊 Wistia Video Analytics — Gold KPIs from Athena")
st.caption("Data source: Athena / Glue Data Catalog ➜ **wistia-analytics-gold**")

# ================================
# Load secrets
# ================================
AWS_ACCESS_KEY = st.secrets["access_key_id"]
AWS_SECRET_KEY = st.secrets["secret_access_key"]
REGION         = st.secrets["region"]
DB             = st.secrets["database"]
WORKGROUP      = st.secrets.get("workgroup", "primary")
S3_STAGING     = st.secrets["s3_staging_dir"]

# ================================
# Athena connection + helper
# ================================
@st.cache_resource(show_spinner=False)
def _conn():
    return connect(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        s3_staging_dir=S3_STAGING,
        region_name=REGION,
        schema_name=DB,
        work_group=WORKGROUP,
    )

@st.cache_data(ttl=180, show_spinner=False)
def run_sql(sql: str) -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql(sql, c)

# Validation query — runs on startup
try:
    _ = run_sql("SELECT 1")
    st.sidebar.success("✅ Connected to Athena successfully.")
except Exception as e:
    st.sidebar.error(f"❌ Athena connection failed: {e}")
    st.stop()

# ---------------- helpers ----------------
def sql_date_literal(d: date) -> str:
    return f"DATE '{d.isoformat()}'"

def sql_list(values: List[str]) -> str:
    return ", ".join([f"'{str(v).replace("'", "''")}'" for v in values]) if values else "''"

def where_clause_for_media_dates(start: date, end: date, media_ids: Optional[List[str]]) -> str:
    clause = f"d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"
    if media_ids:
        clause += f" AND media_id IN ({sql_list(media_ids)})"
    return "WHERE " + clause

def where_clause_for_dates_only(start: date, end: date) -> str:
    return f"WHERE d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"

# ================================
# Sidebar filters
# ================================
with st.sidebar:
    st.header("🔧 Filters")

    today = date.today()
    default_start = today - timedelta(days=30)

    dr = st.date_input(
        label="Date Range",
        value=(default_start, today),
        min_value=today - timedelta(days=365),
        max_value=today,
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

# =====================================================================================
# Queries
# =====================================================================================
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

# ================================
# Layout
# ================================
st.markdown("### 📌 Executive Summary")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Plays", f"{int(summary_df.iloc[0]['total_plays'] or 0):,}")
c2.metric("Unique Visitors", f"{int(vis_df.iloc[0]['unique_visitors'] or 0):,}")
c3.metric("Avg Play Rate", f"{float(summary_df.iloc[0]['avg_play_rate_pct'] or 0.0):.2f}%")
c4.metric("Interactions per Play", f"{float(vis_df.iloc[0]['interactions_per_play'] or 0.0):.2f}")

st.markdown("### 🎯 Engagement KPIs (by Media)")
left, right = st.columns([1.4, 1.0])

with left:
    if media_kpis.empty:
        st.info("No media rows in the selected range/filters.")
    else:
        st.dataframe(media_kpis[["media_title", "media_id", "plays", "loads", "play_rate_pct"]], use_container_width=True)

with right:
    if visitor_kpis.empty:
        st.info("No visitor rows in the selected range.")
    else:
        st.markdown("**Top Visitors (by plays)**")
        st.dataframe(visitor_kpis[["visitor_email", "visitor_org_name", "plays"]], use_container_width=True)

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
