# streamlit_app/app.py

import os
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
import streamlit as st
import boto3
from pyathena import connect

# ================================
# Page config (first line in app)
# ================================
st.set_page_config(page_title="Wistia Video Analytics — Gold KPIs", layout="wide")
st.title("📊 Wistia Video Analytics — Gold KPIs from Athena")
st.caption("Data source: Athena / Glue Data Catalog ➜ **wistia-analytics-gold**")

# ================================
# Secrets / config
# ================================
def _get_secret(*names: str, default: Optional[str] = None) -> Optional[str]:
    # 1) Top-level keys
    for n in names:
        if n in st.secrets:
            return st.secrets[n]
    # 2) [default] section (if present)
    if "default" in st.secrets:
        for n in names:
            if n in st.secrets["default"]:
                return st.secrets["default"][n]
    # 3) environment
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default

REGION     = _get_secret("ATHENA_REGION", "AWS_REGION", default="us-east-1")
DB         = _get_secret("ATHENA_DATABASE", default="wistia-analytics-gold")
S3_STAGING = _get_secret("ATHENA_S3_STAGING", "ATHENA_S3_OUTPUT")
WORKGROUP  = _get_secret("ATHENA_WORKGROUP", default="primary")
AWS_PROFILE = os.getenv("AWS_PROFILE", "wistia-dev")  # your local profile

if not (REGION and DB and S3_STAGING):
    st.error("❌ Missing Athena config. Set `ATHENA_REGION`, `ATHENA_DATABASE`, `ATHENA_S3_STAGING` in `.streamlit/secrets.toml`.")
    st.stop()

# ================================
# Athena connection + query helper
# ================================
@st.cache_resource(show_spinner=False)
def _conn():
    # use your local profile to mirror CLI behavior
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=REGION)
    creds = session.get_credentials().get_frozen_credentials()
    return connect(
        s3_staging_dir=S3_STAGING,
        region_name=REGION,
        schema_name=DB,
        work_group=WORKGROUP if WORKGROUP else None,
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
    )

@st.cache_data(ttl=180, show_spinner=False)
def run_sql(sql: str) -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql(sql, c)

# ---------------- helpers: SQL fragments (no f-string backslash pitfalls) -------------
def sql_date_literal(d: date) -> str:
    # Athena/Presto date literal
    return "DATE '" + d.isoformat() + "'"

def sql_list(values: List[str]) -> str:
    # Safe quote each with single quotes; escape single quotes for SQL
    quoted = []
    for v in values:
        if v is None:
            continue
        quoted.append("'" + str(v).replace("'", "''") + "'")
    return ", ".join(quoted) if quoted else "''"

def where_clause_for_media_dates(start: date, end: date, media_ids: Optional[List[str]]) -> str:
    parts: List[str] = []
    parts.append(f"d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}")
    if media_ids:
        parts.append("media_id IN (" + sql_list(media_ids) + ")")
    return "WHERE " + " AND ".join(parts)

def where_clause_for_dates_only(start: date, end: date) -> str:
    return f"WHERE d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"

# ================================
# Sidebar filters (Date range + Media)
# ================================
with st.sidebar:
    st.header("🔧 Filters")

    today = date.today()
    default_start = today - timedelta(days=30)

    # Date range
    dr = st.date_input(
        label="Date Range",
        value=(default_start, today),
        min_value=today - timedelta(days=365),
        max_value=today,
        help="Filters use the `d` column in the daily trend views."
    )

    # Normalize tuple
    if isinstance(dr, (list, tuple)) and len(dr) == 2:
        start_date, end_date = dr
    else:
        start_date, end_date = default_start, today

    # Limit guard
    if start_date > end_date:
        start_date, end_date = end_date, end_date

    # Media options (distinct in date window)
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
        default=media_options,  # default to all
        placeholder="Choose one or more media_id values"
    )

# =====================================================================================
# Queries (apply date+media where possible; date-only where media_id is absent)
# =====================================================================================

# ---- Executive Summary (plays/loads from media trend; unique visitors+interactions from visitor trend) ----
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

# ---- Media leaderboard (aggregate from daily trend) ----
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

# ---- Top visitors (date-only; media_id not present in this view) ----
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

# ---- Trends (media + date, and visitors + date) ----
media_trend_sql = f"""
SELECT
    d,
    SUM(plays) AS plays
FROM gold_media_daily_trend_30d
{where_clause_for_media_dates(start_date, end_date, selected_media)}
GROUP BY d
ORDER BY d
"""
media_trend = run_sql(media_trend_sql)

visitor_trend_sql = f"""
SELECT
    d,
    SUM(plays) AS plays
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
total_plays = int(summary_df.get("total_plays", pd.Series([0])).iloc[0] or 0)
unique_visitors = int(vis_df.get("unique_visitors", pd.Series([0])).iloc[0] or 0)
avg_play_rate = float(summary_df.get("avg_play_rate_pct", pd.Series([0.0])).iloc[0] or 0.0)
inter_per_play = float(vis_df.get("interactions_per_play", pd.Series([0.0])).iloc[0] or 0.0)

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
        show_cols = ["media_title", "media_id", "plays", "loads", "play_rate_pct"]
        st.dataframe(media_kpis[show_cols], use_container_width=True)

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
