import streamlit as st
import pandas as pd
from sqlalchemy.engine import create_engine
from typing import List, Optional
import os

# ==============================
# Load secrets from environment
# ==============================
AWS_KEY = os.getenv("aws_access_key_id")
AWS_SECRET = os.getenv("aws_secret_access_key")
REGION = os.getenv("region_name")

DB = os.getenv("database")
WORKGROUP = os.getenv("workgroup")
S3_STAGING = os.getenv("s3_staging_dir")

# ==============================
# Athena connection (SQLAlchemy)
# ==============================
def _engine():
    conn_str = (
        f"awsathena+rest://{AWS_KEY}:{AWS_SECRET}"
        f"@athena.{REGION}.amazonaws.com:443/{DB}"
        f"?s3_staging_dir={S3_STAGING}&work_group={WORKGROUP}"
    )
    return create_engine(conn_str, connect_args={"poll_interval": 1})

# ==============================
# Query wrapper
# ==============================
@st.cache_data(ttl=600, show_spinner=False)
def run_sql(sql: str) -> pd.DataFrame:
    try:
        with _engine().connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception as e:
        st.error(f"❌ Query failed: {e}")
        return pd.DataFrame()

# ==============================
# Validation query
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
def sql_date_literal(d: pd.Timestamp) -> str:
    return f"DATE '{d.date().isoformat()}'"

def sql_list(values: List[str]) -> str:
    if not values:
        return "''"
    quoted = []
    for v in values:
        if v is None:
            continue
        safe_v = str(v).replace("'", "''")
        quoted.append(f"'{safe_v}'")
    return ", ".join(quoted)

def where_clause_for_media_dates(start: pd.Timestamp, end: pd.Timestamp,
                                 media_ids: Optional[List[str]]) -> str:
    clause = f"d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"
    if media_ids:
        clause += f" AND media_id IN ({sql_list(media_ids)})"
    return "WHERE " + clause

def where_clause_for_dates_only(start: pd.Timestamp, end: pd.Timestamp) -> str:
    return f"WHERE d BETWEEN {sql_date_literal(start)} AND {sql_date_literal(end)}"

# ==============================
# Streamlit UI
# ==============================
st.title("📊 Wistia Video Analytics — Gold KPIs from Athena")
st.caption(f"Data source: Athena / Glue Data Catalog ➜ {DB}")

# Sidebar filters
with st.sidebar:
    st.header("🔧 Filters")
    start_date = pd.to_datetime(st.date_input("Start Date", pd.to_datetime("2025-08-29")))
    end_date = pd.to_datetime(st.date_input("End Date", pd.to_datetime("2025-09-28")))

# Example query: get distinct media_ids
media_opt_sql = f"""
    SELECT DISTINCT media_id
    FROM gold_media_daily_trend_30d
    {where_clause_for_dates_only(start_date, end_date)}
    ORDER BY media_id
"""
media_options_df = run_sql(media_opt_sql)

if not media_options_df.empty:
    media_ids = st.multiselect(
        "Select Media IDs",
        options=media_options_df["media_id"].tolist()
    )
    st.write("You selected:", media_ids)

    # Example KPI query
    kpi_sql = f"""
        SELECT d, media_id, views, engagement
        FROM gold_media_daily_trend_30d
        {where_clause_for_media_dates(start_date, end_date, media_ids)}
        ORDER BY d
    """
    kpi_df = run_sql(kpi_sql)
    st.dataframe(kpi_df.head())
else:
    st.warning("No media IDs available for this date range.")
