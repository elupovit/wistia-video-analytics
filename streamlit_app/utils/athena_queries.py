def media_ids_query() -> str:
    """
    Fetch distinct media list for the sidebar selector.
    From Gold table agg_media_daily_p.
    """
    return """
    SELECT DISTINCT
        media_id,
        media_title
    FROM agg_media_daily_p
    WHERE media_id IS NOT NULL
    ORDER BY media_title
    """


def summary_kpis_query(placeholders: str, start_param: str, end_param: str) -> str:
    """
    Summary KPIs (visitors, plays, loads, play rate %) for selected media/date range.
    Joins media-level + visitor-level aggregates.
    """
    media_filter = ""
    if placeholders:
        media_filter = f"AND m.media_id IN {placeholders}"

    return f"""
    WITH media AS (
        SELECT
            SUM(total_plays) AS total_plays,
            SUM(total_loads) AS total_loads
        FROM agg_media_daily_p m
        WHERE event_date BETWEEN {start_param} AND {end_param}
        {media_filter}
    ),
    visitors AS (
        SELECT
            COUNT(DISTINCT visitor_id) AS unique_visitors
        FROM agg_visitor_daily_p v
        WHERE event_date BETWEEN {start_param} AND {end_param}
    )
    SELECT
        v.unique_visitors,
        m.total_plays,
        m.total_loads,
        CASE
            WHEN m.total_loads = 0 THEN 0.0
            ELSE 100.0 * m.total_plays / NULLIF(m.total_loads, 0)
        END AS play_rate_pct
    FROM media m
    CROSS JOIN visitors v
    """


def media_daily_query(placeholders: str, start_param: str, end_param: str) -> str:
    """
    Daily media metrics time series for selected media/date range.
    """
    media_filter = ""
    if placeholders:
        media_filter = f"AND media_id IN {placeholders}"

    return f"""
    SELECT
        event_date,
        media_id,
        media_title,
        CAST(COALESCE(total_plays, 0) AS BIGINT) AS total_plays,
        CAST(COALESCE(total_loads, 0) AS BIGINT) AS total_loads
    FROM agg_media_daily_p
    WHERE event_date BETWEEN {start_param} AND {end_param}
    {media_filter}
    ORDER BY event_date, media_title
    """


def visitor_daily_query(start_param: str, end_param: str) -> str:
    """
    Daily visitor metrics for selected date range.
    Note: agg_visitor_daily_p does NOT contain media_id, only visitor aggregates.
    """
    return f"""
    SELECT
        event_date,
        CAST(COALESCE(SUM(media_interactions), 0) AS BIGINT) AS media_interactions,
        CAST(COALESCE(SUM(total_plays), 0) AS BIGINT)        AS total_plays
    FROM agg_visitor_daily_p
    WHERE event_date BETWEEN {start_param} AND {end_param}
    GROUP BY event_date
    ORDER BY event_date
    """
