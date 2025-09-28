import os
import json
import boto3
import urllib3
from datetime import datetime, date, timedelta
from typing import List, Dict, Union
import logging

http = urllib3.PoolManager()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3 = boto3.client("s3")
REGION = os.environ.get("AWS_REGION", "us-east-1")

# -------------------------------
# Secrets
# -------------------------------
def get_secret(secret_name: str) -> str:
    client = boto3.client("secretsmanager", region_name=REGION)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])["WISTIA_API_TOKEN"]

# -------------------------------
# Wistia API calls
# -------------------------------
def _get(url: str, token: str, timeout: float = 15.0) -> Union[Dict, List]:
    r = http.request("GET", url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    if r.status != 200:
        raise Exception(f"GET {url} failed {r.status}: {r.data.decode('utf-8')}")
    return json.loads(r.data.decode("utf-8"))

def call_wistia_metadata(token: str, media_id: str) -> Dict:
    return _get(f"https://api.wistia.com/v1/medias/{media_id}.json", token, timeout=10.0)

def call_wistia_medias(token: str, media_id: str) -> Dict:
    return _get(f"https://api.wistia.com/v1/stats/medias/{media_id}.json", token, timeout=10.0)

def call_wistia_visitors(token: str, media_id: str, page: int = 1) -> List[Dict]:
    return _get(f"https://api.wistia.com/v1/stats/visitors.json?media_id={media_id}&page={page}", token, timeout=20.0)

# -------------------------------
# S3 helpers (idempotent write)
# -------------------------------
def _exists_any(bucket: str, prefix: str) -> bool:
    resp = S3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return "Contents" in resp

def _delete_prefix(bucket: str, prefix: str) -> None:
    # delete all objects under prefix (paged)
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "ContinuationToken": token} if token else {"Bucket": bucket, "Prefix": prefix}
        resp = S3.list_objects_v2(**kwargs)
        if "Contents" in resp:
            S3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in resp["Contents"]]},
            )
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

def write_ndjson(bucket: str, key: str, data: Union[List, Dict]) -> None:
    if isinstance(data, list):
        body = "\n".join(json.dumps(row, separators=(",", ":")) for row in data)
    else:
        body = json.dumps(data, separators=(",", ":"))
    S3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
        ServerSideEncryption="AES256",
    )

# -------------------------------
# Date utils
# -------------------------------
def parse_event_dt(ev_dt: str) -> date:
    # supports 'YYYY-MM-DD' or ISO times like 'YYYY-MM-DDTHH:MM:SSZ'
    try:
        return datetime.fromisoformat(ev_dt.replace("Z", "+00:00")).date()
    except Exception:
        return datetime.strptime(ev_dt[:10], "%Y-%m-%d").date()

def daterange(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)

# -------------------------------
# Ingestion for a single day (idempotent per day)
# -------------------------------
def ingest_one_day(token: str, bucket: str, target_date: date, media_ids: List[str], overwrite: bool = True, max_visitor_pages: int = 50) -> None:
    dt_str = target_date.isoformat()
    paths = {
        "metadata": f"bronze/wistia/metadata/dt={dt_str}/",
        "medias":   f"bronze/wistia/medias/dt={dt_str}/",
        "visitors": f"bronze/wistia/visitors/dt={dt_str}/",
    }

    # If overwrite, clear existing partitions first (so partial runs get fixed)
    if overwrite:
        for p in paths.values():
            if _exists_any(bucket, p):
                logger.info(f"Overwriting -> deleting {p}")
                _delete_prefix(bucket, p)

    for m in media_ids:
        # Metadata
        md = call_wistia_metadata(token, m)
        write_ndjson(bucket, f"{paths['metadata']}metadata_{m}_{dt_str}.ndjson", md)

        # Medias (aggregates)
        ms = call_wistia_medias(token, m)
        write_ndjson(bucket, f"{paths['medias']}medias_{m}_{dt_str}.ndjson", ms)

        # Visitors (paginated)
        page = 1
        pages_written = 0
        while page <= max_visitor_pages:
            vs = call_wistia_visitors(token, m, page)
            if not vs:
                break
            for v in vs:
                v["media_id"] = m  # ensure join key
                v["event_date"] = dt_str  # useful downstream

            write_ndjson(bucket, f"{paths['visitors']}visitors_{m}_page{page}_{dt_str}.ndjson", vs)
            pages_written += 1
            page += 1
        logger.info(f"{dt_str} media {m}: visitor pages written = {pages_written}")

    logger.info(f"✅ Wrote bronze partitions for {dt_str}")

# -------------------------------
# Handler with bounded backfill
# -------------------------------
def handler(event, context):
    bucket = os.environ["DATA_BUCKET"]
    secret_name = os.environ["SECRET_NAME"]
    media_ids = os.environ.get("MEDIA_IDS", "gskhw4w4lm,v08dlrgr7v").split(",")
    start_date = datetime.strptime(os.environ["START_DATE"], "%Y-%m-%d").date()
    end_date   = datetime.strptime(os.environ["END_DATE"],   "%Y-%m-%d").date()
    overwrite  = os.environ.get("OVERWRITE_DAY", "true").lower() == "true"
    max_pages  = int(os.environ.get("MAX_VISITOR_PAGES", "50"))

    token = get_secret(secret_name)

    # Determine the scheduler's execution day (or 'today' if ad-hoc)
    if "dt" in (event or {}):
        exec_day = parse_event_dt(event["dt"])
    else:
        exec_day = datetime.utcnow().date()

    # Clamp to configured window
    upper = min(exec_day, end_date)
    if upper < start_date:
        logger.info(f"Execution day {upper} < START_DATE {start_date}. Nothing to do.")
        return {"statusCode": 200, "body": json.dumps({"status": "no-op"})}

    for d in daterange(start_date, upper):
        ingest_one_day(token, bucket, d, media_ids, overwrite=overwrite, max_visitor_pages=max_pages)

    return {"statusCode": 200, "body": json.dumps({"status": "ok", "from": start_date.isoformat(), "to": upper.isoformat()})}
