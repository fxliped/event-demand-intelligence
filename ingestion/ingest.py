import requests
import boto3
import json
import os
import io
import time
import logging
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()

# --- Credentials ---
ACCESS_TOKEN = os.environ.get("PREDICTHQ_TOKEN")
S3_BUCKET = os.environ.get("S3_BUCKET")

# --- Tunable Configurations ---
PAGE_SIZE = 500
CHUNK_DAYS = 30
REQUEST_SLEEP = 0.3
RETRY_ATTEMPTS = 3

VOLUME_LOG = "volume_log.csv"

# --- Date Range ---
NOW = datetime.now(timezone.utc) 
DATE_END = (NOW - timedelta(days = 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
DATE_START = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%dT%H:%M:%SZ")
WEEK_AGO = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

# --- Logging ---
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)-8s %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("pdq_ingest")

# --- HTTP Session ---
SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json",
})

# Functions:
    # fetch_chunk() -> one date chunk from the data, handles pagination
    # fetch_date_range() -> calls fetch_chunk() for each XX(90?)-day chunks from the 2 years, dedupes by id
    # to_ndjson_bytes() -> turns dfs into NDJSON
    # upload_to_s3() -> uploads bytes to S3 at partitioned path
    # log_volume() -> to see how many GBs (daily bytes) of data stored


# HTTP GET Request
def _get(params: dict) -> dict:
    """
    Single GET to the events endpoint with retry logic.
    - 429 -> sleep Retry-After header (or 10s) then retry
    - 5xx -> exponential backoff then retry
    - 4xx -> raise immediately (caller bug)
    - Network error -> retry with backoff
    Returns parsed JSON dict.
    """

    url = "https://api.predicthq.com/v1/events/"

    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = SESSION.get(url = url, params = params, timeout = 30)

            if r.status_code == 200:
                return r.json()
            
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 10)) * (attempt + 1)

                log.warning(f"Rate limited = sleeping {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
                continue

            if r.status_code >= 500:
                wait = 5 * (2 ** attempt)
                log.warning(f"Server error {r.status_code} - sleeping {wait}s")

                time.sleep(wait)
                continue

            r.raise_for_status()

        except requests.exceptions.ConnectionError as e:
            wait = 5 * (2 ** attempt)
            log.warning(f"Connection error - sleeping {wait}s: {e}")
            time.sleep(wait)
        
    raise RuntimeError(f"All {RETRY_ATTEMPTS} attempts failed for params {params}")



# test best chunk and date sizing that is not limited - how?
# how to best check errors - which errors to check?
# what exceptions are there?
# why bool return in the tuple?
def fetch_chunk(start: str, end: str) -> tuple[list[dict], bool]:
    """
    Fetch all events where start >= 'start' and start <= 'end'.
    Paginates until exhausted.

    Returns:
        (records, overflowed)
        overflowed = True means the chunk hit the subscription cap
        and we should re-fetch with a smaller date window
    
    Args: 
        start: "YYYY-MM-DD"
        end:   "YYYY-MM-DD"
    """

    all_results = []
    overflowed = False
    offset = 0
    total = None

    log.info(f" Chunk {start} -> {end}")

    while True:
        params = {
            "start.gte": start,     #TWO_YEARS_AGO,  # events that started within the last 2 years
            "start.lte": end,         #YESTERDAY,        # events that have already ended (finished)
            #"state": "active",
            "sort": "-start",
            "limit": PAGE_SIZE,                # max page size per request
            "offset": offset,
        }

        data = _get(params)
        results = data.get("results", [])
        total = total or data.get("count", 0)
        
        if data.get("overflow"):
            overflowed = True
            log.warning(
                f"  OVERFLOW on {start} -> {end} "
                f"(count={total}) - will split chunk"
            )
            break
        
        all_results.extend(results)
        offset += len(results)

        log.info(f" {offset:>6,} / {total:>6,}", )

        if not data.get("next") or not results:
            break

        time.sleep(REQUEST_SLEEP)

    return all_results, overflowed

# def fetch_date_range():
        # 90 day chunk size should keep under PredictHQ's 10,000 result cap,
        # if not, then adjust to a smaller day chunk




# example S3 path: bronze/events/year=2024/month=01/day=15/events__20240115T120000Z.ndjson
# def upload_to_s3(df, bucket_name, s3_key):
#     """Upload a DataFrame as CSV directly to S3 without writing a local file."""
#     import io
#     buffer = io.BytesIO()
#     df.to_csv(buffer, index=False)
#     buffer.seek(0)

#     s3_client = boto3.client("s3")
#     try:
#         s3_client.put_object(
#             Bucket=bucket_name, 
#             Key=s3_key, 
#             Body=buffer #json.dumps(df)
#         ) 
#         print(f"Uploaded {len(df)} rows to s3://{bucket_name}/{s3_key}")
#     except Exception as e:
#         print(f"S3 upload failed: {e}")
#         raise



if __name__ == "__main__":
    test_start = "2026-01-01"
    test_end   = "2026-01-21"

    log.info(f"Testing single chunk: {test_start} → {test_end}")
    records, overflowed = fetch_chunk(test_start, test_end)

        # What you want to verify:
    print(f"\n── fetch_chunk results ───────────────────────")
    print(f"  Records returned : {len(records)}")
    print(f"  Overflowed       : {overflowed}")

    if records:
            # Verify the shape of one record
        sample = records[0]
        print(f"\n── First record keys ─────────────────────────")
        print(f"  {list(sample.keys())}")
    


    # useless cols: description, scope, parent_event.parent_event_id
    # want private to be false, state is not active?


    # run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # s3_key = f"predicthq/events/{run_date}.csv"
    # upload_to_s3(df, S3_BUCKET, s3_key)
    
    
    



