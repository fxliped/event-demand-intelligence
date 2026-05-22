import requests
import boto3
import json
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from botocore.exceptions import ClientError


load_dotenv()

# --- Credentials ---
ACCESS_TOKEN = os.environ.get("PREDICTHQ_TOKEN")
S3_BUCKET    = os.environ.get("S3_BUCKET")

missing = [k for k, v in {"PREDICTHQ_TOKEN": ACCESS_TOKEN, "S3_BUCKET": S3_BUCKET}.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

# --- Tunable Configurations ---
PAGE_SIZE = 500
CHUNK_DAYS = 30
REQUEST_SLEEP = 0.3
RETRY_ATTEMPTS = 3

# --- Date Range ---
NOW = datetime.now(timezone.utc)
DATE_END   = (NOW - timedelta(days=1)).strftime("%Y-%m-%d")

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

        except requests.exceptions.HTTPError:
            raise
        except requests.exceptions.RequestException as e:
            wait = 5 * (2 ** attempt)
            log.warning(f"Request error - sleeping {wait}s: {e}")
            time.sleep(wait)
        
    raise RuntimeError(f"All {RETRY_ATTEMPTS} attempts failed for params {params}")



def fetch_data(start: str, end: str, stop_on_overflow: bool = False) -> tuple[list[dict], bool]:
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
            "start.gte": start,     
            "start.lte": end,         
            #"state": "active",
            "sort": "-start",
            "limit": PAGE_SIZE,                # max page size per request
            "offset": offset,
        }

        data = _get(params)
        results = data.get("results", [])
        total = total or data.get("count", 0)
        
        # overflow indicates there are more results than the subscription allows
        if data.get("overflow") and not overflowed:
            overflowed = True
            log.warning(
                f"  OVERFLOW on {start} -> {end} "
                f"(count={total}) - hit subscription cap, collecting available records"
            )
            if stop_on_overflow:
                break

        all_results.extend(results)
        offset += len(results)

        log.info(f" {offset:>6,} / {total:>6,}", )

        if not data.get("next") or not results:
            break

        time.sleep(REQUEST_SLEEP)

    return all_results, overflowed


def _fetch_split(start: str, end: str) -> list[dict]:
    """
    Walk day-by-day through an overflowed window, fetching each day exactly once.

    Returns:
        all_records: A list of events within the given overflowed timeframe
    
    Args: 
        start: "YYYY-MM-DD"
        end:   "YYYY-MM-DD"
    """
    date_fmt = "%Y-%m-%d"
    cur = datetime.strptime(start, date_fmt)
    d_end = datetime.strptime(end, date_fmt)
    all_records = []

    while cur <= d_end:
        day = cur.strftime(date_fmt)
        records, capped = fetch_data(day, day)
        if capped:
            log.warning(f"  {day}: hit 5,000 cap — {len(records):,} records collected, some may be missing")
        all_records.extend(records)
        cur += timedelta(days=1)

    return all_records




def to_ndjson_bytes(records: list[dict]) -> bytes:
    """
    Convert records from list format to NDJSON, which is easily queryable in AWS

    Returns:
       The NDJSON formatte records encoded as bytes
    
    Args: 
        records: A list of events
    """
    return "\n".join(json.dumps(r) for r in records).encode("utf-8")


def upload_to_s3(body: bytes, s3_key: str, skip_if_exists: bool = True) -> None:
    """
    Upload event records in the form of bytes to our s3 bucket

    Returns:
       None
    
    Args: 
        body: The event records converted to byte format
        s3_key: A string for the folder the data will live in
        skip_if_exists: If the folder and record already exist, we skip over reuploading the bucket 
    """

    s3_client = boto3.client("s3")
    if skip_if_exists:
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            log.info(f"Skipping {s3_key} — already exists")
            return
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                log.error(f"S3 head_object failed for {s3_key}: {e}")
                raise
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=body)
        log.info(f"Uploaded {len(body):,} bytes → s3://{S3_BUCKET}/{s3_key}")
    except ClientError as e:
        log.error(f"S3 upload failed for {s3_key}: {e.response['Error']['Code']} — {e}")
        raise


def retrieve_and_upload(start: str, end: str, upload: bool = True) -> int:
    """
    Retrieve the events given the start and end dates.
        Walk from start -> end in CHUNK_DAYS windows.
        Overflowed chunks are walked day-by-day and events are deduplicated by event id. 
    
    Returns:
        Count of total unique records
    
    Args:
        start:  The beginning of the window in string format
        end:    The end of the window in string format
        upload: Whether or not to upload the retrieved events to AWS S3
    """
    date_fmt = "%Y-%m-%d"
    d_start  = datetime.strptime(start, date_fmt)
    d_end    = datetime.strptime(end,   date_fmt)

    seen_ids    = set()
    total_count = 0
    cur = d_start

    while cur <= d_end:
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS - 1), d_end)
        s = cur.strftime(date_fmt)
        e = chunk_end.strftime(date_fmt)

        records, overflowed = fetch_data(s, e, stop_on_overflow=True)

        if overflowed:
            records = _fetch_split(s, e)

        if not records:
            log.info(f"  No records for {s} -> {e}, skipping")
            cur = chunk_end + timedelta(days=1)
            continue

        # Deduplicate across chunks
        unique = [r for r in records if r["id"] not in seen_ids]
        seen_ids.update(r["id"] for r in unique)
        total_count += len(unique)

        if upload:
            by_day: dict[tuple, list] = {}
            for event in unique:
                ymd = (event["start"][:4], event["start"][5:7], event["start"][8:10])
                by_day.setdefault(ymd, []).append(event)

            for (year, month, day), group in by_day.items():
                s3_key = f"bronze/events/year={year}/month={month}/day={day}/events_{year}-{month}-{day}.ndjson"
                upload_to_s3(to_ndjson_bytes(group), s3_key)

        cur = chunk_end + timedelta(days=1)

    log.info(f"Total unique records: {total_count:,}")
    return total_count


if __name__ == "__main__":
    start = (NOW - timedelta(days=31)).strftime("%Y-%m-%d")
    end   = DATE_END

    log.info(f"Starting ingest: {start} → {end}")
    total = retrieve_and_upload(start, end, upload=True)
    log.info(f"Done — {total:,} unique records uploaded to s3://{S3_BUCKET}/bronze/events/")

