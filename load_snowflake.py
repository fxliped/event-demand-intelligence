import os
import logging
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent / 'ingestion' / '.env'
load_dotenv(dotenv_path=env_path)

# --- Credentials ---
SF_USER      = os.environ.get("SNOWFLAKE_USER")
SF_PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD")
SF_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE")
SF_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA")
S3_BUCKET    = os.environ.get("S3_BUCKET")
AWS_KEY      = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY")

missing = [k for k, v in {
    "SNOWFLAKE_USER": SF_USER, "SNOWFLAKE_PASSWORD": SF_PASSWORD,
    "SNOWFLAKE_ACCOUNT": SF_ACCOUNT, "S3_BUCKET": S3_BUCKET,
    "AWS_ACCESS_KEY_ID": AWS_KEY, "AWS_SECRET_ACCESS_KEY": AWS_SECRET }.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sf_load")


def _connect() -> snowflake.connector.SnowflakeConnection:
    con = snowflake.connector.connect(
        user          = SF_USER,
        password      = SF_PASSWORD,
        account       = SF_ACCOUNT,
        warehouse     = SF_WAREHOUSE,
        database      = SF_DATABASE,
        schema        = SF_SCHEMA,
        role          = 'SYSADMIN',
    )
    return con
    


def setup(cur) -> None:
    """Create the S3 external stage and raw table if they don't exist."""

    # credentials are visible in Snowflake's query history
    cur.execute(f"""
        CREATE STAGE IF NOT EXISTS s3_events_stage
            URL         = 's3://{S3_BUCKET}/bronze/events/' 
            CREDENTIALS = (AWS_KEY_ID = '{AWS_KEY}' AWS_SECRET_KEY = '{AWS_SECRET}') 
            FILE_FORMAT = (TYPE = JSON)
    """)
    log.info("Stage ready: s3_events_stage")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS events_raw (
            raw        VARIANT,
            file_name  VARCHAR,
            loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    log.info("Table ready: events_raw")


def load_from_stage(cur) -> None:
    """
    COPY new NDJSON files from S3 into events_raw.
    Snowflake tracks load history — already-loaded files are skipped automatically.
    Each line in the NDJSON becomes one row in the VARIANT column.
    """
    cur.execute("""
        COPY INTO events_raw (raw, file_name)
        FROM (
            SELECT $1, METADATA$FILENAME
            FROM @s3_events_stage
        )
        FILE_FORMAT = (TYPE = JSON, STRIP_OUTER_ARRAY = FALSE)
        PATTERN     = '.*\\.ndjson'
        ON_ERROR    = 'CONTINUE'
    """)
    results = cur.fetchall()
    col     = {desc[0].upper(): i for i, desc in enumerate(cur.description)}

    loaded  = sum(1 for r in results if r[col["STATUS"]] == "LOADED")
    failed  = sum(1 for r in results if r[col["STATUS"]] == "LOAD_FAILED")
    rows_in = sum(r[col["ROWS_PARSED"]] for r in results)
    rows_ok = sum(r[col["ROWS_LOADED"]] for r in results)

    log.info(f"Files: {len(results)} total | {loaded} loaded | {failed} failed")
    log.info(f"Rows:  {rows_in:,} parsed  | {rows_ok:,} loaded")

    if failed:
        for r in results:
            if r[col["STATUS"]] == "LOAD_FAILED":
                log.error(f"  Failed: {r[col['FILE']]} — {r[col['FIRST_ERROR']]}")


if __name__ == "__main__":
    conn = _connect()
    cur  = conn.cursor()
    try:
        setup(cur)
        load_from_stage(cur)
        log.info(f"Done — query with: SELECT raw:id::STRING, raw:title::STRING FROM {SF_DATABASE}.{SF_SCHEMA}.events_raw LIMIT 10")
    finally:
        cur.close()
        conn.close()
