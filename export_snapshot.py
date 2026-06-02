import os
import logging
import pandas as pd
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

env_path = Path(__file__).resolve().parent / 'ingestion' / '.env'
load_dotenv(dotenv_path=env_path)

SF_USER      = os.environ.get("SNOWFLAKE_USER")
SF_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE")
SF_KEY_PATH  = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")

OUTPUT_PATH = Path(__file__).resolve().parent / 'analysis' / 'us_events_snapshot.csv'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("export")

QUERY = """
WITH deduped AS (
    SELECT raw
    FROM PREDICTHQ_DB.RAW.EVENTS_RAW
    WHERE raw:id IS NOT NULL
      AND COALESCE(raw:private::boolean, FALSE) = FALSE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY raw:id::string
        ORDER BY loaded_at DESC
    ) = 1
),

day_counts AS (
    SELECT
        (raw:start_local::timestamp_ntz)::date AS day,
        COUNT(*)                               AS event_count
    FROM deduped
    WHERE raw:country::string = 'US'
    GROUP BY 1
    HAVING COUNT(*) > 100
),

active_days AS (
    SELECT day
    FROM (
        SELECT
            day,
            LAG(day)  OVER (ORDER BY day) AS prev_day,
            LEAD(day) OVER (ORDER BY day) AS next_day
        FROM day_counts
    )
    WHERE day - prev_day = 1
       OR next_day - day = 1
),
labels_agg AS (
    SELECT 
    raw:id::string AS event_id,
    ARRAY_AGG(f.value:label::string)
        WITHIN GROUP (ORDER BY f.value:weight::float DESC)[0]::string AS primary_label, 
    COUNT(f.value) AS label_count
    FROM deduped, 
    LATERAL FLATTEN(input => raw:phq_labels::array, outer => true) f
    WHERE raw:country::string = 'US'
    GROUP BY raw:id::string
    ORDER BY label_count DESC
)
SELECT 
    raw:id::string AS unique_id,
    raw:title::string AS event_title,
    raw:category::string AS category,
    COALESCE(
        REGEXP_SUBSTR(raw:description, '\\.com - (.*)$', 1, 1, 'e'), 
        raw:description) AS description,

    raw:start_local::timestamp_ntz AS start_local,
    raw:end_local::timestamp_ntz AS end_local,
    (raw:start_local::timestamp_ntz)::date AS event_date,
    DAYOFWEEK(raw:start_local::timestamp_ntz) AS day_of_week,
    HOUR(raw:start_local::timestamp_ntz) AS start_hour,
    raw:timezone::string AS timezone,
    
    raw:geo:address:locality::string AS locality,
    raw:geo:address:region::string AS state,
    raw:geo:address:postcode::string AS postal_code,
    raw:geo:address:formatted_address::string AS address,
    CASE WHEN raw:geo:geometry:type::string = 'Point'
         THEN raw:geo:geometry:coordinates[1]::float END AS latitude,
    CASE WHEN raw:geo:geometry:type::string = 'Point'
         THEN raw:geo:geometry:coordinates[0]::float END AS longitude,

    GET(raw:entities, ARRAY_SIZE(raw:entities) - 1):name::string AS venue_name,
    
    raw:phq_attendance::int AS attendance,
    raw:local_rank::int AS local_rank,
    raw:rank::int AS national_rank,
    raw:local_rank::int - raw:rank::int AS local_lift,

    raw:duration::int AS duration,
    DATEDIFF('day', raw:first_seen::timestamp_tz, raw:start_local::timestamp_ntz) AS days_advance,

    raw:phq_labels AS event_labels,
    la.primary_label,
    la.label_count
    
FROM deduped
JOIN active_days 
    ON (raw:start_local::timestamp_ntz)::date = active_days.day
LEFT JOIN labels_agg la
    ON la.event_id = raw:id::string
WHERE raw:country = 'US' 
        AND raw:category::string NOT IN ('severe-weather',
            'disasters', 'airport-delays',
            'daylight-savings', 'health-warnings',
            'terror')
ORDER BY raw:start_local::timestamp_ntz DESC
"""


def _connect() -> snowflake.connector.SnowflakeConnection:
    assert SF_KEY_PATH is not None
    with open(SF_KEY_PATH, "rb") as fh:
        private_key = serialization.load_pem_private_key(fh.read(), password=None)
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        user        = SF_USER,
        private_key = private_key_bytes,
        account     = SF_ACCOUNT,
        warehouse   = SF_WAREHOUSE,
        database    = SF_DATABASE,
        schema      = 'staging',
        role        = 'SYSADMIN',
    )


if __name__ == "__main__":
    log.info("Connecting to Snowflake...")
    conn = _connect()
    try:
        log.info("Running export query...")
        df = pd.read_sql(QUERY, conn)
        df.columns = df.columns.str.lower()
        log.info(f"Fetched {len(df):,} rows across {df['unique_id'].nunique():,} unique events")

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False)
        log.info(f"Saved → {OUTPUT_PATH}")

        log.info("\n--- Preview ---")
        log.info(f"Rows (1 per event)  : {len(df):,}")
        log.info(f"Date range          : {df['event_date'].min()} → {df['event_date'].max()}")
        log.info(f"Categories          : {sorted(df['category'].dropna().unique().tolist())}")
        log.info(f"States              : {df['state'].nunique()} unique")
        log.info(f"Cities              : {df['locality'].nunique()} unique")
        log.info(f"Primary labels      : {sorted(df['primary_label'].dropna().unique().tolist())}")
        log.info(f"Local rank          : min={df['local_rank'].min():.0f}  mean={df['local_rank'].mean():.1f}  max={df['local_rank'].max():.0f}")
        log.info(f"National rank       : min={df['national_rank'].min():.0f}  mean={df['national_rank'].mean():.1f}  max={df['national_rank'].max():.0f}")
        log.info(f"Local lift (gap)    : mean={df['local_lift'].mean():.1f}  std={df['local_lift'].std():.1f}")
        log.info(f"Attendance          : {df['attendance'].notna().sum():,} non-null  ({df['attendance'].notna().mean()*100:.1f}%)")
    finally:
        conn.close()
