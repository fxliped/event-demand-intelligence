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
WITH labels_agg AS (
    SELECT
        e.event_id,
        -- primary label = highest-weight industry tag; null if event has no labels
        ARRAY_AGG(f.value:label::string)
            WITHIN GROUP (ORDER BY f.value:weight::float DESC)[0]::string  AS primary_label,
        COUNT(f.value)                                                      AS label_count
    FROM predicthq_db.staging.stg_events e,
    LATERAL FLATTEN(input => e.phq_labels, outer => true) f
    WHERE e.country     = 'US'
      AND e.event_start IS NOT NULL
      AND e.phq_rank    IS NOT NULL
    GROUP BY e.event_id
)

SELECT
    -- identifiers
    e.event_id,
    e.title,
    e.category,

    -- time (temporal structure for model)
    e.event_start,
    e.event_end,
    e.event_start::date                                              AS event_date,
    YEAR(e.event_start)                                              AS event_year,
    MONTH(e.event_start)                                             AS event_month,
    DAYOFWEEK(e.event_start)                                         AS day_of_week,
    CASE
        WHEN MONTH(e.event_start) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(e.event_start) IN (3, 4, 5)  THEN 'Spring'
        WHEN MONTH(e.event_start) IN (6, 7, 8)  THEN 'Summer'
        ELSE                                          'Fall'
    END                                                              AS season,

    -- geography (random effects: region; city for maps)
    e.city,
    e.region,
    e.latitude,
    e.longitude,

    -- demand outcomes
    e.phq_rank,         -- primary outcome: 0-100, always present
    e.local_rank,       -- local demand rank
    e.aviation_rank,    -- travel/airport demand impact
    e.phq_attendance,   -- secondary outcome: count, 86% non-null

    -- event characteristics (fixed-effect predictors)
    e.scope,            -- locality / region / national / international
    e.duration_hours,
    e.brand_safe,
    DATEDIFF('day', e.first_seen::date, e.event_start::date)         AS days_advance,

    -- industry labels (collapsed to one row per event)
    la.primary_label,   -- highest-weight industry tag
    la.label_count      -- number of industry tags (0 = uncategorized)

FROM predicthq_db.staging.stg_events e
LEFT JOIN labels_agg la ON la.event_id = e.event_id
WHERE e.country     = 'US'
  AND e.event_start IS NOT NULL
  AND e.phq_rank    IS NOT NULL
ORDER BY e.event_start
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
        log.info(f"Fetched {len(df):,} rows across {df['event_id'].nunique():,} unique events")

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False)
        log.info(f"Saved → {OUTPUT_PATH}")

        log.info("\n--- Preview ---")
        log.info(f"Rows (1 per event) : {len(df):,}")
        log.info(f"Date range         : {df['event_date'].min()} → {df['event_date'].max()}")
        log.info(f"Categories         : {sorted(df['category'].dropna().unique().tolist())}")
        log.info(f"Regions            : {df['region'].nunique()} unique")
        log.info(f"Cities             : {df['city'].nunique()} unique")
        log.info(f"Primary labels     : {sorted(df['primary_label'].dropna().unique().tolist())}")
        log.info(f"PHQ rank           : min={df['phq_rank'].min():.0f}  mean={df['phq_rank'].mean():.1f}  max={df['phq_rank'].max():.0f}")
        log.info(f"Attendance         : {df['phq_attendance'].notna().sum():,} non-null  ({df['phq_attendance'].notna().mean()*100:.1f}%)")
    finally:
        conn.close()
