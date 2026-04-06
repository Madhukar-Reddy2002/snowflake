import os
import json
from dotenv import load_dotenv
from datetime import date, timedelta, datetime
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

# ─────────────────────────────────────────────
#  LOAD ENV
# ─────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

GA4_PROPERTY_ID              = "271541970"
KEY_JSON_PATH                = os.path.join(BASE_DIR, "key.json")
SNOWFLAKE_ACCOUNT            = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER               = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PRIVATE_KEY_PATH   = os.path.join(BASE_DIR, os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"))
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
SNOWFLAKE_WAREHOUSE          = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE           = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA             = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE               = os.getenv("SNOWFLAKE_ROLE")
TABLE_NAME                   = "GA4_DAILY_TEST"

# ─────────────────────────────────────────────
#  FIGURE OUT WHICH DATE TO FETCH NEXT
# ─────────────────────────────────────────────
def get_last_monday():
    today = date.today()
    days_since_monday = today.weekday()  # Monday=0, Sunday=6
    last_monday = today - timedelta(days=days_since_monday + 7)
    return last_monday

def get_next_fetch_date(cursor):
    """Returns the next date to fetch — last monday if table empty, else last inserted date + 1"""
    cursor.execute(f"SELECT MAX(DATE) FROM {TABLE_NAME}")
    result = cursor.fetchone()[0]
    if result is None:
        next_date = get_last_monday()
        print(f"📅 Table is empty — starting from last Monday: {next_date}")
    else:
        next_date = result + timedelta(days=1)
        print(f"📅 Last inserted date: {result} → Fetching next: {next_date}")
    return next_date

# ─────────────────────────────────────────────
#  SNOWFLAKE CONNECTION
# ─────────────────────────────────────────────
def get_snowflake_connection():
    with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key_file:
        passphrase = SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.encode() if SNOWFLAKE_PRIVATE_KEY_PASSPHRASE else None
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase,
            backend=default_backend()
        )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=private_key_bytes,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

# ─────────────────────────────────────────────
#  GA4 FETCH
# ─────────────────────────────────────────────
def fetch_ga4_data(fetch_date):
    date_str = fetch_date.strftime("%Y-%m-%d")
    print(f"\n📊 Fetching GA4 data for: {date_str}")

    with open(KEY_JSON_PATH) as f:
        key_data = json.load(f)

    credentials = service_account.Credentials.from_service_account_info(
        key_data,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    client = BetaAnalyticsDataClient(credentials=credentials)

    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="deviceCategory"),
        ],
        metrics=[
            Metric(name="sessions"),
        ],
        date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
    )

    response = client.run_report(request)
    rows = []
    for row in response.rows:
        rows.append({
            "date": datetime.strptime(row.dimension_values[0].value, "%Y%m%d").strftime("%Y-%m-%d"),
            "device_category": row.dimension_values[1].value,
            "sessions"       : int(row.metric_values[0].value),
        })

    print(f"✅ GA4 rows fetched: {len(rows)}")
    for r in rows:
        print(f"   {r}")
    return rows

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    print("\n🚀 Starting GA4 → Snowflake Pipeline")

    # ── Connect to Snowflake ──
    print("\n❄️  Connecting to Snowflake...")
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    print("✅ Snowflake connected.")

    # ── Create table if not exists ──
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            DATE            DATE,
            DEVICE_CATEGORY VARCHAR(50),
            SESSIONS        INTEGER,
            INSERTED_AT     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    # ── Get next date to fetch ──
    fetch_date = get_next_fetch_date(cursor)

    # ── Stop if we've caught up to yesterday ──
    yesterday = date.today() - timedelta(days=1)
    if fetch_date > yesterday:
        print(f"\n✅ Already up to date! Latest date {fetch_date - timedelta(days=1)} is yesterday.")
        cursor.close()
        conn.close()
        return

    # ── Fetch from GA4 ──
    rows = fetch_ga4_data(fetch_date)

    # ── Insert into Snowflake ──
    print(f"\n📥 Inserting {len(rows)} rows into {TABLE_NAME}...")
    insert_query = f"""
        INSERT INTO {TABLE_NAME} (DATE, DEVICE_CATEGORY, SESSIONS)
        VALUES (%s, %s, %s)
    """
    for row in rows:
        cursor.execute(insert_query, (row["date"], row["device_category"], row["sessions"]))

    conn.commit()
    print(f"✅ Done! {len(rows)} rows inserted for {fetch_date}.")

    cursor.close()
    conn.close()
    print("\n🎉 Pipeline run complete!\n")

if __name__ == "__main__":
    main()
