import os
from dotenv import load_dotenv
from datetime import date, timedelta
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

GA4_PROPERTY_ID        = "271541970"
KEY_JSON_PATH          = os.path.join(BASE_DIR, "key.json")

SNOWFLAKE_ACCOUNT      = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER         = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PRIVATE_KEY_PATH = os.path.join(BASE_DIR, os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"))
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
SNOWFLAKE_WAREHOUSE    = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE     = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA       = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE         = os.getenv("SNOWFLAKE_ROLE")

TABLE_NAME             = "GA4_DAILY_TEST"
YESTERDAY              = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# ─────────────────────────────────────────────
#  STEP 1 — FETCH FROM GA4
# ─────────────────────────────────────────────
print(f"\n📊 Fetching GA4 data for: {YESTERDAY}")

import json
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
    date_ranges=[DateRange(start_date=YESTERDAY, end_date=YESTERDAY)],
)

response = client.run_report(request)

rows = []
for row in response.rows:
    rows.append({
        "date"           : row.dimension_values[0].value,
        "device_category": row.dimension_values[1].value,
        "sessions"       : int(row.metric_values[0].value),
    })

print(f"✅ GA4 rows fetched: {len(rows)}")
for r in rows:
    print(f"   {r}")

# ─────────────────────────────────────────────
#  STEP 2 — CONNECT TO SNOWFLAKE
# ─────────────────────────────────────────────
print(f"\n❄️  Connecting to Snowflake...")

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

conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    private_key=private_key_bytes,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)
cursor = conn.cursor()
print("✅ Snowflake connected.")

# ─────────────────────────────────────────────
#  STEP 3 — CREATE TABLE IF NOT EXISTS
# ─────────────────────────────────────────────
print(f"\n🛠️  Ensuring table {TABLE_NAME} exists...")

cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        DATE            DATE,
        DEVICE_CATEGORY VARCHAR(50),
        SESSIONS        INTEGER,
        INSERTED_AT     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
""")
print(f"✅ Table ready.")

# ─────────────────────────────────────────────
#  STEP 4 — INSERT ROWS
# ─────────────────────────────────────────────
print(f"\n📥 Inserting {len(rows)} rows into {TABLE_NAME}...")

insert_query = f"""
    INSERT INTO {TABLE_NAME} (DATE, DEVICE_CATEGORY, SESSIONS)
    VALUES (%s, %s, %s)
"""

for row in rows:
    cursor.execute(insert_query, (row["date"], row["device_category"], row["sessions"]))

conn.commit()
print(f"✅ Done! {len(rows)} rows inserted.")

cursor.close()
conn.close()
print("\n🎉 Pipeline complete!\n")
