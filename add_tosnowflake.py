import os
import csv
from dotenv import load_dotenv
import snowflake.connector

# ─── Load environment variables ─────────────────────────────
load_dotenv()

# ─── Snowflake Setup ───────────────────────────────────────
snowflake_conn_params = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

def connect_to_snowflake():
    try:
        return snowflake.connector.connect(**snowflake_conn_params)
    except Exception as e:
        print(f"❌ Snowflake connection error: {e}")
        return None

# ─── Upload CSV ─────────────────────────────────────────────
def upload_csv_to_snowflake(csv_path, connection, table_name="properties"):
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        if not rows:
            print("⚠️ CSV is empty.")
            return

        columns = rows[0].keys()
        columns_sql = ", ".join(f'{col}' for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))

        insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"

        try:
            with connection.cursor() as cursor:
                for row in rows:
                    values = [row[col] for col in columns]
                    cursor.execute(insert_sql, values)
            connection.commit()
            print(f"✅ Inserted {len(rows)} rows into '{table_name}'.")
        except Exception as e:
            print("❌ Failed to insert data:", e)

# ─── Run Upload ─────────────────────────────────────────────
if __name__ == "__main__":
    conn = connect_to_snowflake()
    if conn:
        upload_csv_to_snowflake("property_urls.csv", conn)
        conn.close()
