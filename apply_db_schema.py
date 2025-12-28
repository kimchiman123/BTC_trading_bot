import psycopg2
import sys

# Connect to localhost since we are running on the host machine
# Port 5432 is mapped in docker-compose
PG_CONN_INFO = {
    "host": "localhost",
    "port": "5432",
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def apply_schema_changes():
    try:
        conn = psycopg2.connect(**PG_CONN_INFO)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("🔌 Connected to Postgres...")
        
        # Add prediction column
        print("🛠 Adding 'prediction' column...")
        cur.execute("ALTER TABLE btc_1m_candles ADD COLUMN IF NOT EXISTS prediction FLOAT;")
        
        # Add status column
        print("🛠 Adding 'status' column...")
        cur.execute("ALTER TABLE btc_1m_candles ADD COLUMN IF NOT EXISTS status TEXT;")
        
        print("✅ Schema changes applied successfully.")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error applying schema changes: {e}")
        sys.exit(1)

if __name__ == "__main__":
    apply_schema_changes()
