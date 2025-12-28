import psycopg2
import os

PG_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

# 로컬 테스트용 (Docker 내부가 아닐 경우)
# PG_CONN_INFO["host"] = "localhost" 
# PG_CONN_INFO["port"] = "5432"

def update_schema():
    print("🛠 DB 스키마 업데이트 시작...")
    
    commands = [
        "ALTER TABLE btc_1m_candles ADD COLUMN IF NOT EXISTS threshold FLOAT;",
        "ALTER TABLE btc_1m_candles ADD COLUMN IF NOT EXISTS shadow_threshold FLOAT;",
        "ALTER TABLE btc_1m_candles ADD COLUMN IF NOT EXISTS volatility FLOAT;",
        "ALTER TABLE btc_1m_candles ADD COLUMN IF NOT EXISTS pnl FLOAT;",
        "ALTER TABLE btc_1m_candles ADD COLUMN IF NOT EXISTS win_rate FLOAT;"
    ]
    
    try:
        with psycopg2.connect(**PG_CONN_INFO) as conn:
            with conn.cursor() as cur:
                for cmd in commands:
                    print(f"Executing: {cmd}")
                    cur.execute(cmd)
            conn.commit()
        print("✅ 스키마 업데이트 성공!")
    except Exception as e:
        print(f"❌ 작업 실패: {e}")

if __name__ == "__main__":
    update_schema()
