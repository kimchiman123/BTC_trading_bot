"""
DB 스키마 관리 모듈
- 테이블 생성 및 컬럼 추가를 하나의 파일에서 관리합니다.
- apply_db_schema.py + update_db_schema.py 통합
"""
import psycopg2
import os
import sys

PG_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

# 필수 컬럼 정의 (컬럼명: 타입)
REQUIRED_COLUMNS = {
    "prediction": "FLOAT",
    "status": "TEXT",
    "threshold": "FLOAT",
    "shadow_threshold": "FLOAT",
    "volatility": "FLOAT",
    "ticker": "TEXT",
    "pnl": "FLOAT",
    "win_rate": "FLOAT",
}


def apply_full_schema():
    """테이블 생성 + 필수 컬럼 추가를 한 번에 수행합니다."""
    print("🛠 DB 스키마 전체 적용 시작...")

    try:
        conn = psycopg2.connect(**PG_CONN_INFO)
        conn.autocommit = True
        cur = conn.cursor()

        print("🔌 Postgres 연결 성공")

        # 필수 컬럼 추가
        for col_name, col_type in REQUIRED_COLUMNS.items():
            try:
                cur.execute(
                    f"ALTER TABLE btc_1m_candles ADD COLUMN IF NOT EXISTS {col_name} {col_type};"
                )
                print(f"  ✅ '{col_name}' ({col_type}) 확인/추가 완료")
            except Exception as e:
                print(f"  ⚠️ '{col_name}' 처리 중 경고: {e}")
                conn.rollback()

        print("✅ 스키마 전체 적용 완료!")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ 스키마 적용 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    apply_full_schema()
