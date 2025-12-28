import multiprocessing
import time
import os
import sys
import psycopg2

from custom_objective import FocalLossObjective

# 모듈 import
import producer_btc_1m_kafka
import kafka_to_postgres
import realtime_bot
import append_data


# DB 설정 (데이터 확인용)
PG_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

def init_db():
    """
    봇 실행 시 DB 테이블이 없으면 자동으로 생성합니다.
    (prediction, status 컬럼 포함)
    """
    print("🛠 [Init] DB 테이블 점검 및 초기화...")
    try:
        conn = psycopg2.connect(**PG_CONN_INFO)
        conn.autocommit = True
        cur = conn.cursor()
        
        # 테이블 생성 쿼리 (BTC 1분봉 + 예측 결과)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS btc_1m_candles (
            ts TIMESTAMP PRIMARY KEY,
            open FLOAT NOT NULL,
            high FLOAT NOT NULL,
            low FLOAT NOT NULL,
            close FLOAT NOT NULL,
            volume FLOAT,
            value FLOAT,
            prediction NUMERIC(10, 8),
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_table_sql)
        
        # 인덱스 생성 (옵션)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_btc_1m_ts ON btc_1m_candles(ts);")
        
        print("✅ [Init] btc_1m_candles 테이블 준비 완료.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ [Init Error] DB 초기화 실패: {e}")
        # DB 연결 실패 시 봇이 돌 수 없으므로 종료가 나을 수 있으나, 
        # 여기서는 로그만 남기고 진행 시도


def check_and_backfill():
    """
    시작 전 DB 데이터를 확인하고, 부족하면 자동으로 채워 넣습니다.
    """
    print("🔍 [Init] 초기 데이터 점검 중...")
    
    try:
        # DB 연결 및 개수 확인
        conn = psycopg2.connect(**PG_CONN_INFO)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM btc_1m_candles")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()

        print(f"📊 현재 DB 데이터: {count}개")

        # 7500개 미만이면 백필 시작
        if count < 7500:
            print(f"⚠️ 데이터 부족 (기준: 7500개). 최신 데이터 백필을 시작합니다...")
            print("⏳ 이 작업은 몇 분 정도 소요될 수 있습니다.")
            
            # append_data 모듈의 함수 호출 (현재 시점부터 과거로 7500개 수집)
            append_data.fetch_upbit_candles_correctly(target_count=7500)
            
            print("✅ 데이터 백필 완료! 시스템을 가동합니다.")
        else:
            print("✅ 데이터 충분함. 시스템을 가동합니다.")

    except Exception as e:
        print(f"❌ [Init Error] 데이터 점검 중 오류 발생: {e}")
        print("⚠️ 봇이 불안정할 수 있습니다.")

def run_producer():
    print(f"[Producer] 시작 (PID: {os.getpid()})")
    try: producer_btc_1m_kafka.main()
    except Exception as e: print(f"[Producer] 에러: {e}")

def run_db_consumer():
    print(f"[DB Consumer] 시작 (PID: {os.getpid()})")
    try: kafka_to_postgres.main()
    except Exception as e: print(f"[DB Consumer] 에러: {e}")

def run_realtime_bot():
    print(f"[RealTime Bot] 시작 (PID: {os.getpid()})")
    try: realtime_bot.main()
    except Exception as e: print(f"[RealTime Bot] 에러: {e}")

if __name__ == "__main__":
    # 0. DB 테이블 자동 생성 (없을 경우)
    init_db()

    # 1. [핵심] 프로세스 시작 전 데이터 점검 및 자동 주입
    check_and_backfill()

    print("-" * 50)

    # 2. 프로세스 정의
    p_producer = multiprocessing.Process(target=run_producer, name="Producer")
    p_db_consumer = multiprocessing.Process(target=run_db_consumer, name="DB_Consumer")
    p_bot = multiprocessing.Process(target=run_realtime_bot, name="Trading_Bot")

    # 3. 프로세스 시작
    p_producer.start()
    p_db_consumer.start()
    p_bot.start()

    print(f"🚀 3개의 프로세스가 실행 중입니다: Producer, DB_Consumer, Trading_Bot")

    try:
        while True:
            time.sleep(1)
            # 죽으면 살리기 (Health Check & Restart)
            if not p_producer.is_alive():
                print("[Main] Producer 재시작")
                p_producer = multiprocessing.Process(target=run_producer, name="Producer")
                p_producer.start()
            
            if not p_db_consumer.is_alive():
                print("[Main] DB Consumer 재시작")
                p_db_consumer = multiprocessing.Process(target=run_db_consumer, name="DB_Consumer")
                p_db_consumer.start()

            if not p_bot.is_alive():
                print("[Main] Trading Bot 재시작")
                p_bot = multiprocessing.Process(target=run_realtime_bot, name="Trading_Bot")
                p_bot.start()

    except KeyboardInterrupt:
        print("\n[Main] 종료 중...")
        p_producer.terminate()
        p_db_consumer.terminate()
        p_bot.terminate()
        p_producer.join()
        p_db_consumer.join()
        p_bot.join()
        print("[Main] 종료 완료.")