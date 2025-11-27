import time
import pyupbit
import psycopg2
import pandas as pd
import os
from datetime import datetime

# ===== [설정] 업비트 API 키 로드 (파일에서 읽기) =====
def load_upbit_keys(file_path="upbit_key.txt"):
    try:
        with open(file_path, "r") as f:
            lines = f.readlines()
            access = lines[0].strip() # 첫째 줄
            secret = lines[1].strip() # 둘째 줄
            return access, secret
    except FileNotFoundError:
        print(f"!!! 에러: '{file_path}' 파일을 찾을 수 없습니다.")
        return None, None
    except IndexError:
        print(f"!!! 에러: '{file_path}' 형식이 잘못되었습니다. (첫줄:Access, 둘째줄:Secret)")
        return None, None

ACCESS_KEY, SECRET_KEY = load_upbit_keys()

# ===== [설정] DB 연결 정보 =====
PG_CONN_INFO = {
    "host": "localhost",     # 로컬에서 실행 시
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

# ===== [설정] 트레이딩 파라미터 =====
TICKER = "KRW-BTC"
BETTING_BUDGET = 10000  # 매수 금액 (최소 5000원 이상)

def get_db_connection():
    return psycopg2.connect(**PG_CONN_INFO)

def get_latest_data():
    """DB에서 가장 최신 1분봉 데이터와 지표를 가져옵니다."""
    sql = """
    SELECT ts, close, rsi_14, threshold
    FROM btc_realtime_features
    ORDER BY ts DESC
    LIMIT 1;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row:
                return {
                    "ts": row[0],
                    "close": row[1],
                    "rsi": row[2],
                    "threshold": row[3]
                }
    return None

def main():
    # 키 로드 실패 시 종료
    if not ACCESS_KEY or not SECRET_KEY:
        print("API 키 로드 실패. 프로그램을 종료합니다.")
        return

    print(f"[{datetime.now()}] 트레이더 시작... (타겟: {TICKER})")
    
    # 1. 업비트 연결
    try:
        upbit = pyupbit.Upbit(ACCESS_KEY, SECRET_KEY)
        print(f"[{datetime.now()}] 업비트 로그인 성공")
        # 잔고 조회 테스트
        krw = upbit.get_balance("KRW")
        btc = upbit.get_balance(TICKER)
        print(f"   - 보유 현금: {krw:,.0f} 원")
        print(f"   - 보유 BTC: {btc:.8f} BTC")
    except Exception as e:
        print(f"!!! 업비트 연결 실패: {e}")
        return

    # 2. 무한 루프
    while True:
        try:
            data = get_latest_data()
            if not data:
                print("DB 데이터 없음. 대기 중...")
                time.sleep(10)
                continue

            ts = data['ts']
            rsi = data['rsi']
            threshold = data['threshold'] if data['threshold'] else 0.7
            current_price = pyupbit.get_current_price(TICKER)

            print(f"\n[{datetime.now()}] 분석 중 (DB시간: {ts})")
            print(f"   - 현재가: {current_price:,.0f} 원")
            print(f"   - RSI: {rsi:.2f} / Threshold: {threshold:.4f}")

            # 매매로직은 학습한 데이터를 기반으로 변경을 수행해야함 
            '''
            # 1) 매수 로직 (예시: RSI < 30)
            if rsi < 30:
                krw_balance = upbit.get_balance("KRW")
                if krw_balance > BETTING_BUDGET:
                    print(f"   >>> 매수 조건 만족 (RSI {rsi:.2f} < 30) -> 시장가 매수 진행")
                    # upbit.buy_market_order(TICKER, BETTING_BUDGET) # 실제 매수
                    print("   >>> (테스트) 매수 주문 로그")
                else:
                    print("   >>> 매수 잔고 부족")

            # 2) 매도 로직 (예시: RSI > 70)
            elif rsi > 70:
                btc_balance = upbit.get_balance(TICKER)
                current_val = btc_balance * current_price
                if current_val > 5000:
                    print(f"   >>> 매도 조건 만족 (RSI {rsi:.2f} > 70) -> 시장가 전량 매도")
                    # upbit.sell_market_order(TICKER, btc_balance) # 실제 매도
                    print("   >>> (테스트) 매도 주문 로그")
                else:
                    print("   >>> 매도 물량 부족")
            
            else:
                print("   >>> 관망")

            time.sleep(60) 
            '''
        except Exception as e:
            print(f"!!! 에러 발생: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
