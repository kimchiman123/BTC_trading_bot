import requests
import pandas as pd
import time
import psycopg2
from datetime import datetime, timedelta

# ===== DB 설정 =====
PG_CONN_INFO = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

def insert_candles_to_db(df):
    sql = """
    INSERT INTO btc_1m_candles (ticker, ts, open, high, low, close, volume, value)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, ts) DO NOTHING;
    """
    
    with psycopg2.connect(**PG_CONN_INFO) as conn:
        with conn.cursor() as cur:
            data_list = []
            for _, row in df.iterrows():
                # DB에는 KST 시간을 저장
                ts = row['timestamp_kst'].to_pydatetime()
                data_list.append((
                    'KRW-BTC', 
                    ts, 
                    float(row['open']), float(row['high']), float(row['low']), float(row['close']), 
                    float(row['volume']), float(row['value'])
                ))
            
            cur.executemany(sql, data_list)
        conn.commit()
    
    # 로그용
    min_kst = df['timestamp_kst'].min()
    max_kst = df['timestamp_kst'].max()
    print(f"✅ {len(df)}개 처리 | KST 기간: {min_kst} ~ {max_kst}")
    return min_kst # 가장 과거 시간을 반환 (참고용)

def fetch_upbit_candles_correctly(target_count=3000):
    url = "https://api.upbit.com/v1/candles/minutes/1"
    headers = {"accept": "application/json"}
    
    # 초기 'to'는 없음 (최신부터 시작)
    current_to_utc = None
    collected_count = 0
    
    print(f"🚀 [Auto Backfill] 데이터 수집 시작 (목표: {target_count}개)...")
    
    while collected_count < target_count:
        params = {
            "market": "KRW-BTC",
            "count": 200
        }
        # to 파라미터는 반드시 UTC 문자열이어야 함
        if current_to_utc:
            params["to"] = current_to_utc

        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code != 200:
                print(f"⚠️ API 요청 실패: {response.status_code}")
                time.sleep(1)
                continue

            data = response.json()
            if not data:
                print("🏁 데이터 끝.")
                break
                
            df = pd.DataFrame(data)

            # [핵심] 1. API 이동용 UTC 시간 확보
            df['timestamp_utc'] = pd.to_datetime(df['candle_date_time_utc'])
            
            # [핵심] 2. DB 저장용 KST 시간 확보
            df['timestamp_kst'] = pd.to_datetime(df['candle_date_time_kst'])

            # 컬럼 매핑
            df = df.rename(columns={
                'opening_price': 'open',
                'high_price': 'high',
                'low_price': 'low',
                'trade_price': 'close',
                'candle_acc_trade_volume': 'volume',
                'candle_acc_trade_price': 'value'
            })
            
            # 중복 제거 (KST 기준)
            df = df.drop_duplicates(subset=['timestamp_kst'])
            
            # DB 저장 함수 호출
            insert_candles_to_db(df)
            
            collected_count += len(df)
            
            # [핵심 로직] 다음 요청을 위한 'to' 계산 (UTC 기준)
            # 현재 배치에서 가장 오래된 UTC 시간을 찾음
            oldest_utc = df['timestamp_utc'].min()
            
            # 그 시간보다 1초 전을 다음 요청의 'to'로 설정
            next_to_dt = oldest_utc - timedelta(seconds=1)
            current_to_utc = next_to_dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # 로그 간소화 (너무 많이 찍히면 정신없으므로)
            if collected_count % 1000 == 0:
                print(f"   👉 진행률: {collected_count}/{target_count} (다음 요청 UTC: {current_to_utc})")
            
            time.sleep(0.12) # API 제한 고려

        except Exception as e:
            print(f"❌ 에러 발생: {e}")
            break

    print("🎉 데이터 자동 주입 완료!")

if __name__ == "__main__":
    # 이 파일을 직접 실행할 때만 동작
    fetch_upbit_candles_correctly(7500)