import json
import time
from datetime import datetime

import pyupbit
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]  # 소문자 localhost 권장
KAFKA_TOPIC = "btc-1m-candle"
TICKER = "KRW-BTC"


def create_producer():
    '''Kafka 프로듀서 생성 및 반환.'''
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )
    return producer


def fetch_latest_candle():
    """가장 최근 1분봉 캔들 조회 후 dict로 반환, 실패 시 None."""
    try:
        df = pyupbit.get_ohlcv(TICKER, interval="minute1", count=1)
    except Exception as e:
        print(f"데이터 조회 중 오류 발생: {e}")
        return None

    if df is None or df.empty:
        print("데이터 프레임이 비어 있습니다.")
        return None

  # 가장 최근 행 추출
    ts = df.index[-1]
    row = df.iloc[-1]

    candle = {
        "ticker": TICKER, 
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
        "value": float(row["value"]),
    }
    return candle


def main():
    producer = create_producer()
    print("Kafka 생성자 생성")

    try:
        while True:
            candle = fetch_latest_candle() # 가장 최근 1분봉 캔들 조회

            if candle is None:
                print("캔들 데이터를 가져오지 못했습니다. 60초 후 재시도.")
                time.sleep(60)
                continue

            key = candle["timestamp"] 
            producer.send(KAFKA_TOPIC, key=key, value=candle) #key, value 지정
            producer.flush() # buffer 비우기
            print(f"[{datetime.utcnow()}] 전송 완료: {candle}")

            time.sleep(60)

    except KeyboardInterrupt:
        print("종료합니다.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
