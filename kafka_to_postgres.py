import json
from datetime import datetime
from kafka import KafkaConsumer
import psycopg
from psycopg.rows import tuple_row

KAFKA_BOOTSTRAP_SERVERS = ["localhost:19092"] # Kafka 브로커 주소
KAFKA_TOPIC = "btc-1m-candle" # 구독할 토픽 이름

# Postgres 연결 정보
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

BATCH_SIZE = 1 # 1분마다 처리하므로, 1건씩 바로바로 INSERT


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True, #오프셋 자동 커밋
        group_id="btc-1m-to-postgres", #그룹 아이디
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
    )


def create_pg_connection():
    '''Postgres 연결 생성'''
    conn = psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        row_factory=tuple_row,
    )
    conn.autocommit = False # 수동 커밋 (배치 insert 후 명시적 커밋)
    return conn


def insert_batch(conn, rows):
    sql = """
        INSERT INTO btc_1m_candles
        (ticker, ts, open, high, low, close, volume, value)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def main():
    consumer = create_consumer() # Kafka 컨슈머 생성
    conn = create_pg_connection() # Postgres 연결 생성
    print("Kafka consumer + Postgres 연결 완료")

    buffer = [] # 배치 처리를 위한 버퍼

    try:
        for msg in consumer:
            candle = msg.value

            try:
                # timestamp 파싱
                ts = datetime.strptime(
                    candle["timestamp"], "%Y-%m-%d %H:%M:%S"
                )
            except Exception as e:
                print(f"timestamp 파싱 오류: {e}, candle={candle}")
                continue

            # DB 삽입을 위한 튜플 생성
            row = (
                candle["ticker"],
                ts,
                float(candle["open"]),
                float(candle["high"]),
                float(candle["low"]),
                float(candle["close"]),
                float(candle["volume"]),
                float(candle["value"]),
            )
            buffer.append(row)

            # 버퍼가 1건 이상이면 적재
            if len(buffer) >= BATCH_SIZE:
                insert_batch(conn, buffer)
                print(f"{len(buffer)}건 INSERT 완료")
                buffer.clear()

    except KeyboardInterrupt:
        print("종료 요청, 남은 버퍼 처리 중...")
        if buffer:
            insert_batch(conn, buffer)
            print(f"{len(buffer)}건 INSERT 완료")
    finally:
        consumer.close()
        conn.close()
        print("정상 종료")


if __name__ == "__main__":
    main()
