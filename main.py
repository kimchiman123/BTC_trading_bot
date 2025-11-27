import multiprocessing
import time
import os
import sys

# 기존 파일들을 모듈로 import (파일이 같은 폴더에 있어야 함)
# 파일명에 .py는 빼고 import 합니다.
import producer_btc_1m_kafka
import kafka_to_postgres

def run_producer():
    print(f"[Producer] 시작 (PID: {os.getpid()})")
    try:
        # producer 모듈의 main() 함수 실행
        producer_btc_1m_kafka.main()
    except Exception as e:
        print(f"[Producer] 에러 발생: {e}")

def run_consumer():
    print(f"[Consumer] 시작 (PID: {os.getpid()})")
    try:
        # consumer 모듈의 main() 함수 실행
        kafka_to_postgres.main()
    except Exception as e:
        print(f"[Consumer] 에러 발생: {e}")

if __name__ == "__main__":
    # 1. 프로세스 생성
    p_producer = multiprocessing.Process(target=run_producer, name="Producer")
    p_consumer = multiprocessing.Process(target=run_consumer, name="Consumer")

    # 2. 프로세스 시작
    p_producer.start()
    p_consumer.start()

    print(f"두 프로세스가 백그라운드에서 실행 중입니다.")
    print(f"종료하려면 Ctrl+C를 누르세요.")

    try:
        # 3. 메인 프로세스는 자식들이 죽지 않게 대기 (모니터링)
        while True:
            time.sleep(1)
            if not p_producer.is_alive():
                print("[Main] Producer 프로세스가 종료되었습니다. 재시작합니다.")
                p_producer = multiprocessing.Process(target=run_producer, name="Producer")
                p_producer.start()
            
            if not p_consumer.is_alive():
                print("[Main] Consumer 프로세스가 종료되었습니다. 재시작합니다.")
                p_consumer = multiprocessing.Process(target=run_consumer, name="Consumer")
                p_consumer.start()

    except KeyboardInterrupt:
        print("\n[Main] 종료 요청 받음. 자식 프로세스 종료 중...")
        p_producer.terminate()
        p_consumer.terminate()
        p_producer.join()
        p_consumer.join()
        print("[Main] 전체 종료 완료.")
