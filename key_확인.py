import time
import pyupbit
import pandas as pd
import os
from datetime import datetime

# ===== [설정] 업비트 API 키 로드 (파일에서 읽기) =====
# trader.py 수정

def load_upbit_keys(file_name="upbit_key.txt"):
    try:
        # 현재 실행 중인 파이썬 파일(trader.py)의 절대 경로를 구함
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, file_name)
        
        print(f"[DEBUG] 키 파일 경로: {file_path}") # 확인용 로그

        with open(file_path, "r") as f:
            lines = f.readlines()
            access = lines[0].strip()
            secret = lines[1].strip()
            return access, secret
    except FileNotFoundError:
        print(f"!!! 에러: '{file_name}' 파일을 찾을 수 없습니다.")
        return None, None
    except IndexError:
        print(f"!!! 에러: 키 파일 형식이 잘못되었습니다.")
        return None, None


ACCESS_KEY, SECRET_KEY = load_upbit_keys()

print(ACCESS_KEY[:10], SECRET_KEY[:10])