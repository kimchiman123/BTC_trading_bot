FROM python:3.10-slim

# 환경 변수 설정
ENV PYTHONUNBUFFERED=1

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 의존성 설치 (psycopg2 등을 위해 필요)
# netcat (nc)는 entrypoint에서 포트 체크용으로 사용
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# 파이썬 라이브러리 설치
# 캐시 활용을 위해 requirements 설치를 먼저 수행하는 것이 좋으나,
# 편의상 한 번에 설치합니다. (변경 사항이 적다면 requirements.txt 분리 권장)
RUN pip install --no-cache-dir \
    pyupbit \
    psycopg2-binary \
    xgboost \
    kafka-python \
    pandas \
    joblib \
    scikit-learn \
    streamlit \
    plotly \
    matplotlib

# Entrypoint 스크립트 복사 및 실행 권한 부여
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

# 소스 코드는 docker-compose에서 볼륨으로 마운트할 예정이지만,
# 기본적으로 복사해두는 것이 안전합니다.
COPY . /app

# Entrypoint 설정
ENTRYPOINT ["entrypoint.sh"]

# 기본 실행 명령
CMD ["python", "main.py"]
