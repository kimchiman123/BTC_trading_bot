# 💰 Real-time BTC Trading Pipeline (Kafka & Multiprocessing)

> 업비트 API 기반 실시간 비트코인 단기 트레이딩 시스템  
> 데이터 수집 → 스트리밍 처리 → AI 추론 → 자동 매매까지 엔드투엔드 파이프라인

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![XGBoost](https://img.shields.io/badge/XGBoost-FF6600?logo=xgboost&logoColor=white)](https://xgboost.readthedocs.io/)

---

## 📌 프로젝트 개요 (Project Overview)

알고리즘 기반 암호화폐 트레이딩 봇이 실제 수익을 창출하는 비즈니스 모델로 자리 잡고 있는 트렌드에서 착안하여,  
**KRW-BTC(비트코인)** 거래쌍을 대상으로 실시간 단기 트레이딩 시스템을 구축한 프로젝트입니다.

### 🎯 데이터 엔지니어링 역량 강조 포인트

| 영역 | 핵심 내용 |
|------|-----------|
| **아키텍처 전환** | 리소스 제약 환경에서 Airflow 배치 구조를 **Kafka 기반 실시간 스트리밍 구조**로 최적화 |
| **시스템 안정성** | Python `multiprocessing`을 활용한 마이크로서비스 오케스트레이션 및 **결합도 분리** |
| **데이터 무결성** | 실시간 1분봉 데이터의 **유실 없는 적재** 및 다중 타임프레임 지표 연산 |

---

## 🏗️ 1. System Architecture

업비트 API로부터 발생하는 실시간 시세 데이터를 안정적으로 **수집 → 가공 → 저장**하고,  
이를 기반으로 AI 모델이 추론을 수행할 수 있도록 **엔드투엔드 파이프라인**을 구축했습니다.

### 아키텍처 다이어그램

<p align="center">
  <img src="assets/architecture.png" alt="System Architecture Diagram" width="800"/>
</p>

### 📋 Component Description

> 아키텍처 다이어그램의 각 컴포넌트와 실제 소스 코드의 매핑입니다.

| 다이어그램 컴포넌트 | 역할 | 관련 파일 |
|:---:|:---|:---|
| **Upbit API (WebSocket)** | 실시간 1분봉 시세 데이터의 원천 소스 | — (외부 API) |
| **Kafka Producer** | 웹소켓으로 수신한 시세 데이터를 Kafka Topic으로 발행 | `producer_btc_1m_kafka.py` |
| **Kafka Broker** | Producer와 Consumer 간 메시지 중개 (Pub/Sub) | Docker Compose 내 서비스 |
| **Kafka Consumer** | 스트리밍 데이터를 구독하여 PostgreSQL에 실시간 적재 | `kafka_to_postgres.py` |
| **PostgreSQL** | 시세 데이터, 예측 결과, 거래 이력 저장소 | `apply_db_schema.py` |
| **Trading Bot** | Kafka 메시지 + DB 과거 데이터를 결합하여 XGBoost 모델로 매매 결정 | `realtime_bot.py` |
| **Streamlit Dashboard UI** | 파이프라인 상태, PnL, 승률 등 실시간 모니터링 | `dashboard.py` |
| **Orchestrator** *(다이어그램 외)* | 모든 프로세스의 생명 주기 관리, 헬스체크 및 자동 재시작 | `main.py` |

### 🔹 데이터 흐름 (Data Flow)

| 흐름 번호 | 단계 | 설명 |
|:---:|:---|:---|
| **①** | **Ingestion** | 업비트 웹소켓 → Kafka Producer → Kafka Broker로 실시간 1분봉 데이터 발행 |
| **②③** | **Storage** | Kafka Consumer가 스트리밍 데이터를 구독하여 전처리 후 PostgreSQL에 적재 |
| **④** | **Inference** | Trading Bot이 DB의 과거 데이터 및 예측 시그널을 조회하여 다중 타임프레임 지표 생성 → XGBoost 모델로 매매 결정 |
| **⑤** | **Execution** | 매수/매도 주문을 Upbit Exchange Order API로 실행 |
| **⑥** | **Monitoring** | Streamlit 대시보드가 DB를 조회하여 파이프라인 상태 및 수익률 시각화 |

---

## 🛠️ 2. Engineering Challenges & Troubleshooting

### 🚨 Issue 1: 리소스 제약 환경에서의 시스템 중단 문제

> **Airflow ➡️ Kafka + Multiprocessing 전환**

<table>
<tr><td><b>📋 배경</b></td>
<td>초기에는 Apache Airflow를 도입하여 1분 단위의 ETL 파이프라인을 구축했으나(<code>btc_dag.py</code>), <b>2GB RAM</b>의 한정된 클라우드 리소스에서 Airflow 스케줄러의 오버헤드가 발생</td></tr>

<tr><td><b>❌ 문제</b></td>
<td>빈번한 <b>OOM(Out of Memory)</b>으로 인한 프로세스 중단 및 배치 지연 → 실시간 트레이딩의 신뢰성 저하</td></tr>

<tr><td><b>✅ 해결</b></td>
<td>
• 무거운 Airflow를 제거하고 <b>Kafka Pub/Sub 구조</b>로 전환하여 데이터 수집과 소비의 결합도 분리<br/>
• <code>main.py</code>에서 Python <code>multiprocessing</code>을 활용해 각 컴포넌트를 <b>독립적인 프로세스</b>로 관리하고 헬스체크 로직 구현
</td></tr>

<tr><td><b>📈 결과</b></td>
<td>리소스 사용량을 <b>60% 이상 절감</b>하면서 1분봉 데이터의 <b>유실 없는 실시간 처리</b> 달성</td></tr>
</table>

### 🚨 Issue 2: 실시간 데이터의 지연 및 정합성 보장

<table>
<tr><td><b>❌ 문제</b></td>
<td>네트워크 지연 발생 시 Kafka에 쌓인 메시지와 DB에 적재된 과거 데이터 간의 <b>시간차</b>로 인해 모델 피처(Feature) 생성 시 데이터 정합성이 깨지는 문제 발생</td></tr>

<tr><td><b>✅ 해결</b></td>
<td>
<b>Windowing 전략:</b> <code>realtime_bot.py</code> 내부에서 일정 크기의 DataFrame 윈도우를 관리하며 새로운 메시지가 올 때마다 상위 타임프레임(15m, 1h 등) 데이터를 DB에서 즉시 보충(Upsert)<br/><br/>
<b>Retry 로직:</b> DB 연결 및 API 호출 실패 시 <b>지수 백오프(Exponential Backoff)</b>를 적용해 파이프라인의 회복 탄력성(Resilience) 확보
</td></tr>
</table>

---

## 📈 3. Project Impact

| 지표 | 성과 |
|------|------|
| **Latency** | 데이터 수집부터 모델 추론까지 지연 시간 **< 1초** |
| **Reliability** | 72시간 연속 가동 테스트 시 **데이터 유실 0%** 달성 |
| **Optimization** | 경량화된 아키텍처를 통해 **저사양 인스턴스**(2GB RAM)에서도 안정적 서빙 가능 |

---

## 🧠 4. 핵심 컴포넌트 및 로직 (Key Technologies)

이 트레이딩 모델은 단순한 가격 지표를 넘어 다음과 같은 복합적 데이터를 피처(Feature)로 활용합니다.

### 다중 타임프레임 병합 (Multi-Timeframe Integration)
`merge_higher_tf` 함수를 통해 1분봉 데이터에 **15분, 30분, 1시간, 4시간, 6시간** 수준의 거시적 추세 지표(이동평균선, MACD, RSI, 볼린저 밴드 등)를 결합하여 노이즈에 강한 데이터셋을 구축합니다.

### 분수 차분 (Fractional Differencing)
시계열 데이터의 정상성(Stationarity)을 확보하면서도 패턴의 메모리를 보존하기 위해 **d=0.4** 수준의 분수 차분을 적용하여 모델의 예측력을 높입니다.

### 트리플 배리어 기법 (Triple Barrier Method)
`check_exit_conditions` 함수를 통해 **목표가(Take Profit)**, **손절가(Stop Loss)**, **시간 제한(Time Limit)** 세 개의 장벽을 두고 포지션을 능동적으로 청산 관리합니다.

### 동적 임계값 (Dynamic Thresholding)
시장의 변동성에 따라 매수 확률 컷오프(0.40 ~ 0.55 범위)를 동적으로 조절하여, 횡보장과 급등락장을 구분하여 안전하게 진입합니다.

### 섀도우 모드 (Shadow Trading)
실제 자산이 투입되지 않더라도 예측 결과를 로깅(`shadow_trades.csv`)하여 향후 전략의 백테스트나 한계점을 검증할 수 있는 기능을 내장하고 있습니다.

---

## 🛠️ 5. Tech Stack

| Category | Technologies |
|----------|-------------|
| **Language** | Python 3.10+ |
| **Streaming** | Apache Kafka, Confluent Kafka |
| **Database** | PostgreSQL (SQLAlchemy) |
| **ML Model** | XGBoost (scikit-learn) |
| **Infrastructure** | Docker, Docker Compose, Azure (Container Apps) |
| **Visualization** | Streamlit |

---

## 🚀 6. 실행 방법 (How to Run)

### Step 1. 보안 키 설정
소스코드 내부 변수 `UPBIT_ACCESS_KEY`, `UPBIT_SECRET_KEY`를 설정하거나 별도의 환경 변수 주입 방식에 본인의 업비트 API Key를 입력합니다.

> ⚠️ **주의**: API Key는 절대 Github에 업로드하지 마세요.

### Step 2. Docker 빌드 및 실행
`docker-compose.yml`을 통해 모든 환경(PostgreSQL, Zookeeper, Kafka, Trading Bot, Dashboard)이 구축됩니다.

```bash
# 백그라운드에서 모든 서비스 실행
docker-compose up -d --build
```

### Step 3. 주요 서비스 확인

```bash
# 트레이딩 시스템 로그 확인
docker logs -f trading-bot
```

웹 브라우저에서 `http://localhost:8501`로 접속하여 대시보드를 통해 현재 시스템 상태 및 예상 수익을 모니터링할 수 있습니다.

---

## 📁 7. 프로젝트 구조 (Project Structure)

```
BTC_for_cloud/
├── main.py                      # 🎛️ 멀티프로세싱 오케스트레이터
├── producer_btc_1m_kafka.py     # 📡 Kafka Producer (1분봉 데이터 수집)
├── kafka_to_postgres.py         # 📥 Kafka Consumer → PostgreSQL 적재
├── realtime_bot.py              # 🤖 실시간 트레이딩 봇 (추론 + 매매)
├── indicators.py                # 📊 보조지표 연산 모듈
├── dashboard.py                 # 📈 Streamlit 대시보드
├── xgb_final_model.joblib       # 🧠 학습된 XGBoost 모델
├── docker-compose.yml           # 🐳 Docker Compose 설정
├── Dockerfile                   # 🐳 컨테이너 빌드 설정
├── entrypoint.sh                # 🚀 컨테이너 엔트리포인트
├── data_preprocess/             # 📓 데이터 전처리 및 모델 학습 노트북
│   └── experiments/             # 🧪 실험 및 백테스팅 기록
└── dags/                        # ⚠️ (Deprecated) Airflow DAG 코드
```

---

<p align="center">
  <sub>Built with ❤️ for Data Engineering & ML Pipeline Architecture</sub>
</p>
