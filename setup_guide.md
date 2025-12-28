# 네트워크 및 보안 설정 가이드

로컬 PC에서 클라우드 DB에 접속하거나 웹 대시보드를 외부에서 확인하기 위해 필요한 설정을 안내합니다.

## 1. 클라우드 방화벽 (Firewall) 설정

### GCP (Google Cloud Platform)
1. **VPC 네트워크** -> **방화벽** 메뉴 이동.
2. **방화벽 규칙 만들기** 클릭.
3. 다음 규칙 추가:
    - **이름**: `allow-postgres-dashboard`
    - **대상**: 지정된 대상 태그 (인스턴스에 태그 추가 필요) 또는 `네트워크의 모든 인스턴스`.
    - **소스 필터**: `IPv4 범위`.
    - **소스 IP 범위**:
        - 보안을 위해 **본인 집/사무실 IP만 허용**하는 것을 권장합니다 (예: `123.45.67.89/32`).
        - 테스트 목적이라면 `0.0.0.0/0` (전체 허용) - **주의: 해킹 위험 있음**.
    - **프로토콜 및 포트**: `tcp:5432`, `tcp:8501`.

### AWS (EC2 Security Groups)
1. 인스턴스의 **보안 그룹(Security Group)** 선택 -> **Inbound Rules** 편집.
2. 규칙 추가:
    - **Type**: Custom TCP
    - **Port Range**: `5432` (Postgres)
    - **Source**: `My IP` (자동 감지됨).
3. 규칙 추가:
    - **Type**: Custom TCP
    - **Port Range**: `8501` (Streamlit Dashboard)
    - **Source**: `Anywhere-IPv4` (0.0.0.0/0) 또는 `My IP`.

---

## 2. Postgres 설정 (외부 접속 허용)

`docker-compose.yml`로 실행된 Postgres는 기본적으로 `0.0.0.0`을 리스닝하지만, 경우에 따라 `pg_hba.conf` 설정이 필요할 수 있습니다.

### 확인 사항
- Postgres 컨테이너는 기본적으로 모든 IP(`0.0.0.0/0`)에서의 접속을 허용하도록 설정되어 있습니다 (`host all all 0.0.0.0/0 trust` 또는 `md5`).
- 만약 접속이 안 된다면 `postgresql.conf`의 `listen_addresses`가 `*`인지 확인해야 하지만, 기본 도커 이미지는 이미 설정되어 있습니다.

### 접속 테스트
로컬 PC에서 `local_monitor.py`를 실행하거나 `psql` 등으로 테스트:
```bash
psql -h [클라우드PublicIP] -p 5432 -U airflow -d airflow
```

---

## 3. 웹 대시보드 접속

브라우저 주소창에 다음 입력:
`http://[클라우드PublicIP]:8501`

- 봇의 실시간 상태, 가격, 예측 점수 그래프를 확인할 수 있습니다.
- 모바일에서도 접속 가능합니다.
