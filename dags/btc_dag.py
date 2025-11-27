from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import numpy as np

import psycopg2
from psycopg2.extras import RealDictCursor


# ===== Postgres 설정 =====
PG_CONN_INFO = {
    "host": "postgres",  # docker-compose 서비스 이름
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

def create_tables(**context):
    """
    필요한 데이터베이스 테이블을 생성합니다.
    현재는 btc_1m_candles만 생성하고, btc_realtime_features는 주석 처리되어 있습니다.
    """
    # 1. 1분봉 캔들 데이터 저장용 테이블 (Kafka -> DB)
    sql_candles = """
    CREATE TABLE IF NOT EXISTS btc_1m_candles (
        ticker VARCHAR(10) NOT NULL,
        ts TIMESTAMP NOT NULL,
        open FLOAT NOT NULL,
        high FLOAT NOT NULL,
        low FLOAT NOT NULL,
        close FLOAT NOT NULL,
        volume FLOAT NOT NULL,
        value FLOAT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ticker, ts)
    );
    -- 인덱스 생성 (최신 데이터 조회 성능 향상)
    CREATE INDEX IF NOT EXISTS idx_btc_1m_ts ON btc_1m_candles (ts DESC);
    """

    sql_features = """
    CREATE TABLE IF NOT EXISTS btc_realtime_features (
        ts                   TIMESTAMP PRIMARY KEY,

        -- [1] 원본 OHLCV 및 기본 피처 (1분봉)
        open                 DOUBLE PRECISION,
        high                 DOUBLE PRECISION,
        low                  DOUBLE PRECISION,
        close                DOUBLE PRECISION,
        volume               DOUBLE PRECISION,
        value                DOUBLE PRECISION,

        sma_5                DOUBLE PRECISION,
        sma_10               DOUBLE PRECISION,
        sma_20               DOUBLE PRECISION,
        sma_60               DOUBLE PRECISION,
        sma_120              DOUBLE PRECISION,
        ema_12               DOUBLE PRECISION,
        ema_26               DOUBLE PRECISION,
        rsi_14               DOUBLE PRECISION,
        macd                 DOUBLE PRECISION,
        macd_sig             DOUBLE PRECISION,
        macd_hist            DOUBLE PRECISION,
        bb_mid               DOUBLE PRECISION,
        bb_width             DOUBLE PRECISION,
        bb_pct               DOUBLE PRECISION,
        atr_14               DOUBLE PRECISION,
        vol_ma_20            DOUBLE PRECISION,
        log_return           DOUBLE PRECISION,
        volatility           DOUBLE PRECISION,

        -- 시간 주기성 피처
        hour_sin             DOUBLE PRECISION,
        hour_cos             DOUBLE PRECISION,
        dow_sin              DOUBLE PRECISION,
        dow_cos              DOUBLE PRECISION,

        -- [2] 3분봉 지표 (Suffix: _3m)
        sma_5_3m             DOUBLE PRECISION,
        sma_10_3m            DOUBLE PRECISION,
        sma_20_3m            DOUBLE PRECISION,
        sma_60_3m            DOUBLE PRECISION,
        sma_120_3m           DOUBLE PRECISION,
        ema_12_3m            DOUBLE PRECISION,
        ema_26_3m            DOUBLE PRECISION,
        rsi_14_3m            DOUBLE PRECISION,
        macd_3m              DOUBLE PRECISION,
        macd_sig_3m          DOUBLE PRECISION,
        macd_hist_3m         DOUBLE PRECISION,
        bb_mid_3m            DOUBLE PRECISION,
        bb_width_3m          DOUBLE PRECISION,
        bb_pct_3m            DOUBLE PRECISION,
        atr_14_3m            DOUBLE PRECISION,
        vol_ma_20_3m         DOUBLE PRECISION,
        log_return_3m        DOUBLE PRECISION,
        volatility_3m        DOUBLE PRECISION,

        -- [3] 5분봉 지표 (Suffix: _5m)
        sma_5_5m             DOUBLE PRECISION,
        sma_10_5m            DOUBLE PRECISION,
        sma_20_5m            DOUBLE PRECISION,
        sma_60_5m            DOUBLE PRECISION,
        sma_120_5m           DOUBLE PRECISION,
        ema_12_5m            DOUBLE PRECISION,
        ema_26_5m            DOUBLE PRECISION,
        rsi_14_5m            DOUBLE PRECISION,
        macd_5m              DOUBLE PRECISION,
        macd_sig_5m          DOUBLE PRECISION,
        macd_hist_5m         DOUBLE PRECISION,
        bb_mid_5m            DOUBLE PRECISION,
        bb_width_5m          DOUBLE PRECISION,
        bb_pct_5m            DOUBLE PRECISION,
        atr_14_5m            DOUBLE PRECISION,
        vol_ma_20_5m         DOUBLE PRECISION,
        log_return_5m        DOUBLE PRECISION,
        volatility_5m        DOUBLE PRECISION,

        -- [4] 15분봉 지표 (Suffix: _15m)
        sma_5_15m            DOUBLE PRECISION,
        sma_10_15m           DOUBLE PRECISION,
        sma_20_15m           DOUBLE PRECISION,
        sma_60_15m           DOUBLE PRECISION,
        sma_120_15m          DOUBLE PRECISION,
        ema_12_15m           DOUBLE PRECISION,
        ema_26_15m           DOUBLE PRECISION,
        rsi_14_15m           DOUBLE PRECISION,
        macd_15m             DOUBLE PRECISION,
        macd_sig_15m         DOUBLE PRECISION,
        macd_hist_15m        DOUBLE PRECISION,
        bb_mid_15m           DOUBLE PRECISION,
        bb_width_15m         DOUBLE PRECISION,
        bb_pct_15m           DOUBLE PRECISION,
        atr_14_15m           DOUBLE PRECISION,
        vol_ma_20_15m        DOUBLE PRECISION,
        log_return_15m       DOUBLE PRECISION,
        volatility_15m       DOUBLE PRECISION,

        -- [5] 분수 차분 피처 (Suffix: _fd0_4)
        close_fd0_4          DOUBLE PRECISION,
        sma_5_fd0_4          DOUBLE PRECISION,
        sma_10_fd0_4         DOUBLE PRECISION,
        sma_20_fd0_4         DOUBLE PRECISION,
        ema_12_fd0_4         DOUBLE PRECISION,
        ema_26_fd0_4         DOUBLE PRECISION,
        rsi_14_fd0_4         DOUBLE PRECISION,
        macd_fd0_4           DOUBLE PRECISION,
        bb_pct_fd0_4         DOUBLE PRECISION,
        volatility_fd0_4     DOUBLE PRECISION,

        -- [6] 동적 Threshold (코드상에서 계산한다면)
        threshold            DOUBLE PRECISION,

        -- 메타데이터 (디버깅용)
        created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """


    try:
        with psycopg2.connect(**PG_CONN_INFO) as conn:
            with conn.cursor() as cur:
                # 1분봉 테이블 생성
                cur.execute(sql_candles)
                cur.execute(sql_features)
                
                conn.commit()
    except Exception as e:
        print(f"[create_tables] 테이블 생성 중 오류 발생: {e}")
        raise e


def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1 / period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1 / period, adjust=False).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def calculate_macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist


def calculate_bollinger(series, period=20, std_dev=2):
    ma = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper = ma + (std * std_dev)
    lower = ma - (std * std_dev)
    width = (upper - lower) / (ma.replace(0, np.nan))
    pct = (series - lower) / (upper - lower)
    return upper, ma, lower, width, pct


def calculate_atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr


def get_weights_ffd(d, thres=1e-4):
    w = [1.0]
    k = 1
    while True:
        w_k = -w[-1] * (d - k + 1) / k
        if abs(w_k) < thres:
            break
        w.append(w_k)
        k += 1
    w = np.array(w[::-1]).reshape(-1, 1)
    return w


def frac_diff_ffd(series, d, thres=1e-4):
    series = series.astype("float64").dropna()
    w = get_weights_ffd(d, thres)
    width = len(w)

    out = pd.Series(index=series.index, dtype="float64")
    for i in range(width - 1, len(series)):
        window = series.iloc[i - width + 1 : i + 1].values
        out.iloc[i] = np.dot(w.T, window)[0]
    return out


def add_indicators(df):
    df = df.copy()
    numeric_cols = ["open", "high", "low", "close", "volume", "value"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for w in [5, 10, 20, 60, 120]:
        # SMA도 데이터가 적을 때 계산되도록 min_periods 추가 (선택사항)
        df[f"sma_{w}"] = df["close"].rolling(window=w, min_periods=1).mean()

    df["ema_12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["ema_26"] = df["close"].ewm(span=26, adjust=False).mean()
    df["rsi_14"] = calculate_rsi(df["close"], 14)
    df["macd"], df["macd_sig"], df["macd_hist"] = calculate_macd(df["close"])
    _, df["bb_mid"], _, df["bb_width"], df["bb_pct"] = calculate_bollinger(df["close"])
    df["atr_14"] = calculate_atr(df)
    
    df["vol_ma_20"] = df["volume"].rolling(window=20, min_periods=1).mean()
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    
    # [수정] volatility 계산 시 최소 5개 데이터만 있어도 계산되도록 완화
    df["volatility"] = df["log_return"].rolling(window=20, min_periods=5).std()

    return df

def add_cyclical_features(df):
    df = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df["hour_sin"] = np.sin(2 * np.pi * df.index.hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df.index.hour / 24)
    df["dow_sin"] = np.sin(2 * np.pi * df.index.dayofweek / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df.index.dayofweek / 7)
    return df


def resample_data(df, interval):
    agg_dict = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "value": "sum",
    }
    resampled = df.resample(interval, closed="left", label="left").agg(agg_dict).dropna()
    return resampled


def merge_higher_tf(original_df, higher_df, suffix):
    # OHLCV 말고 지표 컬럼만 사용
    cols_to_merge = [
        c
        for c in higher_df.columns
        if c not in ["open", "high", "low", "close", "volume", "value"]
    ]
    subset = higher_df[cols_to_merge].copy()

    # 미래누수 방지: 상위 TF 지표는 한 칸 뒤로 밀기
    subset = subset.shift(1)

    # rsi_14 -> rsi_14_3m 이런 식으로 suffix 부여 (오프라인 코드와 동일)
    subset.columns = [f"{c}_{suffix}" for c in subset.columns]

    # 1분봉 인덱스에 맞춰 ffill
    subset_reindexed = subset.reindex(original_df.index, method="ffill")
    return pd.concat([original_df, subset_reindexed], axis=1)


# ===== 2. DB에서 최근 N개 1분봉 읽기 =====
def fetch_recent_1m_candles(limit: int = 2000) -> pd.DataFrame:
    sql = """
        SELECT
            ts,
            open,
            high,
            low,
            close,
            volume,
            value
        FROM btc_1m_candles
        ORDER BY ts DESC
        LIMIT %s;
    """
    with psycopg2.connect(**PG_CONN_INFO) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()

    if not rows:
        raise ValueError("btc_1m_candles 테이블에 데이터가 없습니다.")

    df = pd.DataFrame(rows)
    df.set_index("ts", inplace=True)
    df.sort_index(inplace=True)

    # 1) ts 기준 중복 제거 (오프라인 코드에서 하던 것)
    df = df[~df.index.duplicated(keep="last")]

    # 2) 숫자형 강제 캐스팅 (이전 답변에서 추가했던 부분)
    numeric_cols = ["open", "high", "low", "close", "volume", "value"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def calculate_dynamic_threshold(df_final: pd.DataFrame, min_threshold: float = 0.585, max_threshold: float = 0.90) -> float:
    """
    MACD 히스토그램과 RSI 지표를 기반으로 동적 Threshold를 계산합니다.
    """
    if df_final.empty:
        return max_threshold  # 데이터가 없으면 가장 보수적인 값 적용
    
    # 가장 최근의 1분봉 데이터 추출
    last_row = df_final.iloc[-1]
    
    # 필요한 지표 값
    rsi = last_row.get('rsi_14')
    macd_hist = last_row.get('macd_hist')
    macd_line = last_row.get('macd')
    
    # 지표가 NaN이면 보수적인 값 적용
    if pd.isna(rsi) or pd.isna(macd_hist) or pd.isna(macd_line):
        return max_threshold

    # 1. MACD Score
    macd_score = 0.0
    if macd_hist > 0 and macd_line > 0:
        macd_score = 1.0
    elif macd_hist > 0:
        macd_score = 0.5
    elif macd_hist < 0 and macd_line < 0:
        macd_score = -1.0
    else:
        macd_score = 0.0

    # 2. RSI Score
    if rsi >= 70 or rsi <= 30:
        rsi_normalized = -1.0 
    elif 50 < rsi < 70:
        rsi_normalized = (rsi - 50) / 20.0 
    elif 30 < rsi <= 50:
        rsi_normalized = (rsi - 50) / 20.0 
    else:
        rsi_normalized = 0.0

    # 3. 종합 Score 계산
    combined_score = (macd_score + rsi_normalized) / 2.0 

    # 4. Score를 Threshold로 매핑
    weight_factor = (combined_score + 1.0) / 2.0
    dynamic_threshold = max_threshold - (max_threshold - min_threshold) * weight_factor

    return round(dynamic_threshold, 4)



# ===== 3. 실시간 1개 시점 피처 생성 =====
def make_realtime_feature(**context):
    # 1) 최근 N개 1분봉 로드
    N = 2000
    df_1m = fetch_recent_1m_candles(limit=N)

    print(f"[DEBUG] 로드된 1분봉 개수: {len(df_1m)}")

    # 2) 1분봉 지표 + 시간 피처
    df_1m = add_indicators(df_1m)
    df_1m = add_cyclical_features(df_1m)

    # 3) 상위 타임프레임 생성 + 지표 계산
    df_3m = add_indicators(resample_data(df_1m, "3min"))
    df_5m = add_indicators(resample_data(df_1m, "5min"))
    df_15m = add_indicators(resample_data(df_1m, "15min"))

    # 4) 상위 TF 지표 merge
    df_final = merge_higher_tf(df_1m, df_3m, "3m")
    df_final = merge_higher_tf(df_final, df_5m, "5m")
    df_final = merge_higher_tf(df_final, df_15m, "15m")
    
    df_final = df_final[~df_final.index.duplicated(keep="last")]

    # 5) 분수 차분 피처 추가
    FRAC_D = 0.4
    THRES = 1e-4
    frac_cols = [
        "close", "sma_5", "sma_10", "sma_20",
        "ema_12", "ema_26", "rsi_14", "macd",
        "bb_pct", "volatility",
    ]
    frac_cols = [c for c in frac_cols if c in df_final.columns]

    for col in frac_cols:
        fd_series = frac_diff_ffd(df_final[col], d=FRAC_D, thres=THRES)
        df_final[f"{col}_fd{FRAC_D}"] = fd_series

    df_final.replace([np.inf, -np.inf], np.nan, inplace=True)

    if df_final.empty:
        print("[make_realtime_feature] 데이터프레임이 비어있어 스킵합니다.")
        return

    # ===== [핵심] 동적 Threshold 계산 로직 적용 =====
    # 전체 DataFrame을 넘겨서 계산 (함수 내부에서 마지막 행 사용)
    calc_threshold = calculate_dynamic_threshold(df_final)
    
    # 계산된 값을 DataFrame의 'threshold' 컬럼에 할당 (전체에 넣거나 마지막 행에만 넣어도 됨)
    df_final['threshold'] = calc_threshold

    # ------------------------------------------------

    latest_ts = df_final.index.max()
    latest_row = df_final.loc[latest_ts].copy()

    # 핵심 피처 NaN 체크
    core_cols = [
        "close", "sma_5", "sma_10", "sma_20",
        "ema_12", "ema_26", "rsi_14", "macd",
        "bb_pct", "volatility",
    ]
    
    missing_core = [c for c in core_cols if c in latest_row.index and pd.isna(latest_row[c])]

    if missing_core:
        print(f"[make_realtime_feature] latest_ts={latest_ts} 핵심 피처 NaN({missing_core}) → 이번 분 스킵")
        return

    # DB 저장 준비
    values = [latest_ts] + latest_row.tolist()
    safe_cols = [c.replace(".", "_") for c in latest_row.index]
    
    with psycopg2.connect(**PG_CONN_INFO) as conn:
        insert_cols = ["ts"] + safe_cols
        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_list = ", ".join(insert_cols)
        update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in safe_cols])

        sql = f"""
            INSERT INTO btc_realtime_features
            ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (ts) DO UPDATE SET
            {update_clause};
        """

        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()

    print(f"[make_realtime_feature] latest_ts={latest_ts} 저장 완료 (threshold={latest_row['threshold']})")

# ===== 4. Airflow DAG 정의 =====
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="btc_realtime_feature_dag",
    default_args=default_args,
    start_date=datetime(2025, 11, 23),
    schedule_interval="*/1 * * * *",  # 1분마다
    catchup=False,
    max_active_runs=1,
    tags=["btc", "realtime", "features"],
) as dag:
    create_table_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
        provide_context=True,
    )

    make_feature_task = PythonOperator(
        task_id="make_realtime_feature",
        python_callable=make_realtime_feature,
        provide_context=True,
    )

    make_feature_task
