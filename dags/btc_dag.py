from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np  # [중요] 가우시안 계산을 위해 필요
import psycopg2
from psycopg2.extras import RealDictCursor

from airflow import DAG
from airflow.operators.python import PythonOperator

# ===== Postgres 설정 =====
PG_CONN_INFO = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

# ===== 1. 테이블 생성 및 DB 유틸 =====
def create_tables(**context):
    """
    필요한 데이터베이스 테이블을 생성합니다.
    주의: 타임프레임 변경으로 컬럼이 바뀌었으므로, 기존 테이블이 있다면 DROP 후 실행하는 것을 권장합니다.
    """
    # 1. 1분봉 캔들 데이터 저장용 테이블
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
    CREATE INDEX IF NOT EXISTS idx_btc_1m_ts ON btc_1m_candles (ts DESC);
    """

    # 2. 실시간 피처 테이블 (타임프레임: 15m, 30m, 1h, 4h, 6h 반영)
    sql_features = """
    CREATE TABLE IF NOT EXISTS btc_realtime_features (
        ts                   TIMESTAMP PRIMARY KEY,

        -- [1] 원본 1분봉 및 기본 피처
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

        -- [2] 15분봉 지표 (Suffix: _15m)
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

        -- [3] 30분봉 지표 (Suffix: _30m)
        sma_5_30m            DOUBLE PRECISION,
        sma_10_30m           DOUBLE PRECISION,
        sma_20_30m           DOUBLE PRECISION,
        sma_60_30m           DOUBLE PRECISION,
        sma_120_30m          DOUBLE PRECISION,
        ema_12_30m           DOUBLE PRECISION,
        ema_26_30m           DOUBLE PRECISION,
        rsi_14_30m           DOUBLE PRECISION,
        macd_30m             DOUBLE PRECISION,
        macd_sig_30m         DOUBLE PRECISION,
        macd_hist_30m        DOUBLE PRECISION,
        bb_mid_30m           DOUBLE PRECISION,
        bb_width_30m         DOUBLE PRECISION,
        bb_pct_30m           DOUBLE PRECISION,
        atr_14_30m           DOUBLE PRECISION,
        vol_ma_20_30m        DOUBLE PRECISION,
        log_return_30m       DOUBLE PRECISION,
        volatility_30m       DOUBLE PRECISION,

        -- [4] 1시간봉 지표 (Suffix: _1h)
        sma_5_1h             DOUBLE PRECISION,
        sma_10_1h            DOUBLE PRECISION,
        sma_20_1h            DOUBLE PRECISION,
        sma_60_1h            DOUBLE PRECISION,
        sma_120_1h           DOUBLE PRECISION,
        ema_12_1h            DOUBLE PRECISION,
        ema_26_1h            DOUBLE PRECISION,
        rsi_14_1h            DOUBLE PRECISION,
        macd_1h              DOUBLE PRECISION,
        macd_sig_1h          DOUBLE PRECISION,
        macd_hist_1h         DOUBLE PRECISION,
        bb_mid_1h            DOUBLE PRECISION,
        bb_width_1h          DOUBLE PRECISION,
        bb_pct_1h            DOUBLE PRECISION,
        atr_14_1h            DOUBLE PRECISION,
        vol_ma_20_1h         DOUBLE PRECISION,
        log_return_1h        DOUBLE PRECISION,
        volatility_1h        DOUBLE PRECISION,

        -- [5] 4시간봉 지표 (Suffix: _4h)
        sma_5_4h             DOUBLE PRECISION,
        sma_10_4h            DOUBLE PRECISION,
        sma_20_4h            DOUBLE PRECISION,
        sma_60_4h            DOUBLE PRECISION,
        sma_120_4h           DOUBLE PRECISION,
        ema_12_4h            DOUBLE PRECISION,
        ema_26_4h            DOUBLE PRECISION,
        rsi_14_4h            DOUBLE PRECISION,
        macd_4h              DOUBLE PRECISION,
        macd_sig_4h          DOUBLE PRECISION,
        macd_hist_4h         DOUBLE PRECISION,
        bb_mid_4h            DOUBLE PRECISION,
        bb_width_4h          DOUBLE PRECISION,
        bb_pct_4h            DOUBLE PRECISION,
        atr_14_4h            DOUBLE PRECISION,
        vol_ma_20_4h         DOUBLE PRECISION,
        log_return_4h        DOUBLE PRECISION,
        volatility_4h        DOUBLE PRECISION,
        
        -- [6] 6시간봉 지표 (Suffix: _6h)
        sma_5_6h             DOUBLE PRECISION,
        sma_10_6h            DOUBLE PRECISION,
        sma_20_6h            DOUBLE PRECISION,
        sma_60_6h            DOUBLE PRECISION,
        sma_120_6h           DOUBLE PRECISION,
        ema_12_6h            DOUBLE PRECISION,
        ema_26_6h            DOUBLE PRECISION,
        rsi_14_6h            DOUBLE PRECISION,
        macd_6h              DOUBLE PRECISION,
        macd_sig_6h          DOUBLE PRECISION,
        macd_hist_6h         DOUBLE PRECISION,
        bb_mid_6h            DOUBLE PRECISION,
        bb_width_6h          DOUBLE PRECISION,
        bb_pct_6h            DOUBLE PRECISION,
        atr_14_6h            DOUBLE PRECISION,
        vol_ma_20_6h         DOUBLE PRECISION,
        log_return_6h        DOUBLE PRECISION,
        volatility_6h        DOUBLE PRECISION,

        -- [7] 분수 차분 및 기타
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

        threshold            DOUBLE PRECISION,
        created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    try:
        with psycopg2.connect(**PG_CONN_INFO) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_candles)
                cur.execute(sql_features)
                conn.commit()
    except Exception as e:
        print(f"[create_tables] 테이블 생성 중 오류 발생: {e}")
        raise e

# ===== 2. 지표 계산 함수들 =====
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
        df[f"sma_{w}"] = df["close"].rolling(window=w, min_periods=1).mean()

    df["ema_12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["ema_26"] = df["close"].ewm(span=26, adjust=False).mean()
    df["rsi_14"] = calculate_rsi(df["close"], 14)
    df["macd"], df["macd_sig"], df["macd_hist"] = calculate_macd(df["close"])
    _, df["bb_mid"], _, df["bb_width"], df["bb_pct"] = calculate_bollinger(df["close"])
    df["atr_14"] = calculate_atr(df)
    
    df["vol_ma_20"] = df["volume"].rolling(window=20, min_periods=1).mean()
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
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
    cols_to_merge = [
        c for c in higher_df.columns
        if c not in ["open", "high", "low", "close", "volume", "value"]
    ]
    subset = higher_df[cols_to_merge].copy()
    subset = subset.shift(1)  # 미래 참조 방지
    subset.columns = [f"{c}_{suffix}" for c in subset.columns]
    
    subset_reindexed = subset.reindex(original_df.index, method="ffill")
    return pd.concat([original_df, subset_reindexed], axis=1)

# ===== 3. 핵심 로직: 동적 Threshold & 피처 생성 =====

def calculate_dynamic_threshold(df_final: pd.DataFrame, 
                              min_threshold: float = 0.50, 
                              max_threshold: float = 0.70) -> float:
    """
    [Logic v2]
    1. RSI: Gaussian Scoring (Target 60) -> 과매수/과매도 사이 적정 구간 우대
    2. MACD: Tanh Scoring (Sigmoid) -> 값의 크기에 따라 연속적인 강도 평가 (가격 정규화 포함)
    3. Multi-Timeframe Weighted Average
    """
    if df_final.empty:
        return max_threshold

    row = df_final.iloc[-1]
    
    # 현재 가격 (MACD 정규화를 위해 필요)
    # df_final에 'close' 컬럼이 있어야 합니다. (이전 단계에서 이미 포함됨)
    current_price = row['close'] if 'close' in row else 100000000 # fallback

    # [내부 함수] 1. Gaussian Scoring (RSI용 - 종 모양)
    def get_gaussian_score(value, target, sigma):
        if pd.isna(value): return 0.0
        return np.exp(-((value - target) ** 2) / (2 * (sigma ** 2)))

    # [내부 함수] 2. Tanh Scoring (MACD용 - S자 모양)
    def get_tanh_score(value, price, sensitivity):
        """
        value: MACD 또는 Hist 값
        price: 현재 주가 (비트코인 가격대가 높으므로 정규화 필수)
        sensitivity: 민감도 (값이 클수록 작은 변화에도 점수가 크게 변함)
        
        Returns: -1.0 ~ 1.0 사이의 값
        """
        if pd.isna(value) or price == 0: return 0.0
        
        # MACD를 가격 대비 퍼센트로 변환 (예: 1억 비트의 MACD 10만 -> 0.1%)
        normalized_val = value / price 
        
        # tanh 함수 적용: 값이 커질수록 1.0에, 작아질수록 -1.0에 수렴
        return np.tanh(normalized_val * sensitivity)

    def get_market_score(rsi_val, macd_hist_val, macd_line_val, price):
        if pd.isna(rsi_val) or pd.isna(macd_hist_val) or pd.isna(macd_line_val):
            return 0.0
        
        # --- 1. RSI Score (0 ~ 1.0) ---
        # Target=60, Sigma=15 (RSI 45~75 구간 선호)
        rsi_score = get_gaussian_score(rsi_val, target=60, sigma=15)
        
        # RSI 점수를 -1.0 ~ 1.0 범위로 확장 (MACD와 스케일 맞춤)
        # rsi_score가 1.0일 때 -> 1.0
        # rsi_score가 0.0일 때 -> -1.0
        rsi_scaled = (rsi_score * 2) - 1.0 

        # --- 2. MACD Score (-1.0 ~ 1.0) ---
        # 비트코인 변동성을 고려하여 민감도(Sensitivity) 설정
        # Trend(Line)는 큰 흐름이므로 민감도를 낮게(300), 
        # Momentum(Hist)은 빠른 반응이 필요하므로 민감도를 높게(500) 설정
        
        score_line = get_tanh_score(macd_line_val, price, sensitivity=300)
        score_hist = get_tanh_score(macd_hist_val, price, sensitivity=500)
        
        # MACD 종합 점수 (추세 + 모멘텀) -> 범위 -1.0 ~ 1.0 유지
        macd_combined = (score_line + score_hist) / 2.0
        
        # --- 3. 최종 결합 ---
        # RSI와 MACD를 50:50으로 반영
        return (rsi_scaled + macd_combined) / 2.0

    # 타임프레임별 가중치 (Trend Follow)
    weights = {
        '6h': 0.35, '4h': 0.25, '1h': 0.20,
        '30m': 0.10, '15m': 0.05, '1m': 0.05
    }

    tf_map = {
        '6h': '_6h', '4h': '_4h', '1h': '_1h', 
        '30m': '_30m', '15m': '_15m', '1m': '' 
    }

    total_score = 0.0
    total_weight = 0.0

    for tf, weight in weights.items():
        suffix = tf_map[tf]
        rsi_col = f"rsi_14{suffix}"
        hist_col = f"macd_hist{suffix}"
        macd_col = f"macd{suffix}"
        
        if rsi_col in row and hist_col in row and macd_col in row:
            # get_market_score 호출 시 current_price 전달 추가
            score = get_market_score(row[rsi_col], row[hist_col], row[macd_col], current_price)
            total_score += score * weight
            total_weight += weight
    
    if total_weight == 0:
        return max_threshold

    # 최종 가중 평균 점수 (-1.0 ~ 1.0)
    final_market_score = total_score / total_weight 

    # Score -> Threshold 매핑
    # Score 1.0 (최상) -> min_threshold (0.50)
    # Score -1.0 (최악) -> max_threshold (0.70)
    normalized_factor = (final_market_score + 1.0) / 2.0
    dynamic_threshold = max_threshold - (max_threshold - min_threshold) * normalized_factor

    return round(dynamic_threshold, 4)

def fetch_recent_1m_candles(limit: int = 50000) -> pd.DataFrame:
    sql = """
        SELECT ts, open, high, low, close, volume, value
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
    df = df[~df.index.duplicated(keep="last")]

    numeric_cols = ["open", "high", "low", "close", "volume", "value"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def make_realtime_feature(**context):
    # 1) 데이터 로드 (6시간봉 계산을 위해 충분한 양)
    df_1m = fetch_recent_1m_candles(limit=50000)
    print(f"[DEBUG] 로드된 1분봉 개수: {len(df_1m)}")

    # 2) 1분봉 지표 + 시간 피처
    df_1m = add_indicators(df_1m)
    df_1m = add_cyclical_features(df_1m)

    # 3) 상위 타임프레임 생성 (15m, 30m, 1h, 4h, 6h)
    df_15m = add_indicators(resample_data(df_1m, "15min"))
    df_30m = add_indicators(resample_data(df_1m, "30min"))
    df_1h  = add_indicators(resample_data(df_1m, "1h"))
    df_4h  = add_indicators(resample_data(df_1m, "4h"))
    df_6h  = add_indicators(resample_data(df_1m, "6h"))

    # 4) 상위 TF 지표 Merge
    df_final = merge_higher_tf(df_1m, df_15m, "15m")
    df_final = merge_higher_tf(df_final, df_30m, "30m")
    df_final = merge_higher_tf(df_final, df_1h,  "1h")
    df_final = merge_higher_tf(df_final, df_4h,  "4h")
    df_final = merge_higher_tf(df_final, df_6h,  "6h")
    
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

    # 6) 동적 Threshold 계산 (고도화 버전 적용)
    calc_threshold = calculate_dynamic_threshold(df_final)
    df_final['threshold'] = calc_threshold

    # 7) 최신 데이터 DB 적재
    latest_ts = df_final.index.max()
    latest_row = df_final.loc[latest_ts].copy()

    # 핵심 피처 NaN 체크
    core_cols = ["close", "rsi_14", "macd"]
    missing_core = [c for c in core_cols if c in latest_row.index and pd.isna(latest_row[c])]

    if missing_core:
        print(f"[make_realtime_feature] latest_ts={latest_ts} 핵심 피처 NaN({missing_core}) → 이번 분 스킵")
        return

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
    schedule_interval="*/1 * * * *",
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

    create_table_task >> make_feature_task