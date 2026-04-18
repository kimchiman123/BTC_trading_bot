import pandas as pd
import numpy as np

# ===== 지표 계산 함수들 =====
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
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum", "value": "sum",
    }
    resampled = df.resample(interval, closed="left", label="left").agg(agg_dict).dropna()
    return resampled

def merge_higher_tf(original_df, higher_df, suffix):
    cols_to_merge = [c for c in higher_df.columns if c not in ["open", "high", "low", "close", "volume", "value"]]
    subset = higher_df[cols_to_merge].copy()
    subset = subset.shift(1)  # 미래 참조 방지
    subset.columns = [f"{c}_{suffix}" for c in subset.columns]
    subset_reindexed = subset.reindex(original_df.index, method="ffill")
    return pd.concat([original_df, subset_reindexed], axis=1)

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

# ===== Threshold 계산 (최신 Tanh Logic 적용) =====
# 12.13일 기준, 굉장히 약세, 횡보장이므로 거래를 확인하기 위해, 0.5 -> 0.45 0.7 -> 0.55로 변경 
def calculate_dynamic_threshold(df_final: pd.DataFrame, 
                              min_threshold: float = 0.45, 
                              max_threshold: float = 0.55) -> float:
    if df_final.empty:
        return max_threshold

    row = df_final.iloc[-1]
    current_price = row['close'] if 'close' in row else 100000000

    def get_gaussian_score(value, target, sigma):
        if pd.isna(value): return 0.0
        return np.exp(-((value - target) ** 2) / (2 * (sigma ** 2)))

    def get_tanh_score(value, price, sensitivity):
        if pd.isna(value) or price == 0: return 0.0
        normalized_val = value / price 
        return np.tanh(normalized_val * sensitivity)

    def get_market_score(rsi_val, macd_hist_val, macd_line_val, price):
        if pd.isna(rsi_val) or pd.isna(macd_hist_val) or pd.isna(macd_line_val):
            return 0.0
        
        rsi_score = get_gaussian_score(rsi_val, target=60, sigma=15)
        rsi_scaled = (rsi_score * 2) - 1.0 

        score_line = get_tanh_score(macd_line_val, price, sensitivity=300)
        score_hist = get_tanh_score(macd_hist_val, price, sensitivity=500)
        macd_combined = (score_line + score_hist) / 2.0
        
        return (rsi_scaled + macd_combined) / 2.0

    weights = {'6h': 0.35, '4h': 0.25, '1h': 0.20, '30m': 0.10, '15m': 0.05, '1m': 0.05}
    tf_map = {'6h': '_6h', '4h': '_4h', '1h': '_1h', '30m': '_30m', '15m': '_15m', '1m': ''}

    total_score = 0.0
    total_weight = 0.0

    for tf, weight in weights.items():
        suffix = tf_map[tf]
        rsi_col = f"rsi_14{suffix}"
        hist_col = f"macd_hist{suffix}"
        macd_col = f"macd{suffix}"
        
        if rsi_col in row and hist_col in row and macd_col in row:
            score = get_market_score(row[rsi_col], row[hist_col], row[macd_col], current_price)
            total_score += score * weight
            total_weight += weight
    
    if total_weight == 0:
        return max_threshold

    final_market_score = total_score / total_weight 
    normalized_factor = (final_market_score + 1.0) / 2.0
    dynamic_threshold = max_threshold - (max_threshold - min_threshold) * normalized_factor

    return round(dynamic_threshold, 4)