import json
import time
import pandas as pd
import numpy as np
import psycopg2
import joblib
import os
import pyupbit
import signal
import sys
import atexit
from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

# 공통 모듈
from core.indicators import add_indicators, merge_higher_tf, calculate_dynamic_threshold, frac_diff_ffd, resample_data, add_cyclical_features
from core.custom_objective import FocalLossObjective 

# ==========================================
# [설정] 업비트 API 및 전략 설정
# ==========================================
UPBIT_ACCESS_KEY = ""
UPBIT_SECRET_KEY = ""
MAX_SLOTS = 3
TICKER = "KRW-BTC"
POSITION_FILE = "active_trades.json"
SHADOW_FILE = "shadow_trades.csv"  # [Shadow] 기록 파일명

TRIPLE_BARRIER = {
    'span': 1440, 'pt': 3.0, 'sl': 0.7, 'time_limit': 1440
}

KAFKA_BOOTSTRAP_SERVERS = [f"{os.getenv('KAFKA_HOST', 'kafka')}:{os.getenv('KAFKA_PORT', '19092')}"]
KAFKA_TOPIC = "btc-1m-candle"
PG_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

class RealTimeBot:
    def __init__(self):
        print("🤖 [Bot] 초기화 및 업비트 연결 중... (Shadow Mode ON)")
        
        signal.signal(signal.SIGINT, self.shutdown_handler)
        signal.signal(signal.SIGTERM, self.shutdown_handler)
        atexit.register(self.cleanup)

        self.upbit = None
        self.active_trades = []
        self.total_pnl = 0.0
        self.win_count = 0
        self.loss_count = 0
        self.start_time = datetime.now()
        self.tick_count = 0
        self.consecutive_pred_failures = 0

        # 1. 업비트 연결
        try:
            self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
            krw = self.get_balance_safe("KRW")
            if krw is not None:
                print(f"✅ [Upbit] 연결 성공! 초기 KRW: {krw:,.0f}원")
            else:
                print("⚠️ [Upbit] 연결 불안정 (잔고 조회 실패)")
        except Exception as e:
            print(f"❌ [Upbit] 연결 실패: {e}")

        # 2. 포지션 복구
        self.load_active_positions()

        # 3. 데이터 초기화
        self.df_window = pd.DataFrame()
        try:
            print("⏳ [Bot] DB 데이터 로딩...")
            self.df_window = self.load_initial_data(limit=50000)
            print(f"✅ [Bot] 데이터 로드 완료: {len(self.df_window)} rows")
        except: pass

        # 4. 모델 로드
        # 4. 모델 로드
        try:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            model_path = os.path.join(project_root, 'model', 'xgb_final_model.joblib')
            print(f"🔍 [Debug] 모델 경로: {model_path}")
            print(f"🔍 [Debug] 파일 존재 여부: {os.path.exists(model_path)}")
            
            if os.path.exists(model_path):
                pack = joblib.load(model_path)
                print(f"🔍 [Debug] joblib.load 완료, 타입: {type(pack)}")
                
                if isinstance(pack, dict):
                    self.model = pack.get('model')
                    print(f"🔍 [Debug] dict에서 추출, model 타입: {type(self.model)}")
                else:
                    self.model = pack
                    print(f"🔍 [Debug] 직접 할당, model 타입: {type(self.model)}")
                
                # 🔥 핵심 체크
                if self.model is None:
                    print("❌ [Bot] 모델이 None입니다! xgb_final_model.joblib 파일 점검 필요")
                else:
                    print(f"✅ [Bot] 모델 로드 완료! (타입: {type(self.model).__name__})")
            else:
                self.model = None
                print("❌ [Bot] 모델 파일이 존재하지 않습니다!")
        except Exception as e:
            self.model = None
            print(f"❌ [Bot] 모델 로드 중 에러: {e}")


    def shutdown_handler(self, signum, frame):
        print("\n🛑 [종료 신호] 안전 종료 프로세스 시작...")
        self.cleanup()
        sys.exit(0)

    def cleanup(self):
        print("💾 포지션 데이터 저장 중...")
        self.save_active_positions()
        print("✅ 안전 종료 완료")

    def get_balance_safe(self, currency, max_retry=3):
        if self.upbit is None: return None
        for i in range(max_retry):
            try:
                bal = self.upbit.get_balance(currency)
                return float(bal)
            except:
                if i == max_retry - 1: return None
                time.sleep(0.5)
        return None

    def load_active_positions(self):
        if not os.path.exists(POSITION_FILE): return
        try:
            with open(POSITION_FILE, 'r') as f:
                content = f.read()
                if not content.strip(): return
                saved = json.loads(content)
                if not isinstance(saved, list): raise ValueError
                
                self.active_trades = []
                req = ['uuid', 'buy_price', 'volume', 'buy_time', 'target_price', 'stop_price']
                for t in saved:
                    if not all(k in t for k in req): continue
                    t['buy_time'] = datetime.fromisoformat(t['buy_time'])
                    self.active_trades.append(t)
                print(f"♻️ [복구 성공] 포지션 {len(self.active_trades)}개 로드")
        except:
            print("❌ 포지션 파일 손상. 백업 후 초기화.")
            os.rename(POSITION_FILE, f"{POSITION_FILE}.bak")

    def save_active_positions(self):
        try:
            saved = []
            for t in self.active_trades:
                t_copy = t.copy()
                t_copy['buy_time'] = t['buy_time'].isoformat()
                saved.append(t_copy)
            with open(POSITION_FILE, 'w') as f:
                json.dump(saved, f, indent=2)
        except: pass

    # [Shadow] CSV 기록 함수
    def record_shadow_trade(self, ts, price, prob, real_th, shadow_th, type_note):
        exists = os.path.exists(SHADOW_FILE)
        try:
            with open(SHADOW_FILE, "a", encoding="utf-8") as f:
                if not exists:
                    f.write("timestamp,price,prob,real_threshold,shadow_threshold,type\n")
                f.write(f"{ts},{price},{prob:.4f},{real_th:.4f},{shadow_th:.4f},{type_note}\n")
            print(f"📝 [Shadow] 기록됨 ({type_note}): {price:,.0f}원 (P:{prob:.4f})")
        except Exception as e:
            print(f"⚠️ Shadow 기록 실패: {e}")

    def load_initial_data(self, limit):
        try:
            with psycopg2.connect(**PG_CONN_INFO) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM btc_1m_candles ORDER BY ts DESC LIMIT %s", (limit,))
                    rows = cur.fetchall()
            df = pd.DataFrame(rows)
            if not df.empty:
                df.set_index('ts', inplace=True)
                df.sort_index(inplace=True)
                cols = ['open', 'high', 'low', 'close', 'volume', 'value']
                df[cols] = df[cols].apply(pd.to_numeric)
                return df
            return pd.DataFrame()
        except: return pd.DataFrame()

    def execute_buy(self, current_price, volatility):
        if self.upbit is None: return

        krw = self.get_balance_safe("KRW")
        btc = self.get_balance_safe(TICKER)
        if krw is None or btc is None: return

        total_equity = krw + (btc * current_price)
        entry_amount = int(total_equity / MAX_SLOTS)

        if len(self.active_trades) >= MAX_SLOTS: return
        if (btc * current_price) > (total_equity * 0.98): return

        buy_amount = min(entry_amount, krw * 0.999)
        if buy_amount < 5000: return

        print(f"🚀 [매수 시도] {buy_amount:,.0f} KRW")
        
        ret = None
        for i in range(2):
            try:
                ret = self.upbit.buy_market_order(TICKER, buy_amount)
                break
            except Exception as e:
                if "rate limit" in str(e).lower(): time.sleep(0.5)
                else: return

        if ret and 'uuid' in ret:
            order_info = None
            for attempt in range(3):
                time.sleep(1.0)
                try:
                    order_info = self.upbit.get_order(ret['uuid'])
                    if order_info:
                        state = order_info.get('state')
                        if state == 'done': break
                        elif state == 'cancel': 
                            print("❌ 주문 취소됨"); return
                except: pass
            
            if not order_info or order_info.get('state') != 'done':
                print("❌ [체결 실패] 확인 불가. 수동 점검 요망.")
                return

            trades = order_info.get('trades', [])
            if not trades:
                 real_vol = float(order_info.get('executed_volume', 0))
                 real_price = float(order_info.get('price', current_price))
            else:
                funds = sum(float(t['funds']) for t in trades)
                vol = sum(float(t['volume']) for t in trades)
                real_price = funds / vol
                real_vol = vol
            
            if real_vol == 0: return

            limit_vol = max(volatility, 0.002)
            t_info = {
                'uuid': ret['uuid'],
                'buy_price': real_price,
                'volume': real_vol,
                'buy_time': datetime.now(),
                'target_price': real_price * (1 + limit_vol * TRIPLE_BARRIER['pt']),
                'stop_price': real_price * (1 - limit_vol * TRIPLE_BARRIER['sl'])
            }
            self.active_trades.append(t_info)
            self.save_active_positions()
            print(f"✅ [매수 완료] {real_price:,.0f}원 | {real_vol:.8f} BTC")

    def check_exit_conditions(self, current_price, current_time):
        if not self.active_trades: return False
        
        executed = False
        for trade in self.active_trades[:]:
            is_tp = current_price >= trade['target_price']
            is_sl = current_price <= trade['stop_price']
            is_time = (current_time - trade['buy_time']).total_seconds()/60 >= TRIPLE_BARRIER['time_limit']

            reason = "익절" if is_tp else "손절" if is_sl else "만료" if is_time else ""
            if reason:
                if self.execute_sell(trade, current_price, reason):
                    executed = True
                    self.active_trades.remove(trade)
                    self.save_active_positions()
        return executed

    def execute_sell(self, trade, current_price, reason):
        vol = trade['volume']
        est_val = vol * current_price
        
        if est_val < 5000:
            if len(self.active_trades) == 1:
                bal = self.get_balance_safe(TICKER)
                if bal and bal * current_price >= 5000:
                    print(f"⚠️ [자투리 청산] 전량 매도 진행")
                    vol = bal
                else:
                    return False
            else:
                return False

        try:
            ret = self.upbit.sell_market_order(TICKER, vol)
            if not ret or 'uuid' not in ret:
                return False

            revenue = None
            real_sell_price = current_price 

            for _ in range(3):
                time.sleep(1.0)
                try:
                    info = self.upbit.get_order(ret['uuid'])
                    if info and info.get('state') == 'done':
                        trades = info.get('trades', [])
                        if trades:
                            total_funds = sum(float(t['funds']) for t in trades)
                            total_volume = sum(float(t['volume']) for t in trades)
                            real_sell_price = total_funds / total_volume
                            revenue = total_funds
                        else:
                            executed_vol = float(info.get('executed_volume', vol))
                            paid_fee = float(info.get('paid_fee', 0))
                            revenue = (current_price * executed_vol) - paid_fee
                            print(f"⚠️ [매도 경고] trades 없음. 근사 계산 사용.")
                        break
                except Exception as e:
                    print(f"⚠️ 주문 조회 시도 실패: {e}")

            if revenue is None:
                print(f"❌ [매도 실패] 체결 확인 불가. 수동 점검 필요.")
                return False

            buy_cost = trade['buy_price'] * trade['volume']
            pnl = revenue - buy_cost
            roi = (real_sell_price - trade['buy_price']) / trade['buy_price'] * 100
            
            self.total_pnl += pnl
            if pnl > 0: self.win_count += 1
            else: self.loss_count += 1
            
            total_trades = self.win_count + self.loss_count
            win_rate = (self.win_count / total_trades * 100) if total_trades > 0 else 0
            
            print(f"📉 [{reason}] {real_sell_price:,.0f}원 매도")
            print(f"   ㄴ PnL: {pnl:,.0f}원 ({roi:.2f}%) | 누적: {self.total_pnl:,.0f}원 ({win_rate:.1f}%)")
            return True

        except Exception as e:
            print(f"❌ 매도 실패: {e}")
            return False

    def process_strategy(self, candle_msg):
        self.tick_count += 1
        
        if self.tick_count % 60 == 0:
            uptime = (datetime.now() - self.start_time).total_seconds() / 3600
            print(f"💚 [Health] 가동: {uptime:.1f}h | 포지션: {len(self.active_trades)} | 손익: {self.total_pnl:,.0f}원")

        try:
            ts = datetime.strptime(candle_msg['timestamp'], "%Y-%m-%d %H:%M:%S")
            new_row = {
                'ts': ts, 'open': float(candle_msg['open']), 'high': float(candle_msg['high']),
                'low': float(candle_msg['low']), 'close': float(candle_msg['close']),
                'volume': float(candle_msg['volume']), 'value': float(candle_msg['value'])
            }
        except: return

        new_df = pd.DataFrame([new_row]).set_index('ts')
        if not self.df_window.empty and ts in self.df_window.index:
            self.df_window.loc[ts] = new_df.loc[ts]
        else:
            self.df_window = pd.concat([self.df_window, new_df])

        if len(self.df_window) > 50000: self.df_window = self.df_window.iloc[-50000:]
        
        MIN_REQUIRED = 1440 # 다시 1440으로 설정
        if len(self.df_window) < MIN_REQUIRED:
            if len(self.df_window) % 100 == 0:
                print(f"⏳ [데이터 수집] {len(self.df_window)}/{MIN_REQUIRED}")
            return

        latest_close = new_row['close']

        if self.check_exit_conditions(latest_close, ts):
            print("⏳ [매매 유예] 청산 직후 대기")
            return

        df_calc = self.df_window.copy()
        df_calc['strat_volatility'] = df_calc['close'].pct_change().ewm(span=TRIPLE_BARRIER['span']).std()
        df_calc = add_indicators(df_calc)
        df_calc = add_cyclical_features(df_calc)
        
        df_15m = add_indicators(resample_data(df_calc, "15min"))
        df_30m = add_indicators(resample_data(df_calc, "30min"))
        df_1h  = add_indicators(resample_data(df_calc, "1h"))
        df_4h  = add_indicators(resample_data(df_calc, "4h"))
        df_6h  = add_indicators(resample_data(df_calc, "6h"))
        
        df_final = merge_higher_tf(df_calc, df_15m, "15m")
        df_final = merge_higher_tf(df_final, df_30m, "30m")
        df_final = merge_higher_tf(df_final, df_1h,  "1h")
        df_final = merge_higher_tf(df_final, df_4h,  "4h")
        df_final = merge_higher_tf(df_final, df_6h,  "6h")
        
        FRAC_D = 0.4
        THRES = 1e-4
        frac_cols = ["close", "sma_5", "sma_10", "sma_20", "ema_12", "ema_26", "rsi_14", "macd", "bb_pct", "volatility"]
        frac_cols = [c for c in frac_cols if c in df_final.columns]
        for col in frac_cols:
            df_final[f"{col}_fd{FRAC_D}"] = frac_diff_ffd(df_final[col], d=FRAC_D, thres=THRES)

        # 수정 - 0.40~0.55 범위 적용
        threshold = calculate_dynamic_threshold(df_final, min_threshold=0.40, max_threshold=0.55)

        # [Shadow] 섀도우 임계값 계산 (-0.20)
        shadow_threshold = threshold - 0.20
        
        MODEL_FEATURES = [
            'open', 'high', 'low', 'close', 'volume', 'value',
            'sma_5', 'sma_10', 'sma_20', 'sma_60', 'sma_120',
            'ema_12', 'ema_26', 'rsi_14', 'macd', 'macd_sig', 'macd_hist',
            'bb_mid', 'bb_width', 'bb_pct', 'atr_14', 'vol_ma_20', 'log_return',
            'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos',
            'sma_5_15m', 'sma_10_15m', 'sma_20_15m', 'sma_60_15m', 'sma_120_15m',
            'ema_12_15m', 'ema_26_15m', 'rsi_14_15m', 'macd_15m', 'macd_sig_15m', 'macd_hist_15m',
            'bb_mid_15m', 'bb_width_15m', 'bb_pct_15m', 'atr_14_15m', 'vol_ma_20_15m', 'log_return_15m', 'volatility_15m',
            'sma_5_30m', 'sma_10_30m', 'sma_20_30m', 'sma_60_30m', 'sma_120_30m',
            'ema_12_30m', 'ema_26_30m', 'rsi_14_30m', 'macd_30m', 'macd_sig_30m', 'macd_hist_30m',
            'bb_mid_30m', 'bb_width_30m', 'bb_pct_30m', 'atr_14_30m', 'vol_ma_20_30m', 'log_return_30m', 'volatility_30m',
            'sma_5_1h', 'sma_10_1h', 'sma_20_1h', 'sma_60_1h', 'sma_120_1h',
            'ema_12_1h', 'ema_26_1h', 'rsi_14_1h', 'macd_1h', 'macd_sig_1h', 'macd_hist_1h',
            'bb_mid_1h', 'bb_width_1h', 'bb_pct_1h', 'atr_14_1h', 'vol_ma_20_1h', 'log_return_1h', 'volatility_1h',
            'sma_5_4h', 'sma_10_4h', 'sma_20_4h', 'sma_60_4h', 'sma_120_4h',
            'ema_12_4h', 'ema_26_4h', 'rsi_14_4h', 'macd_4h', 'macd_sig_4h', 'macd_hist_4h',
            'bb_mid_4h', 'bb_width_4h', 'bb_pct_4h', 'atr_14_4h', 'vol_ma_20_4h', 'log_return_4h', 'volatility_4h',
            'sma_5_6h', 'sma_10_6h', 'sma_20_6h', 'sma_60_6h', 'sma_120_6h',
            'ema_12_6h', 'ema_26_6h', 'rsi_14_6h', 'macd_6h', 'macd_sig_6h', 'macd_hist_6h',
            'bb_mid_6h', 'bb_width_6h', 'bb_pct_6h', 'atr_14_6h', 'vol_ma_20_6h', 'log_return_6h', 'volatility_6h',
            'close_fd0.4', 'sma_5_fd0.4', 'sma_10_fd0.4', 'sma_20_fd0.4',
            'ema_12_fd0.4', 'ema_26_fd0.4', 'rsi_14_fd0.4', 'macd_fd0.4',
            'bb_pct_fd0.4', 'volatility_fd0.4'
        ]
        
        latest_row_df = df_final.iloc[[-1]][MODEL_FEATURES]
        if latest_row_df.isna().sum().sum() > 0: return

        pred_prob = 0.0
        if self.model:
            try:
                # [디버깅 1] 입력 피처 값 3개만 찍어보기 (값이 제대로 들어가는지 확인)
                # print(f"🔍 입력 Feats: Close={latest_row_df['close'].values[0]}, RSI={latest_row_df['rsi_14'].values[0]:.2f}, Vol={latest_row_df['volatility'].values[0]:.6f}")
                
                raw_prob = self.model.predict_proba(latest_row_df)[0][1]
                pred_prob = raw_prob
                self.consecutive_pred_failures = 0
                
                # [디버깅 2] 만약 0.0001보다 작으면 정밀하게 출력
                if pred_prob < 0.0001:
                    print(f"⚠️ [초저확률 감지] Raw Score: {pred_prob:.10f}") # 소수점 10자리까지 확인

            except Exception as e:
                self.consecutive_pred_failures += 1
                pred_prob = 0.0
                print(f"❌ [예측 치명적 에러] {e}")

        # [Shadow] 상태 표시 강화
        status_icon = "⚪ WATCH"
        if pred_prob > threshold:
            status_icon = "🟢 BUY (REAL)"
        elif pred_prob > shadow_threshold:
            status_icon = "🔵 BUY (SHADOW)"
        
        print(f"============================================================")
        print(f"⏰ {ts} | 💰 {latest_close:,.0f} KRW")
        # [수정] 예측 확률을 소수점 6자리까지 늘려서 출력
        print(f"📊 예측: {pred_prob:.6f}") 
        print(f"   ㄴ 실전 TH: {threshold:.4f} | 섀도우 TH: {shadow_threshold:.4f}")
        print(f"🚀 상태: {status_icon}")
        print(f"============================================================")

        # [DB 저장] 예측 결과 및 상태 저장 (Upsert)
        try:
            # 텍스트 status 정제 (이모지 제거 등 단순화가 필요하면 여기서 처리)
            # 여기서는 status_icon 값을 그대로 저장하거나, 요청대로 BUY/WATCH로 매핑
            save_status = "WATCH"
            if "BUY" in status_icon: save_status = "BUY"
            
            # PnL 및 승률 계산
            total_trades = self.win_count + self.loss_count
            win_rate = (self.win_count / total_trades * 100) if total_trades > 0 else 0.0

            # 변동성 값 안전하게 가져오기
            volatility_val = df_calc['strat_volatility'].iloc[-1]
            if pd.isna(volatility_val): volatility_val = 0.0

            with psycopg2.connect(**PG_CONN_INFO) as conn:
                with conn.cursor() as cur:
                    # 이미 kafka_to_postgres가 넣었을 수도 있고 아닐 수도 있음.
                    # ON CONFLICT 두 경우 모두 대응
                    # prediction, status 컬럼이 DB에 존재해야 함 (ALTER TABLE 선행 필요)
                    sql = """
                        INSERT INTO btc_1m_candles 
                        (ts, open, high, low, close, volume, value, prediction, status, threshold, shadow_threshold, volatility)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ts) 
                        DO UPDATE SET 
                            prediction = EXCLUDED.prediction,
                            status = EXCLUDED.status,
                            threshold = EXCLUDED.threshold,
                            shadow_threshold = EXCLUDED.shadow_threshold,
                            volatility = EXCLUDED.volatility;
                    """
                    cur.execute(sql, (
                        ts, 
                        new_row['open'], new_row['high'], new_row['low'], new_row['close'], 
                        new_row['volume'], new_row['value'],
                        float(pred_prob), save_status,
                        float(threshold), float(shadow_threshold), float(volatility_val)
                    ))
                conn.commit()
        except Exception as e:
            # DB 컬럼이 없을 경우 에러가 날 수 있음 (Schema update 필요)
            print(f"⚠️ [DB 저장 실패] {e}")


        if pred_prob > threshold:
            vol = df_calc['strat_volatility'].iloc[-1]
            if pd.isna(vol): vol = 0.005
            self.execute_buy(latest_close, vol)
            # 실전 매매도 Shadow 데이터에 기록 (분석용)
            self.record_shadow_trade(ts, latest_close, pred_prob, threshold, shadow_threshold, "REAL")
            
        elif pred_prob > shadow_threshold:
            # Shadow 모드 기록
            self.record_shadow_trade(ts, latest_close, pred_prob, threshold, shadow_threshold, "SHADOW_ONLY")

    def run(self):
        consumer = None
        reconnect_attempts = 0
        MAX_RECONNECT = 5
        
        while reconnect_attempts < MAX_RECONNECT:
            try:
                if consumer is None:
                    consumer = KafkaConsumer(
                        KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        auto_offset_reset="latest", value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                        request_timeout_ms=20000
                    )
                    print("✅ [Kafka] 연결 성공. 감시 시작.")
                    reconnect_attempts = 0
                
                consecutive_errors = 0
                for msg in consumer:
                    try:
                        self.process_strategy(msg.value)
                        consecutive_errors = 0
                    except Exception as e:
                        consecutive_errors += 1
                        print(f"❌ [처리 에러] {e}")
                        if consecutive_errors >= 10:
                            print("🚨 처리 에러 반복. 재연결 시도.")
                            if consumer:
                                try: consumer.close()
                                except: pass
                                consumer = None
                            break
            
            except Exception as e:
                reconnect_attempts += 1
                print(f"❌ [Kafka 에러] {e}. 재연결 ({reconnect_attempts}/{MAX_RECONNECT})")
                if consumer:
                    try: consumer.close()
                    except: pass
                    consumer = None
                time.sleep(5)
        
        print("💀 [Fatal] Kafka 복구 불가. 종료.")

def main():
    bot = RealTimeBot()
    bot.run()

if __name__ == "__main__":
    main()