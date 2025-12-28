import streamlit as st
import pandas as pd
import psycopg2
import time
import os
import plotly.graph_objects as go

# 페이지 설정
st.set_page_config(page_title="BTC 트레이딩 봇 대시보드", layout="wide", page_icon="📈")

# DB 연결 정보 (환경 변수 또는 기본값)
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "airflow")
PG_USER = os.getenv("POSTGRES_USER", "airflow")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")

# @st.cache_resource 제거: 연결 객체를 캐싱하면 close() 후 재사용 시 오류 발생
def init_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS
    )

def load_data(limit=100):
    conn = init_connection()
    # 컬럼 추가 (threshold, shadow_threshold, volatility 등)
    query = f"""
        SELECT ts, open, high, low, close, volume, prediction, status, threshold, shadow_threshold, volatility
        FROM btc_1m_candles 
        ORDER BY ts DESC LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# 헤더
st.title("🤖 BTC 자동매매 봇 대시보드")
st.markdown("클라우드 기반 실시간 모니터링 시스템")

# 자동 새로고침 (Streamlit 특성상 버튼이나 rerun 루프 필요, 여기선 수동+주기적)
if st.button('🔄 데이터 새로고침'):
    st.rerun()

# 데이터 로드
try:
    df = load_data(200)
    
    # 1. 데이터가 비어있는지 먼저 체크
    if df.empty:
        st.warning("⚠️ 데이터가 없습니다. 봇이 실행 중인지 확인하세요.")
        st.info("DB에 데이터가 쌓일 때까지 기다려 주세요.")
    else:
        # 최신 상태 표시
        latest = df.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            # None 체크 (데이터가 막 생성되었을 때 numeric 필드가 None일 수 있음)
            close_price = latest['close'] if latest['close'] is not None else 0.0
            prev_close = df.iloc[1]['close'] if len(df) > 1 and df.iloc[1]['close'] is not None else close_price
            
            st.metric("현재가", f"{close_price:,.0f} KRW", 
                      delta=f"{close_price - prev_close:,.0f} KRW")
        with col2:
            # Prediction None 처리
            pred = latest['prediction']
            if pred is None:
                st.metric("예측 점수", "0.0000 (대기)")
            else:
                st.metric("예측 점수", f"{pred:.4f}")
        with col3:
            # Status None 처리
            status = latest['status'] if latest['status'] is not None else "Unknown"
            status_color = "🟢" if "BUY" in str(status) else ("🔴" if "SELL" in str(status) else "⚪")
            st.metric("상태", f"{status_color} {status}")
        with col4:
            # Time 처리
            ts = latest['ts']
            if pd.isna(ts):
                st.metric("마지막 업데이트", "대기 중...")
            else:
                st.metric("마지막 업데이트", str(ts))

        # 차트 그리기
        st.subheader("📊 가격 및 예측 추세")
        
        # 캔버스 생성
        fig = go.Figure()

        # 가격 차트
        fig.add_trace(go.Scatter(
            x=df['ts'], y=df['close'], name='종가',
            line=dict(color='cyan', width=2), yaxis='y1'
        ))

        # 예측 점수 차트 (보조축)
        fig.add_trace(go.Scatter(
            x=df['ts'], y=df['prediction'], name='예측 점수',
            line=dict(color='orange', width=1, dash='dot'), yaxis='y2'
        ))

        # 레이아웃 설정
        fig.update_layout(
            title="비트코인 가격 vs 예측 확률",
            yaxis=dict(title="가격 (KRW)", showgrid=False),
            yaxis2=dict(title="예측 확률", overlaying='y', side='right', range=[0, 1], showgrid=False),
            hovermode="x unified",
            template="plotly_dark",
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)

        # 데이터 테이블
        st.subheader("📋 최근 데이터 (20건)")
        st.dataframe(df.head(20))

except Exception as e:
    st.error(f"DB 연결 실패 또는 데이터 오류: {e}")
