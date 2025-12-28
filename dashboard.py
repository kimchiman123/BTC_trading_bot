import streamlit as st
import pandas as pd
import psycopg2
import time
import os
import plotly.graph_objects as go

# 페이지 설정
st.set_page_config(page_title="BTC Trading Bot Dashboard", layout="wide", page_icon="📈")

# DB 연결 정보 (환경 변수 또는 기본값)
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "airflow")
PG_USER = os.getenv("POSTGRES_USER", "airflow")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")

@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS
    )

def load_data(limit=100):
    conn = init_connection()
    query = f"""
        SELECT ts, open, high, low, close, volume, prediction, status 
        FROM btc_1m_candles 
        ORDER BY ts DESC LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# 헤더
st.title("🤖 BTC Auto-Trading Bot Dashboard")
st.markdown("Cloud-based Real-time Monitoring System")

# 자동 새로고침 (Streamlit 특성상 버튼이나 rerun 루프 필요, 여기선 수동+주기적)
if st.button('🔄 Refresh Data'):
    st.rerun()

# 데이터 로드
try:
    df = load_data(200)
    if df.empty:
        st.warning("데이터가 없습니다. 봇이 실행 중인지 확인하세요.")
    else:
        # 최신 상태 표시
        latest = df.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Latest Price", f"{latest['close']:,.0f} KRW", 
                      delta=f"{latest['close'] - df.iloc[1]['close']:,.0f} KRW")
        with col2:
            st.metric("Prediction Score", f"{latest['prediction']:.4f}")
        with col3:
            status_color = "🟢" if "BUY" in str(latest['status']) else "⚪"
            st.metric("Status", f"{status_color} {latest['status']}")
        with col4:
            st.metric("Last Update", str(latest['ts']))

        # 차트 그리기
        st.subheader("📊 Price & Prediction Trend")
        
        # 캔버스 생성
        fig = go.Figure()

        # 가격 차트
        fig.add_trace(go.Scatter(
            x=df['ts'], y=df['close'], name='Close Price',
            line=dict(color='cyan', width=2), yaxis='y1'
        ))

        # 예측 점수 차트 (보조축)
        fig.add_trace(go.Scatter(
            x=df['ts'], y=df['prediction'], name='Pred Score',
            line=dict(color='orange', width=1, dash='dot'), yaxis='y2'
        ))

        # 레이아웃 설정
        fig.update_layout(
            title="BTC Price vs Prediction",
            yaxis=dict(title="Price (KRW)", showgrid=False),
            yaxis2=dict(title="Prediction Probability", overlaying='y', side='right', range=[0, 1], showgrid=False),
            hovermode="x unified",
            template="plotly_dark",
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)

        # 데이터 테이블
        st.subheader("📋 Recent Data (Last 20)")
        st.dataframe(df.head(20))

except Exception as e:
    st.error(f"DB 연결 실패 또는 데이터 오류: {e}")
