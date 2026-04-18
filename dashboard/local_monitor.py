
import sys
import psycopg2
import pandas as pd
import pyqtgraph as pg
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QTableWidget, QTableWidgetItem, QLineEdit, QPushButton, QGroupBox, QMessageBox
)
from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtGui import QFont, QColor
import os

pg.setConfigOption('background', 'k')
pg.setConfigOption('foreground', 'w')

class BTCMonitor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BTC Bot Enhanced Monitor (Real-Time)")
        self.setGeometry(100, 100, 1400, 900)
        
        # 기본 설정
        self.conn = None
        self.last_ts = None  # 마지막 로드한 데이터의 타임스탬프 (Incremental Update용)
        self.df_all = pd.DataFrame() # 전체 데이터 보관용
        
        self.initUI()
        self.setup_timer()

    def initUI(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)

        # 1. 연결 설정 패널
        conn_group = QGroupBox("Cloud DB Connection")
        conn_layout = QHBoxLayout()
        
        # PG_CONN_INFO 기본값 또는 환경 변수 활용 가능
        default_ip = os.getenv("POSTGRES_HOST", "4.188.82.253")
        
        self.ip_input = QLineEdit(default_ip)
        self.ip_input.setPlaceholderText("DB Host IP")
        self.port_input = QLineEdit("5432")
        self.user_input = QLineEdit("airflow")
        self.pw_input = QLineEdit("airflow")
        self.pw_input.setEchoMode(QLineEdit.Password)
        
        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self.connect_db)
        self.disconnect_btn = QPushButton("Disconnect")
        self.disconnect_btn.clicked.connect(self.disconnect_db)
        self.disconnect_btn.setEnabled(False)

        conn_layout.addWidget(QLabel("IP:"))
        conn_layout.addWidget(self.ip_input)
        conn_layout.addWidget(QLabel("Port:"))
        conn_layout.addWidget(self.port_input)
        conn_layout.addWidget(QLabel("User:"))
        conn_layout.addWidget(self.user_input)
        conn_layout.addWidget(QLabel("PW:"))
        conn_layout.addWidget(self.pw_input)
        conn_layout.addWidget(self.connect_btn)
        conn_layout.addWidget(self.disconnect_btn)
        
        conn_group.setLayout(conn_layout)
        layout.addWidget(conn_group)

        # 2. 상태 요약 패널 (PnL 추가)
        status_group = QGroupBox("Bot Status & Performance")
        status_layout = QHBoxLayout()
        
        self.lbl_price = QLabel("Price: -")
        self.lbl_pred = QLabel("Pred: -")
        self.lbl_thresh = QLabel("Thresh: -")
        self.lbl_status = QLabel("Status: -")
        self.lbl_pnl = QLabel("PnL: -")
        self.lbl_winrate = QLabel("WinRate: -")
        self.lbl_time = QLabel("Last Update: -")
        
        font = QFont("Arial", 11, QFont.Bold)
        for lbl in [self.lbl_price, self.lbl_pred, self.lbl_thresh, self.lbl_status, self.lbl_pnl, self.lbl_winrate, self.lbl_time]:
            lbl.setFont(font)
            status_layout.addWidget(lbl)
            
        status_group.setLayout(status_layout)
        layout.addWidget(status_group)

        # 3. 차트 (PyQtGraph Layout)
        # GraphicsLayoutWidget을 사용하여 여러 플롯을 세로로 배치
        self.win = pg.GraphicsLayoutWidget(show=True, title="BTC Analysis")
        layout.addWidget(self.win)

        # [차트 1] Price (Top)
        self.p_price = self.win.addPlot(title="BTC Price (1m Candle)")
        self.p_price.setLabel('left', 'Price', units='KRW')
        self.p_price.showGrid(x=True, y=True)
        self.curve_price = self.p_price.plot(pen=pg.mkPen('c', width=2))
        
        self.win.nextRow()

        # [차트 2] Prediction vs Threshold (Middle)
        self.p_pred = self.win.addPlot(title="Prediction Score vs Threshold")
        self.p_pred.setLabel('left', 'Probability')
        self.p_pred.setYRange(0, 1) # 확률은 0~1
        self.p_pred.showGrid(x=True, y=True)
        self.p_pred.addLegend()
        
        self.curve_pred = self.p_pred.plot(pen=pg.mkPen('m', width=2), name='Pred Score')
        self.curve_th = self.p_pred.plot(pen=pg.mkPen('g', width=1, style=Qt.DashLine), name='Real TH')
        self.curve_shadow = self.p_pred.plot(pen=pg.mkPen('b', width=1, style=Qt.DotLine), name='Shadow TH')
        
        # X축 연동 (줌/팬 동기화)
        self.p_pred.setXLink(self.p_price)

        self.win.nextRow()

        # [차트 3] Volatility (Bottom)
        self.p_vol = self.win.addPlot(title="Volatility")
        self.p_vol.setLabel('left', 'Vol')
        self.p_vol.showGrid(x=True, y=True)
        self.curve_vol = self.p_vol.plot(pen=pg.mkPen('y', width=1))
        
        self.p_vol.setXLink(self.p_price)

        # 4. 데이터 테이블
        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels(["Time", "Price", "Pred", "Real TH", "Shadow TH", "Vol", "Status"])
        layout.addWidget(self.table)
        
        # 테이블 높이 제한 (UI 공간 확보)
        self.table.setMaximumHeight(150)

    def setup_timer(self):
        self.timer = QTimer(self)
        self.timer.setInterval(30000) # 30초마다 갱신
        self.timer.timeout.connect(self.update_data)

    def connect_db(self):
        host = self.ip_input.text()
        port = self.port_input.text()
        user = self.user_input.text()
        pw = self.pw_input.text()

        try:
            self.conn = psycopg2.connect(
                host=host, port=port, dbname="airflow", user=user, password=pw, connect_timeout=3
            )
            QMessageBox.information(self, "Success", "Connected to Cloud DB!")
            self.connect_btn.setEnabled(False)
            self.disconnect_btn.setEnabled(True)
            self.lbl_status.setText("Status: Connected")
            
            # 초기 로딩은 전체 데이터(또는 대량)
            self.df_all = pd.DataFrame() 
            self.last_ts = None
            self.update_data(init_load=True) 
            
            self.timer.start()
        except Exception as e:
            QMessageBox.critical(self, "Connection Failed", str(e))

    def disconnect_db(self):
        if self.conn:
            self.conn.close()
            self.conn = None
        self.timer.stop()
        self.connect_btn.setEnabled(True)
        self.disconnect_btn.setEnabled(False)
        self.lbl_status.setText("Status: Disconnected")

    def update_data(self, init_load=False):
        if not self.conn: return

        try:
            # 연결 체크
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")

            # 쿼리 작성
            # 필요한 경우 'LIMIT' 조절 가능
            if init_load:
                # 초기에는 최근 5000개 로드
                query = """
                    SELECT ts, close, prediction, threshold, shadow_threshold, volatility, status, pnl, win_rate
                    FROM btc_1m_candles 
                    ORDER BY ts DESC 
                    LIMIT 5000
                """
            else:
                # 증분 업데이트 (마지막 ts 이후 데이터)
                if self.last_ts:
                    query = f"""
                        SELECT ts, close, prediction, threshold, shadow_threshold, volatility, status, pnl, win_rate
                        FROM btc_1m_candles 
                        WHERE ts > '{self.last_ts}'
                        ORDER BY ts ASC
                    """
                else:
                    query = "SELECT * FROM btc_1m_candles LIMIT 0" # Fallback

            new_df = pd.read_sql(query, self.conn)

            if new_df.empty: 
                return

            if init_load:
                self.df_all = new_df.sort_values('ts').reset_index(drop=True)
            else:
                self.df_all = pd.concat([self.df_all, new_df]).drop_duplicates(subset=['ts']).sort_values('ts').reset_index(drop=True)
                # 너무 많아지면 자르기 (메모리/성능 관리)
                if len(self.df_all) > 10000:
                    self.df_all = self.df_all.iloc[-10000:]

            # 마지막 타임스탬프 갱신
            self.last_ts = self.df_all['ts'].max()

            # --- UI 업데이트 ---
            latest = self.df_all.iloc[-1]
            
            # 1. 텍스트 라벨
            self.lbl_price.setText(f"Price: {latest['close']:,.0f}")
            self.lbl_pred.setText(f"Pred: {latest['prediction']:.4f}")
            self.lbl_thresh.setText(f"TH: {latest['threshold']:.4f}")
            
            # Status & Color
            status_text = latest['status']
            if "BUY" in status_text:
                # REAL vs SHADOW 구분 (봇은 'BUY'로 저장하지만, threshold 비교로 화면엔 상세 표시 가능)
                # 다만 DB에 저장된 status 문자열을 우선 따름. 필요시 로직 추가.
                if latest['prediction'] > latest['threshold']:
                    self.lbl_status.setText("🟢 BUY (REAL)")
                    self.lbl_status.setStyleSheet("color: #00FF00") # Bright Green
                else:
                    self.lbl_status.setText("🔵 BUY (SHADOW)")
                    self.lbl_status.setStyleSheet("color: #00BFFF") # Deep Sky Blue
            else:
                self.lbl_status.setText("⚪ WATCH")
                self.lbl_status.setStyleSheet("color: #D3D3D3") # Light Gray

            # Performance
            pnl = latest['pnl'] if pd.notnull(latest['pnl']) else 0.0
            win_rate = latest['win_rate'] if pd.notnull(latest['win_rate']) else 0.0
            
            self.lbl_pnl.setText(f"PnL: {pnl:,.0f} KRW")
            pnl_color = "red" if pnl < 0 else "green" if pnl > 0 else "white"
            self.lbl_pnl.setStyleSheet(f"color: {pnl_color}")
            
            self.lbl_winrate.setText(f"WinRate: {win_rate:.1f}%")
            self.lbl_time.setText(f"Updated: {latest['ts'].strftime('%H:%M:%S')}")

            # 2. 차트 그리기
            # x축은 정수 인덱스 사용 (시간축은 틱으로 처리하거나 생략)
            # 여기선 간단히 range 사용
            
            # 성능 최적화를 위해 최근 N개만 그리기 (예: 1440분 = 24시간)
            plot_df = self.df_all.tail(2000)
            x_vals = range(len(plot_df))
            
            # Top: Price
            self.curve_price.setData(x_vals, plot_df['close'].values)
            
            # Mid: Pred vs Thresholds
            # DB에 값이 없는 경우(옛날 데이터) 0 처리
            self.curve_pred.setData(x_vals, plot_df['prediction'].fillna(0).values)
            self.curve_th.setData(x_vals, plot_df['threshold'].fillna(0.5).values)
            self.curve_shadow.setData(x_vals, plot_df['shadow_threshold'].fillna(0.3).values)
            
            # Bottom: Volatility
            self.curve_vol.setData(x_vals, plot_df['volatility'].fillna(0).values)

            # 3. 테이블 (최근 50개)
            table_df = self.df_all.tail(50).sort_values('ts', ascending=False)
            self.table.setRowCount(len(table_df))
            for i in range(len(table_df)):
                row = table_df.iloc[i]
                self.table.setItem(i, 0, QTableWidgetItem(str(row['ts'])))
                self.table.setItem(i, 1, QTableWidgetItem(f"{row['close']:,.0f}"))
                self.table.setItem(i, 2, QTableWidgetItem(f"{row['prediction']:.4f}"))
                self.table.setItem(i, 3, QTableWidgetItem(f"{row['threshold']:.4f}"))
                self.table.setItem(i, 4, QTableWidgetItem(f"{row['shadow_threshold']:.4f}"))
                self.table.setItem(i, 5, QTableWidgetItem(f"{row['volatility']:.5f}"))
                self.table.setItem(i, 6, QTableWidgetItem(str(row['status'])))

        except Exception as e:
            self.lbl_status.setText("Status: Error")
            print(f"Update Error: {e}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = BTCMonitor()
    ex.show()
    sys.exit(app.exec_())
