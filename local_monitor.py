import sys
import psycopg2
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QTableWidget, QTableWidgetItem, QLineEdit, QPushButton, QGroupBox, QMessageBox)
from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtGui import QColor, QFont
import pyqtgraph as pg
import pandas as pd
from datetime import datetime

class BTCMonitor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BTC Bot Local Monitor")
        self.setGeometry(100, 100, 1200, 800)
        
        # 기본 설정
        self.conn = None
        self.fetch_limit = 100
        
        self.initUI()
        self.setup_timer()

    def initUI(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)

        # 1. 연결 설정 패널
        conn_group = QGroupBox("Cloud DB Connection")
        conn_layout = QHBoxLayout()
        
        self.ip_input = QLineEdit("34.64.xxx.xxx") # 예시 IP
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

        # 2. 상태 요약 패널
        status_group = QGroupBox("Current Status")
        status_layout = QHBoxLayout()
        
        self.lbl_price = QLabel("Price: -")
        self.lbl_pred = QLabel("Pred: -")
        self.lbl_status = QLabel("Status: -")
        self.lbl_time = QLabel("Last Update: -")
        
        font = QFont("Arial", 12, QFont.Bold)
        for lbl in [self.lbl_price, self.lbl_pred, self.lbl_status, self.lbl_time]:
            lbl.setFont(font)
            status_layout.addWidget(lbl)
            
        status_group.setLayout(status_layout)
        layout.addWidget(status_group)

        # 3. 차트 (pyqtgraph)
        self.graph_widget = pg.PlotWidget()
        self.graph_widget.setBackground('k')
        self.graph_widget.setTitle("BTC Price & Prediction", color='w', size="12pt")
        self.graph_widget.setLabel('left', 'Price (KRW)')
        self.graph_widget.setLabel('bottom', 'Time')
        self.graph_widget.showGrid(x=True, y=True)
        self.graph_widget.addLegend()
        layout.addWidget(self.graph_widget)

        # 4. 데이터 테이블
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Time", "Price", "Volume", "Pred", "Status"])
        layout.addWidget(self.table)

    def setup_timer(self):
        self.timer = QTimer(self)
        self.timer.setInterval(60000) # 1분마다 갱신
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
            self.update_data() # 즉시 갱신
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

    def update_data(self):
        if not self.conn: return

        try:
            # 연결 상태 확인 (핑)
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            
            # 최신 데이터 조회 (인덱스 활용)
            query = """
                SELECT ts, close, volume, prediction, status 
                FROM btc_1m_candles 
                WHERE ts > NOW() - INTERVAL '3 hours'
                ORDER BY ts DESC 
                LIMIT 100
            """
            df = pd.read_sql(query, self.conn)
            
            if df.empty: return

            # 최신 데이터 업데이트
            latest = df.iloc[0]
            self.lbl_price.setText(f"Price: {latest['close']:,.0f} KRW")
            self.lbl_pred.setText(f"Pred: {latest['prediction'] if latest['prediction'] else 0:.4f}")
            self.lbl_status.setText(f"Status: {latest['status']}")
            self.lbl_time.setText(f"Last Update: {latest['ts']}")

            # 스타일링 (Status)
            if "BUY" in str(latest['status']):
                self.lbl_status.setStyleSheet("color: green")
            else:
                self.lbl_status.setStyleSheet("color: gray")

            # 테이블 업데이트
            self.table.setRowCount(min(20, len(df))) # 최근 20개만 표시
            for i in range(min(20, len(df))):
                row = df.iloc[i]
                self.table.setItem(i, 0, QTableWidgetItem(str(row['ts'])))
                self.table.setItem(i, 1, QTableWidgetItem(f"{row['close']:,.0f}"))
                self.table.setItem(i, 2, QTableWidgetItem(f"{row['volume']:.2f}"))
                self.table.setItem(i, 3, QTableWidgetItem(f"{row['prediction'] if row['prediction'] else 0:.4f}"))
                self.table.setItem(i, 4, QTableWidgetItem(str(row['status'])))

            # 차트 업데이트
            self.graph_widget.clear()
            
            # 시간 축은 간소화를 위해 인덱스로 처리 (실제 타임스탬프 변환 등은 복잡하므로)
            # x축: 과거 -> 현재
            df_rev = df.iloc[::-1].reset_index(drop=True) 
            x = df_rev.index.values
            y_close = df_rev['close'].values
            
            # 가격 (Main)
            self.graph_widget.plot(x, y_close, pen=pg.mkPen(color='c', width=2), name="Price")

            # 예측값 표시 (별도 축이 없으므로, 정규화해서 겹쳐보거나 텍스트로 대체해야 함)
            # 여기서는 가격만 그림. 예측이 중요한 경우 ViewBox를 추가해야 하나 코드가 복잡해짐.
            # 요청사항: "예측값의 변화를 꺾은선 그래프로 그려줘"
            # -> 가격 스케일과 예측 확률(0~1) 스케일이 다르므로, 별도 플롯 또는 2축이 필요.
            # 간편하게: 예측 확률 * 100,000 + BasePrice 등으로 시각화하거나 별도 뷰.
            # 여기서는 별도 ViewBox 구현 대신, 간단히 Log를 찍거나 2번째 PlotItem을 추가하는 방식 권장.
            # 한 화면에 겹쳐 그리기 위해 오른쪽 축 추가 (PyQtGraph의 ViewBox 기능 활용)
            
            p1 = self.graph_widget.plotItem
            p2 = pg.ViewBox()
            p1.showAxis('right')
            p1.scene().addItem(p2)
            p1.getAxis('right').linkToView(p2)
            p2.setXLink(p1)
            
            y_pred = df_rev['prediction'].fillna(0).values
            p2.addItem(pg.PlotCurveItem(x, y_pred, pen=pg.mkPen(color='m', width=2, style=Qt.DashLine)))
            
            def updateViews():
                p2.setGeometry(p1.vb.sceneBoundingRect())
                p2.linkedViewChanged(p1.vb, p2.XAxis)
                
            updateViews()
            p1.vb.sigResized.connect(updateViews)

        except Exception as e:
            self.lbl_status.setText(f"Error: {str(e)[:20]}...")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = BTCMonitor()
    ex.show()
    sys.exit(app.exec_())
