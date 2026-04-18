# 파일명: custom_objective.py
import numpy as np

class FocalLossObjective:
    """
    불균형 데이터 처리를 위한 Focal Loss 클래스.
    이 파일은 학습(train)과 추론(bot) 양쪽에서 모두 import 해야 합니다.
    """
    def __init__(self, alpha, gamma):
        self.alpha = alpha
        self.gamma = gamma

    def get_objective(self, y_true, y_pred):
        # y_true: 실제 라벨 (0 또는 1)
        # y_pred: 모델의 예측값 (Log-odds 형태, margin)
        
        labels = y_true
        preds = y_pred
        
        # Log-odds -> Probability (Sigmoid 적용)
        preds = 1.0 / (1.0 + np.exp(-preds))
        preds = np.clip(preds, 1e-7, 1.0 - 1e-7)

        pt = np.where(labels == 1, preds, 1 - preds)
        alpha_t = np.where(labels == 1, self.alpha, 1 - self.alpha)

        # Gradient (1차 미분)
        # 식: alpha_t * (1 - pt)^gamma * (preds - labels)
        grad = alpha_t * (1 - pt)**self.gamma * (preds - labels)

        # Hessian (2차 미분)
        # Scaled Logistic Hessian 근사식 사용 (XGBoost 안정성 확보)
        hess = alpha_t * (1 - pt)**self.gamma * preds * (1 - preds)

        return grad, np.maximum(hess, 1e-6)