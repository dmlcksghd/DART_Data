import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error


def train_and_predict(data):
    # 피처 및 타겟 설정
    features = ['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름',
                'ATR', 'Bollinger_High', 'Bollinger_Low', 'MACD', 'MACD_Signal',
                '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']
    target = '종가'

    X = data[features]
    y = data[target]

    # 데이터 분할
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 데이터 스케일링
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    # 모델 학습
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # 예측
    y_pred = model.predict(X_test)

    # 평가
    mae = mean_absolute_error(y_test, y_pred)
    print(f"Mean Absolute Error: {mae}")

    return model


if __name__ == "__main__":
    # CSV에서 데이터 로드
    save_dir = 'data'
    data_path = os.path.join(save_dir, 'prepared_data.csv')
    data = pd.read_csv(data_path)

    # 모델 학습 및 예측
    model = train_and_predict(data)

    # 전체 데이터로 예측
    features = ['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름',
                'ATR', 'Bollinger_High', 'Bollinger_Low', 'MACD', 'MACD_Signal',
                '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']
    X_all = data[features]
    X_all = StandardScaler().fit_transform(X_all)

    data['예측 주가'] = model.predict(X_all)
    print(data[['종목명', '종가', '예측 주가']])

    # 결과를 CSV 파일로 저장
    data.to_csv(os.path.join(save_dir, 'Predicted_Stock_Prices.csv'), index=False, encoding='utf-8-sig')
