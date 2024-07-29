import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error


def train_and_predict(data):
    # 피처 및 타겟 설정
    features = ['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름',
                '시가', '고가', '저가', '거래량', '상장주식수']
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
    data_files = [f for f in os.listdir(save_dir) if f.endswith('_prepared_data.csv')]

    # 빈 파일 또는 모든 값이 NA인 파일을 제외한 데이터프레임 리스트 생성
    data_list = []
    for file in data_files:
        df = pd.read_csv(os.path.join(save_dir, file))
        if not df.empty and df.dropna(how='all').shape[0] > 0:
            data_list.append(df)

    data = pd.concat(data_list, ignore_index=True)

    # 결측값 처리 (결측값이 있는 행을 제거)
    data.dropna(inplace=True)

    # 회사별로 데이터 분할
    company_groups = data.groupby('종목명')

    # 예측 결과를 저장할 리스트
    predictions = []

    # 각 회사별로 모델 학습 및 예측
    for company, group in company_groups:
        print(f"Processing company: {company}")
        model = train_and_predict(group)

        # 가장 최신 데이터 선택
        latest_data = group.sort_values(by='날짜').iloc[-1]
        features = ['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름',
                    '시가', '고가', '저가', '거래량', '상장주식수']
        X_latest = latest_data[features].values.reshape(1, -1)
        X_latest = StandardScaler().fit_transform(X_latest)

        # 최신 데이터로 예측
        predicted_price = model.predict(X_latest)[0]
        predictions.append({'종목명': company, '날짜': latest_data['날짜'], '종가': latest_data['종가'], '예측 주가': predicted_price})

    # 예측 결과를 데이터프레임으로 변환
    prediction_df = pd.DataFrame(predictions)
    print(prediction_df)

    # 결과를 CSV 파일로 저장
    predicted_price_dir = 'predicted_price'
    if not os.path.exists(predicted_price_dir):
        os.makedirs(predicted_price_dir)

    prediction_df.to_csv(os.path.join(predicted_price_dir, 'Predicted_Stock_Prices.csv'), index=False, encoding='utf-8-sig')
