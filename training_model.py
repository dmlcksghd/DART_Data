import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import yfinance as yf
from datetime import datetime

def get_index_data(start_date, end_date):
    indices = {
        'KOSPI': '^KS11',
        'KOSDAQ': '^KQ11',
        'NASDAQ': '^IXIC',
        'Dow Jones': '^DJI',
        'S&P 500': '^GSPC',
        'Nikkei': '^N225'
    }

    data_dict = {}
    for name, ticker in indices.items():
        data = yf.download(ticker, start=start_date, end=end_date)['Close']
        data_dict[name] = data

    combined_df = pd.DataFrame(data_dict)
    return combined_df

def train_and_predict(data, index_data):
    features = ['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름', '상장주식수',
                'ROE', 'EPS', 'BPS', '부채비율', '영업이익률', '순이익률', 'ROA', '매출액 증가율',
                'KOSPI', 'KOSDAQ', 'NASDAQ', 'Dow Jones', 'S&P 500', 'Nikkei']
    target = '종가'

    # 지수 데이터를 날짜 기준으로 병합
    data = data.merge(index_data, left_on='날짜', right_index=True, how='left')

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
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    print(f"Mean Absolute Error: {mae}")
    print(f"Mean Squared Error: {mse}")
    print(f"R² Score: {r2}")

    return model, scaler, mae, mse, r2

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

    # 시작 날짜와 종료 날짜
    start_date = '2023-01-01'
    end_date = datetime.today().strftime('%Y-%m-%d')

    # 지수 데이터 가져오기
    index_data = get_index_data(start_date, end_date)

    # 각 회사별로 모델 학습 및 예측
    for company, group in company_groups:
        print(f"Processing company: {company}")
        model, scaler, mae, mse, r2 = train_and_predict(group, index_data)

        # 가장 최신 데이터 선택
        latest_data = group.sort_values(by='날짜').iloc[-1]
        features = ['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름',
                    '상장주식수', 'ROE', 'EPS', 'BPS', '부채비율', '영업이익률', '순이익률', 'ROA', '매출액 증가율',
                    'KOSPI', 'KOSDAQ', 'NASDAQ', 'Dow Jones', 'S&P 500', 'Nikkei']
        X_latest = latest_data[features].values.reshape(1, -1)
        X_latest = scaler.transform(X_latest)

        # 최신 데이터로 예측
        predicted_price = model.predict(X_latest)[0]
        actual_price = latest_data['종가']
        price_difference = predicted_price - actual_price

        # 결과 저장
        predictions.append({'종목명': company, '날짜': latest_data['날짜'], '실제 종가': actual_price,
                            '예측 주가': predicted_price, 'MAE': mae, 'MSE': mse, 'R²': r2, '차이': price_difference})

    # 예측 결과를 데이터프레임으로 변환
    prediction_df = pd.DataFrame(predictions)
    print(prediction_df)

    # 모델 및 스케일러 저장
    model_dir = 'models'
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    for company, group in company_groups:
        print(f"Saving model for company: {company}")
        model, scaler, _, _, _ = train_and_predict(group, index_data)
        joblib.dump(model, os.path.join(model_dir, f'{company}_model.pkl'))
        joblib.dump(scaler, os.path.join(model_dir, f'{company}_scaler.pkl'))

    # 예측 결과를 CSV 파일로 저장
    prediction_df.to_csv(os.path.join(save_dir, 'latest_predictions.csv'), index=False, encoding='utf-8-sig')
    print("Predictions saved to latest_predictions.csv")
