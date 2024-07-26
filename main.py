import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from financialStatements import get_report, split_report, df_listed
from stock_data import get_stock_data
from data_PBR import get_pbr_one_companies

# 모델 학습 및 예측
def train_model(financial_df):
    # 결측치가 있는 행 제거
    financial_df = financial_df.dropna()

    X = financial_df[['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름', 'PBR']]
    y = financial_df['종가']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f'Mean Squared Error: {mse}')

    return model

# 주식 가치 예측
def predict_stock_value(trdDd):
    pbr_one_names, pbr_less_one_df = get_pbr_one_companies(trdDd)
    stock_data = get_stock_data(trdDd)

    if '종가' not in stock_data.columns:
        print("주가 데이터에 '종가' 열이 없습니다.")
        return

    financial_data = []
    for corp_name in pbr_less_one_df['종목명']:
        try:
            year = '2024'
            quarter = '1'
            df = get_report(df_listed, corp_name, year, quarter, 'CFS')
            report_df = split_report(corp_name, year, quarter, df)
            if report_df.empty:
                continue

            # 데이터가 누락되지 않도록 각 재무 항목이 존재하는지 확인합니다.
            if all(col in report_df for col in ['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름']):
                if corp_name in stock_data['종목명'].values:
                    종가 = stock_data[stock_data['종목명'] == corp_name]['종가'].values[0]
                    financial_data.append({
                        '종목명': corp_name,
                        '자산 총액': report_df['자산 총액'][0],
                        '부채 총액': report_df['부채 총액'][0],
                        '자본 총액': report_df['자본 총액'][0],
                        '매출액': report_df['매출액'][0],
                        '영업이익': report_df['영업이익'][0],
                        '순이익': report_df['순이익'][0],
                        '현금 흐름': report_df['현금 흐름'][0],
                        'PBR': pbr_less_one_df[pbr_less_one_df['종목명'] == corp_name]['PBR'].values[0],
                        '종가': 종가
                    })
                else:
                    print(f"{corp_name}의 '종가' 데이터를 찾을 수 없습니다.")
        except Exception as e:
            print(f'{corp_name}의 {year}년 {quarter}분기 데이터 가져오기에 실패했습니다: {e}')

    financial_df = pd.DataFrame(financial_data)
    if financial_df.empty:
        print("학습에 사용할 데이터가 없습니다.")
        return

    # 결측값 있는 행 제거
    financial_df = financial_df.dropna()

    model = train_model(financial_df)

    X_predict = financial_df[['자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름', 'PBR']]

    financial_df['예측주가'] = model.predict(X_predict)
    financial_df['저평가여부'] = financial_df['종가'] < financial_df['예측주가']

    print(financial_df[['종목명', '예측주가', '종가', '저평가여부']])

if __name__ == "__main__":
    trdDd = '20240724'  # 거래일자 설정
    predict_stock_value(trdDd)
