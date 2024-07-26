import pandas as pd
from financialStatements import get_financial_statements
from stock_data import get_pbr_less_or_equal_stock_data
import os


def calculate_bvps(row):
    return row['자본 총액'] / row['상장주식수']


def predict_stock_price(financial_data, stock_data):
    # 데이터 병합
    merged_data = pd.merge(financial_data, stock_data, on='종목명')

    # '자본 총액'과 '상장주식수'를 숫자형으로 변환
    merged_data['자본 총액'] = pd.to_numeric(merged_data['자본 총액'], errors='coerce')
    merged_data['상장주식수'] = pd.to_numeric(merged_data['상장주식수'], errors='coerce')

    # 주당 순자산가치(BVPS) 계산
    merged_data['BVPS'] = merged_data.apply(calculate_bvps, axis=1)

    # 적정 주가 계산 (적정 PBR 값을 1로 가정)
    merged_data['적정 주가'] = merged_data['BVPS'] * 1  # 적정 PBR 값을 1로 가정

    return merged_data


if __name__ == "__main__":
    trdDd = '20240724'

    # 재무제표 데이터 가져오기
    financial_data = get_financial_statements(trdDd)
    # 주가 데이터 가져오기
    stock_data = get_pbr_less_or_equal_stock_data(trdDd)

    # 주가 예측
    result = predict_stock_price(financial_data, stock_data)

    # pandas 옵션 설정
    pd.set_option('display.max_columns', None)  # 모든 열을 출력하도록 설정
    pd.set_option('display.max_rows', None)  # 모든 행을 출력하도록 설정
    pd.set_option('display.max_colwidth', None)  # 각 열의 최대 너비 설정

    # 결과 출력
    print(result[['종목명', '종가', '적정 주가']])

    # 결과를 CSV 파일로 저장
    save_dir = 'data'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    result.to_csv(os.path.join(save_dir, 'Predicted_Stock_Prices.csv'), index=False, encoding='utf-8-sig')
