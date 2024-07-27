import pandas as pd
import numpy as np
from financialStatements import get_financial_statements
from stock_data import get_pbr_less_or_equal_stock_data, find_latest_csv
import os
from datetime import datetime

def prepare_data(start_date, end_date, listing_shares_df):
    # 재무제표 데이터 가져오기
    financial_data = get_financial_statements(end_date, listing_shares_df)

    # 주가 데이터 가져오기
    stock_data = get_pbr_less_or_equal_stock_data(start_date, end_date)

    # 데이터 병합
    merged_data = pd.merge(financial_data, stock_data, on='종목명')

    # 필수 항목 제외 결측값 처리
    merged_data.fillna(0, inplace=True)

    return merged_data

if __name__ == "__main__":
    start_date = '20230101'
    end_date = '20240331'

    # 최근에 생성된 CSV 파일에서 상장주식수와 종목명 컬럼만 추출하여 데이터프레임으로 로드
    save_dir = 'stock_data'
    latest_csv_file = find_latest_csv(save_dir)
    listing_shares_df = pd.read_csv(latest_csv_file, encoding='cp949', usecols=['종목명', '상장주식수'])

    data = prepare_data(start_date, end_date, listing_shares_df)
    print(data.head())

    # 데이터를 CSV로 저장하여 training_model.py에서 불러올 수 있도록 합니다.
    save_dir = 'data'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    data.to_csv(os.path.join(save_dir, 'prepared_data.csv'), index=False, encoding='utf-8-sig')
