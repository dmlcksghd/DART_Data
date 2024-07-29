import os
import pandas as pd
from datetime import datetime


def load_stock_data(directory):
    stock_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    stock_data_list = []
    for file in stock_files:
        df = pd.read_csv(os.path.join(directory, file), encoding='utf-8-sig')
        stock_data_list.append(df)
    return pd.concat(stock_data_list, ignore_index=True)


def load_financial_statements_data(directory):
    financial_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    financial_data_list = []
    for file in financial_files:
        df = pd.read_csv(os.path.join(directory, file), encoding='utf-8-sig')
        # 필요한 컬럼만 선택
        filtered_df = df[['종목명', 'ROE', 'EPS', 'BPS', '자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름']]
        financial_data_list.append(filtered_df)
    return pd.concat(financial_data_list, ignore_index=True)


def merge_stock_and_financial_data(stock_data, financial_data):
    # 주가 데이터와 재무제표 데이터 병합
    stock_data['날짜'] = pd.to_datetime(stock_data['날짜'], format='%Y%m%d', errors='coerce')

    # 병합 키를 종목명으로 설정
    merged_data = pd.merge(stock_data, financial_data, on='종목명', how='left')

    # 부채비율, 영업이익률, 순이익률 계산
    merged_data['부채비율'] = merged_data['부채 총액'] / merged_data['자본 총액']
    merged_data['영업이익률'] = merged_data['영업이익'] / merged_data['매출액']
    merged_data['순이익률'] = merged_data['순이익'] / merged_data['매출액']

    return merged_data


def save_individual_prepared_data_files(final_data, save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    grouped = final_data.groupby('종목명')
    for stock_name, group in grouped:
        group['날짜'] = group['날짜'].dt.strftime('%Y-%m-%d')  # 날짜 형식을 문자열로 변환
        group.to_csv(os.path.join(save_dir, f'{stock_name}_prepared_data.csv'), index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    stock_data_dir = 'stock_data'
    financial_statements_dir = 'financialStatements'
    save_dir = 'data'

    # Load stock data
    stock_data = load_stock_data(stock_data_dir)
    print("Stock data loaded successfully")

    # Load financial statements data
    financial_data = load_financial_statements_data(financial_statements_dir)
    print("Financial statements data loaded successfully")

    # Merge stock data and financial statements data
    prepared_data = merge_stock_and_financial_data(stock_data, financial_data)
    print("Data merged successfully")

    # 필요한 컬럼만 추출 및 순서 정리
    columns_needed = ['종목명', '날짜', 'PBR', '시가', '고가', '저가', '종가', '거래량', '상장주식수',
                      '자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름',
                      'ROE', 'EPS', 'BPS', '부채비율', '영업이익률', '순이익률']
    final_filtered_data = prepared_data[columns_needed]

    # 병합 후 데이터 확인
    print(final_filtered_data.head())

    # Save individual prepared data files
    save_individual_prepared_data_files(final_filtered_data, save_dir)
    print("Data preparation complete.")
