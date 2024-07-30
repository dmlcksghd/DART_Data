import os
import pandas as pd
from datetime import datetime

# 주가데이터 stock_data 가져오기 (이 안에 pbr과 주가데이터 전부 들어있음)
def load_stock_data(directory):
    stock_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    stock_data_list = []
    for file in stock_files:
        df = pd.read_csv(os.path.join(directory, file), encoding='utf-8-sig')
        stock_data_list.append(df)
    return pd.concat(stock_data_list, ignore_index=True)

# 재무제표 데이터 가져오기
def load_financial_statements_data(directory):
    financial_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    financial_data_list = []
    for file in financial_files:
        # 분기를 파일명에서 추출 (예: BGF_2023_1Q_report.csv)
        quarter = int(file.split('_')[2][0])
        year = int(file.split('_')[1])
        df = pd.read_csv(os.path.join(directory, file), encoding='utf-8-sig')
        df['분기'] = quarter
        df['연도'] = year
        # 필요한 컬럼만 선택
        filtered_df = df[['종목명', '연도', '분기', 'ROE', 'EPS', 'BPS', '자산 총액', '부채 총액', '자본 총액', '매출액', '영업이익', '순이익', '현금 흐름', 'ROA', '매출액 증가율']]
        financial_data_list.append(filtered_df)
    return pd.concat(financial_data_list, ignore_index=True)

# stock파일 안에 있는 데이터 중 날짜를 읽어 분기로 나누기
def get_financial_quarter(date):
    month = date.month
    if 1 <= month <= 3:
        return 1
    elif 4 <= month <= 6:
        return 2
    elif 7 <= month <= 9:
        return 3
    else:
        return 4

# stock_data와 financial_data 합치기
def merge_stock_and_financial_data(stock_data, financial_data):
    stock_data['날짜'] = pd.to_datetime(stock_data['날짜'], format='%Y%m%d', errors='coerce')
    stock_data['연도'] = stock_data['날짜'].dt.year
    stock_data['분기'] = stock_data['날짜'].apply(get_financial_quarter)

    # 병합 키를 종목명, 연도, 분기로 설정
    merged_data = pd.merge(stock_data, financial_data, on=['종목명', '연도', '분기'], how='left')

    # 부채비율, 영업이익률, 순이익률 계산
    merged_data['부채비율'] = merged_data['부채 총액'] / merged_data['자본 총액']
    merged_data['영업이익률'] = merged_data['영업이익'] / merged_data['매출액']
    merged_data['순이익률'] = merged_data['순이익'] / merged_data['매출액']

    return merged_data

# 합쳐진 데이터를 어떤 기준으로 묶을지
def save_individual_prepared_data_files(final_data, save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    grouped = final_data.groupby('종목명')
    for stock_name, group in grouped:
        group['날짜'] = group['날짜'].dt.strftime('%Y-%m-%d')  # 날짜 형식을 문자열로 변환
        group.dropna(inplace=True)  # 결측값이 있는 행 제거
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
                      'ROE', 'EPS', 'BPS', '부채비율', '영업이익률', '순이익률', 'ROA', '매출액 증가율']
    final_filtered_data = prepared_data[columns_needed]

    # 병합 후 데이터 확인
    print(final_filtered_data.head())

    # Save individual prepared data files
    save_individual_prepared_data_files(final_filtered_data, save_dir)
    print("Data preparation complete.")
