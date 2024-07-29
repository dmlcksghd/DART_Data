import os
import pandas as pd


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
        filtered_df = df[['종목명', '분기', 'ROE', 'EPS', 'BPS']]
        financial_data_list.append(filtered_df)
    return pd.concat(financial_data_list, ignore_index=True)


def merge_stock_and_financial_data(stock_data, financial_data):
    # 주가 데이터와 재무제표 데이터 병합
    stock_data['날짜'] = pd.to_datetime(stock_data['날짜'])
    financial_data['분기'] = pd.to_datetime(financial_data['분기'])
    financial_data.rename(columns={'분기': '날짜'}, inplace=True)
    merged_data = pd.merge(stock_data, financial_data, on=['종목명', '날짜'], how='left')
    return merged_data


def save_individual_prepared_data_files(final_data, save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    grouped = final_data.groupby('종목명')
    for stock_name, group in grouped:
        group.to_csv(os.path.join(save_dir, f'{stock_name}_prepared_data.csv'), index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    stock_data_dir = 'stock_data'
    financial_statements_dir = 'financialStatements'
    save_dir = 'data'

    # Load stock data
    stock_data = load_stock_data(stock_data_dir)

    # Load financial statements data
    financial_data = load_financial_statements_data(financial_statements_dir)

    # Merge stock data and financial statements data
    prepared_data = merge_stock_and_financial_data(stock_data, financial_data)

    # 필요한 컬럼만 추출
    columns_needed = ['종목명', '날짜', 'PBR', '시가', '고가', '저가', '종가', '거래량', '상장주식수', 'ROE', 'BPS', 'EPS']
    final_filtered_data = prepared_data[columns_needed]

    # Save individual prepared data files
    save_individual_prepared_data_files(final_filtered_data, save_dir)
    print("Data preparation complete.")
