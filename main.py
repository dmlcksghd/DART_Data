import os
import pandas as pd
import chardet
from datetime import datetime

# 디렉토리 설정
stock_dir = 'stock_data'
financial_dir = 'financialStatements'
save_dir = 'data'
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

def detect_encoding(file_path):
    try:
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        return result['encoding']
    except Exception as e:
        print(f"Error detecting encoding for {file_path}: {e}")
        return 'utf-8'  # Default encoding if detection fails

def load_csv_files_from_directory(directory, encoding=None):
    dataframes = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            if encoding is None:
                encoding = detect_encoding(file_path)
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                dataframes.append(df)
            except Exception as e:
                print(f"Error reading {file_path} with encoding {encoding}: {e}")
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    return pd.DataFrame()

def process_stock_data(stock_df):
    # 주식 데이터 열 이름 정리 및 선택
    stock_df.columns = stock_df.columns.str.strip()
    stock_df = stock_df[
        ['종목코드', '종목명', '시장구분', '소속부', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']]
    stock_df = stock_df.rename(columns={'상장주식수': '상장주식수_x'})
    return stock_df

def process_financial_data(financial_df):
    # 열 이름을 확인
    print("Available columns in financial_df:", financial_df.columns.tolist())

    # 재무제표 데이터 열 이름 정리 및 선택
    financial_df.columns = financial_df.columns.str.strip()

    financial_df = financial_df.rename(columns={
        '자산 총액': 'Total Assets',
        '부채 총액': 'Total Liabilities',
        '자본 총액': 'Total Equity',
        '매출액': 'Revenue',
        '영업이익': 'Operating Income',
        '순이익': 'Net Income',
        '상장주식수': 'Shares Outstanding_y',  # 수정된 부분
        'ROE': 'ROE',
        'EPS': 'EPS',
        'BPS': 'BPS',
        '부채비율': 'Debt Ratio',
        '영업이익률': 'Operating Margin',
        '순이익률': 'Net Margin',
        '분기': 'Quarter',
        '현금 흐름': 'Cash Flow'
    })

    # 필요한 열만 선택
    try:
        financial_df = financial_df[['종목명', 'Quarter', 'Total Assets', 'Total Liabilities', 'Total Equity', 'Revenue',
                                     'Operating Income', 'Net Income', 'Cash Flow', 'Shares Outstanding_y', 'ROE', 'EPS',
                                     'BPS', 'Debt Ratio', 'Operating Margin', 'Net Margin']]
    except KeyError as e:
        print(f"Column error: {e}")
        print("Available columns in financial_df after renaming:", financial_df.columns.tolist())

    return financial_df

if __name__ == "__main__":
    today = datetime.now()
    recent_weekday = today - pd.DateOffset(weekday=(today.weekday() - 0) % 7)
    trdDd = recent_weekday.strftime('%Y%m%d')

    # 주식 데이터와 재무제표 데이터 로드
    stock_df = load_csv_files_from_directory(stock_dir, encoding='euc-kr')
    financial_df = load_csv_files_from_directory(financial_dir)

    # 데이터 처리
    stock_df = process_stock_data(stock_df)
    financial_df = process_financial_data(financial_df)

    # 데이터 결합 (종목명과 분기, 날짜 기준)
    prepared_data = pd.merge(stock_df, financial_df, how='left', on='종목명')

    # 파일 이름에 날짜와 시간 포함
    timestamp = today.strftime('%Y%m%d_%H%M%S')
    prepared_data_file_path = os.path.join(save_dir, f'prepared_data_{timestamp}.csv')
    prepared_data.to_csv(prepared_data_file_path, index=False, encoding='euc-kr')
    print(f"Prepared data saved to {prepared_data_file_path}")
