import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

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

    # 인덱스를 '날짜' 칼럼으로 변경하고 날짜 형식으로 변환
    combined_df.reset_index(inplace=True)
    combined_df.rename(columns={'Date': '날짜'}, inplace=True)
    combined_df['날짜'] = pd.to_datetime(combined_df['날짜']).dt.strftime('%Y%m%d')  # 날짜 형식 변경

    return combined_df

def get_today_data():
    today = datetime.now()
    indices = {
        'KOSPI': '^KS11',
        'KOSDAQ': '^KQ11',
        'NASDAQ': '^IXIC',
        'Dow Jones': '^DJI',
        'S&P 500': '^GSPC',
        'Nikkei': '^N225'
    }

    today_data_dict = {}
    for name, ticker in indices.items():
        data = yf.download(ticker, start=today, end=today)['Close']
        today_data_dict[name] = data

    today_df = pd.DataFrame(today_data_dict)

    # 인덱스를 '날짜' 칼럼으로 변경하고 날짜 형식으로 변환
    today_df.reset_index(inplace=True)
    today_df.rename(columns={'Date': '날짜'}, inplace=True)
    today_df['날짜'] = pd.to_datetime(today_df['날짜']).dt.strftime('%Y%m%d')  # 날짜 형식 변경

    return today_df

if __name__ == "__main__":
    # 시작 날짜와 종료 날짜
    start_date = datetime(2024, 3, 1)
    end_date = datetime(2024, 3, 10)

    # 지수 데이터 가져오기
    index_data = get_index_data(start_date, end_date)

    # 오늘 날짜의 지수 데이터 가져오기
    today_data = get_today_data()

    # 데이터 병합
    all_data = pd.concat([index_data, today_data], ignore_index=True)

    # 데이터 저장 디렉토리 설정
    save_dir = 'index_data'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # 데이터 저장
    file_path = os.path.join(save_dir, 'index_data.csv')
    all_data.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"Index data saved to {file_path}")