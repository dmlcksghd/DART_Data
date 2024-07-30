import yfinance as yf
import pandas as pd
from datetime import datetime

if __name__=='__main__':

   

    # 가져올 지수들의 티커 심볼
    indices = {
        'KOSPI': '^KS11',
        'KOSDAQ': '^KQ11',
        'NASDAQ': '^IXIC',
        'Dow Jones': '^DJI',
        'S&P 500': '^GSPC',
        'Nikkei': '^N225'
    }

    # 시작 날짜와 종료 날짜
    start_date = '2023-01-01'
    end_date = datetime.today().strftime('%Y-%m-%d')  # 현재 날짜

    # 데이터 저장용 딕셔너리
    data_dict = {}

    # 각 지수에 대해 데이터 다운로드
    for name, ticker in indices.items():
        data = yf.download(ticker, start=start_date, end=end_date)['Close']
        data_dict[name] = data

    # 모든 데이터를 하나의 데이터프레임으로 결합
    combined_df = pd.DataFrame(data_dict)

    # 결합된 데이터프레임 출력
    print(combined_df.head())
