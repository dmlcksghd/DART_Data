import requests
import pandas as pd
from io import StringIO
from pbr_data import get_pbr_less_one_companies
import os
from datetime import datetime, timedelta
import holidays
import numpy as np

# 한국 공휴일 데이터 로드 (korean-holidays 라이브러리 사용)
kr_holidays = holidays.KR(years=[2023, 2024])  # 공휴일 범위 설정

def is_trading_day(date):
    """ 주말과 공휴일을 제외한 거래일 확인 """
    if date.weekday() >= 5:  # 토요일(5)과 일요일(6)은 거래일이 아님
        return False
    if date in kr_holidays:
        return False
    return True

def get_last_trading_day_of_year(year):
    """ 해당 연도의 마지막 거래일 찾기 """
    last_day = datetime(year, 12, 31)
    while not is_trading_day(last_day):
        last_day -= timedelta(days=1)
    return last_day

def get_stock_data(trdDd):
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

    otp_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }

    otp_payload = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
        'name': 'fileDown',
        'filetype': 'csv',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01501',
        'mktId': 'ALL',
        'trdDd': '20240726',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false'
    }

    otp_response = requests.post(otp_url, headers=otp_headers, data=otp_payload)
    otp = otp_response.text

    download_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': '__smVisitorID=5AohOPWW0ba; JSESSIONID=NgLTkCOznyh1w25KbqjlhknhEfpU4WTwJr7ufBZnaN9BdHRUzXaGMqdgGCor71AW.bWRjX2RvbWFpbi9tZGNvd2FwMi1tZGNhcHAxMQ==',
        'Host': 'data.krx.co.kr',
        'Origin': 'http://data.krx.co.kr',
        'Upgrade-Insecure-Requests': '1'
    }

    download_payload = {
        'code': otp,
        'name': 'fileDown',
        'filetype': 'csv'
    }

    csv_response = requests.post(download_url, headers=download_headers, data=download_payload)

    save_dir = 'stock_data'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    csv_file_path = os.path.join(save_dir, f'stock_data_{trdDd}.csv')
    with open(csv_file_path, 'wb') as file:
        file.write(csv_response.content)

    print(f"{trdDd} 파일 저장 완료")

    csv_content = csv_response.content.decode('euc-kr')
    data = StringIO(csv_content)
    stock_df = pd.read_csv(data)

    return stock_df

def get_pbr_less_or_equal_stock_data(start_date, end_date):
    stock_data_list = []
    
    # 마지막 거래일 제외
    last_trading_days = [get_last_trading_day_of_year(year) for year in range(start_date.year, end_date.year + 1)]
    last_trading_days = set(last_trading_days)  # 중복 제거

    date_range = pd.date_range(start=start_date, end=end_date)
    for single_date in date_range:
        if single_date not in last_trading_days and is_trading_day(single_date):  # 거래일인 경우만 데이터 다운로드
            trdDd = single_date.strftime('%Y%m%d')
            print(f"Downloading data for {trdDd}...")
            stock_data = get_stock_data(trdDd)
            stock_data['날짜'] = trdDd
            stock_data_list.append(stock_data)

    if not stock_data_list:
        raise ValueError("No trading days found in the given range.")

    all_stock_data = pd.concat(stock_data_list, ignore_index=True)

    pbr_less_one_df = get_pbr_less_one_companies(end_date.strftime('%Y%m%d'))

    pbr_less_or_equal_stock_data = all_stock_data[all_stock_data['종목명'].isin(pbr_less_one_df['종목명'])]

    return pbr_less_or_equal_stock_data

# 최근에 생성된 CSV 파일 찾기
def find_latest_csv(directory):
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    latest_file = max(csv_files, key=lambda x: os.path.getctime(os.path.join(directory, x)))
    return os.path.join(directory, latest_file)

# 상장주식수와 종목명 컬럼 추출하여 저장
def extract_and_save_listing_shares_and_names(latest_file):
    df = pd.read_csv(latest_file, encoding='cp949')
    if '상장주식수' in df.columns and '종목명' in df.columns:
        listing_shares_df = df[['종목명', '상장주식수']]
        #output_file = os.path.join('stock_data', 'listing_shares_and_names.csv')
        #listing_shares_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        #print(f'{output_file} 파일로 저장되었습니다.')
    else:
        missing_cols = [col for col in ['종목명', '상장주식수'] if col not in df.columns]
        print(f'다음 컬럼이 존재하지 않습니다: {", ".join(missing_cols)}')

    return listing_shares_df

if __name__ == "__main__":
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 3, 31)

    pbr_less_or_equal_stock_data = get_pbr_less_or_equal_stock_data(start_date, end_date)

    pd.set_option('display.max_columns', None)

    print(pbr_less_or_equal_stock_data.head())

    save_dir = 'stock_data'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    #pbr_less_or_equal_stock_data.to_csv(os.path.join(save_dir, 'PBR_Less_Or_Equal_Stock_Data.csv'), index=False, encoding='utf-8-sig')
    #print("PBR Less Or Equal Stock Data CSV 파일 저장 완료")

    # 가장 최근에 저장된 CSV 파일에서 상장주식수와 종목명 컬럼만 추출하여 저장
    latest_csv_file = find_latest_csv(save_dir)
    extract_and_save_listing_shares_and_names(latest_csv_file)
