import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import os
import holidays
import time

# 공휴일 확인 함수
def is_holiday(date):
    kr_holidays = holidays.KR(years=date.year)
    return date in kr_holidays

# 가장 최근 평일을 찾는 함수
def get_recent_weekday(date):
    while date.weekday() > 4 or is_holiday(date):
        date -= timedelta(days=1)
    return date

# 영업일 리스트를 반환하는 함수
def get_business_days(start_date, end_date):
    business_days = []
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5 and not is_holiday(current_date):
            business_days.append(current_date)
        current_date += timedelta(days=1)
    return business_days

# 특정 날짜의 주가 데이터를 크롤링하는 함수
def get_stock_data_for_date(trdDd, retries=3, backoff_factor=1.0):
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
        'trdDd': trdDd,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false'
    }

    download_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': '_ga=GA1.1.1420298847.1720981646; _ga_EGZWJ6FGKM=GS1.1.1720981858.1.1.1720981880.0.0.0; __smVisitorID=Gp68vON1abl; _ga_1EV6XZXVDT=GS1.1.1720981645.1.1.1720983151.0.0.0; _ga_808R2EHLL3=GS1.1.1720986709.1.1.1720986730.0.0.0; _ga_Z6N0DBVT2W=GS1.1.1721740575.3.0.1721740584.0.0.0; JSESSIONID=WA1BGKSbePUVAyniRc3GX3pV2af01VvNbFFgV9iyZO2il8pOzRNNgadwxkxoEyzN.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAwMQ==',
        'Host': 'data.krx.co.kr',
        'Origin': 'http://data.krx.co.kr',
        'Upgrade-Insecure-Requests': '1'
    }

    download_payload = {
        'code': '',
        'name': 'fileDown',
        'filetype': 'csv'
    }

    print(f"Starting download for date: {trdDd}")

    # 실패시 재시도하는 함수
    for attempt in range(retries):
        try:
            otp_response = requests.post(otp_url, headers=otp_headers, data=otp_payload)
            otp = otp_response.text
            download_payload['code'] = otp
            csv_response = requests.post(download_url, headers=download_headers, data=download_payload)
            csv_content = csv_response.content.decode('euc-kr')
            data = StringIO(csv_content)
            df = pd.read_csv(data)

            # 목록 만들기
            # 로그로 컬럼 이름 확인
            print("Downloaded DataFrame columns:", df.columns.tolist())

            # 날짜 컬럼 추가
            df['날짜'] = trdDd

            # 필요한 컬럼만 선택
            df = df[['종목코드', '종목명', '날짜', '시가', '종가', '고가', '저가', '거래량', '상장주식수']]

            print(f"Successfully downloaded data for {trdDd}")
            return df
        except KeyError as e:
            print(f"KeyError occurred: {e}. Retrying in {backoff_factor} seconds...")
            time.sleep(backoff_factor)
            backoff_factor *= 2
        except Exception as e:
            print(f"Error occurred: {e}. Retrying in {backoff_factor} seconds...")
            time.sleep(backoff_factor)
            backoff_factor *= 2
    print(f"Failed to download data for {trdDd} after {retries} attempts.")
    return None

# 주어진 기간의 주가 데이터 가져오기
def get_stock_data_for_period(start_date, end_date, stock_names):
    business_days = get_business_days(start_date, end_date)
    stock_data_dir = 'stock_data_period'
    if not os.path.exists(stock_data_dir):
        os.makedirs(stock_data_dir)

    for business_day in business_days:
        trdDd = business_day.strftime('%Y%m%d')
        print(f"Fetching stock data for {trdDd}...")
        stock_data = get_stock_data_for_date(trdDd)
        if stock_data is not None:
            filtered_stock_data = stock_data[stock_data['종목명'].isin(stock_names)]
            for stock_name in stock_names:
                stock_file_name = f"{stock_name}_{trdDd}.csv"
                stock_file_path = os.path.join(stock_data_dir, stock_file_name)
                stock_df = filtered_stock_data[filtered_stock_data['종목명'] == stock_name]
                if not stock_df.empty:
                    stock_df.to_csv(stock_file_path, index=False, encoding='utf-8-sig')
                    print(f"Saved stock data for {stock_name} on {trdDd} to {stock_file_path}")
                else:
                    print(f"No data found for {stock_name} on {trdDd}")
        else:
            print(f"Failed to retrieve stock data for {trdDd}")

def merge_recent_data_with_pbr(stock_names, pbr_dir):
    target_date = datetime.strptime('2024-07-29', '%Y-%m-%d')  # 날짜를 7월 29일로 설정
    trdDd = target_date.strftime('%Y%m%d')
    print(f"Fetching stock data for the specific date: {trdDd}...")

    stock_data = get_stock_data_for_date(trdDd)
    if stock_data is not None:
        filtered_stock_data = stock_data[stock_data['종목명'].isin(stock_names)]
        now_data_dir = 'now_data'
        if not os.path.exists(now_data_dir):
            os.makedirs(now_data_dir)

        for stock_name in stock_names:
            pbr_file_prefix = f'{stock_name}_'
            pbr_files = [f for f in os.listdir(pbr_dir) if f.startswith(pbr_file_prefix) and f.endswith('.csv')]
            if pbr_files:
                # Assuming the latest PBR file based on filename
                pbr_file_path = os.path.join(pbr_dir, pbr_files[0])
                pbr_df = pd.read_csv(pbr_file_path)
                pbr_value = pbr_df['PBR'].values[0]

                stock_df = filtered_stock_data[filtered_stock_data['종목명'] == stock_name]
                if not stock_df.empty:
                    merged_df = stock_df.copy()
                    merged_df['PBR'] = pbr_value
                    save_path = os.path.join(now_data_dir, f'{stock_name}_{trdDd}.csv')
                    merged_df.to_csv(save_path, index=False, encoding='utf-8-sig')
                    print(f"Saved merged data for {stock_name} to {save_path}")
                else:
                    print(f"No stock data found for {stock_name} on {trdDd}")
            else:
                print(f"PBR data file for {stock_name} not found in {pbr_dir}.")
    else:
        print(f"Failed to retrieve stock data for {trdDd}")

# pbr_data와 주가 데이터를 합치는 함수
if __name__ == "__main__":
    # 날짜를 7월 29일로 설정
    target_date = datetime.strptime('2024-07-29', '%Y-%m-%d')

    # PBR 데이터가 저장된 디렉토리
    pbr_dir = 'pbr_data'

    # 주가 데이터 가져오기
    stock_names = [f.split('_')[0] for f in os.listdir(pbr_dir) if f.endswith('.csv')]
    merge_recent_data_with_pbr(stock_names, pbr_dir)

