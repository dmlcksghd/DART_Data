import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import os
import holidays
import time

# 공휴일 확인
def is_holiday(date):
    kr_holidays = holidays.KR(years=date.year)
    return date in kr_holidays

# 가장 최근 평일 찾기
def get_recent_weekday(date):
    while date.weekday() > 4 or is_holiday(date):
        date -= timedelta(days=1)
    return date

# 영업일 계산
def get_business_days(start_date, end_date):
    business_days = []
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5 and not is_holiday(current_date):
            business_days.append(current_date)
        current_date += timedelta(days=1)
    return business_days

# 주어진 날짜의 주가 데이터를 가져오는 함수
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
        'Cookie': '_ga=GA1.1.1420298847.1720981646; _ga_EGZWJ6FGKM=GS1.1.1720981858.1.1.1720981880.0.0.0; __smVisitorID=Gp68vON1abl; _ga_1EV6XZXVDT=GS1.1.1720981645.1.1.1720983151.0.0.0; _ga_808R2EHLL3=GS1.1.1720986709.1.1.1720986730.0.0.0; _ga_Z6N0DBVT2W=GS1.1.1721740575.3.0.1721740584.0.0.0; JSESSIONID=WA1BGKSbePUVAyniRc3GX3pV2af01VvNbFFgV9iyZO2il8pOzRNNgadwxkxoEyzN.bWRjX2RvbWFpbi9tZGNhcHAwMQ==',
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
    all_data = pd.DataFrame()

    for business_day in business_days:
        trdDd = business_day.strftime('%Y%m%d')
        print(f"Fetching stock data for {trdDd}...")
        stock_data = get_stock_data_for_date(trdDd)
        if stock_data is not None:
            filtered_stock_data = stock_data[stock_data['종목명'].isin(stock_names)]
            all_data = pd.concat([all_data, filtered_stock_data], ignore_index=True)
            print(f"Successfully retrieved stock data for {trdDd}")
        else:
            print(f"Failed to retrieve stock data for {trdDd}")

    return all_data

# pbr_data와 stock_data 합치기
def merge_data(pbr_dir, stock_data_dir, stock_data):
    if not os.path.exists(stock_data_dir):
        os.makedirs(stock_data_dir)

    for file in os.listdir(pbr_dir):
        if file.endswith('.csv'):
            stock_name = file.split('_')[0]
            pbr_file_path = os.path.join(pbr_dir, file)
            pbr_df = pd.read_csv(pbr_file_path)
            pbr_value = pbr_df['PBR'].values[0]

            matching_stock_data = stock_data[stock_data['종목명'] == stock_name] # pbr데이터와 stock데이터 합칠때 종목명으로 일치시켜서 합치기

            if not matching_stock_data.empty:
                merged_df = matching_stock_data.copy()
                merged_df['PBR'] = pbr_value
                save_path = os.path.join(stock_data_dir, f'{stock_name}_merged.csv')
                merged_df.to_csv(save_path, index=False, encoding='utf-8-sig')
                print(f"Saved merged data for {stock_name} to {save_path}")

# pbr 데이터 가져오기
def get_pbr_data(pbr_dir, trdDd):
    pbr_file_path = os.path.join(pbr_dir, f'pbr_data_{trdDd}.csv')
    if os.path.exists(pbr_file_path):
        pbr_df = pd.read_csv(pbr_file_path)
        return pbr_df
    else:
        print(f"PBR data file {pbr_file_path} not found.")
        return pd.DataFrame()

# 7월 30일 데이터 저장 함수 추가
def save_stock_data_for_july_30(stock_data, save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    for stock_name, group in stock_data.groupby('종목명'):
        save_path = os.path.join(save_dir, f'{stock_name}_20240730.csv')
        group.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"Saved July 30 data for {stock_name} to {save_path}")

if __name__ == "__main__":
    # 기간 설정
    start_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2024-03-31', '%Y-%m-%d')

    # PBR 데이터가 저장된 디렉토리
    pbr_dir = 'pbr_data'

    # 주가 데이터 가져오기
    stock_names = [f.split('_')[0] for f in os.listdir(pbr_dir) if f.endswith('.csv')]
    stock_df = get_stock_data_for_period(start_date, end_date, stock_names)

    if stock_df is not None and not stock_df.empty:
        print("주가 데이터를 성공적으로 가져왔습니다.")

        # 주가 데이터와 PBR 데이터 병합 및 저장
        stock_data_dir = 'stock_data'
        merge_data(pbr_dir, stock_data_dir, stock_df)
    else:
        print("주가 데이터를 가져오지 못했습니다.")

    # 7월 30일 데이터 가져오기
    july_30 = datetime(2024, 7, 30).strftime('%Y%m%d')
    stock_data_july_30 = get_stock_data_for_date(july_30)

    if stock_data_july_30 is not None and not stock_data_july_30.empty:
        print("7월 30일 주가 데이터를 성공적으로 가져왔습니다.")
        now_dir = 'now'
        save_stock_data_for_july_30(stock_data_july_30, now_dir)
    else:
        print("7월 30일 주가 데이터를 가져오지 못했습니다.")
