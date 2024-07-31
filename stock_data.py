import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import holidays
import time
import os


# 공휴일을 확인하는 함수
def is_holiday(date):
    kr_holidays = holidays.KR(years=date.year)
    return date in kr_holidays


# 가장 최근의 영업일을 찾는 함수
def get_recent_weekday(date):
    while date.weekday() > 4 or is_holiday(date):
        date -= timedelta(days=1)
    return date


# 시작일과 종료일 사이의 영업일 리스트를 반환하는 함수
def get_business_days(start_date, end_date):
    business_days = []
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5 and not is_holiday(current_date):
            business_days.append(current_date)
        current_date += timedelta(days=1)
    return business_days


# 특정 날짜의 주가 데이터를 가져오는 함수
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

            # 필요한 컬럼만 선택 (등락률 추가)
            df = df[['종목코드', '종목명', '날짜', '종가', '거래량', '상장주식수', '등락률']]

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


# 특정 기간의 주가 데이터를 가져오는 함수
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
            print(all_data)
        else:
            print(f"Failed to retrieve stock data for {trdDd}")

    return all_data


def get_pbr_less_one_companies(trdDd):
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

    otp_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }

    otp_payload = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03501',
        'name': 'fileDown',
        'filetype': 'csv',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03501',
        'mktId': 'ALL',
        'trdDd': f'{trdDd}',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false'
    }

    otp_response = requests.post(otp_url, headers=otp_headers, data=otp_payload)
    otp = otp_response.text

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
        'code': otp,
        'name': 'fileDown',
        'filetype': 'csv'
    }

    csv_response = requests.post(download_url, headers=download_headers, data=download_payload)
    csv_content = csv_response.content.decode('euc-kr')
    data = StringIO(csv_content)
    df = pd.read_csv(data)

    # 필요한 컬럼만 선택
    df = df[['종목코드', '종목명', 'EPS', 'BPS', 'PER', 'PBR', '배당수익률']]

    # ROE 계산
    df['ROE'] = df.apply(lambda row: (row['EPS'] / row['BPS']) * 100 if row['BPS'] != 0 else None, axis=1)

    return df


# 주가 데이터를 병합하는 함수
def merge_stock_data(df1, df2):
    df = pd.merge(left=df1, right=df2, how='left', on=['종목코드', '종목명'])
    return df


if __name__ == '__main__':
    start_date = datetime(2024, 3, 1)
    end_date = datetime(2024, 3, 10)

    now = datetime.now()
    if now.hour < 10:
        date = get_recent_weekday(now - timedelta(days=1)).strftime('%Y%m%d')
    else:
        date = get_recent_weekday(now).strftime('%Y%m%d')

    pbr_data = get_pbr_less_one_companies(date)

    stock_names = pbr_data['종목명'].tolist()
    stock_data = get_stock_data_for_period(start_date, end_date, stock_names)

    # 데이터 병합
    filtered_data = merge_stock_data(pbr_data, stock_data)

    # 디렉토리 생성
    output_dir = 'stock_and_pbr'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    now_output_dir = 'now_stock_and_pbr'
    if not os.path.exists(now_output_dir):
        os.makedirs(now_output_dir)

    # 종목명별로 파일 저장
    for stock_name in stock_names:
        stock_specific_data = filtered_data[filtered_data['종목명'] == stock_name]
        file_path = os.path.join(output_dir, f'{stock_name}.csv')
        stock_specific_data.to_csv(file_path, index=False, encoding='euc-kr')

    # 오늘의 데이터 저장
    today_stock_data = get_stock_data_for_date(date)
    if today_stock_data is not None:
        today_filtered_data = today_stock_data[today_stock_data['종목명'].isin(stock_names)]
        today_filtered_data = merge_stock_data(pbr_data, today_filtered_data)
        for stock_name in stock_names:
            stock_specific_data = today_filtered_data[today_filtered_data['종목명'] == stock_name]
            file_path = os.path.join(now_output_dir, f'{stock_name}.csv')
            stock_specific_data.to_csv(file_path, index=False, encoding='euc-kr')

    print(f"Data saved to {output_dir} and today's data saved to {now_output_dir} directory.")
