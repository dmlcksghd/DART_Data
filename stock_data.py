import requests
import pandas as pd
from io import StringIO
from pbr_data import get_pbr_less_one_companies, is_holiday, is_last_day_of_month, get_recent_weekday
import os
from datetime import datetime, timedelta
import holidays
import time

# 한국 공휴일 데이터 로드 (korean-holidays 라이브러리 사용)
kr_holidays = holidays.KR(years=[2023, 2024])  # 공휴일 범위 설정


def is_trading_day(date):
    """ 주말과 공휴일을 제외한 거래일 확인 """
    if date.weekday() >= 5:  # 토요일(5)과 일요일(6)은 거래일이 아님
        return False
    if date in kr_holidays:
        return False
    return True


def get_stock_data(trdDd, retries=3, backoff_factor=1.0):
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

    otp_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=EUC-KR',
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
        'code': '',
        'name': 'fileDown',
        'filetype': 'csv'
    }

    with requests.Session() as session:
        for attempt in range(retries):
            try:
                otp_response = session.post(otp_url, headers=otp_headers, data=otp_payload)
                otp_response.raise_for_status()
                download_payload['code'] = otp_response.text

                csv_response = session.post(download_url, headers=download_headers, data=download_payload)
                csv_response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)

                csv_content = csv_response.content.decode('euc-kr')
                data = StringIO(csv_content)
                stock_df = pd.read_csv(data)

                return stock_df
            except (requests.RequestException, requests.exceptions.ChunkedEncodingError) as e:
                if attempt < retries - 1:
                    time.sleep(backoff_factor * (2 ** attempt))  # Exponential backoff
                else:
                    raise e


def get_filtered_stock_data(start_date, end_date, pbr_stocks):
    stock_data_list = []

    date_range = pd.date_range(start=start_date, end=end_date)
    for single_date in date_range:
        if not is_trading_day(single_date):
            continue  # Skip holidays and weekends

        trdDd = single_date.strftime('%Y%m%d')
        print(f"Downloading data for {trdDd}...")
        stock_data = get_stock_data(trdDd)
        filtered_stock_data = stock_data[stock_data['종목명'].isin(pbr_stocks['종목명'])].copy()
        filtered_stock_data.loc[:, '날짜'] = trdDd
        stock_data_list.append(filtered_stock_data)

    if not stock_data_list:
        raise ValueError("No trading days found in the given range.")

    all_filtered_stock_data = pd.concat(stock_data_list, ignore_index=True)

    return all_filtered_stock_data


def save_individual_stock_files(final_data, save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    grouped = final_data.groupby('종목명')
    for stock_name, group in grouped:
        group.to_csv(os.path.join(save_dir, f'{stock_name}.csv'), index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 3, 31)

    # PBR 데이터에서 종목명과 PBR 값을 가져오기
    pbr_data, _ = get_pbr_less_one_companies(end_date.strftime('%Y%m%d'))

    all_filtered_stock_data = get_filtered_stock_data(start_date, end_date, pbr_data)

    # PBR 데이터를 종목명 기준으로 주가 데이터와 병합
    final_data = pd.merge(all_filtered_stock_data, pbr_data[['종목명', 'PBR']], on='종목명')

    # 필요한 컬럼만 추출
    columns_needed = ['날짜', '종목명', 'PBR', '시가', '고가', '저가', '종가', '거래량', '상장주식수']
    final_filtered_data = final_data[columns_needed]

    # 개별 종목별로 CSV 파일 저장
    save_dir = 'stock_data'
    save_individual_stock_files(final_filtered_data, save_dir)
    print("Individual Stock Data CSV 파일 저장 완료")
