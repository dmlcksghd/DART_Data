import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import os
import holidays

def is_holiday(date):
    kr_holidays = holidays.KR(years=date.year)
    return date in kr_holidays

def is_last_day_of_month(date):
    next_day = date + timedelta(days=1)
    return next_day.month != date.month

def get_recent_weekday(date):
    while date.weekday() > 4 or is_holiday(date) or is_last_day_of_month(date):
        date -= timedelta(days=1)
    return date

def get_pbr_less_one_companies(trdDd):
    # OTP 생성 URL
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

    # 다운로드 URL
    download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

    # OTP 요청 헤더
    otp_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }

    # OTP 요청 페이로드
    otp_payload = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03501',
        'name': 'fileDown',
        'filetype': 'csv',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03501',
        'mktId': 'ALL',  # 전체 시장
        'trdDd': trdDd,  # 입력된 거래일자 사용
        'share': '1',  # 매개변수 (필요시 조정)
        'money': '1',  # 매개변수 (필요시 조정)
        'csvxls_isNo': 'false'
    }

    # OTP 생성 요청
    otp_response = requests.post(otp_url, headers=otp_headers, data=otp_payload)
    otp = otp_response.text  # OTP 값 추출

    # 다운로드 요청 헤더
    download_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020502',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': '__smVisitorID=5AohOPWW0ba; JSESSIONID=yZCQcKhrcJJOPjvX1kAmTdKqUEdUJKR6JrrsJR3D5jxgBUkTz4fRAJUN2QWtM1rs.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAwMQ==',
        'Host': 'data.krx.co.kr',
        'Origin': 'http://data.krx.co.kr',
        'Upgrade-Insecure-Requests': '1'
    }

    # 다운로드 요청 페이로드
    download_payload = {
        'code': otp,
        'name': 'fileDown',
        'filetype': 'csv'
    }

    # CSV 파일 다운로드
    csv_response = requests.post(download_url, headers=download_headers, data=download_payload)

    # CSV 데이터를 데이터 프레임으로 읽기
    csv_content = csv_response.content.decode('euc-kr')  # 한글 인코딩 처리
    data = StringIO(csv_content)
    df = pd.read_csv(data)

    # PBR 값이 1보다 작은 종목 찾기
    pbr_less_one_df = df[df['PBR'] < 1].sort_values(by='PBR', ascending=True).head(100)

    # 데이터프레임 반환
    return pbr_less_one_df

if __name__ == "__main__":
    # 현재 날짜와 시간으로 trdDd 설정
    today = datetime.now()
    trdDd = today.strftime('%Y%m%d')

    # 현재 날짜와 시간 기록
    request_time = today.strftime('%Y-%m-%d %H:%M:%S')

    # 주말, 공휴일, 월 마지막 날 확인 후 최근 평일로 조정
    date_to_use = get_recent_weekday(today)
    trdDd = date_to_use.strftime('%Y%m%d')

    pbr_less_one_df = get_pbr_less_one_companies(trdDd)

    # 결과 출력
    print("PBR 값이 1보다 작은 종목과 PBR 값 (상위 100개):")
    print(pbr_less_one_df)

    # 저장 디렉토리 설정
    save_dir = 'pbr_data'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # CSV 파일명 결정 (우선순위: 월 마지막 날 > 공휴일 > 주말)
    file_suffix = ''
    if today != date_to_use:
        if is_last_day_of_month(today):
            file_suffix = '_monthlast'
        elif today.weekday() > 4:
            file_suffix = '_weekend'
        elif is_holiday(today):
            file_suffix = '_holiday'


    csv_file_path = os.path.join(save_dir, f'pbr_data_{trdDd}{file_suffix}.csv')

    # CSV 파일로 저장
    pbr_less_one_df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')

    # 요청 시간과 저장 경로 출력
    print(f"데이터 요청 시간: {request_time}")
    print(f"PBR 데이터가 {csv_file_path}에 저장되었습니다.")
