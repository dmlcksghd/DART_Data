import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import os
import holidays

# 공휴일 확인
def is_holiday(date):
    kr_holidays = holidays.KR(years=date.year)
    return date in kr_holidays

# 주말 확인
def is_weekend(date):
    return date.weekday() >= 5  # 토요일(5) 또는 일요일(6)

# 최근 영업일 반환
def get_recent_weekday(date):
    while is_weekend(date) or is_holiday(date):
        date -= timedelta(days=1)
    return date

# 영업일 목록 생성
def get_trading_days(start_date, end_date):
    trading_days = []
    current_date = start_date
    while current_date <= end_date:
        if not is_weekend(current_date) and not is_holiday(current_date):
            trading_days.append(current_date)
        current_date += timedelta(days=1)
    return trading_days

# 날짜 포맷 결정
def format_date(date):
    return date.strftime('%Y%m%d')

# PBR과 PER 데이터 가져오기
def get_pbr_per_data(trdDd):
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
        'trdDd': trdDd,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false'
    }

    try:
        otp_response = requests.post(otp_url, headers=otp_headers, data=otp_payload)
        otp_response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
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
            'Cookie': '_ga=GA1.1.1420298847.1720981646; _ga_EGZWJ6FGKM=GS1.1.1720981858.1.1.1720981880.0.0.0; __smVisitorID=Gp68vON1abl; _ga_1EV6XZXVDT=GS1.1.1720981645.1.1.1720983151.0.0.0; _ga_808R2EHLL3=GS1.1.1720986709.1.1.1720986730.0.0.0; _ga_Z6N0DBVT2W=GS1.1.1721740575.3.0.1721740584.0.0.0',
            'Host': 'data.krx.co.kr',
            'Origin': 'http://data.krx.co.kr',
            'Upgrade-Insecure-Requests': '1'
        }

        download_payload = {
            'code': otp,
            'name': 'fileDown',
            'filetype': 'csv'
        }

        # 재시도 로직 추가
        for attempt in range(3):
            try:
                csv_response = requests.post(download_url, headers=download_headers, data=download_payload)
                csv_response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
                csv_content = csv_response.content.decode('euc-kr')
                data = StringIO(csv_content)
                df = pd.read_csv(data)
                print("컬럼명: ", df.columns)  # 컬럼명 확인

                # 종목명과 종목코드를 포함하여 PBR과 PER 데이터를 필터링
                pbr_per_df = df[df['PBR'] < 1].sort_values(by='PBR', ascending=True).head(10)
                pbr_per_df = pbr_per_df[['종목코드', '종목명', 'PBR', 'PER']]  # 종목코드도 포함
                pbr_per_df['날짜'] = trdDd  # 날짜 칼럼 추가
                return pbr_per_df
            except requests.RequestException as e:
                print(f"다운로드 시도 실패 (시도 {attempt + 1}): {e}")
                if attempt == 2:
                    raise

    except requests.RequestException as e:
        print(f"HTTP 요청 오류: {e}")
        return pd.DataFrame()  # 빈 데이터프레임 반환

# 종목별 데이터 저장
def save_pbr_data_by_stock(pbr_df, save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # 종목명별로 데이터를 분리하여 저장
    grouped = pbr_df.groupby('종목명')
    for stock_name, group_df in grouped:
        file_path = os.path.join(save_dir, f'{stock_name}.csv')
        if not os.path.exists(file_path):
            group_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        else:
            # 파일이 이미 존재하면 데이터 추가
            existing_df = pd.read_csv(file_path)
            updated_df = pd.concat([existing_df, group_df], ignore_index=True).drop_duplicates()
            updated_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"파일 저장: {file_path}")

# 현재 날짜에 대한 PBR 데이터 저장
def save_today_pbr_data():
    try:
        now = datetime.now()
        today = format_date(now)

        # 현재 날짜 데이터 저장
        pbr_per_df_today = get_pbr_per_data(today)
        save_dir_now = 'now_data'
        save_pbr_data_by_stock(pbr_per_df_today, save_dir_now)
        print(f"현재 날짜 ({today})의 PBR 데이터가 {save_dir_now}에 저장되었습니다.")

    except Exception as e:
        print(f"오류 발생: {e}")

# 1년치 PBR 데이터 저장 (2023년)
def save_yearly_pbr_data(start_date, end_date):
    try:
        trading_days = get_trading_days(start_date, end_date)
        save_dir_pbr_data = 'pbr_data'

        all_data = []
        for trading_day in trading_days:
            trdDd = format_date(trading_day)
            pbr_per_df = get_pbr_per_data(trdDd)
            all_data.append(pbr_per_df)

        # 모든 데이터 통합
        all_data_df = pd.concat(all_data, ignore_index=True)
        save_pbr_data_by_stock(all_data_df, save_dir_pbr_data)
        print(f"2023년 PBR 데이터가 {save_dir_pbr_data}에 저장되었습니다.")

    except Exception as e:
        print(f"오류 발생: {e}")

# 테스트 데이터 저장 (2024년 초반)
def save_test_pbr_data(start_date, end_date):
    try:
        trading_days = get_trading_days(start_date, end_date)
        save_dir_test_data = 'pbr_test_data'

        all_data = []
        for trading_day in trading_days:
            trdDd = format_date(trading_day)
            pbr_per_df = get_pbr_per_data(trdDd)
            all_data.append(pbr_per_df)

        # 모든 데이터 통합
        all_data_df = pd.concat(all_data, ignore_index=True)
        save_pbr_data_by_stock(all_data_df, save_dir_test_data)
        print(f"2024년 초반 PBR 테스트 데이터가 {save_dir_test_data}에 저장되었습니다.")

    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    # 현재 날짜의 PBR 데이터 저장
    save_today_pbr_data()

    # 2023년 PBR 데이터 저장
    start_date_2023 = datetime(2023, 1, 2)
    end_date_2023 = datetime(2023, 12, 29)
    save_yearly_pbr_data(start_date_2023, end_date_2023)

    # 2024년 초반 PBR 테스트 데이터 저장
    start_date_2024 = datetime(2024, 1, 2)
    end_date_2024 = datetime(2024, 4, 30)
    save_test_pbr_data(start_date_2024, end_date_2024)
