import requests
import pandas as pd
from io import StringIO
from data_PBR import get_pbr_one_companies

def get_stock_data(trdDd):
    # OTP 생성 URL (주가 데이터용)
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

    # 다운로드 URL
    download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

    # OTP 요청 헤더
    otp_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }

    # OTP 요청 페이로드 (주가 데이터용)
    otp_payload = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',  # 주가 데이터에 해당하는 bld 값
        'name': 'fileDown',
        'filetype': 'csv',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01501',
        'mktId': 'ALL',       # 전체 시장
        'trdDd': trdDd,  # 거래일자
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false'
    }

    # OTP 생성 요청
    otp_response = requests.post(otp_url, headers=otp_headers, data=otp_payload)
    otp = otp_response.text  # OTP 값 추출

    # 다운로드 요청 헤더
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

    # 다운로드 요청 페이로드
    download_payload = {
        'code': otp,
        'name': 'fileDown',
        'filetype': 'csv'
    }

    # CSV 파일 다운로드
    csv_response = requests.post(download_url, headers=download_headers, data=download_payload)

    # CSV 파일 저장
    csv_file_path = 'stock_data.csv'
    with open(csv_file_path, 'wb') as file:
        file.write(csv_response.content)

    # CSV 데이터를 데이터 프레임으로 읽기
    csv_content = csv_response.content.decode('euc-kr')  # 한글 인코딩 처리
    data = StringIO(csv_content)
    stock_df = pd.read_csv(data)

    return stock_df

def get_pbr_one_stock_data(trdDd):
    # 전체 종목 주가 데이터 가져오기
    stock_data = get_stock_data(trdDd)

    # PBR이 1인 기업들의 종목명 가져오기
    pbr_one_names, _ = get_pbr_one_companies(trdDd)

    # PBR이 1인 기업들의 주가 데이터 필터링
    pbr_one_stock_data = stock_data[stock_data['종목명'].isin(pbr_one_names)]

    return pbr_one_stock_data

if __name__ == "__main__":
    trdDd = '20240724'
    # 전체 종목 주가 데이터 가져오기
    stock_data = get_stock_data(trdDd)

    # PBR이 1인 기업들의 종목명 가져오기
    pbr_one_names, pbr_less_one_df = get_pbr_one_companies(trdDd)

    # PBR이 1인 기업들의 주가 데이터 필터링
    pbr_one_stock_data = stock_data[stock_data['종목명'].isin(pbr_one_names)]
    # 결과 출력
    print(pbr_one_stock_data)

    # PBR이 1인 기업들의 주가 데이터를 CSV 파일로 저장
    pbr_one_stock_data.to_csv('PBR_One_Stock_Data.csv', index=False, encoding='utf-8-sig')

    # stock_data = get_stock_data(trdDd)
    # print(stock_data.head())
