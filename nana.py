import requests as rq
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
import re
import boto3  # NCP의 Object Storage를 사용하기 위한 라이브러리


# 네이버 클라우드 플랫폼 Object Storage의 기본 설정
def get_ncp_client():
    return boto3.client(
        's3',
        endpoint_url='https://kr.object.ncloudstorage.com',
        aws_access_key_id='hV9URHB6YPU1yqoZ9bQO',
        aws_secret_access_key='5smCCHD3jIUX66EQvXQc1EEkWcEZejMuUdSqmDeg'
    )


def upload_to_object_storage(file_content, bucket_name, object_name):
    s3 = get_ncp_client()
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=file_content)
    print(f"File uploaded to {bucket_name}/{object_name}")


# 최근 영업일 데이터 추출
def get_recent_business_day():
    url = 'https://finance.naver.com/sise/sise_deposit.nhn'
    data = rq.get(url)
    data_html = BeautifulSoup(data.content, 'html.parser')
    parse_day = data_html.select_one('div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text
    biz_day = re.findall('[0-9]+', parse_day)
    return ''.join(biz_day)


# 업종 분류 현황 크롤링
def get_sector_data(biz_day):
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

    gen_otp_stk = {
        'mktId': 'STK',
        'trdDd': biz_day,
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
    }

    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
    otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
    down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
    sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')

    return sector_stk


# 개별 종목 데이터 크롤링
def get_individual_data(biz_day):
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

    gen_otp_data = {
        'searchType': '1',
        'mktId': 'ALL',
        'trdDd': biz_day,
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
    }

    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
    otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text
    krx_ind = rq.post(down_url, {'code': otp}, headers=headers)
    krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')

    return krx_ind


# 주식 데이터 크롤링 및 NCP Object Storage에 저장
def process_and_upload_data():
    biz_day = get_recent_business_day()

    # 업종 분류 데이터 크롤링
    sector_stk = get_sector_data(biz_day)

    # 개별 종목 데이터 크롤링
    krx_ind = get_individual_data(biz_day)

    # DataFrame을 CSV로 변환
    sector_stk_csv = BytesIO()
    sector_stk.to_csv(sector_stk_csv, index=False, encoding='utf-8')
    sector_stk_csv.seek(0)

    ind_data_csv = BytesIO()
    krx_ind.to_csv(ind_data_csv, index=False, encoding='utf-8')
    ind_data_csv.seek(0)

    # NCP Object Storage에 업로드
    upload_to_object_storage(sector_stk_csv, 'financial', f'sector_data_{biz_day}.csv')
    upload_to_object_storage(ind_data_csv, 'financial', f'individual_data_{biz_day}.csv')
g

if __name__ == "__main__":
    process_and_upload_data()
