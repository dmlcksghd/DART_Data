import dart_fss
import pandas as pd
from fpdf import FPDF

# DART API 키 설정
api_key = ""
dart_fss.set_api_key(api_key=api_key)

# 종목 목록 가져오기
corp_list = dart_fss.get_corp_list()
all_corps = dart_fss.api.filings.get_corp_code()
df = pd.DataFrame(all_corps)
df_listed = df[df['stock_code'].notnull()].reset_index(drop=True)


# 특정 재무제표 데이터 가져오기
def get_report(corp_df, corp_name, bsns_year, num, fs_div):
    """기업명과 사업연도, 분기, 재무제표 구분을 입력받아 재무제표 데이터를 가져오는 함수"""
    corp_code = corp_df[corp_df['corp_name'] == corp_name].iloc[0, 0]
    bsns_year = str(bsns_year)

    if num == '4':
        reprt_code = '11011'
    elif num == '3':
        reprt_code = '11014'
    elif num == '2':
        reprt_code = '11012'
    else:  # num == 1 or else
        reprt_code = '11013'

    data = dart_fss.api.finance.fnltt_singl_acnt_all(corp_code, bsns_year, reprt_code, fs_div, api_key=api_key)['list']
    df = pd.DataFrame(data)

    return df


# pandas 출력 옵션 설정
pd.set_option('display.max_columns', None)  # 모든 열 출력
pd.set_option('display.max_rows', None)  # 모든 행 출력
pd.set_option('display.max_colwidth', None)  # 모든 열 너비 출력


# 영업이익과 총매출 데이터 추출
def extract_key_metrics(IS):
    # 데이터 확인을 위해 출력
    print("손익계산서 데이터:")
    print(IS)

    # 영업이익과 총매출 데이터 추출
    try:
        sales = IS[IS['account_nm'].isin(['수익(매출액)', '매출액'])]['thstrm_amount'].values[0]
    except IndexError:
        sales = '데이터 없음'

    try:
        operating_income = IS[IS['account_nm'] == '영업이익']['thstrm_amount'].values[0]
    except IndexError:
        operating_income = '데이터 없음'

    return sales, operating_income


# PDF 생성
def create_pdf(corp_name, year, quarter, sales, operating_income):
    pdf = FPDF()
    pdf.add_page()
    font_path = 'NanumGothic.ttf'  # NanumGothic 폰트 파일 경로
    pdf.add_font('NanumGothic', '', font_path)
    pdf.set_font('NanumGothic', size=12)

    pdf.cell(200, 10, text=f"{corp_name} {year}년 {quarter}분기 재무보고서", new_x="LMARGIN", new_y="NEXT", align='C')

    pdf.ln(10)
    pdf.cell(200, 10, text=f"총매출: {sales}", new_x="LMARGIN", new_y="NEXT", align='L')
    pdf.cell(200, 10, text=f"영업이익: {operating_income}", new_x="LMARGIN", new_y="NEXT", align='L')

    pdf.output(f"{corp_name}_{year}_{quarter}_재무보고서.pdf")


# 사용자 입력 받기
corp_name = input("종목명을 입력하세요: ")
years = input("조회할 연도들을 입력하세요 (콤마로 구분, 예: 2022,2023,2024): ").split(',')
quarters = input("조회할 분기들을 입력하세요 (콤마로 구분, 예: 1,2,3,4): ").split(',')
fs_div = 'CFS'  # 연결재무제표

for year in years:
    for quarter in quarters:
        try:
            print(f'{corp_name}의 {year}년 {quarter}분기 데이터를 가져오는 중...')
            df = get_report(df_listed, corp_name, year.strip(), quarter.strip(), fs_div)

            # 손익계산서 추출
            IS = df[df['sj_div'] == 'IS'].dropna(axis=1).reset_index(drop=True)

            # 영업이익 및 총매출 추출
            sales, operating_income = extract_key_metrics(IS)

            # PDF 생성
            create_pdf(corp_name, year.strip(), quarter.strip(), sales, operating_income)

        except Exception as e:
            print(f'{corp_name}의 {year}년 {quarter}분기 데이터 가져오기에 실패했습니다: {e}')
