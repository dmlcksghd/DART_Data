import dart_fss
import pandas as pd

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
    df.to_excel(f'{corp_name} {bsns_year}년 {num}분기 재무보고서.xlsx')

    return df

# pandas 출력 옵션 설정
pd.set_option('display.max_columns', None)  # 모든 열 출력
pd.set_option('display.max_rows', None)  # 모든 행 출력
pd.set_option('display.max_colwidth', None)  # 모든 열 너비 출력

# 재무제표 데이터를 분할하여 저장 및 출력
def split_report(corp_name, bsns_year, num, df):
    """재무제표 데이터를 각각의 Excel 파일로 저장하고 출력하는 함수"""
    BS = df[df['sj_div'] == 'BS'].dropna(axis=1).reset_index(drop=True)  # 재무상태표
    IS = df[df['sj_div'] == 'IS'].dropna(axis=1).reset_index(drop=True)  # 손익계산서
    CF = df[df['sj_div'] == 'CF'].dropna(axis=1).reset_index(drop=True)  # 현금흐름표
    CIS = df[df['sj_div'] == 'CIS'].dropna(axis=1).reset_index(drop=True)  # 포괄손익계산서
    SCE = df[df['sj_div'] == 'SCE'].dropna(axis=1).reset_index(drop=True)  # 자본변동표

    # 샘플 데이터 출력
    print("재무상태표(BS):")
    print(BS.head(), "\n")
    print("손익계산서(IS):")
    print(IS.head(), "\n")
    print("현금흐름표(CF):")
    print(CF.head(), "\n")
    print("포괄손익계산서(CIS):")
    print(CIS.head(), "\n")
    print("자본변동표(SCE):")
    print(SCE.head(), "\n")

    BS.to_excel(f'{corp_name} {bsns_year}년 {num}분기 재무상태표.xlsx')
    IS.to_excel(f'{corp_name} {bsns_year}년 {num}분기 손익계산서.xlsx')
    CF.to_excel(f'{corp_name} {bsns_year}년 {num}분기 현금흐름표.xlsx')
    CIS.to_excel(f'{corp_name} {bsns_year}년 {num}분기 포괄손익계산서.xlsx')
    SCE.to_excel(f'{corp_name} {bsns_year}년 {num}분기 자본변동표.xlsx')

    return BS, IS, CF, CIS, SCE

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
            BS, IS, CF, CIS, SCE = split_report(corp_name, year.strip(), quarter.strip(), df)
        except Exception as e:
            print(f'{corp_name}의 {year}년 {quarter}분기 데이터 가져오기에 실패했습니다: {e}')
