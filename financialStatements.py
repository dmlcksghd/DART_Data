import dart_fss
import pandas as pd
import os

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

# 재무제표 데이터를 분할하여 저장 및 출력
def split_report(corp_name, bsns_year, num, df):
    """재무제표 데이터를 각각의 CSV 파일로 저장하고 출력하는 함수"""
    # 필요한 항목만 추출
    items_of_interest = {
        '자산 총액': 'ifrs-full_Assets',
        '부채 총액': 'ifrs-full_Liabilities',
        '자본 총액': 'ifrs-full_Equity',
        '매출액': 'ifrs-full_Revenue',
        '영업이익': 'dart_OperatingIncomeLoss',
        '순이익': 'ifrs-full_ProfitLoss',
        '현금 흐름': 'ifrs-full_CashFlowsFromUsedInOperatingActivities'
    }

    report_data = {}

    for item_name, item_code in items_of_interest.items():
        filtered_df = df[df['account_id'] == item_code]
        if not filtered_df.empty:
            report_data[item_name] = filtered_df.iloc[0]['thstrm_amount']
        else:
            report_data[item_name] = None

    report_df = pd.DataFrame([report_data])

    # 디렉토리 생성
    directory = 'financial_reports'
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_name = os.path.join(directory, f'{corp_name}_{bsns_year}_{num}Q_report.csv')
    report_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f'{file_name} 파일로 저장되었습니다.')

    return report_df