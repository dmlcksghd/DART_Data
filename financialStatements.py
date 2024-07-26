import os
import dart_fss
import pandas as pd
from pbr_data import get_pbr_less_one_companies
from stock_data import get_stock_data, extract_and_save_listing_shares_and_names, find_latest_csv
from datetime import datetime

# DART API 키 설정
api_key = "016a4403b670e2278235ce4bd28752e47bb33a30"
dart_fss.set_api_key(api_key=api_key)

# 종목 목록 가져오기
corp_list = dart_fss.get_corp_list()
all_corps = dart_fss.api.filings.get_corp_code()
df = pd.DataFrame(all_corps)
df_listed = df[df['stock_code'].notnull()].reset_index(drop=True)

# 특정 재무제표 데이터 가져오기
def get_report(corp_df, corp_name, bsns_year, num, fs_div):
    corp_code = corp_df[corp_df['corp_name'] == corp_name].iloc[0, 0]
    bsns_year = str(bsns_year)

    if num == '4':
        reprt_code = '11011'
    elif num == '3':
        reprt_code = '11014'
    elif num == '2':
        reprt_code = '11012'
    else:
        reprt_code = '11013'

    data = dart_fss.api.finance.fnltt_singl_acnt_all(corp_code, bsns_year, reprt_code, fs_div, api_key=api_key)['list']
    df = pd.DataFrame(data)

    return df

# 재무제표 데이터를 분할하여 저장 및 출력
def split_report(corp_name, bsns_year, num, df, listing_shares_df):
    items_of_interest = {
        '자산 총액': 'ifrs-full_Assets',
        '부채 총액': 'ifrs-full_Liabilities',
        '자본 총액': 'ifrs-full_Equity',
        '매출액': 'ifrs-full_Revenue',
        '영업이익': 'dart_OperatingIncomeLoss',
        '순이익': 'ifrs-full_ProfitLoss',
        '현금 흐름': 'ifrs-full_CashFlowsFromUsedInOperatingActivities'
    }

    report_data = {'종목명': corp_name}

    for item_name, item_code in items_of_interest.items():
        filtered_df = df[df['account_id'] == item_code]
        if not filtered_df.empty:
            report_data[item_name] = filtered_df.iloc[0]['thstrm_amount']
        else:
            report_data[item_name] = None

    report_df = pd.DataFrame([report_data])

    # 상장주식수와 조인
    report_df = report_df.merge(listing_shares_df, on='종목명', how='left')

    # 디렉토리 생성
    if not os.path.exists('financialStatements'):
        os.makedirs('financialStatements')

    file_name = os.path.join('financialStatements', f'{corp_name}_{bsns_year}_{num}Q_report.csv')
    report_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f'{file_name} 파일로 저장되었습니다.')

    return report_df

def get_financial_statements(trdDd, listing_shares_df):
    pbr_less_one_df = get_pbr_less_one_companies(trdDd)

    # 현재 연도와 과거 3년 포함
    current_year = datetime.now().year
    years = [str(year) for year in range(current_year - 1, current_year + 1)]

    quarters = ['1', '2', '3', '4']  # 모든 분기 포함
    fs_div = 'CFS'

    financial_data = []

    for corp_name in pbr_less_one_df['종목명'].tolist():
        for year in years:
            for quarter in quarters:
                try:
                    df = get_report(df_listed, corp_name, year.strip(), quarter.strip(), fs_div)
                    report_df = split_report(corp_name, year.strip(), quarter.strip(), df, listing_shares_df)
                    financial_data.append(report_df)
                except Exception as e:
                    print(f'{corp_name}의 {year}년 {quarter}분기 데이터 가져오기에 실패했습니다: {e}')

    return pd.concat(financial_data, ignore_index=True)

if __name__ == "__main__":
    trdDd = '20240726'   #datetime.now().strftime('%Y%m%d')

    # 최근에 생성된 CSV 파일에서 상장주식수와 종목명 컬럼만 추출하여 데이터프레임으로 로드
    save_dir = 'stock_data'
    latest_csv_file = find_latest_csv(save_dir)
    listing_shares_df = pd.read_csv(latest_csv_file, encoding='cp949', usecols=['종목명', '상장주식수'])

    financial_statements = get_financial_statements(trdDd, listing_shares_df)
    print(financial_statements)
