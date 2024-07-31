import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler

# 디렉토리 경로 설정
data_merge_final_dir = 'data_merge_final'  # 데이터가 저장된 디렉토리
data_output_dir = 'data'

# 가중치 설정 (조정된 가중치 예시)
weights = {
    'PER': 0.30,
    'Dividend Yield': 0.20,
    'ROE': 0.30,
    'PBR': 0.20
}

def load_and_process_file(file_path):
    try:
        # 데이터 로드
        data = pd.read_csv(file_path, sep=',', encoding='euc-kr')  # 데이터 인코딩과 구분자 확인

        # 필요한 컬럼만 선택하고 결측값 제거
        columns = ['종목코드', '종목명', 'EPS', 'BPS', 'PER', 'PBR', '배당수익률', 'ROE', '날짜', '종가', '거래량', '상장주식수', '등락률',
                   'KOSPI', 'KOSDAQ', 'NASDAQ', 'Dow Jones', 'S&P 500', 'Nikkei']

        # 실제 존재하는 컬럼만 선택
        existing_columns = [col for col in columns if col in data.columns]
        data = data[existing_columns].dropna()

        return data

    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # 오류가 발생하면 빈 데이터프레임 반환

# 모든 파일에서 데이터 로드 및 처리
all_data = []
for file_name in os.listdir(data_merge_final_dir):
    if file_name.endswith('.csv'):
        file_path = os.path.join(data_merge_final_dir, file_name)
        df = load_and_process_file(file_path)
        all_data.append(df)

# 데이터 병합
full_data = pd.concat(all_data, ignore_index=True)

# 데이터 정규화
scaler = MinMaxScaler()
full_data[['Normalized PER', 'Normalized 배당수익률', 'Normalized ROE', 'Normalized PBR']] = scaler.fit_transform(
    full_data[['PER', '배당수익률', 'ROE', 'PBR']]
)

# 가중치 적용
full_data['Final Score'] = (
    (1 - full_data['Normalized PER']) * weights['PER'] +
    full_data['Normalized 배당수익률'] * weights['Dividend Yield'] +
    full_data['Normalized ROE'] * weights['ROE'] +
    (1 - full_data['Normalized PBR']) * weights['PBR']
)

# 종목명 기준으로 상위 50개 기업 선정
ranked_companies = full_data.groupby('종목명').mean().sort_values(by='Final Score', ascending=False)
top_50_companies = ranked_companies.head(50).copy()  # 명시적으로 복사

# 상위 50개 기업의 데이터 추출 및 저장
for file_name in os.listdir(data_merge_final_dir):
    if file_name.endswith('.csv'):
        file_path = os.path.join(data_merge_final_dir, file_name)
        df = load_and_process_file(file_path)

        # 종목 코드 또는 이름이 상위 50개 목록에 있는지 확인
        for company_name in top_50_companies.index:
            if company_name in df['종목명'].values:
                output_path = os.path.join(data_output_dir, f'{company_name}.csv')
                df[df['종목명'] == company_name].to_csv(output_path, index=False, encoding='euc-kr')
                print(f'Saved data for {company_name} to {output_path}')

# 상위 50개 종목의 순위와 점수를 포함한 CSV 파일 생성
top_50_companies['Rank'] = range(1, 51)
top_50_companies = top_50_companies.reset_index()[['Rank', '종목코드', '종목명', 'Final Score']]
top_50_companies.rename(columns={'Final Score': 'Score'}, inplace=True)

# 순위와 점수가 포함된 파일 저장
rank_file_path = os.path.join(data_output_dir, 'top_50_companies_ranking.csv')
top_50_companies.to_csv(rank_file_path, index=False, encoding='euc-kr')
print(f"Ranking data saved to {rank_file_path}")

print('Processing complete.')
