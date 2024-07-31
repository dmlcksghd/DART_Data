import os
import pandas as pd

def merge_stock_and_index_data(stock_data_dir, index_data_file, now_stock_data_dir, output_dir, now_output_dir):
    # 지수 데이터 읽기
    index_data = pd.read_csv(index_data_file, encoding='utf-8-sig')

    # 날짜를 문자열로 변환 (날짜 형식은 YYYYMMDD)
    index_data['날짜'] = index_data['날짜'].astype(str)

    # 현재 날짜 데이터 추출 (마지막 행)
    current_index_data = index_data.iloc[-1:]

    # 결과 저장 디렉토리 설정
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(now_output_dir):
        os.makedirs(now_output_dir)

    # 주가 데이터 디렉토리의 모든 파일에 대해 반복
    for filename in os.listdir(stock_data_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(stock_data_dir, filename)
            now_file_path = os.path.join(now_stock_data_dir, filename)

            # 종목별 데이터 읽기
            stock_data = pd.read_csv(file_path, encoding='euc-kr')
            # 현재 종목별 데이터 읽기
            now_stock_data = pd.read_csv(now_file_path, encoding='euc-kr')

            # 날짜를 문자열로 변환 (날짜 형식은 YYYYMMDD)
            stock_data['날짜'] = stock_data['날짜'].astype(str)
            now_stock_data['날짜'] = now_stock_data['날짜'].astype(str)

            # 지수 데이터를 종목별 데이터에 병합 (기간 데이터)
            combined_data = pd.merge(stock_data, index_data, on='날짜', how='left')

            # 병합된 기간 데이터를 저장
            output_file_path = os.path.join(output_dir, filename)
            combined_data.to_csv(output_file_path, index=False, encoding='euc-kr')
            print(f"Updated period data saved to {output_file_path}")

            # 현재 데이터 필터링 (오늘 날짜 데이터만 포함)
            today_date = now_stock_data['날짜'].max()  # assuming the last date in the file is the current date
            now_stock_data_today = now_stock_data[now_stock_data['날짜'] == today_date]

            # 오늘 날짜의 지수 데이터를 포함한 병합
            now_combined_data_today = pd.merge(now_stock_data_today, current_index_data, on='날짜', how='left')

            # 현재 데이터를 포함한 병합된 데이터 저장
            now_output_file_path = os.path.join(now_output_dir, filename)
            now_combined_data_today.to_csv(now_output_file_path, index=False, encoding='euc-kr')
            print(f"Updated data with current stock saved to {now_output_file_path}")

if __name__ == "__main__":
    stock_data_dir = 'stock_and_pbr'  # 주가 데이터가 저장된 디렉토리
    index_data_file = 'index_data/index_data.csv'  # 지수 데이터 파일 경로
    now_stock_data_dir = 'now_stock_and_pbr'  # 현재 주가 데이터가 저장된 디렉토리
    output_dir = 'data_merge_final'  # 기간 데이터를 병합한 결과 저장 디렉토리
    now_output_dir = 'now_data_merge_final'  # 현재 데이터를 병합한 결과 저장 디렉토리

    merge_stock_and_index_data(stock_data_dir, index_data_file, now_stock_data_dir, output_dir, now_output_dir)
