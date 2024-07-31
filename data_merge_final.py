import os
import pandas as pd


def add_index_data_to_files(filtered_data_dir, index_data_file, output_dir):
    # 지수 데이터 읽기
    index_data = pd.read_csv(index_data_file, encoding='utf-8-sig')

    # 날짜를 문자열로 변환 (날짜 형식은 YYYYMMDD)
    index_data['날짜'] = index_data['날짜'].astype(str)

    # stock_and_pbr 디렉토리의 모든 파일에 대해 반복
    for filename in os.listdir(filtered_data_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(filtered_data_dir, filename)

            # 종목별 데이터 읽기
            stock_data = pd.read_csv(file_path, encoding='euc-kr')

            # 날짜를 문자열로 변환 (날짜 형식은 YYYYMMDD)
            stock_data['날짜'] = stock_data['날짜'].astype(str)

            # 지수 데이터를 종목별 데이터에 병합
            combined_data = pd.merge(stock_data, index_data, on='날짜', how='left')

            # 결과 저장 디렉토리 설정
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # 병합된 데이터 저장
            output_file_path = os.path.join(output_dir, filename)
            combined_data.to_csv(output_file_path, index=False, encoding='euc-kr')
            print(f"Updated data saved to {output_file_path}")


if __name__ == "__main__":
    filtered_data_dir = 'stock_and_pbr'  # 주가 데이터가 저장된 디렉토리
    index_data_file = 'index_data/index_data.csv'  # 지수 데이터 파일 경로
    output_dir = 'data_merge_final'  # 결과 저장 디렉토리

    add_index_data_to_files(filtered_data_dir, index_data_file, output_dir)
