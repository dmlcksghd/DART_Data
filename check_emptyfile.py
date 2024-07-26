import os
import pandas as pd

def check_empty_files(directory):
    # 빈 파일 리스트 초기화
    empty_files = []

    # 디렉토리 내의 파일들 순회
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            # 파일 크기 확인
            if os.path.getsize(file_path) == 0:
                empty_files.append(filename)
            else:
                # 파일이 비어있는지 pandas를 통해 확인 (인코딩 지정)
                try:
                    df = pd.read_csv(file_path, encoding='euc-kr')
                    if df.empty:
                        empty_files.append(filename)
                except Exception as e:
                    print(f"Error reading {filename}: {e}")

    return empty_files

if __name__ == "__main__":
    directory = 'stock_data'
    empty_files = check_empty_files(directory)

    if empty_files:
        print(f"빈 파일 목록: {empty_files}")
    else:
        print("빈 파일이 없습니다.")
