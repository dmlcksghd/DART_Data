import requests
import pandas as pd
from io import StringIO

# OTP 생성 URL
otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

# 다운로드 URL
download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

# OTP 요청 헤더
otp_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
}

# OTP 요청 페이로드
otp_payload = {
    'bld': 'dbms/MDC/STAT/standard/MDCSTAT03501',
    'name': 'fileDown',
    'filetype': 'csv',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03501',
    'mktId': 'ALL',       # 전체 시장
    'trdDd': '20240724',  # 최근 거래일자 사용
    'share': '1',         # 매개변수 (필요시 조정)
    'money': '1',         # 매개변수 (필요시 조정)
    'csvxls_isNo': 'false'
}

# OTP 생성 요청
otp_response = requests.post(otp_url, headers=otp_headers, data=otp_payload)
otp = otp_response.text  # OTP 값 추출

# 다운로드 요청 헤더
download_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020502',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': '__smVisitorID=5AohOPWW0ba; JSESSIONID=yZCQcKhrcJJOPjvX1kAmTdKqUEdUJKR6JrrsJR3D5jxgBUkTz4fRAJUN2QWtM1rs.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAwMQ==',
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

# CSV 데이터를 데이터 프레임으로 읽기
csv_content = csv_response.content.decode('euc-kr')  # 한글 인코딩 처리
data = StringIO(csv_content)
df = pd.read_csv(data)

# PBR 값이 1인 종목 찾기
pbr_one_df = df[df['PBR'] == 1]

# PBR 값이 1인 종목 개수 출력
pbr_one_count = pbr_one_df.shape[0]

# 결과 출력
print("PBR 값이 1인 종목 개수:", pbr_one_count)
print(pbr_one_df)
@@@@@@@@@@@
import requests
import pandas as pd
from io import StringIO

# OTP 생성 URL
otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

# 다운로드 URL
download_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

# OTP 요청 헤더
otp_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
}

# OTP 요청 페이로드
otp_payload = {
    'bld': 'dbms/MDC/STAT/standard/MDCSTAT03501',
    'name': 'fileDown',
    'filetype': 'csv',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03501',
    'mktId': 'ALL',       # 전체 시장
    'trdDd': '20240724',  # 최근 거래일자 사용
    'share': '1',         # 매개변수 (필요시 조정)
    'money': '1',         # 매개변수 (필요시 조정)
    'csvxls_isNo': 'false'
}

# OTP 생성 요청
otp_response = requests.post(otp_url, headers=otp_headers, data=otp_payload)
otp = otp_response.text  # OTP 값 추출

# 다운로드 요청 헤더
download_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020502',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': '__smVisitorID=5AohOPWW0ba; JSESSIONID=yZCQcKhrcJJOPjvX1kAmTdKqUEdUJKR6JrrsJR3D5jxgBUkTz4fRAJUN2QWtM1rs.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAwMQ==',
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

# CSV 데이터를 데이터 프레임으로 읽기
csv_content = csv_response.content.decode('euc-kr')  # 한글 인코딩 처리
data = StringIO(csv_content)
df = pd.read_csv(data)

# PBR 값이 1인 종목 찾기
pbr_one_df = df[df['PBR'] == 1]

# 필요한 열만 선택 (ex: '종목명', '주가', 'EPS', 'PER', 'BPS', 'PBR')
selected_columns = ['종목명', '주가', 'EPS', 'PER', 'BPS', 'PBR']
pbr_one_df = pbr_one_df[selected_columns]

# 주가 낮추기 (ex: 90%)
pbr_one_df['낮춘주가'] = pbr_onr_df['주가'] * 0.9

# 모델 학습을 위한 데이터 준비
X = pbr_one_df[['낮춘주가', 'EPS', 'PER', 'BPS', 'PBR']]
y = pbr_one_df['주가']

# 데이터셋을 학습용과 테스트용으로 분리
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 선형 회귀 모델 학습
model = LinearRegression()
model.fit(X_train, y_train)

# 테스트 데이터로 예측 수행
y_pred = model.predict(X_test)

모델 성능 평가
mse = mean_squard_error(y_test, y_pred)
print(f"평균 제곱 오차 (MSE) : {mse}")

# 예측 결과 출력
results_df = pd.DataFrame({'실제 주가': y_test, '예측 주가': y_pred})
print(result_df)

'''
# PBR 값이 1인 종목 개수 출력
pbr_one_count = pbr_one_df.shape[0]

# 결과 출력
print("PBR 값이 1인 종목 개수:", pbr_one_count)
print(pbr_one_df)'''
