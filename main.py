import requests
from bs4 import BeautifulSoup
from io import BytesIO
import pdfplumber
import csv

# PDF 다운로드 링크가 포함된 HTML 페이지 요청
url = "https://dart.fss.or.kr/pdf/download/main.do?rcp_no=20240516001421&dcm_no=9949777"
resp = requests.get(url)

# BeautifulSoup을 사용하여 HTML 파싱
soup = BeautifulSoup(resp.content, 'html.parser')

# 다운로드 링크 추출
download_tag = soup.find('a', class_='btnFile')
if download_tag:
    download_link = download_tag['href']
    base_url = "https://dart.fss.or.kr"
    pdf_url = base_url + download_link

    # 실제 PDF 파일 다운로드 (추가 요청 헤더 포함)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': url  # Referer 헤더 추가
    }
    pdf_resp = requests.get(pdf_url, headers=headers)
    pdf_file = BytesIO(pdf_resp.content)

    # PDF 파일을 로컬에 저장
    with open("downloaded_file.pdf", "wb") as file:
        file.write(pdf_resp.content)

    print("PDF 파일이 로컬에 저장되었습니다: 'downloaded_file.pdf'")

    # PDF 파일을 처리하여 특정 섹션의 표 추출 및 CSV 파일로 저장
    try:
        with pdfplumber.open(pdf_file) as pdf:
            sections = ["연결 재무상태표", "연결 손익계산서", "연결 포괄손익계산서", "연결 자본변동표", "연결 현금흐름표"]
            tables_combined = []
            extracting = False

            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    if any(section in text for section in sections):
                        extracting = True

                    if extracting:
                        print(f"테이블이 포함된 페이지 {page.page_number}를 처리 중입니다.")
                        tables = page.extract_tables()
                        for table in tables:
                            if table:
                                tables_combined.extend(table)

                    if "재무에 관한 사항" not in text and extracting:
                        extracting = False

            if tables_combined:
                with open("financial_statements.csv", mode='w', newline='', encoding='utf-8') as csv_file:
                    csv_writer = csv.writer(csv_file)
                    for row in tables_combined:
                        if any(cell for cell in row if cell):  # 빈 행은 무시
                            csv_writer.writerow(row)
                print("연결재무제표가 'financial_statements.csv' 파일로 저장되었습니다.")
            else:
                print("연결재무제표를 포함한 페이지를 찾을 수 없습니다.")
    except Exception as e:
        print(f"PDF 파일을 처리하는 중 오류가 발생했습니다: {e}")
else:
    print("다운로드 링크를 찾을 수 없습니다.")
