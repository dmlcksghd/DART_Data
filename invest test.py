import pandas as pd
import lxml
import requests
from bs4 import BeautifulSoup

def get_low_pbr():
    url = 'https://www.hisl.co.kr/new_home/community/search_stock_4.php?mcode=03_05&s_sect1=&s_sect2=&s_sect3=1_2'
    response = requests.get(url)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'lxml')

    table = soup.select_one('#contents > div.subcontents > div.subright > div > div.sub_list_mid > table')
    df = pd.read_html(str(table))[0]

    return df


if __name__=='__main__':
    get_low_pbr()
    