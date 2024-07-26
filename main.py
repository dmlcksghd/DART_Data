import pandas as pd
import numpy as np
from financialStatements import get_financial_statements
from stock_data import get_pbr_less_or_equal_stock_data
import os
import ta
from datetime import datetime


def calculate_indicators(df):
    # 기술적 지표 계산
    df['ATR'] = ta.volatility.AverageTrueRange(df['고가'], df['저가'], df['종가']).average_true_range()
    df['Bollinger_High'], df['Bollinger_Low'] = ta.volatility.BollingerBands(
        df['종가']).bollinger_hband(), ta.volatility.BollingerBands(df['종가']).bollinger_lband()
    df['MACD'], df['MACD_Signal'], _ = ta.trend.MACD(df['종가']).macd(), ta.trend.MACD(
        df['종가']).macd_signal(), ta.trend.MACD(df['종가']).macd_diff()
    return df


def prepare_data(start_date, end_date):
    # 재무제표 데이터 가져오기
    financial_data = get_financial_statements(end_date)

    # 주가 데이터 가져오기
    stock_data = get_pbr_less_or_equal_stock_data(start_date, end_date)

    # 기술적 지표 계산
    stock_data = calculate_indicators(stock_data)

    # 데이터 병합
    merged_data = pd.merge(financial_data, stock_data, on='종목명')

    # 필요시 추가 데이터 로드 (예: 지수 데이터)
    # index_data = pd.read_csv('path_to_index_data.csv')
    # merged_data = pd.merge(merged_data, index_data, on='날짜')

    # 필수 항목 제외 결측값 처리
    merged_data.fillna(0, inplace=True)

    return merged_data


if __name__ == "__main__":
    start_date = '20230101'
    end_date = '20240331'
    data = prepare_data(start_date, end_date)
    print(data.head())

    # 데이터를 CSV로 저장하여 training_model.py에서 불러올 수 있도록 합니다.
    save_dir = 'data'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    data.to_csv(os.path.join(save_dir, 'prepared_data.csv'), index=False, encoding='utf-8-sig')
