import pandas as pd
from stock import get_pbr_one_stock_data
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# PBR이 1인 기업들의 주가 데이터 가져오기
trdDd = '20240724'
pbr_one_stock_data = get_pbr_one_stock_data(trdDd)

# 인위적으로 주가를 낮춘 데이터 생성
pbr_one_stock_data['Lowered_Price'] = pbr_one_stock_data['종가'] * 0.7

# 데이터 분할
X = pbr_one_stock_data['Lowered_Price'].values.reshape(-1, 1)
y = pbr_one_stock_data['종가'].values
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

# 모델 학습
model = LinearRegression()
model.fit(X_train, y_train)

# 예측
pbr_one_stock_data['Predicted_Price'] = model.predict(X)

# 결과 출력
print(pbr_one_stock_data[['종목명', 'Lowered_Price', '종가', 'Predicted_Price']])

# 모델 평가
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
print(f'Mean Squared Error: {mse}')
print(f'R^2 Score: {r2}')

# 인위적으로 낮춘 데이터와 예측 데이터 시각화
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.plot(pbr_one_stock_data['종가'].values, label='Actual Price', color='blue')
plt.plot(pbr_one_stock_data['Lowered_Price'].values, label='Lowered Price', color='orange')
plt.plot(pbr_one_stock_data['Predicted_Price'].values, label='Predicted Price', color='green')
plt.xlabel('Sample Index')
plt.ylabel('Price')
plt.legend()
plt.title('Actual, Lowered, and Predicted Prices')
plt.show()
