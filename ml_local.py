import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error

# load CSV
df = pd.read_csv("ml_dataset/part-00000*.csv")

X = df[["PULocationID", "pickup_hour", "pickup_day_of_week", "pickup_month", "is_weekend"]]
y = df["trip_count"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = RandomForestRegressor(n_estimators=50)
model.fit(X_train, y_train)

pred = model.predict(X_test)

rmse = mean_squared_error(y_test, pred, squared=False)
mae = mean_absolute_error(y_test, pred)

print("RMSE:", rmse)
print("MAE:", mae)