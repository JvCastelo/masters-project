# %%
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.metrics import r2_score, root_mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler

# %%
sns.set_theme(style="whitegrid")
# %%
df = pd.read_csv(
    "../data/processed/final_features_st_2022-01-01_et_2022-03-31_CACHOEIRA_PAULISTA.csv"
)
df
# %%
target_col = "glo_avg"
# %%
X = df.copy().drop(columns=[target_col, "timestamp"])
y = df.copy()[target_col]
# %%
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
# %%
scaler = StandardScaler()

X_train_scaled = scaler.fit_transform(X_train)

X_test_scaled = scaler.fit_transform(X_test)
# %%
model = KNeighborsRegressor(n_neighbors=5, n_jobs=-1)
# %%
model.fit(X_train_scaled, y_train)
# %%
y_pred = model.predict(X_test_scaled)
# %%
rmse = root_mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
# %%
plt.figure(figsize=(8, 8))
plt.scatter(y_test, y_pred, alpha=0.3, color="blue")
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], "r--", lw=2)
plt.xlabel("Actual SONDA Radiation")
plt.ylabel("KNN Predicted Radiation")
plt.title("KNN: Actual vs Predicted")
plt.show()
