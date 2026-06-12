# %%
import os

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from geopy.distance import geodesic
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    r2_score,
    root_mean_squared_error,
)

from masters_project.settings import settings

os.chdir("..")
print("Current Working Directory: ", os.getcwd())
sns.set_theme(style="whitegrid")

# %%
model_cachoeira_paulista = joblib.load(
    "data/models/knn_st_2022-01-01_et_2022-01-31_CACHOEIRA_PAULISTA_v1.joblib"
)
model_ourinhos = joblib.load(
    "data/models/knn_st_2022-01-01_et_2022-01-31_OURINHOS_v1.joblib"
)
model_sao_martinho_da_serra = joblib.load(
    "data/models/knn_st_2022-01-01_et_2022-01-31_SAO_MARTINHO_DA_SERRA_v1.joblib"
)

# %%
df_input_test = pd.read_csv(
    "data/processed/model_test_st_2023-01-01_et_2023-01-31_OURINHOS.csv"
)

# Convert timestamp just to be safe for plotting
df_input_test["timestamp"] = pd.to_datetime(df_input_test["timestamp"])

target_col = "glo_avg"
model_name = settings.execution.selected_model
df_final = pd.DataFrame()

# %%
X_input_test = df_input_test.copy().drop(columns=[target_col, "timestamp"])

# %%
loc_cachoeira_paulista = (-22.690, -45.006)  # usar settings depois
loc_ourinhos = (-23.000, -49.844)
loc_sao_martinho_da_serra = (-29.443, -53.823)

# FIX 1: Extract the numerical value (.km) from the geodesic object
distance_our_cp = geodesic(loc_ourinhos, loc_cachoeira_paulista).km
distance_our_sms = geodesic(loc_ourinhos, loc_sao_martinho_da_serra).km

# FIX 2: Swap the numerators for inverse distance weighting (closer = higher weight)
total_distance = distance_our_cp + distance_our_sms
weight_our_cp = distance_our_sms / total_distance
weight_our_sms = distance_our_cp / total_distance

# %%
df_final["timestamp"] = df_input_test["timestamp"]
df_final["y_true"] = df_input_test[target_col]

# 1. Cachoeira Paulista
scaler_CP = model_cachoeira_paulista["scaler"]
# Reordena o X_input_test para bater exatamente com o que o modelo CP espera
X_input_CP = X_input_test[scaler_CP.feature_names_in_]
X_test_scaled_CP = scaler_CP.transform(X_input_CP)
df_final["y_pred_CP"] = model_cachoeira_paulista["model"].predict(X_test_scaled_CP)

# 2. Ourinhos
scaler_OUR = model_ourinhos["scaler"]
# Reordena o X_input_test para bater com o modelo OUR
X_input_OUR = X_input_test[scaler_OUR.feature_names_in_]
X_test_scaled_OUR = scaler_OUR.transform(X_input_OUR)
df_final["y_pred_OUR"] = model_ourinhos["model"].predict(X_test_scaled_OUR)

# 3. São Martinho da Serra
scaler_SMS = model_sao_martinho_da_serra["scaler"]
# Reordena o X_input_test para bater com o modelo SMS
X_input_SMS = X_input_test[scaler_SMS.feature_names_in_]
X_test_scaled_SMS = scaler_SMS.transform(X_input_SMS)
df_final["y_pred_SMS"] = model_sao_martinho_da_serra["model"].predict(X_test_scaled_SMS)

# 4. Calcular a predição ponderada (IDW)
df_final["y_pred_weight_CP_SMS"] = (
    weight_our_cp * df_final["y_pred_CP"] + weight_our_sms * df_final["y_pred_SMS"]
)


# %%
def calcular_metricas(y_true, y_pred, nome_modelo):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = root_mean_squared_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    mask = y_true > 0  # Filter out nighttime/zero irradiance
    if mask.sum() > 0:
        mape = mean_absolute_percentage_error(y_true[mask], y_pred[mask])
    else:
        mape = np.nan
    mbe = np.mean(y_pred - y_true)

    return {
        "Model": nome_modelo,
        "MAE": mae,
        "RMSE": rmse,
        "R²": r2,
        "MAPE": mape,
        "MBE": mbe,
    }


# %%
metricas = [
    calcular_metricas(df_final["y_true"], df_final["y_pred_CP"], "CP"),
    calcular_metricas(df_final["y_true"], df_final["y_pred_OUR"], "OUR"),
    calcular_metricas(df_final["y_true"], df_final["y_pred_SMS"], "SMS"),
    calcular_metricas(df_final["y_true"], df_final["y_pred_weight_CP_SMS"], "CP_SMS"),
]

df_metrics = pd.DataFrame(metricas)
df_metrics.to_csv("data/results/test_metrics.csv", index=False)

# %%
# %%
# Create a figure with 4 subplots (4 rows, 1 column) sharing the same X-axis
fig, axes = plt.subplots(4, 1, figsize=(16, 16), sharex=True)

# List of models, their display labels, and a distinct color for each
models_to_plot = [
    ("y_pred_CP", "Model CP", "tab:blue"),
    ("y_pred_OUR", "Model OUR", "tab:orange"),
    ("y_pred_SMS", "Model SMS", "tab:green"),
    ("y_pred_weight_CP_SMS", "Model CP_SMS", "tab:red"),
]

# Loop through the axes and models to generate each plot
for ax, (col_name, label, color) in zip(axes, models_to_plot):
    # Plot Real Value (always in black)
    ax.plot(
        df_final["timestamp"],
        df_final["y_true"],
        label="Real Value",
        color="black",
        linewidth=2,
    )

    # Plot the specific model's prediction
    ax.plot(
        df_final["timestamp"], df_final[col_name], label=label, color=color, alpha=0.7
    )

    # Format each subplot
    ax.set_title(f"Real Value vs {label}", fontsize=14)
    ax.set_ylabel("Irradiance", fontsize=12)
    ax.legend(loc="upper right")

# Set the X-axis label only on the bottom-most graph
axes[-1].set_xlabel("Time", fontsize=12)

# Adjust spacing, save, and show
plt.tight_layout()
plt.savefig("data/results/timeseries_result.png", dpi=300, bbox_inches="tight")
plt.show()

# %%
fig, axes = plt.subplots(1, 5, figsize=(22, 5))

# Gráfico de MAE
sns.barplot(data=df_metrics, x="Model", y="MAE", ax=axes[0], palette="Blues_d")
axes[0].set_title("Mean Absolute Error (MAE)")

# Gráfico de RMSE
sns.barplot(data=df_metrics, x="Model", y="RMSE", ax=axes[1], palette="Reds_d")
axes[1].set_title("Root Mean Squared Error (RMSE)")

# Gráfico de R²
sns.barplot(data=df_metrics, x="Model", y="R²", ax=axes[2], palette="Greens_d")
axes[2].set_title("Coefficient of Determination (R²)")
axes[2].set_ylim(0, 1.1)  # Increased slightly so the label doesn't hit the ceiling

# Gráfico de MAPE
sns.barplot(data=df_metrics, x="Model", y="MAPE", ax=axes[3], palette="Oranges_d")
axes[3].set_title("Mean Absolute Percentage Error (MAPE)")

# Gráfico de MBE
sns.barplot(data=df_metrics, x="Model", y="MBE", ax=axes[4], palette="Purples_d")
axes[4].set_title("Mean Bias Error (MBE)")

# VISUAL FIX: Loop through all axes and all containers to label every bar
for ax in axes:
    for container in ax.containers:
        ax.bar_label(container, fmt="%.3f", padding=3)

# BEST PRACTICE: tight_layout should go BEFORE savefig
plt.tight_layout()
plt.savefig("data/results/model_metrics.png", dpi=300, bbox_inches="tight")
plt.show()
# %%
